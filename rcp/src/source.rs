use anyhow::Context;
use async_recursion::async_recursion;
// trait-only import: brings FileMeta::size()/uid()/... into scope without shadowing std::fs::Metadata
use common::preserve::Metadata as _;
use common::safedir::Dir;
use remote::protocol::ExtendedMetadataCapture;
use std::collections::HashMap;
use std::os::fd::AsFd as _;
use std::sync::Arc;
use tracing::{Instrument, instrument};

fn progress() -> &'static common::progress::Progress {
    common::get_progress()
}

/// How the source reads file DATA for this copy. Chosen once per operation in
/// [`handle_connection`] and threaded through both passes.
///
/// This makes the hardened/dereference distinction *explicit* instead of encoding
/// it as `Option<Arc<SourceDirMap>>`, where `None` (dereference) and `Some(map)` +
/// a lookup *miss* both collapsed into the same path-based fallback. That collapse
/// was a TOCTOU fail-*open*: in hardened mode the held directory fd IS the safety
/// boundary, so a miss means the pinned handle is gone and the source MUST fail
/// closed rather than silently re-resolve the data read by path.
///
/// - [`SourceRead::Hardened`]: every directory is opened `O_NOFOLLOW` from its
///   parent's held fd (Pass 1) and its `Arc<Dir>` stored in the [`SourceDirMap`];
///   Pass 2 consumes that handle and opens file data fd-relative via
///   `dir.open_file_read(name)` — never re-resolving the path from the root. A map
///   miss is fatal (the whole copy aborts).
/// - [`SourceRead::DereferencePath`]: the `-L`/`--dereference` path-based walk,
///   where nested symlink following is intentional and the data open is the
///   ordinary `File::open(src)`. It retains no pinned Pass-1 directory fd across
///   phases, although each `ReadDir` enumeration owns a transient descriptor. It
///   DOES retain a path-keyed Pass-1 entry per directory: with no count echoed back over the wire,
///   Pass 2 (`-L`) reads its expected count from here, while an owned credit paces the entry through
///   its direct-file work. A `-L` miss is NOT a TOCTOU violation (this path is not hardened), so it
///   is treated as count 0 + a debug log rather than failing closed.
#[derive(Clone)]
enum SourceRead {
    Hardened(Arc<SourceDirMap>),
    DereferencePath(DereferencePass1Map),
}

impl SourceRead {
    /// Close whichever Pass-1 pacing gate this mode uses — the hardened dir-fd budget or the `-L`
    /// outstanding-directory credit — releasing a parked walk with the typed [`FdBudgetClosed`].
    fn close_pass1_gate(&self) {
        match self {
            SourceRead::Hardened(map) => map.close_fd_budget(),
            SourceRead::DereferencePath(state) => state.close_credit(),
        }
    }
}

/// What Pass 1 counted inside one directory, carried across the network round-trip to Pass 2.
///
/// Pass 2 does not inherit Pass 1's classification — it re-enumerates the directory with a fresh
/// `readdir` — so without this the two passes can both account for the SAME name. An entry Pass 1
/// counted as a directory or symlink has already been accounted for BY Pass 1 (its own
/// `Directory`/`Symlink` message, or a compensating `FileSkipped`); if it is a regular FILE by the
/// time Pass 2 enumerates, Pass 2 reads it as one of that directory's expected files and takes a
/// SECOND of the parent's `entry_count` slots. The `files_found > file_count` truncation then drops
/// a genuinely counted sibling to keep the total at `file_count`, the destination still reaches
/// `entries_expected` and completes, and the copy exits 0 with a source file silently missing —
/// strictly worse than the hang it replaced.
///
/// The passes are therefore made mutually exclusive BY NAME: Pass 1 owns every name in
/// `non_files`, Pass 2 owns the rest and drops any enumerated name in this set before it counts
/// anything. The set has to be complete when the `Directory` message is sent, and it is — the
/// destination can ack that message, and Pass 2 can start, before Pass 1 has even descended into
/// those children, so a set accumulated as the compensations happen would race Pass 2 and lose.
///
/// Only non-file names are carried, not the file names: they are the smaller set (Pass 1 already
/// drops its file list as soon as the count is taken), and they are the whole of what Pass 1 owns.
/// The residual is a name Pass 1 never counted at all — a file genuinely created mid-copy — which
/// can still displace a counted file under truncation; that is reported as an error rather than
/// silently warned (see [`send_files_in_directory_tcp`]).
struct Pass1Contents {
    /// Number of child FILES Pass 1 counted. Authoritative for Pass 2's truncation and for its
    /// synthetic-`FileSkipped` deficit logic.
    file_count: usize,
    /// Names Pass 1 counted as directories or symlinks, and therefore accounts for itself.
    non_files: std::collections::HashSet<std::ffi::OsString>,
}

impl Pass1Contents {
    /// The bookkeeping for a directory committed to the wire with a 0-entry `Directory` (unreadable
    /// or vanished): no files for Pass 2 to send, and no names for it to avoid.
    fn empty() -> Self {
        Self {
            file_count: 0,
            non_files: std::collections::HashSet::new(),
        }
    }
}

/// Source-side path-keyed Pass-1 map for the `-L`/`--dereference` walk, the dereference analogue of
/// the hardened [`SourceDirMap`]. Its entries own a pacing credit instead of a pinned directory fd
/// and fd-budget permit.
///
/// Because `DirectoryCreated` does not carry `file_count`, the `-L` path (which holds no fd-map)
/// retains its own Pass-1 bookkeeping: Pass 1 inserts each directory's contents as it sends the
/// `Directory` message, and
/// [`resolve_pass2_source`] takes it back when the matching `DirectoryCreated`
/// triggers Pass 2. A missing entry is treated as empty contents with a debug log — `-L`
/// is intentionally not hardened, so a miss is not a TOCTOU/fail-closed condition.
type DereferencePass1Map = Arc<DereferenceWalkState>;

/// The `-L` walk's shared Pass-1 state: path-keyed contents bundled with the owned credit that paces
/// the entry until its direct-file Pass-2 work finishes. Without it the path-based walk recursively
/// sends `Directory` for the entire tree with no acknowledgement budget, while the destination
/// retains an open fd, stored metadata, and (for reused directories) a queued manifest task per
/// registered directory — so one slow root manifest lets an arbitrarily large tree exhaust
/// destination fds or memory, bounded by nothing.
/// One credit is taken per `Directory` sent. `DirectoryCreated` transfers it into that directory's
/// Pass-2 task and returns it when the task finishes; `DirectorySkipped` returns it immediately.
/// The budget size mirrors the hardened walk's (`max_pending_files`). `close_credit` releases a
/// parked walk on teardown with the same typed [`FdBudgetClosed`] marker the hardened budget uses,
/// so teardown attribution is unchanged.
struct DereferenceWalkState {
    contents: std::sync::Mutex<HashMap<std::path::PathBuf, DereferencePass1Entry>>,
    credit: Arc<tokio::sync::Semaphore>,
}

/// One `-L` directory committed by Pass 1 and not yet acknowledged by the destination.
///
/// The owned credit lives in the same map entry as the directory bookkeeping, so an
/// acknowledgement can only act on a credit that a matching `Directory` consumed.
/// `DirectoryCreated` transfers the entry into Pass 2 and releases it when that work finishes;
/// `DirectorySkipped` drops it immediately. A missing or duplicate acknowledgement has no permit
/// to invent.
struct DereferencePass1Entry {
    contents: Pass1Contents,
    _credit: tokio::sync::OwnedSemaphorePermit,
}

impl DereferenceWalkState {
    fn new(credit: usize) -> Self {
        Self {
            contents: std::sync::Mutex::new(HashMap::new()),
            credit: Arc::new(tokio::sync::Semaphore::new(credit)),
        }
    }
    /// Record one directory and retain its credit through its matching acknowledgement and Pass 2.
    async fn insert(&self, src: std::path::PathBuf, contents: Pass1Contents) -> anyhow::Result<()> {
        let credit = self
            .credit
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::Error::new(FdBudgetClosed))?;
        self.contents.lock().unwrap().insert(
            src,
            DereferencePass1Entry {
                contents,
                _credit: credit,
            },
        );
        Ok(())
    }

    /// Transfer one created directory's bookkeeping and credit into its Pass-2 work.
    fn take_for_created(&self, src: &std::path::Path) -> Option<DereferencePass1Entry> {
        self.contents.lock().unwrap().remove(src)
    }

    /// Consume a skipped directory and release its outstanding credit.
    fn take_for_skipped(&self, src: &std::path::Path) -> bool {
        self.contents.lock().unwrap().remove(src).is_some()
    }
    /// Close the credit gate so a parked walk fails with [`FdBudgetClosed`] instead of hanging.
    fn close_credit(&self) {
        self.credit.close();
    }
}

impl SourceRead {
    /// The hardened fd-map, or `None` in dereference mode. Used by Pass 1 to decide
    /// between the fd-relative walk and the path-based walk.
    fn dir_map(&self) -> Option<&Arc<SourceDirMap>> {
        match self {
            SourceRead::Hardened(map) => Some(map),
            SourceRead::DereferencePath(_) => None,
        }
    }
}

/// Source-side `path → MapEntry` map that bridges the network round-trip between
/// the two source passes, making file DATA reads TOCTOU-safe.
///
/// Pass 1 ([`send_directories_and_symlinks`]) opens every directory `O_NOFOLLOW`
/// from its parent's held fd and stores the resulting `Arc<Dir>` here, keyed by
/// the directory's source path. Pass 2 ([`send_files_in_directory_tcp`], spawned
/// later from the `DirectoryCreated` handler) takes ownership of that entry and
/// opens file data via `dir.open_file_read(name)` — fd-relative, never
/// re-resolving the path from the root — instead of the path-based
/// `File::open(src)` that a concurrent symlink swap could redirect.
///
/// # One-shot entry ownership (linear Pass 1 → Pass 2 handoff)
///
/// Each entry is *consumed* by exactly one of the destination's two mutually
/// exclusive responses, removing it from the map under the lock:
/// - [`Self::take_for_created`] (on `DirectoryCreated`): the owned [`MapEntry`]
///   (its `Arc<Dir>` plus the held fd-budget permit) moves INTO the spawned Pass 2
///   task, which releases the permit when it drops the entry after all files are
///   sent. Pass 2 never reacquires the dir-fd budget whose permit it holds, so this
///   handoff introduces no self-cycle; progress can still depend on network,
///   filesystem, rate, and leaf-admission gates.
/// - [`Self::take_for_skipped`] (on `DirectorySkipped`): the entry is dropped
///   immediately (releasing the permit) since no files are ever requested for a
///   skipped directory.
///
/// Ownership is therefore linear: there is no clone-and-leave + deferred RAII
/// cleanup. A second/absent take for the same path returns `None` (see the
/// dispatch loop for how each response handles that).
///
/// # Bounding semaphore (prevents EMFILE)
///
/// Pass 1 is an independently running full-tree DFS while Pass 2 is network-paced,
/// so without a bound the peak number of held directory fds would approach the
/// whole tree's directory count → `EMFILE` on large trees. The map is gated by a
/// dir-fd-in-flight semaphore: Pass 1 acquires one permit per [`Self::insert`]
/// (awaiting if the bound is reached), and the permit is released when the
/// directory's [`MapEntry`] is dropped (after a take).
///
/// # Release invariant (deadlock-free)
///
/// Pass 1 only ever *acquires*; it must never release. During normal protocol flow,
/// **every Pass-1 insert is matched by exactly one response-driven release** through
/// the two `take_*` methods above. This keeps the budget both *effective* (a large
/// no-ack subtree releases its fds promptly via `DirectorySkipped` nacks instead of
/// accumulating to connection-end) and *deadlock-free*. On hard teardown the gate
/// closes to wake any blocked insert, then dropping the map releases residual held
/// entries. Pass 2 must never acquire a dir-fd permit.
///
/// # Fail-closed teardown
///
/// [`Self::close_fd_budget`] closes the bounding semaphore so any pending or
/// future [`Self::insert`] fails immediately. The dispatch loop calls it when it
/// must fail closed on a `DirectoryCreated` miss, unblocking a Pass-1 walk that
/// might otherwise be parked on the budget so the whole operation tears down
/// cleanly instead of hanging.
struct SourceDirMap {
    entries: std::sync::Mutex<HashMap<std::path::PathBuf, MapEntry>>,
    fd_budget: Arc<tokio::sync::Semaphore>,
}

/// A consumed source-directory map entry, in one of two states; dropping it (after a
/// take) releases any held fd-budget permit. Encoding the state as an enum makes the
/// lifecycle explicit and makes its only two valid combinations (all-set versus all-clear)
/// representable by construction.
///
/// # Tombstone entries (committed unreadable directories)
///
/// When Pass 1 commits a directory to the wire but cannot read it (its
/// `open_root_dir`/`open_dir` failed, or its enumeration failed), it sends a 0-entry
/// `Directory` and stores a [`MapEntry::Tombstone`]. The destination still creates an
/// empty directory and acks `DirectoryCreated`, which must be CONSUMED normally — not
/// treated as a fail-closed miss. A tombstone holds no fd, so it deliberately consumes
/// no fd-budget permit, and Pass 2 for it sends zero files and needs no fd.
enum MapEntry {
    /// A readable directory: its held fd (Pass 2 opens file DATA fd-relative through
    /// it), the Pass-1 bookkeeping ([`Pass1Contents`] — the expected `file_count` plus the names
    /// Pass 1 accounts for itself), and the fd-budget permit that bounds how many
    /// real directory fds Pass 1 holds in flight (released when this drops).
    Readable {
        dir: Arc<Dir>,
        contents: Pass1Contents,
        _permit: tokio::sync::OwnedSemaphorePermit,
    },
    /// A committed-but-unreadable directory (0-entry `Directory` sent). Holds no fd and
    /// no permit; its file count is implicitly 0 and Pass 2 sends no files.
    Tombstone,
}

/// Marker error raised when the dir-fd budget semaphore is closed (by `close_fd_budget`). It is a
/// SYNTHETIC wakeup used to unblock a Pass-1 walk parked on the budget during teardown, NOT a root
/// cause — the caller detects it BY TYPE through [`is_fd_budget_closed`] to prefer the dispatch
/// task's real error (the transport/task failure that triggered the close) when reporting. A typed
/// marker (rather than a matched-on string) keeps that detection robust against message rewording
/// and context-wrapping. Its Display text is kept stable because it can still surface on abnormal
/// teardown paths.
#[derive(Debug, thiserror::Error)]
#[error("source dir-fd budget semaphore closed")]
struct FdBudgetClosed;

fn is_fd_budget_closed(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<FdBudgetClosed>())
}

impl SourceDirMap {
    /// Create a map bounded to at most `fd_budget` directory fds held in flight
    /// across the round-trip between Pass 1 and Pass 2.
    fn new(fd_budget: usize) -> Self {
        Self {
            entries: std::sync::Mutex::new(HashMap::new()),
            fd_budget: Arc::new(tokio::sync::Semaphore::new(fd_budget)),
        }
    }

    /// Store the directory's `Arc<Dir>` plus its Pass-1 [`Pass1Contents`],
    /// keyed by source path, first acquiring a dir-fd-in-flight permit (awaiting if
    /// the bound is reached). Only Pass 1 calls this, and only with the contents COMPLETE — see
    /// [`Pass1Contents`] for why they cannot be amended afterwards.
    async fn insert(
        &self,
        src: std::path::PathBuf,
        dir: Arc<Dir>,
        contents: Pass1Contents,
    ) -> anyhow::Result<()> {
        let permit = self
            .fd_budget
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::Error::new(FdBudgetClosed))?;
        self.entries.lock().unwrap().insert(
            src,
            MapEntry::Readable {
                dir,
                contents,
                _permit: permit,
            },
        );
        Ok(())
    }

    /// Store a *tombstone* for a directory Pass 1 committed to the wire (a 0-entry
    /// `Directory`) but could not open/enumerate. The destination still creates an
    /// empty directory and acks `DirectoryCreated`; the tombstone makes that ack
    /// CONSUME a real entry (via [`Self::take_for_created`]) instead of hitting the
    /// fail-closed miss path. A tombstone holds NO fd, so it acquires NO fd-budget
    /// permit (`dir: None`, `_permit: None`, `file_count: 0`). Only Pass 1 calls
    /// this. Infallible — there is no permit to await.
    fn insert_tombstone(&self, src: std::path::PathBuf) {
        self.entries
            .lock()
            .unwrap()
            .insert(src, MapEntry::Tombstone);
    }

    /// Consume the entry for a directory the destination created, transferring
    /// ownership of its `Arc<Dir>` + fd-budget permit to the caller (Pass 2).
    /// Returns `None` if no entry is present — in hardened mode the caller treats
    /// that as a fail-closed condition (the pinned handle is gone).
    fn take_for_created(&self, src: &std::path::Path) -> Option<MapEntry> {
        self.entries.lock().unwrap().remove(src)
    }

    /// Consume and drop the entry for a directory the destination did NOT create
    /// (the `DirectorySkipped` nack), releasing its dir-fd permit. Returns whether
    /// an entry was actually present so the caller can log an absent/double nack.
    fn take_for_skipped(&self, src: &std::path::Path) -> bool {
        self.entries.lock().unwrap().remove(src).is_some()
    }

    /// Close the dir-fd-in-flight semaphore so any pending or future [`Self::insert`] fails
    /// immediately, allowing fail-closed teardown without hanging a Pass-1 walk parked on the
    /// budget (see the struct-level docs).
    fn close_fd_budget(&self) {
        self.fd_budget.close();
    }
}

/// RAII backstop that closes the applicable Pass-1 pacing gate on drop.
///
/// The dispatch loop closes the active gate explicitly (once, before draining its Pass-2 tasks) on
/// every normal exit: the hardened walk uses its dir-fd budget, while `-L` uses an
/// outstanding-directory credit. Once installed at function entry, this guard covers destruction
/// before that point: cancellation can drop the dispatch future, and a panic can unwind out of the
/// loop. Without the close, a Pass-1 walk parked on either gate would keep the caller from reaching
/// the point where it awaits the dispatch task. The guard releases that walk whenever destructors
/// run; the walk then returns the synthetic `FdBudgetClosed` and teardown can continue.
/// `close_pass1_gate` is idempotent, so on the normal path (explicit close already done) this drop is
/// a no-op.
struct FdBudgetCloser(SourceRead);

impl Drop for FdBudgetCloser {
    fn drop(&mut self) {
        self.0.close_pass1_gate();
    }
}

/// increment the appropriate skipped counter based on file type.
/// special files (sockets, FIFOs, devices) that are filtered out count as files_skipped,
/// matching local copy behavior. specials_skipped is only for --skip-specials.
fn count_skipped(metadata: &std::fs::Metadata) {
    common::walk::EntryKind::from_metadata(metadata).inc_skipped(progress());
}

/// Collected child entry from a directory pre-read.
struct ChildEntry {
    src_path: std::path::PathBuf,
    dst_path: std::path::PathBuf,
    metadata: std::fs::Metadata,
}

/// Open the trusted parent prefix of a root operand and return it as a hardened `Dir` plus the
/// operand's final component, so the root file/symlink can be read fd-relative (`O_NOFOLLOW`) — the
/// same trusted-parent + hardened-final-component model the local copy uses. `open_parent_dir`
/// follows symlinks in the prefix (the caller's trust responsibility, per docs/tocttou.md), then
/// the final component is opened/classified `O_NOFOLLOW` below it, so a swap of the root entry in a
/// writable parent is caught at open. This hardens the remote source root the same way nested
/// entries are already hardened by the fd-map.
async fn open_root_parent(src: &std::path::Path) -> anyhow::Result<(Arc<Dir>, std::ffi::OsString)> {
    let operand = common::walk::split_root_operand(src).await?;
    let parent = Dir::open_parent_dir(&operand.parent, common::Side::Source)
        .await
        .with_context(|| format!("cannot open parent directory of root operand {src:?}"))?
        .into_tree();
    Ok((Arc::new(parent), operand.name))
}

/// Send a `SymlinkSkipped` for a symlink the source could not read (so the destination accounts for
/// it; for a root symlink this also signals root completion). The single source-side helper for the
/// three symlink-skip sites (root path-based, hardened root, nested).
async fn send_symlink_skipped(
    src: &std::path::Path,
    dst: &std::path::Path,
    is_root: bool,
    control_send_stream: &remote::streams::BoxedSharedSendStream,
) -> anyhow::Result<()> {
    let skip_msg = remote::protocol::SourceMessage::SymlinkSkipped {
        src_dst: remote::protocol::SrcDst {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
        },
        is_root,
    };
    control_send_stream
        .lock()
        .await
        .send_batch_message(&skip_msg)
        .await
}

/// Read a directory's access + default ACLs on the `-L`/`--dereference` walk, which retains no
/// pinned enumeration fd for the later ACL capture.
///
/// The hardened walk reads a directory's ACLs from the very fd whose contents it enumerates
/// ([`Dir::read_acls`]); `-L` has already closed its transient `ReadDir` descriptor, so the
/// directory is opened by path here. That is the same choice the rest of this walk already makes —
/// following symlinks is the point of `-L`, and the path is documented as not hardened — and it is
/// strictly better than the alternative of reporting "no ACL", which would silently WIDEN the
/// destination (a source with no ACL is copied by CLEARING the destination's, so an unread ACL is
/// indistinguishable from an absent one).
/// Called only when the master asked for directory ACLs, so a copy without `d:acl` pays nothing.
///
/// The open in front of the probe is rate-gated like every other path-based metadata read in this
/// module: `read_acls_fd` gates its own `flistxattr`/`fgetxattr` as `MetadataOp::Stat`, and an
/// unthrottled `open` bolted onto the front would leave the `-L` ACL probe the one metadata
/// operation in rcp that escapes the throttle.
async fn read_dir_acls_by_path(src: &std::path::Path) -> std::io::Result<common::safedir::Acls> {
    let dir = common::walk::run_metadata_probed(
        common::Side::Source,
        common::MetadataOp::Stat,
        tokio::fs::File::open(src), // rcp-toctou-allow: -L path (dereference, documented not hardened)
    )
    .await?;
    common::safedir::read_acls_fd(dir.as_fd(), common::Side::Source, true).await
}

/// Whether one step of the `-L` Pass-1 walk sent a protocol message accounting for its entry.
///
/// The destination expects exactly one response per entry its parent's `Directory { entry_count }`
/// tallied, so a nested step that ends without sending anything has to be compensated for —
/// otherwise that parent never reaches `entries_expected`, `DestinationDone` is never sent, and the
/// copy HANGS with both peers alive, which no keepalive or timeout ends (docs/remote_protocol.md
/// §2.2, §3.3). Returning this from the walk body instead of `()` turns "did this exit path account
/// for the entry?" into a question the compiler asks at every `return`, and lets the single funnel
/// in [`send_directories_and_symlinks`] compensate for all the "nothing sent" exits in one place
/// rather than each of them having to remember to.
#[derive(Debug, Clone, Copy)]
enum Pass1Commit {
    /// A message accounting for this entry was sent: a `Directory`, or (from the walk's symlink
    /// arms, which `-L` never reaches — see [`send_pass1_entry`]) a `Symlink`/`SymlinkSkipped`.
    Sent,
    /// Nothing was sent for this entry — it vanished, changed type, or stopped passing the filter
    /// between its parent's pre-read and this step.
    Nothing,
}

/// A complete `-L` directory commit waiting to be registered and sent.
///
/// Keeping the bookkeeping and wire fields together lets one funnel enforce the protocol order:
/// acquire an outstanding-directory credit, register the Pass-1 contents, then send exactly one
/// `Directory`. No path-based directory arm can register or send only part of that transaction.
struct DereferenceDirectoryCommit {
    src: std::path::PathBuf,
    dst: std::path::PathBuf,
    metadata: remote::protocol::Metadata,
    is_root: bool,
    entry_count: usize,
    keep_if_empty: bool,
    contents: Pass1Contents,
}

impl DereferenceDirectoryCommit {
    async fn send(
        self,
        state: &DereferenceWalkState,
        control_send_stream: &remote::streams::BoxedSharedSendStream,
    ) -> anyhow::Result<()> {
        let Self {
            src,
            dst,
            metadata,
            is_root,
            entry_count,
            keep_if_empty,
            contents,
        } = self;
        let file_count = contents.file_count;
        state.insert(src.clone(), contents).await?;
        tracing::debug!(
            "Sending directory: {:?} -> {:?} (entries={}, files={})",
            src,
            dst,
            entry_count,
            file_count
        );
        let message = remote::protocol::SourceMessage::Directory {
            src,
            dst,
            metadata,
            is_root,
            entry_count,
            keep_if_empty,
        };
        control_send_stream
            .lock()
            .await
            .send_batch_message(&message)
            .await
    }
}

/// The `-L`/`--dereference` path-based Pass-1 walk (directories + symlinks). The hardened
/// (non-`-L`) walk lives in [`send_directory_fd_walk`] (nested) and [`send_root_hardened`] (root);
/// this function is reached only in dereference mode, so every read here is path-based by design
/// (following symlinks is requested; documented not hardened).
///
/// This is the per-entry accounting funnel: [`send_pass1_entry`] does the work and reports whether
/// it committed a message, and a NESTED entry that committed none is accounted for here with one
/// [`send_child_failed_skip`] — the same compensation the hardened walk applies to a child it
/// counted and then failed to `open_dir`.
///
/// A ROOT that commits nothing needs no compensation and must not be given a `FileSkipped`, which
/// would not set the destination's `root_complete`. The only root exits that send nothing are the
/// ones [`send_fs_objects_tcp`] has already resolved to `has_root_item: false` — a filtered-out root
/// or a skipped special — and it decides that from the very snapshot it passes in as
/// `root_metadata`, so the two cannot disagree; the destination is released by
/// `DirStructureComplete { has_root_item: false }`.
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn send_directories_and_symlinks(
    settings: &common::copy::Settings,
    capture: ExtendedMetadataCapture,
    src: &std::path::Path,
    dst: &std::path::Path,
    source_root: &std::path::Path,
    root_metadata: Option<&std::fs::Metadata>,
    control_send_stream: &remote::streams::BoxedSharedSendStream,
    error_collector: &std::sync::Arc<common::error_collector::ErrorCollector>,
    // mandatory `-L` Pass-1 state: every directory commit records its contents and owns one
    // outstanding credit here until a skip or the matching created-directory Pass 2 finishes.
    deref_state: &DereferenceWalkState,
) -> anyhow::Result<()> {
    let commit = send_pass1_entry(
        settings,
        capture,
        src,
        dst,
        source_root,
        root_metadata,
        control_send_stream,
        error_collector,
        deref_state,
    )
    .await?;
    match commit {
        Pass1Commit::Sent => Ok(()),
        // the root needs no compensation and must not be given one (see this function's docs).
        Pass1Commit::Nothing if root_metadata.is_some() => Ok(()),
        Pass1Commit::Nothing => send_child_failed_skip(src, dst, control_send_stream).await,
    }
}

/// One step of the `-L` Pass-1 walk: classify `src`, send its `Directory`/`Symlink` message and
/// recurse into its children, reporting whether it committed a message for `src` (see
/// [`Pass1Commit`] and the funnel in [`send_directories_and_symlinks`], the only caller).
///
/// `root_metadata` is `Some` for the ROOT call ONLY, and carries the single classification
/// [`send_fs_objects_tcp`] already made of it — the same snapshot that decided `has_root_item` and
/// the file-vs-directory dispatch. The root is deliberately NOT re-stat'ed here: doing so is the
/// double-stat window [`send_root_hardened`] closes for the hardened walk, where a root that is a
/// directory at the caller's stat and a regular file at this one announces `has_root_item: true`,
/// sends no root message, and hangs the destination on `root_complete` forever. With one snapshot
/// driving both, a root that changes type is instead caught where every other unreadable directory
/// is — the enumeration below fails `ENOTDIR`/`ENOENT` and commits a 0-entry `Directory`, exactly as
/// the hardened root does when its `open_dir` fails.
///
/// A nested child passes `None` and IS re-classified here, mirroring the hardened walk's per-child
/// `open_dir`: a child that changed under us is caught rather than trusted, and the funnel accounts
/// for it.
///
/// The symlink arms below (here and in the child loop) are UNREACHABLE in practice and kept only as
/// defensive classification: every classification on this walk comes from `tokio::fs::metadata`,
/// which follows symlinks, so `is_symlink()` is never true — that is what `-L` means. A symlink to
/// a directory arrives as a directory, one to a file as a file, and a broken one fails the
/// classification. They are left in place rather than deleted so the arm set still mirrors the
/// hardened walk's, but nothing in `-L` exercises them.
#[instrument(
    skip(error_collector, control_send_stream, deref_state),
    fields(is_root = root_metadata.is_some())
)]
#[allow(clippy::too_many_arguments)]
async fn send_pass1_entry(
    settings: &common::copy::Settings,
    capture: ExtendedMetadataCapture,
    src: &std::path::Path,
    dst: &std::path::Path,
    source_root: &std::path::Path,
    root_metadata: Option<&std::fs::Metadata>,
    control_send_stream: &remote::streams::BoxedSharedSendStream,
    error_collector: &std::sync::Arc<common::error_collector::ErrorCollector>,
    deref_state: &DereferenceWalkState,
) -> anyhow::Result<Pass1Commit> {
    tracing::debug!("Sending data from {:?} to {:?}", &src, dst);
    let is_root = root_metadata.is_some();
    let src_metadata = match root_metadata {
        // the root's classification is the caller's, made once — see this function's docs.
        Some(metadata) => metadata.clone(),
        None => match common::walk::run_metadata_probed(
            common::Side::Source,
            common::MetadataOp::Stat,
            // `-L`-only path: always follow symlinks (dereference is always set here).
            tokio::fs::metadata(&src),
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("Failed reading metadata from src {src:?}: {e:#}");
                if settings.fail_early {
                    // the parent's child loop accounts for this entry when the descent
                    // returns `Err`, so the funnel must not also skip it.
                    return Err(e.into());
                }
                error_collector.push(e.into());
                // the child vanished between its parent's pre-read (which counted it) and
                // this classification: nothing to send, so the funnel compensates.
                return Ok(Pass1Commit::Nothing);
            }
        },
    };
    // apply filter if configured (applies to all items including root)
    if let Some(ref filter) = settings.filter {
        // for root items, use the file name with should_include_root_item
        // (anchored patterns match paths inside the source, not the source itself)
        // for nested items, use relative path with should_include
        let is_dir = src_metadata.is_dir();
        let result = if is_root {
            let file_name = src.file_name().map(std::path::Path::new).unwrap_or(src);
            filter.should_include_root_item(file_name, is_dir)
        } else {
            let relative_path = src.strip_prefix(source_root).unwrap_or(src);
            filter.should_include(relative_path, is_dir)
        };
        match result {
            common::filter::FilterResult::Included => { /* proceed */ }
            _ => {
                tracing::debug!("Filtered out {:?}: {:?}", src, result);
                count_skipped(&src_metadata);
                // a nested entry only lands here when it changed type since its parent
                // pre-read it — the same path and patterns gave `Included` then, and
                // `should_include` folds `could_contain_matches` in, so a directory whose
                // kind did not change cannot flip. Its parent counted it either way, so the
                // funnel compensates. For the root this is `has_root_item: false`, decided
                // by the caller from this same snapshot.
                return Ok(Pass1Commit::Nothing);
            }
        }
    }
    if src_metadata.is_file() {
        // a counted child that is a regular file NOW but was a directory when its parent
        // pre-read it. It is not copied at all: the walk sends directories and symlinks, and its
        // NAME belongs to Pass 1 (`Pass1Contents`), so Pass 2 will not pick it up as a file
        // either. A source entry that is silently not copied must not leave the copy reporting
        // success, so this is recorded as an error — the same answer the hardened walk gives when
        // its `open_dir` on a counted child fails `ENOTDIR`. The funnel then compensates the
        // parent's count. The root never reaches here — `send_fs_objects_tcp` calls this
        // walk only for a root its single classification says is not a file, and that same
        // classification is what `src_metadata` holds.
        let err = anyhow::anyhow!(
            "copy: {:?} -> {:?} failed, source entry changed from a directory to a regular file \
             during the copy",
            src,
            dst
        );
        tracing::error!("{:#}", &err);
        if settings.fail_early {
            // the parent's child loop accounts for this entry when the descent returns `Err`,
            // so the funnel must not also skip it.
            return Err(err);
        }
        error_collector.push(err);
        return Ok(Pass1Commit::Nothing);
    }
    if src_metadata.is_symlink() {
        let target = match common::walk::run_metadata_probed(
            common::Side::Source,
            common::MetadataOp::ReadLink,
            tokio::fs::read_link(&src), // rcp-toctou-allow: -L path (dereference, documented not hardened)
        )
        .await
        {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Failed reading symlink {src:?}: {e:#}");
                // notify destination that this symlink was skipped
                // for root symlinks, this also signals root completion (even if failed)
                send_symlink_skipped(src, dst, is_root, control_send_stream).await?;
                if settings.fail_early {
                    return Err(e.into());
                }
                error_collector.push(e.into());
                // `SymlinkSkipped` is itself the accounting message (it advances the
                // parent's tally, and for a root it sets `root_complete`).
                return Ok(Pass1Commit::Sent);
            }
        };
        let symlink = remote::protocol::SourceMessage::Symlink {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            target: target.clone(),
            metadata: remote::protocol::Metadata::from(&src_metadata),
            is_root,
        };
        control_send_stream
            .lock()
            .await
            .send_batch_message(&symlink)
            .await?;
        return Ok(Pass1Commit::Sent);
    }
    if !src_metadata.is_dir() {
        if !src_metadata.is_file() {
            // special file (socket, FIFO, device)
            if settings.skip_specials {
                tracing::debug!(
                    "skipping special file {:?} (type: {:?})",
                    src,
                    src_metadata.file_type()
                );
                progress().specials_skipped.inc();
            } else {
                let err = anyhow::anyhow!(
                    "copy: {:?} -> {:?} failed, unsupported src file type: {:?}",
                    src,
                    dst,
                    src_metadata.file_type()
                );
                tracing::error!("{:#}", &err);
                if settings.fail_early || is_root {
                    return Err(err);
                }
                error_collector.push(err);
            }
        }
        // specials produce no protocol message at all. For a nested entry that means one its
        // parent counted as a directory or symlink and that has since become a socket / FIFO /
        // device, so the funnel compensates; for a root it is the `has_root_item: false` the
        // caller derived from this same snapshot.
        return Ok(Pass1Commit::Nothing);
    }
    // pre-read directory children to compute entry counts before sending Directory message.
    // The open takes an ops token like every other metadata syscall in this walk — the hardened
    // twin (`Dir::read_entries`) already does, and without it this one call escapes
    // `--ops-throttle` entirely.
    let mut file_children: Vec<ChildEntry> = Vec::new();
    let mut dir_children: Vec<ChildEntry> = Vec::new();
    let mut symlink_children: Vec<ChildEntry> = Vec::new();
    throttle::get_ops_token().await;
    let mut entries = match tokio::fs::read_dir(&src).await {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("Cannot open directory {src:?} for reading: {e:#}");
            if settings.fail_early {
                return Err(e.into());
            }
            error_collector.push(e.into());
            // directory unreadable but we already committed to sending it - send with 0 entries
            // so destination can still complete. This is also where a ROOT that changed type under
            // us lands (`ENOTDIR`), and it is the same answer `send_root_hardened` gives when its
            // `open_dir` fails. The shared commit funnel records empty contents and owns the same
            // acknowledgement credit as every other `-L` directory.
            // ACLs stay UNKNOWN on this one: the directory could not be opened, so there is no fd
            // to read them from and no honest answer to give. `WireAcls::Unknown` tells the
            // destination to leave the destination directory's ACLs ALONE (a locked reused
            // directory gets its original default ACL back from the lockdown guard) — an absence
            // we never observed must not arrive as an authoritative CLEAR of a reused
            // destination's ACLs. The entry's copy is already recorded as failed above.
            DereferenceDirectoryCommit {
                src: src.to_path_buf(),
                dst: dst.to_path_buf(),
                metadata: remote::protocol::Metadata::from(&src_metadata),
                is_root,
                entry_count: 0,
                keep_if_empty: true,
                contents: Pass1Contents::empty(),
            }
            .send(deref_state, control_send_stream)
            .await?;
            return Ok(Pass1Commit::Sent);
        }
    };
    loop {
        match common::walk::next_entry_probed(&mut entries, common::Side::Source, || {
            format!("failed traversing src directory {:?}", &src)
        })
        .await
        {
            Ok(Some((entry, _file_type))) => {
                let entry_path = entry.path();
                let entry_name = entry_path.file_name().unwrap();
                let dst_path = dst.join(entry_name);
                let entry_metadata = match common::walk::run_metadata_probed(
                    common::Side::Source,
                    common::MetadataOp::Stat,
                    // `-L`-only path: always follow symlinks (dereference is always set here).
                    tokio::fs::metadata(&entry_path),
                )
                .await
                {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::error!("Failed reading metadata from {entry_path:?}: {e:#}");
                        if settings.fail_early {
                            return Err(e.into());
                        }
                        error_collector.push(e.into());
                        continue;
                    }
                };
                // apply filter for child entries
                if let Some(ref filter) = settings.filter {
                    let relative_path = entry_path.strip_prefix(source_root).unwrap_or(&entry_path);
                    let is_dir = entry_metadata.is_dir();
                    match filter.should_include(relative_path, is_dir) {
                        common::filter::FilterResult::Included => { /* proceed */ }
                        common::filter::FilterResult::ExcludedByPattern(_) => {
                            tracing::debug!("Filtered out {:?}", entry_path);
                            // only count dirs/symlinks here; files are counted
                            // in send_files_in_directory_tcp which re-traverses
                            if !entry_metadata.is_file() {
                                count_skipped(&entry_metadata);
                            }
                            continue;
                        }
                        common::filter::FilterResult::ExcludedByDefault => {
                            // for directories, check if they could contain matches
                            if is_dir {
                                let mut could_match = false;
                                for pattern in &filter.includes {
                                    if filter.could_contain_matches(relative_path, pattern) {
                                        could_match = true;
                                        break;
                                    }
                                }
                                if !could_match {
                                    tracing::debug!("Filtered out {:?}", entry_path);
                                    count_skipped(&entry_metadata);
                                    continue;
                                }
                                // directory might contain matches - include it
                            } else {
                                tracing::debug!("Filtered out {:?}", entry_path);
                                // only count symlinks here; files are counted
                                // in send_files_in_directory_tcp
                                if !entry_metadata.is_file() {
                                    count_skipped(&entry_metadata);
                                }
                                continue;
                            }
                        }
                    }
                }
                let child = ChildEntry {
                    src_path: entry_path,
                    dst_path,
                    metadata: entry_metadata,
                };
                if child.metadata.is_file() {
                    file_children.push(child);
                } else if child.metadata.is_symlink() {
                    symlink_children.push(child);
                } else if child.metadata.is_dir() {
                    dir_children.push(child);
                } else if settings.skip_specials {
                    tracing::debug!("skipping special file {:?}", &child.src_path);
                    progress().specials_skipped.inc();
                } else {
                    let err = anyhow::anyhow!(
                        "copy: {:?} -> {:?} failed, unsupported src file type: {:?}",
                        &child.src_path,
                        &child.dst_path,
                        child.metadata.file_type()
                    );
                    tracing::error!("{:#}", &err);
                    if settings.fail_early {
                        return Err(err);
                    }
                    error_collector.push(err);
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::error!("Failed traversing src directory {src:?}: {e:#}");
                if settings.fail_early {
                    return Err(e);
                }
                error_collector.push(e);
                break;
            }
        }
    }
    drop(entries);
    // compute counts and keep_if_empty
    let file_count = file_children.len();
    let entry_count = file_count + dir_children.len() + symlink_children.len();
    let keep_if_empty = if is_root {
        true
    } else if let Some(ref filter) = settings.filter {
        let relative_path = src.strip_prefix(source_root).unwrap_or(src);
        filter.directly_matches_include(relative_path, true)
    } else {
        true
    };
    // this directory's ACLs, when the master asked for them (`d:acl`). `-L` retains no pinned
    // enumeration fd for this later capture, so the read opens the directory by path — the same
    // choice the rest of this walk already makes (following symlinks is what `-L` is for; documented
    // not hardened). Read only AFTER the enumeration above succeeded, so an unreadable directory
    // still degrades to the 0-entry `Directory` above rather than becoming a hard failure it is not
    // today.
    //
    // A failure here FAILS the directory rather than degrading to "no ACL": an all-`None` `Acls` is
    // a request to CLEAR, so sending one would make an unreadable ACL STRIP the destination's —
    // including a directory's default ACL, which then governs everything created beneath it. That
    // is strictly worse than failing, and it mirrors the destination's rule (D5) for the same
    // situation in the other direction. This returns BEFORE the `Directory` message is sent, so the
    // destination is never told to expect this subtree; both callers account for that — the child
    // loops below with `send_child_failed_skip`, and `send_fs_objects_tcp` by failing the copy for
    // a root.
    let metadata = remote::protocol::Metadata::from(&src_metadata);
    let metadata = if capture.dir_acl {
        metadata.with_acls(
            &read_dir_acls_by_path(src)
                .await
                .with_context(|| format!("cannot read ACLs from directory {src:?}"))?,
        )
    } else {
        metadata
    };
    // register this `-L` directory's Pass-1 bookkeeping, acquire its outstanding credit, and send
    // the pre-computed entry count through the same commit funnel as unreadable directories.
    DereferenceDirectoryCommit {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        metadata,
        is_root,
        entry_count,
        keep_if_empty,
        contents: Pass1Contents {
            file_count,
            non_files: dir_children
                .iter()
                .chain(symlink_children.iter())
                .filter_map(|child| child.src_path.file_name().map(|n| n.to_owned()))
                .collect(),
        },
    }
    .send(deref_state, control_send_stream)
    .await?;
    // recurse into non-file children (symlinks first, then directories) through the funnel, which
    // accounts for any child it sends nothing for. this path-based body only runs when the fd-map
    // is inactive (`-L` mode), so the recursive calls carry `None` for the root classification
    // (they are not the root) and forward the dereference Pass-1 state.
    //
    // ordinary child errors are accounted with `send_child_failed_skip`, exactly as the hardened
    // walk does: the child was already counted in this directory's `entry_count`, so without it the
    // destination's parent never reaches `entries_expected`, `DestinationDone` is never sent, and
    // the copy HANGS with both peers alive (no timeout saves it). See `send_child_failed_skip` for
    // why liveness is favored over precision when the child had already self-accounted.
    //
    // `FdBudgetClosed` is different: dispatch closed the `-L` pacing gate to tear the copy down.
    // it is unconditional control flow, not a child failure to compensate or collect; return it
    // immediately so the caller can replace this synthetic marker with dispatch's published cause.
    for (kind, child) in symlink_children
        .into_iter()
        .map(|child| ("symlink", child))
        .chain(dir_children.into_iter().map(|child| ("directory", child)))
    {
        if let Err(e) = send_directories_and_symlinks(
            settings,
            capture,
            &child.src_path,
            &child.dst_path,
            source_root,
            None,
            control_send_stream,
            error_collector,
            deref_state,
        )
        .await
        {
            handle_child_walk_error(
                kind,
                &child.src_path,
                &child.dst_path,
                e,
                settings.fail_early,
                control_send_stream,
                error_collector,
            )
            .await?;
        }
    }
    Ok(Pass1Commit::Sent)
}

/// Emit a 0-entry `Directory` message for a directory that we committed to
/// sending but cannot read (open/enumerate failed), so the destination can still
/// complete its tracking. Shared by the path-based and fd-walk directory bodies.
///
/// # Hardened-map bookkeeping (fail-closed correctness)
///
/// In hardened mode (`dir_map` is `Some`) the destination will still create an
/// empty directory and ack `DirectoryCreated` for the 0-entry `Directory` sent
/// here. That ack MUST consume a real map entry, otherwise it hits
/// [`resolve_pass2_source`]'s fail-closed miss path and spuriously aborts the
/// copy — the very bug this committed-unreadable-directory case must avoid. So
/// before sending we register an entry keyed by `src`:
/// - `dir: Some(_)` (enumeration failed but the directory fd is held): insert a
///   real entry via [`SourceDirMap::insert`] (file_count 0, holds the fd's
///   permit). Its `DirectoryCreated` ack consumes it; Pass 2 sends no files.
/// - `dir: None` (the directory could not even be opened): insert a *tombstone*
///   via [`SourceDirMap::insert_tombstone`] (no fd, no permit, file_count 0).
///
/// This registration happens only on the non-fail-early path: with `fail_early`
/// we return `Err` before sending the `Directory`, so no ack will ever arrive and
/// no entry is needed. `-L` mode passes `dir_map: None` (it has no fd-map), so
/// nothing is registered there.
#[allow(clippy::too_many_arguments)]
async fn send_unreadable_directory(
    src: &std::path::Path,
    dst: &std::path::Path,
    metadata: remote::protocol::Metadata,
    is_root: bool,
    control_send_stream: &remote::streams::BoxedSharedSendStream,
    error_collector: &std::sync::Arc<common::error_collector::ErrorCollector>,
    fail_early: bool,
    err: anyhow::Error,
    // hardened fd-map (None under `-L`) and the held directory fd if one was
    // opened (None when the open itself failed). Together they decide whether to
    // register a real entry or a tombstone so the destination's `DirectoryCreated`
    // ack is consumed instead of failing closed.
    dir_map: Option<&Arc<SourceDirMap>>,
    dir: Option<Arc<Dir>>,
) -> anyhow::Result<()> {
    if fail_early {
        return Err(err);
    }
    error_collector.push(err);
    // register the map entry (real or tombstone) BEFORE sending the `Directory`, so
    // the entry is present before the destination can echo `DirectoryCreated` and
    // Pass 2 / the dispatch loop looks it up.
    if let Some(dir_map) = dir_map {
        match dir {
            Some(dir) => {
                dir_map
                    .insert(src.to_path_buf(), dir, Pass1Contents::empty())
                    .await?
            }
            None => dir_map.insert_tombstone(src.to_path_buf()),
        }
    }
    let dir_msg = remote::protocol::SourceMessage::Directory {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        metadata,
        is_root,
        entry_count: 0,
        keep_if_empty: true,
    };
    control_send_stream
        .lock()
        .await
        .send_batch_message(&dir_msg)
        .await?;
    Ok(())
}

/// Emit a per-child accounting message for a counted child the source can no
/// longer process (e.g. `open_dir` failed, or the recursive descent returned an
/// error).
///
/// The child was already tallied in its parent's `entry_count` when the parent's
/// `Directory` message was sent, so the destination's `DirectoryTracker` is waiting
/// for exactly one response accounting for it. Without this message that parent
/// never completes → `DestinationDone` is never sent → the copy hangs. We send
/// `FileSkipped`, which the destination handles via `process_file(parent)`,
/// incrementing the parent's processed-entry count by exactly one. `FileSkipped`
/// (rather than a directory-specific message) is appropriate because it is the
/// source-side "counted but not sent" signal already used for vanished/unreadable
/// children, and after a failed open the source has no trustworthy type to assert.
///
/// Liveness over precision (the `fail_early` edge): except for the synthetic [`FdBudgetClosed`]
/// teardown marker, this is sent whenever the child recursion returns `Err`, even in the case where
/// the child had already sent its own `Directory` *and* self-accounted before erroring (e.g. a
/// deeper `fail_early` abort after a grandchild failed). There the extra `FileSkipped` over-counts
/// the parent by one, which can complete it before the (incomplete, never-completing)
/// child subtree does. That is deliberately accepted: the alternative — withholding
/// the skip whenever the child sent its `Directory` — would hang the far more common
/// case where the child sent its `Directory` but errored *before* completing (its
/// subtree then never propagates upward, so the parent needs this skip). The
/// over-count is benign because (a) it only arises during a `fail_early` teardown
/// that fails the whole copy, and (b) remote `--delete` is rejected up front, so an
/// early parent completion cannot prune — at worst directory metadata is applied
/// early on a copy that is aborting anyway.
async fn send_child_failed_skip(
    src: &std::path::Path,
    dst: &std::path::Path,
    control_send_stream: &remote::streams::BoxedSharedSendStream,
) -> anyhow::Result<()> {
    let skip_msg = remote::protocol::SourceMessage::FileSkipped {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
    };
    control_send_stream
        .lock()
        .await
        .send_batch_message(&skip_msg)
        .await?;
    tracing::debug!(
        "Sent FileSkipped to account for unprocessable child {:?} -> {:?}",
        src,
        dst
    );
    Ok(())
}

/// Funnel a recursive Pass-1 child failure through teardown-marker classification and ordinary
/// child compensation.
///
/// [`FdBudgetClosed`] is synthetic teardown control flow: propagate it immediately so the caller
/// can surface the dispatch task's published cause. Every other error belongs to the counted child
/// and therefore takes the normal `FileSkipped` / fail-early / collection path.
async fn handle_child_walk_error(
    kind: &str,
    src: &std::path::Path,
    dst: &std::path::Path,
    error: anyhow::Error,
    fail_early: bool,
    control_send_stream: &remote::streams::BoxedSharedSendStream,
    error_collector: &std::sync::Arc<common::error_collector::ErrorCollector>,
) -> anyhow::Result<()> {
    if is_fd_budget_closed(&error) {
        return Err(error);
    }
    tracing::error!("Failed to send {kind} {src:?}: {error:#}");
    send_child_failed_skip(src, dst, control_send_stream).await?;
    if fail_early {
        return Err(error);
    }
    error_collector.push(error);
    Ok(())
}

/// Collected child entry from an fd-relative directory pre-read (Pass 1, hardened
/// path). Carries the child's name (for fd-relative recursion) plus its display
/// paths. Metadata is NOT cached here: each entry's wire metadata is read at send
/// time from the same fd as its payload (files via `open_file_read`, symlinks via
/// `Handle::read_symlink`, dirs via `Dir::meta`) for read-side fidelity.
struct FdChildEntry {
    name: std::ffi::OsString,
    src_path: std::path::PathBuf,
    dst_path: std::path::PathBuf,
}

/// Pass 1 directory body, hardened: enumerate `dir` via `read_entries()` and
/// classify each child via `child()` (fd-relative `fstat`, never following a
/// symlink), send the same `Directory`/`Symlink` protocol messages as the
/// path-based body, store `dir`'s `Arc<Dir>` in the fd-map for Pass 2, and recurse
/// into child directories opened `O_NOFOLLOW` from `dir`.
///
/// `dir` is the already-open handle to `src` itself; its wire metadata — including its ACLs when
/// the master asked for them — is read from that same fd (`Dir::meta` / `Dir::read_acls`), so the
/// directory's metadata pairs with the contents enumerated here (read-side fidelity). `is_root`
/// drives `keep_if_empty` and the `Directory`/`Symlink` message flags exactly as the path-based
/// body does.
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn send_directory_fd_walk(
    settings: &common::copy::Settings,
    capture: ExtendedMetadataCapture,
    src: &std::path::Path,
    dst: &std::path::Path,
    source_root: &std::path::Path,
    is_root: bool,
    dir: Arc<Dir>,
    control_send_stream: &remote::streams::BoxedSharedSendStream,
    error_collector: &std::sync::Arc<common::error_collector::ErrorCollector>,
    dir_map: &Arc<SourceDirMap>,
) -> anyhow::Result<()> {
    // the directory's wire metadata comes from its OWN held fd (the fd whose contents we enumerate
    // below), so a same-name dir swap can't pair the enumerated contents with another inode's
    // metadata (read-side fidelity, docs/tocttou.md).
    let metadata = remote::protocol::Metadata::from(
        &dir.meta()
            .await
            .with_context(|| format!("cannot read directory metadata from {src:?}"))?,
    );
    // both ACLs, from that same fd. Only when the master asked (`d:acl`): the probe is a syscall
    // per directory that `stat` cannot fold in, so a copy that does not want ACLs must not pay it.
    // A directory's DEFAULT ACL rides along because it is what the destination's children inherit —
    // dropping it would silently change the destination tree's inheritance policy.
    let metadata = if capture.dir_acl {
        metadata.with_acls(
            &dir.read_acls()
                .await
                .with_context(|| format!("cannot read ACLs from directory {src:?}"))?,
        )
    } else {
        metadata
    };
    // enumerate children; `read_entries` returns names + a best-effort d_type hint
    // (advisory only — `child()` re-classifies authoritatively via fstat below).
    // the directory's held fd is stored in the map only once `file_count` is known
    // (just before the `Directory` message is sent, below) so the map entry carries
    // the authoritative Pass-1 count for Pass 2. The destination cannot echo
    // `DirectoryCreated` before that message, so Pass 2 never looks up early.
    let raw_entries = match dir.read_entries().await {
        Ok(entries) => entries,
        Err(e) => {
            tracing::error!("Cannot enumerate directory {src:?} for reading: {e:#}");
            // we still committed to sending a (0-entry) `Directory` for this dir.
            // The directory fd IS held (open succeeded, only enumeration failed), so
            // register a real 0-file entry (it holds the fd's permit):
            // `send_unreadable_directory` does the insert before sending so the
            // destination's `DirectoryCreated` ack is consumed instead of being a
            // hardened map miss → spurious fail-closed.
            return send_unreadable_directory(
                src,
                dst,
                metadata,
                is_root,
                control_send_stream,
                error_collector,
                settings.fail_early,
                e.into(),
                Some(dir_map),
                Some(dir.clone()),
            )
            .await;
        }
    };
    let mut file_children: Vec<FdChildEntry> = Vec::new();
    let mut dir_children: Vec<FdChildEntry> = Vec::new();
    let mut symlink_children: Vec<FdChildEntry> = Vec::new();
    for (entry_name, _hint) in raw_entries {
        let entry_path = src.join(&entry_name);
        let dst_path = dst.join(&entry_name);
        // classify authoritatively via fd-relative fstat (never follows a symlink).
        let handle = match dir.child(&entry_name).await {
            Ok(h) => h,
            Err(e) => {
                let e: anyhow::Error = e.into();
                tracing::error!("Failed reading metadata from {entry_path:?}: {e:#}");
                if settings.fail_early {
                    return Err(e);
                }
                error_collector.push(e);
                continue;
            }
        };
        let kind = handle.kind();
        let is_dir = kind == common::walk::EntryKind::Dir;
        // apply filter for child entries (same logic as the path-based body)
        if let Some(ref filter) = settings.filter {
            let relative_path = common::walk::relative_to_root(&entry_path, source_root);
            match filter.should_include(relative_path, is_dir) {
                common::filter::FilterResult::Included => { /* proceed */ }
                common::filter::FilterResult::ExcludedByPattern(_) => {
                    tracing::debug!("Filtered out {:?}", entry_path);
                    // only count dirs/symlinks here; files are counted in
                    // send_files_in_directory_tcp which re-traverses
                    if kind != common::walk::EntryKind::File {
                        kind.inc_skipped(progress());
                    }
                    continue;
                }
                common::filter::FilterResult::ExcludedByDefault => {
                    if is_dir {
                        let mut could_match = false;
                        for pattern in &filter.includes {
                            if filter.could_contain_matches(relative_path, pattern) {
                                could_match = true;
                                break;
                            }
                        }
                        if !could_match {
                            tracing::debug!("Filtered out {:?}", entry_path);
                            kind.inc_skipped(progress());
                            continue;
                        }
                        // directory might contain matches - include it
                    } else {
                        tracing::debug!("Filtered out {:?}", entry_path);
                        // only count symlinks here; files are counted in
                        // send_files_in_directory_tcp
                        if kind != common::walk::EntryKind::File {
                            kind.inc_skipped(progress());
                        }
                        continue;
                    }
                }
            }
        }
        let child = FdChildEntry {
            name: entry_name,
            src_path: entry_path,
            dst_path,
        };
        match kind {
            common::walk::EntryKind::File => file_children.push(child),
            common::walk::EntryKind::Symlink => symlink_children.push(child),
            common::walk::EntryKind::Dir => dir_children.push(child),
            common::walk::EntryKind::Special => {
                if settings.skip_specials {
                    tracing::debug!("skipping special file {:?}", &child.src_path);
                    progress().specials_skipped.inc();
                } else {
                    let err = anyhow::anyhow!(
                        "copy: {:?} -> {:?} failed, unsupported src file type",
                        &child.src_path,
                        &child.dst_path,
                    );
                    tracing::error!("{:#}", &err);
                    if settings.fail_early {
                        return Err(err);
                    }
                    error_collector.push(err);
                }
            }
        }
    }
    // compute counts and keep_if_empty (identical semantics to the path-based body)
    let file_count = file_children.len();
    let entry_count = file_count + dir_children.len() + symlink_children.len();
    let keep_if_empty = if is_root {
        true
    } else if let Some(ref filter) = settings.filter {
        let relative_path = common::walk::relative_to_root(src, source_root);
        filter.directly_matches_include(relative_path, true)
    } else {
        true
    };
    let dir_msg = remote::protocol::SourceMessage::Directory {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        metadata,
        is_root,
        entry_count,
        keep_if_empty,
    };
    // store this directory's held fd + its authoritative Pass-1 contents so Pass 2 can open
    // file data fd-relative, size its truncation / synthetic-skip logic, and skip the names
    // Pass 1 accounts for itself (`Pass1Contents`). Acquiring
    // the permit here bounds how many dir fds Pass 1 holds ahead of the network-paced
    // Pass 2 (prevents EMFILE); it must precede the `Directory` send so the entry is
    // present before the destination can echo `DirectoryCreated` and trigger Pass 2's
    // lookup — which is also why the contents must be complete HERE and cannot be amended as
    // the child loops below discover failures.
    dir_map
        .insert(
            src.to_path_buf(),
            dir.clone(),
            Pass1Contents {
                file_count,
                non_files: dir_children
                    .iter()
                    .chain(symlink_children.iter())
                    .map(|child| child.name.clone())
                    .collect(),
            },
        )
        .await?;
    tracing::debug!(
        "Sending directory: {:?} -> {:?} (entries={}, files={})",
        &src,
        dst,
        entry_count,
        file_count
    );
    control_send_stream
        .lock()
        .await
        .send_batch_message(&dir_msg)
        .await?;
    // send symlink children: re-classify each at send time and read BOTH its target and metadata
    // from that one pinned handle (never following the link), so a same-name symlink swap can't pair
    // one link's target with another link's owner/timestamps — target and metadata are a faithful
    // pair, matching the file path. A swap to a non-symlink fails the classify/read and is skipped.
    for child in symlink_children {
        let read = async {
            let handle = dir.child(&child.name).await?;
            let (target, meta) = handle.read_symlink(dir.side()).await?;
            std::io::Result::Ok((target, remote::protocol::Metadata::from(&meta)))
        }
        .await;
        let (target, metadata) = match read {
            Ok(v) => v,
            Err(e) => {
                let e: anyhow::Error = e.into();
                tracing::error!("Failed reading symlink {:?}: {e:#}", child.src_path);
                send_symlink_skipped(&child.src_path, &child.dst_path, false, control_send_stream)
                    .await?;
                if settings.fail_early {
                    return Err(e);
                }
                error_collector.push(e);
                continue;
            }
        };
        let symlink = remote::protocol::SourceMessage::Symlink {
            src: child.src_path.clone(),
            dst: child.dst_path.clone(),
            target,
            metadata,
            is_root: false,
        };
        control_send_stream
            .lock()
            .await
            .send_batch_message(&symlink)
            .await?;
    }
    // recurse into child directories: open each `O_NOFOLLOW` from this dir's held
    // fd and hand the resulting `Arc<Dir>` to the recursive call. The child's wire
    // metadata is built from its fd-pinned `FileMeta` (captured at classify time).
    for child in dir_children {
        let child_dir = match dir.open_dir(&child.name).await {
            Ok(d) => Arc::new(d),
            Err(e) => {
                let e: anyhow::Error = e.into();
                tracing::error!("Failed to open directory {:?}: {e:#}", child.src_path);
                // this child was counted in this directory's `entry_count`, but we
                // never sent its `Directory` message — account for it with a
                // `FileSkipped` so the destination's parent count can still reach
                // zero (otherwise the parent waits forever and the copy hangs).
                send_child_failed_skip(&child.src_path, &child.dst_path, control_send_stream)
                    .await?;
                if settings.fail_early {
                    return Err(e);
                }
                error_collector.push(e);
                continue;
            }
        };
        if let Err(e) = send_directory_fd_walk(
            settings,
            capture,
            &child.src_path,
            &child.dst_path,
            source_root,
            false,
            child_dir,
            control_send_stream,
            error_collector,
            dir_map,
        )
        .await
        {
            // the shared funnel propagates the typed teardown marker immediately. ordinary child
            // errors are compensated with `FileSkipped`, then returned or collected according to
            // fail-early mode; see `send_child_failed_skip` for the accounting rationale.
            handle_child_walk_error(
                "directory",
                &child.src_path,
                &child.dst_path,
                e,
                settings.fail_early,
                control_send_stream,
                error_collector,
            )
            .await?;
        }
    }
    Ok(())
}

#[instrument(skip(error_collector, stream_pool, control_send_stream, source_read))]
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn send_fs_objects_tcp(
    settings: &common::copy::Settings,
    capture: ExtendedMetadataCapture,
    src: &std::path::Path,
    dst: &std::path::Path,
    control_send_stream: remote::streams::BoxedSharedSendStream,
    stream_pool: std::sync::Arc<AcceptingSendStreamPool>,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
    // explicit source read mode: hardened (fd-map active) or `-L` path-based walk.
    source_read: SourceRead,
) -> anyhow::Result<()> {
    tracing::info!("Sending data from {:?} to {:?}", src, dst);
    // hardened (non-`-L`) root: classify the named root ONCE via its trusted parent's fd, driving
    // has_root_item, filtering, metadata, and dispatch from that single snapshot — no second path
    // stat. This closes the root-kind-swap double-stat window (a dir/symlink→file swap between the
    // has_root_item decision and the dispatch could otherwise announce a root item but send none,
    // hanging the destination). `-L` follows the path-based flow below — still unhardened by design
    // (it follows symlinks), but classifying its root exactly once in the same way: the one stat
    // taken here drives has_root_item, the dispatch, AND the walk, which is handed that snapshot
    // instead of taking a second.
    if !settings.dereference {
        return send_root_hardened(
            settings,
            capture,
            src,
            dst,
            control_send_stream,
            stream_pool,
            error_collector,
            source_read,
        )
        .await;
    }
    let SourceRead::DereferencePath(deref_state) = &source_read else {
        anyhow::bail!("dereference walk started without dereference Pass-1 state");
    };
    let src_metadata = match common::walk::run_metadata_probed(
        common::Side::Source,
        common::MetadataOp::Stat,
        async {
            if settings.dereference {
                tokio::fs::metadata(src).await
            } else {
                tokio::fs::symlink_metadata(src).await
            }
        },
    )
    .await
    {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("Failed reading metadata from src {src:?}: {e:#}");
            return Err(e.into());
        }
    };
    // the same constant source-root ACL warning the hardened root gets in `send_root_hardened`.
    // This walk holds no root handle, so the root is classified through its parent for the probe
    // alone — `O_NOFOLLOW`, hence a root that is itself a symlink is skipped rather than followed;
    // that narrows an already-heuristic warning and never widens it. A parent that cannot be opened
    // is left to the walk below to diagnose.
    // The guard comes FIRST: `open_root_parent` is a `split_root_operand` plus an `open_parent_dir`
    // that this walk needs for nothing else, so calling it before asking whether a probe is even
    // wanted would charge every remote `-L` copy — including `all+acl`, which has nothing to warn
    // about — for a probe that then declines to run.
    let notice = common::safedir::RootAclNotice::from(capture);
    if common::safedir::root_acl_probe_worth_reaching(notice)
        && let Ok((parent, name)) = open_root_parent(src).await
    {
        common::safedir::warn_if_root_acl_unpreserved_at(&parent, &name, src, notice).await;
    }
    // determine if we have a root item to send (for DirStructureComplete message)
    // special files (sockets, FIFOs, devices) never produce protocol messages,
    // so they never count as root items regardless of --skip-specials
    let is_special =
        !src_metadata.is_file() && !src_metadata.is_dir() && !src_metadata.is_symlink();
    let has_root_item = if is_special {
        false
    } else if let Some(ref filter) = settings.filter {
        // for root items, use should_include_root_item which skips anchored patterns
        // (anchored patterns match paths inside the source, not the source itself)
        let file_name = src.file_name().map(std::path::Path::new).unwrap_or(src);
        let is_dir = src_metadata.is_dir();
        matches!(
            filter.should_include_root_item(file_name, is_dir),
            common::filter::FilterResult::Included
        )
    } else {
        true
    };
    if !src_metadata.is_file()
        && let Err(e) = send_directories_and_symlinks(
            settings,
            capture,
            src,
            dst,
            src, // source_root is src for the root item
            // the walk gets THIS classification of the root rather than taking another stat
            // of its own — the same single-snapshot rule `send_root_hardened` follows, and what
            // keeps `has_root_item` below and the walk's dispatch from ever disagreeing.
            Some(&src_metadata),
            &control_send_stream,
            &error_collector,
            deref_state,
        )
        .await
    {
        // a root walk failure is ALWAYS fatal, even in non-fail-early mode (protocol §3.3 Root Item
        // Failure Invariant), matching the hardened twin in `send_root_hardened`. The walk returns
        // `Err` for the root only when it committed NOTHING for it — an ACL read that failed before
        // the `Directory` was sent, an unsupported root type, or a transport failure; every case it
        // can compensate for (an unreadable or vanished directory, an unreadable symlink, a failed
        // child) it handles internally and returns `Ok`, and the cases where it sends nothing at all
        // are exactly the ones `has_root_item` is false for (see the walk's funnel). Continuing
        // would send `DirStructureComplete { has_root_item: true }` below with no root message ever
        // committed, leaving the destination waiting on `root_complete` forever — a hang with both
        // peers alive, which no timeout ends.
        tracing::error!("Failed to send root directories and symlinks: {e:#}");
        return Err(e);
    }
    let mut stream = control_send_stream.lock().await;
    stream
        .send_control_message(&remote::protocol::SourceMessage::DirStructureComplete {
            has_root_item,
        })
        .await?;
    drop(stream);
    if src_metadata.is_file() && !has_root_item {
        // root file was filtered out
        progress().files_skipped.inc();
    }
    if src_metadata.is_file() && has_root_item {
        // `-L` root file: path-based open (following symlinks is requested by design and documented
        // not hardened). The hardened (non-`-L`) root file is handled in `send_root_hardened`.
        if let Err(e) = send_file_tcp(
            settings,
            capture,
            src,
            dst,
            src_metadata.len(),
            remote::protocol::Metadata::from(&src_metadata),
            true,
            stream_pool,
            &error_collector,
            control_send_stream.clone(),
            FileRead::Path,
        )
        .await
        {
            tracing::error!("Failed to send root file: {e:#}");
            // always return error for root file failures -
            // there's nothing else to transfer and the protocol would hang
            return Err(e);
        }
    }
    Ok(())
}

/// Hardened (non-`-L`) root handling: classify the named root ONCE via its trusted parent's fd and
/// drive `has_root_item`, filtering, wire metadata, and the file/symlink/dir/special dispatch from
/// that single authoritative snapshot — there is no second path stat. This closes the double-stat
/// TOCTOU window where a root *kind* swap (e.g. dir→file) between the `has_root_item` decision and
/// the dispatch could announce `has_root_item: true` yet send no root message, hanging the
/// destination. Wire metadata comes from the fd-pinned classification (Guarantee 2) and the file /
/// symlink reads are fd-relative `O_NOFOLLOW` (Guarantee 1).
#[instrument(skip(error_collector, stream_pool, control_send_stream, source_read))]
#[allow(clippy::too_many_arguments)]
async fn send_root_hardened(
    settings: &common::copy::Settings,
    capture: ExtendedMetadataCapture,
    src: &std::path::Path,
    dst: &std::path::Path,
    control_send_stream: remote::streams::BoxedSharedSendStream,
    stream_pool: std::sync::Arc<AcceptingSendStreamPool>,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
    source_read: SourceRead,
) -> anyhow::Result<()> {
    use common::preserve::Metadata as _;
    use common::walk::EntryKind;
    let dir_map = source_read
        .dir_map()
        .expect("hardened source_read carries an fd-map")
        .clone();
    // open the trusted parent prefix and classify the root ONCE (O_PATH | O_NOFOLLOW + fstat).
    let (parent, name) = open_root_parent(src).await?;
    let handle = match parent.child(&name).await {
        Ok(h) => h,
        Err(e) => {
            tracing::error!("Failed reading metadata from src {src:?}: {e:#}");
            // root classification failure is fatal (matches the old root stat failure).
            return Err(e.into());
        }
    };
    let kind = handle.kind();
    // One `listxattr` on the source ROOT per rcpd run — a constant, not a per-entry probe — warning
    // that a source root carrying an ACL is about to be copied by settings that drop it. The
    // warning reaches the user over the existing tracing connection like any other rcpd log line.
    // It goes through the `/proc/self/fd` form precisely because `handle` is `O_PATH` (see below).
    common::safedir::warn_if_root_acl_unpreserved(
        &handle,
        src,
        common::safedir::RootAclNotice::from(capture),
    )
    .await;
    // ACLs stay UNKNOWN here: the classify handle's inode is not necessarily the one whose bytes
    // will be sent (an ACL read from it could describe an inode replaced before copy), so every use
    // below that needs them re-reads from the real fd it
    // is about to use — a root FILE from its data fd in `send_file_tcp`, a root DIRECTORY from
    // the `O_NOFOLLOW` handle the fd-walk opens — and a root SYMLINK has no ACL to carry. The one
    // use that keeps this value is the unopenable-root-directory case, which has no fd to read
    // from and is already an error; `WireAcls::Unknown` then tells the destination to leave the
    // (often REUSED) destination root's ACLs alone rather than clearing state the source never
    // observed.
    let meta = remote::protocol::Metadata::from(handle.meta());
    // has_root_item from the authoritative kind: specials never produce a message; otherwise the
    // root-item filter decides (anchored patterns match inside the source, not the root itself).
    let has_root_item = match kind {
        EntryKind::Special => false,
        _ => match &settings.filter {
            Some(filter) => {
                let file_name = src.file_name().map(std::path::Path::new).unwrap_or(src);
                matches!(
                    filter.should_include_root_item(file_name, kind == EntryKind::Dir),
                    common::filter::FilterResult::Included
                )
            }
            None => true,
        },
    };
    // ── pre-DirStructureComplete: send the root directory tree / symlink (NOT the file) ──
    match kind {
        EntryKind::Dir if has_root_item => {
            // open the dir O_NOFOLLOW from the parent fd and descend via the fd-walk engine. On an
            // open failure (e.g. a swap to a non-dir between classify and open) emit a 0-entry
            // Directory + tombstone so the destination still completes (no hang), mirroring the
            // nested unreadable-directory path.
            match parent.open_dir(&name).await {
                Ok(dir) => {
                    if let Err(e) = send_directory_fd_walk(
                        settings,
                        capture,
                        src,
                        dst,
                        src, // source_root is src for the root item
                        true,
                        Arc::new(dir),
                        &control_send_stream,
                        &error_collector,
                        &dir_map,
                    )
                    .await
                    {
                        // a root-directory walk failure is ALWAYS fatal, even in non-fail-early mode
                        // (protocol §3.3 Root Item Failure Invariant). The walk returns Err only on a
                        // pre-`Directory`-commit failure (root metadata read / fd-map insert) or a
                        // transport failure; collected nested child errors keep it Ok (compensated by
                        // per-child `FileSkipped`s), so there is no "collect and continue" case here.
                        // Continuing would send `DirStructureComplete { has_root_item: true }` with no
                        // root `Directory` committed, hanging the destination forever on `root_complete`.
                        tracing::error!("Failed to send root directory {src:?}: {e:#}");
                        return Err(e);
                    }
                }
                Err(e) => {
                    tracing::error!("Cannot open root directory {src:?} for reading: {e:#}");
                    send_unreadable_directory(
                        src,
                        dst,
                        meta.clone(),
                        true,
                        &control_send_stream,
                        &error_collector,
                        settings.fail_early,
                        e.into(),
                        Some(&dir_map),
                        None,
                    )
                    .await?;
                }
            }
        }
        EntryKind::Symlink if has_root_item => {
            // read the target AND metadata inode-exact from the one pinned handle (`read_symlink`),
            // so a same-name symlink swap can't pair one link's target with another link's
            // owner/timestamps — target and metadata are a faithful pair, matching the file path. A
            // swap to a non-symlink fails the read and is accounted as skipped.
            match handle.read_symlink(parent.side()).await {
                Ok((target, sym_meta)) => {
                    let symlink = remote::protocol::SourceMessage::Symlink {
                        src: src.to_path_buf(),
                        dst: dst.to_path_buf(),
                        target,
                        metadata: remote::protocol::Metadata::from(&sym_meta),
                        is_root: true,
                    };
                    control_send_stream
                        .lock()
                        .await
                        .send_batch_message(&symlink)
                        .await?;
                }
                Err(e) => {
                    let e: anyhow::Error = e.into();
                    tracing::error!("Failed reading root symlink {src:?}: {e:#}");
                    send_symlink_skipped(src, dst, true, &control_send_stream).await?;
                    if settings.fail_early {
                        return Err(e);
                    }
                    error_collector.push(e);
                }
            }
        }
        EntryKind::Special if !settings.skip_specials => {
            let err = anyhow::anyhow!(
                "copy: {src:?} -> {dst:?} failed, unsupported src file type (special file)"
            );
            tracing::error!("{:#}", &err);
            // a special root with no --skip-specials is fatal (matches the path-based body).
            return Err(err);
        }
        EntryKind::Special => {
            progress().specials_skipped.inc();
        }
        // filtered-out dir/symlink: account the skip. The root file is handled after
        // DirStructureComplete (its data rides the file stream).
        EntryKind::Dir | EntryKind::Symlink => {
            kind.inc_skipped(progress());
        }
        EntryKind::File => {}
    }
    // ── DirStructureComplete ──
    control_send_stream
        .lock()
        .await
        .send_control_message(&remote::protocol::SourceMessage::DirStructureComplete {
            has_root_item,
        })
        .await?;
    // ── post-DirStructureComplete: the root file's data (fd-relative, O_NOFOLLOW) ──
    if kind == EntryKind::File {
        if !has_root_item {
            progress().files_skipped.inc();
        } else {
            let size = handle.meta().size();
            // the data task opens and pins the authoritative file independently through the held
            // parent. close this earlier classification fd before acquiring that region's admission.
            drop(handle);
            if let Err(e) = send_file_tcp(
                settings,
                capture,
                src,
                dst,
                size,
                meta,
                true,
                stream_pool,
                &error_collector,
                control_send_stream.clone(),
                FileRead::Hardened(parent, name),
            )
            .await
            {
                tracing::error!("Failed to send root file: {e:#}");
                // nothing else to transfer; returning the error avoids a protocol hang.
                return Err(e);
            }
        }
    }
    Ok(())
}

/// How a single file's DATA is opened for sending.
///
/// This is the file-open analogue of [`SourceRead`]: the hardened/path choice is a
/// type, not a nullable handle, so there is no ambiguous "hardened but no handle"
/// state at this seam. Hardened Pass 2 always holds the directory's `Arc<Dir>` (it
/// took the owned [`MapEntry`]), so it always constructs [`FileRead::Hardened`];
/// only the `-L` walk (which follows symlinks by design) constructs
/// [`FileRead::Path`]. The hardened root file is read via [`FileRead::Hardened`]
/// in [`send_root_hardened`]. The hardened-miss fail-closed decision is made once,
/// earlier, in the dispatch loop — not re-litigated per file.
enum FileRead {
    /// Open fd-relative from the directory's held fd via `open_file_read(name)`
    /// (TOCTOU-safe: `O_NOFOLLOW` + `S_ISREG`, no path re-resolution).
    Hardened(Arc<Dir>, std::ffi::OsString),
    /// Path-based `File::open(src)`: the `-L`/`--dereference` walk (follows symlinks by design).
    Path,
}

#[instrument(skip(error_collector, control_send_stream, stream_pool, file_read))]
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn send_file_tcp(
    settings: &common::copy::Settings,
    capture: ExtendedMetadataCapture,
    src: &std::path::Path,
    dst: &std::path::Path,
    size: u64,
    metadata: remote::protocol::Metadata,
    is_root: bool,
    stream_pool: std::sync::Arc<AcceptingSendStreamPool>,
    error_collector: &std::sync::Arc<common::error_collector::ErrorCollector>,
    control_send_stream: remote::streams::BoxedSharedSendStream,
    // how to open this file's data: fd-relative (hardened) or by path (`-L`).
    file_read: FileRead,
) -> anyhow::Result<()> {
    let prog = progress();
    let _ops_guard = prog.ops.guard();
    tracing::debug!("Sending file content for {:?}", src);
    // borrow a stream FIRST to provide backpressure. files are only opened after we have
    // a stream available, which limits memory usage when destination is slow.
    let mut pooled_stream = stream_pool
        .borrow()
        .instrument(tracing::trace_span!("borrow_stream"))
        .await?;
    // now that we have a stream, acquire file-related resources
    let open_file_guard = throttle::open_file_permit()
        .instrument(tracing::trace_span!("open_file_permit"))
        .await;
    let admission = open_file_guard.admission();
    common::safedir::with_fd_admission(admission, async move {
        let _open_file_guard = open_file_guard;
        throttle::get_file_iops_tokens(settings.chunk_size, size)
            .instrument(tracing::trace_span!("iops_throttle", size))
            .await;
        // open the file AFTER borrowing a stream for backpressure. on the hardened path
        // open fd-relative (O_NOFOLLOW + S_ISREG, no path re-resolution) so a concurrent
        // symlink swap can't redirect the read; the path-based open is only for the
        // `-L`/`--dereference` walk (which follows symlinks by design).
        let open_result = match &file_read {
            FileRead::Hardened(dir, name) => dir
                .open_file_read(name)
                .instrument(tracing::trace_span!("file_open"))
                .await
                .map(|(file, meta)| (tokio::fs::File::from_std(file), Some(meta))),
            FileRead::Path => {
                let src = src.to_owned();
                common::safedir::run_metadata_probed_blocking(
                    common::Side::Source,
                    common::MetadataOp::Stat,
                    move || std::fs::File::open(src), // rcp-toctou-allow: -L path (dereference, documented not hardened)
                )
                .instrument(tracing::trace_span!("file_open"))
                .await
                .map(|file| (tokio::fs::File::from_std(file), None))
            }
        };
        // read the source ACL from the SAME fd whose bytes are about to be sent (read-side fidelity,
        // docs/tocttou.md): a probe by path could be answered by a different inode than the one being
        // transferred, pairing one file's permissions with another's contents. Files have no default
        // ACL, so only the access one is asked for. Only when the master asked at all — with `f:acl`
        // off this issues no xattr syscall, which is the whole reason the capture field is on the wire.
        // Folded into `open_result` so a failure takes the same accounted path as a failed open: the
        // header has not been sent, so the destination is still owed exactly one entry for this file.
        let open_result = match open_result {
            Ok((file, meta)) if capture.file_acl => {
                common::safedir::read_acls_fd(file.as_fd(), common::Side::Source, false)
                    .await
                    .map(|acls| (file, meta, Some(acls)))
            }
            Ok((file, meta)) => Ok((file, meta, None)),
            Err(e) => Err(e),
        };
        let (file, read_meta, src_acls) = match open_result {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("Failed to read file {src:?} for sending: {e:#}");
                // stream is returned to pool via Drop when pooled_stream goes out of scope
                // for root file copies, failing to open the file is a fatal error -
                // there's nothing else to transfer and the protocol would hang
                if is_root {
                    return Err(e.into());
                }
                // notify destination that this file was skipped (for directory tracking)
                let skip_msg = remote::protocol::SourceMessage::FileSkipped {
                    src: src.to_path_buf(),
                    dst: dst.to_path_buf(),
                };
                control_send_stream
                    .lock()
                    .await
                    .send_batch_message(&skip_msg)
                    .await?;
                if settings.fail_early {
                    // Defense-in-depth: also push to error_collector so
                    // run_source's take_error() catches this even when the
                    // Err return below loses a race against the destination's
                    // DestinationDone (which causes the shutdown drain in
                    // dispatch_control_messages_tcp to swallow this task's
                    // Err). anyhow::Error isn't Clone, so push a formatted
                    // copy and keep the original chain in the Err return.
                    let err: anyhow::Error = e.into();
                    error_collector.push(anyhow::anyhow!("{err:#}"));
                    return Err(err);
                }
                error_collector.push(e.into());
                return Ok(());
            }
        };
        // Permission/ownership fidelity (Guarantee 2, docs/tocttou.md): the wire header must
        // describe the bytes we actually send. On the hardened path the data fd was opened
        // fd-relative by name, so a concurrent same-name swap can change which regular file it
        // resolves to; derive size + metadata (mode/owner/times) from THAT fd's fstat, not the
        // Pass-1 classification, so the destination never writes one file's contents under
        // another's size/mode and the stream honors the "exactly size bytes" invariant. The
        // `-L`/root path keeps its caller-supplied values (read_meta is None).
        let (size, metadata) = match &read_meta {
            Some(meta) => {
                use common::preserve::Metadata as _;
                (meta.size(), remote::protocol::Metadata::from(meta))
            }
            None => (size, metadata),
        };
        // attach the ACLs read above (both branches: they came from the data fd either way).
        let metadata = match &src_acls {
            Some(acls) => metadata.with_acls(acls),
            None => metadata,
        };
        // wrap file in a buffered reader for better network throughput
        // buffer size is set by tcp_config.effective_remote_copy_buffer_size() based on network profile,
        // but capped at file size to avoid over-allocation for small files
        let file_size = size.min(usize::MAX as u64) as usize;
        let buffer_size = settings.remote_copy_buffer_size.min(file_size).max(1);
        let mut buffered_file = tokio::io::BufReader::with_capacity(buffer_size, file);
        let file_header = remote::protocol::File {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            size,
            metadata,
            is_root,
        };
        let send_result = pooled_stream
            .stream_mut()
            .send_message_with_data_buffered(&file_header, &mut buffered_file)
            .instrument(tracing::trace_span!("send_data", size, buffer_size))
            .await;
        match send_result {
            Ok(_bytes_sent) => {
                // stream is returned to pool when pooled_stream is dropped
                prog.files_copied.inc();
                prog.bytes_copied.add(size);
                tracing::info!("Sent file: {:?} -> {:?}", src, dst);
                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to send file content for {src:?}: {e:#}");
                // don't return stream to pool on error - it's in a bad state.
                // close it immediately.
                if let Some(mut bad_stream) = pooled_stream.take_and_discard() {
                    // best effort close; ignore errors since stream is already broken
                    let _ = bad_stream.close().await;
                }
                // transport failure is always fatal - destination is waiting on this connection
                // and we can't recover from stream corruption.
                Err(e)
            }
        }
    })
    .await
}

/// A file selected for sending in Pass 2, with its size and wire metadata already
/// resolved (from an fd-pinned `FileMeta` on the hardened path, or a path
/// `std::fs::Metadata` on the `-L` fallback). `name` lets the data open go
/// fd-relative against the held source directory.
struct FileToSend {
    src_path: std::path::PathBuf,
    dst_path: std::path::PathBuf,
    name: std::ffi::OsString,
    size: u64,
    metadata: remote::protocol::Metadata,
}

/// The owned input to Pass 2 for one directory. Ownership is linear: the dispatch
/// loop consumes the directory's map entry (or builds the dereference variant) and
/// moves the result into the spawned Pass-2 task — there is no clone-and-leave +
/// deferred RAII cleanup.
///
/// - [`Pass2Source::Hardened`] carries the owned [`MapEntry`]: its `Arc<Dir>` (when
///   present) lets the file data opens go fd-relative, its `file_count` is the
///   authoritative Pass-1 count, and its held fd-budget permit is released when the
///   entry drops at the end of Pass 2 (the entry is owned by the task for its whole
///   lifetime). For a *tombstone* (committed-but-unreadable directory) the entry is a
///   [`MapEntry::Tombstone`] (no fd, file count 0), so Pass 2 returns immediately,
///   sending no files and needing no fd.
/// - [`Pass2Source::DereferencePath`] carries the source-side `-L` map entry, including its
///   outstanding-directory credit. The `-L` walk retains no pinned Pass-1 directory fd and
///   re-enumerates by path, with a transient `ReadDir` descriptor, but the credit stays owned until
///   this Pass-2 task finishes so it has the same pacing lifetime as the hardened map permit.
/// - [`Pass2Source::DereferencePathMissing`] represents an absent or duplicate trusted-peer
///   acknowledgement. Dereference mode remains fail-open with empty contents, but this variant has
///   no credit and therefore cannot inflate the configured budget.
enum Pass2Source {
    Hardened(MapEntry),
    DereferencePath(DereferencePass1Entry),
    DereferencePathMissing,
}

impl Pass2Source {
    /// The authoritative expected file count for this directory: the Pass-1 count
    /// stored in the map entry for either source mode.
    fn file_count(&self) -> usize {
        match self {
            Pass2Source::Hardened(MapEntry::Readable { contents, .. }) => contents.file_count,
            Pass2Source::Hardened(MapEntry::Tombstone) => 0,
            Pass2Source::DereferencePath(entry) => entry.contents.file_count,
            Pass2Source::DereferencePathMissing => 0,
        }
    }

    /// Names Pass 1 already accounts for and Pass 2 must therefore ignore, whatever they look like
    /// now. Empty for a tombstone (a 0-entry `Directory` claims no children at all). See
    /// [`Pass1Contents`] for what goes wrong without this.
    fn non_files(&self) -> Option<&std::collections::HashSet<std::ffi::OsString>> {
        match self {
            Pass2Source::Hardened(MapEntry::Readable { contents, .. }) => Some(&contents.non_files),
            Pass2Source::Hardened(MapEntry::Tombstone) => None,
            Pass2Source::DereferencePath(entry) => Some(&entry.contents.non_files),
            Pass2Source::DereferencePathMissing => None,
        }
    }

    /// The held source directory fd for fd-relative file opens, or `None` under
    /// `-L` (path-based enumeration) and for a hardened tombstone (no fd, 0 files).
    fn dir(&self) -> Option<&Arc<Dir>> {
        match self {
            Pass2Source::Hardened(MapEntry::Readable { dir, .. }) => Some(dir),
            Pass2Source::Hardened(MapEntry::Tombstone) => None,
            Pass2Source::DereferencePath(_) | Pass2Source::DereferencePathMissing => None,
        }
    }
}

/// Resolve the owned Pass-2 input for a `DirectoryCreated { src, dst }`, applying
/// the hardened fail-closed rule. This is the TOCTOU-safety seam. The file count is recovered from
/// the consumed source-side map entry in either mode.
///
/// - `SourceRead::Hardened`: CONSUME the directory's held fd-map entry (one-shot
///   ownership) and use its stored Pass-1 `file_count`. The entry may be a real
///   held-fd entry or a tombstone (committed-but-unreadable directory, `dir: None`,
///   `file_count: 0`) — both are legitimately committed entries, so both are
///   consumed normally. On a MISS — the entry is gone (never inserted, or already
///   consumed by a prior `DirectoryCreated` for the same `src`) — FAIL CLOSED: this
///   is a TOCTOU-safety / protocol-invariant violation, so return an error (the
///   dispatch loop then breaks and releases the fd-budget ONCE post-loop, unblocking
///   any parked Pass-1 walk and tearing the copy down cleanly). NEVER fall back to a
///   path-based read.
/// - `SourceRead::DereferencePath`: the `-L` walk retains no pinned directory fd across phases.
///   Recover the Pass-1 count and owned credit from the path-keyed map. A missing entry is treated
///   as count 0 with a debug log — `-L` is intentionally NOT hardened, so a miss is not a
///   TOCTOU/fail-closed condition (the destination's `entries_expected` is the Pass-1 count, and
///   Pass 2 does not re-count). The entry is CONSUMED (removed) and transferred into Pass 2,
///   mirroring the hardened one-shot lifecycle: a directory's files are requested exactly once,
///   map memory stays bounded, and its credit remains held until that task finishes.
fn resolve_pass2_source(
    source_read: &SourceRead,
    src: &std::path::Path,
) -> anyhow::Result<Pass2Source> {
    match source_read {
        SourceRead::Hardened(map) => match map.take_for_created(src) {
            Some(entry) => Ok(Pass2Source::Hardened(entry)),
            None => {
                let err = anyhow::anyhow!(
                    "hardened source read: no held directory fd for {src:?} on DirectoryCreated \
                     (TOCTOU-safety violation: refusing to re-resolve by path)"
                );
                tracing::error!("{:#}", &err);
                Err(err)
            }
        },
        SourceRead::DereferencePath(counts) => Ok(match counts.take_for_created(src) {
            Some(entry) => Pass2Source::DereferencePath(entry),
            None => {
                tracing::debug!(
                    "no recorded -L Pass-1 contents for {src:?} on DirectoryCreated; defaulting \
                         to empty (dereference path is not hardened, so this is not a fail-closed \
                         condition)"
                );
                Pass2Source::DereferencePathMissing
            }
        }),
    }
}

/// Handle Pass 2 failing to read a directory it committed to: emit a synthetic
/// `FileSkipped` for every expected file so the destination's per-directory tally
/// still reaches zero and it can complete, then propagate per the `fail_early`
/// policy. Shared by the hardened and path-based enumeration paths.
async fn send_files_missing_directory(
    src: &std::path::Path,
    dst: &std::path::Path,
    file_count: usize,
    settings: &common::copy::Settings,
    error_collector: &std::sync::Arc<common::error_collector::ErrorCollector>,
    control_send_stream: &remote::streams::BoxedSharedSendStream,
    err: anyhow::Error,
) -> anyhow::Result<()> {
    for i in 0..file_count {
        let skip_msg = remote::protocol::SourceMessage::FileSkipped {
            src: src.join(format!("<missing-{i}>")),
            dst: dst.join(format!("<missing-{i}>")),
        };
        control_send_stream
            .lock()
            .await
            .send_batch_message(&skip_msg)
            .await?;
    }
    if settings.fail_early {
        // Defense-in-depth: the FileSkipped messages above let the destination
        // tally to zero and emit DestinationDone, which can race with the Err
        // return below and cause the shutdown drain in
        // dispatch_control_messages_tcp to swallow this task's error. Push a
        // formatted copy into the collector so run_source's take_error() catches
        // it, keeping the original chain in the Err return.
        error_collector.push(anyhow::anyhow!("{err:#}"));
        return Err(err);
    }
    error_collector.push(err);
    Ok(())
}

#[instrument(skip(
    error_collector,
    control_send_stream,
    stream_pool,
    pending_limit,
    pass2_source,
    existing
))]
#[allow(clippy::too_many_arguments)]
async fn send_files_in_directory_tcp(
    settings: common::copy::Settings,
    capture: ExtendedMetadataCapture,
    src: std::path::PathBuf,
    dst: std::path::PathBuf,
    source_root: std::path::PathBuf,
    // owned Pass-2 input: the held map entry from either source mode. Carries the authoritative
    // `file_count`; hardened entries also carry the `Dir` fd used to open file DATA fd-relative
    // (None for a tombstone), while `-L` entries carry the outstanding-directory credit. The owned
    // permit/credit is released when this function returns (entry dropped here).
    pass2_source: Pass2Source,
    stream_pool: std::sync::Arc<AcceptingSendStreamPool>,
    pending_limit: std::sync::Arc<tokio::sync::Semaphore>,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
    control_send_stream: remote::streams::BoxedSharedSendStream,
    existing: std::sync::Arc<
        std::collections::HashMap<std::path::PathBuf, remote::protocol::ExistingEntry>,
    >,
) -> anyhow::Result<()> {
    // the Pass-1 count is authoritative for this directory's send logic (truncation and synthetic
    // `FileSkipped`). It comes entirely from the consumed source-mode map entry; the destination
    // echoes nothing.
    let file_count = pass2_source.file_count();
    // names Pass 1 counted as directories or symlinks and therefore accounts for itself. This
    // enumeration re-reads the directory, so any of them that has since become a regular file
    // would otherwise be counted a SECOND time against the same parent — see `Pass1Contents`.
    let owned_by_pass1 = |name: &std::ffi::OsStr| {
        pass2_source
            .non_files()
            .is_some_and(|names| names.contains(name))
    };
    let src_dir = pass2_source.dir();
    tracing::info!(
        "Sending files from {src:?} (expected file_count={})",
        file_count
    );
    // if no files are expected, nothing to do. the owned entry, if any, drops here and releases its
    // hardened dir-fd permit or `-L` credit back to Pass 1.
    if file_count == 0 {
        return Ok(());
    }
    // iterate directory and collect files to send
    let mut file_entries: Vec<FileToSend> = Vec::new();
    if let Some(dir) = src_dir {
        // hardened enumeration: list + classify fd-relative (never follows a symlink).
        let raw_entries = match dir.read_entries().await {
            Ok(entries) => entries,
            Err(e) => {
                tracing::error!("Cannot enumerate directory {src:?} for reading: {e:#}");
                return send_files_missing_directory(
                    &src,
                    &dst,
                    file_count,
                    &settings,
                    &error_collector,
                    &control_send_stream,
                    e.into(),
                )
                .await;
            }
        };
        for (entry_name, _hint) in raw_entries {
            // Pass 1 owns this name (it counted it as a directory or symlink) — skip it before
            // the classify, which would otherwise cost a syscall to reach the same conclusion.
            if owned_by_pass1(&entry_name) {
                continue;
            }
            let entry_path = src.join(&entry_name);
            let dst_path = dst.join(&entry_name);
            let handle = match dir.child(&entry_name).await {
                Ok(h) => h,
                Err(e) => {
                    let e: anyhow::Error = e.into();
                    tracing::error!("Failed reading metadata from {entry_path:?}: {e:#}");
                    if settings.fail_early {
                        return Err(e);
                    }
                    error_collector.push(e);
                    continue;
                }
            };
            if handle.kind() != common::walk::EntryKind::File {
                continue;
            }
            // apply filter if configured
            if let Some(ref filter) = settings.filter {
                let relative_path = common::walk::relative_to_root(&entry_path, &source_root);
                match filter.should_include(relative_path, false) {
                    common::filter::FilterResult::Included => { /* proceed */ }
                    result => {
                        tracing::debug!(
                            "Filtered out file {:?} (relative: {:?}): {:?}",
                            entry_path,
                            relative_path,
                            result
                        );
                        progress().files_skipped.inc();
                        continue;
                    }
                }
            }
            let meta = handle.meta();
            file_entries.push(FileToSend {
                src_path: entry_path,
                dst_path,
                name: entry_name,
                size: {
                    use common::preserve::Metadata as _;
                    meta.size()
                },
                metadata: remote::protocol::Metadata::from(meta),
            });
        }
    } else {
        // path-based enumeration (`-L`/--dereference): nested symlink following is intentionally
        // not hardened. The open takes an ops token for the same reason Pass 1's does — the
        // hardened `Dir::read_entries` in the branch above already does.
        throttle::get_ops_token().await;
        let mut entries = match tokio::fs::read_dir(&src).await {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("Cannot open directory {src:?} for reading: {e:#}");
                return send_files_missing_directory(
                    &src,
                    &dst,
                    file_count,
                    &settings,
                    &error_collector,
                    &control_send_stream,
                    e.into(),
                )
                .await;
            }
        };
        loop {
            match common::walk::next_entry_probed(&mut entries, common::Side::Source, || {
                format!("failed traversing src directory {:?}", &src)
            })
            .await
            {
                Ok(Some((entry, _file_type))) => {
                    let entry_path = entry.path();
                    let entry_name = entry_path.file_name().unwrap().to_owned();
                    // Pass 1 owns this name (it counted it as a directory or symlink) — skip it
                    // before the stat, which would otherwise cost a syscall to reach the same
                    // conclusion.
                    if owned_by_pass1(&entry_name) {
                        continue;
                    }
                    let dst_path = dst.join(&entry_name);
                    let entry_metadata = match common::walk::run_metadata_probed(
                        common::Side::Source,
                        common::MetadataOp::Stat,
                        async {
                            if settings.dereference {
                                tokio::fs::metadata(&entry_path).await
                            } else {
                                tokio::fs::symlink_metadata(&entry_path).await
                            }
                        },
                    )
                    .await
                    {
                        Ok(m) => m,
                        Err(e) => {
                            tracing::error!("Failed reading metadata from {entry_path:?}: {e:#}");
                            if settings.fail_early {
                                return Err(e.into());
                            }
                            error_collector.push(e.into());
                            continue;
                        }
                    };
                    if entry_metadata.is_file() {
                        // apply filter if configured
                        if let Some(ref filter) = settings.filter {
                            let relative_path =
                                entry_path.strip_prefix(&source_root).unwrap_or(&entry_path);
                            match filter.should_include(relative_path, false) {
                                common::filter::FilterResult::Included => { /* proceed */ }
                                result => {
                                    tracing::debug!(
                                        "Filtered out file {:?} (relative: {:?}): {:?}",
                                        entry_path,
                                        relative_path,
                                        result
                                    );
                                    progress().files_skipped.inc();
                                    continue;
                                }
                            }
                        }
                        file_entries.push(FileToSend {
                            src_path: entry_path,
                            dst_path,
                            name: entry_name,
                            size: entry_metadata.len(),
                            metadata: remote::protocol::Metadata::from(&entry_metadata),
                        });
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::error!("Failed traversing src directory {src:?}: {e:#}");
                    if settings.fail_early {
                        return Err(e);
                    }
                    error_collector.push(e);
                    break;
                }
            }
        }
        drop(entries);
    }
    let files_found = file_entries.len();
    tracing::info!(
        "Directory {:?} has {} files to send (expected {})",
        src,
        files_found,
        file_count
    );
    // handle discrepancy between the authoritative (Pass-1) file_count and the
    // files actually found at send time (the directory may have changed since the
    // Pass-1 pre-read)
    if files_found > file_count {
        // More files than Pass 1 counted, and the destination is expecting exactly `file_count`
        // responses for this directory, so `files_found - file_count` of them cannot be sent at
        // all. Which ones are dropped is `readdir` order, so the casualty can be a file Pass 1
        // genuinely counted — a source file silently missing from the destination. That must
        // never leave a copy reporting success, so it is an ERROR rather than a warning:
        // truncating keeps the destination's accounting balanced (it still completes rather than
        // hanging), and the recorded error makes the copy exit non-zero naming the directory.
        //
        // Names Pass 1 counted as directories or symlinks are already excluded above, so this is
        // now reached only by a name Pass 1 never counted — a file genuinely created mid-copy.
        let err = anyhow::anyhow!(
            "directory {:?} contents changed: expected {} files, found {} — {} file(s) will not \
             be copied (the destination expects exactly the traversal-time count)",
            src,
            file_count,
            files_found,
            files_found - file_count
        );
        tracing::error!("{:#}", &err);
        if settings.fail_early {
            return Err(err);
        }
        error_collector.push(err);
        file_entries.truncate(file_count);
    }
    let files_to_send = file_entries.len();
    // send the files
    let mut join_set = tokio::task::JoinSet::new();
    for file in file_entries {
        throttle::get_ops_token().await;
        // skip transfer entirely when the destination already has a matching entry (per the
        // manifest the destination sent in DirectoryCreated). this never opens a data connection.
        let src_fm = remote::protocol::FileMetadata {
            metadata: &file.metadata,
            size: file.size,
        };
        let skip = match existing.get(std::path::Path::new(&file.name)) {
            Some(e) => {
                let dst_fm = remote::protocol::FileMetadata {
                    metadata: &e.metadata,
                    size: e.size,
                };
                common::copy::skip_unchanged_send(
                    &settings.overwrite_compare,
                    settings.overwrite_filter,
                    settings.ignore_existing,
                    &src_fm,
                    Some(common::copy::ExistingDst {
                        meta: &dst_fm,
                        is_file: e.is_file,
                    }),
                )
            }
            None => false,
        };
        if skip {
            tracing::info!(
                "destination already has identical file, skipping transfer (manifest): {:?} -> {:?}",
                file.src_path,
                file.dst_path
            );
            let msg = remote::protocol::SourceMessage::FileUnchanged {
                src: file.src_path.clone(),
                dst: file.dst_path.clone(),
            };
            control_send_stream
                .lock()
                .await
                .send_batch_message(&msg)
                .await?;
            continue;
        }
        // wait for a pending slot - this is the main backpressure point
        let permit = pending_limit
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("pending limit semaphore closed"))?;
        let pool = stream_pool.clone();
        let collector = error_collector.clone();
        let control_stream = control_send_stream.clone();
        let settings = settings.clone();
        // on the hardened path, open file data fd-relative against the held source
        // directory (clone the Arc per file so all spawned tasks share the one fd).
        // in `-L` mode there is no held fd, so the data open is by path.
        let file_read = match src_dir {
            Some(dir) => FileRead::Hardened(dir.clone(), file.name.clone()),
            None => FileRead::Path,
        };
        let FileToSend {
            src_path,
            dst_path,
            size,
            metadata,
            ..
        } = file;
        join_set.spawn(async move {
            let result = send_file_tcp(
                &settings,
                capture,
                &src_path,
                &dst_path,
                size,
                metadata,
                false,
                pool,
                &collector,
                control_stream,
                file_read,
            )
            .await;
            drop(permit); // release permit when file is done
            result
        });
    }
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                // transport errors from send_file_tcp are always fatal regardless of fail_early.
                // file-level errors (permission denied, etc.) are handled inside send_file_tcp
                // by sending FileSkipped and returning Ok(()).
                tracing::error!("Transport failure sending file from {src:?}: {e:#}");
                return Err(e);
            }
            Err(e) => {
                tracing::error!("Task panicked while sending file from {src:?}: {e:#}");
                return Err(e.into());
            }
        }
    }
    // handle deficit: files disappeared since traversal
    // send synthetic FileSkipped messages so destination's entry count still completes
    if files_to_send < file_count {
        let deficit = file_count - files_to_send;
        tracing::warn!(
            "Directory {:?} has {} fewer files since traversal, sending synthetic FileSkipped",
            src,
            deficit
        );
        for i in 0..deficit {
            let skip_msg = remote::protocol::SourceMessage::FileSkipped {
                src: src.join(format!("<disappeared-{i}>")),
                dst: dst.join(format!("<disappeared-{i}>")),
            };
            control_send_stream
                .lock()
                .await
                .send_batch_message(&skip_msg)
                .await?;
        }
    }
    Ok(())
}

/// Result of receiving a message from the control stream
enum RecvResult {
    Message(remote::protocol::DestinationMessage),
    StreamClosed,
    Error(anyhow::Error),
}

/// Dispatches control messages from destination and coordinates file sending.
///
/// # Shutdown Flow
///
/// This function must signal pool shutdown before draining tasks to prevent deadlock.
/// The flow differs between graceful and unexpected shutdown:
///
/// ## Graceful Shutdown (DestinationDone received)
/// 1. Receive `DestinationDone` message from destination
/// 2. Set `shutdown_initiated = true`, break main loop
/// 3. Signal pool shutdown via `pool_shutdown.cancel()` - this closes the pool's
///    send channel, causing any `borrow()` calls to return error
/// 4. Drain remaining tasks in `join_set` - they complete quickly since pool is closed
/// 5. Close control stream, return Ok
///
/// ## Unexpected Shutdown (StreamClosed without DestinationDone)
/// This happens when destination fails (e.g., fail-early error) and closes connections
/// without sending DestinationDone:
/// 1. Receive `StreamClosed` from control stream
/// 2. Warn that the destination aborted or died, break main loop with Ok (not an error *here* —
///    see the arm itself for why re-reporting it would be wrong)
/// 3. **Critical**: Signal pool shutdown via `pool_shutdown.cancel()` BEFORE draining
/// 4. Drain remaining tasks - they now return error from `borrow()` instead of hanging
/// 5. Return Ok (errors during unexpected shutdown are logged but not propagated)
///
/// ## Deadlock Prevention
/// Without step 3 in unexpected shutdown, tasks waiting on `stream_pool.borrow()` would
/// hang forever because:
/// - The pool's recv channel waits for streams from accept loop
/// - Accept loop waits for connections from destination
/// - Destination has already closed and won't connect
/// - Pool shutdown only happens AFTER this function returns (in handle_connection)
/// - Deadlock: this function waits for tasks, tasks wait for pool, pool waits for shutdown
#[instrument(skip(
    error_collector,
    stream_pool,
    control_recv_stream,
    control_send_stream,
    pool_shutdown,
    source_read
))]
#[allow(clippy::too_many_arguments)]
async fn dispatch_control_messages_tcp(
    settings: common::copy::Settings,
    capture: ExtendedMetadataCapture,
    source_root: std::path::PathBuf,
    mut control_recv_stream: remote::streams::BoxedRecvStream,
    control_send_stream: remote::streams::BoxedSharedSendStream,
    stream_pool: std::sync::Arc<AcceptingSendStreamPool>,
    max_pending_files: usize,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
    pool_shutdown: PoolShutdownToken,
    // shared slot into which this task publishes its fatal loop error (if any) BEFORE releasing the
    // applicable Pass-1 pacing gate, so the caller can report the real cause even when the gate
    // wakeup would mask it
    fatal_error: std::sync::Arc<std::sync::Mutex<Option<anyhow::Error>>>,
    // explicit source read mode. In hardened mode each directory's `DirectoryCreated`
    // consumes the held fd-map entry (the owned `Dir` + permit) for Pass 2 and a miss
    // fails closed; under `-L` there is no fd-map and Pass 2 re-enumerates by path.
    source_read: SourceRead,
) -> anyhow::Result<()> {
    // destruction backstop: cancellation or panic unwind closes the applicable Pass-1 pacing gate
    // (the hardened dir-fd budget or `-L` credit) so a parked walk is released. every normal exit
    // still closes it explicitly before the task drain; hold the guard from here so it covers the
    // whole body
    let _fd_budget_closer = FdBudgetCloser(source_read.clone());
    // create semaphore to limit pending file tasks for backpressure
    let pending_limit = std::sync::Arc::new(tokio::sync::Semaphore::new(max_pending_files));
    tracing::info!(
        "Created pending file limiter with {} permits",
        max_pending_files
    );
    let mut join_set = tokio::task::JoinSet::new();
    // flag to track when graceful shutdown has been initiated (DestinationDone received).
    // after this, task errors (like "unknown stream") are expected and should be ignored.
    let mut shutdown_initiated = false;
    // spawn a separate task to receive messages from destination.
    // this is needed because recv_object is NOT cancel-safe (it reads length-prefixed messages),
    // so we can't use it directly in select!. channel recv IS cancel-safe.
    let (msg_tx, mut msg_rx) = tokio::sync::mpsc::channel::<RecvResult>(16);
    let recv_task = tokio::spawn(async move {
        loop {
            match control_recv_stream
                .recv_object::<remote::protocol::DestinationMessage>()
                .await
            {
                Ok(Some(msg)) => {
                    if msg_tx.send(RecvResult::Message(msg)).await.is_err() {
                        break; // receiver dropped
                    }
                }
                Ok(None) => {
                    let _ = msg_tx.send(RecvResult::StreamClosed).await;
                    break;
                }
                Err(e) => {
                    let _ = msg_tx.send(RecvResult::Error(e)).await;
                    break;
                }
            }
        }
        control_recv_stream.close().await;
    });
    // accumulate per-directory manifest chunks (keyed by dst) until the directory's
    // DirectoryCreated arrives; the FIFO control stream delivers all of a directory's chunks
    // before its trigger, so the manifest is complete when we assemble it.
    let mut manifest_chunks: std::collections::HashMap<
        std::path::PathBuf,
        Vec<remote::protocol::ExistingEntry>,
    > = std::collections::HashMap::new();
    // main loop - select between task completions and messages (both are cancel-safe)
    let result = loop {
        tokio::select! {
            // biased ensures we check tasks first, giving priority to error detection
            biased;
            // check for task completions/failures.
            // transport errors are always fatal - they indicate stream corruption or connection
            // failure, leaving the destination waiting for files that will never arrive.
            // file-level errors are handled inside send_files_in_directory_tcp by returning Ok(()).
            task_result = join_set.join_next(), if !join_set.is_empty() => {
                if let Some(result) = task_result {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            tracing::error!("Transport failure in directory send task: {e:#}");
                            break Err(e);
                        }
                        Err(e) => {
                            tracing::error!("Task panicked: {e:#}");
                            break Err(e.into());
                        }
                    }
                }
            }
            // receive message from destination (via channel - cancel safe)
            recv_result = msg_rx.recv() => {
                let message = match recv_result {
                    Some(RecvResult::Message(m)) => m,
                    // clean EOF on the destination's control stream. `shutdown_initiated` can never
                    // be true here — the `DestinationDone` arm below breaks the loop itself — so this
                    // arm ALWAYS means "EOF without DestinationDone": the destination aborted or
                    // died mid-copy, the unexpected-shutdown flow this function's doc comment
                    // describes. Both cases reach it, and the wording must fit both — a `--fail-early`
                    // destination closes this stream DELIBERATELY, while still alive, as the teardown
                    // signal (docs/remote_protocol.md, `DirectorySkipped` §2.2, "a data-path abort is
                    // signaled by closing the control stream"), whereas a crashed or SIGKILLed one
                    // sends nothing at all.
                    // Deliberately NOT an error, and the reason is the MASTER's read — NOT that the
                    // destination reports its own failure, which is only true when it is alive to do
                    // so. The master's read of the destination's `RcpdResult` is MANDATORY (see
                    // `rcp/src/bin/rcp.rs`): a destination that dies silently yields `Err` or
                    // `Ok(None)` there and the master `?`s out non-zero regardless. So staying quiet
                    // here conceals nothing, while returning `Err` would add a second report of one
                    // event. Note the claim is about this BRANCH, not the source process — the same
                    // close still fails the source through the budget-wakeup teardown at the end of
                    // `handle_connection`, and its own in-flight sends fail against a dead peer.
                    // The warning is the whole delivered value — a log read after the fact says the
                    // destination went away instead of "finished dispatching control messages".
                    // The fd-budget is released ONCE after the loop, covering every arm.
                    Some(RecvResult::StreamClosed) => {
                        tracing::warn!(
                            "Destination closed its control stream without sending DestinationDone - \
                             it aborted or died mid-copy. Not failed here: the master's read of the \
                             destination's result is mandatory, so the copy fails there either way - \
                             see the master's output for the destination's own error, or for its \
                             report that the destination never sent one"
                        );
                        break Ok(());
                    }
                    // not the same event as a peer EOF, and a defensive arm rather than a reachable
                    // one: the recv task always sends a terminal `StreamClosed`/`Error` before it
                    // ends, and the channel is FIFO, so a bare `None` means its sender was dropped
                    // without one — a panic in the receive path (re-raised where we join it, below),
                    // or the runtime dropping the task. Not the destination going away, so it does
                    // not borrow that wording; the control flow is identical because there is
                    // likewise nothing here that the master's mandatory read would miss.
                    None => {
                        tracing::warn!(
                            "Control receive task ended without reporting stream closure or an error \
                             - ending dispatch"
                        );
                        break Ok(());
                    }
                    Some(RecvResult::Error(e)) => break Err(e),
                };
                match message {
                    remote::protocol::DestinationMessage::DirectoryManifestChunk {
                        dst,
                        entries,
                    } => {
                        // accumulate this directory's manifest; all chunks arrive (FIFO) before the
                        // matching DirectoryCreated assembles and consumes them.
                        manifest_chunks.entry(dst).or_default().extend(entries);
                    }
                    remote::protocol::DestinationMessage::DirectoryCreated {
                        ref src,
                        ref dst,
                    } => {
                        // take the manifest accumulated from this directory's chunks (empty if none).
                        let existing = manifest_chunks.remove(dst.as_path()).unwrap_or_default();
                        tracing::info!(
                            "Received directory creation confirmation for: {:?} -> {:?} ({} manifest entries)",
                            src,
                            dst,
                            existing.len()
                        );
                        let existing_map: std::sync::Arc<
                            std::collections::HashMap<std::path::PathBuf, remote::protocol::ExistingEntry>,
                        > = std::sync::Arc::new(
                            // move each entry into the map (only the small name key is cloned).
                            existing
                                .into_iter()
                                .map(|e| (e.name.clone(), e))
                                .collect(),
                        );
                        // build the owned Pass-2 input. This consumes the source-mode-specific map
                        // entry and moves its permit/credit into the spawned task. Hardened mode
                        // fails closed on a miss; `-L` uses an empty no-credit variant (see
                        // `resolve_pass2_source`). The file count is recovered source-side — no
                        // wire echo.
                        let pass2_source = match resolve_pass2_source(&source_read, src) {
                            Ok(source) => source,
                            // fail closed: break the loop; the post-loop teardown publishes this
                            // error and releases the fd-budget, unblocking any parked Pass-1 walk.
                            Err(e) => break Err(e),
                        };
                        let collector = error_collector.clone();
                        let settings = settings.clone();
                        join_set.spawn(send_files_in_directory_tcp(
                            settings,
                            capture,
                            src.clone(),
                            dst.clone(),
                            source_root.clone(),
                            pass2_source,
                            stream_pool.clone(),
                            pending_limit.clone(),
                            collector,
                            control_send_stream.clone(),
                            existing_map,
                        ));
                    }
                    remote::protocol::DestinationMessage::DirectorySkipped {
                        ref src,
                        ref dst,
                    } => {
                        tracing::info!(
                            "Received directory skipped for: {:?} -> {:?}",
                            src,
                            dst
                        );
                        // the destination did not create this directory and will not
                        // request its files, so Pass 2 never runs for it. Consume this
                        // directory's Pass-1 bookkeeping now — the nack that matches the
                        // Pass-1 insert — so a no-ack subtree doesn't accumulate to
                        // connection-end. Unlike a `DirectoryCreated` miss this does not
                        // fail closed: there is nothing TOCTOU-sensitive to do and an
                        // absent/double nack is at worst a benign protocol-invariant
                        // violation we just log.
                        match &source_read {
                            // hardened: drop the held fd-map entry (releasing its dir-fd
                            // budget permit, keeping the budget deadlock-free).
                            SourceRead::Hardened(map) => {
                                if !map.take_for_skipped(src) {
                                    tracing::warn!(
                                        "DirectorySkipped for {src:?} but no held directory fd present \
                                         (absent or duplicate nack — protocol-invariant violation under trusted rcpd)"
                                    );
                                }
                            }
                            // -L: no fd is held, but Pass 1 recorded a contents+credit entry. Remove
                            // it here so a skipped subtree cannot retain either until connection
                            // end. A missing or duplicate nack has no credit to invent.
                            SourceRead::DereferencePath(counts) => {
                                if !counts.take_for_skipped(src) {
                                    tracing::warn!(
                                        "DirectorySkipped for {src:?} but no -L Pass-1 entry present \
                                         (absent or duplicate nack — protocol-invariant violation \
                                         under trusted rcpd)"
                                    );
                                }
                            }
                        }
                    }
                    remote::protocol::DestinationMessage::DestinationDone => {
                        tracing::info!("Received DestinationDone message");
                        // set shutdown flag - we'll drain remaining tasks and close.
                        // any task errors after this point are expected (destination is done).
                        shutdown_initiated = true;
                        break Ok(());
                    }
                }
            }
        }
    };
    // publish a fatal loop error to the shared slot BEFORE releasing the applicable Pass-1 pacing
    // gate below. closing the hardened dir-fd budget or `-L` outstanding-directory credit wakes the
    // Pass-1 walk (awaited inline in the caller), which then returns the synthetic `FdBudgetClosed`;
    // the caller reads THIS slot to report the real cause instead — and reads it WITHOUT awaiting
    // this task, which still has to drain its Pass-2 tasks below (a slow drain must not re-mask the
    // cause). publishing before the close guarantees the slot is populated by the time the walk
    // wakes
    let had_fatal_error = result.is_err();
    if let Err(e) = result {
        *fatal_error.lock().unwrap() = Some(e);
    }
    // release the applicable Pass-1 pacing gate after every normal dispatch-loop result — a
    // control-stream close, transport-task error, or child-task panic surfaced as JoinError — before
    // draining tasks. the RAII closer above covers cancellation or panic unwind before this point.
    // the walk may be parked on the hardened dir-fd budget or `-L` credit, and only a permit/credit
    // release or this close unblocks it; closing here once post-loop is what stops a Pass-2 task
    // error under `--fail-early` from deadlocking the walk. idempotent, and a no-op on a clean close
    // (the walk has already finished by then)
    source_read.close_pass1_gate();
    // if we're exiting with an error, abort the recv task immediately
    // (otherwise it would block waiting for more messages from destination)
    if had_fatal_error {
        recv_task.abort();
    }
    // CRITICAL: Signal pool shutdown BEFORE draining tasks to prevent deadlock.
    // Without this, tasks waiting on `stream_pool.borrow()` would hang forever because:
    // - borrow() waits on the pool's recv channel
    // - recv channel waits for streams from accept loop
    // - accept loop waits for connections from destination
    // - destination has already closed (or will never connect)
    // Cancelling the token signals the accept loop to close and close the channel,
    // causing borrow() to return an error immediately.
    pool_shutdown.cancel();
    // drain remaining tasks.
    // since we called pool_shutdown.cancel() above, any tasks waiting on borrow()
    // will get "pool closed" errors. these are expected and should be logged but
    // not propagated, unless the main loop already returned an error (result.is_err()).
    //
    // error handling during drain:
    // - shutdown_initiated=true (DestinationDone received): all errors expected, log debug
    // - shutdown_initiated=false, result=Ok (unexpected close): pool errors expected, log debug
    // - result=Err: we already have an error, just log additional errors
    let pool_shutdown_errors_expected = !had_fatal_error; // pool was just cancelled
    while let Some(task_result) = join_set.join_next().await {
        match task_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if shutdown_initiated || pool_shutdown_errors_expected {
                    tracing::debug!("Task failed during shutdown (expected): {e:#}");
                } else {
                    // transport errors are always fatal - we can't recover
                    tracing::error!("Transport failure in file send task: {e:#}");
                    error_collector.push(e);
                    // don't return error here - result already has an error
                }
            }
            Err(e) => {
                if shutdown_initiated || pool_shutdown_errors_expected {
                    tracing::debug!("Task panicked during shutdown: {e:#}");
                } else {
                    tracing::error!("Task panicked: {e:#}");
                    error_collector.push(e.into());
                    // don't return error here - result already has an error
                }
            }
        }
    }
    // close send stream after all tasks complete
    if shutdown_initiated {
        tracing::info!("All file send tasks completed, closing send stream");
        let mut stream = control_send_stream.lock().await;
        if let Err(e) = stream.close().await {
            tracing::debug!("Failed to close control stream: {e:#}");
        }
    }
    // wait for recv task to finish (it will close the stream). A normal end is expected; propagate a
    // genuine panic (rather than silently swallowing the JoinError) so a bug in the receive path
    // surfaces as a task failure instead of masquerading as a clean stream close.
    if let Err(join_err) = recv_task.await
        && join_err.is_panic()
    {
        std::panic::resume_unwind(join_err.into_panic());
    }
    tracing::info!("Finished dispatching control messages");
    // any fatal loop error is in the shared slot the caller reads; this task returns clean success
    // (a panic surfaces as a JoinError on the caller's await).
    Ok(())
}

/// Cancellation token alias for pool shutdown signaling.
///
/// Uses `CancellationToken` instead of oneshot because:
/// - Clonable: Multiple places can hold a reference (dispatch task, handle_connection)
/// - Idempotent: Can call `cancel()` multiple times safely
/// - Check-able: Can check `is_cancelled()` without consuming
type PoolShutdownToken = tokio_util::sync::CancellationToken;

/// Accepts data connections and provides SendStreams for file transfer.
///
/// The source accepts incoming TCP connections from the destination on its data port,
/// wraps them as SendStreams, and provides them via a channel for file sending tasks.
/// Connections are reused for multiple files - the `size` field in file headers delimits
/// file boundaries within a connection.
struct AcceptingSendStreamPool {
    recv: async_channel::Receiver<remote::streams::BoxedSendStream>,
    return_tx: async_channel::Sender<remote::streams::BoxedSendStream>,
}

impl AcceptingSendStreamPool {
    /// Create a new pool that accepts connections from the given listener.
    /// Returns the pool, a shutdown token, and the accept task handle.
    ///
    /// The shutdown token should be cancelled to signal the pool to close. It can be
    /// cloned and shared between multiple tasks - any clone can trigger shutdown.
    fn new(
        data_listener: tokio::net::TcpListener,
        pool_size: usize,
        profile: remote::NetworkProfile,
        keepalive_sec: u64,
        conn_timeout_sec: u64,
        tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    ) -> (Self, PoolShutdownToken, tokio::task::JoinHandle<()>) {
        let (send_tx, recv) = async_channel::bounded(pool_size);
        let (return_tx, return_rx) =
            async_channel::bounded::<remote::streams::BoxedSendStream>(pool_size);
        let shutdown_token = PoolShutdownToken::new();
        let shutdown_token_clone = shutdown_token.clone();
        // bound each data-connection TLS accept: this handshake runs INLINE in the accept loop below,
        // so a destination that connects TCP then stalls the handshake would otherwise block ALL
        // further data connections (a hang, not just a lost connection).
        let accept_tls_timeout = std::time::Duration::from_secs(conn_timeout_sec);
        // spawn task to accept data connections and manage pool
        let accept_task = tokio::spawn(async move {
            // wrap the main loop so we can handle shutdown
            tokio::select! {
                _ = async {
                    loop {
                        tokio::select! {
                            // accept new connections from destination (the helper applies the
                            // Data socket options — no TCP_USER_TIMEOUT, see configure_tcp_socket)
                            result = remote::accept_tcp_data(&data_listener, profile, keepalive_sec) => {
                                match result {
                                    Ok((stream, addr)) => {
                                        tracing::debug!("Accepted data connection from {}", addr);
                                        // Wrap with TLS if configured. This handshake runs INLINE in
                                        // the accept loop, so its bound is what stops one stalled
                                        // peer from blocking every further data connection. Only the
                                        // write half is kept (the destination never sends here), and
                                        // a failure drops just this connection.
                                        let send_stream = match remote::tls::accept_bounded(
                                            tls_acceptor.as_deref(),
                                            stream,
                                            accept_tls_timeout,
                                            "data",
                                        ).await {
                                            Ok((send_stream, _recv_stream)) => send_stream,
                                            Err(e) => {
                                                tracing::warn!("Dropping data connection: {:#}", &e);
                                                continue;
                                            }
                                        };
                                        if send_tx.send(send_stream).await.is_err() {
                                            tracing::debug!("Pool closed, stopping accept loop");
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        tracing::debug!("Data listener accept error: {:#}", e);
                                        break;
                                    }
                                }
                            }
                            // re-queue returned streams for reuse
                            result = return_rx.recv() => {
                                match result {
                                    Ok(stream) => {
                                        // return stream to pool for reuse by another file transfer.
                                        // file boundaries are delimited by length-prefixed headers
                                        // and the size field, so streams can be safely reused.
                                        if send_tx.send(stream).await.is_err() {
                                            tracing::debug!("Pool closed while returning stream");
                                            break;
                                        }
                                    }
                                    Err(_) => break, // return channel closed
                                }
                            }
                        }
                    }
                } => {}
                // shutdown signal received - close all streams
                _ = shutdown_token_clone.cancelled() => {
                    tracing::debug!("Pool shutdown signal received");
                }
            }
            // drain and close all streams in the pool so destination sees EOF
            // close the sender to stop any pending borrows
            send_tx.close();
            // drain streams from the return channel (streams being returned by workers)
            while let Ok(mut stream) = return_rx.try_recv() {
                let _ = stream.close().await;
            }
            return_rx.close();
            tracing::debug!("Pool accept task completed, all streams closed");
        });
        (Self { recv, return_tx }, shutdown_token, accept_task)
    }

    /// Borrow a SendStream from the pool (waits for a connection from destination).
    async fn borrow(&self) -> anyhow::Result<PooledAcceptedSendStream> {
        let stream = self
            .recv
            .recv()
            .await
            .map_err(|_| anyhow::anyhow!("data connection pool closed"))?;
        Ok(PooledAcceptedSendStream {
            stream: Some(stream),
            return_tx: self.return_tx.clone(),
        })
    }
}

/// RAII guard that returns the connection to the pool on drop.
/// Connections are reused for multiple files via length-prefixed framing.
struct PooledAcceptedSendStream {
    stream: Option<remote::streams::BoxedSendStream>,
    return_tx: async_channel::Sender<remote::streams::BoxedSendStream>,
}

impl PooledAcceptedSendStream {
    fn stream_mut(&mut self) -> &mut remote::streams::BoxedSendStream {
        self.stream.as_mut().expect("stream already taken")
    }

    fn take_and_discard(&mut self) -> Option<remote::streams::BoxedSendStream> {
        self.stream.take()
    }
}

impl Drop for PooledAcceptedSendStream {
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            // best effort return for cleanup
            let _ = self.return_tx.try_send(stream);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    control_stream: tokio::net::TcpStream,
    data_listener: tokio::net::TcpListener,
    settings: &common::copy::Settings,
    capture: ExtendedMetadataCapture,
    src: &std::path::Path,
    dst: &std::path::Path,
    tcp_config: &remote::TcpConfig,
    concurrency: remote::ResolvedRemoteConcurrency,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
    tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
) -> anyhow::Result<()> {
    tracing::info!("Destination control connection established");
    let pool_size = concurrency.max_connections().get();
    let max_pending_files = concurrency.max_pending_files().get();
    // the control stream's socket options (no-delay, buffers, liveness) were applied by the caller
    // when it accepted the connection
    // wrap control connection with TLS if configured; the handshake is bounded because a peer that
    // establishes TCP then stalls it would otherwise hang the source here indefinitely, before any
    // teardown state exists
    let (control_send_stream, control_recv_stream) = remote::tls::accept_bounded(
        tls_acceptor.as_deref(),
        control_stream,
        std::time::Duration::from_secs(tcp_config.conn_timeout_sec),
        "control",
    )
    .await?;
    // wrap in Arc<Mutex<>> for shared access
    let control_send_stream = std::sync::Arc::new(tokio::sync::Mutex::new(control_send_stream));
    tracing::info!("Created control streams for directory transfer");
    // create a pool that accepts data connections from destination and provides SendStreams
    let (stream_pool, pool_shutdown, accept_task) = AcceptingSendStreamPool::new(
        data_listener,
        pool_size,
        tcp_config.network_profile,
        tcp_config.keepalive_sec,
        tcp_config.conn_timeout_sec,
        tls_acceptor,
    );
    let stream_pool = std::sync::Arc::new(stream_pool);
    tracing::info!(
        "Created accepting send stream pool with {} slots",
        pool_size
    );
    // explicit source read mode (Phase 5a, hardened TOCTOU-safe file reads). Hardened
    // unless dereferencing: with `-L` the walk must follow nested symlinks, which the
    // O_NOFOLLOW fd primitives intentionally don't (that path stays as-is, matching
    // local copy). The hardened fd-map is shared (via the `Arc` inside `SourceRead`)
    // between Pass 1 (`send_fs_objects_tcp`, below) and Pass 2 (spawned inside
    // dispatch from each `DirectoryCreated`). Its dir-fd-in-flight budget bounds how far Pass 1 can
    // race ahead of the network-paced Pass 2 (prevents EMFILE); sized like the file pending-writes
    // pool. The `-L` variant instead carries a shared
    // path-keyed Pass-1 contents and an owned outstanding-directory credit with the same pacing
    // lifetime, so Pass 2 can recover each directory's count without an unbounded destination or
    // task backlog (`DirectoryCreated` does not carry the count over the wire).
    let source_read = if settings.dereference {
        SourceRead::DereferencePath(Arc::new(DereferenceWalkState::new(max_pending_files)))
    } else {
        SourceRead::Hardened(Arc::new(SourceDirMap::new(max_pending_files)))
    };
    // pass a clone of the shutdown token to dispatch - it will signal shutdown before
    // draining its tasks to prevent deadlock when destination closes unexpectedly.
    // see dispatch_control_messages_tcp doc comment for detailed shutdown flow.
    // shared slot for the dispatch task's fatal loop error. it publishes here BEFORE releasing the
    // applicable Pass-1 pacing gate that wakes the parked walk, so on a `--fail-early` saturation
    // failure we report the REAL cause instead of the synthetic gate wakeup — without awaiting the
    // dispatch task's possibly slow Pass-2 drain
    let dispatch_fatal_error: std::sync::Arc<std::sync::Mutex<Option<anyhow::Error>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let dispatch_task = tokio::spawn(dispatch_control_messages_tcp(
        settings.clone(),
        capture,
        src.to_path_buf(),
        control_recv_stream,
        control_send_stream.clone(),
        stream_pool.clone(),
        max_pending_files,
        error_collector.clone(),
        pool_shutdown.clone(),
        dispatch_fatal_error.clone(),
        source_read.clone(),
    ));
    // send files to destination. returns Err only for fatal errors (e.g., root file failure).
    // individual file failures with fail_early=false return Ok but push errors to collector,
    // and destination is notified via FileSkipped messages on the control channel.
    let send_result = send_fs_objects_tcp(
        settings,
        capture,
        src,
        dst,
        control_send_stream,
        stream_pool,
        error_collector.clone(),
        source_read,
    )
    .await;
    // if send failed, we need to close the pool FIRST so destination's data connections
    // see EOF and can complete. Otherwise destination hangs waiting for file data and
    // never sends DestinationDone, causing dispatch_task to hang forever.
    if send_result.is_err() {
        tracing::info!("Send failed, shutting down data pool to unblock destination");
        // signal pool to shutdown (closes all streams so destination sees EOF)
        // note: cancel() is idempotent, safe to call even if dispatch already called it
        pool_shutdown.cancel();
        // If the walk failed ONLY because `close_fd_budget()` woke it with the synthetic
        // `FdBudgetClosed`, the dispatch loop published the REAL root cause (the transport/task
        // failure that triggered the close) to `dispatch_fatal_error` BEFORE closing the budget — so
        // it is already present here. Report it instead of the meaningless wakeup, reading the slot
        // WITHOUT awaiting the dispatch task (its Pass-2 drain can be slow, and a timed-out await
        // would re-mask the cause). Then abort the now-doomed dispatch task — dropping its join set
        // aborts the in-flight Pass-2 tasks. For any OTHER walk failure, that IS the real cause, so
        // abort and report it as before.
        let is_budget_wakeup = send_result.as_ref().err().is_some_and(is_fd_budget_closed);
        // take the real cause out of the slot up front (dropping the guard before any await); None
        // for a non-budget-wakeup failure or if nothing was published.
        let published_cause = if is_budget_wakeup {
            dispatch_fatal_error.lock().unwrap().take()
        } else {
            None
        };
        // abort the now-doomed dispatch task (dropping its join set aborts in-flight Pass-2 tasks)
        // and let the accept task finish closing streams.
        dispatch_task.abort();
        let _ = accept_task.await;
        return match published_cause {
            // the budget wakeup masked the real cause; report the published cause.
            Some(dispatch_err) => Err(dispatch_err),
            // budget wakeup but NOTHING was published: the dispatch loop exited abnormally (a
            // destination-side control-stream close before the copy finished, or a panic whose cause
            // was discarded). Report a meaningful teardown cause rather than leaking the internal
            // `FdBudgetClosed` marker to the user.
            None if is_budget_wakeup => Err(anyhow::anyhow!(
                "destination closed the control connection before the source finished sending"
            )),
            // a genuine walk failure that was NOT a budget wakeup: `send_result` is the real cause.
            None => send_result,
        };
    }
    // send succeeded - wait for dispatch task to complete (handles destination responses).
    // note: dispatch_control_messages_tcp always calls pool_shutdown.cancel() before
    // returning, so the pool will be shut down when dispatch_task completes.
    let dispatch_result = dispatch_task.await;
    // wait for accept task to finish (pool shutdown was signaled by dispatch)
    let _ = accept_task.await;
    // propagate a dispatch-task panic (JoinError); its inner result is always clean success now —
    // any fatal loop error was published to the shared slot, which we surface next.
    dispatch_result??;
    let published_cause = dispatch_fatal_error.lock().unwrap().take();
    if let Some(dispatch_err) = published_cause {
        return Err(dispatch_err);
    }
    tracing::info!("Data sent successfully");
    Ok(())
}

/// Traverse filesystem and report dry-run entries via tracing — `-L`/`--dereference` ONLY.
/// This path-based reporter follows symlinks by request (documented not hardened); the
/// default path uses the fd-relative [`dry_run_traverse_fd`].
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn dry_run_traverse(
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    source_root: &std::path::Path,
    is_root: bool,
    dry_run_mode: common::config::DryRunMode,
    summary: &mut common::copy::Summary,
) -> anyhow::Result<()> {
    let src_metadata = match common::walk::run_metadata_probed(
        common::Side::Source,
        common::MetadataOp::Stat,
        async {
            if settings.dereference {
                tokio::fs::metadata(src).await
            } else {
                tokio::fs::symlink_metadata(src).await
            }
        },
    )
    .await
    {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("Failed reading metadata from src {src:?}: {e:#}");
            if settings.fail_early || is_root {
                return Err(e.into());
            }
            return Ok(());
        }
    };
    let is_dir = src_metadata.is_dir();
    // apply filter - use should_include_root_item for root items
    // (anchored patterns match paths inside the source, not the source itself)
    let filter_result = if let Some(ref filter) = settings.filter {
        if is_root {
            let file_name = src.file_name().map(std::path::Path::new).unwrap_or(src);
            filter.should_include_root_item(file_name, is_dir)
        } else {
            let relative_path = src.strip_prefix(source_root).unwrap_or(src);
            filter.should_include(relative_path, is_dir)
        }
    } else {
        common::filter::FilterResult::Included
    };
    let should_process = matches!(filter_result, common::filter::FilterResult::Included);
    let skip_reason = common::dry_run::format_skip_reason(&filter_result);
    // determine if we should report this entry based on dry-run mode
    let should_report = match dry_run_mode {
        common::config::DryRunMode::Brief => should_process,
        common::config::DryRunMode::All | common::config::DryRunMode::Explain => true,
    };
    // helper to format status for output
    let format_status = |process: bool, reason: &Option<String>| -> String {
        if process {
            "would copy".to_string()
        } else if matches!(dry_run_mode, common::config::DryRunMode::Explain) {
            format!("skip ({})", reason.as_deref().unwrap_or("filtered"))
        } else {
            "skip".to_string()
        }
    };
    if src_metadata.is_file() {
        if should_report {
            let size = src_metadata.len();
            tracing::info!(
                target: "dry_run",
                "{}: {:?} -> {:?} [file ({})]",
                format_status(should_process, &skip_reason),
                src,
                dst,
                bytesize::ByteSize(size)
            );
        }
        if should_process {
            summary.files_copied += 1;
            summary.bytes_copied += src_metadata.len();
        } else {
            summary.files_skipped += 1;
            progress().files_skipped.inc();
        }
        return Ok(());
    }
    if src_metadata.is_symlink() {
        let target = match common::walk::run_metadata_probed(
            common::Side::Source,
            common::MetadataOp::ReadLink,
            tokio::fs::read_link(src), // rcp-toctou-allow: -L path (dereference, documented not hardened)
        )
        .await
        {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Failed reading symlink {src:?}: {e:#}");
                if settings.fail_early {
                    return Err(e.into());
                }
                return Ok(());
            }
        };
        if should_report {
            tracing::info!(
                target: "dry_run",
                "{}: {:?} -> {:?} [symlink -> {:?}]",
                format_status(should_process, &skip_reason),
                src,
                dst,
                target
            );
        }
        if should_process {
            summary.symlinks_created += 1;
        } else {
            summary.symlinks_skipped += 1;
            progress().symlinks_skipped.inc();
        }
        return Ok(());
    }
    if !src_metadata.is_dir() {
        // special file (socket, FIFO, device)
        if !should_process {
            // filtered out by include/exclude - count as files_skipped (matching local copy)
            summary.files_skipped += 1;
            progress().files_skipped.inc();
        } else if settings.skip_specials {
            if should_report {
                tracing::info!(
                    target: "dry_run",
                    "skip (special file): {:?} -> {:?} [type: {:?}]",
                    src,
                    dst,
                    src_metadata.file_type()
                );
            }
            summary.specials_skipped += 1;
            progress().specials_skipped.inc();
        } else {
            // without --skip-specials, real copy would error on this file type
            let err = anyhow::anyhow!(
                "dry-run: {:?} -> {:?} unsupported file type: {:?}",
                src,
                dst,
                src_metadata.file_type()
            );
            tracing::error!("{:#}", &err);
            if settings.fail_early {
                return Err(err);
            }
        }
        return Ok(());
    }
    // directory
    if should_report {
        tracing::info!(
            target: "dry_run",
            "{}: {:?} -> {:?} [dir]",
            format_status(should_process, &skip_reason),
            src,
            dst
        );
    }
    // if filtered out, check whether to stop or still traverse
    if !should_process {
        match &filter_result {
            // explicitly excluded by pattern - never traverse (excludes are absolute)
            common::filter::FilterResult::ExcludedByPattern(_) => {
                summary.directories_skipped += 1;
                progress().directories_skipped.inc();
                return Ok(());
            }
            // no include pattern matched - traverse only if could contain matches
            common::filter::FilterResult::ExcludedByDefault => {
                if let Some(ref filter) = settings.filter {
                    let relative_path = if is_root {
                        src.file_name().map(std::path::Path::new).unwrap_or(src)
                    } else {
                        src.strip_prefix(source_root).unwrap_or(src)
                    };
                    let mut should_traverse = false;
                    for pattern in &filter.includes {
                        if filter.could_contain_matches(relative_path, pattern) {
                            should_traverse = true;
                            break;
                        }
                    }
                    if !should_traverse {
                        summary.directories_skipped += 1;
                        progress().directories_skipped.inc();
                        return Ok(());
                    }
                    // will traverse looking for matches - defer created/skipped decision
                } else {
                    summary.directories_skipped += 1;
                    progress().directories_skipped.inc();
                    return Ok(());
                }
            }
            // included - will be processed, continue to recurse
            common::filter::FilterResult::Included => {}
        }
    }
    // save current counts before recursing to detect if anything was added
    let before_files = summary.files_copied;
    let before_symlinks = summary.symlinks_created;
    let before_dirs = summary.directories_created;
    if should_process {
        summary.directories_created += 1;
    }
    // recurse into children
    let mut entries = match tokio::fs::read_dir(src).await {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("Cannot open directory {src:?} for reading: {e:#}");
            if settings.fail_early {
                return Err(e.into());
            }
            return Ok(());
        }
    };
    loop {
        match common::walk::next_entry_probed(&mut entries, common::Side::Source, || {
            format!("failed traversing src directory {:?}", &src)
        })
        .await
        {
            Ok(Some((entry, _file_type))) => {
                let entry_path = entry.path();
                let entry_name = entry_path.file_name().unwrap();
                let dst_path = dst.join(entry_name);
                if let Err(e) = dry_run_traverse(
                    settings,
                    &entry_path,
                    &dst_path,
                    source_root,
                    false,
                    dry_run_mode,
                    summary,
                )
                .await
                {
                    tracing::error!("Failed to traverse {entry_path:?}: {e:#}");
                    if settings.fail_early {
                        return Err(e);
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::error!("Failed traversing src directory {src:?}: {e:#}");
                if settings.fail_early {
                    return Err(e);
                }
                break;
            }
        }
    }
    // after recursing, check if anything was added inside this directory.
    // if nothing was added AND this directory doesn't directly match an include pattern,
    // we should not count it (it was only traversed to look for potential matches).
    // the root directory is never uncounted — it's the user-specified source.
    if !is_root {
        let child_content_added = summary.files_copied > before_files
            || summary.symlinks_created > before_symlinks
            || summary.directories_created > before_dirs + if should_process { 1 } else { 0 };
        if should_process {
            // directly matched directory: un-count if nothing was added and not
            // directly matched by an include pattern
            if !child_content_added && let Some(filter) = &settings.filter {
                let relative_path = src.strip_prefix(source_root).unwrap_or(src);
                if !filter.directly_matches_include(relative_path, true) {
                    summary.directories_created -= 1;
                }
            }
        } else {
            // traversed-only directory: promote to created if descendants matched,
            // otherwise count as skipped
            if child_content_added {
                summary.directories_created += 1;
            } else {
                summary.directories_skipped += 1;
                progress().directories_skipped.inc();
            }
        }
    }
    Ok(())
}

/// Where [`dry_run_traverse_fd`] classifies and opens the CURRENT entry from.
///
/// Nested entries and the strict-mode root are always `Opened` (fd-relative, the hardened
/// shape). The default-path root is `Lazy`: classified by path stat, its parent opened only
/// when traversal actually proceeds past the root filter — so an excluded root under an
/// execute-only (0111, searchable-not-readable) parent skips cleanly (the O_RDONLY parent
/// open would fail EACCES there), matching the local copy's root-filter behavior and the
/// historical path-based dry run. An INCLUDED root still requires the parent open, exactly
/// like the real remote copy this dry run previews.
enum DryRunDirSource {
    Opened(Arc<Dir>),
    /// The operand's parent path, opened on demand (default-path root only).
    Lazy(std::path::PathBuf),
}

/// Traverse and report dry-run entries via tracing, fd-relative — the default (non-`-L`) path.
///
/// The hardened twin of [`dry_run_traverse`]: the entry is classified via its parent's held fd
/// (`child(name)`, `O_NOFOLLOW`), a symlink's target is read inode-exact off the classified
/// handle, and directories are enumerated through their own opened fd — the walk never
/// re-resolves a multi-component path, so a concurrent swap cannot make the dry run report
/// names, sizes, or targets from outside the source tree (the same shape as the real copy's
/// hardened walk; this one only *reports*). The sole exception is the default-path root
/// (see [`DryRunDirSource::Lazy`]). `src` is the display path for reporting/filters.
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn dry_run_traverse_fd(
    settings: &common::copy::Settings,
    parent: &DryRunDirSource,
    name: &std::ffi::OsStr,
    src: &std::path::Path,
    dst: &std::path::Path,
    source_root: &std::path::Path,
    is_root: bool,
    dry_run_mode: common::config::DryRunMode,
    summary: &mut common::copy::Summary,
) -> anyhow::Result<()> {
    // classify the entry: fd-relative via the held parent fd, or — for the default-path
    // root only (`Lazy`) — by path stat, so a root the filter excludes never requires
    // read permission on its parent
    let (kind, size, handle) = match parent {
        DryRunDirSource::Opened(dir) => match dir.child(name).await {
            Ok(handle) => (handle.kind(), handle.meta().size(), Some(handle)),
            Err(e) => {
                tracing::error!("Failed reading metadata from src {src:?}: {e:#}");
                if settings.fail_early || is_root {
                    return Err(e.into());
                }
                return Ok(());
            }
        },
        DryRunDirSource::Lazy(_) => {
            match common::walk::run_metadata_probed(
                common::Side::Source,
                common::MetadataOp::Stat,
                tokio::fs::symlink_metadata(src),
            )
            .await
            {
                Ok(md) => (common::walk::EntryKind::from_metadata(&md), md.len(), None),
                Err(e) => {
                    tracing::error!("Failed reading metadata from src {src:?}: {e:#}");
                    if settings.fail_early || is_root {
                        return Err(e.into());
                    }
                    return Ok(());
                }
            }
        }
    };
    let is_dir = kind == common::walk::EntryKind::Dir;
    // apply filter - use should_include_root_item for root items
    // (anchored patterns match paths inside the source, not the source itself)
    let filter_result = if let Some(ref filter) = settings.filter {
        if is_root {
            let file_name = src.file_name().map(std::path::Path::new).unwrap_or(src);
            filter.should_include_root_item(file_name, is_dir)
        } else {
            let relative_path = src.strip_prefix(source_root).unwrap_or(src);
            filter.should_include(relative_path, is_dir)
        }
    } else {
        common::filter::FilterResult::Included
    };
    let should_process = matches!(filter_result, common::filter::FilterResult::Included);
    let skip_reason = common::dry_run::format_skip_reason(&filter_result);
    // determine if we should report this entry based on dry-run mode
    let should_report = match dry_run_mode {
        common::config::DryRunMode::Brief => should_process,
        common::config::DryRunMode::All | common::config::DryRunMode::Explain => true,
    };
    // helper to format status for output
    let format_status = |process: bool, reason: &Option<String>| -> String {
        if process {
            "would copy".to_string()
        } else if matches!(dry_run_mode, common::config::DryRunMode::Explain) {
            format!("skip ({})", reason.as_deref().unwrap_or("filtered"))
        } else {
            "skip".to_string()
        }
    };
    // A default-path Lazy ROOT that will be COPIED (INCLUDED) opens its parent HERE — for every
    // kind (file/symlink/special/dir) — matching the real remote copy, which opens the source
    // parent to read the root. So an execute-only (0111) parent, or a nonexistent one, fails the
    // dry run identically (exit 1) instead of succeeding where the real copy would not. An EXCLUDED
    // root was never materialized, preserving the skip-without-parent-read behavior; nested entries
    // and the strict-mode root are already Opened. Held for the Dir arm's enumeration below.
    let opened_parent: Option<Arc<Dir>> = match parent {
        DryRunDirSource::Opened(dir) => Some(dir.clone()),
        DryRunDirSource::Lazy(parent_path) if should_process => Some(Arc::new(
            Dir::open_parent_dir(parent_path, common::Side::Source)
                .await
                .with_context(|| {
                    format!("cannot open parent directory of dry-run root {parent_path:?}")
                })?
                .into_tree(),
        )),
        DryRunDirSource::Lazy(_) => None,
    };
    match kind {
        common::walk::EntryKind::File => {
            if should_report {
                tracing::info!(
                    target: "dry_run",
                    "{}: {:?} -> {:?} [file ({})]",
                    format_status(should_process, &skip_reason),
                    src,
                    dst,
                    bytesize::ByteSize(size)
                );
            }
            if should_process {
                summary.files_copied += 1;
                summary.bytes_copied += size;
            } else {
                summary.files_skipped += 1;
                progress().files_skipped.inc();
            }
            Ok(())
        }
        common::walk::EntryKind::Symlink => {
            // target read inode-exact off the classified handle (read-side fidelity); the
            // default-path ROOT (`Lazy`, no handle) reads by path like the historical dry
            // run — the operand itself sits at the trusted boundary, and every nested
            // entry is read via the fd walk
            let target = match &handle {
                Some(handle) => handle
                    .read_symlink(common::Side::Source)
                    .await
                    .map(|(target, _meta)| target),
                None => {
                    common::walk::run_metadata_probed(
                        common::Side::Source,
                        common::MetadataOp::ReadLink,
                        tokio::fs::read_link(src), // rcp-toctou-allow: default-path dry-run ROOT only (Lazy operand at the trusted boundary); nested entries read inode-exact via the fd walk
                    )
                    .await
                }
            };
            let target = match target {
                Ok(target) => target,
                Err(e) => {
                    tracing::error!("Failed reading symlink {src:?}: {e:#}");
                    if settings.fail_early {
                        return Err(e.into());
                    }
                    return Ok(());
                }
            };
            if should_report {
                tracing::info!(
                    target: "dry_run",
                    "{}: {:?} -> {:?} [symlink -> {:?}]",
                    format_status(should_process, &skip_reason),
                    src,
                    dst,
                    target
                );
            }
            if should_process {
                summary.symlinks_created += 1;
            } else {
                summary.symlinks_skipped += 1;
                progress().symlinks_skipped.inc();
            }
            Ok(())
        }
        common::walk::EntryKind::Special => {
            if !should_process {
                // filtered out by include/exclude - count as files_skipped (matching local copy)
                summary.files_skipped += 1;
                progress().files_skipped.inc();
            } else if settings.skip_specials {
                if should_report {
                    tracing::info!(
                        target: "dry_run",
                        "skip (special file): {:?} -> {:?}",
                        src,
                        dst
                    );
                }
                summary.specials_skipped += 1;
                progress().specials_skipped.inc();
            } else {
                // without --skip-specials, real copy would error on this file type — so the dry
                // run must too. A root special is always fatal (matches the real copy's exit 1);
                // a nested one respects --fail-early.
                let err = anyhow::anyhow!(
                    "dry-run: {:?} -> {:?} unsupported (special) file type",
                    src,
                    dst
                );
                tracing::error!("{:#}", &err);
                if settings.fail_early || is_root {
                    return Err(err);
                }
            }
            Ok(())
        }
        common::walk::EntryKind::Dir => {
            if should_report {
                tracing::info!(
                    target: "dry_run",
                    "{}: {:?} -> {:?} [dir]",
                    format_status(should_process, &skip_reason),
                    src,
                    dst
                );
            }
            // if filtered out, check whether to stop or still traverse
            if !should_process {
                match &filter_result {
                    // explicitly excluded by pattern - never traverse (excludes are absolute)
                    common::filter::FilterResult::ExcludedByPattern(_) => {
                        summary.directories_skipped += 1;
                        progress().directories_skipped.inc();
                        return Ok(());
                    }
                    // no include pattern matched - traverse only if could contain matches
                    common::filter::FilterResult::ExcludedByDefault => {
                        if let Some(ref filter) = settings.filter {
                            let relative_path = if is_root {
                                src.file_name().map(std::path::Path::new).unwrap_or(src)
                            } else {
                                src.strip_prefix(source_root).unwrap_or(src)
                            };
                            let mut should_traverse = false;
                            for pattern in &filter.includes {
                                if filter.could_contain_matches(relative_path, pattern) {
                                    should_traverse = true;
                                    break;
                                }
                            }
                            if !should_traverse {
                                summary.directories_skipped += 1;
                                progress().directories_skipped.inc();
                                return Ok(());
                            }
                            // will traverse looking for matches - defer created/skipped decision
                        } else {
                            summary.directories_skipped += 1;
                            progress().directories_skipped.inc();
                            return Ok(());
                        }
                    }
                    // included - will be processed, continue to recurse
                    common::filter::FilterResult::Included => {}
                }
            }
            // save current counts before recursing to detect if anything was added
            let before_files = summary.files_copied;
            let before_symlinks = summary.symlinks_created;
            let before_dirs = summary.directories_created;
            if should_process {
                summary.directories_created += 1;
            }
            // open the directory through the held parent fd (O_NOFOLLOW) and enumerate it
            // via its own fd — a swapped-in symlink fails closed here, never redirects. An
            // INCLUDED root and every Opened source already hold the parent fd
            // (`opened_parent`); a traversed-but-excluded Lazy dir opens its parent here (it is
            // being traversed to look for matches, so parent read is needed, matching the real
            // copy).
            let dir = match &opened_parent {
                Some(parent_dir) => parent_dir.open_dir(name).await,
                None => match parent {
                    DryRunDirSource::Lazy(parent_path) => {
                        match Dir::open_parent_dir(parent_path, common::Side::Source).await {
                            Ok(trusted) => trusted.into_tree().open_dir(name).await,
                            Err(e) => Err(e),
                        }
                    }
                    // Opened always populates opened_parent above; unreachable
                    DryRunDirSource::Opened(_) => unreachable!("Opened sets opened_parent"),
                },
            };
            let dir = dir.map(Arc::new);
            let entries = match &dir {
                Ok(dir) => dir.read_entries().await,
                Err(_) => Ok(Vec::new()),
            };
            match (dir, entries) {
                (Ok(dir), Ok(entries)) => {
                    let dir_source = DryRunDirSource::Opened(dir);
                    for (entry_name, _kind_hint) in entries {
                        let entry_src = src.join(&entry_name);
                        let entry_dst = dst.join(&entry_name);
                        if let Err(e) = dry_run_traverse_fd(
                            settings,
                            &dir_source,
                            &entry_name,
                            &entry_src,
                            &entry_dst,
                            source_root,
                            false,
                            dry_run_mode,
                            summary,
                        )
                        .await
                        {
                            tracing::error!("Failed to traverse {entry_src:?}: {e:#}");
                            if settings.fail_early {
                                return Err(e);
                            }
                        }
                    }
                }
                (Err(e), _) | (_, Err(e)) => {
                    tracing::error!("Cannot open directory {src:?} for reading: {e:#}");
                    // a root open/enumeration failure is fatal (matches the real copy's exit 1);
                    // a nested one respects --fail-early and is otherwise skipped.
                    if settings.fail_early || is_root {
                        return Err(e.into());
                    }
                    return Ok(());
                }
            }
            // after recursing, check if anything was added inside this directory.
            // if nothing was added AND this directory doesn't directly match an include pattern,
            // we should not count it (it was only traversed to look for potential matches).
            // the root directory is never uncounted — it's the user-specified source.
            if !is_root {
                let child_content_added = summary.files_copied > before_files
                    || summary.symlinks_created > before_symlinks
                    || summary.directories_created
                        > before_dirs + if should_process { 1 } else { 0 };
                if should_process {
                    // directly matched directory: un-count if nothing was added and not
                    // directly matched by an include pattern
                    if !child_content_added && let Some(filter) = &settings.filter {
                        let relative_path = src.strip_prefix(source_root).unwrap_or(src);
                        if !filter.directly_matches_include(relative_path, true) {
                            summary.directories_created -= 1;
                        }
                    }
                } else {
                    // traversed-only directory: promote to created if descendants matched,
                    // otherwise count as skipped
                    if child_content_added {
                        summary.directories_created += 1;
                    } else {
                        summary.directories_skipped += 1;
                        progress().directories_skipped.inc();
                    }
                }
            }
            Ok(())
        }
    }
}

/// Handle a dry-run connection: traverse, log entries, and complete without transferring data.
/// Destination sees an empty copy and completes immediately.
async fn handle_dry_run_connection(
    stream: tokio::net::TcpStream,
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    dry_run_mode: common::config::DryRunMode,
    tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    conn_timeout_sec: u64,
) -> anyhow::Result<(String, common::copy::Summary)> {
    tracing::info!("Handling dry-run connection");
    // set up TLS if needed; the handshake is bounded like every other (a peer that establishes TCP
    // then stalls would otherwise hang this connection indefinitely)
    let (control_send_stream, mut control_recv_stream) = remote::tls::accept_bounded(
        tls_acceptor.as_deref(),
        stream,
        std::time::Duration::from_secs(conn_timeout_sec),
        "dry-run control",
    )
    .await?;
    let control_send_stream: remote::streams::BoxedSharedSendStream =
        std::sync::Arc::new(tokio::sync::Mutex::new(control_send_stream));
    // traverse and log dry-run entries (output goes via tracing). The default path uses the
    // fd-relative walker — the same hardened shape as the real copy, so a concurrent swap
    // cannot make the dry run report content from outside the source tree, and under strict
    // operand resolution a symlinked operand prefix fails closed at the parent open. `-L`
    // keeps the path-based reporter (follows symlinks by request; documented not hardened).
    let mut summary = common::copy::Summary::default();
    if settings.dereference {
        dry_run_traverse(settings, src, dst, src, true, dry_run_mode, &mut summary).await?;
    } else {
        let operand = common::walk::split_root_operand(src).await?;
        let display = operand.display.clone();
        // strict operand resolution: open the parent EAGERLY (openat2 RESOLVE_NO_SYMLINKS)
        // so a symlinked operand prefix fails closed before anything is reported. On the
        // default path the parent is opened lazily — only if the walk proceeds past the
        // root filter — so an excluded root under an execute-only (0111) parent skips
        // cleanly, matching the local copy's root-filter behavior (see DryRunDirSource).
        let parent = if common::safedir::strict_operand_resolution() {
            let parent = Dir::open_parent_dir(&operand.parent, common::Side::Source)
                .await
                .with_context(|| format!("cannot open parent directory of dry-run source {src:?}"))?
                .into_tree();
            DryRunDirSource::Opened(Arc::new(parent))
        } else {
            DryRunDirSource::Lazy(operand.parent.clone())
        };
        dry_run_traverse_fd(
            settings,
            &parent,
            &operand.name,
            &display,
            dst,
            &display,
            true,
            dry_run_mode,
            &mut summary,
        )
        .await?;
    }
    // tell destination we're done with directory structure (nothing was sent in dry-run)
    {
        let mut stream = control_send_stream.lock().await;
        stream
            .send_control_message(&remote::protocol::SourceMessage::DirStructureComplete {
                has_root_item: false,
            })
            .await?;
    }
    tracing::info!("Sent DirStructureComplete, waiting for DestinationDone");
    // wait for destination to acknowledge it's done
    loop {
        match control_recv_stream
            .recv_object::<remote::protocol::DestinationMessage>()
            .await?
        {
            Some(remote::protocol::DestinationMessage::DestinationDone) => {
                tracing::info!("Received DestinationDone");
                break;
            }
            Some(other) => {
                tracing::debug!("Ignoring message during dry-run: {:?}", other);
            }
            None => {
                tracing::debug!("Control stream closed");
                break;
            }
        }
    }
    // close streams
    control_send_stream.lock().await.close().await.ok();
    tracing::info!("Dry-run complete");
    // print summary
    tracing::info!(
        target: "dry_run",
        "Summary: {} files ({} bytes), {} directories, {} symlinks would be copied",
        summary.files_copied,
        summary.bytes_copied,
        summary.directories_created,
        summary.symlinks_created
    );
    Ok(("dry-run complete".to_string(), summary))
}

#[instrument(skip(master_send_stream, cert_key))]
#[allow(clippy::too_many_arguments)]
pub async fn run_source<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
    master_send_stream: remote::streams::SharedSendStream<W>,
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &common::copy::Settings,
    // what extended per-entry metadata (ACLs) the master wants read; all-false means this source
    // issues no xattr syscall at all.
    capture: ExtendedMetadataCapture,
    tcp_config: &remote::TcpConfig,
    concurrency: remote::ResolvedRemoteConcurrency,
    bind_ip: Option<&str>,
    cert_key: Option<&remote::tls::CertifiedKey>,
    dest_cert_fingerprint: Option<remote::protocol::CertFingerprint>,
) -> anyhow::Result<(String, common::copy::Summary)> {
    // create TLS acceptor if encryption is enabled (requires both cert and dest fingerprint)
    let tls_acceptor = match (cert_key, dest_cert_fingerprint) {
        (Some(cert), Some(dest_fp)) => {
            // create server config with client certificate verification
            let server_config = remote::tls::create_server_config_with_client_auth(cert, dest_fp)
                .context("failed to create TLS server config with client auth")?;
            Some(std::sync::Arc::new(tokio_rustls::TlsAcceptor::from(
                server_config,
            )))
        }
        _ => None,
    };
    tracing::info!(
        "Source TLS encryption: {}",
        if tls_acceptor.is_some() {
            "enabled (mutual TLS)"
        } else {
            "disabled"
        }
    );
    // create TCP listeners for control and data connections
    let control_listener = remote::create_tcp_control_listener(tcp_config, bind_ip).await?;
    let data_listener = remote::create_tcp_data_listener(tcp_config, bind_ip).await?;
    let control_addr = remote::get_tcp_listener_addr(&control_listener, bind_ip)?;
    let data_addr = remote::get_tcp_listener_addr(&data_listener, bind_ip)?;
    tracing::info!(
        "Source TCP listeners: control={}, data={}",
        control_addr,
        data_addr
    );
    let master_hello = remote::protocol::SourceMasterHello {
        control_addr,
        data_addr,
        server_name: remote::get_random_server_name(),
    };
    tracing::info!("Sending master hello: {:?}", master_hello);
    master_send_stream
        .lock()
        .await
        .send_control_message(&master_hello)
        .await?;
    tracing::info!("Waiting for connection from destination");
    // wait for destination to connect with a timeout
    let error_collector = std::sync::Arc::new(common::error_collector::ErrorCollector::default());
    let accept_timeout = std::time::Duration::from_secs(tcp_config.conn_timeout_sec);
    // the accept helper applies the Control socket options before returning, so the dry-run path
    // below gets the same configuration as the normal one
    match tokio::time::timeout(
        accept_timeout,
        remote::accept_tcp_control(&control_listener, tcp_config),
    )
    .await
    {
        Ok(Ok((stream, addr))) => {
            tracing::info!("Destination control connection from {}", addr);
            // in dry-run mode, do simplified flow: traverse, log, and tell destination we're done
            if let Some(dry_run_mode) = settings.dry_run {
                return handle_dry_run_connection(
                    stream,
                    settings,
                    src,
                    dst,
                    dry_run_mode,
                    tls_acceptor,
                    tcp_config.conn_timeout_sec,
                )
                .await;
            }
            // normal flow
            handle_connection(
                stream,
                data_listener,
                settings,
                capture,
                src,
                dst,
                tcp_config,
                concurrency,
                error_collector.clone(),
                tls_acceptor,
            )
            .await?;
        }
        Ok(Err(e)) => {
            tracing::error!("Failed to accept control connection: {:#}", e);
            return Err(e.into());
        }
        Err(_) => {
            tracing::error!(
                "Timed out waiting for destination to connect after {:?}. \
                This usually means the destination cannot reach the source. \
                Check network connectivity and firewall rules.",
                accept_timeout
            );
            return Err(anyhow::anyhow!(
                "Timed out waiting for destination to connect after {:?}",
                accept_timeout
            ));
        }
    }
    tracing::info!("Source is done");
    // destination is authoritative for copy/unchanged/removed counts, but
    // skip counts are source-side only (destination never encounters skipped items)
    let summary = common::copy::Summary {
        files_skipped: progress().files_skipped.get() as usize,
        symlinks_skipped: progress().symlinks_skipped.get() as usize,
        directories_skipped: progress().directories_skipped.get() as usize,
        specials_skipped: progress().specials_skipped.get() as usize,
        ..Default::default()
    };
    match error_collector.take_error() {
        Some(err) => Err(common::copy::Error {
            source: err,
            summary,
        }
        .into()),
        None => Ok(("source OK".to_string(), summary)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dereference_settings() -> common::copy::Settings {
        common::copy::Settings {
            dereference: true,
            fail_early: false,
            overwrite: false,
            overwrite_compare: Default::default(),
            overwrite_filter: None,
            ignore_existing: false,
            chunk_size: 0,
            skip_specials: false,
            remote_copy_buffer_size: 0,
            filter: None,
            dry_run: None,
            delete: None,
        }
    }

    #[tokio::test]
    async fn closed_dereference_credit_propagates_from_child_in_collect_errors_mode()
    -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let src = temp.path().join("source");
        let dst = temp.path().join("destination");
        std::fs::create_dir_all(src.join("first"))?;
        std::fs::create_dir(src.join("second"))?;
        let root_metadata = std::fs::metadata(&src)?;
        let state = Arc::new(DereferenceWalkState::new(1));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let (wire_send, wire_recv) = tokio::io::duplex(64 * 1024);
        let writer: remote::streams::BoxedWrite = Box::new(wire_send);
        let control = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            writer,
        )));
        let mut recv = remote::streams::RecvStream::new(wire_recv);
        let settings = dereference_settings();

        let walk = send_directories_and_symlinks(
            &settings,
            ExtendedMetadataCapture::default(),
            &src,
            &dst,
            &src,
            Some(&root_metadata),
            &control,
            &errors,
            &state,
        );
        let close_after_root = async {
            let message = recv
                .recv_object::<remote::protocol::SourceMessage>()
                .await?
                .context("dereference walk did not send its root Directory message")?;
            assert!(matches!(
                message,
                remote::protocol::SourceMessage::Directory { entry_count: 2, .. }
            ));
            state.close_credit();
            anyhow::Ok(())
        };
        let (walk_result, close_result) =
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::join!(walk, close_after_root)
            })
            .await
            .context("dereference walk did not stop after its credit gate closed")?;
        close_result?;

        let error = walk_result.expect_err(
            "a closed dereference credit must abort child recursion even in collect-errors mode",
        );
        assert!(
            is_fd_budget_closed(&error),
            "walk returned the wrong error: {error:#}"
        );
        assert!(
            errors.take_error().is_none(),
            "the teardown marker must not be added to collected user errors"
        );
        drop(control);
        assert!(
            recv.recv_object::<remote::protocol::SourceMessage>()
                .await?
                .is_none(),
            "gate closure must not send a compensating skip or continue to a sibling"
        );
        // production mutation caught: removing the `FdBudgetClosed` early return from the unified
        // child-error funnel makes collect-errors mode compensate, collect, and continue here.
        Ok(())
    }

    #[tokio::test]
    async fn closed_hardened_fd_budget_propagates_from_child_in_collect_errors_mode()
    -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let src = temp.path().join("source");
        let dst = temp.path().join("destination");
        std::fs::create_dir_all(src.join("first"))?;
        std::fs::create_dir(src.join("second"))?;
        let root = Arc::new(Dir::open_root_dir(&src, false, common::Side::Source).await?);
        let dir_map = Arc::new(SourceDirMap::new(1));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let (wire_send, wire_recv) = tokio::io::duplex(64 * 1024);
        let writer: remote::streams::BoxedWrite = Box::new(wire_send);
        let control = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            writer,
        )));
        let mut recv = remote::streams::RecvStream::new(wire_recv);
        let mut settings = dereference_settings();
        settings.dereference = false;

        let walk = send_directory_fd_walk(
            &settings,
            ExtendedMetadataCapture::default(),
            &src,
            &dst,
            &src,
            true,
            root,
            &control,
            &errors,
            &dir_map,
        );
        let close_after_root = async {
            let message = recv
                .recv_object::<remote::protocol::SourceMessage>()
                .await?
                .context("hardened walk did not send its root Directory message")?;
            assert!(matches!(
                message,
                remote::protocol::SourceMessage::Directory { entry_count: 2, .. }
            ));
            assert_eq!(
                dir_map.fd_budget.available_permits(),
                0,
                "the retained root entry must saturate this one-permit fd budget"
            );
            dir_map.close_fd_budget();
            anyhow::Ok(())
        };
        let (walk_result, close_result) =
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::join!(walk, close_after_root)
            })
            .await
            .context("hardened walk did not stop after its fd budget closed")?;
        close_result?;

        let error = walk_result.expect_err(
            "a closed hardened fd budget must abort child recursion even in collect-errors mode",
        );
        assert!(
            is_fd_budget_closed(&error),
            "walk returned the wrong error: {error:#}"
        );
        assert!(
            errors.take_error().is_none(),
            "the teardown marker must not be added to collected user errors"
        );
        drop(control);
        assert!(
            recv.recv_object::<remote::protocol::SourceMessage>()
                .await?
                .is_none(),
            "gate closure must not send a compensating skip or continue to a sibling"
        );
        // production mutation caught: routing `FdBudgetClosed` through the ordinary child-error
        // path makes collect-errors mode compensate, collect, and continue here.
        Ok(())
    }

    #[tokio::test]
    async fn unreadable_dereference_directory_balances_outstanding_credit() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let src = temp.path().join("source");
        let dst = temp.path().join("destination");
        std::fs::create_dir(&src)?;
        let directory_metadata = std::fs::metadata(&src)?;
        std::fs::remove_dir(&src)?;
        std::fs::write(&src, b"changed type")?;
        let state = Arc::new(DereferenceWalkState::new(1));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let (wire_send, wire_recv) = tokio::io::duplex(64 * 1024);
        let writer: remote::streams::BoxedWrite = Box::new(wire_send);
        let control = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            writer,
        )));
        let mut recv = remote::streams::RecvStream::new(wire_recv);

        let commit = send_pass1_entry(
            &dereference_settings(),
            ExtendedMetadataCapture::default(),
            &src,
            &dst,
            &src,
            Some(&directory_metadata),
            &control,
            &errors,
            &state,
        )
        .await?;

        assert!(matches!(commit, Pass1Commit::Sent));
        let message = recv
            .recv_object::<remote::protocol::SourceMessage>()
            .await?
            .context("unreadable directory did not send its Directory message")?;
        assert!(matches!(
            message,
            remote::protocol::SourceMessage::Directory {
                entry_count: 0,
                keep_if_empty: true,
                ..
            }
        ));
        assert_eq!(
            state.credit.available_permits(),
            0,
            "every sent dereference Directory must consume one outstanding credit"
        );
        let pass2 = resolve_pass2_source(&SourceRead::DereferencePath(state.clone()), &src)?;
        assert_eq!(pass2.file_count(), 0);
        assert_eq!(
            state.credit.available_permits(),
            0,
            "DirectoryCreated must transfer the credit into its Pass-2 work"
        );
        drop(pass2);
        assert_eq!(
            state.credit.available_permits(),
            1,
            "finishing Pass 2 must return exactly the credit consumed by the Directory"
        );
        Ok(())
    }

    #[test]
    fn unknown_dereference_ack_does_not_create_credit() -> anyhow::Result<()> {
        let state = Arc::new(DereferenceWalkState::new(1));

        let pass2 = resolve_pass2_source(
            &SourceRead::DereferencePath(state.clone()),
            std::path::Path::new("missing"),
        )?;

        assert_eq!(pass2.file_count(), 0);
        assert_eq!(
            state.credit.available_permits(),
            1,
            "an absent or duplicate acknowledgement cannot increase the configured credit"
        );
        Ok(())
    }

    #[tokio::test]
    async fn skipped_dereference_directory_returns_only_its_owned_credit() -> anyhow::Result<()> {
        let state = Arc::new(DereferenceWalkState::new(1));
        let src = std::path::PathBuf::from("skipped");
        state.insert(src.clone(), Pass1Contents::empty()).await?;
        assert_eq!(state.credit.available_permits(), 0);

        assert!(state.take_for_skipped(&src));
        assert_eq!(state.credit.available_permits(), 1);
        assert!(!state.take_for_skipped(&src));
        assert_eq!(
            state.credit.available_permits(),
            1,
            "a duplicate DirectorySkipped cannot increase the configured credit"
        );
        Ok(())
    }
}
