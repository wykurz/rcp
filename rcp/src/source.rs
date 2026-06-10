use anyhow::Context;
use async_recursion::async_recursion;
use common::safedir::Dir;
use std::collections::HashMap;
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
///   ordinary `File::open(src)`. It holds no directory fd, but it DOES retain the
///   Pass-1 file count per directory in a `path → file_count` map: with no count
///   echoed back over the wire, Pass 2 (`-L`) reads its expected count from here.
///   A `-L` miss is NOT a TOCTOU violation (this path is not hardened), so it is
///   treated as count 0 + a debug log rather than failing closed.
#[derive(Clone)]
enum SourceRead {
    Hardened(Arc<SourceDirMap>),
    DereferencePath(DereferenceCountMap),
}

/// Source-side `path → file_count` map for the `-L`/`--dereference` walk, the
/// dereference analogue of the hardened [`SourceDirMap`] minus the held fd and the
/// fd-budget permit.
///
/// With the destination no longer echoing `file_count` in `DirectoryCreated`, the
/// `-L` path (which holds no fd-map) must retain its own Pass-1 count: Pass 1
/// inserts each directory's count as it sends the `Directory` message, and
/// [`resolve_pass2_source`] takes it back when the matching `DirectoryCreated`
/// triggers Pass 2. A missing entry is treated as count 0 with a debug log — `-L`
/// is intentionally not hardened, so a miss is not a TOCTOU/fail-closed condition.
type DereferenceCountMap = Arc<std::sync::Mutex<HashMap<std::path::PathBuf, usize>>>;

impl SourceRead {
    /// The hardened fd-map, or `None` in dereference mode. Used by Pass 1 to decide
    /// between the fd-relative walk and the path-based walk.
    fn dir_map(&self) -> Option<&Arc<SourceDirMap>> {
        match self {
            SourceRead::Hardened(map) => Some(map),
            SourceRead::DereferencePath(_) => None,
        }
    }

    /// The `-L`/--dereference `path → file_count` map, or `None` in hardened mode.
    /// Used by Pass 1's path-based body to record each directory's count and by
    /// [`resolve_pass2_source`] to recover it (no count is echoed over the wire).
    fn deref_counts(&self) -> Option<&DereferenceCountMap> {
        match self {
            SourceRead::Hardened(_) => None,
            SourceRead::DereferencePath(counts) => Some(counts),
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
///   sent. Pass 2 is network-bound and independent of the dir-fd permit, so it
///   always makes progress.
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
/// Pass 1 is an *unthrottled* full-tree DFS while Pass 2 is network-paced, so
/// without a bound the peak number of held directory fds would approach the whole
/// tree's directory count → `EMFILE` on large trees. The map is gated by a
/// dir-fd-in-flight semaphore: Pass 1 acquires one permit per [`Self::insert`]
/// (awaiting if the bound is reached), and the permit is released when the
/// directory's [`MapEntry`] is dropped (after a take).
///
/// # Release invariant (deadlock-free)
///
/// Pass 1 only ever *acquires*; it must never release. **Every Pass-1 insert is
/// matched by exactly one release**, driven by the destination's exactly-one
/// response per `Directory` message (the two `take_*` methods above). This keeps
/// the budget both *effective* (a large no-ack subtree releases its fds promptly
/// via `DirectorySkipped` nacks instead of accumulating to connection-end) and
/// *deadlock-free*. Pass 2 must never acquire a dir-fd permit.
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
/// lifecycle explicit — the previous `Option`-triple had only two valid combinations
/// (all-set vs. all-clear), which lived in comments rather than the type.
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
    /// it), the Pass-1 expected `file_count` (authoritative for Pass 2's truncation /
    /// synthetic-`FileSkipped` logic), and the fd-budget permit that bounds how many
    /// real directory fds Pass 1 holds in flight (released when this drops).
    Readable {
        dir: Arc<Dir>,
        file_count: usize,
        _permit: tokio::sync::OwnedSemaphorePermit,
    },
    /// A committed-but-unreadable directory (0-entry `Directory` sent). Holds no fd and
    /// no permit; its file count is implicitly 0 and Pass 2 sends no files.
    Tombstone,
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

    /// Store the directory's `Arc<Dir>` plus its Pass-1 expected `file_count`,
    /// keyed by source path, first acquiring a dir-fd-in-flight permit (awaiting if
    /// the bound is reached). Only Pass 1 calls this.
    async fn insert(
        &self,
        src: std::path::PathBuf,
        dir: Arc<Dir>,
        file_count: usize,
    ) -> anyhow::Result<()> {
        let permit = self
            .fd_budget
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("source dir-fd budget semaphore closed"))?;
        self.entries.lock().unwrap().insert(
            src,
            MapEntry::Readable {
                dir,
                file_count,
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

    /// Close the dir-fd-in-flight semaphore so any pending or future
    /// [`Self::insert`] fails immediately. Used to fail closed without hanging a
    /// Pass-1 walk parked on the budget (see the struct-level docs).
    fn close_fd_budget(&self) {
        self.fd_budget.close();
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

/// The `-L`/`--dereference` path-based Pass-1 walk (directories + symlinks). The hardened
/// (non-`-L`) walk lives in [`send_directory_fd_walk`] (nested) and [`send_root_hardened`] (root);
/// this function is reached only in dereference mode, so every read here is path-based by design
/// (following symlinks is requested; documented not hardened).
#[instrument(skip(error_collector, control_send_stream, deref_counts))]
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn send_directories_and_symlinks(
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    source_root: &std::path::Path,
    is_root: bool,
    control_send_stream: &remote::streams::BoxedSharedSendStream,
    error_collector: &std::sync::Arc<common::error_collector::ErrorCollector>,
    // the `-L`/--dereference `path → file_count` map: Pass 1 records each directory's Pass-1 file
    // count here so [`resolve_pass2_source`] can recover it without a wire echo (no count is echoed
    // over the wire).
    deref_counts: Option<&DereferenceCountMap>,
) -> anyhow::Result<()> {
    tracing::debug!("Sending data from {:?} to {:?}", &src, dst);
    let src_metadata = match common::walk::run_metadata_probed(
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
            // for root items, failing to read metadata is fatal - we can't proceed
            // and the protocol would hang waiting for root completion
            if settings.fail_early || is_root {
                return Err(e.into());
            }
            error_collector.push(e.into());
            return Ok(());
        }
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
                return Ok(());
            }
        }
    }
    if src_metadata.is_file() {
        return Ok(());
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
                return Ok(());
            }
        };
        let symlink = remote::protocol::SourceMessage::Symlink {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            target: target.clone(),
            metadata: remote::protocol::Metadata::from(&src_metadata),
            is_root,
        };
        return control_send_stream
            .lock()
            .await
            .send_batch_message(&symlink)
            .await;
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
        return Ok(());
    }
    // pre-read directory children to compute entry counts before sending Directory message
    let mut file_children: Vec<ChildEntry> = Vec::new();
    let mut dir_children: Vec<ChildEntry> = Vec::new();
    let mut symlink_children: Vec<ChildEntry> = Vec::new();
    let mut entries = match tokio::fs::read_dir(&src).await {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("Cannot open directory {src:?} for reading: {e:#}");
            if settings.fail_early {
                return Err(e.into());
            }
            error_collector.push(e.into());
            // directory unreadable but we already committed to sending it -
            // send with 0 entries so destination can still complete. Record a
            // 0 count for this `-L` directory so Pass 2's count lookup resolves
            // (no wire echo carries it anymore).
            if let Some(deref_counts) = deref_counts {
                deref_counts.lock().unwrap().insert(src.to_path_buf(), 0);
            }
            let dir = remote::protocol::SourceMessage::Directory {
                src: src.to_path_buf(),
                dst: dst.to_path_buf(),
                metadata: remote::protocol::Metadata::from(&src_metadata),
                is_root,
                entry_count: 0,
                keep_if_empty: true,
            };
            control_send_stream
                .lock()
                .await
                .send_batch_message(&dir)
                .await?;
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
    // record this `-L` directory's Pass-1 file count so Pass 2 can recover it
    // without a wire echo (the count lives only on the source now). Inserted before
    // the `Directory` send so it is present before the destination can ack
    // `DirectoryCreated` and trigger the Pass-2 lookup.
    if let Some(deref_counts) = deref_counts {
        deref_counts
            .lock()
            .unwrap()
            .insert(src.to_path_buf(), file_count);
    }
    // send Directory message with pre-computed entry count
    let dir = remote::protocol::SourceMessage::Directory {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        metadata: remote::protocol::Metadata::from(&src_metadata),
        is_root,
        entry_count,
        keep_if_empty,
    };
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
        .send_batch_message(&dir)
        .await?;
    // recurse into non-file children (symlinks first, then directories).
    // this path-based body only runs when the fd-map is inactive (`-L` mode), so
    // the recursive calls carry `None` for the hardened fd-map and forward the
    // dereference count map.
    for child in symlink_children {
        if let Err(e) = send_directories_and_symlinks(
            settings,
            &child.src_path,
            &child.dst_path,
            source_root,
            false,
            control_send_stream,
            error_collector,
            deref_counts,
        )
        .await
        {
            tracing::error!("Failed to send symlink {:?}: {e:#}", child.src_path);
            if settings.fail_early {
                return Err(e);
            }
            error_collector.push(e);
        }
    }
    for child in dir_children {
        if let Err(e) = send_directories_and_symlinks(
            settings,
            &child.src_path,
            &child.dst_path,
            source_root,
            false,
            control_send_stream,
            error_collector,
            deref_counts,
        )
        .await
        {
            tracing::error!("Failed to send directory {:?}: {e:#}", child.src_path);
            if settings.fail_early {
                return Err(e);
            }
            error_collector.push(e);
        }
    }
    Ok(())
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
            Some(dir) => dir_map.insert(src.to_path_buf(), dir, 0).await?,
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
/// Liveness over precision (the `fail_early` edge): this is sent whenever the child
/// recursion returns `Err`, even in the case where the child had already sent its
/// own `Directory` *and* self-accounted before erroring (e.g. a deeper `fail_early`
/// abort after a grandchild failed). There the extra `FileSkipped` over-counts the
/// parent by one, which can complete it before the (incomplete, never-completing)
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
/// `dir` is the already-open handle to `src` itself; its wire metadata is read from that same fd
/// (`Dir::meta`), so the directory's metadata pairs with the contents enumerated here (read-side
/// fidelity). `is_root` drives `keep_if_empty` and the `Directory`/`Symlink` message flags exactly
/// as the path-based body does.
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn send_directory_fd_walk(
    settings: &common::copy::Settings,
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
    // store this directory's held fd + authoritative file_count so Pass 2 can open
    // file data fd-relative and size its truncation / synthetic-skip logic. Acquiring
    // the permit here bounds how many dir fds Pass 1 holds ahead of the network-paced
    // Pass 2 (prevents EMFILE); it must precede the `Directory` send so the entry is
    // present before the destination can echo `DirectoryCreated` and trigger Pass 2's
    // lookup.
    dir_map
        .insert(src.to_path_buf(), dir.clone(), file_count)
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
            tracing::error!("Failed to send directory {:?}: {e:#}", child.src_path);
            // the recursive descent returned an error. the child is counted in this
            // directory's `entry_count`, so account for it with a `FileSkipped` to keep the
            // destination's parent count balanced and avoid a hang when the child's subtree
            // never completes. This is sent even if the child had already sent its own
            // `Directory` (a deeper `fail_early` abort) — a deliberate, benign over-count;
            // see `send_child_failed_skip` for why liveness is favored over precision here.
            // (If the error was a transport failure, this send also fails and propagates — the
            // copy is already tearing down.)
            send_child_failed_skip(&child.src_path, &child.dst_path, control_send_stream).await?;
            if settings.fail_early {
                return Err(e);
            }
            error_collector.push(e);
        }
    }
    Ok(())
}

#[instrument(skip(error_collector, stream_pool, control_send_stream, source_read))]
#[async_recursion]
async fn send_fs_objects_tcp(
    settings: &common::copy::Settings,
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
    // hanging the destination). `-L` keeps the path-based flow below (documented not hardened).
    if !settings.dereference {
        return send_root_hardened(
            settings,
            src,
            dst,
            control_send_stream,
            stream_pool,
            error_collector,
            source_read,
        )
        .await;
    }
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
            src,
            dst,
            src, // source_root is src for the root item
            true,
            &control_send_stream,
            &error_collector,
            source_read.deref_counts(),
        )
        .await
    {
        tracing::error!("Failed to send directories and symlinks: {e:#}");
        if settings.fail_early {
            return Err(e);
        }
        error_collector.push(e);
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
async fn send_root_hardened(
    settings: &common::copy::Settings,
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
        } else if let Err(e) = send_file_tcp(
            settings,
            src,
            dst,
            handle.meta().size(),
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
    let _open_file_guard = throttle::open_file_permit()
        .instrument(tracing::trace_span!("open_file_permit"))
        .await;
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
        FileRead::Path => common::walk::run_metadata_probed(
            common::Side::Source,
            common::MetadataOp::Stat,
            tokio::fs::File::open(src).instrument(tracing::trace_span!("file_open")), // rcp-toctou-allow: -L path (dereference, documented not hardened)
        )
        .await
        .map(|file| (file, None)),
    };
    let (file, read_meta) = match open_result {
        Ok(f) => f,
        Err(e) => {
            tracing::error!("Failed to open file {src:?}: {e:#}");
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
/// - [`Pass2Source::DereferencePath`] carries only the `file_count` recovered from
///   the source-side `-L` count map: the `-L` walk holds no directory fd and
///   re-enumerates by path (unchanged).
enum Pass2Source {
    Hardened(MapEntry),
    DereferencePath { file_count: usize },
}

impl Pass2Source {
    /// The authoritative expected file count for this directory: the Pass-1 count
    /// stored in the map entry (hardened) or recovered from the `-L` count map.
    fn file_count(&self) -> usize {
        match self {
            Pass2Source::Hardened(MapEntry::Readable { file_count, .. }) => *file_count,
            Pass2Source::Hardened(MapEntry::Tombstone) => 0,
            Pass2Source::DereferencePath { file_count } => *file_count,
        }
    }

    /// The held source directory fd for fd-relative file opens, or `None` under
    /// `-L` (path-based enumeration) and for a hardened tombstone (no fd, 0 files).
    fn dir(&self) -> Option<&Arc<Dir>> {
        match self {
            Pass2Source::Hardened(MapEntry::Readable { dir, .. }) => Some(dir),
            Pass2Source::Hardened(MapEntry::Tombstone) => None,
            Pass2Source::DereferencePath { .. } => None,
        }
    }
}

/// Resolve the owned Pass-2 input for a `DirectoryCreated { src, dst }`, applying
/// the hardened fail-closed rule. This is the TOCTOU-safety seam. The destination
/// no longer echoes a file count, so the count is recovered source-side: from the
/// consumed map entry (hardened) or the `-L` count map (dereference).
///
/// - `SourceRead::Hardened`: CONSUME the directory's held fd-map entry (one-shot
///   ownership) and use its stored Pass-1 `file_count`. The entry may be a real
///   held-fd entry or a tombstone (committed-but-unreadable directory, `dir: None`,
///   `file_count: 0`) — both are legitimately committed entries, so both are
///   consumed normally. On a MISS — the entry is gone (never inserted, or already
///   consumed by a prior `DirectoryCreated` for the same `src`) — FAIL CLOSED: this
///   is a TOCTOU-safety / protocol-invariant violation, so close the fd-budget (so
///   a Pass-1 walk parked on it unblocks and the whole copy tears down cleanly) and
///   return an error. NEVER fall back to a path-based read.
/// - `SourceRead::DereferencePath`: the `-L` walk holds no fd. Recover the Pass-1
///   count from the `path → file_count` map. A missing entry is treated as count 0
///   with a debug log — `-L` is intentionally NOT hardened, so a miss is not a
///   TOCTOU/fail-closed condition (the destination's `entries_expected` is the
///   Pass-1 count, and Pass 2 does not re-count). The entry is CONSUMED (removed),
///   mirroring the hardened one-shot lifecycle — a directory's files are requested
///   exactly once, so this bounds the map's memory during large dereference copies.
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
                map.close_fd_budget();
                Err(err)
            }
        },
        SourceRead::DereferencePath(counts) => {
            let file_count = counts.lock().unwrap().remove(src).unwrap_or_else(|| {
                tracing::debug!(
                    "no recorded -L file count for {src:?} on DirectoryCreated; defaulting to 0 \
                     (dereference path is not hardened, so this is not a fail-closed condition)"
                );
                0
            });
            Ok(Pass2Source::DereferencePath { file_count })
        }
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
    src: std::path::PathBuf,
    dst: std::path::PathBuf,
    source_root: std::path::PathBuf,
    // owned Pass-2 input: the held map entry (hardened) or the `-L` file count
    // recovered from the source-side count map. Carries the authoritative
    // `file_count` and, in hardened mode, the held `Dir` fd used to open file DATA
    // fd-relative (None for a tombstone, which has 0 files). The owned entry's
    // fd-budget permit is released when this function returns (entry dropped here).
    pass2_source: Pass2Source,
    stream_pool: std::sync::Arc<AcceptingSendStreamPool>,
    pending_limit: std::sync::Arc<tokio::sync::Semaphore>,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
    control_send_stream: remote::streams::BoxedSharedSendStream,
    existing: std::sync::Arc<
        std::collections::HashMap<std::path::PathBuf, remote::protocol::ExistingEntry>,
    >,
) -> anyhow::Result<()> {
    // the Pass-1 count is authoritative for this directory's send logic (truncation
    // and synthetic `FileSkipped`). It comes entirely from the source side now (the
    // consumed map entry or the `-L` count map); the destination echoes nothing.
    let file_count = pass2_source.file_count();
    let src_dir = pass2_source.dir();
    tracing::info!(
        "Sending files from {src:?} (expected file_count={})",
        file_count
    );
    // if no files expected, nothing to do (the owned entry, if any, drops here,
    // releasing its dir-fd-in-flight permit back to Pass 1)
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
        // path-based enumeration (`-L`/--dereference): unchanged from the original
        // behavior — nested symlink following is intentionally not hardened.
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
        // extra files appeared since traversal - only send up to file_count
        tracing::warn!(
            "Directory {:?} has {} extra files since traversal, ignoring extras",
            src,
            files_found - file_count
        );
        if settings.fail_early {
            return Err(anyhow::anyhow!(
                "directory {:?} contents changed: expected {} files, found {}",
                src,
                file_count,
                files_found
            ));
        }
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
/// 2. Break main loop with Ok (stream closed is not an error)
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
    source_root: std::path::PathBuf,
    mut control_recv_stream: remote::streams::BoxedRecvStream,
    control_send_stream: remote::streams::BoxedSharedSendStream,
    stream_pool: std::sync::Arc<AcceptingSendStreamPool>,
    max_pending_files: usize,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
    pool_shutdown: PoolShutdownToken,
    // explicit source read mode. In hardened mode each directory's `DirectoryCreated`
    // consumes the held fd-map entry (the owned `Dir` + permit) for Pass 2 and a miss
    // fails closed; under `-L` there is no fd-map and Pass 2 re-enumerates by path.
    source_read: SourceRead,
) -> anyhow::Result<()> {
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
                    Some(RecvResult::StreamClosed) | None => break Ok(()), // stream closed
                    Some(RecvResult::Error(e)) => break Err(e),
                };
                match message {
                    remote::protocol::DestinationMessage::DirectoryCreated {
                        ref src,
                        ref dst,
                        existing,
                    } => {
                        tracing::info!(
                            "Received directory creation confirmation for: {:?} -> {:?} ({} existing)",
                            src,
                            dst,
                            existing.len()
                        );
                        let existing_map: std::sync::Arc<
                            std::collections::HashMap<std::path::PathBuf, remote::protocol::ExistingEntry>,
                        > = std::sync::Arc::new(
                            // move each entry into the map (only the small name key is cloned),
                            // avoiding a full per-entry clone of the received manifest.
                            existing
                                .into_iter()
                                .map(|e| (e.name.clone(), e))
                                .collect(),
                        );
                        // build the owned Pass-2 input. In hardened mode this CONSUMES the
                        // directory's held fd-map entry (one-shot ownership) and fails closed
                        // on a miss (see `resolve_pass2_source`); the owned `Dir` + permit
                        // then move into the spawned task. The file count is recovered
                        // source-side (map entry or `-L` count map) — no wire echo.
                        let pass2_source = match resolve_pass2_source(&source_read, src) {
                            Ok(source) => source,
                            // fail closed: the fd-budget was already closed inside the helper
                            // to unblock any parked Pass-1 walk; abort the dispatch loop.
                            Err(e) => break Err(e),
                        };
                        let collector = error_collector.clone();
                        let settings = settings.clone();
                        join_set.spawn(send_files_in_directory_tcp(
                            settings,
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
                            // -L: no fd is held, but Pass 1 recorded a count entry; remove it
                            // here so a skipped subtree's counts don't grow until the connection
                            // ends (mirrors the DirectoryCreated consume + the hardened nack).
                            SourceRead::DereferencePath(counts) => {
                                counts.lock().unwrap().remove(src);
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
    // if we're exiting with an error, abort the recv task immediately
    // (otherwise it would block waiting for more messages from destination)
    if result.is_err() {
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
    let pool_shutdown_errors_expected = result.is_ok(); // pool was just cancelled
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
    // wait for recv task to finish (it will close the stream)
    let _ = recv_task.await;
    tracing::info!("Finished dispatching control messages");
    result
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
        tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    ) -> (Self, PoolShutdownToken, tokio::task::JoinHandle<()>) {
        let (send_tx, recv) = async_channel::bounded(pool_size);
        let (return_tx, return_rx) =
            async_channel::bounded::<remote::streams::BoxedSendStream>(pool_size);
        let shutdown_token = PoolShutdownToken::new();
        let shutdown_token_clone = shutdown_token.clone();
        // spawn task to accept data connections and manage pool
        let accept_task = tokio::spawn(async move {
            // wrap the main loop so we can handle shutdown
            tokio::select! {
                _ = async {
                    loop {
                        tokio::select! {
                            // accept new connections from destination
                            result = data_listener.accept() => {
                                match result {
                                    Ok((stream, addr)) => {
                                        tracing::debug!("Accepted data connection from {}", addr);
                                        stream.set_nodelay(true).ok();
                                        remote::configure_tcp_buffers(&stream, profile);
                                        // wrap with TLS if configured
                                        let send_stream = if let Some(ref acceptor) = tls_acceptor {
                                            match acceptor.accept(stream).await {
                                                Ok(tls_stream) => {
                                                    let (_read_half, write_half) = tokio::io::split(tls_stream);
                                                    remote::streams::SendStream::new(
                                                        Box::new(write_half) as remote::streams::BoxedWrite
                                                    )
                                                }
                                                Err(e) => {
                                                    tracing::warn!("TLS handshake failed for data connection: {}", e);
                                                    continue;
                                                }
                                            }
                                        } else {
                                            let (_read_half, write_half) = stream.into_split();
                                            remote::streams::SendStream::new(
                                                Box::new(write_half) as remote::streams::BoxedWrite
                                            )
                                        };
                                        if send_tx.send(send_stream).await.is_err() {
                                            tracing::debug!("Pool closed, stopping accept loop");
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        tracing::debug!("Data listener accept error: {}", e);
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
    src: &std::path::Path,
    dst: &std::path::Path,
    pool_size: usize,
    max_pending_files: usize,
    network_profile: remote::NetworkProfile,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
    tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
) -> anyhow::Result<()> {
    tracing::info!("Destination control connection established");
    // configure TCP buffers for high throughput
    remote::configure_tcp_buffers(&control_stream, network_profile);
    // wrap control connection with TLS if configured
    let (control_send_stream, control_recv_stream) = if let Some(ref acceptor) = tls_acceptor {
        let tls_stream = acceptor
            .accept(control_stream)
            .await
            .context("TLS handshake failed for control connection")?;
        let (read_half, write_half) = tokio::io::split(tls_stream);
        let recv_stream =
            remote::streams::RecvStream::new(Box::new(read_half) as remote::streams::BoxedRead);
        let send_stream =
            remote::streams::SendStream::new(Box::new(write_half) as remote::streams::BoxedWrite);
        (send_stream, recv_stream)
    } else {
        let (read_half, write_half) = control_stream.into_split();
        let recv_stream =
            remote::streams::RecvStream::new(Box::new(read_half) as remote::streams::BoxedRead);
        let send_stream =
            remote::streams::SendStream::new(Box::new(write_half) as remote::streams::BoxedWrite);
        (send_stream, recv_stream)
    };
    // wrap in Arc<Mutex<>> for shared access
    let control_send_stream = std::sync::Arc::new(tokio::sync::Mutex::new(control_send_stream));
    tracing::info!("Created control streams for directory transfer");
    // create a pool that accepts data connections from destination and provides SendStreams
    let (stream_pool, pool_shutdown, accept_task) =
        AcceptingSendStreamPool::new(data_listener, pool_size, network_profile, tls_acceptor);
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
    // dispatch from each `DirectoryCreated`). Its dir-fd-in-flight budget bounds how
    // far the unthrottled Pass-1 walk can race ahead of the network-paced Pass 2
    // (prevents EMFILE); sized like the file pending-writes pool. The `-L` variant
    // instead carries a shared `path → file_count` map so Pass 2 can recover each
    // directory's Pass-1 count (the destination no longer echoes it over the wire).
    let source_read = if settings.dereference {
        SourceRead::DereferencePath(Arc::new(std::sync::Mutex::new(HashMap::new())))
    } else {
        SourceRead::Hardened(Arc::new(SourceDirMap::new(max_pending_files)))
    };
    // pass a clone of the shutdown token to dispatch - it will signal shutdown before
    // draining its tasks to prevent deadlock when destination closes unexpectedly.
    // see dispatch_control_messages_tcp doc comment for detailed shutdown flow.
    let dispatch_task = tokio::spawn(dispatch_control_messages_tcp(
        settings.clone(),
        src.to_path_buf(),
        control_recv_stream,
        control_send_stream.clone(),
        stream_pool.clone(),
        max_pending_files,
        error_collector.clone(),
        pool_shutdown.clone(),
        source_read.clone(),
    ));
    // send files to destination. returns Err only for fatal errors (e.g., root file failure).
    // individual file failures with fail_early=false return Ok but push errors to collector,
    // and destination is notified via FileSkipped messages on the control channel.
    let send_result = send_fs_objects_tcp(
        settings,
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
        // abort dispatch task since we're not going to get a clean shutdown
        dispatch_task.abort();
        // wait for accept task to finish closing all streams
        let _ = accept_task.await;
        return send_result;
    }
    // send succeeded - wait for dispatch task to complete (handles destination responses).
    // note: dispatch_control_messages_tcp always calls pool_shutdown.cancel() before
    // returning, so the pool will be shut down when dispatch_task completes.
    let dispatch_result = dispatch_task.await;
    // wait for accept task to finish (pool shutdown was signaled by dispatch)
    let _ = accept_task.await;
    // propagate dispatch errors after cleanup
    dispatch_result??;
    tracing::info!("Data sent successfully");
    Ok(())
}

/// Traverse filesystem and report dry-run entries via tracing.
/// This function outputs what would be copied without actually copying.
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

/// Handle a dry-run connection: traverse, log entries, and complete without transferring data.
/// Destination sees an empty copy and completes immediately.
async fn handle_dry_run_connection(
    stream: tokio::net::TcpStream,
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    dry_run_mode: common::config::DryRunMode,
    tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
) -> anyhow::Result<(String, common::copy::Summary)> {
    tracing::info!("Handling dry-run connection");
    // set up TLS if needed
    let (control_send_stream, mut control_recv_stream): (
        remote::streams::BoxedSendStream,
        remote::streams::BoxedRecvStream,
    ) = if let Some(acceptor) = tls_acceptor {
        let tls_stream = acceptor.accept(stream).await.context("TLS accept failed")?;
        let (read_half, write_half) = tokio::io::split(tls_stream);
        (
            remote::streams::SendStream::new(Box::new(write_half) as remote::streams::BoxedWrite),
            remote::streams::RecvStream::new(Box::new(read_half) as remote::streams::BoxedRead),
        )
    } else {
        let (read_half, write_half) = stream.into_split();
        (
            remote::streams::SendStream::new(Box::new(write_half) as remote::streams::BoxedWrite),
            remote::streams::RecvStream::new(Box::new(read_half) as remote::streams::BoxedRead),
        )
    };
    let control_send_stream: remote::streams::BoxedSharedSendStream =
        std::sync::Arc::new(tokio::sync::Mutex::new(control_send_stream));
    // traverse and log dry-run entries (output goes via tracing)
    let mut summary = common::copy::Summary::default();
    dry_run_traverse(settings, src, dst, src, true, dry_run_mode, &mut summary).await?;
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
    tcp_config: &remote::TcpConfig,
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
    let pool_size = tcp_config.max_connections;
    let max_pending_files = pool_size * tcp_config.pending_writes_multiplier;
    match tokio::time::timeout(accept_timeout, control_listener.accept()).await {
        Ok(Ok((stream, addr))) => {
            tracing::info!("Destination control connection from {}", addr);
            stream.set_nodelay(true)?;
            // in dry-run mode, do simplified flow: traverse, log, and tell destination we're done
            if let Some(dry_run_mode) = settings.dry_run {
                return handle_dry_run_connection(
                    stream,
                    settings,
                    src,
                    dst,
                    dry_run_mode,
                    tls_acceptor,
                )
                .await;
            }
            // normal flow
            handle_connection(
                stream,
                data_listener,
                settings,
                src,
                dst,
                pool_size,
                max_pending_files,
                tcp_config.network_profile,
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
