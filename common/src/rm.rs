use anyhow::{Context, anyhow};
use std::ffi::OsStr;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::instrument;

use crate::filter::TimeFilter;
use crate::progress;
use crate::safedir::{self, Dir, Handle};
use crate::walk::{EntryKind, LeafPermit, PermitKind};
use crate::walk_driver::{
    DirAction, DirPreResult, EntryCx, ProcessedChildren, WalkVisitor, process_entry,
};

/// Error type for remove operations. See [`crate::error::OperationError`] for
/// logging conventions and rationale.
pub type Error = crate::error::OperationError<Summary>;

#[derive(Debug, Clone)]
pub struct Settings {
    pub fail_early: bool,
    /// filter settings for include/exclude patterns
    pub filter: Option<crate::filter::FilterSettings>,
    /// time-based filter (mtime/btime); applied to each entry individually (files,
    /// symlinks, and directories). This is an entry filter, not a subtree gate:
    /// directories are always traversed, and the filter only decides whether each
    /// entry — including the directory itself, after its children are processed — is
    /// eligible for removal. A directory whose own timestamps are too recent is left
    /// intact even when its children have been removed; a non-empty leftover directory
    /// is logged at info and not treated as an error.
    pub time_filter: Option<TimeFilter>,
    /// dry-run mode for previewing operations
    pub dry_run: Option<crate::config::DryRunMode>,
}

/// Returns true when `err`'s chain contains an `io::Error` with `ErrorKind::Unsupported`.
/// Used to downgrade time-filter eval failures on filesystems / entry types that don't
/// report btime (e.g. many symlinks) from `error!` to `warn!` so they don't flood logs
/// on otherwise-successful runs.
fn is_unsupported_io_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_err| io_err.kind() == std::io::ErrorKind::Unsupported)
    })
}

/// Summary with the appropriate `*_skipped` counter set to 1 for the given entry kind.
/// Special files count as `files_skipped` to match the historical mapping used
/// when filters skip an entry.
fn skipped_summary_for(kind: EntryKind) -> Summary {
    match kind {
        EntryKind::Dir => Summary {
            directories_skipped: 1,
            ..Default::default()
        },
        EntryKind::Symlink => Summary {
            symlinks_skipped: 1,
            ..Default::default()
        },
        EntryKind::File | EntryKind::Special => Summary {
            files_skipped: 1,
            ..Default::default()
        },
    }
}

#[derive(Copy, Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Summary {
    pub bytes_removed: u64,
    pub files_removed: usize,
    pub symlinks_removed: usize,
    pub directories_removed: usize,
    pub files_skipped: usize,
    pub symlinks_skipped: usize,
    pub directories_skipped: usize,
}

impl std::ops::Add for Summary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            bytes_removed: self.bytes_removed + other.bytes_removed,
            files_removed: self.files_removed + other.files_removed,
            symlinks_removed: self.symlinks_removed + other.symlinks_removed,
            directories_removed: self.directories_removed + other.directories_removed,
            files_skipped: self.files_skipped + other.files_skipped,
            symlinks_skipped: self.symlinks_skipped + other.symlinks_skipped,
            directories_skipped: self.directories_skipped + other.directories_skipped,
        }
    }
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "bytes removed: {}\n\
            files removed: {}\n\
            symlinks removed: {}\n\
            directories removed: {}\n\
            files skipped: {}\n\
            symlinks skipped: {}\n\
            directories skipped: {}\n",
            bytesize::ByteSize(self.bytes_removed),
            self.files_removed,
            self.symlinks_removed,
            self.directories_removed,
            self.files_skipped,
            self.symlinks_skipped,
            self.directories_skipped
        )
    }
}

/// RAII guard that restores a relaxed directory's original mode on drop, fd-pinned.
///
/// To clear a read-only directory's contents, [`RmVisitor::dir_pre`] chmod-relaxes it (via the directory's
/// own `O_PATH` handle, before `open_dir`). If the directory is then retained (filter-protected
/// children, time-filter skip, ENOTEMPTY) or any error occurs after the relax, the original mode
/// must be restored — otherwise an `rrm` run leaves a protected tree world-readable/executable.
/// Drop fires on every exit path (retain, error, panic-unwind) without callers having to remember
/// it; the success-remove path calls [`Self::defuse`] because the directory no longer exists.
///
/// # Race safety
///
/// The guard holds a dup of the directory's `O_PATH` handle fd, and restores via
/// [`safedir::chmod_via_proc_fd_sync`] — a chmod of the EXACT inode that fd pins, through its
/// `/proc/self/fd/N` magic symlink. This is inode-exact and immune to a concurrent
/// rename/symlink swap of the directory's name: there is no path re-resolution that an attacker
/// could redirect. It is the synchronous counterpart of the relax (`chmod_via_proc_fd`), so the
/// relax and the restore target the same pinned inode. This is the TOCTOU-safe replacement for
/// the old path-based `std::fs::set_permissions(path, ...)` restore.
///
/// Drop runs synchronously (it can't be async), so it issues one ungated `fchmodat` — negligible
/// cost, no need to round-trip through the tokio blocking pool just for cleanup. Best-effort: a
/// restore failure is logged, not fatal.
///
/// # Lifecycle (leak-free even under fd exhaustion)
///
/// The guard is built in two steps so the relax can never outlive the ability to restore:
/// 1. [`Self::prepare`] dups the `O_PATH` fd and records the original mode while leaving the guard
///    *disarmed* (Drop is a no-op). This is the only fallible step (the dup) — and it runs BEFORE
///    the relax chmod. If it fails (e.g. `EMFILE`), no relax has happened yet, so there is nothing
///    to leak.
/// 2. After the relax chmod succeeds, [`Self::arm`] flips the guard to active, so every subsequent
///    exit path (retain, error, panic-unwind) restores the original mode.
///
/// The success-delete path calls [`Self::defuse`] (the directory no longer exists).
struct RelaxedDirGuard {
    /// Dup of the directory's `O_PATH` handle fd, pinning the inode to restore.
    fd: OwnedFd,
    /// Original mode to restore on drop. `None` means disarmed/defused (no restore).
    mode: Option<u32>,
}

impl RelaxedDirGuard {
    /// Dup the directory's `O_PATH` handle fd, returning a *disarmed* guard (Drop restores nothing
    /// yet). Call this BEFORE relaxing the directory's mode so the restore fd is secured first; if
    /// the dup fails (e.g. `EMFILE`) the caller must not relax — nothing to leak. Once the relax
    /// chmod succeeds, call [`Self::arm`] with the original mode to make the restore fire on every
    /// later exit path.
    fn prepare(handle: &Handle) -> std::io::Result<Self> {
        let fd = handle.as_fd().try_clone_to_owned()?;
        Ok(Self { fd, mode: None })
    }
    /// Arm the guard so Drop restores `original_mode`. Called after the relax chmod succeeds.
    fn arm(&mut self, original_mode: u32) {
        self.mode = Some(original_mode);
    }
    /// Cancel the pending restore (call after successfully removing the directory).
    fn defuse(&mut self) {
        self.mode = None;
    }
}

impl Drop for RelaxedDirGuard {
    fn drop(&mut self) {
        if let Some(mode) = self.mode.take()
            && let Err(err) = safedir::chmod_via_proc_fd_sync(self.fd.as_fd(), mode)
        {
            tracing::warn!(
                "failed to restore original permissions on retained directory (fd-pinned inode): {:#}",
                err
            );
        }
    }
}

/// Public entry point for remove operations.
///
/// The walk is fd-based (see [`crate::safedir`]): the root operand is opened relative to its
/// parent directory and every entry is classified and removed through file-descriptor-relative
/// syscalls. A privileged `rrm` therefore cannot be redirected by a concurrent symlink swap
/// into deleting a tree outside the intended target (the classic `rm -rf` symlink race) — the
/// `O_NOFOLLOW|O_DIRECTORY` opens in [`Dir::open_dir`] catch a directory→symlink swap mid-walk
/// and fail closed (ELOOP/ENOTDIR) rather than descending the link, and leaf removal uses
/// `unlinkat` (never follows a symlink). The recursive walk skeleton — enumeration, the
/// leaf-permit lifecycle, spawning, the single drop-before-recurse site, and the error fold —
/// lives in the generic [`crate::walk_driver`]; this module supplies the remove visitor.
#[instrument(skip(prog_track, settings))]
pub async fn rm(
    prog_track: &'static progress::Progress,
    path: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    // decompose the operand into (parent dir, final component) so the root entry is opened and
    // classified relative to a directory fd — the same fd-relative shape every nested entry takes.
    // `.`/`..` operands (e.g. `rrm .`) are canonicalized so they still name a directory; `/` is
    // rejected. rm reads and removes "the" tree; we gate it on the Source side (matching the
    // existing rm code, which probes its stat/readdir on Source and only the destructive
    // unlink/rmdir on Destination).
    let operand = crate::walk::split_root_operand(path)
        .await
        .map_err(|err| Error::new(err, Default::default()))?;
    let parent_path = operand.parent.as_path();
    let name = operand.name.as_os_str();
    let path = operand.display.as_path();
    // the operand's TRUSTED parent prefix is resolved following symlinks normally (the prefix is
    // trusted up to and including the operand's container — only entries strictly below the named
    // root are O_NOFOLLOW-hardened). a symlinked parent (e.g. `rrm symlinkdir/foo`) is followed; the
    // operand itself is still classified via `child(name)` with O_NOFOLLOW (a symlink root is
    // removed as the link itself).
    let parent = Dir::open_parent_dir(parent_path, congestion::Side::Source)
        .await
        .with_context(|| format!("cannot open parent directory {parent_path:?}"))
        .map_err(|err| Error::new(err, Default::default()))?;
    // cross from the trusted parent prefix into the hardened tree (O_NOFOLLOW below here).
    let parent = Arc::new(parent.into_tree());
    if let Some(ref filter) = settings.filter {
        // the top-level include/exclude filter is checked against `path` itself (its file name);
        // classify the root via its parent fd purely to evaluate the root filter. the driver
        // re-classifies the root authoritatively in `process_entry`, so this handle is just a probe.
        let root_handle = parent
            .child(name)
            .await
            .with_context(|| format!("failed reading metadata from {path:?}"))
            .map_err(|err| Error::new(err, Default::default()))?;
        let name_path = std::path::Path::new(name);
        let result =
            filter.should_include_root_item(name_path, root_handle.kind() == EntryKind::Dir);
        match result {
            crate::filter::FilterResult::Included => {}
            result => {
                let kind = root_handle.kind();
                if let Some(mode) = settings.dry_run {
                    crate::dry_run::report_skip(path, &result, mode, kind.label_long());
                }
                kind.inc_skipped(prog_track);
                return Ok(skipped_summary_for(kind));
            }
        }
    }
    // the root entry's owned context: rel_path/filter_path empty (the root), real_path = the
    // operand. rm has no delegated subtree, so `filter_path == rel_path`. The root is processed
    // exactly like a nested child via `process_entry`, with no pre-acquired permit (it is not
    // spawned from the backpressure-limited walk loop).
    let visitor = Arc::new(RmVisitor {
        prog_track,
        settings: settings.clone(),
    });
    let root_cx = EntryCx {
        parent: Arc::clone(&parent),
        name: name.to_owned(),
        rel_path: PathBuf::new(),
        filter_path: PathBuf::new(),
        real_path: path.to_path_buf(),
        dry_run: settings.dry_run.is_some(),
        prog_track,
    };
    process_entry(visitor, root_cx, (), None).await // rm has no second tree → root context `()`
}

/// Remove a single child entry of an already-open directory, fd-relative.
///
/// This is the fd-based counterpart of path-based [`rm`]. It is used by two callers that
/// already hold the relevant directory as an open [`Dir`] and want to remove one of its children
/// through that pinned fd rather than re-resolving the entry by absolute path (the redirectable
/// window):
/// - `--delete` pruning (see [`crate::delete::prune_extraneous`]) removes each extraneous entry.
/// - the remote-copy destination (`rcpd`) replaces a non-matching destination subtree (a
///   directory/file/symlink in the way of the entry it must create) through the parent directory
///   fd held in its directory tracker.
///
/// The entry is classified via `parent.child(name)` (`O_PATH|O_NOFOLLOW`, so a symlink is
/// classified as a symlink, never followed) and then removed through the same fd-relative remove
/// machinery (driven by [`crate::walk_driver::process_entry`]): leaves via `unlinkat`, directories
/// by recursing with `O_NOFOLLOW|O_DIRECTORY` descent and the fd-pinned relax guard. A privileged
/// caller therefore cannot be redirected by a concurrent symlink swap of `name` into deleting a
/// tree outside the directory it holds.
///
/// `rel_path` is the entry's path relative to the mirror/destination root: seeded as the root
/// entry's `rel_path`/`filter_path`/`real_path`, it both anchors include/exclude filter matching
/// against the entry's destination-root-relative path and reconstructs the entry's display path for
/// diagnostics / dry-run output. The caller is responsible for the top-level decision on `name`
/// (keep-set membership, exclude-protection, overwrite-recheck); this only removes the subtree.
pub async fn rm_child(
    prog_track: &'static progress::Progress,
    parent: &Arc<Dir>,
    name: &OsStr,
    rel_path: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    // build the child's owned context rooted at `rel_path`: the display path and the filter path
    // then each equal `rel_path` (the destination-root-relative path), anchoring include/exclude
    // matching against the entry's full root-relative path, and each descendant extends it by one
    // component (exactly the bare-roots behavior the old `WalkRoots { operand: "", filter: "" }`
    // produced via `operand.join(rel_path)`). these paths are pure display/filter strings — they
    // are never opened, so there is no dst path for an attacker to redirect.
    let visitor = Arc::new(RmVisitor {
        prog_track,
        settings: settings.clone(),
    });
    let root_cx = EntryCx {
        parent: Arc::clone(parent),
        name: name.to_owned(),
        rel_path: rel_path.to_path_buf(),
        filter_path: rel_path.to_path_buf(),
        real_path: rel_path.to_path_buf(),
        dry_run: settings.dry_run.is_some(),
        prog_track,
    };
    // a `rm_child` entry point is not spawned from the backpressure-limited walk loop, so it never
    // pre-acquires a pending-meta permit. `process_entry` classifies `name` authoritatively via
    // `child()` (a swap of `name` to a symlink is caught there — classified as a symlink leaf,
    // never descended) and dispatches to the visitor.
    process_entry(visitor, root_cx, (), None).await
}
/// The remove walk's [`WalkVisitor`]. The driver owns enumeration, the leaf-permit lifecycle,
/// spawning, the single drop-before-recurse site, and the error fold; this visitor supplies rm's
/// per-entry bodies (leaf removal in [`WalkVisitor::visit_leaf`], the relax-guard + metadata
/// snapshot in [`WalkVisitor::dir_pre`], and the post-order `rmdir` decision in
/// [`WalkVisitor::dir_post`]). rm has no second tree, so [`Self::DirContext`] is `()`.
struct RmVisitor {
    prog_track: &'static progress::Progress,
    settings: Settings,
}

/// State threaded from [`WalkVisitor::dir_pre`] to [`WalkVisitor::dir_post`] (same task) — what the
/// post-order `rmdir` decision needs.
///
/// The [`RelaxedDirGuard`] lives here precisely so the error path restores the relaxed mode: on a
/// child failure the `?` after the contents walk in the driver's `process_entry` returns early
/// (fail-early) or `dir_post` returns the combined error (keep-going) — either way `dir_post` never
/// reaches its `defuse()`, so the guard's `Drop` restores the original directory mode (matching the
/// old code's local `relaxed_guard` dropping on the early-return error paths).
struct RmDirState {
    /// Restores the relaxed directory mode on every retain/error path (fd-pinned, inode-exact);
    /// `defuse`d only after a successful `rmdir`. `None` in dry-run (no relax happens) and when the
    /// directory already had owner rwx (no relax needed).
    guard: Option<RelaxedDirGuard>,
    /// The directory's PRISTINE full metadata, snapshotted in `dir_pre` BEFORE the relax or any
    /// child removal, so the directory's own time filter sees timestamps unperturbed by clearing
    /// its contents (removing children bumps mtime). `Some` iff a time filter is configured.
    time_metadata: Option<anyhow::Result<std::fs::Metadata>>,
    /// Whether the directory does NOT directly match an include pattern while includes are active —
    /// the path-only half of the "traversed only" decision. Combined in `dir_post` with
    /// "nothing was removed" (derived from the children's folded summary) to reproduce the old
    /// `traversed_only` exactly.
    matches_no_include: bool,
}

impl WalkVisitor for RmVisitor {
    type Summary = Summary;
    type DirContext = ();
    type DirState = RmDirState;

    fn root_dir_context(&self) {}

    fn permit_kind(&self) -> PermitKind {
        // we use the pending-meta semaphore (not open-files) because rm is reachable from
        // copy_file's overwrite path, which already holds an open-files permit; using a distinct
        // semaphore avoids that cross-pool deadlock. rm holds no open fd across leaf work.
        PermitKind::PendingMeta
    }

    fn want_permit(&self, hint: Option<EntryKind>) -> bool {
        // pre-acquire only for a positively-known leaf hint. We deliberately skip pre-acquire when
        // the getdents hint is unknown (DT_UNKNOWN -> None): the entry could actually be a
        // directory, and a chain of such unknown-typed directories holding permits while recursing
        // would deadlock the pending-meta pool. Directories also skip pre-acquire for the same
        // reason. The authoritative type is still the child's fstat; this is only the hint.
        hint.is_some_and(|ft| ft != EntryKind::Dir)
    }

    fn fail_early(&self) -> bool {
        self.settings.fail_early
    }

    fn filter(&self) -> Option<&crate::filter::FilterSettings> {
        self.settings.filter.as_ref()
    }

    fn on_skip(
        &self,
        cx: &EntryCx,
        kind: EntryKind,
        skip_result: &crate::filter::FilterResult,
    ) -> Summary {
        // mirror the old spawn loop's inline filter-skip: the dry-run "skip ..." line plus the
        // matching `*_skipped` counter. the driver already did the shared progress increment.
        if let Some(mode) = self.settings.dry_run {
            crate::dry_run::report_skip(&cx.real_path, skip_result, mode, kind.label());
        }
        skipped_summary_for(kind)
    }

    async fn visit_leaf(
        &self,
        cx: &EntryCx,
        _parent_ctx: &(),
        handle: Handle,
        kind: EntryKind,
        permit: Option<LeafPermit>,
    ) -> Result<Summary, Error> {
        // leaf: hold the permit through the (non-recursive) removal, then drop on return (the
        // driver drops it for directories, never here).
        let _permit = permit;
        let prog_track = self.prog_track;
        let settings = &self.settings;
        let parent = &cx.parent;
        let name = cx.name.as_os_str();
        let path = cx.real_path.as_path();
        tracing::debug!("not a directory, just remove");
        let is_symlink = kind == EntryKind::Symlink;
        let file_size = if is_symlink {
            0
        } else {
            crate::preserve::Metadata::size(handle.meta())
        };
        // apply time filter before removing (files/symlinks only)
        if let Some(ref time_filter) = settings.time_filter {
            let entry_type = if is_symlink { "symlink" } else { "file" };
            let make_skipped_summary = || {
                tracing::debug!("skipping {:?} due to time filter", &path);
                if is_symlink {
                    prog_track.symlinks_skipped.inc();
                    Summary {
                        symlinks_skipped: 1,
                        ..Default::default()
                    }
                } else {
                    prog_track.files_skipped.inc();
                    Summary {
                        files_skipped: 1,
                        ..Default::default()
                    }
                }
            };
            // the time filter needs full std metadata (btime for --created-before), which the
            // fd snapshot does not carry; read it inode-exact through the pinned handle so a
            // concurrent swap of `name` cannot redirect the stat to a different inode.
            let metadata =
                match safedir::stat_meta_via_proc_fd(&handle, congestion::Side::Source).await {
                    Ok(md) => md,
                    Err(err) => {
                        let err = anyhow::Error::new(err).context(format!(
                            "failed reading metadata for time filter on {path:?}"
                        ));
                        if settings.fail_early {
                            return Err(Error::new(err, Default::default()));
                        }
                        // log and skip — never delete an entry whose age we cannot verify.
                        if is_unsupported_io_error(&err) {
                            tracing::warn!(
                                "time filter evaluation unsupported for {} {:?}, skipping: {:#}",
                                entry_type,
                                &path,
                                &err
                            );
                        } else {
                            tracing::error!(
                                "time filter evaluation failed for {} {:?}, skipping: {:#}",
                                entry_type,
                                &path,
                                &err
                            );
                        }
                        return Ok(make_skipped_summary());
                    }
                };
            match time_filter.matches(&metadata) {
                Ok(result) => {
                    if let Some(skip_reason) = result.as_skip_reason() {
                        if let Some(mode) = settings.dry_run {
                            crate::dry_run::report_time_skip(path, skip_reason, mode, entry_type);
                        }
                        return Ok(make_skipped_summary());
                    }
                }
                Err(err) => {
                    let err = err.context(format!("failed evaluating time filter on {:?}", &path));
                    if settings.fail_early {
                        return Err(Error::new(err, Default::default()));
                    }
                    // log and skip — never delete an entry whose age we cannot verify.
                    // btime being unsupported (common for symlinks) is expected noise, so
                    // downgrade to warn; anything else is unexpected and stays at error.
                    if is_unsupported_io_error(&err) {
                        tracing::warn!(
                            "time filter evaluation unsupported for {} {:?}, skipping: {:#}",
                            entry_type,
                            &path,
                            &err
                        );
                    } else {
                        tracing::error!(
                            "time filter evaluation failed for {} {:?}, skipping: {:#}",
                            entry_type,
                            &path,
                            &err
                        );
                    }
                    return Ok(make_skipped_summary());
                }
            }
        }
        // handle dry-run mode for files/symlinks
        if settings.dry_run.is_some() {
            let entry_type = if is_symlink { "symlink" } else { "file" };
            crate::dry_run::report_action("remove", path, None, entry_type);
            return Ok(Summary {
                bytes_removed: file_size,
                files_removed: if is_symlink { 0 } else { 1 },
                symlinks_removed: if is_symlink { 1 } else { 0 },
                ..Default::default()
            });
        }
        // fd-relative removal of the link/file itself: unlink_at never follows a symlink, and is
        // resolved relative to the parent dir fd, so it cannot be redirected outside the tree.
        // gated on the Destination side to match the side the path-based rm used for remove_file.
        if let Err(err) = parent
            .unlink_at_on(name, congestion::Side::Destination)
            .await
            .with_context(|| format!("failed removing {:?}", &path))
        {
            return Err(Error::new(err, Default::default()));
        }
        if is_symlink {
            prog_track.symlinks_removed.inc();
            return Ok(Summary {
                symlinks_removed: 1,
                ..Default::default()
            });
        }
        prog_track.files_removed.inc();
        prog_track.bytes_removed.add(file_size);
        Ok(Summary {
            bytes_removed: file_size,
            files_removed: 1,
            ..Default::default()
        })
    }

    async fn dir_pre(&self, cx: &EntryCx, _parent_ctx: &(), handle: &Handle) -> DirPreResult<Self> {
        let settings = &self.settings;
        let path = cx.real_path.as_path();
        tracing::debug!("remove contents of the directory first");
        // Snapshot the directory's full metadata NOW, before relaxing its mode or removing any
        // child. The directory's own time filter (evaluated in `dir_post`, after its children are
        // processed) must use these pristine timestamps: removing children bumps the directory's
        // mtime, so a fresh stat at the end would wrongly see the directory as "just modified".
        // This mirrors the old code's `src_metadata` captured at entry. We read full std metadata
        // (not just the fd snapshot) because `--created-before` needs btime; it's read inode-exact
        // through the pinned handle. Only fetched when a time filter is configured. The relax below
        // changes only mode/ctime, never mtime/btime, so taking the snapshot before it is correct.
        let time_metadata: Option<anyhow::Result<std::fs::Metadata>> =
            if settings.time_filter.is_some() {
                Some(
                    safedir::stat_meta_via_proc_fd(handle, congestion::Side::Source)
                        .await
                        .map_err(|err| {
                            anyhow::Error::new(err).context(format!(
                                "failed reading metadata for time filter on {path:?}"
                            ))
                        }),
                )
            } else {
                None
            };
        // the path-only half of the "traversed only" decision: a directory is "traversed only" when
        // include filters are active and the directory itself doesn't directly match an include
        // pattern (it was entered only to search for matching content). `dir_post` combines this
        // with "nothing was removed". exclude-only filters never produce traversed-only directories
        // because `directly_matches_include` returns true when no includes exist.
        let matches_no_include = settings.filter.as_ref().is_some_and(|f| {
            f.has_includes() && !f.directly_matches_include(&cx.filter_path, true)
        });
        // When the directory lacks owner write/execute we relax it so its contents can be cleared.
        // The relax goes through the directory's OWN `O_PATH` handle (via /proc), which works even
        // on a 0000-mode dir a non-root owner cannot open O_RDONLY — so it must happen BEFORE
        // `open_dir`. The guard (carried in `RmDirState`) restores the original mode on Drop
        // (fd-pinned, inode-exact) — covering every retain branch (filter-protected children,
        // time-filter skip, ENOTEMPTY) AND every error path that returns before the directory is
        // removed (the driver drops the state without calling `dir_post`, or `dir_post` returns the
        // error before `defuse`). The success-remove path calls `defuse()` because the directory no
        // longer exists. Dry-run skips the relax entirely, so no guard.
        let mut guard: Option<RelaxedDirGuard> = None;
        if settings.dry_run.is_none() {
            let original_mode =
                crate::preserve::Metadata::permissions(handle.meta()).mode() & 0o7777;
            // relax unless the owner already has full r/w/x. read is needed to enumerate the dir
            // (open_dir + getdents), and write+execute to unlink/rmdir its entries. 0o700 = u+rwx.
            // this is a superset of the old `readonly()` (no-write) trigger, and also covers dirs
            // missing owner read/execute that the old path-based read_dir would have failed on.
            if original_mode & 0o700 != 0o700 {
                tracing::debug!("directory is not writable/traversable - relax the permissions");
                // SECURE the restore fd BEFORE relaxing: dup the O_PATH handle into a (still
                // disarmed) guard first. The dup is the only fallible step and it runs while the dir
                // still has its original restrictive mode — so if it fails (e.g. EMFILE under fd
                // exhaustion) we return without ever relaxing, and there is no more-permissive mode
                // left behind.
                let mut g = RelaxedDirGuard::prepare(handle)
                    .with_context(|| {
                        format!("failed to set up permission-restore guard for {path:?}")
                    })
                    .map_err(|err| Error::new(err, Default::default()))?;
                // gated as a Destination Chmod: the relax is a permission mutation that enables the
                // removal, so it shares the destructive side/bucket with the unlink/rmdir below.
                safedir::chmod_via_proc_fd(handle, congestion::Side::Destination, 0o700)
                    .await
                    .with_context(|| {
                        format!("failed to make {path:?} directory readable and writeable")
                    })
                    .map_err(|err| Error::new(err, Default::default()))?;
                // relax succeeded: arm so Drop restores the original mode on every exit path below.
                g.arm(original_mode);
                guard = Some(g);
            }
        }
        // open the directory's real fd via the parent fd with O_NOFOLLOW|O_DIRECTORY: a directory
        // entry swapped to a symlink mid-walk fails closed here (ELOOP/ENOTDIR) — descent never
        // follows it outside the tree. this is the core rm -rf race closure. an open failure
        // returns the error with the guard still armed in scope, so the relaxed mode is restored on
        // Drop (matching the old early-return error path).
        let dir = cx
            .parent
            .open_dir(&cx.name)
            .await
            .with_context(|| format!("failed reading directory {path:?}"))
            .map_err(|err| Error::new(err, Default::default()))?;
        Ok(DirAction::Descend {
            dir: Arc::new(dir),
            child_ctx: (),
            state: RmDirState {
                guard,
                time_metadata,
                matches_no_include,
            },
        })
    }

    async fn dir_post(
        &self,
        cx: &EntryCx,
        state: RmDirState,
        _processed: &ProcessedChildren,
        child_result: Result<Summary, Error>,
    ) -> Result<Summary, Error> {
        let prog_track = self.prog_track;
        let settings = &self.settings;
        let path = cx.real_path.as_path();
        let RmDirState {
            mut guard,
            time_metadata,
            matches_no_include,
        } = state;
        // a child failed (keep-going mode — fail-early aborts before `dir_post`). do NOT rmdir:
        // return the combined error with the partial summary, and let `guard` drop to restore the
        // relaxed mode (the old code's early-return-on-error left its local guard to do the same).
        let mut rm_summary = match child_result {
            Ok(summary) => summary,
            Err(err) => return Err(err),
        };
        tracing::debug!("finally remove the empty directory");
        let anything_removed = rm_summary.files_removed > 0
            || rm_summary.symlinks_removed > 0
            || rm_summary.directories_removed > 0;
        let anything_skipped = rm_summary.files_skipped > 0
            || rm_summary.symlinks_skipped > 0
            || rm_summary.directories_skipped > 0;
        // directories that directly match an include pattern (e.g. --include target/) should be
        // removed even if empty; only those merely traversed for matches are left intact.
        let traversed_only = !anything_removed && matches_no_include;
        // evaluate the directory's own time filter to decide whether to remove it.
        // the time filter is an entry filter, not a subtree gate: children are already handled
        // by their own recursive calls, so this decision only controls the final rmdir.
        // the metadata SNAPSHOT taken in `dir_pre` — before relaxing the mode or removing any
        // child — is used so those mutations (which bump the directory's mtime) don't change the
        // answer. this mirrors the old code's `src_metadata` captured at entry.
        let dir_passes_time_filter: bool = if let Some(ref time_filter) = settings.time_filter {
            // unwrap is safe: time_metadata is Some whenever time_filter is Some (both gated on
            // settings.time_filter.is_some()).
            let matched = time_metadata.unwrap().and_then(|md| {
                time_filter.matches(&md).map_err(|err| {
                    err.context(format!("failed evaluating time filter on {path:?}"))
                })
            });
            match matched {
                Ok(result) => match result.as_skip_reason() {
                    Some(reason) => {
                        if let Some(mode) = settings.dry_run {
                            crate::dry_run::report_time_skip(path, reason, mode, "dir");
                        }
                        false
                    }
                    None => true,
                },
                Err(err) => {
                    if settings.fail_early {
                        return Err(Error::new(err, rm_summary));
                    }
                    // log and skip — never remove a directory whose age we cannot verify.
                    // btime being unsupported on the filesystem is expected noise; downgrade
                    // to warn. anything else is unexpected and stays at error.
                    if is_unsupported_io_error(&err) {
                        tracing::warn!(
                            "time filter evaluation unsupported for dir {:?}, leaving it intact: {:#}",
                            &path,
                            &err
                        );
                    } else {
                        tracing::error!(
                            "time filter evaluation failed for dir {:?}, leaving it intact: {:#}",
                            &path,
                            &err
                        );
                    }
                    false
                }
            }
        } else {
            true
        };
        // handle dry-run mode for directories.
        // `traversed_only` catches dirs only entered to search for include pattern matches.
        // `anything_skipped` catches dirs that would still have content after partial removal.
        // `!dir_passes_time_filter` catches dirs whose own timestamps disqualify removal.
        // the real-mode path below only needs `traversed_only` and `!dir_passes_time_filter`
        // because the subsequent `rmdir_at` call handles the non-empty case via ENOTEMPTY.
        if settings.dry_run.is_some() {
            if traversed_only || anything_skipped || !dir_passes_time_filter {
                tracing::debug!(
                    "dry-run: directory {:?} would not be removed (removed={}, skipped={}, time_ok={})",
                    &path,
                    anything_removed,
                    anything_skipped,
                    dir_passes_time_filter
                );
                if !dir_passes_time_filter {
                    prog_track.directories_skipped.inc();
                    rm_summary.directories_skipped += 1;
                }
            } else {
                crate::dry_run::report_action("remove", path, None, "dir");
                rm_summary.directories_removed += 1;
            }
            return Ok(rm_summary);
        }
        // skip directories that were only traversed to look for include matches.
        // not needed for exclude-only filters or directly-matched directories.
        // non-empty directories are handled by the ENOTEMPTY check below.
        if traversed_only {
            tracing::debug!(
                "directory {:?} had nothing removed, leaving it intact",
                &path
            );
            return Ok(rm_summary);
        }
        // skip directories whose own timestamps don't satisfy the time filter.
        // children have already been processed; this only gates the dir's own removal.
        if !dir_passes_time_filter {
            tracing::debug!(
                "directory {:?} skipped by time filter, leaving it intact",
                &path
            );
            prog_track.directories_skipped.inc();
            rm_summary.directories_skipped += 1;
            return Ok(rm_summary);
        }
        // when filtering is active, directories may not be empty because we only removed
        // matching files (includes) or skipped excluded files; use rmdir (not a recursive remove)
        // so non-empty directories fail gracefully with ENOTEMPTY. the rmdir is fd-relative
        // (resolved against the parent fd) so it cannot be redirected to a different directory.
        // gated on the Destination side to match the side the path-based rm used for remove_dir.
        let any_filter_active = settings.filter.is_some() || settings.time_filter.is_some();
        match cx
            .parent
            .rmdir_at_on(&cx.name, congestion::Side::Destination)
            .await
        {
            Ok(()) => {
                prog_track.directories_removed.inc();
                rm_summary.directories_removed += 1;
                // the directory is gone, so there's nothing to restore on drop.
                if let Some(guard) = guard.as_mut() {
                    guard.defuse();
                }
            }
            Err(err) if any_filter_active => {
                // with filtering, it's expected that directories may not be empty because we only
                // removed matching files; raw_os_error 39 is ENOTEMPTY on Linux. this is not an
                // error — surface it at info so users can see which directories survived.
                if err.kind() == std::io::ErrorKind::DirectoryNotEmpty
                    || err.raw_os_error() == Some(39)
                {
                    tracing::info!(
                        "directory {:?} not empty after filtering, leaving it intact",
                        &path
                    );
                } else {
                    return Err(Error::new(
                        anyhow!(err).context(format!("failed removing directory {:?}", &path)),
                        rm_summary,
                    ));
                }
            }
            Err(err) => {
                return Err(Error::new(
                    anyhow!(err).context(format!("failed removing directory {:?}", &path)),
                    rm_summary,
                ));
            }
        }
        Ok(rm_summary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DryRunMode;
    use crate::testutils;
    use tracing_test::traced_test;

    static PROGRESS: std::sync::LazyLock<progress::Progress> =
        std::sync::LazyLock::new(progress::Progress::new);

    #[tokio::test]
    #[traced_test]
    async fn no_write_permission() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let filepaths = vec![
            test_path.join("foo").join("0.txt"),
            test_path.join("foo").join("bar").join("2.txt"),
            test_path.join("foo").join("baz").join("4.txt"),
            test_path.join("foo").join("baz"),
        ];
        for fpath in &filepaths {
            // change file permissions to not readable and not writable
            tokio::fs::set_permissions(&fpath, std::fs::Permissions::from_mode(0o555)).await?;
        }
        let summary = rm(
            &PROGRESS,
            &test_path.join("foo"),
            &Settings {
                fail_early: false,
                filter: None,
                dry_run: None,
                time_filter: None,
            },
        )
        .await?;
        assert!(!test_path.join("foo").exists());
        assert_eq!(summary.files_removed, 5);
        assert_eq!(summary.symlinks_removed, 2);
        assert_eq!(summary.directories_removed, 3);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn relaxed_dir_mode_restored_on_error_exit() -> Result<(), anyhow::Error> {
        // Regression: when rm_internal chmod-relaxes a read-only directory to 0o777 to clear
        // its contents, it must restore the original mode on ERROR paths too — not just on the
        // retain paths (traversed-only, time-filtered skip, ENOTEMPTY under filter). Without
        // that, a partial failure leaves the directory world-writable.
        //
        // We trigger the remove_dir error path by making the dst's PARENT non-writable: rm can
        // chmod-relax dst and successfully unlink its contents (write on dst is granted by the
        // relax), but the final remove_dir(dst) needs write permission on the parent, which it
        // doesn't have → EACCES. The fix's inline restore at that error site must put dst back
        // to 0o555 before propagating.
        let tmp = tempfile::tempdir()?;
        let parent = tmp.path().join("parent");
        let dst = parent.join("dst");
        tokio::fs::create_dir(&parent).await?;
        tokio::fs::create_dir(&dst).await?;
        tokio::fs::write(dst.join("inside.txt"), b"x").await?;
        // dst is read-only → rm relaxes it.
        tokio::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o555)).await?;
        // parent is read-only (still traversable via the execute bit) → remove_dir(dst) fails.
        tokio::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o555)).await?;

        let result = rm(
            &PROGRESS,
            &dst,
            &Settings {
                fail_early: false,
                filter: None,
                dry_run: None,
                time_filter: None,
            },
        )
        .await;

        // restore parent writability so we can stat dst and clean up regardless of the assertion.
        tokio::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).await?;

        assert!(
            result.is_err(),
            "rm must fail when its dst can be emptied but the parent dir blocks remove_dir"
        );
        let mode = tokio::fs::metadata(&dst).await?.permissions().mode() & 0o7777;
        assert_eq!(
            mode, 0o555,
            "relaxed-then-erroring directory must be restored to its original mode (got {mode:o}o); leaving it 0o777 leaks permissions on partial failure"
        );

        // cleanup
        tokio::fs::set_permissions(&dst, std::fs::Permissions::from_mode(0o755)).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn parent_dir_no_write_permission() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        // make parent directory read-only (no write permission)
        tokio::fs::set_permissions(
            &test_path.join("foo").join("bar"),
            std::fs::Permissions::from_mode(0o555),
        )
        .await?;
        let result = rm(
            &PROGRESS,
            &test_path.join("foo").join("bar").join("2.txt"),
            &Settings {
                fail_early: true,
                filter: None,
                dry_run: None,
                time_filter: None,
            },
        )
        .await;
        // should fail with permission denied error
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_string = format!("{:#}", err);
        // verify the error chain includes "Permission denied"
        assert!(
            err_string.contains("Permission denied") || err_string.contains("permission denied"),
            "Error should contain 'Permission denied' but got: {}",
            err_string
        );
        Ok(())
    }

    mod filter_tests {
        use super::*;
        use crate::filter::FilterSettings;
        /// Test that path-based patterns (with /) work correctly with nested paths.
        #[tokio::test]
        #[traced_test]
        async fn test_path_pattern_matches_nested_files() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // create filter that should only remove files in bar/ directory
            let mut filter = FilterSettings::new();
            filter.add_include("bar/*.txt").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path.join("foo"),
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // should only remove files matching bar/*.txt pattern (bar/1.txt, bar/2.txt, bar/3.txt)
            assert_eq!(
                summary.files_removed, 3,
                "should remove 3 files matching bar/*.txt"
            );
            // each file is 1 byte ("1", "2", "3")
            assert_eq!(summary.bytes_removed, 3, "should report 3 bytes removed");
            // verify the right files were removed
            assert!(
                !test_path.join("foo/bar/1.txt").exists(),
                "bar/1.txt should be removed"
            );
            assert!(
                !test_path.join("foo/bar/2.txt").exists(),
                "bar/2.txt should be removed"
            );
            assert!(
                !test_path.join("foo/bar/3.txt").exists(),
                "bar/3.txt should be removed"
            );
            // verify files outside the pattern still exist
            assert!(
                test_path.join("foo/0.txt").exists(),
                "0.txt should still exist"
            );
            Ok(())
        }
        /// Test that filters are applied to top-level file arguments.
        #[tokio::test]
        #[traced_test]
        async fn test_filter_applies_to_single_file_source() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // create filter that excludes .txt files
            let mut filter = FilterSettings::new();
            filter.add_exclude("*.txt").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path.join("foo/0.txt"), // single file source
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // the file should NOT be removed because it matches the exclude pattern
            assert_eq!(
                summary.files_removed, 0,
                "file matching exclude pattern should not be removed"
            );
            assert!(
                test_path.join("foo/0.txt").exists(),
                "excluded file should still exist"
            );
            Ok(())
        }
        /// Test that filters apply to root directories with simple exclude patterns.
        #[tokio::test]
        #[traced_test]
        async fn test_filter_applies_to_root_directory() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create a directory that should be excluded
            tokio::fs::create_dir_all(test_path.join("excluded_dir")).await?;
            tokio::fs::write(test_path.join("excluded_dir/file.txt"), "content").await?;
            // create filter that excludes *_dir/ directories
            let mut filter = FilterSettings::new();
            filter.add_exclude("*_dir/").unwrap();
            let result = rm(
                &PROGRESS,
                &test_path.join("excluded_dir"),
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // directory should NOT be removed because it matches exclude pattern
            assert_eq!(
                result.directories_removed, 0,
                "root directory matching exclude should not be removed"
            );
            assert!(
                test_path.join("excluded_dir").exists(),
                "excluded root directory should still exist"
            );
            Ok(())
        }
        /// Test that filters apply to root symlinks with simple exclude patterns.
        #[tokio::test]
        #[traced_test]
        async fn test_filter_applies_to_root_symlink() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create a target file and a symlink to it
            tokio::fs::write(test_path.join("target.txt"), "content").await?;
            tokio::fs::symlink(
                test_path.join("target.txt"),
                test_path.join("excluded_link"),
            )
            .await?;
            // create filter that excludes *_link
            let mut filter = FilterSettings::new();
            filter.add_exclude("*_link").unwrap();
            let result = rm(
                &PROGRESS,
                &test_path.join("excluded_link"),
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // symlink should NOT be removed because it matches exclude pattern
            assert_eq!(
                result.symlinks_removed, 0,
                "root symlink matching exclude should not be removed"
            );
            assert!(
                test_path.join("excluded_link").exists(),
                "excluded root symlink should still exist"
            );
            Ok(())
        }
        /// Test combined include and exclude patterns (exclude takes precedence).
        #[tokio::test]
        #[traced_test]
        async fn test_combined_include_exclude_patterns() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // test structure from setup_test_dir:
            // foo/
            //   0.txt
            //   bar/ (1.txt, 2.txt, 3.txt)
            //   baz/ (4.txt, 5.txt symlink, 6.txt symlink)
            // include all .txt files in bar/, but exclude 2.txt specifically
            let mut filter = FilterSettings::new();
            filter.add_include("bar/*.txt").unwrap();
            filter.add_exclude("bar/2.txt").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path.join("foo"),
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // should remove: bar/1.txt, bar/3.txt = 2 files
            // should skip: bar/2.txt (excluded by pattern), 0.txt (excluded by default - no match) = 2 files
            assert_eq!(summary.files_removed, 2, "should remove 2 files");
            assert_eq!(
                summary.files_skipped, 2,
                "should skip 2 files (bar/2.txt excluded, 0.txt no match)"
            );
            // verify
            assert!(
                !test_path.join("foo/bar/1.txt").exists(),
                "bar/1.txt should be removed"
            );
            assert!(
                test_path.join("foo/bar/2.txt").exists(),
                "bar/2.txt should be excluded"
            );
            assert!(
                !test_path.join("foo/bar/3.txt").exists(),
                "bar/3.txt should be removed"
            );
            Ok(())
        }
        /// Test that skipped counts accurately reflect what was filtered.
        #[tokio::test]
        #[traced_test]
        async fn test_skipped_counts_comprehensive() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // test structure from setup_test_dir:
            // foo/
            //   0.txt
            //   bar/ (1.txt, 2.txt, 3.txt)
            //   baz/ (4.txt, 5.txt symlink, 6.txt symlink)
            // exclude bar/ directory entirely
            let mut filter = FilterSettings::new();
            filter.add_exclude("bar/").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path.join("foo"),
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // removed: 0.txt, baz/4.txt = 2 files
            // removed: baz/5.txt symlink, baz/6.txt symlink = 2 symlinks
            // removed: baz = 1 directory (foo cannot be removed because bar still exists)
            // skipped: bar directory (1 dir) - contents not counted since whole dir skipped
            assert_eq!(summary.files_removed, 2, "should remove 2 files");
            assert_eq!(summary.symlinks_removed, 2, "should remove 2 symlinks");
            assert_eq!(
                summary.directories_removed, 1,
                "should remove 1 directory (baz only, foo not empty)"
            );
            assert_eq!(
                summary.directories_skipped, 1,
                "should skip 1 directory (bar)"
            );
            // bar should still exist
            assert!(
                test_path.join("foo/bar").exists(),
                "bar directory should still exist"
            );
            // foo should still exist (not empty because bar is still there)
            assert!(
                test_path.join("foo").exists(),
                "foo directory should still exist (contains bar)"
            );
            Ok(())
        }
        /// Test that empty directories are not removed when they were only traversed to look
        /// for matches (regression test for bug where --include='foo' would remove empty dir baz).
        #[tokio::test]
        #[traced_test]
        async fn test_empty_dir_not_removed_when_only_traversed() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // test/
            //   foo (file)
            //   bar (file)
            //   baz/ (empty directory)
            tokio::fs::write(test_path.join("foo"), "content").await?;
            tokio::fs::write(test_path.join("bar"), "content").await?;
            tokio::fs::create_dir(test_path.join("baz")).await?;
            // include only 'foo' file
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // only 'foo' should be removed
            assert_eq!(summary.files_removed, 1, "should remove only 'foo' file");
            assert_eq!(
                summary.directories_removed, 0,
                "should NOT remove empty 'baz' directory"
            );
            // verify foo was removed
            assert!(!test_path.join("foo").exists(), "foo should be removed");
            // verify bar still exists (not matching include pattern)
            assert!(test_path.join("bar").exists(), "bar should still exist");
            // verify empty baz directory still exists
            assert!(
                test_path.join("baz").exists(),
                "empty baz directory should NOT be removed"
            );
            Ok(())
        }
        /// Test that empty directories ARE removed with exclude-only filters.
        /// Unlike include filters (where empty dirs are only traversed for matches),
        /// exclude-only filters should not prevent removal of empty directories.
        #[tokio::test]
        #[traced_test]
        async fn test_exclude_only_removes_empty_directory() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // test/
            //   foo (file)
            //   bar.log (file)
            //   baz/ (empty directory)
            tokio::fs::write(test_path.join("foo"), "content").await?;
            tokio::fs::write(test_path.join("bar.log"), "content").await?;
            tokio::fs::create_dir(test_path.join("baz")).await?;
            // exclude only .log files
            let mut filter = FilterSettings::new();
            filter.add_exclude("*.log").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            // foo should be removed, bar.log should be skipped, baz/ should be removed
            assert_eq!(summary.files_removed, 1, "should remove 'foo'");
            assert_eq!(summary.files_skipped, 1, "should skip 'bar.log'");
            assert_eq!(
                summary.directories_removed, 1,
                "should remove empty 'baz' directory"
            );
            assert!(!test_path.join("foo").exists(), "foo should be removed");
            assert!(
                test_path.join("bar.log").exists(),
                "bar.log should still exist"
            );
            assert!(
                !test_path.join("baz").exists(),
                "empty baz directory should be removed"
            );
            Ok(())
        }
        /// Test that empty directories are not removed in dry-run mode when only traversed.
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_empty_dir_not_reported_as_removed() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // test/
            //   foo (file)
            //   bar (file)
            //   baz/ (empty directory)
            tokio::fs::write(test_path.join("foo"), "content").await?;
            tokio::fs::write(test_path.join("bar"), "content").await?;
            tokio::fs::create_dir(test_path.join("baz")).await?;
            // include only 'foo' file
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: Some(DryRunMode::Explain),
                    time_filter: None,
                },
            )
            .await?;
            // only 'foo' should be reported as would-be-removed
            assert_eq!(
                summary.files_removed, 1,
                "should report only 'foo' would be removed"
            );
            assert_eq!(
                summary.directories_removed, 0,
                "should NOT report empty 'baz' would be removed"
            );
            // verify nothing was actually removed (dry-run mode)
            assert!(test_path.join("foo").exists(), "foo should still exist");
            assert!(test_path.join("bar").exists(), "bar should still exist");
            assert!(test_path.join("baz").exists(), "baz should still exist");
            Ok(())
        }
        /// Test that an empty directory directly matching an include pattern IS removed.
        /// Unlike traversed-only directories, directly matched ones are explicit targets.
        #[tokio::test]
        #[traced_test]
        async fn test_include_directly_matched_empty_dir_is_removed() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // test/
            //   foo (file)
            //   baz/ (empty directory)
            tokio::fs::write(test_path.join("foo"), "content").await?;
            tokio::fs::create_dir(test_path.join("baz")).await?;
            // include pattern that directly matches the directory
            let mut filter = FilterSettings::new();
            filter.add_include("baz/").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            assert_eq!(
                summary.directories_removed, 1,
                "should remove directly matched empty 'baz' directory"
            );
            assert_eq!(summary.files_removed, 0, "should not remove 'foo'");
            assert!(test_path.join("foo").exists(), "foo should still exist");
            assert!(
                !test_path.join("baz").exists(),
                "directly matched empty baz directory should be removed"
            );
            Ok(())
        }
    }
    mod dry_run_tests {
        use super::*;
        use crate::filter::FilterSettings;
        /// Test that dry-run mode doesn't modify permissions on read-only directories.
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_preserves_readonly_permissions() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            let readonly_dir = test_path.join("foo/bar");
            // make the directory read-only
            tokio::fs::set_permissions(&readonly_dir, std::fs::Permissions::from_mode(0o555))
                .await?;
            // verify it's read-only
            let before_mode = tokio::fs::metadata(&readonly_dir)
                .await?
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(
                before_mode, 0o555,
                "directory should be read-only before dry-run"
            );
            let summary = rm(
                &PROGRESS,
                &readonly_dir,
                &Settings {
                    fail_early: false,
                    filter: None,
                    dry_run: Some(DryRunMode::Brief),
                    time_filter: None,
                },
            )
            .await?;
            // verify the directory still exists (dry-run shouldn't remove it)
            assert!(
                readonly_dir.exists(),
                "directory should still exist after dry-run"
            );
            // verify permissions weren't changed
            let after_mode = tokio::fs::metadata(&readonly_dir)
                .await?
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(
                after_mode, 0o555,
                "dry-run should not modify directory permissions"
            );
            // verify summary shows what would be removed
            assert!(
                summary.directories_removed > 0 || summary.files_removed > 0,
                "dry-run should report what would be removed"
            );
            Ok(())
        }
        /// Test that dry-run mode with filtering correctly handles directories that
        /// wouldn't be empty after filtering.
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_with_filter_non_empty_directory() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // test structure from setup_test_dir:
            // foo/
            //   0.txt
            //   bar/ (1.txt, 2.txt, 3.txt)
            //   baz/ (4.txt, 5.txt symlink, 6.txt symlink)
            // exclude bar/ - so foo would not be empty after removing (bar still there)
            let mut filter = crate::filter::FilterSettings::new();
            filter.add_exclude("bar/").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path.join("foo"),
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: Some(DryRunMode::Brief),
                    time_filter: None,
                },
            )
            .await?;
            // dry-run shouldn't actually remove anything
            assert!(
                test_path.join("foo").exists(),
                "foo should still exist after dry-run"
            );
            // verify summary reflects what WOULD happen:
            // - files: 0.txt, baz/4.txt would be removed = 2
            // - symlinks: baz/5.txt, baz/6.txt would be removed = 2
            // - directories: baz would be removed, but NOT foo (bar is skipped, so foo not empty)
            // - skipped: bar directory = 1
            assert_eq!(
                summary.files_removed, 2,
                "should report 2 files would be removed"
            );
            assert_eq!(
                summary.symlinks_removed, 2,
                "should report 2 symlinks would be removed"
            );
            assert_eq!(
                summary.directories_removed, 1,
                "should report only baz (not foo) would be removed"
            );
            assert_eq!(
                summary.directories_skipped, 1,
                "should report bar directory skipped"
            );
            Ok(())
        }
        /// Test that dry-run with exclude-only filter correctly reports empty directories
        /// as would-be-removed (unlike include filters where empty dirs are only traversed).
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_exclude_only_reports_empty_dir_removed() -> Result<(), anyhow::Error>
        {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // test/
            //   foo (file)
            //   bar.log (file)
            //   baz/ (empty directory)
            tokio::fs::write(test_path.join("foo"), "content").await?;
            tokio::fs::write(test_path.join("bar.log"), "content").await?;
            tokio::fs::create_dir(test_path.join("baz")).await?;
            // exclude only .log files
            let mut filter = FilterSettings::new();
            filter.add_exclude("*.log").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: Some(DryRunMode::Explain),
                    time_filter: None,
                },
            )
            .await?;
            // foo should be reported as would-be-removed, bar.log skipped, baz/ removed
            assert_eq!(
                summary.files_removed, 1,
                "should report 'foo' would be removed"
            );
            assert_eq!(
                summary.files_skipped, 1,
                "should report 'bar.log' would be skipped"
            );
            assert_eq!(
                summary.directories_removed, 1,
                "should report empty 'baz' directory would be removed"
            );
            // verify nothing was actually removed (dry-run mode)
            assert!(test_path.join("foo").exists(), "foo should still exist");
            assert!(
                test_path.join("bar.log").exists(),
                "bar.log should still exist"
            );
            assert!(test_path.join("baz").exists(), "baz should still exist");
            Ok(())
        }
        /// Test that dry-run correctly reports removal of an empty directory that directly
        /// matches an include pattern (not merely traversed).
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_include_directly_matched_empty_dir_reported()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // test/
            //   foo (file)
            //   baz/ (empty directory)
            tokio::fs::write(test_path.join("foo"), "content").await?;
            tokio::fs::create_dir(test_path.join("baz")).await?;
            // include pattern that directly matches the directory
            let mut filter = FilterSettings::new();
            filter.add_include("baz/").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    dry_run: Some(DryRunMode::Explain),
                    time_filter: None,
                },
            )
            .await?;
            assert_eq!(
                summary.directories_removed, 1,
                "should report directly matched empty 'baz' would be removed"
            );
            assert_eq!(summary.files_removed, 0, "should not report 'foo'");
            // verify nothing was actually removed (dry-run mode)
            assert!(test_path.join("foo").exists(), "foo should still exist");
            assert!(test_path.join("baz").exists(), "baz should still exist");
            Ok(())
        }
    }
    mod time_filter_tests {
        use super::*;
        use crate::filter::TimeFilter;

        fn set_mtime_age(path: &std::path::Path, age: std::time::Duration) -> anyhow::Result<()> {
            let past = filetime::FileTime::from_system_time(std::time::SystemTime::now() - age);
            filetime::set_file_mtime(path, past)?;
            Ok(())
        }

        /// File with mtime older than threshold is removed.
        #[tokio::test]
        #[traced_test]
        async fn removes_files_older_than_modified_before() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let file = test_path.join("old.txt");
            tokio::fs::write(&file, "x").await?;
            set_mtime_age(&file, std::time::Duration::from_secs(7200))?;
            // age test_path so the root dir passes its own time filter check
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            assert_eq!(summary.files_removed, 1, "old file should be removed");
            assert_eq!(summary.files_skipped, 0);
            assert!(!file.exists(), "old.txt should be removed");
            Ok(())
        }

        /// File with mtime newer than threshold is skipped.
        #[tokio::test]
        #[traced_test]
        async fn keeps_files_newer_than_modified_before() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let file = test_path.join("new.txt");
            tokio::fs::write(&file, "x").await?;
            set_mtime_age(&file, std::time::Duration::from_secs(60))?;
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            assert_eq!(summary.files_removed, 0, "new file should not be removed");
            assert_eq!(summary.files_skipped, 1, "new file should be skipped");
            assert!(file.exists(), "new.txt should still exist");
            Ok(())
        }

        /// A fresh subdirectory is descended into (children are handled individually),
        /// but the fresh_dir itself is not removed because its own mtime is too recent.
        #[tokio::test]
        #[traced_test]
        async fn fresh_subdirectory_is_descended_but_not_removed() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let old_file = test_path.join("old.txt");
            let fresh_dir = test_path.join("fresh_dir");
            let fresh_child = fresh_dir.join("fresh_child.txt");
            let old_child = fresh_dir.join("old_child.txt");
            tokio::fs::write(&old_file, "x").await?;
            tokio::fs::create_dir(&fresh_dir).await?;
            tokio::fs::write(&fresh_child, "x").await?;
            tokio::fs::write(&old_child, "x").await?;
            set_mtime_age(&old_file, std::time::Duration::from_secs(7200))?;
            set_mtime_age(&old_child, std::time::Duration::from_secs(7200))?;
            // fresh_child keeps its recent mtime; so does fresh_dir (we took the mtime
            // snapshot before remove_file mutates it)
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            // we descend into fresh_dir: old_child removed, fresh_child skipped
            assert_eq!(summary.files_removed, 2, "old.txt and old_child removed");
            assert_eq!(
                summary.files_skipped, 1,
                "fresh_child skipped inside fresh_dir"
            );
            assert_eq!(
                summary.directories_skipped, 1,
                "fresh_dir itself is skipped at removal time"
            );
            assert_eq!(
                summary.directories_removed, 0,
                "root survives because fresh_dir is still inside it"
            );
            assert!(!old_file.exists());
            assert!(!old_child.exists(), "old_child inside fresh_dir removed");
            assert!(
                fresh_dir.exists(),
                "fresh_dir kept despite its old child being removed"
            );
            assert!(fresh_child.exists(), "fresh_child inside fresh_dir kept");
            Ok(())
        }

        /// An old directory that still holds a new (skipped) file survives as non-empty.
        /// The leftover-dir case is not treated as an error.
        #[tokio::test]
        #[traced_test]
        async fn old_dir_with_new_file_leaves_non_empty_dir_without_error()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let old_dir = test_path.join("old_dir");
            tokio::fs::create_dir(&old_dir).await?;
            let new_file = old_dir.join("new.txt");
            tokio::fs::write(&new_file, "x").await?;
            set_mtime_age(&new_file, std::time::Duration::from_secs(60))?;
            set_mtime_age(&old_dir, std::time::Duration::from_secs(7200))?;
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let result = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await;
            let summary = result.expect("ENOTEMPTY should not surface as an error");
            assert_eq!(summary.files_skipped, 1, "new file should be skipped");
            assert_eq!(
                summary.directories_removed, 0,
                "old_dir cannot be removed while new.txt remains"
            );
            assert!(old_dir.exists(), "old_dir should still exist");
            assert!(new_file.exists(), "new.txt should still exist");
            // the 'left intact' message is logged at info level
            assert!(
                logs_contain("not empty after filtering, leaving it intact"),
                "should log ENOTEMPTY case at info"
            );
            Ok(())
        }

        /// An old, already-empty directory is removed by the time filter run.
        #[tokio::test]
        #[traced_test]
        async fn old_empty_directory_is_removed() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let old_empty = test_path.join("old_empty");
            tokio::fs::create_dir(&old_empty).await?;
            set_mtime_age(&old_empty, std::time::Duration::from_secs(7200))?;
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            // both old_empty and test_path itself are removed
            assert_eq!(summary.directories_removed, 2);
            assert!(!old_empty.exists());
            assert!(!test_path.exists());
            Ok(())
        }

        /// Time filter combines with glob exclude — both must pass for removal.
        #[tokio::test]
        #[traced_test]
        async fn time_filter_combines_with_glob_exclude() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let old_keep = test_path.join("keep.log");
            let old_drop = test_path.join("drop.txt");
            let new_drop = test_path.join("recent.txt");
            tokio::fs::write(&old_keep, "x").await?;
            tokio::fs::write(&old_drop, "x").await?;
            tokio::fs::write(&new_drop, "x").await?;
            set_mtime_age(&old_keep, std::time::Duration::from_secs(7200))?;
            set_mtime_age(&old_drop, std::time::Duration::from_secs(7200))?;
            set_mtime_age(&new_drop, std::time::Duration::from_secs(60))?;
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let mut filter = crate::filter::FilterSettings::new();
            filter.add_exclude("*.log").unwrap();
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: Some(filter),
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            // only old_drop passes both filters
            assert_eq!(summary.files_removed, 1, "only old_drop should be removed");
            assert_eq!(
                summary.files_skipped, 2,
                "old_keep and recent_drop should be skipped"
            );
            assert!(
                old_keep.exists(),
                "keep.log excluded by glob, should remain"
            );
            assert!(!old_drop.exists(), "drop.txt should be removed");
            assert!(new_drop.exists(), "recent.txt should remain (too new)");
            Ok(())
        }

        /// Dry-run with time filter previews removal without modifying files.
        #[tokio::test]
        #[traced_test]
        async fn time_filter_with_dry_run() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let old_file = test_path.join("old.txt");
            let new_file = test_path.join("new.txt");
            tokio::fs::write(&old_file, "x").await?;
            tokio::fs::write(&new_file, "x").await?;
            set_mtime_age(&old_file, std::time::Duration::from_secs(7200))?;
            set_mtime_age(&new_file, std::time::Duration::from_secs(60))?;
            set_mtime_age(&test_path, std::time::Duration::from_secs(7200))?;
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: Some(DryRunMode::Explain),
                },
            )
            .await?;
            assert_eq!(
                summary.files_removed, 1,
                "should report old file would be removed"
            );
            assert_eq!(
                summary.files_skipped, 1,
                "should report new file would be skipped"
            );
            assert!(old_file.exists(), "old.txt should still exist (dry-run)");
            assert!(new_file.exists(), "new.txt should still exist (dry-run)");
            Ok(())
        }

        /// A fresh top-level directory is traversed (its old children are removed),
        /// but the root itself is not removed because its own mtime is too recent.
        #[tokio::test]
        #[traced_test]
        async fn fresh_top_level_directory_is_traversed_but_not_removed()
        -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let old_inside = test_path.join("old.txt");
            tokio::fs::write(&old_inside, "x").await?;
            set_mtime_age(&old_inside, std::time::Duration::from_secs(7200))?;
            // test_path itself is left fresh (recent mtime)
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            assert_eq!(
                summary.files_removed, 1,
                "old child should be removed despite fresh parent"
            );
            assert_eq!(
                summary.directories_skipped, 1,
                "fresh root itself is skipped at removal time"
            );
            assert_eq!(
                summary.directories_removed, 0,
                "fresh root must not be removed"
            );
            assert!(test_path.exists(), "fresh root should still exist");
            assert!(!old_inside.exists(), "old child should be gone");
            Ok(())
        }

        /// Time filter on a single-file root argument increments skip when too new.
        #[tokio::test]
        #[traced_test]
        async fn time_filter_on_root_file_argument() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let new_file = test_path.join("new.txt");
            tokio::fs::write(&new_file, "x").await?;
            set_mtime_age(&new_file, std::time::Duration::from_secs(60))?;
            let summary = rm(
                &PROGRESS,
                &new_file,
                &Settings {
                    fail_early: false,
                    filter: None,
                    time_filter: Some(TimeFilter {
                        modified_before: Some(std::time::Duration::from_secs(3600)),
                        created_before: None,
                    }),
                    dry_run: None,
                },
            )
            .await?;
            assert_eq!(summary.files_removed, 0);
            assert_eq!(
                summary.files_skipped, 1,
                "root file too new should be skipped"
            );
            assert!(new_file.exists(), "root file should still exist");
            Ok(())
        }
    }

    /// Stress tests exercising max-open-files saturation during rm.
    mod max_open_files_tests {
        use super::*;

        /// wide rm: many files with a very low open-files limit.
        /// verifies all files are removed correctly under permit saturation.
        #[tokio::test]
        #[traced_test]
        async fn wide_rm_under_open_files_saturation() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let file_count = 200;
            for i in 0..file_count {
                tokio::fs::write(
                    test_path.join(format!("{}.txt", i)),
                    format!("content-{}", i),
                )
                .await?;
            }
            // set a very low limit to force permit contention
            throttle::set_max_open_files(4);
            let summary = rm(
                &PROGRESS,
                &test_path,
                &Settings {
                    fail_early: true,
                    filter: None,
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?;
            assert_eq!(summary.files_removed, file_count);
            assert_eq!(summary.directories_removed, 1);
            assert!(!test_path.exists());
            Ok(())
        }

        /// deep + wide rm: directory tree deeper than the open-files limit, with files
        /// at every level. verifies no deadlock occurs (directories don't consume permits).
        #[tokio::test]
        #[traced_test]
        async fn deep_tree_no_deadlock_under_open_files_saturation() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            let depth = 20;
            let files_per_level = 5;
            let limit = 4;
            // create a directory chain deeper than the permit limit, with files at each level
            let mut dir = test_path.clone();
            for level in 0..depth {
                tokio::fs::create_dir_all(&dir).await?;
                for f in 0..files_per_level {
                    tokio::fs::write(
                        dir.join(format!("f{}_{}.txt", level, f)),
                        format!("L{}F{}", level, f),
                    )
                    .await?;
                }
                dir = dir.join(format!("d{}", level));
            }
            throttle::set_max_open_files(limit);
            let summary = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                rm(
                    &PROGRESS,
                    &test_path,
                    &Settings {
                        fail_early: true,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                ),
            )
            .await
            .context("rm timed out — possible deadlock")?
            .context("rm failed")?;
            assert_eq!(summary.files_removed, depth * files_per_level);
            assert_eq!(summary.directories_removed, depth);
            assert!(!test_path.exists());
            Ok(())
        }

        /// Locks down the boolean used at the rm spawn site to decide whether
        /// to pre-acquire a pending-meta permit. A naive `entry_is_dir = false
        /// ⇒ pre-acquire` policy treats unknown-typed entries (when
        /// `DirEntry::file_type()` fails) as leaves, so the spawned task
        /// holds the permit even if the entry is actually a directory and
        /// recurses. A chain of such entries can deadlock the pool. The
        /// safer pattern — `pre-acquire iff positively-known-not-directory`
        /// — keeps the predicate `false` for unknown types.
        #[test]
        fn pre_acquire_skips_unknown_filetype() -> Result<(), anyhow::Error> {
            let tmp = std::env::temp_dir().join(format!(
                "rcp_pre_acquire_test_{}_{}",
                std::process::id(),
                rand::random::<u64>()
            ));
            std::fs::create_dir(&tmp)?;
            let dir_path = tmp.join("d");
            std::fs::create_dir(&dir_path)?;
            let file_path = tmp.join("f");
            std::fs::write(&file_path, "x")?;
            let dir_ft = std::fs::metadata(&dir_path)?.file_type();
            let file_ft = std::fs::metadata(&file_path)?.file_type();
            // The exact predicate used in the rm spawn site:
            let known_leaf =
                |ft: Option<std::fs::FileType>| ft.as_ref().is_some_and(|t| !t.is_dir());
            assert!(!known_leaf(None), "unknown filetype must skip pre-acquire");
            assert!(!known_leaf(Some(dir_ft)), "directory must skip pre-acquire");
            assert!(known_leaf(Some(file_ft)), "regular file must pre-acquire");
            std::fs::remove_dir_all(&tmp).ok();
            Ok(())
        }

        /// Regression for the hold-and-wait deadlock when a getdents leaf-hint entry is actually a
        /// directory (the DT_UNKNOWN edge, or a swap between `getdents` and the authoritative
        /// `child()`). The walk pre-acquires a `pending_meta` permit for hinted leaves only; if such
        /// an entry turns out to be a directory, the spawned task must DROP that permit before
        /// recursing — otherwise it holds the permit AND its children block trying to acquire one,
        /// and with a saturated pool the whole walk hangs.
        ///
        /// This reproduces that exact shape deterministically by driving the `RmVisitor` through the
        /// driver's [`process_entry`] with the one-and-only permit pre-acquired by the caller
        /// (mirroring the spawn loop's hinted-leaf pre-acquire). With the bug, the directory branch
        /// would hold that permit while recursing into the directory, whose child file then blocks
        /// forever on `pending_meta` (pool size 1, already held by us) — the timeout fires. With the
        /// fix, the driver's single drop-before-recurse site releases the permit on the directory
        /// path before recursing, the child acquires it, and removal completes well within the
        /// timeout.
        #[tokio::test]
        #[traced_test]
        async fn hinted_leaf_that_is_dir_drops_permit_before_recursion() -> anyhow::Result<()> {
            let root = testutils::create_temp_dir().await?;
            // `d` is a directory (the authoritative type) holding one child file `c`.
            let dir_path = root.join("d");
            tokio::fs::create_dir(&dir_path).await?;
            tokio::fs::write(dir_path.join("c"), b"x").await?;
            // size the pending-meta pool to a single permit so a held-across-recursion permit
            // strands the child's pre-acquire — the saturation the fd-walk must tolerate.
            throttle::set_max_open_files(1);
            // open the container of `d` and classify `d` itself: an authoritative directory.
            let parent = Dir::open_parent_dir(&root, congestion::Side::Source)
                .await
                .context("open parent dir")?;
            let parent = Arc::new(parent.into_tree());
            let name = std::ffi::OsStr::new("d");
            let handle = parent.child(name).await.context("classify d")?;
            assert_eq!(
                handle.kind(),
                EntryKind::Dir,
                "fixture `d` must be a directory"
            );
            drop(handle);
            let settings = Settings {
                fail_early: true,
                filter: None,
                dry_run: None,
                time_filter: None,
            };
            let visitor = Arc::new(RmVisitor {
                prog_track: &PROGRESS,
                settings: settings.clone(),
            });
            let cx = EntryCx {
                parent: Arc::clone(&parent),
                name: name.to_owned(),
                rel_path: std::path::PathBuf::new(),
                filter_path: std::path::PathBuf::new(),
                real_path: dir_path.clone(),
                dry_run: false,
                prog_track: &PROGRESS,
            };
            // pre-acquire the single permit exactly as the spawn loop does for a hinted leaf, and
            // hand it to `process_entry`. The fix drops it before recursing into the directory.
            let permit = crate::walk::preacquire_leaf_permit(
                PermitKind::PendingMeta,
                Some(EntryKind::File),
                |_| true,
            )
            .await;
            assert!(permit.is_some(), "the pre-acquire must take the one permit");
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(20),
                process_entry(visitor, cx, (), permit),
            )
            .await;
            // restore the default (disabled) pool before asserting so a failure here can't strand
            // the tiny limit for any concurrent test (the serial group already isolates us, but
            // this keeps the process-global knob clean on the failure path too).
            throttle::set_max_open_files(0);
            let summary = result
                .context(
                    "process_entry hung — leaf permit held across directory recursion (deadlock)",
                )?
                .map_err(|e| e.source)?;
            assert_eq!(summary.files_removed, 1, "child file should be removed");
            assert_eq!(
                summary.directories_removed, 1,
                "directory should be removed"
            );
            assert!(!dir_path.exists(), "directory `d` should be gone");
            Ok(())
        }
    }

    /// TOCTOU race tests for the fd-based recursive removal.
    mod race_tests {
        use super::*;

        static RACE_PROGRESS: std::sync::LazyLock<progress::Progress> =
            std::sync::LazyLock::new(progress::Progress::new);

        /// Repeatedly swap `tree/sub` between a real directory (holding a real file) and a symlink
        /// to an OUT-OF-TREE sentinel directory, using rename so each individual state is atomic.
        /// Two staging names live alongside `sub` and are renamed over it in a tight loop until
        /// `stop` is set. Runs on a dedicated OS thread so it makes progress regardless of the
        /// tokio runtime's scheduling. Mirrors copy's `intermediate_dir_swap_never_follows_symlink`
        /// swapper.
        fn spawn_dir_symlink_swapper(
            tree: std::path::PathBuf,
            sentinel: std::path::PathBuf,
            stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
        ) -> std::thread::JoinHandle<()> {
            std::thread::spawn(move || {
                let sub = tree.join("sub");
                let staged_dir = tree.join("__staged_sub_dir");
                let staged_link = tree.join("__staged_sub_link");
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // stage a real directory (with a real file) then swap it in over `sub`.
                    let _ = std::fs::remove_dir_all(&staged_dir);
                    if std::fs::create_dir(&staged_dir).is_ok() {
                        let _ = std::fs::write(staged_dir.join("real.txt"), b"REAL");
                        // RENAME_EXCHANGE isn't portable here; remove-then-rename. The window where
                        // `sub` is briefly absent is fine — rm may error, an accepted failed-closed
                        // outcome. (rm must still never touch the out-of-tree sentinel.)
                        let _ = std::fs::remove_dir_all(&sub);
                        let _ = std::fs::remove_file(&sub);
                        let _ = std::fs::rename(&staged_dir, &sub);
                    }
                    // stage a symlink to the out-of-tree sentinel dir, then swap it in over `sub`.
                    let _ = std::fs::remove_file(&staged_link);
                    if std::os::unix::fs::symlink(&sentinel, &staged_link).is_ok() {
                        let _ = std::fs::remove_dir_all(&sub);
                        let _ = std::fs::remove_file(&sub);
                        let _ = std::fs::rename(&staged_link, &sub);
                    }
                }
            })
        }

        /// While `rm` removes a tree, a background thread rapidly flips an intermediate directory
        /// `tree/sub` between a real directory and a symlink to a SENTINEL directory tree that
        /// lives OUTSIDE the target, holding files that must never be deleted.
        ///
        /// [`RmVisitor::dir_pre`] descends a directory via [`Dir::open_dir`] (`O_NOFOLLOW|O_DIRECTORY`), so if
        /// `sub` is a symlink at the moment of descent the open fails closed (ELOOP/ENOTDIR) and
        /// the walk never follows it into the sentinel. If `sub` is a symlink at the moment of
        /// classification it is treated as a leaf and `unlink_at` removes the LINK, never its
        /// target. Either way the out-of-tree sentinel files survive — that is the safety
        /// assertion, checked on every iteration regardless of timing. Also confirms the run
        /// terminates (per-op timeout) rather than hanging or following the link.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn intermediate_dir_swap_never_deletes_out_of_tree_sentinel() -> anyhow::Result<()> {
            let tmp = testutils::create_temp_dir().await?;
            let root = tmp.as_path();
            // the sentinel tree lives OUTSIDE the rm target, reachable only via the swapped symlink.
            let sentinel = root.join("sentinel_tree");
            tokio::fs::create_dir(&sentinel).await?;
            tokio::fs::write(sentinel.join("secret1.txt"), b"SECRET-1").await?;
            tokio::fs::create_dir(sentinel.join("subdir")).await?;
            tokio::fs::write(sentinel.join("subdir").join("secret2.txt"), b"SECRET-2").await?;

            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let swapper =
                spawn_dir_symlink_swapper(root.to_path_buf(), sentinel.clone(), stop.clone());

            let settings = Settings {
                fail_early: false,
                filter: None,
                dry_run: None,
                time_filter: None,
            };
            let mut removed = 0usize;
            let mut errored = 0usize;
            for i in 0..400 {
                // (re)create the target tree. To MAXIMIZE the chance rm encounters `tree/sub` while
                // the background thread has it in the symlink state, give `tree` many sibling
                // subdirectories with files: rm spends real time enumerating/removing them
                // concurrently with the swapper's flips, widening the window in which `sub` is
                // classified/descended mid-swap. On even iterations we additionally seed `sub` as a
                // symlink-to-sentinel up front, deterministically exercising the "intermediate is a
                // symlink at classification time → unlink the link, never recurse the target" path.
                let tree = root.join("tree");
                let _ = tokio::fs::create_dir(&tree).await;
                for d in 0..16 {
                    let sib = tree.join(format!("sib_{d}"));
                    let _ = tokio::fs::create_dir(&sib).await;
                    for f in 0..4 {
                        let _ = tokio::fs::write(sib.join(format!("f{f}.txt")), b"x").await;
                    }
                }
                let sub = tree.join("sub");
                if i % 2 == 0 {
                    // deterministically place a symlink-to-sentinel at `sub` (best-effort; the
                    // swapper may immediately flip it — that's fine, both states are safe).
                    let _ = tokio::fs::remove_dir_all(&sub).await;
                    let _ = tokio::fs::remove_file(&sub).await;
                    let _ = tokio::fs::symlink(&sentinel, &sub).await;
                } else if tokio::fs::symlink_metadata(&sub).await.is_err() {
                    let _ = tokio::fs::create_dir(&sub).await;
                    let _ = tokio::fs::write(sub.join("real.txt"), b"REAL").await;
                }
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    super::rm(&RACE_PROGRESS, &tree, &settings),
                )
                .await
                .expect("rm must not hang under concurrent dir swapping");
                match result {
                    Ok(_) => removed += 1,
                    Err(_) => errored += 1, // a swap was caught mid-walk (failed closed) — accepted
                }
                // CORE SAFETY ASSERTION (holds on every iteration regardless of timing): the
                // out-of-tree sentinel tree and its files are NEVER deleted by rm — neither by
                // following a symlinked `sub` (unlink removes the link, not the target) nor by
                // descending it (open_dir's O_NOFOLLOW fails closed).
                assert!(
                    sentinel.exists(),
                    "iteration {i}: sentinel directory was deleted — rm followed the symlink"
                );
                let s1 = tokio::fs::read(sentinel.join("secret1.txt")).await;
                assert!(
                    matches!(&s1, Ok(b) if b == b"SECRET-1"),
                    "iteration {i}: sentinel/secret1.txt was deleted or altered — rm followed the symlink"
                );
                let s2 = tokio::fs::read(sentinel.join("subdir").join("secret2.txt")).await;
                assert!(
                    matches!(&s2, Ok(b) if b == b"SECRET-2"),
                    "iteration {i}: sentinel/subdir/secret2.txt was deleted — rm recursed through the symlink"
                );
                // clean up any leftover tree before the next iteration (best-effort).
                let _ = tokio::fs::remove_dir_all(root.join("tree")).await;
            }

            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            swapper.join().expect("dir swapper thread panicked");
            // sanity (not the safety assertion): the run did observable work across the iterations.
            tracing::info!("intermediate dir swap: removed={removed}, errored={errored}");
            assert!(
                removed + errored > 0,
                "expected at least one observable outcome across the iterations"
            );
            Ok(())
        }
    }
}
