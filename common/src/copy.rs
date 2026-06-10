use std::ffi::{OsStr, OsString};
use std::os::fd::AsFd;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use throttle::get_file_iops_tokens;
use tracing::instrument;

use crate::config::DryRunMode;
use crate::copy_data::copy_file_range_all;
use crate::filecmp;
use crate::preserve;
use crate::progress;
use crate::rm;
use crate::rm::{Settings as RmSettings, Summary as RmSummary};
use crate::safedir::{self, Dir, FileMeta, Handle};
use crate::walk::{EntryKind, LeafPermit, PermitKind};
use crate::walk_driver::{
    DirAction, DirPreResult, EntryCx, ProcessedChildren, WalkVisitor, process_entry,
};

/// Error type for copy operations. See [`crate::error::OperationError`] for
/// logging conventions and rationale.
pub type Error = crate::error::OperationError<Summary>;

/// Filter condition for overwrite operations.
///
/// Used with `--overwrite-filter` to skip overwriting files that match
/// a directional condition (e.g., destination is newer than source).
#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum OverwriteFilter {
    /// Skip overwriting if the destination file is strictly newer (by mtime).
    #[value(name = "newer")]
    Newer,
}

impl std::fmt::Display for OverwriteFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OverwriteFilter::Newer => write!(f, "newer"),
        }
    }
}

/// A destination entry the source compares against, taken from the directory manifest.
pub struct ExistingDst<'a, M: crate::preserve::Metadata> {
    /// The destination entry's metadata (with `size()` populated).
    pub meta: &'a M,
    /// Whether the destination entry is a regular file (only files are eligible for the
    /// metadata-equality compare; a non-file means the source must send so the destination
    /// can remove-and-replace it).
    pub is_file: bool,
}

/// Decide whether the source may SKIP sending a file because the destination already holds a
/// matching entry. `dst` is `None` when the destination has no entry at that name. Returns
/// `true` to skip (the caller sends a `FileUnchanged` notification instead of file data).
///
/// This mirrors the destination's `process_single_file` identical/`--ignore-existing`/newer
/// checks, so the source pre-filters exactly the files the destination would otherwise receive
/// and drain. Skipping performs no filesystem mutation.
pub fn skip_unchanged_send<S, D>(
    overwrite_compare: &filecmp::MetadataCmpSettings,
    overwrite_filter: Option<OverwriteFilter>,
    ignore_existing: bool,
    src: &S,
    dst: Option<ExistingDst<'_, D>>,
) -> bool
where
    S: crate::preserve::Metadata + std::fmt::Debug,
    D: crate::preserve::Metadata + std::fmt::Debug,
{
    let Some(dst) = dst else {
        return false; // nothing at the destination — must send
    };
    if ignore_existing {
        return true; // any pre-existing entry (file/dir/symlink) — skip
    }
    if !dst.is_file {
        return false; // dest is a non-file under --overwrite — send so dest replaces it
    }
    if filecmp::metadata_equal(overwrite_compare, src, dst.meta) {
        return true; // identical per --overwrite-compare
    }
    if overwrite_filter == Some(OverwriteFilter::Newer) && filecmp::dest_is_newer(src, dst.meta) {
        return true; // --overwrite-filter=newer and destination is strictly newer
    }
    false
}

/// Settings controlling rsync-style `--delete` (mirror) behavior.
///
/// Present (`Some`) only when `--delete` was requested. `None` means the
/// destination is never enumerated and no pruning work is done, so the default
/// copy path pays nothing for this feature.
#[derive(Debug, Clone)]
pub struct DeleteSettings {
    /// Also remove destination entries that match an exclude pattern
    /// (rsync `--delete-excluded`). When false, excluded entries are protected.
    pub delete_excluded: bool,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub dereference: bool,
    pub fail_early: bool,
    pub overwrite: bool,
    pub overwrite_compare: filecmp::MetadataCmpSettings,
    pub overwrite_filter: Option<OverwriteFilter>,
    pub ignore_existing: bool,
    pub chunk_size: u64,
    /// Skip special files (sockets, FIFOs, devices) without error.
    pub skip_specials: bool,
    /// Buffer size for remote copy file transfer operations in bytes.
    ///
    /// This is only used for remote copy operations and controls the buffer size
    /// when copying data between files and network streams. The actual buffer is
    /// capped to the file size to avoid over-allocation for small files.
    pub remote_copy_buffer_size: usize,
    /// filter settings for include/exclude patterns
    pub filter: Option<crate::filter::FilterSettings>,
    /// dry-run mode for previewing operations
    pub dry_run: Option<crate::config::DryRunMode>,
    /// rsync-style `--delete` settings; `None` disables deletion entirely.
    pub delete: Option<DeleteSettings>,
}

/// Summary with the appropriate `*_skipped` counter set to 1 for the given entry kind.
/// Special files count as `files_skipped` to match the historical mapping used
/// when filters skip an entry (`specials_skipped` is reserved for `--skip-specials`).
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

/// Result of checking if an empty directory should be cleaned up.
/// Used when filtering is active and a directory we created ended up empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyDirAction {
    /// keep the directory (directly matched or no filter active)
    Keep,
    /// directory was only traversed, remove it
    Remove,
    /// dry-run mode, don't count this directory in summary
    DryRunSkip,
}

/// Determine what to do with an empty directory when filtering is active.
///
/// This is called when we created a directory but nothing was copied into it
/// (no files, symlinks, or child directories). The decision depends on whether
/// the directory itself was directly matched by an include pattern, or if we
/// only entered it to look for potential matches inside.
///
/// # Arguments
/// * `filter` - the active filter settings (None means no filtering)
/// * `we_created_dir` - whether we created this directory (vs it already existed)
/// * `anything_copied` - whether any content was copied into this directory
/// * `relative_path` - path relative to the source root (for pattern matching)
/// * `is_root` - whether this is the root (user-specified) source directory
/// * `is_dry_run` - whether we're in dry-run mode
pub fn check_empty_dir_cleanup(
    filter: Option<&crate::filter::FilterSettings>,
    we_created_dir: bool,
    anything_copied: bool,
    relative_path: &std::path::Path,
    is_root: bool,
    is_dry_run: bool,
) -> EmptyDirAction {
    // if no filter active or something was copied, keep the directory
    if filter.is_none() || anything_copied {
        return EmptyDirAction::Keep;
    }
    // if we didn't create this directory, don't remove it
    if !we_created_dir {
        return EmptyDirAction::Keep;
    }
    // never remove the root directory — it's the user-specified source
    if is_root {
        return EmptyDirAction::Keep;
    }
    // filter is guaranteed to be Some here (checked above)
    let f = filter.unwrap();
    // check if directory directly matches include pattern
    if f.directly_matches_include(relative_path, true) {
        return EmptyDirAction::Keep;
    }
    // directory was only traversed for potential matches
    if is_dry_run {
        EmptyDirAction::DryRunSkip
    } else {
        EmptyDirAction::Remove
    }
}

#[instrument]
pub fn is_file_type_same(md1: &std::fs::Metadata, md2: &std::fs::Metadata) -> bool {
    let ft1 = md1.file_type();
    let ft2 = md2.file_type();
    ft1.is_dir() == ft2.is_dir()
        && ft1.is_file() == ft2.is_file()
        && ft1.is_symlink() == ft2.is_symlink()
}

#[derive(Copy, Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Summary {
    pub bytes_copied: u64,
    pub files_copied: usize,
    pub symlinks_created: usize,
    pub directories_created: usize,
    pub files_unchanged: usize,
    pub symlinks_unchanged: usize,
    pub directories_unchanged: usize,
    pub files_skipped: usize,
    pub symlinks_skipped: usize,
    pub directories_skipped: usize,
    pub specials_skipped: usize,
    pub rm_summary: RmSummary,
}

impl std::ops::Add for Summary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            bytes_copied: self.bytes_copied + other.bytes_copied,
            files_copied: self.files_copied + other.files_copied,
            symlinks_created: self.symlinks_created + other.symlinks_created,
            directories_created: self.directories_created + other.directories_created,
            files_unchanged: self.files_unchanged + other.files_unchanged,
            symlinks_unchanged: self.symlinks_unchanged + other.symlinks_unchanged,
            directories_unchanged: self.directories_unchanged + other.directories_unchanged,
            files_skipped: self.files_skipped + other.files_skipped,
            symlinks_skipped: self.symlinks_skipped + other.symlinks_skipped,
            directories_skipped: self.directories_skipped + other.directories_skipped,
            specials_skipped: self.specials_skipped + other.specials_skipped,
            rm_summary: self.rm_summary + other.rm_summary,
        }
    }
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "copy:\n\
            -----\n\
            bytes copied: {}\n\
            files copied: {}\n\
            symlinks created: {}\n\
            directories created: {}\n\
            files unchanged: {}\n\
            symlinks unchanged: {}\n\
            directories unchanged: {}\n\
            files skipped: {}\n\
            symlinks skipped: {}\n\
            directories skipped: {}\n\
            specials skipped: {}\n\
            \n\
            delete:\n\
            -------\n\
            {}",
            bytesize::ByteSize(self.bytes_copied),
            self.files_copied,
            self.symlinks_created,
            self.directories_created,
            self.files_unchanged,
            self.symlinks_unchanged,
            self.directories_unchanged,
            self.files_skipped,
            self.symlinks_skipped,
            self.directories_skipped,
            self.specials_skipped,
            &self.rm_summary,
        )
    }
}

/// Public entry point for copy operations.
/// Internally delegates to [`copy_with_filter_base`] with an empty filter base.
#[instrument(skip(prog_track, settings, preserve))]
pub async fn copy(
    prog_track: &'static progress::Progress,
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &Settings,
    preserve: &preserve::Settings,
    is_fresh: bool,
) -> Result<Summary, Error> {
    copy_with_filter_base(
        prog_track,
        src,
        dst,
        settings,
        preserve,
        is_fresh,
        std::path::Path::new(""),
    )
    .await
}

/// Like [`copy`], but treats `src` as living at `filter_base` relative to the original filter
/// root. Used when `rlink` delegates an update-only entry to `copy`: `--delete` pruning inside
/// the delegated subtree then matches the include/exclude filter at the entry's true relative
/// path (e.g. `cache/*.log`) instead of relative to the delegated root.
///
/// The local copy walk is fd-based: the source and destination roots are opened relative to
/// their parent directories and every per-entry operation is performed through
/// file-descriptor-relative syscalls (see [`crate::safedir`]). This closes the TOCTOU window the
/// old path-based walk had between classifying an entry and acting on it. `--dereference` is the
/// one exception — it still resolves symlinks by path (`canonicalize`) and is not hardened.
#[instrument(skip(prog_track, settings, preserve))]
#[allow(clippy::too_many_arguments)]
pub async fn copy_with_filter_base(
    prog_track: &'static progress::Progress,
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &Settings,
    preserve: &preserve::Settings,
    is_fresh: bool,
    filter_base: &std::path::Path,
) -> Result<Summary, Error> {
    // Source: decompose via the shared helper so `.`/`..` operands (e.g. `rcp . dst`, `rcp src/..
    // dst`) are canonicalized to a real directory + basename instead of being rejected; `/` is still
    // rejected. (The helper also maps a single-component relative path's empty parent to ".".)
    let src_operand = crate::walk::split_root_operand(src)
        .await
        .map_err(|err| Error::new(err, Default::default()))?;
    let src_parent_path = src_operand.parent.as_path();
    let src_name = src_operand.name.as_os_str();
    let src = src_operand.display.as_path();
    // check filter for top-level source (files, directories, and symlinks)
    if let Some(ref filter) = settings.filter {
        let src_metadata = crate::walk::run_metadata_probed(
            congestion::Side::Source,
            congestion::MetadataOp::Stat,
            tokio::fs::symlink_metadata(src),
        )
        .await
        .with_context(|| format!("failed reading metadata from src: {:?}", &src))
        .map_err(|err| Error::new(err, Default::default()))?;
        let is_dir = src_metadata.is_dir();
        // for a delegated subtree (non-empty filter_base) the source is not the true filter
        // root, so match it at its logical path with nested semantics; for a normal copy use
        // root-item semantics (anchored patterns don't apply to the root itself).
        let result = if filter_base.as_os_str().is_empty() {
            filter.should_include_root_item(std::path::Path::new(src_name), is_dir)
        } else {
            filter.should_include(filter_base, is_dir)
        };
        match result {
            crate::filter::FilterResult::Included => {}
            result => {
                let kind = EntryKind::from_metadata(&src_metadata);
                if let Some(mode) = settings.dry_run {
                    crate::dry_run::report_skip(src, &result, mode, kind.label_long());
                }
                kind.inc_skipped(prog_track);
                return Ok(skipped_summary_for(kind));
            }
        }
    }
    // Open the parent directories of the source and destination roots so the root entry itself can
    // be opened and classified relative to a directory fd (the same fd-relative path every nested
    // entry takes). The root is then handed to the walk driver (via `run_copy_root`) by its
    // basename, exactly like a child entry. The destination keeps the direct split — a `.`/`..`
    // destination is not a meaningful copy target, and rejecting it avoids clobbering the cwd.
    let (Some(dst_parent_path), Some(_dst_name)) = (dst.parent(), dst.file_name()) else {
        return Err(Error::new(
            anyhow!(
                "copy destination {:?} has no parent directory or file name",
                dst
            ),
            Default::default(),
        ));
    };
    // empty parent (relative path with a single component) means the current directory.
    let dst_parent_path = if dst_parent_path.as_os_str().is_empty() {
        std::path::Path::new(".")
    } else {
        dst_parent_path
    };
    // open the operand's TRUSTED parent prefix following symlinks normally (the prefix is trusted
    // up to and including the operand's container — only entries strictly below the named root are
    // O_NOFOLLOW-hardened). a symlinked parent (e.g. `rcp src symlinkdir/out`) is followed.
    let src_parent = Dir::open_parent_dir(src_parent_path, congestion::Side::Source)
        .await
        .with_context(|| format!("cannot open source parent directory {:?}", src_parent_path))
        .map_err(|err| Error::new(err, Default::default()))?;
    // cross from the trusted parent prefix into the hardened tree (O_NOFOLLOW below here).
    let src_parent = Arc::new(src_parent.into_tree());
    // In dry-run we never touch the destination, so we don't open its parent at all (the parent
    // may not even exist). `dst_parent == None` is the signal throughout the walk that destination
    // operations must be skipped.
    let dst_parent = if settings.dry_run.is_some() {
        None
    } else {
        // the destination's TRUSTED parent prefix is resolved following symlinks (see the source
        // parent above): `rcp file symlink_to_dir/out` must copy into the real directory the
        // symlinked parent points at, not fail closed on it.
        let dir = Dir::open_parent_dir(dst_parent_path, congestion::Side::Destination)
            .await
            .with_context(|| {
                format!(
                    "cannot open destination parent directory {:?}",
                    dst_parent_path
                )
            })
            .map_err(|err| Error::new(err, Default::default()))?;
        // cross from the trusted parent prefix into the hardened tree (O_NOFOLLOW below here).
        Some(Arc::new(dir.into_tree()))
    };
    run_copy_root(
        prog_track,
        &src_parent,
        dst_parent,
        src_name,
        src,
        dst,
        filter_base,
        settings,
        preserve,
        is_fresh,
        None,
    )
    .await
}

/// Copy a single entry `name` (within `src_parent`) into `dst_parent`, using held directory
/// handles rather than re-resolving any path. This is the fd-based delegation entry point for
/// `rlink`: when an entry must be COPIED rather than hard-linked (a file that changed vs the
/// update tree, a symlink, a type-mismatch, or an update-only entry), `link` hands its already-open
/// parent `Dir`s plus the entry `name` here, so the copy inherits the same intermediate-component
/// TOCTOU safety the link walk has — no path is re-walked from a root.
///
/// `src_path`/`dst_path` are the entry's reconstructed real paths; they serve as the copy walk's
/// roots so diagnostics, `--dereference` (`canonicalize`), path-based `rm`, and `--delete` pruning
/// reconstruct the right paths inside the delegated subtree. `filter_base` is the entry's logical
/// path relative to the original filter root, so any `--delete` pruning inside the subtree matches
/// include/exclude patterns at the entry's true relative path (e.g. `cache/*.log`). `dst_parent`
/// is `None` only in dry-run (no destination mutation).
#[instrument(skip(
    prog_track,
    src_parent,
    dst_parent,
    settings,
    preserve,
    open_file_guard
))]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn copy_child(
    prog_track: &'static progress::Progress,
    src_parent: &Arc<Dir>,
    dst_parent: Option<&Arc<Dir>>,
    name: &OsStr,
    src_path: &std::path::Path,
    dst_path: &std::path::Path,
    filter_base: &std::path::Path,
    settings: &Settings,
    preserve: &preserve::Settings,
    is_fresh: bool,
    open_file_guard: Option<throttle::OpenFileGuard>,
) -> Result<Summary, Error> {
    run_copy_root(
        prog_track,
        src_parent,
        dst_parent.map(Arc::clone),
        name,
        src_path,
        dst_path,
        filter_base,
        settings,
        preserve,
        is_fresh,
        open_file_guard.map(LeafPermit::OpenFile),
    )
    .await
}

/// The copy walk's [`WalkVisitor`]: holds the run-constant state shared by every entry and maps
/// each per-entry decision onto the generic [`crate::walk_driver`] driver.
///
/// The driver owns enumeration, the leaf-permit lifecycle, spawning, the single
/// drop-before-recurse site, and the error fold; this visitor supplies copy's per-entry bodies
/// (`copy_file_fd`, `copy_symlink_fd`, `resolve_dst_dir`, `remove_existing`, the empty-dir cleanup,
/// `--delete` prune, and directory metadata). The destination tree is threaded as an *inherited
/// attribute* through [`Self::DirContext`] (each directory's children read their destination parent
/// from `parent_ctx`), so the single-tree driver carries copy's second tree without modeling it.
struct CopyVisitor {
    prog_track: &'static progress::Progress,
    /// The destination root operand. The root's destination basename comes from here (it may differ
    /// from the source basename, e.g. copying `foo` → `bar`); nested entries reuse the source name.
    /// The *source* root needs no field: an entry's source real path is the driver-maintained
    /// `EntryCx::real_path` (seeded with the source root in [`run_copy_root`]).
    dst_root: PathBuf,
    /// The logical filter base: an entry's filter path is `filter_base.join(rel_path)`.
    filter_base: PathBuf,
    settings: Settings,
    preserve: preserve::Settings,
    /// The opened top-level destination parent directory (`None` in dry-run). Seeds the
    /// [`Self::DirContext`] chain via [`WalkVisitor::root_dir_context`].
    dst_parent: Option<Arc<Dir>>,
    /// The initial freshness for the root entry's containing directory.
    root_is_fresh: bool,
}

/// Inherited per-directory context: the destination parent directory for one level plus its
/// freshness. `dst_dir == None` is the dry-run signal threaded throughout the walk (no destination
/// mutation). This is how the single-tree driver carries copy's destination tree — each child reads
/// its destination parent from here rather than the driver knowing a second tree exists.
#[derive(Clone)]
struct CopyDirContext {
    dst_dir: Option<Arc<Dir>>,
    is_fresh: bool,
}

/// State threaded from [`WalkVisitor::dir_pre`] to [`WalkVisitor::dir_post`] in the same task:
/// everything `dir_post` needs to run empty-dir cleanup, `--delete` prune, and apply directory
/// metadata.
struct CopyDirState {
    /// The destination directory just created/reused (`None` in dry-run).
    dst_dir: Option<Arc<Dir>>,
    /// The destination parent directory and this directory's destination name within it — used to
    /// remove an empty directory fd-relative (`dst_parent.rmdir_at(dst_name)`). `None` in dry-run.
    dst_parent: Option<Arc<Dir>>,
    dst_name: OsString,
    /// Whether we created the destination directory (vs. reused an existing one).
    we_created: bool,
    /// The source directory's metadata, applied to the destination post-order.
    src_meta: FileMeta,
    /// Whether this is the user-specified root directory (never empty-dir-cleaned up).
    is_root: bool,
    /// The `directories_created`/`directories_unchanged` contribution from resolving this
    /// directory's destination, folded into the final summary.
    base: Summary,
}

impl CopyVisitor {
    /// The destination real path for the entry described by `cx` (`dst_root.join(rel_path)`, or the
    /// root verbatim when `rel_path` is empty — joining an empty `rel_path` would append a trailing
    /// separator that `canonicalize`/ENOTDIR-sensitive paths reject). Mirrors `copy_internal`.
    fn dst_path_for(&self, cx: &EntryCx) -> PathBuf {
        if cx.rel_path.as_os_str().is_empty() {
            self.dst_root.clone()
        } else {
            self.dst_root.join(&cx.rel_path)
        }
    }

    /// The destination entry's name within its parent. For nested entries this equals the source
    /// `cx.name`, but for the root the source and destination basenames may differ (e.g. copying
    /// `foo` → `bar`), so the root's destination name comes from `dst_root`. Mirrors `copy_internal`.
    // copy's `Error` carries a `Summary` (intrinsically large); a fallible helper returning it trips
    // `result_large_err` only because the `Ok` variant here is a small `OsString`. The error arm is
    // unreachable in practice (`copy_with_filter_base` pre-validates the root's file name and a
    // delegated `copy_child` root path always has one) — keep it as defense-in-depth.
    #[allow(clippy::result_large_err)]
    fn dst_name_for(&self, cx: &EntryCx) -> Result<OsString, Error> {
        if cx.rel_path.as_os_str().is_empty() {
            self.dst_root
                .file_name()
                .map(OsStr::to_owned)
                .ok_or_else(|| {
                    Error::new(
                        anyhow!("copy destination {:?} has no file name", &self.dst_root),
                        Default::default(),
                    )
                })
        } else {
            Ok(cx.name.clone())
        }
    }

    /// rsync-style `--delete` prune for one finished directory: remove destination entries with no
    /// source counterpart. Runs only when `--delete` was requested AND every child of this directory
    /// succeeded — skipping the prune on any error (deleting based on a run that did not fully
    /// succeed could remove data unexpectedly). Prune enumerates and removes through the
    /// destination's own pinned directory fd, so a concurrent symlink swap cannot redirect it outside
    /// the destination.
    ///
    /// Folds the prune's `RmSummary` into `copy_summary`. On a non-fail-early prune error it records
    /// the error in `child_error` (surfaced later by [`Self::finalize_dir`]); it returns `Err` only
    /// in fail-early mode or when the dry-run delete-scan open fails outright.
    async fn prune_finished_dir(
        &self,
        copy_summary: &mut Summary,
        child_error: &mut Option<anyhow::Error>,
        processed: &ProcessedChildren,
        dst_dir: &Option<Arc<Dir>>,
        dst_path: &std::path::Path,
        rel_path: &std::path::Path,
    ) -> Result<(), Error> {
        if child_error.is_some() && self.settings.delete.is_some() {
            tracing::warn!(
                "skipping --delete pruning of {:?} because the copy reported errors",
                dst_path
            );
        }
        let Some(delete_settings) = &self.settings.delete else {
            return Ok(());
        };
        if child_error.is_some() {
            return Ok(());
        }
        // the keep-set: every source child the driver spawned for this directory (filter-IN, special
        // files included — they have a source counterpart so must not be pruned). this is exactly the
        // set `copy_dir_contents` built inline before the skip-specials check.
        let keep_set: std::collections::HashSet<OsString> =
            processed.names().iter().cloned().collect();
        let relative_dir = self.filter_base.join(rel_path);
        // obtain the destination directory as an open `Dir`. in a real copy we already hold it
        // (`dst_dir`). in --dry-run the create-or-overwrite step was skipped, so there is no held
        // handle and the path could still be a symlink-to-directory or a non-directory; open it
        // O_NOFOLLOW|O_DIRECTORY (dereference=false) so a symlink or non-directory fails closed here
        // and prune is simply skipped — never following the symlink to preview deletions OUTSIDE the
        // destination. a missing dir likewise skips.
        let prune_dir: Option<Arc<Dir>> = match dst_dir {
            Some(dir) => Some(Arc::clone(dir)),
            None => {
                match Dir::open_root_dir(dst_path, false, congestion::Side::Destination).await {
                    Ok(dir) => Some(Arc::new(dir)),
                    Err(err)
                        if matches!(
                            err.kind(),
                            std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                        ) || err.raw_os_error() == Some(libc::ELOOP)
                            || err.raw_os_error() == Some(libc::ENOTDIR) =>
                    {
                        tracing::debug!(
                            "skipping --delete pruning of {:?}: not a real directory in dry-run",
                            dst_path
                        );
                        None
                    }
                    Err(err) => {
                        let err = anyhow::Error::new(err).context(format!(
                            "cannot open destination {dst_path:?} for delete scan"
                        ));
                        // a scan-open failure is surfaced as this directory's own error (this runs only
                        // when children all succeeded, so there is no prior error to combine with —
                        // matching the old fail-early/collect handling).
                        return Err(Error::new(err, *copy_summary));
                    }
                }
            }
        };
        let Some(prune_dir) = prune_dir else {
            return Ok(());
        };
        match crate::delete::prune_extraneous(
            self.prog_track,
            &prune_dir,
            &relative_dir,
            &keep_set,
            self.settings.filter.as_ref(),
            delete_settings,
            self.settings.fail_early,
            self.settings.dry_run,
        )
        .await
        {
            Ok(rm_summary) => {
                copy_summary.rm_summary = copy_summary.rm_summary + rm_summary;
            }
            Err(err) => {
                copy_summary.rm_summary = copy_summary.rm_summary + err.summary;
                if self.settings.fail_early {
                    return Err(Error::new(err.source, *copy_summary));
                }
                // non-fail-early: remember the prune error but still apply this fully-copied
                // directory's own metadata below (matching main's collect-then-finalize), surfacing
                // the prune error from the tail.
                *child_error = Some(err.source);
            }
        }
        Ok(())
    }

    /// Finish a directory after its children and `--delete` prune have run: when filtering left it
    /// empty, clean it up (or, in dry-run, suppress its would-be-created count); otherwise apply the
    /// source directory's metadata to the destination through its own held fd. A collected
    /// `child_error` always takes precedence and is surfaced here (even when the empty directory was
    /// pruned), so a failed child copy can never be reported as overall success.
    async fn finalize_dir(&self, fin: FinalizeDir, cx: &EntryCx) -> Result<Summary, Error> {
        let FinalizeDir {
            mut copy_summary,
            child_error,
            dst_dir,
            dst_parent,
            dst_name,
            src_meta,
            we_created,
            is_root,
        } = fin;
        let src_path = &cx.real_path;
        let dst_path = self.dst_path_for(cx);
        let rel_path = &cx.rel_path;
        // when filtering is active and we created this directory, check whether anything was
        // actually copied into it. if not, we may need to clean up the empty directory.
        let this_dir_count = usize::from(we_created);
        let child_dirs_created = copy_summary
            .directories_created
            .saturating_sub(this_dir_count);
        let anything_copied = copy_summary.files_copied > 0
            || copy_summary.symlinks_created > 0
            || child_dirs_created > 0;
        let relative_path = self.filter_base.join(rel_path);
        match check_empty_dir_cleanup(
            self.settings.filter.as_ref(),
            we_created,
            anything_copied,
            &relative_path,
            is_root,
            self.settings.dry_run.is_some(),
        ) {
            EmptyDirAction::Keep => { /* proceed with metadata application */ }
            EmptyDirAction::DryRunSkip => {
                tracing::debug!(
                    "dry-run: directory {:?} would not be created (nothing to copy inside)",
                    dst_path
                );
                copy_summary.directories_created = 0;
                // the directory itself is not created, but a child error collected during the walk
                // must still surface — otherwise a traversal-only directory whose only child FAILED
                // becomes "empty", gets skipped here, and the failed copy is reported as success.
                return match child_error {
                    Some(child_error) => Err(Error::new(child_error, copy_summary)),
                    None => Ok(copy_summary),
                };
            }
            EmptyDirAction::Remove => {
                tracing::debug!(
                    "directory {:?} has nothing to copy inside, removing empty directory",
                    dst_path
                );
                // remove the empty directory fd-relative, through its parent dir handle:
                // `rmdir_at` operates on `dst_name` within the held `dst_parent` fd (never by path)
                // and only succeeds on an empty directory, so it is contained to `dst_parent` and
                // cannot be redirected. our still-open fd to the directory itself simply detaches.
                // `dst_parent` is always Some here: it is None only in dry-run, where this arm is
                // unreachable (`check_empty_dir_cleanup` returns `DryRunSkip`, not `Remove`).
                let rmdir_result = match &dst_parent {
                    Some(dst_parent) => dst_parent.rmdir_at(&dst_name).await,
                    None => {
                        crate::walk::run_metadata_probed(
                            congestion::Side::Destination,
                            congestion::MetadataOp::RmDir,
                            tokio::fs::remove_dir(&dst_path),
                        )
                        .await
                    }
                };
                match rmdir_result {
                    Ok(()) => {
                        copy_summary.directories_created = 0;
                        // the empty directory is removed, but a child error collected during the
                        // walk must still surface — otherwise a traversal-only directory whose only
                        // child FAILED becomes "empty", is removed here, and the failed copy is
                        // reported as success.
                        return match child_error {
                            Some(child_error) => Err(Error::new(child_error, copy_summary)),
                            None => Ok(copy_summary),
                        };
                    }
                    Err(err) => {
                        tracing::debug!(
                            "failed to remove empty directory {:?}: {:#}, keeping",
                            dst_path,
                            &err
                        );
                        // fall through to apply metadata.
                    }
                }
            }
        }
        // apply directory metadata regardless of whether all children copied successfully (the
        // directory itself was created/opened in dir_pre). skipped in dry-run (no directory
        // exists). a child error takes precedence over a metadata error (matching the old tail:
        // log the metadata error, return the child error).
        tracing::debug!("set 'dst' directory metadata");
        let metadata_result = match &dst_dir {
            Some(dst_dir) => safedir::set_dir_metadata_fd(&self.preserve, &src_meta, dst_dir).await,
            None => Ok(()),
        };
        if let Some(child_error) = child_error {
            if let Err(metadata_err) = metadata_result {
                tracing::error!(
                    "copy: {:?} -> {:?} failed to set directory metadata: {:#}",
                    src_path,
                    dst_path,
                    &metadata_err
                );
            }
            return Err(Error::new(child_error, copy_summary));
        }
        // no child failures, so the metadata error is the primary error.
        metadata_result
            .with_context(|| format!("failed setting directory metadata on {:?}", dst_path))
            .map_err(|err| Error::new(err, copy_summary))?;
        Ok(copy_summary)
    }
}

/// The owned per-directory state [`CopyVisitor::dir_post`] hands to [`CopyVisitor::finalize_dir`]
/// after the child summaries are folded and `--delete` pruning has run: the accumulated summary, any
/// collected child error to surface, and this directory's destination handles / classification.
struct FinalizeDir {
    copy_summary: Summary,
    child_error: Option<anyhow::Error>,
    dst_dir: Option<Arc<Dir>>,
    dst_parent: Option<Arc<Dir>>,
    dst_name: OsString,
    src_meta: FileMeta,
    we_created: bool,
    is_root: bool,
}

/// Build the [`CopyVisitor`] for one copy operation and process the root entry through the generic
/// driver. Shared by [`copy_with_filter_base`] (root entry, no pre-acquired permit) and
/// [`copy_child`] (rlink's fd-based delegation entry point, which may pass an already-held
/// open-files permit). The root is processed exactly like a nested child via
/// [`crate::walk_driver::process_entry`]: it is classified authoritatively, then dispatched to
/// `visit_leaf` (file/symlink/special) or `dir_pre`/recurse/`dir_post` (directory).
#[allow(clippy::too_many_arguments)]
async fn run_copy_root(
    prog_track: &'static progress::Progress,
    src_parent: &Arc<Dir>,
    dst_parent: Option<Arc<Dir>>,
    name: &OsStr,
    src_root: &std::path::Path,
    dst_root: &std::path::Path,
    filter_base: &std::path::Path,
    settings: &Settings,
    preserve: &preserve::Settings,
    is_fresh: bool,
    permit: Option<LeafPermit>,
) -> Result<Summary, Error> {
    let visitor = Arc::new(CopyVisitor {
        prog_track,
        dst_root: dst_root.to_path_buf(),
        filter_base: filter_base.to_path_buf(),
        settings: settings.clone(),
        preserve: *preserve,
        dst_parent,
        root_is_fresh: is_fresh,
    });
    // the root entry's owned context: parent = the hardened source parent, name = the source root
    // basename, rel_path = "" (the root), real_path = the source root. `filter_path` is seeded with
    // `filter_base` so that inside a delegated subtree (rlink handing an update-only/type-changed
    // subtree to `copy_child`, rooted BELOW the original filter root) every descendant's filter
    // decision is evaluated at its true logical path (e.g. `cache/keep.txt`, not the bare basename
    // relative to the delegated root). For a normal `copy()` (`filter_base` empty) this is "", so
    // `filter_path == rel_path` throughout. dry_run is the destination's None signal carried on the
    // context for the driver's bookkeeping.
    let root_cx = EntryCx {
        parent: Arc::clone(src_parent),
        name: name.to_owned(),
        rel_path: PathBuf::new(),
        filter_path: filter_base.to_path_buf(),
        real_path: src_root.to_path_buf(),
        dry_run: settings.dry_run.is_some(),
        prog_track,
    };
    let root_ctx = visitor.root_dir_context();
    process_entry(visitor, root_cx, root_ctx, permit).await
}

impl WalkVisitor for CopyVisitor {
    type Summary = Summary;
    type DirContext = CopyDirContext;
    type DirState = CopyDirState;

    fn root_dir_context(&self) -> CopyDirContext {
        CopyDirContext {
            dst_dir: self.dst_parent.clone(),
            is_fresh: self.root_is_fresh,
        }
    }

    fn permit_kind(&self) -> PermitKind {
        // copy holds an open file descriptor across a regular-file copy.
        PermitKind::OpenFile
    }

    fn want_permit(&self, hint: Option<EntryKind>) -> bool {
        // pre-acquire the open-files permit only for a known regular-file hint. symlinks are
        // excluded (with --dereference they may resolve to directories — deadlock risk) and so is
        // DT_UNKNOWN (it might be a directory). this matches `copy_dir_contents`'s old policy.
        hint == Some(EntryKind::File)
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
        // mirror `copy_dir_contents`'s inline filter-skip: the dry-run "skip ..." line plus the
        // matching `*_skipped` counter. the driver already did the shared progress increment.
        if let Some(mode) = self.settings.dry_run {
            crate::dry_run::report_skip(&cx.real_path, skip_result, mode, kind.label());
        }
        skipped_summary_for(kind)
    }

    async fn visit_leaf(
        &self,
        cx: &EntryCx,
        parent_ctx: &CopyDirContext,
        handle: Handle,
        kind: EntryKind,
        permit: Option<LeafPermit>,
    ) -> Result<Summary, Error> {
        let src_parent = &cx.parent;
        let dst_parent = parent_ctx.dst_dir.as_ref();
        let name = cx.name.as_os_str();
        let src_path = &cx.real_path;
        let dst_path = self.dst_path_for(cx);
        let dst_name = self.dst_name_for(cx)?;
        let is_fresh = parent_ctx.is_fresh;
        // --dereference: resolve a symlink to its target by path and copy that instead. this is the
        // one path-based branch we intentionally keep (hardening -L is out of scope). drop the
        // (regular-file-only) pre-acquired permit first: it is meaningless for a symlink that may
        // resolve to a directory and could deadlock the recursive `copy()` under a saturated pool.
        if self.settings.dereference && kind == EntryKind::Symlink {
            drop(permit);
            // invariant: only the explicit `--dereference` path may re-resolve an entry by path
            // (`canonicalize`). the non-dereference walk is fully fd-based and must never reach
            // here. a future refactor that wires `dereference == false` into this branch trips in
            // debug/tests.
            debug_assert!(
                self.settings.dereference,
                "canonicalize reached with dereference == false; the non-dereference copy path \
                 must never re-resolve a path"
            );
            let link = crate::walk::run_metadata_probed(
                congestion::Side::Source,
                congestion::MetadataOp::Stat,
                tokio::fs::canonicalize(src_path),
            )
            .await
            .with_context(|| format!("failed reading src symlink {:?}", src_path))
            .map_err(|err| Error::new(err, Default::default()))?;
            return copy(
                self.prog_track,
                &link,
                &dst_path,
                &self.settings,
                &self.preserve,
                is_fresh,
            )
            .await;
        }
        match kind {
            EntryKind::File => {
                // hold the open-files permit across the file copy. the driver pre-acquired it for a
                // regular-file hint; if it didn't (an unknown hint that turned out to be a file),
                // acquire it now.
                let _guard = match permit {
                    Some(p) => p,
                    None => LeafPermit::OpenFile(throttle::open_file_permit().await),
                };
                copy_file_fd(
                    self.prog_track,
                    src_parent,
                    dst_parent,
                    name,
                    &dst_name,
                    &dst_path,
                    src_path,
                    &handle,
                    &self.settings,
                    &self.preserve,
                    is_fresh,
                )
                .await
            }
            EntryKind::Symlink => {
                drop(permit);
                copy_symlink_fd(
                    self.prog_track,
                    src_parent,
                    dst_parent,
                    &dst_name,
                    src_path,
                    &dst_path,
                    &handle,
                    &self.settings,
                    &self.preserve,
                    is_fresh,
                )
                .await
            }
            EntryKind::Special => {
                drop(permit);
                if self.settings.skip_specials {
                    tracing::debug!("skipping special file {:?}", src_path);
                    if let Some(mode) = self.settings.dry_run {
                        match mode {
                            DryRunMode::Brief => {}
                            DryRunMode::All => println!("skip special {:?}", src_path),
                            DryRunMode::Explain => {
                                println!("skip special {:?} (unsupported file type)", src_path);
                            }
                        }
                    }
                    self.prog_track.specials_skipped.inc();
                    return Ok(Summary {
                        specials_skipped: 1,
                        ..Default::default()
                    });
                }
                Err(Error::new(
                    anyhow!(
                        "copy: {:?} -> {:?} failed, unsupported src file type",
                        src_path,
                        dst_path
                    ),
                    Default::default(),
                ))
            }
            // a directory never reaches `visit_leaf`: the driver dispatches it to dir_pre/dir_post.
            EntryKind::Dir => unreachable!("directory entries are handled by dir_pre/dir_post"),
        }
    }

    async fn dir_pre(
        &self,
        cx: &EntryCx,
        parent_ctx: &CopyDirContext,
        _handle: &Handle,
    ) -> DirPreResult<Self> {
        let src_parent = &cx.parent;
        let name = cx.name.as_os_str();
        let src_path = &cx.real_path;
        let dst_path = self.dst_path_for(cx);
        let dst_name = self.dst_name_for(cx)?;
        let is_root = cx.rel_path.as_os_str().is_empty();
        let is_fresh = parent_ctx.is_fresh;
        // open the source directory's contents (O_NOFOLLOW) — this is the `dir` the driver walks.
        let src_dir = src_parent
            .open_dir(name)
            .await
            .with_context(|| format!("cannot open directory {:?} for reading", src_path))
            .map_err(|err| Error::new(err, Default::default()))?;
        let src_dir = Arc::new(src_dir);
        // the directory metadata applied to the destination comes from the SAME fd whose contents we
        // enumerate (read-side fidelity, docs/tocttou.md), not the classify `Handle`: a swap of the
        // dir entry between classify and `open_dir` is caught by O_NOFOLLOW (fail-closed), and on a
        // same-name dir swap the applied metadata pairs with the contents actually copied.
        let src_meta = src_dir
            .meta()
            .await
            .with_context(|| format!("cannot read directory metadata from {:?}", src_path))
            .map_err(|err| Error::new(err, Default::default()))?;
        if self.settings.dry_run.is_some() {
            // dry-run: if --ignore-existing and a non-directory already exists at the destination,
            // the whole subtree would be skipped. otherwise report the directory and traverse its
            // contents (with no destination directory).
            if self.settings.ignore_existing
                && !is_fresh
                && crate::walk::run_metadata_probed(
                    congestion::Side::Destination,
                    congestion::MetadataOp::Stat,
                    tokio::fs::symlink_metadata(&dst_path),
                )
                .await
                .is_ok()
                && !dst_path.is_dir()
            {
                match self.settings.dry_run {
                    Some(DryRunMode::Brief) | None => {}
                    Some(DryRunMode::All) => println!("skip dir {:?}", dst_path),
                    Some(DryRunMode::Explain) => {
                        println!(
                            "skip dir {:?} (destination exists, not a directory)",
                            dst_path
                        );
                    }
                }
                return Ok(DirAction::Skip(Summary {
                    directories_unchanged: 1,
                    ..Default::default()
                }));
            }
            crate::dry_run::report_action("copy", src_path, Some(&dst_path), "dir");
            let base = Summary {
                directories_created: 1, // report as would-be-created
                ..Default::default()
            };
            return Ok(DirAction::Descend {
                dir: src_dir,
                child_ctx: CopyDirContext {
                    dst_dir: None, // dry-run: no destination parent (no destination mutation)
                    is_fresh,
                },
                state: CopyDirState {
                    dst_dir: None,
                    dst_parent: None,
                    dst_name,
                    // treat as "created" so empty-dir cleanup can suppress the dry-run count.
                    we_created: true,
                    src_meta,
                    is_root,
                    base,
                },
            });
        }
        // real copy: the destination parent is Some (None only in dry-run, handled above).
        let dst_parent = parent_ctx
            .dst_dir
            .as_ref()
            .expect("destination parent must be open for a real copy");
        let DirSlot {
            dir: dst_dir,
            summary: base,
            is_fresh: child_is_fresh,
            we_created,
        } = match resolve_dst_dir(
            self.prog_track,
            dst_parent,
            &dst_name,
            &dst_path,
            &self.settings,
            is_fresh,
        )
        .await?
        {
            DirResolution::Skip(summary) => return Ok(DirAction::Skip(summary)),
            DirResolution::Proceed(slot) => slot,
        };
        Ok(DirAction::Descend {
            dir: src_dir,
            child_ctx: CopyDirContext {
                dst_dir: Some(Arc::clone(&dst_dir)),
                is_fresh: child_is_fresh,
            },
            state: CopyDirState {
                dst_dir: Some(dst_dir),
                dst_parent: Some(Arc::clone(dst_parent)),
                dst_name,
                we_created,
                src_meta,
                is_root,
                base,
            },
        })
    }

    async fn dir_post(
        &self,
        cx: &EntryCx,
        state: CopyDirState,
        processed: &ProcessedChildren,
        child_result: Result<Summary, Error>,
    ) -> Result<Summary, Error> {
        let CopyDirState {
            dst_dir,
            dst_parent,
            dst_name,
            we_created,
            src_meta,
            is_root,
            base,
        } = state;
        // whether any child failed (non-fail-early: the driver still calls dir_post on error so we
        // can apply post-order directory metadata, but we must skip the destructive `--delete`
        // prune and surface the child error). the partial child summary is folded either way,
        // seeded with `base` (this directory's own create/unchanged contribution) — exactly as
        // `copy_dir_contents` seeded `copy_summary = base` before joining the children.
        let (child_summary, mut child_error) = match child_result {
            Ok(summary) => (summary, None),
            Err(err) => (err.summary, Some(err.source)),
        };
        let mut copy_summary = base + child_summary;
        let dst_path = self.dst_path_for(cx);
        // rsync-style --delete prune (folds the RmSummary; may record a non-fail-early child error
        // or fail-early). A no-op unless --delete was requested AND every child succeeded.
        self.prune_finished_dir(
            &mut copy_summary,
            &mut child_error,
            processed,
            &dst_dir,
            &dst_path,
            &cx.rel_path,
        )
        .await?;
        // empty-dir cleanup + post-order directory metadata; also surfaces any collected child error.
        self.finalize_dir(
            FinalizeDir {
                copy_summary,
                child_error,
                dst_dir,
                dst_parent,
                dst_name,
                src_meta,
                we_created,
                is_root,
            },
            cx,
        )
        .await
    }
}

/// Copy a regular file fd-relative: create (or overwrite) the destination via `dst_parent`,
/// copy the bytes with `copy_file_range_all`, then apply metadata through the destination's own
/// fd — the open is held from creation through metadata, closing the path-based re-open TOCTOU
/// window. `--overwrite`/`--ignore-existing`/`is_fresh`/dry-run semantics mirror the path-based
/// [`copy_file`].
#[allow(clippy::too_many_arguments)]
async fn copy_file_fd(
    prog_track: &'static progress::Progress,
    src_parent: &Arc<Dir>,
    dst_parent: Option<&Arc<Dir>>,
    name: &OsStr,
    dst_name: &OsStr,
    dst_path: &std::path::Path,
    src_path: &std::path::Path,
    src_handle: &Handle,
    settings: &Settings,
    preserve: &preserve::Settings,
    is_fresh: bool,
) -> Result<Summary, Error> {
    // bring `FileMeta::size()` into scope locally; importing the trait at module level would
    // collide with `std::os::unix::fs::MetadataExt` on `std::fs::Metadata` elsewhere in this file.
    use crate::preserve::Metadata as _;
    let src_meta = src_handle.meta();
    // --ignore-existing: skip if the destination already exists (any type, including a dangling
    // symlink). probe via the held dst fd in a real copy; in dry-run there is no held fd
    // (`dst_parent` is None), so probe by path — dry-run is already deliberately path-based — so the
    // preview output and counters reflect the real skip decision.
    let dst_exists = match dst_parent {
        Some(dst_parent) => dst_parent.child(dst_name).await.is_ok(),
        None => tokio::fs::symlink_metadata(dst_path).await.is_ok(),
    };
    if !is_fresh && settings.ignore_existing && dst_exists {
        if let Some(mode) = settings.dry_run {
            match mode {
                DryRunMode::Brief => {}
                DryRunMode::All => println!("skip file {:?}", dst_path),
                DryRunMode::Explain => println!("skip file {:?} (destination exists)", dst_path),
            }
        }
        tracing::debug!("destination exists, skipping (--ignore-existing)");
        prog_track.files_unchanged.inc();
        return Ok(Summary {
            files_unchanged: 1,
            ..Default::default()
        });
    }
    // dry-run: report and return what would happen without touching the destination.
    if settings.dry_run.is_some() {
        crate::dry_run::report_action("copy", src_path, Some(dst_path), "file");
        return Ok(Summary {
            files_copied: 1,
            bytes_copied: src_meta.size(),
            ..Default::default()
        });
    }
    // dst_parent is guaranteed Some here: it is None only in dry-run, which returned above.
    let dst_parent = dst_parent.expect("destination parent must be open for a real copy");
    get_file_iops_tokens(settings.chunk_size, src_meta.size()).await;
    let mut rm_summary = RmSummary::default();
    // when the destination tree is not known-fresh, an entry may already exist. classify it and
    // decide whether to skip (identical / newer), error (no --overwrite), or remove it first.
    if !is_fresh && let Ok(dst_handle) = dst_parent.child(dst_name).await {
        if !settings.overwrite {
            return Err(Error::new(
                anyhow!(
                    "destination {:?} already exists, did you intend to specify --overwrite?",
                    dst_path
                ),
                Default::default(),
            ));
        }
        tracing::debug!("file exists, check if it's identical");
        if dst_handle.kind() == EntryKind::File {
            if filecmp::metadata_equal(&settings.overwrite_compare, src_meta, dst_handle.meta()) {
                tracing::debug!("file is identical, skipping");
                prog_track.files_unchanged.inc();
                return Ok(Summary {
                    files_unchanged: 1,
                    ..Default::default()
                });
            }
            if let Some(OverwriteFilter::Newer) = settings.overwrite_filter
                && filecmp::dest_is_newer(src_meta, dst_handle.meta())
            {
                tracing::debug!("dest is newer than source, skipping");
                prog_track.files_unchanged.inc();
                return Ok(Summary {
                    files_unchanged: 1,
                    ..Default::default()
                });
            }
        }
        tracing::info!("destination differs, removing existing entry");
        rm_summary = remove_existing(
            prog_track,
            dst_parent,
            dst_name,
            dst_path,
            &dst_handle,
            settings,
        )
        .await?;
    }
    // open the source for reading (fstat confirms it is still a regular file) and create the
    // destination fresh. the creation mode matches what the metadata applier will chmod to, so the
    // file has correct permissions even before metadata is fully applied. our write fd is writable
    // regardless of those bits (it was opened O_WRONLY at creation).
    let copy_summary = Summary {
        rm_summary,
        ..Default::default()
    };
    let (src_file, open_meta) = src_parent
        .open_file_read(name)
        .await
        .with_context(|| format!("failed opening src file {:?} for reading", src_path))
        .map_err(|err| Error::new(err, copy_summary))?;
    let create_mode = preserve::masked_mode(preserve.file.mode_mask, &open_meta);
    let dst_file = dst_parent
        .create_file(dst_name, create_mode)
        .await
        .with_context(|| format!("failed creating {:?}", dst_path))
        .map_err(|err| Error::new(err, copy_summary))?;
    tracing::debug!("copying data");
    let len = open_meta.size();
    // the data copy is the data path, not a metadata syscall — it is deliberately NOT wrapped in a
    // congestion probe (matching the old `tokio::fs::copy`), so the large/variable copy latency
    // never pollutes the per-metadata-op controller baseline. backpressure comes from the
    // open-files permit the caller holds. the dst file is returned so its still-open fd can carry
    // the metadata application that follows, closing the path-based re-open TOCTOU window.
    let (copied, dst_file) = tokio::task::spawn_blocking(move || {
        copy_file_range_all(&src_file, &dst_file, len).map(|copied| (copied, dst_file))
    })
    .await
    .map_err(std::io::Error::other)
    .and_then(|res| res)
    .with_context(|| format!("failed copying data to {:?}", dst_path))
    .map_err(|err| Error::new(err, copy_summary))?;
    // account for the bytes ACTUALLY copied, not `len` (the size snapshotted at open): if the source
    // is concurrently truncated, `copy_file_range_all` returns the shorter real count, so using `len`
    // would over-report. `len` still drives the copy loop and the iops-token reservation above.
    prog_track.files_copied.inc();
    prog_track.bytes_copied.add(copied);
    tracing::debug!("setting permissions");
    safedir::set_file_metadata_fd(
        preserve,
        &open_meta,
        dst_file.as_fd(),
        congestion::Side::Destination,
    )
    .await
    .with_context(|| format!("failed setting metadata on {:?}", dst_path))
    .map_err(|err| Error::new(err, copy_summary))?;
    let mut copy_summary = copy_summary;
    // count the file as copied only after all metadata has been applied (actual bytes, see above).
    copy_summary.bytes_copied += copied;
    copy_summary.files_copied += 1;
    Ok(copy_summary)
}

/// Create a symlink fd-relative and apply its metadata through the created link's own handle.
/// `--overwrite`/`--ignore-existing`/dry-run semantics mirror the path-based symlink handling.
#[allow(clippy::too_many_arguments)]
async fn copy_symlink_fd(
    prog_track: &'static progress::Progress,
    src_parent: &Arc<Dir>,
    dst_parent: Option<&Arc<Dir>>,
    dst_name: &OsStr,
    src_path: &std::path::Path,
    dst_path: &std::path::Path,
    src_handle: &Handle,
    settings: &Settings,
    preserve: &preserve::Settings,
    is_fresh: bool,
) -> Result<Summary, Error> {
    // --ignore-existing: skip if the destination already exists (any type). probe via the held dst
    // fd in a real copy; in dry-run there is no held fd (`dst_parent` is None), so probe by path, so
    // the preview output and counters reflect the real skip decision.
    let dst_exists = match dst_parent {
        Some(dst_parent) => dst_parent.child(dst_name).await.is_ok(),
        None => tokio::fs::symlink_metadata(dst_path).await.is_ok(),
    };
    if !is_fresh && settings.ignore_existing && dst_exists {
        if let Some(mode) = settings.dry_run {
            match mode {
                DryRunMode::Brief => {}
                DryRunMode::All => println!("skip symlink {:?}", dst_path),
                DryRunMode::Explain => println!("skip symlink {:?} (destination exists)", dst_path),
            }
        }
        tracing::debug!("destination exists, skipping symlink (--ignore-existing)");
        prog_track.symlinks_unchanged.inc();
        return Ok(Summary {
            symlinks_unchanged: 1,
            ..Default::default()
        });
    }
    if settings.dry_run.is_some() {
        crate::dry_run::report_action("copy", src_path, Some(dst_path), "symlink");
        return Ok(Summary {
            symlinks_created: 1,
            ..Default::default()
        });
    }
    let dst_parent = dst_parent.expect("destination parent must be open for a real copy");
    // read the target AND metadata from the SAME pinned handle (one paired call), so a concurrent
    // same-name symlink swap can't pair one link's target with another link's owner/timestamps
    // (target/metadata fidelity, matching the regular-file path which takes both from one fd).
    let (target, src_meta) = src_handle
        .read_symlink(src_parent.side())
        .await
        .with_context(|| format!("failed reading symlink {:?}", src_path))
        .map_err(|err| Error::new(err, Default::default()))?;
    // fast path: the destination slot is empty, create the link directly.
    match dst_parent.symlink_at(dst_name, &target).await {
        Ok(link_handle) => {
            safedir::set_symlink_metadata_fd(
                preserve,
                &src_meta,
                &link_handle,
                congestion::Side::Destination,
            )
            .await
            .with_context(|| format!("failed setting symlink metadata on {:?}", dst_path))
            .map_err(|err| Error::new(err, Default::default()))?;
            prog_track.symlinks_created.inc();
            Ok(Summary {
                symlinks_created: 1,
                ..Default::default()
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if settings.ignore_existing {
                tracing::debug!("destination exists, skipping symlink (--ignore-existing)");
                prog_track.symlinks_unchanged.inc();
                return Ok(Summary {
                    symlinks_unchanged: 1,
                    ..Default::default()
                });
            }
            if !settings.overwrite {
                return Err(Error::new(
                    anyhow!("failed creating symlink {:?}", dst_path),
                    Default::default(),
                ));
            }
            let dst_handle = dst_parent
                .child(dst_name)
                .await
                .with_context(|| format!("failed reading metadata from dst: {:?}", dst_path))
                .map_err(|err| Error::new(err, Default::default()))?;
            if dst_handle.kind() == EntryKind::Symlink {
                let dst_link = dst_parent
                    .read_link_at(dst_name) // rcp-toctou-allow: destination symlink (overwrite compare), not a source payload
                    .await
                    .with_context(|| format!("failed reading dst symlink: {:?}", dst_path))
                    .map_err(|err| Error::new(err, Default::default()))?;
                if dst_link == target {
                    tracing::debug!("'dst' is a symlink and points to the same location as 'src'");
                    if preserve.symlink.any()
                        && !filecmp::metadata_equal(
                            &settings.overwrite_compare,
                            &src_meta,
                            dst_handle.meta(),
                        )
                    {
                        tracing::debug!("'dst' metadata is different, updating");
                        safedir::set_symlink_metadata_fd(
                            preserve,
                            &src_meta,
                            &dst_handle,
                            congestion::Side::Destination,
                        )
                        .await
                        .with_context(|| {
                            format!("failed setting symlink metadata on {:?}", dst_path)
                        })
                        .map_err(|err| Error::new(err, Default::default()))?;
                        prog_track.symlinks_removed.inc();
                        prog_track.symlinks_created.inc();
                        return Ok(Summary {
                            rm_summary: RmSummary {
                                symlinks_removed: 1,
                                ..Default::default()
                            },
                            symlinks_created: 1,
                            ..Default::default()
                        });
                    }
                    tracing::debug!("symlink already exists, skipping");
                    prog_track.symlinks_unchanged.inc();
                    return Ok(Summary {
                        symlinks_unchanged: 1,
                        ..Default::default()
                    });
                }
                tracing::debug!("'dst' is a symlink but points to a different path, updating");
            } else {
                tracing::info!("'dst' is not a symlink, updating");
            }
            // remove the conflicting destination entry, then create the link fresh.
            let rm_summary = remove_existing(
                prog_track,
                dst_parent,
                dst_name,
                dst_path,
                &dst_handle,
                settings,
            )
            .await?;
            let copy_summary = Summary {
                rm_summary,
                ..Default::default()
            };
            let link_handle = dst_parent
                .symlink_at(dst_name, &target)
                .await
                .with_context(|| format!("failed creating symlink {:?}", dst_path))
                .map_err(|err| Error::new(err, copy_summary))?;
            safedir::set_symlink_metadata_fd(
                preserve,
                &src_meta,
                &link_handle,
                congestion::Side::Destination,
            )
            .await
            .with_context(|| format!("failed setting symlink metadata on {:?}", dst_path))
            .map_err(|err| Error::new(err, copy_summary))?;
            prog_track.symlinks_created.inc();
            Ok(Summary {
                rm_summary,
                symlinks_created: 1,
                ..Default::default()
            })
        }
        Err(error) => Err(Error::new(
            anyhow::Error::new(error).context(format!("failed creating symlink {:?}", dst_path)),
            Default::default(),
        )),
    }
}

/// An opened destination directory plus the bookkeeping the caller needs.
pub(crate) struct DirSlot {
    pub(crate) dir: Arc<Dir>,
    pub(crate) summary: Summary,
    pub(crate) is_fresh: bool,
    pub(crate) we_created: bool,
}

/// Outcome of resolving a destination directory.
pub(crate) enum DirResolution {
    /// Proceed into the directory.
    Proceed(DirSlot),
    /// Skip the whole subtree (e.g. `--ignore-existing` and a non-directory already exists).
    Skip(Summary),
}

/// Create the destination directory `name` under `dst_parent`, or reuse / replace an existing
/// entry per `--overwrite`/`--ignore-existing`. Returns an open [`Dir`] handle to it.
///
/// New directories are created mode `0o700` (writable so children can be populated) — the real
/// source mode is applied later by [`CopyVisitor::dir_post`] after all children are copied, matching
/// the path-based behavior of creating a writable directory and restricting it last.
pub(crate) async fn resolve_dst_dir(
    prog_track: &'static progress::Progress,
    dst_parent: &Arc<Dir>,
    name: &OsStr,
    dst_path: &std::path::Path,
    settings: &Settings,
    is_fresh: bool,
) -> Result<DirResolution, Error> {
    match dst_parent.make_dir(name, 0o700).await {
        Ok(dir) => {
            // freshly created: children may assume the destination is empty (no conflict checks).
            prog_track.directories_created.inc();
            Ok(DirResolution::Proceed(DirSlot {
                dir: Arc::new(dir),
                summary: Summary {
                    directories_created: 1,
                    ..Default::default()
                },
                is_fresh: true,
                we_created: true,
            }))
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            assert!(
                !is_fresh,
                "unexpected pre-existing directory in a fresh destination: {dst_path:?}"
            );
            let dst_handle = dst_parent
                .child(name)
                .await
                .with_context(|| format!("failed reading metadata from dst: {:?}", dst_path))
                .map_err(|err| Error::new(err, Default::default()))?;
            if dst_handle.kind() == EntryKind::Dir {
                // an existing directory: leave it as is and reuse it.
                //
                // N.B. its permissions may stop us writing into it, but the alternative (opening
                // it up while we copy) isn't safe.
                tracing::debug!("'dst' is a directory, leaving it as is");
                let dir = dst_parent
                    .open_dir(name)
                    .await
                    .with_context(|| format!("cannot open existing directory {:?}", dst_path))
                    .map_err(|err| Error::new(err, Default::default()))?;
                prog_track.directories_unchanged.inc();
                Ok(DirResolution::Proceed(DirSlot {
                    dir: Arc::new(dir),
                    summary: Summary {
                        directories_unchanged: 1,
                        ..Default::default()
                    },
                    is_fresh,
                    we_created: false,
                }))
            } else if settings.ignore_existing {
                // a non-directory exists and --ignore-existing was set: skip the whole subtree.
                tracing::debug!(
                    "destination exists but is not a directory, skipping subtree (--ignore-existing)"
                );
                prog_track.directories_unchanged.inc();
                Ok(DirResolution::Skip(Summary {
                    directories_unchanged: 1,
                    ..Default::default()
                }))
            } else if settings.overwrite {
                // a non-directory exists and --overwrite was set: remove it and create the dir.
                tracing::info!("'dst' is not a directory, removing and creating a new one");
                let rm_summary = remove_existing(
                    prog_track,
                    dst_parent,
                    name,
                    dst_path,
                    &dst_handle,
                    settings,
                )
                .await?;
                let dir = dst_parent
                    .make_dir(name, 0o700)
                    .await
                    .with_context(|| format!("cannot create directory {:?}", dst_path))
                    .map_err(|err| {
                        Error::new(
                            err,
                            Summary {
                                rm_summary,
                                ..Default::default()
                            },
                        )
                    })?;
                prog_track.directories_created.inc();
                Ok(DirResolution::Proceed(DirSlot {
                    dir: Arc::new(dir),
                    summary: Summary {
                        rm_summary,
                        directories_created: 1,
                        ..Default::default()
                    },
                    is_fresh: true,
                    we_created: true,
                }))
            } else {
                Err(Error::new(
                    anyhow!(
                        "destination {:?} already exists, did you intend to specify --overwrite?",
                        dst_path
                    ),
                    Default::default(),
                ))
            }
        }
        Err(error) => {
            let error = anyhow::Error::new(error)
                .context(format!("cannot create directory {:?}", dst_path));
            tracing::error!("{:#}", &error);
            Err(Error::new(error, Default::default()))
        }
    }
}

/// Remove an existing destination entry (file, symlink, directory, or special) so a fresh copy can
/// take its place. The returned summary is folded into the caller's copy summary.
///
/// # Removal contract
///
/// The entry was already classified into `dst_handle` (via [`Dir::child`]) by the caller. Removal
/// here is **fd-relative and recheck-guarded**:
///
/// 1. [`Dir::recheck`] re-opens `dst_name` and confirms it is STILL the same inode that was
///    classified (same `dev`/`ino`). If the directory entry was swapped to a different inode
///    between classification and now — an intermediate-dir-to-symlink swap is the canonical
///    attack — `recheck` returns `ESTALE` and we fail closed, removing nothing.
/// 2. The entry is then removed through the held `dst_parent` fd by its kind:
///    - File / Symlink / Special → [`Dir::unlink_at`] (single entry, never follows a symlink).
///    - Empty directory → [`Dir::rmdir_at`].
///    - Non-empty directory → fd-relative recursive [`rm::rm_child`] on the held `dst_parent`
///      (`O_NOFOLLOW|O_DIRECTORY` descent), the same mechanism the remote destination uses.
///
/// Because every removal is fd-relative, it is CONTAINED to the held `dst_parent` directory: even in
/// the residual window between `recheck` and the removal, a final-component swap could at worst
/// remove a different inode that currently occupies `dst_name` WITHIN this directory — it can never
/// escape `dst_parent` into a privileged delete elsewhere (design invariant 6, best-effort). The
/// non-empty-directory subtree is descended through the parent fd with `O_NOFOLLOW`, so a
/// directory→symlink swap mid-walk fails closed (ELOOP/ENOTDIR) rather than following the link.
pub(crate) async fn remove_existing(
    prog_track: &'static progress::Progress,
    dst_parent: &Arc<Dir>,
    dst_name: &OsStr,
    dst_path: &std::path::Path,
    dst_handle: &Handle,
    settings: &Settings,
) -> Result<RmSummary, Error> {
    // recheck: confirm the entry is still the same inode we classified. fail closed on a swap.
    dst_parent
        .recheck(dst_name, dst_handle)
        .await
        .with_context(|| {
            format!(
                "destination {:?} changed identity before removal (possible TOCTOU swap)",
                dst_path
            )
        })
        .map_err(|err| Error::new(err, Default::default()))?;
    match dst_handle.kind() {
        EntryKind::File | EntryKind::Symlink | EntryKind::Special => {
            // capture the removed entry's size before unlinking, to account its bytes (matching the
            // remote destination's overwrite accounting — see rcp::destination::remove_existing_dst).
            let removed_size = {
                use crate::preserve::Metadata as _;
                dst_handle.meta().size()
            };
            // single-entry, fd-relative removal contained to dst_parent; never follows a symlink.
            dst_parent
                .unlink_at(dst_name)
                .await
                .with_context(|| format!("failed removing existing destination {:?}", dst_path))
                .map_err(|err| Error::new(err, Default::default()))?;
            let is_symlink = dst_handle.kind() == EntryKind::Symlink;
            if is_symlink {
                prog_track.symlinks_removed.inc();
            } else {
                prog_track.files_removed.inc();
                prog_track.bytes_removed.add(removed_size);
            }
            Ok(RmSummary {
                files_removed: usize::from(!is_symlink),
                symlinks_removed: usize::from(is_symlink),
                ..Default::default()
            })
        }
        EntryKind::Dir => {
            // try the fd-relative fast path first: an empty directory removes cleanly via rmdir_at.
            match dst_parent.rmdir_at(dst_name).await {
                Ok(()) => {
                    prog_track.directories_removed.inc();
                    Ok(RmSummary {
                        directories_removed: 1,
                        ..Default::default()
                    })
                }
                // POSIX permits either ENOTEMPTY or EEXIST for a non-empty directory.
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::ENOTEMPTY) | Some(libc::EEXIST)
                    ) =>
                {
                    // fd-relative recursive removal of the subtree on the held parent (guarded by
                    // recheck above): descent uses O_NOFOLLOW, so it cannot be redirected out of
                    // dst_parent by a concurrent symlink swap. Mirrors the remote destination path.
                    rm::rm_child(
                        prog_track,
                        dst_parent,
                        dst_name,
                        dst_path,
                        &RmSettings {
                            fail_early: settings.fail_early,
                            filter: None,
                            dry_run: None,
                            time_filter: None,
                        },
                    )
                    .await
                    .map_err(|err| {
                        Error::new(
                            err.source,
                            Summary {
                                rm_summary: err.summary,
                                ..Default::default()
                            },
                        )
                    })
                }
                Err(error) => Err(Error::new(
                    anyhow::Error::new(error)
                        .context(format!("failed removing existing directory {:?}", dst_path)),
                    Default::default(),
                )),
            }
        }
    }
}

#[cfg(test)]
mod copy_tests {
    use crate::testutils;
    use anyhow::Context;
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::PermissionsExt;
    use tracing_test::traced_test;

    use super::*;

    static PROGRESS: std::sync::LazyLock<progress::Progress> =
        std::sync::LazyLock::new(progress::Progress::new);
    static NO_PRESERVE_SETTINGS: std::sync::LazyLock<preserve::Settings> =
        std::sync::LazyLock::new(preserve::preserve_none);
    static DO_PRESERVE_SETTINGS: std::sync::LazyLock<preserve::Settings> =
        std::sync::LazyLock::new(preserve::preserve_all);

    fn settings_with_delete(delete: Option<DeleteSettings>) -> Settings {
        Settings {
            dereference: false,
            fail_early: false,
            overwrite: delete.is_some(), // --delete implies --overwrite
            overwrite_compare: filecmp::MetadataCmpSettings {
                size: true,
                mtime: true,
                ..Default::default()
            },
            overwrite_filter: None,
            ignore_existing: false,
            chunk_size: 0,
            skip_specials: false,
            remote_copy_buffer_size: 0,
            filter: None,
            dry_run: None,
            delete,
        }
    }

    fn delete_on() -> Option<DeleteSettings> {
        Some(DeleteSettings {
            delete_excluded: false,
        })
    }

    // Regression: a source operand whose final component is `.`/`..` (e.g. `rcp tree/sub/.. dst`,
    // `rcp . dst`) must be copied, not rejected — `split_root_operand` canonicalizes it. Uses
    // `tree/sub/..` (== `tree`) rather than `.` to avoid touching the process-wide cwd.
    #[tokio::test]
    async fn copies_dot_dot_source_operand() -> Result<(), anyhow::Error> {
        let tmp = testutils::create_temp_dir().await?;
        let tree = tmp.join("tree");
        tokio::fs::create_dir(&tree).await?;
        tokio::fs::write(tree.join("a.txt"), "hello").await?;
        tokio::fs::create_dir(tree.join("sub")).await?;
        let src = tree.join("sub").join(".."); // == tree
        let dst = tmp.join("dst");
        let summary = copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(None),
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(
            summary.files_copied, 1,
            "the dot-dot source's file must be copied"
        );
        assert_eq!(tokio::fs::read_to_string(dst.join("a.txt")).await?, "hello");
        assert!(
            dst.join("sub").is_dir(),
            "the dot-dot source's subdir must be copied"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn delete_protects_skipped_special_name() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("src_dir");
        let dst = test_path.join("dst_dir");
        tokio::fs::create_dir(&src).await?;
        tokio::fs::write(src.join("file.txt"), "hello").await?;
        // a special file in the source that --skip-specials will skip copying
        nix::unistd::mkfifo(
            &src.join("pipe"),
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )?;
        // pre-existing destination: a counterpart for the skipped special, plus a genuine extra
        tokio::fs::create_dir(&dst).await?;
        tokio::fs::write(dst.join("pipe"), "old").await?;
        tokio::fs::write(dst.join("stale.txt"), "junk").await?;

        let mut settings = settings_with_delete(delete_on());
        settings.skip_specials = true;
        let summary = copy(
            &PROGRESS,
            &src,
            &dst,
            &settings,
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;

        assert_eq!(summary.specials_skipped, 1);
        assert!(dst.join("file.txt").exists());
        assert!(
            dst.join("pipe").exists(),
            "a destination entry matching a skipped special must not be pruned (it has a source counterpart)"
        );
        assert!(!dst.join("stale.txt").exists()); // genuine extra removed
        Ok(())
    }

    // FIX A (PR #247 review): the operand's TRUSTED parent prefix must be resolved following
    // symlinks. `rcp file symlink_to_dir/out` copies into the REAL directory the symlinked parent
    // points at (the parent prefix is trusted up to and including the operand's container); it must
    // NOT fail closed with ELOOP/ENOTDIR. The hardening only applies strictly below the named root.
    #[tokio::test]
    #[traced_test]
    async fn copies_into_symlinked_destination_parent() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("src.txt");
        tokio::fs::write(&src, b"payload").await?;
        // a real destination directory and a symlink-to-dir parent prefix pointing at it.
        let real_dir = test_path.join("real_dst_dir");
        tokio::fs::create_dir(&real_dir).await?;
        let link_dir = test_path.join("link_dst_dir");
        tokio::fs::symlink(&real_dir, &link_dir).await?;
        // destination operand sits UNDER the symlinked (trusted) parent prefix.
        let dst = link_dir.join("out.txt");
        let summary = copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(None),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 1);
        // the file landed in the REAL directory (the symlinked parent was followed).
        let written = tokio::fs::read(real_dir.join("out.txt")).await?;
        assert_eq!(written, b"payload");
        Ok(())
    }

    // FIX A (PR #247 review): a symlinked SOURCE parent prefix is likewise followed —
    // `rcp symlinkdir/src dst` reads the file through the real directory.
    #[tokio::test]
    #[traced_test]
    async fn copies_from_symlinked_source_parent() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        // a real source directory containing the file, reached via a symlinked parent prefix.
        let real_src_dir = test_path.join("real_src_dir");
        tokio::fs::create_dir(&real_src_dir).await?;
        tokio::fs::write(real_src_dir.join("src.txt"), b"payload").await?;
        let link_src_dir = test_path.join("link_src_dir");
        tokio::fs::symlink(&real_src_dir, &link_src_dir).await?;
        let src = link_src_dir.join("src.txt");
        let dst = test_path.join("out.txt");
        let summary = copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(None),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 1);
        let written = tokio::fs::read(&dst).await?;
        assert_eq!(written, b"payload");
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn delete_removes_extraneous_destination_entries() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("foo");
        let dst = test_path.join("bar");
        // initial copy (no delete)
        copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(None),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        // introduce extraneous entries at the destination
        tokio::fs::write(dst.join("extraneous.txt"), b"junk").await?;
        tokio::fs::create_dir(dst.join("extra_dir")).await?;
        tokio::fs::write(dst.join("extra_dir").join("nested.txt"), b"junk").await?;
        // re-copy with --delete
        let summary = copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(delete_on()),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.rm_summary.files_removed, 2); // extraneous.txt + extra_dir/nested.txt
        assert_eq!(summary.rm_summary.directories_removed, 1); // extra_dir
        assert!(!dst.join("extraneous.txt").exists());
        assert!(!dst.join("extra_dir").exists());
        testutils::check_dirs_identical(&src, &dst, testutils::FileEqualityCheck::Basic).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn delete_prunes_extraneous_at_depth() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("foo");
        let dst = test_path.join("bar");
        copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(None),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        // foo/bar is a common subdirectory; place a stale file inside the dst copy of it
        let nested = dst.join("bar");
        assert!(
            nested.is_dir(),
            "expected common subdirectory bar/ to exist at destination"
        );
        tokio::fs::write(nested.join("stale_nested.txt"), b"junk").await?;
        let summary = copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(delete_on()),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert!(
            !nested.join("stale_nested.txt").exists(),
            "stale entry inside a common subdirectory must be pruned"
        );
        assert!(summary.rm_summary.files_removed >= 1);
        testutils::check_dirs_identical(&src, &dst, testutils::FileEqualityCheck::Basic).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn delete_removes_extraneous_symlink() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("foo");
        let dst = test_path.join("bar");
        copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(None),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        // an extraneous symlink at the destination root (no source counterpart)
        tokio::fs::symlink("/nonexistent/target", dst.join("stale_link")).await?;
        let summary = copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(delete_on()),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert!(
            tokio::fs::symlink_metadata(dst.join("stale_link"))
                .await
                .is_err(),
            "extraneous symlink must be removed"
        );
        assert_eq!(summary.rm_summary.symlinks_removed, 1);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn delete_skips_pruning_when_copy_has_errors() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("foo");
        let dst = test_path.join("bar");
        // baseline copy establishes the destination
        copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(None),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        // an extraneous file that --delete would normally prune
        tokio::fs::write(dst.join("extraneous.txt"), b"junk").await?;
        // make a source sub-directory unreadable so traversal fails (fail_early is false).
        // a directory (not a file) is used because --overwrite with mtime-equal files skips
        // copying identical files; a directory's read_dir fails unconditionally when mode is 0o000.
        let unreadable = src.join("baz");
        let original = tokio::fs::metadata(&unreadable).await?.permissions();
        tokio::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).await?;

        let result = copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(delete_on()),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await;

        tokio::fs::set_permissions(&unreadable, original).await?;

        assert!(
            result.is_err(),
            "copy of the unreadable directory should fail"
        );
        assert!(
            dst.join("extraneous.txt").exists(),
            "pruning must be skipped when the copy reported errors"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn check_basic_copy() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 5);
        assert_eq!(summary.symlinks_created, 2);
        assert_eq!(summary.directories_created, 3);
        testutils::check_dirs_identical(
            &test_path.join("foo"),
            &test_path.join("bar"),
            testutils::FileEqualityCheck::Basic,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn no_read_permission() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let filepaths = vec![
            test_path.join("foo").join("0.txt"),
            test_path.join("foo").join("baz"),
        ];
        for fpath in &filepaths {
            // change file permissions to not readable
            tokio::fs::set_permissions(&fpath, std::fs::Permissions::from_mode(0o000)).await?;
        }
        match copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await
        {
            Ok(_) => panic!("Expected the copy to error!"),
            Err(error) => {
                tracing::info!("{}", &error);
                // foo
                // |- 0.txt  // <- no read permission
                // |- bar
                //    |- 1.txt
                //    |- 2.txt
                //    |- 3.txt
                // |- baz   // <- no read permission
                //    |- 4.txt
                //    |- 5.txt -> ../bar/2.txt
                //    |- 6.txt -> (absolute path) .../foo/bar/3.txt
                assert_eq!(error.summary.files_copied, 3);
                assert_eq!(error.summary.symlinks_created, 0);
                assert_eq!(error.summary.directories_created, 2);
            }
        }
        // make source directory same as what we expect destination to be
        for fpath in &filepaths {
            tokio::fs::set_permissions(&fpath, std::fs::Permissions::from_mode(0o700)).await?;
            if tokio::fs::symlink_metadata(fpath).await?.is_file() {
                tokio::fs::remove_file(fpath).await?;
            } else {
                tokio::fs::remove_dir_all(fpath).await?;
            }
        }
        testutils::check_dirs_identical(
            &test_path.join("foo"),
            &test_path.join("bar"),
            testutils::FileEqualityCheck::Basic,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn check_default_mode() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        // set file to executable
        tokio::fs::set_permissions(
            tmp_dir.join("foo").join("0.txt"),
            std::fs::Permissions::from_mode(0o700),
        )
        .await?;
        // set file executable AND also set sticky bit, setuid and setgid
        let exec_sticky_file = tmp_dir.join("foo").join("bar").join("1.txt");
        tokio::fs::set_permissions(&exec_sticky_file, std::fs::Permissions::from_mode(0o3770))
            .await?;
        let test_path = tmp_dir.as_path();
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 5);
        assert_eq!(summary.symlinks_created, 2);
        assert_eq!(summary.directories_created, 3);
        // clear the setuid, setgid and sticky bit for comparison
        tokio::fs::set_permissions(
            &exec_sticky_file,
            std::fs::Permissions::from_mode(
                std::fs::symlink_metadata(&exec_sticky_file)?
                    .permissions()
                    .mode()
                    & 0o0777,
            ),
        )
        .await?;
        testutils::check_dirs_identical(
            &test_path.join("foo"),
            &test_path.join("bar"),
            testutils::FileEqualityCheck::Basic,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn no_write_permission() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        // directory - readable and non-executable
        let non_exec_dir = test_path.join("foo").join("bogey");
        tokio::fs::create_dir(&non_exec_dir).await?;
        tokio::fs::set_permissions(&non_exec_dir, std::fs::Permissions::from_mode(0o400)).await?;
        // directory - readable and executable
        tokio::fs::set_permissions(
            &test_path.join("foo").join("baz"),
            std::fs::Permissions::from_mode(0o500),
        )
        .await?;
        // file
        tokio::fs::set_permissions(
            &test_path.join("foo").join("baz").join("4.txt"),
            std::fs::Permissions::from_mode(0o440),
        )
        .await?;
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 5);
        assert_eq!(summary.symlinks_created, 2);
        assert_eq!(summary.directories_created, 4);
        testutils::check_dirs_identical(
            &test_path.join("foo"),
            &test_path.join("bar"),
            testutils::FileEqualityCheck::Basic,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn dereference() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        // make files pointed to by symlinks have different permissions than the symlink itself
        let src1 = &test_path.join("foo").join("bar").join("2.txt");
        let src2 = &test_path.join("foo").join("bar").join("3.txt");
        let test_mode = 0o440;
        for f in [src1, src2] {
            tokio::fs::set_permissions(f, std::fs::Permissions::from_mode(test_mode)).await?;
        }
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: true, // <- important!
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 7);
        assert_eq!(summary.symlinks_created, 0);
        assert_eq!(summary.directories_created, 3);
        // ...
        // |- baz
        //    |- 4.txt
        //    |- 5.txt -> ../bar/2.txt
        //    |- 6.txt -> (absolute path) .../foo/bar/3.txt
        let dst1 = &test_path.join("bar").join("baz").join("5.txt");
        let dst2 = &test_path.join("bar").join("baz").join("6.txt");
        for f in [dst1, dst2] {
            let metadata = tokio::fs::symlink_metadata(f)
                .await
                .with_context(|| format!("failed reading metadata from {:?}", &f))?;
            assert!(metadata.is_file());
            // check that the permissions are the same as the source file modulo no sticky bit, setuid and setgid
            assert_eq!(metadata.permissions().mode() & 0o777, test_mode);
        }
        Ok(())
    }

    async fn cp_compare(
        cp_args: &[&str],
        rcp_settings: &Settings,
        preserve: bool,
    ) -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        // run a cp command to copy the files
        let cp_output = tokio::process::Command::new("cp")
            .args(cp_args)
            .arg(test_path.join("foo"))
            .arg(test_path.join("bar"))
            .output()
            .await?;
        assert!(cp_output.status.success());
        // now run rcp
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("baz"),
            rcp_settings,
            if preserve {
                &DO_PRESERVE_SETTINGS
            } else {
                &NO_PRESERVE_SETTINGS
            },
            false,
        )
        .await?;
        if rcp_settings.dereference {
            assert_eq!(summary.files_copied, 7);
            assert_eq!(summary.symlinks_created, 0);
        } else {
            assert_eq!(summary.files_copied, 5);
            assert_eq!(summary.symlinks_created, 2);
        }
        assert_eq!(summary.directories_created, 3);
        testutils::check_dirs_identical(
            &test_path.join("bar"),
            &test_path.join("baz"),
            if preserve {
                testutils::FileEqualityCheck::Timestamp
            } else {
                testutils::FileEqualityCheck::Basic
            },
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_compat() -> Result<(), anyhow::Error> {
        cp_compare(
            &["-r"],
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            false,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_compat_preserve() -> Result<(), anyhow::Error> {
        cp_compare(
            &["-r", "-p"],
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            true,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_compat_dereference() -> Result<(), anyhow::Error> {
        cp_compare(
            &["-r", "-L"],
            &Settings {
                dereference: true,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            false,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_compat_preserve_and_dereference() -> Result<(), anyhow::Error> {
        cp_compare(
            &["-r", "-p", "-L"],
            &Settings {
                dereference: true,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            true,
        )
        .await?;
        Ok(())
    }

    async fn setup_test_dir_and_copy() -> Result<std::path::PathBuf, anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 5);
        assert_eq!(summary.symlinks_created, 2);
        assert_eq!(summary.directories_created, 3);
        Ok(tmp_dir)
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_overwrite_basic() -> Result<(), anyhow::Error> {
        let tmp_dir = setup_test_dir_and_copy().await?;
        let output_path = &tmp_dir.join("bar");
        {
            // bar
            // |- 0.txt
            // |- bar  <---------------------------------------- REMOVE
            //    |- 1.txt  <----------------------------------- REMOVE
            //    |- 2.txt  <----------------------------------- REMOVE
            //    |- 3.txt  <----------------------------------- REMOVE
            // |- baz
            //    |- 4.txt
            //    |- 5.txt -> ../bar/2.txt <-------------------- REMOVE
            //    |- 6.txt -> (absolute path) .../foo/bar/3.txt
            let summary = rm::rm(
                &PROGRESS,
                &output_path.join("bar"),
                &RmSettings {
                    fail_early: false,
                    filter: None,
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz").join("5.txt"),
                    &RmSettings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                )
                .await?;
            assert_eq!(summary.files_removed, 3);
            assert_eq!(summary.symlinks_removed, 1);
            assert_eq!(summary.directories_removed, 1);
        }
        let summary = copy(
            &PROGRESS,
            &tmp_dir.join("foo"),
            output_path,
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: true, // <- important!
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 3);
        assert_eq!(summary.symlinks_created, 1);
        assert_eq!(summary.directories_created, 1);
        testutils::check_dirs_identical(
            &tmp_dir.join("foo"),
            output_path,
            testutils::FileEqualityCheck::Timestamp,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_overwrite_dir_file() -> Result<(), anyhow::Error> {
        let tmp_dir = setup_test_dir_and_copy().await?;
        let output_path = &tmp_dir.join("bar");
        {
            // bar
            // |- 0.txt
            // |- bar
            //    |- 1.txt  <------------------------------------- REMOVE
            //    |- 2.txt
            //    |- 3.txt
            // |- baz  <------------------------------------------ REMOVE
            //    |- 4.txt  <------------------------------------- REMOVE
            //    |- 5.txt -> ../bar/2.txt <---------------------- REMOVE
            //    |- 6.txt -> (absolute path) .../foo/bar/3.txt <- REMOVE
            let summary = rm::rm(
                &PROGRESS,
                &output_path.join("bar").join("1.txt"),
                &RmSettings {
                    fail_early: false,
                    filter: None,
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz"),
                    &RmSettings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                )
                .await?;
            assert_eq!(summary.files_removed, 2);
            assert_eq!(summary.symlinks_removed, 2);
            assert_eq!(summary.directories_removed, 1);
        }
        {
            // replace bar/1.txt file with a directory
            tokio::fs::create_dir(&output_path.join("bar").join("1.txt")).await?;
            // replace baz directory with a file
            tokio::fs::write(&output_path.join("baz"), "baz").await?;
        }
        let summary = copy(
            &PROGRESS,
            &tmp_dir.join("foo"),
            output_path,
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: true, // <- important!
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.rm_summary.files_removed, 1);
        assert_eq!(summary.rm_summary.symlinks_removed, 0);
        assert_eq!(summary.rm_summary.directories_removed, 1);
        assert_eq!(summary.files_copied, 2);
        assert_eq!(summary.symlinks_created, 2);
        assert_eq!(summary.directories_created, 1);
        testutils::check_dirs_identical(
            &tmp_dir.join("foo"),
            output_path,
            testutils::FileEqualityCheck::Timestamp,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_overwrite_symlink_file() -> Result<(), anyhow::Error> {
        let tmp_dir = setup_test_dir_and_copy().await?;
        let output_path = &tmp_dir.join("bar");
        {
            // bar
            // |- 0.txt
            // |- baz
            //    |- 4.txt  <------------------------------------- REMOVE
            //    |- 5.txt -> ../bar/2.txt <---------------------- REMOVE
            // ...
            let summary = rm::rm(
                &PROGRESS,
                &output_path.join("baz").join("4.txt"),
                &RmSettings {
                    fail_early: false,
                    filter: None,
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz").join("5.txt"),
                    &RmSettings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                )
                .await?;
            assert_eq!(summary.files_removed, 1);
            assert_eq!(summary.symlinks_removed, 1);
            assert_eq!(summary.directories_removed, 0);
        }
        {
            // replace baz/4.txt file with a symlink
            tokio::fs::symlink("../0.txt", &output_path.join("baz").join("4.txt")).await?;
            // replace baz/5.txt symlink with a file
            tokio::fs::write(&output_path.join("baz").join("5.txt"), "baz").await?;
        }
        let summary = copy(
            &PROGRESS,
            &tmp_dir.join("foo"),
            output_path,
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: true, // <- important!
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.rm_summary.files_removed, 1);
        assert_eq!(summary.rm_summary.symlinks_removed, 1);
        assert_eq!(summary.rm_summary.directories_removed, 0);
        assert_eq!(summary.files_copied, 1);
        assert_eq!(summary.symlinks_created, 1);
        assert_eq!(summary.directories_created, 0);
        testutils::check_dirs_identical(
            &tmp_dir.join("foo"),
            output_path,
            testutils::FileEqualityCheck::Timestamp,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_overwrite_symlink_dir() -> Result<(), anyhow::Error> {
        let tmp_dir = setup_test_dir_and_copy().await?;
        let output_path = &tmp_dir.join("bar");
        {
            // bar
            // |- 0.txt
            // |- bar  <------------------------------------------ REMOVE
            //    |- 1.txt  <------------------------------------- REMOVE
            //    |- 2.txt  <------------------------------------- REMOVE
            //    |- 3.txt  <------------------------------------- REMOVE
            // |- baz
            //    |- 5.txt -> ../bar/2.txt <---------------------- REMOVE
            // ...
            let summary = rm::rm(
                &PROGRESS,
                &output_path.join("bar"),
                &RmSettings {
                    fail_early: false,
                    filter: None,
                    dry_run: None,
                    time_filter: None,
                },
            )
            .await?
                + rm::rm(
                    &PROGRESS,
                    &output_path.join("baz").join("5.txt"),
                    &RmSettings {
                        fail_early: false,
                        filter: None,
                        dry_run: None,
                        time_filter: None,
                    },
                )
                .await?;
            assert_eq!(summary.files_removed, 3);
            assert_eq!(summary.symlinks_removed, 1);
            assert_eq!(summary.directories_removed, 1);
        }
        {
            // replace bar directory with a symlink
            tokio::fs::symlink("0.txt", &output_path.join("bar")).await?;
            // replace baz/5.txt symlink with a directory
            tokio::fs::create_dir(&output_path.join("baz").join("5.txt")).await?;
        }
        let summary = copy(
            &PROGRESS,
            &tmp_dir.join("foo"),
            output_path,
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: true, // <- important!
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.rm_summary.files_removed, 0);
        assert_eq!(summary.rm_summary.symlinks_removed, 1);
        assert_eq!(summary.rm_summary.directories_removed, 1);
        assert_eq!(summary.files_copied, 3);
        assert_eq!(summary.symlinks_created, 1);
        assert_eq!(summary.directories_created, 1);
        assert_eq!(summary.files_unchanged, 2);
        assert_eq!(summary.symlinks_unchanged, 1);
        assert_eq!(summary.directories_unchanged, 2);
        testutils::check_dirs_identical(
            &tmp_dir.join("foo"),
            output_path,
            testutils::FileEqualityCheck::Timestamp,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_overwrite_error() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let summary = copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &NO_PRESERVE_SETTINGS, // we want timestamps to differ!
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 5);
        assert_eq!(summary.symlinks_created, 2);
        assert_eq!(summary.directories_created, 3);
        let source_path = &test_path.join("foo");
        let output_path = &tmp_dir.join("bar");
        // unreadable
        tokio::fs::set_permissions(
            &source_path.join("bar"),
            std::fs::Permissions::from_mode(0o000),
        )
        .await?;
        tokio::fs::set_permissions(
            &source_path.join("baz").join("4.txt"),
            std::fs::Permissions::from_mode(0o000),
        )
        .await?;
        // bar
        // |- 0.txt
        // |- bar  <---------------------------------------- NON READABLE
        // |- baz
        //    |- 4.txt  <----------------------------------- NON READABLE
        //    |- 5.txt -> ../bar/2.txt
        //    |- 6.txt -> (absolute path) .../foo/bar/3.txt
        match copy(
            &PROGRESS,
            &tmp_dir.join("foo"),
            output_path,
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: true, // <- important!
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await
        {
            Ok(_) => panic!("Expected the copy to error!"),
            Err(error) => {
                tracing::info!("{}", &error);
                assert_eq!(error.summary.files_copied, 1);
                assert_eq!(error.summary.symlinks_created, 2);
                assert_eq!(error.summary.directories_created, 0);
                assert_eq!(error.summary.rm_summary.files_removed, 2);
                assert_eq!(error.summary.rm_summary.symlinks_removed, 2);
                assert_eq!(error.summary.rm_summary.directories_removed, 0);
            }
        }
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_dereference_symlink_chain() -> Result<(), anyhow::Error> {
        // Create a fresh temporary directory to avoid conflicts
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // Create a chain of symlinks: foo -> bar -> baz (actual file)
        let baz_file = test_path.join("baz_file.txt");
        tokio::fs::write(&baz_file, "final content").await?;
        let bar_link = test_path.join("bar_link");
        let foo_link = test_path.join("foo_link");
        // Create chain: foo_link -> bar_link -> baz_file.txt
        tokio::fs::symlink(&baz_file, &bar_link).await?;
        tokio::fs::symlink(&bar_link, &foo_link).await?;
        // Create source directory with the symlink chain
        let src_dir = test_path.join("src_chain");
        tokio::fs::create_dir(&src_dir).await?;
        // Copy the chain into the source directory
        tokio::fs::symlink("../foo_link", &src_dir.join("foo")).await?;
        tokio::fs::symlink("../bar_link", &src_dir.join("bar")).await?;
        tokio::fs::symlink("../baz_file.txt", &src_dir.join("baz")).await?;
        // Test with dereference - should copy 3 files with same content
        let summary = copy(
            &PROGRESS,
            &src_dir,
            &test_path.join("dst_with_deref"),
            &Settings {
                dereference: true, // <- important!
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 3); // foo, bar, baz all copied as files
        assert_eq!(summary.symlinks_created, 0); // dereference is set
        assert_eq!(summary.directories_created, 1);
        let dst_dir = test_path.join("dst_with_deref");
        // Verify all three are now regular files with the same content
        let foo_content = tokio::fs::read_to_string(dst_dir.join("foo")).await?;
        let bar_content = tokio::fs::read_to_string(dst_dir.join("bar")).await?;
        let baz_content = tokio::fs::read_to_string(dst_dir.join("baz")).await?;
        assert_eq!(foo_content, "final content");
        assert_eq!(bar_content, "final content");
        assert_eq!(baz_content, "final content");
        // Verify they are all regular files, not symlinks
        assert!(dst_dir.join("foo").is_file());
        assert!(dst_dir.join("bar").is_file());
        assert!(dst_dir.join("baz").is_file());
        assert!(!dst_dir.join("foo").is_symlink());
        assert!(!dst_dir.join("bar").is_symlink());
        assert!(!dst_dir.join("baz").is_symlink());
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_dereference_symlink_to_directory() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // Create a directory with specific permissions and content
        let target_dir = test_path.join("target_dir");
        tokio::fs::create_dir(&target_dir).await?;
        tokio::fs::set_permissions(&target_dir, std::fs::Permissions::from_mode(0o755)).await?;
        // Add some files to the directory
        tokio::fs::write(target_dir.join("file1.txt"), "content1").await?;
        tokio::fs::write(target_dir.join("file2.txt"), "content2").await?;
        tokio::fs::set_permissions(
            &target_dir.join("file1.txt"),
            std::fs::Permissions::from_mode(0o644),
        )
        .await?;
        tokio::fs::set_permissions(
            &target_dir.join("file2.txt"),
            std::fs::Permissions::from_mode(0o600),
        )
        .await?;
        // Create a symlink pointing to the directory
        let dir_symlink = test_path.join("dir_symlink");
        tokio::fs::symlink(&target_dir, &dir_symlink).await?;
        // Test copying the symlink with dereference - should copy as a directory
        let summary = copy(
            &PROGRESS,
            &dir_symlink,
            &test_path.join("copied_dir"),
            &Settings {
                dereference: true, // <- important!
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 2); // file1.txt, file2.txt
        assert_eq!(summary.symlinks_created, 0); // dereference is set
        assert_eq!(summary.directories_created, 1); // copied_dir
        let copied_dir = test_path.join("copied_dir");
        // Verify the directory and its contents were copied
        assert!(copied_dir.is_dir());
        assert!(!copied_dir.is_symlink()); // Should be a real directory, not a symlink
        // Verify files were copied with correct content
        let file1_content = tokio::fs::read_to_string(copied_dir.join("file1.txt")).await?;
        let file2_content = tokio::fs::read_to_string(copied_dir.join("file2.txt")).await?;
        assert_eq!(file1_content, "content1");
        assert_eq!(file2_content, "content2");
        // Verify permissions were preserved
        let copied_dir_metadata = tokio::fs::metadata(&copied_dir).await?;
        let file1_metadata = tokio::fs::metadata(copied_dir.join("file1.txt")).await?;
        let file2_metadata = tokio::fs::metadata(copied_dir.join("file2.txt")).await?;
        assert_eq!(copied_dir_metadata.permissions().mode() & 0o777, 0o755);
        assert_eq!(file1_metadata.permissions().mode() & 0o777, 0o644);
        assert_eq!(file2_metadata.permissions().mode() & 0o777, 0o600);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_dereference_permissions_preserved() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // Create files with specific permissions
        let file1 = test_path.join("file1.txt");
        let file2 = test_path.join("file2.txt");
        tokio::fs::write(&file1, "content1").await?;
        tokio::fs::write(&file2, "content2").await?;
        tokio::fs::set_permissions(&file1, std::fs::Permissions::from_mode(0o755)).await?;
        tokio::fs::set_permissions(&file2, std::fs::Permissions::from_mode(0o640)).await?;
        // Create symlinks pointing to these files
        let symlink1 = test_path.join("symlink1");
        let symlink2 = test_path.join("symlink2");
        tokio::fs::symlink(&file1, &symlink1).await?;
        tokio::fs::symlink(&file2, &symlink2).await?;
        // Test copying symlinks with dereference and preserve
        let summary1 = copy(
            &PROGRESS,
            &symlink1,
            &test_path.join("copied_file1.txt"),
            &Settings {
                dereference: true, // <- important!
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings::default(),
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &DO_PRESERVE_SETTINGS, // <- important!
            false,
        )
        .await?;
        let summary2 = copy(
            &PROGRESS,
            &symlink2,
            &test_path.join("copied_file2.txt"),
            &Settings {
                dereference: true,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings::default(),
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary1.files_copied, 1);
        assert_eq!(summary1.symlinks_created, 0);
        assert_eq!(summary2.files_copied, 1);
        assert_eq!(summary2.symlinks_created, 0);
        let copied1 = test_path.join("copied_file1.txt");
        let copied2 = test_path.join("copied_file2.txt");
        // Verify files are regular files, not symlinks
        assert!(copied1.is_file());
        assert!(!copied1.is_symlink());
        assert!(copied2.is_file());
        assert!(!copied2.is_symlink());
        // Verify content was copied correctly
        let content1 = tokio::fs::read_to_string(&copied1).await?;
        let content2 = tokio::fs::read_to_string(&copied2).await?;
        assert_eq!(content1, "content1");
        assert_eq!(content2, "content2");
        // Verify permissions from the target files were preserved (not symlink permissions)
        let copied1_metadata = tokio::fs::metadata(&copied1).await?;
        let copied2_metadata = tokio::fs::metadata(&copied2).await?;
        assert_eq!(copied1_metadata.permissions().mode() & 0o777, 0o755);
        assert_eq!(copied2_metadata.permissions().mode() & 0o777, 0o640);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cp_dereference_dir() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        // symlink bar to bar-link
        tokio::fs::symlink("bar", &tmp_dir.join("foo").join("bar-link")).await?;
        // symlink bar-link to bar-link-link
        tokio::fs::symlink("bar-link", &tmp_dir.join("foo").join("bar-link-link")).await?;
        let summary = copy(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &Settings {
                dereference: true, // <- important!
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 13); // 0.txt, 3x bar/(1.txt, 2.txt, 3.txt), baz/(4.txt, 5.txt, 6.txt)
        assert_eq!(summary.symlinks_created, 0); // dereference is set
        assert_eq!(summary.directories_created, 5);
        // check_dirs_identical doesn't handle dereference so let's do it manually
        tokio::process::Command::new("cp")
            .args(["-r", "-L"])
            .arg(tmp_dir.join("foo"))
            .arg(tmp_dir.join("bar-cp"))
            .output()
            .await?;
        testutils::check_dirs_identical(
            &tmp_dir.join("bar"),
            &tmp_dir.join("bar-cp"),
            testutils::FileEqualityCheck::Basic,
        )
        .await?;
        Ok(())
    }

    /// Tests to verify error messages include root causes for debugging
    mod error_message_tests {
        use super::*;

        /// Helper to extract full error message with chain
        fn get_full_error_message(error: &Error) -> String {
            format!("{:#}", error.source)
        }

        #[tokio::test]
        #[traced_test]
        async fn test_nonexistent_source_includes_root_cause() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;

            let result = copy(
                &PROGRESS,
                &tmp_dir.join("does_not_exist.txt"),
                &tmp_dir.join("dest.txt"),
                &Settings {
                    dereference: false,
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
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await;

            assert!(result.is_err());
            let err_msg = get_full_error_message(&result.unwrap_err());

            assert!(
                err_msg.to_lowercase().contains("no such file")
                    || err_msg.to_lowercase().contains("not found")
                    || err_msg.contains("ENOENT"),
                "Error message must include file not found text. Got: {}",
                err_msg
            );
            Ok(())
        }

        #[tokio::test]
        #[traced_test]
        async fn test_unreadable_directory_includes_root_cause() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let unreadable_dir = tmp_dir.join("unreadable_dir");
            tokio::fs::create_dir(&unreadable_dir).await?;
            tokio::fs::set_permissions(&unreadable_dir, std::fs::Permissions::from_mode(0o000))
                .await?;

            let result = copy(
                &PROGRESS,
                &unreadable_dir,
                &tmp_dir.join("dest"),
                &Settings {
                    dereference: false,
                    fail_early: true,
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
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await;

            assert!(result.is_err());
            let err_msg = get_full_error_message(&result.unwrap_err());

            assert!(
                err_msg.to_lowercase().contains("permission")
                    || err_msg.contains("EACCES")
                    || err_msg.contains("denied"),
                "Error message must include permission-related text. Got: {}",
                err_msg
            );

            // Clean up - restore permissions so cleanup can remove it
            tokio::fs::set_permissions(&unreadable_dir, std::fs::Permissions::from_mode(0o700))
                .await?;
            Ok(())
        }

        #[tokio::test]
        #[traced_test]
        async fn test_destination_permission_error_includes_root_cause() -> Result<(), anyhow::Error>
        {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            let readonly_parent = test_path.join("readonly_dest");
            tokio::fs::create_dir(&readonly_parent).await?;
            tokio::fs::set_permissions(&readonly_parent, std::fs::Permissions::from_mode(0o555))
                .await?;

            let result = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &readonly_parent.join("copy"),
                &Settings {
                    dereference: false,
                    fail_early: true,
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
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await;

            // restore permissions so cleanup succeeds even when copy fails
            tokio::fs::set_permissions(&readonly_parent, std::fs::Permissions::from_mode(0o755))
                .await?;

            assert!(result.is_err(), "copy into read-only parent should fail");
            let err_msg = get_full_error_message(&result.unwrap_err());

            assert!(
                err_msg.to_lowercase().contains("permission denied") || err_msg.contains("EACCES"),
                "Error message must include permission denied text. Got: {}",
                err_msg
            );
            Ok(())
        }
    }

    mod empty_dir_cleanup_tests {
        use super::*;
        use crate::filter::FilterSettings;
        use std::path::Path;
        #[test]
        fn test_check_empty_dir_cleanup_no_filter() {
            // when no filter, always keep
            assert_eq!(
                check_empty_dir_cleanup(None, true, false, Path::new("any"), false, false),
                EmptyDirAction::Keep
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_something_copied() {
            // when content was copied, keep
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(Some(&filter), true, true, Path::new("any"), false, false),
                EmptyDirAction::Keep
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_not_created() {
            // when we didn't create the directory, keep
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(
                    Some(&filter),
                    false,
                    false,
                    Path::new("any"),
                    false,
                    false
                ),
                EmptyDirAction::Keep
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_directly_matched() {
            // when directory directly matches include pattern, keep
            let mut filter = FilterSettings::new();
            filter.add_include("target/").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(
                    Some(&filter),
                    true,
                    false,
                    Path::new("target"),
                    false,
                    false
                ),
                EmptyDirAction::Keep
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_traversed_only() {
            // when directory was only traversed, remove
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(Some(&filter), true, false, Path::new("src"), false, false),
                EmptyDirAction::Remove
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_dry_run() {
            // in dry-run mode, skip instead of remove
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(Some(&filter), true, false, Path::new("src"), false, true),
                EmptyDirAction::DryRunSkip
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_root_always_kept() {
            // root directory is never removed, even with filter and nothing copied
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(Some(&filter), true, false, Path::new(""), true, false),
                EmptyDirAction::Keep
            );
        }
        #[test]
        fn test_check_empty_dir_cleanup_root_kept_in_dry_run() {
            // root directory is kept even in dry-run mode
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            assert_eq!(
                check_empty_dir_cleanup(Some(&filter), true, false, Path::new(""), true, true),
                EmptyDirAction::Keep
            );
        }
    }

    /// Verify that directory metadata is applied even when child operations fail.
    /// This is a regression test for a bug where directory permissions were not preserved
    /// when copying with fail_early=false and some children failed to copy.
    #[tokio::test]
    #[traced_test]
    async fn test_directory_metadata_applied_on_child_error() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // create source directory with specific permissions
        let src_dir = test_path.join("src");
        tokio::fs::create_dir(&src_dir).await?;
        tokio::fs::set_permissions(&src_dir, std::fs::Permissions::from_mode(0o750)).await?;
        // create a readable file and an unreadable file inside
        let readable_file = src_dir.join("readable.txt");
        tokio::fs::write(&readable_file, "content").await?;
        let unreadable_file = src_dir.join("unreadable.txt");
        tokio::fs::write(&unreadable_file, "secret").await?;
        tokio::fs::set_permissions(&unreadable_file, std::fs::Permissions::from_mode(0o000))
            .await?;
        let dst_dir = test_path.join("dst");
        // copy with fail_early=false and preserve=all
        let result = copy(
            &PROGRESS,
            &src_dir,
            &dst_dir,
            &Settings {
                dereference: false,
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
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await;
        // restore permissions so cleanup can succeed
        tokio::fs::set_permissions(&unreadable_file, std::fs::Permissions::from_mode(0o644))
            .await?;
        // verify the operation returned an error (unreadable file should fail)
        assert!(result.is_err(), "copy should fail due to unreadable file");
        let error = result.unwrap_err();
        // verify some files were copied (the readable one)
        assert_eq!(error.summary.files_copied, 1);
        assert_eq!(error.summary.directories_created, 1);
        // verify the destination directory exists and has the correct permissions
        let dst_metadata = tokio::fs::metadata(&dst_dir).await?;
        assert!(dst_metadata.is_dir());
        let actual_mode = dst_metadata.permissions().mode() & 0o7777;
        assert_eq!(
            actual_mode, 0o750,
            "directory should have preserved source permissions (0o750), got {:o}",
            actual_mode
        );
        Ok(())
    }

    /// A child copy failure inside a filter-traversal-only directory must surface as an overall
    /// error even though the now-empty directory is pruned (regression: the empty-dir cleanup path
    /// returned Ok and swallowed the collected child error, so a failed copy looked successful).
    #[tokio::test]
    #[traced_test]
    async fn pruned_empty_traversal_dir_still_surfaces_child_error() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        let src_dir = test_path.join("src");
        let sub = src_dir.join("sub");
        tokio::fs::create_dir_all(&sub).await?;
        // the only entry under `sub` is unreadable, so its copy fails and nothing lands in `sub`,
        // leaving it an empty traversal-only directory that the filter cleanup prunes.
        let unreadable = sub.join("file.txt");
        tokio::fs::write(&unreadable, "secret").await?;
        tokio::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).await?;
        let dst_dir = test_path.join("dst");
        let mut filter = crate::filter::FilterSettings::new();
        filter.add_include("*.txt").unwrap();
        let result = copy(
            &PROGRESS,
            &src_dir,
            &dst_dir,
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
                overwrite_compare: Default::default(),
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: Some(filter),
                dry_run: None,
                delete: None,
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await;
        // restore permissions so the temp dir can be cleaned up
        tokio::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o644)).await?;
        assert!(
            result.is_err(),
            "the failed child copy must surface even though the empty traversal dir was pruned"
        );
        // the empty traversal directory itself was still cleaned up (count adjustment preserved).
        assert!(
            !dst_dir.join("sub").exists(),
            "empty traversal directory should have been removed"
        );
        Ok(())
    }

    /// Verify that fail-early does not apply parent directory metadata after a child fails.
    #[tokio::test]
    #[traced_test]
    async fn test_fail_early_does_not_apply_parent_directory_metadata_after_child_error()
    -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        let src_dir = test_path.join("src");
        tokio::fs::create_dir(&src_dir).await?;
        tokio::fs::write(src_dir.join("readable.txt"), "content").await?;
        let unreadable_file = src_dir.join("unreadable.txt");
        tokio::fs::write(&unreadable_file, "secret").await?;
        tokio::fs::set_permissions(&unreadable_file, std::fs::Permissions::from_mode(0o000))
            .await?;
        let fixed_secs = 946684800;
        let fixed_nsec = 123_456_789;
        let fixed_time = nix::sys::time::TimeSpec::new(fixed_secs, fixed_nsec);
        nix::sys::stat::utimensat(
            nix::fcntl::AT_FDCWD,
            &src_dir,
            &fixed_time,
            &fixed_time,
            nix::sys::stat::UtimensatFlags::NoFollowSymlink,
        )?;
        let src_metadata = tokio::fs::metadata(&src_dir).await?;
        let dst_dir = test_path.join("dst");
        let result = copy(
            &PROGRESS,
            &src_dir,
            &dst_dir,
            &Settings {
                dereference: false,
                fail_early: true,
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
            },
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await;
        tokio::fs::set_permissions(&unreadable_file, std::fs::Permissions::from_mode(0o644))
            .await?;
        assert!(result.is_err(), "copy should fail due to unreadable file");
        let dst_metadata = tokio::fs::metadata(&dst_dir).await?;
        assert!(dst_metadata.is_dir());
        assert_ne!(
            (dst_metadata.mtime(), dst_metadata.mtime_nsec()),
            (src_metadata.mtime(), src_metadata.mtime_nsec()),
            "fail-early should return before applying preserved directory timestamps"
        );
        Ok(())
    }
    mod filter_tests {
        use super::*;
        use crate::filter::FilterSettings;
        /// Test that path-based patterns (with /) work correctly with nested paths.
        /// This test exposes the bug where only entry_name is passed to the filter
        /// instead of the relative path.
        #[tokio::test]
        #[traced_test]
        async fn test_path_pattern_matches_nested_files() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // test directory structure from setup_test_dir:
            // foo/
            //   0.txt
            //   bar/
            //     1.txt
            //     2.txt
            //   baz/
            //     3.txt -> ../0.txt (symlink)
            //     4.txt
            //     5 -> ../bar (symlink)
            // create filter that should match bar/*.txt (files in bar directory)
            let mut filter = FilterSettings::new();
            filter.add_include("bar/*.txt").unwrap();
            let summary = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // should only copy files matching bar/*.txt pattern
            // bar/1.txt, bar/2.txt, and bar/3.txt should be copied
            assert_eq!(
                summary.files_copied, 3,
                "should copy 3 files matching bar/*.txt"
            );
            // verify the right files exist
            assert!(
                test_path.join("dst/bar/1.txt").exists(),
                "bar/1.txt should be copied"
            );
            assert!(
                test_path.join("dst/bar/2.txt").exists(),
                "bar/2.txt should be copied"
            );
            assert!(
                test_path.join("dst/bar/3.txt").exists(),
                "bar/3.txt should be copied"
            );
            // verify files outside the pattern don't exist
            assert!(
                !test_path.join("dst/0.txt").exists(),
                "0.txt should not be copied"
            );
            Ok(())
        }
        /// Test that anchored patterns (starting with /) match only at root.
        #[tokio::test]
        #[traced_test]
        async fn test_anchored_pattern_matches_only_at_root() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // create filter that should match /bar/** (bar directory and all its contents)
            let mut filter = FilterSettings::new();
            filter.add_include("/bar/**").unwrap();
            let summary = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // should only copy bar directory and its contents
            assert!(
                test_path.join("dst/bar").exists(),
                "bar directory should be copied"
            );
            assert!(
                !test_path.join("dst/baz").exists(),
                "baz directory should not be copied"
            );
            assert!(
                !test_path.join("dst/0.txt").exists(),
                "0.txt should not be copied"
            );
            // verify summary counts
            assert_eq!(
                summary.files_copied, 3,
                "should copy 3 files in bar (1.txt, 2.txt, 3.txt)"
            );
            assert_eq!(
                summary.directories_created, 2,
                "should create 2 directories (root dst + bar)"
            );
            // skipped: 0.txt (file) and baz (directory) - baz contents not counted since dir is skipped
            assert_eq!(summary.files_skipped, 1, "should skip 1 file (0.txt)");
            assert_eq!(
                summary.directories_skipped, 1,
                "should skip 1 directory (baz)"
            );
            Ok(())
        }
        /// Test that double-star patterns (**) match across directories.
        #[tokio::test]
        #[traced_test]
        async fn test_double_star_pattern_matches_nested() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            // create filter that should match all .txt files at any depth
            let mut filter = FilterSettings::new();
            filter.add_include("**/*.txt").unwrap();
            let summary = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // should copy all .txt files: 0.txt, bar/1.txt, bar/2.txt, bar/3.txt, baz/4.txt
            assert_eq!(
                summary.files_copied, 5,
                "should copy all 5 .txt files with **/*.txt pattern"
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
            let result = copy(
                &PROGRESS,
                &test_path.join("foo/0.txt"), // single file source
                &test_path.join("dst.txt"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // the file should NOT be copied because it matches the exclude pattern
            assert_eq!(
                result.files_copied, 0,
                "file matching exclude pattern should not be copied"
            );
            assert!(
                !test_path.join("dst.txt").exists(),
                "excluded file should not exist at destination"
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
            let result = copy(
                &PROGRESS,
                &test_path.join("excluded_dir"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // directory should NOT be copied because it matches exclude pattern
            assert_eq!(
                result.directories_created, 0,
                "root directory matching exclude should not be created"
            );
            assert!(
                !test_path.join("dst").exists(),
                "excluded root directory should not exist at destination"
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
            let result = copy(
                &PROGRESS,
                &test_path.join("excluded_link"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // symlink should NOT be copied because it matches exclude pattern
            assert_eq!(
                result.symlinks_created, 0,
                "root symlink matching exclude should not be created"
            );
            assert!(
                !test_path.join("dst").exists(),
                "excluded root symlink should not exist at destination"
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
            // include all .txt files, but exclude bar/2.txt specifically
            let mut filter = FilterSettings::new();
            filter.add_include("**/*.txt").unwrap();
            filter.add_exclude("bar/2.txt").unwrap();
            let summary = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // should copy: 0.txt, bar/1.txt, bar/3.txt, baz/4.txt = 4 files
            // should skip: bar/2.txt (excluded by pattern) = 1 file
            // symlinks 5.txt and 6.txt don't match *.txt include pattern (symlinks, not files)
            assert_eq!(summary.files_copied, 4, "should copy 4 .txt files");
            assert_eq!(
                summary.files_skipped, 1,
                "should skip 1 file (bar/2.txt excluded)"
            );
            // verify specific files
            assert!(
                test_path.join("dst/bar/1.txt").exists(),
                "bar/1.txt should be copied"
            );
            assert!(
                !test_path.join("dst/bar/2.txt").exists(),
                "bar/2.txt should be excluded"
            );
            assert!(
                test_path.join("dst/bar/3.txt").exists(),
                "bar/3.txt should be copied"
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
            let summary = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // copied: 0.txt (1 file), baz/4.txt (1 file), 5.txt symlink, 6.txt symlink
            // skipped: bar directory (1 dir) - contents not counted since whole dir skipped
            // directories: foo (root), baz = 2
            assert_eq!(summary.files_copied, 2, "should copy 2 files");
            assert_eq!(summary.symlinks_created, 2, "should copy 2 symlinks");
            assert_eq!(
                summary.directories_created, 2,
                "should create 2 directories"
            );
            assert_eq!(
                summary.directories_skipped, 1,
                "should skip 1 directory (bar)"
            );
            assert_eq!(
                summary.files_skipped, 0,
                "no files skipped (bar contents not counted)"
            );
            Ok(())
        }
        /// Test that empty directories are not created when they were only traversed to look
        /// for matches (regression test for bug where --include='foo' would create empty dir baz).
        #[tokio::test]
        #[traced_test]
        async fn test_empty_dir_not_created_when_only_traversed() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // src/
            //   foo (file)
            //   bar (file)
            //   baz/ (empty directory)
            let src_path = test_path.join("src");
            tokio::fs::create_dir(&src_path).await?;
            tokio::fs::write(src_path.join("foo"), "content").await?;
            tokio::fs::write(src_path.join("bar"), "content").await?;
            tokio::fs::create_dir(src_path.join("baz")).await?;
            // include only 'foo' file
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let summary = copy(
                &PROGRESS,
                &src_path,
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // only 'foo' should be copied
            assert_eq!(summary.files_copied, 1, "should copy only 'foo' file");
            assert_eq!(
                summary.directories_created, 1,
                "should create only root directory (not empty 'baz')"
            );
            // verify foo was copied
            assert!(
                test_path.join("dst").join("foo").exists(),
                "foo should be copied"
            );
            // verify bar was not copied (not matching include pattern)
            assert!(
                !test_path.join("dst").join("bar").exists(),
                "bar should not be copied"
            );
            // verify empty baz directory was NOT created
            assert!(
                !test_path.join("dst").join("baz").exists(),
                "empty baz directory should NOT be created"
            );
            Ok(())
        }
        /// Test that directories with only non-matching content are not created at destination.
        /// This is different from empty directories - the source dir has content but none matches.
        #[tokio::test]
        #[traced_test]
        async fn test_dir_with_nonmatching_content_not_created() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // src/
            //   foo (file)
            //   baz/
            //     qux (file - doesn't match 'foo')
            //     quux (file - doesn't match 'foo')
            let src_path = test_path.join("src");
            tokio::fs::create_dir(&src_path).await?;
            tokio::fs::write(src_path.join("foo"), "content").await?;
            tokio::fs::create_dir(src_path.join("baz")).await?;
            tokio::fs::write(src_path.join("baz").join("qux"), "content").await?;
            tokio::fs::write(src_path.join("baz").join("quux"), "content").await?;
            // include only 'foo' file
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let summary = copy(
                &PROGRESS,
                &src_path,
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // only 'foo' should be copied
            assert_eq!(summary.files_copied, 1, "should copy only 'foo' file");
            assert_eq!(
                summary.files_skipped, 2,
                "should skip 2 files (qux and quux)"
            );
            assert_eq!(
                summary.directories_created, 1,
                "should create only root directory (not 'baz' with non-matching content)"
            );
            // verify foo was copied
            assert!(
                test_path.join("dst").join("foo").exists(),
                "foo should be copied"
            );
            // verify baz directory was NOT created (even though source baz has content)
            assert!(
                !test_path.join("dst").join("baz").exists(),
                "baz directory should NOT be created (no matching content inside)"
            );
            Ok(())
        }
        /// Test that empty directories are not reported as created in dry-run mode
        /// when they were only traversed.
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_empty_dir_not_reported_as_created() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // src/
            //   foo (file)
            //   bar (file)
            //   baz/ (empty directory)
            let src_path = test_path.join("src");
            tokio::fs::create_dir(&src_path).await?;
            tokio::fs::write(src_path.join("foo"), "content").await?;
            tokio::fs::write(src_path.join("bar"), "content").await?;
            tokio::fs::create_dir(src_path.join("baz")).await?;
            // include only 'foo' file
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let summary = copy(
                &PROGRESS,
                &src_path,
                &test_path.join("dst"),
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: Some(crate::config::DryRunMode::Explain),
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // only 'foo' should be reported as would-be-copied
            assert_eq!(
                summary.files_copied, 1,
                "should report only 'foo' would be copied"
            );
            assert_eq!(
                summary.directories_created, 1,
                "should report only root directory would be created (not empty 'baz')"
            );
            // verify nothing was actually created (dry-run mode)
            assert!(
                !test_path.join("dst").exists(),
                "dst should not exist in dry-run"
            );
            Ok(())
        }
        /// Test that existing directories are NOT removed when using --overwrite,
        /// even if nothing is copied into them due to filters.
        #[tokio::test]
        #[traced_test]
        async fn test_existing_dir_not_removed_with_overwrite() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create source structure:
            // src/
            //   foo (file)
            //   bar (file)
            //   baz/ (empty directory)
            let src_path = test_path.join("src");
            tokio::fs::create_dir(&src_path).await?;
            tokio::fs::write(src_path.join("foo"), "content").await?;
            tokio::fs::write(src_path.join("bar"), "content").await?;
            tokio::fs::create_dir(src_path.join("baz")).await?;
            // create destination with baz directory already existing
            let dst_path = test_path.join("dst");
            tokio::fs::create_dir(&dst_path).await?;
            tokio::fs::create_dir(dst_path.join("baz")).await?;
            // add a marker file inside dst/baz to verify we don't touch it
            tokio::fs::write(dst_path.join("baz").join("marker.txt"), "existing").await?;
            // include only 'foo' file - baz should not match
            let mut filter = FilterSettings::new();
            filter.add_include("foo").unwrap();
            let summary = copy(
                &PROGRESS,
                &src_path,
                &dst_path,
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: true, // enable overwrite mode
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // foo should be copied
            assert_eq!(summary.files_copied, 1, "should copy only 'foo' file");
            // dst and baz should be unchanged (both already existed)
            assert_eq!(
                summary.directories_unchanged, 2,
                "root dst and baz directories should be unchanged"
            );
            assert_eq!(
                summary.directories_created, 0,
                "should not create any directories"
            );
            // verify foo was copied
            assert!(dst_path.join("foo").exists(), "foo should be copied");
            // verify bar was NOT copied
            assert!(!dst_path.join("bar").exists(), "bar should not be copied");
            // verify existing baz directory still exists with its content
            assert!(
                dst_path.join("baz").exists(),
                "existing baz directory should still exist"
            );
            assert!(
                dst_path.join("baz").join("marker.txt").exists(),
                "existing content in baz should still exist"
            );
            Ok(())
        }
    }
    mod dry_run_tests {
        use super::*;
        use crate::filter::FilterSettings;
        /// Test that dry-run mode for directories doesn't create the destination
        /// and doesn't try to set metadata on non-existent directories.
        #[tokio::test]
        #[traced_test]
        async fn test_dry_run_directory_does_not_create_destination() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            let dst_path = test_path.join("nonexistent_dst");
            // verify destination doesn't exist
            assert!(
                !dst_path.exists(),
                "destination should not exist before dry-run"
            );
            let summary = copy(
                &PROGRESS,
                &test_path.join("foo"),
                &dst_path,
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: None,
                    dry_run: Some(crate::config::DryRunMode::Brief),
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // verify destination still doesn't exist
            assert!(
                !dst_path.exists(),
                "dry-run should not create destination directory"
            );
            // verify summary reports what would be created
            assert!(
                summary.directories_created > 0,
                "dry-run should report directories that would be created"
            );
            assert!(
                summary.files_copied > 0,
                "dry-run should report files that would be copied"
            );
            Ok(())
        }
        /// Regression: in dry-run, `--ignore-existing` must report an existing destination file as
        /// skipped (files_unchanged), not as a would-be copy. The dry-run signal is `dst_parent ==
        /// None`, so the skip must probe existence by path rather than via a held dst fd.
        #[tokio::test]
        async fn dry_run_ignore_existing_reports_file_as_skipped() -> Result<(), anyhow::Error> {
            let tmp = testutils::create_temp_dir().await?;
            let src = tmp.join("src.txt");
            let dst = tmp.join("dst.txt");
            tokio::fs::write(&src, "new").await?;
            tokio::fs::write(&dst, "old").await?; // destination already exists and differs
            let summary = copy(
                &PROGRESS,
                &src,
                &dst,
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: true,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: None,
                    dry_run: Some(crate::config::DryRunMode::Brief),
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            assert_eq!(
                summary.files_unchanged, 1,
                "existing file must be reported skipped in dry-run under --ignore-existing"
            );
            assert_eq!(
                summary.files_copied, 0,
                "must not report a would-be copy when --ignore-existing skips"
            );
            assert_eq!(
                tokio::fs::read_to_string(&dst).await?,
                "old",
                "dry-run must not mutate the destination"
            );
            Ok(())
        }
        /// Regression: the symlink counterpart of `dry_run_ignore_existing_reports_file_as_skipped`
        /// — an existing destination must be reported as `symlinks_unchanged`, not `symlinks_created`.
        #[tokio::test]
        async fn dry_run_ignore_existing_reports_symlink_as_skipped() -> Result<(), anyhow::Error> {
            let tmp = testutils::create_temp_dir().await?;
            let src = tmp.join("src_link");
            let dst = tmp.join("dst_link");
            tokio::fs::symlink("some/target", &src).await?;
            tokio::fs::write(&dst, "existing").await?; // destination already exists (any type)
            let summary = copy(
                &PROGRESS,
                &src,
                &dst,
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: true,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: None,
                    dry_run: Some(crate::config::DryRunMode::Brief),
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            assert_eq!(
                summary.symlinks_unchanged, 1,
                "existing symlink dst must be reported skipped in dry-run under --ignore-existing"
            );
            assert_eq!(
                summary.symlinks_created, 0,
                "must not report a would-be symlink creation when --ignore-existing skips"
            );
            Ok(())
        }
        /// Test that root directory is always created even when nothing matches
        /// the include pattern. The root is the user-specified source — it should
        /// never be removed/skipped due to empty-dir cleanup.
        #[tokio::test]
        #[traced_test]
        async fn test_root_dir_preserved_when_nothing_matches() -> Result<(), anyhow::Error> {
            let test_path = testutils::create_temp_dir().await?;
            // create structure:
            // src/
            //   bar.log (doesn't match *.txt)
            //   baz/ (empty directory)
            let src_path = test_path.join("src");
            tokio::fs::create_dir(&src_path).await?;
            tokio::fs::write(src_path.join("bar.log"), "content").await?;
            tokio::fs::create_dir(src_path.join("baz")).await?;
            // include only *.txt - nothing in source matches
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            let dst_path = test_path.join("dst");
            let summary = copy(
                &PROGRESS,
                &src_path,
                &dst_path,
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            // no files should be copied
            assert_eq!(summary.files_copied, 0, "no files match *.txt");
            // root directory should still be created
            assert_eq!(
                summary.directories_created, 1,
                "root directory should always be created"
            );
            assert!(dst_path.exists(), "root destination directory should exist");
            // non-matching subdirectories should not be created
            assert!(
                !dst_path.join("baz").exists(),
                "empty baz should not be created"
            );
            Ok(())
        }
        /// Test that root directory is counted in dry-run even when nothing matches.
        #[tokio::test]
        #[traced_test]
        async fn test_root_dir_counted_in_dry_run_when_nothing_matches() -> Result<(), anyhow::Error>
        {
            let test_path = testutils::create_temp_dir().await?;
            let src_path = test_path.join("src");
            tokio::fs::create_dir(&src_path).await?;
            tokio::fs::write(src_path.join("bar.log"), "content").await?;
            // include only *.txt - nothing matches
            let mut filter = FilterSettings::new();
            filter.add_include("*.txt").unwrap();
            let dst_path = test_path.join("dst");
            let summary = copy(
                &PROGRESS,
                &src_path,
                &dst_path,
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: false,
                    remote_copy_buffer_size: 0,
                    filter: Some(filter),
                    dry_run: Some(crate::config::DryRunMode::Explain),
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            assert_eq!(summary.files_copied, 0, "no files match *.txt");
            assert_eq!(
                summary.directories_created, 1,
                "root directory should be counted in dry-run"
            );
            assert!(
                !dst_path.exists(),
                "nothing should be created in dry-run mode"
            );
            Ok(())
        }
    }

    /// stress tests exercising max-open-files saturation during copy
    mod max_open_files_tests {
        use super::*;

        /// wide copy: many files with a very low open-files limit.
        /// verifies all files are copied correctly under permit saturation.
        #[tokio::test]
        #[traced_test]
        async fn wide_copy_under_open_files_saturation() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let src = tmp_dir.join("src");
            let dst = tmp_dir.join("dst");
            tokio::fs::create_dir(&src).await?;
            let file_count = 200;
            for i in 0..file_count {
                tokio::fs::write(src.join(format!("{}.txt", i)), format!("content-{}", i)).await?;
            }
            // set a very low limit to force permit contention
            throttle::set_max_open_files(4);
            let summary = copy(
                &PROGRESS,
                &src,
                &dst,
                &Settings {
                    dereference: false,
                    fail_early: true,
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
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            assert_eq!(summary.files_copied, file_count);
            assert_eq!(summary.directories_created, 1);
            for i in 0..file_count {
                let content = tokio::fs::read_to_string(dst.join(format!("{}.txt", i))).await?;
                assert_eq!(content, format!("content-{}", i));
            }
            Ok(())
        }

        /// deep + wide copy: directory tree deeper than the open-files limit, with files
        /// at every level. verifies no deadlock occurs (directories don't consume permits).
        #[tokio::test]
        #[traced_test]
        async fn deep_tree_no_deadlock_under_open_files_saturation() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let src = tmp_dir.join("src");
            let dst = tmp_dir.join("dst");
            let depth = 20;
            let files_per_level = 5;
            let limit = 4;
            // create a directory chain deeper than the permit limit, with files at each level
            let mut dir = src.clone();
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
                copy(
                    &PROGRESS,
                    &src,
                    &dst,
                    &Settings {
                        dereference: false,
                        fail_early: true,
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
                    },
                    &NO_PRESERVE_SETTINGS,
                    false,
                ),
            )
            .await
            .context("copy timed out — possible deadlock")?
            .context("copy failed")?;
            assert_eq!(summary.files_copied, depth * files_per_level);
            assert_eq!(summary.directories_created, depth);
            // spot-check content at a few levels
            let mut check_dir = dst.clone();
            for level in 0..depth {
                let content =
                    tokio::fs::read_to_string(check_dir.join(format!("f{}_0.txt", level))).await?;
                assert_eq!(content, format!("L{}F0", level));
                check_dir = check_dir.join(format!("d{}", level));
            }
            Ok(())
        }

        /// Regression: copy_file → rm cross-pool deadlock.
        ///
        /// Scenario: many parallel copies overwrite destinations that are
        /// directories (so each copy_file path takes the
        /// "remove existing then copy" branch, and rm recurses). Each copy
        /// task holds an open-files permit during copy_file; if rm also
        /// drew permits from the open-files pool, a saturated pool would
        /// deadlock — every permit held by a copy task waiting for rm to
        /// release one. Decoupling rm onto pending-meta avoids that.
        #[tokio::test]
        #[traced_test]
        async fn parallel_overwrite_dir_with_file_no_deadlock() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::create_temp_dir().await?;
            let src = tmp_dir.join("src");
            let dst = tmp_dir.join("dst");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::create_dir(&dst).await?;
            // 8 sources are regular files; the 8 corresponding destinations
            // are directories with nested files — copy with --overwrite
            // forces rm of each dst directory tree from inside copy_file.
            let n = 8;
            for i in 0..n {
                tokio::fs::write(src.join(format!("e{}", i)), format!("file-{}", i)).await?;
                let dst_subdir = dst.join(format!("e{}", i));
                tokio::fs::create_dir(&dst_subdir).await?;
                for j in 0..3 {
                    tokio::fs::write(
                        dst_subdir.join(format!("inner_{}.txt", j)),
                        format!("inner-{}-{}", i, j),
                    )
                    .await?;
                }
            }
            // Saturate the open-files pool: if rm shared this pool, every
            // outer copy task would hold its single permit and the inner rm
            // recursion would block forever.
            throttle::set_max_open_files(2);
            let summary = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                copy(
                    &PROGRESS,
                    &src,
                    &dst,
                    &Settings {
                        dereference: false,
                        fail_early: true,
                        overwrite: true,
                        overwrite_compare: Default::default(),
                        overwrite_filter: None,
                        ignore_existing: false,
                        chunk_size: 0,
                        skip_specials: false,
                        remote_copy_buffer_size: 0,
                        filter: None,
                        dry_run: None,
                        delete: None,
                    },
                    &NO_PRESERVE_SETTINGS,
                    false,
                ),
            )
            .await
            .context(
                "copy timed out — deadlock between copy_file's open-files permit and inner rm",
            )?
            .context("copy failed")?;
            assert_eq!(summary.files_copied, n);
            assert_eq!(summary.rm_summary.files_removed, n * 3);
            assert_eq!(summary.rm_summary.directories_removed, n);
            for i in 0..n {
                let path = dst.join(format!("e{}", i));
                let content = tokio::fs::read_to_string(&path).await?;
                assert_eq!(content, format!("file-{}", i));
            }
            Ok(())
        }
    }

    mod skip_specials_tests {
        use super::*;

        #[tokio::test]
        #[traced_test]
        async fn skip_specials_skips_socket_in_directory() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            let src = test_path.join("src_dir");
            let dst = test_path.join("dst_dir");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::write(src.join("file.txt"), "hello").await?;
            // create a unix socket inside the source directory
            let _listener = std::os::unix::net::UnixListener::bind(src.join("test.sock"))?;
            let summary = copy(
                &PROGRESS,
                &src,
                &dst,
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: true,
                    remote_copy_buffer_size: 0,
                    filter: None,
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            assert_eq!(summary.files_copied, 1);
            assert_eq!(summary.specials_skipped, 1);
            assert!(dst.join("file.txt").exists());
            assert!(!dst.join("test.sock").exists());
            Ok(())
        }

        #[tokio::test]
        #[traced_test]
        async fn skip_specials_skips_fifo_in_directory() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            let src = test_path.join("src_dir");
            let dst = test_path.join("dst_dir");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::write(src.join("file.txt"), "hello").await?;
            // create a FIFO inside the source directory
            nix::unistd::mkfifo(
                &src.join("test.fifo"),
                nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
            )?;
            let summary = copy(
                &PROGRESS,
                &src,
                &dst,
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: true,
                    remote_copy_buffer_size: 0,
                    filter: None,
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            assert_eq!(summary.files_copied, 1);
            assert_eq!(summary.specials_skipped, 1);
            assert!(dst.join("file.txt").exists());
            assert!(!dst.join("test.fifo").exists());
            Ok(())
        }

        #[tokio::test]
        #[traced_test]
        async fn special_file_errors_without_skip_specials() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            let src = test_path.join("src_dir");
            let dst = test_path.join("dst_dir");
            tokio::fs::create_dir(&src).await?;
            tokio::fs::write(src.join("file.txt"), "hello").await?;
            let _listener = std::os::unix::net::UnixListener::bind(src.join("test.sock"))?;
            let result = copy(
                &PROGRESS,
                &src,
                &dst,
                &Settings {
                    dereference: false,
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
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await;
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(
                format!("{:#}", err).contains("unsupported src file type"),
                "error should mention unsupported file type, got: {:#}",
                err
            );
            Ok(())
        }

        #[tokio::test]
        #[traced_test]
        async fn skip_specials_top_level_socket() -> Result<(), anyhow::Error> {
            let tmp_dir = testutils::setup_test_dir().await?;
            let test_path = tmp_dir.as_path();
            let src_socket = test_path.join("test.sock");
            let dst = test_path.join("dst.sock");
            let _listener = std::os::unix::net::UnixListener::bind(&src_socket)?;
            let summary = copy(
                &PROGRESS,
                &src_socket,
                &dst,
                &Settings {
                    dereference: false,
                    fail_early: false,
                    overwrite: false,
                    overwrite_compare: Default::default(),
                    overwrite_filter: None,
                    ignore_existing: false,
                    chunk_size: 0,
                    skip_specials: true,
                    remote_copy_buffer_size: 0,
                    filter: None,
                    dry_run: None,
                    delete: None,
                },
                &NO_PRESERVE_SETTINGS,
                false,
            )
            .await?;
            assert_eq!(summary.specials_skipped, 1);
            assert_eq!(summary.files_copied, 0);
            assert!(!dst.exists());
            Ok(())
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn delete_protects_excluded_then_removes_with_delete_excluded()
    -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("foo");
        let dst = test_path.join("bar");
        copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(None),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        // a destination-only file that matches an exclude pattern
        tokio::fs::write(dst.join("keep.log"), b"protected").await?;

        let mut filter = crate::filter::FilterSettings::new();
        filter.add_exclude("*.log")?;

        // default --delete: keep.log is protected
        let mut settings = settings_with_delete(delete_on());
        settings.filter = Some(filter.clone());
        copy(
            &PROGRESS,
            &src,
            &dst,
            &settings,
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert!(
            dst.join("keep.log").exists(),
            "*.log must be protected by default"
        );

        // --delete-excluded: keep.log is removed
        let mut settings = settings_with_delete(Some(DeleteSettings {
            delete_excluded: true,
        }));
        settings.filter = Some(filter);
        copy(
            &PROGRESS,
            &src,
            &dst,
            &settings,
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert!(!dst.join("keep.log").exists());
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn delete_dry_run_reports_without_removing() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("foo");
        let dst = test_path.join("bar");
        copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(None),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        tokio::fs::write(dst.join("stale.txt"), b"junk").await?;

        let mut settings = settings_with_delete(delete_on());
        settings.dry_run = Some(crate::config::DryRunMode::Brief);
        let summary = copy(
            &PROGRESS,
            &src,
            &dst,
            &settings,
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;

        // the key invariant: dry-run must NOT remove anything
        assert!(
            dst.join("stale.txt").exists(),
            "dry-run must not remove anything"
        );
        // rm's dry-run does count would-be removals in files_removed
        assert_eq!(summary.rm_summary.files_removed, 1);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn delete_does_not_prune_when_source_unreadable() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("foo");
        let dst = test_path.join("bar");
        copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(None),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        tokio::fs::write(dst.join("stale.txt"), b"junk").await?;
        // make the source directory unreadable so enumeration fails
        let original = tokio::fs::metadata(&src).await?.permissions();
        tokio::fs::set_permissions(&src, std::fs::Permissions::from_mode(0o000)).await?;

        let result = copy(
            &PROGRESS,
            &src,
            &dst,
            &settings_with_delete(delete_on()),
            &DO_PRESERVE_SETTINGS,
            false,
        )
        .await;

        // restore permissions before asserting (so the temp dir cleans up)
        tokio::fs::set_permissions(&src, original).await?;

        assert!(result.is_err(), "unreadable source must error");
        assert!(
            dst.join("stale.txt").exists(),
            "destination must not be pruned when source enumeration fails"
        );
        Ok(())
    }

    // Overwriting an existing regular file goes through the fd-relative + recheck-guarded removal
    // path: the destination file is removed (counted as a single `files_removed`) and recreated
    // with the source content. This exercises `remove_existing`'s `unlink_at` branch via the real
    // fd-based `copy` walk (not the legacy path-based `copy_file`).
    #[tokio::test]
    #[traced_test]
    async fn overwrite_regular_file_removes_and_recreates_via_fd() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        let src_file = test_path.join("src.txt");
        let dst_file = test_path.join("dst.txt");
        // distinct sizes so the metadata comparison treats them as different and overwrite proceeds.
        tokio::fs::write(&src_file, "fresh source content").await?;
        tokio::fs::write(&dst_file, "old").await?;
        let summary = copy(
            &PROGRESS,
            &src_file,
            &dst_file,
            &Settings {
                dereference: false,
                fail_early: false,
                overwrite: true, // <- exercises the overwrite removal path
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                skip_specials: false,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
                delete: None,
            },
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        // the existing regular file was removed (fd-relative unlink_at) and the source recreated.
        assert_eq!(summary.files_copied, 1);
        assert_eq!(summary.rm_summary.files_removed, 1);
        assert_eq!(summary.rm_summary.symlinks_removed, 0);
        assert_eq!(summary.rm_summary.directories_removed, 0);
        let content = tokio::fs::read_to_string(&dst_file).await?;
        assert_eq!(content, "fresh source content");
        Ok(())
    }

    // ── TOCTOU hardening: -L separability, swap-loop races, fd-budget ────────────────

    /// Default non-dereference copy settings used by the TOCTOU/fd-budget tests below.
    /// `overwrite` is on so a fresh destination per iteration is never required to be empty.
    fn toctou_settings() -> Settings {
        Settings {
            dereference: false,
            fail_early: false,
            overwrite: true,
            overwrite_compare: filecmp::MetadataCmpSettings {
                size: true,
                mtime: true,
                ..Default::default()
            },
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

    // Task 1.4: with `dereference == false`, a symlink in the source is copied as a symlink — the
    // non-dereference (fd-based) path is taken and `canonicalize` is never invoked. The
    // `debug_assert!(settings.dereference, ...)` guarding the canonicalize call is the actual
    // invariant guard (it would fire in debug/tests if a refactor reached it); this test exercises
    // that non-dereference symlink path end-to-end and proves the link is preserved as a link.
    #[tokio::test]
    #[traced_test]
    async fn non_dereference_copy_never_canonicalizes() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("src");
        let dst = test_path.join("dst");
        tokio::fs::create_dir(&src).await?;
        // a real file plus a symlink to it living inside the source tree.
        tokio::fs::write(src.join("real.txt"), "REAL_CONTENT").await?;
        tokio::fs::symlink("real.txt", src.join("link")).await?;
        let summary = copy(
            &PROGRESS,
            &src,
            &dst,
            &toctou_settings(),
            &NO_PRESERVE_SETTINGS,
            false,
        )
        .await?;
        assert_eq!(summary.files_copied, 1);
        assert_eq!(
            summary.symlinks_created, 1,
            "symlink must be copied as a symlink"
        );
        // the destination entry must itself be a symlink (non-deref path), not a dereferenced file.
        let link_md = tokio::fs::symlink_metadata(dst.join("link")).await?;
        assert!(
            link_md.file_type().is_symlink(),
            "with dereference == false the source symlink must remain a symlink in the destination"
        );
        // and its target text is preserved verbatim (it was never resolved/canonicalized).
        let target = tokio::fs::read_link(dst.join("link")).await?;
        assert_eq!(target, std::path::Path::new("real.txt"));
        Ok(())
    }

    // Task 1.5 helper: repeatedly swap `path` between a real regular file (content `REAL_CONTENT`)
    // and a symlink pointing at `sentinel`, using rename so each individual state is atomic. Two
    // staging names (a prepared symlink and the real file) live alongside `path` and are renamed
    // over it in a tight loop until `stop` is set. Runs on a dedicated OS thread (std::thread), so
    // it makes progress regardless of the tokio runtime's scheduling.
    fn spawn_file_symlink_swapper(
        dir: std::path::PathBuf,
        entry_name: &'static str,
        sentinel: std::path::PathBuf,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let entry = dir.join(entry_name);
            let staged_real = dir.join("__staged_real");
            let staged_link = dir.join("__staged_link");
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                // prepare the real file at a staging name, then rename it over `entry`.
                let _ = std::fs::remove_file(&staged_real);
                if std::fs::write(&staged_real, b"REAL_CONTENT").is_err() {
                    continue;
                }
                let _ = std::fs::rename(&staged_real, &entry);
                // prepare a symlink-to-sentinel at the other staging name, then rename it over.
                let _ = std::fs::remove_file(&staged_link);
                let _ = std::os::unix::fs::symlink(&sentinel, &staged_link);
                let _ = std::fs::rename(&staged_link, &entry);
            }
        })
    }

    // Task 1.5 (a): final-component file<->symlink swap. While the copy runs, `src/sub/entry` is
    // rapidly flipped between a real regular file and a symlink to a sentinel outside the tree. The
    // copy may grab the real file, or O_NOFOLLOW/fstat may catch the symlink (error or skip), but
    // the sentinel's secret content must NEVER end up in the destination as data.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_symlink_swap_never_leaks_sentinel() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // sentinel lives OUTSIDE the source tree with distinctive content.
        let sentinel = test_path.join("sentinel_secret");
        tokio::fs::write(&sentinel, "SENTINEL_SECRET_CONTENT").await?;
        let src = test_path.join("src");
        let sub = src.join("sub");
        tokio::fs::create_dir(&src).await?;
        tokio::fs::create_dir(&sub).await?;
        tokio::fs::write(sub.join("entry"), "REAL_CONTENT").await?;

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let swapper =
            spawn_file_symlink_swapper(sub.clone(), "entry", sentinel.clone(), stop.clone());

        let settings = toctou_settings();
        let mut caught_swaps = 0usize;
        let mut copied_real = 0usize;
        for i in 0..200 {
            let dst = test_path.join(format!("dst_{i}"));
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                copy(
                    &PROGRESS,
                    &src,
                    &dst,
                    &settings,
                    &NO_PRESERVE_SETTINGS,
                    false,
                ),
            )
            .await
            .expect("copy must not hang under concurrent swapping");
            match result {
                Ok(_) => {}
                Err(_) => caught_swaps += 1, // a swap was caught mid-copy (failed closed)
            }
            // CORE ASSERTION: if a regular file landed at the destination, it is the real content —
            // never the sentinel's secret. The entry may instead be absent or a symlink.
            let entry_dst = dst.join("sub").join("entry");
            if let Ok(md) = tokio::fs::symlink_metadata(&entry_dst).await
                && md.file_type().is_file()
            {
                let content = tokio::fs::read_to_string(&entry_dst).await?;
                assert_ne!(
                    content, "SENTINEL_SECRET_CONTENT",
                    "iteration {i}: sentinel content leaked into the destination as a regular file"
                );
                assert_eq!(
                    content, "REAL_CONTENT",
                    "iteration {i}: a regular destination file must hold the real content"
                );
                copied_real += 1;
            }
            let _ = tokio::fs::remove_dir_all(&dst).await;
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        swapper.join().expect("swapper thread panicked");
        // sanity: the run did meaningful work (caught some swaps or copied the real file at least
        // once). This is not the safety assertion — the safety assertion is "sentinel never leaks"
        // above and holds on every iteration regardless of timing.
        tracing::info!("file/symlink swap: caught_swaps={caught_swaps}, copied_real={copied_real}");
        assert!(
            caught_swaps + copied_real > 0,
            "expected at least one observable outcome across 200 iterations"
        );
        Ok(())
    }

    // Task 1.5 (b): intermediate-directory swap. While the copy runs, `src/sub` is flipped between a
    // real directory (containing a real file) and a symlink to a directory outside the tree holding
    // a sentinel. open_dir uses O_NOFOLLOW, so the walk either descends the real directory or fails
    // closed — it must never follow the symlink and copy the outside sentinel.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn intermediate_dir_swap_never_follows_symlink() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // an "outside" directory tree holding a sentinel file, reachable only via a symlink.
        let outside = test_path.join("outside_dir");
        tokio::fs::create_dir(&outside).await?;
        tokio::fs::write(outside.join("sentinel.txt"), "SENTINEL_SECRET_CONTENT").await?;
        let src = test_path.join("src");
        tokio::fs::create_dir(&src).await?;
        // the real `sub` directory with a real file inside.
        let real_sub = src.join("sub");
        tokio::fs::create_dir(&real_sub).await?;
        tokio::fs::write(real_sub.join("real.txt"), "REAL_CONTENT").await?;

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let swapper = {
            let src = src.clone();
            let outside = outside.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let sub = src.join("sub");
                let staged_dir = src.join("__staged_sub_dir");
                let staged_link = src.join("__staged_sub_link");
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // stage a real directory (with the real file) then rename it over `sub`.
                    let _ = std::fs::remove_dir_all(&staged_dir);
                    if std::fs::create_dir(&staged_dir).is_ok() {
                        let _ = std::fs::write(staged_dir.join("real.txt"), b"REAL_CONTENT");
                        // RENAME_EXCHANGE isn't available portably here; remove-then-rename. The
                        // window where `sub` is briefly absent is fine — the copy may error, which
                        // is an accepted failed-closed outcome.
                        let _ = std::fs::remove_dir_all(&sub);
                        let _ = std::fs::remove_file(&sub);
                        let _ = std::fs::rename(&staged_dir, &sub);
                    }
                    // stage a symlink to the outside dir, then swap it in over `sub`.
                    let _ = std::fs::remove_file(&staged_link);
                    if std::os::unix::fs::symlink(&outside, &staged_link).is_ok() {
                        let _ = std::fs::remove_dir_all(&sub);
                        let _ = std::fs::remove_file(&sub);
                        let _ = std::fs::rename(&staged_link, &sub);
                    }
                }
            })
        };

        let settings = toctou_settings();
        for i in 0..200 {
            let dst = test_path.join(format!("dst_{i}"));
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                copy(
                    &PROGRESS,
                    &src,
                    &dst,
                    &settings,
                    &NO_PRESERVE_SETTINGS,
                    false,
                ),
            )
            .await
            .expect("copy must not hang under concurrent dir swapping");
            // CORE ASSERTION: the walk must never DESCEND through the symlink and copy the outside
            // tree's contents as data. Classify `dst/sub` WITHOUT following symlinks: if it is a
            // symlink, the link was copied verbatim (safe — `outside` was never read); if it is a
            // real directory, the copy descended a real `sub`, and the sentinel — which only ever
            // lived in `outside`, reachable solely via the symlink — must NOT have been reproduced
            // inside it. (`Path::exists` follows symlinks, so it would falsely "see" the sentinel
            // through a verbatim-copied symlink; `symlink_metadata` is the correct probe here.)
            // when `dst/sub` is a symlink it was copied verbatim (accepted failed-closed outcome,
            // `outside` was never read); only a REAL destination directory means the walk descended,
            // and then it must have descended the real `sub` — never the symlink into `outside`.
            let sub_dst = dst.join("sub");
            if let Ok(sub_md) = tokio::fs::symlink_metadata(&sub_dst).await
                && sub_md.file_type().is_dir()
            {
                let leaked = sub_dst.join("sentinel.txt");
                let leaked_is_real_file = tokio::fs::symlink_metadata(&leaked)
                    .await
                    .map(|m| m.file_type().is_file())
                    .unwrap_or(false);
                assert!(
                    !leaked_is_real_file,
                    "iteration {i}: intermediate-dir symlink was followed; the outside \
                     sentinel was copied into a real destination directory"
                );
                // a real `sub` directory copied from the real source holds the real file.
                let real_dst = sub_dst.join("real.txt");
                if let Ok(content) = tokio::fs::read_to_string(&real_dst).await {
                    assert_eq!(
                        content, "REAL_CONTENT",
                        "iteration {i}: copied file must hold real content"
                    );
                }
            }
            let _ = tokio::fs::remove_dir_all(&dst).await;
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        swapper.join().expect("dir swapper thread panicked");
        Ok(())
    }

    // Task 1.5 (d): destination NON-EMPTY-directory overwrite removal under an intermediate-component
    // swap — the racy mirror of `test_remote_dest_overwrite_replaces_intermediate_symlink_in_tree`,
    // targeting the removal path B1 hardened. The source has regular files at `src/mid/deeper/subK`,
    // while the destination has NON-EMPTY directories at `dst/mid/deeper/subK`. A `--overwrite` copy
    // must therefore REMOVE those non-empty directories (`remove_existing` → ENOTEMPTY →
    // `rm::rm_child`) before creating the files. Concurrently, a swapper flips the INTERMEDIATE
    // component `dst/mid` between a real directory (holding `deeper/subK`) and a symlink to an
    // out-of-tree directory mirroring `deeper/subK/`, whose `subK/` each hold a sentinel.
    //
    // This is a fail-closed STRESS test: under the fd code the copy either descends a real `dst/mid`
    // and removes through the pinned `deeper` fd, or fails closed at the intermediate `make_dir`
    // O_NOFOLLOW when it observes the symlink — never escaping. CORE ASSERTION: no out-of-tree
    // sentinel is ever deleted, and the copy never hangs. (Because the walk fails closed at the
    // intermediate `make_dir` whenever it sees the symlink, this racy form rarely even reaches the
    // removal branch and does NOT reliably reproduce the path-based escape; the deterministic
    // `nonempty_dir_overwrite_removal_uses_pinned_fd_not_path` below is the regression-catcher that
    // FAILS when the branch is reverted to `rm::rm`.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dest_nonempty_dir_overwrite_removal_contained_under_intermediate_swap()
    -> Result<(), anyhow::Error> {
        const N: usize = 8;
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        // an out-of-tree tree mirroring `deeper/`, holding several non-empty `subK/` each guarding a
        // sentinel. A path-based rm(dst/mid/deeper/subK) through a `mid`->out_of_tree symlink would
        // delete out_of_tree/deeper/subK. Several targets widen the window: once `mid` flips
        // to a symlink mid-copy (after `deeper` was opened), every remaining path-based rm escapes.
        let out_of_tree = test_path.join("out_of_tree");
        let oot_deeper = out_of_tree.join("deeper");
        for k in 0..N {
            let oot_sub = oot_deeper.join(format!("sub{k}"));
            tokio::fs::create_dir_all(&oot_sub).await?;
            tokio::fs::write(oot_sub.join("sentinel.txt"), "SENTINEL").await?;
        }

        // source: src/mid/deeper/subK are regular FILES (so the copy must replace the dst dirs).
        let src = test_path.join("src");
        let src_deeper = src.join("mid").join("deeper");
        tokio::fs::create_dir_all(&src_deeper).await?;
        for k in 0..N {
            tokio::fs::write(src_deeper.join(format!("sub{k}")), "REAL_FILE_CONTENT").await?;
        }

        // destination root persists; the swapper owns the intermediate `dst/mid`.
        let dst = test_path.join("dst");
        tokio::fs::create_dir(&dst).await?;

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let swapper = {
            let dst = dst.clone();
            let out_of_tree = out_of_tree.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mid = dst.join("mid");
                let staged_dir = dst.join("__staged_mid_dir");
                let staged_link = dst.join("__staged_mid_link");
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // stage a real `mid` directory holding `deeper/subK` as NON-EMPTY directories,
                    // then rename it over `mid`. This is the state in which the overwrite-removal
                    // branch fires (src has FILES at mid/deeper/subK, dst has non-empty dirs).
                    let _ = std::fs::remove_dir_all(&staged_dir);
                    let staged_deeper = staged_dir.join("deeper");
                    if std::fs::create_dir_all(&staged_deeper).is_ok() {
                        for k in 0..N {
                            let s = staged_deeper.join(format!("sub{k}"));
                            if std::fs::create_dir(&s).is_ok() {
                                let _ = std::fs::write(s.join("inner.txt"), b"INNER_CONTENT");
                            }
                        }
                        let _ = std::fs::remove_dir_all(&mid);
                        let _ = std::fs::remove_file(&mid);
                        let _ = std::fs::rename(&staged_dir, &mid);
                    }
                    // stage a symlink to the out-of-tree directory, then swap it in over `mid`.
                    let _ = std::fs::remove_file(&staged_link);
                    if std::os::unix::fs::symlink(&out_of_tree, &staged_link).is_ok() {
                        let _ = std::fs::remove_dir_all(&mid);
                        let _ = std::fs::remove_file(&mid);
                        let _ = std::fs::rename(&staged_link, &mid);
                    }
                }
            })
        };

        let settings = toctou_settings();
        for i in 0..200 {
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                copy(
                    &PROGRESS,
                    &src,
                    &dst,
                    &settings,
                    &NO_PRESERVE_SETTINGS,
                    false,
                ),
            )
            .await
            .expect("copy must not hang under concurrent intermediate-component swapping");
            // CORE ASSERTION (every iteration, regardless of timing): no out-of-tree sentinel is
            // ever deleted. The fd-relative removal is contained to the pinned `dst/mid/deeper` fd
            // and its O_NOFOLLOW descent fails closed on a swapped-in symlink — it can never
            // re-resolve `dst/mid/deeper/subK` through a `mid`->out_of_tree symlink and delete
            // `out_of_tree/deeper/subK`.
            for k in 0..N {
                assert!(
                    oot_deeper
                        .join(format!("sub{k}"))
                        .join("sentinel.txt")
                        .exists(),
                    "iteration {i}: out-of-tree sentinel sub{k} was deleted — the non-empty-\
                     directory overwrite removal escaped the destination tree through a swapped \
                     intermediate symlink (path-based rm regression)"
                );
            }
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        swapper
            .join()
            .expect("intermediate-swap swapper thread panicked");
        // sanity: the out-of-tree subtree itself also survived intact.
        assert!(
            oot_deeper.join("sub0").is_dir(),
            "out-of-tree sub directory must survive"
        );
        Ok(())
    }

    // Deterministic companion to the racy test above, proving the B1 fix at the contract level: the
    // non-empty-directory overwrite removal acts through the PINNED parent fd, never by re-resolving
    // the display path. We open the REAL parent directory as `dst_parent`, classify the real
    // non-empty child, but pass a `dst_path` whose INTERMEDIATE component is an out-of-tree symlink
    // (the post-classification redirect a racy attacker would plant). The fd-relative `rm_child`
    // removes the real child through the held fd and leaves the out-of-tree tree untouched. The old
    // path-based `rm::rm(dst_path)` would instead re-resolve `dst_path` through the symlink and
    // delete the out-of-tree subtree — so this test FAILS if the branch is reverted to `rm::rm`
    // (verified by hand). `recheck` passes because the real child's identity is unchanged.
    #[tokio::test]
    #[traced_test]
    async fn nonempty_dir_overwrite_removal_uses_pinned_fd_not_path() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();

        // the REAL destination tree: base/realdeeper/victim is a NON-EMPTY directory.
        let real_deeper = test_path.join("base").join("realdeeper");
        let real_victim = real_deeper.join("victim");
        tokio::fs::create_dir_all(&real_victim).await?;
        tokio::fs::write(real_victim.join("inner.txt"), "INNER").await?;

        // an out-of-tree tree the stale display path would resolve into. Its `victim/` holds a
        // sentinel; a path-based rm of the redirected path would delete it.
        let out_of_tree = test_path.join("out_of_tree");
        let oot_victim = out_of_tree.join("realdeeper").join("victim");
        tokio::fs::create_dir_all(&oot_victim).await?;
        tokio::fs::write(oot_victim.join("sentinel.txt"), "SENTINEL").await?;

        // the redirected display path: base/mid -> out_of_tree, so `mid/realdeeper/victim`
        // resolves to out_of_tree/realdeeper/victim by PATH, but the pinned fd points at the real
        // base/realdeeper.
        let mid_link = test_path.join("base").join("mid");
        tokio::fs::symlink(&out_of_tree, &mid_link).await?;
        let stale_dst_path = mid_link.join("realdeeper").join("victim");

        // open the REAL parent fd and classify the real non-empty child through it.
        let dst_parent =
            Arc::new(Dir::open_root_dir(&real_deeper, false, congestion::Side::Destination).await?);
        let victim_name: &OsStr = OsStr::new("victim");
        let dst_handle = dst_parent.child(victim_name).await?;
        assert_eq!(dst_handle.kind(), EntryKind::Dir);

        let settings = toctou_settings();
        let summary = remove_existing(
            &PROGRESS,
            &dst_parent,
            victim_name,
            &stale_dst_path,
            &dst_handle,
            &settings,
        )
        .await?;
        assert_eq!(
            summary.directories_removed, 1,
            "the real non-empty victim directory should have been removed via the pinned fd"
        );

        // the real child was removed through the held fd...
        assert!(
            !real_victim.exists(),
            "the real non-empty victim should have been removed through the pinned parent fd"
        );
        // ...and the out-of-tree subtree (which the stale path resolves into) was NOT touched.
        assert!(
            oot_victim.join("sentinel.txt").exists(),
            "out-of-tree sentinel was deleted — removal re-resolved the display path through the \
             intermediate symlink instead of using the pinned fd (path-based rm regression)"
        );
        Ok(())
    }

    // Task 1.5 (c): FIFO swap. While the copy runs, `src/sub/entry` is flipped between a real file
    // and a FIFO (named pipe). open_file_read uses O_NONBLOCK + an fstat S_ISREG check, so a FIFO is
    // rejected without blocking. The whole copy is wrapped in a generous timeout: the assertion is
    // that it never hangs, and a FIFO is never copied as file data.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fifo_swap_never_hangs_or_copies_pipe() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("src");
        let sub = src.join("sub");
        tokio::fs::create_dir(&src).await?;
        tokio::fs::create_dir(&sub).await?;
        tokio::fs::write(sub.join("entry"), "REAL_CONTENT").await?;

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let swapper = {
            let sub = sub.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let entry = sub.join("entry");
                let staged_real = sub.join("__staged_real");
                let staged_fifo = sub.join("__staged_fifo");
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // real file staged then renamed over `entry`.
                    let _ = std::fs::remove_file(&staged_real);
                    if std::fs::write(&staged_real, b"REAL_CONTENT").is_ok() {
                        let _ = std::fs::rename(&staged_real, &entry);
                    }
                    // FIFO staged then renamed over `entry`. mkfifo can't target an existing name,
                    // so create it at a staging path and rename it in.
                    let _ = std::fs::remove_file(&staged_fifo);
                    let made = nix::unistd::mkfifo(
                        &staged_fifo,
                        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
                    )
                    .is_ok();
                    if made {
                        let _ = std::fs::rename(&staged_fifo, &entry);
                    }
                }
            })
        };

        let settings = toctou_settings();
        for i in 0..200 {
            let dst = test_path.join(format!("dst_{i}"));
            // the generous timeout is the anti-hang assertion: a FIFO opened without O_NONBLOCK
            // would block the copy forever waiting for a writer.
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                copy(
                    &PROGRESS,
                    &src,
                    &dst,
                    &settings,
                    &NO_PRESERVE_SETTINGS,
                    false,
                ),
            )
            .await
            .expect("copy must not hang when an entry is swapped to a FIFO");
            // CORE ASSERTION: whatever landed at the destination is either absent or a real regular
            // file with the real content — a FIFO is never copied as data, and never copied as a
            // special (no specials are expected in the destination).
            let entry_dst = dst.join("sub").join("entry");
            if let Ok(md) = tokio::fs::symlink_metadata(&entry_dst).await {
                use std::os::unix::fs::FileTypeExt as _;
                let ft = md.file_type();
                assert!(
                    !ft.is_fifo(),
                    "iteration {i}: a FIFO was reproduced at the destination"
                );
                if ft.is_file() {
                    let content = tokio::fs::read_to_string(&entry_dst).await?;
                    assert_eq!(
                        content, "REAL_CONTENT",
                        "iteration {i}: a regular destination file must hold the real content"
                    );
                }
            }
            let _ = tokio::fs::remove_dir_all(&dst).await;
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        swapper.join().expect("fifo swapper thread panicked");
        Ok(())
    }

    // Task 1.6: a deep, narrow chain `a/a/a/.../file` copies fully without hanging or exhausting
    // fds. `copy_internal` is `#[async_recursion]`, so depth exercises the recursive chain; 800 is
    // deep enough to stress the fd budget / hold-and-wait avoidance without risking a stack blowup.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_deep_narrow_tree_completes() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("src");
        const DEPTH: usize = 800;
        // build src/a/a/.../a/leaf.txt by appending one "a" per level.
        let mut cur = src.clone();
        for _ in 0..DEPTH {
            cur = cur.join("a");
        }
        tokio::fs::create_dir_all(&cur).await?;
        tokio::fs::write(cur.join("leaf.txt"), "DEEP_LEAF").await?;

        let dst = test_path.join("dst");
        let summary = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            copy(
                &PROGRESS,
                &src,
                &dst,
                &toctou_settings(),
                &NO_PRESERVE_SETTINGS,
                false,
            ),
        )
        .await
        .expect("deep-narrow copy must not hang (fd budget / hold-and-wait)")?;
        assert_eq!(
            summary.files_copied, 1,
            "the single leaf file must be copied"
        );
        // the destination root directory plus the DEPTH-level `a` chain copied into it.
        assert_eq!(summary.directories_created, DEPTH + 1);
        // verify the leaf actually landed at the bottom of the destination chain.
        let mut leaf = dst.clone();
        for _ in 0..DEPTH {
            leaf = leaf.join("a");
        }
        let content = tokio::fs::read_to_string(leaf.join("leaf.txt")).await?;
        assert_eq!(content, "DEEP_LEAF");
        Ok(())
    }

    // Task 1.6: a single very wide directory (many sibling files) copies fully within a timeout. The
    // per-entry open-files permit bounds concurrency; this confirms the wide fan-out drains without
    // fd exhaustion or a hang.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_wide_tree_completes() -> Result<(), anyhow::Error> {
        let tmp_dir = testutils::create_temp_dir().await?;
        let test_path = tmp_dir.as_path();
        let src = test_path.join("src");
        tokio::fs::create_dir(&src).await?;
        const WIDTH: usize = 2000;
        for i in 0..WIDTH {
            tokio::fs::write(src.join(format!("f{i}.txt")), format!("content {i}")).await?;
        }
        let dst = test_path.join("dst");
        let summary = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            copy(
                &PROGRESS,
                &src,
                &dst,
                &toctou_settings(),
                &NO_PRESERVE_SETTINGS,
                false,
            ),
        )
        .await
        .expect("wide copy must not hang (fd exhaustion / permit deadlock)")?;
        assert_eq!(summary.files_copied, WIDTH, "every file must be copied");
        // spot-check a few files actually landed with the right content.
        for i in [0usize, WIDTH / 2, WIDTH - 1] {
            let content = tokio::fs::read_to_string(dst.join(format!("f{i}.txt"))).await?;
            assert_eq!(content, format!("content {i}"));
        }
        Ok(())
    }

    // fake metadata carrying the fields --overwrite-compare can use (size, mtime, uid, gid, mode).
    #[derive(Clone, Debug)]
    struct CmpMeta {
        uid: u32,
        gid: u32,
        mode: u32,
        size: u64,
        mtime: i64,
    }
    impl crate::preserve::Metadata for CmpMeta {
        fn uid(&self) -> u32 {
            self.uid
        }
        fn gid(&self) -> u32 {
            self.gid
        }
        fn atime(&self) -> i64 {
            0
        }
        fn atime_nsec(&self) -> i64 {
            0
        }
        fn mtime(&self) -> i64 {
            self.mtime
        }
        fn mtime_nsec(&self) -> i64 {
            0
        }
        fn permissions(&self) -> std::fs::Permissions {
            std::os::unix::fs::PermissionsExt::from_mode(self.mode)
        }
        fn size(&self) -> u64 {
            self.size
        }
    }

    fn size_mtime() -> filecmp::MetadataCmpSettings {
        filecmp::MetadataCmpSettings {
            size: true,
            mtime: true,
            ..Default::default()
        }
    }
    fn base() -> CmpMeta {
        CmpMeta {
            uid: 0,
            gid: 0,
            mode: 0o644,
            size: 10,
            mtime: 100,
        }
    }

    #[test]
    fn skip_send_no_existing_entry_sends() {
        let src = base();
        let r = skip_unchanged_send(
            &size_mtime(),
            None,
            false,
            &src,
            None::<ExistingDst<CmpMeta>>,
        );
        assert!(!r);
    }

    #[test]
    fn skip_send_ignore_existing_skips_any_type() {
        let src = base();
        let dst = base();
        let r = skip_unchanged_send(
            &size_mtime(),
            None,
            true,
            &src,
            Some(ExistingDst {
                meta: &dst,
                is_file: false,
            }),
        );
        assert!(r);
    }

    #[test]
    fn skip_send_overwrite_identical_skips() {
        let src = base();
        let dst = base();
        let r = skip_unchanged_send(
            &size_mtime(),
            None,
            false,
            &src,
            Some(ExistingDst {
                meta: &dst,
                is_file: true,
            }),
        );
        assert!(r);
    }

    #[test]
    fn skip_send_overwrite_different_size_sends() {
        let src = base();
        let dst = CmpMeta { size: 11, ..base() };
        let r = skip_unchanged_send(
            &size_mtime(),
            None,
            false,
            &src,
            Some(ExistingDst {
                meta: &dst,
                is_file: true,
            }),
        );
        assert!(!r);
    }

    #[test]
    fn skip_send_overwrite_different_mtime_sends() {
        let src = base();
        let dst = CmpMeta {
            mtime: 200,
            ..base()
        };
        let r = skip_unchanged_send(
            &size_mtime(),
            None,
            false,
            &src,
            Some(ExistingDst {
                meta: &dst,
                is_file: true,
            }),
        );
        assert!(!r);
    }

    #[test]
    fn skip_send_overwrite_non_file_sends() {
        let src = base();
        let dst = base();
        let r = skip_unchanged_send(
            &size_mtime(),
            None,
            false,
            &src,
            Some(ExistingDst {
                meta: &dst,
                is_file: false,
            }),
        );
        assert!(!r);
    }

    #[test]
    fn skip_send_filter_newer_skips_when_dest_newer() {
        let src = CmpMeta {
            mtime: 100,
            size: 5,
            ..base()
        };
        let dst = CmpMeta {
            mtime: 200,
            size: 11,
            ..base()
        };
        let r = skip_unchanged_send(
            &size_mtime(),
            Some(OverwriteFilter::Newer),
            false,
            &src,
            Some(ExistingDst {
                meta: &dst,
                is_file: true,
            }),
        );
        assert!(r);
    }

    #[test]
    fn skip_send_filter_newer_sends_when_dest_older() {
        let src = CmpMeta {
            mtime: 200,
            size: 5,
            ..base()
        };
        let dst = CmpMeta {
            mtime: 100,
            size: 11,
            ..base()
        };
        let r = skip_unchanged_send(
            &size_mtime(),
            Some(OverwriteFilter::Newer),
            false,
            &src,
            Some(ExistingDst {
                meta: &dst,
                is_file: true,
            }),
        );
        assert!(!r);
    }
}
