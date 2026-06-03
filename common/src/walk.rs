//! Shared primitives for directory-walking operations (copy, link, rm).
//!
//! [`EntryKind`] classifies a directory entry by file type, and exposes the
//! per-type bits (dry-run label, skipped-counter increment) so callers don't
//! re-implement the dispatch.
//!
//! [`next_entry_probed`] wraps `tokio::fs::ReadDir::next_entry` (plus the
//! follow-up `file_type()` lookup) with the static ops rate gate so
//! copy/link/rm share a single source of truth for walk iteration. The
//! walk path is deliberately not congestion-probed — see the function's
//! own docs for why.

use crate::filter::{FilterResult, FilterSettings};
use crate::progress::Progress;
use anyhow::Context;

/// Classification of a filesystem entry by type.
///
/// `Special` covers sockets, FIFOs, block/character devices — anything that
/// isn't a regular file, directory, or symlink. When a caller has only a best
/// effort `Option<FileType>` (e.g. `entry.file_type().await.ok()`), an unknown
/// type is treated as `File` to match historical behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    File,
    Dir,
    Symlink,
    Special,
}

impl EntryKind {
    /// Classify from a `Metadata` (root-level entries, where we always have full metadata).
    #[must_use]
    pub fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        if metadata.is_dir() {
            Self::Dir
        } else if metadata.is_symlink() {
            Self::Symlink
        } else if metadata.is_file() {
            Self::File
        } else {
            Self::Special
        }
    }
    /// Classify from an `Option<FileType>`. Unknown types (`None`) are treated
    /// as `File` to match historical behavior across copy/link/rm: when
    /// `entry.file_type()` fails, callers proceed as if the entry were a
    /// regular file.
    #[must_use]
    pub fn from_file_type(file_type: Option<&std::fs::FileType>) -> Self {
        match file_type {
            Some(ft) if ft.is_dir() => Self::Dir,
            Some(ft) if ft.is_symlink() => Self::Symlink,
            Some(ft) if ft.is_file() => Self::File,
            Some(_) => Self::Special,
            None => Self::File,
        }
    }
    /// Short dry-run label used during directory iteration (`"dir"`, `"symlink"`, `"file"`).
    /// `Special` maps to `"file"` to match historical behavior — the old bool-triplet
    /// dispatch in copy/link/rm fell through `is_dir`/`is_symlink` to "file" for any
    /// other type. The explicit `--skip-specials` path uses its own literal "special"
    /// string and does not call this helper.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Dir => "dir",
            Self::Symlink => "symlink",
            Self::File | Self::Special => "file",
        }
    }
    /// Long dry-run label used at the root level (`"directory"` instead of `"dir"`).
    /// `Special` maps to `"file"` for the same reason as [`Self::label`].
    #[must_use]
    pub fn label_long(self) -> &'static str {
        match self {
            Self::Dir => "directory",
            Self::Symlink => "symlink",
            Self::File | Self::Special => "file",
        }
    }
    /// Increment the skipped counter that matches this entry kind. Special
    /// files count as `files_skipped` — `specials_skipped` is reserved for
    /// the explicit `--skip-specials` path, not filter skips.
    pub fn inc_skipped(self, prog: &Progress) {
        match self {
            Self::Dir => prog.directories_skipped.inc(),
            Self::Symlink => prog.symlinks_skipped.inc(),
            Self::File | Self::Special => prog.files_skipped.inc(),
        }
    }
}

/// Which backpressure pool a leaf's pre-acquired permit comes from.
///
/// The two pools are deliberately distinct (see [`LeafPermit`]) — this enum
/// selects between them per tool, plus a `None` variant for metadata-only
/// walks (e.g. rcmp-style traversals) that take no leaf permit at all.
///
/// Unifying the *choice* of pool here — alongside [`LeafPermit`] and
/// [`preacquire_leaf_permit`] — is what lets the traversal driver own the
/// "drop the leaf permit before recursing into a directory" invariant in a
/// single place. Hand-coding that drop at every per-tool branch is the root
/// cause of the hold-and-wait deadlock class this lifecycle eliminates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermitKind {
    /// File-descriptor backpressure ([`throttle::open_file_permit`]) — for
    /// tools that hold an open fd across leaf work (copy, link).
    OpenFile,
    /// Task-spawn backpressure ([`throttle::pending_meta_permit`]) — for
    /// recursive metadata-only walks that don't hold an fd (chmod, rm).
    PendingMeta,
    /// The tool takes no leaf permit (it doesn't gate at the leaf).
    None,
}

/// A pre-acquired leaf permit, type-erased over the two distinct backpressure
/// pools so a single caller (the traversal driver) can hold either uniformly
/// and drop it in exactly one place before recursing into a directory.
///
/// The two pools must stay distinct: [`throttle::OpenFileGuard`] gates open
/// file descriptors while [`throttle::PendingMetaGuard`] gates in-flight
/// metadata-only tasks. They are sized independently so a path that composes
/// the two operations (e.g. `copy_file → rm` when overwriting a directory
/// destination) cannot self-deadlock against a saturated open-files pool.
/// This enum unifies only the *lifecycle*, never the pools themselves.
///
/// Neither guard is `Clone`; dropping a `LeafPermit` releases exactly the
/// underlying permit it wraps. The driver drops it before descending so a
/// directory entry never holds a leaf permit across its recursive walk —
/// the single home for the invariant that previously lived at 4/7/1/1
/// per-tool branch sites and shipped as a deadlock when a single-site tool
/// forgot it.
pub enum LeafPermit {
    /// A permit from the open-files pool.
    OpenFile(throttle::OpenFileGuard),
    /// A permit from the pending-metadata pool.
    PendingMeta(throttle::PendingMetaGuard),
}

/// Pre-acquire a leaf permit for a child about to be spawned, per the tool's
/// policy.
///
/// Returns `None` when `kind == PermitKind::None` or when `!want(hint)` — the
/// latter lets a tool opt out based on the cheap `getdents` `d_type` hint. The
/// key case: when the hint says "directory", the tool passes a `want` that
/// returns `false`, so no leaf permit is taken. That matters because a hinted
/// directory must NOT hold a leaf permit across recursion (the hold-and-wait
/// deadlock). Otherwise this acquires from the pool selected by `kind` and
/// wraps it in [`LeafPermit`].
///
/// `want` receives the raw hint (`None` for `DT_UNKNOWN`); the authoritative
/// type is only resolved later by the per-entry worker. A hint of `None`
/// therefore takes a permit when the tool's `want` admits it (matching the
/// historical "treat unknown as a leaf" behavior), and the worker re-classifies
/// and drops it if the entry turns out to be a directory.
pub async fn preacquire_leaf_permit(
    kind: PermitKind,
    hint: Option<EntryKind>,
    want: impl Fn(Option<EntryKind>) -> bool,
) -> Option<LeafPermit> {
    if kind == PermitKind::None || !want(hint) {
        return None;
    }
    match kind {
        PermitKind::OpenFile => Some(LeafPermit::OpenFile(throttle::open_file_permit().await)),
        PermitKind::PendingMeta => Some(LeafPermit::PendingMeta(
            throttle::pending_meta_permit().await,
        )),
        // unreachable: the early return above already handled `None`.
        PermitKind::None => None,
    }
}

/// Resolve the `throttle::Side` from the matching `congestion::Side`.
///
/// The two crates carry independent enum definitions to keep `throttle`
/// free of any congestion dependency; this is the canonical bridge
/// (paired with [`throttle_op`]) reused everywhere a congestion-side
/// signal needs to address a throttle resource.
pub(crate) fn throttle_side(side: congestion::Side) -> throttle::Side {
    match side {
        congestion::Side::Source => throttle::Side::Source,
        congestion::Side::Destination => throttle::Side::Destination,
    }
}

/// Resolve the `throttle::MetadataOp` from the matching `congestion::MetadataOp`.
pub(crate) fn throttle_op(op: congestion::MetadataOp) -> throttle::MetadataOp {
    match op {
        congestion::MetadataOp::Stat => throttle::MetadataOp::Stat,
        congestion::MetadataOp::ReadLink => throttle::MetadataOp::ReadLink,
        congestion::MetadataOp::MkDir => throttle::MetadataOp::MkDir,
        congestion::MetadataOp::RmDir => throttle::MetadataOp::RmDir,
        congestion::MetadataOp::Unlink => throttle::MetadataOp::Unlink,
        congestion::MetadataOp::HardLink => throttle::MetadataOp::HardLink,
        congestion::MetadataOp::Symlink => throttle::MetadataOp::Symlink,
        congestion::MetadataOp::Chmod => throttle::MetadataOp::Chmod,
        congestion::MetadataOp::OpenCreate => throttle::MetadataOp::OpenCreate,
    }
}

/// Resolve the [`throttle::Resource`] for a single per-file metadata
/// syscall on the given side.
pub(crate) fn meta_resource(
    side: congestion::Side,
    op: congestion::MetadataOp,
) -> throttle::Resource {
    throttle::Resource::meta(throttle_side(side), throttle_op(op))
}

/// Pull the next directory entry, gated only by the static ops rate
/// gate.
///
/// Walks are deliberately not probed: `tokio::fs::ReadDir::next_entry`
/// returns buffered entries from a prior `getdents` batch without
/// entering the kernel, so most "walk probes" don't measure filesystem
/// service time at all. The resulting bimodal latency distribution
/// (cache hit vs. real `getdents`) collapses any baseline a controller
/// could derive from it. The fix is to probe only the per-file
/// metadata syscalls that follow the walk, where each sample reflects
/// real filesystem work.
///
/// The prologue is therefore just:
///
/// 1. Await the static ops rate gate.
/// 2. Call `next_entry()` and, on success, classify via `file_type()`.
///
/// `side` is currently unused at runtime but kept on the signature so
/// callers stay self-documenting and future per-side gating can be
/// reintroduced without touching every call site.
///
/// The error is left as `anyhow::Error` so each caller can wrap it in the
/// site-specific error type (`copy::Error`, `link::Error`, `rm::Error`)
/// without this helper needing to be generic over the summary payload.
pub async fn next_entry_probed<F>(
    entries: &mut tokio::fs::ReadDir,
    _side: congestion::Side,
    context: F,
) -> anyhow::Result<Option<(tokio::fs::DirEntry, Option<std::fs::FileType>)>>
where
    F: FnOnce() -> String,
{
    throttle::get_ops_token().await;
    let maybe_entry = entries.next_entry().await.with_context(context)?;
    let Some(entry) = maybe_entry else {
        return Ok(None);
    };
    let entry_file_type = entry.file_type().await.ok();
    Ok(Some((entry, entry_file_type)))
}

/// Bracket a single metadata-producing future with the full per-op
/// gating prologue: the static ops rate gate, the cwnd permit for the
/// matching `(side, op_kind)` resource, and a congestion probe. The
/// probe completes successfully when `fut` returns `Ok`, and is
/// discarded on error so error paths don't skew the controller's
/// latency baseline.
///
/// `op_kind` selects which per-syscall controller this call is
/// reported to and gated by — `Stat`, `MkDir`, `Unlink`, etc. Pick the
/// variant that matches the underlying syscall (`metadata` /
/// `symlink_metadata` / `File::open(read)` / `canonicalize` all map to
/// `Stat`; `create_dir` to `MkDir`; `remove_file` to `Unlink`; and so
/// on — see [`congestion::MetadataOp`] for the full mapping).
///
/// `--ops-throttle` is the shared metadata rate gate, so this helper
/// acquires it on every call — same as [`next_entry_probed`]. Callers
/// that already rate-gate upstream (such as filegen, which gates at
/// per-task spawn time so we don't fan out an unbounded task queue
/// before any token is consumed) must use
/// [`run_metadata_probed_no_rate`] instead to avoid double-counting.
pub async fn run_metadata_probed<F, T, E>(
    side: congestion::Side,
    op_kind: congestion::MetadataOp,
    fut: F,
) -> Result<T, E>
where
    F: std::future::Future<Output = Result<T, E>>,
{
    throttle::get_ops_token().await;
    run_metadata_probed_no_rate(side, op_kind, fut).await
}

/// Variant of [`run_metadata_probed`] that skips the static ops rate
/// gate — for callers that already rate-limit at a coarser granularity
/// upstream and would otherwise consume two tokens per metadata op.
///
/// Concretely: `filegen` gates the rate at task-spawn time so the
/// number of in-flight `write_file` futures stays bounded by the rate.
/// The `OpenOptions::open(O_CREAT)` inside the spawned task is the only
/// metadata syscall in that path; rate-gating it again would halve the
/// effective rate.
pub async fn run_metadata_probed_no_rate<F, T, E>(
    side: congestion::Side,
    op_kind: congestion::MetadataOp,
    fut: F,
) -> Result<T, E>
where
    F: std::future::Future<Output = Result<T, E>>,
{
    let ops_permit = throttle::ops_in_flight_permit(meta_resource(side, op_kind)).await;
    let probe = congestion::Probe::start_metadata(side, op_kind);
    let result = fut.await;
    match &result {
        Ok(_) => probe.complete_ok(0),
        Err(_) => probe.discard(),
    }
    drop(ops_permit);
    result
}

/// Determine the `is_dir` value to feed an include/exclude FILTER decision for a
/// directory entry, using the authoritative `fstat` type when the cheap
/// `getdents` `d_type` hint is unavailable.
///
/// `read_entries` returns each entry's `d_type` as a best-effort hint; on
/// filesystems that don't populate it (NFS, some FUSE mounts) the hint is `None`
/// (`DT_UNKNOWN`). Treating `None` as "not a directory" for an `is_dir`-dependent
/// filter (e.g. `--include '/sub/**'`) would wrongly EXCLUDE a real directory and
/// omit its entire subtree. To match the old path-based walk (which fell back to
/// an `lstat` via `DirEntry::file_type()`), this resolves the type
/// AUTHORITATIVELY via `dir.child(name)`'s `fstat` — but ONLY when the hint is
/// `None` AND the type is actually needed: either a filter is active, or the
/// caller passes `force_authoritative` because its own control flow depends on
/// the result (e.g. a dry-run recurse-vs-leaf decision). When the hint is
/// reliable, or the type is unneeded, the cheap hint is used directly (no extra
/// syscall), preserving the optimization.
///
/// `child(name)` uses `O_PATH|O_NOFOLLOW`, so this never follows a symlink (a
/// symlink entry classifies as `Symlink`, not `Dir`) and never blocks: the
/// hardening of the walk below the named root is unaffected. On a `child` error
/// (e.g. the entry vanished mid-walk) the hint-derived value is used as a
/// fallback — the entry's own per-entry worker will then surface the error
/// authoritatively.
pub async fn filter_is_dir(
    filter: Option<&FilterSettings>,
    dir: &crate::safedir::Dir,
    name: &std::ffi::OsStr,
    hint: Option<EntryKind>,
    force_authoritative: bool,
) -> bool {
    match hint {
        Some(kind) => kind == EntryKind::Dir,
        // DT_UNKNOWN: only pay for the authoritative fstat when the type is actually
        // needed — a filter needs it, or the caller's control flow depends on it
        // (`force_authoritative`, e.g. a dry-run recurse-vs-leaf decision). Otherwise
        // the value is unused, so default cheaply.
        None if filter.is_some() || force_authoritative => match dir.child(name).await {
            Ok(handle) => handle.kind() == EntryKind::Dir,
            // entry changed/vanished: fall back to the historical non-dir default;
            // the per-entry worker re-classifies and reports any real error.
            Err(_) => false,
        },
        None => false,
    }
}

/// Decide whether an entry should be skipped by the filter, returning the
/// `FilterResult` that caused the skip. Returns `None` if there is no filter
/// or the entry is included.
#[must_use]
pub fn should_skip_entry(
    filter: &Option<FilterSettings>,
    relative_path: &std::path::Path,
    is_dir: bool,
) -> Option<FilterResult> {
    should_skip_entry_ref(filter.as_ref(), relative_path, is_dir)
}

/// [`should_skip_entry`] taking the filter by `Option<&_>` rather than
/// `&Option<_>`, so callers that already hold an `Option<&FilterSettings>` (the
/// traversal driver) don't have to clone it. Identical semantics otherwise.
#[must_use]
pub fn should_skip_entry_ref(
    filter: Option<&FilterSettings>,
    relative_path: &std::path::Path,
    is_dir: bool,
) -> Option<FilterResult> {
    if let Some(f) = filter {
        let result = f.should_include(relative_path, is_dir);
        match result {
            FilterResult::Included => None,
            _ => Some(result),
        }
    } else {
        None
    }
}

/// Path of `entry` relative to `root` (typically `source_root` or `dest_root` at the call
/// site), with the `unwrap_or(entry)` defensive fallback rcp uses when `entry` isn't
/// actually under `root`. Naming the pattern lets call sites read "the entry's path inside
/// the tree" instead of `entry.strip_prefix(root).unwrap_or(entry)` — and removes a class
/// of "did I get strip_prefix the right way round?" regressions.
///
/// Use with `filter_base.join(...)` for a logical filter path inside a delegated subtree
/// (`copy_with_filter_base`'s non-empty `filter_base` case), or on its own when filter_base
/// is empty.
#[must_use]
pub fn relative_to_root<'a>(
    entry: &'a std::path::Path,
    root: &std::path::Path,
) -> &'a std::path::Path {
    entry.strip_prefix(root).unwrap_or(entry)
}

/// Strip trailing path separators from a root operand. A trailing slash forces the OS to resolve
/// the final component as a directory, which would dereference a symlink root like `link/`
/// (following it to its target). Stripping makes `link/` behave like `link` (the symlink itself),
/// which is then classified/operated on `O_NOFOLLOW` relative to its parent fd.
#[must_use]
pub fn without_trailing_separators(path: &std::path::Path) -> std::path::PathBuf {
    use std::os::unix::ffi::OsStrExt;
    let bytes = path.as_os_str().as_bytes();
    let mut end = bytes.len();
    while end > 1 && bytes[end - 1] == b'/' {
        end -= 1;
    }
    std::path::PathBuf::from(std::ffi::OsStr::from_bytes(&bytes[..end]))
}

/// A root operand decomposed for the fd-relative walk entry point.
pub struct RootOperand {
    /// The operand's parent directory — opened TRUSTED (follows symlinks) via
    /// [`crate::safedir::Dir::open_parent_dir`].
    pub parent: std::path::PathBuf,
    /// The operand's final component — classified `O_NOFOLLOW` via `child(name)` below the parent.
    pub name: std::ffi::OsString,
    /// The operand path for diagnostics / `real_path` reconstruction: the operand as typed (with
    /// trailing slashes stripped) for a normal operand, or the canonicalized path for a `.`/`..`
    /// operand that had to be resolved.
    pub display: std::path::PathBuf,
}

/// Decompose a root operand into the `(parent, final_component)` the fd-relative walk needs (see
/// [`RootOperand`]): the parent prefix is opened with [`crate::safedir::Dir::open_parent_dir`]
/// (trusted, follows symlinks) and the final component is then classified `O_NOFOLLOW` via
/// `child(name)` below it.
///
/// Most operands split directly via `parent()` / `file_name()` (an empty parent meaning the current
/// directory). An operand whose final component is `.` or `..` (e.g. `.`, `tree/..`) has no
/// `file_name()`, so it is first canonicalized to a concrete path — `.` becomes the current
/// directory, `tree/..` its grandparent — restoring the behavior of the pre-fd-walk path code,
/// where `rrm .` / `rchm -R … .` operated on the current tree. (Canonicalizing only this branch
/// never touches a normal operand, so a symlinked final component on the normal path is still
/// opened `O_NOFOLLOW`; `.`/`..` are themselves never symlinks.) The filesystem root `/` has no
/// parent and cannot be expressed as parent + component, so it is rejected with a clear error.
pub async fn split_root_operand(path: &std::path::Path) -> anyhow::Result<RootOperand> {
    let stripped = without_trailing_separators(path);
    if let Some(name) = stripped.file_name() {
        let parent = match stripped.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            // empty parent (a single-component relative path) means the current directory.
            _ => std::path::PathBuf::from("."),
        };
        let name = name.to_owned();
        return Ok(RootOperand {
            parent,
            name,
            display: stripped,
        });
    }
    // final component is `.`/`..` (or the operand is `/`): canonicalize to a concrete path so it can
    // be split into parent + component.
    let canonical = tokio::fs::canonicalize(&stripped)
        .await
        .with_context(|| format!("cannot resolve operand {stripped:?}"))?;
    let name = canonical.file_name().map(std::ffi::OsStr::to_owned);
    let parent = canonical.parent().map(std::path::Path::to_path_buf);
    let (Some(parent), Some(name)) = (parent, name) else {
        anyhow::bail!("cannot operate on the filesystem root {canonical:?}");
    };
    Ok(RootOperand {
        parent,
        name,
        display: canonical,
    })
}

/// The basename a root operand contributes to the trailing-slash "copy INTO directory" rule: the
/// name of the entry the walk will create for `path`, so a CLI forming `dst/<name>` matches it
/// exactly (`rcp . out/` / `rlink . out/` -> `out/<cwd-name>`; `… tree/.. out/` ->
/// `out/<parent-name>`).
///
/// This is the synchronous twin of [`split_root_operand`]'s `name`, for the CLI path-resolution
/// layer (which is not async): a normal operand uses [`std::path::Path::file_name`]; an operand
/// with no `file_name()` (`.`/`..`/`dir/..`) is canonicalized first, exactly as
/// [`split_root_operand`] does. The filesystem root has no basename and is rejected. The
/// `root_operand_basename_matches_split_root_operand` test keeps the two in lock-step.
pub fn root_operand_basename(path: &std::path::Path) -> anyhow::Result<std::ffi::OsString> {
    let stripped = without_trailing_separators(path);
    if let Some(name) = stripped.file_name() {
        return Ok(name.to_owned());
    }
    // final component is `.`/`..` (or the operand is `/`): canonicalize to a concrete path so it
    // has a real basename — the same resolution `split_root_operand` performs for the walk.
    let canonical = std::fs::canonicalize(&stripped)
        .with_context(|| format!("cannot resolve operand {stripped:?}"))?;
    canonical
        .file_name()
        .map(std::ffi::OsStr::to_owned)
        .ok_or_else(|| anyhow::anyhow!("cannot operate on the filesystem root {canonical:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::safedir::Dir;
    use crate::testutils;
    use std::ffi::OsStr;

    fn include_filter(pattern: &str) -> Option<FilterSettings> {
        let mut f = FilterSettings::new();
        f.add_include(pattern).unwrap();
        Some(f)
    }

    // FIX B (PR #247 review): when the getdents `d_type` hint is unavailable (DT_UNKNOWN -> None)
    // AND a filter is active, the filter `is_dir` decision must come from the AUTHORITATIVE fstat,
    // not default to non-dir. Otherwise a real directory reported as DT_UNKNOWN would be wrongly
    // excluded by an is_dir-dependent include filter, omitting its whole subtree. We can't force
    // DT_UNKNOWN on a normal local fs, so we drive `filter_is_dir` with `hint = None` directly —
    // exactly the value `read_entries` would yield on NFS/FUSE — to exercise the authoritative path.
    #[tokio::test]
    async fn filter_is_dir_authoritatively_classifies_dt_unknown_directory() -> anyhow::Result<()> {
        let tmp = testutils::setup_test_dir().await?;
        // fixture: tmp/foo holds `bar` (a real directory) and `0.txt` (a real file).
        let dir = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        let filter = include_filter("/bar/**");
        // DT_UNKNOWN + active filter on a real DIRECTORY -> must resolve to `true` via fstat, so an
        // include filter does NOT omit its subtree (the regression this fix closes).
        assert!(
            filter_is_dir(filter.as_ref(), &dir, OsStr::new("bar"), None, false).await,
            "a real directory reported as DT_UNKNOWN must classify as a directory for the filter"
        );
        // DT_UNKNOWN + active filter on a real FILE -> resolves to `false` authoritatively.
        assert!(
            !filter_is_dir(filter.as_ref(), &dir, OsStr::new("0.txt"), None, false).await,
            "a real file reported as DT_UNKNOWN must classify as a non-directory"
        );
        Ok(())
    }

    // FIX B: a reliable hint is used directly (no fstat), and with no filter active the value is
    // the cheap default — the optimization is preserved (we never pay an fstat we don't need).
    #[tokio::test]
    async fn filter_is_dir_uses_hint_when_available_and_skips_when_no_filter() -> anyhow::Result<()>
    {
        let tmp = testutils::setup_test_dir().await?;
        let dir = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        let filter = include_filter("/bar/**");
        // reliable Dir hint -> true regardless of fstat.
        assert!(
            filter_is_dir(
                filter.as_ref(),
                &dir,
                OsStr::new("bar"),
                Some(EntryKind::Dir),
                false
            )
            .await
        );
        // reliable File hint -> false.
        assert!(
            !filter_is_dir(
                filter.as_ref(),
                &dir,
                OsStr::new("0.txt"),
                Some(EntryKind::File),
                false
            )
            .await
        );
        // DT_UNKNOWN, NO filter, and not forced -> cheap non-dir default (no authoritative fstat
        // needed); the `name` is never resolved, so it need not even exist.
        assert!(!filter_is_dir(None, &dir, OsStr::new("does_not_exist"), None, false).await);
        Ok(())
    }

    // force_authoritative (e.g. rlink --dry-run, which uses is_dir for its recurse-vs-leaf
    // branch): a DT_UNKNOWN hint with NO filter must still classify AUTHORITATIVELY when the
    // caller's control flow depends on the result, so a dry-run on NFS/FUSE doesn't preview a
    // real directory as a leaf and skip its subtree.
    #[tokio::test]
    async fn filter_is_dir_forces_authoritative_classification_when_requested() -> anyhow::Result<()>
    {
        let tmp = testutils::setup_test_dir().await?;
        let dir = Dir::open_root_dir(&tmp.join("foo"), false, congestion::Side::Source).await?;
        // DT_UNKNOWN + NO filter, but force_authoritative -> a real directory must classify as dir.
        assert!(
            filter_is_dir(None, &dir, OsStr::new("bar"), None, true).await,
            "force_authoritative must fstat a DT_UNKNOWN directory even with no filter"
        );
        // ...and a real file as non-dir.
        assert!(
            !filter_is_dir(None, &dir, OsStr::new("0.txt"), None, true).await,
            "force_authoritative must fstat a DT_UNKNOWN file even with no filter"
        );
        Ok(())
    }

    // The leaf-permit lifecycle: `PermitKind::None` and a rejecting `want` both opt
    // out (the basis for "a hinted directory takes no permit, so it can't deadlock
    // by holding one across recursion"); a matching `want` acquires from the
    // requested pool. The pools are process-global; configure a small cap so the
    // OpenFile case exercises a real acquire rather than the disabled-pool no-op.
    #[tokio::test]
    async fn preacquire_leaf_permit_respects_kind_and_want() {
        throttle::set_max_open_files(4);
        // `None` kind never takes a permit, regardless of hint/want.
        assert!(
            preacquire_leaf_permit(PermitKind::None, Some(EntryKind::File), |_| true)
                .await
                .is_none()
        );
        // matching want on the OpenFile pool yields an OpenFile permit.
        let permit = preacquire_leaf_permit(PermitKind::OpenFile, Some(EntryKind::File), |h| {
            h == Some(EntryKind::File)
        })
        .await;
        assert!(matches!(permit, Some(LeafPermit::OpenFile(_))));
        drop(permit);
        // a rejecting want (e.g. the hint says "directory") opts out even though the
        // pool is configured — the hinted-dir-takes-no-permit case.
        assert!(
            preacquire_leaf_permit(PermitKind::OpenFile, Some(EntryKind::Dir), |_| false)
                .await
                .is_none()
        );
    }

    // split_root_operand: a normal operand splits via parent()/file_name() (single component ->
    // parent "."), a trailing slash is stripped, a trailing `/.` names the directory itself, and —
    // the regression fix — a bare `.` or an operand ending in `..` (no file_name()) is canonicalized
    // so it still names a directory instead of being rejected. The filesystem root `/` is rejected.
    #[tokio::test]
    async fn split_root_operand_handles_dot_and_normal_operands() -> anyhow::Result<()> {
        use std::ffi::OsStr;
        use std::path::Path;
        // normal nested operand: split verbatim.
        let op = split_root_operand(Path::new("a/b")).await?;
        assert_eq!(op.parent, Path::new("a"));
        assert_eq!(op.name, OsStr::new("b"));
        assert_eq!(op.display, Path::new("a/b"));
        // single component: parent defaults to the current directory.
        let op = split_root_operand(Path::new("foo")).await?;
        assert_eq!(op.parent, Path::new("."));
        assert_eq!(op.name, OsStr::new("foo"));
        // trailing slash stripped (so a symlink root is classified O_NOFOLLOW, not dereferenced).
        let op = split_root_operand(Path::new("foo/")).await?;
        assert_eq!(op.name, OsStr::new("foo"));
        // trailing `/.` names the directory itself: `file_name()` normalizes the `.` away, so it
        // splits verbatim (no canonicalize) — `dir/.` -> parent ".", name "dir"; `a/b/.` -> "a","b".
        let op = split_root_operand(Path::new("dir/.")).await?;
        assert_eq!(op.parent, Path::new("."));
        assert_eq!(op.name, OsStr::new("dir"));
        let op = split_root_operand(Path::new("a/b/.")).await?;
        assert_eq!(op.parent, Path::new("a"));
        assert_eq!(op.name, OsStr::new("b"));
        // bare `.` (no file_name): canonicalized to the current directory.
        let cwd = tokio::fs::canonicalize(".").await?;
        let op = split_root_operand(Path::new(".")).await?;
        assert_eq!(op.parent, cwd.parent().unwrap());
        assert_eq!(op.name, cwd.file_name().unwrap());
        assert_eq!(op.display, cwd);
        // operand ending in `..` (no file_name): canonicalized so it still names a directory.
        // tmp/sub/.. resolves to tmp — the `rrm .` / `rchm -R … .` regression this fix closes.
        let tmp = testutils::create_temp_dir().await?;
        let sub = tmp.join("sub");
        tokio::fs::create_dir(&sub).await?;
        let canonical_tmp = tokio::fs::canonicalize(&tmp).await?;
        let op = split_root_operand(&sub.join("..")).await?;
        assert_eq!(op.parent, canonical_tmp.parent().unwrap());
        assert_eq!(op.name, canonical_tmp.file_name().unwrap());
        assert_eq!(op.display, canonical_tmp);
        // the filesystem root has no parent and is rejected with a clear error.
        assert!(split_root_operand(Path::new("/")).await.is_err());
        Ok(())
    }

    // `root_operand_basename` (the sync CLI helper) must return exactly `split_root_operand`'s
    // `name` for every operand shape, so a `dst/<name>` the CLI builds always matches the entry the
    // walk creates. This lock-step is what makes the trailing-slash result deterministic.
    #[tokio::test]
    async fn root_operand_basename_matches_split_root_operand() -> anyhow::Result<()> {
        use std::path::Path;
        // operands with a real `file_name()` (no canonicalize): both take the lexical basename.
        for p in ["a/b", "foo", "foo/", "dir/.", "a/b/."] {
            assert_eq!(
                root_operand_basename(Path::new(p))?,
                split_root_operand(Path::new(p)).await?.name,
                "operand {p:?}"
            );
        }
        // `.`/`..`/`dir/..` (no `file_name()`): both canonicalize to a real path first. Use real
        // dirs so canonicalize succeeds; assert only that the two helpers agree (the concrete
        // basename depends on the cwd / temp path).
        let tmp = testutils::create_temp_dir().await?;
        let sub = tmp.join("sub");
        tokio::fs::create_dir(&sub).await?;
        let dot_operands = [
            std::path::PathBuf::from("."),
            std::path::PathBuf::from(".."),
            sub.join(".."),
            sub.join("../.."),
        ];
        for p in &dot_operands {
            assert_eq!(
                root_operand_basename(p)?,
                split_root_operand(p).await?.name,
                "operand {p:?}"
            );
        }
        // the filesystem root has no basename: both reject it.
        assert!(root_operand_basename(Path::new("/")).is_err());
        assert!(split_root_operand(Path::new("/")).await.is_err());
        Ok(())
    }
}
