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

/// Decide whether an entry should be skipped by the filter, returning the
/// `FilterResult` that caused the skip. Returns `None` if there is no filter
/// or the entry is included.
#[must_use]
pub fn should_skip_entry(
    filter: &Option<FilterSettings>,
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
