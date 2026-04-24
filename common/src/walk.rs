//! Shared primitives for directory-walking operations (copy, link, rm).
//!
//! [`EntryKind`] classifies a directory entry by file type, and exposes the
//! per-type bits (dry-run label, skipped-counter increment) so callers don't
//! re-implement the dispatch.
//!
//! [`next_entry_probed`] wraps `tokio::fs::ReadDir::next_entry` with the full
//! per-entry throttle + congestion-probe prologue so copy/link/rm share a
//! single source of truth for the "probe after permit, permit before
//! children" invariant.

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

/// Pull the next directory entry with the full per-entry throttle + probe
/// prologue applied:
///
/// 1. Await the static ops rate gate.
/// 2. Acquire the dynamic ops-in-flight permit (a no-op when
///    `--auto-meta-throttle` is not enabled).
/// 3. Start a metadata [`congestion::Probe`] — *after* the permit is held,
///    so self-inflicted queueing on the in-flight semaphore is not reported
///    back to the controller as filesystem latency.
/// 4. Call `next_entry()` and, on success, classify via `file_type()`.
/// 5. Complete the probe on success; discard it on error or exhaustion —
///    error paths must not skew the controller's latency baseline.
/// 6. Release the permit before returning, so the caller can spawn / await
///    children without holding it across task boundaries (which would
///    deadlock at any tree depth greater than cwnd).
///
/// The error is left as `anyhow::Error` so each caller can wrap it in the
/// site-specific error type (`copy::Error`, `link::Error`, `rm::Error`)
/// without this helper needing to be generic over the summary payload.
pub async fn next_entry_probed<F>(
    entries: &mut tokio::fs::ReadDir,
    context: F,
) -> anyhow::Result<Option<(tokio::fs::DirEntry, Option<std::fs::FileType>)>>
where
    F: FnOnce() -> String,
{
    throttle::get_ops_token().await;
    let ops_permit = throttle::ops_in_flight_permit().await;
    let probe = congestion::Probe::start_metadata();
    let maybe_entry = entries.next_entry().await.with_context(context)?;
    let Some(entry) = maybe_entry else {
        probe.discard();
        drop(ops_permit);
        return Ok(None);
    };
    let entry_file_type = match entry.file_type().await {
        Ok(file_type) => {
            probe.complete_ok(0);
            Some(file_type)
        }
        Err(_) => {
            probe.discard();
            None
        }
    };
    drop(ops_permit);
    Ok(Some((entry, entry_file_type)))
}

/// Bracket a single metadata-producing future with the cwnd permit and a
/// congestion probe. The probe completes successfully when `op` returns
/// `Ok`, and is discarded on error so error paths don't skew the
/// controller's latency baseline.
///
/// Use this at sites that perform one isolated metadata op (e.g. filegen's
/// `create_dir` / `open`). For directory iteration, prefer
/// [`next_entry_probed`] which also handles the `next_entry` +
/// `file_type` classification in one unit.
///
/// Note: unlike [`next_entry_probed`], this helper does **not** acquire
/// the static ops rate token — callers that rate-limit at a different
/// granularity (such as filegen, which gates at per-task spawn time)
/// would otherwise double-count.
pub async fn run_metadata_probed<F, T, E>(op: F) -> Result<T, E>
where
    F: std::future::Future<Output = Result<T, E>>,
{
    let ops_permit = throttle::ops_in_flight_permit().await;
    let probe = congestion::Probe::start_metadata();
    let result = op.await;
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
