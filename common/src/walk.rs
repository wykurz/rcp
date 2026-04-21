//! Shared primitives for directory-walking operations (copy, link, rm).
//!
//! [`EntryKind`] classifies a directory entry by file type, and exposes the
//! per-type bits (dry-run label, skipped-counter increment) so callers don't
//! re-implement the dispatch.

use crate::filter::{FilterResult, FilterSettings};
use crate::progress::Progress;

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
