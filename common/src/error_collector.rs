//! Collects and deduplicates errors for non-fail-early operation modes.
//!
//! All RCP tools (rcp, rrm, rlink, rcmp) can encounter multiple errors during a single run.
//! When `--fail-early` is not set, tools continue past errors and report them at the end.
//! This module provides [`ErrorCollector`] to accumulate those errors, deduplicate cascading
//! failures (e.g., "Permission denied" from many files in the same unwritable directory),
//! and produce a final error with the root cause chain intact.
//!
//! # Thread Safety
//!
//! [`ErrorCollector`] uses [`std::sync::Mutex`] internally, making it safe to share across
//! async tasks via `Arc<ErrorCollector>`. The lock is held only for brief push/check
//! operations, never across `.await` points.
//!
//! # Example
//!
//! ```
//! use common::error_collector::ErrorCollector;
//!
//! let collector = ErrorCollector::new(20);
//! collector.push(anyhow::anyhow!("Permission denied (os error 13)"));
//! collector.push(anyhow::anyhow!("Permission denied (os error 13)"));
//! assert!(collector.has_errors());
//! // returns the first error with its chain intact since there's only one unique cause
//! let error = collector.into_error().unwrap();
//! assert!(format!("{:#}", error).contains("Permission denied"));
//! ```
use std::collections::HashSet;

const DEFAULT_MAX_UNIQUE: usize = 20;

/// Thread-safe error collector that deduplicates errors by root cause.
///
/// Designed to replace the `error_occurred: bool` + generic-error-message pattern
/// used throughout the codebase. Stores the first error with its full [`anyhow::Error`]
/// chain intact, deduplicates subsequent errors by root cause string, and caps the
/// number of unique causes tracked.
pub struct ErrorCollector {
    inner: std::sync::Mutex<Inner>,
}

impl std::fmt::Debug for ErrorCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        f.debug_struct("ErrorCollector")
            .field("total_count", &inner.total_count)
            .field("unique_causes", &inner.unique_causes.len())
            .finish()
    }
}

struct Inner {
    /// first error preserved with full anyhow chain
    first_error: Option<anyhow::Error>,
    /// deduplicated root cause strings, insertion-ordered
    unique_causes: Vec<String>,
    /// set for O(1) dedup lookups
    seen_causes: HashSet<String>,
    /// total errors pushed, including duplicates
    total_count: usize,
    /// maximum number of unique causes to store
    max_unique: usize,
    /// number of unique causes dropped because the cap was reached
    dropped_unique_count: usize,
}

impl ErrorCollector {
    /// Creates a new collector that tracks up to `max_unique` distinct root causes.
    #[must_use]
    pub fn new(max_unique: usize) -> Self {
        Self {
            inner: std::sync::Mutex::new(Inner {
                first_error: None,
                unique_causes: Vec::new(),
                seen_causes: HashSet::new(),
                total_count: 0,
                max_unique,
                dropped_unique_count: 0,
            }),
        }
    }

    /// Records an error. The first error's full chain is preserved; subsequent errors
    /// are deduplicated by their root cause string.
    pub fn push(&self, error: anyhow::Error) {
        let root_cause = error.root_cause().to_string();
        let mut inner = self.inner.lock().unwrap();
        inner.total_count += 1;
        if inner.first_error.is_none() {
            inner.first_error = Some(error);
        }
        if inner.seen_causes.contains(&root_cause) {
            return;
        }
        if inner.unique_causes.len() < inner.max_unique {
            inner.seen_causes.insert(root_cause.clone());
            inner.unique_causes.push(root_cause);
        } else {
            // past cap: don't grow seen_causes (keeps memory bounded).
            // repeated beyond-cap causes may increment this counter more than once,
            // so it's a count of dropped errors, not necessarily unique.
            inner.dropped_unique_count += 1;
        }
    }

    /// Returns `true` if any errors have been recorded.
    pub fn has_errors(&self) -> bool {
        self.inner.lock().unwrap().total_count > 0
    }

    /// Returns the final error, or `None` if no errors occurred.
    ///
    /// - If there is exactly one unique root cause, returns the original first error
    ///   with its full anyhow chain intact (so `{:#}` works correctly downstream).
    /// - If there are multiple unique root causes, returns a new error listing all of them.
    /// - If more errors were seen than tracked, appends a count of suppressed causes.
    ///
    /// Can be called on a shared reference (e.g., through `Arc`). Takes the first error
    /// out of the collector, so subsequent calls will return a synthesized error if any
    /// errors were recorded.
    pub fn take_error(&self) -> Option<anyhow::Error> {
        let mut inner = self.inner.lock().unwrap();
        if inner.total_count == 0 {
            return None;
        }
        // single unique cause with nothing dropped: return the original error with full chain
        if inner.unique_causes.len() <= 1 && inner.dropped_unique_count == 0 {
            if let Some(err) = inner.first_error.take() {
                return Some(err);
            }
            // first_error already taken by a previous call - synthesize from stored cause
            if let Some(cause) = inner.unique_causes.first() {
                return Some(anyhow::anyhow!("{}", cause));
            }
            return None;
        }
        // multiple unique causes: build a summary listing each
        let mut msg = String::from("multiple errors occurred:");
        for cause in &inner.unique_causes {
            msg.push_str("\n- ");
            msg.push_str(cause);
        }
        if inner.dropped_unique_count > 0 {
            msg.push_str(&format!(
                "\n({} additional errors suppressed)",
                inner.dropped_unique_count
            ));
        }
        Some(anyhow::anyhow!("{msg}"))
    }
    /// Consumes the collector and returns the final error. Equivalent to [`Self::take_error`]
    /// but takes ownership, guaranteeing no other references exist.
    pub fn into_error(self) -> Option<anyhow::Error> {
        self.take_error()
    }
}

impl Default for ErrorCollector {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_UNIQUE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_errors_returns_none() {
        let collector = ErrorCollector::default();
        assert!(!collector.has_errors());
        assert!(collector.into_error().is_none());
    }

    #[test]
    fn single_error_preserves_chain() {
        let collector = ErrorCollector::default();
        let original =
            anyhow::anyhow!("Permission denied (os error 13)").context("failed to create file");
        collector.push(original);
        assert!(collector.has_errors());
        let err = collector.into_error().unwrap();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("failed to create file"),
            "expected context in '{msg}'"
        );
        assert!(
            msg.contains("Permission denied"),
            "expected root cause in '{msg}'"
        );
    }

    #[test]
    fn duplicate_root_causes_deduped() {
        let collector = ErrorCollector::default();
        collector.push(anyhow::anyhow!("Permission denied (os error 13)"));
        collector.push(anyhow::anyhow!("Permission denied (os error 13)"));
        collector.push(anyhow::anyhow!("Permission denied (os error 13)"));
        // single unique cause -> returns original first error
        let err = collector.into_error().unwrap();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("Permission denied"),
            "expected root cause in '{msg}'"
        );
        assert!(
            !msg.contains("multiple errors"),
            "single unique cause should not say 'multiple errors': '{msg}'"
        );
    }

    #[test]
    fn multiple_unique_causes_listed() {
        let collector = ErrorCollector::default();
        collector.push(anyhow::anyhow!("Permission denied (os error 13)"));
        collector.push(anyhow::anyhow!("No space left on device (os error 28)"));
        let err = collector.into_error().unwrap();
        let msg = format!("{}", err);
        assert!(
            msg.contains("multiple errors occurred:"),
            "expected multi-error header in '{msg}'"
        );
        assert!(
            msg.contains("Permission denied"),
            "expected first cause in '{msg}'"
        );
        assert!(
            msg.contains("No space left on device"),
            "expected second cause in '{msg}'"
        );
    }

    #[test]
    fn respects_max_unique_cap() {
        let collector = ErrorCollector::new(2);
        collector.push(anyhow::anyhow!("error A"));
        collector.push(anyhow::anyhow!("error B"));
        collector.push(anyhow::anyhow!("error C")); // exceeds cap
        collector.push(anyhow::anyhow!("error C")); // not in seen_causes (bounded), counted again
        let err = collector.into_error().unwrap();
        let msg = format!("{}", err);
        assert!(msg.contains("error A"), "expected first cause in '{msg}'");
        assert!(msg.contains("error B"), "expected second cause in '{msg}'");
        assert!(
            !msg.contains("error C"),
            "third cause should be suppressed in '{msg}'"
        );
        assert!(
            msg.contains("2 additional errors suppressed"),
            "expected suppression count in '{msg}'"
        );
    }
    #[test]
    fn max_unique_one_with_multiple_causes() {
        // with max_unique=1, a second distinct cause should still produce a multi-error summary
        let collector = ErrorCollector::new(1);
        collector.push(anyhow::anyhow!("error A"));
        collector.push(anyhow::anyhow!("error B"));
        let err = collector.into_error().unwrap();
        let msg = format!("{}", err);
        assert!(
            msg.contains("multiple errors occurred:"),
            "expected multi-error header in '{msg}'"
        );
        assert!(msg.contains("error A"), "expected tracked cause in '{msg}'");
        assert!(
            msg.contains("1 additional errors suppressed"),
            "expected suppression count in '{msg}'"
        );
    }

    #[test]
    fn context_wrapped_errors_dedup_by_root_cause() {
        let collector = ErrorCollector::default();
        let e1 = anyhow::anyhow!("Permission denied (os error 13)")
            .context("failed to create /dst/foo/a.txt");
        let e2 = anyhow::anyhow!("Permission denied (os error 13)")
            .context("failed to create /dst/foo/b.txt");
        collector.push(e1);
        collector.push(e2);
        // same root cause, different context -> single unique cause -> returns first error with chain
        let err = collector.into_error().unwrap();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("failed to create /dst/foo/a.txt"),
            "expected first error's context in '{msg}'"
        );
        assert!(
            msg.contains("Permission denied"),
            "expected root cause in '{msg}'"
        );
    }

    #[test]
    fn has_errors_is_threadsafe() {
        let collector = std::sync::Arc::new(ErrorCollector::default());
        let c = collector.clone();
        let handle = std::thread::spawn(move || {
            c.push(anyhow::anyhow!("error from thread"));
        });
        handle.join().unwrap();
        assert!(collector.has_errors());
    }

    #[test]
    fn take_error_idempotent() {
        let collector = ErrorCollector::default();
        collector.push(anyhow::anyhow!("Permission denied (os error 13)"));
        // first call takes the original error
        let err1 = collector.take_error();
        assert!(err1.is_some(), "first take_error should return Some");
        // second call should still return an error synthesized from the stored cause
        let err2 = collector.take_error();
        assert!(err2.is_some(), "second take_error should return Some");
        let msg = format!("{:#}", err2.unwrap());
        assert!(
            msg.contains("Permission denied"),
            "expected root cause in '{msg}'"
        );
    }
}
