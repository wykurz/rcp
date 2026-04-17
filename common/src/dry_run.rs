//! Dry-run reporting helpers shared by copy, link, and rm operations.

use crate::config::DryRunMode;
use crate::filter::{FilterResult, TimeSkipReason};

/// Reports a dry-run action (would copy/link/remove) to stdout.
/// When `dst` is `None`, prints only the source path (used by rm).
pub fn report_action(
    verb: &str,
    src: &std::path::Path,
    dst: Option<&std::path::Path>,
    entry_type: &str,
) {
    match dst {
        Some(dst) => println!("would {} {} {:?} -> {:?}", verb, entry_type, src, dst),
        None => println!("would {} {} {:?}", verb, entry_type, src),
    }
}

/// Reports a skipped entry during dry-run to stdout.
/// Respects dry-run mode: Brief suppresses, All shows path, Explain shows reason.
pub fn report_skip(
    path: &std::path::Path,
    result: &FilterResult,
    mode: DryRunMode,
    entry_type: &str,
) {
    match mode {
        DryRunMode::Brief => { /* brief mode doesn't show skipped files */ }
        DryRunMode::All => {
            println!("skip {} {:?}", entry_type, path);
        }
        DryRunMode::Explain => match result {
            FilterResult::ExcludedByDefault => {
                println!(
                    "skip {} {:?} (no include pattern matched)",
                    entry_type, path
                );
            }
            FilterResult::ExcludedByPattern(pattern) => {
                println!("skip {} {:?} (excluded by '{}')", entry_type, path, pattern);
            }
            FilterResult::Included => { /* shouldn't happen */ }
        },
    }
}

/// Returns a human-readable skip reason for a FilterResult.
/// Returns None for Included (which is not a skip).
/// Used by the remote dry-run path to format structured log messages.
pub fn format_skip_reason(result: &FilterResult) -> Option<String> {
    match result {
        FilterResult::Included => None,
        FilterResult::ExcludedByDefault => Some("no include pattern matched".to_string()),
        FilterResult::ExcludedByPattern(p) => Some(format!("excluded by '{}'", p)),
    }
}

/// Reports an entry skipped by a time filter during dry-run to stdout.
/// Respects dry-run mode: Brief suppresses, All shows path, Explain shows reason.
pub fn report_time_skip(
    path: &std::path::Path,
    reason: TimeSkipReason,
    mode: DryRunMode,
    entry_type: &str,
) {
    match mode {
        DryRunMode::Brief => { /* brief mode doesn't show skipped entries */ }
        DryRunMode::All => {
            println!("skip {} {:?}", entry_type, path);
        }
        DryRunMode::Explain => {
            let reason_str = match reason {
                TimeSkipReason::TooNewModified => "mtime is too recent",
                TimeSkipReason::TooNewCreated => "btime is too recent",
                TimeSkipReason::TooNewBoth => "mtime and btime are too recent",
            };
            println!("skip {} {:?} ({})", entry_type, path, reason_str);
        }
    }
}
