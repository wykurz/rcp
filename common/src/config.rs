//! Configuration types for runtime and execution settings

use serde::{Deserialize, Serialize};

/// Dry-run mode for previewing operations without executing them
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
pub enum DryRunMode {
    /// show only what would be copied/linked/removed
    #[value(name = "brief")]
    Brief,
    /// also show skipped files
    #[value(name = "all")]
    All,
    /// show skipped files with the pattern that caused the skip
    #[value(name = "explain")]
    Explain,
}

/// Runtime configuration for tokio and thread pools
#[derive(Debug, Clone, Copy, Default)]
pub struct RuntimeConfig {
    /// Number of worker threads (0 = number of CPU cores)
    pub max_workers: usize,
    /// Number of blocking threads (0 = tokio default of 512)
    pub max_blocking_threads: usize,
}

/// Throttling configuration for resource control
#[derive(Debug, Clone, Copy, Default)]
pub struct ThrottleConfig {
    /// Maximum number of open files (None = 80% of system limit)
    pub max_open_files: Option<usize>,
    /// Operations per second throttle (0 = no throttle)
    pub ops_throttle: usize,
    /// I/O operations per second throttle (0 = no throttle)
    pub iops_throttle: usize,
    /// Chunk size for I/O operations (bytes)
    pub chunk_size: u64,
}

impl ThrottleConfig {
    /// Validate configuration and return errors if invalid
    pub fn validate(&self) -> Result<(), String> {
        if self.iops_throttle > 0 && self.chunk_size == 0 {
            return Err("chunk_size must be specified when using iops_throttle".to_string());
        }
        Ok(())
    }
}

/// Output and logging configuration
#[derive(Debug, Clone, Copy, Default)]
pub struct OutputConfig {
    /// Suppress error output
    pub quiet: bool,
    /// Verbosity level: 0=ERROR, 1=INFO, 2=DEBUG, 3=TRACE
    pub verbose: u8,
    /// Print summary statistics at the end
    pub print_summary: bool,
}

/// Warnings and adjustments for dry-run mode.
///
/// When dry-run is active, progress is suppressed (it interferes with stdout
/// output) and `--summary` is suppressed unless `-v` is also active (verbose
/// independently enables summary in `common::run()`). This struct collects
/// warnings about the suppressed flags to print after the operation completes.
pub struct DryRunWarnings {
    warnings: Vec<String>,
}
impl DryRunWarnings {
    /// Build dry-run warnings based on which flags were specified.
    ///
    /// `has_progress` — whether any progress flags were specified.
    /// `has_summary` — whether --summary was specified.
    /// `verbose` — verbosity level; when > 0 summary is printed by `common::run()`
    ///   regardless of `print_summary`, so we skip the "ignored" warning.
    /// `has_overwrite` — whether --overwrite was specified (not applicable to rrm).
    /// `has_filters` — whether --include/--exclude/--filter-file was specified.
    /// `has_destination` — true for rcp/rlink (copy/link to destination), false for rrm.
    #[must_use]
    pub fn new(
        has_progress: bool,
        has_summary: bool,
        verbose: u8,
        has_overwrite: bool,
        has_filters: bool,
        has_destination: bool,
    ) -> Self {
        let mut warnings = Vec::new();
        if has_progress {
            warnings.push("dry-run: --progress was ignored".to_string());
        }
        if has_summary && verbose == 0 {
            warnings.push("dry-run: --summary was ignored".to_string());
        }
        if has_overwrite {
            warnings.push(
                "dry-run: --overwrite was ignored; dry-run does not check destination state"
                    .to_string(),
            );
        }
        if !has_filters {
            if has_destination {
                warnings.push(
                    "dry-run: no filtering specified. dry-run is primarily useful to preview \
                     --include/--exclude/--filter-file filtering; it does not check whether \
                     files already exist at the destination."
                        .to_string(),
                );
            } else {
                warnings.push(
                    "dry-run: no filtering specified. dry-run is primarily useful to preview \
                     --include/--exclude/--filter-file filtering."
                        .to_string(),
                );
            }
        }
        Self { warnings }
    }
    /// Print all collected warnings to stderr.
    pub fn print(&self) {
        for warning in &self.warnings {
            eprintln!("{warning}");
        }
    }
}
/// Tracing configuration for debugging and profiling
#[derive(Debug)]
pub struct TracingConfig {
    /// Remote tracing layer for distributed tracing
    pub remote_layer: Option<crate::remote_tracing::RemoteTracingLayer>,
    /// Debug log file path
    pub debug_log_file: Option<String>,
    /// Chrome trace output prefix (produces JSON viewable in Perfetto UI)
    pub chrome_trace_prefix: Option<String>,
    /// Flamegraph output prefix (produces folded stacks for inferno)
    pub flamegraph_prefix: Option<String>,
    /// Identifier for trace filenames (e.g., "rcp-master", "rcpd-source", "rcpd-destination")
    pub trace_identifier: String,
    /// Log level for profiling layers (chrome trace, flamegraph)
    /// Defaults to "trace" when profiling is enabled
    pub profile_level: Option<String>,
    /// Enable tokio-console for live async debugging
    pub tokio_console: bool,
    /// Port for tokio-console server (default: 6669)
    pub tokio_console_port: Option<u16>,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            remote_layer: None,
            debug_log_file: None,
            chrome_trace_prefix: None,
            flamegraph_prefix: None,
            trace_identifier: "unknown".to_string(),
            profile_level: None,
            tokio_console: false,
            tokio_console_port: None,
        }
    }
}
