//! Common CLI arguments shared by every RCP binary.
//!
//! Each binary flattens [`CommonArgs`] into its own clap struct via
//! `#[command(flatten)]`. Tool-specific arguments live in the binary itself.
//!
//! `chunk_size` is intentionally NOT in this struct: rcp/rcpd parse it as
//! `bytesize::ByteSize` (to accept "16MiB" etc.), while rrm/rlink/rcmp/filegen
//! parse it as a bare `u64`. Each binary keeps its own field.
//!
//! `summary` is also kept per-binary: rcpd never prints a summary (it streams
//! results back to the master), so giving it a no-op `--summary` flag would
//! mislead users.

#[derive(Debug, Clone, clap::Args)]
pub struct CommonArgs {
    // Progress & output
    /// Show progress
    #[arg(long, help_heading = "Progress & output")]
    pub progress: bool,
    /// Set the type of progress display
    ///
    /// If specified, --progress flag is implied.
    #[arg(long, value_name = "TYPE", help_heading = "Progress & output")]
    pub progress_type: Option<crate::ProgressType>,
    /// Set delay between progress updates
    ///
    /// Default is 200ms for interactive mode (`ProgressBar`) and 10s for non-interactive
    /// mode (`TextUpdates`). If specified, --progress flag is implied. Accepts
    /// human-readable durations like "200ms", "10s", "5min".
    #[arg(long, value_name = "DELAY", help_heading = "Progress & output")]
    pub progress_delay: Option<String>,
    /// Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count, help_heading = "Progress & output")]
    pub verbose: u8,
    /// Quiet mode, don't report errors
    #[arg(short = 'q', long = "quiet", help_heading = "Progress & output")]
    pub quiet: bool,
    // Performance & throttling
    /// Maximum number of open files (0 = no limit, unspecified = 80% of system limit)
    #[arg(long, value_name = "N", help_heading = "Performance & throttling")]
    pub max_open_files: Option<usize>,
    /// Throttle the number of operations per second (0 = no throttle)
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Performance & throttling"
    )]
    pub ops_throttle: usize,
    /// Limit I/O operations per second (0 = no throttle)
    ///
    /// Requires --chunk-size to calculate I/O operations per file: ((`file_size` - 1) / `chunk_size`) + 1
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Performance & throttling"
    )]
    pub iops_throttle: usize,
    // Advanced settings
    /// Number of worker threads (0 = number of CPU cores)
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Advanced settings"
    )]
    pub max_workers: usize,
    /// Number of blocking worker threads (0 = Tokio default of 512)
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Advanced settings"
    )]
    pub max_blocking_threads: usize,
}

impl CommonArgs {
    /// Build a [`crate::OutputConfig`] from these args plus the binary's
    /// per-instance `print_summary` decision.
    #[must_use]
    pub fn output_config(&self, print_summary: bool) -> crate::OutputConfig {
        crate::OutputConfig {
            quiet: self.quiet,
            verbose: self.verbose,
            print_summary,
            ..Default::default()
        }
    }
    /// Build a [`crate::RuntimeConfig`] from these args.
    #[must_use]
    pub fn runtime_config(&self) -> crate::RuntimeConfig {
        crate::RuntimeConfig {
            max_workers: self.max_workers,
            max_blocking_threads: self.max_blocking_threads,
        }
    }
    /// Build a [`crate::ThrottleConfig`] from these args. `chunk_size` is
    /// supplied by the caller because each binary parses it as a different
    /// type at the CLI layer.
    #[must_use]
    pub fn throttle_config(&self, chunk_size: u64) -> crate::ThrottleConfig {
        crate::ThrottleConfig {
            max_open_files: self.max_open_files,
            ops_throttle: self.ops_throttle,
            iops_throttle: self.iops_throttle,
            chunk_size,
        }
    }
    /// Returns true if any progress-related flag was set.
    #[must_use]
    pub fn progress_requested(&self) -> bool {
        self.progress || self.progress_type.is_some() || self.progress_delay.is_some()
    }
    /// Build user-facing [`crate::ProgressSettings`] when any progress flag was
    /// set, else `None`. For `rcp`'s remote-master and `rcpd`'s remote progress
    /// modes, build `ProgressSettings` directly instead of using this helper.
    #[must_use]
    pub fn user_progress_settings(&self) -> Option<crate::ProgressSettings> {
        if !self.progress_requested() {
            return None;
        }
        Some(crate::ProgressSettings {
            progress_type: crate::GeneralProgressType::User(self.progress_type.unwrap_or_default()),
            progress_delay: self.progress_delay.clone(),
        })
    }
}
