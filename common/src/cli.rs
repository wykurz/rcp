//! Common CLI arguments shared by every RCP binary.
//!
//! Each binary flattens [`CommonArgs`] into its own clap struct via
//! `#[command(flatten)]`. Tool-specific arguments live in the binary itself.
//!
//! Fields intentionally NOT in this struct, so each binary can document them
//! accurately:
//! - `chunk_size` — rcp/rcpd parse as `bytesize::ByteSize` (e.g. "16MiB"),
//!   others as bare `u64`.
//! - `summary` — rcpd streams results to the master and never prints a summary.
//! - `max_open_files` — filegen falls back to physical CPU cores instead of
//!   80% of the system rlimit, because random-data generation is CPU-bound.
//! - `quiet` — rcmp's `--quiet` also suppresses stdout differences (not just
//!   error output), so its help text differs from the other tools.

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
    // Performance & throttling
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
    // Congestion control (experimental, opt-in)
    /// Enable adaptive metadata-ops throttling (Vegas-style latency controller)
    #[arg(long, help_heading = "Congestion control")]
    pub auto_meta_throttle: bool,
    /// Initial concurrency window for adaptive metadata throttle
    #[arg(
        long,
        default_value = "1",
        value_name = "N",
        help_heading = "Congestion control"
    )]
    pub auto_meta_initial_cwnd: u32,
    /// Minimum concurrency window (floor below which cwnd cannot shrink)
    #[arg(
        long,
        default_value = "1",
        value_name = "N",
        help_heading = "Congestion control (advanced)"
    )]
    pub auto_meta_min_cwnd: u32,
    /// Maximum concurrency window (ceiling on adaptive growth)
    #[arg(
        long,
        default_value = "4096",
        value_name = "N",
        help_heading = "Congestion control"
    )]
    pub auto_meta_max_cwnd: u32,
    /// Latency ratio below which cwnd grows (current / baseline).
    /// Default 1.1: with matched-percentile signal, ratio sits near 1.0
    /// at steady state, so a tight alpha is the right scale.
    #[arg(
        long,
        default_value = "1.1",
        value_name = "F",
        help_heading = "Congestion control (advanced)"
    )]
    pub auto_meta_alpha: f64,
    /// Latency ratio above which cwnd shrinks. Default 1.5.
    #[arg(
        long,
        default_value = "1.5",
        value_name = "F",
        help_heading = "Congestion control (advanced)"
    )]
    pub auto_meta_beta: f64,
    /// Percentile (in `(0.0, 1.0)`) used to summarize each sample window.
    /// The same percentile is used for both long-horizon (baseline) and
    /// short-horizon (current) statistics — see `--auto-meta-long-window`
    /// and `--auto-meta-short-window`.
    #[arg(
        long,
        default_value = "0.5",
        value_name = "F",
        help_heading = "Congestion control (advanced)"
    )]
    pub auto_meta_percentile: f64,
    /// How much to grow cwnd on each under-shoot tick
    #[arg(
        long,
        default_value = "1",
        value_name = "N",
        help_heading = "Congestion control (advanced)"
    )]
    pub auto_meta_increase_step: u32,
    /// How much to shrink cwnd on each over-shoot tick
    #[arg(
        long,
        default_value = "1",
        value_name = "N",
        help_heading = "Congestion control (advanced)"
    )]
    pub auto_meta_decrease_step: u32,
    /// Long-horizon sample window (e.g. "10s"). Drives the baseline
    /// percentile; samples older than this are evicted on every tick.
    #[arg(
        long,
        default_value = "10s",
        value_name = "DUR",
        help_heading = "Congestion control (advanced)"
    )]
    pub auto_meta_long_window: humantime::Duration,
    /// Short-horizon sample window (e.g. "1s"). Drives the current-state
    /// percentile; must be strictly less than `--auto-meta-long-window`.
    #[arg(
        long,
        default_value = "1s",
        value_name = "DUR",
        help_heading = "Congestion control (advanced)"
    )]
    pub auto_meta_short_window: humantime::Duration,
    /// Control-loop tick interval (e.g. "50ms")
    #[arg(
        long,
        default_value = "50ms",
        value_name = "DUR",
        help_heading = "Congestion control (advanced)"
    )]
    pub auto_meta_tick_interval: humantime::Duration,
}

impl CommonArgs {
    /// Build a [`crate::OutputConfig`]. `quiet` and `print_summary` are
    /// supplied by the caller (each binary owns its own `--quiet` and
    /// `--summary` flags so it can document binary-specific semantics).
    #[must_use]
    pub fn output_config(&self, quiet: bool, print_summary: bool) -> crate::OutputConfig {
        crate::OutputConfig {
            quiet,
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
    /// Build a [`crate::ThrottleConfig`]. `max_open_files` and `chunk_size`
    /// are supplied by the caller (filegen has its own `--max-open-files`
    /// default; chunk_size has different parser types per binary).
    #[must_use]
    pub fn throttle_config(
        &self,
        max_open_files: Option<usize>,
        chunk_size: u64,
    ) -> crate::ThrottleConfig {
        let auto_meta = self
            .auto_meta_throttle
            .then(|| crate::AutoMetaThrottleConfig {
                initial_cwnd: self.auto_meta_initial_cwnd,
                min_cwnd: self.auto_meta_min_cwnd,
                max_cwnd: self.auto_meta_max_cwnd,
                alpha: self.auto_meta_alpha,
                beta: self.auto_meta_beta,
                increase_step: self.auto_meta_increase_step,
                decrease_step: self.auto_meta_decrease_step,
                percentile: self.auto_meta_percentile,
                long_window: self.auto_meta_long_window.into(),
                short_window: self.auto_meta_short_window.into(),
                tick_interval: self.auto_meta_tick_interval.into(),
            });
        crate::ThrottleConfig {
            max_open_files,
            ops_throttle: self.ops_throttle,
            iops_throttle: self.iops_throttle,
            chunk_size,
            auto_meta,
        }
    }
    /// Returns true if any progress-related flag was set.
    #[must_use]
    pub fn progress_requested(&self) -> bool {
        self.progress || self.progress_type.is_some() || self.progress_delay.is_some()
    }
    /// Build user-facing [`crate::ProgressSettings`] when any progress flag was
    /// set, else `None`. `kind` selects the tool-specific printer. For `rcp`'s
    /// remote-master and `rcpd`'s remote progress modes, build `ProgressSettings`
    /// directly instead of using this helper.
    #[must_use]
    pub fn user_progress_settings(
        &self,
        kind: crate::progress::LocalProgressKind,
    ) -> Option<crate::ProgressSettings> {
        if !self.progress_requested() {
            return None;
        }
        Some(crate::ProgressSettings {
            progress_type: crate::GeneralProgressType::User {
                progress_type: self.progress_type.unwrap_or_default(),
                kind,
            },
            progress_delay: self.progress_delay.clone(),
        })
    }
}
