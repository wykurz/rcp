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

/// Tunables for the adaptive metadata-throttle control loop.
///
/// Populated from CLI flags when `--auto-meta-throttle` is set; otherwise
/// this field is `None` on [`ThrottleConfig`] and the control loop is not
/// spawned. Serializable so that `rcp` can propagate the settings to
/// remote `rcpd` processes over the control channel.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct AutoMetaThrottleConfig {
    pub initial_cwnd: u32,
    pub min_cwnd: u32,
    pub max_cwnd: u32,
    pub alpha: f64,
    pub beta: f64,
    pub increase_step: u32,
    pub decrease_step: u32,
    /// Percentile (in `[0.0, 1.0)`) applied to the long-horizon window
    /// to derive the baseline statistic. Must be `<= current_percentile`.
    pub baseline_percentile: f64,
    /// Percentile (in `[0.0, 1.0)`) applied to the short-horizon window
    /// to derive the current statistic. Must be `>= baseline_percentile`.
    pub current_percentile: f64,
    /// Long-horizon window age. Drives the baseline statistic.
    pub long_window: std::time::Duration,
    /// Short-horizon window age. Drives the current statistic.
    pub short_window: std::time::Duration,
    pub tick_interval: std::time::Duration,
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
    /// Adaptive metadata-ops throttle, if enabled via `--auto-meta-throttle`.
    pub auto_meta: Option<AutoMetaThrottleConfig>,
}

/// Minimum static `--ops-throttle` when `--auto-meta-throttle` is on.
///
/// Auto-meta forces the ops-throttle to a fixed 100ms replenish interval
/// so the adapter's `Decision::rate_per_sec` → tokens-per-interval
/// conversion is always correct. That means the per-interval token count
/// is `rate / 10` and rounds to zero for rates below 10 ops/sec — which
/// would silently pause the gate after the initial drain. Reject the
/// combination explicitly so the user hits a clear error instead of
/// mysterious quiescence.
pub const AUTO_META_MIN_OPS_THROTTLE: usize = 10;

impl ThrottleConfig {
    /// Validate configuration and return errors if invalid
    pub fn validate(&self) -> Result<(), String> {
        if self.iops_throttle > 0 && self.chunk_size == 0 {
            return Err("chunk_size must be specified when using iops_throttle".to_string());
        }
        if let Some(auto) = &self.auto_meta {
            if auto.max_cwnd == 0 {
                return Err("auto-meta-max-cwnd must be > 0".to_string());
            }
            if auto.min_cwnd == 0 {
                return Err("auto-meta-min-cwnd must be >= 1".to_string());
            }
            if auto.min_cwnd > auto.max_cwnd {
                return Err("auto-meta-min-cwnd must be <= auto-meta-max-cwnd".to_string());
            }
            if !(0.0..1.0).contains(&auto.baseline_percentile) {
                return Err("auto-meta-baseline-percentile must be in [0.0, 1.0)".to_string());
            }
            if !(0.0..1.0).contains(&auto.current_percentile) {
                return Err("auto-meta-current-percentile must be in [0.0, 1.0)".to_string());
            }
            if auto.baseline_percentile > auto.current_percentile {
                return Err(
                    "auto-meta-baseline-percentile must be <= auto-meta-current-percentile"
                        .to_string(),
                );
            }
            // alpha and beta gate the ratio = current / baseline:
            // ratio < alpha → grow, ratio > beta → shrink. The only
            // hard invariant is `0 < alpha < beta`. The "natural" placement
            // of alpha and beta relative to 1.0 depends on the percentile
            // pair: matched percentiles produce a steady-state ratio
            // near 1.0, while cross percentiles produce a steady-state
            // ratio above 1.0 set by the inter-quantile spread of the
            // latency distribution. Either case may want alpha below or
            // above 1.0 depending on whether the operator wants the
            // controller to actively probe past the knee or sit passively
            // until queueing crosses the beta threshold.
            if !auto.alpha.is_finite() || auto.alpha <= 0.0 {
                return Err("auto-meta-alpha must be a finite value > 0".to_string());
            }
            if !auto.beta.is_finite() || auto.beta <= 0.0 {
                return Err("auto-meta-beta must be a finite value > 0".to_string());
            }
            if auto.alpha >= auto.beta {
                return Err("auto-meta-alpha must be < auto-meta-beta".to_string());
            }
            if auto.tick_interval.is_zero() {
                return Err("auto-meta-tick-interval must be > 0".to_string());
            }
            if auto.long_window.is_zero() {
                return Err("auto-meta-long-window must be > 0".to_string());
            }
            if auto.short_window.is_zero() {
                return Err("auto-meta-short-window must be > 0".to_string());
            }
            if auto.short_window >= auto.long_window {
                return Err("auto-meta-short-window must be < auto-meta-long-window".to_string());
            }
            if self.ops_throttle > 0 && self.ops_throttle < AUTO_META_MIN_OPS_THROTTLE {
                return Err(format!(
                    "--auto-meta-throttle is incompatible with --ops-throttle={} \
                     (auto-meta uses a fixed 100ms replenish interval; rates below \
                     {} ops/sec round to zero tokens per interval and would pause \
                     the throttle after the initial token). Either raise ops-throttle \
                     to >= {} or drop --auto-meta-throttle to get the legacy adaptive \
                     interval.",
                    self.ops_throttle, AUTO_META_MIN_OPS_THROTTLE, AUTO_META_MIN_OPS_THROTTLE,
                ));
            }
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
    /// When true, `run()` will not print text runtime stats after the summary.
    /// Used when the summary itself includes runtime stats (e.g. JSON format).
    pub suppress_runtime_stats: bool,
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
    /// `has_ignore_existing` — whether --ignore-existing was specified (checks destination state).
    #[must_use]
    pub fn new(
        has_progress: bool,
        has_summary: bool,
        verbose: u8,
        has_overwrite: bool,
        has_filters: bool,
        has_destination: bool,
        has_ignore_existing: bool,
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
        if !has_filters && !has_ignore_existing {
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

#[cfg(test)]
mod auto_meta_validation_tests {
    use super::*;

    fn valid_auto_meta() -> AutoMetaThrottleConfig {
        AutoMetaThrottleConfig {
            initial_cwnd: 1,
            min_cwnd: 1,
            max_cwnd: 4096,
            alpha: 1.1,
            beta: 1.5,
            increase_step: 1,
            decrease_step: 1,
            baseline_percentile: 0.5,
            current_percentile: 0.5,
            long_window: std::time::Duration::from_secs(10),
            short_window: std::time::Duration::from_secs(1),
            tick_interval: std::time::Duration::from_millis(50),
        }
    }

    fn config_with(auto: AutoMetaThrottleConfig) -> ThrottleConfig {
        ThrottleConfig {
            max_open_files: None,
            ops_throttle: 0,
            iops_throttle: 0,
            chunk_size: 0,
            auto_meta: Some(auto),
        }
    }

    #[test]
    fn defaults_validate() {
        assert!(config_with(valid_auto_meta()).validate().is_ok());
    }

    #[test]
    fn min_cwnd_zero_is_rejected() {
        let mut auto = valid_auto_meta();
        auto.min_cwnd = 0;
        let err = config_with(auto).validate().unwrap_err();
        assert!(err.contains("min-cwnd"), "got: {err}");
    }

    #[test]
    fn alpha_at_or_below_zero_is_rejected() {
        let mut auto = valid_auto_meta();
        auto.alpha = 0.0;
        assert!(config_with(auto).validate().is_err());
        let mut auto = valid_auto_meta();
        auto.alpha = -0.5;
        assert!(config_with(auto).validate().is_err());
    }

    #[test]
    fn alpha_below_one_is_accepted() {
        // Passive-controller mode: alpha < 1.0 means "grow only when
        // recent is meaningfully faster than baseline" — the explicit
        // use case for relaxing the previous alpha > 1.0 constraint.
        let mut auto = valid_auto_meta();
        auto.alpha = 0.9;
        auto.beta = 1.1;
        assert!(config_with(auto).validate().is_ok());
    }

    #[test]
    fn beta_at_or_below_zero_is_rejected() {
        let mut auto = valid_auto_meta();
        auto.alpha = 0.5;
        auto.beta = 0.0;
        let err = config_with(auto).validate().unwrap_err();
        assert!(err.contains("beta"), "got: {err}");
    }

    #[test]
    fn cross_percentile_config_validates() {
        // Cross-percentile mode: baseline at p40, current at p60, with
        // alpha/beta straddling the steady-state ratio. The validator
        // accepts both percentiles in (0, 1) with baseline <= current.
        let mut auto = valid_auto_meta();
        auto.baseline_percentile = 0.4;
        auto.current_percentile = 0.6;
        assert!(config_with(auto).validate().is_ok());
    }

    #[test]
    fn baseline_percentile_above_current_is_rejected() {
        let mut auto = valid_auto_meta();
        auto.baseline_percentile = 0.6;
        auto.current_percentile = 0.4;
        let err = config_with(auto).validate().unwrap_err();
        assert!(
            err.contains("baseline-percentile") && err.contains("current-percentile"),
            "got: {err}",
        );
    }

    #[test]
    fn baseline_percentile_out_of_range_is_rejected() {
        let mut auto = valid_auto_meta();
        auto.baseline_percentile = 1.0;
        let err = config_with(auto).validate().unwrap_err();
        assert!(err.contains("baseline-percentile"), "got: {err}");
    }

    #[test]
    fn non_finite_alpha_or_beta_is_rejected() {
        // NaN comparisons return false in either direction, so a plain
        // `auto.alpha <= 0.0` check would silently pass NaN through and
        // the controller would freeze in the hold band forever. The
        // `is_finite()` guard catches that.
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut auto = valid_auto_meta();
            auto.alpha = bad;
            assert!(
                config_with(auto).validate().is_err(),
                "alpha={bad} must be rejected",
            );
            let mut auto = valid_auto_meta();
            auto.beta = bad;
            assert!(
                config_with(auto).validate().is_err(),
                "beta={bad} must be rejected",
            );
        }
    }

    #[test]
    fn current_percentile_out_of_range_is_rejected() {
        let mut auto = valid_auto_meta();
        auto.current_percentile = 1.0;
        let err = config_with(auto).validate().unwrap_err();
        assert!(err.contains("current-percentile"), "got: {err}");
    }

    #[test]
    fn ops_throttle_below_floor_is_rejected_under_auto_meta() {
        let mut config = config_with(valid_auto_meta());
        config.ops_throttle = 5;
        let err = config.validate().unwrap_err();
        assert!(
            err.contains("ops-throttle") && err.contains("auto-meta-throttle"),
            "got: {err}",
        );
    }

    #[test]
    fn ops_throttle_at_or_above_floor_is_accepted_under_auto_meta() {
        let mut config = config_with(valid_auto_meta());
        config.ops_throttle = AUTO_META_MIN_OPS_THROTTLE;
        assert!(config.validate().is_ok());
        config.ops_throttle = AUTO_META_MIN_OPS_THROTTLE + 100;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn ops_throttle_below_floor_is_fine_without_auto_meta() {
        // The floor only applies when auto-meta forces a fixed 100ms
        // cadence. Without auto-meta, the adaptive get_replenish_interval
        // picks an interval that works for any rate.
        let config = ThrottleConfig {
            max_open_files: None,
            ops_throttle: 5,
            iops_throttle: 0,
            chunk_size: 0,
            auto_meta: None,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn alpha_greater_than_beta_is_rejected() {
        let mut auto = valid_auto_meta();
        auto.alpha = 1.6;
        auto.beta = 1.5;
        let err = config_with(auto).validate().unwrap_err();
        assert!(err.contains("alpha") && err.contains("beta"), "got: {err}");
    }
}
