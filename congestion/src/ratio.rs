//! Latency-based adaptive controller using two-window percentile statistics.
//!
//! The controller maintains a single sliding window of recent operation
//! latencies and, on every tick, derives two summary statistics from it:
//!
//! - **Baseline** — the `baseline_percentile` over the full long-horizon
//!   window (default 10s).
//! - **Current** — the `current_percentile` computed over a subset of the
//!   most recent samples (default 1s).
//!
//! Their ratio (`current / baseline`) drives the control law,
//! parameterized by two thresholds `alpha < beta`:
//!
//! - `ratio < alpha` → grow concurrency. The recent distribution sits
//!   below the alpha threshold relative to baseline; with `alpha > 1.0`
//!   that includes "recent slightly slower than baseline" (active mode
//!   probing for headroom), with `alpha < 1.0` it requires the recent
//!   distribution to be meaningfully *faster* than baseline (passive
//!   mode).
//! - `ratio > beta`  → shrink. Recent is meaningfully slower than
//!   baseline (or, in cross mode, the inter-quantile spread has widened
//!   past the beta threshold) — queue build-up signal.
//! - otherwise       → hold.
//!
//! ## Choosing the two percentiles
//!
//! Real metadata syscalls on networked filesystems (Weka, Lustre, NFS) have
//! per-op latency variance that routinely spans an order of magnitude even
//! at fixed `cwnd` and steady offered load.
//!
//! - **Matched percentiles** (`baseline_percentile == current_percentile`,
//!   e.g. both at p50): at steady state both windows estimate the same
//!   population statistic, so the ratio sits near 1.0 regardless of
//!   how heavy-tailed the distribution is. Distribution shape cancels;
//!   defaults travel cleanly across filesystems. The trade-off: matched
//!   percentiles cancel the *level* of queueing once both windows have
//!   equilibrated to it, so the controller can only see *changes* in load,
//!   not steady-state queueing.
//! - **Cross percentiles** (`baseline_percentile < current_percentile`,
//!   e.g. p40 baseline / p60 current): the ratio measures the inter-quantile
//!   spread of the recent distribution, which is a direct queueing signal —
//!   queueing fattens the upper tail asymmetrically, so the spread grows
//!   with offered load even at steady state. The trade-off: the steady-state
//!   ratio depends on the per-syscall distribution shape, so `alpha` and
//!   `beta` may need per-filesystem tuning.
//!
//! Defaults ship cross (baseline p10, current p50) with `alpha = 1.3` and
//! `beta = 1.8` bracketing the steady-state inter-quantile spread of
//! typical metadata syscalls. Matched mode is reachable by setting the
//! two percentiles to the same value (and tightening `alpha` / `beta`
//! around 1.0).
//!
//! ## Why a single window holding all samples
//!
//! Both statistics come from the same `VecDeque<(latency, observed_at)>`
//! buffer. The "short" window is computed as a filter over that buffer —
//! samples whose `observed_at` is within the last `short_window` of the
//! current tick. This keeps the on_sample hot path trivial (one push,
//! one capped pop) and makes age-out a single retain pass per tick.

use crate::controller::{Controller, ControllerSnapshot, Decision, Sample};

/// Maximum number of samples retained in the sliding window. Older samples
/// are evicted FIFO once this cap is reached, bounding memory under
/// sustained high sample rates while still leaving plenty of resolution
/// for the percentile computation.
const SAMPLE_WINDOW_CAP: usize = 4096;

/// Tunable parameters for [`RatioController`].
///
/// All fields are public to make controller behavior fully observable in
/// tests; most users should start from [`RatioConfig::default`] and override
/// only the knobs they care about.
#[derive(Debug, Clone, Copy)]
pub struct RatioConfig {
    /// Concurrency the controller starts at, before any samples are seen.
    pub initial_cwnd: u32,
    /// Floor on concurrency. Must be >= 1 to make progress.
    pub min_cwnd: u32,
    /// Ceiling on concurrency. Caps adaptive growth.
    pub max_cwnd: u32,
    /// If `current / baseline < alpha`, cwnd is increased.
    ///
    /// Interpretation depends on the percentile pair: with matched
    /// percentiles the steady-state ratio sits near 1.0, so `alpha < 1.0`
    /// means "grow only when recent is meaningfully faster than baseline"
    /// (a passive controller that doesn't push past the knee), while
    /// `alpha > 1.0` means "grow even at steady state" (an active explorer
    /// that climbs to the saturation point). With cross percentiles the
    /// steady-state ratio sits above 1.0, set by the inter-quantile
    /// spread of the latency distribution, and `alpha` is placed around
    /// that ratio.
    pub alpha: f64,
    /// If `current / baseline > beta`, cwnd is decreased.
    ///
    /// `beta` should be set wide enough to absorb ordinary variance and
    /// tight enough to react to genuine queueing. Default 1.8, sized
    /// to sit above the steady-state p10/p50 spread of typical
    /// metadata syscalls so only queueing-driven tail growth shrinks
    /// cwnd.
    pub beta: f64,
    /// How much to grow cwnd on each under-shoot tick.
    pub increase_step: u32,
    /// How much to shrink cwnd on each over-shoot tick.
    pub decrease_step: u32,
    /// Percentile (in `(0.0, 1.0)`) applied to the long-horizon window
    /// to derive the baseline statistic.
    ///
    /// Matched mode (`baseline_percentile == current_percentile`) cancels
    /// distribution shape so the steady-state ratio sits near 1.0 across
    /// any latency distribution. Cross mode
    /// (`baseline_percentile < current_percentile`) measures the
    /// inter-quantile spread, which grows under queueing — preserving a
    /// signal at steady-state heavy load that matched mode loses once
    /// both windows equilibrate.
    pub baseline_percentile: f64,
    /// Percentile (in `(0.0, 1.0)`) applied to the short-horizon window
    /// to derive the current statistic. Must be `>= baseline_percentile`
    /// so the steady-state ratio is `>= 1.0`.
    pub current_percentile: f64,
    /// Long-horizon window age. Samples older than this are evicted on
    /// every tick. Sets the memory of the baseline statistic — too short
    /// and the baseline drifts up under sustained load, losing the anchor;
    /// too long and the baseline is slow to forget transient slow phases.
    pub long_window: std::time::Duration,
    /// Short-horizon window age. The "current" statistic is the percentile
    /// of samples observed within this window. Must be strictly less than
    /// [`long_window`][Self::long_window] for the long-vs-short window
    /// comparison to be meaningful. Default 1s.
    pub short_window: std::time::Duration,
}

impl Default for RatioConfig {
    fn default() -> Self {
        Self {
            initial_cwnd: 1,
            min_cwnd: 1,
            max_cwnd: 4096,
            alpha: 1.3,
            beta: 1.8,
            increase_step: 1,
            decrease_step: 1,
            baseline_percentile: 0.1,
            current_percentile: 0.5,
            long_window: std::time::Duration::from_secs(10),
            short_window: std::time::Duration::from_secs(1),
        }
    }
}

/// Adaptive controller driven by a two-window latency-percentile ratio.
///
/// The configured `baseline_percentile` is computed over the long-horizon
/// sample window and the configured `current_percentile` over a
/// short-horizon subset. The ratio (`current / baseline`) is the
/// congestion signal. With matched percentiles
/// (`baseline_percentile == current_percentile`) both windows estimate
/// the same population statistic, so the ratio sits near 1.0 at steady
/// state and a recent distribution shift toward slower latencies pushes
/// it above 1.0. With cross percentiles
/// (`baseline_percentile < current_percentile`) the ratio measures the
/// inter-quantile spread of the recent distribution, which grows with
/// queueing — a level signal rather than a change signal. See the
/// module-level docstring for the trade-offs between the two modes.
pub struct RatioController {
    config: RatioConfig,
    cwnd: u32,
    /// Sliding window of recent samples, used to derive both baseline and
    /// current percentiles each tick. Capped at [`SAMPLE_WINDOW_CAP`]
    /// entries: when full, oldest is evicted FIFO on push. Each entry is
    /// `(latency_ns, observed_at)`; the timestamp drives age-out so a
    /// stale window can be discarded after `long_window`.
    samples: std::collections::VecDeque<(u64, std::time::Instant)>,
    /// Baseline percentile (in ns) over the long-horizon window, recomputed
    /// each tick from `samples`. `None` when the window is empty.
    baseline_latency_ns: Option<u64>,
    /// Current percentile (in ns) over the short-horizon subset, recomputed
    /// each tick from `samples`. `None` when no samples fall inside the
    /// short window.
    current_latency_ns: Option<u64>,
    /// Cumulative number of samples consumed across the controller's
    /// lifetime. Surfaced via [`ControllerSnapshot::samples_seen`] for
    /// observability.
    total_samples: u64,
    /// `total_samples` as of the previous tick. Used to detect ticks
    /// that fire without any new sample arriving since the last
    /// decision — those must hold `cwnd` rather than re-applying the
    /// same ratio decision over and over. With a short-window of 1s
    /// and a tick cadence of 50ms, a single sample otherwise drives
    /// ~20 grow / shrink steps before it ages out.
    last_tick_total_samples: u64,
}

impl RatioController {
    pub fn new(config: RatioConfig) -> Self {
        let cwnd = config
            .initial_cwnd
            .clamp(config.min_cwnd.max(1), config.max_cwnd.max(1));
        Self {
            config,
            cwnd,
            samples: std::collections::VecDeque::with_capacity(SAMPLE_WINDOW_CAP),
            baseline_latency_ns: None,
            current_latency_ns: None,
            total_samples: 0,
            last_tick_total_samples: 0,
        }
    }
    /// Current concurrency target. Useful for tests and metrics.
    pub fn cwnd(&self) -> u32 {
        self.cwnd
    }
    /// Most recently computed long-horizon percentile, or `None` if the
    /// window is empty.
    pub fn baseline_latency(&self) -> Option<std::time::Duration> {
        self.baseline_latency_ns
            .map(std::time::Duration::from_nanos)
    }
    /// Most recently computed short-horizon percentile, or `None` if no
    /// samples fell within the short window on the last tick.
    pub fn current_latency(&self) -> Option<std::time::Duration> {
        self.current_latency_ns.map(std::time::Duration::from_nanos)
    }
}

/// Pick the entry at the given percentile from an unsorted slice.
///
/// Uses `select_nth_unstable` to partition the slice in O(n) average
/// time rather than the O(n log n) of a full sort — meaningful when
/// many controllers tick at sub-second cadence over thousands of
/// samples each. The slice is reordered in place but no total order is
/// imposed, which is fine: callers throw the slice away after the
/// percentile is read.
///
/// Returns the value at index `floor(len * percentile)`, clamped into
/// bounds. `percentile` must be in `[0.0, 1.0]`; values outside that
/// range produce the floor or ceiling element, which keeps the
/// function total under unexpected config.
fn percentile_via_select(samples: &mut [u64], percentile: f64) -> u64 {
    debug_assert!(!samples.is_empty());
    let p = percentile.clamp(0.0, 1.0);
    let idx = ((samples.len() as f64) * p) as usize;
    let idx = idx.min(samples.len() - 1);
    *samples.select_nth_unstable(idx).1
}

impl Controller for RatioController {
    fn on_sample(&mut self, sample: &Sample) {
        // u64 nanos fit any realistic latency; saturate defensively.
        // Clamp to >= 1 so a 0-duration sample (possible when `Instant::now()`
        // resolution coarsely groups back-to-back probes) never lands as
        // baseline = 0 — that would make the ratio below divide by zero
        // and collapse cwnd to the floor.
        let latency_ns = u64::try_from(sample.latency().as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        // Bound memory under sustained high sample rates. Pop *before*
        // push when at the cap: `VecDeque::push_back` reallocates if at
        // capacity, and `VecDeque` never shrinks its underlying buffer,
        // so a post-push pop would leave the allocation grown past the
        // cap even though `len` is brought back down immediately.
        if self.samples.len() >= SAMPLE_WINDOW_CAP {
            self.samples.pop_front();
        }
        self.samples.push_back((latency_ns, sample.completed_at));
        self.total_samples = self.total_samples.saturating_add(1);
    }
    fn on_tick(&mut self, now: std::time::Instant) -> Decision {
        // Discard samples older than the long-horizon window. Use `retain`
        // rather than `pop_front while old` because samples arrive in
        // mpsc-receive order, not sorted by `completed_at`: under
        // concurrent producers a sample with an older completion time can
        // land in the deque after a newer one, so the front isn't
        // guaranteed to be the oldest. `retain` is O(N) per tick, but
        // N <= SAMPLE_WINDOW_CAP, negligible at the 50ms tick cadence.
        // `checked_sub` because `now - long_window` can underflow for
        // very early `Instant`s in tests with mocked clocks.
        if let Some(cutoff) = now.checked_sub(self.config.long_window) {
            self.samples
                .retain(|&(_, observed_at)| observed_at >= cutoff);
        }
        if self.samples.is_empty() {
            self.baseline_latency_ns = None;
            self.current_latency_ns = None;
            return Decision::with_concurrency(self.cwnd);
        }
        // Baseline: `baseline_percentile` over the full long-horizon window.
        // Materialize a local copy because the deque must stay
        // time-ordered for age-out, but `select_nth_unstable` reorders
        // the slice in place.
        let mut all_lat: Vec<u64> = self.samples.iter().map(|&(ns, _)| ns).collect();
        let baseline = percentile_via_select(&mut all_lat, self.config.baseline_percentile);
        self.baseline_latency_ns = Some(baseline);
        // Current: `current_percentile` over the short-horizon subset.
        // `checked_sub` underflows when `short_window` exceeds the
        // duration since the `Instant` epoch (only seen in tests with
        // freshly minted clocks). Fall back to the oldest retained
        // sample's timestamp so every sample qualifies — deterministic
        // and dependent only on caller-visible state, unlike a fresh
        // `Instant::now()` which would mix wall-clock with the supplied
        // `now`.
        let short_cutoff = now
            .checked_sub(self.config.short_window)
            .unwrap_or_else(|| {
                self.samples
                    .front()
                    .map(|&(_, observed_at)| observed_at)
                    .unwrap_or(now)
            });
        let mut short_lat: Vec<u64> = self
            .samples
            .iter()
            .filter(|&&(_, observed_at)| observed_at >= short_cutoff)
            .map(|&(ns, _)| ns)
            .collect();
        if short_lat.is_empty() {
            // No fresh samples — hold cwnd. The baseline above is still
            // updated so a renderer can show the current long-horizon
            // estimate even during a brief activity gap.
            self.current_latency_ns = None;
            return Decision::with_concurrency(self.cwnd);
        }
        let current = percentile_via_select(&mut short_lat, self.config.current_percentile);
        self.current_latency_ns = Some(current);
        // Adjust `cwnd` only on ticks that consumed at least one fresh
        // sample. Without this guard, a single sample observed late in
        // the short window drives one decision per tick — ~20
        // grow / shrink steps over the 1s window at the default 50ms
        // tick cadence — even though the underlying signal hasn't
        // changed. Snapshots above are still updated every tick so the
        // progress bar reflects the latest baseline / current values.
        let saw_fresh_sample = self.total_samples > self.last_tick_total_samples;
        self.last_tick_total_samples = self.total_samples;
        if saw_fresh_sample {
            let ratio = (current as f64) / (baseline as f64);
            if ratio < self.config.alpha {
                self.cwnd = self
                    .cwnd
                    .saturating_add(self.config.increase_step)
                    .min(self.config.max_cwnd);
            } else if ratio > self.config.beta {
                self.cwnd = self
                    .cwnd
                    .saturating_sub(self.config.decrease_step)
                    .max(self.config.min_cwnd.max(1));
            }
        }
        Decision::with_concurrency(self.cwnd)
    }
    fn name(&self) -> &'static str {
        "ratio"
    }
    fn snapshot(&self) -> ControllerSnapshot {
        ControllerSnapshot {
            cwnd: self.cwnd,
            baseline_latency: self.baseline_latency().unwrap_or_default(),
            current_latency: self.current_latency().unwrap_or_default(),
            samples_seen: self.total_samples,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::Outcome;

    fn sample(start: std::time::Instant, latency: std::time::Duration) -> Sample {
        Sample {
            started_at: start,
            completed_at: start + latency,
            bytes: 0,
            outcome: Outcome::Ok,
        }
    }

    #[test]
    fn initial_cwnd_is_clamped_into_bounds() {
        let c = RatioController::new(RatioConfig {
            initial_cwnd: 1000,
            min_cwnd: 1,
            max_cwnd: 64,
            ..RatioConfig::default()
        });
        assert_eq!(c.cwnd(), 64);
        let c = RatioController::new(RatioConfig {
            initial_cwnd: 0,
            min_cwnd: 4,
            max_cwnd: 64,
            ..RatioConfig::default()
        });
        assert_eq!(c.cwnd(), 4);
    }

    #[test]
    fn without_samples_tick_holds_cwnd() {
        let mut c = RatioController::new(RatioConfig {
            initial_cwnd: 10,
            ..RatioConfig::default()
        });
        let now = std::time::Instant::now();
        for _ in 0..5 {
            assert_eq!(c.on_tick(now).max_in_flight, Some(10));
        }
    }

    #[test]
    fn baseline_picks_configured_percentile() {
        // 100 samples: 90 fast (1ms) + 10 slow (100ms). At p10 the
        // baseline is in the fast bucket; at p50 it's still fast (median
        // of 100 = 50th smallest, which is in the fast 90); at p95 it's
        // the slow bucket because the upper 10% is all slow.
        let mut c = RatioController::new(RatioConfig {
            baseline_percentile: 0.95,
            current_percentile: 0.95,
            ..RatioConfig::default()
        });
        let t0 = std::time::Instant::now();
        for i in 0..90 {
            c.on_sample(&sample(
                t0 + std::time::Duration::from_micros(i),
                std::time::Duration::from_millis(1),
            ));
        }
        for i in 0..10 {
            c.on_sample(&sample(
                t0 + std::time::Duration::from_micros(90 + i),
                std::time::Duration::from_millis(100),
            ));
        }
        c.on_tick(t0 + std::time::Duration::from_millis(200));
        // p95 of 90×1ms + 10×100ms lands in the slow bucket (idx 95).
        assert_eq!(
            c.baseline_latency(),
            Some(std::time::Duration::from_millis(100)),
            "p95 of 90×1ms + 10×100ms must be 100ms",
        );
    }

    #[test]
    fn default_baseline_is_robust_to_outliers() {
        // Default baseline percentile (p10) sits well inside the fast
        // bucket of 90 fast + 10 slow, so outliers don't pin it.
        let mut c = RatioController::new(RatioConfig::default());
        let t0 = std::time::Instant::now();
        for i in 0..90 {
            c.on_sample(&sample(
                t0 + std::time::Duration::from_micros(i),
                std::time::Duration::from_millis(1),
            ));
        }
        for i in 0..10 {
            c.on_sample(&sample(
                t0 + std::time::Duration::from_micros(90 + i),
                std::time::Duration::from_millis(100),
            ));
        }
        c.on_tick(t0 + std::time::Duration::from_millis(200));
        assert_eq!(
            c.baseline_latency(),
            Some(std::time::Duration::from_millis(1)),
            "default-percentile baseline of 90×1ms + 10×100ms must be 1ms",
        );
    }

    #[test]
    fn degenerate_distribution_at_steady_state_yields_unit_ratio_and_grows() {
        // Steady state: identical latency across every sample, so any
        // percentile pair (matched or cross) collapses to the same
        // value and the ratio is exactly 1.0 — well below the default
        // alpha, so cwnd grows. No false "queueing" signal from natural
        // variance.
        let mut c = RatioController::new(RatioConfig {
            initial_cwnd: 5,
            increase_step: 1,
            max_cwnd: 1000,
            short_window: std::time::Duration::from_millis(500),
            long_window: std::time::Duration::from_secs(5),
            ..RatioConfig::default()
        });
        let t0 = std::time::Instant::now();
        // Spread samples across the long window so both short and long
        // subsets are populated; latency identical means the percentile
        // pair (whichever it is) reads the same value in each.
        for i in 0..200 {
            let observed_at = t0 + std::time::Duration::from_millis(i * 20);
            c.on_sample(&sample(observed_at, std::time::Duration::from_millis(2)));
        }
        // Tick at the end of the spread; the last 500ms of samples land
        // inside the short window, the rest inside the long window.
        let cwnd_before = c.cwnd();
        c.on_tick(t0 + std::time::Duration::from_millis(199 * 20));
        assert!(
            c.cwnd() > cwnd_before,
            "ratio at steady state must drive growth, got {} → {}",
            cwnd_before,
            c.cwnd(),
        );
    }

    #[test]
    fn shrinks_cwnd_when_short_window_distribution_shifts_up() {
        // Long window contains a mix of historical fast (2ms) and recent
        // slow (10ms) samples; short window holds only the slow recent
        // samples. The current percentile in the short window is much
        // higher than the baseline percentile in the long window, so the
        // ratio crosses beta and cwnd shrinks.
        let mut c = RatioController::new(RatioConfig {
            initial_cwnd: 50,
            decrease_step: 2,
            beta: 1.5,
            short_window: std::time::Duration::from_millis(500),
            long_window: std::time::Duration::from_secs(10),
            ..RatioConfig::default()
        });
        let t0 = std::time::Instant::now();
        // Phase 1: fast historical samples spread over 5 seconds.
        for i in 0..500 {
            let observed_at = t0 + std::time::Duration::from_millis(i * 10);
            c.on_sample(&sample(observed_at, std::time::Duration::from_millis(2)));
        }
        // Phase 2: a burst of slow samples in the last 200ms.
        let burst_start = t0 + std::time::Duration::from_millis(5_000);
        for i in 0..100 {
            let observed_at = burst_start + std::time::Duration::from_millis(i * 2);
            c.on_sample(&sample(observed_at, std::time::Duration::from_millis(10)));
        }
        let cwnd_before = c.cwnd();
        c.on_tick(burst_start + std::time::Duration::from_millis(200));
        assert!(
            c.cwnd() < cwnd_before,
            "expected shrink under burst (current/baseline > beta), got {} → {}",
            cwnd_before,
            c.cwnd(),
        );
    }

    #[test]
    fn holds_cwnd_when_ratio_between_alpha_and_beta() {
        let mut c = RatioController::new(RatioConfig {
            initial_cwnd: 10,
            alpha: 1.1,
            beta: 1.5,
            short_window: std::time::Duration::from_millis(500),
            long_window: std::time::Duration::from_secs(5),
            ..RatioConfig::default()
        });
        let t0 = std::time::Instant::now();
        // 4500ms of historical samples at 2ms.
        for i in 0..450 {
            let observed_at = t0 + std::time::Duration::from_millis(i * 10);
            c.on_sample(&sample(observed_at, std::time::Duration::from_millis(2)));
        }
        // 500ms of samples at 2.6ms — ratio = 2.6 / 2.0 = 1.3, between alpha and beta.
        let burst_start = t0 + std::time::Duration::from_millis(4_500);
        for i in 0..50 {
            let observed_at = burst_start + std::time::Duration::from_millis(i * 10);
            c.on_sample(&sample(
                observed_at,
                std::time::Duration::from_micros(2_600),
            ));
        }
        let cwnd_before = c.cwnd();
        c.on_tick(burst_start + std::time::Duration::from_millis(500));
        assert_eq!(
            c.cwnd(),
            cwnd_before,
            "ratio in [alpha, beta] must hold cwnd",
        );
    }

    #[test]
    fn cwnd_respects_min_floor() {
        let mut c = RatioController::new(RatioConfig {
            initial_cwnd: 3,
            min_cwnd: 2,
            decrease_step: 10,
            beta: 1.1,
            short_window: std::time::Duration::from_millis(200),
            long_window: std::time::Duration::from_millis(2_000),
            ..RatioConfig::default()
        });
        let t0 = std::time::Instant::now();
        // Establish a fast baseline.
        for i in 0..100 {
            let observed_at = t0 + std::time::Duration::from_millis(i * 10);
            c.on_sample(&sample(observed_at, std::time::Duration::from_millis(2)));
        }
        c.on_tick(t0 + std::time::Duration::from_millis(1_000));
        // Then a burst of slow samples — ratio rockets, but cwnd cannot
        // drop below the floor.
        let burst_start = t0 + std::time::Duration::from_millis(1_900);
        for i in 0..20 {
            let observed_at = burst_start + std::time::Duration::from_millis(i * 5);
            c.on_sample(&sample(observed_at, std::time::Duration::from_millis(50)));
        }
        c.on_tick(burst_start + std::time::Duration::from_millis(200));
        assert_eq!(c.cwnd(), 2);
    }

    #[test]
    fn cwnd_respects_max_ceiling() {
        let mut c = RatioController::new(RatioConfig {
            initial_cwnd: 4,
            max_cwnd: 6,
            increase_step: 10,
            short_window: std::time::Duration::from_millis(200),
            long_window: std::time::Duration::from_secs(2),
            ..RatioConfig::default()
        });
        let t0 = std::time::Instant::now();
        for i in 0..200 {
            let observed_at = t0 + std::time::Duration::from_millis(i * 10);
            c.on_sample(&sample(observed_at, std::time::Duration::from_millis(2)));
        }
        c.on_tick(t0 + std::time::Duration::from_millis(2_000));
        assert_eq!(c.cwnd(), 6);
    }

    #[test]
    fn baseline_window_ages_out_and_is_re_established() {
        let mut c = RatioController::new(RatioConfig {
            initial_cwnd: 10,
            long_window: std::time::Duration::from_millis(100),
            short_window: std::time::Duration::from_millis(50),
            ..RatioConfig::default()
        });
        let t0 = std::time::Instant::now();
        c.on_sample(&sample(t0, std::time::Duration::from_micros(500)));
        // Tick before age-out preserves the window.
        c.on_tick(t0 + std::time::Duration::from_millis(50));
        assert_eq!(c.samples.len(), 1);
        assert_eq!(
            c.baseline_latency(),
            Some(std::time::Duration::from_micros(500)),
        );
        // Tick after the long_window has elapsed evicts the stale window.
        c.on_tick(t0 + std::time::Duration::from_millis(200));
        assert!(c.samples.is_empty(), "stale samples must be evicted");
        assert_eq!(c.baseline_latency(), None);
        assert_eq!(c.current_latency(), None);
        // A fresh sample re-establishes the baseline.
        let t_new = t0 + std::time::Duration::from_millis(201);
        c.on_sample(&sample(t_new, std::time::Duration::from_millis(3)));
        c.on_tick(t_new);
        assert_eq!(
            c.baseline_latency(),
            Some(std::time::Duration::from_millis(3)),
        );
    }

    #[test]
    fn empty_short_window_holds_cwnd_without_resetting_baseline() {
        // If a tick arrives with no recent samples — but older samples are
        // still inside the long window — the baseline is still valid;
        // we just have nothing fresh to compare against. The controller
        // must hold cwnd rather than fabricating a comparison.
        let mut c = RatioController::new(RatioConfig {
            initial_cwnd: 10,
            long_window: std::time::Duration::from_secs(10),
            short_window: std::time::Duration::from_millis(500),
            ..RatioConfig::default()
        });
        let t0 = std::time::Instant::now();
        c.on_sample(&sample(t0, std::time::Duration::from_millis(2)));
        // Tick well past the short window but inside the long window.
        c.on_tick(t0 + std::time::Duration::from_secs(2));
        assert_eq!(c.cwnd(), 10, "cwnd must hold when short window is empty");
        assert_eq!(
            c.baseline_latency(),
            Some(std::time::Duration::from_millis(2)),
            "baseline still derived from long-horizon samples",
        );
        assert_eq!(c.current_latency(), None);
    }

    #[test]
    fn zero_latency_samples_do_not_collapse_cwnd() {
        // Regression: a 0-duration sample (possible when Instant::now()
        // resolution groups back-to-back probes) previously set
        // baseline=0, making the ratio divide by zero or turn into NaN
        // — and either way not drive a principled cwnd decision.
        // `on_sample` clamps latency to >= 1ns, so the ratio stays
        // finite and cwnd follows the normal trajectory.
        let mut c = RatioController::new(RatioConfig {
            initial_cwnd: 10,
            min_cwnd: 1,
            max_cwnd: 100,
            ..RatioConfig::default()
        });
        let start = std::time::Instant::now();
        for i in 0..10 {
            let zero_sample = Sample {
                started_at: start,
                completed_at: start,
                bytes: 0,
                outcome: Outcome::Ok,
            };
            c.on_sample(&zero_sample);
            c.on_tick(start + std::time::Duration::from_millis(i * 10));
        }
        assert!(
            c.cwnd() >= 1 && c.cwnd() <= 100,
            "cwnd {} out of configured bounds under 0-latency samples",
            c.cwnd(),
        );
    }

    #[test]
    fn sample_window_is_capped_to_prevent_unbounded_growth() {
        // Push far more samples than the cap; len must stay at the cap on
        // every observation, and the underlying allocation must not grow
        // past its post-construction capacity. Pinning to the initial
        // capacity (rather than a looser `<= 2× cap` bound) catches the
        // post-push pop regression: at the cap a post-push push_back
        // would round up to the next power of two, doubling capacity.
        let mut c = RatioController::new(RatioConfig::default());
        let initial_capacity = c.samples.capacity();
        let start = std::time::Instant::now();
        let n = SAMPLE_WINDOW_CAP + 10_000;
        for i in 0..n {
            c.on_sample(&sample(
                start + std::time::Duration::from_micros(i as u64),
                std::time::Duration::from_millis(1),
            ));
            assert!(
                c.samples.len() <= SAMPLE_WINDOW_CAP,
                "len {} exceeded cap {} at iteration {}",
                c.samples.len(),
                SAMPLE_WINDOW_CAP,
                i,
            );
            assert_eq!(
                c.samples.capacity(),
                initial_capacity,
                "underlying deque capacity grew at iteration {i}",
            );
        }
        assert_eq!(c.samples.len(), SAMPLE_WINDOW_CAP);
    }

    #[test]
    fn age_out_evicts_old_samples_regardless_of_deque_order() {
        // Samples arrive in mpsc-receive order, not sorted by
        // `completed_at`: under concurrent producers a sample with an
        // older completion time can land in the deque after a newer one.
        // The age-out path must still evict every stale entry — not just
        // a contiguous prefix at the front.
        let mut c = RatioController::new(RatioConfig {
            initial_cwnd: 10,
            long_window: std::time::Duration::from_millis(100),
            short_window: std::time::Duration::from_millis(50),
            ..RatioConfig::default()
        });
        let t0 = std::time::Instant::now();
        let old_offset = std::time::Duration::from_millis(10);
        let newer_offset = std::time::Duration::from_millis(80);
        for i in 0..10 {
            let offset = if i % 2 == 0 { newer_offset } else { old_offset };
            c.on_sample(&sample(t0 + offset, std::time::Duration::from_millis(1)));
        }
        assert_eq!(c.samples.len(), 10);
        // Tick at t0 + 130ms with long_window = 100ms → cutoff = t0 + 30ms.
        // 5 newer entries (~t0+81ms) are retained; 5 old entries (~t0+11ms)
        // are evicted.
        let t_first = t0 + std::time::Duration::from_millis(130);
        c.on_tick(t_first);
        assert_eq!(
            c.samples.len(),
            5,
            "out-of-order age-out must evict every stale entry, not just a front prefix",
        );
        let cutoff = t0 + std::time::Duration::from_millis(30);
        for &(_, observed_at) in &c.samples {
            assert!(observed_at >= cutoff);
        }
        // Ticking past every retained sample empties the deque entirely;
        // both summary statistics reset to None.
        let t_all_expired = t0 + std::time::Duration::from_millis(300);
        c.on_tick(t_all_expired);
        assert!(c.samples.is_empty(), "every stale sample must age out");
        assert_eq!(c.baseline_latency(), None);
        assert_eq!(c.current_latency(), None);
    }

    #[test]
    fn cwnd_does_not_drift_on_ticks_without_fresh_samples() {
        // Regression: with short_window = 1s and tick = 50ms, a single
        // sample is visible to ~20 consecutive ticks. Each tick was
        // re-applying the same ratio decision and adjusting cwnd, so
        // one sample in the right phase drove cwnd by ~20 steps even
        // though no new operation completed. Only ticks that consumed
        // a new sample may adjust cwnd.
        let mut c = RatioController::new(RatioConfig {
            initial_cwnd: 10,
            increase_step: 1,
            short_window: std::time::Duration::from_secs(1),
            long_window: std::time::Duration::from_secs(10),
            ..RatioConfig::default()
        });
        let t0 = std::time::Instant::now();
        // Single sample. Both baseline and current percentiles agree
        // on this value, so ratio = 1.0 — under alpha, normally a
        // grow signal.
        c.on_sample(&sample(t0, std::time::Duration::from_millis(2)));
        // First sample-bearing tick may grow once.
        c.on_tick(t0 + std::time::Duration::from_millis(50));
        let cwnd_after_first_tick = c.cwnd();
        // Subsequent ticks within the short window arrive without any
        // new sample. Each one would previously have re-grown cwnd by
        // increase_step.
        for i in 2..=20 {
            c.on_tick(t0 + std::time::Duration::from_millis(50 * i));
        }
        assert_eq!(
            c.cwnd(),
            cwnd_after_first_tick,
            "cwnd must not drift on ticks that consumed no new samples",
        );
    }

    #[test]
    fn snapshot_published_on_every_tick_with_samples() {
        // Empty short window holds cwnd but baseline is still reported
        // so the progress bar can show the live long-horizon estimate.
        // Once the short-window subset is non-empty, the snapshot must
        // include the current percentile too — even if no fresh sample
        // arrived since the last tick (in which case cwnd holds, but
        // the current value is still observable in the snapshot).
        let mut c = RatioController::new(RatioConfig {
            initial_cwnd: 5,
            short_window: std::time::Duration::from_secs(1),
            long_window: std::time::Duration::from_secs(10),
            ..RatioConfig::default()
        });
        let t0 = std::time::Instant::now();
        c.on_sample(&sample(t0, std::time::Duration::from_millis(3)));
        // First tick consumes the fresh sample.
        c.on_tick(t0 + std::time::Duration::from_millis(50));
        assert!(c.baseline_latency().is_some());
        assert!(c.current_latency().is_some());
        // A later tick (still within the short window) sees no new
        // sample, but the snapshot fields stay populated.
        c.on_tick(t0 + std::time::Duration::from_millis(500));
        assert!(c.baseline_latency().is_some());
        assert!(c.current_latency().is_some());
    }

    #[test]
    fn snapshot_reports_total_samples_seen() {
        let mut c = RatioController::new(RatioConfig::default());
        assert_eq!(c.snapshot().samples_seen, 0);
        let start = std::time::Instant::now();
        for _ in 0..7 {
            c.on_sample(&sample(start, std::time::Duration::from_millis(2)));
        }
        assert_eq!(c.snapshot().samples_seen, 7);
    }
}
