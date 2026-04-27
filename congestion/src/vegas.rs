//! Vegas-style latency-based adaptive controller.
//!
//! Maintains a windowed p10-percentile of recent operation latencies as the
//! uncongested baseline and an EWMA-smoothed recent latency (`ewma_latency`)
//! as the current estimate. On every tick, the ratio
//! `ewma_latency / baseline` is compared to the configured thresholds:
//!
//! - `ratio < alpha` → the queue is shallow; grow concurrency.
//! - `ratio > beta`  → the queue is building; shrink concurrency.
//! - otherwise       → hold.
//!
//! ## Why p10 over a sliding window
//!
//! Real metadata syscalls on networked filesystems (Weka, Lustre, NFS) have
//! natural per-op latency variance much wider than 50% — even with a fixed
//! `cwnd` and a steady offered load, individual stat/open latencies routinely
//! span an order of magnitude. A strict running minimum would latch onto a
//! single fast outlier and treat ordinary variance as queueing, ratcheting
//! `cwnd` down indefinitely. The p10 of the recent sample window is robust
//! to a single fast outlier (the floor moves only when ten percent of recent
//! samples beat it), naturally weighted toward recent samples (older entries
//! age out of the window), and gracefully accommodates the natural latency
//! variance of real filesystems.
//!
//! This is simpler than classic TCP Vegas (which compares expected vs. actual
//! rates), but carries the same signal. It is a starting point for adaptive
//! filesystem control; more sophisticated schemes (periodic drains, BBR) can
//! be layered on top in their own `impl Controller`s.

use crate::controller::{Controller, ControllerSnapshot, Decision, Sample};

/// Maximum number of samples retained in the sliding window used to
/// compute the p10 baseline. Older samples are evicted FIFO once this
/// cap is reached, bounding memory under sustained high sample rates
/// while still leaving plenty of resolution for the percentile.
const SAMPLE_WINDOW_CAP: usize = 4096;

/// Tunable parameters for [`VegasController`].
///
/// All fields are public to make controller behavior fully observable in
/// tests; most users should start from [`VegasConfig::default`] and override
/// only the knobs they care about.
#[derive(Debug, Clone, Copy)]
pub struct VegasConfig {
    /// Concurrency the controller starts at, before any samples are seen.
    pub initial_cwnd: u32,
    /// Floor on concurrency. Must be >= 1 to make progress.
    pub min_cwnd: u32,
    /// Ceiling on concurrency. Caps adaptive growth.
    pub max_cwnd: u32,
    /// If `ewma_latency / baseline < alpha`, cwnd is increased.
    ///
    /// Defaulted at `1.3` to leave breathing room for the natural latency
    /// variance of real filesystems: ordinary per-op jitter can briefly
    /// push the EWMA above the p10 baseline without indicating queueing,
    /// and a tighter alpha would misread that variance as lack of
    /// headroom — `ratio < alpha` is rarely satisfied, so `cwnd` stalls
    /// at the floor even when the filesystem has plenty of capacity.
    pub alpha: f64,
    /// If `ewma_latency / baseline > beta`, cwnd is decreased.
    ///
    /// Defaulted at `2.5` for the same natural-variance reason as `alpha`:
    /// real-world metadata syscalls routinely show ratios in the 1.5–2.0×
    /// band even at modest concurrency without the filesystem actually
    /// being congested, and a tighter beta would misread that variance as
    /// queueing and ratchet `cwnd` toward the floor.
    pub beta: f64,
    /// EWMA smoothing factor in `[0.0, 1.0]`. Higher = more responsive.
    pub ewma_alpha: f64,
    /// How much to grow cwnd on each under-shoot tick.
    pub increase_step: u32,
    /// How much to shrink cwnd on each over-shoot tick.
    pub decrease_step: u32,
    /// Maximum age of samples retained in the baseline window. Each tick,
    /// samples older than this are evicted; if the eviction empties the
    /// window, the EWMA is reset alongside it so the next tick re-runs
    /// the cold-start path. Prevents stale samples from anchoring the
    /// baseline against a workload that has changed shape since they
    /// were collected.
    pub min_latency_max_age: std::time::Duration,
}

impl Default for VegasConfig {
    fn default() -> Self {
        Self {
            initial_cwnd: 1,
            min_cwnd: 1,
            max_cwnd: 4096,
            alpha: 1.3,
            beta: 2.5,
            ewma_alpha: 0.3,
            increase_step: 1,
            decrease_step: 1,
            min_latency_max_age: std::time::Duration::from_secs(10),
        }
    }
}

/// Adaptive controller driven by latency inflation relative to the
/// uncongested baseline.
///
/// The baseline is the p10-percentile of recent samples held in a
/// sliding window — robust to a single fast outlier, naturally weighted
/// toward recent samples (older entries age out of the window), and
/// gracefully accommodating the natural latency variance of real
/// filesystems.
pub struct VegasController {
    config: VegasConfig,
    cwnd: u32,
    /// Sliding window of recent samples, used to compute the p10
    /// baseline each tick. Capped at [`SAMPLE_WINDOW_CAP`] entries:
    /// when full, oldest is evicted FIFO on push. Each entry is
    /// `(latency_ns, observed_at)`; the timestamp drives age-out so a
    /// stale window can be discarded after `min_latency_max_age`.
    samples: std::collections::VecDeque<(u64, std::time::Instant)>,
    ewma_latency_ns: Option<f64>,
    /// p10 baseline (in ns) recomputed each tick from `samples`.
    /// `None` if the window is empty.
    baseline_latency_ns: Option<u64>,
    tick_sum_latency_ns: u128,
    tick_sample_count: u64,
    /// Cumulative number of samples consumed across the controller's
    /// lifetime. Surfaced via [`ControllerSnapshot::samples_seen`] for
    /// observability — distinct from `tick_sample_count`, which resets
    /// each tick.
    total_samples: u64,
}

impl VegasController {
    pub fn new(config: VegasConfig) -> Self {
        let cwnd = config
            .initial_cwnd
            .clamp(config.min_cwnd.max(1), config.max_cwnd.max(1));
        Self {
            config,
            cwnd,
            samples: std::collections::VecDeque::with_capacity(SAMPLE_WINDOW_CAP),
            ewma_latency_ns: None,
            baseline_latency_ns: None,
            tick_sum_latency_ns: 0,
            tick_sample_count: 0,
            total_samples: 0,
        }
    }
    /// Current concurrency target. Useful for tests and metrics.
    pub fn cwnd(&self) -> u32 {
        self.cwnd
    }
    /// p10-percentile baseline latency over the recent sample window,
    /// or `None` if the window is empty.
    pub fn min_latency(&self) -> Option<std::time::Duration> {
        self.baseline_latency_ns
            .map(std::time::Duration::from_nanos)
    }
    /// EWMA latency estimate over recent ticks, or `None` if no
    /// sample-bearing tick has fired yet.
    pub fn ewma_latency(&self) -> Option<std::time::Duration> {
        self.ewma_latency_ns
            .map(|ns| std::time::Duration::from_nanos(ns as u64))
    }
}

impl Controller for VegasController {
    fn on_sample(&mut self, sample: &Sample) {
        // u64 nanos fit any realistic latency; saturate defensively.
        // Clamp to >= 1 so a 0-duration sample (possible when `Instant::now()`
        // resolution coarsely groups back-to-back probes) never lands as
        // `baseline = 0` — that would make the ratio below divide by zero
        // and collapse cwnd to the floor.
        let latency_ns = u64::try_from(sample.latency().as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        // bound memory under sustained high sample rates. Pop *before*
        // push when at the cap: `VecDeque::push_back` reallocates if at
        // capacity, and `VecDeque` never shrinks its underlying buffer,
        // so a post-push pop would leave the allocation grown past the
        // cap even though `len` is brought back down immediately.
        if self.samples.len() >= SAMPLE_WINDOW_CAP {
            self.samples.pop_front();
        }
        self.samples.push_back((latency_ns, sample.completed_at));
        self.tick_sum_latency_ns = self
            .tick_sum_latency_ns
            .saturating_add(u128::from(latency_ns));
        self.tick_sample_count = self.tick_sample_count.saturating_add(1);
        self.total_samples = self.total_samples.saturating_add(1);
    }
    fn on_tick(&mut self, now: std::time::Instant) -> Decision {
        // discard samples older than the window's max age. The EWMA must
        // be reset alongside an empty window: an EWMA frozen during a
        // congested period carries forward a "current latency" that is
        // not comparable to the new baseline the next samples will draw,
        // and would otherwise produce a spurious growth burst on the
        // first tick after the window emptied under sustained congestion.
        //
        // We use `retain` rather than a `pop_front while front is old`
        // loop because samples arrive in mpsc-receive order, not sorted
        // by `completed_at`: under concurrent producers a sample with an
        // older completion time can land in the deque after a newer one,
        // so the front isn't guaranteed to be the oldest. `retain` is
        // O(N) per tick, but N <= SAMPLE_WINDOW_CAP, negligible at the
        // 50ms tick cadence. `checked_sub` because `now - max_age` can
        // underflow for very early `Instant`s in tests with mocked clocks.
        if let Some(cutoff) = now.checked_sub(self.config.min_latency_max_age) {
            self.samples
                .retain(|&(_, observed_at)| observed_at >= cutoff);
        }
        if self.samples.is_empty() {
            self.baseline_latency_ns = None;
            self.ewma_latency_ns = None;
        } else {
            // p10 baseline: collect, sort, pick the 10th-percentile entry.
            // the window is bounded at SAMPLE_WINDOW_CAP, so this is
            // O(N log N) on at most a few thousand u64s — negligible at
            // tick cadence (50ms by default).
            let mut latencies: Vec<u64> = self.samples.iter().map(|&(ns, _)| ns).collect();
            latencies.sort_unstable();
            let idx = ((latencies.len() as f64) * 0.1) as usize;
            self.baseline_latency_ns = Some(latencies[idx.min(latencies.len() - 1)]);
        }
        if self.tick_sample_count == 0 {
            return Decision::with_concurrency(self.cwnd);
        }
        let mean_ns = (self.tick_sum_latency_ns / u128::from(self.tick_sample_count)) as f64;
        // the very first sample-bearing tick establishes the baseline —
        // EWMA equals baseline by construction so ratio is always 1.0.
        // skipping the adjustment on that tick prevents an unconditional
        // cwnd bump from a cold start.
        let is_first_sample_tick = self.ewma_latency_ns.is_none();
        self.ewma_latency_ns = Some(match self.ewma_latency_ns {
            Some(prev) => self.config.ewma_alpha * mean_ns + (1.0 - self.config.ewma_alpha) * prev,
            None => mean_ns,
        });
        self.tick_sum_latency_ns = 0;
        self.tick_sample_count = 0;
        if !is_first_sample_tick
            && let (Some(ewma), Some(baseline)) = (self.ewma_latency_ns, self.baseline_latency_ns)
        {
            let ratio = ewma / (baseline as f64);
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
        "vegas"
    }
    fn snapshot(&self) -> ControllerSnapshot {
        ControllerSnapshot {
            cwnd: self.cwnd,
            min_latency: self.min_latency().unwrap_or_default(),
            ewma_latency: self.ewma_latency().unwrap_or_default(),
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
        let c = VegasController::new(VegasConfig {
            initial_cwnd: 1000,
            min_cwnd: 1,
            max_cwnd: 64,
            ..VegasConfig::default()
        });
        assert_eq!(c.cwnd(), 64);
        let c = VegasController::new(VegasConfig {
            initial_cwnd: 0,
            min_cwnd: 4,
            max_cwnd: 64,
            ..VegasConfig::default()
        });
        assert_eq!(c.cwnd(), 4);
    }

    #[test]
    fn without_samples_tick_holds_cwnd() {
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 10,
            ..VegasConfig::default()
        });
        let now = std::time::Instant::now();
        for _ in 0..5 {
            assert_eq!(c.on_tick(now).max_in_flight, Some(10));
        }
    }

    #[test]
    fn tracks_baseline_latency_across_samples() {
        // p10 over a small window: index = (len * 0.1) as usize. with 3
        // samples that is index 0, the smallest value after sorting.
        let mut c = VegasController::new(VegasConfig::default());
        let start = std::time::Instant::now();
        c.on_sample(&sample(start, std::time::Duration::from_millis(10)));
        c.on_sample(&sample(start, std::time::Duration::from_millis(2)));
        c.on_sample(&sample(start, std::time::Duration::from_millis(5)));
        // the baseline is computed inside on_tick; trigger one to populate.
        c.on_tick(start);
        assert_eq!(c.min_latency(), Some(std::time::Duration::from_millis(2)));
    }

    #[test]
    fn baseline_picks_p10_not_strict_min() {
        // pin down the percentile semantics: 90 fast samples + 10 slow
        // samples at 100× the fast latency. the strict min would be the
        // single fastest sample; the p10 admits the lower 10% of the
        // window, which here is uniformly the fast bucket — so the
        // baseline lands at 1ms regardless of the slow tail.
        let mut c = VegasController::new(VegasConfig::default());
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
            c.min_latency(),
            Some(std::time::Duration::from_millis(1)),
            "p10 of 90×1ms + 10×100ms must be 1ms",
        );
    }

    #[test]
    fn grows_cwnd_when_latency_matches_baseline() {
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 2,
            increase_step: 1,
            max_cwnd: 100,
            ..VegasConfig::default()
        });
        let start = std::time::Instant::now();
        // consistent baseline latency over many ticks should grow cwnd
        for _ in 0..5 {
            c.on_sample(&sample(start, std::time::Duration::from_millis(2)));
            c.on_tick(start);
        }
        assert!(c.cwnd() > 2, "expected growth, got {}", c.cwnd());
    }

    #[test]
    fn shrinks_cwnd_when_latency_exceeds_beta() {
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 50,
            decrease_step: 2,
            beta: 1.5,
            ..VegasConfig::default()
        });
        let start = std::time::Instant::now();
        // first sample establishes the baseline at 2ms
        c.on_sample(&sample(start, std::time::Duration::from_millis(2)));
        c.on_tick(start);
        // subsequent samples at 10ms are 5× baseline — well above beta
        for _ in 0..5 {
            c.on_sample(&sample(start, std::time::Duration::from_millis(10)));
            c.on_tick(start);
        }
        assert!(c.cwnd() < 50, "expected shrink, got {}", c.cwnd());
    }

    #[test]
    fn holds_cwnd_when_ratio_between_alpha_and_beta() {
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 10,
            alpha: 1.1,
            beta: 1.5,
            ewma_alpha: 1.0,
            ..VegasConfig::default()
        });
        let start = std::time::Instant::now();
        // the first tick always sees ratio=1.0 (baseline == mean) and grows,
        // so snapshot cwnd after baseline is established
        c.on_sample(&sample(start, std::time::Duration::from_millis(2)));
        c.on_tick(start);
        let cwnd_after_baseline = c.cwnd();
        // subsequent ticks at 1.3× baseline sit between alpha and beta, so hold
        for _ in 0..5 {
            c.on_sample(&sample(start, std::time::Duration::from_micros(2600)));
            c.on_tick(start);
        }
        assert_eq!(c.cwnd(), cwnd_after_baseline);
    }

    #[test]
    fn cwnd_respects_min_floor() {
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 3,
            min_cwnd: 2,
            decrease_step: 10,
            beta: 1.1,
            ..VegasConfig::default()
        });
        let start = std::time::Instant::now();
        c.on_sample(&sample(start, std::time::Duration::from_millis(2)));
        c.on_tick(start);
        c.on_sample(&sample(start, std::time::Duration::from_millis(20)));
        c.on_tick(start);
        assert_eq!(c.cwnd(), 2);
    }

    #[test]
    fn first_sample_tick_does_not_adjust_cwnd() {
        // with baseline == mean by construction, the naive ratio
        // calculation returns 1.0 on the first sample-bearing tick and
        // would always grow cwnd. The controller must skip the
        // adjustment on this tick to avoid a baseline-inflation bias.
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 5,
            ..VegasConfig::default()
        });
        let start = std::time::Instant::now();
        c.on_sample(&sample(start, std::time::Duration::from_millis(2)));
        assert_eq!(c.on_tick(start).max_in_flight, Some(5));
    }

    #[test]
    fn baseline_window_ages_out_and_is_re_established() {
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 10,
            min_latency_max_age: std::time::Duration::from_millis(100),
            ..VegasConfig::default()
        });
        let t0 = std::time::Instant::now();
        c.on_sample(&sample(t0, std::time::Duration::from_micros(500)));
        // a tick before the age expires preserves the window contents
        c.on_tick(t0 + std::time::Duration::from_millis(50));
        assert_eq!(c.samples.len(), 1);
        assert_eq!(c.min_latency(), Some(std::time::Duration::from_micros(500)));
        // after the max age, on_tick discards the stale window
        c.on_tick(t0 + std::time::Duration::from_millis(200));
        assert!(c.samples.is_empty(), "stale samples must be evicted");
        assert_eq!(c.min_latency(), None);
        // a fresh, much larger sample becomes the new baseline
        let t_new = t0 + std::time::Duration::from_millis(201);
        c.on_sample(&sample(t_new, std::time::Duration::from_millis(3)));
        c.on_tick(t_new);
        assert_eq!(c.min_latency(), Some(std::time::Duration::from_millis(3)));
    }

    #[test]
    fn baseline_age_out_under_persistent_congestion_does_not_trigger_growth() {
        // sustained congestion: window populated with samples whose mean
        // is well above the baseline. when the window ages out and the
        // EWMA is reset alongside, the very next sample-bearing tick is
        // a first-sample-tick — the controller must not interpret the
        // freshly-reset state as headroom and grow `cwnd`. the original
        // strict-min controller had the same invariant; the percentile
        // version preserves it via the empty-deque EWMA reset.
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 30,
            min_latency_max_age: std::time::Duration::from_millis(100),
            // no smoothing, so ewma = mean each tick — makes the scenario
            // crisp to reason about.
            ewma_alpha: 1.0,
            ..VegasConfig::default()
        });
        let t0 = std::time::Instant::now();
        // phase 1: establish a low baseline at 2ms
        c.on_sample(&sample(t0, std::time::Duration::from_millis(2)));
        c.on_tick(t0);
        // phase 2: sustained congestion at 8ms — 4× baseline, > beta=2.5
        let cwnd_before_congestion = c.cwnd();
        for i in 1..=5 {
            let now = t0 + std::time::Duration::from_millis(10 * i);
            c.on_sample(&sample(now, std::time::Duration::from_millis(8)));
            c.on_tick(now);
        }
        assert!(
            c.cwnd() < cwnd_before_congestion,
            "expected shrink under congestion, got {} (from {})",
            c.cwnd(),
            cwnd_before_congestion,
        );
        let cwnd_during_congestion = c.cwnd();
        // phase 3: a tick after the window's max-age elapses with no
        // new sample drives the age-out path: every entry in the deque
        // is older than the cutoff, the deque is emptied, and the EWMA
        // is reset. cwnd is unchanged at this tick because there are no
        // new samples to act on.
        let t_expired = t0 + std::time::Duration::from_millis(200);
        c.on_tick(t_expired);
        assert!(c.samples.is_empty(), "stale samples must be evicted");
        assert_eq!(
            c.ewma_latency(),
            None,
            "EWMA must reset when window empties"
        );
        assert_eq!(c.cwnd(), cwnd_during_congestion);
        // phase 4: a new sample arrives — congestion persists. because
        // the EWMA was reset, this is a first-sample-tick: ratio is 1.0
        // by construction and the controller must not adjust cwnd.
        let t_next = t_expired + std::time::Duration::from_millis(1);
        c.on_sample(&sample(t_next, std::time::Duration::from_millis(8)));
        c.on_tick(t_next);
        assert_eq!(
            c.cwnd(),
            cwnd_during_congestion,
            "cwnd must not grow on the first sample-bearing tick after window age-out",
        );
    }

    #[test]
    fn empty_ticks_between_samples_preserve_ewma() {
        // if a tick arrives with no new samples, the controller must hold
        // its current EWMA and cwnd rather than reset either — otherwise a
        // brief workload pause would discard hard-won state.
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 10,
            ewma_alpha: 1.0,
            ..VegasConfig::default()
        });
        let start = std::time::Instant::now();
        c.on_sample(&sample(start, std::time::Duration::from_millis(2)));
        c.on_tick(start);
        let baseline_ewma = c.ewma_latency();
        // several empty ticks within the age-out window
        for i in 0..5 {
            c.on_tick(start + std::time::Duration::from_millis(i * 10));
        }
        assert_eq!(c.ewma_latency(), baseline_ewma);
        assert_eq!(c.cwnd(), 10);
    }

    #[test]
    fn zero_latency_samples_do_not_collapse_cwnd() {
        // Regression: a 0-duration sample (possible when Instant::now()
        // resolution groups back-to-back probes) previously set
        // baseline=0, making the ewma/baseline ratio divide by zero or
        // turn into NaN — and either way not drive a principled cwnd
        // decision. `on_sample` now clamps latency to >= 1ns, so the
        // ratio stays finite and cwnd follows the normal trajectory.
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 10,
            min_cwnd: 1,
            max_cwnd: 100,
            ..VegasConfig::default()
        });
        let start = std::time::Instant::now();
        // feed many zero-duration samples across multiple ticks.
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
        // The exact trajectory depends on the clamped ratio behavior;
        // the invariant we care about is that cwnd stays within the
        // configured bounds (did not collapse to 0 or diverge).
        assert!(
            c.cwnd() >= 1 && c.cwnd() <= 100,
            "cwnd {} out of configured bounds under 0-latency samples",
            c.cwnd(),
        );
    }

    #[test]
    fn sample_window_is_capped_to_prevent_unbounded_growth() {
        // push far more samples than the cap; len must stay at the cap on
        // every observation, and the underlying allocation must not grow
        // past its post-construction capacity — `VecDeque::push_back`
        // reallocates if the buffer is at capacity, and `VecDeque` never
        // shrinks, so a post-push pop pattern would leak the allocation
        // even though `len` is brought back down immediately. Pinning to
        // the initial capacity (rather than a looser `<= 2× cap` bound)
        // catches that regression: at the cap a post-push push_back would
        // round up to the next power of two, doubling capacity.
        let mut c = VegasController::new(VegasConfig::default());
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
        // older completion time can land in the deque *after* a newer
        // one. The age-out path must still evict every stale entry —
        // not just a contiguous prefix at the front — and the empty-
        // deque-after-age-out path (EWMA reset, first-sample-tick
        // semantics) must still fire.
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 10,
            min_latency_max_age: std::time::Duration::from_millis(100),
            ewma_alpha: 1.0,
            ..VegasConfig::default()
        });
        let t0 = std::time::Instant::now();
        // Push samples in *interleaved* order: [newer, old, newer, old, ...].
        // The "newer" entries sit at the front of the deque and the "old"
        // entries sit immediately behind them. A `pop_front while front
        // is old` loop would inspect the front (newer, not stale at the
        // first tick), break, and leave every buried "old" entry behind.
        // `retain` correctly drops the old ones from arbitrary positions.
        let old_offset = std::time::Duration::from_millis(10);
        let newer_offset = std::time::Duration::from_millis(80);
        for i in 0..10 {
            let offset = if i % 2 == 0 { newer_offset } else { old_offset };
            c.on_sample(&sample(t0 + offset, std::time::Duration::from_millis(1)));
        }
        assert_eq!(c.samples.len(), 10);
        // First tick at t0 + 130ms, max_age = 100ms → cutoff = t0 + 30ms.
        //   - "newer" entries: observed_at ≈ t0 + 81ms (>= cutoff) → retain
        //   - "old"   entries: observed_at ≈ t0 + 11ms (<  cutoff) → evict
        // The buggy front-only loop sees a non-stale front and breaks
        // immediately, leaving all 10. `retain` evicts exactly the 5 old
        // ones.
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
        // EWMA was established by the first tick (5 retained samples
        // produced a non-empty window). Now tick well past every
        // observed_at — every remaining entry ages out, the deque
        // empties, and the EWMA must reset alongside it so the next
        // sample-bearing tick re-runs first-sample-tick semantics.
        assert!(c.ewma_latency().is_some());
        let t_all_expired = t0 + std::time::Duration::from_millis(300);
        c.on_tick(t_all_expired);
        assert!(c.samples.is_empty(), "every stale sample must age out");
        assert_eq!(
            c.ewma_latency(),
            None,
            "EWMA must reset when age-out empties the window",
        );
        assert_eq!(c.min_latency(), None);
    }

    #[test]
    fn cwnd_respects_max_ceiling() {
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 4,
            max_cwnd: 6,
            increase_step: 10,
            alpha: 1.5,
            ..VegasConfig::default()
        });
        let start = std::time::Instant::now();
        for _ in 0..10 {
            c.on_sample(&sample(start, std::time::Duration::from_millis(2)));
            c.on_tick(start);
        }
        assert_eq!(c.cwnd(), 6);
    }
}
