//! Vegas-style latency-based adaptive controller.
//!
//! Maintains a running minimum latency (`min_latency`) as the uncongested
//! baseline and an EWMA-smoothed recent latency (`ewma_latency`) as the
//! current estimate. On every tick, the ratio `ewma_latency / min_latency` is
//! compared to the configured thresholds:
//!
//! - `ratio < alpha` → the queue is shallow; grow concurrency.
//! - `ratio > beta`  → the queue is building; shrink concurrency.
//! - otherwise       → hold.
//!
//! This is simpler than classic TCP Vegas (which compares expected vs. actual
//! rates), but carries the same signal. It is a starting point for adaptive
//! filesystem control; more sophisticated schemes (periodic drains, BBR) can
//! be layered on top in their own `impl Controller`s.

use crate::controller::{Controller, ControllerSnapshot, Decision, Sample};

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
    /// If `ewma_latency / min_latency < alpha`, cwnd is increased.
    pub alpha: f64,
    /// If `ewma_latency / min_latency > beta`, cwnd is decreased.
    pub beta: f64,
    /// EWMA smoothing factor in `[0.0, 1.0]`. Higher = more responsive.
    pub ewma_alpha: f64,
    /// How much to grow cwnd on each under-shoot tick.
    pub increase_step: u32,
    /// How much to shrink cwnd on each over-shoot tick.
    pub decrease_step: u32,
    /// Maximum age of the minimum-latency estimate before it is discarded
    /// and re-established from incoming samples. Prevents a single stale
    /// low-latency outlier from pinning the baseline and causing Vegas to
    /// progressively over-throttle.
    pub min_latency_max_age: std::time::Duration,
}

impl Default for VegasConfig {
    fn default() -> Self {
        Self {
            initial_cwnd: 1,
            min_cwnd: 1,
            max_cwnd: 4096,
            alpha: 1.1,
            beta: 1.5,
            ewma_alpha: 0.3,
            increase_step: 1,
            decrease_step: 1,
            min_latency_max_age: std::time::Duration::from_secs(10),
        }
    }
}

/// Adaptive controller driven by latency inflation relative to the
/// uncongested baseline.
pub struct VegasController {
    config: VegasConfig,
    cwnd: u32,
    /// The minimum latency observed, plus the `Instant` at which it was
    /// last lowered. The `Instant` lets us discard stale baselines so a
    /// single outlier sample cannot pin cwnd at the floor.
    min_latency: Option<(u64, std::time::Instant)>,
    ewma_latency_ns: Option<f64>,
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
            min_latency: None,
            ewma_latency_ns: None,
            tick_sum_latency_ns: 0,
            tick_sample_count: 0,
            total_samples: 0,
        }
    }
    /// Current concurrency target. Useful for tests and metrics.
    pub fn cwnd(&self) -> u32 {
        self.cwnd
    }
    /// Running minimum latency, or `None` if no samples have been observed
    /// or the previously-observed minimum has aged out.
    pub fn min_latency(&self) -> Option<std::time::Duration> {
        self.min_latency
            .map(|(ns, _)| std::time::Duration::from_nanos(ns))
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
        // `min_latency = 0` — that would make the ratio below divide by
        // zero and collapse cwnd to the floor.
        let latency_ns = u64::try_from(sample.latency().as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        // Refresh `observed_at` whenever a sample is at or below the
        // recorded minimum so repeated confirmation of a stable
        // baseline keeps it alive. If latency equals current, the min
        // value is unchanged but the age timer resets — otherwise a
        // workload that sits exactly at the true min would see the
        // baseline expire on `min_latency_max_age` even though it's
        // continuously confirming the same floor.
        self.min_latency = Some(match self.min_latency {
            Some((current, at)) if latency_ns > current => (current, at),
            _ => (latency_ns, sample.completed_at),
        });
        self.tick_sum_latency_ns = self
            .tick_sum_latency_ns
            .saturating_add(u128::from(latency_ns));
        self.tick_sample_count = self.tick_sample_count.saturating_add(1);
        self.total_samples = self.total_samples.saturating_add(1);
    }
    fn on_tick(&mut self, now: std::time::Instant) -> Decision {
        // discard a stale min estimate so the next sample can re-establish
        // the baseline; prevents an outlier from pinning cwnd at the floor.
        // The EWMA must be reset alongside it: an EWMA frozen during a
        // congested period carries forward a "current latency" that is
        // not comparable to the new baseline the next samples will draw,
        // and would otherwise produce a spurious growth burst on the
        // first tick after expiry under sustained congestion.
        if let Some((_, observed_at)) = self.min_latency
            && now.saturating_duration_since(observed_at) > self.config.min_latency_max_age
        {
            self.min_latency = None;
            self.ewma_latency_ns = None;
        }
        if self.tick_sample_count == 0 {
            return Decision::with_concurrency(self.cwnd);
        }
        let mean_ns = (self.tick_sum_latency_ns / u128::from(self.tick_sample_count)) as f64;
        // the very first sample-bearing tick establishes the baseline —
        // EWMA equals min by construction so ratio is always 1.0. Skipping
        // the adjustment on that tick prevents an unconditional cwnd bump
        // from a cold start.
        let is_first_sample_tick = self.ewma_latency_ns.is_none();
        self.ewma_latency_ns = Some(match self.ewma_latency_ns {
            Some(prev) => self.config.ewma_alpha * mean_ns + (1.0 - self.config.ewma_alpha) * prev,
            None => mean_ns,
        });
        self.tick_sum_latency_ns = 0;
        self.tick_sample_count = 0;
        if !is_first_sample_tick
            && let (Some(ewma), Some((min, _))) = (self.ewma_latency_ns, self.min_latency)
        {
            let ratio = ewma / (min as f64);
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
    fn tracks_minimum_latency_across_samples() {
        let mut c = VegasController::new(VegasConfig::default());
        let start = std::time::Instant::now();
        c.on_sample(&sample(start, std::time::Duration::from_millis(10)));
        c.on_sample(&sample(start, std::time::Duration::from_millis(2)));
        c.on_sample(&sample(start, std::time::Duration::from_millis(5)));
        assert_eq!(c.min_latency(), Some(std::time::Duration::from_millis(2)));
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
        // consistent minimum latency over many ticks should grow cwnd
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
        // with min == mean by construction, the naive ratio calculation
        // returns 1.0 on the first sample-bearing tick and would always
        // grow cwnd. The controller must skip the adjustment on this tick
        // to avoid a baseline-inflation bias.
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 5,
            ..VegasConfig::default()
        });
        let start = std::time::Instant::now();
        c.on_sample(&sample(start, std::time::Duration::from_millis(2)));
        assert_eq!(c.on_tick(start).max_in_flight, Some(5));
    }

    #[test]
    fn min_latency_ages_out_and_is_re_established() {
        let mut c = VegasController::new(VegasConfig {
            initial_cwnd: 10,
            min_latency_max_age: std::time::Duration::from_millis(100),
            ..VegasConfig::default()
        });
        let t0 = std::time::Instant::now();
        c.on_sample(&sample(t0, std::time::Duration::from_micros(500)));
        // an on_tick before the age expires preserves the min
        c.on_tick(t0 + std::time::Duration::from_millis(50));
        assert_eq!(c.min_latency(), Some(std::time::Duration::from_micros(500)),);
        // after the max age, on_tick discards the stale baseline
        c.on_tick(t0 + std::time::Duration::from_millis(200));
        assert_eq!(c.min_latency(), None);
        // a fresh, much larger sample becomes the new baseline
        c.on_sample(&sample(
            t0 + std::time::Duration::from_millis(201),
            std::time::Duration::from_millis(3),
        ));
        assert_eq!(c.min_latency(), Some(std::time::Duration::from_millis(3)));
    }

    #[test]
    fn min_latency_age_out_under_persistent_congestion_does_not_trigger_growth() {
        // sustained congestion: min_latency at 2ms, ewma inflated at 5ms.
        // When the min ages out but the congestion is still present, we
        // must not immediately grow cwnd just because the baseline was
        // reset. Fix: age-out clears ewma too, so the next tick re-runs
        // the first-tick skip on the re-established baseline.
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
        // phase 2: sustained congestion at 5ms — cwnd shrinks
        let cwnd_before_congestion = c.cwnd();
        for i in 1..=5 {
            let now = t0 + std::time::Duration::from_millis(10 * i);
            c.on_sample(&sample(now, std::time::Duration::from_millis(5)));
            c.on_tick(now);
        }
        assert!(
            c.cwnd() < cwnd_before_congestion,
            "expected shrink under congestion, got {} (from {})",
            c.cwnd(),
            cwnd_before_congestion,
        );
        let cwnd_during_congestion = c.cwnd();
        // phase 3: age out (now > t0 + max_age = 100ms). Congestion persists —
        // samples still at 5ms. Controller should NOT treat this as new
        // headroom and grow.
        let t_expired = t0 + std::time::Duration::from_millis(200);
        c.on_sample(&sample(t_expired, std::time::Duration::from_millis(5)));
        c.on_tick(t_expired);
        assert_eq!(
            c.cwnd(),
            cwnd_during_congestion,
            "cwnd must not grow on the tick immediately after min age-out under persistent congestion",
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
        // several empty ticks
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
        // min_latency=0, making the ewma/min ratio divide by zero or
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
    fn equal_min_latency_samples_refresh_observed_at() {
        // Regression: previously `observed_at` only advanced on a
        // *strictly* lower sample. A workload that consistently observed
        // the true minimum would see the baseline expire on
        // `min_latency_max_age` even though every sample confirmed the
        // floor. Now, equal-latency samples also refresh the timestamp.
        let mut c = VegasController::new(VegasConfig {
            min_latency_max_age: std::time::Duration::from_millis(100),
            ..VegasConfig::default()
        });
        let t0 = std::time::Instant::now();
        // establish baseline at 2ms
        c.on_sample(&sample(t0, std::time::Duration::from_millis(2)));
        assert_eq!(c.min_latency(), Some(std::time::Duration::from_millis(2)));
        // feed an equal-latency sample 200ms later — past the age-out
        // window. The baseline must stay alive because the sample
        // confirmed it.
        let t_late = t0 + std::time::Duration::from_millis(200);
        c.on_sample(&Sample {
            started_at: t_late,
            completed_at: t_late + std::time::Duration::from_millis(2),
            bytes: 0,
            outcome: Outcome::Ok,
        });
        c.on_tick(t_late + std::time::Duration::from_millis(1));
        assert_eq!(
            c.min_latency(),
            Some(std::time::Duration::from_millis(2)),
            "baseline aged out despite continuous confirmation at the true min",
        );
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
