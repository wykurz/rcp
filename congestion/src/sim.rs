//! Deterministic, event-driven simulator for evaluating [`Controller`]s.
//!
//! The simulator models a single bottleneck with a fluid, BBR-style
//! latency/throughput relationship: up to the bandwidth-delay product (BDP)
//! each operation completes in `min_latency`; beyond it, service time grows
//! linearly with the in-flight count. This is the simplest model that
//! reproduces the key qualitative behavior adaptive controllers must
//! handle — latency inflation as a saturation signal.
//!
//! The simulator is test infrastructure; it is pure synchronous Rust. The
//! only real-clock call is `Instant::now()` at the start of `run_scenario`
//! to anchor the simulated timeline; every downstream timestamp is
//! `start + Duration` arithmetic driven by the model, so the *deltas* are
//! fully deterministic across runs even if absolute `Instant` values are
//! not.
//!
//! # Example
//!
//! ```
//! use congestion::{FixedController};
//! use congestion::sim::{BottleneckModel, ScenarioConfig, run_scenario};
//!
//! let mut controller = FixedController::with_concurrency(8);
//! let bottleneck = BottleneckModel {
//!     capacity_per_sec: 10_000.0,
//!     min_latency: std::time::Duration::from_millis(2),
//! };
//! let config = ScenarioConfig {
//!     duration: std::time::Duration::from_secs(1),
//!     tick_interval: std::time::Duration::from_millis(50),
//!     op_bytes: 0,
//!     workload_max_in_flight: 1024,
//! };
//! let result = run_scenario(&mut controller, &bottleneck, &config);
//! assert!(result.total_ops > 0);
//! ```

use crate::controller::{Controller, Decision, Outcome, Sample};

/// Single-bottleneck fluid model.
///
/// When the number of in-flight operations is at or below the
/// bandwidth-delay product (`capacity_per_sec * min_latency`), each op
/// completes after exactly `min_latency`. Beyond that, the bottleneck
/// becomes the constraint and service time is `in_flight / capacity_per_sec`.
#[derive(Debug, Clone, Copy)]
pub struct BottleneckModel {
    /// Steady-state capacity of the bottleneck, in submissions/sec.
    pub capacity_per_sec: f64,
    /// Minimum observed service time when uncongested (the "RTprop").
    pub min_latency: std::time::Duration,
}

impl BottleneckModel {
    /// Bandwidth-delay product, in units of in-flight ops.
    pub fn bdp(&self) -> f64 {
        self.capacity_per_sec * self.min_latency.as_secs_f64()
    }
    /// Service time for an operation submitted when `in_flight_at_submit`
    /// operations (including itself) are outstanding.
    ///
    /// Computed as `max(min_latency, in_flight / capacity_per_sec)` — a
    /// single expression rather than a piecewise form so fractional BDP
    /// values don't produce a discontinuity at the `in_flight == bdp`
    /// boundary.
    pub fn service_time(&self, in_flight_at_submit: u32) -> std::time::Duration {
        let bottleneck = std::time::Duration::from_secs_f64(
            f64::from(in_flight_at_submit) / self.capacity_per_sec,
        );
        std::cmp::max(self.min_latency, bottleneck)
    }
}

/// Configuration for a simulation run.
#[derive(Debug, Clone, Copy)]
pub struct ScenarioConfig {
    /// Total wall-clock duration to simulate.
    pub duration: std::time::Duration,
    /// How often the controller's `on_tick` is invoked.
    pub tick_interval: std::time::Duration,
    /// Bytes per operation. Zero models a metadata-only workload where
    /// `rate_per_sec` is interpreted as ops/sec.
    pub op_bytes: u64,
    /// Upper bound on operations the workload can have in flight, independent
    /// of controller decisions. Models the OS/runtime concurrency ceiling
    /// (e.g. spawned-task count). Required to keep simulations bounded when
    /// the controller emits [`Decision::UNLIMITED`].
    pub workload_max_in_flight: u32,
}

/// Outcome of a simulation run.
#[derive(Debug, Clone)]
pub struct ScenarioResult {
    pub total_ops: u64,
    pub total_bytes: u64,
    pub samples: Vec<Sample>,
    pub decisions: Vec<(std::time::Instant, Decision)>,
}

impl ScenarioResult {
    /// Arithmetic mean operation latency, or `None` if no samples were
    /// recorded.
    pub fn mean_latency(&self) -> Option<std::time::Duration> {
        if self.samples.is_empty() {
            return None;
        }
        let total_nanos: u128 = self.samples.iter().map(|s| s.latency().as_nanos()).sum();
        let mean_nanos = total_nanos / (self.samples.len() as u128);
        Some(std::time::Duration::from_nanos(mean_nanos as u64))
    }
    /// Observed throughput over the given window, in ops/sec.
    pub fn throughput_ops_per_sec(&self, window: std::time::Duration) -> f64 {
        let window_secs = window.as_secs_f64();
        if window_secs <= 0.0 {
            return 0.0;
        }
        (self.total_ops as f64) / window_secs
    }
    /// Observed throughput over the given window, in bytes/sec.
    pub fn throughput_bytes_per_sec(&self, window: std::time::Duration) -> f64 {
        let window_secs = window.as_secs_f64();
        if window_secs <= 0.0 {
            return 0.0;
        }
        (self.total_bytes as f64) / window_secs
    }
}

/// Drive `controller` against `bottleneck` for `config.duration` and return a
/// full trace of samples and decisions.
pub fn run_scenario<C: Controller + ?Sized>(
    controller: &mut C,
    bottleneck: &BottleneckModel,
    config: &ScenarioConfig,
) -> ScenarioResult {
    let start = std::time::Instant::now();
    let end = start + config.duration;
    let mut now = start;
    let mut decision = controller.on_tick(now);
    let mut decisions = vec![(now, decision)];
    let mut next_tick = start + config.tick_interval;
    let mut in_flight: std::collections::BinaryHeap<std::cmp::Reverse<InFlightOp>> =
        std::collections::BinaryHeap::new();
    let mut samples: Vec<Sample> = Vec::new();
    let mut last_submit: Option<std::time::Instant> = None;
    let mut total_bytes: u64 = 0;
    while now < end {
        let next_completion = in_flight.peek().map(|op| op.0.complete_at);
        let pacing_gate = next_pacing_time(last_submit, &decision, config.op_bytes);
        let mut next_event = end.min(next_tick);
        if let Some(completion) = next_completion {
            next_event = next_event.min(completion);
        }
        // only consider the pacing gate if we're otherwise ready to submit
        if can_submit(&decision, in_flight.len(), config.workload_max_in_flight)
            && let Some(gate) = pacing_gate
            && gate > now
        {
            next_event = next_event.min(gate);
        }
        now = next_event;
        // drain all completions at or before `now`
        while let Some(&std::cmp::Reverse(op)) = in_flight.peek() {
            if op.complete_at > now {
                break;
            }
            in_flight.pop();
            let sample = Sample {
                started_at: op.submit_at,
                completed_at: op.complete_at,
                bytes: op.bytes,
                outcome: Outcome::Ok,
            };
            controller.on_sample(&sample);
            samples.push(sample);
            total_bytes = total_bytes.saturating_add(op.bytes);
        }
        // fire any ticks we just reached. Defensive while-loop in case a
        // scenario configures a very short tick interval.
        while now >= next_tick && next_tick <= end {
            decision = controller.on_tick(next_tick);
            decisions.push((next_tick, decision));
            next_tick += config.tick_interval;
        }
        // submit as many ops as the decision and workload cap allow, respecting
        // pacing. Submissions at a given `now` are instantaneous bursts.
        loop {
            if !can_submit(&decision, in_flight.len(), config.workload_max_in_flight) {
                break;
            }
            if let Some(gate) = next_pacing_time(last_submit, &decision, config.op_bytes)
                && gate > now
            {
                break;
            }
            let new_in_flight = u32::try_from(in_flight.len() + 1)
                .expect("in-flight count bounded by workload_max_in_flight");
            let service = bottleneck.service_time(new_in_flight);
            in_flight.push(std::cmp::Reverse(InFlightOp {
                submit_at: now,
                complete_at: now + service,
                bytes: config.op_bytes,
            }));
            last_submit = Some(now);
        }
    }
    ScenarioResult {
        total_ops: samples.len() as u64,
        total_bytes,
        samples,
        decisions,
    }
}

fn can_submit(decision: &Decision, in_flight: usize, workload_max: u32) -> bool {
    if in_flight >= (workload_max as usize) {
        return false;
    }
    // rate_per_sec == 0.0 means "halt submissions"; anything above 0 is a
    // pacing rate that's applied via `next_pacing_time`. Negative rates are
    // treated as halt rather than panicking so controllers can express a
    // circuit-breaker state without a runtime failure mode.
    if let Some(rate) = decision.rate_per_sec
        && rate <= 0.0
    {
        return false;
    }
    match decision.max_in_flight {
        Some(max) => in_flight < (max as usize),
        None => true,
    }
}

fn next_pacing_time(
    last_submit: Option<std::time::Instant>,
    decision: &Decision,
    op_bytes: u64,
) -> Option<std::time::Instant> {
    let rate = decision.rate_per_sec?;
    if rate <= 0.0 {
        // caller is expected to have already refused via `can_submit`;
        // returning None here is the "no further pacing gate" response.
        return None;
    }
    let last = last_submit?;
    let weight = if op_bytes == 0 { 1.0 } else { op_bytes as f64 };
    let spacing = std::time::Duration::from_secs_f64(weight / rate);
    Some(last + spacing)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InFlightOp {
    submit_at: std::time::Instant,
    complete_at: std::time::Instant,
    bytes: u64,
}

impl PartialOrd for InFlightOp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InFlightOp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.complete_at
            .cmp(&other.complete_at)
            .then_with(|| self.submit_at.cmp(&other.submit_at))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bdp_is_capacity_times_min_latency() {
        let m = BottleneckModel {
            capacity_per_sec: 10_000.0,
            min_latency: std::time::Duration::from_millis(2),
        };
        assert!((m.bdp() - 20.0).abs() < 1e-9);
    }

    #[test]
    fn service_time_at_or_below_bdp_is_min_latency() {
        let m = BottleneckModel {
            capacity_per_sec: 10_000.0,
            min_latency: std::time::Duration::from_millis(2),
        };
        assert_eq!(m.service_time(1), std::time::Duration::from_millis(2));
        assert_eq!(m.service_time(20), std::time::Duration::from_millis(2));
    }

    #[test]
    fn service_time_above_bdp_grows_with_in_flight() {
        let m = BottleneckModel {
            capacity_per_sec: 10_000.0,
            min_latency: std::time::Duration::from_millis(2),
        };
        // in_flight=100, capacity=10k → service = 100 / 10_000 = 10ms
        assert_eq!(m.service_time(100), std::time::Duration::from_millis(10));
    }

    #[test]
    fn service_time_at_fractional_bdp_is_continuous() {
        // capacity 500/sec, min_latency 1ms → BDP = 0.5 (fractional)
        let m = BottleneckModel {
            capacity_per_sec: 500.0,
            min_latency: std::time::Duration::from_millis(1),
        };
        // below BDP (in_flight=0 would hit the formula at zero), at BDP
        // (in_flight=1 >> 0.5), and above BDP (in_flight=2) all behave
        // consistently: max(min_latency, in_flight/capacity).
        assert_eq!(m.service_time(1), std::time::Duration::from_millis(2));
        assert_eq!(m.service_time(2), std::time::Duration::from_millis(4));
    }
}
