//! Property-based tests (via `proptest`) for invariants that must hold
//! across the whole input space — complementing the example-based unit
//! tests that pin down specific scenarios.

use congestion::sim::{BottleneckModel, ScenarioConfig, run_scenario};
use congestion::{
    Controller, Decision, FixedController, NoopController, Outcome, Sample, VegasConfig,
    VegasController,
};
use proptest::prelude::*;

/// Strategy: a `VegasConfig` whose `min_cwnd <= max_cwnd`, with bounds
/// drawn from ranges that make bugs observable (broad enough to explore
/// clamping, narrow enough to stay fast).
fn valid_vegas_config() -> impl Strategy<Value = VegasConfig> {
    (1u32..50, 1u32..2000, 1u32..5000).prop_map(|(min_raw, max_raw, initial_raw)| {
        let min = min_raw;
        let max = min.saturating_add(max_raw);
        VegasConfig {
            initial_cwnd: initial_raw,
            min_cwnd: min,
            max_cwnd: max,
            ..VegasConfig::default()
        }
    })
}

/// Strategy for latency sample values in nanoseconds. Spans from
/// near-zero (degenerate) up to ~1s so both the fast-path and
/// saturation regimes are exercised.
fn latency_ns_strategy() -> impl Strategy<Value = u64> {
    prop_oneof![
        // degenerate: single-nanosecond latency (probes tend to this
        // in tight test loops — the controller must not divide by 0
        // or NaN).
        Just(1u64),
        // realistic metadata-op range: 100us .. 100ms.
        100_000u64..100_000_000,
        // saturation: 100ms .. 1s.
        100_000_000u64..1_000_000_000,
    ]
}

fn sample(start: std::time::Instant, latency_ns: u64, offset_ns: u64) -> Sample {
    let started_at = start + std::time::Duration::from_nanos(offset_ns);
    Sample {
        started_at,
        completed_at: started_at + std::time::Duration::from_nanos(latency_ns),
        bytes: 0,
        outcome: Outcome::Ok,
    }
}

proptest! {
    /// Core invariant: a `VegasController`'s emitted cwnd is always within
    /// the configured `[min_cwnd.max(1), max_cwnd]` band, across any
    /// sample/tick sequence — no matter how pathological the latencies.
    ///
    /// Historically useful bug-finder: catches off-by-one in clamping,
    /// overflow in `saturating_add`/`saturating_sub`, and any code path
    /// that bypasses the clamp on initial_cwnd.
    #[test]
    fn vegas_cwnd_always_within_configured_bounds(
        config in valid_vegas_config(),
        latencies in prop::collection::vec(latency_ns_strategy(), 1..50),
    ) {
        let mut c = VegasController::new(config);
        let start = std::time::Instant::now();
        let expected_min = config.min_cwnd.max(1);
        let expected_max = config.max_cwnd.max(1);
        // the controller clamps initial_cwnd — verify that up front.
        prop_assert!(c.cwnd() >= expected_min);
        prop_assert!(c.cwnd() <= expected_max);
        for (i, latency_ns) in latencies.iter().copied().enumerate() {
            let offset_ns = (i as u64) * 1_000_000; // 1ms between submissions
            let s = sample(start, latency_ns, offset_ns);
            c.on_sample(&s);
            // intersperse ticks so the controller has chances to adjust.
            if i % 3 == 2 {
                let decision = c.on_tick(s.completed_at);
                // Vegas must always emit concurrency (never rate) and the
                // value must be within bounds.
                prop_assert!(decision.rate_per_sec.is_none());
                let cwnd = decision.max_in_flight.expect("vegas emits max_in_flight");
                prop_assert!(
                    cwnd >= expected_min,
                    "cwnd {cwnd} below min {expected_min} (config={config:?})",
                );
                prop_assert!(
                    cwnd <= expected_max,
                    "cwnd {cwnd} above max {expected_max} (config={config:?})",
                );
            }
        }
    }

    /// Vegas must never emit a `rate_per_sec` decision — it's a
    /// concurrency-based controller, and the Decision contract requires
    /// the adapter to differentiate rate-only vs concurrency-only
    /// controllers by which dimension is `Some`.
    #[test]
    fn vegas_never_emits_rate_per_sec(
        config in valid_vegas_config(),
        latency_ns in latency_ns_strategy(),
        tick_count in 1usize..20,
    ) {
        let mut c = VegasController::new(config);
        let start = std::time::Instant::now();
        for i in 0..tick_count {
            let s = sample(start, latency_ns, i as u64 * 1_000_000);
            c.on_sample(&s);
            let decision = c.on_tick(s.completed_at);
            prop_assert!(decision.rate_per_sec.is_none());
        }
    }

    /// `FixedController`'s output is independent of inputs — whatever
    /// sample sequence we feed it, `on_tick` returns the configured
    /// decision unchanged. This pins down its role as the regression
    /// baseline for adaptive algorithms.
    #[test]
    fn fixed_controller_output_is_constant(
        cap in 1u32..10_000,
        latency_ns in latency_ns_strategy(),
        sample_count in 1usize..30,
    ) {
        let mut c = FixedController::with_concurrency(cap);
        let expected = Decision::with_concurrency(cap);
        let start = std::time::Instant::now();
        for i in 0..sample_count {
            let s = sample(start, latency_ns, i as u64 * 1_000_000);
            c.on_sample(&s);
            prop_assert_eq!(c.on_tick(s.completed_at), expected);
        }
    }

    /// `NoopController` emits `Decision::UNLIMITED` forever — no matter
    /// the sample stream.
    #[test]
    fn noop_controller_always_unlimited(
        latencies in prop::collection::vec(latency_ns_strategy(), 1..30),
    ) {
        let mut c = NoopController::new();
        let start = std::time::Instant::now();
        for (i, l) in latencies.iter().copied().enumerate() {
            let s = sample(start, l, i as u64 * 1_000_000);
            c.on_sample(&s);
            prop_assert_eq!(c.on_tick(s.completed_at), Decision::UNLIMITED);
        }
    }

    /// The simulator must be deterministic: identical inputs produce
    /// identical sample sequences (by latency, bytes, and ordering) and
    /// identical total counts.
    ///
    /// Absolute `Instant` values differ between runs because
    /// `run_scenario` anchors at `Instant::now()`, but every downstream
    /// timestamp is `start + Duration` arithmetic — so the deltas must
    /// match bit-for-bit.
    #[test]
    fn simulator_is_deterministic(
        capacity in 100.0f64..50_000.0,
        min_latency_us in 100u64..5_000,
        duration_ms in 200u64..1500,
        workload_cap in 4u32..512,
        cwnd in 1u32..256,
    ) {
        let bottleneck = BottleneckModel {
            capacity_per_sec: capacity,
            min_latency: std::time::Duration::from_micros(min_latency_us),
        };
        let config = ScenarioConfig {
            duration: std::time::Duration::from_millis(duration_ms),
            tick_interval: std::time::Duration::from_millis(50),
            op_bytes: 0,
            workload_max_in_flight: workload_cap,
        };
        let mut c1 = FixedController::with_concurrency(cwnd);
        let mut c2 = FixedController::with_concurrency(cwnd);
        let r1 = run_scenario(&mut c1, &bottleneck, &config);
        let r2 = run_scenario(&mut c2, &bottleneck, &config);
        prop_assert_eq!(r1.total_ops, r2.total_ops);
        prop_assert_eq!(r1.total_bytes, r2.total_bytes);
        prop_assert_eq!(r1.samples.len(), r2.samples.len());
        prop_assert_eq!(r1.decisions.len(), r2.decisions.len());
        for (s1, s2) in r1.samples.iter().zip(r2.samples.iter()) {
            prop_assert_eq!(s1.latency(), s2.latency());
            prop_assert_eq!(s1.bytes, s2.bytes);
        }
        for ((_, d1), (_, d2)) in r1.decisions.iter().zip(r2.decisions.iter()) {
            prop_assert_eq!(d1, d2);
        }
    }

    /// Under a `NoopController` (unlimited), scenario throughput cannot
    /// exceed the configured bottleneck capacity (modulo a small slop
    /// for discretization). This is the "service time is the floor" law
    /// of the simulator's fluid model.
    #[test]
    fn scenario_throughput_respects_bottleneck_capacity(
        capacity in 100.0f64..50_000.0,
        min_latency_us in 100u64..2_000,
        duration_ms in 500u64..2000,
    ) {
        let bottleneck = BottleneckModel {
            capacity_per_sec: capacity,
            min_latency: std::time::Duration::from_micros(min_latency_us),
        };
        let duration = std::time::Duration::from_millis(duration_ms);
        let config = ScenarioConfig {
            duration,
            tick_interval: std::time::Duration::from_millis(50),
            op_bytes: 0,
            // generous workload cap so the bottleneck — not the workload —
            // is what limits throughput.
            workload_max_in_flight: 4096,
        };
        let mut controller = NoopController::new();
        let result = run_scenario(&mut controller, &bottleneck, &config);
        let throughput = result.throughput_ops_per_sec(duration);
        // Allow a small (10%) slop: the simulator's fluid model rounds
        // op completion times, and the first tick's ramp-up can shift
        // accounting slightly. Exceeding this threshold is a real bug.
        prop_assert!(
            throughput <= capacity * 1.10,
            "throughput {throughput:.1} exceeded capacity {capacity:.1} (duration={duration:?})",
        );
    }

    /// Mean observed latency is at least `min_latency` for any run —
    /// the bottleneck model clamps service time from below at
    /// `min_latency`, so no sample can be faster.
    #[test]
    fn scenario_mean_latency_is_at_least_min_latency(
        capacity in 100.0f64..10_000.0,
        min_latency_us in 500u64..10_000,
        duration_ms in 200u64..1000,
        cwnd in 1u32..64,
    ) {
        let bottleneck = BottleneckModel {
            capacity_per_sec: capacity,
            min_latency: std::time::Duration::from_micros(min_latency_us),
        };
        let config = ScenarioConfig {
            duration: std::time::Duration::from_millis(duration_ms),
            tick_interval: std::time::Duration::from_millis(50),
            op_bytes: 0,
            workload_max_in_flight: 1024,
        };
        let mut controller = FixedController::with_concurrency(cwnd);
        let result = run_scenario(&mut controller, &bottleneck, &config);
        if let Some(mean) = result.mean_latency() {
            prop_assert!(
                mean >= bottleneck.min_latency,
                "mean latency {mean:?} is below min_latency {:?}",
                bottleneck.min_latency,
            );
        }
    }
}
