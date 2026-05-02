//! Integration-style scenario tests that drive the built-in controllers
//! through the simulator and assert aggregate throughput/latency behavior.

use congestion::sim::{BottleneckModel, ScenarioConfig, run_scenario};
use congestion::{FixedController, NoopController, RatioConfig, RatioController};

/// 10k ops/sec capacity, 2ms min latency → BDP = 20 in-flight. These are
/// the numbers every scenario below reuses unless it overrides them.
fn default_bottleneck() -> BottleneckModel {
    BottleneckModel {
        capacity_per_sec: 10_000.0,
        min_latency: std::time::Duration::from_millis(2),
    }
}

fn metadata_config(duration: std::time::Duration, workload_max_in_flight: u32) -> ScenarioConfig {
    ScenarioConfig {
        duration,
        tick_interval: std::time::Duration::from_millis(50),
        op_bytes: 0,
        workload_max_in_flight,
    }
}

fn approx_eq(actual: f64, expected: f64, tolerance_pct: f64) -> bool {
    let tolerance = expected * (tolerance_pct / 100.0);
    (actual - expected).abs() <= tolerance
}

#[test]
fn noop_controller_saturates_bottleneck_at_capacity() {
    // with no limits and a workload ceiling far above BDP, the bottleneck is
    // the constraint; throughput should converge to capacity.
    let mut controller = NoopController::new();
    let bottleneck = default_bottleneck();
    let duration = std::time::Duration::from_secs(5);
    let config = metadata_config(duration, 2048);
    let result = run_scenario(&mut controller, &bottleneck, &config);
    let throughput = result.throughput_ops_per_sec(duration);
    assert!(
        approx_eq(throughput, bottleneck.capacity_per_sec, 5.0),
        "expected throughput near {} ops/sec, got {}",
        bottleneck.capacity_per_sec,
        throughput,
    );
}

#[test]
fn noop_controller_inflates_latency_when_saturated() {
    // when pushed far beyond BDP, service time grows with queue depth.
    let mut controller = NoopController::new();
    let bottleneck = default_bottleneck();
    let workload = 2048_u32;
    let duration = std::time::Duration::from_secs(5);
    let config = metadata_config(duration, workload);
    let result = run_scenario(&mut controller, &bottleneck, &config);
    let mean = result.mean_latency().expect("samples were produced");
    // steady-state in-flight ≈ workload, so service time ≈ workload/capacity
    let expected_secs = f64::from(workload) / bottleneck.capacity_per_sec;
    let actual_secs = mean.as_secs_f64();
    assert!(
        actual_secs > bottleneck.min_latency.as_secs_f64() * 10.0,
        "expected latency inflation, got {:?}",
        mean,
    );
    assert!(
        approx_eq(actual_secs, expected_secs, 30.0),
        "expected mean latency near {:.3}s, got {:?}",
        expected_secs,
        mean,
    );
}

#[test]
fn fixed_controller_below_bdp_is_concurrency_capped() {
    // concurrency cap of 8 is under BDP=20, so the bottleneck is never
    // saturated; throughput = concurrency / min_latency.
    let concurrency = 8_u32;
    let mut controller = FixedController::with_concurrency(concurrency);
    let bottleneck = default_bottleneck();
    let duration = std::time::Duration::from_secs(5);
    let config = metadata_config(duration, 2048);
    let result = run_scenario(&mut controller, &bottleneck, &config);
    let expected_throughput = f64::from(concurrency) / bottleneck.min_latency.as_secs_f64();
    let throughput = result.throughput_ops_per_sec(duration);
    assert!(
        approx_eq(throughput, expected_throughput, 5.0),
        "expected ~{} ops/sec, got {}",
        expected_throughput,
        throughput,
    );
    let mean = result.mean_latency().expect("samples were produced");
    assert_eq!(
        mean, bottleneck.min_latency,
        "under-saturated run should stay at min_latency",
    );
}

#[test]
fn fixed_controller_above_bdp_saturates_bottleneck() {
    // concurrency cap of 100 is above BDP=20, so the bottleneck is the
    // constraint; throughput should still approach capacity.
    let mut controller = FixedController::with_concurrency(100);
    let bottleneck = default_bottleneck();
    let duration = std::time::Duration::from_secs(5);
    let config = metadata_config(duration, 2048);
    let result = run_scenario(&mut controller, &bottleneck, &config);
    let throughput = result.throughput_ops_per_sec(duration);
    assert!(
        approx_eq(throughput, bottleneck.capacity_per_sec, 5.0),
        "expected near-capacity throughput, got {}",
        throughput,
    );
}

#[test]
fn fixed_controller_rate_limited_below_capacity() {
    // rate of 1000 ops/sec with capacity 10_000; the rate is the bottleneck.
    let rate = 1_000.0;
    let mut controller = FixedController::with_rate(rate);
    let bottleneck = default_bottleneck();
    let duration = std::time::Duration::from_secs(5);
    let config = metadata_config(duration, 2048);
    let result = run_scenario(&mut controller, &bottleneck, &config);
    let throughput = result.throughput_ops_per_sec(duration);
    assert!(
        approx_eq(throughput, rate, 5.0),
        "expected ~{} ops/sec, got {}",
        rate,
        throughput,
    );
    let mean = result.mean_latency().expect("samples were produced");
    // paced submissions keep in-flight ≈ 1 so there's no queueing.
    assert!(
        mean <= bottleneck.min_latency + std::time::Duration::from_micros(100),
        "paced workload should not inflate latency, got {:?}",
        mean,
    );
}

#[test]
fn fixed_controller_rate_above_capacity_caps_at_bottleneck() {
    // rate of 100k ops/sec with capacity 10k; bottleneck is the constraint.
    let mut controller = FixedController::with_rate(100_000.0);
    let bottleneck = default_bottleneck();
    let duration = std::time::Duration::from_secs(5);
    let config = metadata_config(duration, 2048);
    let result = run_scenario(&mut controller, &bottleneck, &config);
    let throughput = result.throughput_ops_per_sec(duration);
    assert!(
        approx_eq(throughput, bottleneck.capacity_per_sec, 5.0),
        "expected near-capacity throughput, got {}",
        throughput,
    );
}

#[test]
fn scenario_emits_one_decision_per_tick_plus_initial() {
    // sanity check on the sim's tick cadence, so future controllers that rely
    // on tick count behave predictably.
    let mut controller = NoopController::new();
    let bottleneck = default_bottleneck();
    let duration = std::time::Duration::from_secs(1);
    let tick = std::time::Duration::from_millis(100);
    let config = ScenarioConfig {
        duration,
        tick_interval: tick,
        op_bytes: 0,
        workload_max_in_flight: 64,
    };
    let result = run_scenario(&mut controller, &bottleneck, &config);
    // one initial tick at t=0, plus one per tick_interval up to (and including) end.
    let expected_ticks = 1 + (duration.as_millis() / tick.as_millis()) as usize;
    assert_eq!(result.decisions.len(), expected_ticks);
}

#[test]
fn ratio_grows_from_cold_start() {
    // The ratio controller starts at cwnd=1 and should grow toward BDP when latency stays
    // near the baseline.
    let mut controller = RatioController::new(RatioConfig {
        initial_cwnd: 1,
        ..RatioConfig::default()
    });
    let bottleneck = default_bottleneck();
    let duration = std::time::Duration::from_secs(3);
    let config = metadata_config(duration, 2048);
    let result = run_scenario(&mut controller, &bottleneck, &config);
    let final_cwnd = controller.cwnd();
    assert!(
        final_cwnd >= u32::try_from(bottleneck.bdp() as u64).unwrap(),
        "expected cwnd to reach at least BDP={}, got {}",
        bottleneck.bdp(),
        final_cwnd,
    );
    assert!(result.total_ops > 1_000, "expected ops to accumulate");
}

#[test]
fn ratio_converges_near_bdp_without_runaway_latency() {
    // Steady-state cwnd should land somewhere above BDP — but not at
    // `max_cwnd`. Use a tighter alpha/beta than defaults: in this
    // deterministic simulator there is no per-op variance, so the
    // percentile-ratio signal is binary (either two windows agree or
    // they disagree by a clean ratio). The defaults (alpha=1.3, beta=1.8)
    // are calibrated for noisy real workloads where the p10/p50 spread
    // carries uncertainty; in the sim a tighter config makes the test
    // assertion meaningful without making the algorithm change behavior
    // in production.
    let mut controller = RatioController::new(RatioConfig {
        initial_cwnd: 1,
        alpha: 1.02,
        beta: 1.10,
        ..RatioConfig::default()
    });
    let bottleneck = default_bottleneck();
    let duration = std::time::Duration::from_secs(5);
    let config = metadata_config(duration, 2048);
    let result = run_scenario(&mut controller, &bottleneck, &config);
    let final_cwnd = f64::from(controller.cwnd());
    let bdp = bottleneck.bdp();
    // Lower bound: cwnd must have grown above BDP (the algorithm makes
    // forward progress). Upper bound: cwnd does not run away to
    // `max_cwnd`. The exact landing point depends on the rate at which
    // long_window's percentile catches up with short_window's.
    assert!(
        final_cwnd >= bdp && final_cwnd <= bdp * 5.0,
        "expected cwnd above BDP={} but well below max (within [{}, {}]), got {}",
        bdp,
        bdp,
        bdp * 5.0,
        final_cwnd,
    );
    let mean = result.mean_latency().expect("samples recorded");
    // With cwnd bounded above, latency is bounded too: latency =
    // cwnd / capacity, and capacity = BDP / min_latency. So latency at
    // cwnd=k*BDP is k*min_latency. Cwnd ≤ 5×BDP ⇒ latency ≤ 5×min_latency
    // — leave some headroom for transient overshoot during ramp-up.
    assert!(
        mean.as_secs_f64() < bottleneck.min_latency.as_secs_f64() * 7.0,
        "mean latency {:?} should stay within ~7× min_latency {:?}",
        mean,
        bottleneck.min_latency,
    );
}

#[test]
fn ratio_achieves_near_capacity_throughput() {
    // once converged, the ratio controller should keep the bottleneck well-utilized.
    let mut controller = RatioController::new(RatioConfig {
        initial_cwnd: 1,
        ..RatioConfig::default()
    });
    let bottleneck = default_bottleneck();
    let duration = std::time::Duration::from_secs(10);
    let config = metadata_config(duration, 2048);
    let result = run_scenario(&mut controller, &bottleneck, &config);
    let throughput = result.throughput_ops_per_sec(duration);
    // The ratio controller is conservative compared to Noop; expect at least 70% of capacity
    // over a long enough run.
    assert!(
        throughput >= bottleneck.capacity_per_sec * 0.7,
        "expected throughput >= 70% of capacity, got {}",
        throughput,
    );
}

#[test]
fn ratio_keeps_latency_bounded_under_saturation_pressure() {
    // Contrast with noop_controller_inflates_latency_when_saturated:
    // the ratio controller's steady-state latency stays within a small
    // multiple of min_latency even when the workload is eager. As in
    // `ratio_converges_near_bdp_without_runaway_latency`, override the
    // shipped cross-percentile defaults with a tighter matched-style
    // hold band straddling 1.0: in this deterministic sim the
    // distribution is degenerate, so any percentile pair collapses to
    // ratio = 1.0 — the explicit alpha/beta keep the hold band tight
    // enough that finite-window noise doesn't push cwnd past the
    // bound this test asserts.
    let mut controller = RatioController::new(RatioConfig {
        initial_cwnd: 1,
        alpha: 1.02,
        beta: 1.10,
        ..RatioConfig::default()
    });
    let bottleneck = default_bottleneck();
    let duration = std::time::Duration::from_secs(5);
    let config = metadata_config(duration, 2048);
    let result = run_scenario(&mut controller, &bottleneck, &config);
    let mean = result.mean_latency().expect("samples recorded");
    // bound matches the cwnd ceiling asserted in the convergence test
    // (5×BDP ⇒ 5×min_latency at saturation), with headroom.
    assert!(
        mean.as_secs_f64() <= bottleneck.min_latency.as_secs_f64() * 7.0,
        "mean latency {:?} exceeded bound; min_latency={:?}",
        mean,
        bottleneck.min_latency,
    );
}

#[test]
fn fixed_controller_with_rate_zero_halts_submissions() {
    // rate == 0.0 in a Decision must mean "no submissions allowed", not
    // "no rate limit" — the circuit-breaker semantics controllers may rely
    // on.
    let mut controller = FixedController::with_rate(0.0);
    let bottleneck = default_bottleneck();
    let duration = std::time::Duration::from_secs(1);
    let config = metadata_config(duration, 2048);
    let result = run_scenario(&mut controller, &bottleneck, &config);
    assert_eq!(result.total_ops, 0, "rate=0 must block every submission");
}

#[test]
fn workload_cap_of_zero_blocks_all_submissions() {
    // a pathological workload config still has to terminate cleanly.
    let mut controller = NoopController::new();
    let bottleneck = default_bottleneck();
    let duration = std::time::Duration::from_millis(200);
    let config = ScenarioConfig {
        duration,
        tick_interval: std::time::Duration::from_millis(50),
        op_bytes: 0,
        workload_max_in_flight: 0,
    };
    let result = run_scenario(&mut controller, &bottleneck, &config);
    assert_eq!(result.total_ops, 0);
}

#[test]
fn data_scenario_tracks_bytes() {
    // 4 KiB per op, rate cap pushes us well below capacity so the test is
    // about byte accounting, not congestion.
    let mut controller = FixedController::with_rate(4096.0 * 500.0);
    let bottleneck = BottleneckModel {
        capacity_per_sec: 1e9,
        min_latency: std::time::Duration::from_micros(100),
    };
    let duration = std::time::Duration::from_secs(2);
    let config = ScenarioConfig {
        duration,
        tick_interval: std::time::Duration::from_millis(50),
        op_bytes: 4096,
        workload_max_in_flight: 128,
    };
    let result = run_scenario(&mut controller, &bottleneck, &config);
    let byte_rate = result.throughput_bytes_per_sec(duration);
    assert!(
        approx_eq(byte_rate, 4096.0 * 500.0, 10.0),
        "expected ~{} B/s, got {}",
        4096.0 * 500.0,
        byte_rate,
    );
}
