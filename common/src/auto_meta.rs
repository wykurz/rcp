//! Pure decision-to-enforcement glue for the adaptive metadata throttle.
//!
//! The auto-meta adapter task (spawned in [`crate::spawn_auto_meta_throttle`])
//! watches a [`congestion::Decision`] stream and applies each new decision to
//! the [`throttle`] enforcement layer. The state machine that diffs
//! consecutive decisions and picks the right throttle calls lives here as a
//! pure function so it can be unit-tested without a tokio runtime or the
//! global throttle singletons.

/// Side effects the adapter dispatches to the throttle layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdapterAction {
    /// Apply a concurrency cap. `0` is the sentinel that disables the cap
    /// via [`throttle::set_max_ops_in_flight`] — new acquires return
    /// immediately without a permit, which is exactly the "no limit"
    /// semantics the controller asks for with `max_in_flight: None`.
    SetMaxInFlight(usize),
    /// Update the ops-throttle per-interval replenish count.
    SetOpsReplenish(usize),
    /// Flip the ops-throttle flag back on after a prior disable so
    /// `consume` / `get_ops_token` once again waits for tokens.
    EnableOpsThrottle,
    /// Disable the ops-throttle flag so `get_ops_token` becomes a no-op.
    /// Matches the "no limit on this dimension" reading of
    /// `rate_per_sec: None`.
    DisableOpsThrottle,
}

/// Assumed replenish-interval when converting ops/sec -> per-interval
/// token count. 100ms matches the common case produced by
/// [`crate::get_replenish_interval`] when `ops_throttle > 100` and keeps
/// the rate conversion simple and consistent across the adapter.
const REPLENISH_INTERVAL_SECS: f64 = 0.1;

/// How often the adapter task reports the `RoutingSink` drop count.
/// Kept in this module (rather than the call site) so tests can use the
/// same cadence.
pub(crate) const DROP_REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// Drive the auto-meta adapter loop for a single [`throttle::Resource`]:
/// consume controller decisions from `decision_rx`, dispatch each to the
/// throttle layer (for the same resource) via [`apply_decision`], and
/// periodically surface the routing sink's dropped-sample count. Exits
/// cleanly when the decision channel closes.
///
/// One adapter task runs per resource. Each resource has its own
/// decision channel (own controller) and its own enforcement gate, but
/// every adapter shares the same `RoutingSink` (so drop counts are
/// process-wide rather than per-resource).
///
/// `apply_rate` controls whether this adapter forwards the decision's
/// `rate_per_sec` dimension to the global ops-throttle. The global
/// `OPS_THROTTLE` is shared across all resources, so it must be driven
/// by exactly one controller (the destination metadata controller, by
/// convention) — the others ignore the rate dimension to avoid
/// fighting over the global rate gate.
///
/// Lives in this module (rather than inline in
/// [`crate::spawn_auto_meta_throttle`]) so unit tests can drive it
/// directly on the current tokio runtime without spawning a dedicated
/// one or touching tool-level setup.
pub(crate) async fn run_adapter(
    resource: throttle::Resource,
    apply_rate: bool,
    mut decision_rx: tokio::sync::watch::Receiver<congestion::Decision>,
    sink: std::sync::Arc<congestion::RoutingSink>,
) {
    let mut drop_report_interval = tokio::time::interval(DROP_REPORT_INTERVAL);
    drop_report_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // swallow the at-t=0 tick so the first report fires after a full
    // interval rather than immediately.
    drop_report_interval.tick().await;
    let mut last_reported = 0u64;
    let mut last_applied: congestion::Decision = congestion::Decision::UNLIMITED;
    loop {
        tokio::select! {
            changed = decision_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let decision = *decision_rx.borrow();
                for action in apply_decision(last_applied, decision) {
                    match action {
                        AdapterAction::SetMaxInFlight(n) => {
                            throttle::set_max_ops_in_flight(resource, n);
                        }
                        AdapterAction::SetOpsReplenish(n) => {
                            if apply_rate {
                                throttle::set_ops_replenish(n);
                            }
                        }
                        AdapterAction::EnableOpsThrottle => {
                            if apply_rate {
                                throttle::enable_ops_throttle();
                            }
                        }
                        AdapterAction::DisableOpsThrottle => {
                            if apply_rate {
                                throttle::disable_ops_throttle();
                            }
                        }
                    }
                }
                last_applied = decision;
            }
            _ = drop_report_interval.tick() => {
                let total = sink.dropped_samples();
                if total > last_reported {
                    tracing::warn!(
                        "auto-meta-throttle ({resource:?}): RoutingSink dropped {} samples in \
                         the last {:?} (total: {}); the control-loop channel is under-scaled \
                         or the task is stalling",
                        total - last_reported,
                        DROP_REPORT_INTERVAL,
                        total,
                    );
                    last_reported = total;
                }
            }
        }
    }
    tracing::debug!(
        "auto-meta-throttle ({resource:?}): adapter/monitor exiting (decision channel closed)",
    );
}

/// Diff two [`congestion::Decision`]s and produce the minimal sequence of
/// [`AdapterAction`]s needed to transition the enforcement layer.
///
/// Honors the contract documented on [`congestion::Decision`]: `None` on a
/// dimension means "no limit on this dimension" and actively *clears* any
/// prior cap. Dimensions that did not change emit nothing.
///
/// The returned actions are safe to apply in the order given:
/// [`AdapterAction::SetOpsReplenish`] before [`AdapterAction::EnableOpsThrottle`]
/// so the new rate is in place when the flag flips on.
pub(crate) fn apply_decision(
    prev: congestion::Decision,
    new: congestion::Decision,
) -> Vec<AdapterAction> {
    let mut actions = Vec::new();
    if new.max_in_flight != prev.max_in_flight {
        match new.max_in_flight {
            Some(max) => actions.push(AdapterAction::SetMaxInFlight(max as usize)),
            None => actions.push(AdapterAction::SetMaxInFlight(0)),
        }
    }
    if new.rate_per_sec != prev.rate_per_sec {
        match new.rate_per_sec {
            Some(rate) => {
                // Preserve rate<=0 (and NaN) as a genuine "halt" signal —
                // matches the simulator's `can_submit` contract. For any
                // strictly-positive rate, enforce a floor of 1 token per
                // interval so rates below 10 ops/sec (which would truncate
                // to 0 at the 100ms interval) don't silently pause the
                // gate after the initial drain. The tradeoff: a
                // rate-aware controller that asks for less than 10 ops/sec
                // gets the 10-ops/sec floor instead of a pause. Static
                // `--ops-throttle` values below the floor are rejected at
                // config-validation time.
                let replenish = if rate > 0.0 {
                    ((rate * REPLENISH_INTERVAL_SECS) as usize).max(1)
                } else {
                    0
                };
                actions.push(AdapterAction::SetOpsReplenish(replenish));
                actions.push(AdapterAction::EnableOpsThrottle);
            }
            None => actions.push(AdapterAction::DisableOpsThrottle),
        }
    }
    actions
}

#[cfg(test)]
mod tests {
    use super::*;
    use congestion::Decision;

    #[test]
    fn identical_decisions_emit_no_actions() {
        let d = Decision::with_concurrency(8);
        assert!(apply_decision(d, d).is_empty());
    }

    #[test]
    fn unlimited_to_unlimited_is_noop() {
        let d = Decision::UNLIMITED;
        assert!(apply_decision(d, d).is_empty());
    }

    #[test]
    fn max_in_flight_none_to_some_applies_cap() {
        let actions = apply_decision(Decision::UNLIMITED, Decision::with_concurrency(10));
        assert_eq!(actions, vec![AdapterAction::SetMaxInFlight(10)]);
    }

    #[test]
    fn max_in_flight_some_to_some_applies_new_cap() {
        let actions = apply_decision(
            Decision::with_concurrency(5),
            Decision::with_concurrency(20),
        );
        assert_eq!(actions, vec![AdapterAction::SetMaxInFlight(20)]);
    }

    #[test]
    fn max_in_flight_some_to_none_disables_via_zero_sentinel() {
        // Per the Decision contract, None clears the cap. The semaphore's
        // set_max(0) achieves this by flipping the gate off so new
        // acquires succeed without a permit — not by stalling at 0.
        let actions = apply_decision(Decision::with_concurrency(10), Decision::UNLIMITED);
        assert_eq!(actions, vec![AdapterAction::SetMaxInFlight(0)]);
    }

    #[test]
    fn rate_none_to_some_enables_after_setting_replenish() {
        // The enable action must come AFTER the replenish update so the
        // first token drop after re-enablement uses the new rate, not a
        // stale one from a prior configuration.
        let actions = apply_decision(Decision::UNLIMITED, Decision::with_rate(1_000.0));
        assert_eq!(
            actions,
            vec![
                AdapterAction::SetOpsReplenish(100),
                AdapterAction::EnableOpsThrottle,
            ],
        );
    }

    #[test]
    fn rate_some_to_some_updates_replenish_and_re_enables() {
        // Always emit enable alongside replenish, even for Some->Some, so
        // the adapter is idempotent: a controller that disables then
        // re-applies the *same* rate still ends up with the gate enabled.
        let actions = apply_decision(Decision::with_rate(100.0), Decision::with_rate(500.0));
        assert_eq!(
            actions,
            vec![
                AdapterAction::SetOpsReplenish(50),
                AdapterAction::EnableOpsThrottle,
            ],
        );
    }

    #[test]
    fn rate_some_to_none_disables_throttle() {
        let actions = apply_decision(Decision::with_rate(1_000.0), Decision::UNLIMITED);
        assert_eq!(actions, vec![AdapterAction::DisableOpsThrottle]);
    }

    #[test]
    fn both_dimensions_diff_independently() {
        let actions = apply_decision(
            Decision::with_concurrency_and_rate(4, 100.0),
            Decision::with_concurrency_and_rate(8, 500.0),
        );
        assert_eq!(
            actions,
            vec![
                AdapterAction::SetMaxInFlight(8),
                AdapterAction::SetOpsReplenish(50),
                AdapterAction::EnableOpsThrottle,
            ],
        );
    }

    #[test]
    fn only_changed_dimension_emits_action() {
        // rate unchanged: no rate actions.
        let actions = apply_decision(
            Decision::with_concurrency_and_rate(4, 100.0),
            Decision::with_concurrency_and_rate(8, 100.0),
        );
        assert_eq!(actions, vec![AdapterAction::SetMaxInFlight(8)]);
        // concurrency unchanged: no concurrency actions.
        let actions = apply_decision(
            Decision::with_concurrency_and_rate(4, 100.0),
            Decision::with_concurrency_and_rate(4, 500.0),
        );
        assert_eq!(
            actions,
            vec![
                AdapterAction::SetOpsReplenish(50),
                AdapterAction::EnableOpsThrottle,
            ],
        );
    }

    #[test]
    fn rate_conversion_uses_100ms_interval() {
        // 10_000 ops/sec at 100ms intervals => 1000 tokens per interval.
        let actions = apply_decision(Decision::UNLIMITED, Decision::with_rate(10_000.0));
        assert_eq!(actions[0], AdapterAction::SetOpsReplenish(1_000));
    }

    #[test]
    fn rate_conversion_rounds_down() {
        // 125 ops/sec at 100ms intervals => 12.5 tokens, truncated to 12.
        let actions = apply_decision(Decision::UNLIMITED, Decision::with_rate(125.0));
        assert_eq!(actions[0], AdapterAction::SetOpsReplenish(12));
    }

    #[test]
    fn rate_conversion_floors_positive_rates_at_one_token() {
        // Regression: rates below the 100ms conversion floor (< 10 ops/sec)
        // previously truncated to 0 tokens/interval, which pauses the gate
        // after the initial drain. Now any strictly-positive rate produces
        // at least 1 token per interval — effectively clamping the enforced
        // rate to a floor of 10 ops/sec. A controller that asks for less
        // than 10 ops/sec gets 10 (over-rate) instead of 0 (halt).
        let actions = apply_decision(Decision::UNLIMITED, Decision::with_rate(5.0));
        assert_eq!(actions[0], AdapterAction::SetOpsReplenish(1));
        let actions = apply_decision(Decision::UNLIMITED, Decision::with_rate(0.1));
        assert_eq!(actions[0], AdapterAction::SetOpsReplenish(1));
        // At exactly the 10 ops/sec threshold, conversion still produces 1.
        let actions = apply_decision(Decision::UNLIMITED, Decision::with_rate(10.0));
        assert_eq!(actions[0], AdapterAction::SetOpsReplenish(1));
    }

    #[test]
    fn rate_conversion_clamps_negative_and_nan_to_zero() {
        // Controllers should never emit these, but we treat them as
        // circuit-breaker states rather than panicking: rate 0 is a paused
        // gate (tokens drain to zero and nothing replenishes). The
        // downstream enable_ops_throttle still fires so the pause is
        // observable.
        let actions_neg = apply_decision(Decision::UNLIMITED, Decision::with_rate(-5.0));
        assert_eq!(actions_neg[0], AdapterAction::SetOpsReplenish(0));
        let actions_nan = apply_decision(Decision::UNLIMITED, Decision::with_rate(f64::NAN));
        assert_eq!(actions_nan[0], AdapterAction::SetOpsReplenish(0));
    }

    /// Property-based coverage: `apply_decision` is pure and finite-state,
    /// so we can drive it across the whole input space to verify
    /// contract invariants, not just the hand-picked transitions above.
    mod properties {
        use super::*;
        use proptest::prelude::*;

        /// Arbitrary Decision, covering both-None through both-Some with
        /// finite rate values (NaN is specifically tested elsewhere).
        fn any_decision() -> impl Strategy<Value = Decision> {
            (
                prop::option::of(1u32..10_000),
                prop::option::of(0.0f64..100_000.0),
            )
                .prop_map(|(max, rate)| Decision {
                    max_in_flight: max,
                    rate_per_sec: rate,
                })
        }

        proptest! {
            /// Identity: re-applying the same decision emits no actions.
            /// This is the no-op case the adapter relies on for idle ticks
            /// when the controller emits the same value tick after tick.
            #[test]
            fn identity_emits_no_actions(d in any_decision()) {
                prop_assert!(apply_decision(d, d).is_empty());
            }

            /// Dimension independence: the set of dimensions that differ
            /// between `prev` and `new` fully determines which kinds of
            /// actions the adapter emits. If only `max_in_flight` changed,
            /// rate actions must not appear, and vice versa.
            #[test]
            fn only_changed_dimensions_produce_actions(
                prev in any_decision(),
                new in any_decision(),
            ) {
                let actions = apply_decision(prev, new);
                let max_changed = prev.max_in_flight != new.max_in_flight;
                let rate_changed = prev.rate_per_sec != new.rate_per_sec;
                let has_max_action = actions
                    .iter()
                    .any(|a| matches!(a, AdapterAction::SetMaxInFlight(_)));
                let has_rate_action = actions.iter().any(|a| {
                    matches!(
                        a,
                        AdapterAction::SetOpsReplenish(_)
                            | AdapterAction::EnableOpsThrottle
                            | AdapterAction::DisableOpsThrottle
                    )
                });
                prop_assert_eq!(has_max_action, max_changed);
                prop_assert_eq!(has_rate_action, rate_changed);
            }

            /// Rate enablement always pairs a SetOpsReplenish with an
            /// EnableOpsThrottle — the ordering matters (replenish first
            /// so the new count is live when the flag flips on) and
            /// neither should ever appear alone.
            #[test]
            fn rate_some_transitions_pair_replenish_and_enable(
                prev in any_decision(),
                rate in 0.0f64..100_000.0,
            ) {
                let new = Decision {
                    max_in_flight: prev.max_in_flight,
                    rate_per_sec: Some(rate),
                };
                let actions = apply_decision(prev, new);
                if prev.rate_per_sec == Some(rate) {
                    // no change on rate dimension — no rate actions.
                    prop_assert!(!actions.iter().any(|a| matches!(a,
                        AdapterAction::SetOpsReplenish(_)
                        | AdapterAction::EnableOpsThrottle
                    )));
                } else {
                    // Some rate change: must emit exactly [SetOpsReplenish, EnableOpsThrottle] in order.
                    let rate_actions: Vec<_> = actions
                        .iter()
                        .filter(|a| matches!(
                            a,
                            AdapterAction::SetOpsReplenish(_)
                                | AdapterAction::EnableOpsThrottle
                                | AdapterAction::DisableOpsThrottle
                        ))
                        .copied()
                        .collect();
                    prop_assert_eq!(rate_actions.len(), 2);
                    prop_assert!(matches!(
                        rate_actions[0],
                        AdapterAction::SetOpsReplenish(_)
                    ));
                    prop_assert_eq!(rate_actions[1], AdapterAction::EnableOpsThrottle);
                }
            }

            /// A Some -> None transition on rate always emits a single
            /// DisableOpsThrottle — and never an EnableOpsThrottle or
            /// SetOpsReplenish (which would contradict "no limit").
            #[test]
            fn rate_none_transitions_only_disable(
                prev_rate in 0.0f64..100_000.0,
                prev_max in prop::option::of(1u32..10_000),
            ) {
                let prev = Decision {
                    max_in_flight: prev_max,
                    rate_per_sec: Some(prev_rate),
                };
                let new = Decision {
                    max_in_flight: prev_max,
                    rate_per_sec: None,
                };
                let actions = apply_decision(prev, new);
                let rate_actions: Vec<_> = actions
                    .iter()
                    .filter(|a| matches!(
                        a,
                        AdapterAction::SetOpsReplenish(_)
                            | AdapterAction::EnableOpsThrottle
                            | AdapterAction::DisableOpsThrottle
                    ))
                    .copied()
                    .collect();
                prop_assert_eq!(rate_actions, vec![AdapterAction::DisableOpsThrottle]);
            }

            /// For any Decision, the total number of emitted actions is
            /// bounded: at most 1 max-in-flight action + at most 2 rate
            /// actions = 3.
            #[test]
            fn action_count_bounded(
                prev in any_decision(),
                new in any_decision(),
            ) {
                let actions = apply_decision(prev, new);
                prop_assert!(actions.len() <= 3);
            }
        }
    }

    #[test]
    fn full_cycle_disable_then_re_enable_applies_enable() {
        // Regression for the rate re-enable bug: after a Some->None that
        // emitted DisableOpsThrottle, a later None->Some must emit
        // EnableOpsThrottle so consume() starts gating again.
        let disabled = apply_decision(Decision::with_rate(100.0), Decision::UNLIMITED);
        assert_eq!(disabled, vec![AdapterAction::DisableOpsThrottle]);
        let re_enabled = apply_decision(Decision::UNLIMITED, Decision::with_rate(200.0));
        assert_eq!(
            re_enabled,
            vec![
                AdapterAction::SetOpsReplenish(20),
                AdapterAction::EnableOpsThrottle,
            ],
        );
    }

    /// End-to-end tests that wire the real ControlUnit + run_adapter and
    /// verify decisions propagate all the way to the global ops-in-flight
    /// semaphore. These touch the process-wide `OPS_IN_FLIGHT_LIMIT` and
    /// the installed `SampleSink`, so they must serialize via `FEEDBACK_GUARD`.
    mod feedback {
        use super::*;
        use congestion::{
            ControlUnit, Controller, Decision, FixedController, RoutingSinkBuilder, Sample, Side,
        };

        /// Serialize these tests against each other. The only other consumer of
        /// the per-side `OPS_IN_FLIGHT_LIMIT_*` globals is the cwnd-deadlock
        /// integration test in `common/tests/probe_metadata.rs`, which lives
        /// in a separate test binary and does not share this process.
        static FEEDBACK_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

        /// Reset the global throttle + sample-sink state after a test so
        /// a subsequent test sees a clean slate regardless of ordering.
        async fn reset_globals() {
            for &side in &throttle::Side::ALL {
                for &op in &throttle::MetadataOp::ALL {
                    throttle::set_max_ops_in_flight(throttle::Resource::meta(side, op), 0);
                }
            }
            throttle::disable_ops_throttle();
            congestion::clear_sample_sink();
        }

        /// Build the wiring used by every feedback test and return the
        /// adapter / unit join handles. Tests run a single resource at a
        /// time; the wired probes and observed cwnd are scoped to that
        /// resource.
        async fn wire_adapter<C: congestion::Controller + 'static>(
            side: Side,
            op: congestion::MetadataOp,
            resource: throttle::Resource,
            controller: C,
            tick: std::time::Duration,
        ) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
            let mut builder = RoutingSinkBuilder::new();
            let metadata_rx = builder.metadata_receiver(side, op);
            let sink = std::sync::Arc::new(builder.build());
            congestion::install_sample_sink(sink.clone());
            let (unit, decision_rx, _snapshot_rx) =
                ControlUnit::new("test", controller, metadata_rx, tick);
            let unit_handle = unit.spawn();
            let adapter_handle = tokio::spawn(run_adapter(resource, true, decision_rx, sink));
            (unit_handle, adapter_handle)
        }

        #[tokio::test]
        async fn fixed_controller_initial_decision_reaches_throttle() {
            let _g = FEEDBACK_GUARD.lock().await;
            reset_globals().await;
            // FixedController(42) emits `with_concurrency(42)` forever. The
            // first decision after startup must land on the source-side
            // OPS_IN_FLIGHT_LIMIT — proving unit -> adapter -> throttle
            // wiring on that side.
            let side = Side::Source;
            let op = congestion::MetadataOp::Stat;
            let resource =
                throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
            let (unit, adapter) = wire_adapter(
                side,
                op,
                resource,
                FixedController::with_concurrency(42),
                std::time::Duration::from_millis(10),
            )
            .await;
            // poll for up to 500ms; adapter runs on another task and the
            // watch's initial value must be consumed before we can observe.
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
            loop {
                if throttle::current_ops_in_flight_limit(resource) == 42 {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    panic!(
                        "adapter did not apply initial cwnd=42 within 500ms; \
                         current limit = {}",
                        throttle::current_ops_in_flight_limit(resource),
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            unit.abort();
            adapter.abort();
            reset_globals().await;
        }

        /// Controller that emits a scripted sequence of decisions on
        /// successive `on_tick` calls — once exhausted, it keeps emitting
        /// the last decision. Used to deterministically probe the
        /// adapter's end-to-end propagation without the variance of a
        /// real algorithm responding to real samples.
        struct ScriptedController {
            script: Vec<Decision>,
            idx: usize,
        }

        impl Controller for ScriptedController {
            fn on_sample(&mut self, _s: &Sample) {}
            fn on_tick(&mut self, _now: std::time::Instant) -> Decision {
                let d = self.script[self.idx];
                if self.idx + 1 < self.script.len() {
                    self.idx += 1;
                }
                d
            }
            fn name(&self) -> &'static str {
                "scripted"
            }
        }

        #[tokio::test]
        async fn scripted_decisions_propagate_in_order_to_throttle() {
            let _g = FEEDBACK_GUARD.lock().await;
            reset_globals().await;
            // Feed a deterministic sequence of concurrency decisions: grow,
            // grow more, shrink. Each tick the scripted controller returns
            // the next entry; the adapter must route each to the throttle
            // in order. Watching throttle::current_ops_in_flight_limit()
            // transition through the sequence proves the decision -> action
            // pipeline is wired end-to-end.
            let side = Side::Source;
            let op = congestion::MetadataOp::Stat;
            let resource =
                throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
            let tick = std::time::Duration::from_millis(20);
            let controller = ScriptedController {
                script: vec![
                    Decision::with_concurrency(5),
                    Decision::with_concurrency(25),
                    Decision::with_concurrency(3),
                ],
                idx: 0,
            };
            let (unit, adapter) = wire_adapter(side, op, resource, controller, tick).await;
            // Observe each target cwnd in sequence. The controller's
            // on_tick produces one per tick, so after ~N ticks each value
            // in the script should land. Allow slack for interleaving.
            for target in [5_usize, 25, 3] {
                let deadline = std::time::Instant::now() + tick * 20;
                while throttle::current_ops_in_flight_limit(resource) != target {
                    if std::time::Instant::now() >= deadline {
                        panic!(
                            "cwnd did not reach scripted value {target} within {:?}; \
                             observed = {}",
                            tick * 20,
                            throttle::current_ops_in_flight_limit(resource),
                        );
                    }
                    tokio::time::sleep(tick / 4).await;
                }
            }
            unit.abort();
            adapter.abort();
            reset_globals().await;
        }

        #[tokio::test]
        async fn decision_with_none_clears_cap_at_throttle() {
            let _g = FEEDBACK_GUARD.lock().await;
            reset_globals().await;
            // Script that grows to a cap, then drops to None. The None
            // transition must land as SetMaxInFlight(0) at the throttle
            // so the semaphore disables its cap, matching the Decision
            // "None means no limit" contract.
            let side = Side::Source;
            let op = congestion::MetadataOp::Stat;
            let resource =
                throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
            let tick = std::time::Duration::from_millis(20);
            let controller = ScriptedController {
                script: vec![Decision::with_concurrency(15), Decision::UNLIMITED],
                idx: 0,
            };
            let (unit, adapter) = wire_adapter(side, op, resource, controller, tick).await;
            // First, observe 15 land.
            let deadline = std::time::Instant::now() + tick * 10;
            while throttle::current_ops_in_flight_limit(resource) != 15 {
                if std::time::Instant::now() >= deadline {
                    panic!("cwnd=15 never landed");
                }
                tokio::time::sleep(tick / 4).await;
            }
            // Then observe the UNLIMITED → SetMaxInFlight(0) land.
            let deadline = std::time::Instant::now() + tick * 20;
            while throttle::current_ops_in_flight_limit(resource) != 0 {
                if std::time::Instant::now() >= deadline {
                    panic!(
                        "cwnd did not clear to 0 after None decision; observed {}",
                        throttle::current_ops_in_flight_limit(resource),
                    );
                }
                tokio::time::sleep(tick / 4).await;
            }
            unit.abort();
            adapter.abort();
            reset_globals().await;
        }

        #[tokio::test]
        async fn adapter_exits_when_decision_channel_closes() {
            let _g = FEEDBACK_GUARD.lock().await;
            reset_globals().await;
            // Dropping the ControlUnit (via the join handle completing after
            // its sample channel closes) must cause the adapter task to
            // exit. We set this up with a tight tick + short-lived unit.
            let side = Side::Source;
            let op = congestion::MetadataOp::Stat;
            let resource =
                throttle::Resource::meta(throttle::Side::Source, throttle::MetadataOp::Stat);
            let mut builder = RoutingSinkBuilder::new();
            let metadata_rx = builder.metadata_receiver(side, op);
            let sink = std::sync::Arc::new(builder.build());
            congestion::install_sample_sink(sink.clone());
            let (unit, decision_rx, _snapshot_rx) = ControlUnit::new(
                "test",
                FixedController::with_concurrency(1),
                metadata_rx,
                std::time::Duration::from_millis(5),
            );
            let unit_handle = unit.spawn();
            let adapter_handle = tokio::spawn(run_adapter(resource, true, decision_rx, sink));
            // Let the initial decision land, then tear down the sink so
            // ControlUnit's sample channel closes and it exits. Its watch
            // sender drops, which closes the adapter's decision channel.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            congestion::clear_sample_sink();
            // The sink Arc's sender end lives on the sink; clearing the
            // global doesn't drop it. Instead rely on abort of the unit.
            // The adapter must survive this and only exit when its
            // decision channel sender is dropped.
            unit_handle.abort();
            // Aborting the unit drops its watch sender → adapter sees
            // the channel close and should exit cleanly within a short
            // window.
            let adapter_result =
                tokio::time::timeout(std::time::Duration::from_secs(1), adapter_handle).await;
            assert!(
                adapter_result.is_ok(),
                "adapter did not exit within 1s of decision channel close"
            );
            reset_globals().await;
        }
    }
}
