//! A controller that honors a static, user-supplied budget. Matches the
//! behavior of the existing manual `--ops-throttle` / `--iops-throttle` flags.

use crate::controller::{Controller, Decision, Sample};

/// Controller that always emits the same configured [`Decision`].
///
/// Useful as an explicit opt-out from adaptive control while still wanting a
/// hard cap, and as a regression baseline when comparing adaptive algorithms
/// under the simulator.
#[derive(Debug, Clone, Copy)]
pub struct FixedController {
    decision: Decision,
}

impl FixedController {
    pub fn new(decision: Decision) -> Self {
        Self { decision }
    }
    pub fn with_concurrency(max_in_flight: u32) -> Self {
        Self::new(Decision::with_concurrency(max_in_flight))
    }
    pub fn with_rate(rate_per_sec: f64) -> Self {
        Self::new(Decision::with_rate(rate_per_sec))
    }
    pub fn with_concurrency_and_rate(max_in_flight: u32, rate_per_sec: f64) -> Self {
        Self::new(Decision::with_concurrency_and_rate(
            max_in_flight,
            rate_per_sec,
        ))
    }
}

impl Controller for FixedController {
    fn on_sample(&mut self, _sample: &Sample) {}
    fn on_tick(&mut self, _now: std::time::Instant) -> Decision {
        self.decision
    }
    fn name(&self) -> &'static str {
        "fixed"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::Outcome;

    #[test]
    fn emits_configured_concurrency_budget() {
        let mut controller = FixedController::with_concurrency(16);
        let decision = controller.on_tick(std::time::Instant::now());
        assert_eq!(decision.max_in_flight, Some(16));
        assert_eq!(decision.rate_per_sec, None);
    }

    #[test]
    fn emits_configured_rate_budget() {
        let mut controller = FixedController::with_rate(5000.0);
        let decision = controller.on_tick(std::time::Instant::now());
        assert_eq!(decision.max_in_flight, None);
        assert_eq!(decision.rate_per_sec, Some(5000.0));
    }

    #[test]
    fn samples_do_not_change_emitted_decision() {
        let mut controller = FixedController::with_concurrency(4);
        let start = std::time::Instant::now();
        controller.on_sample(&Sample {
            started_at: start,
            completed_at: start + std::time::Duration::from_secs(60),
            bytes: 0,
            outcome: Outcome::Backpressure,
        });
        assert_eq!(controller.on_tick(start).max_in_flight, Some(4));
    }
}
