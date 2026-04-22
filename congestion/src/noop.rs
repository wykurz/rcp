//! A controller that never limits. Default when congestion control is off.

use crate::controller::{Controller, Decision, Sample};

/// Controller that always emits [`Decision::UNLIMITED`].
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopController;

impl NoopController {
    pub fn new() -> Self {
        Self
    }
}

impl Controller for NoopController {
    fn on_sample(&mut self, _sample: &Sample) {}
    fn on_tick(&mut self, _now: std::time::Instant) -> Decision {
        Decision::UNLIMITED
    }
    fn name(&self) -> &'static str {
        "noop"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::Outcome;

    #[test]
    fn always_emits_unlimited_regardless_of_samples() {
        let mut controller = NoopController::new();
        let start = std::time::Instant::now();
        for _ in 0..100 {
            controller.on_sample(&Sample {
                started_at: start,
                completed_at: start + std::time::Duration::from_millis(50),
                bytes: 4096,
                outcome: Outcome::Backpressure,
            });
        }
        let decision = controller.on_tick(start + std::time::Duration::from_secs(1));
        assert_eq!(decision, Decision::UNLIMITED);
    }
}
