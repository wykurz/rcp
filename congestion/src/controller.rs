//! Core trait and supporting types for pluggable congestion-control algorithms.

/// The outcome of a single measured operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Operation completed with no backpressure signal.
    Ok,
    /// Operation completed but the underlying system signaled backpressure
    /// (e.g. `EAGAIN`, `ECONNREFUSED`, or an observably long response that
    /// suggests saturation).
    Backpressure,
    /// Operation failed for a reason unrelated to congestion (permission
    /// denied, no such file, etc.).
    Error,
}

/// A single observation of an operation's behavior, fed to a [`Controller`].
///
/// The `started_at` / `completed_at` pair should enclose the actual syscall or
/// network round-trip, not the permit acquisition; the controller reasons
/// about service time at the bottleneck, not queueing time ahead of it.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    /// When the operation was submitted to the underlying system.
    pub started_at: std::time::Instant,
    /// When the operation reported completion.
    pub completed_at: std::time::Instant,
    /// Bytes transferred. Zero for metadata operations.
    pub bytes: u64,
    /// How the operation concluded.
    pub outcome: Outcome,
}

impl Sample {
    /// Wall-clock duration of the measured operation.
    pub fn latency(&self) -> std::time::Duration {
        self.completed_at.duration_since(self.started_at)
    }
}

/// Absolute limits emitted by a controller for the enforcement layer to apply.
///
/// A `None` field means "no limit on this dimension." Controllers emit a fresh
/// `Decision` on every tick; the enforcement layer is responsible for diffing
/// consecutive decisions and applying only what changed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Decision {
    /// Maximum number of operations that may be in flight concurrently.
    pub max_in_flight: Option<u32>,
    /// Maximum submission rate in resource-appropriate units:
    /// ops/sec for metadata controllers, bytes/sec for data controllers.
    pub rate_per_sec: Option<f64>,
}

impl Decision {
    /// A decision that imposes no limits.
    pub const UNLIMITED: Decision = Decision {
        max_in_flight: None,
        rate_per_sec: None,
    };
    /// Decision bounded only by concurrency.
    pub fn with_concurrency(max_in_flight: u32) -> Decision {
        Decision {
            max_in_flight: Some(max_in_flight),
            rate_per_sec: None,
        }
    }
    /// Decision bounded only by rate.
    pub fn with_rate(rate_per_sec: f64) -> Decision {
        Decision {
            max_in_flight: None,
            rate_per_sec: Some(rate_per_sec),
        }
    }
    /// Decision bounded by both concurrency and rate.
    pub fn with_concurrency_and_rate(max_in_flight: u32, rate_per_sec: f64) -> Decision {
        Decision {
            max_in_flight: Some(max_in_flight),
            rate_per_sec: Some(rate_per_sec),
        }
    }
}

/// A pluggable congestion-control algorithm.
///
/// A `Controller` is a stateful, synchronous state machine that consumes
/// operation-completion [`Sample`]s and, on each tick, emits an absolute
/// [`Decision`] expressing the current permitted concurrency and/or rate.
///
/// Controllers must be `Send` so the enforcement layer can own them from a
/// dedicated control task. They do not need to be `Sync`: the enforcement
/// layer guarantees single-threaded access.
///
/// All methods are synchronous and do not perform I/O. Time is always passed
/// in; controllers must not read clocks directly. This keeps algorithms
/// deterministic under the simulator in [`crate::sim`].
pub trait Controller: Send {
    /// Record a completed operation.
    fn on_sample(&mut self, sample: &Sample);
    /// Produce the current decision. Called periodically by the enforcement
    /// layer. The controller must return an absolute limit (not a delta).
    fn on_tick(&mut self, now: std::time::Instant) -> Decision;
    /// Short, stable identifier used in logs and metrics (e.g. "noop",
    /// "fixed", "vegas").
    fn name(&self) -> &'static str;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_latency_is_difference_of_timestamps() {
        let start = std::time::Instant::now();
        let sample = Sample {
            started_at: start,
            completed_at: start + std::time::Duration::from_millis(5),
            bytes: 0,
            outcome: Outcome::Ok,
        };
        assert_eq!(sample.latency(), std::time::Duration::from_millis(5));
    }

    #[test]
    fn decision_unlimited_imposes_no_limits() {
        assert_eq!(Decision::UNLIMITED.max_in_flight, None);
        assert_eq!(Decision::UNLIMITED.rate_per_sec, None);
    }

    #[test]
    fn decision_constructors_set_only_the_named_dimension() {
        let c = Decision::with_concurrency(8);
        assert_eq!(c.max_in_flight, Some(8));
        assert_eq!(c.rate_per_sec, None);
        let r = Decision::with_rate(100.0);
        assert_eq!(r.max_in_flight, None);
        assert_eq!(r.rate_per_sec, Some(100.0));
        let both = Decision::with_concurrency_and_rate(4, 50.0);
        assert_eq!(both.max_in_flight, Some(4));
        assert_eq!(both.rate_per_sec, Some(50.0));
    }
}
