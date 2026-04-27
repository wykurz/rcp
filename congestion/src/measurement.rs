//! Hot-path measurement primitives.
//!
//! A [`Probe`] brackets a single filesystem or network operation and emits a
//! [`Sample`] to the currently installed [`SampleSink`] on completion. When
//! no sink is installed, probes are effectively free: the only cost is two
//! `Instant::now()` calls plus a single `RwLock::read` that returns `None`.
//!
//! The sink is a process-wide singleton installed by the enforcement layer
//! (typically in tool main functions). Tests can install a
//! [`testing::CollectingSink`](crate::testing::CollectingSink) to assert that
//! probes fire as expected.

use crate::controller::{Outcome, Sample};

/// Which side of an operation a probe is on.
///
/// Tools like `rcp` and `rcmp` touch two filesystems with different
/// service-time profiles; we run an independent controller per side so a
/// saturated source doesn't drag the destination's `cwnd` down or vice
/// versa. Single-path tools (`rrm`, `filegen`) still partition reads
/// (`Source`) from writes/mutations (`Destination`) since those have
/// different latency profiles even on the same filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    /// Reads of the source filesystem — directory walks, source-side stats.
    Source,
    /// Writes/mutations of the destination filesystem — `create_dir`,
    /// `hard_link`, `unlink`, `open(O_CREAT)`, etc.
    Destination,
}

/// Which resource a probe is measuring.
///
/// Separate kinds feed independent controllers in the control loop.
/// Metadata kinds are split by [`Side`]: different filesystems typically
/// have different service-time profiles, so one cap per side prevents a
/// saturated source from dragging down the destination's `cwnd` and vice
/// versa.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceKind {
    /// Single per-file metadata syscall: `stat`, `symlink_metadata`,
    /// `mkdir`, `unlink`, `hard_link`, `symlink`, `chmod`,
    /// `open(O_CREAT)`, `read_link`, etc. Real round-trip work each
    /// time, lookup or mutation alike.
    Metadata(Side),
    /// Individual read chunks in a copy pipeline. Reserved for future
    /// data-path controllers; not currently routed to any sink channel.
    DataRead,
    /// Individual write chunks in a copy or filegen pipeline. Reserved
    /// for future data-path controllers.
    DataWrite,
}

/// Consumer of completed operation samples.
///
/// Implementations must be cheap under a high sample rate since `record` is
/// called on the hot path. The sink trait is intentionally minimal; richer
/// behavior (windowing, sharding, routing to multiple controllers) belongs
/// in the control-loop layer that hosts the sink.
pub trait SampleSink: Send + Sync {
    fn record(&self, kind: ResourceKind, sample: &Sample);
}

static SINK: std::sync::RwLock<Option<std::sync::Arc<dyn SampleSink>>> =
    std::sync::RwLock::new(None);

/// Install the process-wide [`SampleSink`]. Replaces any prior sink.
pub fn install_sample_sink(sink: std::sync::Arc<dyn SampleSink>) {
    *SINK.write().expect("sample sink lock poisoned") = Some(sink);
}

/// Remove the current sink, if any. After this call, probes are no-ops again.
pub fn clear_sample_sink() {
    *SINK.write().expect("sample sink lock poisoned") = None;
}

fn emit(kind: ResourceKind, sample: &Sample) {
    // clone the Arc out of the lock before dispatching so a slow sink
    // does not block `install_sample_sink`/`clear_sample_sink` (which
    // need the write lock) and so a sink implementation that re-enters
    // the sink API from within `record` cannot deadlock.
    let sink = SINK
        .read()
        .expect("sample sink lock poisoned")
        .as_ref()
        .cloned();
    if let Some(sink) = sink {
        sink.record(kind, sample);
    }
}

/// A measurement-in-progress for a single operation.
///
/// # Lifecycle
///
/// ```no_run
/// use congestion::{Probe, Side};
///
/// # async fn example() {
/// let probe = Probe::start_metadata(Side::Source);
/// // ... perform the syscall or operation ...
/// probe.complete_ok(0);
/// # }
/// ```
///
/// Forgetting to call [`Probe::complete`] / [`Probe::complete_ok`] drops the
/// probe without recording anything. This is intentional: error paths that
/// bail out early should not produce misleading latency samples.
#[must_use = "call Probe::complete_ok or Probe::complete to record the measurement"]
pub struct Probe {
    kind: ResourceKind,
    started_at: std::time::Instant,
}

impl Probe {
    /// Begin measuring an operation of the given kind.
    pub fn start(kind: ResourceKind) -> Self {
        Self {
            kind,
            started_at: std::time::Instant::now(),
        }
    }
    /// Shorthand for `Probe::start(ResourceKind::Metadata(side))`. Use
    /// this to bracket a single per-file metadata syscall (lookup or
    /// mutation).
    pub fn start_metadata(side: Side) -> Self {
        Self::start(ResourceKind::Metadata(side))
    }
    /// Shorthand for `Probe::start(ResourceKind::DataRead)`.
    pub fn start_read() -> Self {
        Self::start(ResourceKind::DataRead)
    }
    /// Shorthand for `Probe::start(ResourceKind::DataWrite)`.
    pub fn start_write() -> Self {
        Self::start(ResourceKind::DataWrite)
    }
    /// Complete the probe with outcome [`Outcome::Ok`] and the given byte
    /// count (use `0` for metadata ops).
    pub fn complete_ok(self, bytes: u64) {
        self.complete(bytes, Outcome::Ok);
    }
    /// Complete the probe with an explicit outcome.
    pub fn complete(self, bytes: u64, outcome: Outcome) {
        emit(
            self.kind,
            &Sample {
                started_at: self.started_at,
                completed_at: std::time::Instant::now(),
                bytes,
                outcome,
            },
        );
    }
    /// Drop the probe without recording a sample.
    pub fn discard(self) {}
}

/// Test-only mutex that serializes access to the process-wide `SampleSink`
/// global across every test that touches it — including tests in sibling
/// modules (see `control_loop::tests`) and downstream integration tests
/// that install their own sinks. Nextest isolates per-process so races are
/// only observable under `cargo test`'s threaded runner, but we guard
/// regardless to keep both runners reliable.
///
/// Uses [`tokio::sync::Mutex`] so `#[tokio::test]` bodies can hold the guard
/// across await points without tripping clippy's `await_holding_lock`.
#[cfg(test)]
pub(crate) static SINK_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::CollectingSink;

    #[test]
    fn probe_without_sink_is_a_no_op() {
        let _guard = SINK_GUARD.blocking_lock();
        clear_sample_sink();
        let probe = Probe::start_metadata(Side::Source);
        probe.complete_ok(0);
    }

    #[test]
    fn probe_records_metadata_samples_to_installed_sink() {
        let _guard = SINK_GUARD.blocking_lock();
        let sink = std::sync::Arc::new(CollectingSink::new());
        install_sample_sink(sink.clone());
        Probe::start_metadata(Side::Source).complete_ok(0);
        Probe::start_metadata(Side::Source).complete_ok(0);
        assert_eq!(sink.metadata_count(), 2);
        clear_sample_sink();
    }

    #[test]
    fn probe_separates_resource_kinds() {
        let _guard = SINK_GUARD.blocking_lock();
        let sink = std::sync::Arc::new(CollectingSink::new());
        install_sample_sink(sink.clone());
        Probe::start_metadata(Side::Source).complete_ok(0);
        Probe::start_read().complete_ok(4096);
        Probe::start_write().complete_ok(8192);
        assert_eq!(sink.metadata_count(), 1);
        assert_eq!(sink.read_count(), 1);
        assert_eq!(sink.write_count(), 1);
        clear_sample_sink();
    }

    #[test]
    fn sample_latency_reflects_wall_time() {
        let _guard = SINK_GUARD.blocking_lock();
        let sink = std::sync::Arc::new(CollectingSink::new());
        install_sample_sink(sink.clone());
        let probe = Probe::start_metadata(Side::Source);
        std::thread::sleep(std::time::Duration::from_millis(5));
        probe.complete_ok(0);
        let samples = sink.metadata_samples();
        assert_eq!(samples.len(), 1);
        assert!(samples[0].latency() >= std::time::Duration::from_millis(5));
        clear_sample_sink();
    }

    #[test]
    fn discarded_probe_produces_no_sample() {
        let _guard = SINK_GUARD.blocking_lock();
        let sink = std::sync::Arc::new(CollectingSink::new());
        install_sample_sink(sink.clone());
        Probe::start_metadata(Side::Source).discard();
        assert_eq!(sink.metadata_count(), 0);
        clear_sample_sink();
    }

    #[test]
    fn probe_dropped_without_complete_produces_no_sample() {
        // matches the behavior of an early-return in an error path: the
        // probe is simply dropped and no sample is emitted, so a failed
        // syscall doesn't pollute the controller's latency signal.
        let _guard = SINK_GUARD.blocking_lock();
        let sink = std::sync::Arc::new(CollectingSink::new());
        install_sample_sink(sink.clone());
        {
            let _probe = Probe::start_metadata(Side::Source);
            // _probe falls out of scope here without complete or discard
        }
        assert_eq!(sink.metadata_count(), 0);
        clear_sample_sink();
    }

    #[test]
    fn installing_a_new_sink_replaces_the_old_one() {
        let _guard = SINK_GUARD.blocking_lock();
        let first = std::sync::Arc::new(CollectingSink::new());
        install_sample_sink(first.clone());
        Probe::start_metadata(Side::Source).complete_ok(0);
        let second = std::sync::Arc::new(CollectingSink::new());
        install_sample_sink(second.clone());
        Probe::start_metadata(Side::Source).complete_ok(0);
        Probe::start_metadata(Side::Source).complete_ok(0);
        assert_eq!(first.metadata_count(), 1);
        assert_eq!(second.metadata_count(), 2);
        clear_sample_sink();
    }
}
