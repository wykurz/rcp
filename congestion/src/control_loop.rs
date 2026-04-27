//! Async control-loop plumbing that ties a [`Controller`] to the hot path.
//!
//! The pipeline per controlled resource is:
//!
//! ```text
//!     Probe::complete_ok(_) ──► RoutingSink ──mpsc──► ControlUnit ──watch──► enforcement
//!                                    (Arc)                (task)               (throttle)
//! ```
//!
//! [`RoutingSink`] receives samples from the global `SampleSink` and fans
//! them out to per-resource bounded MPSC channels. Each resource has its
//! own [`ControlUnit`] running as a tokio task: it owns a `Controller`,
//! drains samples, ticks on a configurable interval, and publishes each
//! [`Decision`] on a `tokio::sync::watch` channel so the enforcement layer
//! can apply it.
//!
//! # Backpressure
//!
//! Samples are a lossy signal: dropping a small fraction does not harm the
//! controller's estimate. To keep a stalled or slow control task from
//! leaking memory under a heavy probe rate, the per-resource channels are
//! bounded (see [`DEFAULT_CHANNEL_CAPACITY`]) and overflow samples are
//! silently dropped. The count of dropped samples is exposed via
//! [`RoutingSink::dropped_samples`] for diagnostics.

use crate::controller::{Controller, ControllerSnapshot, Decision, Sample};
use crate::measurement::{ResourceKind, SampleSink, Side};

/// Default cadence at which a control unit calls `on_tick`.
pub const DEFAULT_TICK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

/// Default per-resource sample channel capacity. At a probe rate of 100k/sec
/// this is ~40ms of headroom; enough to survive a brief stall in the
/// control-task scheduler without dropping samples.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 4096;

/// A single resource's control task.
///
/// Construct with [`ControlUnit::new`], receive the decision and snapshot
/// watches via the returned `(unit, decision_rx, snapshot_rx)` tuple, then
/// spawn the unit with [`ControlUnit::spawn`]. The task runs until the
/// sample channel's senders are all dropped (typically because
/// [`clear_sample_sink`][crate::clear_sample_sink] was called and the
/// `RoutingSink` went away).
///
/// # Watches
///
/// Two watches keep enforcement and observability separate:
///
/// - `decision_rx` carries the [`Decision`] the enforcement layer applies.
///   Subscribers wake on every change so caps land promptly.
/// - `snapshot_rx` carries a [`ControllerSnapshot`] for diagnostics —
///   used by the progress bar and other UI surfaces. Snapshot-only
///   changes (e.g. baseline drift on an unchanged `cwnd`) do not wake
///   enforcement, and a busy enforcement subscriber does not stall
///   snapshot publication.
///
/// # Logging
///
/// Each unit emits structured `tracing` events keyed by its [`label`] so
/// multiple units can be told apart in mixed logs:
///
/// - `tracing::trace!` on every tick with the published [`Decision`].
/// - `tracing::debug!` whenever the published decision differs from the
///   prior tick's. This is the right level for watching cwnd evolve in
///   production without drowning in per-tick noise.
///
/// [`label`]: ControlUnit::label
pub struct ControlUnit<C: Controller> {
    label: &'static str,
    controller: C,
    sample_rx: tokio::sync::mpsc::Receiver<Sample>,
    decision_tx: tokio::sync::watch::Sender<Decision>,
    snapshot_tx: tokio::sync::watch::Sender<ControllerSnapshot>,
    tick_interval: std::time::Duration,
}

impl<C: Controller + 'static> ControlUnit<C> {
    /// Build a new control unit. Returns the unit, a receiver for its
    /// decision stream, and a receiver for its snapshot stream. The
    /// initial decision is [`Decision::UNLIMITED`] and the initial
    /// snapshot is [`ControllerSnapshot::default`] until the first tick
    /// fires.
    ///
    /// `label` is a short, stable string that identifies this unit in
    /// log events and in the snapshot registry (e.g. `"meta-src"`,
    /// `"meta-dst"`). Multiple units of the same controller type are
    /// typical, so using only the controller name would make logs
    /// ambiguous.
    pub fn new(
        label: &'static str,
        controller: C,
        sample_rx: tokio::sync::mpsc::Receiver<Sample>,
        tick_interval: std::time::Duration,
    ) -> (
        Self,
        tokio::sync::watch::Receiver<Decision>,
        tokio::sync::watch::Receiver<ControllerSnapshot>,
    ) {
        let (decision_tx, decision_rx) = tokio::sync::watch::channel(Decision::UNLIMITED);
        let (snapshot_tx, snapshot_rx) = tokio::sync::watch::channel(ControllerSnapshot::default());
        (
            Self {
                label,
                controller,
                sample_rx,
                decision_tx,
                snapshot_tx,
                tick_interval,
            },
            decision_rx,
            snapshot_rx,
        )
    }

    /// The label this unit was constructed with, used as the
    /// `unit` field on every emitted tracing event.
    pub fn label(&self) -> &'static str {
        self.label
    }

    /// Spawn the control loop on the current tokio runtime. Returns a
    /// `JoinHandle` that resolves when the sample channel closes.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run())
    }

    /// Run the control loop until the sample channel closes.
    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(self.tick_interval);
        // the first `interval.tick()` resolves immediately (at t=0); use it
        // to publish the controller's initial decision deterministically.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        let initial = self.controller.on_tick(std::time::Instant::now());
        Self::log_tick(self.label, self.controller.name(), &initial, None);
        let _ = self.decision_tx.send(initial);
        let _ = self.snapshot_tx.send(self.controller.snapshot());
        let mut last = initial;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let decision = self.controller.on_tick(std::time::Instant::now());
                    Self::log_tick(self.label, self.controller.name(), &decision, Some(&last));
                    let _ = self.decision_tx.send(decision);
                    let _ = self.snapshot_tx.send(self.controller.snapshot());
                    last = decision;
                }
                sample = self.sample_rx.recv() => {
                    match sample {
                        Some(s) => {
                            self.controller.on_sample(&s);
                            // drain any other immediately-available samples
                            // so a high sample rate doesn't starve the tick.
                            while let Ok(s) = self.sample_rx.try_recv() {
                                self.controller.on_sample(&s);
                            }
                        }
                        None => break,
                    }
                }
            }
        }
        tracing::debug!(
            unit = %self.label,
            controller = %self.controller.name(),
            "control loop exiting: sample channel closed",
        );
    }

    fn log_tick(
        label: &'static str,
        controller: &'static str,
        decision: &Decision,
        previous: Option<&Decision>,
    ) {
        // every tick at trace; only changes at debug, so production logs
        // can stay at debug without drowning in unchanged-cwnd churn.
        tracing::trace!(
            unit = %label,
            controller = %controller,
            max_in_flight = ?decision.max_in_flight,
            rate_per_sec = ?decision.rate_per_sec,
            "control tick",
        );
        if previous.is_none_or(|p| p != decision) {
            tracing::debug!(
                unit = %label,
                controller = %controller,
                max_in_flight = ?decision.max_in_flight,
                rate_per_sec = ?decision.rate_per_sec,
                prev_max_in_flight = ?previous.and_then(|p| p.max_in_flight),
                prev_rate_per_sec = ?previous.and_then(|p| p.rate_per_sec),
                "decision changed",
            );
        }
    }
}

/// A [`SampleSink`] that fans samples out to per-resource bounded MPSC
/// channels, typically each drained by one [`ControlUnit`].
///
/// Built via [`RoutingSinkBuilder`]. When a channel is full, samples are
/// dropped rather than blocking or allocating; the drop count is exposed
/// by [`RoutingSink::dropped_samples`].
pub struct RoutingSink {
    metadata_src: Option<tokio::sync::mpsc::Sender<Sample>>,
    metadata_dst: Option<tokio::sync::mpsc::Sender<Sample>>,
    read: Option<tokio::sync::mpsc::Sender<Sample>>,
    write: Option<tokio::sync::mpsc::Sender<Sample>>,
    dropped: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl RoutingSink {
    /// Cumulative count of samples dropped because the destination channel
    /// was full. Closed-channel drops (receiver went away) are not counted.
    pub fn dropped_samples(&self) -> u64 {
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl SampleSink for RoutingSink {
    fn record(&self, kind: ResourceKind, sample: &Sample) {
        let tx = match kind {
            ResourceKind::Metadata(Side::Source) => self.metadata_src.as_ref(),
            ResourceKind::Metadata(Side::Destination) => self.metadata_dst.as_ref(),
            ResourceKind::DataRead => self.read.as_ref(),
            ResourceKind::DataWrite => self.write.as_ref(),
        };
        if let Some(tx) = tx {
            match tx.try_send(*sample) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    self.dropped
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    // the ControlUnit exited; nothing to do.
                }
            }
        }
    }
}

/// Incrementally opt resources into the routing sink. Each `*_receiver`
/// call registers a channel for the corresponding [`ResourceKind`] and
/// returns the receiver the caller must hand to a [`ControlUnit`].
pub struct RoutingSinkBuilder {
    metadata_src: Option<tokio::sync::mpsc::Sender<Sample>>,
    metadata_dst: Option<tokio::sync::mpsc::Sender<Sample>>,
    read: Option<tokio::sync::mpsc::Sender<Sample>>,
    write: Option<tokio::sync::mpsc::Sender<Sample>>,
    capacity: usize,
    dropped: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl Default for RoutingSinkBuilder {
    fn default() -> Self {
        Self {
            metadata_src: None,
            metadata_dst: None,
            read: None,
            write: None,
            capacity: DEFAULT_CHANNEL_CAPACITY,
            dropped: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

impl RoutingSinkBuilder {
    pub fn new() -> Self {
        Self::default()
    }
    /// Override the per-channel capacity. Must be at least 1.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity.max(1);
        self
    }
    /// Register a channel for per-file metadata samples on the given
    /// [`Side`] and return its receiver. Covers single metadata
    /// syscalls — both lookups (`stat`) and mutations (`mkdir`,
    /// `unlink`, `hard_link`, etc.).
    pub fn metadata_receiver(&mut self, side: Side) -> tokio::sync::mpsc::Receiver<Sample> {
        let (tx, rx) = tokio::sync::mpsc::channel(self.capacity);
        match side {
            Side::Source => self.metadata_src = Some(tx),
            Side::Destination => self.metadata_dst = Some(tx),
        }
        rx
    }
    /// Register a channel for read-throughput samples and return its receiver.
    pub fn read_receiver(&mut self) -> tokio::sync::mpsc::Receiver<Sample> {
        let (tx, rx) = tokio::sync::mpsc::channel(self.capacity);
        self.read = Some(tx);
        rx
    }
    /// Register a channel for write-throughput samples and return its receiver.
    pub fn write_receiver(&mut self) -> tokio::sync::mpsc::Receiver<Sample> {
        let (tx, rx) = tokio::sync::mpsc::channel(self.capacity);
        self.write = Some(tx);
        rx
    }
    pub fn build(self) -> RoutingSink {
        RoutingSink {
            metadata_src: self.metadata_src,
            metadata_dst: self.metadata_dst,
            read: self.read,
            write: self.write,
            dropped: self.dropped,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::Outcome;
    use crate::measurement::{Probe, clear_sample_sink, install_sample_sink};
    use crate::{FixedController, NoopController};

    fn make_sample(latency_ms: u64) -> Sample {
        let start = std::time::Instant::now();
        Sample {
            started_at: start,
            completed_at: start + std::time::Duration::from_millis(latency_ms),
            bytes: 0,
            outcome: Outcome::Ok,
        }
    }

    #[tokio::test]
    async fn control_unit_publishes_initial_decision_on_spawn() {
        let (_tx, rx) = tokio::sync::mpsc::channel::<Sample>(32);
        let controller = FixedController::with_concurrency(42);
        let (unit, mut decision_rx, _snapshot_rx) =
            ControlUnit::new("test", controller, rx, std::time::Duration::from_millis(10));
        unit.spawn();
        // initial decision published by the first tick
        decision_rx
            .changed()
            .await
            .expect("initial decision delivered");
        let decision = *decision_rx.borrow();
        assert_eq!(decision.max_in_flight, Some(42));
    }

    #[tokio::test]
    async fn control_unit_feeds_samples_through_to_controller() {
        // a controller that just counts samples
        struct CountingController {
            count: std::sync::Arc<std::sync::atomic::AtomicU64>,
        }
        impl Controller for CountingController {
            fn on_sample(&mut self, _s: &Sample) {
                self.count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            fn on_tick(&mut self, _now: std::time::Instant) -> Decision {
                Decision::UNLIMITED
            }
            fn name(&self) -> &'static str {
                "counting"
            }
        }
        let count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (tx, rx) = tokio::sync::mpsc::channel::<Sample>(32);
        let controller = CountingController {
            count: count.clone(),
        };
        let (unit, _decision_rx, _snapshot_rx) = ControlUnit::new(
            "test",
            controller,
            rx,
            std::time::Duration::from_millis(100),
        );
        let handle = unit.spawn();
        for _ in 0..5 {
            tx.send(make_sample(1)).await.expect("send sample");
        }
        // closing the sender terminates the control loop
        drop(tx);
        handle.await.expect("control loop exits cleanly");
        assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn routing_sink_dispatches_by_resource_kind() {
        let mut builder = RoutingSinkBuilder::new();
        let mut meta_rx = builder.metadata_receiver(Side::Source);
        let mut read_rx = builder.read_receiver();
        let sink = builder.build();
        // metadata sample reaches metadata_rx
        sink.record(ResourceKind::Metadata(Side::Source), &make_sample(2));
        let s = meta_rx.recv().await.expect("metadata sample delivered");
        assert_eq!(s.bytes, 0);
        // read sample reaches read_rx
        sink.record(ResourceKind::DataRead, &make_sample(2));
        let s = read_rx.recv().await.expect("read sample delivered");
        assert_eq!(s.bytes, 0);
        // write sample has no registered receiver; it is silently dropped
        sink.record(ResourceKind::DataWrite, &make_sample(2));
        // no assertion needed — the drop must not panic or block
    }

    #[tokio::test]
    async fn routing_sink_counts_dropped_samples_when_channel_is_full() {
        // tight capacity + no receiver draining → excess samples are dropped.
        let mut builder = RoutingSinkBuilder::new().with_capacity(2);
        let _meta_rx = builder.metadata_receiver(Side::Source);
        let sink = builder.build();
        for _ in 0..5 {
            sink.record(ResourceKind::Metadata(Side::Source), &make_sample(1));
        }
        // first 2 fit in the channel buffer; remaining 3 are dropped.
        assert_eq!(sink.dropped_samples(), 3);
    }

    #[tokio::test]
    async fn routing_sink_integrates_with_global_probe_api() {
        // guard the process-wide SampleSink mutation so this test can't race
        // with measurement::tests or other sink-touching tests under
        // `cargo test`'s threaded runner.
        let _guard = crate::measurement::SINK_GUARD.lock().await;
        let mut builder = RoutingSinkBuilder::new();
        let mut meta_rx = builder.metadata_receiver(Side::Source);
        let sink = builder.build();
        install_sample_sink(std::sync::Arc::new(sink));
        Probe::start_metadata(Side::Source).complete_ok(0);
        let s = meta_rx.recv().await.expect("sample flowed through");
        assert_eq!(s.bytes, 0);
        clear_sample_sink();
    }

    #[tokio::test]
    async fn control_unit_exits_when_all_senders_dropped() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Sample>(32);
        let (unit, _decision_rx, _snapshot_rx) = ControlUnit::new(
            "test",
            NoopController::new(),
            rx,
            std::time::Duration::from_millis(10),
        );
        let handle = unit.spawn();
        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("control loop exits within timeout")
            .expect("control loop joins without panic");
    }
}
