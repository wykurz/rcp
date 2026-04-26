//! Test utilities for asserting on emitted samples.
//!
//! These types are used by both this crate's tests and downstream integration
//! tests that want to verify their own `Probe` placements.

use crate::controller::Sample;
use crate::measurement::{ResourceKind, SampleSink, Side};

/// A sink that retains every sample it receives, bucketed by [`ResourceKind`]
/// (and by [`Side`] for metadata samples).
///
/// Thread-safe and intended for tests. For high-rate production use a real
/// control-loop sink instead.
#[derive(Default)]
pub struct CollectingSink {
    inner: std::sync::Mutex<CollectingInner>,
}

#[derive(Default)]
struct CollectingInner {
    metadata_src: Vec<Sample>,
    metadata_dst: Vec<Sample>,
    read: Vec<Sample>,
    write: Vec<Sample>,
}

impl CollectingSink {
    pub fn new() -> Self {
        Self::default()
    }
    /// Total number of metadata samples recorded across both sides.
    pub fn metadata_count(&self) -> usize {
        let inner = self.inner.lock().expect("collecting sink mutex poisoned");
        inner.metadata_src.len() + inner.metadata_dst.len()
    }
    /// Number of metadata samples recorded for the given [`Side`].
    pub fn metadata_count_for(&self, side: Side) -> usize {
        let inner = self.inner.lock().expect("collecting sink mutex poisoned");
        match side {
            Side::Source => inner.metadata_src.len(),
            Side::Destination => inner.metadata_dst.len(),
        }
    }
    /// Number of read samples recorded.
    pub fn read_count(&self) -> usize {
        self.inner
            .lock()
            .expect("collecting sink mutex poisoned")
            .read
            .len()
    }
    /// Number of write samples recorded.
    pub fn write_count(&self) -> usize {
        self.inner
            .lock()
            .expect("collecting sink mutex poisoned")
            .write
            .len()
    }
    /// Snapshot of all metadata samples recorded so far, both sides combined.
    pub fn metadata_samples(&self) -> Vec<Sample> {
        let inner = self.inner.lock().expect("collecting sink mutex poisoned");
        let mut out = inner.metadata_src.clone();
        out.extend(inner.metadata_dst.iter().copied());
        out
    }
    /// Snapshot of all metadata samples recorded so far for the given [`Side`].
    pub fn metadata_samples_for(&self, side: Side) -> Vec<Sample> {
        let inner = self.inner.lock().expect("collecting sink mutex poisoned");
        match side {
            Side::Source => inner.metadata_src.clone(),
            Side::Destination => inner.metadata_dst.clone(),
        }
    }
    /// Snapshot of all read samples recorded so far.
    pub fn read_samples(&self) -> Vec<Sample> {
        self.inner
            .lock()
            .expect("collecting sink mutex poisoned")
            .read
            .clone()
    }
    /// Snapshot of all write samples recorded so far.
    pub fn write_samples(&self) -> Vec<Sample> {
        self.inner
            .lock()
            .expect("collecting sink mutex poisoned")
            .write
            .clone()
    }
    /// Forget everything recorded so far. Useful between test phases.
    pub fn reset(&self) {
        let mut inner = self.inner.lock().expect("collecting sink mutex poisoned");
        inner.metadata_src.clear();
        inner.metadata_dst.clear();
        inner.read.clear();
        inner.write.clear();
    }
}

impl SampleSink for CollectingSink {
    fn record(&self, kind: ResourceKind, sample: &Sample) {
        let mut inner = self.inner.lock().expect("collecting sink mutex poisoned");
        match kind {
            ResourceKind::Metadata(Side::Source) => inner.metadata_src.push(*sample),
            ResourceKind::Metadata(Side::Destination) => inner.metadata_dst.push(*sample),
            ResourceKind::DataRead => inner.read.push(*sample),
            ResourceKind::DataWrite => inner.write.push(*sample),
        }
    }
}
