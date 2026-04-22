//! Test utilities for asserting on emitted samples.
//!
//! These types are used by both this crate's tests and downstream integration
//! tests that want to verify their own `Probe` placements.

use crate::controller::Sample;
use crate::measurement::{ResourceKind, SampleSink};

/// A sink that retains every sample it receives, bucketed by [`ResourceKind`].
///
/// Thread-safe and intended for tests. For high-rate production use a real
/// control-loop sink instead.
#[derive(Default)]
pub struct CollectingSink {
    inner: std::sync::Mutex<CollectingInner>,
}

#[derive(Default)]
struct CollectingInner {
    metadata: Vec<Sample>,
    read: Vec<Sample>,
    write: Vec<Sample>,
}

impl CollectingSink {
    pub fn new() -> Self {
        Self::default()
    }
    /// Number of metadata samples recorded.
    pub fn metadata_count(&self) -> usize {
        self.inner
            .lock()
            .expect("collecting sink mutex poisoned")
            .metadata
            .len()
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
    /// Snapshot of all metadata samples recorded so far.
    pub fn metadata_samples(&self) -> Vec<Sample> {
        self.inner
            .lock()
            .expect("collecting sink mutex poisoned")
            .metadata
            .clone()
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
        inner.metadata.clear();
        inner.read.clear();
        inner.write.clear();
    }
}

impl SampleSink for CollectingSink {
    fn record(&self, kind: ResourceKind, sample: &Sample) {
        let mut inner = self.inner.lock().expect("collecting sink mutex poisoned");
        match kind {
            ResourceKind::MetadataOps => inner.metadata.push(*sample),
            ResourceKind::DataRead => inner.read.push(*sample),
            ResourceKind::DataWrite => inner.write.push(*sample),
        }
    }
}
