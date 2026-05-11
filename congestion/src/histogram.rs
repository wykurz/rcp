//! Per-controller HDR latency histograms.
//!
//! A `HistogramAccumulator` wraps a single `hdrhistogram::Histogram<u64>`
//! configured for filesystem-metadata-syscall latencies (1µs to 1h, 3 sig
//! figures). It records `Duration` values in microseconds and supports a
//! "snapshot and reset" operation so a logger task can periodically harvest
//! distributions for offline reconstruction without disturbing the live
//! sample path.
//!
//! HDR is mergeable: distinct snapshots over disjoint time windows can be
//! folded together to reconstruct any window's distribution. That's the
//! property the offline log reader relies on to materialize "current"
//! (1s) and "baseline" (10s) windows from a single per-bucket stream.

/// Minimum value representable in the histogram, in microseconds.
pub const HDR_LOWEST_DISCERNIBLE_MICROS: u64 = 1;
/// Maximum value representable in the histogram, in microseconds (1 hour).
pub const HDR_HIGHEST_TRACKABLE_MICROS: u64 = 3_600_000_000;
/// Significant figures of precision tracked by the histogram.
pub const HDR_SIGNIFICANT_FIGURES: u8 = 3;

/// Per-controller HDR latency accumulator.
///
/// Records sample latencies into an HDR histogram with fixed bounds and
/// precision (see the `HDR_*` constants). `record` is the hot-path
/// accessor; `snapshot_and_reset` is called by the logger task at its
/// configured interval and returns a clone of the histogram while
/// resetting the local one to empty.
pub struct HistogramAccumulator {
    histogram: hdrhistogram::Histogram<u64>,
}

impl HistogramAccumulator {
    /// Construct a fresh empty accumulator using the workspace HDR settings.
    #[must_use]
    pub fn new() -> Self {
        // Histogram::new_with_bounds returns Result; the configured bounds
        // are constants we control, so panicking on failure is correct — a
        // failure here is a build-time bug, not a runtime concern.
        let histogram = hdrhistogram::Histogram::<u64>::new_with_bounds(
            HDR_LOWEST_DISCERNIBLE_MICROS,
            HDR_HIGHEST_TRACKABLE_MICROS,
            HDR_SIGNIFICANT_FIGURES,
        )
        .expect("HDR bounds are valid by construction");
        Self { histogram }
    }

    /// Record one sample latency.
    ///
    /// Sub-microsecond durations round up to 1 µs (the lowest discernible
    /// value); samples longer than 1 hour saturate at the upper bound
    /// rather than being dropped. Both clamps preserve sample count, so
    /// the histogram's `len()` always matches the number of `record`
    /// calls.
    pub fn record(&mut self, latency: std::time::Duration) {
        let micros = u64::try_from(latency.as_micros())
            .unwrap_or(HDR_HIGHEST_TRACKABLE_MICROS)
            .clamp(HDR_LOWEST_DISCERNIBLE_MICROS, HDR_HIGHEST_TRACKABLE_MICROS);
        self.histogram.saturating_record(micros);
    }

    /// Take a snapshot of the current histogram and reset the accumulator
    /// to empty. The returned histogram owns its data and can be
    /// serialized or merged independently.
    #[must_use]
    pub fn snapshot_and_reset(&mut self) -> hdrhistogram::Histogram<u64> {
        let snapshot = self.histogram.clone();
        self.histogram.reset();
        snapshot
    }
}

impl Default for HistogramAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_snapshot_returns_count() {
        let mut acc = HistogramAccumulator::new();
        acc.record(std::time::Duration::from_micros(100));
        acc.record(std::time::Duration::from_micros(200));
        acc.record(std::time::Duration::from_micros(300));
        let snap = acc.snapshot_and_reset();
        assert_eq!(snap.len(), 3);
    }

    #[test]
    fn snapshot_and_reset_clears_underlying_histogram() {
        // After snapshot_and_reset, the accumulator is empty — a follow-up
        // record only contributes one sample to the next snapshot.
        let mut acc = HistogramAccumulator::new();
        acc.record(std::time::Duration::from_micros(50));
        let _ = acc.snapshot_and_reset();
        acc.record(std::time::Duration::from_micros(75));
        let snap = acc.snapshot_and_reset();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap.value_at_percentile(50.0), 75);
    }

    #[test]
    fn record_clamps_zero_duration_to_one_micro() {
        // A zero-duration sample (possible when Instant::now() resolution
        // groups back-to-back probes) lands at the lowest discernible
        // value rather than silently being dropped or panicking on the
        // HDR's strict-positive precondition.
        let mut acc = HistogramAccumulator::new();
        acc.record(std::time::Duration::ZERO);
        let snap = acc.snapshot_and_reset();
        assert_eq!(snap.len(), 1);
        assert_eq!(
            snap.value_at_percentile(50.0),
            HDR_LOWEST_DISCERNIBLE_MICROS
        );
    }

    #[test]
    fn record_saturates_at_upper_bound() {
        // A sample longer than the configured upper bound (1h) saturates
        // rather than panicking. HDR's `record` would otherwise return Err
        // on out-of-range values.
        let mut acc = HistogramAccumulator::new();
        acc.record(std::time::Duration::from_secs(7200)); // 2 hours
        let snap = acc.snapshot_and_reset();
        assert_eq!(snap.len(), 1);
        let p50 = snap.value_at_percentile(50.0);
        // HDR bucket boundaries can round slightly above the nominal trackable
        // max (3-sig-fig buckets at 3.6B are ~0.8M wide). The important
        // invariant is that we didn't panic and that the value is close to the
        // configured ceiling, not that it's strictly below it.
        let tolerance = HDR_HIGHEST_TRACKABLE_MICROS / 1000; // 0.1% tolerance
        assert!(
            p50 >= HDR_HIGHEST_TRACKABLE_MICROS - 1_000_000, // allow up to 1s of HDR quantization below the nominal max
            "p50={p50} too far below max"
        );
        assert!(
            p50 <= HDR_HIGHEST_TRACKABLE_MICROS + tolerance,
            "p50={p50} too far above max"
        );
    }

    #[test]
    fn record_converts_nanoseconds_to_microseconds() {
        // Sub-microsecond samples truncate to the micro floor (1 µs) rather
        // than being binned at zero.
        let mut acc = HistogramAccumulator::new();
        acc.record(std::time::Duration::from_nanos(500)); // 0.5 µs → 1 µs
        let snap = acc.snapshot_and_reset();
        assert_eq!(snap.value_at_percentile(50.0), 1);
    }
}
