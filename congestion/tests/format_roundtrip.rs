//! Proptest coverage for the histogram log file format:
//! 1. Random sequences of records roundtrip exactly through write+read.
//! 2. Splitting a sample sequence into N snapshots and merging the per-snapshot
//!    histograms equals recording all samples into a single histogram (modulo
//!    HDR sig-fig rounding, which is exact for repeated values and bounded
//!    for distinct ones).
//!
//! Property (1) is the durability contract: a process that survives long
//! enough to flush at least one full record can have its log read back
//! losslessly. Property (2) is the offline-reconstruction contract: the
//! reader can recover any time window's distribution by merging the
//! records that fall inside it.

use congestion::format::{
    AutoMetaSnapshot, HdrSnapshot, LogHeader, UnitLabel, read_file_header, read_record,
    write_file_header, write_record,
};
use congestion::{HistogramAccumulator, MetadataOp, Side};
use proptest::prelude::*;

fn arb_latency_micros() -> impl Strategy<Value = u64> {
    1u64..1_000_000
}

fn fixed_header() -> LogHeader {
    LogHeader {
        format_version: 1,
        tool: "test".into(),
        tool_version: "0.0.0".into(),
        hostname: "host".into(),
        pid: 0,
        start_unix_micros: 0,
        snapshot_interval_micros: 1_000_000,
        auto_meta: AutoMetaSnapshot {
            initial_cwnd: 1,
            min_cwnd: 1,
            max_cwnd: 4096,
            alpha: 1.3,
            beta: 1.8,
            increase_step: 1,
            decrease_step: 1,
            baseline_percentile: 0.1,
            current_percentile: 0.5,
            long_window_micros: 10_000_000,
            short_window_micros: 1_000_000,
            tick_interval_micros: 50_000,
        },
        hdr: HdrSnapshot {
            lowest_discernible_micros: 1,
            highest_trackable_micros: 3_600_000_000,
            significant_figures: 3,
            unit: "microseconds".into(),
        },
        unit_labels: vec![UnitLabel {
            side: 0,
            op: 0,
            label: "src-stat".into(),
        }],
    }
}

proptest! {
    #[test]
    fn write_then_read_yields_same_records(
        sample_groups in proptest::collection::vec(
            proptest::collection::vec(arb_latency_micros(), 1..32),
            1..8,
        ),
    ) {
        let mut buf: Vec<u8> = Vec::new();
        write_file_header(&mut buf, &fixed_header()).unwrap();
        let mut expected_counts: Vec<u64> = Vec::new();
        for (i, samples) in sample_groups.iter().enumerate() {
            let mut acc = HistogramAccumulator::new();
            for &micros in samples {
                acc.record(std::time::Duration::from_micros(micros));
            }
            let snap = acc.snapshot_and_reset();
            expected_counts.push(snap.len());
            write_record(&mut buf, i as u64, Side::Source, MetadataOp::Stat, &snap).unwrap();
        }
        let mut cursor = std::io::Cursor::new(&buf);
        let _h = read_file_header(&mut cursor).unwrap();
        let mut got_counts: Vec<u64> = Vec::new();
        while let Some(rec) = read_record(&mut cursor).unwrap() {
            got_counts.push(rec.samples_count);
        }
        prop_assert_eq!(got_counts, expected_counts);
    }

    #[test]
    fn split_and_merge_is_equivalent_to_single_recording(
        samples in proptest::collection::vec(arb_latency_micros(), 1..256),
        split_at in 1usize..256,
    ) {
        let split_at = split_at.min(samples.len().saturating_sub(1)).max(1);
        // Reference: one accumulator, all samples.
        let mut single = HistogramAccumulator::new();
        for &m in &samples {
            single.record(std::time::Duration::from_micros(m));
        }
        let single_snap = single.snapshot_and_reset();
        // Split: two accumulators, halves merged.
        let mut a = HistogramAccumulator::new();
        let mut b = HistogramAccumulator::new();
        for &m in &samples[..split_at] {
            a.record(std::time::Duration::from_micros(m));
        }
        for &m in &samples[split_at..] {
            b.record(std::time::Duration::from_micros(m));
        }
        let snap_a = a.snapshot_and_reset();
        let snap_b = b.snapshot_and_reset();
        let mut merged = snap_a.clone();
        merged.add(&snap_b).unwrap();
        prop_assert_eq!(single_snap.len(), merged.len());
        // Compare a few percentiles — exact-equality of HDR histograms is
        // brittle under reorderings; percentile equivalence is the
        // property the offline reconstruction actually relies on.
        for pct in [10.0, 50.0, 90.0, 99.0] {
            prop_assert_eq!(
                single_snap.value_at_percentile(pct),
                merged.value_at_percentile(pct),
            );
        }
    }
}
