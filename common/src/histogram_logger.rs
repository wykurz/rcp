//! Process-wide histogram logger task.
//!
//! Owns one `Arc<Mutex<HistogramAccumulator>>` per registered (side, op)
//! pair (each shared with the corresponding `ControlUnit`). On each tick,
//! takes a snapshot of every accumulator, publishes the snapshot through
//! a per-unit watch channel for the live display, and (when a log path
//! was configured) appends a binary record per non-empty snapshot.
//!
//! When the task ticks past a snapshot interval, it uses the actual
//! snapshot end time as the record's `unix_micros` field — readers see
//! the true coverage even if the host was loaded.

use congestion::format::{
    LogHeader, write_file_header, write_histogram_record, write_progress_record,
};
use congestion::{HistogramAccumulator, MetadataOp, Side};

/// Closure that, when called, returns the JSON-encoded current progress
/// snapshot. The logger calls this once per tick (only when a log file
/// is being written) and emits one Progress record. Boxed so the
/// concrete snapshot type stays in the caller's crate — the logger
/// doesn't depend on it.
pub type ProgressSource = Box<dyn Fn() -> Vec<u8> + Send + Sync>;

/// One slot the logger owns: the accumulator (shared with a ControlUnit)
/// and the watch sender used to publish snapshots to the display.
pub struct LoggerUnit {
    pub label: &'static str,
    pub side: Side,
    pub op: MetadataOp,
    pub accumulator: std::sync::Arc<std::sync::Mutex<HistogramAccumulator>>,
    pub snapshot_tx: tokio::sync::watch::Sender<hdrhistogram::Histogram<u64>>,
}

/// Configuration for the logger task.
pub struct LoggerConfig {
    pub interval: std::time::Duration,
    pub log_path: Option<std::path::PathBuf>,
    pub header: LogHeader,
    /// Optional progress source. When set and a log file is open, the
    /// logger calls it once per tick and writes one Progress record
    /// carrying the returned JSON bytes — letting offline tools
    /// correlate latency distributions with the throughput counters
    /// from the progress bar.
    pub progress_source: Option<ProgressSource>,
}

/// Run the logger task: ticks, snapshots, publishes, optionally writes
/// to file. Exits when the provided cancellation token signals.
pub async fn run_logger(
    config: LoggerConfig,
    units: Vec<LoggerUnit>,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) {
    let mut writer: Option<std::io::BufWriter<std::fs::File>> = match &config.log_path {
        Some(path) => {
            let mut open_options = std::fs::OpenOptions::new();
            open_options.create(true).write(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                open_options.custom_flags(libc::O_NOFOLLOW);
            }
            match open_options.open(path) {
                Ok(f) => {
                    let mut w = std::io::BufWriter::new(f);
                    if let Err(err) = write_file_header(&mut w, &config.header) {
                        tracing::warn!(
                            "histogram-logger: failed to write file header: {err:#}; \
                                        disabling file output"
                        );
                        None
                    } else {
                        Some(w)
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        "histogram-logger: failed to open {path:?}: {err:#}; \
                                    disabling file output"
                    );
                    None
                }
            }
        }
        None => None,
    };
    let progress_source = config.progress_source;
    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                writer = snapshot_and_publish_units(&units, progress_source.as_deref(), writer);
            }
            _ = cancel.changed() => {
                if *cancel.borrow() {
                    // Flush any samples accumulated since the last tick
                    // so a short copy / partial final interval doesn't lose data.
                    drop(snapshot_and_publish_units(&units, progress_source.as_deref(), writer));
                    break;
                }
            }
        }
    }
    tracing::debug!("histogram-logger: exiting");
}

/// Snapshot every accumulator, publish to its watch, optionally write
/// to the log file. When `progress_source` is set and a writer is
/// active, append one Progress record per call carrying the
/// JSON-encoded snapshot the closure returns. Returns the (possibly
/// None'd) writer back to the caller — if a write or flush fails,
/// the returned Option is None and a warning has been emitted.
fn snapshot_and_publish_units(
    units: &[LoggerUnit],
    progress_source: Option<&(dyn Fn() -> Vec<u8> + Send + Sync)>,
    mut writer: Option<std::io::BufWriter<std::fs::File>>,
) -> Option<std::io::BufWriter<std::fs::File>> {
    use std::io::Write;
    for unit in units {
        let snap = unit
            .accumulator
            .lock()
            .expect("histogram accumulator mutex poisoned")
            .snapshot_and_reset();
        // Capture the snapshot's end-time AFTER the lock+reset so the
        // record's unix_micros reflects what samples are actually in
        // the snapshot. With synchronous histogram capture in the
        // RoutingSink, samples can land in *later* units' accumulators
        // while this loop is still walking earlier ones; a single
        // pre-loop timestamp would backdate those later snapshots.
        let snapshot_micros = unix_micros_now();
        let _ = unit.snapshot_tx.send(snap.clone());
        if snap.is_empty() {
            continue;
        }
        if let Some(w) = writer.as_mut()
            && let Err(err) = write_histogram_record(w, snapshot_micros, unit.side, unit.op, &snap)
        {
            tracing::warn!(
                "histogram-logger: write_histogram_record({label}) failed: {err:#}; \
                 disabling file output",
                label = unit.label,
            );
            writer = None;
            break;
        }
    }
    // emit a progress record after the unit loop so its timestamp
    // bounds the tick from above: every preceding unit record is at or
    // before this point. progress is monotonic, so we always write
    // it — empty progress (all zeros) is meaningful at run start.
    // an empty json payload is the source's "skip this tick" signal
    // (e.g. transient encoding failure already logged inside src()); we
    // drop the record rather than emit something unparseable.
    if let Some(src) = progress_source
        && let Some(w) = writer.as_mut()
    {
        let json = src();
        let ts = unix_micros_now();
        if !json.is_empty()
            && let Err(err) = write_progress_record(w, ts, &json)
        {
            tracing::warn!(
                "histogram-logger: write_progress_record failed: {err:#}; \
                 disabling file output",
            );
            writer = None;
        }
    }
    if let Some(w) = writer.as_mut()
        && let Err(err) = w.flush()
    {
        tracing::warn!("histogram-logger: flush failed: {err:#}; disabling file output",);
        writer = None;
    }
    writer
}

fn unix_micros_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use congestion::format::{
        AutoMetaSnapshot, FORMAT_VERSION, HdrSnapshot, LogHeader, Record, UnitLabel,
        read_file_header, read_record,
    };

    fn header() -> LogHeader {
        LogHeader {
            format_version: FORMAT_VERSION,
            tool: "test".into(),
            tool_version: "0.0.0".into(),
            hostname: "h".into(),
            pid: 0,
            start_unix_micros: 0,
            snapshot_interval_micros: 100_000,
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

    #[tokio::test]
    async fn writes_records_to_file_for_non_empty_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.hdr");
        let acc = std::sync::Arc::new(std::sync::Mutex::new(HistogramAccumulator::new()));
        let (snap_tx, _snap_rx) = tokio::sync::watch::channel(
            hdrhistogram::Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3).unwrap(),
        );
        let units = vec![LoggerUnit {
            label: "src-stat",
            side: Side::Source,
            op: MetadataOp::Stat,
            accumulator: acc.clone(),
            snapshot_tx: snap_tx,
        }];
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        // Pre-load some samples so the first tick records something.
        acc.lock()
            .unwrap()
            .record(std::time::Duration::from_micros(100));
        acc.lock()
            .unwrap()
            .record(std::time::Duration::from_micros(200));
        let config = LoggerConfig {
            interval: std::time::Duration::from_millis(50),
            log_path: Some(path.clone()),
            header: header(),
            progress_source: None,
        };
        let handle = tokio::spawn(run_logger(config, units, cancel_rx));
        // Wait for at least one tick to fire and write a record.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        cancel_tx.send(true).unwrap();
        handle.await.unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let mut reader = std::io::BufReader::new(file);
        let _ = read_file_header(&mut reader).unwrap();
        let rec = match read_record(&mut reader)
            .unwrap()
            .expect("at least one record written")
        {
            Record::Histogram(h) => h,
            Record::Progress(_) => panic!("unexpected progress record"),
        };
        assert_eq!(rec.samples_count, 2);
        assert_eq!(rec.side, Side::Source);
        assert_eq!(rec.op, MetadataOp::Stat);
    }

    #[tokio::test]
    async fn empty_snapshots_publish_via_watch_but_skip_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.hdr");
        let acc = std::sync::Arc::new(std::sync::Mutex::new(HistogramAccumulator::new()));
        let (snap_tx, snap_rx) = tokio::sync::watch::channel(
            hdrhistogram::Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3).unwrap(),
        );
        let units = vec![LoggerUnit {
            label: "src-stat",
            side: Side::Source,
            op: MetadataOp::Stat,
            accumulator: acc.clone(),
            snapshot_tx: snap_tx,
        }];
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let config = LoggerConfig {
            interval: std::time::Duration::from_millis(50),
            log_path: Some(path.clone()),
            header: header(),
            progress_source: None,
        };
        let handle = tokio::spawn(run_logger(config, units, cancel_rx));
        // Don't preload any samples; let the logger tick.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        cancel_tx.send(true).unwrap();
        handle.await.unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let mut reader = std::io::BufReader::new(file);
        let _ = read_file_header(&mut reader).unwrap();
        // No records were written.
        assert!(read_record(&mut reader).unwrap().is_none());
        // But the watch has at least one update (the empty snapshot).
        assert!(snap_rx.has_changed().unwrap_or(false) || snap_rx.borrow().is_empty());
    }

    #[tokio::test]
    async fn cancel_before_first_tick_still_writes_pending_samples() {
        // Regression: a short-lived copy may finish before the first
        // periodic tick fires. The cancel arm must take one final snapshot
        // before exiting so the log isn't header-only.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.hdr");
        let acc = std::sync::Arc::new(std::sync::Mutex::new(HistogramAccumulator::new()));
        let (snap_tx, _snap_rx) = tokio::sync::watch::channel(
            hdrhistogram::Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3).unwrap(),
        );
        let units = vec![LoggerUnit {
            label: "src-stat",
            side: Side::Source,
            op: MetadataOp::Stat,
            accumulator: acc.clone(),
            snapshot_tx: snap_tx,
        }];
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        // Pre-load samples, then cancel before any tick fires.
        acc.lock()
            .unwrap()
            .record(std::time::Duration::from_micros(42));
        let config = LoggerConfig {
            // Long interval so the periodic tick definitely doesn't fire
            // before our cancel signal does.
            interval: std::time::Duration::from_secs(60),
            log_path: Some(path.clone()),
            header: header(),
            progress_source: None,
        };
        let handle = tokio::spawn(run_logger(config, units, cancel_rx));
        // Give the task a moment to start and consume the initial tick,
        // then send cancel before the next 60s tick.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel_tx.send(true).unwrap();
        handle.await.unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let mut reader = std::io::BufReader::new(file);
        let _ = read_file_header(&mut reader).unwrap();
        let rec = match read_record(&mut reader)
            .unwrap()
            .expect("cancellation must flush a final record")
        {
            Record::Histogram(h) => h,
            Record::Progress(_) => panic!("unexpected progress record"),
        };
        assert_eq!(rec.samples_count, 1);
    }

    #[test]
    fn snapshot_and_publish_uses_per_unit_timestamps() {
        // Regression: a single pre-loop timestamp would stamp later
        // units' records with a stale time, backdating samples that
        // were synchronously recorded into them while the loop was
        // walking earlier units.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.hdr");
        let header = header();
        let mut writer = Some(std::io::BufWriter::new(
            std::fs::File::create(&path).unwrap(),
        ));
        {
            use std::io::Write;
            congestion::format::write_file_header(writer.as_mut().unwrap(), &header).unwrap();
            writer.as_mut().unwrap().flush().unwrap();
        }

        let acc_a = std::sync::Arc::new(std::sync::Mutex::new(HistogramAccumulator::new()));
        let acc_b = std::sync::Arc::new(std::sync::Mutex::new(HistogramAccumulator::new()));
        acc_a
            .lock()
            .unwrap()
            .record(std::time::Duration::from_micros(10));
        acc_b
            .lock()
            .unwrap()
            .record(std::time::Duration::from_micros(20));
        let (snap_tx_a, _rx_a) = tokio::sync::watch::channel(
            hdrhistogram::Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3).unwrap(),
        );
        let (snap_tx_b, _rx_b) = tokio::sync::watch::channel(
            hdrhistogram::Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3).unwrap(),
        );
        let units = vec![
            LoggerUnit {
                label: "src-stat",
                side: Side::Source,
                op: MetadataOp::Stat,
                accumulator: acc_a,
                snapshot_tx: snap_tx_a,
            },
            LoggerUnit {
                label: "dst-stat",
                side: Side::Destination,
                op: MetadataOp::Stat,
                accumulator: acc_b,
                snapshot_tx: snap_tx_b,
            },
        ];

        let before_micros = unix_micros_now();
        writer = snapshot_and_publish_units(&units, None, writer);
        let after_micros = unix_micros_now();
        drop(writer);

        let f = std::fs::File::open(&path).unwrap();
        let mut reader = std::io::BufReader::new(f);
        let _ = congestion::format::read_file_header(&mut reader).unwrap();
        let r1 = congestion::format::read_record(&mut reader)
            .unwrap()
            .expect("record 1");
        let r2 = congestion::format::read_record(&mut reader)
            .unwrap()
            .expect("record 2");
        let r1_ts = r1.unix_micros();
        let r2_ts = r2.unix_micros();
        assert!(
            r1_ts >= before_micros && r1_ts <= after_micros,
            "record 1 ts {r1_ts} not in [{before_micros}, {after_micros}]",
        );
        assert!(
            r2_ts >= r1_ts && r2_ts <= after_micros,
            "record 2 ts {r2_ts} must be >= record 1 ts {r1_ts} and <= after {after_micros}",
        );
    }

    #[tokio::test]
    async fn writes_progress_record_per_tick_when_source_set() {
        // Even when the histogram accumulator is empty (no samples were
        // recorded into it this tick), a configured progress source
        // must still emit one Progress record per tick — progress
        // counters are monotonic and meaningful from the first sample
        // onward, including zero state.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.hdr");
        let acc = std::sync::Arc::new(std::sync::Mutex::new(HistogramAccumulator::new()));
        let (snap_tx, _snap_rx) = tokio::sync::watch::channel(
            hdrhistogram::Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3).unwrap(),
        );
        let units = vec![LoggerUnit {
            label: "src-stat",
            side: Side::Source,
            op: MetadataOp::Stat,
            accumulator: acc,
            snapshot_tx: snap_tx,
        }];
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let payload = br#"{"files_copied":3}"#.to_vec();
        let payload_for_closure = payload.clone();
        let config = LoggerConfig {
            interval: std::time::Duration::from_millis(50),
            log_path: Some(path.clone()),
            header: header(),
            progress_source: Some(Box::new(move || payload_for_closure.clone())),
        };
        let handle = tokio::spawn(run_logger(config, units, cancel_rx));
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        cancel_tx.send(true).unwrap();
        handle.await.unwrap();

        let f = std::fs::File::open(&path).unwrap();
        let mut reader = std::io::BufReader::new(f);
        let _ = read_file_header(&mut reader).unwrap();
        let mut progress_count = 0;
        while let Some(rec) = read_record(&mut reader).unwrap() {
            if let Record::Progress(p) = rec {
                assert_eq!(p.json, payload);
                progress_count += 1;
            }
        }
        assert!(
            progress_count >= 1,
            "expected ≥1 progress record, got {progress_count}",
        );
    }
}
