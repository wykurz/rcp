//! End-to-end: spawn auto-meta with histogram logging, generate a few
//! probes, terminate, parse the resulting log, assert structure.

use congestion::format::{read_file_header, read_record};
use std::io::BufReader;

#[test]
fn auto_meta_histogram_log_records_real_probes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.hdr");

    let auto = common::AutoMetaThrottleConfig {
        initial_cwnd: 1,
        min_cwnd: 1,
        max_cwnd: 4096,
        alpha: 1.3,
        beta: 1.8,
        increase_step: 1,
        decrease_step: 1,
        baseline_percentile: 0.1,
        current_percentile: 0.5,
        long_window: std::time::Duration::from_secs(10),
        short_window: std::time::Duration::from_secs(1),
        tick_interval: std::time::Duration::from_millis(50),
    };
    let throttle = common::ThrottleConfig {
        max_open_files: None,
        ops_throttle: 0,
        iops_throttle: 0,
        chunk_size: 0,
        auto_meta: Some(auto),
        histogram_enabled: true,
        histogram_log_path: Some(path.clone()),
        histogram_interval: std::time::Duration::from_millis(150),
    };
    let workdir = tempfile::tempdir().unwrap();
    let workdir_path = workdir.path().to_path_buf();
    let summary = common::run::<_, String, anyhow::Error>(
        None,
        common::OutputConfig::default(),
        common::RuntimeConfig::default(),
        throttle,
        common::TracingConfig {
            trace_identifier: "rcp".to_string(),
            ..Default::default()
        },
        || async move {
            // Fire stat probes by creating + statting files via the
            // probed walk API so the auto-meta sample sink receives samples.
            for i in 0..50 {
                let p = workdir_path.join(format!("f{i}"));
                tokio::fs::write(&p, b"x").await?;
                let _ = common::walk::run_metadata_probed(
                    common::Side::Source,
                    common::MetadataOp::Stat,
                    tokio::fs::metadata(&p),
                )
                .await?;
            }
            // Wait long enough for at least one snapshot tick (interval = 150ms).
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            Ok::<_, anyhow::Error>("done".to_string())
        },
    );
    assert!(summary.is_some());

    // Parse the log file (suffixed with the trace identifier).
    let actual_path = path.with_file_name("test.rcp.hdr");
    let f = std::fs::File::open(&actual_path)
        .unwrap_or_else(|e| panic!("expected log at {actual_path:?}: {e}"));
    let mut reader = BufReader::new(f);
    let header = read_file_header(&mut reader).expect("header parses");
    assert_eq!(header.format_version, 1);
    assert_eq!(header.tool, "rcp");
    assert_eq!(header.snapshot_interval_micros, 150_000);

    let mut total_samples = 0u64;
    while let Some(rec) = read_record(&mut reader).expect("reads cleanly") {
        total_samples += rec.samples_count;
    }
    assert!(
        total_samples > 0,
        "expected at least one sample recorded; got 0"
    );
}
