#[test]
fn check_filegen_help() {
    let mut cmd = assert_cmd::Command::cargo_bin("filegen").unwrap();
    cmd.arg("--help").assert().success();
}

#[test]
fn parses_finite_and_unlimited_max_files_in_flight_before_help() {
    assert_cmd::Command::cargo_bin("filegen")
        .unwrap()
        .args(["--max-files-in-flight=1", "--help"])
        .assert()
        .success();
    assert_cmd::Command::cargo_bin("filegen")
        .unwrap()
        .args(["--max-files-in-flight=unlimited", "--help"])
        .assert()
        .success();
    let output = assert_cmd::Command::cargo_bin("filegen")
        .unwrap()
        .args(["--max-files-in-flight=0", "--help"])
        .output()
        .expect("filegen must start");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("at least 1"));
}

#[test]
fn legacy_unlimited_file_limit_remains_accepted_with_one_warning() {
    let root = tempfile::tempdir().expect("temporary directory must be created");
    let output = assert_cmd::Command::cargo_bin("filegen")
        .unwrap()
        .args([
            root.path().to_str().unwrap(),
            "1",
            "0",
            "0",
            "--max-open-files=0",
        ])
        .output()
        .expect("filegen must start");
    assert!(
        output.status.success(),
        "legacy filegen invocation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let warning = "--max-open-files=0 is deprecated";
    let warning_count = String::from_utf8_lossy(&output.stdout)
        .matches(warning)
        .count()
        + String::from_utf8_lossy(&output.stderr)
            .matches(warning)
            .count();
    assert_eq!(warning_count, 1, "legacy spelling must warn exactly once");
}

#[test]
fn json_summary_is_one_object_with_exact_counters_and_stats() {
    let dir = tempfile::tempdir().unwrap();
    let output = assert_cmd::Command::cargo_bin("filegen")
        .unwrap()
        .args(["--summary", "--output-format", "json"])
        .arg(dir.path())
        .args(["2", "3", "10", "--leaf-files"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.lines().count(), 1, "{stdout}");
    let summary: serde_json::Value = serde_json::from_str(stdout.trim_end()).unwrap();
    assert_eq!(summary["files_created"], 6);
    assert_eq!(summary["bytes_written"], 60);
    for key in [
        "cpu_time_user_ms",
        "cpu_time_kernel_ms",
        "peak_rss_bytes",
        "voluntary_context_switches",
        "involuntary_context_switches",
        "minor_page_faults",
        "major_page_faults",
    ] {
        assert!(summary[key].is_u64(), "missing {key}: {summary}");
    }
    assert!(summary["walltime_ms"].is_u64(), "{summary}");
}
