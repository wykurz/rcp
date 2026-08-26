#[test]
fn check_filegen_help() {
    let mut cmd = assert_cmd::Command::cargo_bin("filegen").unwrap();
    cmd.arg("--help").assert().success();
}

#[test]
fn parses_positive_max_files_in_flight_before_help() {
    assert_cmd::Command::cargo_bin("filegen")
        .unwrap()
        .args(["--max-files-in-flight=1", "--help"])
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
