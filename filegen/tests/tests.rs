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
