use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn check_rcmp_help() {
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    cmd.arg("--help").assert();
}

#[test]
fn outputs_differences_to_stdout_by_default() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    // create file only in src so dst is missing it
    std::fs::write(src.join("file.txt"), "hello").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    cmd.arg(&src)
        .arg(&dst)
        .assert()
        .code(1) // differences found
        .stdout(predicate::str::contains("\"result\":\"dst_missing\""));
}

#[test]
fn quiet_suppresses_stdout_output() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    // create file only in src so dst is missing it
    std::fs::write(src.join("file.txt"), "hello").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    cmd.arg("--quiet")
        .arg(&src)
        .arg(&dst)
        .assert()
        .code(1) // differences found
        .stdout(predicate::str::is_empty());
}

#[test]
fn log_writes_to_file_not_stdout() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    let log = dir.path().join("compare.log");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    // create file only in src so dst is missing it
    std::fs::write(src.join("file.txt"), "hello").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    cmd.arg("--log")
        .arg(&log)
        .arg(&src)
        .arg(&dst)
        .assert()
        .code(1) // differences found
        .stdout(predicate::str::is_empty());
    // verify log file contains the difference in JSON format
    let log_content = std::fs::read_to_string(&log).unwrap();
    assert!(log_content.contains("\"result\":\"dst_missing\""));
}

#[test]
fn text_output_format_produces_legacy_output() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    // create file only in src so dst is missing it
    std::fs::write(src.join("file.txt"), "hello").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    cmd.arg("--output-format")
        .arg("text")
        .arg(&src)
        .arg(&dst)
        .assert()
        .code(1) // differences found
        .stdout(predicate::str::contains("[DstMissing]"));
}

#[test]
fn text_log_writes_legacy_format_to_file() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    let log = dir.path().join("compare.log");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("file.txt"), "hello").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    cmd.arg("--output-format")
        .arg("text")
        .arg("--log")
        .arg(&log)
        .arg(&src)
        .arg(&dst)
        .assert()
        .code(1)
        .stdout(predicate::str::is_empty());
    let log_content = std::fs::read_to_string(&log).unwrap();
    assert!(log_content.contains("[DstMissing]"));
}

#[test]
fn json_summary_is_valid_json_without_trailing_text() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    std::fs::write(src.join("file.txt"), "hello").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    let output = cmd.arg("--summary").arg(&src).arg(&dst).output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "expected exit code 1 (differences found)"
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout.lines().collect();
    // last line should be the summary JSON, and every line should be valid JSON
    for line in &lines {
        assert!(
            serde_json::from_str::<serde_json::Value>(line).is_ok(),
            "not valid JSON: {line}"
        );
    }
    // summary line should contain runtime stats
    let summary: serde_json::Value = serde_json::from_str(lines.last().unwrap()).unwrap();
    assert!(summary.get("walltime_ms").is_some());
    assert!(summary.get("src_bytes").is_some());
    assert!(summary.get("mismatch").is_some());
}

#[test]
fn json_mismatch_output_handles_non_utf8_paths() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(&dst).unwrap();
    // create a file with a non-UTF-8 byte in its name
    let bad_name = OsStr::from_bytes(b"bad\xffname.txt");
    std::fs::write(src.join(bad_name), "data").unwrap();
    let mut cmd = Command::cargo_bin("rcmp").unwrap();
    let output = cmd.arg(&src).arg(&dst).output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "expected exit code 1 (differences found)"
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    // the output should contain the escaped byte, not the replacement character
    assert!(
        stdout.contains("\\xff"),
        "expected \\xff escape in output, got: {stdout}"
    );
    assert!(
        !stdout.contains('\u{FFFD}'),
        "should not contain replacement character"
    );
    // should be valid JSON
    for line in stdout.lines() {
        assert!(
            serde_json::from_str::<serde_json::Value>(line).is_ok(),
            "not valid JSON: {line}"
        );
    }
}
