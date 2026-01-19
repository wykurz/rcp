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
        .stdout(predicate::str::contains("[DstMissing]"));
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
    // verify log file contains the difference
    let log_content = std::fs::read_to_string(&log).unwrap();
    assert!(log_content.contains("[DstMissing]"));
}
