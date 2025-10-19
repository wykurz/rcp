//! CLI Argument Parsing Compatibility Tests for rcmp
//!
//! These tests verify that command-line arguments are parsed correctly and maintain
//! backward compatibility. The focus is on ensuring that argument values, aliases,
//! and formats continue to work as expected across versions.
//!
//! NOTE: rcmp currently does not support --progress-type (unlike rcp, rrm, and rlink).
//! If this is added in the future, tests should be added here.

use assert_cmd::Command;

#[test]
fn test_help_runs() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .arg("--help")
        .assert()
        .success();
}

#[test]
fn test_version_runs() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .arg("--version")
        .assert()
        .success();
}

// ============================================================================
// Boolean Flag Tests
// ============================================================================

#[test]
fn test_fail_early_flag() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["--fail-early", "--help"])
        .assert()
        .success();
}

#[test]
fn test_fail_early_short_flag() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["-e", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_flag() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["--progress", "--help"])
        .assert()
        .success();
}

#[test]
fn test_summary_flag() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["--summary", "--help"])
        .assert()
        .success();
}

#[test]
fn test_quiet_flag() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["--quiet", "--help"])
        .assert()
        .success();
}

#[test]
fn test_quiet_short_flag() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["-q", "--help"])
        .assert()
        .success();
}

#[test]
fn test_verbose_single() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["-v", "--help"])
        .assert()
        .success();
}

#[test]
fn test_verbose_double() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["-vv", "--help"])
        .assert()
        .success();
}

#[test]
fn test_verbose_triple() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["-vvv", "--help"])
        .assert()
        .success();
}

#[test]
fn test_max_workers_numeric() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["--max-workers", "4", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_delay_duration() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["--progress-delay", "500ms", "--help"])
        .assert()
        .success();
}

// ============================================================================
// rcmp-specific Flags
// ============================================================================

#[test]
fn test_exit_early_flag() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["--exit-early", "--help"])
        .assert()
        .success();
}

#[test]
fn test_metadata_compare_flag() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["--metadata-compare", "f:size,mtime", "--help"])
        .assert()
        .success();
}

#[test]
fn test_log_flag() {
    Command::cargo_bin("rcmp")
        .unwrap()
        .args(["--log", "/tmp/test.log", "--help"])
        .assert()
        .success();
}
