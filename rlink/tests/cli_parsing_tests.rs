//! CLI Argument Parsing Compatibility Tests for rlink
//!
//! These tests verify that command-line arguments are parsed correctly and maintain
//! backward compatibility. The focus is on ensuring that argument values, aliases,
//! and formats continue to work as expected across versions.

use assert_cmd::Command;

#[test]
fn test_help_runs() {
    Command::cargo_bin("rlink")
        .unwrap()
        .arg("--help")
        .assert()
        .success();
}

#[test]
fn test_version_runs() {
    Command::cargo_bin("rlink")
        .unwrap()
        .arg("--version")
        .assert()
        .success();
}

// ============================================================================
// ProgressType Argument Parsing Tests
// ============================================================================

#[test]
fn test_progress_type_auto_lowercase() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--progress-type", "auto", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_type_auto_capitalized() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--progress-type", "Auto", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_type_progress_bar_pascal_case() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--progress-type", "ProgressBar", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_type_progress_bar_kebab_case() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--progress-type", "progress-bar", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_type_text_updates_pascal_case() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--progress-type", "TextUpdates", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_type_text_updates_kebab_case() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--progress-type", "text-updates", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_type_invalid_value() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--progress-type", "invalid-value", "--help"])
        .assert()
        .failure()
        .stderr(predicates::str::contains("invalid value 'invalid-value'"));
}

// ============================================================================
// Boolean Flag Tests
// ============================================================================

#[test]
fn test_fail_early_flag() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--fail-early", "--help"])
        .assert()
        .success();
}

#[test]
fn test_fail_early_short_flag() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["-e", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_flag() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--progress", "--help"])
        .assert()
        .success();
}

#[test]
fn test_summary_flag() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--summary", "--help"])
        .assert()
        .success();
}

#[test]
fn test_quiet_flag() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--quiet", "--help"])
        .assert()
        .success();
}

#[test]
fn test_quiet_short_flag() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["-q", "--help"])
        .assert()
        .success();
}

#[test]
fn test_verbose_single() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["-v", "--help"])
        .assert()
        .success();
}

#[test]
fn test_verbose_double() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["-vv", "--help"])
        .assert()
        .success();
}

#[test]
fn test_verbose_triple() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["-vvv", "--help"])
        .assert()
        .success();
}

#[test]
fn test_max_workers_numeric() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--max-workers", "4", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_delay_duration() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--progress-delay", "500ms", "--help"])
        .assert()
        .success();
}

// ============================================================================
// rlink-specific Flags
// ============================================================================

#[test]
fn test_update_flag() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--update", "/some/path", "--help"])
        .assert()
        .success();
}

#[test]
fn test_update_exclusive_flag() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--update-exclusive", "--help"])
        .assert()
        .success();
}

#[test]
fn test_update_compare_flag() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--update-compare", "size,mtime", "--help"])
        .assert()
        .success();
}
