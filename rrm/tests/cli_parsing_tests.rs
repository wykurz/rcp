//! CLI Argument Parsing Compatibility Tests for rrm
//!
//! These tests verify that command-line arguments are parsed correctly and maintain
//! Backward compatibility. The focus is on ensuring that argument values, aliases, and formats continue to work as expected across versions.

use assert_cmd::Command;

#[test]
fn test_help_runs() {
    Command::cargo_bin("rrm")
        .unwrap()
        .arg("--help")
        .assert()
        .success();
}

#[test]
fn test_version_runs() {
    Command::cargo_bin("rrm")
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
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--progress-type", "auto", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_type_auto_capitalized() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--progress-type", "Auto", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_type_progress_bar_pascal_case() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--progress-type", "ProgressBar", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_type_progress_bar_kebab_case() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--progress-type", "progress-bar", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_type_text_updates_pascal_case() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--progress-type", "TextUpdates", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_type_text_updates_kebab_case() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--progress-type", "text-updates", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_type_invalid_value() {
    Command::cargo_bin("rrm")
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
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--fail-early", "--help"])
        .assert()
        .success();
}

#[test]
fn test_fail_early_short_flag() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["-e", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_flag() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--progress", "--help"])
        .assert()
        .success();
}

#[test]
fn test_summary_flag() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--summary", "--help"])
        .assert()
        .success();
}

#[test]
fn test_quiet_flag() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--quiet", "--help"])
        .assert()
        .success();
}

#[test]
fn test_quiet_short_flag() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["-q", "--help"])
        .assert()
        .success();
}

#[test]
fn test_verbose_single() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["-v", "--help"])
        .assert()
        .success();
}

#[test]
fn test_verbose_double() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["-vv", "--help"])
        .assert()
        .success();
}

#[test]
fn test_verbose_triple() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["-vvv", "--help"])
        .assert()
        .success();
}

#[test]
fn test_max_workers_numeric() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--max-workers", "4", "--help"])
        .assert()
        .success();
}

#[test]
fn test_progress_delay_duration() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--progress-delay", "500ms", "--help"])
        .assert()
        .success();
}

// ============================================================================
// Time-filter Argument Parsing Tests
// ============================================================================

// Helpers for duration-accept tests: run against an empty temp dir in dry-run
// mode so parsing is actually exercised (unlike --help, which short-circuits
// clap before the duration is forwarded to build_time_filter).
fn accept_duration(flag: &str, value: &str, dir_name: &str) {
    let tmp = std::env::temp_dir().join(dir_name);
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir(&tmp).unwrap();
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--dry-run", "brief", flag, value, tmp.to_str().unwrap()])
        .assert()
        .success();
    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn accepts_modified_before_year() {
    accept_duration("--modified-before", "1y", "rrm_accept_mod_year");
}

#[test]
fn accepts_modified_before_days() {
    accept_duration("--modified-before", "30d", "rrm_accept_mod_days");
}

#[test]
fn accepts_modified_before_months_uppercase_m() {
    accept_duration("--modified-before", "6M", "rrm_accept_mod_months");
}

#[test]
fn accepts_created_before_year() {
    accept_duration("--created-before", "1y", "rrm_accept_created_year");
}

#[test]
fn accepts_created_before_long_form() {
    accept_duration("--created-before", "6months", "rrm_accept_created_longform");
}

#[test]
fn rejects_invalid_modified_before_duration() {
    // create a temp directory that won't be removed (parsing fails first)
    let tmp = std::env::temp_dir().join("rrm_parse_test_dir");
    let _ = std::fs::create_dir(&tmp);
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--modified-before", "foo", tmp.to_str().unwrap()])
        .assert()
        .failure()
        .stdout(predicates::str::contains("--modified-before"));
    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn rejects_invalid_created_before_duration() {
    let tmp = std::env::temp_dir().join("rrm_parse_test_dir2");
    let _ = std::fs::create_dir(&tmp);
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--created-before", "not-a-duration", tmp.to_str().unwrap()])
        .assert()
        .failure()
        .stdout(predicates::str::contains("--created-before"));
    let _ = std::fs::remove_dir_all(&tmp);
}
