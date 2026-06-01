//! CLI Argument Parsing Compatibility Tests for rlink
//!
//! These tests verify that command-line arguments are parsed correctly and maintain
//! Backward compatibility. The focus is on ensuring that argument values, aliases, and formats continue to work as expected across versions.

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

#[test]
fn test_allow_lossy_update_flag() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--allow-lossy-update", "--help"])
        .assert()
        .success();
}

// ============================================================================
// Preserve Settings Tests
// ============================================================================

#[test]
fn test_preserve_settings_all_preset() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--preserve-settings", "all", "--help"])
        .assert()
        .success();
}

#[test]
fn test_preserve_settings_none_preset() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--preserve-settings", "none", "--help"])
        .assert()
        .success();
}

#[test]
fn test_preserve_settings_custom_value() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args([
            "--preserve-settings",
            "f:uid,gid,time,0777 d:uid,gid,time,0777 l:uid,gid,time",
            "--help",
        ])
        .assert()
        .success();
}

// ============================================================================
// TOCTOU Safety Flag Tests
// ============================================================================

/// Test that --toctou-check exits without performing the link operation
#[test]
fn toctou_check_exits_without_operating() {
    let output = Command::cargo_bin("rlink")
        .unwrap()
        .args(["--toctou-check", "/nonexistent-src", "/nonexistent-dst"])
        .output()
        .expect("failed to run rlink");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("TOCTOU"),
        "expected TOCTOU verdict in stdout, got: {stdout}"
    );
    // prove it stopped at the linter and did NOT proceed into the link operation:
    // a --toctou-check verdict prints to stdout and exits before operating, so
    // there is no operation-side error (e.g. about the nonexistent source) on
    // stderr.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.is_empty(),
        "--toctou-check must not proceed into operation (stderr should be empty), got: {stderr}"
    );
}

/// Test that --toctou-check reports safe on Linux (rlink has no --dereference)
#[cfg(target_os = "linux")]
#[test]
fn toctou_check_reports_safe_on_linux() {
    let output = Command::cargo_bin("rlink")
        .unwrap()
        .args(["--toctou-check", "/tmp/src", "/tmp/dst"])
        .output()
        .expect("failed to run rlink");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("SAFE") && !stdout.contains("NOT SAFE"),
        "expected SAFE on Linux for rlink, got: {stdout}"
    );
    assert!(
        output.status.success(),
        "--toctou-check should exit 0 on Linux"
    );
}

/// Test that --toctou-check and --require-toctou-safe conflict
#[test]
fn toctou_check_and_require_toctou_safe_conflict() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args([
            "--toctou-check",
            "--require-toctou-safe",
            "/tmp/src",
            "/tmp/dst",
        ])
        .assert()
        .failure()
        .stderr(predicates::str::contains("--require-toctou-safe"));
}
