//! CLI argument parsing and validation tests for rchm.
//!
//! These tests verify that rchm's command-line arguments are parsed correctly
//! and that invalid operations (missing op, symlink mode, unknown group) are
//! rejected with the expected diagnostics.

use assert_cmd::Command;

fn rchm() -> Command {
    Command::cargo_bin("rchm").unwrap()
}

#[test]
fn errors_when_no_operation_given() {
    // the common harness routes runtime errors to stdout (see common::run)
    rchm()
        .arg("/tmp")
        .assert()
        .failure()
        .stdout(predicates::str::contains("nothing to do"));
}

#[test]
fn errors_when_no_path_given() {
    // an operation but no path operand must fail loudly, not silently succeed
    rchm()
        .args(["--mode", "g+w"])
        .assert()
        .failure()
        .stdout(predicates::str::contains("no paths given"));
}

#[test]
fn rejects_symlink_mode_section() {
    // the common harness routes runtime errors to stdout (see common::run)
    rchm()
        .args(["--mode", "l:g+w", "/tmp"])
        .assert()
        .failure()
        .stdout(predicates::str::contains("symlink mode (l:)"));
}

#[test]
fn rejects_unknown_group() {
    // the common harness routes runtime errors to stdout (see common::run)
    rchm()
        .args(["--group", "definitely-no-such-group-xyz", "/tmp"])
        .assert()
        .failure()
        .stdout(predicates::str::contains("unknown group"));
}

#[test]
fn help_lists_operation_options() {
    rchm()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicates::str::contains("--mode"))
        .stdout(predicates::str::contains("--group"))
        .stdout(predicates::str::contains("--owner"));
}

// ============================================================================
// TOCTOU Safety Flag Tests
// ============================================================================

/// Test that --toctou-check exits without performing the chmod operation
#[test]
fn toctou_check_exits_without_operating() {
    // even without a valid path, --toctou-check should print verdict and exit
    let output = rchm()
        .args(["--toctou-check", "--mode", "g+r", "/nonexistent-path"])
        .output()
        .expect("failed to run rchm");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("TOCTOU"),
        "expected TOCTOU verdict in stdout, got: {stdout}"
    );
    // prove it stopped at the linter and did NOT proceed into the chmod: a
    // --toctou-check verdict prints to stdout and exits before operating, so there
    // is no operation-side error (e.g. about the nonexistent path) on stderr.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.is_empty(),
        "--toctou-check must not proceed into operation (stderr should be empty), got: {stderr}"
    );
}

/// Test that --toctou-check reports safe on Linux (rchm has no --dereference)
#[cfg(target_os = "linux")]
#[test]
fn toctou_check_reports_safe_on_linux() {
    let output = rchm()
        .args(["--toctou-check", "--mode", "g+r", "/tmp"])
        .output()
        .expect("failed to run rchm");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("SAFE") && !stdout.contains("NOT SAFE"),
        "expected SAFE on Linux for rchm, got: {stdout}"
    );
    assert!(
        output.status.success(),
        "--toctou-check should exit 0 on Linux"
    );
}

/// Test that --toctou-check and --require-toctou-safe conflict
#[test]
fn toctou_check_and_require_toctou_safe_conflict() {
    rchm()
        .args([
            "--toctou-check",
            "--require-toctou-safe",
            "--mode",
            "g+r",
            "/tmp",
        ])
        .assert()
        .failure()
        .stderr(predicates::str::contains("--require-toctou-safe"));
}
