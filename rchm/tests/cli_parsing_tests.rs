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
fn no_setid_alone_is_not_an_operation() {
    // the safety policy only constrains an explicit mode or ownership operation
    rchm()
        .args(["--no-setid", "/tmp"])
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
fn rejects_relative_getent_path() {
    // a relative --getent-path would re-introduce a PATH/cwd lookup — rejected up front.
    rchm()
        .args(["--group", "data", "--getent-path", "getent", "/tmp"])
        .assert()
        .failure()
        .stdout(predicates::str::contains("must be an absolute path"));
}

#[test]
fn rejects_duplicate_getent_path() {
    // a duplicate could override the path a wildcard sudo rule baked in — rejected, not last-wins.
    rchm()
        .args([
            "--group",
            "data",
            "--getent-path",
            "/usr/bin/getent",
            "--getent-path",
            "/tmp/evil/getent",
            "/tmp",
        ])
        .assert()
        .failure()
        .stdout(predicates::str::contains("at most once"));
}

#[test]
fn help_lists_mode_ownership_and_no_setid_options() {
    rchm()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicates::str::contains("--mode"))
        .stdout(predicates::str::contains("--group"))
        .stdout(predicates::str::contains("--owner"))
        .stdout(predicates::str::contains("--no-setid"));
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

/// --require-toctou-safe refuses a relative operand: the strict operand contract
/// requires absolute, lexically normal paths
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_rejects_relative_operand() {
    rchm()
        .args(["--require-toctou-safe", "--mode", "g+r", "rel/path"])
        .assert()
        .failure()
        .stdout(predicates::str::contains("absolute"));
}

/// --require-toctou-safe refuses an operand containing a `..` component
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_rejects_dotdot_operand() {
    rchm()
        .args(["--require-toctou-safe", "--mode", "g+r", "/tmp/../victim"])
        .assert()
        .failure()
        .stdout(predicates::str::contains("`..` component"));
}

/// --require-toctou-safe with a fully-resolved absolute operand performs the chmod
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_chmods_with_resolved_absolute_operand() {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    // canonicalize: TMPDIR itself may contain symlinked components (e.g. under
    // nix-shell), which strict resolution would — correctly — refuse
    let tmp = tmp.path().canonicalize().unwrap();
    std::fs::create_dir(tmp.join("dir")).unwrap();
    rchm()
        .args(["--require-toctou-safe", "--mode", "0700"])
        .arg(tmp.join("dir"))
        .assert()
        .success();
    let mode = std::fs::metadata(tmp.join("dir"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o7777, 0o700);
}

/// --require-toctou-safe fails closed when the operand path crosses a symlink —
/// and changes nothing through it
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_refuses_symlinked_prefix() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let tmp = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(tmp.join("real/dir")).unwrap();
    std::fs::set_permissions(tmp.join("real/dir"), std::fs::Permissions::from_mode(0o755)).unwrap();
    std::os::unix::fs::symlink(tmp.join("real"), tmp.join("link")).unwrap();
    rchm()
        .args(["--require-toctou-safe", "--mode", "0700"])
        .arg(tmp.join("link/dir"))
        .assert()
        .failure();
    let mode = std::fs::metadata(tmp.join("real/dir"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(
        mode & 0o7777,
        0o755,
        "mode must be unchanged through a symlinked prefix"
    );
}
