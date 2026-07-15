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
// clap before the duration is forwarded to TimeFilter::from_cli_args).
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
#[cfg(not(target_env = "musl"))]
fn accepts_created_before_year() {
    accept_duration("--created-before", "1y", "rrm_accept_created_year");
}

#[test]
#[cfg(not(target_env = "musl"))]
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
#[cfg(not(target_env = "musl"))]
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

/// On musl, --created-before is accepted by clap but rejected at startup because
/// std::fs::Metadata::created() cannot read btime under musl's stat wrapper.
#[test]
#[cfg(target_env = "musl")]
fn rejects_created_before_on_musl() {
    let tmp = std::env::temp_dir().join("rrm_musl_created_before");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir(&tmp).unwrap();
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--created-before", "1y", tmp.to_str().unwrap()])
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "--created-before is not supported on musl builds",
        ));
    let _ = std::fs::remove_dir_all(&tmp);
}

// ============================================================================
// TOCTOU Safety Flag Tests
// ============================================================================

/// Test that --toctou-check exits without performing the removal
#[test]
fn toctou_check_exits_without_operating() {
    let output = Command::cargo_bin("rrm")
        .unwrap()
        .args(["--toctou-check", "/nonexistent-path"])
        .output()
        .expect("failed to run rrm");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("TOCTOU"),
        "expected TOCTOU verdict in stdout, got: {stdout}"
    );
    // prove it stopped at the linter and did NOT proceed into the removal: with a
    // --toctou-check verdict the tool prints to stdout and exits before operating,
    // so there is no operation-side error (e.g. "failed reading metadata" for the
    // nonexistent path) on stderr.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.is_empty(),
        "--toctou-check must not proceed into operation (stderr should be empty), got: {stderr}"
    );
}

/// Test that --toctou-check reports safe on Linux (rrm has no --dereference)
#[cfg(target_os = "linux")]
#[test]
fn toctou_check_reports_safe_on_linux() {
    let output = Command::cargo_bin("rrm")
        .unwrap()
        .args(["--toctou-check", "/tmp"])
        .output()
        .expect("failed to run rrm");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("SAFE") && !stdout.contains("NOT SAFE"),
        "expected SAFE on Linux for rrm, got: {stdout}"
    );
    assert!(
        output.status.success(),
        "--toctou-check should exit 0 on Linux"
    );
}

/// Test that --toctou-check and --require-toctou-safe conflict
#[test]
fn toctou_check_and_require_toctou_safe_conflict() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--toctou-check", "--require-toctou-safe", "/tmp"])
        .assert()
        .failure()
        .stderr(predicates::str::contains("--require-toctou-safe"));
}

/// --require-toctou-safe refuses a relative operand: the strict operand contract
/// requires absolute, lexically normal paths
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_rejects_relative_operand() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--require-toctou-safe", "rel/path"])
        .assert()
        .failure()
        .stdout(predicates::str::contains("absolute"));
}

/// --require-toctou-safe refuses an operand containing a `..` component
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_rejects_dotdot_operand() {
    Command::cargo_bin("rrm")
        .unwrap()
        .args(["--require-toctou-safe", "/tmp/../victim"])
        .assert()
        .failure()
        .stdout(predicates::str::contains("`..` component"));
}

/// --require-toctou-safe with a fully-resolved absolute operand performs the removal
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_removes_with_resolved_absolute_operand() {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    // canonicalize: TMPDIR itself may contain symlinked components (e.g. under
    // nix-shell), which strict resolution would — correctly — refuse
    let tmp = tmp.path().canonicalize().unwrap();
    std::fs::create_dir(tmp.join("victim")).unwrap();
    std::fs::write(tmp.join("victim/a.txt"), b"x").unwrap();
    Command::cargo_bin("rrm")
        .unwrap()
        .arg("--require-toctou-safe")
        .arg(tmp.join("victim"))
        .assert()
        .success();
    assert!(!tmp.join("victim").exists());
}

/// --require-toctou-safe fails closed when the operand path crosses a symlink —
/// and removes nothing through it
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_refuses_symlinked_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let tmp = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(tmp.join("real/victim")).unwrap();
    std::fs::write(tmp.join("real/victim/a.txt"), b"x").unwrap();
    std::os::unix::fs::symlink(tmp.join("real"), tmp.join("link")).unwrap();
    Command::cargo_bin("rrm")
        .unwrap()
        .arg("--require-toctou-safe")
        .arg(tmp.join("link/victim"))
        .assert()
        .failure();
    assert!(
        tmp.join("real/victim/a.txt").exists(),
        "nothing must be removed through a symlinked prefix"
    );
}

/// A symlink OPERAND under --require-toctou-safe keeps the tools' non--L
/// semantics: the link itself is removed, its target is never touched
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_removes_symlink_operand_as_link() {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let tmp = tmp.path().canonicalize().unwrap();
    std::fs::write(tmp.join("target.txt"), b"payload").unwrap();
    std::os::unix::fs::symlink(tmp.join("target.txt"), tmp.join("link")).unwrap();
    Command::cargo_bin("rrm")
        .unwrap()
        .arg("--require-toctou-safe")
        .arg(tmp.join("link"))
        .assert()
        .success();
    assert!(
        std::fs::symlink_metadata(tmp.join("link")).is_err(),
        "the link itself must be removed"
    );
    assert!(
        tmp.join("target.txt").exists(),
        "the link target must never be touched"
    );
}
