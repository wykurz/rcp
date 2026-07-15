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

/// --require-toctou-safe refuses a relative operand: the strict operand contract
/// requires absolute, lexically normal paths
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_rejects_relative_operand() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--require-toctou-safe", "rel/src", "/tmp/dst"])
        .assert()
        .failure()
        .stdout(predicates::str::contains("absolute"));
}

/// --require-toctou-safe refuses a `..` component in any operand, including --update
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_rejects_dotdot_update_operand() {
    Command::cargo_bin("rlink")
        .unwrap()
        .args([
            "--require-toctou-safe",
            "--update",
            "/tmp/upd/../x",
            "/tmp/src",
            "/tmp/dst",
        ])
        .assert()
        .failure()
        .stdout(predicates::str::contains("`..` component"));
}

/// --require-toctou-safe with fully-resolved absolute operands performs the link
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_links_with_resolved_absolute_operands() {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    // canonicalize: TMPDIR itself may contain symlinked components (e.g. under
    // nix-shell), which strict resolution would — correctly — refuse
    let tmp = tmp.path().canonicalize().unwrap();
    std::fs::create_dir(tmp.join("src")).unwrap();
    std::fs::write(tmp.join("src/a.txt"), b"hello").unwrap();
    Command::cargo_bin("rlink")
        .unwrap()
        .arg("--require-toctou-safe")
        .arg(tmp.join("src"))
        .arg(tmp.join("dst"))
        .assert()
        .success();
    assert_eq!(std::fs::read(tmp.join("dst/a.txt")).unwrap(), b"hello");
}

/// --require-toctou-safe fails closed when an operand path crosses a symlink
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_refuses_symlinked_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let tmp = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(tmp.join("real/src")).unwrap();
    std::fs::write(tmp.join("real/src/a.txt"), b"x").unwrap();
    std::os::unix::fs::symlink(tmp.join("real"), tmp.join("link")).unwrap();
    Command::cargo_bin("rlink")
        .unwrap()
        .arg("--require-toctou-safe")
        .arg(tmp.join("link/src"))
        .arg(tmp.join("dst"))
        .assert()
        .failure();
    assert!(
        !tmp.join("dst").exists(),
        "nothing must be linked through a symlinked prefix"
    );
}

/// An excluded source under an execute-only (searchable, not readable) parent skips
/// cleanly on the default path: the root filter classifies via a path stat and must
/// not require opening the parent directory for read (regression test — the strict
/// mode's pre-filter parent open must stay strict-only)
#[test]
fn filtered_out_root_skips_under_execute_only_parent() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let tmp = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(tmp.join("parent/src")).unwrap();
    std::fs::write(tmp.join("parent/src/a.txt"), b"x").unwrap();
    std::fs::set_permissions(tmp.join("parent"), std::fs::Permissions::from_mode(0o111)).unwrap();
    let assert = Command::cargo_bin("rlink")
        .unwrap()
        .arg("--exclude=src")
        .arg(tmp.join("parent/src"))
        .arg(tmp.join("dst"))
        .assert();
    // restore permissions before asserting so the tempdir cleanup works either way
    std::fs::set_permissions(tmp.join("parent"), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert.success();
    assert!(!tmp.join("dst").exists());
}

// ── Strict destination/update-prefix validation (round-5 regression matrix) ───

/// A symlinked DESTINATION prefix fails closed even when the SOURCE root is filtered out.
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_refuses_symlinked_dst_prefix_when_source_filtered() {
    if !common::safedir::openat2_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(base.join("src")).unwrap();
    std::fs::write(base.join("src/a.txt"), b"x").unwrap();
    std::fs::create_dir_all(base.join("real")).unwrap();
    std::os::unix::fs::symlink(base.join("real"), base.join("link")).unwrap();
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--require-toctou-safe", "--exclude=src"])
        .arg(base.join("src"))
        .arg(base.join("link/out"))
        .assert()
        .failure();
    assert!(!base.join("real/out").exists());
}

/// A symlinked `--update` prefix fails closed under PLAIN `--update` (no --delete/--update-exclusive)
/// even when the source root is filtered out — the update prefix is validated up front, before the
/// source-filter early-return that plain --update would otherwise take.
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_refuses_symlinked_update_prefix_plain_update_filtered_src() {
    if !common::safedir::openat2_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(base.join("src")).unwrap();
    std::fs::write(base.join("src/a.txt"), b"x").unwrap();
    std::fs::create_dir_all(base.join("realupd/tree")).unwrap();
    std::os::unix::fs::symlink(base.join("realupd"), base.join("updlink")).unwrap();
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--require-toctou-safe", "--exclude=src", "--update"])
        .arg(base.join("updlink/tree"))
        .arg(base.join("src"))
        .arg(base.join("dst"))
        .assert()
        .failure();
    assert!(!base.join("dst").exists());
}

/// A symlinked `--update` prefix fails closed under destructive `--delete` (the up-front strict
/// update-prefix validation replaces the old path-based existence guard).
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_refuses_symlinked_update_prefix_destructive() {
    if !common::safedir::openat2_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(base.join("src")).unwrap();
    std::fs::write(base.join("src/a.txt"), b"x").unwrap();
    std::fs::create_dir_all(base.join("realupd/tree")).unwrap();
    std::os::unix::fs::symlink(base.join("realupd"), base.join("updlink")).unwrap();
    Command::cargo_bin("rlink")
        .unwrap()
        .args(["--require-toctou-safe", "--delete", "--update"])
        .arg(base.join("updlink/tree"))
        .arg(base.join("src"))
        .arg(base.join("dst"))
        .assert()
        .failure();
}
