//! CLI Argument Parsing Compatibility Tests
//!
//! These tests verify that command-line arguments are parsed correctly and maintain backward compatibility.
//! The focus is on ensuring that argument values, aliases, and formats continue to work as expected across versions.
//!
//! Tests in this file should NOT be modified to match new behavior unless it's intentional and documented in the changelog.
//! Breaking changes here indicate potential issues for existing users.

use assert_cmd::Command;

/// Test that --help output is generated without errors
#[test]
fn test_help_runs() {
    Command::cargo_bin("rcp")
        .unwrap()
        .arg("--help")
        .assert()
        .success();
}

/// Test --version flag works
#[test]
fn test_version_runs() {
    Command::cargo_bin("rcp")
        .unwrap()
        .arg("--version")
        .assert()
        .success();
}

/// Test --protocol-version flag works and returns valid JSON with git info
#[test]
fn test_protocol_version_has_git_info() {
    let output = Command::cargo_bin("rcp")
        .unwrap()
        .arg("--protocol-version")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let output_str = String::from_utf8(output).expect("output should be valid UTF-8");
    let version: serde_json::Value =
        serde_json::from_str(&output_str).expect("output should be valid JSON");

    // verify semantic version is present
    assert!(
        version["semantic"].is_string(),
        "semantic version should be present"
    );
    assert!(
        !version["semantic"].as_str().unwrap().is_empty(),
        "semantic version should not be empty"
    );

    // verify git info is populated (this catches build.rs not being activated)
    assert!(
        version["git_describe"].is_string(),
        "git_describe should be present when building from git repo (check that build.rs is activated in common/Cargo.toml)"
    );
    assert!(
        version["git_hash"].is_string(),
        "git_hash should be present when building from git repo (check that build.rs is activated in common/Cargo.toml)"
    );
}

/// Test that rcpd also has --protocol-version with git info
#[test]
fn test_rcpd_protocol_version_has_git_info() {
    let output = Command::cargo_bin("rcpd")
        .unwrap()
        .arg("--protocol-version")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let output_str = String::from_utf8(output).expect("output should be valid UTF-8");
    let version: serde_json::Value =
        serde_json::from_str(&output_str).expect("output should be valid JSON");

    // verify git info is populated for rcpd too
    assert!(
        version["git_describe"].is_string(),
        "rcpd git_describe should be present (check that build.rs is activated in common/Cargo.toml)"
    );
    assert!(
        version["git_hash"].is_string(),
        "rcpd git_hash should be present (check that build.rs is activated in common/Cargo.toml)"
    );
}

/// Test that --protocol-version after -- is treated as a filename (Unix convention)
#[test]
fn test_protocol_version_after_separator_is_filename() {
    // with --protocol-version after --, it should be treated as a filename
    // and require both source and destination paths
    let output = Command::cargo_bin("rcp")
        .unwrap()
        .args(["--", "--protocol-version"])
        .assert()
        .failure(); // should fail because we need src and dst

    let out = output.get_output();
    let stderr = String::from_utf8(out.stderr.clone()).unwrap();
    let stdout = String::from_utf8(out.stdout.clone()).unwrap();

    // should complain about missing paths, not print version JSON
    // error might be on stdout or stderr
    let combined = format!("{}{}", stdout, stderr);
    assert!(
        combined.contains("must specify") || combined.contains("required"),
        "should complain about missing arguments, got stdout: '{}', stderr: '{}'",
        stdout,
        stderr
    );
}

/// Test that --protocol-version works when it appears before --
#[test]
fn test_protocol_version_before_separator_works() {
    let output = Command::cargo_bin("rcp")
        .unwrap()
        .args(["--protocol-version", "--", "some-file"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let output_str = String::from_utf8(output).expect("output should be valid UTF-8");
    // should be valid JSON (version output)
    let _version: serde_json::Value =
        serde_json::from_str(&output_str).expect("output should be valid JSON");
}

// ============================================================================
// ProgressType Argument Parsing Tests
// ============================================================================
//
// These tests verify that all historical formats for --progress-type continue
// to be accepted. This is critical for backward compatibility.

/// Test that the original "auto" format is accepted
#[test]
fn test_progress_type_auto_lowercase() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--progress-type", "auto", "--help"])
        .assert()
        .success();
}

/// Test that the "Auto" capitalized alias is accepted (backward compatibility)
#[test]
fn test_progress_type_auto_capitalized() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--progress-type", "Auto", "--help"])
        .assert()
        .success();
}

/// Test that the original "ProgressBar" PascalCase format is accepted
#[test]
fn test_progress_type_progress_bar_pascal_case() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--progress-type", "ProgressBar", "--help"])
        .assert()
        .success();
}

/// Test that the new "progress-bar" kebab-case alias is accepted
#[test]
fn test_progress_type_progress_bar_kebab_case() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--progress-type", "progress-bar", "--help"])
        .assert()
        .success();
}

/// Test that the original "TextUpdates" PascalCase format is accepted
#[test]
fn test_progress_type_text_updates_pascal_case() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--progress-type", "TextUpdates", "--help"])
        .assert()
        .success();
}

/// Test that the new "text-updates" kebab-case alias is accepted
#[test]
fn test_progress_type_text_updates_kebab_case() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--progress-type", "text-updates", "--help"])
        .assert()
        .success();
}

/// Test that invalid progress type values are rejected with appropriate error
#[test]
fn test_progress_type_invalid_value() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--progress-type", "invalid-value", "--help"])
        .assert()
        .failure()
        .stderr(predicates::str::contains("invalid value 'invalid-value'"));
}

// ============================================================================
// Boolean Flag Tests
// ============================================================================

/// Test that --preserve flag is accepted
#[test]
fn test_preserve_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--preserve", "--help"])
        .assert()
        .success();
}

/// Test that short -p for --preserve works
#[test]
fn test_preserve_short_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["-p", "--help"])
        .assert()
        .success();
}

/// Test that --overwrite flag is accepted
#[test]
fn test_overwrite_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--overwrite", "--help"])
        .assert()
        .success();
}

/// Test that short -o for --overwrite works
#[test]
fn test_overwrite_short_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["-o", "--help"])
        .assert()
        .success();
}

/// Test that --fail-early flag is accepted
#[test]
fn test_fail_early_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--fail-early", "--help"])
        .assert()
        .success();
}

/// Test that short -e for --fail-early works
#[test]
fn test_fail_early_short_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["-e", "--help"])
        .assert()
        .success();
}

/// Test that --dereference flag is accepted
#[test]
fn test_dereference_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--dereference", "--help"])
        .assert()
        .success();
}

/// Test that short -L for --dereference works
#[test]
fn test_dereference_short_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["-L", "--help"])
        .assert()
        .success();
}

/// Test that --progress flag is accepted
#[test]
fn test_progress_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--progress", "--help"])
        .assert()
        .success();
}

/// Test that --summary flag is accepted
#[test]
fn test_summary_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--summary", "--help"])
        .assert()
        .success();
}

/// Test that --quiet flag is accepted
#[test]
fn test_quiet_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--quiet", "--help"])
        .assert()
        .success();
}

/// Test that short -q for --quiet works
#[test]
fn test_quiet_short_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["-q", "--help"])
        .assert()
        .success();
}

// ============================================================================
// Verbose Flag Tests
// ============================================================================

/// Test that -v for verbose works
#[test]
fn test_verbose_single() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["-v", "--help"])
        .assert()
        .success();
}

/// Test that -vv for more verbose works
#[test]
fn test_verbose_double() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["-vv", "--help"])
        .assert()
        .success();
}

/// Test that -vvv for trace level works
#[test]
fn test_verbose_triple() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["-vvv", "--help"])
        .assert()
        .success();
}

/// Test that --verbose long form works
#[test]
fn test_verbose_long_form() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--verbose", "--help"])
        .assert()
        .success();
}

// ============================================================================
// Value-based Argument Tests
// ============================================================================

/// Test that --max-workers accepts numeric values
#[test]
fn test_max_workers_numeric() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--max-workers", "4", "--help"])
        .assert()
        .success();
}

/// Test that --max-workers accepts 0 (special meaning: use defaults)
#[test]
fn test_max_workers_zero() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--max-workers", "0", "--help"])
        .assert()
        .success();
}

/// Test that --ops-throttle accepts numeric values
#[test]
fn test_ops_throttle_numeric() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--ops-throttle", "100", "--help"])
        .assert()
        .success();
}

/// Test that --chunk-size accepts byte size values
#[test]
fn test_chunk_size_bytes() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--chunk-size", "4096", "--help"])
        .assert()
        .success();
}

/// Test that --progress-delay accepts duration strings
#[test]
fn test_progress_delay_duration() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--progress-delay", "500ms", "--help"])
        .assert()
        .success();
}

/// Test that --overwrite-compare accepts comma-separated values
#[test]
fn test_overwrite_compare_values() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--overwrite-compare", "size,mtime", "--help"])
        .assert()
        .success();
}

/// Test default --overwrite-compare value is accepted
#[test]
fn test_overwrite_compare_default() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--overwrite-compare", "size,mtime", "--help"])
        .assert()
        .success();
}

// ============================================================================
// Overwrite Filter Tests
// ============================================================================

/// Test that --overwrite-filter=newer is accepted with --overwrite
#[test]
fn test_overwrite_filter_newer_with_overwrite() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--overwrite", "--overwrite-filter", "newer", "--help"])
        .assert()
        .success();
}

/// Test that --overwrite-filter without --overwrite (or --delete) fails
#[test]
fn test_overwrite_filter_requires_overwrite() {
    // The error is now a runtime check (stdout) rather than a clap parse error (stderr),
    // because --overwrite-filter also works with --delete. Check any output for the message.
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--overwrite-filter", "newer", "/tmp/src", "/tmp/dst"])
        .assert()
        .failure()
        .stdout(predicates::str::contains(
            "--overwrite-filter requires --overwrite",
        ));
}

/// Test that --overwrite-filter rejects invalid values
#[test]
fn test_overwrite_filter_invalid_value() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args([
            "--overwrite",
            "--overwrite-filter",
            "oldest",
            "/tmp/src",
            "/tmp/dst",
        ])
        .assert()
        .failure()
        .stderr(predicates::str::contains("invalid value 'oldest'"));
}

/// Test that --ignore-existing flag is accepted
#[test]
fn test_ignore_existing_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--ignore-existing", "--help"])
        .assert()
        .success();
}

/// Test that --ignore-existing conflicts with --overwrite
#[test]
fn test_ignore_existing_conflicts_with_overwrite() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--ignore-existing", "--overwrite", "/tmp/src", "/tmp/dst"])
        .assert()
        .failure()
        .stderr(predicates::str::contains("--overwrite"));
}

// ============================================================================
// Argument Combination Tests
// ============================================================================

/// Test that multiple flags can be combined
#[test]
fn test_multiple_flags_combined() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args([
            "--preserve",
            "--overwrite",
            "--progress",
            "--summary",
            "--help",
        ])
        .assert()
        .success();
}

/// Test that short flags can be combined
#[test]
fn test_short_flags_combined() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["-pov", "--help"])
        .assert()
        .success();
}

/// Test combining progress-type with progress flag (should work)
#[test]
fn test_progress_type_with_progress_flag() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--progress", "--progress-type", "TextUpdates", "--help"])
        .assert()
        .success();
}

// ============================================================================
// Flag Conflict Tests
// ============================================================================

/// Test that --quiet and --verbose are mutually exclusive (should fail)
#[test]
fn test_quiet_and_verbose_conflict() {
    // This test documents expected behavior - quiet and verbose should conflict
    // Note: The actual conflict check happens at runtime in common::run()
    // so this test just ensures both flags are parseable
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--quiet", "--verbose", "--help"])
        .assert()
        .success(); // parsing succeeds, conflict detected at runtime
}

// ============================================================================
// Preserve Settings Format Tests
// ============================================================================

/// Test that --preserve-settings accepts file type with attributes
#[test]
fn test_preserve_settings_file_format() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--preserve-settings", "f:uid,gid,time", "--help"])
        .assert()
        .success();
}

/// Test that --preserve-settings accepts octal mode mask
#[test]
fn test_preserve_settings_octal_mode() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--preserve-settings", "f:0755", "--help"])
        .assert()
        .success();
}

/// Test that --preserve-settings accepts multiple types
#[test]
fn test_preserve_settings_multiple_types() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args([
            "--preserve-settings",
            "f:uid,gid,time,0777 d:uid,gid,time,0755",
            "--help",
        ])
        .assert()
        .success();
}

/// Test that --preserve-settings accepts "all" preset
#[test]
fn test_preserve_settings_all_preset() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--preserve-settings", "all", "--help"])
        .assert()
        .success();
}

/// Test that --preserve-settings accepts "none" preset
#[test]
fn test_preserve_settings_none_preset() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--preserve-settings", "none", "--help"])
        .assert()
        .success();
}

// ============================================================================
// Remote Copy Argument Tests
// ============================================================================

/// Test that --port-ranges accepts range format
#[test]
fn test_port_ranges_single() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--port-ranges", "8000-8999", "--help"])
        .assert()
        .success();
}

/// Test that --port-ranges accepts multiple ranges
#[test]
fn test_port_ranges_multiple() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--port-ranges", "8000-8999,10000-10999", "--help"])
        .assert()
        .success();
}

/// Test that --remote-copy-conn-timeout-sec accepts numeric values
#[test]
fn test_remote_copy_timeout() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--remote-copy-conn-timeout-sec", "30", "--help"])
        .assert()
        .success();
}

// ============================================================================
// TOCTOU Safety Flag Tests
// ============================================================================

/// Test that --toctou-check exits without performing the copy
#[test]
fn toctou_check_exits_without_operating() {
    // should exit (with any code) without needing valid src/dst
    let output = Command::cargo_bin("rcp")
        .unwrap()
        .args(["--toctou-check", "/nonexistent-src", "/nonexistent-dst"])
        .output()
        .expect("failed to run rcp");
    // must not hang and must produce output about TOCTOU
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("TOCTOU"),
        "expected TOCTOU verdict in stdout, got: {stdout}"
    );
    // prove it stopped at the linter and did NOT proceed into the copy: a
    // --toctou-check verdict prints to stdout and exits before operating, so there
    // is no operation-side error (e.g. about the nonexistent source) on stderr.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.is_empty(),
        "--toctou-check must not proceed into operation (stderr should be empty), got: {stderr}"
    );
}

/// Test that --toctou-check -L reports not safe
#[test]
fn toctou_check_with_dereference_reports_not_safe() {
    let output = Command::cargo_bin("rcp")
        .unwrap()
        .args(["-L", "--toctou-check", "/tmp/src", "/tmp/dst"])
        .output()
        .expect("failed to run rcp");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("NOT SAFE"),
        "expected NOT SAFE with -L, got: {stdout}"
    );
    assert!(
        !output.status.success(),
        "--toctou-check -L should exit non-zero"
    );
}

/// Test that --toctou-check without -L reports safe on Linux
#[cfg(target_os = "linux")]
#[test]
fn toctou_check_without_dereference_reports_safe_on_linux() {
    let output = Command::cargo_bin("rcp")
        .unwrap()
        .args(["--toctou-check", "/tmp/src", "/tmp/dst"])
        .output()
        .expect("failed to run rcp");
    let stdout = String::from_utf8_lossy(&output.stdout);
    // on Linux without -L the verdict should be SAFE
    assert!(
        stdout.contains("SAFE") && !stdout.contains("NOT SAFE"),
        "expected SAFE (without NOT SAFE) on Linux without -L, got: {stdout}"
    );
    assert!(
        output.status.success(),
        "--toctou-check without -L should exit 0 on Linux"
    );
}

/// Test that --require-toctou-safe -L refuses to run
#[test]
fn require_toctou_safe_refuses_with_dereference() {
    let output = Command::cargo_bin("rcp")
        .unwrap()
        .args(["-L", "--require-toctou-safe", "/tmp/src", "/tmp/dst"])
        .output()
        .expect("failed to run rcp");
    assert!(
        !output.status.success(),
        "--require-toctou-safe -L should exit non-zero"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Refusing") || stdout.contains("not TOCTOU-safe"),
        "expected refusal message, got: {stdout}"
    );
}

/// Test that --toctou-check and --require-toctou-safe conflict
#[test]
fn toctou_check_and_require_toctou_safe_conflict() {
    Command::cargo_bin("rcp")
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

/// `--max-connections=0` must be rejected: a zero-size connection pool / zero
/// pending-file budget (`max_connections * multiplier`) would deadlock the remote
/// source. Regression for PR #247 review — the nonzero parser was previously wired
/// only to `--pending-writes-multiplier`, not `--max-connections`.
#[test]
fn test_max_connections_zero_rejected() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--max-connections=0", "/tmp/src", "/tmp/dst"])
        .assert()
        .failure()
        .stderr(predicates::str::contains("at least 1"));
}

/// `--pending-writes-multiplier=0` is likewise rejected (the sibling nonzero
/// validation that was already wired — locked in here alongside max-connections).
#[test]
fn test_pending_writes_multiplier_zero_rejected() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--pending-writes-multiplier=0", "/tmp/src", "/tmp/dst"])
        .assert()
        .failure()
        .stderr(predicates::str::contains("at least 1"));
}

/// --require-toctou-safe refuses a relative operand: the strict operand contract
/// requires absolute, lexically normal paths
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_rejects_relative_operand() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--require-toctou-safe", "rel/src", "/tmp/dst"])
        .assert()
        .failure()
        .stdout(predicates::str::contains("absolute"));
}

/// --require-toctou-safe refuses an operand containing a `..` component
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_rejects_dotdot_operand() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--require-toctou-safe", "/tmp/../src", "/tmp/dst"])
        .assert()
        .failure()
        .stdout(predicates::str::contains("`..` component"));
}

/// --require-toctou-safe refuses home-relative and relative remote operands: the
/// raw path part (tilde NOT expanded, host stripped) must be absolute as written.
/// Covers a real remote host with `~`, a force-remote localhost `~`, and a
/// relative path via the localhost escape hatch.
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_rejects_home_relative_remote_operand() {
    // a real remote host with a tilde source: `host:~/src` → raw part `~/src` → not absolute
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--require-toctou-safe", "examplehost:~/src", "/abs/dst"])
        .assert()
        .failure()
        .stdout(predicates::str::contains("absolute"));
    // --force-remote makes `localhost:~/src` a genuine remote tilde operand → rejected
    Command::cargo_bin("rcp")
        .unwrap()
        .args([
            "--require-toctou-safe",
            "--force-remote",
            "localhost:~/src",
            "examplehost:/abs/dst",
        ])
        .assert()
        .failure()
        .stdout(predicates::str::contains("absolute"));
    // a relative path via the localhost escape hatch → raw part `rel/src` → not absolute
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--require-toctou-safe", "localhost:rel/src", "/abs/dst"])
        .assert()
        .failure()
        .stdout(predicates::str::contains("absolute"));
}

/// --toctou-check keeps its verdict-based exit code for operands that
/// --require-toctou-safe would refuse, but notes the strict-form violation
#[cfg(target_os = "linux")]
#[test]
fn toctou_check_notes_strict_form_violation_without_failing() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--toctou-check", "rel/src", "/tmp/dst"])
        .assert()
        .success()
        .stdout(predicates::str::contains(
            "--require-toctou-safe would refuse",
        ));
}

/// --require-toctou-safe with fully-resolved absolute operands performs the copy
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_copies_with_resolved_absolute_operands() {
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
    Command::cargo_bin("rcp")
        .unwrap()
        .arg("--require-toctou-safe")
        .arg(tmp.join("src"))
        .arg(tmp.join("dst"))
        .assert()
        .success();
    assert_eq!(std::fs::read(tmp.join("dst/a.txt")).unwrap(), b"hello");
}

/// --require-toctou-safe fails closed when an operand path crosses a symlink:
/// the strict open resolves RESOLVE_NO_SYMLINKS and gets ELOOP
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_refuses_symlinked_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let tmp = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(tmp.join("real/src")).unwrap();
    std::fs::write(tmp.join("real/src/a.txt"), b"x").unwrap();
    std::os::unix::fs::symlink(tmp.join("real"), tmp.join("link")).unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .arg("--require-toctou-safe")
        .arg(tmp.join("link/src"))
        .arg(tmp.join("dst"))
        .assert()
        .failure();
    assert!(
        !tmp.join("dst").exists(),
        "nothing must be copied through a symlinked prefix"
    );
}

/// Without --require-toctou-safe a symlinked prefix is followed normally (the
/// documented trusted-boundary default is unchanged)
#[test]
fn symlinked_prefix_followed_without_require_toctou_safe() {
    let tmp = tempfile::tempdir().unwrap();
    let tmp = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(tmp.join("real/src")).unwrap();
    std::fs::write(tmp.join("real/src/a.txt"), b"x").unwrap();
    std::os::unix::fs::symlink(tmp.join("real"), tmp.join("link")).unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .arg(tmp.join("link/src"))
        .arg(tmp.join("dst"))
        .assert()
        .success();
    assert_eq!(std::fs::read(tmp.join("dst/a.txt")).unwrap(), b"x");
}

/// --require-toctou-safe fails closed on a symlinked prefix even when a filter
/// would skip the root: the strict operand open runs before the root filter
/// short-circuit (regression test for the filter early-return bypass)
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_refuses_symlinked_prefix_even_when_filter_excludes_root() {
    let tmp = tempfile::tempdir().unwrap();
    let tmp = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(tmp.join("real/src")).unwrap();
    std::fs::write(tmp.join("real/src/a.txt"), b"x").unwrap();
    std::os::unix::fs::symlink(tmp.join("real"), tmp.join("link")).unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--require-toctou-safe", "--exclude=src"])
        .arg(tmp.join("link/src"))
        .arg(tmp.join("dst"))
        .assert()
        .failure();
    assert!(!tmp.join("dst").exists());
}

/// A symlink OPERAND under --require-toctou-safe keeps the tools' non--L
/// semantics: it is copied as the link object itself, never followed
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_copies_symlink_operand_as_link() {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let tmp = tmp.path().canonicalize().unwrap();
    std::fs::write(tmp.join("target.txt"), b"payload").unwrap();
    std::os::unix::fs::symlink("target.txt", tmp.join("link")).unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .arg("--require-toctou-safe")
        .arg(tmp.join("link"))
        .arg(tmp.join("out"))
        .assert()
        .success();
    let out_meta = std::fs::symlink_metadata(tmp.join("out")).unwrap();
    assert!(
        out_meta.file_type().is_symlink(),
        "the destination must be the copied LINK, not the followed target"
    );
    assert_eq!(
        std::fs::read_link(tmp.join("out")).unwrap(),
        std::path::PathBuf::from("target.txt")
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
    let assert = Command::cargo_bin("rcp")
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

/// --require-toctou-safe validates the raw filesystem path part of each operand
/// (`host:` prefix stripped, tilde NOT expanded); the `localhost:/abs` colon escape
/// hatch yields an absolute `/abs`, so it is accepted.
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_accepts_localhost_colon_escape_hatch() {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let tmp = tmp.path().canonicalize().unwrap();
    std::fs::create_dir(tmp.join("src")).unwrap();
    std::fs::write(tmp.join("src/a.txt"), b"hello").unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .arg("--require-toctou-safe")
        .arg(format!("localhost:{}", tmp.join("src").display()))
        .arg(format!("localhost:{}", tmp.join("dst").display()))
        .assert()
        .success();
    assert_eq!(std::fs::read(tmp.join("dst/a.txt")).unwrap(), b"hello");
}

// ── Strict destination-prefix validation (round-5 regression matrix) ──────────
// Each of these exits 0 on the pre-fix code (the destination prefix was validated
// only conditionally / after the source filter), and must now fail closed.

/// Helper: a canonicalized tempdir with `real/` (a dir) and `link -> real` (a symlinked prefix).
#[cfg(target_os = "linux")]
fn symlinked_dst_prefix_fixture() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(base.join("src")).unwrap();
    std::fs::write(base.join("src/a.txt"), b"x").unwrap();
    std::fs::create_dir_all(base.join("real")).unwrap();
    std::os::unix::fs::symlink(base.join("real"), base.join("link")).unwrap();
    tmp
}

/// A symlinked DESTINATION prefix fails closed even when the SOURCE root is filtered out
/// (the up-front dst validation runs before the source-filter early-return).
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_refuses_symlinked_dst_prefix_when_source_filtered() {
    if !common::safedir::openat2_available() {
        return;
    }
    let tmp = symlinked_dst_prefix_fixture();
    let base = tmp.path().canonicalize().unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--require-toctou-safe", "--exclude=src"])
        .arg(base.join("src"))
        .arg(base.join("link/out"))
        .assert()
        .failure();
    assert!(!base.join("real/out").exists());
}

/// A symlinked DESTINATION prefix fails closed with a trailing-slash destination
/// (the conditional CLI "already exists" check is skipped there; the engine still validates).
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_refuses_symlinked_dst_prefix_with_trailing_slash() {
    if !common::safedir::openat2_available() {
        return;
    }
    let tmp = symlinked_dst_prefix_fixture();
    let base = tmp.path().canonicalize().unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .arg("--require-toctou-safe")
        .arg(base.join("src"))
        .arg(format!("{}/", base.join("link").display()))
        .assert()
        .failure();
    assert!(!base.join("real/src").exists());
}

/// A symlinked DESTINATION prefix fails closed with --overwrite (the CLI check is skipped there).
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_refuses_symlinked_dst_prefix_with_overwrite() {
    if !common::safedir::openat2_available() {
        return;
    }
    let tmp = symlinked_dst_prefix_fixture();
    let base = tmp.path().canonicalize().unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--require-toctou-safe", "--overwrite"])
        .arg(base.join("src"))
        .arg(base.join("link/out"))
        .assert()
        .failure();
    assert!(!base.join("real/out").exists());
}

/// A symlinked DESTINATION prefix fails closed in --dry-run (which otherwise touches no dst).
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_dry_run_refuses_symlinked_dst_prefix() {
    if !common::safedir::openat2_available() {
        return;
    }
    let tmp = symlinked_dst_prefix_fixture();
    let base = tmp.path().canonicalize().unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--require-toctou-safe", "--dry-run=brief"])
        .arg(base.join("src"))
        .arg(base.join("link/out"))
        .assert()
        .failure();
}

/// `--dry-run --delete` onto a destination that is itself a final symlink must SUCCEED (the final
/// symlink is a replaceable operand, not an intermediate-prefix violation): the dry-run
/// --ignore-existing probes are gated off, so no child spuriously fails with ELOOP.
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_dry_run_delete_onto_final_symlink_succeeds() {
    if !common::safedir::openat2_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(base.join("src")).unwrap();
    std::fs::write(base.join("src/a.txt"), b"x").unwrap();
    std::fs::write(base.join("src/b.txt"), b"y").unwrap();
    // the destination operand's FINAL component is a symlink (to a real dir); its prefix is clean
    std::fs::create_dir_all(base.join("realdst")).unwrap();
    std::os::unix::fs::symlink(base.join("realdst"), base.join("dstlink")).unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--require-toctou-safe", "--dry-run=brief", "--delete"])
        .arg(base.join("src"))
        .arg(base.join("dstlink"))
        .assert()
        .success();
}

/// A symlinked SOURCE prefix fails closed even when the source root is filtered out
/// (the source parent is opened+validated up front, before the filter early-return).
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_refuses_symlinked_src_prefix_when_filtered() {
    if !common::safedir::openat2_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(base.join("real/src")).unwrap();
    std::fs::write(base.join("real/src/a.txt"), b"x").unwrap();
    std::os::unix::fs::symlink(base.join("real"), base.join("link")).unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--require-toctou-safe", "--exclude=src"])
        .arg(base.join("link/src"))
        .arg(base.join("dst"))
        .assert()
        .failure();
    assert!(!base.join("dst").exists());
}

/// --require-toctou-safe rejects a home-relative `~/…` operand: its expansion is
/// environment-dependent, so the raw path part (`~/src`) is not absolute as written
/// and must be refused (the linter validates the pre-tilde-expansion string).
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_rejects_tilde_operand() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--require-toctou-safe", "~/src", "/abs/dst"])
        .env("HOME", "/tmp/attacker")
        .assert()
        .failure()
        .stdout(predicates::str::contains("absolute"));
}

/// The `localhost:/abs` colon escape hatch keeps its absolute path and is accepted
/// under --require-toctou-safe (regression guard alongside the tilde rejection).
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_accepts_localhost_escape_after_tilde_fix() {
    if !common::safedir::openat2_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().canonicalize().unwrap();
    std::fs::create_dir(base.join("src")).unwrap();
    std::fs::write(base.join("src/a.txt"), b"x").unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .arg("--require-toctou-safe")
        .arg(format!("localhost:{}", base.join("src").display()))
        .arg(base.join("dst"))
        .assert()
        .success();
    assert_eq!(std::fs::read(base.join("dst/a.txt")).unwrap(), b"x");
}

/// Strict `--dry-run --delete` must exit 0 (like the real run) when a destination
/// intermediate is a symlink the real run would replace with a directory — the
/// below-root prune reopen skips such entries instead of failing with ELOOP.
#[cfg(target_os = "linux")]
#[test]
fn require_toctou_safe_dry_run_delete_over_replaced_intermediate_symlink() {
    if !common::safedir::openat2_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().canonicalize().unwrap();
    std::fs::create_dir_all(base.join("src/child/grand")).unwrap();
    std::fs::write(base.join("src/child/grand/f.txt"), b"x").unwrap();
    std::fs::create_dir_all(base.join("other/grand/extra")).unwrap();
    std::fs::create_dir(base.join("dst")).unwrap();
    // dst/child is a symlink the real --overwrite run would replace with a directory
    std::os::unix::fs::symlink(base.join("other"), base.join("dst/child")).unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .args([
            "--require-toctou-safe",
            "--dry-run=brief",
            "--overwrite",
            "--delete",
        ])
        .arg(base.join("src"))
        .arg(base.join("dst"))
        .assert()
        .success();
    // the real run indeed succeeds and leaves the symlink target untouched
    std::fs::remove_dir_all(base.join("dst")).unwrap();
    std::fs::create_dir(base.join("dst")).unwrap();
    std::os::unix::fs::symlink(base.join("other"), base.join("dst/child")).unwrap();
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--require-toctou-safe", "--overwrite", "--delete"])
        .arg(base.join("src"))
        .arg(base.join("dst"))
        .assert()
        .success();
}
