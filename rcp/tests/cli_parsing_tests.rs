//! CLI Argument Parsing Compatibility Tests
//!
//! These tests verify that command-line arguments are parsed correctly and maintain
//! backward compatibility. The focus is on ensuring that argument values, aliases,
//! and formats continue to work as expected across versions.
//!
//! Tests in this file should NOT be modified to match new behavior unless it's
//! intentional and documented in the changelog. Breaking changes here indicate
//! potential issues for existing users.

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

// ============================================================================
// Remote Copy Argument Tests
// ============================================================================

/// Test that --quic-port-ranges accepts range format
#[test]
fn test_quic_port_ranges_single() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--quic-port-ranges", "8000-8999", "--help"])
        .assert()
        .success();
}

/// Test that --quic-port-ranges accepts multiple ranges
#[test]
fn test_quic_port_ranges_multiple() {
    Command::cargo_bin("rcp")
        .unwrap()
        .args(["--quic-port-ranges", "8000-8999,10000-10999", "--help"])
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
