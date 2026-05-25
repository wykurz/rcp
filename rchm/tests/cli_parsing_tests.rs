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
