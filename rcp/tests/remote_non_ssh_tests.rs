//! Local and pre-SSH remote behavior tests.
//!
//! These checks either treat localhost paths as local or fail validation before any SSH setup, so
//! they remain active in the Nix sandbox.

use std::os::unix::fs::PermissionsExt;

#[path = "support/fixtures.rs"]
mod fixtures;
#[path = "support/remote_command.rs"]
mod remote_command;
#[path = "support/remote_log.rs"]
mod remote_log;

use fixtures::{create_test_file, get_file_content, setup_test_env};
use remote_command::{
    print_command_output, run_rcp_with_args, run_rcp_without_force_remote, shell_quote_for_test,
};
use remote_log::rcpd_role_hellos_received;

fn marking_rcpd_wrapper(directory: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let wrapper = directory.join("marking-rcpd");
    let marker = directory.join("rcpd-invocations");
    let rcpd = assert_cmd::cargo::cargo_bin("rcpd");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> {}\nexec {} \"$@\"\n",
            shell_quote_for_test(&marker),
            shell_quote_for_test(&rcpd),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    (wrapper, marker)
}

#[test]
fn rcpd_role_hello_readiness_requires_both_roles() {
    let log_dir = tempfile::TempDir::new().expect("Failed to create rcpd debug log dir");
    std::fs::write(
        log_dir.path().join("source.log"),
        "Received side: Source { ... }",
    )
    .expect("Failed to write source rcpd log");
    assert!(!rcpd_role_hellos_received(log_dir.path()));

    std::fs::write(
        log_dir.path().join("destination.log"),
        "Received side: Destination { ... }",
    )
    .expect("Failed to write destination rcpd log");
    assert!(rcpd_role_hellos_received(log_dir.path()));
}

#[test]
fn automatic_capacity_failure_precedes_remote_side_effects() {
    let scratch = tempfile::tempdir().unwrap();
    let destination = scratch.path().join("destination");
    let multiplier = format!("--pending-writes-multiplier={}", usize::MAX);
    let output = run_rcp_with_args(&[
        &multiplier,
        "unreachable.invalid:~/source",
        destination.to_str().unwrap(),
    ]);
    print_command_output(&output);
    assert!(
        !output.status.success(),
        "invalid automatic capacity must fail"
    );
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("pending file capacity overflow"),
        "initiating rcp must report the master-side capacity error: {combined}"
    );
    assert!(
        !combined.contains("Connecting to SSH destination"),
        "capacity validation must precede SSH setup and remote HOME lookup: {combined}"
    );
    assert!(
        !combined.contains("Starting prepared rcpd server on:"),
        "capacity validation must precede rcpd spawn: {combined}"
    );
}

#[test]
fn localhost_without_force_remote_is_local() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "local copy test", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output = run_rcp_without_force_remote(&[&src_remote, &dst_remote]);
    print_command_output(&output);
    assert!(output.status.success(), "Copy should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Paths with 'localhost:' prefix are treated as local"),
        "Expected localhost warning in output, got: {stdout}"
    );
    assert!(
        !stdout.contains("Starting prepared rcpd server on:"),
        "Should NOT use rcpd without --force-remote, but got: {stdout}"
    );
    assert_eq!(get_file_content(&dst_file), "local copy test");
}

#[test]
fn pure_local_invalid_glob_precedes_home_and_probe() {
    let scratch = tempfile::tempdir().unwrap();
    let (wrapper, marker) = marking_rcpd_wrapper(scratch.path());
    let rcpd_path = format!("--rcpd-path={}", wrapper.display());
    let destination = scratch.path().join("destination");
    let output = run_rcp_with_args(&[
        &rcpd_path,
        "--include=[",
        "localhost:~/source",
        destination.to_str().unwrap(),
    ]);
    print_command_output(&output);
    assert!(!output.status.success(), "invalid glob must fail");
    assert!(
        !String::from_utf8_lossy(&output.stdout).contains("Connecting to SSH destination"),
        "invalid local filter configuration must precede remote HOME SSH"
    );
    assert!(
        !marker.exists(),
        "invalid initiating-host configuration must fail before any rcpd probe"
    );
}

#[test]
fn pure_local_unreadable_filter_file_precedes_home_and_probe() {
    let scratch = tempfile::tempdir().unwrap();
    let (wrapper, marker) = marking_rcpd_wrapper(scratch.path());
    let unreadable = scratch.path().join("filter-is-a-directory");
    std::fs::create_dir(&unreadable).unwrap();
    let rcpd_path = format!("--rcpd-path={}", wrapper.display());
    let filter_file = format!("--filter-file={}", unreadable.display());
    let destination = scratch.path().join("destination");
    let output = run_rcp_with_args(&[
        &rcpd_path,
        &filter_file,
        "localhost:~/source",
        destination.to_str().unwrap(),
    ]);
    print_command_output(&output);
    assert!(!output.status.success(), "unreadable filter file must fail");
    assert!(
        !String::from_utf8_lossy(&output.stdout).contains("Connecting to SSH destination"),
        "unreadable local filter configuration must precede remote HOME SSH"
    );
    assert!(
        !marker.exists(),
        "unreadable initiating-host filter must fail before any rcpd probe"
    );
}
