//! Local and pre-SSH remote behavior tests.
//!
//! These checks either treat localhost paths as local or fail validation before any SSH setup, so
//! they remain active in the Nix sandbox.

use std::os::unix::fs::PermissionsExt;

#[path = "support/fixtures.rs"]
mod fixtures;
#[path = "support/remote_log.rs"]
mod remote_log;

use fixtures::{create_test_file, get_file_content, setup_test_env};
use remote_log::rcpd_role_hellos_received;

fn assert_not_timeout(output: &std::process::Output) {
    assert_ne!(
        output.status.code(),
        Some(124),
        "rcp was killed by the 90-second timeout wrapper"
    );
}

fn run_rcp_with_args(args: &[&str]) -> std::process::Output {
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    let mut cmd = std::process::Command::new("timeout");
    cmd.args(["90", rcp_path.to_str().unwrap()]);
    cmd.arg("-vv");
    cmd.arg("--force-remote");
    cmd.args(args);
    let output = cmd.output().expect("Failed to execute rcp command");
    assert_not_timeout(&output);
    output
}

fn run_rcp_without_force_remote(args: &[&str]) -> std::process::Output {
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    let mut cmd = std::process::Command::new("timeout");
    cmd.args(["30", rcp_path.to_str().unwrap()]);
    cmd.arg("-vv");
    cmd.args(args);
    cmd.output().expect("Failed to execute rcp command")
}

fn shell_quote_for_test(value: &std::path::Path) -> String {
    format!("'{}'", value.to_string_lossy().replace('\'', "'\"'\"'"))
}

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

fn interpret_exit_code(code: i32) -> String {
    match code {
        0 => "Success".to_string(),
        1 => "General error".to_string(),
        2 => "Misuse of shell command".to_string(),
        124 => "Timeout (command exceeded time limit)".to_string(),
        125 => "Command not found".to_string(),
        126 => "Command found but not executable".to_string(),
        127 => "Command not found (PATH issue)".to_string(),
        128 => "Invalid exit argument".to_string(),
        130 => "Terminated by Ctrl+C (SIGINT)".to_string(),
        137 => "Killed by SIGKILL".to_string(),
        143 => "Terminated by SIGTERM".to_string(),
        code if code >= 128 => format!("Terminated by signal {}", code - 128),
        code => format!("Exit code {code}"),
    }
}

fn print_command_output(output: &std::process::Output) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("=== RCP COMMAND OUTPUT ===");
    if let Some(code) = output.status.code() {
        eprintln!("Exit status: {} ({})", code, interpret_exit_code(code));
    } else {
        eprintln!("Exit status: terminated by signal");
    }
    if !stdout.is_empty() {
        eprintln!("--- STDOUT ---");
        eprintln!("{stdout}");
    }
    if !stderr.is_empty() {
        eprintln!("--- STDERR ---");
        eprintln!("{stderr}");
    }
    eprintln!("=== END RCP OUTPUT ===");
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
fn test_remote_automatic_capacity_failure_precedes_remote_side_effects() {
    let scratch = tempfile::tempdir().unwrap();
    let destination = scratch.path().join("destination");
    let multiplier = format!("--pending-writes-multiplier={}", usize::MAX);
    let output = run_rcp_with_args(&[
        &multiplier,
        "unreachable.invalid:~/source",
        destination.to_str().unwrap(),
    ]);
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
fn test_remote_localhost_without_force_remote_is_local() {
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
fn test_remote_pure_local_invalid_glob_precedes_home_and_probe() {
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
fn test_remote_pure_local_unreadable_filter_file_precedes_home_and_probe() {
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
