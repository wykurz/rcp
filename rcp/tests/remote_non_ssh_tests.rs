#[path = "support/remote_log.rs"]
mod remote_log;

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
