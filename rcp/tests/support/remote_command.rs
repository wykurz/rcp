//! Shared command helpers for rcp integration tests.
//!
//! Each integration-test binary includes this module independently, so not every consumer uses
//! every helper.
#![allow(dead_code)]

/// Quote a path for the POSIX shell wrappers used by remote tests.
pub(super) fn shell_quote_for_test(value: &std::path::Path) -> String {
    format!("'{}'", value.to_string_lossy().replace('\'', "'\"'\"'"))
}

/// Interpret a command exit code for test diagnostics.
pub(super) fn interpret_exit_code(code: i32) -> String {
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

/// Exit code 124 indicates the timeout wrapper killed rcp.
const TIMEOUT_EXIT_CODE: i32 = 124;

/// Fail when the timeout wrapper, rather than rcp itself, ended the command.
pub(super) fn assert_not_timeout(output: &std::process::Output) {
    if let Some(code) = output.status.code()
        && code == TIMEOUT_EXIT_CODE
    {
        panic!(
            "rcp was killed by timeout wrapper (exit code 124). \
             This indicates rcp hung and did not complete within the time limit. \
             This is NOT the same as an expected failure from rcp."
        );
    }
}

fn run_rcp_with_args_internal(
    args: &[&str],
    home: Option<&std::path::Path>,
    extra_env: &[(&str, &str)],
) -> std::process::Output {
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    let mut cmd = std::process::Command::new("timeout");
    // 90 seconds allows SSH setup and auto-deployment for both remote sides
    cmd.args(["90", rcp_path.to_str().unwrap()]);
    cmd.arg("-vv");
    cmd.arg("--force-remote");
    cmd.args(args);
    if let Some(home) = home {
        cmd.env("HOME", home);
    }
    for (key, value) in extra_env {
        // an empty value removes the variable because unset and empty can select different paths
        if value.is_empty() {
            cmd.env_remove(key);
        } else {
            cmd.env(key, value);
        }
    }
    let output = cmd.output().expect("Failed to execute rcp command");
    // check before returning so expected-failure tests cannot mistake a hang for an rcp failure
    assert_not_timeout(&output);
    output
}

/// Run rcp verbosely in forced-remote mode with the standard timeout.
pub(super) fn run_rcp_with_args(args: &[&str]) -> std::process::Output {
    run_rcp_with_args_internal(args, None, &[])
}

/// Run rcp in forced-remote mode without adding a verbosity flag.
pub(super) fn run_rcp_with_args_at_default_verbosity(args: &[&str]) -> std::process::Output {
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    let mut cmd = std::process::Command::new("timeout");
    cmd.args(["90", rcp_path.to_str().unwrap(), "--force-remote"]);
    cmd.args(args);
    let output = cmd.output().expect("Failed to execute rcp command");
    assert_not_timeout(&output);
    output
}

/// Run rcp verbosely in forced-remote mode with a controlled HOME and environment.
pub(super) fn run_rcp_with_args_home_and_env(
    args: &[&str],
    home: &std::path::Path,
    envs: &[(&str, &str)],
) -> std::process::Output {
    run_rcp_with_args_internal(args, Some(home), envs)
}

/// Run rcp verbosely without forcing localhost operands through SSH.
pub(super) fn run_rcp_without_force_remote(args: &[&str]) -> std::process::Output {
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    let mut cmd = std::process::Command::new("timeout");
    cmd.args(["30", rcp_path.to_str().unwrap()]);
    cmd.arg("-vv");
    cmd.args(args);
    let output = cmd.output().expect("Failed to execute rcp command");
    assert_not_timeout(&output);
    output
}

/// Print captured command output with an interpreted exit status.
pub(super) fn print_command_output(output: &std::process::Output) {
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
