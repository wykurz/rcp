//! Localhost-SSH integration tests.
//!
//! The Nix build sandbox has no localhost SSH service, so this homogeneous target is excluded
//! when `rcp_nix_sandbox` is active. Ordinary and CI test runs do not set that cfg.

#![cfg(not(rcp_nix_sandbox))]

use std::os::unix::fs::PermissionsExt;

#[path = "support/acl.rs"]
mod acl;
#[path = "support/fixtures.rs"]
mod fixtures;
#[path = "support/remote_command.rs"]
mod remote_command;
#[path = "support/remote_log.rs"]
mod remote_log;
use fixtures::{
    create_test_file, describe_samples, get_file_content, get_file_mode, sample_while_running,
    setup_test_env,
};
use remote_command::{
    assert_not_timeout, interpret_exit_code, print_command_output, run_rcp_with_args,
    run_rcp_with_args_at_default_verbosity, run_rcp_with_args_home_and_env, shell_quote_for_test,
};
use remote_log::{rcpd_logs_contain, rcpd_role_hellos_received};

fn cache_bin_dir(home: &std::path::Path) -> std::path::PathBuf {
    home.join(".cache/rcp/bin")
}

fn link_real_ssh_dir(temp_home: &std::path::Path) {
    if let Ok(real_home) = std::env::var("HOME") {
        let ssh_src = std::path::Path::new(&real_home).join(".ssh");
        let ssh_dest = temp_home.join(".ssh");
        if ssh_src.exists() && !ssh_dest.exists() {
            // allow SSH to find existing keys/known_hosts when we override HOME
            let _ = std::os::unix::fs::symlink(&ssh_src, &ssh_dest);
        }
    }
}

fn make_test_home() -> tempfile::TempDir {
    let temp_home = tempfile::tempdir().unwrap();
    link_real_ssh_dir(temp_home.path());
    temp_home
}

/// A test HOME deliberately longer than the ~48 bytes that the SSH control socket path used to
/// leave for it. Rooted at `/tmp` rather than at the ambient temp dir so its length is a property
/// of this fixture and not of whatever `TMPDIR` the caller happens to have -- under nix-shell, for
/// example, `TMPDIR` alone is already 39 bytes, which is how this bug was found.
fn make_long_test_home() -> tempfile::TempDir {
    let temp_home = tempfile::Builder::new()
        .prefix("rcp-home-long-enough-to-overflow-the-ssh-control-socket-")
        .tempdir_in("/tmp")
        .unwrap();
    link_real_ssh_dir(temp_home.path());
    temp_home
}

fn local_ssh_available() -> bool {
    static SSH_AVAILABLE: std::sync::OnceLock<(bool, String)> = std::sync::OnceLock::new();
    let (ok, msg) = SSH_AVAILABLE.get_or_init(|| {
        match std::process::Command::new("ssh")
            .args(["-o", "BatchMode=yes", "localhost", "true"])
            .output()
        {
            Ok(output) => (
                output.status.success(),
                format!(
                    "ssh exit: {:?}, stdout: {}, stderr: {}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ),
            ),
            Err(err) => (false, format!("failed to invoke ssh: {err:#}")),
        }
    });
    if !ok {
        eprintln!("localhost ssh check failed: {msg}");
    }
    *ok
}

fn require_local_ssh() {
    assert!(
        local_ssh_available(),
        "localhost SSH is required for remote tests. Please ensure sshd is running and accessible."
    );
}

#[tokio::test]
async fn remote_scoped_timings_cover_single_pass_discovery_and_all_process_roles() {
    require_local_ssh();
    static PROGRESS: std::sync::LazyLock<common::progress::Progress> =
        std::sync::LazyLock::new(common::progress::Progress::new);
    let fixture = tempfile::tempdir().unwrap();
    let src = fixture.path().join("source");
    std::fs::create_dir_all(src.join("nested")).unwrap();
    std::fs::create_dir(src.join("empty")).unwrap();
    for name in ["root-file", "nested/child-file"] {
        common::filegen::write_file(&PROGRESS, src.join(name), 1024, 1024, 0)
            .await
            .unwrap();
    }
    for (detail, dereference, destination) in [
        (false, false, "coarse-hardened"),
        (true, false, "detail-hardened"),
        (true, true, "detail-dereferenced"),
    ] {
        let artifacts = tempfile::tempdir().unwrap();
        let dst = fixture.path().join(destination);
        let src_remote = format!("localhost:{}", src.display());
        let dst_remote = format!("localhost:{}", dst.display());
        let daemon = format!(
            "--rcpd-path={}",
            assert_cmd::cargo::cargo_bin("rcpd").display()
        );
        let prefix = artifacts.path().join("profile");
        let timings = format!("--timings={}", prefix.display());
        let chrome = format!("--chrome-trace={}", prefix.display());
        let mut args = vec![daemon.as_str(), timings.as_str()];
        if dereference {
            args.push("-L");
        }
        if detail {
            args.extend(["--timings-detail", chrome.as_str()]);
        }
        args.extend([src_remote.as_str(), dst_remote.as_str()]);
        let output = run_rcp_with_args_at_default_verbosity(&args);
        print_command_output(&output);
        assert!(output.status.success());
        for name in ["root-file", "nested/child-file"] {
            assert_eq!(
                std::fs::read(src.join(name)).unwrap(),
                std::fs::read(dst.join(name)).unwrap()
            );
        }
        let paths: Vec<_> = std::fs::read_dir(artifacts.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        let reports: Vec<serde_json::Value> = paths
            .iter()
            .filter(|path| path.to_string_lossy().ends_with(".timings.json"))
            .map(|path| serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap())
            .collect();
        assert_eq!(reports.len(), 3);
        for role in ["rcp-master", "rcpd-source", "rcpd-destination"] {
            let report = reports
                .iter()
                .find(|report| report["identifier"] == role)
                .unwrap();
            assert_eq!(report["schema_version"], 1);
            let scopes = report["scopes"].as_array().unwrap();
            let operation = scopes
                .iter()
                .find(|scope| scope["name"] == "operation")
                .unwrap();
            assert_eq!(operation["count"], 1);
            assert_eq!(operation["finished"], 1);
            assert_eq!(operation["interrupted"], 0);
            let metadata_operation = match role {
                "rcpd-source" => Some("source.metadata.stat"),
                "rcpd-destination" => Some("destination.metadata.open-create"),
                _ => None,
            };
            if let Some(operation) = metadata_operation {
                let mut counts = Vec::new();
                for phase in ["wait_rate", "wait_admission", "wait_worker", "execute"] {
                    let name = format!("{operation}.{phase}");
                    let scope = scopes.iter().find(|scope| scope["name"] == name);
                    assert_eq!(scope.is_some(), detail, "{name}");
                    if let Some(scope) = scope {
                        let count = scope["count"].as_u64().unwrap();
                        assert!(count > 0, "{name}");
                        assert_eq!(scope["finished"], count, "{name}");
                        assert_eq!(scope["interrupted"], 0, "{name}");
                        counts.push(count);
                    }
                }
                if detail {
                    assert_eq!(
                        counts[0], counts[1],
                        "each metadata operation in this remote fixture requests a rate token"
                    );
                    assert_eq!(counts[1], counts[3], "every admitted operation executes");
                    assert!(
                        counts[2] <= counts[3],
                        "only blocking work has a worker scope"
                    );
                    let async_stats = if role == "rcpd-source" && dereference {
                        5
                    } else {
                        0
                    };
                    assert_eq!(
                        counts[3] - counts[2],
                        async_stats,
                        "-L classifies the root, two directories, and two files through async stat"
                    );
                }
            }
            if role == "rcpd-source" {
                for (name, count) in [("source.discovery", 1), ("source.directory.scan", 3)] {
                    let scope = scopes.iter().find(|scope| scope["name"] == name).unwrap();
                    assert_eq!(scope["count"], count, "{name}");
                    assert_eq!(scope["interrupted"], 0, "{name}");
                }
                assert_eq!(
                    scopes
                        .iter()
                        .any(|scope| scope["name"] == "source.file.send"),
                    detail
                );
            }
            if role == "rcpd-destination" {
                let prepare = scopes
                    .iter()
                    .find(|scope| scope["name"] == "destination.directory.prepare")
                    .unwrap();
                assert_eq!(prepare["count"], 3, "root, nested, and empty are prepared");
                assert_eq!(prepare["finished"], 3);
                assert_eq!(prepare["interrupted"], 0);
                let finalizations: Vec<_> = scopes
                    .iter()
                    .filter(|scope| {
                        matches!(
                            scope["name"].as_str(),
                            Some(
                                "destination.directory.finalize.control"
                                    | "destination.directory.finalize.announce"
                                    | "destination.directory.finalize.data"
                            )
                        )
                    })
                    .collect();
                let mut cascades = 0;
                for scope in finalizations {
                    let count = scope["count"].as_u64().unwrap();
                    assert_eq!(scope["finished"], count);
                    assert_eq!(scope["interrupted"], 0);
                    cascades += count;
                }
                assert!((1..=3).contains(&cascades));
                for name in [
                    "destination.tracker.access",
                    "destination.directory.finalize.metadata",
                ] {
                    let scope = scopes.iter().find(|scope| scope["name"] == name);
                    assert_eq!(scope.is_some(), detail, "{name}");
                    if let Some(scope) = scope {
                        let count = scope["count"].as_u64().unwrap();
                        if name == "destination.directory.finalize.metadata" {
                            assert_eq!(count, 3, "root, nested, and empty each finalize once");
                        } else {
                            assert!(count > 0);
                        }
                        assert_eq!(scope["finished"], count, "{name}");
                        assert_eq!(scope["interrupted"], 0, "{name}");
                    }
                }
                assert!(
                    !scopes
                        .iter()
                        .any(|scope| scope["name"] == "destination.directory.finalize.prune"),
                    "ordinary empty directories are retained"
                );
                for stage in [
                    "wait_open",
                    "wait_rate",
                    "parent",
                    "plan",
                    "wait_iops",
                    "create",
                    "receive",
                    "flush",
                    "metadata",
                    "complete",
                ] {
                    let name = format!("destination.file.{stage}");
                    let scope = scopes.iter().find(|scope| scope["name"] == name);
                    assert_eq!(scope.is_some(), detail, "{name}");
                    if let Some(scope) = scope {
                        assert_eq!(scope["count"], 2, "{name}");
                        assert_eq!(scope["finished"], 2, "{name}");
                        assert_eq!(scope["interrupted"], 0, "{name}");
                    }
                }
            }
        }
        if detail {
            assert_eq!(
                paths.len(),
                9,
                "each process writes summary, poll and elapsed artifacts"
            );
            for path in paths
                .iter()
                .filter(|path| path.to_string_lossy().ends_with(".scopes.json"))
            {
                let events: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
                assert!(
                    events
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|event| event["name"] == "operation")
                );
            }
        }
    }
}

fn assert_two_rcpd_logs_report_connection_count(
    log_dir: &std::path::Path,
    expected_connections: usize,
) {
    let logs: Vec<_> = std::fs::read_dir(log_dir)
        .expect("rcpd log directory must be readable")
        .map(|entry| entry.expect("rcpd log entry must be readable").path())
        .filter(|path| path.is_file())
        .collect();
    assert_eq!(logs.len(), 2, "source and destination rcpd must each log");
    let expected = format!("Effective remote connection count: {expected_connections}");
    for log in logs {
        let contents = std::fs::read_to_string(&log).expect("rcpd log must be readable");
        assert!(
            contents.contains(&expected),
            "{} did not report {expected}",
            log.display()
        );
    }
}

#[test]
fn remote_timing_flush_failure_fails_master_after_copy_completes() {
    require_local_ssh();
    let fixture = tempfile::tempdir().unwrap();
    let src = fixture.path().join("source-file");
    let dst = fixture.path().join("destination-file");
    create_test_file(&src, "copied before timing flush", 0o644);
    let wrapper = fixture.path().join("limited-rcpd");
    let real_daemon = shell_quote_for_test(&assert_cmd::cargo::cargo_bin("rcpd"));
    // allow artifact creation, then force writes to fail at daemon shutdown. Ignoring SIGXFSZ
    // turns the source's zero file-size limit into EFBIG instead of killing the process.
    std::fs::write(&wrapper, format!(
        "#!/bin/sh\nif [ \"$1\" = --role ] && [ \"$2\" = source ]; then\n  trap '' XFSZ\n  ulimit -f 0\nfi\nexec {real_daemon} \"$@\"\n"
    )).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let daemon_arg = format!("--rcpd-path={}", wrapper.display());
    let timing_arg = format!("--timings={}", fixture.path().join("timing").display());
    let src_remote = format!("localhost:{}", src.display());
    let dst_remote = format!("localhost:{}", dst.display());
    let output = run_rcp_with_args_at_default_verbosity(&[
        &daemon_arg,
        &timing_arg,
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert_eq!(get_file_content(&dst), "copied before timing flush");
    assert!(
        !output.status.success(),
        "a late daemon timing failure must fail the master"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("Failed to write scoped timings"),
        "{combined}"
    );
}

fn run_rcp_and_expect_success(args: &[&str]) -> std::process::Output {
    let output = run_rcp_with_args(args);
    print_command_output(&output);
    // note: timeout check is already done in run_rcp_with_args_internal
    if !output.status.success() {
        if let Some(code) = output.status.code() {
            panic!(
                "Command failed with exit code {} ({})",
                code,
                interpret_exit_code(code)
            );
        } else {
            panic!("Command failed - terminated by signal");
        }
    }
    output
}

fn run_rcp_and_expect_failure(args: &[&str]) -> std::process::Output {
    let output = run_rcp_with_args(args);
    print_command_output(&output);
    // note: timeout check is already done in run_rcp_with_args_internal
    assert!(
        !output.status.success(),
        "Command succeeded when failure was expected"
    );
    output
}

macro_rules! parse_field {
    ($line:expr, $prefix:expr, $target:expr, $found_any:expr) => {
        if let Some(value) = $line.strip_prefix($prefix) {
            $target = value.parse().ok()?;
            $found_any = true;
            continue;
        }
    };
}

#[rustfmt::skip]
fn parse_summary_from_output(output: &std::process::Output) -> Option<common::copy::Summary> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut summary = common::copy::Summary::default();
    let mut found_any = false;
    for line in stdout.lines() {
        // special handling for bytes_copied which has a unit suffix (e.g., "40 B")
        if let Some(value_str) = line.strip_prefix("bytes copied: ") {
            // strip unit suffix by taking only the numeric part
            if let Some(num_str) = value_str.split_whitespace().next() {
                summary.bytes_copied = num_str.parse().ok()?;
                found_any = true;
                continue;
            }
        }
        parse_field!(line, "files copied: ", summary.files_copied, found_any);
        parse_field!(line, "symlinks created: ", summary.symlinks_created, found_any);
        parse_field!(line, "directories created: ", summary.directories_created, found_any);
        parse_field!(line, "files unchanged: ", summary.files_unchanged, found_any);
        parse_field!(line, "symlinks unchanged: ", summary.symlinks_unchanged, found_any);
        parse_field!(line, "directories unchanged: ", summary.directories_unchanged, found_any);
        parse_field!(line, "files removed: ", summary.rm_summary.files_removed, found_any);
        parse_field!(line, "symlinks removed: ", summary.rm_summary.symlinks_removed, found_any);
        parse_field!(line, "directories removed: ", summary.rm_summary.directories_removed, found_any);
        parse_field!(line, "files skipped: ", summary.files_skipped, found_any);
        parse_field!(line, "symlinks skipped: ", summary.symlinks_skipped, found_any);
        parse_field!(line, "directories skipped: ", summary.directories_skipped, found_any);
        parse_field!(line, "specials skipped: ", summary.specials_skipped, found_any);
        // special handling for bytes_removed which has a unit suffix (e.g., "40 B")
        if let Some(value_str) = line.strip_prefix("bytes removed: ")
            && let Some(num_str) = value_str.split_whitespace().next()
        {
            summary.rm_summary.bytes_removed = num_str.parse().ok()?;
            found_any = true;
            continue;
        }
        // If no prefix matched, do nothing.
    }
    if found_any {
        Some(summary)
    } else {
        None
    }
}

/// A long `$HOME` must not break remote copies.
///
/// The SSH connection-multiplexing socket used to live under `$HOME/.local/state`, and
/// `sockaddr_un` caps the entire socket path at 108 bytes -- a budget that also has to cover the
/// `/.ssh-connectionXXXXXX/master` the ssh library appends and the further `.XXXXXXXXXXXXXXXX`
/// that ssh(1) itself appends while creating the socket before renaming it. That left roughly 48
/// bytes for `$HOME`; anything longer died with
///
///     unix_listener: path "..." too long for Unix domain socket
///
/// which names neither `$HOME` nor rcp. 48 bytes is not much: container workspaces, network homes
/// and nested temp dirs all exceed it, and this was originally hit by nine tests at once simply
/// because `TMPDIR` under nix-shell is 39 bytes before the fixture adds anything.
///
/// Both environment variables are set explicitly rather than inherited, so the test asserts the
/// same thing on every machine: a home long enough to have failed before, and a runtime dir short
/// enough that the fix has somewhere to put the socket.
#[test]
fn test_remote_copy_with_a_home_too_long_for_the_ssh_control_socket() {
    require_local_ssh();
    let home = make_long_test_home();
    // Guard the fixture itself: if the prefix above were ever shortened, this test would keep
    // passing while no longer exercising anything.
    let home_len = home.path().as_os_str().len();
    assert!(
        home_len > 48,
        "fixture home is only {home_len} bytes, too short to exercise the old limit: {}",
        home.path().display()
    );
    let runtime_dir = tempfile::Builder::new()
        .prefix("rt")
        .tempdir_in("/tmp")
        .unwrap();

    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("long_home.txt");
    let dst_file = dst_dir.path().join("long_home.txt");
    create_test_file(&src_file, "long home content", 0o644);
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());

    let output = run_rcp_with_args_home_and_env(
        &[src_file.to_str().unwrap(), &dst_remote],
        home.path(),
        &[("XDG_RUNTIME_DIR", runtime_dir.path().to_str().unwrap())],
    );
    print_command_output(&output);
    assert!(
        output.status.success(),
        "remote copy failed with a {home_len}-byte HOME"
    );
    assert_eq!(get_file_content(&dst_file), "long home content");
}

/// The same long `$HOME`, but with no `$XDG_RUNTIME_DIR` at all.
///
/// This is the case that actually matters in production and the one the first fix missed. The
/// environments cited as motivation for caring about long homes -- containers, CI runners, `su`
/// sessions -- are precisely the ones that tend not to set `$XDG_RUNTIME_DIR`, so a fix that only
/// consulted that variable would have helped desktop users, who rarely have a long home anyway,
/// and left everyone else on the broken path. Here the socket has to land in the temp-dir fallback
/// instead.
#[test]
fn test_remote_copy_with_a_long_home_and_no_runtime_dir() {
    require_local_ssh();
    let home = make_long_test_home();
    let home_len = home.path().as_os_str().len();
    assert!(
        home_len > 48,
        "fixture home is only {home_len} bytes, too short to exercise the old limit: {}",
        home.path().display()
    );

    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("no_runtime_dir.txt");
    let dst_file = dst_dir.path().join("no_runtime_dir.txt");
    create_test_file(&src_file, "no runtime dir content", 0o644);
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());

    let output = run_rcp_with_args_home_and_env(
        &[src_file.to_str().unwrap(), &dst_remote],
        home.path(),
        // Empty value means env_remove: genuinely unset, not set-to-empty.
        &[("XDG_RUNTIME_DIR", "")],
    );
    print_command_output(&output);
    assert!(
        output.status.success(),
        "remote copy failed with a {home_len}-byte HOME and no XDG_RUNTIME_DIR"
    );
    assert_eq!(get_file_content(&dst_file), "no runtime dir content");
}

#[test]
fn test_remote_copy_basic() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "remote test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_success(&[&src_remote, &dst_remote]);
}

/// Test remote copy with --no-encryption flag (plain TCP, no TLS)
#[test]
fn test_remote_copy_no_encryption() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "no encryption test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_success(&["--no-encryption", &src_remote, &dst_remote]);
    assert_eq!(get_file_content(&dst_file), "no encryption test content");
}

#[test]
fn test_remote_copy_localhost() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "remote test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_success(&[&src_remote, &dst_remote]);
    assert_eq!(get_file_content(&dst_file), "remote test content");
}

#[test]
fn test_remote_copy_tilde_source_to_local() {
    require_local_ssh();
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    let src_file = home.path().join("tilde_source.txt");
    create_test_file(&src_file, "tilde home content", 0o644);
    let dst_dir = tempfile::tempdir().unwrap();
    let dst_file = dst_dir.path().join("tilde_source.txt");
    let src_remote = "localhost:~/tilde_source.txt".to_string();
    let output = run_rcp_with_args_home_and_env(
        &[&src_remote, dst_file.to_str().unwrap()],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output);
    assert!(output.status.success());
    assert_eq!(get_file_content(&dst_file), "tilde home content");
}

#[test]
fn test_remote_copy_local_to_tilde_destination() {
    require_local_ssh();
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    let (src_dir, _) = setup_test_env();
    let src_file = src_dir.path().join("tilde_dest.txt");
    create_test_file(&src_file, "tilde dest content", 0o644);
    let dst_remote = "localhost:~/tilde_dest.txt".to_string();
    let output = run_rcp_with_args_home_and_env(
        &[src_file.to_str().unwrap(), &dst_remote],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output);
    assert!(output.status.success());
    let remote_dst = home.path().join("tilde_dest.txt");
    assert_eq!(get_file_content(&remote_dst), "tilde dest content");
}

#[test]
fn test_remote_copy_local_to_tilde_home_directory() {
    require_local_ssh();
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    let (src_dir, _) = setup_test_env();
    let src_file = src_dir.path().join("tilde_home_dir.txt");
    create_test_file(&src_file, "tilde home dir content", 0o644);
    let dst_remote = "localhost:~/".to_string();
    let output = run_rcp_with_args_home_and_env(
        &[src_file.to_str().unwrap(), &dst_remote],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output);
    assert!(output.status.success());
    let remote_dst = home.path().join("tilde_home_dir.txt");
    assert_eq!(get_file_content(&remote_dst), "tilde home dir content");
}

#[test]
fn test_remote_copy_localhost_to_local() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("source.txt");
    let dst_file = dst_dir.path().join("destination.txt");
    create_test_file(&src_file, "localhost to local content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    run_rcp_and_expect_success(&[&src_remote, dst_file.to_str().unwrap()]);
    assert_eq!(get_file_content(&dst_file), "localhost to local content");
}

#[test]
fn test_remote_copy_local_to_localhost() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("local_source.txt");
    let dst_file = dst_dir.path().join("remote_destination.txt");
    create_test_file(&src_file, "local to localhost content", 0o644);
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_success(&[src_file.to_str().unwrap(), &dst_remote]);
    assert_eq!(get_file_content(&dst_file), "local to localhost content");
}

#[test]
fn test_remote_copy_with_preserve() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("preserve_test.txt");
    let dst_file = dst_dir.path().join("preserve_test.txt");
    create_test_file(&src_file, "preserve permissions content", 0o755);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_success(&["--preserve", &src_remote, &dst_remote]);
    assert_eq!(get_file_content(&dst_file), "preserve permissions content");
    let mode = std::fs::metadata(&dst_file).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode, 0o755);
}

#[test]
fn test_remote_copy_directory() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_subdir = src_dir.path().join("remote_subdir");
    let dst_subdir = dst_dir.path().join("remote_subdir");
    std::fs::create_dir(&src_subdir).unwrap();
    let src_file1 = src_subdir.join("file1.txt");
    let src_file2 = src_subdir.join("file2.txt");
    create_test_file(&src_file1, "remote dir content 1", 0o644);
    create_test_file(&src_file2, "remote dir content 2", 0o755);
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--preserve", "--summary", &src_remote, &dst_remote]);
    let dst_file1 = dst_subdir.join("file1.txt");
    let dst_file2 = dst_subdir.join("file2.txt");
    assert_eq!(get_file_content(&dst_file1), "remote dir content 1");
    assert_eq!(get_file_content(&dst_file2), "remote dir content 2");
    let mode1 = std::fs::metadata(&dst_file1).unwrap().permissions().mode() & 0o7777;
    let mode2 = std::fs::metadata(&dst_file2).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode1, 0o644);
    assert_eq!(mode2, 0o755);
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 2);
    assert_eq!(summary.directories_created, 1);
    assert_eq!(summary.bytes_copied, 40); // "remote dir content 1" (20) + "remote dir content 2" (20)
}

#[test]
fn test_remote_max_files_in_flight_clamps_both_daemons() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("finite_clamp.txt");
    let dst_file = dst_dir.path().join("finite_clamp.txt");
    create_test_file(&src_file, "finite clamp", 0o644);
    let src_remote = format!("localhost:{}", src_file.display());
    let dst_remote = format!("localhost:{}", dst_file.display());
    let logs = tempfile::tempdir().unwrap();
    let log_prefix = logs.path().join("rcpd");
    let log_arg = format!("--rcpd-debug-log-prefix={}", log_prefix.display());
    let output = run_rcp_with_args(&[
        "--max-files-in-flight=1",
        "--max-connections=4",
        &log_arg,
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(output.status.success(), "finite-clamped remote copy failed");
    assert_eq!(get_file_content(&dst_file), "finite clamp");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("Effective remote connection count: 1"),
        "the master must clamp four configured connections to the finite file ceiling"
    );
    assert_two_rcpd_logs_report_connection_count(logs.path(), 1);
}

#[test]
fn test_remote_legacy_unlimited_warns_once_from_initiating_rcp() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("legacy_unlimited.txt");
    let dst_file = dst_dir.path().join("legacy_unlimited.txt");
    create_test_file(&src_file, "legacy unlimited", 0o644);
    let src_remote = format!("localhost:{}", src_file.display());
    let dst_remote = format!("localhost:{}", dst_file.display());
    let logs = tempfile::tempdir().unwrap();
    let log_prefix = logs.path().join("rcpd");
    let log_arg = format!("--rcpd-debug-log-prefix={}", log_prefix.display());
    let output = run_rcp_with_args(&[
        "--max-open-files=0",
        "--max-connections=3",
        &log_arg,
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(
        output.status.success(),
        "legacy-unlimited remote copy failed"
    );
    assert_eq!(get_file_content(&dst_file), "legacy unlimited");
    let warning = "--max-open-files=0 is deprecated";
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stdout.matches(warning).count(),
        1,
        "the initiating rcp must emit exactly one traced deprecation warning"
    );
    assert_eq!(
        stderr.matches(warning).count(),
        0,
        "startup notices must not bypass tracing on raw stderr"
    );
    assert_two_rcpd_logs_report_connection_count(logs.path(), 3);
}

#[test]
fn test_remote_explicit_unlimited_uses_connection_ceiling_without_deprecation() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("explicit_unlimited.txt");
    let dst_file = dst_dir.path().join("explicit_unlimited.txt");
    create_test_file(&src_file, "explicit unlimited", 0o644);
    let src_remote = format!("localhost:{}", src_file.display());
    let dst_remote = format!("localhost:{}", dst_file.display());
    let logs = tempfile::tempdir().unwrap();
    let log_prefix = logs.path().join("rcpd");
    let log_arg = format!("--rcpd-debug-log-prefix={}", log_prefix.display());
    let output = run_rcp_with_args(&[
        "--max-files-in-flight=unlimited",
        "--max-connections=3",
        &log_arg,
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(
        output.status.success(),
        "explicit-unlimited remote copy failed"
    );
    assert_eq!(get_file_content(&dst_file), "explicit unlimited");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let deprecated_option = "--max-open-files";
    assert!(
        !stdout.contains(deprecated_option) && !stderr.contains(deprecated_option),
        "the canonical unlimited spelling must not emit a deprecation warning"
    );
    assert_two_rcpd_logs_report_connection_count(logs.path(), 3);
}

#[test]
fn test_remote_chrome_trace_dry_run_forwards_daemon_artifact_notices() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("chrome_trace.txt");
    let dst_file = dst_dir.path().join("chrome_trace.txt");
    create_test_file(&src_file, "chrome trace", 0o644);
    let src_remote = format!("localhost:{}", src_file.display());
    let dst_remote = format!("localhost:{}", dst_file.display());
    let traces = tempfile::tempdir().unwrap();
    let trace_prefix = traces.path().join("remote-trace");
    let trace_arg = format!("--chrome-trace={}", trace_prefix.display());
    let output = run_rcp_with_args(&["--dry-run=brief", &trace_arg, &src_remote, &dst_remote]);
    print_command_output(&output);
    assert!(
        output.status.success(),
        "remote Chrome-trace dry run failed"
    );
    assert!(
        !dst_file.exists(),
        "a dry run must not create the destination"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        stdout.matches("Chrome trace will be written to:").count(),
        3,
        "the master must surface its own and both daemon artifact notices"
    );
    assert_eq!(
        stdout
            .matches("Elapsed scope timeline will be written to:")
            .count(),
        3,
        "elapsed timeline notices must also reach the master"
    );
    assert!(
        stdout.contains("rcpd-source"),
        "the source daemon artifact notice must reach master output"
    );
    assert!(
        stdout.contains("rcpd-destination"),
        "the destination daemon artifact notice must reach master output"
    );

    let trace_files: Vec<_> = std::fs::read_dir(traces.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect();
    assert_eq!(
        trace_files.len(),
        6,
        "each process must write both trace artifacts"
    );
    assert_eq!(
        trace_files
            .iter()
            .filter(|path| path.to_string_lossy().ends_with(".scopes.json"))
            .count(),
        3
    );
    for path in trace_files {
        let trace: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert!(trace.is_array());
    }
}

#[test]
fn test_remote_destination_startup_failure_drains_source_tracing_notices() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("source_notice.txt");
    let dst_file = dst_dir.path().join("source_notice.txt");
    create_test_file(&src_file, "source startup notice", 0o644);
    let src_remote = format!("localhost:{}", src_file.display());
    let dst_remote = format!("localhost:{}", dst_file.display());

    let scratch = tempfile::tempdir().unwrap();
    let wrapper = scratch
        .path()
        .join("source-tracing-destination-refusal-rcpd");
    let rcpd = assert_cmd::cargo::cargo_bin("rcpd");
    let trace_prefix = scratch.path().join("source-startup-trace");
    let source_trace_arg = format!("--chrome-trace={}", trace_prefix.display());
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\n\
             if [ \"$1\" = \"--protocol-version\" ]; then\n\
               exec {} \"$@\"\n\
             fi\n\
             if [ \"$1\" = \"--role\" ] && [ \"$2\" = \"source\" ]; then\n\
               exec {} \"$@\" {}\n\
             fi\n\
             if [ \"$1\" = \"--role\" ] && [ \"$2\" = \"destination\" ]; then\n\
               printf '%s\\n' \
                 'RCP_ERROR destination startup refused for source tracing drain regression' >&2\n\
               exit 17\n\
             fi\n\
             printf '%s\\n' 'RCP_ERROR unexpected test wrapper invocation' >&2\n\
             exit 18\n",
            shell_quote_for_test(&rcpd),
            shell_quote_for_test(&rcpd),
            shell_quote_for_test(std::path::Path::new(&source_trace_arg)),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();

    let rcpd_path = format!("--rcpd-path={}", wrapper.display());
    let output = run_rcp_with_args_at_default_verbosity(&[
        &rcpd_path,
        "--no-encryption",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(
        !output.status.success(),
        "the destination startup refusal must fail the remote copy"
    );
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("destination startup refused for source tracing drain regression"),
        "the destination's typed startup failure must remain visible: {combined}"
    );
    assert!(
        combined.contains("Chrome trace will be written to:"),
        "the source startup notice must be drained before the destination error returns: {combined}"
    );
    assert!(
        combined.contains("rcpd-source"),
        "the drained startup notice must retain source-daemon context: {combined}"
    );
}

#[test]
fn test_remote_source_connection_failure_waits_for_source_rcpd_cleanup() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("source_connect_failure.txt");
    let dst_file = dst_dir.path().join("source_connect_failure.txt");
    create_test_file(&src_file, "source cleanup", 0o644);
    let src_remote = format!("localhost:{}", src_file.display());
    let dst_remote = format!("localhost:{}", dst_file.display());

    let unavailable = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let unavailable_port = unavailable.local_addr().unwrap().port();
    drop(unavailable);

    let scratch = tempfile::tempdir().unwrap();
    let wrapper = scratch.path().join("source-connect-failure-rcpd");
    let cleanup_marker = scratch.path().join("source-cleaned-up");
    let destination_marker = scratch.path().join("destination-spawned");
    let rcpd = assert_cmd::cargo::cargo_bin("rcpd");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\n\
             if [ \"$1\" = \"--protocol-version\" ]; then\n\
               exec {} \"$@\"\n\
             fi\n\
             if [ \"$1\" = \"--role\" ] && [ \"$2\" = \"source\" ]; then\n\
               printf 'RCP_TCP 127.0.0.1:{} 4 4\\n' >&2\n\
               cat >/dev/null\n\
               sleep 0.5\n\
               printf 'cleaned\\n' > {}\n\
               exit 19\n\
             fi\n\
             printf 'spawned\\n' > {}\n\
             exit 20\n",
            shell_quote_for_test(&rcpd),
            unavailable_port,
            shell_quote_for_test(&cleanup_marker),
            shell_quote_for_test(&destination_marker),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();

    let rcpd_path = format!("--rcpd-path={}", wrapper.display());
    let output = run_rcp_with_args_at_default_verbosity(&[
        &rcpd_path,
        "--no-encryption",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(
        !output.status.success(),
        "the unavailable source listener must fail the copy"
    );
    assert_eq!(
        std::fs::read_to_string(&cleanup_marker).unwrap(),
        "cleaned\n",
        "rcp must wait for source cleanup before returning the connection error"
    );
    assert!(
        !destination_marker.exists(),
        "destination must not spawn after a source connection failure"
    );
}

/// Test copying many small files to exercise stream pooling.
///
/// This test creates 150 small files in a directory and verifies they are all
/// copied correctly. The default connection ceiling permits at most 100 streams,
/// and the source's automatic file ceiling may lower that further, so this
/// exercises stream reuse as multiple files are sent over the same streams.
#[test]
fn test_remote_copy_many_small_files() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_subdir = src_dir.path().join("many_files");
    let dst_subdir = dst_dir.path().join("many_files");
    std::fs::create_dir(&src_subdir).unwrap();
    // create 150 small files, more than the default connection ceiling
    // this exercises stream reuse without causing pool exhaustion issues
    let num_files: usize = 150;
    for i in 0..num_files {
        let content = format!("content of file {i}");
        create_test_file(
            &src_subdir.join(format!("file_{i:04}.txt")),
            &content,
            0o644,
        );
    }
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify all files were copied
    for i in 0..num_files {
        let dst_file = dst_subdir.join(format!("file_{i:04}.txt"));
        assert!(dst_file.exists(), "file_{i:04}.txt should exist");
        let expected_content = format!("content of file {i}");
        assert_eq!(
            get_file_content(&dst_file),
            expected_content,
            "file_{i:04}.txt content mismatch"
        );
    }
    // verify files were copied by checking stdout for the count
    // (parse_summary_from_output may fail due to bytes_copied having KB/MB suffix)
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&format!("files copied: {num_files}")),
        "Expected 'files copied: {num_files}' in output"
    );
    assert!(
        stdout.contains("directories created: 1"),
        "Expected 'directories created: 1' in output"
    );
}

#[test]
fn test_remote_copy_symlink_no_dereference() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let target_file = src_dir.path().join("target.txt");
    let symlink_file = src_dir.path().join("symlink.txt");
    let dst_symlink = dst_dir.path().join("symlink.txt");
    create_test_file(&target_file, "target content", 0o644);
    std::os::unix::fs::symlink(&target_file, &symlink_file).unwrap();
    let src_remote = format!("localhost:{}", symlink_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_symlink.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify destination is a symlink
    assert!(dst_symlink.is_symlink());
    let link_target = std::fs::read_link(&dst_symlink).unwrap();
    assert_eq!(link_target, target_file);
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.symlinks_created, 1);
    assert_eq!(summary.files_copied, 0);
}

#[test]
fn test_remote_copy_symlink_with_dereference() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let target_file = src_dir.path().join("target.txt");
    let symlink_file = src_dir.path().join("symlink.txt");
    let dst_file = dst_dir.path().join("symlink.txt");
    create_test_file(&target_file, "target content for dereference", 0o644);
    std::os::unix::fs::symlink(&target_file, &symlink_file).unwrap();
    let src_remote = format!("localhost:{}", symlink_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_success(&["-L", &src_remote, &dst_remote]);
    // verify destination is a regular file, not a symlink
    assert!(!dst_file.is_symlink());
    assert!(dst_file.is_file());
    assert_eq!(
        get_file_content(&dst_file),
        "target content for dereference"
    );
}

#[test]
fn test_remote_copy_with_overwrite() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("overwrite_test.txt");
    let dst_file = dst_dir.path().join("overwrite_test.txt");
    // create source file with longer content to ensure different size
    create_test_file(&src_file, "new content that is longer", 0o644);
    // create existing destination file with different, shorter content
    create_test_file(&dst_file, "old content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    // verify content was overwritten
    assert_eq!(get_file_content(&dst_file), "new content that is longer");
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 1);
    assert_eq!(summary.rm_summary.files_removed, 1); // file-to-file overwrite removes the old file first
    assert_eq!(summary.rm_summary.bytes_removed, 11); // "old content"
    assert_eq!(summary.bytes_copied, 26); // "new content that is longer"
}

#[test]
fn test_remote_copy_overwrite_filter_newer_skips_when_dest_is_newer() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("filter_newer.txt");
    let dst_file = dst_dir.path().join("filter_newer.txt");
    // create source file
    create_test_file(&src_file, "old source", 0o644);
    // create destination file with different content
    create_test_file(&dst_file, "newer dest", 0o644);
    // make dest strictly newer than source
    let future_time = filetime::FileTime::from_unix_time(
        std::fs::metadata(&src_file)
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + 10,
        0,
    );
    filetime::set_file_mtime(&dst_file, future_time).unwrap();
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output = run_rcp_and_expect_success(&[
        "--overwrite",
        "--overwrite-filter=newer",
        "--summary",
        &src_remote,
        &dst_remote,
    ]);
    // dest should not be overwritten because it is newer
    assert_eq!(get_file_content(&dst_file), "newer dest");
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_unchanged, 1);
    assert_eq!(summary.files_copied, 0);
}

#[test]
fn test_remote_copy_overwrite_filter_newer_copies_when_dest_is_older() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("filter_older.txt");
    let dst_file = dst_dir.path().join("filter_older.txt");
    // create destination file first with old content
    create_test_file(&dst_file, "old dest", 0o644);
    // set dest mtime to the past
    let past_time = filetime::FileTime::from_unix_time(1_000_000, 0);
    filetime::set_file_mtime(&dst_file, past_time).unwrap();
    // create source file with different content (will have a newer mtime)
    create_test_file(&src_file, "new source content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output = run_rcp_and_expect_success(&[
        "--overwrite",
        "--overwrite-filter=newer",
        "--summary",
        &src_remote,
        &dst_remote,
    ]);
    // dest should be overwritten because source is newer
    assert_eq!(get_file_content(&dst_file), "new source content");
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 1);
}

#[test]
fn test_remote_copy_ignore_existing_skips_existing_file() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("ignore_test.txt");
    let dst_file = dst_dir.path().join("ignore_test.txt");
    create_test_file(&src_file, "source content", 0o644);
    create_test_file(&dst_file, "existing content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--ignore-existing", "--summary", &src_remote, &dst_remote]);
    // destination should not be overwritten
    assert_eq!(get_file_content(&dst_file), "existing content");
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_unchanged, 1);
    assert_eq!(summary.files_copied, 0);
}

#[test]
fn test_remote_copy_ignore_existing_copies_new_file() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("new_file.txt");
    let dst_file = dst_dir.path().join("new_file.txt");
    create_test_file(&src_file, "source content", 0o644);
    // no destination file
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--ignore-existing", "--summary", &src_remote, &dst_remote]);
    assert_eq!(get_file_content(&dst_file), "source content");
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 1);
}

#[test]
fn test_remote_copy_ignore_existing_dir_over_non_dir_skips() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // source is a directory with a file inside
    let src_subdir = src_dir.path().join("mydir");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("inner.txt"), "inner", 0o644);
    // destination has a regular file where the directory would be
    let dst_file = dst_dir.path().join("mydir");
    create_test_file(&dst_file, "i am a file", 0o644);
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // should succeed (skip silently), not fail
    let output =
        run_rcp_and_expect_success(&["--ignore-existing", "--summary", &src_remote, &dst_remote]);
    // destination file should still be the original file, not a directory
    assert!(dst_file.is_file());
    assert_eq!(get_file_content(&dst_file), "i am a file");
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 0);
}

#[test]
fn test_remote_copy_without_overwrite_fails() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("no_overwrite_test.txt");
    let dst_file = dst_dir.path().join("no_overwrite_test.txt");
    // create source file
    create_test_file(&src_file, "new content", 0o644);
    // create existing destination file with different content
    create_test_file(&dst_file, "old content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&["--summary", &src_remote, &dst_remote]);
    // verify content was not overwritten
    assert_eq!(get_file_content(&dst_file), "old content");
    // verify summary shows no files copied (error occurred before copy)
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 0);
    assert_eq!(summary.bytes_copied, 0);
}

#[test]
fn test_remote_copy_comprehensive() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create a complex directory structure with files and symlinks
    let src_subdir = src_dir.path().join("comprehensive");
    std::fs::create_dir(&src_subdir).unwrap();
    let target_file = src_subdir.join("target.txt");
    let regular_file = src_subdir.join("regular.txt");
    let symlink_file = src_subdir.join("symlink.txt");
    create_test_file(&target_file, "target content", 0o644);
    create_test_file(&regular_file, "regular content", 0o755);
    std::os::unix::fs::symlink(&target_file, &symlink_file).unwrap();
    // create destination directory with existing file to test overwrite
    let dst_subdir = dst_dir.path().join("comprehensive");
    std::fs::create_dir(&dst_subdir).unwrap();
    let existing_file = dst_subdir.join("regular.txt");
    create_test_file(&existing_file, "old content", 0o644);
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    run_rcp_and_expect_success(&["--preserve", "--overwrite", "-L", &src_remote, &dst_remote]);
    // verify regular file was copied with permissions preserved and overwritten
    let dst_regular = dst_subdir.join("regular.txt");
    assert_eq!(get_file_content(&dst_regular), "regular content");
    let mode = std::fs::metadata(&dst_regular)
        .unwrap()
        .permissions()
        .mode()
        & 0o7777;
    assert_eq!(mode, 0o755);
    // verify symlink was dereferenced (copied as regular file due to -L)
    let dst_symlink = dst_subdir.join("symlink.txt");
    assert!(!dst_symlink.is_symlink());
    assert!(dst_symlink.is_file());
    assert_eq!(get_file_content(&dst_symlink), "target content");
    // verify target file was also copied
    let dst_target = dst_subdir.join("target.txt");
    assert_eq!(get_file_content(&dst_target), "target content");
}

#[test]
fn test_remote_symlink_chain_dereference() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // Create a chain of symlinks: foo -> bar -> baz (actual file)
    let baz_file = src_dir.path().join("baz_file.txt");
    create_test_file(&baz_file, "final content", 0o644);
    let bar_link = src_dir.path().join("bar");
    let foo_link = src_dir.path().join("foo");
    // Create chain: foo -> bar -> baz_file.txt
    std::os::unix::fs::symlink(&baz_file, &bar_link).unwrap();
    std::os::unix::fs::symlink(&bar_link, &foo_link).unwrap();
    // Create a source directory with the symlink chain
    let src_subdir = src_dir.path().join("chain_test");
    std::fs::create_dir(&src_subdir).unwrap();
    // Create symlinks in the test directory that represent the chain
    std::os::unix::fs::symlink(&foo_link, src_subdir.join("foo")).unwrap();
    std::os::unix::fs::symlink(&bar_link, src_subdir.join("bar")).unwrap();
    std::os::unix::fs::symlink(&baz_file, src_subdir.join("baz")).unwrap();
    let dst_subdir = dst_dir.path().join("chain_test");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // Test with dereference - should copy 3 files with same content
    run_rcp_and_expect_success(&["-L", &src_remote, &dst_remote]);
    // Verify all three are now regular files with the same content
    let foo_content = get_file_content(&dst_subdir.join("foo"));
    let bar_content = get_file_content(&dst_subdir.join("bar"));
    let baz_content = get_file_content(&dst_subdir.join("baz"));
    assert_eq!(foo_content, "final content");
    assert_eq!(bar_content, "final content");
    assert_eq!(baz_content, "final content");
    // Verify they are all regular files, not symlinks
    assert!(dst_subdir.join("foo").is_file());
    assert!(dst_subdir.join("bar").is_file());
    assert!(dst_subdir.join("baz").is_file());
    assert!(!dst_subdir.join("foo").is_symlink());
    assert!(!dst_subdir.join("bar").is_symlink());
    assert!(!dst_subdir.join("baz").is_symlink());
}

#[test]
fn test_remote_symlink_chain_no_dereference() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // Create a chain of symlinks: foo -> bar -> baz (actual file)
    let baz_file = src_dir.path().join("baz_file.txt");
    create_test_file(&baz_file, "final content", 0o644);
    let bar_link = src_dir.path().join("bar");
    let foo_link = src_dir.path().join("foo");
    // Create chain: foo -> bar -> baz_file.txt
    std::os::unix::fs::symlink(&baz_file, &bar_link).unwrap();
    std::os::unix::fs::symlink(&bar_link, &foo_link).unwrap();
    // Create a source directory with the symlink chain
    let src_subdir = src_dir.path().join("chain_test");
    std::fs::create_dir(&src_subdir).unwrap();
    // Create symlinks in the test directory that represent the chain
    std::os::unix::fs::symlink(&foo_link, src_subdir.join("foo")).unwrap();
    std::os::unix::fs::symlink(&bar_link, src_subdir.join("bar")).unwrap();
    std::os::unix::fs::symlink(&baz_file, src_subdir.join("baz")).unwrap();
    let dst_subdir = dst_dir.path().join("chain_test");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // Test without dereference - should preserve symlinks
    run_rcp_and_expect_success(&[&src_remote, &dst_remote]);
    // Verify all three remain as symlinks
    assert!(dst_subdir.join("foo").is_symlink());
    assert!(dst_subdir.join("bar").is_symlink());
    assert!(dst_subdir.join("baz").is_symlink());
    // Verify symlink targets are preserved
    assert_eq!(
        std::fs::read_link(dst_subdir.join("foo")).unwrap(),
        foo_link
    );
    assert_eq!(
        std::fs::read_link(dst_subdir.join("bar")).unwrap(),
        bar_link
    );
    assert_eq!(
        std::fs::read_link(dst_subdir.join("baz")).unwrap(),
        baz_file
    );
}

#[test]
fn test_remote_dereference_directory_symlink() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // Create a directory with specific permissions and files
    let target_dir = src_dir.path().join("target_directory");
    std::fs::create_dir(&target_dir).unwrap();
    std::fs::set_permissions(&target_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    create_test_file(&target_dir.join("file1.txt"), "content1", 0o644);
    create_test_file(&target_dir.join("file2.txt"), "content2", 0o600);
    // Create a symlink pointing to the directory
    let dir_symlink = src_dir.path().join("dir_link");
    std::os::unix::fs::symlink(&target_dir, &dir_symlink).unwrap();
    let dst_path = dst_dir.path().join("copied_directory");
    let src_remote = format!("localhost:{}", dir_symlink.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    // Test with dereference - should copy as a directory with preserved permissions
    run_rcp_and_expect_success(&["-L", "--preserve", &src_remote, &dst_remote]);
    // Verify the result is a directory, not a symlink
    assert!(dst_path.is_dir());
    assert!(!dst_path.is_symlink());
    // Verify directory permissions preserved
    let mode = std::fs::metadata(&dst_path).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode, 0o755);
    // Verify files were copied with correct content and permissions
    assert_eq!(get_file_content(&dst_path.join("file1.txt")), "content1");
    assert_eq!(get_file_content(&dst_path.join("file2.txt")), "content2");
    let mode1 = std::fs::metadata(dst_path.join("file1.txt"))
        .unwrap()
        .permissions()
        .mode()
        & 0o7777;
    let mode2 = std::fs::metadata(dst_path.join("file2.txt"))
        .unwrap()
        .permissions()
        .mode()
        & 0o7777;
    assert_eq!(mode1, 0o644);
    assert_eq!(mode2, 0o600);
}

#[test]
fn test_remote_dereference_file_symlink_permissions() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // Create files with different permissions
    let file1 = src_dir.path().join("file1.txt");
    let file2 = src_dir.path().join("file2.txt");
    create_test_file(&file1, "content1", 0o755);
    create_test_file(&file2, "content2", 0o640);
    // Create symlinks to these files
    let symlink1 = src_dir.path().join("symlink1");
    let symlink2 = src_dir.path().join("symlink2");
    std::os::unix::fs::symlink(&file1, &symlink1).unwrap();
    std::os::unix::fs::symlink(&file2, &symlink2).unwrap();
    let dst_file1 = dst_dir.path().join("copied1.txt");
    let dst_file2 = dst_dir.path().join("copied2.txt");
    let src_remote1 = format!("localhost:{}", symlink1.to_str().unwrap());
    let dst_remote1 = format!("localhost:{}", dst_file1.to_str().unwrap());
    let src_remote2 = format!("localhost:{}", symlink2.to_str().unwrap());
    let dst_remote2 = format!("localhost:{}", dst_file2.to_str().unwrap());
    // Test copying with dereference and preserve
    run_rcp_and_expect_success(&["-L", "--preserve", &src_remote1, &dst_remote1]);
    run_rcp_and_expect_success(&["-L", "--preserve", &src_remote2, &dst_remote2]);
    // Verify results are regular files, not symlinks
    assert!(dst_file1.is_file());
    assert!(!dst_file1.is_symlink());
    assert!(dst_file2.is_file());
    assert!(!dst_file2.is_symlink());
    // Verify content and permissions of target files were preserved
    assert_eq!(get_file_content(&dst_file1), "content1");
    assert_eq!(get_file_content(&dst_file2), "content2");
    let mode1 = std::fs::metadata(&dst_file1).unwrap().permissions().mode() & 0o7777;
    let mode2 = std::fs::metadata(&dst_file2).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode1, 0o755);
    assert_eq!(mode2, 0o640);
}

#[test]
fn test_remote_debug_log_file_creation() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("debug_log_test.txt");
    let dst_file = dst_dir.path().join("debug_log_test.txt");
    create_test_file(&src_file, "debug log test content", 0o644);
    // Use a unique prefix for this test
    let temp_dir = std::env::temp_dir()
        .to_str()
        .expect("No default temp directory?")
        .to_owned();
    let log_prefix = format!("{temp_dir}/rcpd-test-{}", std::process::id());
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // Run rcp with debug log prefix
    let output = run_rcp_with_args(&[
        "--rcpd-debug-log-prefix",
        &log_prefix,
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    // Copy should succeed
    assert!(output.status.success(), "rcp command should succeed");
    assert_eq!(get_file_content(&dst_file), "debug log test content");
    // Check that debug log files were created
    let tmp_entries = std::fs::read_dir(temp_dir)
        .expect("Failed to read temp directory")
        .filter_map(std::result::Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(&format!("rcpd-test-{}", std::process::id())))
        })
        .collect::<Vec<_>>();
    eprintln!(
        "Found debug log files: {:?}",
        tmp_entries
            .iter()
            .map(std::fs::DirEntry::file_name)
            .collect::<Vec<_>>()
    );
    assert!(!tmp_entries.is_empty(), "Debug log files should be created");
    // Verify log files contain actual log entries
    for entry in tmp_entries {
        let log_content =
            std::fs::read_to_string(entry.path()).expect("Should be able to read debug log file");
        eprintln!(
            "Log file {} contents (first 200 chars): {}",
            entry.file_name().to_str().unwrap(),
            &log_content[..std::cmp::min(200, log_content.len())]
        );
        assert!(!log_content.is_empty(), "Log files should contain content");
        // Clean up test log files
        std::fs::remove_file(entry.path()).ok();
    }
}

#[test]
fn test_remote_copy_port_range() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("port_range_test.txt");
    let dst_file = dst_dir.path().join("port_range_test.txt");
    create_test_file(&src_file, "port range test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // use a port range that's unlikely to conflict with other tests
    // we'll use a high port range to avoid conflicts with system services
    let port_range = "25000-25999";
    eprintln!("Testing remote copy with port range: {port_range}");
    run_rcp_and_expect_success(&["--port-ranges", port_range, &src_remote, &dst_remote]);
    // verify the file was copied successfully
    assert_eq!(get_file_content(&dst_file), "port range test content");
}

#[test]
fn test_remote_overwrite_directory_with_directory() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source directory structure
    let src_subdir = src_dir.path().join("mydir");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file1.txt"), "content1", 0o644);
    create_test_file(&src_subdir.join("file2.txt"), "content2", 0o644);
    create_test_file(&src_subdir.join("file3.txt"), "content3", 0o644);
    // create destination directory with different contents
    let dst_subdir = dst_dir.path().join("mydir");
    std::fs::create_dir(&dst_subdir).unwrap();
    create_test_file(&dst_subdir.join("file1.txt"), "old content1", 0o644);
    create_test_file(&dst_subdir.join("file4.txt"), "old file4", 0o644); // will remain
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    // verify the directory was updated recursively
    assert_eq!(get_file_content(&dst_subdir.join("file1.txt")), "content1"); // updated
    assert_eq!(get_file_content(&dst_subdir.join("file2.txt")), "content2"); // new
    assert_eq!(get_file_content(&dst_subdir.join("file3.txt")), "content3"); // new
    assert_eq!(get_file_content(&dst_subdir.join("file4.txt")), "old file4"); // unchanged
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 3); // file1, file2, file3
    assert_eq!(summary.rm_summary.files_removed, 1); // file1.txt overwrite removes the old file first
    assert_eq!(summary.directories_created, 0); // directory already existed
    assert_eq!(summary.bytes_copied, 24); // "content1" (8) + "content2" (8) + "content3" (8)
}

/// A reused destination directory announces Ready only after building its overwrite manifest.
/// Symlink children and End can arrive earlier; completion must wait until Ready is flushed.
/// A large destination manifest exercises that ordering without delaying source discovery.
#[test]
fn test_remote_overwrite_reused_dir_completes_only_after_announce() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // symlink target: a real file OUTSIDE the copied root, so the tree holds NO file entries —
    // file sends wait for Ready and would mask the race by keeping the
    // directory's count open until after the announce
    let target = src_dir.path().join("outside.txt");
    create_test_file(&target, "symlink target", 0o644);
    let src_root = src_dir.path().join("tree");
    std::fs::create_dir(&src_root).unwrap();
    std::os::unix::fs::symlink(&target, src_root.join("link")).unwrap();
    // reused destination root, pre-populated so the manifest build is slow relative to source discovery
    let dst_root = dst_dir.path().join("tree");
    std::fs::create_dir(&dst_root).unwrap();
    for i in 0..20_000 {
        std::fs::File::create(dst_root.join(format!("pre{i:05}"))).unwrap();
    }
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    let dst_link = dst_root.join("link");
    assert!(dst_link.is_symlink(), "the symlink must have been copied");
    assert_eq!(std::fs::read_link(&dst_link).unwrap(), target);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.symlinks_created, 1);
    assert_eq!(summary.directories_unchanged, 1); // the reused root
}

#[test]
fn test_remote_overwrite_file_with_directory() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source directory
    let src_subdir = src_dir.path().join("mydir");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("nested.txt"), "nested content", 0o644);
    // create destination as a file (will be replaced with directory)
    let dst_path = dst_dir.path().join("mydir");
    create_test_file(&dst_path, "this is a file", 0o644);
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    // verify the file was replaced with a directory
    assert!(dst_path.is_dir());
    assert_eq!(
        get_file_content(&dst_path.join("nested.txt")),
        "nested content"
    );
    // verify summary shows file removed and directory + nested file created
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.rm_summary.files_removed, 1); // old "mydir" file was removed
    assert_eq!(summary.directories_created, 1); // new "mydir" directory created
    assert_eq!(summary.files_copied, 1); // nested.txt copied
    assert_eq!(summary.bytes_copied, 14); // "nested content"
}

#[test]
fn test_remote_overwrite_directory_with_file() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source file
    let src_file = src_dir.path().join("myfile.txt");
    create_test_file(&src_file, "file content", 0o644);
    // create destination as a directory (will be replaced with file)
    let dst_path = dst_dir.path().join("myfile.txt");
    std::fs::create_dir(&dst_path).unwrap();
    create_test_file(&dst_path.join("nested.txt"), "nested", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    // verify the directory was replaced with a file
    assert!(dst_path.is_file());
    assert_eq!(get_file_content(&dst_path), "file content");
    // verify summary shows directory and nested file removed, then file copied
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.rm_summary.files_removed, 1); // nested.txt was removed
    assert_eq!(summary.rm_summary.directories_removed, 1); // old directory was removed
    assert_eq!(summary.files_copied, 1); // new file copied
    assert_eq!(summary.bytes_copied, 12); // "file content"
}

/// Startup raises the soft limit before removal fans out, including inside a single file job.
#[test]
fn remote_overwrite_removes_branching_directories_with_a_low_inherited_soft_limit() {
    require_local_ssh();
    let fixture = tempfile::tempdir().unwrap();
    let wrapper = fixture.path().join("limited-rcpd");
    let daemon = shell_quote_for_test(&assert_cmd::cargo::cargo_bin("rcpd"));
    // keep a fixed hard ceiling; only the daemon may raise this low soft limit
    std::fs::write(&wrapper, format!(
        "#!/bin/sh\nulimit -n 1024 || exit 1\nulimit -S -n 128 || exit 1\nexec {daemon} \"$@\"\n"
    )).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    for single_file in [false, true] {
        let root = fixture
            .path()
            .join(if single_file { "single" } else { "multiple" });
        std::fs::create_dir(&root).unwrap();
        let generated = std::process::Command::new(assert_cmd::cargo::cargo_bin("filegen"))
            .args([
                root.to_str().unwrap(),
                "40,1,1,1,1,1,1,1,1,1,1,1",
                "1",
                "8",
                "--leaf-files",
                "--bufsize=8",
            ])
            .output()
            .unwrap();
        assert!(
            generated.status.success(),
            "{}",
            String::from_utf8_lossy(&generated.stderr)
        );
        let source = root.join("source");
        let destination = root.join("filegen");
        if single_file {
            create_test_file(&source, "replacement", 0o600);
        } else {
            std::fs::create_dir(&source).unwrap();
            for i in 0..40 {
                create_test_file(&source.join(format!("dir{i}")), "replacement", 0o600);
            }
        }
        let output = run_rcp_and_expect_success(&[
            "--overwrite",
            "--summary",
            if single_file {
                "--max-files-in-flight=1"
            } else {
                "--max-files-in-flight=20"
            },
            &format!("--rcpd-path={}", wrapper.display()),
            &format!("localhost:{}", source.display()),
            &format!("localhost:{}", destination.display()),
        ]);
        let summary = parse_summary_from_output(&output).unwrap();
        assert_eq!(summary.files_copied, if single_file { 1 } else { 40 });
        assert_eq!(summary.rm_summary.files_removed, 40);
        assert_eq!(
            summary.rm_summary.directories_removed,
            if single_file { 481 } else { 480 }
        );
        if single_file {
            assert_eq!(std::fs::read(destination).unwrap(), b"replacement");
        } else {
            for i in 0..40 {
                assert_eq!(
                    std::fs::read(destination.join(format!("dir{i}"))).unwrap(),
                    b"replacement"
                );
            }
        }
    }
}

#[test]
fn test_remote_overwrite_symlink_with_symlink_same_target() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create target file
    let target = src_dir.path().join("target.txt");
    create_test_file(&target, "target content", 0o644);
    // create source symlink
    let src_link = src_dir.path().join("link.txt");
    std::os::unix::fs::symlink("target.txt", &src_link).unwrap();
    // create destination symlink pointing to same target
    let dst_target = dst_dir.path().join("target.txt");
    create_test_file(&dst_target, "target content", 0o644);
    let dst_link = dst_dir.path().join("link.txt");
    std::os::unix::fs::symlink("target.txt", &dst_link).unwrap();
    let src_remote = format!("localhost:{}", src_link.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_link.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify symlink still points to same target
    assert!(dst_link.is_symlink());
    assert_eq!(
        std::fs::read_link(&dst_link).unwrap().to_str().unwrap(),
        "target.txt"
    );
}

#[test]
fn test_remote_overwrite_symlink_with_symlink_different_target() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source symlink
    let src_link = src_dir.path().join("link.txt");
    std::os::unix::fs::symlink("new_target.txt", &src_link).unwrap();
    // create destination symlink pointing to different target
    let dst_link = dst_dir.path().join("link.txt");
    std::os::unix::fs::symlink("old_target.txt", &dst_link).unwrap();
    let src_remote = format!("localhost:{}", src_link.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_link.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify symlink was updated to new target
    assert!(dst_link.is_symlink());
    assert_eq!(
        std::fs::read_link(&dst_link).unwrap().to_str().unwrap(),
        "new_target.txt"
    );
}

#[test]
fn test_remote_overwrite_file_with_symlink() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source symlink
    let src_link = src_dir.path().join("item.txt");
    std::os::unix::fs::symlink("target.txt", &src_link).unwrap();
    // create destination as a file (will be replaced with symlink)
    let dst_path = dst_dir.path().join("item.txt");
    create_test_file(&dst_path, "this is a file", 0o644);
    let src_remote = format!("localhost:{}", src_link.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify the file was replaced with a symlink
    assert!(dst_path.is_symlink());
    assert_eq!(
        std::fs::read_link(&dst_path).unwrap().to_str().unwrap(),
        "target.txt"
    );
}

#[test]
fn test_remote_overwrite_symlink_with_file() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source file
    let src_file = src_dir.path().join("item.txt");
    create_test_file(&src_file, "file content", 0o644);
    // create destination as a symlink (will be replaced with file)
    let dst_path = dst_dir.path().join("item.txt");
    std::os::unix::fs::symlink("target.txt", &dst_path).unwrap();
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify the symlink was replaced with a file
    assert!(dst_path.is_file());
    assert!(!dst_path.is_symlink());
    assert_eq!(get_file_content(&dst_path), "file content");
}

#[test]
fn test_remote_overwrite_directory_with_symlink() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source symlink
    let src_link = src_dir.path().join("item");
    std::os::unix::fs::symlink("target", &src_link).unwrap();
    // create destination as a directory (will be replaced with symlink)
    let dst_path = dst_dir.path().join("item");
    std::fs::create_dir(&dst_path).unwrap();
    create_test_file(&dst_path.join("nested.txt"), "nested", 0o644);
    let src_remote = format!("localhost:{}", src_link.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify the directory was replaced with a symlink
    assert!(dst_path.is_symlink());
    assert_eq!(
        std::fs::read_link(&dst_path).unwrap().to_str().unwrap(),
        "target"
    );
}

#[test]
fn test_remote_overwrite_symlink_with_directory() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // Create source directory
    let src_subdir = src_dir.path().join("item");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
    // create destination as a symlink (will be replaced with directory)
    let dst_path = dst_dir.path().join("item");
    std::os::unix::fs::symlink("target", &dst_path).unwrap();
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    // verify the symlink was replaced with a directory
    assert!(dst_path.is_dir());
    assert!(!dst_path.is_symlink());
    assert_eq!(get_file_content(&dst_path.join("file.txt")), "content");
}

#[test]
fn test_remote_copy_nonexistent_source() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let nonexistent_src = src_dir.path().join("does_not_exist.txt");
    let dst_file = dst_dir.path().join("destination.txt");
    let src_remote = format!("localhost:{}", nonexistent_src.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&[&src_remote, &dst_remote]);
    // verify error message mentions the source file
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");
    assert!(combined.contains("does_not_exist") && combined.contains("No such file"));
}

#[test]
fn test_remote_copy_destination_parent_missing() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("source.txt");
    create_test_file(&src_file, "content", 0o644);
    // destination parent doesn't exist
    let dst_file = dst_dir.path().join("nonexistent_dir/destination.txt");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&[&src_remote, &dst_remote]);
    // verify error message mentions the missing directory
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");
    assert!(combined.contains("No such file") || combined.contains("nonexistent_dir"));
}

#[test]
fn test_remote_copy_unreadable_source() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // test with a single unreadable file case (no permissions)
    let src_file = src_dir.path().join("unreadable.txt");
    let dst_file = dst_dir.path().join("unreadable.txt");
    create_test_file(&src_file, "no permissions", 0o000);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_failure(&[&src_remote, &dst_remote]);
    // verify the destination file was not created
    assert!(!dst_file.exists());
}

#[test]
fn test_remote_copy_directory_with_unreadable_files_continue() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create directory structure with some unreadable files
    let src_subdir = src_dir.path().join("mixed_dir");
    std::fs::create_dir(&src_subdir).unwrap();
    // readable files
    create_test_file(&src_subdir.join("file1.txt"), "readable content 1", 0o644);
    create_test_file(&src_subdir.join("file2.txt"), "readable content 2", 0o644);
    // unreadable files
    create_test_file(&src_subdir.join("unreadable1.txt"), "secret 1", 0o000);
    create_test_file(&src_subdir.join("file3.txt"), "readable content 3", 0o644);
    create_test_file(&src_subdir.join("unreadable2.txt"), "secret 2", 0o000);
    let dst_subdir = dst_dir.path().join("mixed_dir");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // without --fail-early, should continue copying readable files
    let output = run_rcp_and_expect_failure(&["--summary", &src_remote, &dst_remote]);
    // verify readable files were copied
    assert!(dst_subdir.join("file1.txt").exists());
    assert!(dst_subdir.join("file2.txt").exists());
    assert!(dst_subdir.join("file3.txt").exists());
    assert_eq!(
        get_file_content(&dst_subdir.join("file1.txt")),
        "readable content 1"
    );
    assert_eq!(
        get_file_content(&dst_subdir.join("file2.txt")),
        "readable content 2"
    );
    assert_eq!(
        get_file_content(&dst_subdir.join("file3.txt")),
        "readable content 3"
    );
    // verify unreadable files were not copied
    assert!(!dst_subdir.join("unreadable1.txt").exists());
    assert!(!dst_subdir.join("unreadable2.txt").exists());
    // verify summary shows partial success: 3 files copied, 1 directory created
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 3);
    assert_eq!(summary.directories_created, 1);
    assert_eq!(summary.bytes_copied, 54); // sum of 3 readable files
    // verify non-zero exit code
    assert!(!output.status.success());
}

#[test]
fn test_remote_copy_directory_with_unreadable_files_fail_early() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create test with readable file first, then unreadable file
    // this ensures directory gets created before failure
    let src_subdir = src_dir.path().join("fail_early_test");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("a_good.txt"), "good", 0o644);
    create_test_file(&src_subdir.join("b_unreadable.txt"), "secret", 0o000);
    create_test_file(&src_subdir.join("c_good.txt"), "also good", 0o644);
    let dst_subdir = dst_dir.path().join("fail_early_test");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // with --fail-early, should stop on first error
    let output =
        run_rcp_and_expect_failure(&["--fail-early", "--summary", &src_remote, &dst_remote]);
    // with fail-early, exact behavior depends on timing
    // we just verify:
    // 1. operation failed (non-zero exit)
    // 2. not all files were copied (< 3)
    // 3. some progress may have been made before the error
    assert!(
        !output.status.success(),
        "Operation should fail with non-zero exit code"
    );

    // try to parse summary, but it might not be available if connection closed too quickly
    if let Some(summary) = parse_summary_from_output(&output) {
        assert!(
            summary.files_copied < 3,
            "Should not copy all files with fail-early, got {}",
            summary.files_copied
        );
    }
}

#[test]
fn test_remote_copy_nested_directories_with_unreadable_files() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create nested directory structure with some unreadable files
    let src_root = src_dir.path().join("root");
    std::fs::create_dir(&src_root).unwrap();
    create_test_file(&src_root.join("root_file.txt"), "root content", 0o644);
    create_test_file(&src_root.join("unreadable_root.txt"), "secret root", 0o000);
    // readable subdirectory with mixed readable/unreadable files
    let subdir = src_root.join("subdir");
    std::fs::create_dir(&subdir).unwrap();
    create_test_file(&subdir.join("good.txt"), "good content", 0o644);
    create_test_file(&subdir.join("secret.txt"), "secret content", 0o000);
    // another readable file
    create_test_file(&src_root.join("zzz_last.txt"), "last content", 0o644);
    let dst_root = dst_dir.path().join("root");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // without --fail-early, should continue despite unreadable files
    let output = run_rcp_and_expect_failure(&["--summary", &src_remote, &dst_remote]);
    // verify readable content was copied
    assert!(dst_root.join("root_file.txt").exists());
    assert!(dst_root.join("subdir").exists());
    assert!(dst_root.join("subdir/good.txt").exists());
    assert!(dst_root.join("zzz_last.txt").exists());
    assert_eq!(
        get_file_content(&dst_root.join("root_file.txt")),
        "root content"
    );
    assert_eq!(
        get_file_content(&dst_root.join("subdir/good.txt")),
        "good content"
    );
    assert_eq!(
        get_file_content(&dst_root.join("zzz_last.txt")),
        "last content"
    );
    // verify unreadable files were not copied
    assert!(!dst_root.join("unreadable_root.txt").exists());
    assert!(!dst_root.join("subdir/secret.txt").exists());
    // verify summary: 3 readable files copied, 2 directories created
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 3);
    assert_eq!(summary.directories_created, 2); // root + subdir
    // verify non-zero exit code
    assert!(!output.status.success());
}

/// Returns true if this process can read a freshly-created `chmod 000` directory.
/// That only happens when running effectively as root (which bypasses the rwx
/// permission bits), in which case the unreadable-directory tests cannot reproduce
/// the EACCES they rely on and must be skipped.
fn can_read_unreadable_dir() -> bool {
    let probe = tempfile::tempdir().unwrap();
    let dir = probe.path().join("probe_000");
    std::fs::create_dir(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).unwrap();
    let readable = std::fs::read_dir(&dir).is_ok();
    // restore perms so the tempdir can be cleaned up
    let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755));
    readable
}

/// A classified but unreadable root sends Begin and End(0), then consumes Ready or Skipped.
/// The directory lands empty and the original read failure remains an error for the operation.
#[test]
fn test_remote_copy_unreadable_root_directory_continues() {
    require_local_ssh();
    if can_read_unreadable_dir() {
        eprintln!(
            "skipping test_remote_copy_unreadable_root_directory_continues: running as root, \
             chmod 000 directories remain readable so EACCES cannot be reproduced"
        );
        return;
    }
    let (src_dir, dst_dir) = setup_test_env();
    // a directory we own that exists but cannot be opened for reading: symlink_metadata
    // still reports it as a directory (the source commits to sending it), but
    // open_root_dir gets EACCES.
    let src_subdir = src_dir.path().join("unreadable_root");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("hidden.txt"), "cannot be read", 0o644);
    std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o000)).unwrap();
    let dst_subdir = dst_dir.path().join("unreadable_root");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // without --fail-early: the unreadable directory becomes an empty destination
    // directory and the copy reports a (partial) error for the unreadable dir, but
    // must NOT hang or abort with a fail-closed "no held directory fd" error.
    let output = run_rcp_with_args(&["--summary", &src_remote, &dst_remote]);
    print_command_output(&output);
    // restore perms so the source tempdir cleans up
    let _ = std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o755));
    // the empty destination directory must have been created (the empty dir "landed").
    // it inherits the source's 0o000 mode, so make it readable before inspecting it.
    assert!(
        dst_subdir.is_dir(),
        "destination directory should exist as an empty directory"
    );
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        std::fs::read_dir(&dst_subdir).unwrap().count(),
        0,
        "destination directory should be empty (its contents were unreadable)"
    );
    // critically: the copy must not have hung / been killed by the timeout wrapper
    // and must not have aborted with the fail-closed message. The unreadable dir is
    // still reported as an error, so the exit code is non-zero, but the run completed.
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !combined.contains("no held directory fd"),
        "copy must not fail closed for a committed unreadable directory (tombstone should be consumed)"
    );
}

/// A counted-child `FileSkipped` received under a FAILED (untracked) parent must be
/// a no-op on the destination, not abort the whole copy with "not being tracked".
///
/// Reproduces PR #247 review: the destination cannot create directory `conflict`
/// because a NON-DIRECTORY already exists there and `--overwrite` is NOT set, so
/// `conflict` lands in `failed_directories` (NOT `pending_directories`). The source
/// meanwhile fails to `open_dir` the counted child `conflict/sub` (it is `chmod 000`)
/// and emits a counted-child `FileSkipped` for it. That `FileSkipped` routes to
/// `process_file(conflict)` on the destination. Before the fix this hit the
/// fail-closed "directory {:?} not being tracked" path and aborted the destination
/// mid-protocol (manifesting as an abort/hang); after the fix it is tolerated and the
/// copy completes (reporting the non-fatal `conflict` non-directory error, exit != 0).
#[test]
fn test_remote_copy_file_skipped_under_failed_parent_does_not_abort() {
    require_local_ssh();
    if can_read_unreadable_dir() {
        eprintln!(
            "skipping test_remote_copy_file_skipped_under_failed_parent_does_not_abort: \
             running as root, chmod 000 directories remain readable so EACCES cannot be reproduced"
        );
        return;
    }
    let (src_dir, dst_dir) = setup_test_env();
    // source: a directory `conflict` whose ONLY child is the subdirectory `sub`
    // (so `conflict`'s entry_count == 1, file_count == 0). `sub` is chmod 000 so the
    // source's `open_dir("sub")` from `conflict`'s held fd fails EACCES after admission,
    // emitting a counted-child `FileSkipped` for `sub` under parent `conflict`.
    let src_conflict = src_dir.path().join("conflict");
    std::fs::create_dir(&src_conflict).unwrap();
    let src_sub = src_conflict.join("sub");
    std::fs::create_dir(&src_sub).unwrap();
    create_test_file(&src_sub.join("hidden.txt"), "cannot be read", 0o644);
    std::fs::set_permissions(&src_sub, std::fs::Permissions::from_mode(0o000)).unwrap();
    // destination: pre-create `dst/` (a real directory) and `dst/conflict` as a FILE,
    // so the destination's mkdir of `conflict` fails as a non-directory conflict and
    // `conflict` is added to `failed_directories` (no `--overwrite`).
    let dst_conflict = dst_dir.path().join("conflict");
    create_test_file(&dst_conflict, "i am a file, not a directory", 0o644);
    let src_remote = format!("localhost:{}", src_conflict.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_conflict.to_str().unwrap());
    // default hardened mode (no --dereference), WITHOUT --overwrite, WITHOUT --fail-early.
    let output = run_rcp_with_args(&["--summary", &src_remote, &dst_remote]);
    print_command_output(&output);
    // restore perms so the source tempdir cleans up
    let _ = std::fs::set_permissions(&src_sub, std::fs::Permissions::from_mode(0o755));
    // the run must have completed (not hung / killed by the timeout wrapper — already
    // asserted in run_rcp_with_args_internal) and must NOT have aborted the destination
    // mid-protocol with the pre-fix "not being tracked" message.
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !combined.contains("not being tracked"),
        "destination must tolerate a FileSkipped under a failed parent, not abort with \
         'not being tracked'. Combined output:\n{combined}"
    );
    // the non-directory `conflict` conflict is still a (non-fatal) error, so exit != 0.
    assert!(
        !output.status.success(),
        "the non-directory conflict should still be reported as an error (exit != 0)"
    );
}

/// Dereference (`-L`) discovery follows the root symlink and copies a real directory tree.
#[test]
fn test_remote_copy_dereference_directory_tree() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // a real directory tree with nested files...
    let target_dir = src_dir.path().join("real_tree");
    std::fs::create_dir(&target_dir).unwrap();
    create_test_file(&target_dir.join("a.txt"), "alpha", 0o644);
    create_test_file(&target_dir.join("b.txt"), "bravo", 0o644);
    let nested = target_dir.join("nested");
    std::fs::create_dir(&nested).unwrap();
    create_test_file(&nested.join("c.txt"), "charlie", 0o644);
    // ...reached through a symlink, so -L must follow it and copy a directory.
    let link = src_dir.path().join("tree_link");
    std::os::unix::fs::symlink(&target_dir, &link).unwrap();
    let dst_path = dst_dir.path().join("copied_tree");
    let src_remote = format!("localhost:{}", link.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["-L", "--summary", &src_remote, &dst_remote]);
    // result is a real directory tree, not a symlink
    assert!(dst_path.is_dir());
    assert!(!dst_path.is_symlink());
    assert_eq!(get_file_content(&dst_path.join("a.txt")), "alpha");
    assert_eq!(get_file_content(&dst_path.join("b.txt")), "bravo");
    assert_eq!(get_file_content(&dst_path.join("nested/c.txt")), "charlie");
    // 3 files across 2 directories (root + nested) copied
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 3);
    assert_eq!(summary.directories_created, 2);
}

#[test]
fn test_remote_copy_mixed_success_with_symlink_errors() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create directory with files and symlinks, some operations will fail
    let src_subdir = src_dir.path().join("mixed_ops");
    std::fs::create_dir(&src_subdir).unwrap();
    // regular file that will succeed
    create_test_file(&src_subdir.join("good_file.txt"), "good content", 0o644);
    // create a symlink to a file
    let target = src_subdir.join("target.txt");
    create_test_file(&target, "target content", 0o644);
    std::os::unix::fs::symlink(&target, src_subdir.join("good_symlink")).unwrap();
    // unreadable file
    create_test_file(&src_subdir.join("unreadable.txt"), "secret", 0o000);
    // another good file
    create_test_file(
        &src_subdir.join("zzz_another.txt"),
        "another content",
        0o644,
    );
    let dst_subdir = dst_dir.path().join("mixed_ops");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&["--summary", &src_remote, &dst_remote]);
    // verify successful operations
    assert!(dst_subdir.join("good_file.txt").exists());
    assert!(dst_subdir.join("good_symlink").exists());
    assert!(dst_subdir.join("target.txt").exists());
    assert!(dst_subdir.join("zzz_another.txt").exists());
    assert_eq!(
        get_file_content(&dst_subdir.join("good_file.txt")),
        "good content"
    );
    assert_eq!(
        get_file_content(&dst_subdir.join("target.txt")),
        "target content"
    );
    assert_eq!(
        get_file_content(&dst_subdir.join("zzz_another.txt")),
        "another content"
    );
    // verify symlink
    assert!(dst_subdir.join("good_symlink").is_symlink());
    // verify failed operations
    assert!(!dst_subdir.join("unreadable.txt").exists());
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 3); // good_file.txt, target.txt, zzz_another.txt
    assert_eq!(summary.symlinks_created, 1); // good_symlink
    assert_eq!(summary.directories_created, 1); // mixed_ops
    // verify non-zero exit code
    assert!(!output.status.success());
}

#[test]
fn test_remote_copy_all_operations_fail() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create a directory with only unreadable files
    let src_subdir = src_dir.path().join("all_fail");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("unreadable1.txt"), "secret 1", 0o000);
    create_test_file(&src_subdir.join("unreadable2.txt"), "secret 2", 0o000);
    create_test_file(&src_subdir.join("unreadable3.txt"), "secret 3", 0o000);
    let dst_subdir = dst_dir.path().join("all_fail");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&["--summary", &src_remote, &dst_remote]);
    // verify directory was created but no files
    assert!(dst_subdir.exists());
    assert!(!dst_subdir.join("unreadable1.txt").exists());
    assert!(!dst_subdir.join("unreadable2.txt").exists());
    assert!(!dst_subdir.join("unreadable3.txt").exists());
    // verify summary shows only directory creation
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 0);
    assert_eq!(summary.directories_created, 1);
    assert_eq!(summary.bytes_copied, 0);
    // verify non-zero exit code
    assert!(!output.status.success());
}

#[test]
fn test_remote_copy_unwritable_destination() {
    // this test verifies behavior when destination directory is not writable
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source file
    let src_file = src_dir.path().join("source.txt");
    create_test_file(&src_file, "source content", 0o644);
    // create destination directory with no write permissions
    let dst_subdir = dst_dir.path().join("readonly_dir");
    std::fs::create_dir(&dst_subdir).unwrap();
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let dst_file = dst_subdir.join("destination.txt");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&["--summary", &src_remote, &dst_remote]);
    // verify file was not created
    assert!(!dst_file.exists());
    // verify summary shows no files copied
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 0);
    // restore permissions for cleanup
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn test_remote_copy_destination_partial_write_failure() {
    // this test verifies that when some files can't be written to the destination,
    // other files in the same transfer still get copied (stream recovery works).
    // this exercises the destination-side error handling with stream pooling.
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source directory structure:
    // src/
    //   good_dir/
    //     file1.txt
    //     file2.txt
    //   bad_dir/
    //     file3.txt  <- will fail (destination not writable)
    //     file4.txt  <- will fail
    //   more_good/
    //     file5.txt
    let src_root = src_dir.path().join("mixed");
    let src_good_dir = src_root.join("good_dir");
    let src_bad_dir = src_root.join("bad_dir");
    let src_more_good = src_root.join("more_good");
    std::fs::create_dir_all(&src_good_dir).unwrap();
    std::fs::create_dir_all(&src_bad_dir).unwrap();
    std::fs::create_dir_all(&src_more_good).unwrap();
    create_test_file(&src_good_dir.join("file1.txt"), "content 1", 0o644);
    create_test_file(&src_good_dir.join("file2.txt"), "content 2", 0o644);
    create_test_file(&src_bad_dir.join("file3.txt"), "content 3", 0o644);
    create_test_file(&src_bad_dir.join("file4.txt"), "content 4", 0o644);
    create_test_file(&src_more_good.join("file5.txt"), "content 5", 0o644);
    // create destination with bad_dir being unwritable
    let dst_root = dst_dir.path().join("mixed");
    let dst_bad_dir = dst_root.join("bad_dir");
    std::fs::create_dir_all(&dst_bad_dir).unwrap();
    std::fs::set_permissions(&dst_bad_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}/", dst_dir.path().to_str().unwrap());
    // run without --fail-early to continue after errors
    let output = run_rcp_with_args(&["-vv", "--summary", &src_remote, &dst_remote]);
    print_command_output(&output);
    // restore permissions before assertions (for cleanup)
    std::fs::set_permissions(&dst_bad_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    // verify: good files should be copied despite bad_dir failures
    let dst_good_dir = dst_root.join("good_dir");
    let dst_more_good = dst_root.join("more_good");
    assert!(
        dst_good_dir.join("file1.txt").exists(),
        "file1.txt should be copied"
    );
    assert!(
        dst_good_dir.join("file2.txt").exists(),
        "file2.txt should be copied"
    );
    assert!(
        dst_more_good.join("file5.txt").exists(),
        "file5.txt should be copied"
    );
    // verify: bad_dir files should NOT be copied
    assert!(
        !dst_bad_dir.join("file3.txt").exists(),
        "file3.txt should NOT be copied (permission denied)"
    );
    assert!(
        !dst_bad_dir.join("file4.txt").exists(),
        "file4.txt should NOT be copied (permission denied)"
    );
    // verify content of copied files
    assert_eq!(
        get_file_content(&dst_good_dir.join("file1.txt")),
        "content 1"
    );
    assert_eq!(
        get_file_content(&dst_good_dir.join("file2.txt")),
        "content 2"
    );
    assert_eq!(
        get_file_content(&dst_more_good.join("file5.txt")),
        "content 5"
    );
    // verify non-zero exit code (some files failed)
    assert!(
        !output.status.success(),
        "should have non-zero exit due to permission errors"
    );
}

#[test]
fn test_remote_copy_deeply_nested_directory_failure() {
    // this test verifies that when a directory fails to be created, all its deeply
    // nested descendants (3+ levels deep) are handled correctly without
    // "Directory not being tracked" errors.
    //
    // the race condition being tested:
    // 1. source sends DirStub for bad_dir, then dir2, dir3, dir4 in quick succession
    // 2. destination tries to create bad_dir but FAILS (already exists as directory)
    // 3. destination sends DirectoryFailed for bad_dir
    // 4. meanwhile, dir2/dir3/dir4 DirStubs arrive and directories are created
    // 5. when dir4 completes and tries to decrement dir3's entry count,
    //    we must check if ANY ancestor (bad_dir) is in failed_directories
    //
    // without the has_failed_ancestor() check, this would crash with:
    // "Directory dir3 not being tracked" because bad_dir failed and wasn't tracked
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // source structure:
    //   src/
    //     good_dir/
    //       file_good.txt
    //     bad_dir/           <- will fail (already exists at destination)
    //       dir2/
    //         dir3/
    //           dir4/
    //             deep_file.txt
    let src_root = src_dir.path().join("src");
    let src_good_dir = src_root.join("good_dir");
    let src_bad_dir = src_root.join("bad_dir");
    let src_dir2 = src_bad_dir.join("dir2");
    let src_dir3 = src_dir2.join("dir3");
    let src_dir4 = src_dir3.join("dir4");
    std::fs::create_dir_all(&src_good_dir).unwrap();
    std::fs::create_dir_all(&src_dir4).unwrap();
    create_test_file(&src_good_dir.join("file_good.txt"), "good content", 0o644);
    create_test_file(&src_dir4.join("deep_file.txt"), "deep content", 0o644);
    // destination structure: pre-create src/ as a directory (will be reused with --overwrite)
    // and create bad_dir as a FILE (not directory) - this will fail even with --overwrite
    // because rcp cannot replace a file with a directory without explicit removal
    let dst_root = dst_dir.path().join("src");
    std::fs::create_dir_all(&dst_root).unwrap();
    // create bad_dir as a FILE (not directory) to block directory creation
    let dst_bad_dir_file = dst_root.join("bad_dir");
    std::fs::write(&dst_bad_dir_file, "i am a file blocking bad_dir").unwrap();
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}/", dst_dir.path().to_str().unwrap());
    // run WITH --overwrite so src/ can be reused (it already exists as directory)
    // but bad_dir will still fail because it's a file and --overwrite will try to
    // replace it - which should work! let's check the behavior...
    // actually with --overwrite, a file WILL be replaced with directory. so we need
    // to run WITHOUT --overwrite to get the failure.
    // BUT then src/ will also fail because it already exists...
    //
    // use --overwrite and make bad_dir non-writable (0o555 = r-xr-xr-x) so dir2 can't be created
    std::fs::remove_file(&dst_bad_dir_file).unwrap();
    std::fs::create_dir(&dst_bad_dir_file).unwrap();
    std::fs::set_permissions(&dst_bad_dir_file, std::fs::Permissions::from_mode(0o555)).unwrap();
    let output = run_rcp_with_args(&["--overwrite", "-vv", "--summary", &src_remote, &dst_remote]);
    // restore permissions for cleanup BEFORE any assertions
    std::fs::set_permissions(&dst_bad_dir_file, std::fs::Permissions::from_mode(0o755)).unwrap();
    print_command_output(&output);
    // the key assertion: no panic with "Directory not being tracked"
    // if we get here without panicking, the ancestor check is working
    // verify the output doesn't contain the error we're trying to prevent
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("not being tracked"),
        "should not have 'Directory not being tracked' error"
    );
    // verify non-zero exit code (directory creation failed)
    assert!(
        !output.status.success(),
        "should have non-zero exit due to directory already exists error"
    );
}

#[test]
fn test_remote_copy_directory_permissions_preserved_despite_file_errors() {
    // this test verifies that DirectoryComplete messages are sent even when
    // some files in the directory fail to copy. DirectoryComplete is responsible
    // for finalizing directory metadata (permissions, mtime). if it's not sent
    // on error paths, the destination directory won't have the correct metadata.
    //
    // we verify this by:
    // 1. creating a source directory with non-default permissions (0o750)
    // 2. having some files inside fail to copy (unreadable source files)
    // 3. using --preserve to copy metadata
    // 4. verifying the destination directory has the correct permissions
    //
    // note: we use unreadable SOURCE files instead of unwritable destination
    // directories because pre-creating destination directories causes "File exists"
    // errors that skip all contents.
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source structure with specific permissions
    let src_root = src_dir.path().join("preserved");
    let src_parent = src_root.join("parent_with_perms");
    std::fs::create_dir_all(&src_parent).unwrap();
    // create files: one readable (will succeed), one unreadable (will fail)
    create_test_file(&src_parent.join("good_file.txt"), "this will copy", 0o644);
    create_test_file(&src_parent.join("bad_file.txt"), "unreadable", 0o000);
    // set parent directory to non-default permissions (0o750 = rwxr-x---)
    // do this AFTER creating files so we can still write to the directory
    std::fs::set_permissions(&src_parent, std::fs::Permissions::from_mode(0o750)).unwrap();
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}/", dst_dir.path().to_str().unwrap());
    // run with --preserve to copy permissions
    let output = run_rcp_with_args(&["--preserve", "--summary", &src_remote, &dst_remote]);
    print_command_output(&output);
    // restore source permissions for cleanup
    std::fs::set_permissions(&src_parent, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(
        src_parent.join("bad_file.txt"),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    // verify destinations
    let dst_root = dst_dir.path().join("preserved");
    let dst_parent = dst_root.join("parent_with_perms");
    // verify the good file was copied
    assert!(
        dst_parent.join("good_file.txt").exists(),
        "good_file.txt should be copied"
    );
    assert_eq!(
        get_file_content(&dst_parent.join("good_file.txt")),
        "this will copy"
    );
    // verify the bad file was NOT copied (unreadable source)
    assert!(
        !dst_parent.join("bad_file.txt").exists(),
        "bad_file.txt should NOT be copied (permission denied on source)"
    );
    // KEY ASSERTION: verify parent directory permissions were preserved
    // this proves DirectoryComplete was sent even though a child file failed
    let dst_parent_mode = dst_parent.metadata().unwrap().permissions().mode() & 0o777;
    assert_eq!(
        dst_parent_mode, 0o750,
        "parent directory should have preserved permissions (0o750), got 0o{:o}. \
         This indicates DirectoryComplete was not sent after file errors.",
        dst_parent_mode
    );
    // non-zero exit due to failures
    assert!(
        !output.status.success(),
        "should have non-zero exit due to permission errors"
    );
    eprintln!("✓ Directory permissions preserved despite file copy errors");
}

// ============================================================================
// Lifecycle Management Tests
// ============================================================================

/// find rcpd processes running on the system
#[cfg(target_os = "linux")]
fn find_rcpd_processes() -> Vec<u32> {
    let output = std::process::Command::new("pgrep")
        .arg("-x") // exact match
        .arg("rcpd")
        .output()
        .expect("Failed to run pgrep");
    if !output.status.success() {
        return vec![];
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .collect()
}

/// Read a file through a held `/proc/<pid>` directory descriptor.
#[cfg(target_os = "linux")]
fn read_held_proc_file(proc_dir: &std::fs::File, name: &std::ffi::CStr) -> Option<Vec<u8>> {
    use std::io::Read;
    use std::os::fd::{AsRawFd, FromRawFd};

    // SAFETY: `proc_dir` is a live directory descriptor, `name` is NUL-terminated, and the
    // returned descriptor is immediately wrapped in `File` to give it exactly one owner.
    let fd = unsafe {
        libc::openat(
            proc_dir.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return None;
    }
    // SAFETY: `openat` returned a new owned descriptor on the success path above.
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    let mut raw = Vec::new();
    file.read_to_end(&mut raw).ok()?;
    Some(raw)
}

/// Read a process's raw argv through its held proc descriptor.
#[cfg(target_os = "linux")]
fn read_proc_argv(proc_dir: &std::fs::File) -> Option<Vec<Vec<u8>>> {
    let raw = read_held_proc_file(proc_dir, c"cmdline")?;
    Some(
        raw.split(|byte| *byte == 0)
            .filter(|arg| !arg.is_empty())
            .map(<[u8]>::to_vec)
            .collect(),
    )
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ProcessIdentity {
    pid: u32,
    start_time_ticks: u64,
}

/// Read the state and kernel start time through a held proc descriptor.
#[cfg(target_os = "linux")]
fn read_process_state_and_start_time(proc_dir: &std::fs::File) -> Option<(char, u64)> {
    let stat = String::from_utf8(read_held_proc_file(proc_dir, c"stat")?).ok()?;
    let (_, fields) = stat.rsplit_once(") ")?;
    let fields = fields.split_whitespace().collect::<Vec<_>>();
    let state = fields.first()?.chars().next()?;
    let start_time_ticks = fields.get(19)?.parse().ok()?;
    Some((state, start_time_ticks))
}

/// A process reference whose proc data and signals stay bound across pid reuse.
#[cfg(target_os = "linux")]
#[derive(Clone)]
struct ProcessHandle {
    identity: ProcessIdentity,
    pidfd: std::sync::Arc<std::fs::File>,
    proc_dir: std::sync::Arc<std::fs::File>,
}

#[cfg(target_os = "linux")]
impl std::fmt::Debug for ProcessHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.identity.fmt(formatter)
    }
}

#[cfg(target_os = "linux")]
impl ProcessHandle {
    /// Open the pidfd before validating argv, then prove that process is still live afterward.
    /// If the pid is reused anywhere in between, the pidfd still names the exited predecessor and
    /// the final signal-zero probe rejects the candidate.
    fn open_marked(pid: u32, marker_arg: &[u8], role: Option<&str>) -> Option<Self> {
        use std::os::fd::FromRawFd;

        // SAFETY: `pidfd_open` has no pointer arguments. Its successful return is a new owned fd.
        let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if pidfd < 0 {
            return None;
        }
        // SAFETY: the successful syscall returned a new owned descriptor.
        let pidfd = unsafe { std::fs::File::from_raw_fd(pidfd as std::os::fd::RawFd) };
        let proc_dir = std::fs::File::open(format!("/proc/{pid}")).ok()?;
        let argv = read_proc_argv(&proc_dir)?;
        if !argv.iter().any(|arg| arg == marker_arg)
            || !role.is_none_or(|role| {
                argv.windows(2).any(|pair| {
                    pair[0].as_slice() == b"--role" && pair[1].as_slice() == role.as_bytes()
                })
            })
        {
            return None;
        }
        let (_, start_time_ticks) = read_process_state_and_start_time(&proc_dir)?;
        let handle = Self {
            identity: ProcessIdentity {
                pid,
                start_time_ticks,
            },
            pidfd: std::sync::Arc::new(pidfd),
            proc_dir: std::sync::Arc::new(proc_dir),
        };
        handle.signal(0).ok()?;
        Some(handle)
    }

    fn pid(&self) -> u32 {
        self.identity.pid
    }

    fn state(&self) -> Option<char> {
        let (state, start_time_ticks) = read_process_state_and_start_time(&self.proc_dir)?;
        (start_time_ticks == self.identity.start_time_ticks).then_some(state)
    }

    fn signal(&self, signal: libc::c_int) -> std::io::Result<()> {
        use std::os::fd::AsRawFd;

        // SAFETY: the pidfd is owned and live, the siginfo pointer is null, and flags must be zero.
        let result = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.pidfd.as_raw_fd(),
                signal,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
}

/// Find only rcpd processes carrying one test run's unique debug-log marker, optionally restricted
/// to a role, and bind every result to held pidfd and proc descriptors.
#[cfg(target_os = "linux")]
fn find_marked_rcpd_processes(marker: &str, role: Option<&str>) -> Vec<ProcessHandle> {
    let marker_arg = format!("--debug-log-prefix={marker}").into_bytes();
    find_rcpd_processes()
        .into_iter()
        .filter_map(|pid| ProcessHandle::open_marked(pid, &marker_arg, role))
        .collect()
}

/// Own the actual rcp master and clean up every marked daemon on any test exit, including panic.
#[cfg(target_os = "linux")]
struct MarkedRemoteRun {
    master: Option<std::process::Child>,
    marker: String,
}

#[cfg(target_os = "linux")]
impl MarkedRemoteRun {
    fn new(master: std::process::Child, marker: String) -> Self {
        Self {
            master: Some(master),
            marker,
        }
    }
    fn master_pid(&self) -> u32 {
        self.master
            .as_ref()
            .expect("rcp master already reaped")
            .id()
    }
    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.master
            .as_mut()
            .expect("rcp master already reaped")
            .try_wait()
    }
    fn kill_master(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let mut master = self.master.take().expect("rcp master already reaped");
        if let Err(error) = master.kill() {
            let _ = master.wait();
            return Err(error);
        }
        master.wait()
    }
}

#[cfg(target_os = "linux")]
impl Drop for MarkedRemoteRun {
    fn drop(&mut self) {
        if let Some(mut master) = self.master.take()
            && !matches!(master.try_wait(), Ok(Some(_)))
        {
            let _ = master.kill();
            let _ = master.wait();
        }
        for process in find_marked_rcpd_processes(&self.marker, None) {
            let _ = process.signal(libc::SIGKILL);
        }
    }
}

/// create a large test file to ensure copy takes several seconds
fn create_large_test_file(path: &std::path::Path, size_mb: usize) {
    use std::io::Write;
    let mut file = std::fs::File::create(path).unwrap();
    let chunk = vec![b'A'; 1024 * 1024]; // 1MB chunk
    for _ in 0..size_mb {
        file.write_all(&chunk).unwrap();
    }
    file.flush().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn test_remote_rcpd_exits_when_master_killed() {
    require_local_ssh();
    // each rcpd reserves 50 one-MiB DATA tokens at five tokens/sec, keeping the copy alive long
    // enough to observe both marked roles without relying on loopback throughput or metadata ops.
    const IOPS_THROTTLE: usize = 5;
    const FILE_SIZE_MIB: usize = 50;
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("throttled_file.dat");
    create_large_test_file(&src_file, FILE_SIZE_MIB);
    let dst_file = dst_dir.path().join("large_file.dat");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    let rcpd_log_dir = tempfile::TempDir::new().expect("Failed to create rcpd debug log dir");
    let rcpd_marker = rcpd_log_dir.path().join("rcpd-debug").display().to_string();
    let rcpd_log_arg = format!("--rcpd-debug-log-prefix={rcpd_marker}");
    let stdout_file = tempfile::NamedTempFile::new().expect("Failed to create stdout capture file");
    let stderr_file = tempfile::NamedTempFile::new().expect("Failed to create stderr capture file");
    let iops_arg = format!("--iops-throttle={IOPS_THROTTLE}");
    let mut command = std::process::Command::new(rcp_path);
    command.args([
        "-vv",
        "--force-remote",
        "--chunk-size=1MiB",
        &iops_arg,
        &rcpd_log_arg,
        &src_remote,
        &dst_remote,
    ]);
    command.stdout(
        stdout_file
            .reopen()
            .expect("Failed to reopen stdout capture file"),
    );
    command.stderr(
        stderr_file
            .reopen()
            .expect("Failed to reopen stderr capture file"),
    );
    let read_captured_output = |status: std::process::ExitStatus| -> std::process::Output {
        std::process::Output {
            status,
            stdout: std::fs::read(stdout_file.path()).unwrap_or_default(),
            stderr: std::fs::read(stderr_file.path()).unwrap_or_default(),
        }
    };
    let scenario_start = std::time::Instant::now();
    let master = command.spawn().expect("Failed to spawn rcp master");
    let mut run = MarkedRemoteRun::new(master, rcpd_marker.clone());
    let marked_processes = loop {
        let source_processes = find_marked_rcpd_processes(&rcpd_marker, Some("source"));
        let destination_processes = find_marked_rcpd_processes(&rcpd_marker, Some("destination"));
        let role_hellos_received = rcpd_role_hellos_received(rcpd_log_dir.path());
        if !source_processes.is_empty() && !destination_processes.is_empty() && role_hellos_received
        {
            break source_processes
                .into_iter()
                .chain(destination_processes)
                .collect::<Vec<_>>();
        }
        if let Some(status) = run.try_wait().expect("Failed to poll rcp master") {
            print_command_output(&read_captured_output(status));
            panic!(
                "the master-kill scenario was never reached: rcp exited before both marked rcpd \
                 roles were running and had consumed their master hellos"
            );
        }
        if scenario_start.elapsed() >= std::time::Duration::from_secs(20) {
            let status = run
                .kill_master()
                .expect("Failed to stop rcp master after scenario timeout");
            print_command_output(&read_captured_output(status));
            panic!(
                "the master-kill scenario was never reached within {:?}: found source {:?}, \
                 destination {:?}, both master hellos received: {role_hellos_received}",
                scenario_start.elapsed(),
                source_processes,
                destination_processes
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    if let Some(status) = run.try_wait().expect("Failed to poll rcp master") {
        print_command_output(&read_captured_output(status));
        panic!("rcp master exited before it could be killed");
    }
    let master_pid = run.master_pid();
    eprintln!("Killing rcp master {master_pid}; marked rcpd processes: {marked_processes:?}");
    let status = run.kill_master().expect("Failed to SIGKILL rcp master");
    let output = read_captured_output(status);
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&output.status),
        Some(libc::SIGKILL),
        "the direct rcp child was not terminated by SIGKILL"
    );
    let exit_start = std::time::Instant::now();
    loop {
        let remaining = marked_processes
            .iter()
            .filter_map(|process| process.state().map(|state| (process.identity, state)))
            .collect::<Vec<_>>();
        if remaining.is_empty() {
            break;
        }
        if exit_start.elapsed() >= std::time::Duration::from_secs(5) {
            print_command_output(&output);
            panic!(
                "marked rcpd processes did not exit within {:?} after master {master_pid} was \
                 killed: {remaining:?}",
                exit_start.elapsed()
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    eprintln!(
        "All marked rcpd processes exited in {:?} after master {master_pid} was killed",
        exit_start.elapsed()
    );
}

#[cfg(target_os = "linux")]
#[test]
fn test_remote_destination_rcpd_killed_reports_error_not_abort() {
    // regression test: the master used to `.expect()` a clean EOF when a peer's control
    // connection closed without a message. Since the workspace sets `panic = "abort"`, that
    // hit SIGABRT (exit 134, core dump, discarded tracing output) instead of the intended
    // "print error chain, exit 1" path. Verify that killing the destination rcpd mid-copy now
    // produces a clean reported error.
    //
    // --no-encryption is required: under TLS a dead peer surfaces as `Err` and already takes
    // the correct path, which would make this test vacuous for the bug being verified.
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // The window in which the destination rcpd is alive but the copy has not finished has to be
    // ENGINEERED, not hoped for. A large file on its own does not do it — 50 MiB over loopback lands
    // in well under a second — and neither does `--ops-throttle`, which gates METADATA syscalls, of
    // which a single-file copy has only a handful. Left to chance the copy finishes before the kill
    // and the test fails on the "no destination rcpd of ours to kill" assert below.
    //
    // `--iops-throttle` is the lever that bites, because it gates the DATA path: rcpd takes
    // `((size - 1) / chunk_size) + 1` tokens per file before it writes any of it (see
    // `rcp::destination::process_single_file`). 50 MiB at a 1 MiB chunk is 50 tokens, and at 5
    // tokens/sec that is a ~10s stall — deterministic, bounded, and comfortably inside the 40s
    // `timeout` below. Both rcpds get the setting (`RcpdConfig::to_args` forwards
    // `--iops-throttle`/`--chunk-size`), so the transfer cannot outrun the kill from either end.
    const IOPS_THROTTLE: usize = 5;
    const FILE_SIZE_MIB: usize = 50;
    let src_file = src_dir.path().join("large_file.dat");
    eprintln!("Creating {FILE_SIZE_MIB}MiB test file...");
    create_large_test_file(&src_file, FILE_SIZE_MIB);
    let dst_file = dst_dir.path().join("large_file.dat");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    // give the rcpd processes this test spawns a marker unique to this test run, so the kill below
    // can be scoped to them. `--rcpd-debug-log-prefix` is the only free-form string the master
    // forwards verbatim into rcpd's argv (as `--debug-log-prefix=<value>`) - everything else rcpd
    // is told, operands included, travels over the control connection rather than the command line.
    // the temp dir's random name supplies the uniqueness, and holding the `TempDir` for the whole
    // test both keeps the prefix directory alive for rcpd (which panics if it cannot create the
    // log file) and cleans the logs up afterwards.
    let rcpd_log_dir = tempfile::TempDir::new().expect("Failed to create rcpd debug log dir");
    let rcpd_marker = rcpd_log_dir.path().join("rcpd-debug").display().to_string();
    let rcpd_log_arg = format!("--rcpd-debug-log-prefix={rcpd_marker}");
    // capture stdout/stderr via real files rather than `Stdio::piped()`: -vv is verbose enough
    // to fill a pipe's kernel buffer well before the process exits, and nothing drains a piped
    // child's output while we're off polling with pgrep below - that combination deadlocks the
    // child on a blocked write(). Files have no such limit.
    let stdout_file = tempfile::NamedTempFile::new().expect("Failed to create stdout capture file");
    let stderr_file = tempfile::NamedTempFile::new().expect("Failed to create stderr capture file");
    // wrap in `timeout` (as run_rcp_with_args_internal does) so a regression that hangs the
    // master instead of erroring fails the test instead of hanging CI forever.
    let mut cmd = std::process::Command::new("timeout");
    cmd.args(["40", rcp_path.to_str().unwrap()]);
    let iops_arg = format!("--iops-throttle={IOPS_THROTTLE}");
    cmd.args([
        "-vv",
        "--force-remote",
        "--no-encryption",
        "--chunk-size=1MiB",
        &iops_arg,
        &rcpd_log_arg,
        &src_remote,
        &dst_remote,
    ]);
    cmd.stdout(
        stdout_file
            .reopen()
            .expect("Failed to reopen stdout capture file"),
    );
    cmd.stderr(
        stderr_file
            .reopen()
            .expect("Failed to reopen stderr capture file"),
    );
    let spawn_start = std::time::Instant::now();
    eprintln!("Spawning rcp subprocess...");
    let mut child = cmd.spawn().expect("Failed to spawn rcp");
    // Barrier: wait until the DESTINATION rcpd has CONSUMED the MasterHello - not merely until the
    // master sent it, and certainly not just until the destination process exists. Two properties
    // follow, and the test needs both:
    //
    //   * the master is at (or immediately about to be at) the `dest_recv_stream.recv_object()`
    //     call this test exercises. Killing earlier - while the destination has merely been spawned,
    //     before it reports its listening address on stdout for the master to read over the SSH
    //     channel - races a *different*, already-correct error path ("unexpected output from rcpd").
    //
    //   * the destination's control socket receive queue is DRAINED. That is what decides FIN vs
    //     RST: SIGKILL yields a clean FIN only when nothing is left unread, and an RST otherwise.
    //     A clean FIN surfaces to the master as `Ok(None)`, which is precisely the branch the
    //     original `.expect()` panicked on. Killing with the hello still queued would produce an
    //     RST and exercise a different branch, so the regression would only be caught by luck.
    //
    // rcpd logs "Received side: Destination { .. }" (rcp/src/bin/rcpd.rs) on the line immediately
    // after that recv returns, into the per-run debug log this test already configures - so the log
    // line IS the acknowledgement. `--role source` produces "Received side: Source", so matching on
    // the variant name picks out the destination without needing to know the log file naming.
    let dest_ready_marker = "Received side: Destination";
    let mut ready = false;
    while spawn_start.elapsed().as_secs() < 20 {
        if rcpd_logs_contain(rcpd_log_dir.path(), dest_ready_marker) {
            ready = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let read_captured_output = |status: std::process::ExitStatus| -> std::process::Output {
        std::process::Output {
            status,
            stdout: std::fs::read(stdout_file.path()).unwrap_or_default(),
            stderr: std::fs::read(stderr_file.path()).unwrap_or_default(),
        }
    };
    if !ready {
        // this is a FAILURE, not a skip. The marker is logged unconditionally on the way to the
        // `recv_object` call this test exercises, so missing it means the destination never got
        // there - a startup regression, a hang, or a copy that finished before the kill could land.
        // Every one of those is a real problem, and returning success here would report all of them
        // as a passing regression test.
        let status = child.wait().expect("Failed to wait for rcp");
        print_command_output(&read_captured_output(status));
        panic!(
            "destination rcpd did not log {dest_ready_marker:?} within 20s of {:?}, so the \
             scenario under test was never reached (startup regression, hang, or a copy that \
             finished too quickly)",
            spawn_start.elapsed()
        );
    }
    // second barrier, for the SOURCE-side half of this test (the close-attribution error asserted
    // at the end). It is the same FIN-vs-RST argument as above, applied to the OTHER control
    // connection: the destination also holds a control connection to the source rcpd, and the source
    // sends exactly one message on it for a single-file copy - `DiscoveryComplete`. Kill the
    // destination with that message still queued and its kernel answers with an RST, which the source
    // reads as `RecvResult::Error` and reports as a transport failure - a different branch, so the
    // clean-EOF error would only be observed by luck. The destination logs this marker immediately after
    // consuming that message (rcp/src/destination.rs), so seeing it means the queue is drained and
    // SIGKILL yields a clean FIN, which is the dispatcher's missing-DestinationDone branch under test.
    //
    // Waiting for it does not weaken the master-side barrier above: the master sends nothing on its
    // own control connection to the destination after `MasterHello::Destination`, so that queue stays
    // drained no matter how much later the kill lands. And the copy cannot finish first: the source
    // reserves the whole file's iops budget before it opens the file at all (rcp/src/source.rs), so
    // it is ~10s from sending even the header when this marker appears.
    let structure_marker = "Received DiscoveryComplete";
    // its own origin, not `spawn_start`: the barrier above may already have consumed most of that
    // budget, and a timeout measured from it would blame this marker for the first barrier's delay
    let structure_wait_start = std::time::Instant::now();
    let mut structure_seen = false;
    while structure_wait_start.elapsed().as_secs() < 20 {
        if rcpd_logs_contain(rcpd_log_dir.path(), structure_marker) {
            structure_seen = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    if !structure_seen {
        let status = child.wait().expect("Failed to wait for rcp");
        print_command_output(&read_captured_output(status));
        panic!(
            "destination rcpd did not log {structure_marker:?} within {:?} of consuming the master's \
             hello, so the source's control stream to it was never drained and the kill would land \
             on the RST branch instead of the clean-EOF branch this test asserts",
            structure_wait_start.elapsed()
        );
    }
    // scope the kill to the destination rcpd THIS test spawned: match on our per-run marker and on
    // the role, rather than `pkill -f 'rcpd --role destination'`, which would take down every
    // matching process on the host - a developer's live remote copy, or another test's daemon on a
    // shared CI runner. nextest's serial group orders tests within one run; it says nothing about
    // what else is running on the machine.
    let destination_processes = find_marked_rcpd_processes(&rcpd_marker, Some("destination"));
    eprintln!(
        "Destination rcpd consumed the master's hello after {:?} (control queue drained), killing \
         it {destination_processes:?}",
        spawn_start.elapsed()
    );
    assert!(
        !destination_processes.is_empty(),
        "found no destination rcpd of ours to kill (it must have already exited - copy finished \
         too quickly after the control connection came up)"
    );
    for process in &destination_processes {
        process.signal(libc::SIGKILL).unwrap_or_else(|error| {
            panic!(
                "failed to SIGKILL destination rcpd {}: {error}",
                process.pid()
            )
        });
    }
    let status = child.wait().expect("Failed to wait for rcp master");
    eprintln!(
        "rcp master exited {status:?} after kill (total elapsed since spawn: {:?})",
        spawn_start.elapsed()
    );
    let output = read_captured_output(status);
    print_command_output(&output);
    assert_not_timeout(&output);
    // errors print via `println!("{err:?}")` (common/src/lib.rs), i.e. to stdout, not stderr -
    // check both combined so this isn't sensitive to exactly which stream carries which text.
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "expected the master to report a clean error (exit 1), not abort (134) or hang; \
         got code {:?}",
        output.status.code()
    );
    assert!(
        !combined.contains("panicked"),
        "master should not panic; output:\n{combined}"
    );
    // Assert the CLEAN-EOF branch specifically, not merely "some error was reported". `Ok(None)` at
    // the master's `recv_object` is the branch the original `.expect()` panicked on; the RST branch
    // (`Err`) was always handled correctly and would make this test vacuous for the regression it
    // exists to catch. The two are distinguishable because the master words them differently -
    // `rcpd_closed_quietly` vs `rcpd_went_quiet` in rcp.rs - so this assertion fails rather than
    // passes if the kill starts landing on the RST path.
    //
    // Reaching clean EOF deterministically needs the destination's control-socket receive queue to be
    // empty when it dies, which is what the marker barrier above establishes: the destination has
    // CONSUMED the MasterHello, and the master sends nothing further on that connection before
    // awaiting the result. SIGKILL with an empty queue yields FIN, not RST.
    assert!(
        combined.contains("destination rcpd on")
            && combined
                .contains("closed its control connection cleanly but did not report a result"),
        "expected the clean-EOF diagnostic naming the dead destination rcpd (not the RST-path \
         wording, which exercises an already-correct branch); got:\n{combined}"
    );
    eprintln!("✓ master reported a clean error (exit 1) instead of aborting");
    // source EOF before DestinationDone is now fatal. Check its own role-marked log so the
    // master's independent clean-EOF diagnostic cannot satisfy the source-attribution assertion.
    let source_log = std::fs::read_dir(rcpd_log_dir.path())
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .find(|log| log.contains("Received side: Source"))
        .expect("source rcpd must have logged its consumed master hello");
    assert!(
        source_log
            .contains("rcpd operation failed: destination closed control before DestinationDone"),
        "expected the source rcpd to fail because the destination closed before DestinationDone; \
         source log:\n{source_log}"
    );
    eprintln!("✓ source rcpd attributed the control-stream close to the destination going away");
}

/// A writer that fills the destination slot AFTER the remote destination classified it as vacant is
/// resolved by `--overwrite`, not failed. The destination creates with `O_CREAT|O_EXCL`, so `EEXIST` is
/// the only way it learns of the new entry; before it recovered from that, such a file failed with
/// "File exists" no matter what `--overwrite` / `--ignore-existing` asked for.
///
/// The race is made DETERMINISTIC by `--iops-throttle`, which gates the data path: each side reserves
/// `((size - 1) / chunk_size) + 1` tokens per file, and the destination reserves them AFTER it has
/// classified the slot — logging that it is about to. 20 MiB at 1 MiB chunks is 20 tokens, so at 4
/// tokens/sec the destination sits in that reservation for ~5s, which is the window this test writes
/// into. The source reserves its own budget before it even sends the header, which is why the barrier
/// keys off the DESTINATION's marker rather than a fixed delay.
///
/// Both markers are asserted. Without the first, the write would race classification; without the
/// second, a write that landed too early would take the ordinary overwrite path and the test would
/// pass while proving nothing.
#[test]
fn test_remote_overwrite_recovers_when_destination_appears_after_classification() {
    require_local_ssh();
    const IOPS_THROTTLE: usize = 4;
    const FILE_SIZE_MIB: usize = 20;
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("appears.dat");
    create_large_test_file(&src_file, FILE_SIZE_MIB);
    let dst_file = dst_dir.path().join("appears.dat");
    // the destination must be ABSENT when the copy classifies it: that is what makes classification
    // decide "vacant", leaving the create as the only thing that can discover the conflict.
    assert!(
        !dst_file.exists(),
        "destination must start absent for the create to be what finds the conflict"
    );
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    // per-run rcpd debug log, as in test_remote_destination_rcpd_killed_reports_error_not_abort: the
    // markers this test synchronizes on and asserts are logged by rcpd, not by the master.
    let rcpd_log_dir = tempfile::TempDir::new().expect("Failed to create rcpd debug log dir");
    let rcpd_marker = rcpd_log_dir.path().join("rcpd-debug").display().to_string();
    let rcpd_log_arg = format!("--rcpd-debug-log-prefix={rcpd_marker}");
    let stdout_file = tempfile::NamedTempFile::new().expect("Failed to create stdout capture file");
    let stderr_file = tempfile::NamedTempFile::new().expect("Failed to create stderr capture file");
    let iops_arg = format!("--iops-throttle={IOPS_THROTTLE}");
    let mut cmd = std::process::Command::new("timeout");
    cmd.args(["60", rcp_path.to_str().unwrap()]);
    cmd.args([
        "-vv",
        "--force-remote",
        "--no-encryption",
        "--overwrite",
        "--chunk-size=1MiB",
        &iops_arg,
        &rcpd_log_arg,
        &src_remote,
        &dst_remote,
    ]);
    cmd.stdout(
        stdout_file
            .reopen()
            .expect("Failed to reopen stdout capture file"),
    );
    cmd.stderr(
        stderr_file
            .reopen()
            .expect("Failed to reopen stderr capture file"),
    );
    let spawn_start = std::time::Instant::now();
    let mut child = cmd.spawn().expect("Failed to spawn rcp");
    // logged by rcp::destination::process_single_file between classifying the slot and reserving its
    // I/O budget, so seeing it means classification is done and the destination is now parked.
    let classified_marker = "destination slot classified, reserving iops budget";
    let mut classified = false;
    while spawn_start.elapsed().as_secs() < 40 {
        if rcpd_logs_contain(rcpd_log_dir.path(), classified_marker) {
            classified = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let read_captured_output = |status: std::process::ExitStatus| -> std::process::Output {
        std::process::Output {
            status,
            stdout: std::fs::read(stdout_file.path()).unwrap_or_default(),
            stderr: std::fs::read(stderr_file.path()).unwrap_or_default(),
        }
    };
    if !classified {
        let status = child.wait().expect("Failed to wait for rcp");
        print_command_output(&read_captured_output(status));
        panic!(
            "destination rcpd did not log {classified_marker:?} within {:?}, so the window this test \
             writes into was never reached",
            spawn_start.elapsed()
        );
    }
    // fill the slot the destination just classified as vacant, while it waits on its I/O budget
    std::fs::write(&dst_file, b"planted after classification")
        .expect("Failed to plant destination");
    let status = child.wait().expect("Failed to wait for rcp master");
    let output = read_captured_output(status);
    print_command_output(&output);
    assert_not_timeout(&output);
    assert!(
        output.status.success(),
        "copy should recover from the conflict and succeed, not fail the file on EEXIST"
    );
    // the destination must hold the SOURCE's content, not the planted file
    let copied = std::fs::metadata(&dst_file).expect("destination should exist");
    assert_eq!(
        copied.len(),
        (FILE_SIZE_MIB * 1024 * 1024) as u64,
        "destination should hold the full source, not the planted file"
    );
    // prove the EEXIST branch is what ran. Without this, a write that landed before classification
    // would take the ordinary overwrite path and this test would pass without exercising the recovery
    // it exists for.
    assert!(
        rcpd_logs_contain(
            rcpd_log_dir.path(),
            "destination appeared after classification"
        ),
        "expected the destination to report recovering from a post-classification EEXIST; without it \
         the plant raced classification and the ordinary overwrite path ran instead"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn test_remote_rcpd_no_zombie_processes() {
    require_local_ssh();
    const IOPS_THROTTLE: usize = 5;
    const FILE_SIZE_MIB: usize = 10;
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("throttled-file.dat");
    create_large_test_file(&src_file, FILE_SIZE_MIB);
    let dst_file = dst_dir.path().join("copied-file.dat");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    let rcpd_log_dir = tempfile::TempDir::new().expect("Failed to create rcpd debug log dir");
    let rcpd_marker = rcpd_log_dir.path().join("rcpd-debug").display().to_string();
    let rcpd_log_arg = format!("--rcpd-debug-log-prefix={rcpd_marker}");
    let iops_arg = format!("--iops-throttle={IOPS_THROTTLE}");
    let stdout_file = tempfile::NamedTempFile::new().expect("Failed to create stdout capture file");
    let stderr_file = tempfile::NamedTempFile::new().expect("Failed to create stderr capture file");
    let mut command = std::process::Command::new(rcp_path);
    command.args([
        "-vv",
        "--force-remote",
        "--chunk-size=1MiB",
        &iops_arg,
        &rcpd_log_arg,
        &src_remote,
        &dst_remote,
    ]);
    command.stdout(
        stdout_file
            .reopen()
            .expect("Failed to reopen stdout capture file"),
    );
    command.stderr(
        stderr_file
            .reopen()
            .expect("Failed to reopen stderr capture file"),
    );
    let read_captured_output = |status: std::process::ExitStatus| -> std::process::Output {
        std::process::Output {
            status,
            stdout: std::fs::read(stdout_file.path()).unwrap_or_default(),
            stderr: std::fs::read(stderr_file.path()).unwrap_or_default(),
        }
    };
    let scenario_start = std::time::Instant::now();
    let master = command.spawn().expect("Failed to spawn rcp master");
    let mut run = MarkedRemoteRun::new(master, rcpd_marker.clone());
    let daemon_processes = loop {
        let source_processes = find_marked_rcpd_processes(&rcpd_marker, Some("source"));
        let destination_processes = find_marked_rcpd_processes(&rcpd_marker, Some("destination"));
        let marked_processes = source_processes
            .iter()
            .chain(&destination_processes)
            .cloned()
            .collect::<Vec<_>>();
        let role_hellos_received = rcpd_role_hellos_received(rcpd_log_dir.path());
        if !source_processes.is_empty() && !destination_processes.is_empty() && role_hellos_received
        {
            break marked_processes;
        }
        if let Some(status) = run.try_wait().expect("Failed to poll rcp master") {
            print_command_output(&read_captured_output(status));
            panic!(
                "the daemon-reaping scenario was never reached: rcp exited before both marked \
                 rcpd roles were running and had consumed their master hellos"
            );
        }
        if scenario_start.elapsed() >= std::time::Duration::from_secs(20) {
            let status = run
                .kill_master()
                .expect("Failed to stop rcp master after scenario timeout");
            print_command_output(&read_captured_output(status));
            panic!(
                "the daemon-reaping scenario was never reached within {:?}: found source {:?}, \
                 destination {:?}, both master hellos received: {role_hellos_received}",
                scenario_start.elapsed(),
                source_processes,
                destination_processes
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    let status = loop {
        if let Some(status) = run.try_wait().expect("Failed to poll rcp master") {
            break status;
        }
        if scenario_start.elapsed() >= std::time::Duration::from_secs(90) {
            let status = run
                .kill_master()
                .expect("Failed to stop timed-out rcp master");
            print_command_output(&read_captured_output(status));
            panic!("rcp did not complete within 90 seconds");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    let output = read_captured_output(status);
    print_command_output(&output);
    assert_not_timeout(&output);
    assert!(output.status.success(), "Copy should succeed");
    assert_eq!(
        std::fs::metadata(&dst_file)
            .expect("destination should exist")
            .len(),
        (FILE_SIZE_MIB * 1024 * 1024) as u64,
        "destination should hold the complete source"
    );

    let cleanup_start = std::time::Instant::now();
    loop {
        let remaining = daemon_processes
            .iter()
            .filter_map(|process| process.state().map(|state| (process.identity, state)))
            .collect::<Vec<_>>();
        if remaining.is_empty() {
            break;
        }
        if cleanup_start.elapsed() >= std::time::Duration::from_secs(5) {
            panic!(
                "rcpd processes owned by this test did not disappear within {:?}: {remaining:?}",
                cleanup_start.elapsed()
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    eprintln!(
        "All owned rcpd processes were reaped in {:?}",
        cleanup_start.elapsed()
    );
}

#[test]
fn test_remote_rcpd_with_custom_tcp_timeouts() {
    // verify that custom TCP timeout values are accepted and work correctly
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    create_test_file(&src_file, "test content", 0o644);
    let dst_file = dst_dir.path().join("test.txt");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // test with custom timeout values
    let output = run_rcp_with_args(&[
        "--remote-copy-conn-timeout-sec=30",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(
        output.status.success(),
        "Copy with custom TCP timeouts should succeed"
    );
    assert!(dst_file.exists(), "Destination file should exist");
    let content = get_file_content(&dst_file);
    assert_eq!(content, "test content");
    eprintln!("✓ Copy with custom TCP timeouts succeeded");
}

#[test]
fn test_remote_rcpd_aggressive_timeout_configuration() {
    // verify that moderately aggressive timeout values work correctly (for datacenter environments)
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    create_test_file(&src_file, "test content", 0o644);
    let dst_file = dst_dir.path().join("test.txt");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // test with moderately aggressive timeouts suitable for fast datacenter environments
    let output = run_rcp_with_args(&[
        "--remote-copy-conn-timeout-sec=10",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(
        output.status.success(),
        "Copy with aggressive timeouts should succeed"
    );
    assert!(dst_file.exists(), "Destination file should exist");
    eprintln!("✓ Copy with aggressive timeouts succeeded");
}

#[test]
fn test_remote_auto_deploy_rcpd() {
    // test automatic deployment of rcpd binary to remote host
    // NOTE: This test temporarily moves rcpd binary to force deployment
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("auto_deploy_test.txt");
    let dst_file = dst_dir.path().join("auto_deploy_test.txt");
    create_test_file(&src_file, "testing auto-deployment", 0o644);

    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // get current version to check for deployed binary
    let version_output = std::process::Command::new(assert_cmd::cargo::cargo_bin("rcp"))
        .arg("--protocol-version")
        .output()
        .expect("Failed to get version");
    let version_json: serde_json::Value =
        serde_json::from_slice(&version_output.stdout).expect("Failed to parse version JSON");
    let semantic_version = version_json["semantic"]
        .as_str()
        .expect("Missing semantic version");
    // the deployed filename carries the wire revision too (see ProtocolVersion::cache_tag)
    let deployed_tag = semantic_version.replace('+', "-");
    // clean up any previously deployed rcpd for this version to force deployment
    let cache_dir = cache_bin_dir(home.path());
    let deployed_rcpd = cache_dir.join(format!("rcpd-{}", deployed_tag));
    if deployed_rcpd.exists() {
        eprintln!(
            "Removing existing deployed rcpd to force re-deployment: {}",
            deployed_rcpd.display()
        );
        std::fs::remove_file(&deployed_rcpd).ok();
    }
    // use --rcpd-path=/nonexistent to force discovery failure and trigger auto-deployment.
    // this allows deployment to find the correct local rcpd binary (same build as rcp) to transfer.
    // we can't reliably hide all rcpd binaries (e.g., nix profile is owned by root).
    eprintln!(
        "Testing auto-deployment with version {} (using --rcpd-path=/nonexistent/rcpd)",
        semantic_version
    );
    let output = run_rcp_with_args_home_and_env(
        &[
            "--auto-deploy-rcpd",
            "--rcpd-path=/nonexistent/rcpd",
            &src_remote,
            &dst_remote,
        ],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output);
    // verify the copy succeeded
    assert!(
        output.status.success(),
        "Copy with auto-deploy should succeed"
    );
    assert!(dst_file.exists(), "Destination file should exist");
    assert_eq!(get_file_content(&dst_file), "testing auto-deployment");
    // verify that rcpd was deployed to cache
    assert!(
        deployed_rcpd.exists(),
        "rcpd should be deployed to {}",
        deployed_rcpd.display()
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        stdout.matches("Preparing rcpd server on:").count(),
        1,
        "same-host source and destination must share one preparation"
    );
    assert_eq!(
        stdout.matches("Successfully deployed rcpd to").count(),
        1,
        "same-host auto-deployment must publish the daemon once"
    );
    assert_eq!(
        stdout.matches("Starting prepared rcpd server on:").count(),
        2,
        "the shared preparation must still spawn both daemon roles"
    );
    // verify it's executable
    let metadata = std::fs::metadata(&deployed_rcpd).expect("Failed to get deployed rcpd metadata");
    let permissions = metadata.permissions();
    assert!(
        permissions.mode() & 0o100 != 0,
        "deployed rcpd should be executable"
    );
    // publication is a rename of the deployment's own temp file, so a successful deployment
    // consumes it. Note what this does NOT prove: the implementation this replaced also left no
    // temp file behind on success (its `mv` renamed the shared `.tmp.$$` away just the same), so
    // this cannot tell the two apart. It guards the *leak* direction — a deployment that stages a
    // file and then returns without publishing or removing it. The unique-name and
    // verify-before-publish guarantees are red-greened by the unit tests in remote/src/deploy.rs.
    let leftover_temps: Vec<_> = std::fs::read_dir(&cache_dir)
        .expect("cache dir should exist after deployment")
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".tmp."))
        .collect();
    assert!(
        leftover_temps.is_empty(),
        "a successful deployment must leave no temp file behind, found: {leftover_temps:?}"
    );

    eprintln!("✓ Auto-deployment test succeeded");
    eprintln!("✓ Deployed binary at: {}", deployed_rcpd.display());
}

#[test]
fn test_remote_auto_deploy_reuses_cached_binary() {
    // test that auto-deployment reuses already-deployed binary
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("cached_deploy_test.txt");
    let dst_file = dst_dir.path().join("cached_deploy_test.txt");
    create_test_file(&src_file, "testing cached deployment", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // first run with --auto-deploy-rcpd to ensure binary is deployed
    // use --rcpd-path=/nonexistent to force deployment (discovery will fail)
    eprintln!("First run: ensuring rcpd is deployed");
    let output1 = run_rcp_with_args_home_and_env(
        &[
            "--auto-deploy-rcpd",
            "--rcpd-path=/nonexistent/rcpd",
            &src_remote,
            &dst_remote,
        ],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output1);
    assert!(
        output1.status.success(),
        "First copy with auto-deploy should succeed"
    );
    // get modification time of deployed binary
    let version_output = std::process::Command::new(assert_cmd::cargo::cargo_bin("rcp"))
        .arg("--protocol-version")
        .output()
        .expect("Failed to get version");
    let version_json: serde_json::Value =
        serde_json::from_slice(&version_output.stdout).expect("Failed to parse version JSON");
    let semantic_version = version_json["semantic"]
        .as_str()
        .expect("Missing semantic version");
    let deployed_tag = semantic_version.replace('+', "-");
    let cache_dir = cache_bin_dir(home.path());
    let deployed_rcpd = cache_dir.join(format!("rcpd-{}", deployed_tag));
    let first_mtime = std::fs::metadata(&deployed_rcpd)
        .expect("deployed rcpd should exist")
        .modified()
        .expect("should have modified time");
    // second run should reuse the deployed binary (no re-deployment needed)
    // to ensure we're testing caching, use a different file
    let src_file2 = src_dir.path().join("cached_deploy_test2.txt");
    let dst_file2 = dst_dir.path().join("cached_deploy_test2.txt");
    create_test_file(&src_file2, "second test", 0o644);
    let src_remote2 = format!("localhost:{}", src_file2.to_str().unwrap());
    let dst_remote2 = format!("localhost:{}", dst_file2.to_str().unwrap());
    eprintln!("Second run: should reuse deployed binary");
    let output2 = run_rcp_with_args_home_and_env(
        &["--auto-deploy-rcpd", &src_remote2, &dst_remote2],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output2);
    assert!(output2.status.success(), "Second copy should also succeed");
    // verify mtime hasn't changed (binary wasn't re-deployed)
    let second_mtime = std::fs::metadata(&deployed_rcpd)
        .expect("deployed rcpd should still exist")
        .modified()
        .expect("should have modified time");
    assert_eq!(
        first_mtime, second_mtime,
        "deployed binary should not be re-deployed (mtime should match)"
    );
    eprintln!("✓ Cached deployment test succeeded");
    eprintln!("✓ Binary was reused, not re-deployed");
}

#[test]
fn test_remote_auto_deploy_cleanup_old_versions() {
    // test that auto-deployment cleans up old versions (keeps last 3)
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("cleanup_test.txt");
    let dst_file = dst_dir.path().join("cleanup_test.txt");
    create_test_file(&src_file, "test cleanup", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // create fake old version binaries in the cache directory
    let cache_dir = cache_bin_dir(home.path());
    std::fs::create_dir_all(&cache_dir).expect("Failed to create cache directory");
    // dynamically generate old versions based on the current version
    // this avoids having to update the test every time we bump the version
    let current_version = env!("CARGO_PKG_VERSION");
    // strip prerelease suffix (e.g., "0.23.0-alpha.1" -> "0.23.0")
    let base_version = current_version.split('-').next().unwrap();
    let version_parts: Vec<u32> = base_version
        .split('.')
        .map(|s| s.parse().expect("valid version number"))
        .collect();
    let (major, minor, patch) = (version_parts[0], version_parts[1], version_parts[2]);
    // create 4 old versions by decrementing the minor version
    // combined with the current version, that's 5 total; cleanup keeps 3, deletes 2 oldest
    assert!(
        minor >= 4,
        "Test requires minor version >= 4 to generate enough old versions for cleanup testing, \
         current version is {current_version}"
    );
    let old_versions: Vec<String> = (1..=4)
        .map(|i| format!("{}.{}.{}", major, minor - i, patch))
        .rev()
        .collect();
    for (idx, version) in old_versions.iter().enumerate() {
        let fake_binary = cache_dir.join(format!("rcpd-{}", version));
        std::fs::write(&fake_binary, "fake old binary").expect("Failed to create fake binary");
        // set mtime to make them old (10 seconds apart, oldest first)
        let mtime = std::time::SystemTime::now()
            - std::time::Duration::from_secs((old_versions.len() - idx) as u64 * 10);
        filetime::set_file_mtime(&fake_binary, filetime::FileTime::from_system_time(mtime))
            .expect("Failed to set mtime");
    }
    // run auto-deployment which should deploy current version and clean up old ones
    let output = run_rcp_with_args_home_and_env(
        &[
            "--auto-deploy-rcpd",
            "--rcpd-path=/nonexistent/rcpd",
            &src_remote,
            &dst_remote,
        ],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output);
    assert!(
        output.status.success(),
        "Copy with auto-deploy should succeed"
    );
    // verify copy succeeded
    assert!(dst_file.exists(), "Destination file should exist");
    // verify cleanup: should keep only the 3 newest versions (current + 2 older)
    // with 5 total versions (4 old fake + 1 current), the 2 oldest should be deleted
    // old_versions are in ascending order: [minor-4, minor-3, minor-2, minor-1]
    // indices:                              [0,       1,       2,       3      ]
    // should delete:                        [oldest,  old                      ]
    // should keep:                          [                 newer,   newest  ] + current
    let oldest_version = cache_dir.join(format!("rcpd-{}", &old_versions[0]));
    let old_version = cache_dir.join(format!("rcpd-{}", &old_versions[1]));
    let newer_version = cache_dir.join(format!("rcpd-{}", &old_versions[2]));
    let newest_old_version = cache_dir.join(format!("rcpd-{}", &old_versions[3]));
    // the deployed filename carries the wire revision (ProtocolVersion::cache_tag)
    let current_version_path = cache_dir.join(format!(
        "rcpd-{}-w{}",
        current_version,
        common::version::WIRE_REVISION
    ));
    // check which versions remain
    let oldest_exists = oldest_version.exists();
    let old_exists = old_version.exists();
    let newer_exists = newer_version.exists();
    let newest_old_exists = newest_old_version.exists();
    let current_exists = current_version_path.exists();
    eprintln!("After cleanup:");
    eprintln!("  {} exists: {}", &old_versions[0], oldest_exists);
    eprintln!("  {} exists: {}", &old_versions[1], old_exists);
    eprintln!("  {} exists: {}", &old_versions[2], newer_exists);
    eprintln!("  {} exists: {}", &old_versions[3], newest_old_exists);
    eprintln!("  {} exists: {}", current_version, current_exists);
    // verify cleanup worked: oldest 2 should be deleted, newest 3 kept
    assert!(
        !oldest_exists,
        "Oldest version {} should be deleted",
        &old_versions[0]
    );
    assert!(
        !old_exists,
        "Old version {} should be deleted",
        &old_versions[1]
    );
    assert!(
        newer_exists,
        "Version {} should be kept (one of newest 3)",
        &old_versions[2]
    );
    assert!(
        newest_old_exists,
        "Version {} should be kept (one of newest 3)",
        &old_versions[3]
    );
    assert!(
        current_exists,
        "Current version {} should be kept",
        current_version
    );
    // cleanup our fake binaries that were kept
    for version in &old_versions[2..] {
        std::fs::remove_file(cache_dir.join(format!("rcpd-{}", version))).ok();
    }
    eprintln!("✓ Cleanup of old versions works correctly");
}

#[test]
fn test_remote_auto_deploy_on_version_mismatch() {
    // test that auto-deployment triggers when --rcpd-path points to an rcpd
    // binary that exists and is executable but reports an incompatible version
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("version_mismatch_test.txt");
    let dst_file = dst_dir.path().join("version_mismatch_test.txt");
    create_test_file(&src_file, "testing version mismatch auto-deploy", 0o644);

    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());

    // create a fake rcpd that reports an incompatible version
    let fake_rcpd_dir = tempfile::tempdir().unwrap();
    let fake_rcpd_path = fake_rcpd_dir.path().join("rcpd");
    std::fs::write(
        &fake_rcpd_path,
        "#!/bin/sh\necho '{\"semantic\":\"0.0.0-fake\"}'\n",
    )
    .expect("Failed to create fake rcpd");
    std::fs::set_permissions(&fake_rcpd_path, std::fs::Permissions::from_mode(0o755))
        .expect("Failed to set permissions on fake rcpd");

    // get current version to verify deployment
    let version_output = std::process::Command::new(assert_cmd::cargo::cargo_bin("rcp"))
        .arg("--protocol-version")
        .output()
        .expect("Failed to get version");
    assert!(
        version_output.status.success(),
        "rcp --protocol-version failed with status {:?}: {}",
        version_output.status,
        String::from_utf8_lossy(&version_output.stderr)
    );
    let version_json: serde_json::Value =
        serde_json::from_slice(&version_output.stdout).expect("Failed to parse version JSON");
    let semantic_version = version_json["semantic"]
        .as_str()
        .expect("Missing semantic version");

    // clean up any previously deployed rcpd for this version to force deployment
    let cache_dir = cache_bin_dir(home.path());
    let deployed_tag = semantic_version.replace('+', "-");
    let deployed_rcpd = cache_dir.join(format!("rcpd-{}", deployed_tag));
    if deployed_rcpd.exists() {
        match std::fs::remove_file(&deployed_rcpd) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!(
                "Failed to remove previously deployed rcpd at {}: {}",
                deployed_rcpd.display(),
                e
            ),
        }
    }

    let rcpd_path_arg = format!("--rcpd-path={}", fake_rcpd_path.to_str().unwrap());
    eprintln!(
        "Testing auto-deployment on version mismatch (fake rcpd reports 0.0.0-fake, expected {})",
        semantic_version
    );

    let output = run_rcp_with_args_home_and_env(
        &[
            "--auto-deploy-rcpd",
            &rcpd_path_arg,
            &src_remote,
            &dst_remote,
        ],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output);

    // verify the copy succeeded despite version mismatch
    assert!(
        output.status.success(),
        "Copy with auto-deploy should succeed when rcpd has version mismatch"
    );
    assert!(dst_file.exists(), "Destination file should exist");
    assert_eq!(
        get_file_content(&dst_file),
        "testing version mismatch auto-deploy"
    );

    // verify that the correct rcpd was deployed to cache
    assert!(
        deployed_rcpd.exists(),
        "rcpd should be deployed to {}",
        deployed_rcpd.display()
    );

    // verify it's executable
    let metadata = std::fs::metadata(&deployed_rcpd).expect("Failed to get deployed rcpd metadata");
    assert!(
        metadata.permissions().mode() & 0o100 != 0,
        "deployed rcpd should be executable"
    );

    eprintln!("✓ Auto-deployment on version mismatch test succeeded");
}

#[test]
fn test_remote_auto_deploy_skips_mismatched_local_candidate() {
    require_local_ssh();
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("local_candidate_mismatch.txt");
    let dst_file = dst_dir.path().join("local_candidate_mismatch.txt");
    create_test_file(&src_file, "compatible fallback deployed", 0o644);
    let src_remote = format!("localhost:{}", src_file.display());
    let dst_remote = format!("localhost:{}", dst_file.display());

    // running a copy of rcp gives the test sole ownership of its same-directory candidate
    let master_dir = tempfile::tempdir().unwrap();
    let copied_rcp = master_dir.path().join("rcp");
    std::fs::copy(assert_cmd::cargo::cargo_bin("rcp"), &copied_rcp)
        .expect("failed to copy rcp test binary");
    std::fs::set_permissions(&copied_rcp, std::fs::Permissions::from_mode(0o755))
        .expect("failed to make copied rcp executable");
    let stale_rcpd = master_dir.path().join("rcpd");
    std::fs::write(
        &stale_rcpd,
        "#!/bin/sh\n\
         if [ \"$1\" = \"--protocol-version\" ]; then\n\
           echo '{\"semantic\":\"0.0.0+w0\"}'\n\
           exit 0\n\
         fi\n\
         echo 'stale local candidate was spawned' >&2\n\
         exit 42\n",
    )
    .expect("failed to create stale local rcpd candidate");
    std::fs::set_permissions(&stale_rcpd, std::fs::Permissions::from_mode(0o755))
        .expect("failed to make stale local rcpd candidate executable");

    // the real test rcpd is a later PATH candidate and must remain available as the fallback
    let compatible_rcpd = assert_cmd::cargo::cargo_bin("rcpd");
    let compatible_dir = compatible_rcpd.parent().expect("rcpd must have a parent");
    let ambient_path = std::env::var_os("PATH").unwrap_or_default();
    let path = std::env::join_paths(
        std::iter::once(compatible_dir.to_path_buf()).chain(std::env::split_paths(&ambient_path)),
    )
    .expect("failed to build fallback PATH");
    let output = std::process::Command::new("timeout")
        .args(["90", copied_rcp.to_str().unwrap()])
        .args(["-vv", "--force-remote", "--auto-deploy-rcpd"])
        .arg("--rcpd-path=/nonexistent/rcpd")
        .args([&src_remote, &dst_remote])
        .env("HOME", home.path())
        .env("PATH", path)
        .env("RCP_REMOTE_HOME_OVERRIDE", &override_home)
        .output()
        .expect("failed to execute copied rcp command");
    assert_not_timeout(&output);
    print_command_output(&output);
    assert!(
        output.status.success(),
        "auto-deploy must skip the stale same-directory candidate and deploy the compatible PATH candidate"
    );
    assert_eq!(get_file_content(&dst_file), "compatible fallback deployed");
}

#[test]
fn test_remote_auto_deploy_skips_hanging_local_candidate() {
    require_local_ssh();
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("local_candidate_hang.txt");
    let dst_file = dst_dir.path().join("local_candidate_hang.txt");
    create_test_file(&src_file, "compatible fallback deployed", 0o644);
    let src_remote = format!("localhost:{}", src_file.display());
    let dst_remote = format!("localhost:{}", dst_file.display());

    // running a copy of rcp gives the test sole ownership of its same-directory candidate
    let master_dir = tempfile::tempdir().unwrap();
    let copied_rcp = master_dir.path().join("rcp");
    std::fs::copy(assert_cmd::cargo::cargo_bin("rcp"), &copied_rcp)
        .expect("failed to copy rcp test binary");
    std::fs::set_permissions(&copied_rcp, std::fs::Permissions::from_mode(0o755))
        .expect("failed to make copied rcp executable");
    let hanging_rcpd = master_dir.path().join("rcpd");
    std::fs::write(
        &hanging_rcpd,
        "#!/bin/sh\n\
         if [ \"$1\" = \"--protocol-version\" ]; then\n\
           exec sleep 60\n\
         fi\n\
         echo 'hanging local candidate was spawned' >&2\n\
         exit 42\n",
    )
    .expect("failed to create hanging local rcpd candidate");
    std::fs::set_permissions(&hanging_rcpd, std::fs::Permissions::from_mode(0o755))
        .expect("failed to make hanging local rcpd candidate executable");

    // the real test rcpd is a later PATH candidate and must remain available as the fallback
    let compatible_rcpd = assert_cmd::cargo::cargo_bin("rcpd");
    let compatible_dir = compatible_rcpd.parent().expect("rcpd must have a parent");
    let ambient_path = std::env::var_os("PATH").unwrap_or_default();
    let path = std::env::join_paths(
        std::iter::once(compatible_dir.to_path_buf()).chain(std::env::split_paths(&ambient_path)),
    )
    .expect("failed to build fallback PATH");
    let output = std::process::Command::new("timeout")
        .args(["90", copied_rcp.to_str().unwrap()])
        .args(["--force-remote", "--auto-deploy-rcpd"])
        .arg("--rcpd-path=/nonexistent/rcpd")
        .args([&src_remote, &dst_remote])
        .env("HOME", home.path())
        .env("PATH", path)
        .env("RCP_REMOTE_HOME_OVERRIDE", &override_home)
        .output()
        .expect("failed to execute copied rcp command");
    print_command_output(&output);
    assert_not_timeout(&output);
    assert!(
        output.status.success(),
        "auto-deploy must time out the hanging same-directory candidate and deploy the compatible PATH candidate"
    );
    assert_eq!(get_file_content(&dst_file), "compatible fallback deployed");
}

#[test]
fn test_remote_auto_deploy_uses_configured_timeout_for_hanging_remote_version_probe() {
    require_local_ssh();
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("remote_probe_hang.txt");
    let dst_file = dst_dir.path().join("remote_probe_hang.txt");
    create_test_file(&src_file, "remote probe fallback deployed", 0o644);
    let src_remote = format!("localhost:{}", src_file.display());
    let dst_remote = format!("localhost:{}", dst_file.display());

    let candidate_dir = tempfile::tempdir().unwrap();
    let probe_marker = candidate_dir.path().join("probe-marker");
    let hanging_rcpd = candidate_dir.path().join("rcpd");
    std::fs::write(
        &hanging_rcpd,
        format!(
            "#!/bin/sh\n\
             if [ \"$1\" = \"--protocol-version\" ]; then\n\
               printf 'probe\\n' >> {}\n\
               exec sleep 60\n\
             fi\n\
             exit 42\n",
            probe_marker.display()
        ),
    )
    .expect("failed to create hanging remote rcpd candidate");
    std::fs::set_permissions(&hanging_rcpd, std::fs::Permissions::from_mode(0o755))
        .expect("failed to make hanging remote rcpd candidate executable");
    let explicit_path = format!("--rcpd-path={}", hanging_rcpd.display());

    let output = run_rcp_with_args_home_and_env(
        &[
            "--auto-deploy-rcpd",
            "--remote-copy-conn-timeout-sec=1",
            &explicit_path,
            &src_remote,
            &dst_remote,
        ],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output);
    assert!(
        output.status.success(),
        "auto-deploy must recover from a hanging remote version probe"
    );
    assert_eq!(
        get_file_content(&dst_file),
        "remote probe fallback deployed"
    );
    assert_eq!(
        std::fs::read_to_string(&probe_marker).unwrap(),
        "probe\n",
        "same-host preparation must attempt the hanging remote candidate exactly once"
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout)
            .matches("Successfully deployed rcpd to")
            .count(),
        1,
        "the timeout must fall back to one successful deployment"
    );
}

#[test]
fn test_remote_auto_deploy_error_explicit_rcpd_not_found() {
    // test error handling when explicit --rcpd-path points to nonexistent binary
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // use explicit path that doesn't exist - should fail with clear error
    let output = run_rcp_with_args(&[
        "--rcpd-path=/this/path/definitely/does/not/exist/rcpd",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    // should fail
    assert!(
        !output.status.success(),
        "should fail when explicit rcpd path not found"
    );
    // check error message quality
    let combined_output = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined_output.contains("rcpd binary not found")
            || combined_output.contains("not found or not executable"),
        "error message should mention rcpd not found"
    );
    assert!(
        combined_output.contains("/this/path/definitely/does/not/exist/rcpd"),
        "error message should include the explicit path that was tried"
    );
    eprintln!("✓ explicit rcpd-path not found error handling works correctly");
}

#[test]
fn test_remote_auto_deploy_error_permission_denied() {
    // test error handling when deployment fails due to permission denied
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // make ~/.cache/rcp/bin read-only to trigger permission denied
    let cache_bin_dir = cache_bin_dir(home.path());
    // create the directory if it doesn't exist
    std::fs::create_dir_all(&cache_bin_dir).expect("failed to create cache directory");
    // make it read-only (mode 555)
    std::fs::set_permissions(&cache_bin_dir, std::fs::Permissions::from_mode(0o555))
        .expect("failed to set permissions");
    // run auto-deployment which should fail due to permission denied
    let output = run_rcp_with_args_home_and_env(
        &[
            "--auto-deploy-rcpd",
            "--rcpd-path=/nonexistent/rcpd",
            &src_remote,
            &dst_remote,
        ],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    // restore write permissions before checking results
    std::fs::set_permissions(&cache_bin_dir, std::fs::Permissions::from_mode(0o755))
        .expect("failed to restore permissions");
    print_command_output(&output);
    // should fail
    assert!(
        !output.status.success(),
        "should fail when deployment directory is read-only"
    );
    // check error message mentions permission issue
    let combined_output = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined_output.contains("Permission denied")
            || combined_output.contains("permission")
            || combined_output.contains("failed to transfer binary")
            || combined_output.contains("Insufficient disk space")
            || combined_output.contains("Permission denied creating")
            || combined_output.contains("failed to deploy rcpd")
            || combined_output.contains("Broken pipe"),
        "error message should mention permission or deployment failure: {}",
        combined_output
    );
    eprintln!("✓ permission denied error handling works correctly");
}

#[test]
fn test_remote_auto_deploy_redeploys_and_verifies_after_cache_eviction() {
    // Renamed from `test_remote_auto_deploy_error_checksum_mismatch`, which is not what it does:
    // nothing here ever produces a mismatch. A mismatch cannot be staged through the CLI — the
    // transfer runs over SSH with no injection point — so this covers what it actually can: that
    // evicting the cached binary triggers a fresh deployment, that the deployment succeeds, and
    // that it reports having verified the checksum.
    //
    // The ORDER of that verification — checksum the staged temp file, publish only if it matches —
    // is pinned by `transfer_command_does_not_publish` and its neighbours in remote/src/deploy.rs,
    // which fail against a staging command that renames before verifying. That is where the
    // red-green for publish-before-verify lives; this test cannot distinguish the two orders.
    let home = make_test_home();
    let override_home = home.path().to_str().unwrap().to_string();
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // first, do a successful deployment
    let output = run_rcp_with_args_home_and_env(
        &[
            "--auto-deploy-rcpd",
            "--rcpd-path=/nonexistent/rcpd",
            &src_remote,
            &dst_remote,
        ],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    assert!(output.status.success(), "initial deployment should succeed");
    assert!(dst_file.exists(), "copy should succeed");
    let cache_dir = cache_bin_dir(home.path());
    // find the deployed rcpd binary (rcpd-0.22.0 or similar)
    let mut deployed_binary = None;
    if let Ok(entries) = std::fs::read_dir(&cache_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(filename) = path.file_name()
                && filename.to_string_lossy().starts_with("rcpd-")
            {
                deployed_binary = Some(path);
                break;
            }
        }
    }
    let deployed_binary = deployed_binary.expect("should find deployed rcpd binary in cache");
    eprintln!("found deployed binary: {}", deployed_binary.display());
    // clear the destination file so we try to copy again
    std::fs::remove_file(&dst_file).ok();
    // note: the current implementation verifies checksum during DEPLOYMENT, not when USING the cached binary.
    // so we need to trigger a re-deployment, not reuse. we can do this by deleting the cached binary and re-deploying.
    // we can't easily simulate checksum mismatch during transfer because the transfer happens over SSH with base64 encoding.
    // the checksum verification happens AFTER successful transfer.
    // to test actual checksum mismatch, we'd need to: (1) intercept the transfer (not possible in integration test),
    // (2) modify the checksum verification code to inject failures (not good), or (3) test at unit level in deploy.rs (better approach).
    // for now, let's just verify that the deployment succeeds and includes checksum verification in the output stderr
    // unlink -- never truncate -- the cached binary: an rcpd from the previous run may still be
    // exiting with it open for execution, which would make an in-place write fail with ETXTBSY.
    std::fs::remove_file(&deployed_binary).expect("failed to remove cached binary");
    // re-deploy (should succeed with checksum verification). use the same temp home as the
    // initial deployment -- otherwise this would deploy a debug rcpd into the developer's real
    // ~/.cache/rcp/bin, and the re-deploy would be measured against the wrong cache entirely.
    let output = run_rcp_with_args_home_and_env(
        &[
            "--auto-deploy-rcpd",
            "--rcpd-path=/nonexistent/rcpd",
            &src_remote,
            &dst_remote,
        ],
        home.path(),
        &[("RCP_REMOTE_HOME_OVERRIDE", override_home.as_str())],
    );
    print_command_output(&output);
    assert!(
        output.status.success(),
        "deployment with checksum verification should succeed"
    );
    // verify that checksum verification happened (check stderr)
    let combined_output = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined_output.contains("Checksum verified")
            || combined_output.contains("SHA-256")
            || combined_output.contains("checksum"),
        "output should mention checksum verification"
    );
    eprintln!(
        "✓ checksum verification is present in deployment (mismatch test requires unit test)"
    );
}

#[test]
fn test_remote_copy_empty_directory_root() {
    // test copying an empty directory via remote protocol
    // verifies DirStub{num_entries=0} is handled correctly
    // verifies DirectoryTracker handles zero-entry directory
    // verifies DirectoryComplete sent immediately
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_subdir = src_dir.path().join("empty_dir");
    std::fs::create_dir(&src_subdir).unwrap();
    let dst_subdir = dst_dir.path().join("empty_dir");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify directory was created
    assert!(dst_subdir.exists());
    assert!(dst_subdir.is_dir());
    // verify summary shows directory created but no files
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 0);
    assert_eq!(summary.directories_created, 1);
    assert_eq!(summary.bytes_copied, 0);
}

#[test]
fn test_remote_copy_empty_nested_directories() {
    // test directory tree with multiple empty directories at various levels
    // verifies all directories created
    // verifies DirectoryTracker properly cascades completion
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_root = src_dir.path().join("nested_empty");
    std::fs::create_dir(&src_root).unwrap();
    std::fs::create_dir(src_root.join("empty1")).unwrap();
    std::fs::create_dir(src_root.join("empty2")).unwrap();
    std::fs::create_dir(src_root.join("empty1/empty1a")).unwrap();
    let dst_root = dst_dir.path().join("nested_empty");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify all directories were created
    assert!(dst_root.exists());
    assert!(dst_root.join("empty1").exists());
    assert!(dst_root.join("empty2").exists());
    assert!(dst_root.join("empty1/empty1a").exists());
    // verify all are directories
    assert!(dst_root.is_dir());
    assert!(dst_root.join("empty1").is_dir());
    assert!(dst_root.join("empty2").is_dir());
    assert!(dst_root.join("empty1/empty1a").is_dir());
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 0);
    assert_eq!(summary.directories_created, 4); // root + 3 subdirs
    assert_eq!(summary.bytes_copied, 0);
}

#[test]
fn test_remote_copy_very_deep_nesting() {
    // test very deep directory structure (100+ levels) via remote protocol
    // verifies no stack overflow in recursive traversal
    // verifies DirectoryTracker handles deep nesting
    // verifies proper completion cascading from deepest to root
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create 100 levels of nesting
    let mut current_path = src_dir.path().join("deep");
    std::fs::create_dir(&current_path).unwrap();
    for i in 0..100 {
        current_path = current_path.join(format!("level{}", i));
        std::fs::create_dir(&current_path).unwrap();
    }
    // create a file at the deepest level
    create_test_file(&current_path.join("deep.txt"), "deepest", 0o644);
    let src_root = src_dir.path().join("deep");
    let dst_root = dst_dir.path().join("deep");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify deepest file exists
    let mut verify_path = dst_root.clone();
    for i in 0..100 {
        verify_path = verify_path.join(format!("level{}", i));
    }
    assert_eq!(get_file_content(&verify_path.join("deep.txt")), "deepest");
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 1);
    assert_eq!(summary.directories_created, 101); // root + 100 levels
}

#[test]
fn test_remote_copy_empty_file_root() {
    // test empty file (0 bytes) copied via remote protocol
    // verifies File{is_root=true} with zero-byte file transfer
    // verifies file stream created and closed correctly for zero-byte file
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("empty.txt");
    let dst_file = dst_dir.path().join("empty.txt");
    create_test_file(&src_file, "", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify empty file was created
    assert_eq!(get_file_content(&dst_file), "");
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 1);
    assert_eq!(summary.bytes_copied, 0);
}

#[test]
fn test_remote_copy_broken_symlink_root() {
    // test symlink pointing to nonexistent target via remote protocol
    // verifies Symlink{is_root=true} message sent
    // verifies broken symlink created at destination
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let nonexistent = src_dir.path().join("does_not_exist.txt");
    let src_link = src_dir.path().join("broken.txt");
    let dst_link = dst_dir.path().join("broken.txt");
    std::os::unix::fs::symlink(&nonexistent, &src_link).unwrap();
    let src_remote = format!("localhost:{}", src_link.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_link.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify symlink was created and points to nonexistent target
    assert!(dst_link.is_symlink());
    assert_eq!(std::fs::read_link(&dst_link).unwrap(), nonexistent);
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.symlinks_created, 1);
}

#[test]
fn test_remote_copy_circular_symlink_root() {
    // test circular symlink reference via remote protocol
    // verifies symlink copied (not dereferenced by default)
    // verifies root symlink handling
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let link1 = src_dir.path().join("link1.txt");
    let link2 = src_dir.path().join("link2.txt");
    let dst_link = dst_dir.path().join("link1.txt");
    std::os::unix::fs::symlink(&link2, &link1).unwrap();
    std::os::unix::fs::symlink(&link1, &link2).unwrap();
    let src_remote = format!("localhost:{}", link1.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_link.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    // verify symlink was created
    assert!(dst_link.is_symlink());
    // verify it points to link2 (circular reference maintained)
    assert_eq!(std::fs::read_link(&dst_link).unwrap(), link2);
    // verify summary
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.symlinks_created, 1);
}

#[test]
fn test_remote_force_remote_flag_uses_rcpd() {
    // verifies that --force-remote with localhost: actually uses rcpd (SSH)
    // this test runs with --force-remote (via test helper) and verifies rcpd was invoked
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "force remote test", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // run_rcp_and_expect_success uses --force-remote
    let output = run_rcp_and_expect_success(&[&src_remote, &dst_remote]);
    // should show the prepared rcpd being started (indicates remote mode was used)
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Starting prepared rcpd server on:"),
        "expected the prepared rcpd spawn in output when using --force-remote, got: {stdout}"
    );
    assert_eq!(get_file_content(&dst_file), "force remote test");
}

// ============================================================================
// Edge Case Tests: Failure Handling with --fail-early
// These tests verify that the protocol handles failures correctly without hanging.
// A hang (timeout) indicates a bug where DirectoryTracker wasn't properly updated
// or send_root_done wasn't called before returning an error.
// ============================================================================

/// Test that child symlink creation failure with --fail-early doesn't cause a hang.
///
/// Bug scenario: When a symlink inside a directory fails to create on the destination
/// and --fail-early is set, the destination should still call decrement_entry() for
/// the parent directory before returning the error. Otherwise, the source hangs
/// waiting for DirectoryComplete.
#[test]
fn test_remote_child_symlink_fail_early_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // create source directory with a file and a symlink
    let src_subdir = src_dir.path().join("symlink_test");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
    std::os::unix::fs::symlink("target", src_subdir.join("link")).unwrap();

    // create destination directory, then make it read-only so symlink creation fails
    let dst_subdir = dst_dir.path().join("symlink_test");
    std::fs::create_dir_all(&dst_subdir).unwrap();
    // make directory read-only - symlink creation will fail with Permission denied
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o555)).unwrap();

    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());

    // with --fail-early and --overwrite (to reuse existing directory)
    // symlink creation will fail due to permission denied
    let output =
        run_rcp_and_expect_failure(&["--fail-early", "--overwrite", &src_remote, &dst_remote]);

    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o755));

    // verify we got an error (not a timeout)
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug where decrement_entry wasn't called"
    );
}

/// Same as test_remote_child_symlink_fail_early_no_hang but with --no-encryption.
/// Used to isolate TLS-related issues from protocol issues.
#[test]
fn test_remote_child_symlink_fail_early_no_hang_no_encryption() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // create source directory with a file and a symlink
    let src_subdir = src_dir.path().join("symlink_test");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
    std::os::unix::fs::symlink("target", src_subdir.join("link")).unwrap();

    // create destination directory, then make it read-only so symlink creation fails
    let dst_subdir = dst_dir.path().join("symlink_test");
    std::fs::create_dir_all(&dst_subdir).unwrap();
    // make directory read-only - symlink creation will fail with Permission denied
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o555)).unwrap();

    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());

    // with --fail-early and --overwrite (to reuse existing directory) and --no-encryption
    // symlink creation will fail due to permission denied
    let output = run_rcp_and_expect_failure(&[
        "--fail-early",
        "--overwrite",
        "--no-encryption",
        &src_remote,
        &dst_remote,
    ]);

    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o755));

    // verify we got an error (not a timeout)
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug where decrement_entry wasn't called"
    );
}

/// A regular FILE that cannot be written into a reused, non-writable destination
/// directory under `--fail-early` must make the whole copy FAIL — not silently exit 0.
///
/// Regression: the symlink cases above travel the CONTROL stream (which propagated the
/// error), but a regular file travels a data connection handled by `handle_file_stream`.
/// That worker logged the file-stream error and `break`-ed with `Ok(())`, so the
/// fail-early write error never reached its `JoinSet`; the destination also still sent
/// `DestinationDone` after the failed file. Both together reported success and dropped
/// the file with a zero exit code — silent data loss.
#[test]
fn test_remote_file_fail_early_reports_failure() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // source: a directory holding a regular file
    let src_subdir = src_dir.path().join("data");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);

    // destination: a pre-existing (reused) directory made non-writable, so the file's
    // create fails with EACCES on the data connection (the directory itself opens fine).
    let dst_subdir = dst_dir.path().join("data");
    std::fs::create_dir_all(&dst_subdir).unwrap();
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o555)).unwrap();

    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());

    let output =
        run_rcp_and_expect_failure(&["--fail-early", "--overwrite", &src_remote, &dst_remote]);

    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o755));

    // not a timeout/hang
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(exit_code != 124, "Command timed out - indicates a hang bug");

    // the real cause (destination Permission denied) must be reported, not swallowed into
    // a bare success. The remote harness routes the final error to stdout, so scan both.
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.to_lowercase().contains("permission denied"),
        "expected the real 'Permission denied' cause in the output, got:\n{combined}"
    );
}

/// A fatal file failure must wake the source even when all remaining payloads are empty.
/// The source can finish sending headers and wait for completion without a data write detecting
/// the receiver's failure. Explicit control teardown must release that wait without sending Done.
#[test]
fn test_remote_multiple_empty_files_fail_early_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // source: a directory of two EMPTY files (header-only transfers). Exactly two is the
    // reliable trigger: the source finishes sending both tiny headers and goes idle before the
    // destination fails the first one, so there is no in-flight data send to break. With more
    // files the source is often still sending when the destination aborts and a broken pipe
    // tears it down instead — hiding the bug.
    let src_subdir = src_dir.path().join("data");
    std::fs::create_dir(&src_subdir).unwrap();
    for i in 0..2 {
        create_test_file(&src_subdir.join(format!("f{i}.txt")), "", 0o644);
    }

    // destination: a pre-existing (reused) directory made non-writable so every file create
    // fails with EACCES while others are still pending.
    let dst_subdir = dst_dir.path().join("data");
    std::fs::create_dir_all(&dst_subdir).unwrap();
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o555)).unwrap();

    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());

    // budget 1 (single data worker) + --preserve to exercise the metadata-completion path.
    let output = run_rcp_and_expect_failure(&[
        "--fail-early",
        "--overwrite",
        "--preserve",
        "--max-connections",
        "1",
        "--pending-writes-multiplier",
        "1",
        &src_remote,
        &dst_remote,
    ]);

    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o755));

    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - the --fail-early multi-file destination hang has regressed"
    );

    // the real cause (destination Permission denied) must be reported, not a teardown symptom.
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.to_lowercase().contains("permission denied"),
        "expected the real 'Permission denied' cause in the output, got:\n{combined}"
    );
}

/// Test that root symlink creation failure with --fail-early doesn't cause a hang.
///
/// Bug scenario: When copying a single symlink as root and it fails to create on
/// the destination with --fail-early, the destination should still call
/// send_root_done() before returning the error. Otherwise, the source hangs
/// waiting for DestinationDone.
#[test]
fn test_remote_root_symlink_fail_early_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // create a symlink as the root item to copy
    let src_symlink = src_dir.path().join("root_link");
    std::os::unix::fs::symlink("target", &src_symlink).unwrap();

    // create destination as a read-only directory to prevent symlink creation
    // (the symlink destination will be inside this directory)
    let dst_path = dst_dir.path().join("root_link");
    std::fs::create_dir(&dst_path).unwrap();
    std::fs::set_permissions(&dst_path, std::fs::Permissions::from_mode(0o555)).unwrap();

    let src_remote = format!("localhost:{}", src_symlink.to_str().unwrap());
    // point to a path inside the read-only directory
    let dst_remote = format!("localhost:{}", dst_path.join("symlink").to_str().unwrap());

    // with --fail-early, this should fail fast, NOT hang
    let output = run_rcp_and_expect_failure(&["--fail-early", &src_remote, &dst_remote]);

    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&dst_path, std::fs::Permissions::from_mode(0o755));

    // verify we got an error (not a timeout)
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug where send_root_done wasn't called"
    );
}

/// Deterministic regression for a metadata failure at FINALIZATION under `--fail-early`: the copy
/// must abort promptly (no hang) AND report the real cause.
///
/// This needs `sudo`: directory (and file) metadata application on entries the copier owns
/// essentially always succeeds, so a metadata failure can only be forced with a foreign-owned
/// source, which is why the former non-sudo version of this test could not induce one — it accepted
/// either outcome and only rejected a timeout (vacuous). A root-owned source tree makes the
/// destination's `--preserve` chown fail with EPERM when it finalizes the created directory/file (we
/// run rcpd as a normal user), which is distinct from a file-CREATION failure. The shared abort
/// mechanism this exercises — `handle_file_stream` returns Err, the worker pool's join loop signals
/// the source once, and `run_destination` reports the recorded cause — is also covered WITHOUT root
/// by `test_remote_multiple_empty_files_fail_early_no_hang`; this pins the finalization ORIGIN.
#[test]
#[ignore = "requires passwordless sudo"]
fn test_remote_sudo_metadata_failure_fail_early_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // ISOLATE the directory-finalization failure: make ONLY the directory root-owned, and give the
    // file back to the invoking user. Under --preserve the destination's chown of the FILE is then a
    // no-op (source file owner == copier), so it succeeds, and the only EPERM is the chown of the
    // DIRECTORY to root at finalization — pinning the directory-finalization path (not a file error).
    // (`$SUDO_UID`/`$SUDO_GID` are the invoking user's ids, set by sudo.) The dir stays 0o755 so the
    // normal-user source rcpd can still enumerate it.
    let src_subdir = src_dir.path().join("meta_dir");
    let status = std::process::Command::new("sudo")
        .args([
            "-n",
            "bash",
            "-c",
            &format!(
                "mkdir -p '{dir}' && echo data > '{dir}/f.txt' && \
                 chown root:root '{dir}' && chown \"$SUDO_UID:$SUDO_GID\" '{dir}/f.txt'",
                dir = src_subdir.display()
            ),
        ])
        .status()
        .expect("Failed to run sudo");
    if !status.success() {
        eprintln!("Skipping test: passwordless sudo not available");
        return;
    }

    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_subdir = dst_dir.path().join("meta_dir");
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());

    // --preserve triggers the finalization chown; --fail-early makes that failure ABORT.
    let output =
        run_rcp_and_expect_failure(&["--preserve", "--fail-early", &src_remote, &dst_remote]);

    // cleanup root-owned source
    let _ = std::process::Command::new("sudo")
        .args(["-n", "rm", "-rf", &src_subdir.to_string_lossy()])
        .status();

    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - a metadata failure under --fail-early hung"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let combined = combined.to_lowercase();
    assert!(
        combined.contains("operation not permitted") || combined.contains("permission denied"),
        "expected the real metadata-failure cause (chown EPERM), got:\n{combined}"
    );
    // the directory is isolated (the file chown is a no-op), so the EPERM can ONLY be the
    // directory's finalization chown — confirm the reported cause is the metadata path, not a file
    // create error (which would read "failed creating", never "metadata").
    assert!(
        combined.contains("metadata"),
        "expected the DIRECTORY metadata-finalization cause (not a file error), got:\n{combined}"
    );
}

/// Test that multiple child symlink failures with --fail-early complete without hanging.
///
/// This tests a directory with multiple symlinks where all fail, ensuring the
/// protocol properly handles multiple failures in sequence.
#[test]
fn test_remote_multiple_child_symlinks_fail_early_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // create source directory with multiple symlinks
    let src_subdir = src_dir.path().join("multi_symlink_test");
    std::fs::create_dir(&src_subdir).unwrap();
    std::os::unix::fs::symlink("target1", src_subdir.join("link1")).unwrap();
    std::os::unix::fs::symlink("target2", src_subdir.join("link2")).unwrap();
    std::os::unix::fs::symlink("target3", src_subdir.join("link3")).unwrap();

    // create destination as read-only so symlink creation fails
    let dst_subdir = dst_dir.path().join("multi_symlink_test");
    std::fs::create_dir_all(&dst_subdir).unwrap();
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o555)).unwrap();

    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());

    // with --fail-early and --overwrite (to reuse existing directory)
    // symlink creation will fail due to permission denied
    let output =
        run_rcp_and_expect_failure(&["--fail-early", "--overwrite", &src_remote, &dst_remote]);

    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o755));

    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug in symlink failure handling"
    );
}

/// Test nested directory with symlink failure and --fail-early.
///
/// Tests that when a symlink fails in a nested directory structure with --fail-early,
/// all parent directories properly complete their tracking.
#[test]
fn test_remote_nested_directory_symlink_fail_early_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // create nested source structure: parent/child/link
    let src_parent = src_dir.path().join("parent");
    let src_child = src_parent.join("child");
    std::fs::create_dir_all(&src_child).unwrap();
    create_test_file(&src_child.join("file.txt"), "content", 0o644);
    std::os::unix::fs::symlink("target", src_child.join("link")).unwrap();

    // create destination directories, make child read-only to fail symlink creation
    let dst_parent = dst_dir.path().join("parent");
    let dst_child = dst_parent.join("child");
    std::fs::create_dir_all(&dst_child).unwrap();
    std::fs::set_permissions(&dst_child, std::fs::Permissions::from_mode(0o555)).unwrap();

    let src_remote = format!("localhost:{}", src_parent.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_parent.to_str().unwrap());

    // should fail on symlink but not hang waiting for parent directories to complete
    // use --overwrite to allow reusing existing directories
    let output =
        run_rcp_and_expect_failure(&["--fail-early", "--overwrite", &src_remote, &dst_remote]);

    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&dst_child, std::fs::Permissions::from_mode(0o755));

    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug where parent directory tracking failed"
    );
}

/// Test that directory metadata is applied when a child file fails WITHOUT --fail-early.
///
/// This is a regression test for ensuring that the DirectoryTracker is properly
/// updated even when errors occur. The test verifies that with --preserve (but
/// without --fail-early), directory permissions are correctly applied even when
/// a child file fails to copy (e.g., due to permission errors).
///
/// Note: With --fail-early, metadata may NOT be applied because we close the
/// connection immediately after the error. This is expected behavior - the user
/// asked for fast failure. This test uses non-fail-early mode to verify the
/// metadata flow works correctly.
#[test]
fn test_remote_file_failure_still_applies_parent_directory_metadata() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();

    // create source directory with specific permissions (0o700 = rwx------)
    let src_subdir = src_dir.path().join("metadata_test");
    std::fs::create_dir(&src_subdir).unwrap();
    std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o700)).unwrap();

    // create an unreadable file (will fail on source side when trying to open)
    create_test_file(&src_subdir.join("unreadable.txt"), "secret", 0o000);

    // verify source directory has the permissions we set
    let src_mode = std::fs::metadata(&src_subdir).unwrap().permissions().mode() & 0o777;
    assert_eq!(src_mode, 0o700, "Source directory should have mode 0o700");

    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_subdir = dst_dir.path().join("metadata_test");
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());

    // run with --preserve but WITHOUT --fail-early
    // the file will fail to open on source, but operation should continue and
    // directory metadata should be applied
    let output = run_rcp_and_expect_failure(&["--preserve", &src_remote, &dst_remote]);

    // verify we didn't timeout
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(exit_code != 124, "Command timed out");

    // THE KEY ASSERTION: directory permissions should have been updated
    // Protocol flow:
    //   1. DirStub{metadata_test, 1} -> Destination creates dir, tracks count=1
    //   2. DirectoryReady sent to source
    //   3. Source tries to open file -> FAILS (Permission denied)
    //   4. Source sends FileSkipped message
    //   5. Destination receives FileSkipped, calls decrement_entry() -> count=0
    //   6. DirectoryComplete sent to source
    //   7. Source receives DirectoryComplete, sends Directory{metadata_test, metadata}
    //   8. Destination applies metadata (including permissions 0o700)
    let dst_mode_after = std::fs::metadata(&dst_subdir).unwrap().permissions().mode() & 0o777;
    assert_eq!(
        dst_mode_after, 0o700,
        "Destination directory permissions should be updated to 0o700 from source, \
         but got {:o}. This indicates the Directory metadata message was never sent.",
        dst_mode_after
    );
}

/// Test that copying a directory to a path where a file already exists (without --overwrite)
/// doesn't cause a hang.
///
/// Bug scenario: When the root directory can't be created because destination already
/// exists as a file (and --overwrite is not set), the destination must still set
/// root_complete to avoid hanging forever waiting for DestinationDone.
#[test]
fn test_remote_root_directory_exists_as_file_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source directory with a file
    let src_subdir = src_dir.path().join("mydir");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
    // create destination as a FILE (not a directory) - this will cause root creation to fail
    let dst_path = dst_dir.path().join("mydir");
    create_test_file(&dst_path, "i am a file", 0o644);
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    // without --overwrite, copying a directory to a file path should fail fast, NOT hang
    let output = run_rcp_and_expect_failure(&[&src_remote, &dst_remote]);
    // verify we didn't timeout (timeout = 124)
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug where root_complete wasn't set"
    );
    // verify the file wasn't replaced
    assert!(dst_path.is_file(), "Destination should still be a file");
    assert_eq!(get_file_content(&dst_path), "i am a file");
}

/// Test that copying an inaccessible root symlink doesn't hang.
///
/// Bug scenario: When a root symlink's metadata can't be read (e.g., parent directory
/// has no execute permission), the source must fail cleanly rather than hanging.
/// Previously, metadata failures for root items would return Ok(()) without sending
/// any message, causing the destination to wait forever for root completion.
#[test]
fn test_remote_root_symlink_inaccessible_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create a symlink inside a subdirectory
    let src_subdir = src_dir.path().join("restricted");
    std::fs::create_dir(&src_subdir).unwrap();
    let src_symlink = src_subdir.join("link");
    std::os::unix::fs::symlink("target", &src_symlink).unwrap();
    // remove all permissions from the parent directory AFTER creating the symlink
    // this makes the symlink inaccessible (stat/lstat will fail with EACCES)
    std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o000)).unwrap();
    let dst_symlink = dst_dir.path().join("link");
    let src_remote = format!("localhost:{}", src_symlink.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_symlink.to_str().unwrap());
    // this should fail (can't access source) but NOT hang
    let output = run_rcp_and_expect_failure(&[&src_remote, &dst_remote]);
    // restore permissions for cleanup
    let _ = std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o755));
    // verify we didn't timeout (timeout = 124)
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "Command timed out - this indicates a hang bug where root metadata failure wasn't handled"
    );
    // verify the destination symlink was NOT created (source was inaccessible)
    assert!(
        !dst_symlink.exists(),
        "Destination should not exist since source was inaccessible"
    );
}

/// Test that stream continues processing files after a metadata error.
///
/// Bug scenario: When file metadata fails to set (e.g., chown fails for root-owned file),
/// the stream should continue processing subsequent files since all data was consumed.
/// Previously, metadata errors marked the stream as corrupted, unnecessarily closing it.
///
/// This test requires passwordless sudo to create a root-owned file.
#[test]
#[ignore = "requires passwordless sudo"]
fn test_remote_sudo_stream_continues_after_metadata_error() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source directory with multiple files
    let src_subdir = src_dir.path().join("metadata_test");
    std::fs::create_dir(&src_subdir).unwrap();
    // file1: normal file (will succeed completely)
    create_test_file(&src_subdir.join("file1.txt"), "content1", 0o644);
    // file2: root-owned file (metadata will fail when copying as non-root with --preserve)
    // use sudo -n to avoid password prompt; skip test if passwordless sudo unavailable
    let root_file = src_subdir.join("root_owned.txt");
    let status = std::process::Command::new("sudo")
        .args([
            "-n",
            "bash",
            "-c",
            &format!(
                "echo 'root content' > '{}' && chown root:root '{}'",
                root_file.display(),
                root_file.display()
            ),
        ])
        .status()
        .expect("Failed to run sudo");
    if !status.success() {
        eprintln!("Skipping test: passwordless sudo not available");
        return;
    }
    // file3: normal file (should still be copied after file2's metadata fails)
    create_test_file(&src_subdir.join("file3.txt"), "content3", 0o644);
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_subdir = dst_dir.path().join("metadata_test");
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // run with --preserve to trigger chown (which will fail for root-owned file)
    let output = run_rcp_with_args(&["--preserve", "--summary", &src_remote, &dst_remote]);
    print_command_output(&output);
    // cleanup root-owned file
    let _ = std::process::Command::new("sudo")
        .args(["-n", "rm", "-f", &root_file.to_string_lossy()])
        .status();
    // verify all files' DATA was transferred (even if metadata failed for some)
    assert!(
        dst_subdir.join("file1.txt").exists(),
        "file1.txt should be copied"
    );
    assert_eq!(get_file_content(&dst_subdir.join("file1.txt")), "content1");
    assert!(
        dst_subdir.join("root_owned.txt").exists(),
        "root_owned.txt data should be copied (even if metadata failed)"
    );
    assert_eq!(
        get_file_content(&dst_subdir.join("root_owned.txt")),
        "root content\n"
    );
    // KEY ASSERTION: file3 should be copied, proving stream continued after file2's metadata error
    assert!(
        dst_subdir.join("file3.txt").exists(),
        "file3.txt should be copied - stream should continue after metadata error"
    );
    assert_eq!(get_file_content(&dst_subdir.join("file3.txt")), "content3");
    // command should report failure (due to chown error) but not hang
    assert!(
        !output.status.success(),
        "should fail due to chown permission error"
    );
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "should not timeout - stream should continue after metadata error"
    );
    eprintln!("✓ Stream continued processing files after metadata error");
}

#[test]
fn test_remote_copy_progress_reporting() {
    // verify that progress updates are received from rcpd processes during remote copy.
    // this test ensures the tracing/progress infrastructure is correctly wired up:
    // - rcpd sends progress updates over the tracing TCP connection
    // - master receives and aggregates progress from both source and destination
    // - progress output shows non-zero file counts
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // use filegen to create many small files (10000 x 1KB) to ensure copy takes long enough
    // for progress updates to be captured
    let filegen_path = assert_cmd::cargo::cargo_bin("filegen");
    let filegen_output = std::process::Command::new(&filegen_path)
        .args([
            src_dir.path().to_str().unwrap(),
            "1",     // single directory
            "10000", // 10000 files
            "1K",    // 1KB each
        ])
        .output()
        .expect("Failed to run filegen");
    assert!(
        filegen_output.status.success(),
        "filegen should succeed: {}",
        String::from_utf8_lossy(&filegen_output.stderr)
    );
    // filegen creates files in src_dir/filegen/
    let filegen_dir = src_dir.path().join("filegen");
    let src_remote = format!("localhost:{}/", filegen_dir.to_str().unwrap());
    let dst_remote = format!("localhost:{}/", dst_dir.path().to_str().unwrap());
    // run with --progress and text-updates to get progress output on stderr
    // use a short progress delay to ensure we get updates quickly
    let output = run_rcp_with_args(&[
        "--progress",
        "--progress-type=text-updates",
        "--progress-delay=100ms",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(output.status.success(), "Copy should succeed");
    // verify files were copied - filegen creates in src_dir/filegen/dir0/
    // with trailing slash on src, rcp copies the directory contents into dst
    let dst_subdir = dst_dir.path().join("filegen").join("dir0");
    assert!(
        dst_subdir.exists(),
        "Destination subdirectory should exist: {:?}",
        dst_subdir
    );
    // check stderr for progress output - should contain progress updates with file counts
    // the progress output includes "files:" lines showing copied file counts
    let stderr = String::from_utf8_lossy(&output.stderr);
    // progress output should contain the separator lines
    assert!(
        stderr.contains("======================="),
        "Progress output should contain separator lines. stderr:\n{stderr}"
    );
    // progress should show that files were copied (files: followed by non-zero count)
    // the format is "files:       N" where N is the count
    let has_files_progress = stderr.lines().any(|line| {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("files:") {
            // parse the number after "files:"
            rest.trim().parse::<u64>().map(|n| n > 0).unwrap_or(false)
        } else {
            false
        }
    });
    assert!(
        has_files_progress,
        "Progress output should show files being copied (files: N where N > 0). stderr:\n{stderr}"
    );
    eprintln!("✓ Progress reporting works correctly");
}

// ============================================================================
// Remote filtering and dry-run tests
// ============================================================================

/// A dry run can outlive the data-handshake deadline without opening any data connections.
#[test]
fn remote_encrypted_dry_run_outlives_data_connection_timeout() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let generated = std::process::Command::new(assert_cmd::cargo::cargo_bin("filegen"))
        .args([
            src_dir.path().to_str().unwrap(),
            "1",
            "1500",
            "1K",
            "--leaf-files",
        ])
        .output()
        .expect("failed to generate dry-run fixture");
    assert!(
        generated.status.success(),
        "filegen failed: {}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let source = src_dir.path().join("filegen");
    let destination = dst_dir.path().join("preview");
    let source_remote = format!("localhost:{}", source.display());
    let destination_remote = format!("localhost:{}", destination.display());
    let daemon = format!(
        "--rcpd-path={}",
        assert_cmd::cargo::cargo_bin("rcpd").display()
    );
    // metadata admission keeps discovery running beyond the unused data handshake's deadline
    let started = std::time::Instant::now();
    let output = run_rcp_with_args(&[
        &daemon,
        "--dry-run=brief",
        "--ops-throttle=100",
        "--remote-copy-conn-timeout-sec=5",
        &source_remote,
        &destination_remote,
    ]);
    print_command_output(&output);
    assert!(
        output.status.success(),
        "a throttled encrypted dry run must not fail when unused data handshakes time out"
    );
    assert!(
        started.elapsed() > std::time::Duration::from_secs(5),
        "the fixture must outlive the configured data-connection timeout"
    );
    assert!(
        !destination.exists(),
        "dry run must not create its destination"
    );
}

/// Test remote dry-run mode with brief output - should not hang and show what would be copied.
#[test]
fn test_remote_dry_run_brief() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("test.txt");
    let dst_file = dst_dir.path().join("test.txt");
    create_test_file(&src_file, "dry run test content", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--dry-run=brief", &src_remote, &dst_remote]);
    // file should NOT exist (dry-run)
    assert!(
        !dst_file.exists(),
        "File should not be created in dry-run mode"
    );
    // output should mention the file
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("test.txt") || stdout.contains("would"),
        "Dry-run output should mention the file or 'would': {stdout}"
    );
}

/// Test remote dry-run mode with a directory - should not hang.
#[test]
fn test_remote_dry_run_directory() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_subdir = src_dir.path().join("mydir");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file1.txt"), "content1", 0o644);
    create_test_file(&src_subdir.join("file2.txt"), "content2", 0o644);
    let dst_subdir = dst_dir.path().join("mydir");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--dry-run=brief", &src_remote, &dst_remote]);
    // directory should NOT exist (dry-run)
    assert!(
        !dst_subdir.exists(),
        "Directory should not be created in dry-run mode"
    );
    // output should mention files
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("file1.txt") || stdout.contains("file2.txt") || stdout.contains("would"),
        "Dry-run output should mention files: {stdout}"
    );
}

/// Test remote copy with filtered root file - should not hang, nothing copied.
#[test]
fn test_remote_filter_excludes_root_file() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("excluded.log");
    let dst_file = dst_dir.path().join("excluded.log");
    create_test_file(&src_file, "should be excluded", 0o644);
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    // exclude *.log files - this should filter out the root file
    let output = run_rcp_and_expect_success(&["--exclude=*.log", &src_remote, &dst_remote]);
    // file should NOT exist (filtered out)
    assert!(
        !dst_file.exists(),
        "File should not be created when filtered out"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    eprintln!("stdout: {stdout}");
}

/// Test remote copy with filtered root directory - should not hang, nothing copied.
#[test]
fn test_remote_filter_excludes_root_directory() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_subdir = src_dir.path().join("excluded_dir");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
    let dst_subdir = dst_dir.path().join("excluded_dir");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // exclude directories ending with _dir
    let output = run_rcp_and_expect_success(&["--exclude=*_dir/", &src_remote, &dst_remote]);
    // directory should NOT exist (filtered out)
    assert!(
        !dst_subdir.exists(),
        "Directory should not be created when filtered out"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    eprintln!("stdout: {stdout}");
}

/// Test remote copy with filtered root symlink - should not hang, nothing copied.
#[test]
fn test_remote_filter_excludes_root_symlink() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("target.txt");
    create_test_file(&src_file, "target content", 0o644);
    let src_symlink = src_dir.path().join("excluded.link");
    std::os::unix::fs::symlink(&src_file, &src_symlink).unwrap();
    let dst_symlink = dst_dir.path().join("excluded.link");
    let src_remote = format!("localhost:{}", src_symlink.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_symlink.to_str().unwrap());
    // exclude *.link files - this should filter out the root symlink
    let output = run_rcp_and_expect_success(&["--exclude=*.link", &src_remote, &dst_remote]);
    // symlink should NOT exist (filtered out)
    assert!(
        !dst_symlink.exists(),
        "Symlink should not be created when filtered out"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    eprintln!("stdout: {stdout}");
}

/// Test remote copy with include pattern - only matching files are copied.
#[test]
fn test_remote_filter_include_pattern() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_subdir = src_dir.path().join("mixed");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("keep.txt"), "keep this", 0o644);
    create_test_file(&src_subdir.join("skip.log"), "skip this", 0o644);
    create_test_file(&src_subdir.join("also_keep.txt"), "also keep", 0o644);
    let dst_subdir = dst_dir.path().join("mixed");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // include only *.txt files
    let output =
        run_rcp_and_expect_success(&["--include=*.txt", "--summary", &src_remote, &dst_remote]);
    // txt files should exist
    assert!(
        dst_subdir.join("keep.txt").exists(),
        "keep.txt should exist"
    );
    assert!(
        dst_subdir.join("also_keep.txt").exists(),
        "also_keep.txt should exist"
    );
    // log file should NOT exist
    assert!(
        !dst_subdir.join("skip.log").exists(),
        "skip.log should not exist (filtered)"
    );
    // verify summary shows 2 files copied
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 2, "Should copy exactly 2 txt files");
}

/// Test remote copy with exclude pattern - matching files are skipped.
#[test]
fn test_remote_filter_exclude_pattern() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_subdir = src_dir.path().join("mixed");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("keep.txt"), "keep this", 0o644);
    create_test_file(&src_subdir.join("skip.log"), "skip this", 0o644);
    create_test_file(&src_subdir.join("also_skip.log"), "also skip", 0o644);
    let dst_subdir = dst_dir.path().join("mixed");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // exclude *.log files
    let output =
        run_rcp_and_expect_success(&["--exclude=*.log", "--summary", &src_remote, &dst_remote]);
    // txt file should exist
    assert!(
        dst_subdir.join("keep.txt").exists(),
        "keep.txt should exist"
    );
    // log files should NOT exist
    assert!(
        !dst_subdir.join("skip.log").exists(),
        "skip.log should not exist (filtered)"
    );
    assert!(
        !dst_subdir.join("also_skip.log").exists(),
        "also_skip.log should not exist (filtered)"
    );
    // verify summary shows 1 file copied
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 1, "Should copy exactly 1 txt file");
}

/// Test remote dry-run with filtering - shows what would be copied respecting filters.
#[test]
fn test_remote_dry_run_with_filter() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_subdir = src_dir.path().join("filtered");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("include.txt"), "included", 0o644);
    create_test_file(&src_subdir.join("exclude.log"), "excluded", 0o644);
    let dst_subdir = dst_dir.path().join("filtered");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // dry-run with exclude filter
    let output = run_rcp_and_expect_success(&[
        "--dry-run=brief",
        "--exclude=*.log",
        &src_remote,
        &dst_remote,
    ]);
    // nothing should be created (dry-run)
    assert!(
        !dst_subdir.exists(),
        "Directory should not be created in dry-run mode"
    );
    // output should mention the included file
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("include.txt"),
        "Dry-run should show include.txt: {stdout}"
    );
}

/// Test remote copy filtering with nested directories - filters apply recursively.
#[test]
fn test_remote_filter_nested_directories() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_subdir = src_dir.path().join("nested");
    std::fs::create_dir_all(src_subdir.join("level1/level2")).unwrap();
    create_test_file(&src_subdir.join("root.txt"), "root", 0o644);
    create_test_file(&src_subdir.join("root.log"), "root log", 0o644);
    create_test_file(&src_subdir.join("level1/l1.txt"), "level1", 0o644);
    create_test_file(&src_subdir.join("level1/l1.log"), "level1 log", 0o644);
    create_test_file(&src_subdir.join("level1/level2/l2.txt"), "level2", 0o644);
    create_test_file(
        &src_subdir.join("level1/level2/l2.log"),
        "level2 log",
        0o644,
    );
    let dst_subdir = dst_dir.path().join("nested");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // exclude all *.log files
    let output =
        run_rcp_and_expect_success(&["--exclude=*.log", "--summary", &src_remote, &dst_remote]);
    // txt files should exist at all levels
    assert!(dst_subdir.join("root.txt").exists());
    assert!(dst_subdir.join("level1/l1.txt").exists());
    assert!(dst_subdir.join("level1/level2/l2.txt").exists());
    // log files should NOT exist at any level
    assert!(!dst_subdir.join("root.log").exists());
    assert!(!dst_subdir.join("level1/l1.log").exists());
    assert!(!dst_subdir.join("level1/level2/l2.log").exists());
    // verify summary shows 3 files copied (only .txt files)
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 3, "Should copy exactly 3 txt files");
}

/// Test that anchored exclude patterns do NOT filter out the root directory itself.
/// Anchored patterns (starting with /) match paths INSIDE the source, not the source root.
#[test]
fn test_remote_filter_anchored_exclude_does_not_filter_root() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create a directory named "excluded_dir" with a file inside
    let src_subdir = src_dir.path().join("excluded_dir");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
    let dst_subdir = dst_dir.path().join("excluded_dir");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // use anchored exclude pattern that matches the root directory's NAME
    // this should NOT filter the root because anchored patterns match paths INSIDE the source
    let output = run_rcp_and_expect_success(&[
        "--exclude=/excluded_dir/",
        "--summary",
        &src_remote,
        &dst_remote,
    ]);
    // the directory and its contents SHOULD be copied (anchored pattern doesn't apply to root)
    assert!(
        dst_subdir.exists(),
        "Root directory should exist (anchored exclude doesn't apply to root)"
    );
    assert!(
        dst_subdir.join("file.txt").exists(),
        "File inside should exist"
    );
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 1, "Should copy the file inside");
}

/// Test that anchored exclude patterns correctly filter subdirectories inside the source.
#[test]
fn test_remote_filter_anchored_exclude_filters_inside() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create a directory with a subdirectory that should be excluded
    let src_subdir = src_dir.path().join("project");
    std::fs::create_dir_all(src_subdir.join("build")).unwrap();
    create_test_file(&src_subdir.join("src.txt"), "source", 0o644);
    create_test_file(&src_subdir.join("build/output.txt"), "build output", 0o644);
    let dst_subdir = dst_dir.path().join("project");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // use anchored exclude to exclude /build inside the source
    let output =
        run_rcp_and_expect_success(&["--exclude=/build/", "--summary", &src_remote, &dst_remote]);
    // src.txt should be copied
    assert!(
        dst_subdir.join("src.txt").exists(),
        "src.txt should be copied"
    );
    // build directory should NOT be copied (excluded by anchored pattern)
    assert!(
        !dst_subdir.join("build").exists(),
        "build directory should be excluded by anchored pattern"
    );
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 1, "Should only copy src.txt");
}

/// Test that dry-run does NOT traverse into excluded directories even when include patterns exist.
/// Excludes should be absolute - an excluded directory should never show its contents.
/// Verify by checking that dry-run summary matches actual copy summary.
#[test]
fn test_remote_dry_run_exclude_is_absolute() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create a directory structure:
    // src/
    //   excluded_dir/
    //     hidden.txt   <- should NOT be counted
    //   included_dir/
    //     visible.txt  <- should be counted
    let src_root = src_dir.path().join("mixed");
    std::fs::create_dir_all(src_root.join("excluded_dir")).unwrap();
    std::fs::create_dir_all(src_root.join("included_dir")).unwrap();
    create_test_file(&src_root.join("excluded_dir/hidden.txt"), "hidden", 0o644);
    create_test_file(&src_root.join("included_dir/visible.txt"), "visible", 0o644);
    // first, do a dry-run to get the predicted summary
    let dst_root_dry = dst_dir.path().join("mixed_dry");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote_dry = format!("localhost:{}", dst_root_dry.to_str().unwrap());
    let dry_output = run_rcp_and_expect_success(&[
        "--dry-run=brief",
        "--summary",
        "--include=*.txt",
        "--exclude=excluded_dir/",
        &src_remote,
        &dst_remote_dry,
    ]);
    let dry_summary =
        parse_summary_from_output(&dry_output).expect("Failed to parse dry-run summary");
    // nothing should be created (dry-run)
    assert!(
        !dst_root_dry.exists(),
        "Directory should not be created in dry-run mode"
    );
    // now do the actual copy
    let dst_root_real = dst_dir.path().join("mixed_real");
    let dst_remote_real = format!("localhost:{}", dst_root_real.to_str().unwrap());
    let real_output = run_rcp_and_expect_success(&[
        "--summary",
        "--include=*.txt",
        "--exclude=excluded_dir/",
        &src_remote,
        &dst_remote_real,
    ]);
    let real_summary =
        parse_summary_from_output(&real_output).expect("Failed to parse real summary");
    // verify real copy behaves as expected
    assert!(
        dst_root_real.join("included_dir/visible.txt").exists(),
        "visible.txt should exist"
    );
    assert!(
        !dst_root_real.join("excluded_dir").exists(),
        "excluded_dir should not exist"
    );
    // dry-run and real should have same file count (excludes are absolute in both)
    assert_eq!(
        dry_summary.files_copied, real_summary.files_copied,
        "Dry-run files_copied should match real copy"
    );
    // both should copy exactly 1 file (visible.txt, not hidden.txt)
    assert_eq!(
        real_summary.files_copied, 1,
        "Should copy only 1 file (visible.txt, not hidden.txt in excluded_dir)"
    );
}

/// Test that remote copy with --include cleans up traversed-only empty directories.
/// Directories that don't match include patterns and end up empty after filtering
/// are removed during directory completion.
#[test]
fn test_remote_include_filter_cleans_up_traversed_dirs() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create structure:
    // src/
    //   foo.txt (matches *.txt)
    //   bar.txt (matches *.txt)
    //   empty_dir/ (directory with nothing inside)
    //   other_dir/
    //     other.log (doesn't match *.txt)
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    create_test_file(&src_root.join("foo.txt"), "foo content", 0o644);
    create_test_file(&src_root.join("bar.txt"), "bar content", 0o644);
    std::fs::create_dir(src_root.join("empty_dir")).unwrap();
    std::fs::create_dir(src_root.join("other_dir")).unwrap();
    create_test_file(&src_root.join("other_dir/other.log"), "log content", 0o644);
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // include only *.txt - should copy foo.txt and bar.txt
    let output =
        run_rcp_and_expect_success(&["--include=*.txt", "--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // should have copied 2 files
    assert_eq!(summary.files_copied, 2, "Should copy 2 .txt files");
    // txt files should exist
    assert!(dst_root.join("foo.txt").exists(), "foo.txt should exist");
    assert!(dst_root.join("bar.txt").exists(), "bar.txt should exist");
    // traversed-only empty directories should be cleaned up
    assert!(
        !dst_root.join("empty_dir").exists(),
        "empty_dir should be removed (traversed-only empty dir)"
    );
    assert!(
        !dst_root.join("other_dir").exists(),
        "other_dir should be removed (traversed-only, no matching files)"
    );
    // only the root directory is kept
    assert_eq!(
        summary.directories_created, 1,
        "Should create 1 directory (dst root only, traversed dirs cleaned up)"
    );
}

/// Test that remote dry-run with --include doesn't count empty directories
/// that were only traversed.
#[test]
fn test_remote_include_filter_dry_run_no_empty_dirs() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create structure:
    // src/
    //   foo.txt (matches *.txt)
    //   empty_dir/ (directory with nothing inside)
    //   other_dir/
    //     other.log (doesn't match *.txt)
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    create_test_file(&src_root.join("foo.txt"), "foo content", 0o644);
    std::fs::create_dir(src_root.join("empty_dir")).unwrap();
    std::fs::create_dir(src_root.join("other_dir")).unwrap();
    create_test_file(&src_root.join("other_dir/other.log"), "log content", 0o644);
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // dry-run with --include=*.txt
    let output = run_rcp_and_expect_success(&[
        "--dry-run=brief",
        "--include=*.txt",
        "--summary",
        &src_remote,
        &dst_remote,
    ]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // should report 1 file would be copied
    assert_eq!(
        summary.files_copied, 1,
        "Should report 1 .txt file would be copied"
    );
    // directories_created should be 1 (only the root dst directory)
    assert_eq!(
        summary.directories_created, 1,
        "Dry-run should report only 1 directory (dst root), not empty traversed dirs"
    );
    // destination should not exist (dry-run)
    assert!(
        !dst_root.exists(),
        "Destination should not exist in dry-run mode"
    );
}

/// Test that directories with content matching include patterns are created.
#[test]
fn test_remote_include_filter_directory_with_matching_content() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create structure:
    // src/
    //   has_txt/
    //     file.txt (matches *.txt)
    //   no_txt/
    //     file.log (doesn't match)
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    std::fs::create_dir(src_root.join("has_txt")).unwrap();
    create_test_file(&src_root.join("has_txt/file.txt"), "content", 0o644);
    std::fs::create_dir(src_root.join("no_txt")).unwrap();
    create_test_file(&src_root.join("no_txt/file.log"), "content", 0o644);
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // include only *.txt files
    let output =
        run_rcp_and_expect_success(&["--include=*.txt", "--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // has_txt should exist with file.txt inside
    assert!(
        dst_root.join("has_txt").exists(),
        "has_txt/ should exist (contains matching file)"
    );
    assert!(
        dst_root.join("has_txt/file.txt").exists(),
        "file.txt should exist (matches pattern)"
    );
    // no_txt directory should NOT exist (traversed-only, no matching content, cleaned up)
    assert!(
        !dst_root.join("no_txt").exists(),
        "no_txt/ should be removed (traversed-only, no matching files)"
    );
    // should have copied 1 file
    assert_eq!(summary.files_copied, 1, "Should copy 1 .txt file");
    // should have created 2 directories (dst root + has_txt; no_txt cleaned up)
    assert_eq!(
        summary.directories_created, 2,
        "Should create 2 directories (dst root + has_txt, no_txt cleaned up)"
    );
}

/// Test that remote copy with --exclude only (no --include) preserves empty directories.
/// When only exclude patterns are active, everything not excluded is "directly included",
/// so empty directories should be kept.
#[test]
fn test_remote_exclude_only_keeps_empty_dirs() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create structure:
    // src/
    //   subdir/ (empty directory - should be preserved)
    //   file.txt
    //   ignored.log (excluded by pattern)
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    std::fs::create_dir(src_root.join("subdir")).unwrap();
    create_test_file(&src_root.join("file.txt"), "content", 0o644);
    create_test_file(&src_root.join("ignored.log"), "log content", 0o644);
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // exclude *.log - subdir/ should still be preserved since no include patterns
    let output =
        run_rcp_and_expect_success(&["--exclude=*.log", "--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // file.txt should exist
    assert!(dst_root.join("file.txt").exists(), "file.txt should exist");
    // ignored.log should NOT exist
    assert!(
        !dst_root.join("ignored.log").exists(),
        "ignored.log should be excluded"
    );
    // subdir should exist (empty directory preserved with exclude-only filter)
    assert!(
        dst_root.join("subdir").exists(),
        "subdir/ should be preserved with exclude-only filter"
    );
    // should have copied 1 file
    assert_eq!(summary.files_copied, 1, "Should copy 1 file (file.txt)");
    // should have created 2 directories (dst root + subdir)
    assert_eq!(
        summary.directories_created, 2,
        "Should create 2 directories (dst root + subdir)"
    );
}

/// Test that remote copy with --include keeps empty directories that directly
/// match the include pattern (e.g., --include=emptydir/).
#[test]
fn test_remote_include_directly_matched_empty_dir_kept() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create structure:
    // src/
    //   keep_me/ (empty directory, directly matches include pattern)
    //   other_dir/ (empty directory, does NOT match include pattern)
    //   file.txt
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    std::fs::create_dir(src_root.join("keep_me")).unwrap();
    std::fs::create_dir(src_root.join("other_dir")).unwrap();
    create_test_file(&src_root.join("file.txt"), "content", 0o644);
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // include *.txt and keep_me/ - keep_me should be preserved, other_dir should not
    let output = run_rcp_and_expect_success(&[
        "--include=*.txt",
        "--include=keep_me/",
        "--summary",
        &src_remote,
        &dst_remote,
    ]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // file.txt should exist
    assert!(dst_root.join("file.txt").exists(), "file.txt should exist");
    // keep_me should exist (directly matches include pattern)
    assert!(
        dst_root.join("keep_me").exists(),
        "keep_me/ should be kept (directly matches include pattern)"
    );
    // other_dir should NOT exist (traversed-only empty dir is cleaned up)
    assert!(
        !dst_root.join("other_dir").exists(),
        "other_dir/ should be removed (traversed-only empty dir)"
    );
    // should have copied 1 file
    assert_eq!(summary.files_copied, 1, "Should copy 1 file (file.txt)");
}

/// Test that remote copy does not remove directories that only contain symlinks.
/// The directory has content (symlinks) and should not be removed.
#[test]
fn test_remote_dir_with_symlinks_not_removed() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create structure:
    // src/
    //   has_symlinks/
    //     link -> ../target.txt (symlink)
    //   target.txt
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    std::fs::create_dir(src_root.join("has_symlinks")).unwrap();
    create_test_file(&src_root.join("target.txt"), "target content", 0o644);
    std::os::unix::fs::symlink("../target.txt", src_root.join("has_symlinks/link")).unwrap();
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // copy with include filter - even with filter active, dir with symlinks should be kept
    let output = run_rcp_and_expect_success(&[
        "--include=*.txt",
        "--include=has_symlinks/",
        "--include=has_symlinks/**",
        "--summary",
        &src_remote,
        &dst_remote,
    ]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // has_symlinks directory should exist
    assert!(
        dst_root.join("has_symlinks").exists(),
        "has_symlinks/ should exist (contains symlinks)"
    );
    // symlink should exist
    assert!(
        dst_root.join("has_symlinks/link").exists(),
        "has_symlinks/link symlink should exist"
    );
    // target.txt should exist
    assert!(
        dst_root.join("target.txt").exists(),
        "target.txt should exist"
    );
    // should have copied 1 file and created 1 symlink
    assert_eq!(summary.files_copied, 1, "Should copy 1 file (target.txt)");
    assert_eq!(
        summary.symlinks_created, 1,
        "Should create 1 symlink (link)"
    );
}

/// Test that remote copy with --include preserves the root directory even when
/// nothing matches. Traversed-only subdirectories are cleaned up.
#[test]
fn test_remote_include_filter_root_preserved_when_nothing_matches() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create structure with no .txt files:
    // src/
    //   bar.log (doesn't match *.txt)
    //   empty_dir/ (empty)
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    create_test_file(&src_root.join("bar.log"), "log content", 0o644);
    std::fs::create_dir(src_root.join("empty_dir")).unwrap();
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // include only *.txt - nothing matches
    let output =
        run_rcp_and_expect_success(&["--include=*.txt", "--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // no files should be copied
    assert_eq!(summary.files_copied, 0, "No files match *.txt");
    // only the root directory is kept; empty_dir is cleaned up as traversed-only
    assert_eq!(
        summary.directories_created, 1,
        "Should create 1 directory (dst root only, empty_dir cleaned up)"
    );
    assert!(dst_root.exists(), "Root destination directory should exist");
    // empty_dir should NOT exist (traversed-only empty dir is cleaned up)
    assert!(
        !dst_root.join("empty_dir").exists(),
        "empty_dir/ should be removed (traversed-only empty dir)"
    );
}

/// Test that remote dry-run with --include still counts the root directory
/// even when nothing matches.
#[test]
fn test_remote_include_filter_dry_run_root_counted_when_nothing_matches() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    create_test_file(&src_root.join("bar.log"), "log content", 0o644);
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    let output = run_rcp_and_expect_success(&[
        "--dry-run=brief",
        "--include=*.txt",
        "--summary",
        &src_remote,
        &dst_remote,
    ]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 0, "No files match *.txt");
    assert_eq!(
        summary.directories_created, 1,
        "Dry-run should count root directory even with no matches"
    );
    assert!(
        !dst_root.exists(),
        "Destination should not exist in dry-run mode"
    );
}

/// Test that multiple --include patterns correctly include both file globs and
/// entire subdirectories. Structure: src/ with links_dir/ containing only symlinks,
/// plus a file.txt. The links_dir/ is explicitly included via pattern matching.
#[test]
fn test_remote_filter_include_with_symlinks_only_dir() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create structure:
    // src/
    //   file.txt
    //   links_dir/
    //     link1 -> ../file.txt (symlink)
    //     link2 -> ../file.txt (symlink)
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    create_test_file(&src_root.join("file.txt"), "file content", 0o644);
    std::fs::create_dir(src_root.join("links_dir")).unwrap();
    std::os::unix::fs::symlink("../file.txt", src_root.join("links_dir/link1")).unwrap();
    std::os::unix::fs::symlink("../file.txt", src_root.join("links_dir/link2")).unwrap();
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // include .txt files and links_dir with all its contents
    let output = run_rcp_and_expect_success(&[
        "--include=*.txt",
        "--include=links_dir/",
        "--include=links_dir/**",
        "--summary",
        &src_remote,
        &dst_remote,
    ]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // links_dir should exist because it has symlinks
    assert!(
        dst_root.join("links_dir").exists(),
        "links_dir/ should exist (contains symlinks)"
    );
    // symlinks should exist
    assert!(
        dst_root.join("links_dir/link1").exists(),
        "links_dir/link1 symlink should exist"
    );
    assert!(
        dst_root.join("links_dir/link2").exists(),
        "links_dir/link2 symlink should exist"
    );
    // file.txt should be copied
    assert!(dst_root.join("file.txt").exists(), "file.txt should exist");
    assert_eq!(summary.files_copied, 1, "Should copy 1 file (file.txt)");
    assert_eq!(
        summary.symlinks_created, 2,
        "Should create 2 symlinks (link1, link2)"
    );
}

/// Test that --exclude filter keeps directories that contain only symlinks.
/// Structure: src/ with subdir/ containing file.txt and a symlink. Exclude *.txt
/// to remove all .txt files but keep the symlink. subdir/ should remain because
/// it still has the symlink.
#[test]
fn test_remote_filter_exclude_with_nested_symlinks() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create structure:
    // src/
    //   target.txt
    //   subdir/
    //     file.txt
    //     link -> ../target.txt (symlink)
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    create_test_file(&src_root.join("target.txt"), "target content", 0o644);
    std::fs::create_dir(src_root.join("subdir")).unwrap();
    create_test_file(&src_root.join("subdir/file.txt"), "subdir file", 0o644);
    std::os::unix::fs::symlink("../target.txt", src_root.join("subdir/link")).unwrap();
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // exclude all .txt files - symlinks are not affected by content filters
    let output =
        run_rcp_and_expect_success(&["--exclude=*.txt", "--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // subdir should exist because it still has the symlink
    assert!(
        dst_root.join("subdir").exists(),
        "subdir/ should exist (contains symlink)"
    );
    // symlink should exist (target.txt is excluded so link is broken — use symlink_metadata)
    assert!(
        std::fs::symlink_metadata(dst_root.join("subdir/link"))
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false),
        "subdir/link symlink should exist"
    );
    // no .txt files should be copied
    assert!(
        !dst_root.join("target.txt").exists(),
        "target.txt should be excluded"
    );
    assert!(
        !dst_root.join("subdir/file.txt").exists(),
        "subdir/file.txt should be excluded"
    );
    assert_eq!(summary.files_copied, 0, "No .txt files should be copied");
    assert_eq!(
        summary.symlinks_created, 1,
        "Should create 1 symlink (link)"
    );
}

/// Test that --include filter with deeply nested directories only keeps paths
/// leading to matching files. Directories without matching descendants are cleaned up.
#[test]
fn test_remote_filter_include_deeply_nested_with_mixed_entries() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create structure:
    // src/
    //   root.txt
    //   level1/
    //     level1.log (doesn't match *.txt)
    //     link -> ../root.txt (symlink at level1)
    //     level2/
    //       deep.txt (matches *.txt)
    //   empty_branch/
    //     leaf/ (no files anywhere)
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    create_test_file(&src_root.join("root.txt"), "root content", 0o644);
    std::fs::create_dir(src_root.join("level1")).unwrap();
    create_test_file(&src_root.join("level1/level1.log"), "log content", 0o644);
    std::os::unix::fs::symlink("../root.txt", src_root.join("level1/link")).unwrap();
    std::fs::create_dir(src_root.join("level1/level2")).unwrap();
    create_test_file(
        &src_root.join("level1/level2/deep.txt"),
        "deep content",
        0o644,
    );
    std::fs::create_dir(src_root.join("empty_branch")).unwrap();
    std::fs::create_dir(src_root.join("empty_branch/leaf")).unwrap();
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // include only *.txt - should keep level1/ and level1/level2/ (path to deep.txt)
    // and root.txt, but clean up empty_branch/ entirely
    let output =
        run_rcp_and_expect_success(&["--include=*.txt", "--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // root.txt should be copied
    assert!(dst_root.join("root.txt").exists(), "root.txt should exist");
    // deep.txt should be copied through the nested path
    assert!(
        dst_root.join("level1/level2/deep.txt").exists(),
        "level1/level2/deep.txt should exist"
    );
    // level1.log should NOT exist (excluded by filter)
    assert!(
        !dst_root.join("level1/level1.log").exists(),
        "level1.log should not exist (doesn't match *.txt)"
    );
    // symlink at level1 should NOT exist (doesn't match *.txt)
    assert!(
        !dst_root.join("level1/link").exists(),
        "level1/link symlink should not exist (doesn't match *.txt)"
    );
    // empty_branch should be cleaned up entirely
    assert!(
        !dst_root.join("empty_branch").exists(),
        "empty_branch/ should be removed (no matching descendants)"
    );
    assert_eq!(summary.files_copied, 2, "Should copy 2 .txt files");
}

/// Test that --include filter cleans up directories with only non-matching children.
/// parent/ has child1/ (with .txt) and child2/ (with .log only).
/// child2/ should be cleaned up, child1/ should remain.
#[test]
fn test_remote_filter_include_dir_with_only_subdirs_no_files() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create structure:
    // src/
    //   parent/
    //     child1/
    //       data.txt (matches *.txt)
    //     child2/
    //       data.log (doesn't match *.txt)
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    std::fs::create_dir(src_root.join("parent")).unwrap();
    std::fs::create_dir(src_root.join("parent/child1")).unwrap();
    create_test_file(
        &src_root.join("parent/child1/data.txt"),
        "txt content",
        0o644,
    );
    std::fs::create_dir(src_root.join("parent/child2")).unwrap();
    create_test_file(
        &src_root.join("parent/child2/data.log"),
        "log content",
        0o644,
    );
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // include only *.txt
    let output =
        run_rcp_and_expect_success(&["--include=*.txt", "--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // parent/ should exist (contains child1/ which has matching file)
    assert!(
        dst_root.join("parent").exists(),
        "parent/ should exist (has child with matching file)"
    );
    // child1/ should exist with data.txt
    assert!(
        dst_root.join("parent/child1/data.txt").exists(),
        "parent/child1/data.txt should exist"
    );
    // child2/ should be cleaned up (no matching files)
    assert!(
        !dst_root.join("parent/child2").exists(),
        "parent/child2/ should be removed (no matching descendants)"
    );
    assert_eq!(summary.files_copied, 1, "Should copy 1 .txt file");
    // directories kept: dst root, parent/, child1/ = 3
    assert_eq!(
        summary.directories_created, 3,
        "Should create 3 directories (root, parent, child1)"
    );
}

/// Test that nested empty directories are cleaned up bottom-up when filtering.
/// Verifies that child directories notify their parent upon completion (not creation),
/// so parent directories can correctly decide to clean up when all children are gone.
///
/// Structure:
///   src/
///     parent/
///       child_match/
///         file.txt     (matches *.txt)
///       child_empty/
///         subchild/    (no matching files)
///
/// With --include=*.txt, child_empty/ and subchild/ should both be removed.
/// parent/ is kept because child_match/ has matching content.
#[test]
fn test_remote_filter_include_nested_empty_dirs_cleaned_up() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    std::fs::create_dir(src_root.join("parent")).unwrap();
    std::fs::create_dir(src_root.join("parent/child_match")).unwrap();
    create_test_file(
        &src_root.join("parent/child_match/file.txt"),
        "matched",
        0o644,
    );
    std::fs::create_dir(src_root.join("parent/child_empty")).unwrap();
    std::fs::create_dir(src_root.join("parent/child_empty/subchild")).unwrap();
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--include=*.txt", "--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // parent/ should exist (has child_match/ with matching file)
    assert!(
        dst_root.join("parent").exists(),
        "parent/ should exist (has child with matching file)"
    );
    // child_match/ should exist with file.txt
    assert!(
        dst_root.join("parent/child_match/file.txt").exists(),
        "parent/child_match/file.txt should exist"
    );
    // child_empty/ should be cleaned up (no matching files anywhere below)
    assert!(
        !dst_root.join("parent/child_empty").exists(),
        "parent/child_empty/ should be removed (no matching descendants)"
    );
    // subchild/ inside child_empty should also be gone
    assert!(
        !dst_root.join("parent/child_empty/subchild").exists(),
        "parent/child_empty/subchild/ should be removed"
    );
    assert_eq!(summary.files_copied, 1, "Should copy 1 .txt file");
    // directories kept: dst root, parent/, child_match/ = 3
    assert_eq!(
        summary.directories_created, 3,
        "Should create 3 directories (root, parent, child_match)"
    );
}

/// Test that remote copy of a directory with only empty subdirectories and no files
/// completes without hanging. Verifies correct summary counts.
#[test]
fn test_remote_copy_dir_with_only_subdirs_no_files_no_filter() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create structure:
    // src/
    //   parent/
    //     child1/ (empty)
    //     child2/ (empty)
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    std::fs::create_dir(src_root.join("parent")).unwrap();
    std::fs::create_dir(src_root.join("parent/child1")).unwrap();
    std::fs::create_dir(src_root.join("parent/child2")).unwrap();
    let dst_root = dst_dir.path().join("dst");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // no filter - copy everything
    let output = run_rcp_and_expect_success(&["--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // all directories should be created
    assert!(dst_root.join("parent").exists(), "parent/ should exist");
    assert!(
        dst_root.join("parent/child1").exists(),
        "parent/child1/ should exist"
    );
    assert!(
        dst_root.join("parent/child2").exists(),
        "parent/child2/ should exist"
    );
    assert_eq!(summary.files_copied, 0, "No files to copy");
    // directories: dst root, parent/, child1/, child2/ = 4
    assert_eq!(
        summary.directories_created, 4,
        "Should create 4 directories (root, parent, child1, child2)"
    );
}

/// Test that remote copy with --fail-early exits with error rather than hanging
/// when the destination becomes unwritable. This verifies that unified tracking
/// handles mixed-entry errors correctly.
#[test]
fn test_remote_copy_mixed_entries_no_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create source with files, subdirs, and symlinks at multiple levels
    // src/
    //   file1.txt
    //   subdir/
    //     file2.txt
    //     link -> ../file1.txt
    let src_root = src_dir.path().join("src");
    std::fs::create_dir(&src_root).unwrap();
    create_test_file(&src_root.join("file1.txt"), "content1", 0o644);
    std::fs::create_dir(src_root.join("subdir")).unwrap();
    create_test_file(&src_root.join("subdir/file2.txt"), "content2", 0o644);
    std::os::unix::fs::symlink("../file1.txt", src_root.join("subdir/link")).unwrap();
    // create destination and make it read-only to force errors
    let dst_root = dst_dir.path().join("dst");
    std::fs::create_dir(&dst_root).unwrap();
    std::fs::set_permissions(&dst_root, std::fs::Permissions::from_mode(0o555)).unwrap();
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // use --fail-early so we fail fast on permission errors
    let output = run_rcp_and_expect_failure(&["--fail-early", &src_remote, &dst_remote]);
    // the critical assertion: we must NOT have timed out (exit code 124).
    // this is already checked in run_rcp_with_args_internal, but make it explicit.
    let exit_code = output.status.code().unwrap_or(-1);
    assert_ne!(
        exit_code, 124,
        "Command timed out (exit code 124) - this indicates a hang bug in unified tracking"
    );
    // restore permissions so tempdir cleanup works
    std::fs::set_permissions(&dst_root, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// When copying a directory tree with `--preserve`, destination-side directory metadata
/// errors (e.g., `fchownat` failing with "Operation not permitted" on root-owned dirs)
/// should be logged and the copy should continue, not abort the entire operation.
#[test]
#[ignore = "requires passwordless sudo"]
fn test_remote_sudo_destination_dir_metadata_error_continues() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // create a directory tree where the root of the copied tree is owned by root.
    // when destination tries to chown the directory with --preserve, it will fail
    // with "Operation not permitted" since we are not running as root.
    let src_subdir = src_dir.path().join("root_dir");
    let status = std::process::Command::new("sudo")
        .args([
            "-n",
            "bash",
            "-c",
            &format!(
                "mkdir -p '{dir}' && \
                 echo 'file1' > '{dir}/file1.txt' && \
                 echo 'file2' > '{dir}/file2.txt' && \
                 mkdir -p '{dir}/child' && \
                 echo 'child_file' > '{dir}/child/child_file.txt' && \
                 chown -R root:root '{dir}'",
                dir = src_subdir.display()
            ),
        ])
        .status()
        .expect("Failed to run sudo");
    if !status.success() {
        eprintln!("Skipping test: passwordless sudo not available");
        return;
    }
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_subdir = dst_dir.path().join("root_dir");
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // run with --preserve to trigger chown attempts on directories and files
    let output = run_rcp_with_args(&["--preserve", "--summary", &src_remote, &dst_remote]);
    print_command_output(&output);
    // cleanup root-owned source files
    let _ = std::process::Command::new("sudo")
        .args(["-n", "rm", "-rf", &src_subdir.to_string_lossy()])
        .status();
    // verify all file DATA was transferred despite directory metadata errors
    assert!(
        dst_subdir.join("file1.txt").exists(),
        "file1.txt should be copied"
    );
    assert_eq!(get_file_content(&dst_subdir.join("file1.txt")), "file1\n");
    assert!(
        dst_subdir.join("file2.txt").exists(),
        "file2.txt should be copied"
    );
    assert_eq!(get_file_content(&dst_subdir.join("file2.txt")), "file2\n");
    assert!(
        dst_subdir.join("child/child_file.txt").exists(),
        "child/child_file.txt should be copied"
    );
    assert_eq!(
        get_file_content(&dst_subdir.join("child/child_file.txt")),
        "child_file\n"
    );
    // command should fail (due to chown errors) but not hang or timeout
    assert!(
        !output.status.success(),
        "should fail due to chown permission errors on directories"
    );
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        exit_code != 124,
        "should not timeout - copy should continue after directory metadata errors"
    );
    // verify summary shows the files were actually copied
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(
        summary.files_copied, 3,
        "all 3 files should be copied despite directory metadata errors"
    );
    assert_eq!(
        summary.directories_created, 2,
        "both directories should be created (metadata errors happen after creation)"
    );
    eprintln!("✓ Copy continued after destination directory metadata errors");
}

#[test]
fn test_remote_preserve_all_special_bits_on_directories() {
    require_local_ssh();
    let test_cases: &[(u32, &str)] = &[
        (0o2755, "setgid"),
        (0o4755, "setuid"),
        (0o1755, "sticky"),
        (0o7755, "setuid+setgid+sticky"),
    ];
    for &(mode, description) in test_cases {
        let (src_dir, dst_dir) = setup_test_env();
        let src_subdir = src_dir.path().join("dir");
        std::fs::create_dir(&src_subdir).unwrap();
        std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(mode)).unwrap();
        create_test_file(&src_subdir.join("file.txt"), "content", 0o644);
        let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
        let dst_subdir = dst_dir.path().join("dir");
        let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
        run_rcp_and_expect_success(&["--preserve-settings=all", &src_remote, &dst_remote]);
        assert_eq!(
            get_file_mode(&dst_subdir),
            mode,
            "directory special bits not preserved for {description} ({mode:o})"
        );
    }
}

#[test]
fn test_remote_default_strips_special_bits() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // directory with setgid
    let src_subdir = src_dir.path().join("dir");
    std::fs::create_dir(&src_subdir).unwrap();
    std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o2755)).unwrap();
    // file with setuid
    create_test_file(&src_subdir.join("file.txt"), "content", 0o4755);
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_subdir = dst_dir.path().join("dir");
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    run_rcp_and_expect_success(&[&src_remote, &dst_remote]);
    assert_eq!(
        get_file_mode(&dst_subdir),
        0o755,
        "directory special bits should be stripped by default"
    );
    assert_eq!(
        get_file_mode(&dst_subdir.join("file.txt")),
        0o755,
        "file special bits should be stripped by default"
    );
}

#[test]
fn test_remote_preserve_settings_dir_7777() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_subdir = src_dir.path().join("parent");
    std::fs::create_dir(&src_subdir).unwrap();
    std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o2755)).unwrap();
    let src_child = src_subdir.join("child");
    std::fs::create_dir(&src_child).unwrap();
    std::fs::set_permissions(&src_child, std::fs::Permissions::from_mode(0o1755)).unwrap();
    create_test_file(&src_child.join("file.txt"), "content", 0o4755);
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_subdir = dst_dir.path().join("parent");
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    run_rcp_and_expect_success(&[
        "--preserve-settings",
        "d:gid,time,7777",
        &src_remote,
        &dst_remote,
    ]);
    assert_eq!(
        get_file_mode(&dst_subdir),
        0o2755,
        "parent dir setgid not preserved"
    );
    assert_eq!(
        get_file_mode(&dst_subdir.join("child")),
        0o1755,
        "child dir sticky not preserved"
    );
    assert_eq!(
        get_file_mode(&dst_subdir.join("child").join("file.txt")),
        0o755,
        "file setuid should be stripped"
    );
}

#[test]
fn test_remote_preserve_all_special_bits_on_files() {
    require_local_ssh();
    // (mode, contents). the empty row pins that special bits survive a ZERO-LENGTH file: its
    // transfer skips the data step entirely, so the closing chmod is the only thing standing
    // between the owner-only create mode and the source mode.
    let test_cases: &[(u32, &str)] = &[
        (0o4755, "setuid"),
        (0o2755, "setgid"),
        (0o1755, "sticky"),
        (0o6755, "setuid+setgid"),
        (0o7755, "setuid+setgid+sticky"),
        (0o4755, ""),
    ];
    for &(mode, contents) in test_cases {
        let label = if contents.is_empty() {
            "zero-length setuid"
        } else {
            contents
        };
        let (src_dir, dst_dir) = setup_test_env();
        let src_file = src_dir.path().join(format!("test_{mode:o}.txt"));
        let dst_file = dst_dir.path().join(format!("test_{mode:o}.txt"));
        create_test_file(&src_file, contents, mode);
        let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
        let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
        run_rcp_and_expect_success(&["--preserve-settings=all", &src_remote, &dst_remote]);
        assert_eq!(get_file_content(&dst_file), contents);
        assert_eq!(
            get_file_mode(&dst_file),
            mode,
            "file special bits not preserved for {label} ({mode:o})"
        );
    }
}

#[test]
fn test_remote_preserve_settings_file_7777() {
    require_local_ssh();
    // (mode, contents), with the same zero-length row as `test_remote_preserve_all_special_bits_on_files`
    let test_cases: &[(u32, &str)] = &[
        (0o4755, "setuid"),
        (0o2755, "setgid"),
        (0o1755, "sticky"),
        (0o6755, "setuid+setgid"),
        (0o7755, "setuid+setgid+sticky"),
        (0o4755, ""),
    ];
    for &(mode, contents) in test_cases {
        let label = if contents.is_empty() {
            "zero-length setuid"
        } else {
            contents
        };
        let (src_dir, dst_dir) = setup_test_env();
        let src_file = src_dir.path().join(format!("test_{mode:o}.txt"));
        let dst_file = dst_dir.path().join(format!("test_{mode:o}.txt"));
        create_test_file(&src_file, contents, mode);
        let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
        let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
        run_rcp_and_expect_success(&["--preserve-settings", "f:7777", &src_remote, &dst_remote]);
        assert_eq!(get_file_content(&dst_file), contents);
        assert_eq!(
            get_file_mode(&dst_file),
            mode,
            "file special bits not preserved for {label} ({mode:o})"
        );
    }
}

/// The remote destination must keep a file owner-only until its contents have arrived, exactly as
/// the local engine does — `rcpd` creates destination files through the same `Dir::create_file`.
///
/// `--ops-throttle=1` (which the master forwards to both rcpd instances) limits each of them to one
/// metadata syscall per second, stretching the create → `fchmod` window into a full second. That is
/// what makes the sampling deterministic instead of a race against loopback TCP.
#[test]
fn test_remote_copy_creates_file_owner_only_until_written() {
    require_local_ssh();
    /// The mode a destination file is created at (`common::safedir::DST_FILE_CREATE_MODE`).
    const CREATE_MODE: u32 = 0o600;
    /// The source mode, and so the mode the destination must end at.
    const SRC_MODE: u32 = 0o4755;
    let (src_dir, dst_dir) = setup_test_env();
    // the reachable case: a world-searchable destination directory, i.e. any repeat or incremental
    // copy into an existing tree
    std::fs::set_permissions(dst_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let src_file = src_dir.path().join("setuid.bin");
    let dst_file = dst_dir.path().join("setuid.bin");
    create_test_file(&src_file, "payload", SRC_MODE);
    let full_size = std::fs::metadata(&src_file).unwrap().len();
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    // capture stdout to a real file rather than discarding it: rcp prints its error chain with
    // `println!("{err:?}")` (common/src/lib.rs), i.e. to stdout, so throwing it away would make any
    // failure here undiagnosable. A file, not `Stdio::piped()` - nothing drains a pipe while
    // `sample_while_running` polls, so a full kernel buffer would deadlock the child.
    let stdout_file = tempfile::NamedTempFile::new().expect("Failed to create stdout capture file");
    let child = std::process::Command::new("timeout")
        // the throttled copy takes ~11s; the wrapper only has to catch a hang
        .args(["120", rcp_path.to_str().unwrap()])
        .args([
            "--force-remote",
            "--preserve-settings=all",
            "--ops-throttle=1",
            &format!("localhost:{}", src_file.to_str().unwrap()),
            &format!("localhost:{}", dst_file.to_str().unwrap()),
        ])
        .stdout(
            stdout_file
                .reopen()
                .expect("Failed to reopen stdout capture file"),
        )
        .spawn()
        .expect("Failed to execute rcp command");
    let (status, samples) = sample_while_running(child, &dst_file);
    let observed = describe_samples(&samples);
    let rcp_output = std::fs::read_to_string(stdout_file.path()).unwrap_or_default();
    assert!(
        status.success(),
        "remote copy failed ({}); rcp output:\n{rcp_output}\nsamples: {observed}",
        status
            .code()
            .map_or_else(|| "signal".to_string(), interpret_exit_code)
    );
    // the destination is only ever seen owner-only while it is being filled in, or complete at the
    // source mode — never in between
    for &(mode, size) in &samples {
        assert!(
            mode == CREATE_MODE || mode == SRC_MODE,
            "destination observed at {mode:o} (size {size}); expected {CREATE_MODE:o} while being \
             written or {SRC_MODE:o} once complete. samples: {observed}"
        );
        assert!(
            size == full_size || mode & 0o7077 == 0,
            "an incomplete destination (size {size} of {full_size}) was observed at {mode:o}, \
             readable outside the copier. samples: {observed}"
        );
    }
    // fail loudly rather than vacuously: without the owner-only create there is no such sample
    assert!(
        samples.iter().any(|&(mode, _)| mode == CREATE_MODE),
        "the destination was never observed owner-only, so rcpd published it at its final mode \
         before writing its contents. samples: {observed}"
    );
    assert_eq!(get_file_mode(&dst_file), SRC_MODE);
    assert_eq!(get_file_content(&dst_file), "payload");
}

/// Test that --skip-specials correctly skips sockets in remote copy and reports the count
#[test]
fn test_remote_skip_specials() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_path = src_dir.path().join("src");
    let dst_path = dst_dir.path().join("dst");
    std::fs::create_dir(&src_path).unwrap();
    create_test_file(&src_path.join("file.txt"), "hello", 0o644);
    // create a unix socket inside the source directory
    let _listener = std::os::unix::net::UnixListener::bind(src_path.join("test.sock")).unwrap();
    let src_remote = format!("localhost:{}", src_path.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_path.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--skip-specials", "--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("should parse summary");
    assert_eq!(summary.files_copied, 1, "should copy the regular file");
    assert_eq!(
        summary.specials_skipped, 1,
        "should report 1 skipped special in summary"
    );
    assert!(dst_path.join("file.txt").exists());
    assert!(!dst_path.join("test.sock").exists());
}

/// Verifies that `--auto-meta-throttle` and its tuning flags propagate
/// from the rcp master to the rcpd processes over the remote protocol.
///
/// Before launching each rcpd, the master logs the exact argument vector it passes at
/// `Will run remote rcpd: path=... role=<source|destination> args=[...]`. The
/// `--auto-meta-*` flags MUST appear there for the role's rcpd to apply them. This stable
/// diagnostic is the deterministic propagation point; it does not depend on an SSH backend's
/// `Debug` representation or on daemon logs being forwarded after startup.
#[test]
fn test_remote_auto_meta_throttle_flags_propagate_to_rcpd() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_path = src_dir.path().join("src");
    std::fs::create_dir(&src_path).unwrap();
    create_test_file(&src_path.join("a.txt"), "a", 0o644);
    create_test_file(&src_path.join("b.txt"), "b", 0o644);
    let src_remote = format!("localhost:{}", src_path.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_dir.path().join("dst").to_str().unwrap(),);
    let output = run_rcp_and_expect_success(&[
        "--auto-meta-throttle",
        "--auto-meta-initial-cwnd=7",
        "--auto-meta-max-cwnd=321",
        "--auto-meta-alpha=1.13",
        "--auto-meta-beta=1.77",
        &src_remote,
        &dst_remote,
    ]);
    // tracing writes through the progress-bar-aware stdout writer in this project
    let log_output = String::from_utf8_lossy(&output.stdout);
    let expected_flags = [
        "--auto-meta-throttle",
        "--auto-meta-initial-cwnd=7",
        "--auto-meta-max-cwnd=321",
        "--auto-meta-alpha=1.13",
        "--auto-meta-beta=1.77",
    ];
    let rcpd_command_line_for_role = |role: &str| -> Option<String> {
        let role_field = format!("role={role}");
        log_output
            .lines()
            .find(|line| line.contains("Will run remote rcpd") && line.contains(&role_field))
            .map(str::to_owned)
    };
    for role in ["source", "destination"] {
        let line = rcpd_command_line_for_role(role).unwrap_or_else(|| {
            panic!(
                "no stable remote-rcpd argv log for role={role}. \
                 stdout length = {} bytes.",
                log_output.len()
            )
        });
        for flag in &expected_flags {
            assert!(
                line.contains(flag),
                "rcpd-{role} command line missing {flag}. Full line:\n{line}",
            );
        }
    }
}

// ── TOCTOU source hardening (Phase 5a) ──────────────────────────────────────────

/// The remote SOURCE must not dereference a nested symlink to read file DATA on
/// the default (non-`-L`) path. A directory child that is a symlink to an
/// out-of-tree sentinel regular file must be copied AS a symlink — its target's
/// bytes must never be streamed into a destination regular file.
///
/// This exercises the source-side discovery classification: it opens the child
/// `O_NOFOLLOW` and `fstat`s it, so a symlink is a symlink (never a File), and the
/// sentinel is never opened for data. (With `-L` the symlink would be followed —
/// that path is intentionally not hardened and is covered by the dereference
/// tests above.)
#[test]
fn test_remote_source_nested_symlink_not_dereferenced_for_data() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // out-of-tree sentinel: content that must never reach the destination as data.
    let sentinel = src_dir.path().join("sentinel_secret.txt");
    create_test_file(&sentinel, "TOP-SECRET-SENTINEL", 0o600);
    // source subtree we actually copy
    let src_subdir = src_dir.path().join("tree");
    std::fs::create_dir(&src_subdir).unwrap();
    let real_file = src_subdir.join("real.txt");
    create_test_file(&real_file, "real data", 0o644);
    // a child symlink pointing at the out-of-tree sentinel
    let link = src_subdir.join("link_to_secret");
    std::os::unix::fs::symlink(&sentinel, &link).unwrap();
    let dst_subdir = dst_dir.path().join("tree");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    run_rcp_and_expect_success(&[&src_remote, &dst_remote]);
    // the real file copies through unchanged
    let dst_real = dst_subdir.join("real.txt");
    assert_eq!(get_file_content(&dst_real), "real data");
    // the symlink is preserved as a symlink; the sentinel's bytes are NOT copied
    // into a regular destination file.
    let dst_link = dst_subdir.join("link_to_secret");
    assert!(
        dst_link.is_symlink(),
        "nested symlink must be copied as a symlink, not dereferenced for data"
    );
    assert!(
        !std::fs::symlink_metadata(&dst_link).unwrap().is_file(),
        "sentinel content must never be written as a destination regular file"
    );
}

/// Best-effort race test: while a remote copy runs, rapidly swap a source file
/// between a real regular file and a symlink-to-sentinel. The source must never
/// transfer the sentinel's content — either it reads the real file, or the
/// fd-relative `O_NOFOLLOW` open fails closed and the entry is skipped.
///
/// This remains a best-effort stress race. The deterministic data-open replacement case is
/// covered by `remote_admitted_file_mutation_uses_opened_object_and_preserves_siblings`.
#[test]
fn test_remote_source_file_swap_never_transfers_sentinel() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let sentinel = src_dir.path().join("sentinel_secret.txt");
    create_test_file(&sentinel, "TOP-SECRET-SENTINEL", 0o600);
    let src_subdir = src_dir.path().join("tree");
    std::fs::create_dir(&src_subdir).unwrap();
    // a bed of real files so the copy has work to do (widens the race window).
    for i in 0..200 {
        create_test_file(&src_subdir.join(format!("f{i}.txt")), "real data", 0o644);
    }
    // the entry we race: starts as a real file.
    let target = src_subdir.join("racer.txt");
    create_test_file(&target, "real racer", 0o644);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_thread = stop.clone();
    let target_thread = target.clone();
    let sentinel_thread = sentinel.clone();
    let swapper = std::thread::spawn(move || {
        let mut as_link = false;
        while !stop_thread.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = std::fs::remove_file(&target_thread);
            if as_link {
                let _ = std::os::unix::fs::symlink(&sentinel_thread, &target_thread);
            } else {
                let _ = std::fs::write(&target_thread, "real racer");
            }
            as_link = !as_link;
        }
        // leave it as a real file for a clean final state
        let _ = std::fs::remove_file(&target_thread);
        let _ = std::fs::write(&target_thread, "real racer");
    });
    let dst_subdir = dst_dir.path().join("tree");
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    // run several iterations to widen the chance of hitting the swap window.
    for _ in 0..3 {
        let _ = std::fs::remove_dir_all(&dst_subdir);
        let _ = run_rcp_with_args(&[&src_remote, &dst_remote]);
        // whatever landed at the destination for `racer.txt`, it must not be the
        // sentinel content. it may be the real content, a symlink, or absent.
        let dst_racer = dst_subdir.join("racer.txt");
        if let Ok(meta) = std::fs::symlink_metadata(&dst_racer)
            && meta.is_file()
            && let Ok(content) = std::fs::read_to_string(&dst_racer)
        {
            assert_ne!(
                content, "TOP-SECRET-SENTINEL",
                "source transferred out-of-tree sentinel content via a symlink swap"
            );
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    swapper.join().unwrap();
}

/// Recursively check whether any regular file under `root` contains `needle`.
fn tree_contains_content(root: &std::path::Path, needle: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(root) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.is_dir() && tree_contains_content(&path, needle) {
            return true;
        } else if meta.is_file()
            && let Ok(content) = std::fs::read_to_string(&path)
            && content.contains(needle)
        {
            return true;
        }
    }
    false
}

/// Best-effort race: while a remote copy runs, swap an intermediate SOURCE directory between a real
/// (empty) directory and a symlink to an out-of-tree sentinel directory. The source's fd-relative
/// `O_NOFOLLOW` descent must never follow the symlink to read the sentinel content into the
/// destination. A leaked sentinel file is a true-positive failure; a clean run is the expected
/// outcome. Mirrors `test_remote_source_file_swap_never_transfers_sentinel` for the
/// intermediate-directory case.
#[test]
fn test_remote_source_intermediate_dir_swap_never_escapes() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let sentinel_dir = src_dir.path().join("sentinel_outside");
    std::fs::create_dir(&sentinel_dir).unwrap();
    create_test_file(
        &sentinel_dir.join("secret.txt"),
        "TOP-SECRET-SENTINEL",
        0o600,
    );
    let tree = src_dir.path().join("tree");
    std::fs::create_dir(&tree).unwrap();
    // a bed of real files so the copy has work to do (widens the race window).
    for i in 0..200 {
        create_test_file(&tree.join(format!("f{i}.txt")), "real data", 0o644);
    }
    // the intermediate dir we race between a real (empty) dir and a symlink to the sentinel dir.
    let mid = tree.join("mid");
    std::fs::create_dir(&mid).unwrap();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_thread = stop.clone();
    let mid_thread = mid.clone();
    let sentinel_thread = sentinel_dir.clone();
    let swapper = std::thread::spawn(move || {
        let mut as_link = false;
        while !stop_thread.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = std::fs::remove_dir_all(&mid_thread);
            let _ = std::fs::remove_file(&mid_thread);
            if as_link {
                let _ = std::os::unix::fs::symlink(&sentinel_thread, &mid_thread);
            } else {
                let _ = std::fs::create_dir(&mid_thread);
            }
            as_link = !as_link;
        }
        // leave a clean real dir as the final state.
        let _ = std::fs::remove_dir_all(&mid_thread);
        let _ = std::fs::remove_file(&mid_thread);
        let _ = std::fs::create_dir(&mid_thread);
    });
    let dst_tree = dst_dir.path().join("tree");
    let src_remote = format!("localhost:{}", tree.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_tree.to_str().unwrap());
    for _ in 0..3 {
        let _ = std::fs::remove_dir_all(&dst_tree);
        let _ = run_rcp_with_args(&[&src_remote, &dst_remote]);
        assert!(
            !tree_contains_content(&dst_tree, "TOP-SECRET-SENTINEL"),
            "source followed a swapped intermediate symlink and copied the out-of-tree sentinel"
        );
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    swapper.join().unwrap();
}

/// Best-effort race: while a remote copy WRITES into the destination, swap an intermediate
/// DESTINATION directory between a real directory and a symlink pointing OUTSIDE the destination
/// tree. The destination's fd-relative writes (pinned parent fd and `O_NOFOLLOW`) must never be
/// redirected through the symlink to create or write outside the tree. Any file leaking into the
/// out-of-tree "escape" directory is a true-positive failure; a clean run is expected.
#[test]
fn test_remote_dest_intermediate_dir_swap_never_escapes() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = src_dir.path().join("tree");
    let src_mid = src_tree.join("mid");
    std::fs::create_dir_all(&src_mid).unwrap();
    for i in 0..200 {
        create_test_file(&src_mid.join(format!("c{i}.txt")), "real data", 0o644);
    }
    // out-of-tree escape directory: must stay empty (nothing redirected here via a symlink swap).
    let escape = dst_dir.path().join("escape_outside");
    std::fs::create_dir(&escape).unwrap();
    let dst_tree = dst_dir.path().join("tree");
    let dst_mid = dst_tree.join("mid");
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_thread = stop.clone();
    let dst_mid_thread = dst_mid.clone();
    let escape_thread = escape.clone();
    let swapper = std::thread::spawn(move || {
        // repeatedly try to replace the destination's intermediate dir with a symlink to escape.
        while !stop_thread.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = std::fs::remove_dir_all(&dst_mid_thread);
            let _ = std::fs::remove_file(&dst_mid_thread);
            let _ = std::os::unix::fs::symlink(&escape_thread, &dst_mid_thread);
            let _ = std::fs::remove_file(&dst_mid_thread);
        }
    });
    let src_remote = format!("localhost:{}", src_tree.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_tree.to_str().unwrap());
    for _ in 0..3 {
        let _ = std::fs::remove_dir_all(&dst_tree);
        // the copy may error as the swapper fights it for the path — that is fine; the invariant is
        // that nothing is ever written OUTSIDE the destination tree.
        let _ = run_rcp_with_args(&[&src_remote, &dst_remote]);
        let escaped: Vec<std::path::PathBuf> = std::fs::read_dir(&escape)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .collect();
        assert!(
            escaped.is_empty(),
            "destination followed a swapped intermediate symlink and wrote outside the tree: {escaped:?}"
        );
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    swapper.join().unwrap();
}

/// The remote source must not dereference a ROOT symlink for data: a root operand that is a symlink
/// to an out-of-tree sentinel is copied as a symlink (read fd-relative via its trusted parent),
/// never followed to write the sentinel's bytes as a destination regular file. Deterministic
/// companion to `test_remote_source_root_file_swap_never_transfers_sentinel`, exercising the
/// hardened root-symlink read.
#[test]
fn test_remote_source_root_symlink_not_dereferenced_for_data() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let sentinel = src_dir.path().join("sentinel_secret.txt");
    create_test_file(&sentinel, "TOP-SECRET-SENTINEL", 0o600);
    // the ROOT operand itself is a symlink to the out-of-tree sentinel.
    let root_link = src_dir.path().join("root_link");
    std::os::unix::fs::symlink(&sentinel, &root_link).unwrap();
    let dst = dst_dir.path().join("out");
    let src_remote = format!("localhost:{}", root_link.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst.to_str().unwrap());
    run_rcp_and_expect_success(&[&src_remote, &dst_remote]);
    // copied as a symlink, NOT dereferenced into a destination regular file of sentinel bytes.
    assert!(
        dst.is_symlink(),
        "root symlink must be copied as a symlink, not dereferenced for data"
    );
    assert!(
        !std::fs::symlink_metadata(&dst).unwrap().is_file(),
        "sentinel content must never be written as a destination regular file"
    );
}

/// Rejected directories must release Begin credits and settle parent entries. A rejected
/// subtree larger than the pending capacity must not stop sibling discovery or file sends.
#[test]
fn test_remote_source_no_ack_subtree_over_budget_does_not_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // source: root/ with a real file, a copyable sibling subtree, and a `blocked`
    // subtree holding more directories than the single Begin credit.
    let src_root = src_dir.path().join("root");
    std::fs::create_dir(&src_root).unwrap();
    create_test_file(&src_root.join("top.txt"), "top content", 0o644);
    // sibling subtree that must copy fine
    let src_ok = src_root.join("ok");
    std::fs::create_dir(&src_ok).unwrap();
    create_test_file(&src_ok.join("ok.txt"), "ok content", 0o644);
    // rejected descendants may already have been submitted when the parent reply arrives
    let src_blocked = src_root.join("blocked");
    std::fs::create_dir(&src_blocked).unwrap();
    let blocked_dir_count = 16usize;
    for i in 0..blocked_dir_count {
        let d = src_blocked.join(format!("d{i}"));
        std::fs::create_dir(&d).unwrap();
        create_test_file(&d.join("inner.txt"), "inner", 0o644);
    }
    // destination: pre-create root/ as a real dir, but block root/blocked with a
    // pre-existing REGULAR FILE so the destination cannot create it (no --overwrite).
    let dst_root = dst_dir.path().join("root");
    std::fs::create_dir(&dst_root).unwrap();
    create_test_file(&dst_root.join("blocked"), "i am a file, not a dir", 0o644);
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    let output = run_rcp_with_args(&[
        "--max-connections=1",
        "--pending-writes-multiplier=1",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(
        !output.status.success(),
        "rejected directories must report failure"
    );
    // the copy must COMPLETE (the harness already failed us on a 90s timeout/hang).
    // it exits non-zero because the blocked dir is a real error, but it must not hang.
    // the sibling subtree and the top-level file must have copied.
    assert_eq!(get_file_content(&dst_root.join("top.txt")), "top content");
    assert_eq!(get_file_content(&dst_root.join("ok/ok.txt")), "ok content");
    // the blocked path stays the pre-existing regular file (subtree skipped).
    assert!(
        dst_root.join("blocked").is_file(),
        "blocked destination path should remain the pre-existing file"
    );
    assert!(
        !dst_root.join("blocked").join("d0").exists(),
        "blocked subtree must not have been copied"
    );
}

/// Inline descent at E=P=1 must not retain a classification permit needed by its descendant.
/// Several cursor batches, deep branches, and sibling files exercise independent progress.
#[tokio::test]
async fn remote_capacity_one_deep_tree_preserves_files_and_directory_metadata() {
    require_local_ssh();
    static PROGRESS: std::sync::LazyLock<common::progress::Progress> =
        std::sync::LazyLock::new(common::progress::Progress::new);
    let fixture = tempfile::tempdir().unwrap();
    let src = fixture.path().join("source");
    std::fs::create_dir(&src).unwrap();
    let mut files = Vec::new();
    let mut directories = Vec::new();
    for branch in 0..3 {
        let mut relative = std::path::PathBuf::from(format!("branch{branch}"));
        for _ in 0..8 {
            std::fs::create_dir(src.join(&relative)).unwrap();
            std::fs::set_permissions(src.join(&relative), std::fs::Permissions::from_mode(0o750))
                .unwrap();
            let file = relative.join("payload");
            common::filegen::write_file(&PROGRESS, src.join(&file), 256, 256, 0)
                .await
                .unwrap();
            files.push(file);
            directories.push(relative.clone());
            relative.push("child");
        }
    }
    for i in 0..70 {
        let file = std::path::PathBuf::from(format!("root-file-{i}"));
        common::filegen::write_file(&PROGRESS, src.join(&file), 128, 128, 0)
            .await
            .unwrap();
        files.push(file);
    }
    for dereference in [false, true] {
        let dst = fixture.path().join(if dereference {
            "dereferenced"
        } else {
            "hardened"
        });
        let src_remote = format!("localhost:{}", src.display());
        let dst_remote = format!("localhost:{}", dst.display());
        let mut args = vec![
            "--max-connections=1",
            "--pending-writes-multiplier=1",
            src_remote.as_str(),
            dst_remote.as_str(),
        ];
        if dereference {
            args.insert(0, "-L");
        }
        run_rcp_and_expect_success(&args);
        for file in &files {
            assert_eq!(
                std::fs::read(src.join(file)).unwrap(),
                std::fs::read(dst.join(file)).unwrap(),
                "{file:?}"
            );
        }
        for directory in &directories {
            assert_eq!(
                get_file_mode(&dst.join(directory)) & 0o777,
                0o750,
                "{directory:?}"
            );
        }
    }
}

/// Receiver directory lifetimes stay bounded while tiny payloads let the source run ahead.
#[test]
fn remote_many_small_directories_fit_the_destination_descriptor_limit() {
    require_local_ssh();
    let fixture = tempfile::tempdir().unwrap();
    let generated = std::process::Command::new(assert_cmd::cargo::cargo_bin("filegen"))
        .args([
            fixture.path().to_str().unwrap(),
            "60,100",
            "10",
            "8",
            "--leaf-files",
            "--bufsize=8",
        ])
        .output()
        .unwrap();
    assert!(
        generated.status.success(),
        "{}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let src = fixture.path().join("filegen");
    let wrapper = fixture.path().join("limited-rcpd");
    let daemon = shell_quote_for_test(&assert_cmd::cargo::cargo_bin("rcpd"));
    std::fs::write(
        &wrapper,
        format!("#!/bin/sh\nulimit -n 1024 || exit 1\nexec {daemon} \"$@\"\n"),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let daemon_arg = format!("--rcpd-path={}", wrapper.display());
    let src_remote = format!("localhost:{}", src.display());
    for preserve in [false, true] {
        let dst = fixture
            .path()
            .join(if preserve { "preserved" } else { "plain" });
        let dst_remote = format!("localhost:{}", dst.display());
        let mut args = vec![
            daemon_arg.as_str(),
            "--max-files-in-flight=8",
            src_remote.as_str(),
            dst_remote.as_str(),
        ];
        if preserve {
            args.insert(0, "--preserve");
        }
        run_rcp_and_expect_success(&args);
        let mut pending = vec![src.clone()];
        let mut files = 0;
        while let Some(directory) = pending.pop() {
            let relative = directory.strip_prefix(&src).unwrap();
            assert!(
                dst.join(relative).is_dir(),
                "missing directory {relative:?}"
            );
            for entry in std::fs::read_dir(directory).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if entry.file_type().unwrap().is_dir() {
                    pending.push(path);
                } else {
                    let copied = dst.join(path.strip_prefix(&src).unwrap());
                    assert_eq!(
                        std::fs::read(&path).unwrap(),
                        std::fs::read(&copied).unwrap(),
                        "{copied:?}"
                    );
                    files += 1;
                }
            }
        }
        assert_eq!(files, 60_000);
    }
}

/// Parallel directory traversal fits a low hard descriptor limit even with large pending capacity.
#[tokio::test]
async fn remote_parallel_directories_preserve_every_subtree_under_a_low_hard_limit() {
    require_local_ssh();
    static PROGRESS: std::sync::LazyLock<common::progress::Progress> =
        std::sync::LazyLock::new(common::progress::Progress::new);
    for (branches, depth, file_limit, streams, multiplier, descriptor_limit) in [
        (64, 13, "64", "64", "16", 1024),
        (20, 80, "20", "20", "4", 1024),
        (1, 60, "200", "100", "4", 1024),
        (20, 30, "200", "100", "4", 1024),
        (4, 6, "20", "20", "4", 160),
        (4, 6, "20", "20", "4", 128),
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let src = fixture.path().join("source");
        std::fs::create_dir(&src).unwrap();
        let mut directories = Vec::new();
        let mut files = Vec::new();
        for branch in 0..branches {
            let mut relative = std::path::PathBuf::from(format!("branch-{branch}"));
            for _ in 0..depth {
                std::fs::create_dir(src.join(&relative)).unwrap();
                directories.push(relative.clone());
                relative.push("child");
            }
            relative.pop();
            let file = relative.join("payload");
            common::filegen::write_file(&PROGRESS, src.join(&file), 1024, 1024, 0)
                .await
                .unwrap();
            files.push(file);
        }
        let wrapper = fixture.path().join("limited-rcpd");
        let daemon = shell_quote_for_test(&assert_cmd::cargo::cargo_bin("rcpd"));
        std::fs::write(
            &wrapper,
            format!("#!/bin/sh\nulimit -n {descriptor_limit} || exit 1\nexec {daemon} \"$@\"\n"),
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let daemon_arg = format!("--rcpd-path={}", wrapper.display());
        let src_remote = format!("localhost:{}", src.display());
        let files_arg = format!("--max-files-in-flight={file_limit}");
        let streams_arg = format!("--max-connections={streams}");
        let pending_arg = format!("--pending-writes-multiplier={multiplier}");
        for dereference in [false, true] {
            let dst = fixture.path().join(if dereference {
                "dereferenced"
            } else {
                "hardened"
            });
            let dst_remote = format!("localhost:{}", dst.display());
            let mut args = vec![
                daemon_arg.as_str(),
                files_arg.as_str(),
                streams_arg.as_str(),
                pending_arg.as_str(),
                src_remote.as_str(),
                dst_remote.as_str(),
            ];
            if dereference {
                args.insert(0, "-L");
            }
            run_rcp_and_expect_success(&args);
            assert!(dst.is_dir());
            for directory in &directories {
                assert!(dst.join(directory).is_dir(), "missing {directory:?}");
            }
            for file in &files {
                assert_eq!(
                    std::fs::read(src.join(file)).unwrap(),
                    std::fs::read(dst.join(file)).unwrap(),
                    "{file:?}"
                );
            }
        }
    }
}

#[test]
fn remote_low_descriptor_limit_supports_single_files_and_high_concurrency_previews() {
    require_local_ssh();
    let fixture = tempfile::tempdir().unwrap();
    let src = fixture.path().join("source");
    create_test_file(&src, "low-limit payload", 0o600);
    let wrapper = fixture.path().join("limited-rcpd");
    let daemon = shell_quote_for_test(&assert_cmd::cargo::cargo_bin("rcpd"));
    std::fs::write(
        &wrapper,
        format!("#!/bin/sh\nulimit -n 128 || exit 1\nexec {daemon} \"$@\"\n"),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    for preview in [false, true] {
        let destination = fixture
            .path()
            .join(if preview { "preview" } else { "destination" });
        let daemon_arg = format!("--rcpd-path={}", wrapper.display());
        let source_arg = format!("localhost:{}", src.display());
        let destination_arg = format!("localhost:{}", destination.display());
        let mut args = vec![
            daemon_arg.as_str(),
            if preview {
                "--max-files-in-flight=200"
            } else {
                "--max-files-in-flight=20"
            },
            source_arg.as_str(),
            destination_arg.as_str(),
        ];
        if preview {
            args.insert(0, "--dry-run=brief");
        }
        run_rcp_and_expect_success(&args);
        if preview {
            assert!(!destination.exists());
        } else {
            assert_eq!(std::fs::read(destination).unwrap(), b"low-limit payload");
        }
    }
}

#[test]
fn remote_insufficient_socket_headroom_is_reported_during_startup() {
    require_local_ssh();
    let fixture = tempfile::tempdir().unwrap();
    let src = fixture.path().join("source");
    create_test_file(&src, "payload", 0o600);
    let daemon = shell_quote_for_test(&assert_cmd::cargo::cargo_bin("rcpd"));
    for role in ["source", "destination"] {
        let destination = fixture.path().join(format!("destination-{role}"));
        let wrapper = fixture.path().join(format!("limited-{role}"));
        std::fs::write(&wrapper, format!(
            "#!/bin/sh\nif [ \"$1\" = --role ] && [ \"$2\" = {role} ]; then\n  ulimit -n 64 || exit 1\nfi\nexec {daemon} \"$@\"\n"
        )).unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
        let output = run_rcp_and_expect_failure(&[
            &format!("--rcpd-path={}", wrapper.display()),
            "--max-files-in-flight=200",
            "--remote-copy-conn-timeout-sec=3",
            &format!("localhost:{}", src.display()),
            &format!("localhost:{}", destination.display()),
        ]);
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(combined.contains("soft RLIMIT_NOFILE=64"), "{combined}");
        assert!(combined.contains("ulimit -H -n"), "{combined}");
        assert!(
            !combined.contains("Timed out waiting for destination"),
            "{combined}"
        );
        assert!(!destination.exists());
    }
}

/// A depth failure is reported without opening excess descriptors or abandoning healthy siblings.
#[test]
fn remote_capacity_one_deep_tree_reports_reserved_depth_exhaustion() {
    require_local_ssh();
    let fixture = tempfile::tempdir().unwrap();
    let src = fixture.path().join("source");
    std::fs::create_dir(&src).unwrap();
    let mut deepest = src.clone();
    for _ in 0..128 {
        deepest.push("child");
        std::fs::create_dir(&deepest).unwrap();
    }
    create_test_file(
        &deepest.join("payload"),
        "beyond the descriptor limit",
        0o600,
    );
    let healthy = src.join("healthy");
    std::fs::create_dir(&healthy).unwrap();
    create_test_file(&healthy.join("payload"), "unaffected sibling", 0o600);
    let wrapper = fixture.path().join("limited-rcpd");
    let daemon = shell_quote_for_test(&assert_cmd::cargo::cargo_bin("rcpd"));
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nif [ \"$1\" = --role ] && [ \"$2\" = source ]; then\n  ulimit -n 64 || exit 1\nfi\nexec {daemon} \"$@\"\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let daemon_arg = format!("--rcpd-path={}", wrapper.display());
    let src_remote = format!("localhost:{}", src.display());
    let cause = "reserved directory depth";
    for dereference in [false, true] {
        let dst = fixture.path().join(if dereference {
            "dereferenced"
        } else {
            "hardened"
        });
        let dst_remote = format!("localhost:{}", dst.display());
        let mut args = vec![
            daemon_arg.as_str(),
            "--max-files-in-flight=1",
            "--max-connections=1",
            "--pending-writes-multiplier=1",
            src_remote.as_str(),
            dst_remote.as_str(),
        ];
        if dereference {
            args.insert(0, "-L");
        }
        let output = run_rcp_and_expect_failure(&args);
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            combined
                .lines()
                .any(|line| line.contains("Source: ") && line.contains(cause)),
            "source depth exhaustion must retain its original cause, dereference={dereference}: {combined}"
        );
        assert!(
            !combined.contains("Source: lost admitted child obligation")
                && !combined.contains("Source: file admission closed")
                && !combined.contains("Source: branch admission closed"),
            "teardown must not replace depth exhaustion, dereference={dereference}: {combined}"
        );
        assert!(
            !combined.contains(&std::io::Error::from_raw_os_error(libc::EMFILE).to_string()),
            "depth must be charged before opening: {combined}"
        );
        assert_eq!(
            std::fs::read(dst.join("healthy/payload")).unwrap(),
            b"unaffected sibling",
            "collect-errors must finish a sibling after reserved-depth exhaustion"
        );
        assert!(dst.is_dir(), "the daemon must reach source traversal");
        assert!(
            !dst.join(deepest.strip_prefix(&src).unwrap())
                .join("payload")
                .exists(),
            "the source must stop at its depth budget before reaching the leaf"
        );
    }
}

/// Rejection inside the reserved subtree must preserve unrelated siblings in both traversal modes.
#[test]
fn remote_rejected_directory_inside_reserve_preserves_healthy_siblings() {
    require_local_ssh();
    let resources = common::RemoteResources::for_limits(
        std::num::NonZeroU64::new(96),
        std::num::NonZeroUsize::MIN,
        std::num::NonZeroUsize::MIN,
        std::num::NonZeroUsize::MIN,
    )
    .unwrap();
    let normal = resources.normal_directories.get();
    let reserve = resources.reserved_directories.get();
    assert!(
        reserve >= 4,
        "the fixture needs a rejected reserve parent and descendants"
    );
    // the root consumes a normal lifetime; rejection occurs below the first reserved level.
    let obstruction_level = normal + 1;
    let depth = normal + reserve - 2;
    let fixture = tempfile::tempdir().unwrap();
    let wrapper = fixture.path().join("limited-rcpd");
    let daemon = shell_quote_for_test(&assert_cmd::cargo::cargo_bin("rcpd"));
    std::fs::write(
        &wrapper,
        format!("#!/bin/sh\nulimit -n 96 || exit 1\nexec {daemon} \"$@\"\n"),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let daemon_arg = format!("--rcpd-path={}", wrapper.display());
    for dereference in [false, true] {
        let (source, destination) = setup_test_env();
        let src = source.path().join("root");
        let dst = destination.path().join("root");
        std::fs::create_dir(&src).unwrap();
        std::fs::create_dir(&dst).unwrap();
        let mut relative = std::path::PathBuf::new();
        for level in 0..depth {
            relative.push("nested");
            std::fs::create_dir_all(src.join(&relative)).unwrap();
            if level < obstruction_level {
                std::fs::create_dir_all(dst.join(&relative)).unwrap();
            } else if level == obstruction_level {
                create_test_file(&dst.join(&relative), "retained obstruction", 0o640);
            }
        }
        create_test_file(
            &src.join(&relative).join("hidden"),
            "rejected subtree",
            0o600,
        );
        create_test_file(&src.join("healthy"), "healthy sibling", 0o600);
        let src_remote = format!("localhost:{}", src.display());
        let dst_remote = format!("localhost:{}", dst.display());
        let mut args = vec![
            daemon_arg.as_str(),
            "--ignore-existing",
            "--summary",
            "--max-files-in-flight=1",
            "--max-connections=1",
            "--pending-writes-multiplier=1",
            src_remote.as_str(),
            dst_remote.as_str(),
        ];
        if dereference {
            args.insert(0, "-L");
        }
        let output = run_rcp_and_expect_success(&args);
        assert_eq!(parse_summary_from_output(&output).unwrap().files_copied, 1);
        assert_eq!(get_file_content(&dst.join("healthy")), "healthy sibling");
        let obstruction: std::path::PathBuf =
            std::iter::repeat_n("nested", obstruction_level + 1).collect();
        assert_eq!(
            get_file_content(&dst.join(obstruction)),
            "retained obstruction"
        );
    }
}

/// Manifest skips and new-file sends must share a capacity-one pipeline across nested directories.
/// Ignore-existing additionally rejects a subtree while unrelated manifest work keeps progressing.
#[test]
fn remote_capacity_one_manifests_and_rejected_subtree_complete() {
    require_local_ssh();
    for dereference in [false, true] {
        for option in ["--overwrite", "--ignore-existing"] {
            let (src_dir, dst_dir) = setup_test_env();
            let src = src_dir.path().join("root");
            let dst = dst_dir.path().join("root");
            let mut relative = std::path::PathBuf::new();
            for _ in 0..6 {
                relative.push("nested");
                std::fs::create_dir_all(src.join(&relative)).unwrap();
                std::fs::create_dir_all(dst.join(&relative)).unwrap();
                for base in [&src, &dst] {
                    let path = base.join(&relative).join("same.txt");
                    create_test_file(&path, "identical", 0o640);
                    filetime::set_file_mtime(
                        path,
                        filetime::FileTime::from_unix_time(1_700_000_000, 0),
                    )
                    .unwrap();
                }
                create_test_file(&src.join(&relative).join("new.txt"), "new", 0o600);
            }
            if option == "--ignore-existing" {
                std::fs::create_dir_all(src.join("blocked/nested/deep")).unwrap();
                create_test_file(&src.join("blocked/nested/deep/hidden"), "hidden", 0o600);
                create_test_file(&dst.join("blocked"), "retained", 0o644);
            }
            let src_remote = format!("localhost:{}", src.display());
            let dst_remote = format!("localhost:{}", dst.display());
            let mut args = vec![
                option,
                "--summary",
                "--max-connections=1",
                "--pending-writes-multiplier=1",
                src_remote.as_str(),
                dst_remote.as_str(),
            ];
            if dereference {
                args.insert(0, "-L");
            }
            let output = run_rcp_and_expect_success(&args);
            let summary = parse_summary_from_output(&output).unwrap();
            assert_eq!(summary.files_copied, 6);
            assert_eq!(summary.files_unchanged, 6);
            assert_eq!(summary.bytes_copied, 18);
            let combined = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                combined.contains("skipping transfer (manifest)"),
                "source manifest skips must execute: {combined}"
            );
            let mut relative = std::path::PathBuf::new();
            for _ in 0..6 {
                relative.push("nested");
                assert_eq!(
                    get_file_content(&dst.join(&relative).join("same.txt")),
                    "identical"
                );
                assert_eq!(
                    get_file_content(&dst.join(&relative).join("new.txt")),
                    "new"
                );
            }
            if option == "--ignore-existing" {
                assert_eq!(get_file_content(&dst.join("blocked")), "retained");
            }
        }
    }
}

#[test]
fn test_remote_dereference_empty_tree_over_budget_completes() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_root = src_dir.path().join("root");
    std::fs::create_dir(&src_root).unwrap();
    for i in 0..3 {
        std::fs::create_dir(src_root.join(format!("empty{i}"))).unwrap();
    }
    let dst_root = dst_dir.path().join("root");
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    run_rcp_and_expect_success(&[
        "-L",
        "--max-connections=1",
        "--pending-writes-multiplier=1",
        &src_remote,
        &dst_remote,
    ]);
    for i in 0..3 {
        assert!(
            dst_root.join(format!("empty{i}")).is_dir(),
            "empty sibling {i} must be copied"
        );
    }
}

#[test]
fn test_remote_dereference_symlinked_root_and_directory_over_budget_completes() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_root = src_dir.path().join("real_root");
    std::fs::create_dir(&src_root).unwrap();
    for i in 0..2 {
        let sibling = src_root.join(format!("sibling{i}"));
        std::fs::create_dir(&sibling).unwrap();
        create_test_file(&sibling.join("file.txt"), &format!("content {i}"), 0o644);
    }
    let linked_target = src_dir.path().join("linked_target");
    std::fs::create_dir(&linked_target).unwrap();
    create_test_file(&linked_target.join("payload.txt"), "linked content", 0o644);
    std::os::unix::fs::symlink(&linked_target, src_root.join("linked_directory")).unwrap();
    let src_link = src_dir.path().join("root_link");
    std::os::unix::fs::symlink(&src_root, &src_link).unwrap();
    let dst_root = dst_dir.path().join("root");
    let src_remote = format!("localhost:{}", src_link.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    run_rcp_and_expect_success(&[
        "-L",
        "--max-connections=1",
        "--pending-writes-multiplier=1",
        &src_remote,
        &dst_remote,
    ]);
    assert!(
        dst_root.is_dir(),
        "the symlinked root must become a directory"
    );
    assert!(
        !dst_root.is_symlink(),
        "the symlinked root must not remain a symlink"
    );
    let dst_linked = dst_root.join("linked_directory");
    assert!(
        dst_linked.is_dir(),
        "the nested directory symlink must become a directory"
    );
    assert!(
        !dst_linked.is_symlink(),
        "the nested directory symlink must not remain a symlink"
    );
    assert_eq!(
        get_file_content(&dst_linked.join("payload.txt")),
        "linked content"
    );
    for i in 0..2 {
        assert_eq!(
            get_file_content(&dst_root.join(format!("sibling{i}/file.txt"))),
            format!("content {i}")
        );
    }
}

struct RestoreDirectoryModesOnDrop {
    paths: Vec<std::path::PathBuf>,
}

impl Drop for RestoreDirectoryModesOnDrop {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
        }
    }
}

#[test]
fn test_remote_dereference_unreadable_nested_directories_over_budget_continues() {
    require_local_ssh();
    if can_read_unreadable_dir() {
        eprintln!(
            "skipping: running as root, cannot make a directory unreadable to the remote user"
        );
        return;
    }
    let (src_dir, dst_dir) = setup_test_env();
    let src_root = src_dir.path().join("root");
    std::fs::create_dir(&src_root).unwrap();
    let dst_root = dst_dir.path().join("root");
    let mut cleanup_paths = Vec::new();
    for i in 0..3 {
        let unreadable = src_root.join(format!("unreadable{i}"));
        std::fs::create_dir(&unreadable).unwrap();
        create_test_file(&unreadable.join("hidden.txt"), "hidden", 0o644);
        cleanup_paths.push(unreadable.clone());
        cleanup_paths.push(dst_root.join(format!("unreadable{i}")));
    }
    let _restore_modes = RestoreDirectoryModesOnDrop {
        paths: cleanup_paths,
    };
    for i in 0..3 {
        std::fs::set_permissions(
            src_root.join(format!("unreadable{i}")),
            std::fs::Permissions::from_mode(0o000),
        )
        .unwrap();
    }
    let readable = src_root.join("zzz_readable");
    std::fs::create_dir(&readable).unwrap();
    create_test_file(&readable.join("payload.txt"), "readable content", 0o644);
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&[
        "-L",
        "--max-connections=1",
        "--pending-writes-multiplier=1",
        &src_remote,
        &dst_remote,
    ]);
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("Permission denied"),
        "the copy must report the unreadable-directory cause; got:\n{combined}"
    );
    assert!(
        !combined.contains("lost admitted child obligation"),
        "the copy must not report the synthetic credit-close wakeup; got:\n{combined}"
    );
    assert_eq!(
        get_file_content(&dst_root.join("zzz_readable/payload.txt")),
        "readable content"
    );
    for i in 0..3 {
        let dst_unreadable = dst_root.join(format!("unreadable{i}"));
        assert!(
            dst_unreadable.is_dir(),
            "unreadable directory {i} must be materialized as an empty directory"
        );
        std::fs::set_permissions(&dst_unreadable, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            std::fs::read_dir(&dst_unreadable).unwrap().count(),
            0,
            "unreadable directory {i} must be empty"
        );
    }
}

#[test]
fn test_remote_dereference_no_ack_subtree_over_budget_does_not_hang() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_root = src_dir.path().join("root");
    std::fs::create_dir(&src_root).unwrap();
    for i in 0..3 {
        let blocked = src_root.join(format!("blocked{i}"));
        std::fs::create_dir(&blocked).unwrap();
        create_test_file(&blocked.join("file.txt"), "blocked content", 0o644);
    }
    let src_ok = src_root.join("ok");
    std::fs::create_dir(&src_ok).unwrap();
    create_test_file(&src_ok.join("file.txt"), "ok content", 0o644);
    let dst_root = dst_dir.path().join("root");
    std::fs::create_dir(&dst_root).unwrap();
    for i in 0..3 {
        create_test_file(
            &dst_root.join(format!("blocked{i}")),
            "not a directory",
            0o644,
        );
    }
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    let output = run_rcp_with_args(&[
        "-L",
        "--max-connections=1",
        "--pending-writes-multiplier=1",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    assert!(
        !output.status.success(),
        "blocked directories must report errors"
    );
    assert_eq!(
        get_file_content(&dst_root.join("ok/file.txt")),
        "ok content"
    );
    for i in 0..3 {
        assert!(
            dst_root.join(format!("blocked{i}")).is_file(),
            "blocked destination path {i} must remain a file"
        );
    }
}

/// A child classified and admitted as a directory can fail its later data-directory open.
/// Every such failure must settle its parent slot while siblings continue at capacity one.
#[test]
fn test_remote_source_counted_child_open_failure_does_not_hang() {
    require_local_ssh();
    // root bypasses directory permission bits, so a `0o000` dir would still open and
    // the counted-child-open-failure path would not be exercised. Skip under root.
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipping: running as root, cannot make a directory unopenable to self");
        return;
    }
    let (src_dir, dst_dir) = setup_test_env();
    // source: root/ with a top-level file, a sibling subtree that must copy fine,
    // and `blocked/` whose children are all counted but un-openable.
    let src_root = src_dir.path().join("root");
    std::fs::create_dir(&src_root).unwrap();
    create_test_file(&src_root.join("top.txt"), "top content", 0o644);
    let src_ok = src_root.join("ok");
    std::fs::create_dir(&src_ok).unwrap();
    create_test_file(&src_ok.join("ok.txt"), "ok content", 0o644);
    // `blocked/` is itself readable (so the source enumerates and counts its
    // children), but each child directory is mode 0o000 so `open_dir` fails EACCES.
    let src_blocked = src_root.join("blocked");
    std::fs::create_dir(&src_blocked).unwrap();
    let blocked_dir_count = 16usize;
    let mut unopenable: Vec<std::path::PathBuf> = Vec::new();
    for i in 0..blocked_dir_count {
        let d = src_blocked.join(format!("d{i}"));
        std::fs::create_dir(&d).unwrap();
        // put a file inside so, were the dir openable, there'd be content to send —
        // it must never be reached because the dir itself cannot be opened.
        create_test_file(&d.join("inner.txt"), "inner", 0o644);
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o000)).unwrap();
        unopenable.push(d);
    }
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!(
        "localhost:{}",
        dst_dir.path().join("root").to_str().unwrap()
    );
    // default mode (no -L → hardened fd-walk); fail_early off so the copy proceeds
    // past the un-openable children and must still COMPLETE.
    let output = run_rcp_with_args(&[
        "--max-connections=1",
        "--pending-writes-multiplier=1",
        &src_remote,
        &dst_remote,
    ]);
    print_command_output(&output);
    // restore permissions so the TempDir cleanup can remove the 0o000 directories.
    for d in &unopenable {
        let _ = std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o755));
    }
    // the copy must COMPLETE (the harness already failed us on a 90s timeout/hang).
    // it exits non-zero because the un-openable dirs are real errors, but it must not
    // hang. The top-level file and the sibling subtree must have copied.
    let dst_root = dst_dir.path().join("root");
    assert_eq!(get_file_content(&dst_root.join("top.txt")), "top content");
    assert_eq!(get_file_content(&dst_root.join("ok/ok.txt")), "ok content");
    // the un-openable children's contents must NOT have been transferred.
    assert!(
        !dst_root
            .join("blocked")
            .join("d0")
            .join("inner.txt")
            .exists(),
        "contents of an un-openable source directory must not have been copied"
    );
    assert!(
        !output.status.success(),
        "copy should report a non-zero exit because some source dirs could not be opened"
    );
}

/// A source-only syscall tracer replaces an admitted name immediately before its data/directory
/// open. O_PATH classification is left intact, and a marker proves the intended open occurred.
/// This works with static musl daemons and avoids entry order, log delivery, and timing windows.
#[cfg(target_os = "linux")]
fn source_open_mutation_tracer() -> tempfile::TempDir {
    let fixture = tempfile::tempdir().unwrap();
    let source = fixture.path().join("mutate.c");
    std::fs::write(
            &source,
            r#"
#define _GNU_SOURCE
#include <sys/ptrace.h>
#include <linux/ptrace.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fired;
static pid_t owned_child = -1;
static void fail(const char *operation) {
    perror(operation);
    if (owned_child > 0) {
        kill(owned_child, SIGKILL);
        while (waitpid(owned_child, 0, 0) < 0 && errno == EINTR) {}
    }
    exit(190);
}
static void read_name(pid_t tid, unsigned long long address, char name[PATH_MAX]) {
    for (size_t offset = 0; offset < PATH_MAX; offset += sizeof(long)) {
        errno = 0;
        long word = ptrace(PTRACE_PEEKDATA, tid, (void *)(address + offset), 0);
        if (errno) fail("reading intercepted source path");
        memcpy(name + offset, &word, sizeof(word));
        if (memchr(&word, 0, sizeof(word))) return;
    }
    fail("intercepted source path is too long");
}
static void mutate_before_open(pid_t tid, int parent, unsigned long long address, int flags) {
    if (fired || (flags & O_PATH)) return;
    const char *target = getenv("RCP_TEST_MUTATION_TARGET");
    char name[PATH_MAX];
    read_name(tid, address, name);
    char full[PATH_MAX];
    if (name[0] == '/') {
        if (strcmp(name, target)) return;
    } else {
        const char *leaf = strrchr(target, '/');
        if (!leaf || strcmp(name, leaf + 1)) return;
        char proc_path[64], parent_path[PATH_MAX];
        if (parent == AT_FDCWD) snprintf(proc_path, sizeof(proc_path), "/proc/%d/cwd", tid);
        else snprintf(proc_path, sizeof(proc_path), "/proc/%d/fd/%d", tid, parent);
        ssize_t count = readlink(proc_path, parent_path, sizeof(parent_path) - 1);
        if (count < 0) fail("reading source parent");
        parent_path[count] = 0;
        if (snprintf(full, sizeof(full), "%s/%s", parent_path, name) >= sizeof(full)) fail("joining source path");
        if (strcmp(full, target)) return;
    }
    fired = 1;
    if (rename(target, getenv("RCP_TEST_MUTATION_MOVED"))) fail("moving admitted source entry");
    const char *replacement = getenv("RCP_TEST_MUTATION_REPLACEMENT");
    if (replacement && replacement[0] && rename(replacement, target)) fail("replacing admitted source entry");
    int fd = open(getenv("RCP_TEST_MUTATION_MARKER"), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0 || write(fd, "mutated", 7) != 7 || close(fd)) fail("writing mutation marker");
}
int main(int argc, char **argv) {
    if (argc < 2) return 191;
    pid_t child = fork();
    if (child < 0) fail("forking source daemon");
    if (child == 0) {
        if (ptrace(PTRACE_TRACEME, 0, 0, 0) < 0) fail("tracing source daemon");
        raise(SIGSTOP);
        execv(argv[1], argv + 1);
        fail("executing source daemon");
    }
    owned_child = child;
    int status, result = 192;
    if (waitpid(child, &status, 0) != child || !WIFSTOPPED(status)) fail("initial source stop");
    long options = PTRACE_O_TRACESYSGOOD | PTRACE_O_TRACECLONE | PTRACE_O_TRACEFORK |
                   PTRACE_O_TRACEVFORK | PTRACE_O_EXITKILL;
    if (ptrace(PTRACE_SETOPTIONS, child, 0, options) < 0) fail("setting source trace options");
    if (ptrace(PTRACE_SYSCALL, child, 0, 0) < 0) fail("starting source trace");
    for (;;) {
        pid_t tid = waitpid(-1, &status, __WALL);
        if (tid < 0) {
            if (errno == EINTR) continue;
            if (errno == ECHILD) break;
            fail("waiting for source syscall");
        }
        if (WIFEXITED(status) || WIFSIGNALED(status)) {
            if (tid == child) {
                result = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
                owned_child = -1;
            }
            continue;
        }
        if (!WIFSTOPPED(status)) continue;
        int signal = WSTOPSIG(status);
        if (signal == (SIGTRAP | 0x80)) {
            struct ptrace_syscall_info info;
            if (ptrace(PTRACE_GET_SYSCALL_INFO, tid, sizeof(info), &info) < 0) fail("reading source syscall");
            if (info.op == PTRACE_SYSCALL_INFO_ENTRY) {
                if (info.entry.nr == SYS_openat)
                    mutate_before_open(tid, (int)info.entry.args[0], info.entry.args[1], (int)info.entry.args[2]);
#ifdef SYS_open
                else if (info.entry.nr == SYS_open)
                    mutate_before_open(tid, AT_FDCWD, info.entry.args[0], (int)info.entry.args[1]);
#endif
            }
            signal = 0;
        } else if (signal == SIGSTOP || signal == SIGTRAP) {
            signal = 0;
        }
        if (ptrace(PTRACE_SYSCALL, tid, 0, signal) < 0 && errno != ESRCH) fail("resuming source thread");
    }
    return result;
}
"#,
        )
        .unwrap();
    let output = std::process::Command::new("cc")
        .args(["-std=c11", "-O2", "-o"])
        .arg(fixture.path().join("mutate"))
        .arg(&source)
        .output()
        .expect("a C compiler is required for deterministic source mutation tests");
    assert!(
        output.status.success(),
        "syscall tracer compilation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    fixture
}

#[cfg(target_os = "linux")]
fn run_rcp_with_source_open_mutation(
    args: &[&str],
    target: &std::path::Path,
    replacement: Option<&std::path::Path>,
) -> std::process::Output {
    let fixture = tempfile::tempdir().unwrap();
    let marker = fixture.path().join("mutated");
    let moved = fixture.path().join("original");
    let wrapper = fixture.path().join("rcpd");
    let tracer_fixture = source_open_mutation_tracer();
    let tracer = tracer_fixture.path().join("mutate");
    let real_daemon = assert_cmd::cargo::cargo_bin("rcpd");
    std::fs::write(&wrapper, format!(
        "#!/bin/sh\nif [ \"$1\" = --role ] && [ \"$2\" = source ]; then\n  export RCP_TEST_MUTATION_TARGET={}\n  export RCP_TEST_MUTATION_MOVED={}\n  export RCP_TEST_MUTATION_REPLACEMENT={}\n  export RCP_TEST_MUTATION_MARKER={}\n  exec {} {} \"$@\"\nfi\nexec {} \"$@\"\n",
        shell_quote_for_test(target),
        shell_quote_for_test(&moved),
        shell_quote_for_test(replacement.unwrap_or_else(|| std::path::Path::new(""))),
        shell_quote_for_test(&marker),
        shell_quote_for_test(&tracer),
        shell_quote_for_test(&real_daemon),
        shell_quote_for_test(&real_daemon),
    )).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let daemon = format!("--rcpd-path={}", wrapper.display());
    let mut command = vec![
        daemon.as_str(),
        "--max-connections=1",
        "--pending-writes-multiplier=1",
    ];
    command.extend_from_slice(args);
    let output = run_rcp_with_args(&command);
    print_command_output(&output);
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap_or_default(),
        "mutated",
        "source must reach the intercepted use after classification"
    );
    output
}

/// Each admitted directory failure must settle its parent slot while unrelated files and
/// subtrees finish. Include filtering must use the original directory classification.
#[cfg(target_os = "linux")]
#[test]
fn remote_admitted_directory_open_failure_preserves_siblings() {
    require_local_ssh();
    for dereference in [false, true] {
        for vanished in [false, true] {
            let (src_dir, dst_dir) = setup_test_env();
            let src = src_dir.path().join("root");
            std::fs::create_dir_all(src.join("child")).unwrap();
            std::fs::create_dir(src.join("keep")).unwrap();
            create_test_file(&src.join("child/hidden.txt"), "hidden", 0o644);
            create_test_file(&src.join("sibling.txt"), "sibling", 0o640);
            create_test_file(&src.join("keep/keep.txt"), "keep", 0o600);
            let replacement = src_dir.path().join("replacement");
            create_test_file(&replacement, "changed directory", 0o644);
            let dst = dst_dir.path().join("root");
            let src_remote = format!("localhost:{}", src.display());
            let dst_remote = format!("localhost:{}", dst.display());
            let mut args = vec!["--include=*.txt", src_remote.as_str(), dst_remote.as_str()];
            if dereference {
                args.insert(0, "-L");
            }
            let output = run_rcp_with_source_open_mutation(
                &args,
                &src.join("child"),
                (!vanished).then_some(replacement.as_path()),
            );
            assert!(
                !output.status.success(),
                "admitted directory open failure must be reported"
            );
            let combined = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let cause = if vanished {
                "No such file or directory"
            } else {
                "Not a directory"
            };
            assert!(
                combined.contains(cause),
                "original open cause missing: {combined}"
            );
            assert_eq!(get_file_content(&dst.join("sibling.txt")), "sibling");
            assert_eq!(get_file_content(&dst.join("keep/keep.txt")), "keep");
            assert_eq!(get_file_mode(&dst.join("sibling.txt")) & 0o777, 0o640);
            assert!(!dst.join("child/hidden.txt").exists());
            assert!(
                !dst.join("child").is_file(),
                "a classified directory must not become a file transfer"
            );
        }
    }
}

/// A classified root that cannot be opened still has a terminal empty-directory result in
/// continue mode, including the explicitly dereferencing source path.
#[cfg(target_os = "linux")]
#[test]
fn remote_root_directory_open_failure_finishes_empty_tombstone() {
    require_local_ssh();
    for dereference in [false, true] {
        let (src_dir, dst_dir) = setup_test_env();
        let src = src_dir.path().join("root");
        std::fs::create_dir(&src).unwrap();
        create_test_file(&src.join("hidden.txt"), "hidden", 0o644);
        let replacement = src_dir.path().join("replacement");
        create_test_file(&replacement, "now a file", 0o644);
        let dst = dst_dir.path().join("root");
        std::fs::create_dir(&dst).unwrap();
        // unavailable source ACLs must remain Unknown, not clear a reused directory's ACL
        set_acl(&dst, ACL_DEFAULT, &granting_acl());
        let src_remote = format!("localhost:{}", src.display());
        let dst_remote = format!("localhost:{}", dst.display());
        let mut args = vec![
            "--preserve-settings=all+acl",
            src_remote.as_str(),
            dst_remote.as_str(),
        ];
        if dereference {
            args.insert(0, "-L");
        }
        let output = run_rcp_with_source_open_mutation(&args, &src, Some(&replacement));
        assert!(!output.status.success());
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            combined.contains("Not a directory"),
            "original root-open failure missing: {combined}"
        );
        assert!(
            dst.is_dir(),
            "committed unreadable root must become an empty directory"
        );
        assert_eq!(std::fs::read_dir(&dst).unwrap().count(), 0);
        assert_eq!(get_acl(&dst, ACL_DEFAULT), Some(granting_acl()));
    }
}

/// Data opens must reject incompatible replacements without following symlinks or waiting for
/// FIFO writers. A compatible replacement supplies both its own bytes and its own metadata.
#[cfg(target_os = "linux")]
#[test]
fn remote_admitted_file_mutation_uses_opened_object_and_preserves_siblings() {
    require_local_ssh();
    for dereference in [false, true] {
        for kind in ["vanished", "symlink", "fifo", "directory", "regular"] {
            if dereference && !matches!(kind, "vanished" | "regular") {
                continue;
            }
            let (src_dir, dst_dir) = setup_test_env();
            let src = src_dir.path().join("root");
            std::fs::create_dir_all(src.join("keep")).unwrap();
            create_test_file(&src.join("victim"), "old", 0o644);
            create_test_file(&src.join("sibling.txt"), "sibling", 0o640);
            create_test_file(&src.join("keep/keep.txt"), "keep", 0o600);
            let replacement = src_dir.path().join("replacement");
            match kind {
                "vanished" => {}
                "symlink" => {
                    let sentinel = src_dir.path().join("sentinel");
                    create_test_file(&sentinel, "SECRET SENTINEL", 0o600);
                    std::os::unix::fs::symlink(&sentinel, &replacement).unwrap();
                }
                "fifo" => {
                    let path =
                        std::ffi::CString::new(replacement.as_os_str().as_encoded_bytes()).unwrap();
                    // SAFETY: path is NUL-terminated and remains valid for this syscall
                    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
                }
                "directory" => std::fs::create_dir(&replacement).unwrap(),
                "regular" => create_test_file(
                    &replacement,
                    "replacement has different bytes and length",
                    0o600,
                ),
                _ => unreachable!(),
            }
            let dst = dst_dir.path().join("root");
            let src_remote = format!("localhost:{}", src.display());
            let dst_remote = format!("localhost:{}", dst.display());
            let mut args = vec![src_remote.as_str(), dst_remote.as_str()];
            if dereference {
                args.insert(0, "-L");
            }
            let output = run_rcp_with_source_open_mutation(
                &args,
                &src.join("victim"),
                (kind != "vanished").then_some(replacement.as_path()),
            );
            assert_eq!(
                output.status.success(),
                kind == "regular",
                "replacement kind {kind}"
            );
            assert_eq!(get_file_content(&dst.join("sibling.txt")), "sibling");
            assert_eq!(get_file_content(&dst.join("keep/keep.txt")), "keep");
            if kind == "regular" {
                assert_eq!(
                    get_file_content(&dst.join("victim")),
                    "replacement has different bytes and length"
                );
                assert_eq!(get_file_mode(&dst.join("victim")) & 0o777, 0o600);
            } else {
                let combined = format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                let errno = match kind {
                    "vanished" => libc::ENOENT,
                    "symlink" => libc::ELOOP,
                    "fifo" | "directory" => libc::EINVAL,
                    _ => unreachable!(),
                };
                let cause = std::io::Error::from_raw_os_error(errno).to_string();
                assert!(
                    combined.contains(&cause),
                    "original {kind} open failure missing: {combined}"
                );
                assert!(
                    !dst.join("victim").exists(),
                    "incompatible replacement must not send a header: {kind}"
                );
            }
        }
    }
}

/// A SPECIAL file as the root operand: skipped cleanly with `--skip-specials`, fatal without it.
///
/// Neither is new behavior, but neither was covered, and the skip case is one of only two ways to
/// reach the `-L` walk's "root committed nothing" exit (the other is a filtered-out root). That
/// exit must NOT be compensated with a `FileSkipped` — a root has no parent to account to, and
/// `FileSkipped` does not set the destination's `root_complete` — so the destination is released by
/// `DiscoveryComplete { has_root_item: false }` instead. If the funnel ever compensated a root,
/// this is the test that would notice the stray message.
#[test]
fn test_remote_special_root_skipped_or_fatal() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // the socket file outlives the listener; hold it so the special exists for both runs.
    let src_socket = src_dir.path().join("root_socket");
    let _listener = std::os::unix::net::UnixListener::bind(&src_socket).unwrap();
    let src_remote = format!("localhost:{}", src_socket.to_str().unwrap());
    let dst_socket = dst_dir.path().join("root_socket");
    let dst_remote = format!("localhost:{}", dst_socket.to_str().unwrap());
    // with --skip-specials the copy succeeds having copied nothing.
    let output = run_rcp_with_args(&["--skip-specials", &src_remote, &dst_remote]);
    print_command_output(&output);
    assert!(
        output.status.success(),
        "a special root with --skip-specials should finish cleanly"
    );
    assert!(
        !dst_socket.exists(),
        "a skipped special root must not create anything at the destination"
    );
    // without it, the unsupported root type fails the copy (matching the hardened root).
    let output = run_rcp_with_args(&[&src_remote, &dst_remote]);
    print_command_output(&output);
    assert!(
        !output.status.success(),
        "a special root without --skip-specials should fail the copy"
    );
    assert!(!dst_socket.exists());
}

/// TOCTOU hardening (Scenario 2 — destination write-escape): a symlink planted at an
/// intermediate DESTINATION directory path must NOT be followed when the remote `rcpd`
/// destination creates the subtree under it. The destination opens each tracked directory
/// `O_NOFOLLOW|O_DIRECTORY` and creates children fd-relative on that pinned fd, so a
/// directory-position-occupied-by-a-symlink fails closed (ELOOP/ENOTDIR) rather than letting
/// the privileged destination write files into the symlink's out-of-tree target.
///
/// This is the deterministic, pre-planted-symlink form of the race (a live mid-copy swap is not
/// reproducible through the subprocess harness): we plant the symlink before the copy, so the
/// create path is guaranteed to encounter it. The assertion that matters is that NO file ever
/// lands in the out-of-tree target directory.
#[test]
fn test_remote_dest_symlink_at_intermediate_dir_not_followed_out_of_tree() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // out-of-tree sentinel directory: nothing must ever be written here.
    let out_of_tree = tempfile::tempdir().unwrap();
    // source tree:  root/sub/file.txt  (sub is a real directory at the source).
    let src_root = src_dir.path().join("root");
    let src_sub = src_root.join("sub");
    std::fs::create_dir_all(&src_sub).unwrap();
    create_test_file(
        &src_sub.join("file.txt"),
        "must stay in the dst tree",
        0o644,
    );
    // destination: pre-create root/, then plant a SYMLINK where root/sub must go,
    // pointing at the out-of-tree directory. If the destination followed it, the file
    // would be written to out_of_tree/file.txt.
    let dst_root = dst_dir.path().join("root");
    std::fs::create_dir(&dst_root).unwrap();
    let dst_sub_link = dst_root.join("sub");
    std::os::unix::fs::symlink(out_of_tree.path(), &dst_sub_link).unwrap();
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // no --overwrite: the destination must NOT replace the symlink, and crucially must
    // not follow it. The copy fails (the symlinked dir position is a real error), but the
    // harness already fails us on a hang/timeout, so a clean non-zero exit is fine.
    let output = run_rcp_with_args(&[&src_remote, &dst_remote]);
    print_command_output(&output);
    // THE load-bearing assertion: the out-of-tree target was never written through.
    assert!(
        !out_of_tree.path().join("file.txt").exists(),
        "file escaped the destination tree through a planted intermediate symlink \
         (O_NOFOLLOW create was bypassed) — TOCTOU destination write-escape"
    );
    // the planted symlink itself must be untouched (not followed, not replaced without --overwrite).
    assert!(
        dst_sub_link.is_symlink(),
        "planted intermediate symlink should remain a symlink (was not followed/replaced)"
    );
    // and no real file landed at the in-tree path either (the subtree was skipped, fail-closed).
    assert!(
        !dst_root.join("sub").join("file.txt").is_file()
            || std::fs::symlink_metadata(dst_root.join("sub"))
                .unwrap()
                .is_symlink(),
        "no regular file should have been created under the symlinked dst path"
    );
}

/// Companion to the test above with `--overwrite`: even when the destination may replace the
/// planted intermediate symlink, removal is fd-relative and parent-contained (`unlink_at` removes
/// the symlink itself without following it), then a real directory is created in its place. The
/// file must end up inside the destination tree, and nothing may be written to the symlink's
/// out-of-tree target.
#[test]
fn test_remote_dest_overwrite_replaces_intermediate_symlink_in_tree() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let out_of_tree = tempfile::tempdir().unwrap();
    let src_root = src_dir.path().join("root");
    let src_sub = src_root.join("sub");
    std::fs::create_dir_all(&src_sub).unwrap();
    create_test_file(&src_sub.join("file.txt"), "lands in the dst tree", 0o644);
    let dst_root = dst_dir.path().join("root");
    std::fs::create_dir(&dst_root).unwrap();
    let dst_sub_link = dst_root.join("sub");
    std::os::unix::fs::symlink(out_of_tree.path(), &dst_sub_link).unwrap();
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    let output = run_rcp_and_expect_success(&["--overwrite", &src_remote, &dst_remote]);
    print_command_output(&output);
    // out-of-tree target must remain empty — the symlink was unlinked, never followed.
    assert!(
        !out_of_tree.path().join("file.txt").exists(),
        "file escaped the destination tree through a replaced intermediate symlink"
    );
    // the dst path is now a real directory containing the copied file, inside the tree.
    let dst_sub = dst_root.join("sub");
    assert!(
        dst_sub.is_dir() && !dst_sub.symlink_metadata().unwrap().is_symlink(),
        "dst/sub should be replaced with a real directory"
    );
    assert_eq!(
        get_file_content(&dst_sub.join("file.txt")),
        "lands in the dst tree",
        "copied file should land inside the destination tree"
    );
}

/// TOCTOU hardening: a symlink planted where a destination FILE must be created must not be
/// followed — the file's bytes must not be written through the symlink to its out-of-tree target.
/// The destination creates files with `O_CREAT|O_EXCL|O_NOFOLLOW` relative to the parent's pinned
/// fd, so a symlink occupying the file's name fails closed (EEXIST) without `--overwrite`.
#[test]
fn test_remote_dest_symlink_at_file_path_not_followed_out_of_tree() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let out_of_tree = tempfile::tempdir().unwrap();
    let out_of_tree_target = out_of_tree.path().join("victim.txt");
    // source: a single file root/data.txt
    let src_root = src_dir.path().join("root");
    std::fs::create_dir(&src_root).unwrap();
    create_test_file(&src_root.join("data.txt"), "secret payload", 0o644);
    // destination: plant a symlink at root/data.txt -> out-of-tree victim path.
    let dst_root = dst_dir.path().join("root");
    std::fs::create_dir(&dst_root).unwrap();
    let dst_file_link = dst_root.join("data.txt");
    std::os::unix::fs::symlink(&out_of_tree_target, &dst_file_link).unwrap();
    let src_remote = format!("localhost:{}", src_root.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_root.to_str().unwrap());
    // no --overwrite: create must fail closed (EEXIST via O_EXCL), never following the symlink.
    let output = run_rcp_with_args(&[&src_remote, &dst_remote]);
    print_command_output(&output);
    // the out-of-tree victim path must never be created/written through the symlink.
    assert!(
        !out_of_tree_target.exists(),
        "file data was written through a planted symlink to an out-of-tree path \
         (O_EXCL|O_NOFOLLOW create was bypassed) — TOCTOU destination write-escape"
    );
    // the planted symlink is left intact (no --overwrite, and never followed).
    assert!(
        dst_file_link.is_symlink(),
        "planted file-path symlink should remain a symlink"
    );
}

/// re-syncing a directory whose files are byte-and-mtime identical must transfer NO file data:
/// every file is files_unchanged, bytes_copied is 0, and the source-skip marker proves the data
/// path was never opened (vs today's "send then drain").
#[test]
fn test_remote_overwrite_skips_identical_files_in_directory() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_sub = src_dir.path().join("tree");
    let dst_sub = dst_dir.path().join("tree");
    std::fs::create_dir(&src_sub).unwrap();
    std::fs::create_dir(&dst_sub).unwrap();
    for i in 0..3 {
        let name = format!("f{i}.txt");
        let content = format!("identical content {i}");
        create_test_file(&src_sub.join(&name), &content, 0o644);
        create_test_file(&dst_sub.join(&name), &content, 0o644);
        // make mtimes identical so the default size,mtime quick-check matches
        let t = filetime::FileTime::from_unix_time(1_700_000_000, 0);
        filetime::set_file_mtime(src_sub.join(&name), t).unwrap();
        filetime::set_file_mtime(dst_sub.join(&name), t).unwrap();
    }
    let src_remote = format!("localhost:{}", src_sub.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_sub.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 0, "no file should be copied");
    assert_eq!(summary.files_unchanged, 3, "all 3 files unchanged");
    assert_eq!(summary.bytes_copied, 0, "no bytes written");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("skipping transfer (manifest)"),
        "source-skip marker should appear (proving data was not sent), got:\n{combined}"
    );
}

/// a mix of identical + changed + new files: only changed/new transfer; identical are skipped.
#[test]
fn test_remote_overwrite_partial_overlap_transfers_only_changed() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_sub = src_dir.path().join("tree");
    let dst_sub = dst_dir.path().join("tree");
    std::fs::create_dir(&src_sub).unwrap();
    std::fs::create_dir(&dst_sub).unwrap();
    let t = filetime::FileTime::from_unix_time(1_700_000_000, 0);
    // identical file
    create_test_file(&src_sub.join("same.txt"), "same", 0o644);
    create_test_file(&dst_sub.join("same.txt"), "same", 0o644);
    filetime::set_file_mtime(src_sub.join("same.txt"), t).unwrap();
    filetime::set_file_mtime(dst_sub.join("same.txt"), t).unwrap();
    // changed file (different content => different size)
    create_test_file(&src_sub.join("changed.txt"), "new longer content", 0o644);
    create_test_file(&dst_sub.join("changed.txt"), "old", 0o644);
    // brand-new file (not at destination)
    create_test_file(&src_sub.join("new.txt"), "brand new", 0o644);
    let src_remote = format!("localhost:{}", src_sub.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_sub.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_unchanged, 1, "same.txt unchanged");
    assert_eq!(summary.files_copied, 2, "changed.txt + new.txt copied");
    // only the changed + new files' bytes cross the wire (18 + 9); same.txt contributes none
    assert_eq!(summary.bytes_copied, 27, "only changed.txt + new.txt bytes");
    assert_eq!(
        get_file_content(&dst_sub.join("changed.txt")),
        "new longer content"
    );
    assert_eq!(get_file_content(&dst_sub.join("new.txt")), "brand new");
}

/// a file whose SIZE matches but mtime differs must be re-sent (default compare is size,mtime).
/// content is held same-length-but-different only so we can confirm the new bytes actually land —
/// the size,mtime quick-check ignores content, so the resend is attributable to the mtime.
#[test]
fn test_remote_overwrite_resends_when_mtime_differs() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_sub = src_dir.path().join("tree");
    let dst_sub = dst_dir.path().join("tree");
    std::fs::create_dir(&src_sub).unwrap();
    std::fs::create_dir(&dst_sub).unwrap();
    // same length (10 bytes), different bytes, different mtime
    create_test_file(&src_sub.join("f.txt"), "fresh data", 0o644);
    create_test_file(&dst_sub.join("f.txt"), "stale data", 0o644);
    filetime::set_file_mtime(
        src_sub.join("f.txt"),
        filetime::FileTime::from_unix_time(2_000_000_000, 0),
    )
    .unwrap();
    filetime::set_file_mtime(
        dst_sub.join("f.txt"),
        filetime::FileTime::from_unix_time(1_000_000_000, 0),
    )
    .unwrap();
    let src_remote = format!("localhost:{}", src_sub.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_sub.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 1, "mtime differs => re-send");
    assert_eq!(summary.files_unchanged, 0);
    // confirm the resend actually wrote the source bytes (not merely counted it)
    assert_eq!(get_file_content(&dst_sub.join("f.txt")), "fresh data");
}

/// --ignore-existing skips any colliding file by name without transfer (content untouched).
#[test]
fn test_remote_ignore_existing_skips_colliding_files_in_directory() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_sub = src_dir.path().join("tree");
    let dst_sub = dst_dir.path().join("tree");
    std::fs::create_dir(&src_sub).unwrap();
    std::fs::create_dir(&dst_sub).unwrap();
    create_test_file(&src_sub.join("keep.txt"), "source version", 0o644);
    create_test_file(&dst_sub.join("keep.txt"), "destination version", 0o644);
    create_test_file(&src_sub.join("fresh.txt"), "fresh", 0o644);
    let src_remote = format!("localhost:{}", src_sub.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_sub.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--ignore-existing", "--summary", &src_remote, &dst_remote]);
    assert_eq!(
        get_file_content(&dst_sub.join("keep.txt")),
        "destination version"
    );
    assert_eq!(get_file_content(&dst_sub.join("fresh.txt")), "fresh");
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_unchanged, 1);
    assert_eq!(summary.files_copied, 1);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("skipping transfer (manifest)"),
        "source-skip marker should appear for the colliding file, got:\n{combined}"
    );
}

/// type mismatch under --overwrite: dest has a SYMLINK where source has a file => must send and
/// replace (the manifest marks it is_file=false, so the source does not skip).
#[test]
fn test_remote_overwrite_sends_when_dest_entry_is_symlink() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_sub = src_dir.path().join("tree");
    let dst_sub = dst_dir.path().join("tree");
    std::fs::create_dir(&src_sub).unwrap();
    std::fs::create_dir(&dst_sub).unwrap();
    create_test_file(&src_sub.join("x"), "real file content", 0o644);
    std::os::unix::fs::symlink("/nonexistent", dst_sub.join("x")).unwrap();
    let src_remote = format!("localhost:{}", src_sub.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_sub.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    assert!(
        !dst_sub.join("x").is_symlink(),
        "symlink should be replaced by a file"
    );
    assert_eq!(get_file_content(&dst_sub.join("x")), "real file content");
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_copied, 1);
    assert_eq!(
        summary.files_unchanged, 0,
        "a type-mismatched dest entry must not be skipped"
    );
}

/// large-directory safeguard: with the cap set to 1, a 2-entry reused dir falls back to
/// transfer-and-drain — identical files are still unchanged, but the source-skip marker is absent.
#[test]
fn test_remote_overwrite_manifest_cap_falls_back_to_transfer() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_sub = src_dir.path().join("tree");
    let dst_sub = dst_dir.path().join("tree");
    std::fs::create_dir(&src_sub).unwrap();
    std::fs::create_dir(&dst_sub).unwrap();
    let t = filetime::FileTime::from_unix_time(1_700_000_000, 0);
    for i in 0..2 {
        let name = format!("f{i}.txt");
        create_test_file(&src_sub.join(&name), "same", 0o644);
        create_test_file(&dst_sub.join(&name), "same", 0o644);
        filetime::set_file_mtime(src_sub.join(&name), t).unwrap();
        filetime::set_file_mtime(dst_sub.join(&name), t).unwrap();
    }
    let src_remote = format!("localhost:{}", src_sub.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_sub.to_str().unwrap());
    let output = run_rcp_and_expect_success(&[
        "--overwrite",
        "--overwrite-manifest-max-entries=1",
        "--max-connections=1",
        "--pending-writes-multiplier=1",
        "--summary",
        &src_remote,
        &dst_remote,
    ]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // still correct: identical files are unchanged (destination drains them), no bytes written
    assert_eq!(summary.files_unchanged, 2);
    assert_eq!(summary.bytes_copied, 0);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");
    assert!(
        !combined.contains("skipping transfer (manifest)"),
        "above the cap the manifest is omitted, so the source must NOT skip-by-manifest"
    );
}

/// nested tree: a reused parent containing a REUSED child subdir (identical file → skipped) and a
/// FRESH child subdir (no destination counterpart → file copied). Proves the manifest is built
/// per-directory: non-empty for the reused child, empty for the freshly-created child.
#[test]
fn test_remote_overwrite_nested_mixed_fresh_and_reused_dirs() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_sub = src_dir.path().join("tree");
    let dst_sub = dst_dir.path().join("tree");
    // reused child subdir exists on both sides with an identical file
    std::fs::create_dir_all(src_sub.join("reused")).unwrap();
    std::fs::create_dir_all(dst_sub.join("reused")).unwrap();
    create_test_file(&src_sub.join("reused/f.txt"), "shared", 0o644);
    create_test_file(&dst_sub.join("reused/f.txt"), "shared", 0o644);
    let t = filetime::FileTime::from_unix_time(1_700_000_000, 0);
    filetime::set_file_mtime(src_sub.join("reused/f.txt"), t).unwrap();
    filetime::set_file_mtime(dst_sub.join("reused/f.txt"), t).unwrap();
    // fresh child subdir exists only on the source
    std::fs::create_dir_all(src_sub.join("fresh")).unwrap();
    create_test_file(&src_sub.join("fresh/g.txt"), "new file", 0o644);
    let src_remote = format!("localhost:{}", src_sub.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_sub.to_str().unwrap());
    let output =
        run_rcp_and_expect_success(&["--overwrite", "--summary", &src_remote, &dst_remote]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    // reused/f.txt skipped (manifest), fresh/g.txt copied into the freshly-created subdir
    assert_eq!(summary.files_unchanged, 1, "reused/f.txt unchanged");
    assert_eq!(summary.files_copied, 1, "fresh/g.txt copied");
    assert_eq!(get_file_content(&dst_sub.join("reused/f.txt")), "shared");
    assert_eq!(get_file_content(&dst_sub.join("fresh/g.txt")), "new file");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("skipping transfer (manifest)"),
        "the reused subdir's identical file should be skipped via the manifest, got:\n{combined}"
    );
}

/// --overwrite-filter=newer through the manifest path (files within a reused directory): a file
/// whose destination is strictly newer is skipped; one whose destination is older is re-sent.
/// Both files differ in content/size so the newer-filter (not metadata-equality) drives the skip.
#[test]
fn test_remote_overwrite_filter_newer_in_directory() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_sub = src_dir.path().join("tree");
    let dst_sub = dst_dir.path().join("tree");
    std::fs::create_dir(&src_sub).unwrap();
    std::fs::create_dir(&dst_sub).unwrap();
    // dest is NEWER => skip (kept). content differs so this is the newer-filter, not equality.
    create_test_file(&src_sub.join("keep_dest.txt"), "src side", 0o644);
    create_test_file(
        &dst_sub.join("keep_dest.txt"),
        "destination is newer",
        0o644,
    );
    filetime::set_file_mtime(
        src_sub.join("keep_dest.txt"),
        filetime::FileTime::from_unix_time(1_000_000_000, 0),
    )
    .unwrap();
    filetime::set_file_mtime(
        dst_sub.join("keep_dest.txt"),
        filetime::FileTime::from_unix_time(2_000_000_000, 0),
    )
    .unwrap();
    // dest is OLDER => re-send (overwritten).
    create_test_file(
        &src_sub.join("update_dest.txt"),
        "fresh source bytes",
        0o644,
    );
    create_test_file(&dst_sub.join("update_dest.txt"), "old", 0o644);
    filetime::set_file_mtime(
        src_sub.join("update_dest.txt"),
        filetime::FileTime::from_unix_time(2_000_000_000, 0),
    )
    .unwrap();
    filetime::set_file_mtime(
        dst_sub.join("update_dest.txt"),
        filetime::FileTime::from_unix_time(1_000_000_000, 0),
    )
    .unwrap();
    let src_remote = format!("localhost:{}", src_sub.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_sub.to_str().unwrap());
    let output = run_rcp_and_expect_success(&[
        "--overwrite",
        "--overwrite-filter=newer",
        "--summary",
        &src_remote,
        &dst_remote,
    ]);
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(summary.files_unchanged, 1, "dest-newer file kept");
    assert_eq!(summary.files_copied, 1, "dest-older file re-sent");
    // newer destination preserved; older destination overwritten with source bytes
    assert_eq!(
        get_file_content(&dst_sub.join("keep_dest.txt")),
        "destination is newer"
    );
    assert_eq!(
        get_file_content(&dst_sub.join("update_dest.txt")),
        "fresh source bytes"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("skipping transfer (manifest)"),
        "the dest-newer file should be skipped via the manifest, got:\n{combined}"
    );
}

/// --require-toctou-safe propagates to both rcpd sides and the copy succeeds for
/// fully-resolved absolute remote operands
#[test]
fn test_remote_require_toctou_safe_copies() {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
        return;
    }
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // canonicalize: TMPDIR itself may contain symlinked components (e.g. under
    // nix-shell), which strict resolution would — correctly — refuse
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    let src_file = src_base.join("strict.txt");
    create_test_file(&src_file, "strict content", 0o644);
    let dst_file = dst_base.join("strict_out.txt");
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_file.to_str().unwrap());
    run_rcp_and_expect_success(&["--require-toctou-safe", &src_remote, &dst_remote]);
    assert_eq!(get_file_content(&dst_file), "strict content");
}

/// The remote reused-destination-directory lockdown+restore path, end-to-end over
/// the real protocol: a `--require-toctou-safe --overwrite` remote copy INTO a
/// PRE-EXISTING (reused) destination directory locks it down for the copy
/// (`create_directory`: verify_same_inode → secure_as_copier → 0o700) and restores
/// it at completion (`DirectoryFinalization::execute`: chown_to + the tracker threading
/// through the owned `DirectoryFinalization.reused_lock`). The reused dir starts NON-WRITABLE at
/// 0o500 (like the local test): without the lockdown (which fchmods it to 0o700) the
/// copier could not write the child into it, so a successful copy PROVES the lockdown
/// fired — not a vacuous pass. The source dir is at 0o755; a successful copy must
/// leave the reused dir at the SOURCE mode (0o755, not the interim 0o700) with its
/// owner unchanged. Runs without extra privilege — the owner value is a no-op
/// without a uid difference (the `sudo` test below covers the foreign-owner
/// restore), but this still exercises the whole remote lockdown machinery and
/// catches gross breakage in the rcpd plumbing.
#[test]
fn test_remote_require_toctou_safe_reused_dir_locked_and_restored() {
    use std::os::unix::fs::MetadataExt;
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
        return;
    }
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // canonicalize: TMPDIR itself may contain symlinked components, which strict resolution refuses
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    // source directory (distinctive mode 0o755) with a child to write into the reused dir
    let src_subdir = src_base.join("tree");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("a.txt"), "payload", 0o644);
    std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o755)).unwrap();
    // pre-existing (reused) destination directory, owned by us, NON-WRITABLE at 0o500 — without the
    // lockdown (which fchmods it to 0o700) the copier could not write the child, so the copy's
    // success is proof the lockdown fired (not a vacuous pass, matching the local test).
    let dst_subdir = dst_base.join("tree");
    std::fs::create_dir(&dst_subdir).unwrap();
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o500)).unwrap();
    let orig_uid = std::fs::metadata(&dst_subdir).unwrap().uid();
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let output = run_rcp_and_expect_success(&[
        "--require-toctou-safe",
        "--overwrite",
        "--summary",
        &src_remote,
        &dst_remote,
    ]);
    // the child landed → the reused dir was made writable for the copy, then restored
    assert_eq!(get_file_content(&dst_subdir.join("a.txt")), "payload");
    // the directory was REUSED (not recreated): directories_created must be 0
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(
        summary.directories_created, 0,
        "the destination directory was reused, not created"
    );
    // final mode is the SOURCE mode (0o755), not the interim lockdown 0o700
    assert_eq!(
        get_file_mode(&dst_subdir),
        0o755,
        "reused dir final mode must equal the source directory mode"
    );
    // owner unchanged (a no-op without a uid difference, but must not be left mangled)
    assert_eq!(
        std::fs::metadata(&dst_subdir).unwrap().uid(),
        orig_uid,
        "reused dir owner uid must be unchanged"
    );
}

/// Remote mirror of the local `test_sudo_strict_reuse_restores_foreign_owned_dir`
/// (rcp/tests/tests.rs): a privileged (root) rcpd copier secures a FOREIGN-owned
/// reused remote destination directory and restores its ORIGINAL owner at
/// completion, so under `preserve_none` the reused dir ends owned by the original
/// unprivileged user — not the root copier (the v1 "no restore" bug).
///
/// Beyond passwordless sudo, this needs the WHOLE rcp+rcpd chain to run as root — i.e.
/// passwordless root→localhost SSH, a higher bar than the other remote `sudo` tests
/// (which run rcpd as the normal user and use sudo only to plant root-owned inputs).
/// The differing uid it needs comes for free: the reused destination directory is
/// created by the invoking user while the copy itself runs as root.
///
/// It SKIPS (does not fail) when sudo or root→localhost SSH is unavailable, so it stays
/// runnable on a workstation, and is `#[ignore]` so a normal `just test` never runs it.
/// CI provisions root SSH and sets `RCP_REQUIRE_ROOT_SSH=1`, which turns every skip below
/// into a hard failure — otherwise a silent regression in that provisioning would quietly
/// stop exercising the assertion.
#[test]
#[ignore = "requires passwordless sudo + root SSH-to-localhost"]
fn test_remote_sudo_strict_reuse_restores_foreign_owned_dir() {
    use std::os::unix::fs::MetadataExt;
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
        return;
    }
    require_local_ssh();
    // `RCP_REQUIRE_ROOT_SSH=1` (set by CI) makes a missing precondition FAIL instead of skip, so a
    // broken provisioning step cannot silently stop exercising the assertion below.
    let must_run = std::env::var_os("RCP_REQUIRE_ROOT_SSH").is_some_and(|v| v == "1");
    let skip_unless = |ok: bool, what: &str| -> bool {
        if ok {
            return false;
        }
        assert!(
            !must_run,
            "RCP_REQUIRE_ROOT_SSH=1 but {what} is unavailable"
        );
        eprintln!("Skipping test: {what} not available");
        true
    };
    let sudo_ok = std::process::Command::new("sudo")
        .args(["-n", "true"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if skip_unless(sudo_ok, "passwordless sudo") {
        return;
    }
    // rcp runs as root and spawns rcpd via ssh, so ROOT must reach localhost passwordlessly
    let root_ssh_ok = std::process::Command::new("sudo")
        .args(["-n", "ssh", "-o", "BatchMode=yes", "localhost", "true"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if skip_unless(root_ssh_ok, "passwordless root SSH-to-localhost") {
        return;
    }
    let (src_dir, dst_dir) = setup_test_env();
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    // source directory + child, owned by us
    let src_subdir = src_base.join("tree");
    std::fs::create_dir(&src_subdir).unwrap();
    create_test_file(&src_subdir.join("a.txt"), "payload", 0o644);
    std::fs::set_permissions(&src_subdir, std::fs::Permissions::from_mode(0o755)).unwrap();
    // reused destination directory, owned by us (the unprivileged user), world-open
    let dst_subdir = dst_base.join("tree");
    std::fs::create_dir(&dst_subdir).unwrap();
    std::fs::set_permissions(&dst_subdir, std::fs::Permissions::from_mode(0o777)).unwrap();
    let orig_uid = std::fs::metadata(&dst_subdir).unwrap().uid();
    let src_remote = format!("localhost:{}", src_subdir.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_subdir.to_str().unwrap());
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    // run the WHOLE chain as root so the destination rcpd is privileged; `timeout` guards a hang.
    let status = std::process::Command::new("sudo")
        .args([
            "-n",
            "timeout",
            "90",
            rcp_path.to_str().unwrap(),
            "--force-remote",
            "--require-toctou-safe",
            "--preserve-settings",
            "none",
            "--overwrite",
        ])
        .arg(&src_remote)
        .arg(&dst_remote)
        .status()
        .expect("failed to run sudo rcp");
    // snapshot observations BEFORE cleanup so the assertions use pre-cleanup state
    let final_uid = std::fs::metadata(&dst_subdir).unwrap().uid();
    let child_exists = dst_subdir.join("a.txt").exists();
    // root copied the child, so the reused subtree may hold root-owned entries; remove with sudo so
    // the tempdir drop can finish regardless of the outcome
    let _ = std::process::Command::new("sudo")
        .args(["-n", "rm", "-rf"])
        .arg(&dst_subdir)
        .status();
    assert!(
        status.success(),
        "sudo remote rcp --require-toctou-safe must succeed"
    );
    assert!(
        child_exists,
        "child must be copied into the reused directory"
    );
    // KEY: the reused remote directory's owner is restored to the original unprivileged user
    assert_eq!(
        final_uid, orig_uid,
        "reused remote dir owner uid must be restored to the original, not left as the root copier"
    );
}

/// --require-toctou-safe fails closed when a remote source operand path crosses a
/// symlink: the source rcpd resolves it RESOLVE_NO_SYMLINKS and gets ELOOP
#[test]
fn test_remote_require_toctou_safe_refuses_symlinked_src_prefix() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    std::fs::create_dir_all(src_base.join("real")).unwrap();
    create_test_file(&src_base.join("real/a.txt"), "x", 0o644);
    std::os::unix::fs::symlink(src_base.join("real"), src_base.join("link")).unwrap();
    let src_remote = format!(
        "localhost:{}",
        src_base.join("link/a.txt").to_str().unwrap()
    );
    let dst_remote = format!("localhost:{}", dst_base.join("out.txt").to_str().unwrap());
    run_rcp_and_expect_failure(&["--require-toctou-safe", &src_remote, &dst_remote]);
    assert!(
        !dst_base.join("out.txt").exists(),
        "nothing must be copied through a symlinked prefix"
    );
}

/// --require-toctou-safe fails closed when the remote DESTINATION operand path
/// crosses a symlink: the destination rcpd resolves it RESOLVE_NO_SYMLINKS
#[test]
fn test_remote_require_toctou_safe_refuses_symlinked_dst_prefix() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    let src_file = src_base.join("a.txt");
    create_test_file(&src_file, "x", 0o644);
    std::fs::create_dir_all(dst_base.join("real")).unwrap();
    std::os::unix::fs::symlink(dst_base.join("real"), dst_base.join("link")).unwrap();
    let src_remote = format!("localhost:{}", src_file.to_str().unwrap());
    let dst_remote = format!(
        "localhost:{}",
        dst_base.join("link/out.txt").to_str().unwrap()
    );
    run_rcp_and_expect_failure(&["--require-toctou-safe", &src_remote, &dst_remote]);
    assert!(
        !dst_base.join("real/out.txt").exists(),
        "nothing must be written through a symlinked prefix"
    );
}

/// --require-toctou-safe holds for remote --dry-run too: a symlinked source
/// prefix fails closed instead of being silently traversed by the path-based
/// dry-run reporter
#[test]
fn test_remote_require_toctou_safe_dry_run_refuses_symlinked_src_prefix() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    std::fs::create_dir_all(src_base.join("real/tree")).unwrap();
    create_test_file(&src_base.join("real/tree/a.txt"), "x", 0o644);
    std::os::unix::fs::symlink(src_base.join("real"), src_base.join("link")).unwrap();
    let src_remote = format!("localhost:{}", src_base.join("link/tree").to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_base.join("out").to_str().unwrap());
    run_rcp_and_expect_failure(&[
        "--require-toctou-safe",
        "--dry-run=brief",
        &src_remote,
        &dst_remote,
    ]);
}

/// A remote dry-run whose root the filter excludes skips cleanly even when the
/// source parent is execute-only (0111): the default-path root filter classifies
/// by path stat and the parent is opened only when traversal proceeds
#[test]
fn test_remote_dry_run_excluded_root_skips_under_execute_only_parent() {
    use std::os::unix::fs::PermissionsExt;
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    std::fs::create_dir_all(src_base.join("parent/src")).unwrap();
    create_test_file(&src_base.join("parent/src/a.txt"), "x", 0o644);
    std::fs::set_permissions(
        src_base.join("parent"),
        std::fs::Permissions::from_mode(0o111),
    )
    .unwrap();
    let src_remote = format!(
        "localhost:{}",
        src_base.join("parent/src").to_str().unwrap()
    );
    let dst_remote = format!("localhost:{}", dst_base.join("out").to_str().unwrap());
    let output = run_rcp_with_args(&["--dry-run=brief", "--exclude=src", &src_remote, &dst_remote]);
    // restore permissions before asserting so the tempdir cleanup works either way
    std::fs::set_permissions(
        src_base.join("parent"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    print_command_output(&output);
    assert!(
        output.status.success(),
        "excluded dry-run root under an execute-only parent must skip cleanly"
    );
}

/// A pre-header file failure under capacity-one saturation must cancel blocked work promptly
/// and keep the original error ahead of synthetic admission-close or lost-obligation errors.
#[test]
fn test_remote_fail_early_saturated_budget_reports_real_cause() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src = src_dir.path().canonicalize().unwrap();
    let dst = dst_dir.path().canonicalize().unwrap();
    // every subtree has an unreadable file so the test does not depend on classification order
    for i in 0..10 {
        let d = src.join(format!("d{i}"));
        std::fs::create_dir(&d).unwrap();
        std::fs::write(d.join("ok.txt"), b"ok").unwrap();
        let bad = d.join("bad.txt");
        std::fs::write(&bad, b"secret").unwrap();
        std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o000)).unwrap();
    }
    let src_remote = format!("localhost:{}", src.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&[
        "--max-connections",
        "1",
        "--pending-writes-multiplier",
        "1",
        "--fail-early",
        &src_remote,
        &dst_remote,
    ]);
    // the -vv tracing and the final error print both go to stdout in this harness, so assert against
    // the combined streams rather than a single one.
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    // the reported top-level Source error must be the real cause, not the synthetic budget wakeup.
    assert!(
        combined.lines().any(|line| {
            line.split_once("Source: ").is_some_and(|(_, source)| {
                source
                    .split("; Destination:")
                    .next()
                    .unwrap()
                    .contains("Permission denied (os error 13)")
            })
        }),
        "top-level Source error must name the real cause (Permission denied); got:\n{combined}"
    );
    assert!(
        !combined.contains("Source: file admission closed")
            && !combined.contains("Source: branch admission closed")
            && !combined.contains("Source: lost admitted child obligation"),
        "top-level Source error must not be the synthetic budget wakeup; got:\n{combined}"
    );
    // restore perms so the tempdir cleans up
    for i in 0..10 {
        let _ = std::fs::set_permissions(
            src.join(format!("d{i}")).join("bad.txt"),
            std::fs::Permissions::from_mode(0o600),
        );
    }
}

#[test]
fn test_remote_dereference_fail_early_saturated_budget_reports_real_cause() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src = src_dir.path().canonicalize().unwrap();
    let dst = dst_dir.path().canonicalize().unwrap();
    for i in 0..10 {
        let sibling = src.join(format!("sibling{i}"));
        std::fs::create_dir(&sibling).unwrap();
        let unreadable = sibling.join("unreadable.txt");
        std::fs::write(&unreadable, b"secret").unwrap();
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();
    }
    let src_remote = format!("localhost:{}", src.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst.to_str().unwrap());
    let output = run_rcp_and_expect_failure(&[
        "-L",
        "--max-connections=1",
        "--pending-writes-multiplier=1",
        "--fail-early",
        &src_remote,
        &dst_remote,
    ]);
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.lines().any(|line| {
            line.split_once("Source: ").is_some_and(|(_, source)| {
                source
                    .split("; Destination:")
                    .next()
                    .unwrap()
                    .contains("Permission denied (os error 13)")
            })
        }),
        "top-level Source error must name the real cause (Permission denied); got:\n{combined}"
    );
    assert!(
        !combined.contains("Source: file admission closed")
            && !combined.contains("Source: branch admission closed")
            && !combined.contains("Source: lost admitted child obligation"),
        "top-level Source error must not be the synthetic budget wakeup; got:\n{combined}"
    );
    for i in 0..10 {
        let _ = std::fs::set_permissions(
            src.join(format!("sibling{i}/unreadable.txt")),
            std::fs::Permissions::from_mode(0o600),
        );
    }
}

// ── POSIX ACLs over the wire ────────────────────────────────────────────────────────────────────
//
// The local engine reads a source entry's ACLs from the fd it is copying and applies them through
// the destination's fd. The remote engine cannot: the two fds live on different hosts, so the ACLs
// travel in `protocol::Metadata` as the source kernel's opaque bytes. These tests pin that
// transport end to end — including the CLEARING half, which is what stops a destination tree's
// default ACL from silently widening entries rcp creates beneath it.

use acl::{ACL_ACCESS, ACL_DEFAULT, denying_acl, describe_acl, get_acl, granting_acl, set_acl};

/// Build the source tree both round-trip directions copy, and arm the destination parent with a
/// permissive default ACL so INHERITANCE is genuinely in play.
///
/// ```text
/// tree/                 access = denying, default = granting
/// tree/secret.txt       access = denying
/// tree/plain.txt        (no ACL)
/// tree/nested/          (no ACL)
/// tree/nested/deep.txt  (no ACL)
/// ```
///
/// The entries with no ACL are the point of the fixture, not filler: every one of them is created
/// under a destination directory carrying a default ACL, so each INHERITS one at creation and the
/// copy has to remove it again to stay faithful to a source that had none.
fn build_acl_fixture(
    src_root: &std::path::Path,
    dst_parent: &std::path::Path,
) -> std::path::PathBuf {
    let tree = src_root.join("tree");
    std::fs::create_dir(&tree).unwrap();
    std::fs::set_permissions(&tree, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::create_dir(tree.join("nested")).unwrap();
    create_test_file(&tree.join("secret.txt"), "secret", 0o700);
    create_test_file(&tree.join("plain.txt"), "plain", 0o640);
    create_test_file(&tree.join("nested/deep.txt"), "deeper", 0o640);
    set_acl(&tree, ACL_ACCESS, &denying_acl());
    set_acl(&tree, ACL_DEFAULT, &granting_acl());
    set_acl(&tree.join("secret.txt"), ACL_ACCESS, &denying_acl());
    // the destination side of §1.2: an administrator's default ACL on the tree rcp writes into
    set_acl(dst_parent, ACL_DEFAULT, &granting_acl());
    tree
}

/// Assert the copy reproduced the fixture's ACLs exactly — both the ones it had to SET and the ones
/// it had to CLEAR.
fn assert_acl_fixture_copied(dst_tree: &std::path::Path) {
    let access = get_acl(dst_tree, ACL_ACCESS);
    assert_eq!(
        access.as_deref(),
        Some(denying_acl().as_slice()),
        "the source directory's access ACL did not survive the wire; got {}",
        describe_acl(access.as_ref())
    );
    let default = get_acl(dst_tree, ACL_DEFAULT);
    assert_eq!(
        default.as_deref(),
        Some(granting_acl().as_slice()),
        "the source directory's DEFAULT ACL did not survive the wire — dropping it silently changes \
         what every entry later created under the destination inherits; got {}",
        describe_acl(default.as_ref())
    );
    let secret = get_acl(&dst_tree.join("secret.txt"), ACL_ACCESS);
    assert_eq!(
        secret.as_deref(),
        Some(denying_acl().as_slice()),
        "the source file's access ACL did not survive the wire; without the named deny, uid 65534 \
         gains the read and execute that `other` grants and the source withheld. Got {}",
        describe_acl(secret.as_ref())
    );
    // and the clearing half: every entry whose source had no ACL must have none, even though each
    // was created under a directory whose default ACL it inherited.
    for rel in ["plain.txt", "nested", "nested/deep.txt"] {
        let path = dst_tree.join(rel);
        let got = get_acl(&path, ACL_ACCESS);
        assert_eq!(
            got,
            None,
            "{path:?} kept an inherited access ACL ({}); its source had none, so uid 65534 was \
             granted access the source never gave",
            describe_acl(got.as_ref())
        );
        let got_default = get_acl(&path, ACL_DEFAULT);
        assert_eq!(
            got_default,
            None,
            "{path:?} kept an inherited default ACL ({}), which would go on to widen anything \
             created under it later",
            describe_acl(got_default.as_ref())
        );
    }
    assert_eq!(get_file_content(&dst_tree.join("secret.txt")), "secret");
    assert_eq!(
        get_file_content(&dst_tree.join("nested/deep.txt")),
        "deeper"
    );
}

/// Local source, remote destination. Also the regression pin for `all+acl` remote copies being
/// possible at all: before ACLs were on the wire, the destination had no source ACLs to apply and
/// failed every entry rather than silently dropping them.
#[test]
fn test_remote_acl_round_trip_local_to_remote() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = build_acl_fixture(src_dir.path(), dst_dir.path());
    let dst_tree = dst_dir.path().join("tree");
    let dst_remote = format!("localhost:{}", dst_tree.to_str().unwrap());
    run_rcp_and_expect_success(&[
        "--preserve-settings=all+acl",
        src_tree.to_str().unwrap(),
        &dst_remote,
    ]);
    assert_acl_fixture_copied(&dst_tree);
}

/// Remote source, local destination — the other direction, so a read path that only worked when the
/// source happened to be the master's own side would fail here.
#[test]
fn test_remote_acl_round_trip_remote_to_local() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = build_acl_fixture(src_dir.path(), dst_dir.path());
    let dst_tree = dst_dir.path().join("tree");
    let src_remote = format!("localhost:{}", src_tree.to_str().unwrap());
    run_rcp_and_expect_success(&[
        "--preserve-settings=all+acl",
        &src_remote,
        dst_tree.to_str().unwrap(),
    ]);
    assert_acl_fixture_copied(&dst_tree);
}

/// Absolute path to `strace`, baked into the wrapper below so it does not depend on the PATH sshd
/// hands a non-interactive command.
fn strace_binary() -> String {
    let output = std::process::Command::new("sh")
        .args(["-c", "command -v strace"])
        .output()
        .expect("failed to look for strace");
    let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
    assert!(
        output.status.success() && !path.is_empty(),
        "cannot find strace. This test asserts on syscall COUNT rather than outcome, because the \
         whole point of putting a capture flag on the wire is that a copy without `acl` does not \
         pay the per-entry probe — an outcome-only check cannot see that regress. Install strace."
    );
    path
}

/// Run a remote copy with both rcpd processes under `strace`, and return the ACL-probe syscall
/// lines they issued.
///
/// `strace` on `rcp` cannot see this: the source and destination rcpds are started by sshd, not by
/// rcp, so they are in a different process tree entirely. `--rcpd-path` is the seam — it points
/// both spawns at a wrapper that execs the real rcpd under strace. One trace file per pid
/// (`rcpd.$$`) keeps the two rcpds, and the separate `--protocol-version` probe, from clobbering
/// each other's output.
fn count_rcpd_xattr_syscalls(args: &[&str]) -> Vec<String> {
    let strace = strace_binary();
    let scratch = tempfile::tempdir().unwrap();
    let traces = scratch.path().join("traces");
    std::fs::create_dir(&traces).unwrap();
    let wrapper = scratch.path().join("rcpd-under-strace");
    let rcpd = assert_cmd::cargo::cargo_bin("rcpd");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nexec {} -f -o \"{}/rcpd.$$\" \
             -e trace=getxattr,fgetxattr,lgetxattr,listxattr,flistxattr,llistxattr {} \"$@\"\n",
            strace,
            traces.display(),
            rcpd.display(),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let rcpd_path = format!("--rcpd-path={}", wrapper.display());
    let mut full = vec![rcpd_path.as_str()];
    full.extend_from_slice(args);
    run_rcp_and_expect_success(&full);
    let mut lines = Vec::new();
    for entry in std::fs::read_dir(&traces).unwrap() {
        let text = std::fs::read_to_string(entry.unwrap().path()).unwrap();
        lines.extend(
            text.lines()
                .filter(|l| l.contains("getxattr(") || l.contains("listxattr("))
                .map(str::to_string),
        );
    }
    lines
}

/// `--preserve-settings=all` must cost the SOURCE nothing in ACL probes on a remote copy.
///
/// This is the whole reason `MasterHello::Source` carries a capture field: the source is told
/// `preserve` by nobody (only the destination hello carries it), so without the flag it would have
/// to probe every entry unconditionally — a syscall per entry that `stat` cannot fold in, on every
/// remote copy including ones that do not want ACLs. Asserted on syscall count, because an
/// outcome-only check passes just as happily when the source probes and throws the answer away.
#[test]
fn test_remote_acl_off_issues_no_source_probe() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = src_dir.path().join("tree");
    std::fs::create_dir(&src_tree).unwrap();
    for i in 0..8 {
        create_test_file(&src_tree.join(format!("f{i}.txt")), "payload", 0o644);
    }
    set_acl(&src_tree.join("f0.txt"), ACL_ACCESS, &denying_acl());
    let plain_dst = dst_dir.path().join("plain");
    let plain_remote = format!("localhost:{}", plain_dst.to_str().unwrap());
    let src_remote = format!("localhost:{}", src_tree.to_str().unwrap());
    let traced =
        count_rcpd_xattr_syscalls(&["--preserve-settings=all", &src_remote, &plain_remote]);
    // exactly ONE ACL syscall for the whole run: the constant source-root probe behind the "this
    // copy drops the root's ACL" warning. The bound is on the CONSTANT rather than on zero, because
    // what must never come back is a probe that scales with the tree.
    assert!(
        traced.len() <= 1,
        "`all` made the rcpd processes issue {} ACL probe syscall(s) — more than the one constant \
         source-root probe, so every remote copy that does not want ACLs now pays per entry:\n{}",
        traced.len(),
        traced.join("\n")
    );
    assert!(
        traced.iter().all(|line| line.contains("/proc/self/fd/")),
        "the ACL syscall `all` issued is not the constant source-root probe (which goes through \
         the root handle's /proc/self/fd magic symlink):\n{}",
        traced.join("\n")
    );
    // `-L` uses a path-based adapter with its own directory-ACL gate, so the check
    // above says nothing about it: removing that gate leaves this assertion passing. Cover it here
    // rather than assume the two walks stay in step.
    let deref_dst = dst_dir.path().join("plain_deref");
    let deref_remote = format!("localhost:{}", deref_dst.to_str().unwrap());
    let traced = count_rcpd_xattr_syscalls(&[
        "--dereference",
        "--preserve-settings=all",
        &src_remote,
        &deref_remote,
    ]);
    assert!(
        traced.len() <= 1,
        "`all --dereference` made the rcpd processes issue {} ACL probe syscall(s) — more than the \
         one constant source-root probe; the `-L` walk has its own gate and must honor the capture \
         flag too:\n{}",
        traced.len(),
        traced.join("\n")
    );
    // and prove the counter is not vacuous: the same copy WITH `acl` must show the per-entry probe.
    // Without this the assertion above would also pass if the wrapper never traced anything at all.
    let acl_dst = dst_dir.path().join("with_acl");
    let acl_remote = format!("localhost:{}", acl_dst.to_str().unwrap());
    let traced =
        count_rcpd_xattr_syscalls(&["--preserve-settings=all+acl", &src_remote, &acl_remote]);
    assert!(
        traced.len() > 1,
        "`all+acl` issued {} ACL syscall(s) — no more than the constant root probe `all` pays, so \
         the counter proves nothing about `all`",
        traced.len()
    );
    assert_eq!(
        get_acl(&acl_dst.join("f0.txt"), ACL_ACCESS).as_deref(),
        Some(denying_acl().as_slice())
    );
    assert_eq!(get_acl(&plain_dst.join("f0.txt"), ACL_ACCESS), None);
}

/// Run `rcp` at the DEFAULT verbosity and return everything it wrote.
///
/// Every other test here goes through `run_rcp_with_args`, which pins `-vv`. That is right for
/// diagnosing a failure but wrong for this one: the whole question is whether a user who passed no
/// verbosity flag sees the notice, and `-vv` answers it for free by turning on `info` globally.
/// So this reimplements the two things that still matter — the timeout guard and `--force-remote`
/// for the `localhost:` operands — and nothing else.
fn run_rcp_at_default_verbosity(args: &[&str]) -> String {
    let rcp_path = assert_cmd::cargo::cargo_bin("rcp");
    let mut cmd = std::process::Command::new("timeout");
    cmd.args(["90", rcp_path.to_str().unwrap()]);
    cmd.arg("--force-remote");
    cmd.args(args);
    let output = cmd.output().expect("Failed to execute rcp command");
    assert_not_timeout(&output);
    assert!(
        output.status.success(),
        "rcp {args:?} failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The source-root ACL notice has to survive TWO filters and a wire hop: the root is on the source
/// host, so the probe runs in the source `rcpd`, whose own filter decides whether the notice is
/// even sent, and the master's filter then decides whether the forwarded line is rendered.
///
/// A local-only test cannot see any of that — `rcpd` is a child of sshd, and its log lines are
/// forwarded over a separate connection rather than printed. This runs at the DEFAULT verbosity
/// deliberately: at `-v` both filters pass everything and the test would hold even with the
/// dedicated tracing target removed from either end.
#[test]
fn test_remote_source_root_acl_warning_reaches_the_master() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = src_dir.path().join("tree");
    std::fs::create_dir(&src_tree).unwrap();
    create_test_file(&src_tree.join("f.txt"), "payload", 0o644);
    set_acl(&src_tree, ACL_ACCESS, &denying_acl());
    let src_remote = format!("localhost:{}", src_tree.to_str().unwrap());
    let warned_remote = format!(
        "localhost:{}",
        dst_dir.path().join("warned").to_str().unwrap()
    );
    let log =
        run_rcp_at_default_verbosity(&["--preserve-settings=all", &src_remote, &warned_remote]);
    assert!(
        log.contains("carries a POSIX ACL that this copy will NOT preserve"),
        "the source rcpd's root ACL notice never reached the master at the default verbosity, so a \
         remote user copying a tree whose root carries an ACL is told nothing:\n{log}"
    );
    assert!(
        log.contains("remote::source::"),
        "the notice must be tagged as coming from the SOURCE rcpd — if it is being emitted by the \
         master instead, it is probing the wrong host's filesystem:\n{log}"
    );
    // and silent when the copy does preserve them, so this is not just "rcpd logs something"
    let quiet_remote = format!(
        "localhost:{}",
        dst_dir.path().join("quiet").to_str().unwrap()
    );
    let log =
        run_rcp_at_default_verbosity(&["--preserve-settings=all+acl", &src_remote, &quiet_remote]);
    assert!(
        !log.contains("carries a POSIX ACL that this copy will NOT preserve"),
        "warned about an ACL the remote copy is preserving:\n{log}"
    );
}

/// A remote copy that asked for no preservation must cost the source NOTHING for a notice it does
/// not want: the arming flag rides in `capture`, so getting this wrong on the wire is invisible
/// locally — the master would stay silent while the source still paid for the probe.
#[test]
fn test_remote_copy_without_preserve_settings_issues_no_root_acl_probe() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = src_dir.path().join("tree");
    std::fs::create_dir(&src_tree).unwrap();
    create_test_file(&src_tree.join("f.txt"), "payload", 0o644);
    set_acl(&src_tree, ACL_ACCESS, &denying_acl());
    let src_remote = format!("localhost:{}", src_tree.to_str().unwrap());
    let bare_remote = format!(
        "localhost:{}",
        dst_dir.path().join("bare").to_str().unwrap()
    );
    let traced = count_rcpd_xattr_syscalls(&[&src_remote, &bare_remote]);
    assert!(
        traced.is_empty(),
        "a remote copy at the shipped default made the rcpd processes issue {} ACL syscall(s); it \
         asked for no preservation, so `capture` should have disarmed the root probe entirely:\n{}",
        traced.len(),
        traced.join("\n")
    );
    // the notice itself is likewise absent at the master end
    let log = run_rcp_at_default_verbosity(&[
        &src_remote,
        &format!(
            "localhost:{}",
            dst_dir.path().join("quiet").to_str().unwrap()
        ),
    ]);
    assert!(
        !log.contains("carries a POSIX ACL that this copy will NOT preserve"),
        "a remote copy that asked for no preservation still warned about a dropped ACL:\n{log}"
    );
}

/// `--require-toctou-safe` arms the notice on the source even when `capture` is all-false, because
/// it reaches the source `rcpd` as its own mirrored flag rather than through the capture struct.
///
/// This is the one case where an all-false `capture` does NOT mean the source touches no xattr, and
/// it is only observable across the wire: locally the two flags are read from the same process, so
/// a local test cannot tell "mirrored correctly" from "never needed mirroring".
#[test]
fn test_remote_strict_mode_arms_the_root_notice_on_an_all_false_capture() {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
        return;
    }
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // canonicalize: TMPDIR may contain symlinked components, which strict resolution refuses
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    let src_tree = src_base.join("tree");
    std::fs::create_dir(&src_tree).unwrap();
    create_test_file(&src_tree.join("f.txt"), "payload", 0o644);
    set_acl(&src_tree, ACL_ACCESS, &denying_acl());
    let src_remote = format!("localhost:{}", src_tree.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_base.join("strict").to_str().unwrap());
    // no --preserve-settings at all, so the master sends `capture` all-false
    let log = run_rcp_at_default_verbosity(&["--require-toctou-safe", &src_remote, &dst_remote]);
    assert!(
        log.contains("carries a POSIX ACL that this copy will NOT preserve"),
        "the strict flag did not arm the source's root notice, so a remote user who reached for \
         --require-toctou-safe — and may well assume it carries source ACLs across — is told \
         nothing:\n{log}"
    );
    assert!(
        log.contains("does not carry the SOURCE's"),
        "the notice reached the master with the generic wording, so the source rcpd did not know \
         it was running strict:\n{log}"
    );
}

/// `-L`/`--dereference` uses a path-based source adapter: it does not retain the
/// transient enumeration descriptor for the later ACL capture, so that read opens the directory by
/// path. Without this the dereference walk would quietly send no directory ACLs — and "no ACL" is a
/// request to CLEAR, so the destination would strip them rather than merely fail to copy them.
#[test]
fn test_remote_acl_round_trip_with_dereference() {
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = build_acl_fixture(src_dir.path(), dst_dir.path());
    let dst_tree = dst_dir.path().join("tree");
    let src_remote = format!("localhost:{}", src_tree.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_tree.to_str().unwrap());
    run_rcp_and_expect_success(&[
        "--dereference",
        "--preserve-settings=all+acl",
        &src_remote,
        &dst_remote,
    ]);
    assert_acl_fixture_copied(&dst_tree);
}

/// The remote reused-destination-directory lockdown, with `d:acl` ON — the case where the finalize
/// re-stat verify has to reconcile a mode that comes from two places at once.
///
/// A `--require-toctou-safe --overwrite --preserve-settings=all+acl` remote copy into a PRE-EXISTING
/// destination directory that carries its own ACLs. The source directory is SETGID and carries both
/// an access and a default ACL, which is what makes this the sharp case: the destination's special
/// bits come from the finalize chmod and its rwx bits from the source's ACL, and the verify checks
/// both against `masked_mode` in one comparison. That only works because the source's own mode's rwx
/// bits were themselves derived from that same ACL by the kernel — reasoning that is load-bearing
/// enough to want a test rather than a comment.
///
/// The lockdown's snapshot of the destination's original ACLs is DISCARDED here, because `d:acl`
/// asked for the source's: a copy preserving ACLs must not resurrect whatever the destination
/// happened to carry before. The reused dir starts at 0o500 so that a successful copy also proves
/// the lockdown fired at all — without the chmod to 0o700 the copier could not write into it.
#[test]
fn test_remote_strict_reused_dir_takes_the_source_acls_over_its_own() {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
        return;
    }
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    // canonicalize: TMPDIR itself may contain symlinked components, which strict resolution refuses
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    let src_tree = src_base.join("tree");
    std::fs::create_dir(&src_tree).unwrap();
    create_test_file(&src_tree.join("a.txt"), "payload", 0o644);
    std::fs::set_permissions(&src_tree, std::fs::Permissions::from_mode(0o2700)).unwrap();
    let access = denying_acl();
    let default = granting_acl();
    set_acl(&src_tree, ACL_ACCESS, &access);
    set_acl(&src_tree, ACL_DEFAULT, &default);
    assert_eq!(
        get_file_mode(&src_tree),
        0o2755,
        "fixture: the source keeps setgid while its access ACL sets the rwx bits"
    );
    // the pre-existing destination directory, with ACLs of its OWN that the copy must overwrite
    // rather than restore, and non-writable so the copy's success proves the lockdown fired
    let dst_tree = dst_base.join("tree");
    std::fs::create_dir(&dst_tree).unwrap();
    set_acl(&dst_tree, ACL_DEFAULT, &denying_acl());
    std::fs::set_permissions(&dst_tree, std::fs::Permissions::from_mode(0o500)).unwrap();
    let src_remote = format!("localhost:{}", src_tree.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_tree.to_str().unwrap());
    let output = run_rcp_and_expect_success(&[
        "--require-toctou-safe",
        "--overwrite",
        "--preserve-settings=all+acl",
        "--summary",
        &src_remote,
        &dst_remote,
    ]);
    // the child landed → the reused dir was made writable for the copy, then restored
    assert_eq!(get_file_content(&dst_tree.join("a.txt")), "payload");
    let summary = parse_summary_from_output(&output).expect("Failed to parse summary");
    assert_eq!(
        summary.directories_created, 0,
        "the destination directory was reused, not created"
    );
    assert_eq!(
        get_file_mode(&dst_tree),
        0o2755,
        "setgid from the finalize chmod, rwx from the source's ACL — the reused-directory verify \
         checks both against the source's mode in one comparison"
    );
    let got_access = get_acl(&dst_tree, ACL_ACCESS);
    assert_eq!(
        got_access.as_deref(),
        Some(access.as_slice()),
        "the source directory's access ACL did not survive lockdown + finalize; got {}",
        describe_acl(got_access.as_ref())
    );
    let got_default = get_acl(&dst_tree, ACL_DEFAULT);
    assert_eq!(
        got_default.as_deref(),
        Some(default.as_slice()),
        "the destination kept its OWN default ACL instead of taking the source's — with `d:acl` the \
         lockdown snapshot must be discarded, not restored; got {}",
        describe_acl(got_default.as_ref())
    );
    // the file was created inside the locked-down (stripped) directory, so it inherited nothing;
    // its source had no ACL and neither does it
    assert_eq!(get_acl(&dst_tree.join("a.txt"), ACL_ACCESS), None);
}

/// The remote mirror of the local `strict_mode_contains_and_restores_a_reused_directorys_acls`: the
/// `d:acl`-OFF branch, where the reused directory's OWN ACLs are what has to come back.
///
/// The remote destination restores through its own site (`DirectoryFinalization::execute`, threading the
/// lock into `DirectoryFinalization`) rather than the local copy's `finalize_dir`, so the two paths can
/// regress independently. As locally, the source directory's mode (`0o700`) differs from the mode
/// the destination's access ACL implies (`0o755`), so the final mode says unambiguously which one
/// won.
#[test]
fn test_remote_strict_reused_dir_restores_its_own_acls() {
    if !common::safedir::openat2_available() {
        eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
        return;
    }
    require_local_ssh();
    let (src_dir, dst_dir) = setup_test_env();
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    // source with no ACLs at all, so every ACL seen on the destination is the destination's own
    let src_tree = src_base.join("tree");
    std::fs::create_dir(&src_tree).unwrap();
    create_test_file(&src_tree.join("a.txt"), "payload", 0o644);
    std::fs::set_permissions(&src_tree, std::fs::Permissions::from_mode(0o700)).unwrap();
    let dst_tree = dst_base.join("tree");
    std::fs::create_dir(&dst_tree).unwrap();
    let access = denying_acl();
    let default = granting_acl();
    set_acl(&dst_tree, ACL_ACCESS, &access);
    set_acl(&dst_tree, ACL_DEFAULT, &default);
    assert_eq!(
        get_file_mode(&dst_tree),
        0o755,
        "fixture: mode comes from the ACL"
    );
    let src_remote = format!("localhost:{}", src_tree.to_str().unwrap());
    let dst_remote = format!("localhost:{}", dst_tree.to_str().unwrap());
    run_rcp_and_expect_success(&[
        "--require-toctou-safe",
        "--overwrite",
        &src_remote,
        &dst_remote,
    ]);
    // containment: the file was created inside the stripped directory, so it inherited nothing
    let child = get_acl(&dst_tree.join("a.txt"), ACL_ACCESS);
    assert_eq!(
        child,
        None,
        "the child inherited the reused directory's default ACL ({}) — the remote lockdown must \
         strip it, not merely restrict the mode",
        describe_acl(child.as_ref())
    );
    // restore: the directory's own ACLs came back, and its mode is the SOURCE's
    let got_default = get_acl(&dst_tree, ACL_DEFAULT);
    assert_eq!(
        got_default.as_deref(),
        Some(default.as_slice()),
        "the reused directory permanently lost the default ACL it had before the copy; got {}",
        describe_acl(got_default.as_ref())
    );
    let got_access = get_acl(&dst_tree, ACL_ACCESS);
    assert!(
        got_access.is_some(),
        "the reused directory's own access ACL was not put back at all"
    );
    assert_eq!(
        get_file_mode(&dst_tree),
        0o700,
        "the reused directory must end at the SOURCE mode; restoring its access ACL after the \
         finalize chmod instead of before would leave it at its own original 0o755"
    );
    assert_eq!(get_file_content(&dst_tree.join("a.txt")), "payload");
}
