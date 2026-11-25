//! Multi-host integration tests using Docker containers.
//!
//! These tests use Docker containers to simulate multiple hosts with SSH connectivity, testing RCP's remote copy functionality across separate machines.
//!
//! ## Prerequisites
//!
//! From the repository root, run the full lifecycle with `just docker-test`. For repeated focused
//! runs, use `just docker-up`, then
//! `just docker-test-only -E 'binary(docker_multi_host)'`, and finish with `just docker-down`.
//!
//! ## Test Organization
//!
//! - Basic copy tests: Single file, simple scenarios
//! - Role ordering tests: Connection timing and role assignment
//! - Error handling tests: Permission errors, network issues
//!
//! See `docs/testing.md` for overall approach.

mod support;

use support::docker_env::{DockerEnv, Result};

fn helper_file_path(label: &str) -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after the Unix epoch")
        .as_nanos();
    format!("/tmp/rcp-helper-{timestamp}-{label}")
}

#[test]
#[ignore = "requires Docker containers to be running"]
fn creates_docker_environment() {
    let env = DockerEnv::new();
    assert!(env.is_ok(), "Failed to create Docker environment");
}

#[test]
#[ignore = "requires Docker containers to be running"]
fn round_trips_container_file() {
    let env = DockerEnv::new().expect("Failed to create environment");
    let test_file = helper_file_path("text");
    // write a file
    env.write_file("host-a", &test_file, b"test content")
        .expect("Failed to write file");
    // check it exists
    assert!(
        env.file_exists("host-a", &test_file)
            .expect("Failed to check file"),
        "File should exist"
    );
    // read it back
    let content = env
        .read_file("host-a", &test_file)
        .expect("Failed to read file");
    assert_eq!(content, b"test content", "Content mismatch");
    // clean up
    env.remove_file("host-a", &test_file)
        .expect("Failed to remove file");
    assert!(
        !env.file_exists("host-a", &test_file)
            .expect("Failed to check removed file"),
        "Removed file should be absent"
    );
    env.remove_file("host-a", &test_file)
        .expect("Removing an absent file should succeed");
}

#[test]
#[ignore = "requires Docker containers to be running"]
fn round_trips_binary_container_file() -> Result<()> {
    let env = DockerEnv::new()?;
    let path = helper_file_path("binary");
    // include every byte value and exceed Linux's single-argument size limit
    let content: Vec<u8> = (0..=255).cycle().take(256 * 1024).collect();
    env.write_file("host-a", &path, &content)?;
    assert_eq!(env.read_file("host-a", &path)?, content);
    env.remove_file("host-a", &path)?;
    assert!(!env.file_exists("host-a", &path)?);
    Ok(())
}

#[test]
#[ignore = "requires Docker containers to be running"]
fn round_trips_non_utf8_container_file() -> Result<()> {
    let env = DockerEnv::new()?;
    let path = helper_file_path("non-utf8");
    let content = b"before\xff\x80after";
    env.write_file("host-a", &path, content)?;
    assert_eq!(env.read_file("host-a", &path)?, content);
    env.remove_file("host-a", &path)?;
    Ok(())
}

#[test]
#[ignore = "requires Docker containers to be running"]
fn round_trips_container_file_with_shell_characters() -> Result<()> {
    let env = DockerEnv::new()?;
    let path = helper_file_path("space ' \"$;?.txt");
    let content = b"literal ' \" $ and trailing newline\n";
    env.write_file("host-a", &path, content)?;
    assert!(env.file_exists("host-a", &path)?);
    assert_eq!(env.read_file("host-a", &path)?, content);
    env.remove_file("host-a", &path)?;
    assert!(!env.file_exists("host-a", &path)?);
    Ok(())
}

#[test]
#[ignore = "requires Docker containers to be running"]
fn round_trips_dash_named_container_file() -> Result<()> {
    let env = DockerEnv::new()?;
    let path = "-";
    let content = b"a literal dash is a filename";
    assert!(!env.file_exists("host-a", path)?);
    env.write_file("host-a", path, content)?;
    assert!(env.file_exists("host-a", path)?);
    let actual = env.read_file("host-a", path)?;
    env.remove_file("host-a", path)?;
    assert!(!env.file_exists("host-a", path)?);
    assert_eq!(actual, content);
    Ok(())
}

#[test]
#[ignore = "requires Docker containers to be running"]
fn file_removal_reports_command_failure() -> Result<()> {
    let env = DockerEnv::new()?;
    let error = env
        .remove_file("host-a", "/tmp")
        .expect_err("rm must reject a directory");
    assert!(
        error.to_string().contains("/tmp"),
        "Missing path in error: {error:#}"
    );
    Ok(())
}

#[test]
#[ignore = "requires Docker containers to be running"]
fn file_operations_report_unavailable_container() -> Result<()> {
    let env = DockerEnv::new()?;
    let error = env
        .file_exists("missing-helper-container", "/tmp/file")
        .expect_err("Docker failure must not look like an absent file");
    assert!(
        error.to_string().contains("missing-helper-container"),
        "Missing container in error: {error:#}"
    );
    env.remove_file("missing-helper-container", "/tmp/file")
        .expect_err("Docker failure must not look like successful removal");
    Ok(())
}

#[test]
#[ignore = "requires Docker containers to be running"]
fn file_write_reports_command_failure() -> Result<()> {
    let env = DockerEnv::new()?;
    let error = env
        .write_file("host-a", "/tmp", b"cannot overwrite a directory")
        .expect_err("Writing over a directory must fail");
    assert!(
        error.to_string().contains("/tmp"),
        "Missing path in error: {error:#}"
    );
    Ok(())
}

/// Test basic file copy between two remote hosts
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_basic_multi_host_copy() -> Result<()> {
    let env = DockerEnv::new()?;
    // create test file on host-a
    let test_content = b"Hello from multi-host test";
    env.write_file("host-a", "/tmp/test-basic.txt", test_content)?;
    // copy from host-a to host-b using rcp
    let output = env.exec_rcp(&["host-a:/tmp/test-basic.txt", "host-b:/tmp/test-basic.txt"])?;
    // check command succeeded
    if !output.status.success() {
        eprintln!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        panic!("rcp command failed");
    }
    // verify file exists on destination
    assert!(
        env.file_exists("host-b", "/tmp/test-basic.txt")?,
        "File should exist on host-b"
    );
    // verify content matches
    let copied_content = env.read_file("host-b", "/tmp/test-basic.txt")?;
    assert_eq!(
        test_content,
        copied_content.as_slice(),
        "File content mismatch"
    );
    // cleanup
    env.remove_file("host-a", "/tmp/test-basic.txt")?;
    env.remove_file("host-b", "/tmp/test-basic.txt")?;
    Ok(())
}

/// Test that copying same file twice requires --overwrite flag
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_overwrite_protection() -> Result<()> {
    let env = DockerEnv::new()?;
    // create test file
    env.write_file("host-a", "/tmp/test-overwrite.txt", b"original")?;
    // first copy should succeed
    let output = env.exec_rcp(&[
        "host-a:/tmp/test-overwrite.txt",
        "host-b:/tmp/test-overwrite.txt",
    ])?;
    assert!(output.status.success(), "First copy should succeed");
    // second copy without --overwrite should fail
    let output = env.exec_rcp(&[
        "host-a:/tmp/test-overwrite.txt",
        "host-b:/tmp/test-overwrite.txt",
    ])?;
    assert!(
        !output.status.success(),
        "Second copy without --overwrite should fail"
    );
    // verify error message mentions overwrite
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined_output = format!("stdout:\n{}\nstderr:\n{}", stdout, stderr);
    assert!(
        combined_output.contains("overwrite") || combined_output.contains("already exists"),
        "Error should mention overwrite requirement. Actual output:\n{}",
        combined_output
    );
    // copy with --overwrite should succeed
    let output = env.exec_rcp(&[
        "--overwrite",
        "host-a:/tmp/test-overwrite.txt",
        "host-b:/tmp/test-overwrite.txt",
    ])?;
    assert!(
        output.status.success(),
        "Copy with --overwrite should succeed"
    );
    // cleanup
    env.remove_file("host-a", "/tmp/test-overwrite.txt")?;
    env.remove_file("host-b", "/tmp/test-overwrite.txt")?;
    Ok(())
}

/// Test directory copy between hosts
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_directory_copy() -> Result<()> {
    let env = DockerEnv::new()?;
    // cleanup any existing directories from previous runs first (as root for permissions)
    env.exec("host-a", None, &["rm", "-rf", "/tmp/test-dir"])?;
    env.exec("host-b", None, &["rm", "-rf", "/tmp/test-dir-copy"])?;
    // create directory with files on host-a (as testuser)
    env.exec(
        "host-a",
        Some("testuser"),
        &["mkdir", "-p", "/tmp/test-dir"],
    )?;
    env.write_file("host-a", "/tmp/test-dir/file1.txt", b"content1")?;
    env.write_file("host-a", "/tmp/test-dir/file2.txt", b"content2")?;
    // create destination directory on host-b (as testuser to match rcp permissions)
    env.exec(
        "host-b",
        Some("testuser"),
        &["mkdir", "-p", "/tmp/test-dir-copy"],
    )?;
    // copy directory from host-a to host-b
    let output = env.exec_rcp(&["host-a:/tmp/test-dir/", "host-b:/tmp/test-dir-copy/"])?;
    if !output.status.success() {
        eprintln!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        panic!("Directory copy failed");
    }
    // verify files exist on destination (rcp creates test-dir subdirectory)
    assert!(
        env.file_exists("host-b", "/tmp/test-dir-copy/test-dir/file1.txt")?,
        "file1.txt should exist"
    );
    assert!(
        env.file_exists("host-b", "/tmp/test-dir-copy/test-dir/file2.txt")?,
        "file2.txt should exist"
    );
    // verify content
    let content1 = env.read_file("host-b", "/tmp/test-dir-copy/test-dir/file1.txt")?;
    assert_eq!(b"content1", content1.as_slice());
    let content2 = env.read_file("host-b", "/tmp/test-dir-copy/test-dir/file2.txt")?;
    assert_eq!(b"content2", content2.as_slice());
    // cleanup
    env.exec("host-a", None, &["rm", "-rf", "/tmp/test-dir"])?;
    env.exec("host-b", None, &["rm", "-rf", "/tmp/test-dir-copy"])?;
    Ok(())
}

/// Test that verbose output shows role assignment
///
/// This verifies the fix for the role-matching bug where source/destination
/// roles could be swapped if rcpd connections arrived out of order.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_role_assignment_logging() -> Result<()> {
    let env = DockerEnv::new()?;
    // create test file
    env.write_file("host-a", "/tmp/test-roles.txt", b"test")?;
    // run with verbose output to see role assignment
    let output = env.exec_rcp(&[
        "-vv",
        "host-a:/tmp/test-roles.txt",
        "host-b:/tmp/test-roles.txt",
    ])?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stdout_lower = stdout.to_lowercase();
    // verify we see role assignment in logs
    // the actual implementation logs which rcpd is source vs destination
    assert!(
        stdout_lower.contains("role")
            || stdout_lower.contains("source")
            || stdout_lower.contains("destination"),
        "Verbose output should show role information. stdout (first 1000 chars):\n{}",
        &stdout[..std::cmp::min(1000, stdout.len())]
    );
    // verify copy succeeded
    assert!(output.status.success(), "Copy should succeed");
    assert!(
        env.file_exists("host-b", "/tmp/test-roles.txt")?,
        "File should exist on destination"
    );
    // cleanup
    env.remove_file("host-a", "/tmp/test-roles.txt")?;
    env.remove_file("host-b", "/tmp/test-roles.txt")?;
    Ok(())
}

/// Test copying a nonexistent file produces clear error
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_nonexistent_source_file() -> Result<()> {
    let env = DockerEnv::new()?;
    // try to copy nonexistent file
    let output = env.exec_rcp(&[
        "host-a:/tmp/does-not-exist.txt",
        "host-b:/tmp/destination.txt",
    ])?;
    // should fail
    assert!(!output.status.success(), "Should fail for nonexistent file");
    // error should be clear
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);
    assert!(
        combined.contains("No such file")
            || combined.contains("does-not-exist")
            || combined.contains("not found"),
        "Error should mention missing file. Output:\n{}",
        combined
    );
    Ok(())
}

/// Test that progress reporting works correctly in multi-host copies.
///
/// This verifies that:
/// - Progress updates are received from rcpd processes on remote hosts
/// - The tracing TCP connections work correctly across network boundaries
/// - Progress output shows non-zero file counts
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_progress_reporting_multi_host() -> Result<()> {
    let env = DockerEnv::new()?;
    // cleanup any existing directories from previous runs first (as root for permissions)
    env.exec("host-a", None, &["rm", "-rf", "/tmp/progress_test"])?;
    env.exec("host-b", None, &["rm", "-rf", "/tmp/progress_test_dst"])?;
    // create three one-chunk files. The I/O throttle below lets one file complete while later files
    // remain gated, making a non-zero in-flight progress snapshot deterministic.
    let setup_output = env.exec(
        "host-a",
        Some("testuser"),
        &[
            "sh",
            "-c",
            "mkdir -p /tmp/progress_test && cd /tmp/progress_test && for i in $(seq 1 3); do dd if=/dev/zero of=file_$i.bin bs=1024 count=1 2>/dev/null; done",
        ],
    )?;
    assert!(
        setup_output.status.success(),
        "file creation should succeed: {}",
        String::from_utf8_lossy(&setup_output.stderr)
    );
    // create destination directory on host-b (as testuser to match rcp permissions)
    env.exec(
        "host-b",
        Some("testuser"),
        &["mkdir", "-p", "/tmp/progress_test_dst"],
    )?;
    // copy with progress enabled
    let output = env.exec_rcp(&[
        "--progress",
        "--progress-type=text-updates",
        "--progress-delay=100ms",
        "--chunk-size=1KiB",
        "--iops-throttle=1",
        "host-a:/tmp/progress_test/",
        "host-b:/tmp/progress_test_dst/",
    ])?;
    // check command succeeded
    if !output.status.success() {
        eprintln!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        panic!("rcp command failed");
    }
    // check stderr for progress output - should contain progress updates with file counts
    let stderr = String::from_utf8_lossy(&output.stderr);
    // progress output should contain the separator lines
    assert!(
        stderr.contains("======================="),
        "Progress output should contain separator lines. stderr:\n{}",
        stderr
    );
    // progress should show that files were copied (files: followed by non-zero count)
    let has_files_progress = stderr.lines().any(|line| {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("files:") {
            rest.trim().parse::<u64>().map(|n| n > 0).unwrap_or(false)
        } else {
            false
        }
    });
    assert!(
        has_files_progress,
        "Progress output should show files being copied (files: N where N > 0). stderr:\n{}",
        stderr
    );
    // cleanup
    let _ = env.exec("host-a", None, &["rm", "-rf", "/tmp/progress_test"]);
    let _ = env.exec("host-b", None, &["rm", "-rf", "/tmp/progress_test_dst"]);
    eprintln!("✓ Progress reporting works correctly in multi-host setup");
    Ok(())
}

/// Test SSH data transport across real separate containers: download (-L), upload (-R),
/// 3-host direct (host-b -> host-a), and 3-host with `--ssh-forward-agent` when host-b lacks
/// a private key to host-a.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_multi_host_ssh_transport_and_agent_forwarding() -> Result<()> {
    struct RestoreHostBKey<'a>(&'a DockerEnv);
    impl Drop for RestoreHostBKey<'_> {
        fn drop(&mut self) {
            let _ = self.0.exec(
                "host-b",
                Some("testuser"),
                &[
                    "sh",
                    "-c",
                    "if [ -f ~/.ssh/id_ed25519.bak ]; then mv -f ~/.ssh/id_ed25519.bak ~/.ssh/id_ed25519; fi",
                ],
            );
        }
    }
    let env = DockerEnv::new()?;
    let payload = b"multi-host ssh transport payload";
    // 1. Download (host-a -> master) over -L
    env.write_file("host-a", "/tmp/ssh-dl-src.txt", payload)?;
    let _ = env.remove_file("master", "/tmp/ssh-dl-dst.txt");
    let out = env.exec_rcp(&[
        "--remote-transport=ssh",
        "--ssh-tunnels=2",
        "host-a:/tmp/ssh-dl-src.txt",
        "/tmp/ssh-dl-dst.txt",
    ])?;
    assert!(
        out.status.success(),
        "SSH download failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(env.read_file("master", "/tmp/ssh-dl-dst.txt")?, payload);
    // 2. Upload (master -> host-b) over -R
    env.write_file("master", "/tmp/ssh-ul-src.txt", payload)?;
    let _ = env.remove_file("host-b", "/tmp/ssh-ul-dst.txt");
    let out = env.exec_rcp(&[
        "--remote-transport=ssh",
        "--ssh-tunnels=2",
        "/tmp/ssh-ul-src.txt",
        "host-b:/tmp/ssh-ul-dst.txt",
    ])?;
    assert!(
        out.status.success(),
        "SSH upload failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(env.read_file("host-b", "/tmp/ssh-ul-dst.txt")?, payload);
    // 3. 3-Host (host-a -> host-b) using host-b's own SSH key
    let _ = env.remove_file("host-b", "/tmp/ssh-3h-dst.txt");
    let out = env.exec_rcp(&[
        "--remote-transport=ssh",
        "--ssh-tunnels=2",
        "host-a:/tmp/ssh-dl-src.txt",
        "host-b:/tmp/ssh-3h-dst.txt",
    ])?;
    assert!(
        out.status.success(),
        "SSH 3-host failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(env.read_file("host-b", "/tmp/ssh-3h-dst.txt")?, payload);
    // 4. 3-Host without a key on host-b: fails with guidance unless --ssh-forward-agent is used
    let _restore_key = RestoreHostBKey(&env);
    let moved = env.exec(
        "host-b",
        Some("testuser"),
        &[
            "mv",
            "/home/testuser/.ssh/id_ed25519",
            "/home/testuser/.ssh/id_ed25519.bak",
        ],
    )?;
    assert!(moved.status.success(), "failed to move host-b key aside");
    let _ = env.remove_file("host-b", "/tmp/ssh-3h-noagent.txt");
    let failed = env.exec_rcp(&[
        "--remote-transport=ssh",
        "--ssh-tunnels=1",
        "host-a:/tmp/ssh-dl-src.txt",
        "host-b:/tmp/ssh-3h-noagent.txt",
    ])?;
    let combined_fail = format!(
        "{}\n{}",
        String::from_utf8_lossy(&failed.stdout),
        String::from_utf8_lossy(&failed.stderr)
    );
    support::docker_env::assert_clean_failure(
        failed.status,
        &combined_fail,
        "3-host SSH without key or forwarded agent",
    );
    assert!(
        combined_fail.contains("--ssh-forward-agent"),
        "expected actionable 3-host guidance mentioning --ssh-forward-agent:\n{combined_fail}"
    );
    // now start an ssh-agent on master and pass --ssh-forward-agent: host-b authenticates to host-a
    // solely through the forwarded agent
    let _ = env.remove_file("host-b", "/tmp/ssh-3h-agent.txt");
    let agent_out = env.exec(
        "master",
        Some("testuser"),
        &[
            "bash",
            "-c",
            "eval \"$(ssh-agent -s)\" >/dev/null && \
             ssh-add ~/.ssh/id_ed25519 >/dev/null 2>&1 && \
             /home/testuser/.local/bin/rcp -vv --remote-transport=ssh --ssh-forward-agent --ssh-tunnels=2 \
               host-a:/tmp/ssh-dl-src.txt host-b:/tmp/ssh-3h-agent.txt; \
             rc=$?; ssh-agent -k >/dev/null 2>&1 || true; exit $rc",
        ],
    )?;
    assert!(
        agent_out.status.success(),
        "3-host SSH with --ssh-forward-agent failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&agent_out.stdout),
        String::from_utf8_lossy(&agent_out.stderr)
    );
    assert_eq!(env.read_file("host-b", "/tmp/ssh-3h-agent.txt")?, payload);
    // 5. 3-Host without --ssh-forward-agent, relying on ~/.ssh/config `ForwardAgent yes`
    //    (AgentForwardMode::InheritConfig)
    struct RestoreMasterSshConfig<'a>(&'a DockerEnv);
    impl Drop for RestoreMasterSshConfig<'_> {
        fn drop(&mut self) {
            let _ = self.0.exec(
                "master",
                Some("testuser"),
                &[
                    "bash",
                    "-c",
                    "if [ -f /home/testuser/.ssh/config.bak ]; then \
                       mv -f /home/testuser/.ssh/config.bak /home/testuser/.ssh/config; \
                     fi",
                ],
            );
        }
    }
    let _restore_config = RestoreMasterSshConfig(&env);
    let enable_cfg = env.exec(
        "master",
        Some("testuser"),
        &[
            "bash",
            "-c",
            "cp /home/testuser/.ssh/config /home/testuser/.ssh/config.bak && \
             { printf 'Host host-b\\n    ForwardAgent yes\\n\\n'; cat /home/testuser/.ssh/config.bak; } \
               > /home/testuser/.ssh/config && \
             chmod 600 /home/testuser/.ssh/config",
        ],
    )?;
    assert!(
        enable_cfg.status.success(),
        "failed to enable ForwardAgent in master ~/.ssh/config: {}",
        String::from_utf8_lossy(&enable_cfg.stderr)
    );
    let _ = env.remove_file("host-b", "/tmp/ssh-3h-inherit-agent.txt");
    let inherit_out = env.exec(
        "master",
        Some("testuser"),
        &[
            "bash",
            "-c",
            "eval \"$(ssh-agent -s)\" >/dev/null && \
             ssh-add ~/.ssh/id_ed25519 >/dev/null 2>&1 && \
             /home/testuser/.local/bin/rcp -vv --remote-transport=ssh --ssh-tunnels=2 \
               host-a:/tmp/ssh-dl-src.txt host-b:/tmp/ssh-3h-inherit-agent.txt; \
             rc=$?; ssh-agent -k >/dev/null 2>&1 || true; exit $rc",
        ],
    )?;
    assert!(
        inherit_out.status.success(),
        "3-host SSH with ~/.ssh/config ForwardAgent failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&inherit_out.stdout),
        String::from_utf8_lossy(&inherit_out.stderr)
    );
    assert_eq!(
        env.read_file("host-b", "/tmp/ssh-3h-inherit-agent.txt")?,
        payload
    );
    drop(_restore_config);
    drop(_restore_key);
    let _ = env.remove_file("host-a", "/tmp/ssh-dl-src.txt");
    let _ = env.remove_file("master", "/tmp/ssh-dl-dst.txt");
    let _ = env.remove_file("master", "/tmp/ssh-ul-src.txt");
    let _ = env.remove_file("host-b", "/tmp/ssh-ul-dst.txt");
    let _ = env.remove_file("host-b", "/tmp/ssh-3h-dst.txt");
    let _ = env.remove_file("host-b", "/tmp/ssh-3h-agent.txt");
    let _ = env.remove_file("host-b", "/tmp/ssh-3h-inherit-agent.txt");
    eprintln!(
        "✓ Multi-host SSH transport (download, upload, 3-host, --ssh-forward-agent, InheritConfig) passed"
    );
    Ok(())
}
