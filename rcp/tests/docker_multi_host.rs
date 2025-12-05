//! Multi-host integration tests using Docker containers.
//!
//! These tests use Docker containers to simulate multiple hosts with SSH connectivity, testing RCP's remote copy functionality across separate machines.
//!
//! ## Prerequisites
//!
//! Before running these tests:
//! 1. Install Docker and docker-compose
//! 2. Start the test containers: `cd tests/docker && ./test-helpers.sh start`
//! 3. Run tests: `cargo test --test docker_multi_host -- --ignored`
//!
//! ## Test Organization
//!
//! - Basic copy tests: Single file, simple scenarios
//! - Role ordering tests: Connection timing and role assignment
//! - Error handling tests: Permission errors, network issues
//!
//! See `docs/testing_strategy.md` for overall approach.

mod support;

use support::docker_env::{DockerEnv, Result};

/// Test basic file copy between two remote hosts
#[test]
#[ignore = "requires Docker containers (run: cd tests/docker && ./test-helpers.sh start)"]
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
#[ignore = "requires Docker containers (run: cd tests/docker && ./test-helpers.sh start)"]
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
#[ignore = "requires Docker containers (run: cd tests/docker && ./test-helpers.sh start)"]
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
#[ignore = "requires Docker containers (run: cd tests/docker && ./test-helpers.sh start)"]
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
#[ignore = "requires Docker containers (run: cd tests/docker && ./test-helpers.sh start)"]
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
#[ignore = "requires Docker containers (run: cd tests/docker && ./test-helpers.sh start)"]
fn test_progress_reporting_multi_host() -> Result<()> {
    let env = DockerEnv::new()?;
    // cleanup any existing directories from previous runs first (as root for permissions)
    env.exec("host-a", None, &["rm", "-rf", "/tmp/progress_test"])?;
    env.exec("host-b", None, &["rm", "-rf", "/tmp/progress_test_dst"])?;
    // create many small files on host-a (1000 x 1KB) using shell loop
    // this ensures copy takes long enough for progress updates to be captured
    let setup_output = env.exec(
        "host-a",
        Some("testuser"),
        &[
            "sh",
            "-c",
            "mkdir -p /tmp/progress_test && cd /tmp/progress_test && for i in $(seq 1 1000); do dd if=/dev/zero of=file_$i.bin bs=1024 count=1 2>/dev/null; done",
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
