//! I/O chaos tests for filesystem error simulation.
//!
//! These tests verify rcp's behavior when filesystem operations fail,
//! including disk full (ENOSPC), permission denied, and other I/O errors.
//!
//! ## Prerequisites
//!
//! Before running these tests:
//! 1. Start containers: `just docker-up`
//! 2. Run: `cargo nextest run --profile docker --run-ignored only -E 'test(~chaos)'`
//!
//! ## Test Naming Convention
//!
//! All chaos tests include "chaos" in their name for easy filtering.

mod support;

use support::docker_env::{DockerEnv, Result};

/// Guard that ensures cleanup always runs, even on early return or panic.
struct IoCleanupGuard<'a> {
    env: &'a DockerEnv,
    tmpfs_mounts: Vec<(String, String)>,
    files_to_remove: Vec<(&'a str, String)>,
    dirs_to_remove: Vec<(&'a str, String)>,
    permissions_to_restore: Vec<(&'a str, String, String)>,
}

impl<'a> IoCleanupGuard<'a> {
    fn new(env: &'a DockerEnv) -> Self {
        Self {
            env,
            tmpfs_mounts: Vec::new(),
            files_to_remove: Vec::new(),
            dirs_to_remove: Vec::new(),
            permissions_to_restore: Vec::new(),
        }
    }
    fn unmount_tmpfs(mut self, container: &str, path: String) -> Self {
        self.tmpfs_mounts.push((container.to_string(), path));
        self
    }
    fn remove_file(mut self, container: &'a str, path: String) -> Self {
        self.files_to_remove.push((container, path));
        self
    }
    fn remove_dir(mut self, container: &'a str, path: String) -> Self {
        self.dirs_to_remove.push((container, path));
        self
    }
    fn restore_permission(mut self, container: &'a str, path: String, mode: String) -> Self {
        self.permissions_to_restore.push((container, path, mode));
        self
    }
}

impl Drop for IoCleanupGuard<'_> {
    fn drop(&mut self) {
        // restore permissions first (so we can delete files)
        for (container, path, mode) in &self.permissions_to_restore {
            let _ = self.env.chmod(container, path, mode);
        }
        // remove files
        for (container, path) in &self.files_to_remove {
            let _ = self.env.remove_file(container, path);
        }
        // unmount tmpfs BEFORE removing directories (umount fails if mount point is removed)
        for (container, path) in &self.tmpfs_mounts {
            let _ = self.env.unmount_tmpfs(container, path);
        }
        // remove directories (including mount points, now unmounted)
        for (container, path) in &self.dirs_to_remove {
            let _ = self.env.exec(container, None, &["rm", "-rf", path]);
        }
    }
}

/// Test that ENOSPC (disk full) error is reported clearly.
///
/// This test creates a small tmpfs on the destination, then tries to copy
/// a file larger than the available space.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_io_enospc_reports_error() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let tmpfs_path = format!("/tmp/chaos-small-{}", timestamp);
    let src_path = format!("/tmp/chaos-enospc-src-{}.bin", timestamp);
    let dst_path = format!("{}/dst.bin", tmpfs_path);
    // set up cleanup guard
    let _guard = IoCleanupGuard::new(&env)
        .unmount_tmpfs("host-b", tmpfs_path.clone())
        .remove_file("host-a", src_path.clone())
        .remove_dir("host-b", tmpfs_path.clone());
    // create a small tmpfs (512KB) on destination
    env.mount_tmpfs("host-b", &tmpfs_path, 512)?;
    // create a source file larger than the tmpfs (1MB)
    env.create_file_with_size("host-a", &src_path, 1024)?;
    // attempt to copy - should fail with ENOSPC
    let output = env.exec_rcp(&[
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ])?;
    // verify copy failed
    assert!(
        !output.status.success(),
        "copy should fail when destination disk is full"
    );
    // verify error message mentions the root cause
    // check both stdout (verbose logs) and stderr for the error
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{}\n{}", stdout, stderr);
    assert!(
        combined.contains("No space left on device") || combined.contains("ENOSPC"),
        "error should mention 'No space left on device' somewhere in output, got stderr: {}",
        stderr
    );
    eprintln!("✓ ENOSPC error reporting test passed");
    Ok(())
}

/// Test that permission denied on destination directory is reported clearly.
///
/// This test removes write permission from the destination directory.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_io_permission_denied_destination() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/chaos-perm-src-{}.txt", timestamp);
    let dst_dir = format!("/tmp/chaos-perm-dst-{}", timestamp);
    let dst_path = format!("{}/file.txt", dst_dir);
    // set up cleanup guard - restore permissions before removing
    let _guard = IoCleanupGuard::new(&env)
        .restore_permission("host-b", dst_dir.clone(), "755".to_string())
        .remove_file("host-a", src_path.clone())
        .remove_dir("host-b", dst_dir.clone());
    // create source file
    env.write_file("host-a", &src_path, b"test content")?;
    // create destination directory and remove write permission
    env.exec("host-b", Some("testuser"), &["mkdir", "-p", &dst_dir])?;
    env.chmod("host-b", &dst_dir, "555")?;
    // attempt to copy - should fail with permission denied
    let output = env.exec_rcp(&[
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ])?;
    // verify copy failed
    assert!(
        !output.status.success(),
        "copy should fail when destination is not writable"
    );
    // verify error message mentions permission denied
    // check both stdout (verbose logs) and stderr for the error
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{}\n{}", stdout, stderr);
    assert!(
        combined.contains("Permission denied") || combined.contains("permission denied"),
        "error should mention 'Permission denied' somewhere in output, got stderr: {}",
        stderr
    );
    eprintln!("✓ Permission denied (destination) error reporting test passed");
    Ok(())
}

/// Test that permission denied on source file is reported clearly.
///
/// This test removes read permission from the source file.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_io_permission_denied_source() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/chaos-perm-src-{}.txt", timestamp);
    let dst_path = format!("/tmp/chaos-perm-dst-{}.txt", timestamp);
    // set up cleanup guard - restore permissions before removing
    let _guard = IoCleanupGuard::new(&env)
        .restore_permission("host-a", src_path.clone(), "644".to_string())
        .remove_file("host-a", src_path.clone())
        .remove_file("host-b", dst_path.clone());
    // create source file and remove read permission
    env.write_file("host-a", &src_path, b"test content")?;
    env.chmod("host-a", &src_path, "000")?;
    // attempt to copy - should fail with permission denied
    let output = env.exec_rcp(&[
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ])?;
    // verify copy failed
    assert!(
        !output.status.success(),
        "copy should fail when source is not readable"
    );
    // verify error message mentions permission denied
    // check both stdout (verbose logs) and stderr for the error
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{}\n{}", stdout, stderr);
    assert!(
        combined.contains("Permission denied") || combined.contains("permission denied"),
        "error should mention 'Permission denied' somewhere in output, got stderr: {}",
        stderr
    );
    eprintln!("✓ Permission denied (source) error reporting test passed");
    Ok(())
}

/// Test that I/O helpers work correctly.
///
/// This is a meta-test to verify the tmpfs mount/unmount helpers work.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_io_helpers_work() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let tmpfs_path = format!("/tmp/chaos-helper-{}", timestamp);
    // set up cleanup guard in case assertions fail
    let _guard = IoCleanupGuard::new(&env)
        .unmount_tmpfs("host-b", tmpfs_path.clone())
        .remove_dir("host-b", tmpfs_path.clone());
    // mount tmpfs
    env.mount_tmpfs("host-b", &tmpfs_path, 1024)?;
    // check available space (should be close to 1024KB = 1MB)
    let avail = env.available_space("host-b", &tmpfs_path)?;
    assert!(
        avail > 500 * 1024 && avail < 1500 * 1024,
        "available space should be around 1MB, got {} bytes",
        avail
    );
    // write a file to it
    let test_file = format!("{}/test.txt", tmpfs_path);
    env.write_file("host-b", &test_file, b"hello from tmpfs")?;
    assert!(env.file_exists("host-b", &test_file)?);
    // unmount
    env.unmount_tmpfs("host-b", &tmpfs_path)?;
    // file should be gone after unmount
    assert!(!env.file_exists("host-b", &test_file)?);
    // cleanup handled by guard (unmount already done, will remove mount point dir)
    eprintln!("✓ I/O helpers test passed");
    Ok(())
}
