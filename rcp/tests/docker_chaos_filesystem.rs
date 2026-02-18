//! Filesystem chaos tests for race condition simulation.
//!
//! These tests verify rcp's behavior when the filesystem changes during a copy
//! operation, including files being deleted, added, or directories being removed
//! while the copy is in progress.
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
struct FsCleanupGuard<'a> {
    env: &'a DockerEnv,
    dirs_to_remove: Vec<(&'a str, String)>,
}

impl<'a> FsCleanupGuard<'a> {
    fn new(env: &'a DockerEnv) -> Self {
        Self {
            env,
            dirs_to_remove: Vec::new(),
        }
    }
    fn remove_dir(mut self, container: &'a str, path: String) -> Self {
        self.dirs_to_remove.push((container, path));
        self
    }
}

impl Drop for FsCleanupGuard<'_> {
    fn drop(&mut self) {
        for (container, path) in &self.dirs_to_remove {
            let _ = self.env.exec(container, None, &["rm", "-rf", path]);
        }
    }
}

/// Test that rcp does not hang when source files are deleted during copy.
///
/// This simulates a race where files disappear between traversal and copy.
/// The copy may succeed partially or report errors, but must not hang.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_filesystem_files_deleted_during_copy_no_hang() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_dir = format!("/tmp/chaos-fsdel-src-{}", timestamp);
    let dst_dir = format!("/tmp/chaos-fsdel-dst-{}", timestamp);
    // set up cleanup guard
    let _guard = FsCleanupGuard::new(&env)
        .remove_dir("host-a", src_dir.clone())
        .remove_dir("host-b", dst_dir.clone());
    // create source directory with many files
    env.exec("host-a", Some("testuser"), &["mkdir", "-p", &src_dir])?;
    for i in 0..20 {
        let file_path = format!("{}/file{:02}.dat", src_dir, i);
        env.write_file(
            "host-a",
            &file_path,
            format!("content of file {}", i).as_bytes(),
        )?;
    }
    // start a background process on host-a that deletes files after a short delay
    let delete_cmd = format!("sleep 0.5 && rm -f {}/file1*.dat", src_dir);
    std::process::Command::new("docker")
        .args([
            "exec",
            "-d",
            "-u",
            "testuser",
            "rcp-test-host-a",
            "sh",
            "-c",
            &delete_cmd,
        ])
        .output()?;
    // run rcp - it may succeed or fail, but must not hang
    let output = env.exec_rcp(&[
        &format!("host-a:{}", src_dir),
        &format!("host-b:{}", dst_dir),
    ])?;
    // the critical check: rcp completed (didn't hang). if we reach this point,
    // the copy finished within docker exec's implicit timeout.
    eprintln!(
        "rcp exited with code: {:?} (success={}) - filesystem deletion race handled",
        output.status.code(),
        output.status.success()
    );
    Ok(())
}

/// Test that rcp does not hang when new files are added to the source during copy.
///
/// This simulates a race where extra files appear after traversal.
/// The surplus handling should allow the copy to complete without hanging.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_filesystem_files_added_during_copy_no_hang() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_dir = format!("/tmp/chaos-fsadd-src-{}", timestamp);
    let dst_dir = format!("/tmp/chaos-fsadd-dst-{}", timestamp);
    // set up cleanup guard
    let _guard = FsCleanupGuard::new(&env)
        .remove_dir("host-a", src_dir.clone())
        .remove_dir("host-b", dst_dir.clone());
    // create source directory with a few files
    env.exec("host-a", Some("testuser"), &["mkdir", "-p", &src_dir])?;
    for i in 0..5 {
        let file_path = format!("{}/file{:02}.dat", src_dir, i);
        env.write_file(
            "host-a",
            &file_path,
            format!("content of file {}", i).as_bytes(),
        )?;
    }
    // start a background process on host-a that adds more files after a short delay
    let add_cmd = format!(
        "sleep 0.3 && for i in $(seq 10 25); do printf 'extra content' > {}/extra_$i.dat; done",
        src_dir
    );
    std::process::Command::new("docker")
        .args([
            "exec",
            "-d",
            "-u",
            "testuser",
            "rcp-test-host-a",
            "sh",
            "-c",
            &add_cmd,
        ])
        .output()?;
    // run rcp - it may copy only the original files or include some extras,
    // but must not hang
    let output = env.exec_rcp(&[
        &format!("host-a:{}", src_dir),
        &format!("host-b:{}", dst_dir),
    ])?;
    // the critical check: rcp completed (didn't hang)
    eprintln!(
        "rcp exited with code: {:?} (success={}) - filesystem addition race handled",
        output.status.code(),
        output.status.success()
    );
    Ok(())
}

/// Test that rcp does not hang when a subdirectory is removed during copy.
///
/// This simulates a race where an entire subtree disappears mid-copy.
/// The deficit handling should allow the copy to complete without hanging.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_filesystem_directory_removed_during_copy_no_hang() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_dir = format!("/tmp/chaos-fsrmdir-src-{}", timestamp);
    let dst_dir = format!("/tmp/chaos-fsrmdir-dst-{}", timestamp);
    // set up cleanup guard
    let _guard = FsCleanupGuard::new(&env)
        .remove_dir("host-a", src_dir.clone())
        .remove_dir("host-b", dst_dir.clone());
    // create source directory tree with files at multiple levels
    env.exec(
        "host-a",
        Some("testuser"),
        &["mkdir", "-p", &format!("{}/subdir/nested", src_dir)],
    )?;
    for i in 0..5 {
        let file_path = format!("{}/top{}.dat", src_dir, i);
        env.write_file("host-a", &file_path, format!("top {}", i).as_bytes())?;
    }
    for i in 0..5 {
        let file_path = format!("{}/subdir/mid{}.dat", src_dir, i);
        env.write_file("host-a", &file_path, format!("mid {}", i).as_bytes())?;
    }
    for i in 0..5 {
        let file_path = format!("{}/subdir/nested/deep{}.dat", src_dir, i);
        env.write_file("host-a", &file_path, format!("deep {}", i).as_bytes())?;
    }
    // start a background process that removes the subdirectory tree after a short delay
    let rm_cmd = format!("sleep 0.5 && rm -rf {}/subdir", src_dir);
    std::process::Command::new("docker")
        .args([
            "exec",
            "-d",
            "-u",
            "testuser",
            "rcp-test-host-a",
            "sh",
            "-c",
            &rm_cmd,
        ])
        .output()?;
    // run rcp - may report errors for missing files, but must not hang
    let output = env.exec_rcp(&[
        &format!("host-a:{}", src_dir),
        &format!("host-b:{}", dst_dir),
    ])?;
    // the critical check: rcp completed (didn't hang)
    eprintln!(
        "rcp exited with code: {:?} (success={}) - directory removal race handled",
        output.status.code(),
        output.status.success()
    );
    Ok(())
}

/// Test that rcp does not hang when files are deleted during a filtered copy.
///
/// This combines filesystem mutation with --include filters to test that deficit
/// handling works correctly when filters are active.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_filesystem_deficit_with_filter_no_hang() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_dir = format!("/tmp/chaos-fsfilt-src-{}", timestamp);
    let dst_dir = format!("/tmp/chaos-fsfilt-dst-{}", timestamp);
    // set up cleanup guard
    let _guard = FsCleanupGuard::new(&env)
        .remove_dir("host-a", src_dir.clone())
        .remove_dir("host-b", dst_dir.clone());
    // create source directory with a mix of .txt and .log files
    env.exec("host-a", Some("testuser"), &["mkdir", "-p", &src_dir])?;
    for i in 0..10 {
        let txt_path = format!("{}/data{:02}.txt", src_dir, i);
        env.write_file("host-a", &txt_path, format!("txt {}", i).as_bytes())?;
        let log_path = format!("{}/data{:02}.log", src_dir, i);
        env.write_file("host-a", &log_path, format!("log {}", i).as_bytes())?;
    }
    // start a background process that deletes some .txt files after a short delay
    let delete_cmd = format!("sleep 0.3 && rm -f {}/data0[5-9].txt", src_dir);
    std::process::Command::new("docker")
        .args([
            "exec",
            "-d",
            "-u",
            "testuser",
            "rcp-test-host-a",
            "sh",
            "-c",
            &delete_cmd,
        ])
        .output()?;
    // run rcp with include filter - only .txt files. some will vanish during copy.
    let output = env.exec_rcp(&[
        "--include=*.txt",
        &format!("host-a:{}", src_dir),
        &format!("host-b:{}", dst_dir),
    ])?;
    // the critical check: rcp completed (didn't hang)
    eprintln!(
        "rcp exited with code: {:?} (success={}) - filtered deficit race handled",
        output.status.code(),
        output.status.success()
    );
    Ok(())
}
