//! Protocol edge case chaos tests.
//!
//! These tests verify rcp's behavior under protocol stress conditions,
//! including backpressure from slow connections, many concurrent files,
//! and large file transfers.
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

/// Guard that ensures network conditions are cleared on drop.
struct NetworkCleanupGuard<'a> {
    env: &'a DockerEnv,
    containers: Vec<String>,
    files_to_remove: Vec<(String, String)>,
    dirs_to_remove: Vec<(String, String)>,
}

impl<'a> NetworkCleanupGuard<'a> {
    fn new(env: &'a DockerEnv) -> Self {
        Self {
            env,
            containers: Vec::new(),
            files_to_remove: Vec::new(),
            dirs_to_remove: Vec::new(),
        }
    }
    fn clear_network(mut self, container: &str) -> Self {
        self.containers.push(container.to_string());
        self
    }
    fn remove_file(mut self, container: &str, path: String) -> Self {
        self.files_to_remove.push((container.to_string(), path));
        self
    }
    fn remove_dir(mut self, container: &str, path: String) -> Self {
        self.dirs_to_remove.push((container.to_string(), path));
        self
    }
}

impl Drop for NetworkCleanupGuard<'_> {
    fn drop(&mut self) {
        // cleanup order: network conditions first so subsequent operations aren't throttled,
        // then files, then directories (files must be removed before their parent dirs)
        for container in &self.containers {
            let _ = self.env.clear_network_conditions(container);
        }
        for (container, path) in &self.files_to_remove {
            let _ = self.env.remove_file(container, path);
        }
        for (container, path) in &self.dirs_to_remove {
            let _ = self.env.exec(container, None, &["rm", "-rf", path]);
        }
    }
}

/// Test backpressure mechanism with extreme bandwidth limiting.
///
/// This test applies very low bandwidth (64 kbit/s) to the destination
/// and transfers a file to exercise the backpressure mechanism.
/// The transfer should complete without errors, just slowly.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_protocol_backpressure_slow_destination() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/chaos-bp-src-{}.bin", timestamp);
    let dst_path = format!("/tmp/chaos-bp-dst-{}.bin", timestamp);
    // set up cleanup guard
    let _guard = NetworkCleanupGuard::new(&env)
        .clear_network("host-b")
        .remove_file("host-a", src_path.clone())
        .remove_file("host-b", dst_path.clone());
    // create a 256KB source file (enough to exercise backpressure but not too slow)
    env.create_file_with_size("host-a", &src_path, 256)?;
    // apply very low bandwidth to destination (64 kbit/s = 8 KB/s)
    // 256KB at 8KB/s should take ~32 seconds, but backpressure should keep it working
    env.add_bandwidth_limit("host-b", 64)?;
    let start = std::time::Instant::now();
    // attempt transfer
    let output = env.exec_rcp(&[
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ])?;
    let elapsed = start.elapsed();
    // verify transfer succeeded
    assert!(
        output.status.success(),
        "transfer should succeed despite slow destination"
    );
    // verify file was copied
    assert!(
        env.file_exists("host-b", &dst_path)?,
        "destination file should exist"
    );
    // verify file size matches
    let dst_size = env.file_size("host-b", &dst_path)?;
    assert_eq!(
        dst_size,
        256 * 1024,
        "destination file should be 256KB, got {} bytes",
        dst_size
    );
    eprintln!(
        "✓ Backpressure test passed (256KB transferred in {:?} with 64kbit limit)",
        elapsed
    );
    Ok(())
}

/// Test backpressure mechanism with slow source.
///
/// Similar to the slow destination test, but applies bandwidth limit to the source.
/// This tests the other direction of backpressure.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_protocol_backpressure_slow_source() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/chaos-bp2-src-{}.bin", timestamp);
    let dst_path = format!("/tmp/chaos-bp2-dst-{}.bin", timestamp);
    // set up cleanup guard
    let _guard = NetworkCleanupGuard::new(&env)
        .clear_network("host-a")
        .remove_file("host-a", src_path.clone())
        .remove_file("host-b", dst_path.clone());
    // create a 256KB source file
    env.create_file_with_size("host-a", &src_path, 256)?;
    // apply bandwidth limit to source
    env.add_bandwidth_limit("host-a", 64)?;
    let start = std::time::Instant::now();
    // attempt transfer
    let output = env.exec_rcp(&[
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ])?;
    let elapsed = start.elapsed();
    // verify transfer succeeded
    assert!(
        output.status.success(),
        "transfer should succeed despite slow source"
    );
    // verify file was copied correctly
    assert!(
        env.file_exists("host-b", &dst_path)?,
        "destination file should exist"
    );
    let dst_size = env.file_size("host-b", &dst_path)?;
    assert_eq!(
        dst_size,
        256 * 1024,
        "destination file should be 256KB, got {} bytes",
        dst_size
    );
    eprintln!(
        "✓ Slow source backpressure test passed (256KB transferred in {:?} with 64kbit limit)",
        elapsed
    );
    Ok(())
}

/// Test connection pool with many small files.
///
/// Creates a directory with many small files and copies it to test
/// the connection pooling mechanism. The default pool size is 100 connections.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_protocol_many_files() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_dir = format!("/tmp/chaos-many-src-{}", timestamp);
    let dst_dir = format!("/tmp/chaos-many-dst-{}", timestamp);
    // set up cleanup guard
    let _guard = NetworkCleanupGuard::new(&env)
        .remove_dir("host-a", src_dir.clone())
        .remove_dir("host-b", dst_dir.clone());
    // create source directory with many files (150 files to exceed default pool of 100)
    let file_count = 150;
    env.exec("host-a", Some("testuser"), &["mkdir", "-p", &src_dir])?;
    for i in 0..file_count {
        let file_path = format!("{}/file_{:04}.txt", src_dir, i);
        env.write_file(
            "host-a",
            &file_path,
            format!("content of file {}", i).as_bytes(),
        )?;
    }
    let start = std::time::Instant::now();
    // copy the directory
    let output = env.exec_rcp(&[
        &format!("host-a:{}", src_dir),
        &format!("host-b:{}", dst_dir),
    ])?;
    let elapsed = start.elapsed();
    // verify transfer succeeded
    assert!(
        output.status.success(),
        "transfer of many files should succeed"
    );
    // verify all files were copied by counting them
    let count_output = env.exec(
        "host-b",
        None,
        &["sh", "-c", &format!("ls -1 {}/ | wc -l", dst_dir)],
    )?;
    let count_str = String::from_utf8_lossy(&count_output.stdout);
    let copied_count: u32 = count_str.trim().parse().unwrap_or(0);
    assert_eq!(
        copied_count, file_count,
        "all {} files should be copied, got {}",
        file_count, copied_count
    );
    eprintln!(
        "✓ Many files test passed ({} files transferred in {:?})",
        file_count, elapsed
    );
    Ok(())
}

/// Test connection pool with limited max connections.
///
/// Same as test_chaos_protocol_many_files but with --max-connections=10
/// to test that the connection limiting works correctly.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_protocol_limited_connections() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_dir = format!("/tmp/chaos-limited-src-{}", timestamp);
    let dst_dir = format!("/tmp/chaos-limited-dst-{}", timestamp);
    // set up cleanup guard
    let _guard = NetworkCleanupGuard::new(&env)
        .remove_dir("host-a", src_dir.clone())
        .remove_dir("host-b", dst_dir.clone());
    // create source directory with files
    let file_count = 50;
    env.exec("host-a", Some("testuser"), &["mkdir", "-p", &src_dir])?;
    for i in 0..file_count {
        let file_path = format!("{}/file_{:04}.txt", src_dir, i);
        env.write_file(
            "host-a",
            &file_path,
            format!("content of file {}", i).as_bytes(),
        )?;
    }
    let start = std::time::Instant::now();
    // copy with limited connections
    let output = env.exec_rcp(&[
        "--max-connections=10",
        &format!("host-a:{}", src_dir),
        &format!("host-b:{}", dst_dir),
    ])?;
    let elapsed = start.elapsed();
    // verify transfer succeeded
    assert!(
        output.status.success(),
        "transfer with limited connections should succeed"
    );
    // verify all files were copied
    let count_output = env.exec(
        "host-b",
        None,
        &["sh", "-c", &format!("ls -1 {}/ | wc -l", dst_dir)],
    )?;
    let count_str = String::from_utf8_lossy(&count_output.stdout);
    let copied_count: u32 = count_str.trim().parse().unwrap_or(0);
    assert_eq!(
        copied_count, file_count,
        "all {} files should be copied, got {}",
        file_count, copied_count
    );
    eprintln!(
        "✓ Limited connections test passed ({} files with max-connections=10 in {:?})",
        file_count, elapsed
    );
    Ok(())
}

/// Test large file transfer to exercise chunking.
///
/// Transfers a 10MB file to ensure the chunking logic works correctly
/// for files larger than the internal buffer sizes.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_protocol_large_file() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/chaos-large-src-{}.bin", timestamp);
    let dst_path = format!("/tmp/chaos-large-dst-{}.bin", timestamp);
    // set up cleanup guard
    let _guard = NetworkCleanupGuard::new(&env)
        .remove_file("host-a", src_path.clone())
        .remove_file("host-b", dst_path.clone());
    // create a 10MB source file
    let size_kb = 10 * 1024; // 10MB
    env.create_file_with_size("host-a", &src_path, size_kb)?;
    let start = std::time::Instant::now();
    // transfer the file
    let output = env.exec_rcp(&[
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ])?;
    let elapsed = start.elapsed();
    // verify transfer succeeded
    assert!(
        output.status.success(),
        "large file transfer should succeed"
    );
    // verify file was copied
    assert!(
        env.file_exists("host-b", &dst_path)?,
        "destination file should exist"
    );
    // verify file size matches
    let dst_size = env.file_size("host-b", &dst_path)?;
    let expected_size = size_kb as u64 * 1024;
    assert_eq!(
        dst_size,
        expected_size,
        "destination file should be {}MB, got {} bytes",
        size_kb / 1024,
        dst_size
    );
    eprintln!(
        "✓ Large file test passed ({}MB transferred in {:?})",
        size_kb / 1024,
        elapsed
    );
    Ok(())
}

/// Test combined stress: many files with bandwidth limit.
///
/// This combines multiple stress factors to ensure they work together:
/// - Many files (exercises connection pool)
/// - Bandwidth limit (exercises backpressure)
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_protocol_combined_stress() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_dir = format!("/tmp/chaos-stress-src-{}", timestamp);
    let dst_dir = format!("/tmp/chaos-stress-dst-{}", timestamp);
    // set up cleanup guard
    let _guard = NetworkCleanupGuard::new(&env)
        .clear_network("host-b")
        .remove_dir("host-a", src_dir.clone())
        .remove_dir("host-b", dst_dir.clone());
    // create source directory with moderate number of files
    let file_count = 30;
    env.exec("host-a", Some("testuser"), &["mkdir", "-p", &src_dir])?;
    for i in 0..file_count {
        let file_path = format!("{}/file_{:04}.txt", src_dir, i);
        // each file is 4KB to make the test meaningful
        let content = "x".repeat(4096);
        env.write_file("host-a", &file_path, content.as_bytes())?;
    }
    // apply moderate bandwidth limit (256 kbit/s = 32 KB/s)
    // 30 files * 4KB = 120KB, should take ~4 seconds
    env.add_bandwidth_limit("host-b", 256)?;
    let start = std::time::Instant::now();
    // copy with limited connections to add more stress
    let output = env.exec_rcp(&[
        "--max-connections=5",
        &format!("host-a:{}", src_dir),
        &format!("host-b:{}", dst_dir),
    ])?;
    let elapsed = start.elapsed();
    // verify transfer succeeded
    assert!(
        output.status.success(),
        "combined stress transfer should succeed"
    );
    // verify all files were copied
    let count_output = env.exec(
        "host-b",
        None,
        &["sh", "-c", &format!("ls -1 {}/ | wc -l", dst_dir)],
    )?;
    let count_str = String::from_utf8_lossy(&count_output.stdout);
    let copied_count: u32 = count_str.trim().parse().unwrap_or(0);
    assert_eq!(
        copied_count, file_count,
        "all {} files should be copied, got {}",
        file_count, copied_count
    );
    eprintln!(
        "✓ Combined stress test passed ({} files with bandwidth limit and limited connections in {:?})",
        file_count, elapsed
    );
    Ok(())
}
