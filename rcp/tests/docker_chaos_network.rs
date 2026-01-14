//! Chaos tests for network condition simulation.
//!
//! These tests verify rcp's behavior under adverse network conditions using Linux `tc`
//! (traffic control) to simulate latency, packet loss, and bandwidth constraints.
//!
//! ## Prerequisites
//!
//! Before running these tests:
//! 1. Start containers: `just docker-up` (rebuilds containers with iproute2)
//! 2. Run: `cargo nextest run --profile docker --run-ignored only -E 'test(~chaos)'`
//!
//! ## Test Naming Convention
//!
//! All chaos tests include "chaos" in their name for easy filtering.

mod support;

use std::time::Instant;
use support::docker_env::{DockerEnv, Result};

/// Helper to ensure network conditions are cleared even if test panics.
struct NetworkConditionGuard<'a> {
    env: &'a DockerEnv,
    containers: Vec<&'a str>,
}

impl<'a> NetworkConditionGuard<'a> {
    fn new(env: &'a DockerEnv, containers: Vec<&'a str>) -> Self {
        Self { env, containers }
    }
}

impl Drop for NetworkConditionGuard<'_> {
    fn drop(&mut self) {
        for container in &self.containers {
            let _ = self.env.clear_network_conditions(container);
        }
    }
}

/// Test that file copy succeeds under high latency conditions.
///
/// This test adds 200ms latency to both hosts and verifies that the copy
/// still succeeds without timeout errors.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_high_latency_copy_succeeds() -> Result<()> {
    let env = DockerEnv::new()?;
    let _guard = NetworkConditionGuard::new(&env, vec!["host-a", "host-b"]);
    // add 200ms latency to both hosts
    env.add_latency("host-a", 200, None)?;
    env.add_latency("host-b", 200, None)?;
    // create test file
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/chaos-latency-{}.txt", timestamp);
    let dst_path = format!("/tmp/chaos-latency-{}-dst.txt", timestamp);
    env.write_file("host-a", &src_path, b"high latency test content")?;
    // time the copy operation (informational only, not asserted)
    let start = Instant::now();
    let output = env.exec_rcp(&[
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ])?;
    let elapsed = start.elapsed();
    // verify copy succeeded
    assert!(
        output.status.success(),
        "copy should succeed despite latency. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // verify file content
    let content = env.read_file("host-b", &dst_path)?;
    assert_eq!(b"high latency test content", content.as_slice());
    // cleanup test files (network conditions cleared automatically by _guard)
    env.remove_file("host-a", &src_path)?;
    env.remove_file("host-b", &dst_path)?;
    eprintln!(
        "✓ High latency test passed (200ms each way, transfer took {:?})",
        elapsed
    );
    Ok(())
}

// NOTE: Packet loss tests are disabled because tc netem loss affects ALL traffic
// on the interface, including the SSH session used by rcp to spawn rcpd. This
// causes the SSH connection to hang or timeout before the copy even starts.
// A future improvement would be to use iptables rules targeting specific ports,
// or apply tc rules after SSH is established.
//
// The helper functions (add_packet_loss, add_network_conditions) are kept for
// potential future use with a different approach.

/// Test that file copy succeeds under bandwidth constraints.
///
/// This test limits bandwidth to 1 Mbit/s and verifies that the copy
/// still succeeds with correct data.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_bandwidth_limit_copy_succeeds() -> Result<()> {
    let env = DockerEnv::new()?;
    let _guard = NetworkConditionGuard::new(&env, vec!["host-a"]);
    // limit source to 1 Mbit/s (128 KB/s)
    env.add_bandwidth_limit("host-a", 1000)?;
    // create a 128KB file
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/chaos-bw-{}.txt", timestamp);
    let dst_path = format!("/tmp/chaos-bw-{}-dst.txt", timestamp);
    env.create_file_with_size("host-a", &src_path, 128)?;
    // time the copy operation (informational only, not asserted due to tc burst behavior)
    let start = Instant::now();
    let output = env.exec_rcp(&[
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ])?;
    let elapsed = start.elapsed();
    // verify copy succeeded
    assert!(
        output.status.success(),
        "copy should succeed despite bandwidth limit. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // verify file size matches
    let src_size = env.file_size("host-a", &src_path)?;
    let dst_size = env.file_size("host-b", &dst_path)?;
    assert_eq!(src_size, dst_size, "file size should match");
    // cleanup test files (network conditions cleared automatically by _guard)
    env.remove_file("host-a", &src_path)?;
    env.remove_file("host-b", &dst_path)?;
    eprintln!(
        "✓ Bandwidth limit test passed (1Mbit/s, 128KB took {:?})",
        elapsed
    );
    Ok(())
}

/// Test that directory copy succeeds under latency.
///
/// This verifies that the more complex directory copy protocol
/// handles latency correctly across multiple messages.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_latency_directory_copy() -> Result<()> {
    let env = DockerEnv::new()?;
    let _guard = NetworkConditionGuard::new(&env, vec!["host-a", "host-b"]);
    // cleanup any existing directories first
    env.exec("host-a", None, &["rm", "-rf", "/tmp/chaos-dir-src"])?;
    env.exec("host-b", None, &["rm", "-rf", "/tmp/chaos-dir-dst"])?;
    // add 150ms latency
    env.add_latency("host-a", 150, None)?;
    env.add_latency("host-b", 150, None)?;
    // create directory with multiple files
    env.exec(
        "host-a",
        Some("testuser"),
        &["mkdir", "-p", "/tmp/chaos-dir-src/subdir"],
    )?;
    env.write_file("host-a", "/tmp/chaos-dir-src/file1.txt", b"content1")?;
    env.write_file("host-a", "/tmp/chaos-dir-src/file2.txt", b"content2")?;
    env.write_file("host-a", "/tmp/chaos-dir-src/subdir/file3.txt", b"content3")?;
    // create destination
    env.exec(
        "host-b",
        Some("testuser"),
        &["mkdir", "-p", "/tmp/chaos-dir-dst"],
    )?;
    let start = Instant::now();
    let output = env.exec_rcp(&["host-a:/tmp/chaos-dir-src/", "host-b:/tmp/chaos-dir-dst/"])?;
    let elapsed = start.elapsed();
    // verify copy succeeded
    assert!(
        output.status.success(),
        "directory copy should succeed. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // verify files exist
    assert!(env.file_exists("host-b", "/tmp/chaos-dir-dst/chaos-dir-src/file1.txt")?);
    assert!(env.file_exists("host-b", "/tmp/chaos-dir-dst/chaos-dir-src/file2.txt")?);
    assert!(env.file_exists(
        "host-b",
        "/tmp/chaos-dir-dst/chaos-dir-src/subdir/file3.txt"
    )?);
    // cleanup test files (network conditions cleared automatically by _guard)
    env.exec("host-a", None, &["rm", "-rf", "/tmp/chaos-dir-src"])?;
    env.exec("host-b", None, &["rm", "-rf", "/tmp/chaos-dir-dst"])?;
    eprintln!(
        "✓ Directory copy under latency passed (150ms each way, took {:?})",
        elapsed
    );
    Ok(())
}

/// Test that network condition helpers work correctly.
///
/// This is a meta-test to verify the tc commands execute properly.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_network_helpers_work() -> Result<()> {
    let env = DockerEnv::new()?;
    // test adding and clearing latency
    env.add_latency("host-a", 50, None)?;
    env.clear_network_conditions("host-a")?;
    // test adding and clearing packet loss
    env.add_packet_loss("host-a", 1.0)?;
    env.clear_network_conditions("host-a")?;
    // test adding and clearing bandwidth limit
    env.add_bandwidth_limit("host-a", 10000)?;
    env.clear_network_conditions("host-a")?;
    // test combined conditions
    env.add_network_conditions("host-a", 10, 0.5)?;
    env.clear_network_conditions("host-a")?;
    // clearing when nothing is set should not error
    env.clear_network_conditions("host-a")?;
    env.clear_network_conditions("host-a")?;
    eprintln!("✓ Network helpers test passed");
    Ok(())
}
