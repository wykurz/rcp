//! Tests for role ordering and connection timing scenarios.
//!
//! These tests verify the fix for the role-matching bug where source/destination
//! roles could be swapped if rcpd connections arrived out of order (commit c03db61).
//!
//! ## Background
//!
//! Previously, rcp would assign roles (Source/Destination) based on connection arrival order.
//! If the destination rcpd connected first, it could be misidentified as the source.
//! The fix uses the host information from the initial connection to properly identify roles.
//!
//! ## Prerequisites
//!
//! Before running these tests:
//! 1. Start containers: `cd tests/docker && ./test-helpers.sh start`
//! 2. Run: `cargo test --test docker_multi_host_role_ordering -- --ignored`

mod support;

use std::time::Duration;
use support::docker_env::{DockerEnv, Result};

/// Test basic multi-host copy to establish baseline
///
/// This test verifies that the basic multi-host scenario works correctly.
/// It serves as a baseline for the more complex role ordering tests.
#[test]
#[ignore = "requires Docker containers (run: cd tests/docker && ./test-helpers.sh start)"]
fn test_baseline_multi_host_copy() -> Result<()> {
    let env = DockerEnv::new()?;
    // create unique test file to avoid conflicts
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/role-test-{}.txt", timestamp);
    let dst_path = format!("/tmp/role-test-{}-dst.txt", timestamp);
    env.write_file("host-a", &src_path, b"role ordering test")?;
    // copy from host-a to host-b
    let output = env.exec_rcp(&[
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ])?;
    if !output.status.success() {
        eprintln!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        panic!("Copy failed");
    }
    // verify file exists and content matches
    assert!(
        env.file_exists("host-b", &dst_path)?,
        "File should exist on destination"
    );
    let content = env.read_file("host-b", &dst_path)?;
    assert_eq!(b"role ordering test", content.as_slice());
    // cleanup
    env.remove_file("host-a", &src_path)?;
    env.remove_file("host-b", &dst_path)?;
    Ok(())
}

/// Test copy with multiple rapid operations
///
/// This test performs multiple copy operations in quick succession to stress-test
/// the role assignment logic. With the bug, connection timing issues could cause
/// role confusion in rapid operations.
#[test]
#[ignore = "requires Docker containers (run: cd tests/docker && ./test-helpers.sh start)"]
fn test_rapid_multi_host_operations() -> Result<()> {
    let env = DockerEnv::new()?;
    let base_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    // perform 3 rapid copy operations
    for i in 0..3 {
        let src_path = format!("/tmp/rapid-test-{}-{}.txt", base_timestamp, i);
        let dst_path = format!("/tmp/rapid-test-{}-{}-dst.txt", base_timestamp, i);
        env.write_file("host-a", &src_path, format!("test {}", i).as_bytes())?;
        let output = env.exec_rcp(&[
            &format!("host-a:{}", src_path),
            &format!("host-b:{}", dst_path),
        ])?;
        assert!(
            output.status.success(),
            "Copy {} should succeed. stderr: {}",
            i,
            String::from_utf8_lossy(&output.stderr)
        );
        // verify
        assert!(
            env.file_exists("host-b", &dst_path)?,
            "File {} should exist",
            i
        );
        // cleanup this iteration's files
        env.remove_file("host-a", &src_path)?;
        env.remove_file("host-b", &dst_path)?;
        // small delay to allow cleanup
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

/// Test bidirectional copies between same hosts
///
/// This test alternates copy direction (A->B, then B->A) to verify that
/// role assignment correctly handles changing source/destination relationships.
#[test]
#[ignore = "requires Docker containers (run: cd tests/docker && ./test-helpers.sh start)"]
fn test_bidirectional_copies() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    // first: A -> B
    let src1 = format!("/tmp/bidir-a-{}.txt", timestamp);
    let dst1 = format!("/tmp/bidir-a-dst-{}.txt", timestamp);
    env.write_file("host-a", &src1, b"from A to B")?;
    let output = env.exec_rcp(&[&format!("host-a:{}", src1), &format!("host-b:{}", dst1)])?;
    assert!(output.status.success(), "A->B copy should succeed");
    std::thread::sleep(Duration::from_millis(200));
    // then: B -> A (reverse direction)
    let src2 = format!("/tmp/bidir-b-{}.txt", timestamp);
    let dst2 = format!("/tmp/bidir-b-dst-{}.txt", timestamp);
    env.write_file("host-b", &src2, b"from B to A")?;
    let output = env.exec_rcp(&[&format!("host-b:{}", src2), &format!("host-a:{}", dst2)])?;
    assert!(output.status.success(), "B->A copy should succeed");
    // verify both
    let content1 = env.read_file("host-b", &dst1)?;
    assert_eq!(b"from A to B", content1.as_slice());
    let content2 = env.read_file("host-a", &dst2)?;
    assert_eq!(b"from B to A", content2.as_slice());
    // cleanup
    env.remove_file("host-a", &src1)?;
    env.remove_file("host-b", &dst1)?;
    env.remove_file("host-b", &src2)?;
    env.remove_file("host-a", &dst2)?;
    Ok(())
}

/// Test role assignment when destination connects FIRST (the bug scenario)
///
/// This is THE critical test for the role-matching bug fix (commit c03db61).
/// By delaying the source rcpd, we force the destination to connect first,
/// which would cause role swapping in the buggy code.
///
/// With the fix, roles are correctly assigned based on host information
/// rather than connection order.
#[test]
#[ignore = "requires Docker containers (run: cd tests/docker && ./test-helpers.sh start)"]
fn test_destination_connects_first() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/role-inverted-{}.txt", timestamp);
    let dst_path = format!("/tmp/role-inverted-{}-dst.txt", timestamp);
    // create test file on source
    env.write_file("host-a", &src_path, b"destination connects first test")?;
    // delay source rcpd by 2 seconds, forcing destination to connect first
    // this tests the exact scenario that caused the bug
    let output = env.exec_rcp_with_delayed_rcpd(
        "host-a", // source host to delay
        "host-b", // destination host (no delay)
        2000,     // 2 second delay on source
        &[
            &format!("host-a:{}", src_path),
            &format!("host-b:{}", dst_path),
        ],
    )?;
    if !output.status.success() {
        eprintln!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        panic!("Copy should succeed even when destination connects first");
    }
    // verify file is on the DESTINATION (host-b), not source
    assert!(
        env.file_exists("host-b", &dst_path)?,
        "File should be on host-b (destination)"
    );
    assert!(
        !env.file_exists("host-a", &dst_path)?,
        "File should NOT be on host-a (source) - this would indicate role swap!"
    );
    // verify content
    let content = env.read_file("host-b", &dst_path)?;
    assert_eq!(b"destination connects first test", content.as_slice());
    // cleanup
    env.remove_file("host-a", &src_path)?;
    env.remove_file("host-b", &dst_path)?;
    Ok(())
}

/// Test that role assignment works correctly regardless of which rcpd connects first
///
/// This test runs multiple operations to verify consistency, but doesn't
/// deterministically force connection ordering (see test_destination_connects_first for that).
#[test]
#[ignore = "requires Docker containers (run: cd tests/docker && ./test-helpers.sh start)"]
fn test_consistent_role_assignment() -> Result<()> {
    let env = DockerEnv::new()?;
    // run multiple copies with slight delays to vary connection timing
    for attempt in 0..5 {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis();
        let src_path = format!("/tmp/role-consistency-{}-{}.txt", timestamp, attempt);
        let dst_path = format!("/tmp/role-consistency-{}-{}-dst.txt", timestamp, attempt);
        env.write_file(
            "host-a",
            &src_path,
            format!("attempt {}", attempt).as_bytes(),
        )?;
        // copy should succeed regardless of connection timing
        let output = env.exec_rcp(&[
            &format!("host-a:{}", src_path),
            &format!("host-b:{}", dst_path),
        ])?;
        if !output.status.success() {
            eprintln!("Attempt {} failed", attempt);
            eprintln!("stdout: {}", String::from_utf8_lossy(&output.stdout));
            eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
            panic!("Copy should succeed on attempt {}", attempt);
        }
        // verify file arrived at correct destination
        assert!(
            env.file_exists("host-b", &dst_path)?,
            "File should be on host-b (destination) for attempt {}",
            attempt
        );
        // verify it's NOT on the source (would indicate role swap)
        assert!(
            !env.file_exists("host-a", &dst_path)?,
            "File should NOT be on host-a (source) for attempt {}",
            attempt
        );
        // verify content
        let content = env.read_file("host-b", &dst_path)?;
        assert_eq!(
            format!("attempt {}", attempt).as_bytes(),
            content.as_slice(),
            "Content mismatch on attempt {}",
            attempt
        );
        // cleanup this iteration's files
        env.remove_file("host-a", &src_path)?;
        env.remove_file("host-b", &dst_path)?;
        // small delay between attempts
        std::thread::sleep(Duration::from_millis(300));
    }
    Ok(())
}
