//! Process chaos tests for failure injection.
//!
//! These tests verify rcp's behavior when rcpd processes die or hang unexpectedly.
//! They test error handling, cleanup, and graceful degradation.
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

use std::time::{Duration, Instant};
use support::docker_env::{DockerEnv, Result};

/// Guard that ensures cleanup always runs, even on early return or panic.
/// This prevents stale network rules and temp files from affecting later tests.
struct TestCleanupGuard<'a> {
    env: &'a DockerEnv,
    network_containers: Vec<&'a str>,
    files_to_remove: Vec<(&'a str, String)>,
    containers_to_kill_rcpd: Vec<&'a str>,
    containers_to_resume_rcpd: Vec<&'a str>,
}

impl<'a> TestCleanupGuard<'a> {
    fn new(env: &'a DockerEnv) -> Self {
        Self {
            env,
            network_containers: Vec::new(),
            files_to_remove: Vec::new(),
            containers_to_kill_rcpd: Vec::new(),
            containers_to_resume_rcpd: Vec::new(),
        }
    }
    fn clear_network_on(mut self, containers: Vec<&'a str>) -> Self {
        self.network_containers = containers;
        self
    }
    fn remove_file(mut self, container: &'a str, path: String) -> Self {
        self.files_to_remove.push((container, path));
        self
    }
    fn kill_rcpd_on(mut self, containers: Vec<&'a str>) -> Self {
        self.containers_to_kill_rcpd = containers;
        self
    }
    fn resume_rcpd_on(mut self, containers: Vec<&'a str>) -> Self {
        self.containers_to_resume_rcpd = containers;
        self
    }
}

impl Drop for TestCleanupGuard<'_> {
    fn drop(&mut self) {
        // resume any paused rcpd first (before killing)
        for container in &self.containers_to_resume_rcpd {
            let _ = self.env.resume_rcpd(container);
        }
        // kill any remaining rcpd processes
        for container in &self.containers_to_kill_rcpd {
            let _ = self.env.kill_rcpd(container);
        }
        // clear network conditions
        for container in &self.network_containers {
            let _ = self.env.clear_network_conditions(container);
        }
        // remove temp files
        for (container, path) in &self.files_to_remove {
            let _ = self.env.remove_file(container, path);
        }
    }
}

/// Wait for rcpd to start on a container, with timeout.
fn wait_for_rcpd(env: &DockerEnv, container: &str, timeout: Duration) -> Result<bool> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if env.is_rcpd_running(container)? {
            return Ok(true);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(false)
}

/// Wait for rcpd to stop on a container, with timeout.
fn wait_for_rcpd_exit(env: &DockerEnv, container: &str, timeout: Duration) -> Result<bool> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if !env.is_rcpd_running(container)? {
            return Ok(true);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(false)
}

/// Wait for a child process to exit with a timeout.
/// Returns Ok(exit_status) if the process exits within the timeout, Err if it times out.
fn wait_with_timeout(
    mut child: std::process::Child,
    timeout: Duration,
) -> Result<std::process::ExitStatus> {
    let start = Instant::now();
    loop {
        match child.try_wait()? {
            Some(status) => {
                return Ok(status);
            }
            None => {
                if start.elapsed() > timeout {
                    child.kill()?;
                    let _ = child.wait(); // reap zombie
                    return Err(format!("process did not exit within {:?}", timeout).into());
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// Test that killing rcpd early (before connections established) causes rcp to fail fast.
///
/// This tests the "connection refused" error path - rcpd starts but is killed before
/// rcp can connect to it.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_kill_rcpd_early() -> Result<()> {
    let env = DockerEnv::new()?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/chaos-kill-early-{}.bin", timestamp);
    let dst_path = format!("/tmp/chaos-kill-early-{}-dst.bin", timestamp);
    // set up cleanup guard before any operations that might fail
    let _guard = TestCleanupGuard::new(&env)
        .clear_network_on(vec!["host-a", "host-b"])
        .kill_rcpd_on(vec!["host-a", "host-b"])
        .remove_file("host-a", src_path.clone())
        .remove_file("host-b", dst_path.clone());
    env.create_file_with_size("host-a", &src_path, 256)?;
    // add latency to slow down connection establishment
    env.add_latency("host-a", 200, None)?;
    env.add_latency("host-b", 200, None)?;
    // spawn rcp in background
    let child = env.spawn_rcp(&[
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ])?;
    // wait for rcpd to start on both hosts
    let rcpd_a_started = wait_for_rcpd(&env, "host-a", Duration::from_secs(10))?;
    let rcpd_b_started = wait_for_rcpd(&env, "host-b", Duration::from_secs(10))?;
    assert!(rcpd_a_started, "rcpd should start on host-a");
    assert!(rcpd_b_started, "rcpd should start on host-b");
    // kill rcpd immediately - before rcp establishes TCP connections
    env.kill_rcpd("host-a")?;
    env.kill_rcpd("host-b")?;
    eprintln!("killed rcpd on both hosts (early, before connections)");
    // should fail quickly with "connection refused"
    let status = wait_with_timeout(child, Duration::from_secs(30))?;
    assert!(
        !status.success(),
        "rcp should fail when rcpd is killed early"
    );
    // cleanup handled by _guard
    eprintln!("✓ Kill rcpd early test passed");
    Ok(())
}

/// Test that killing rcpd mid-transfer (after connections established) causes rcp to fail.
///
/// This tests the TCP connection failure path - rcpd dies while transfer is in progress.
/// Note: This test may take longer as it relies on TCP timeout detection.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_kill_rcpd_mid_transfer() -> Result<()> {
    let env = DockerEnv::new()?;
    // create a larger file (4MB) to ensure transfer takes time
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/chaos-kill-mid-{}.bin", timestamp);
    let dst_path = format!("/tmp/chaos-kill-mid-{}-dst.bin", timestamp);
    // set up cleanup guard before any operations that might fail
    let _guard = TestCleanupGuard::new(&env)
        .clear_network_on(vec!["host-a", "host-b"])
        .kill_rcpd_on(vec!["host-a", "host-b"])
        .remove_file("host-a", src_path.clone())
        .remove_file("host-b", dst_path.clone());
    env.create_file_with_size("host-a", &src_path, 4096)?;
    // add latency to slow down transfer significantly
    env.add_latency("host-a", 200, None)?;
    env.add_latency("host-b", 200, None)?;
    // spawn rcp with shorter connection timeout to speed up failure detection
    let child = env.spawn_rcp(&[
        "--remote-copy-conn-timeout-sec=5",
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ])?;
    // wait for rcpd to start on both hosts
    let rcpd_a_started = wait_for_rcpd(&env, "host-a", Duration::from_secs(10))?;
    let rcpd_b_started = wait_for_rcpd(&env, "host-b", Duration::from_secs(10))?;
    assert!(rcpd_a_started, "rcpd should start on host-a");
    assert!(rcpd_b_started, "rcpd should start on host-b");
    // wait for transfer to actually begin (connections established)
    std::thread::sleep(Duration::from_secs(2));
    // kill rcpd mid-transfer
    env.kill_rcpd("host-a")?;
    env.kill_rcpd("host-b")?;
    eprintln!("killed rcpd on both hosts (mid-transfer)");
    // longer timeout - TCP may take a while to detect dead peer
    let status = wait_with_timeout(child, Duration::from_secs(120))?;
    assert!(
        !status.success(),
        "rcp should fail when rcpd is killed mid-transfer"
    );
    // cleanup handled by _guard
    eprintln!("✓ Kill rcpd mid-transfer test passed");
    Ok(())
}

/// Test that pausing rcpd (simulating hang) eventually causes timeout.
///
/// This test verifies that rcp doesn't hang forever when rcpd stops responding.
/// Note: This test may take a while due to timeout behavior.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_pause_rcpd_causes_timeout() -> Result<()> {
    let env = DockerEnv::new()?;
    // create a larger file (2MB) so transfer takes time
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/chaos-pause-{}.bin", timestamp);
    let dst_path = format!("/tmp/chaos-pause-{}-dst.bin", timestamp);
    // set up cleanup guard before any operations that might fail
    // note: resume before kill, since kill on stopped process may leave zombies
    let _guard = TestCleanupGuard::new(&env)
        .resume_rcpd_on(vec!["host-a", "host-b"])
        .kill_rcpd_on(vec!["host-a", "host-b"])
        .clear_network_on(vec!["host-a", "host-b"])
        .remove_file("host-a", src_path.clone())
        .remove_file("host-b", dst_path.clone());
    env.create_file_with_size("host-a", &src_path, 2048)?;
    // add latency to ensure transfer takes time
    env.add_latency("host-a", 100, None)?;
    env.add_latency("host-b", 100, None)?;
    // spawn rcp in background
    let mut child = env.spawn_rcp(&[
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ])?;
    // wait for rcpd to start on both hosts (ensures transfer is in progress)
    let rcpd_a_started = wait_for_rcpd(&env, "host-a", Duration::from_secs(10))?;
    let rcpd_b_started = wait_for_rcpd(&env, "host-b", Duration::from_secs(10))?;
    assert!(rcpd_a_started, "rcpd should start on host-a");
    assert!(rcpd_b_started, "rcpd should start on host-b");
    // verify transfer is still in progress (child hasn't exited yet)
    assert!(
        child.try_wait()?.is_none(),
        "transfer should still be in progress"
    );
    // pause rcpd (SIGSTOP) - simulates hung process
    env.pause_rcpd("host-a")?;
    eprintln!("paused rcpd on host-a (SIGSTOP)");
    let start = Instant::now();
    // wait for rcp to complete (should timeout eventually)
    // use a reasonable timeout for the test itself
    let test_timeout = Duration::from_secs(60);
    loop {
        match child.try_wait()? {
            Some(status) => {
                let elapsed = start.elapsed();
                eprintln!("rcp exited after {:?} with status: {:?}", elapsed, status);
                // rcp should have failed due to timeout
                assert!(!status.success(), "rcp should fail when rcpd is paused");
                break;
            }
            None => {
                if start.elapsed() > test_timeout {
                    // guard handles cleanup (resume rcpd, kill rcpd, clear network, remove files)
                    child.kill()?;
                    let _ = child.wait(); // reap zombie
                    panic!(
                        "rcp did not timeout after {:?} - it may be hanging",
                        test_timeout
                    );
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
    // cleanup handled by _guard
    eprintln!("✓ Pause rcpd timeout test passed");
    Ok(())
}

/// Test that no orphaned rcpd processes remain after killing the master (rcp).
///
/// This tests the stdin watchdog: when rcp dies, rcpd should detect EOF on stdin
/// and exit cleanly.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_master_killed_rcpd_cleanup() -> Result<()> {
    let env = DockerEnv::new()?;
    // create a large file so transfer takes time
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/chaos-master-{}.bin", timestamp);
    let dst_path = format!("/tmp/chaos-master-{}-dst.bin", timestamp);
    // set up cleanup guard before any operations that might fail
    let _guard = TestCleanupGuard::new(&env)
        .kill_rcpd_on(vec!["host-a", "host-b"])
        .clear_network_on(vec!["host-a", "host-b"])
        .remove_file("host-a", src_path.clone())
        .remove_file("host-b", dst_path.clone());
    env.create_file_with_size("host-a", &src_path, 1024)?;
    // add significant latency to ensure transfer takes time
    env.add_latency("host-a", 200, None)?;
    env.add_latency("host-b", 200, None)?;
    // spawn rcp in background
    let mut child = env.spawn_rcp(&[
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ])?;
    // wait for rcpd to start on both hosts
    let rcpd_a = wait_for_rcpd(&env, "host-a", Duration::from_secs(10))?;
    let rcpd_b = wait_for_rcpd(&env, "host-b", Duration::from_secs(10))?;
    assert!(rcpd_a, "rcpd should start on host-a");
    assert!(rcpd_b, "rcpd should start on host-b");
    eprintln!("rcpd running on both hosts");
    // verify transfer is still in progress
    assert!(
        child.try_wait()?.is_none(),
        "transfer should still be in progress"
    );
    // kill the master (docker exec process running rcp)
    child.kill()?;
    let _ = child.wait(); // reap zombie
    eprintln!("killed master (rcp) process");
    // poll for rcpd to exit on both hosts (stdin watchdog should trigger)
    let watchdog_timeout = Duration::from_secs(10);
    let rcpd_a_exited = wait_for_rcpd_exit(&env, "host-a", watchdog_timeout)?;
    let rcpd_b_exited = wait_for_rcpd_exit(&env, "host-b", watchdog_timeout)?;
    // assertions - guard will clean up even if these fail
    assert!(
        rcpd_a_exited,
        "rcpd on host-a should exit after master is killed (stdin watchdog)"
    );
    assert!(
        rcpd_b_exited,
        "rcpd on host-b should exit after master is killed (stdin watchdog)"
    );
    // cleanup handled by _guard
    eprintln!("✓ Master killed cleanup test passed");
    Ok(())
}

/// Test that process helpers work correctly.
///
/// This is a meta-test to verify the kill/pause/resume helpers work.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_process_helpers_work() -> Result<()> {
    let env = DockerEnv::new()?;
    // initially no rcpd should be running
    assert!(
        !env.is_rcpd_running("host-a")?,
        "no rcpd should be running initially"
    );
    // start a quick copy to spawn rcpd
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    let src_path = format!("/tmp/chaos-helper-{}.txt", timestamp);
    let dst_path = format!("/tmp/chaos-helper-{}-dst.txt", timestamp);
    // set up cleanup guard before any operations that might fail
    let _guard = TestCleanupGuard::new(&env)
        .kill_rcpd_on(vec!["host-a", "host-b"])
        .remove_file("host-a", src_path.clone())
        .remove_file("host-b", dst_path.clone());
    env.write_file("host-a", &src_path, b"test")?;
    // this will spawn rcpd briefly
    let _ = env.exec_rcp(&[
        &format!("host-a:{}", src_path),
        &format!("host-b:{}", dst_path),
    ]);
    // after copy completes, rcpd should be gone - poll to avoid flakiness
    let rcpd_exited = wait_for_rcpd_exit(&env, "host-a", Duration::from_secs(5))?;
    assert!(rcpd_exited, "rcpd should exit after copy completes");
    // killing when none running should not error
    env.kill_rcpd("host-a")?;
    env.pause_rcpd("host-a")?;
    env.resume_rcpd("host-a")?;
    // cleanup handled by _guard
    eprintln!("✓ Process helpers test passed");
    Ok(())
}
