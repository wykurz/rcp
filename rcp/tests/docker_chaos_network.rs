//! Chaos tests for network condition simulation.
//!
//! These tests verify rcp's behavior under adverse network conditions using Linux `tc`
//! (traffic control) to simulate latency, packet loss, and bandwidth constraints.
//!
//! ## Prerequisites
//!
//! From the repository root, run the full chaos lifecycle with `just docker-chaos-test`. For
//! repeated runs, use `just docker-up`, `just docker-chaos-test-only`, then `just docker-down`.
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

/// Guard that flushes container iptables INPUT rules on Drop so firewall chaos tests cannot
/// leave containers partitioned after a failure or panic.
struct FirewallGuard<'a> {
    env: &'a DockerEnv,
    containers: Vec<&'a str>,
}

impl<'a> FirewallGuard<'a> {
    fn block_non_ssh_inbound(env: &'a DockerEnv, containers: Vec<&'a str>) -> Result<Self> {
        let guard = Self {
            env,
            containers: containers.clone(),
        };
        for container in containers {
            // allow loopback (needed by SSH port forwards) and SSH port 22; drop all other new
            // inbound TCP SYNs to simulate a stateful firewall that permits only SSH
            for args in [
                &["iptables", "-F", "INPUT"][..],
                &["iptables", "-A", "INPUT", "-i", "lo", "-j", "ACCEPT"][..],
                &[
                    "iptables", "-A", "INPUT", "-p", "tcp", "--dport", "22", "-j", "ACCEPT",
                ][..],
                &[
                    "iptables", "-A", "INPUT", "-p", "tcp", "--syn", "-j", "DROP",
                ][..],
            ] {
                let out = env.exec(container, None, args)?;
                if !out.status.success() {
                    return Err(format!(
                        "iptables {:?} failed on {container}: {}",
                        args,
                        String::from_utf8_lossy(&out.stderr)
                    )
                    .into());
                }
            }
        }
        Ok(guard)
    }
}

impl Drop for FirewallGuard<'_> {
    fn drop(&mut self) {
        for container in &self.containers {
            let _ = self.env.exec(container, None, &["iptables", "-F", "INPUT"]);
        }
    }
}

/// Verify that when a firewall drops all non-SSH TCP ports, default `--remote-transport=tcp`
/// fails while `--remote-transport=auto` detects the block and automatically completes over SSH
/// across download (-L), upload (-R), and 3-host (host-b -> host-a) topologies.
#[test]
#[ignore = "requires Docker containers (run: just docker-up)"]
fn test_chaos_firewall_blocks_direct_tcp_auto_falls_back_to_ssh() -> Result<()> {
    let env = DockerEnv::new()?;
    let _fw = FirewallGuard::block_non_ssh_inbound(&env, vec!["host-a", "master"])?;
    let payload = b"firewall blocked direct tcp - delivered via ssh fallback";
    env.write_file("host-a", "/tmp/chaos-fw-src.txt", payload)?;
    env.write_file("master", "/tmp/chaos-fw-ul-src.txt", payload)?;
    let _ = env.remove_file("host-b", "/tmp/chaos-fw-tcp.txt");
    let _ = env.remove_file("master", "/tmp/chaos-fw-dl.txt");
    let _ = env.remove_file("host-b", "/tmp/chaos-fw-ul.txt");
    let _ = env.remove_file("host-b", "/tmp/chaos-fw-3h.txt");
    // 1. Confirm direct TCP truly fails under the firewall
    let tcp_out = env.exec_rcp(&[
        "--remote-transport=tcp",
        "--remote-copy-conn-timeout-sec=2",
        "host-a:/tmp/chaos-fw-src.txt",
        "host-b:/tmp/chaos-fw-tcp.txt",
    ])?;
    let tcp_combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&tcp_out.stdout),
        String::from_utf8_lossy(&tcp_out.stderr)
    );
    support::docker_env::assert_clean_failure(
        tcp_out.status,
        &tcp_combined,
        "direct TCP must fail when non-SSH ports are blocked",
    );
    // 2. Download (host-a -> master): auto probes, fails, master opens -L fallback tunnels
    //    and switches from direct-TCP TLS to loopback HMAC-SHA256 without double encryption
    let dl_out = env.exec_rcp(&[
        "-vv",
        "--remote-transport=auto",
        "--remote-direct-probe-timeout-sec=2",
        "--ssh-tunnels=2",
        "host-a:/tmp/chaos-fw-src.txt",
        "/tmp/chaos-fw-dl.txt",
    ])?;
    let dl_combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&dl_out.stdout),
        String::from_utf8_lossy(&dl_out.stderr)
    );
    assert!(
        dl_out.status.success(),
        "auto download fallback failed:\n{dl_combined}"
    );
    assert!(
        dl_combined.contains("falling back to SSH"),
        "expected SSH fallback notice on download:\n{dl_combined}"
    );
    assert!(
        dl_combined.contains("Source control stream authenticated (SSH loopback HMAC-SHA256 auth)")
            && dl_combined.contains("Destination stream security: SSH loopback HMAC-SHA256 auth"),
        "auto SSH fallback must switch to loopback HMAC-SHA256 without double TLS:\n{dl_combined}"
    );
    assert_eq!(env.read_file("master", "/tmp/chaos-fw-dl.txt")?, payload);
    // 3. Upload (master -> host-b): auto probes blocked master listener, master opens -R tunnels
    let ul_out = env.exec_rcp(&[
        "--remote-transport=auto",
        "--remote-direct-probe-timeout-sec=2",
        "--ssh-tunnels=2",
        "/tmp/chaos-fw-ul-src.txt",
        "host-b:/tmp/chaos-fw-ul.txt",
    ])?;
    let ul_combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&ul_out.stdout),
        String::from_utf8_lossy(&ul_out.stderr)
    );
    assert!(
        ul_out.status.success(),
        "auto upload fallback failed:\n{ul_combined}"
    );
    assert!(
        ul_combined.contains("falling back to SSH"),
        "expected SSH fallback notice on upload:\n{ul_combined}"
    );
    assert_eq!(env.read_file("host-b", "/tmp/chaos-fw-ul.txt")?, payload);
    // 4. 3-Host (host-a -> host-b) with --port-ranges: auto probes blocked port range, then
    //    destination (host-b) opens direct -L tunnels to host-a
    let h3_out = env.exec_rcp(&[
        "--remote-transport=auto",
        "--remote-direct-probe-timeout-sec=2",
        "--port-ranges=20000-20010",
        "--ssh-tunnels=2",
        "host-a:/tmp/chaos-fw-src.txt",
        "host-b:/tmp/chaos-fw-3h.txt",
    ])?;
    let h3_combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&h3_out.stdout),
        String::from_utf8_lossy(&h3_out.stderr)
    );
    assert!(
        h3_out.status.success(),
        "auto 3-host fallback failed:\n{h3_combined}"
    );
    assert!(
        h3_combined.contains("falling back to SSH"),
        "expected SSH fallback notice on 3-host:\n{h3_combined}"
    );
    assert_eq!(env.read_file("host-b", "/tmp/chaos-fw-3h.txt")?, payload);
    let _ = env.remove_file("host-a", "/tmp/chaos-fw-src.txt");
    let _ = env.remove_file("master", "/tmp/chaos-fw-ul-src.txt");
    let _ = env.remove_file("master", "/tmp/chaos-fw-dl.txt");
    let _ = env.remove_file("host-b", "/tmp/chaos-fw-ul.txt");
    let _ = env.remove_file("host-b", "/tmp/chaos-fw-3h.txt");
    eprintln!("✓ Firewall-blocked auto SSH fallback (download, upload, 3-host) passed");
    Ok(())
}
