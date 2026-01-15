//! Docker test environment helpers for multi-host integration tests.
//!
//! This module provides utilities for running RCP tests across multiple Docker containers that simulate separate hosts with SSH connectivity.

use std::process::{Command, Output};

/// Result type for docker operations
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Print formatted command output for debugging
fn print_command_output(output: &Output, context: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("\n=== RCP COMMAND OUTPUT ({}) ===", context);
    if let Some(code) = output.status.code() {
        eprintln!("Exit status: {}", code);
    } else {
        eprintln!("Exit status: terminated by signal");
    }
    if !stdout.is_empty() {
        eprintln!("--- STDOUT ---");
        eprintln!("{}", stdout);
    }
    if !stderr.is_empty() {
        eprintln!("--- STDERR ---");
        eprintln!("{}", stderr);
    }
    eprintln!("=== END RCP OUTPUT ===\n");
}

/// Represents the Docker test environment with multiple containers
pub struct DockerEnv {
    /// Path to the rcp binary to use
    rcp_binary: String,
}

impl DockerEnv {
    /// Create a new Docker test environment
    ///
    /// Assumes containers are already running (via docker-compose up)
    pub fn new() -> Result<Self> {
        // check if Docker is available
        let docker_check = Command::new("docker").arg("info").output();
        if docker_check.is_err() || !docker_check?.status.success() {
            return Err("Docker is not available. Is Docker daemon running?".into());
        }
        // check if test containers are running
        let containers_check = Command::new("docker")
            .args(["ps", "--filter", "name=rcp-test", "--format", "{{.Names}}"])
            .output()?;
        let running_containers = String::from_utf8_lossy(&containers_check.stdout);
        if !running_containers.contains("rcp-test-master") {
            return Err(
                "Test containers are not running. Run: cd tests/docker && ./test-helpers.sh start"
                    .into(),
            );
        }
        Ok(Self {
            rcp_binary: "/home/testuser/.local/bin/rcp".to_string(),
        })
    }

    /// Execute rcp command from master container as testuser
    pub fn exec_rcp(&self, args: &[&str]) -> Result<Output> {
        let mut cmd = Command::new("docker");
        cmd.args([
            "exec",
            "-u",
            "testuser",
            "rcp-test-master",
            &self.rcp_binary,
        ]);
        // always use verbose output for better debugging in CI
        cmd.arg("-vv");
        cmd.args(args);
        let output = cmd.output()?;
        // print output if command failed (for debugging in CI)
        if !output.status.success() {
            print_command_output(&output, &format!("rcp {}", args.join(" ")));
        }
        Ok(output)
    }

    /// Spawn rcp command in the background, returning a Child handle.
    ///
    /// This allows killing rcpd or the master process while rcp is running.
    /// The caller is responsible for waiting on the child and handling output.
    /// Output is sent to null to avoid pipe buffer blocking issues.
    #[allow(dead_code)]
    pub fn spawn_rcp(&self, args: &[&str]) -> Result<std::process::Child> {
        let mut cmd = Command::new("docker");
        cmd.args([
            "exec",
            "-u",
            "testuser",
            "rcp-test-master",
            &self.rcp_binary,
        ]);
        cmd.arg("-vv");
        cmd.args(args);
        // don't pipe - avoids blocking if output buffer fills
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::null());
        let child = cmd.spawn()?;
        Ok(child)
    }

    /// Execute arbitrary command in a container
    #[allow(dead_code)]
    pub fn exec(&self, container: &str, user: Option<&str>, args: &[&str]) -> Result<Output> {
        let container_name = format!("rcp-test-{}", container);
        let mut cmd = Command::new("docker");
        cmd.arg("exec");
        if let Some(u) = user {
            cmd.args(["-u", u]);
        }
        cmd.arg(&container_name);
        cmd.args(args);
        let output = cmd.output()?;
        Ok(output)
    }

    /// Write a file to a container
    pub fn write_file(&self, container: &str, path: &str, content: &[u8]) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        // use printf to write content without adding newlines
        let content_str = String::from_utf8_lossy(content);
        // escape single quotes in content for shell
        let escaped_content = content_str.replace('\'', "'\\''");
        let mut cmd = Command::new("docker");
        cmd.args([
            "exec",
            &container_name,
            "sh",
            "-c",
            &format!("printf '%s' '{}' > {}", escaped_content, path),
        ]);
        let output = cmd.output()?;
        if !output.status.success() {
            return Err(format!(
                "Failed to write file: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    /// Read a file from a container
    pub fn read_file(&self, container: &str, path: &str) -> Result<Vec<u8>> {
        let container_name = format!("rcp-test-{}", container);
        let output = Command::new("docker")
            .args(["exec", &container_name, "cat", path])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "Failed to read file: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(output.stdout)
    }

    /// Check if a file exists in a container
    pub fn file_exists(&self, container: &str, path: &str) -> Result<bool> {
        let container_name = format!("rcp-test-{}", container);
        let output = Command::new("docker")
            .args(["exec", &container_name, "test", "-f", path])
            .output()?;
        Ok(output.status.success())
    }

    /// Remove a file from a container
    pub fn remove_file(&self, container: &str, path: &str) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        Command::new("docker")
            .args(["exec", &container_name, "rm", "-f", path])
            .output()?;
        Ok(())
    }

    /// Create a file of specified size filled with a pattern.
    ///
    /// This uses `dd` to create files, which is more efficient for larger files
    /// than `write_file` (which uses printf and has shell argument limits).
    ///
    /// # Arguments
    /// * `container` - Container name (e.g., "host-a")
    /// * `path` - Path to create the file at
    /// * `size_kb` - Size in kilobytes
    #[allow(dead_code)]
    pub fn create_file_with_size(&self, container: &str, path: &str, size_kb: u32) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        let output = Command::new("docker")
            .args([
                "exec",
                &container_name,
                "dd",
                "if=/dev/zero",
                &format!("of={}", path),
                "bs=1024",
                &format!("count={}", size_kb),
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "failed to create file: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    /// Get the size of a file in a container.
    #[allow(dead_code)]
    pub fn file_size(&self, container: &str, path: &str) -> Result<u64> {
        let container_name = format!("rcp-test-{}", container);
        let output = Command::new("docker")
            .args(["exec", &container_name, "stat", "-c", "%s", path])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "failed to get file size: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        let size_str = String::from_utf8_lossy(&output.stdout);
        let size = size_str
            .trim()
            .parse::<u64>()
            .map_err(|e| format!("failed to parse file size: {}", e))?;
        Ok(size)
    }

    /// Create a delayed rcpd wrapper script on a host
    ///
    /// This creates a wrapper that delays before executing the real rcpd,
    /// useful for testing connection ordering scenarios.
    /// Returns the path to the created wrapper script.
    ///
    /// If wrapper_path is provided, uses that path. Otherwise generates a unique path.
    /// This allows creating wrappers at the same path on multiple hosts.
    #[allow(dead_code)]
    fn create_delayed_rcpd_wrapper_at_path(
        &self,
        container: &str,
        delay_ms: u64,
        wrapper_path: String,
    ) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        // create wrapper script using a heredoc to avoid stdin issues
        let delay_sec = delay_ms as f64 / 1000.0;
        let create_script = format!(
            r#"cat > {} <<'WRAPPER_EOF'
#!/bin/sh
# Delayed rcpd wrapper for connection ordering tests
sleep {}
# Call the real rcpd binary (absolute path to avoid recursion)
exec /home/testuser/.local/bin/rcpd "$@"
WRAPPER_EOF
chmod +x {}"#,
            wrapper_path, delay_sec, wrapper_path
        );
        // execute the script creation command
        let output = Command::new("docker")
            .args(["exec", &container_name, "sh", "-c", &create_script])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "Failed to create wrapper script: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    /// Execute rcp command with a delayed rcpd on the source host
    ///
    /// This creates wrapper scripts on both source and destination hosts:
    /// - Source: delayed wrapper (to force destination to connect first)
    /// - Destination: non-delayed wrapper (to use the same --rcpd-path)
    ///
    /// Both wrappers are created at the same unique path on their respective hosts
    /// to avoid concurrent test interference while ensuring rcp can find them.
    // used in role ordering tests which are #[ignore]'d by default
    #[allow(dead_code)]
    pub fn exec_rcp_with_delayed_rcpd(
        &self,
        source_host: &str,
        dest_host: &str,
        delay_ms: u64,
        args: &[&str],
    ) -> Result<Output> {
        // generate unique wrapper path (same for both hosts)
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let wrapper_path = format!("/tmp/rcpd-delayed-{}", timestamp);
        // create delayed wrapper on source host
        self.create_delayed_rcpd_wrapper_at_path(source_host, delay_ms, wrapper_path.clone())?;
        // create non-delayed wrapper on destination host (same path)
        self.create_delayed_rcpd_wrapper_at_path(dest_host, 0, wrapper_path.clone())?;
        // build rcp args with custom rcpd path
        let mut rcp_args = vec!["--rcpd-path".to_string(), wrapper_path];
        // add original args
        for arg in args {
            rcp_args.push(arg.to_string());
        }
        // convert to &str refs
        let rcp_args_refs: Vec<&str> = rcp_args.iter().map(|s| s.as_str()).collect();
        self.exec_rcp(&rcp_args_refs)
    }

    /// Clean up test files from all containers
    #[allow(dead_code)]
    pub fn cleanup(&self) -> Result<()> {
        for container in &["master", "host-a", "host-b"] {
            // remove common test file patterns
            let patterns = &[
                "/tmp/test*",
                "/tmp/role-*",
                "/tmp/rapid-*",
                "/tmp/bidir-*",
                "/tmp/rcpd-delayed*",
            ];
            for pattern in patterns {
                let _ = self.exec(
                    container,
                    None,
                    &["sh", "-c", &format!("rm -rf {}", pattern)],
                );
            }
        }
        Ok(())
    }
}

// Note: Drop cleanup removed to avoid interference with concurrent tests.
// Each test is responsible for cleaning up its own files.
// For manual cleanup between test runs, use: cd tests/docker && ./test-helpers.sh cleanup

/// Network condition simulation for chaos testing.
///
/// These functions use Linux `tc` (traffic control) to simulate adverse network
/// conditions like latency, packet loss, and bandwidth limits.
///
/// Requirements:
/// - Containers must have `iproute2` package installed
/// - Containers must have `CAP_NET_ADMIN` capability
impl DockerEnv {
    /// Add latency to a container's network interface.
    ///
    /// This adds a fixed delay to all outgoing packets on the specified interface.
    /// Use `clear_network_conditions` to remove the latency.
    ///
    /// # Arguments
    /// * `container` - Container name (e.g., "host-a")
    /// * `delay_ms` - Delay in milliseconds to add to each packet
    /// * `jitter_ms` - Optional jitter (variation) in milliseconds
    #[allow(dead_code)]
    pub fn add_latency(
        &self,
        container: &str,
        delay_ms: u32,
        jitter_ms: Option<u32>,
    ) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        let delay_spec = match jitter_ms {
            Some(jitter) => format!("{}ms {}ms", delay_ms, jitter),
            None => format!("{}ms", delay_ms),
        };
        let output = Command::new("docker")
            .args([
                "exec",
                &container_name,
                "tc",
                "qdisc",
                "add",
                "dev",
                "eth0",
                "root",
                "netem",
                "delay",
                &delay_spec,
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "failed to add latency: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    /// Add packet loss to a container's network interface.
    ///
    /// This causes a percentage of outgoing packets to be dropped randomly.
    /// Use `clear_network_conditions` to remove the packet loss.
    ///
    /// # Arguments
    /// * `container` - Container name (e.g., "host-a")
    /// * `loss_percent` - Percentage of packets to drop (0.0 to 100.0)
    #[allow(dead_code)]
    pub fn add_packet_loss(&self, container: &str, loss_percent: f32) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        let loss_spec = format!("{}%", loss_percent);
        let output = Command::new("docker")
            .args([
                "exec",
                &container_name,
                "tc",
                "qdisc",
                "add",
                "dev",
                "eth0",
                "root",
                "netem",
                "loss",
                &loss_spec,
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "failed to add packet loss: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    /// Add bandwidth limit to a container's network interface.
    ///
    /// This limits the outgoing bandwidth using a token bucket filter.
    /// Use `clear_network_conditions` to remove the limit.
    ///
    /// # Arguments
    /// * `container` - Container name (e.g., "host-a")
    /// * `rate_kbit` - Maximum rate in kilobits per second
    #[allow(dead_code)]
    pub fn add_bandwidth_limit(&self, container: &str, rate_kbit: u32) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        let rate_spec = format!("{}kbit", rate_kbit);
        // tbf requires burst and latency parameters.
        // burst must be at least rate/HZ where HZ is the kernel timer frequency (typically 250-1000).
        // using rate/8 provides a safe margin that works across different kernel configs while
        // keeping burst small enough for effective rate limiting. minimum 32kbit ensures the
        // bucket can hold at least a few packets.
        let burst = std::cmp::max(rate_kbit / 8, 32);
        let burst_spec = format!("{}kbit", burst);
        let output = Command::new("docker")
            .args([
                "exec",
                &container_name,
                "tc",
                "qdisc",
                "add",
                "dev",
                "eth0",
                "root",
                "tbf",
                "rate",
                &rate_spec,
                "burst",
                &burst_spec,
                "latency",
                "400ms",
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "failed to add bandwidth limit: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    /// Add combined network conditions (latency + packet loss).
    ///
    /// This is useful for simulating realistic degraded network conditions
    /// where multiple issues occur simultaneously.
    ///
    /// # Arguments
    /// * `container` - Container name (e.g., "host-a")
    /// * `delay_ms` - Delay in milliseconds
    /// * `loss_percent` - Percentage of packets to drop
    #[allow(dead_code)]
    pub fn add_network_conditions(
        &self,
        container: &str,
        delay_ms: u32,
        loss_percent: f32,
    ) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        let delay_spec = format!("{}ms", delay_ms);
        let loss_spec = format!("{}%", loss_percent);
        let output = Command::new("docker")
            .args([
                "exec",
                &container_name,
                "tc",
                "qdisc",
                "add",
                "dev",
                "eth0",
                "root",
                "netem",
                "delay",
                &delay_spec,
                "loss",
                &loss_spec,
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "failed to add network conditions: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    /// Clear all network conditions from a container's interface.
    ///
    /// This removes any tc qdisc rules, returning the interface to normal operation.
    /// Safe to call even if no conditions were previously set (ignores "not found" errors).
    #[allow(dead_code)]
    pub fn clear_network_conditions(&self, container: &str) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        let output = Command::new("docker")
            .args([
                "exec",
                &container_name,
                "tc",
                "qdisc",
                "del",
                "dev",
                "eth0",
                "root",
            ])
            .output()?;
        // ignore "RTNETLINK answers: No such file or directory" which means no qdisc was set
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.contains("No such file or directory")
                && !stderr.contains("Cannot delete qdisc")
            {
                return Err(format!("failed to clear network conditions: {}", stderr).into());
            }
        }
        Ok(())
    }
}

/// Process chaos helpers for testing failure scenarios.
///
/// These functions allow killing, pausing, and resuming rcpd processes
/// to test rcp's behavior when remote processes fail unexpectedly.
impl DockerEnv {
    /// Kill all rcpd processes in a container.
    ///
    /// Uses pkill to send SIGKILL to all processes named "rcpd".
    /// Returns Ok even if no rcpd process was running.
    #[allow(dead_code)]
    pub fn kill_rcpd(&self, container: &str) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        let output = Command::new("docker")
            .args(["exec", &container_name, "pkill", "-9", "rcpd"])
            .output()?;
        // pkill returns 1 if no process matched, which is fine
        if !output.status.success() && output.status.code() != Some(1) {
            return Err(format!(
                "failed to kill rcpd: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    /// Pause all rcpd processes in a container (SIGSTOP).
    ///
    /// The process will be frozen until resumed with `resume_rcpd`.
    /// This simulates a hung process.
    #[allow(dead_code)]
    pub fn pause_rcpd(&self, container: &str) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        let output = Command::new("docker")
            .args(["exec", &container_name, "pkill", "-STOP", "rcpd"])
            .output()?;
        if !output.status.success() && output.status.code() != Some(1) {
            return Err(format!(
                "failed to pause rcpd: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    /// Resume all paused rcpd processes in a container (SIGCONT).
    #[allow(dead_code)]
    pub fn resume_rcpd(&self, container: &str) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        let output = Command::new("docker")
            .args(["exec", &container_name, "pkill", "-CONT", "rcpd"])
            .output()?;
        if !output.status.success() && output.status.code() != Some(1) {
            return Err(format!(
                "failed to resume rcpd: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    /// Check if any rcpd process is running in a container.
    #[allow(dead_code)]
    pub fn is_rcpd_running(&self, container: &str) -> Result<bool> {
        let container_name = format!("rcp-test-{}", container);
        let output = Command::new("docker")
            .args(["exec", &container_name, "pgrep", "rcpd"])
            .output()?;
        Ok(output.status.success())
    }

    /// Get PIDs of all rcpd processes in a container.
    #[allow(dead_code)]
    pub fn get_rcpd_pids(&self, container: &str) -> Result<Vec<u32>> {
        let container_name = format!("rcp-test-{}", container);
        let output = Command::new("docker")
            .args(["exec", &container_name, "pgrep", "rcpd"])
            .output()?;
        if !output.status.success() {
            return Ok(vec![]);
        }
        let pids = String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| line.trim().parse::<u32>().ok())
            .collect();
        Ok(pids)
    }
}

/// I/O chaos helpers for testing filesystem error scenarios.
///
/// These functions allow simulating disk full conditions, permission errors,
/// and other I/O failures.
impl DockerEnv {
    /// Mount a tmpfs filesystem at a specified path with a size limit.
    ///
    /// This is useful for simulating disk full (ENOSPC) conditions by creating
    /// a small filesystem that fills up quickly.
    ///
    /// Note: Runs as root since mount requires elevated privileges.
    ///
    /// # Arguments
    /// * `container` - Container name (e.g., "host-b")
    /// * `path` - Mount point path (will be created if it doesn't exist)
    /// * `size_kb` - Size limit in kilobytes
    #[allow(dead_code)]
    pub fn mount_tmpfs(&self, container: &str, path: &str, size_kb: u32) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        // create mount point if it doesn't exist (as root)
        let mkdir_output = Command::new("docker")
            .args(["exec", &container_name, "mkdir", "-p", path])
            .output()?;
        if !mkdir_output.status.success() {
            return Err(format!(
                "failed to create mount point: {}",
                String::from_utf8_lossy(&mkdir_output.stderr)
            )
            .into());
        }
        // mount tmpfs with size limit (as root - mount requires elevated privileges)
        let size_spec = format!("size={}k", size_kb);
        let output = Command::new("docker")
            .args([
                "exec",
                &container_name,
                "mount",
                "-t",
                "tmpfs",
                "-o",
                &size_spec,
                "tmpfs",
                path,
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "failed to mount tmpfs: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        // make it writable by testuser
        let chown_output = Command::new("docker")
            .args(["exec", &container_name, "chown", "testuser:testuser", path])
            .output()?;
        if !chown_output.status.success() {
            return Err(format!(
                "failed to chown tmpfs: {}",
                String::from_utf8_lossy(&chown_output.stderr)
            )
            .into());
        }
        Ok(())
    }

    /// Unmount a tmpfs filesystem.
    ///
    /// Safe to call even if nothing is mounted (ignores "not mounted" errors).
    #[allow(dead_code)]
    pub fn unmount_tmpfs(&self, container: &str, path: &str) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        let output = Command::new("docker")
            .args(["exec", &container_name, "umount", path])
            .output()?;
        // ignore "not mounted" errors
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.contains("not mounted") && !stderr.contains("no mount point") {
                return Err(format!("failed to unmount tmpfs: {}", stderr).into());
            }
        }
        Ok(())
    }

    /// Get available space in bytes at a path in a container.
    #[allow(dead_code)]
    pub fn available_space(&self, container: &str, path: &str) -> Result<u64> {
        let container_name = format!("rcp-test-{}", container);
        // use df to get available space in bytes
        let output = Command::new("docker")
            .args(["exec", &container_name, "df", "--output=avail", "-B1", path])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "failed to get available space: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        // parse output (skip header line)
        let output_str = String::from_utf8_lossy(&output.stdout);
        let avail = output_str
            .lines()
            .nth(1)
            .ok_or("no output from df")?
            .trim()
            .parse::<u64>()
            .map_err(|e| format!("failed to parse df output: {}", e))?;
        Ok(avail)
    }

    /// Change file permissions in a container.
    #[allow(dead_code)]
    pub fn chmod(&self, container: &str, path: &str, mode: &str) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        let output = Command::new("docker")
            .args(["exec", &container_name, "chmod", mode, path])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "failed to chmod: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    /// Change file ownership in a container (requires root).
    #[allow(dead_code)]
    pub fn chown(&self, container: &str, path: &str, owner: &str) -> Result<()> {
        let container_name = format!("rcp-test-{}", container);
        let output = Command::new("docker")
            .args(["exec", &container_name, "chown", owner, path])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "failed to chown: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires Docker containers to be running"]
    fn test_docker_env_creation() {
        let env = DockerEnv::new();
        assert!(env.is_ok(), "Failed to create Docker environment");
    }

    #[test]
    #[ignore = "requires Docker containers to be running"]
    fn test_file_operations() {
        let env = DockerEnv::new().expect("Failed to create environment");
        // use unique filename to avoid collisions with other test binaries
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let test_file = format!("/tmp/test-write-{}.txt", timestamp);
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
        assert!(
            String::from_utf8_lossy(&content).contains("test content"),
            "Content mismatch"
        );
        // clean up
        env.remove_file("host-a", &test_file)
            .expect("Failed to remove file");
    }
}
