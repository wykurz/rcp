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
