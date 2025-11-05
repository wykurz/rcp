//! Binary deployment for rcpd
//!
//! This module handles automatic deployment of rcpd binaries to remote hosts.
//! It transfers static rcpd binaries via SSH using base64 encoding, verifies
//! integrity with SHA-256 checksums, and manages cached versions.

use anyhow::Context;
use std::path::PathBuf;
use std::sync::Arc;

/// Find local static rcpd binary suitable for deployment
///
/// Searches in the following order:
/// 1. Same directory as the current rcp executable
/// 2. PATH via `which rcpd`
///
/// This covers:
/// - Development builds (cargo run/test): rcpd is in same directory as rcp in target/
/// - cargo install: rcpd is in ~/.cargo/bin (which should be in PATH)
/// - nixpkgs: rcpd is available via nix profile (which adds to PATH)
/// - Production deployments: rcp and rcpd are co-located
///
/// # Returns
///
/// Path to the local rcpd binary suitable for deployment
///
/// # Errors
///
/// Returns an error if no suitable binary is found
pub fn find_local_rcpd_binary() -> anyhow::Result<PathBuf> {
    let mut searched_paths = Vec::new();

    // try same directory as current executable first
    // this ensures we use the same build (debug/release) as the running rcp
    // and covers development builds where rcp and rcpd are both in target/
    if let Ok(current_exe) = std::env::current_exe() {
        if let Some(bin_dir) = current_exe.parent() {
            let path = bin_dir.join("rcpd");
            searched_paths.push(format!("Same directory: {}", path.display()));
            if path.exists() && path.is_file() {
                tracing::info!("Found local rcpd binary at {}", path.display());
                return Ok(path);
            }
        }
    }

    // try PATH (covers cargo install, nixpkgs, and other system installations)
    tracing::debug!("Trying to find rcpd in PATH");
    let which_output = std::process::Command::new("which")
        .arg("rcpd")
        .output()
        .ok();

    if let Some(output) = which_output {
        if output.status.success() {
            let path_str = String::from_utf8_lossy(&output.stdout);
            let path_str = path_str.trim();
            if !path_str.is_empty() {
                let path = PathBuf::from(path_str);
                searched_paths.push(format!("PATH: {}", path.display()));
                if path.exists() && path.is_file() {
                    tracing::info!("Found local rcpd binary in PATH: {}", path.display());
                    return Ok(path);
                }
            }
        }
    }

    anyhow::bail!(
        "no local rcpd binary found for deployment\n\
        \n\
        Searched in:\n\
        {}\n\
        \n\
        To use auto-deployment, ensure rcpd is available:\n\
        - cargo install rcp-tools-rcp (installs to ~/.cargo/bin)\n\
        - or add rcpd to PATH\n\
        - or build with: cargo build --release --bin rcpd",
        searched_paths
            .iter()
            .map(|p| format!("- {}", p))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

/// Deploy rcpd binary to remote host
///
/// Transfers the local static rcpd binary to the remote host at
/// `~/.cache/rcp/bin/rcpd-{version}`, verifies the checksum, and returns
/// the path to the deployed binary.
///
/// # Arguments
///
/// * `session` - SSH session to the remote host
/// * `local_rcpd_path` - Path to the local static rcpd binary to deploy
/// * `version` - Semantic version string for the binary
/// * `remote_host` - Hostname for logging/error messages
///
/// # Returns
///
/// The path to the deployed binary on the remote host
///
/// # Errors
///
/// Returns an error if:
/// - Local binary cannot be read
/// - Remote directory creation fails
/// - Transfer fails
/// - Checksum verification fails
pub async fn deploy_rcpd(
    session: &Arc<openssh::Session>,
    local_rcpd_path: &std::path::Path,
    version: &str,
    remote_host: &str,
) -> anyhow::Result<String> {
    tracing::info!(
        "Deploying rcpd {} to remote host '{}'",
        version,
        remote_host
    );

    // read local binary
    let binary = tokio::fs::read(local_rcpd_path).await.with_context(|| {
        format!(
            "failed to read local rcpd binary from {}",
            local_rcpd_path.display()
        )
    })?;

    tracing::info!(
        "Read local rcpd binary ({} bytes) from {}",
        binary.len(),
        local_rcpd_path.display()
    );

    // compute checksum before transfer
    let expected_checksum = compute_sha256(&binary);
    tracing::debug!("Expected SHA-256: {}", hex::encode(&expected_checksum));

    // validate HOME is set and construct remote path
    let home = crate::get_remote_home(session).await?;
    let remote_path = format!("{}/.cache/rcp/bin/rcpd-{}", home, version);

    // transfer binary via base64 over SSH
    transfer_binary_base64(session, &binary, &remote_path).await?;

    tracing::info!("Binary transferred to {}", remote_path);

    // verify checksum on remote
    verify_remote_checksum(session, &remote_path, &expected_checksum).await?;

    tracing::info!("Checksum verified successfully");

    Ok(remote_path)
}

/// Transfer binary to remote host using base64 encoding
///
/// Creates the target directory if needed, transfers the binary via base64
/// encoding through SSH stdin, and sets appropriate permissions (700).
///
/// # Arguments
///
/// * `session` - SSH session to the remote host
/// * `binary` - Binary content to transfer
/// * `remote_path` - Destination path on remote host (should use $HOME, will be created)
///
/// # Errors
///
/// Returns an error if directory creation, transfer, or permission setting fails
async fn transfer_binary_base64(
    session: &Arc<openssh::Session>,
    binary: &[u8],
    remote_path: &str,
) -> anyhow::Result<()> {
    use base64::Engine;

    // encode binary as base64
    let encoded = base64::engine::general_purpose::STANDARD.encode(binary);

    // extract directory and filename from remote_path
    // remote_path format: $HOME/.cache/rcp/bin/rcpd-{version}
    let path = std::path::Path::new(remote_path);
    let dir = path
        .parent()
        .context("remote path must have a parent directory")?
        .to_str()
        .context("remote path parent must be valid UTF-8")?;
    let filename = path
        .file_name()
        .context("remote path must have a filename")?
        .to_str()
        .context("remote filename must be valid UTF-8")?;

    // use $$ (shell PID) for unique temp filename
    // extract version from filename (format: rcpd-{version})
    let temp_filename = if let Some(version) = filename.strip_prefix("rcpd-") {
        format!(".rcpd-{}.tmp.$$", version)
    } else {
        format!(".{}.tmp.$$", filename)
    };

    // escape all variables for safe shell usage
    let dir_escaped = crate::shell_escape(dir);
    let temp_path = format!("{}/{}", dir, temp_filename);
    let temp_path_escaped = crate::shell_escape(&temp_path);
    let final_path = format!("{}/{}", dir, filename);
    let final_path_escaped = crate::shell_escape(&final_path);

    let cmd = format!(
        "mkdir -p {} && \
         base64 -d > {} && \
         chmod 700 {} && \
         mv -f {} {}",
        dir_escaped, temp_path_escaped, temp_path_escaped, temp_path_escaped, final_path_escaped
    );

    tracing::debug!("Running remote command: mkdir && base64 && chmod");

    let mut child = session
        .command("sh")
        .arg("-c")
        .arg(&cmd)
        .stdin(openssh::Stdio::piped())
        .stdout(openssh::Stdio::piped())
        .stderr(openssh::Stdio::piped())
        .spawn()
        .await
        .context("failed to spawn remote command for binary transfer")?;

    // take handles for all streams
    let mut stdin = child
        .stdin()
        .take()
        .context("failed to get stdin for remote command")?;

    let mut stdout = child
        .stdout()
        .take()
        .context("failed to get stdout for remote command")?;

    let mut stderr = child
        .stderr()
        .take()
        .context("failed to get stderr for remote command")?;

    // write to stdin and close it before reading stdout/stderr
    // this ensures the child process receives EOF on stdin before we wait for it to finish
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // write all base64 data to stdin
    stdin
        .write_all(encoded.as_bytes())
        .await
        .context("failed to write base64 data to remote stdin")?;

    // shutdown and explicitly drop stdin to ensure EOF is sent to child process
    stdin.shutdown().await.context("failed to shutdown stdin")?;
    drop(stdin);

    // now read stdout and stderr to completion
    // these will complete once the child process exits and closes the pipes
    let stdout_fut = async {
        let mut buf = Vec::new();
        let _ = stdout.read_to_end(&mut buf).await;
        buf
    };

    let stderr_fut = async {
        let mut buf = Vec::new();
        let _ = stderr.read_to_end(&mut buf).await;
        buf
    };

    let (_stdout_data, stderr_data) = tokio::join!(stdout_fut, stderr_fut);

    // wait for command to complete
    let status = child
        .wait()
        .await
        .context("failed to wait for remote command completion")?;

    if !status.success() {
        let stderr = String::from_utf8_lossy(&stderr_data);
        anyhow::bail!(
            "failed to transfer binary to remote host\n\
            \n\
            stderr: {}\n\
            \n\
            This may indicate:\n\
            - Insufficient disk space on remote host\n\
            - Permission denied creating $HOME/.cache/rcp/bin\n\
            - base64 command not available on remote host",
            stderr
        );
    }

    Ok(())
}

/// Verify checksum of transferred binary on remote host
///
/// Runs `sha256sum` on the remote host and compares the result with
/// the expected checksum.
///
/// # Arguments
///
/// * `session` - SSH session to the remote host
/// * `remote_path` - Path to the binary on the remote host (should use $HOME)
/// * `expected_checksum` - Expected SHA-256 digest
///
/// # Errors
///
/// Returns an error if the checksum command fails or doesn't match
async fn verify_remote_checksum(
    session: &Arc<openssh::Session>,
    remote_path: &str,
    expected_checksum: &[u8],
) -> anyhow::Result<()> {
    // escape remote_path for safe shell usage
    let cmd = format!("sha256sum {}", crate::shell_escape(remote_path));

    tracing::debug!("Verifying checksum on remote host");

    let output = session
        .command("sh")
        .arg("-c")
        .arg(&cmd)
        .output()
        .await
        .context("failed to run sha256sum on remote host")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "failed to compute checksum on remote host\n\
            stderr: {}",
            stderr
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    // sha256sum output format: "checksum filename"
    let remote_checksum = stdout
        .split_whitespace()
        .next()
        .context("unexpected sha256sum output format")?;

    let expected_hex = hex::encode(expected_checksum);

    if remote_checksum != expected_hex {
        anyhow::bail!(
            "checksum mismatch after transfer\n\
            \n\
            Expected: {}\n\
            Got:      {}\n\
            \n\
            The binary transfer may have been corrupted.\n\
            Please try again or check network connectivity.",
            expected_hex,
            remote_checksum
        );
    }

    Ok(())
}

/// Compute SHA-256 hash of data
fn compute_sha256(data: &[u8]) -> Vec<u8> {
    use ring::digest;
    digest::digest(&digest::SHA256, data).as_ref().to_vec()
}

/// Clean up old rcpd versions on remote host
///
/// Keeps the most recent `keep_count` versions and removes older ones.
/// This prevents disk space from growing unbounded as versions are deployed.
///
/// # Arguments
///
/// * `session` - SSH session to the remote host
/// * `keep_count` - Number of recent versions to keep (default: 3)
///
/// # Errors
///
/// Returns an error if the cleanup command fails (but this is not fatal)
pub async fn cleanup_old_versions(
    session: &Arc<openssh::Session>,
    keep_count: usize,
) -> anyhow::Result<()> {
    tracing::debug!("Cleaning up old rcpd versions (keeping {})", keep_count);

    // validate HOME is set before constructing the cache path
    // if this fails, we log and return Ok since cleanup is best-effort
    let home = match crate::get_remote_home(session).await {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(
                "cleanup of old versions skipped (HOME not available): {:#}",
                e
            );
            return Ok(());
        }
    };

    // list all rcpd-* files sorted by modification time (newest first)
    // keep the newest N, remove the rest
    let cache_dir = format!("{}/.cache/rcp/bin", home);
    let cmd = format!(
        "cd {} 2>/dev/null && ls -t rcpd-* 2>/dev/null | tail -n +{} | xargs -r rm -f",
        crate::shell_escape(&cache_dir),
        keep_count + 1
    );

    let output = session
        .command("sh")
        .arg("-c")
        .arg(&cmd)
        .output()
        .await
        .context("failed to run cleanup command on remote host")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // log but don't fail - cleanup is best-effort
        tracing::warn!("cleanup of old versions failed (non-fatal): {}", stderr);
    } else {
        tracing::debug!("Old versions cleaned up successfully");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_sha256() {
        let data = b"hello world";
        let hash = compute_sha256(data);
        // known SHA-256 of "hello world"
        let expected =
            hex::decode("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
                .unwrap();
        assert_eq!(hash, expected);
    }

    #[test]
    fn test_compute_sha256_empty() {
        let data = b"";
        let hash = compute_sha256(data);
        // known SHA-256 of empty string
        let expected =
            hex::decode("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
                .unwrap();
        assert_eq!(hash, expected);
    }

    #[test]
    fn test_compute_sha256_binary() {
        // test with actual binary data (non-UTF8)
        let data: Vec<u8> = (0..256).map(|i| i as u8).collect();
        let hash = compute_sha256(&data);
        // verify it produces a 32-byte hash
        assert_eq!(hash.len(), 32);
        // verify it's deterministic
        let hash2 = compute_sha256(&data);
        assert_eq!(hash, hash2);
    }
}
