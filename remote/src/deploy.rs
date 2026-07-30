//! Binary deployment for rcpd
//!
//! This module handles automatic deployment of rcpd binaries to remote hosts.
//! It transfers static rcpd binaries via SSH using base64 encoding, verifies integrity with SHA-256 checksums, and manages cached versions.
//!
//! ## Atomicity and Concurrent Deployment Safety
//!
//! The deployment mechanism is designed to handle concurrent deployments from multiple rcp instances safely:
//!
//! ### Atomic Operations
//!
//! 1. **Unique Temporary Files**: Each deployment writes to a temp file whose name is generated
//!    locally, by the deploying process — `.rcpd-{version}.tmp.{pid}-{random}` — so no two
//!    deployments, from this host or any other, can share one. The name must NOT be built from a
//!    shell expansion such as `$$`: every path this module passes to the remote shell goes through
//!    `shell_escape`, which single-quotes it, so an expansion would be taken literally and every
//!    deployment would collide on the same file.
//!
//! 2. **Atomic Rename**: The final deployment step uses `mv -f` which is atomic
//!    on POSIX-compliant filesystems. This means:
//!    - The binary is either fully present at the final location or not present at all
//!    - No partial writes are visible to readers
//!    - Concurrent renames of the same file complete in a well-defined order
//!
//! 3. **Verify-Then-Publish**: The deployment sequence ensures the binary is:
//!    - Fully written to the temp file
//!    - Marked executable (chmod 700)
//!    - Checksummed **on the temp file**
//!    - Only then moved atomically to the final location
//!
//!    The order matters: checksumming after publication would mean a corrupt or truncated transfer
//!    is briefly reachable under the name other processes execute, and is still sitting there when
//!    the deployment reports failure.
//!
//! ### Race Condition Scenarios
//!
//! **Scenario 1: Multiple rcp instances deploying the same version concurrently**
//!
//! - Each uses a unique temp file (`.rcpd-0.22.0.tmp.1234-a1b2c3d4e5f60718`, ...)
//! - Both successfully write and verify their own temp file
//! - Both then attempt `mv -f <their temp> rcpd-0.22.0`
//! - The filesystem ensures one wins atomically, the other overwrites atomically
//! - Result: Final binary is valid — each candidate was complete and checksummed before its rename,
//!   so no descriptor is still writing into whichever inode ends up published
//!
//! **Scenario 2: One deployment while another is reading**
//!
//! - Reader opens `rcpd-0.22.0` and gets a valid file descriptor
//! - Writer completes deployment and `mv -f` replaces the inode
//! - Reader continues reading from the original inode (POSIX semantics)
//! - Result: Reader gets the old version (but it's still valid)
//!
//! **Scenario 3: Deployment interrupted (network failure, SIGKILL)**
//!
//! - Temp file may be left in `.cache/rcp/bin/.rcpd-{version}.tmp.*`
//! - Final file is either:
//!   - Not present (deployment never completed)
//!   - Present and valid (mv completed before interruption)
//! - Temp files are hidden (dotfiles) and don't interfere with discovery
//! - Result: Safe to retry; old temp files are harmless — each name is unique to the deployment
//!   that created it, so nothing else ever opens or executes one, and a retry never adopts a
//!   half-written file left by an earlier attempt
//!
//! ### Assumptions
//!
//! 1. **POSIX Filesystem Semantics**: The deployment assumes the remote filesystem
//!    supports atomic `mv` (rename) operations. This is true for all POSIX-compliant
//!    filesystems (ext4, xfs, btrfs, etc.) but may not hold for network filesystems
//!    with relaxed consistency (NFSv3 without proper locking).
//!
//! 2. **Unique Temp Names**: The random component of a temp file name is assumed not to collide
//!    with a concurrent deployment's. Two deployments would have to draw the same 64-bit value
//!    *and* run from the same pid to collide.
//!
//! 3. **Checksum Integrity**: SHA-256 checksums are assumed to be collision-resistant.
//!    If two different binaries produce the same checksum (astronomically unlikely),
//!    the deployment would consider them identical.
//!
//! 4. **No Malicious Interference**: The deployment assumes the remote host is not
//!    actively malicious (no adversary replacing files during deployment). Protection
//!    against malicious hosts is provided by SSH authentication, not by this module.
//!
//! ### Non-Atomic Operations
//!
//! The following operations are **not** atomic and may observe intermediate states:
//!
//! - **Cleanup of old versions**: Uses `ls -t | tail | xargs rm` which may race with
//!   concurrent deployments. This is acceptable because cleanup only removes old
//!   versions, never the current version being deployed. Worst case: a version is
//!   not cleaned up and remains on disk.
//!
//! - **Directory creation**: `mkdir -p` may race with concurrent deployments creating
//!   the same directory. This is safe because `mkdir -p` is idempotent and succeeds
//!   if the directory already exists.

use anyhow::Context;
use std::path::PathBuf;
use std::sync::Arc;

const TRANSFER_HINTS: &str = "\
    This may indicate:\n\
    - Insufficient disk space on remote host\n\
    - Permission denied creating $HOME/.cache/rcp/bin\n\
    - base64 command not available on remote host";

/// Build an error message for a failed stdin write during binary transfer.
///
/// When writing base64 data to the remote SSH process fails (typically a broken
/// pipe because the remote command exited early), this formats the error to
/// include remote stderr (which reveals the actual cause) and the exit status.
fn format_write_error(
    write_err: &std::io::Error,
    stderr_data: &[u8],
    status: &dyn std::fmt::Display,
) -> String {
    let stderr = String::from_utf8_lossy(stderr_data);
    let stderr = stderr.trim();
    if stderr.is_empty() {
        format!(
            "failed to write base64 data to remote stdin: {write_err}\n\
            \n\
            remote command exited with status: {status}\n\
            remote stderr was empty\n\
            \n\
            {TRANSFER_HINTS}"
        )
    } else {
        format!(
            "failed to write base64 data to remote stdin: {write_err}\n\
            \n\
            remote stderr: {stderr}\n\
            \n\
            {TRANSFER_HINTS}"
        )
    }
}

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
    if let Ok(current_exe) = std::env::current_exe()
        && let Some(bin_dir) = current_exe.parent()
    {
        let path = bin_dir.join("rcpd");
        searched_paths.push(format!("Same directory: {}", path.display()));
        if path.exists() && path.is_file() {
            tracing::info!("Found local rcpd binary at {}", path.display());
            return Ok(path);
        }
    }

    // try PATH (covers cargo install, nixpkgs, and other system installations)
    tracing::debug!("Trying to find rcpd in PATH");
    let which_output = std::process::Command::new("which")
        .arg("rcpd")
        .output()
        .ok();

    if let Some(output) = which_output
        && output.status.success()
    {
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

    // The temp path is chosen HERE, before anything can create it, so that every failure below has
    // a name to clean up. Picking it inside the transfer and returning it only on success meant a
    // transfer that failed *after* the remote shell had created the file — a broken pipe, a full
    // disk, a killed command — leaked it silently, and every retry leaked another:
    // `cleanup_old_versions` globs `rcpd-*`, which never matches these dotfiles.
    let temp_path = remote_temp_path(&remote_path)?;
    let staged = stage_and_publish(
        session,
        &binary,
        &remote_path,
        &temp_path,
        &expected_checksum,
    )
    .await;
    if staged.is_err() {
        // ONE funnel. Everything between choosing the name and publishing it exits through here, so
        // no path — present or future — can forget to clean up after itself.
        remove_remote_temp(session, &temp_path).await;
    }
    staged?;

    Ok(remote_path)
}

/// Transfer the binary to `temp_path`, verify it there, and only then publish it to `remote_path`.
///
/// Every error is the caller's cue to remove `temp_path`; this function deliberately does no
/// cleanup of its own, so there is exactly one place that decides what happens to a temp file that
/// will not be published (see [`deploy_rcpd_binary`]).
async fn stage_and_publish(
    session: &Arc<openssh::Session>,
    binary: &[u8],
    remote_path: &str,
    temp_path: &str,
    expected_checksum: &[u8],
) -> anyhow::Result<()> {
    transfer_binary_base64(session, binary, temp_path).await?;
    tracing::info!("Binary transferred to {}", temp_path);

    // verify BEFORE publishing: a truncated or corrupt transfer must never become visible under the
    // name other processes execute, so the checksum is taken on the temp file and the rename
    // happens only if it matches.
    verify_remote_checksum(session, temp_path, expected_checksum).await?;
    tracing::info!("Checksum verified successfully");

    publish_remote_binary(session, temp_path, remote_path).await
}

/// Build the remote shell command that STAGES the binary — and only stages it.
///
/// 1. `mkdir -p` — create the cache directory (idempotent, safe under concurrency)
/// 2. `base64 -d >` — decode stdin into this deployment's own temp file
/// 3. `chmod 700` — mark it executable
///
/// Publication is deliberately NOT part of this command, and must not be added back to it: the
/// caller checksums the temp file and renames it only if that matches, so a truncated or corrupt
/// transfer is never reachable under the name other processes execute. `&&` throughout, to stop at
/// the first failure.
fn transfer_command(dir: &str, temp_path: &str) -> String {
    let temp_path_escaped = crate::shell_escape(temp_path);
    format!(
        "mkdir -p {} && \
         base64 -d > {} && \
         chmod 700 {}",
        crate::shell_escape(dir),
        temp_path_escaped,
        temp_path_escaped
    )
}

/// Build the remote shell command that PUBLISHES a verified temp file.
///
/// Separate from [`transfer_command`] because the checksum runs between the two.
fn publish_command(temp_path: &str, remote_path: &str) -> String {
    format!(
        "mv -f {} {}",
        crate::shell_escape(temp_path),
        crate::shell_escape(remote_path)
    )
}

/// Build this deployment's private temp path from the final path it will eventually be renamed to.
///
/// Split out so the caller can own the name for the whole deployment — see [`deploy_rcpd_binary`].
fn remote_temp_path(remote_path: &str) -> anyhow::Result<String> {
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
    Ok(format!("{}/{}", dir, unique_temp_filename(filename)))
}

/// Publish a verified temp file to its final path with an atomic rename.
///
/// `mv -f` is `rename(2)` within one directory: either the new inode appears under `remote_path` or
/// the old one remains, never a partial write. Concurrent deployments each complete their own
/// rename in some order, and every one of them publishes a fully written, checksum-verified binary,
/// so whichever lands last is as good as any other.
async fn publish_remote_binary(
    session: &Arc<openssh::Session>,
    temp_path: &str,
    remote_path: &str,
) -> anyhow::Result<()> {
    let cmd = publish_command(temp_path, remote_path);
    let output = session
        .command("sh")
        .arg("-c")
        .arg(&cmd)
        .output()
        .await
        .context("failed to run the publishing rename on the remote host")?;
    if !output.status.success() {
        // no cleanup here: `deploy_rcpd_binary`'s single funnel removes the temp file on ANY error out
        // of `stage_and_publish`, this one included. Removing it here too issued a second remote
        // `rm -f` for the same path and contradicted the one-funnel contract this call chain documents.
        anyhow::bail!(
            "failed to publish the transferred binary to {}\n\
            \n\
            stderr: {}",
            remote_path,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

/// Best-effort removal of a temp file that will not be published.
///
/// Failures here are logged, never propagated: the caller is already reporting the real problem,
/// and a leftover temp file is inert — it carries a name unique to this deployment, so nothing
/// else will ever open or execute it.
async fn remove_remote_temp(session: &Arc<openssh::Session>, temp_path: &str) {
    let cmd = format!("rm -f {}", crate::shell_escape(temp_path));
    match session.command("sh").arg("-c").arg(&cmd).output().await {
        Ok(output) if output.status.success() => {}
        Ok(output) => tracing::warn!(
            "could not remove the temp file {} on the remote host: {}",
            temp_path,
            String::from_utf8_lossy(&output.stderr)
        ),
        Err(error) => tracing::warn!(
            "could not remove the temp file {} on the remote host: {:#}",
            temp_path,
            &error
        ),
    }
}

/// Build the name of the temp file a single deployment writes to.
///
/// Unique per deployment, and generated HERE rather than by the remote shell. Every path this
/// module hands to the remote shell goes through [`shell_escape`](crate::shell_escape), which wraps
/// it in single quotes — so a shell expansion such as `$$` embedded in the name would be passed
/// through *literally*, and every concurrent deployment would collide on one `.tmp.$$` file. That
/// is not merely wasteful: one deployment could `mv` the shared file to the final path while
/// another was still writing through its own descriptor, landing those writes directly on the
/// published inode.
///
/// `rand::random` is what makes the name unique; the pid rides along only so a temp file left
/// behind by an interrupted deployment can be traced to the process that created it. The result is
/// a dotfile, so it does not interfere with binary discovery.
fn unique_temp_filename(filename: &str) -> String {
    let unique = format!("{}-{:016x}", std::process::id(), rand::random::<u64>());
    // extract version from filename (format: rcpd-{version})
    match filename.strip_prefix("rcpd-") {
        Some(version) => format!(".rcpd-{}.tmp.{}", version, unique),
        None => format!(".{}.tmp.{}", filename, unique),
    }
}

/// Transfer binary to remote host using base64 encoding
///
/// Creates the target directory if needed, transfers the binary via base64
/// encoding through SSH stdin, and sets appropriate permissions (700).
///
/// The binary lands on a temp file private to this deployment and is NOT published under
/// `remote_path` — the caller verifies the transfer and renames it. See [`publish_remote_binary`].
///
/// # Arguments
///
/// * `session` - SSH session to the remote host
/// * `binary` - Binary content to transfer
/// * `temp_path` - This deployment's private staging path, from [`remote_temp_path`]. Its parent
///   directory is created; the final path is not touched here.
///
/// # Errors
///
/// Returns an error if directory creation, transfer, or permission setting fails. The caller owns
/// `temp_path` and is responsible for removing it on any error — see [`deploy_rcpd_binary`].
async fn transfer_binary_base64(
    session: &Arc<openssh::Session>,
    binary: &[u8],
    temp_path: &str,
) -> anyhow::Result<()> {
    use base64::Engine;

    // encode binary as base64
    let encoded = base64::engine::general_purpose::STANDARD.encode(binary);

    // the staging directory is the temp file's own parent — the same directory the final path lives
    // in, which is what makes the publishing `mv` a same-directory rename.
    let dir = std::path::Path::new(temp_path)
        .parent()
        .context("temp path must have a parent directory")?
        .to_str()
        .context("temp path parent must be valid UTF-8")?;

    let cmd = transfer_command(dir, temp_path);

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

    // write all base64 data to stdin, capturing errors instead of returning
    // immediately — if this fails (e.g. broken pipe), we still need to read
    // stderr to learn why the remote command failed
    let write_result = stdin.write_all(encoded.as_bytes()).await;

    if write_result.is_ok() {
        // shutdown stdin to send EOF to the remote `base64 -d` process
        stdin.shutdown().await.context("failed to shutdown stdin")?;
    }
    // drop stdin so the remote process can finish even if the write failed
    drop(stdin);

    // read stdout and stderr to completion — stderr is critical for diagnostics
    // when the remote command fails before accepting all input
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

    // if writing to stdin failed (broken pipe), the remote command exited early —
    // include stderr so the user sees the actual cause (e.g. "Permission denied")
    if let Err(write_err) = write_result {
        anyhow::bail!("{}", format_write_error(&write_err, &stderr_data, &status));
    }

    if !status.success() {
        let stderr = String::from_utf8_lossy(&stderr_data);
        anyhow::bail!(
            "failed to transfer binary to remote host\n\
            \n\
            stderr: {}\n\
            \n\
            {TRANSFER_HINTS}",
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
    use sha2::{Digest, Sha256};
    Sha256::digest(data).to_vec()
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

    // the temp name is what keeps two concurrent deployments off each other's file. It has to be
    // built here rather than by the remote shell: `shell_escape` single-quotes every path, so a
    // `$$` in the name would never expand and every deployment would share one `.tmp.$$` file —
    // letting one rename it into place while another was still writing through its descriptor.
    #[test]
    fn unique_temp_filename_carries_no_shell_expansion() {
        let name = unique_temp_filename("rcpd-0.22.0");
        assert!(
            !name.contains('$'),
            "a shell expansion in the name is inert once single-quoted: {name}"
        );
        assert!(
            !name.contains('/') && !name.contains('\''),
            "the name must stay a single, quote-free path component: {name}"
        );
        assert!(
            name.starts_with(".rcpd-0.22.0.tmp."),
            "the version must stay recoverable from a leftover temp file: {name}"
        );
    }

    #[test]
    fn unique_temp_filename_differs_between_deployments() {
        let names: std::collections::HashSet<String> = (0..100)
            .map(|_| unique_temp_filename("rcpd-0.22.0"))
            .collect();
        assert_eq!(
            names.len(),
            100,
            "every deployment must get its own temp file"
        );
    }

    // a filename that is not `rcpd-{version}` still has to produce a hidden, unique temp name
    #[test]
    fn unique_temp_filename_handles_an_unversioned_name() {
        let name = unique_temp_filename("rcpd");
        assert!(name.starts_with(".rcpd.tmp."), "{name}");
        assert!(!name.contains('$'), "{name}");
    }

    // The two deployment commands are what actually encode "stage, verify, then publish", and they
    // are the only part of that sequence testable without a remote host. Both assertions below fail
    // against the implementation this replaced: it appended `mv -f ... rcpd-{version}` to the
    // staging command (publishing before the checksum ran) and named the temp file `.tmp.$$`, which
    // `shell_escape` single-quotes so it never expanded.

    #[test]
    fn transfer_command_does_not_publish() {
        let temp_path = remote_temp_path("/home/u/.cache/rcp/bin/rcpd-0.22.0").unwrap();
        let cmd = transfer_command("/home/u/.cache/rcp/bin", &temp_path);
        assert!(
            !cmd.contains("mv "),
            "staging must not publish - the checksum runs between the two: {cmd}"
        );
        assert!(
            !cmd.contains("rcpd-0.22.0'") && !cmd.contains("/rcpd-0.22.0 "),
            "the final path must not appear in the staging command: {cmd}"
        );
        // it does stage: mkdir, decode into the temp file, make it executable
        assert!(cmd.contains("mkdir -p"), "{cmd}");
        assert!(cmd.contains("base64 -d >"), "{cmd}");
        assert!(cmd.contains("chmod 700"), "{cmd}");
    }

    #[test]
    fn transfer_command_stages_a_shell_inert_unique_path() {
        let final_path = "/home/u/.cache/rcp/bin/rcpd-0.22.0";
        let first = transfer_command(
            "/home/u/.cache/rcp/bin",
            &remote_temp_path(final_path).unwrap(),
        );
        let second = transfer_command(
            "/home/u/.cache/rcp/bin",
            &remote_temp_path(final_path).unwrap(),
        );
        for cmd in [&first, &second] {
            assert!(
                !cmd.contains('$'),
                "every path is single-quoted, so a shell expansion would be inert: {cmd}"
            );
            assert!(
                cmd.contains("'/home/u/.cache/rcp/bin/.rcpd-0.22.0.tmp."),
                "the staging path must be a hidden sibling of the final path: {cmd}"
            );
        }
        assert_ne!(
            first, second,
            "two deployments must never stage through the same file"
        );
    }

    #[test]
    fn publish_command_renames_the_staged_file_over_the_final_path() {
        let cmd = publish_command(
            "/home/u/.cache/rcp/bin/.rcpd-0.22.0.tmp.7-abc",
            "/home/u/.cache/rcp/bin/rcpd-0.22.0",
        );
        assert_eq!(
            cmd,
            "mv -f '/home/u/.cache/rcp/bin/.rcpd-0.22.0.tmp.7-abc' \
             '/home/u/.cache/rcp/bin/rcpd-0.22.0'"
        );
    }

    #[test]
    fn remote_temp_path_stages_beside_the_final_path() {
        let temp = remote_temp_path("/home/u/.cache/rcp/bin/rcpd-0.22.0").unwrap();
        assert_eq!(
            std::path::Path::new(&temp).parent().unwrap(),
            std::path::Path::new("/home/u/.cache/rcp/bin"),
            "same directory, so publishing is a rename rather than a cross-device copy"
        );
        assert!(
            remote_temp_path("rcpd-0.22.0").is_ok(),
            "bare relative name"
        );
        assert!(remote_temp_path("/").is_err(), "no filename to derive from");
    }

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

    #[test]
    fn write_error_with_stderr_includes_remote_output() {
        let err = std::io::Error::from_raw_os_error(32); // EPIPE
        let stderr = b"mkdir: cannot create directory: Permission denied";
        let msg = format_write_error(&err, stderr, &"exited with 1");
        assert!(msg.contains("Broken pipe"), "should contain write error");
        assert!(
            msg.contains("Permission denied"),
            "should contain remote stderr"
        );
        assert!(
            msg.contains("This may indicate"),
            "should contain hint text"
        );
        // should NOT contain the exit status line when stderr is present
        assert!(
            !msg.contains("remote command exited with status"),
            "should omit status when stderr is available"
        );
    }

    #[test]
    fn write_error_without_stderr_includes_exit_status() {
        let err = std::io::Error::from_raw_os_error(32);
        let stderr = b"";
        let msg = format_write_error(&err, stderr, &"exited with 126");
        assert!(msg.contains("Broken pipe"), "should contain write error");
        assert!(
            msg.contains("remote command exited with status: exited with 126"),
            "should contain exit status"
        );
        assert!(
            msg.contains("remote stderr was empty"),
            "should note stderr was empty"
        );
        assert!(
            msg.contains("This may indicate"),
            "should contain hint text"
        );
    }

    #[test]
    fn write_error_trims_whitespace_only_stderr() {
        let err = std::io::Error::from_raw_os_error(32);
        let stderr = b"  \n\t  ";
        let msg = format_write_error(&err, stderr, &"exited with 1");
        // whitespace-only stderr should be treated as empty
        assert!(
            msg.contains("remote stderr was empty"),
            "whitespace-only stderr should be treated as empty"
        );
    }
}
