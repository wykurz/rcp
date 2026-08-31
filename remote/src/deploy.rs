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
//! 2. **Atomic Rename**: The final deployment step uses Linux GNU/BusyBox `mv -fT`, preventing an
//!    existing directory target from absorbing the temp file while preserving a same-directory
//!    atomic rename. This means:
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
//! - Both then attempt `mv -fT <their temp> rcpd-0.22.0`
//! - The filesystem ensures one wins atomically, the other overwrites atomically
//! - Result: Final binary is valid — each candidate was complete and checksummed before its rename,
//!   so no descriptor is still writing into whichever inode ends up published
//!
//! **Scenario 2: One deployment while another is reading**
//!
//! - Reader opens `rcpd-0.22.0` and gets a valid file descriptor
//! - Writer completes deployment and `mv -fT` replaces the inode
//! - Reader continues reading from the original inode (POSIX semantics)
//! - Result: Reader gets the old version (but it's still valid)
//!
//! **Scenario 3: Deployment interrupted**
//!
//! - Transfer failure, SSH-channel disconnect, and handled `HUP`/`INT`/`TERM` run the remote
//!   shell's `EXIT` trap and remove the temp file
//! - An unhandled remote `SIGKILL` may leave a temp file in
//!   `.cache/rcp/bin/.rcpd-{version}.tmp.*`
//! - For process and channel interruption, the final file is either:
//!   - Not present (deployment never completed)
//!   - Present and valid (mv completed before interruption)
//! - Temp files are hidden (dotfiles) and don't interfere with discovery
//! - Result: Safe to retry; old temp files are harmless — each name is unique to the deployment
//!   that created it, so nothing else ever opens or executes one, and a retry never adopts a
//!   half-written file left by an earlier attempt
//! - Host crash and power-loss durability follows the remote filesystem's guarantees. Deployment
//!   does not fsync the staged file or cache directory, so atomic visibility does not imply that a
//!   completed rename survives a crash.
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
//! - **Cleanup of old versions**: Uses `ls -t | tail | xargs rm` and may race with another
//!   invocation between that invocation selecting a cached binary and spawning it. Cleanup is
//!   best-effort cache hygiene, not part of the atomic publication guarantee.
//!
//! - **Directory creation**: `mkdir -p` may race with concurrent deployments creating
//!   the same directory. This is safe because `mkdir -p` is idempotent and succeeds
//!   if the directory already exists.

use anyhow::Context;
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::time::Duration;

const LOCAL_RCPD_VERSION_TIMEOUT: Duration = Duration::from_secs(2);
const REMOTE_STAGING_OWNER_GRACE: Duration = Duration::from_secs(1);
const REMOTE_STAGING_FINALIZATION_MINIMUM: Duration = Duration::from_secs(60);
const DEPLOYMENT_READY_RECORD: &str = "RCP_DEPLOY_READY";
const DEPLOYMENT_READY_OUTPUT_LIMIT: usize = 4096;
const DEPLOYMENT_WRITE_CHUNK_SIZE: usize = 64 * 1024;

const TRANSFER_HINTS: &str = "\
    This may indicate:\n\
    - Insufficient disk space on remote host\n\
    - Permission denied creating $HOME/.cache/rcp/bin\n\
    - base64 command not available on remote host";

/// Build diagnostic context for a failed stdin write during binary transfer.
///
/// The original write error remains the source in the error chain. This context adds remote stderr
/// when available, or the exit status when stderr is empty.
fn format_write_error_context(stderr_data: &[u8], status: &dyn std::fmt::Display) -> String {
    let stderr = String::from_utf8_lossy(stderr_data);
    let stderr = stderr.trim();
    if stderr.is_empty() {
        format!(
            "failed to write base64 data to remote stdin\n\
            \n\
            remote command exited with status: {status}\n\
            remote stderr was empty\n\
            \n\
            {TRANSFER_HINTS}"
        )
    } else {
        format!(
            "failed to write base64 data to remote stdin\n\
            \n\
            remote stderr: {stderr}\n\
            \n\
            {TRANSFER_HINTS}"
        )
    }
}

fn validate_transfer_completion(
    write_result: Option<anyhow::Result<()>>,
    shutdown_result: Option<std::io::Result<()>>,
    stderr_data: &crate::CapturedOutput,
    status: &std::process::ExitStatus,
) -> anyhow::Result<()> {
    if let Some(Err(write_error)) = write_result {
        let context = format_write_error_context(stderr_data.rendered().as_bytes(), status);
        return Err(write_error.context(context));
    }

    if !status.success() {
        let stderr = stderr_data.rendered();
        anyhow::bail!(
            "failed to transfer binary to remote host\n\
            \n\
            stderr: {}\n\
            \n\
            {TRANSFER_HINTS}",
            stderr
        );
    }

    if let Some(shutdown_result) = shutdown_result {
        shutdown_result.context("failed to shutdown stdin")?;
    }
    Ok(())
}

fn reconcile_transfer_finish<T>(
    write_result: Option<anyhow::Result<()>>,
    finish_result: anyhow::Result<T>,
) -> anyhow::Result<(Option<anyhow::Result<()>>, T)> {
    match (write_result, finish_result) {
        (Some(Err(write_error)), Err(finish_error)) => {
            tracing::debug!(
                "remote deployment finalization also failed after the payload write failed: {finish_error:#}"
            );
            Err(write_error)
        }
        (write_result, Ok(finish)) => Ok((write_result, finish)),
        (_, Err(finish_error)) => Err(finish_error),
    }
}

fn path_discovery_result(
    preparation: &crate::PreparationContext,
    result: anyhow::Result<Output>,
    searched_paths: &mut Vec<String>,
) -> anyhow::Result<Option<Output>> {
    match result {
        Ok(output) => Ok(Some(output)),
        Err(error) => {
            // a peer cancellation is part of the preparation result, not a missing PATH entry
            preparation.ensure_active()?;
            tracing::debug!("local rcpd PATH discovery failed: {error:#}");
            searched_paths.push(format!("PATH discovery failed: {error:#}"));
            Ok(None)
        }
    }
}

fn path_candidate_from_output(output: Output, searched_paths: &mut Vec<String>) -> Option<PathBuf> {
    if output.status.success() {
        let path = String::from_utf8_lossy(&output.stdout);
        let path = path.trim();
        if !path.is_empty() {
            let path = PathBuf::from(path);
            searched_paths.push(format!("PATH: {}", path.display()));
            return Some(path);
        }
    }
    searched_paths.push("PATH (via 'command -v rcpd'): not found".to_string());
    None
}

/// Find local static rcpd binary suitable for deployment
///
/// Searches in the following order:
/// 1. Same directory as the current rcp executable
/// 2. PATH via the shell's `command -v rcpd`
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
/// Returns an error if no compatible binary is found
pub(crate) async fn find_local_rcpd_binary_with_context(
    preparation: &crate::PreparationContext,
) -> anyhow::Result<PathBuf> {
    find_local_rcpd_binary_with_context_from_current_exe(preparation, std::env::current_exe().ok())
        .await
}

async fn find_local_rcpd_binary_with_context_from_current_exe(
    preparation: &crate::PreparationContext,
    current_exe: Option<PathBuf>,
) -> anyhow::Result<PathBuf> {
    preparation.ensure_active()?;
    let mut searched_paths = Vec::new();
    let local_version = common::version::ProtocolVersion::current();

    // try same directory as current executable first
    // this normally finds the same build (debug/release) as the running rcp and covers development
    // builds where rcp and rcpd are both in target/. Compatibility is still verified: co-location
    // is a search preference, not proof that two independently replaced files match.
    if let Some(current_exe) = current_exe
        && let Some(bin_dir) = current_exe.parent()
    {
        let path = bin_dir.join("rcpd");
        searched_paths.push(format!("Same directory: {}", path.display()));
        // the probe owns a fixed local shell child and has its own hard two-second deadline plus
        // bounded reap funnel. Executing the candidate happens in that child, so a stalled candidate
        // path is a rejection and cannot prevent the PATH fallback.
        match check_local_rcpd_version(
            preparation,
            &path,
            &local_version,
            LOCAL_RCPD_VERSION_TIMEOUT,
        )
        .await
        {
            Ok(()) => {
                preparation.ensure_active()?;
                tracing::info!("Found compatible local rcpd binary at {}", path.display());
                return Ok(path);
            }
            Err(error) => {
                preparation.ensure_active()?;
                tracing::warn!(
                    "skipping local rcpd candidate {}: {:#}",
                    path.display(),
                    &error
                );
                searched_paths.push(format!("  rejected {}: {error:#}", path.display()));
            }
        }
    }

    // try PATH (covers cargo install, nixpkgs, and other system installations)
    tracing::debug!("Trying to find rcpd in PATH");
    let path_lookup = tokio::spawn(async {
        // keep PATH traversal in a known-local shell child. Spawning `which` by name would perform
        // the same potentially stalled PATH lookup in the parent's synchronous spawn call.
        tokio::process::Command::new("/bin/sh")
            .args([
                "-c",
                crate::RCPD_PATH_DISCOVERY_SCRIPT,
                "rcp-path-discovery",
                "rcpd",
            ])
            .kill_on_drop(true)
            .output()
            .await
            .context("failed to run local rcpd PATH discovery")
    });
    let path_output = path_discovery_result(
        preparation,
        preparation
            .run_abortable_with_deadline(
                path_lookup,
                "local rcpd PATH discovery",
                crate::BootstrapDeadline::new(LOCAL_RCPD_VERSION_TIMEOUT),
            )
            .await,
        &mut searched_paths,
    )?;

    if let Some(path) =
        path_output.and_then(|output| path_candidate_from_output(output, &mut searched_paths))
    {
        // keep child ownership inside the probe's fixed-deadline termination/reap funnel.
        match check_local_rcpd_version(
            preparation,
            &path,
            &local_version,
            LOCAL_RCPD_VERSION_TIMEOUT,
        )
        .await
        {
            Ok(()) => {
                preparation.ensure_active()?;
                tracing::info!(
                    "Found compatible local rcpd binary in PATH: {}",
                    path.display()
                );
                return Ok(path);
            }
            Err(error) => {
                preparation.ensure_active()?;
                tracing::warn!(
                    "skipping local rcpd candidate {}: {:#}",
                    path.display(),
                    &error
                );
                searched_paths.push(format!("  rejected {}: {error:#}", path.display()));
            }
        }
    }

    final_local_candidate_failure(preparation, &searched_paths)
}

fn final_local_candidate_failure(
    preparation: &crate::PreparationContext,
    searched_paths: &[String],
) -> anyhow::Result<PathBuf> {
    preparation.ensure_active()?;
    anyhow::bail!(
        "no compatible local rcpd binary found for deployment\n\
        \n\
        Searched in:\n\
        {}\n\
        \n\
        To use auto-deployment, ensure rcpd is available:\n\
        - cargo install rcp-tools-rcp (installs to ~/.cargo/bin)\n\
        - or add rcpd to PATH\n\
        - or build for this host with: ./scripts/cargo-host.sh build --release --bin rcpd",
        searched_paths
            .iter()
            .map(|p| format!("- {}", p))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

/// Verify that one local deployment candidate implements the master's protocol contract.
async fn check_local_rcpd_version(
    preparation: &crate::PreparationContext,
    path: &Path,
    local_version: &common::version::ProtocolVersion,
    probe_timeout: Duration,
) -> anyhow::Result<()> {
    let output = run_local_version_probe(preparation, path, probe_timeout).await?;
    if !output.status.success() {
        anyhow::bail!(
            "local rcpd candidate {} failed to run --protocol-version with status {:?}: {}",
            path.display(),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let candidate_version =
        common::version::ProtocolVersion::from_json(String::from_utf8_lossy(&output.stdout).trim())
            .with_context(|| {
                format!(
                    "failed to parse --protocol-version output from local rcpd candidate {}",
                    path.display()
                )
            })?;
    if !local_version.is_compatible_with(&candidate_version) {
        anyhow::bail!(
            "local rcpd candidate {} reports {}, but rcp requires {}",
            path.display(),
            candidate_version,
            local_version
        );
    }
    preparation.ensure_active()?;
    Ok(())
}

enum LocalProbeOutcome {
    Completed(anyhow::Result<Output>),
    Cancelled,
    TimedOut(tokio::time::error::Elapsed),
}

/// Run one version probe without blocking a Tokio worker or trusting pipe EOF indefinitely.
async fn run_local_version_probe(
    preparation: &crate::PreparationContext,
    path: &Path,
    probe_timeout: Duration,
) -> anyhow::Result<Output> {
    preparation.ensure_active()?;
    // launch a known-local shell first, then exec the candidate in that child. Spawning the
    // candidate path directly makes the parent's spawn call wait for exec(2); a path on a stalled
    // network filesystem could therefore block the Tokio worker before the probe timer starts.
    let mut child = tokio::process::Command::new("/bin/sh")
        .args(["-c", "exec \"$1\" --protocol-version", "rcp-version-probe"])
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| {
            format!(
                "failed to execute local rcpd candidate {} with --protocol-version",
                path.display()
            )
        })?;
    let mut stdout = child
        .stdout
        .take()
        .context("local rcpd version probe stdout was not piped")?;
    let mut stderr = child
        .stderr
        .take()
        .context("local rcpd version probe stderr was not piped")?;

    let collect_output = async {
        let (status, stdout_data, stderr_data) = tokio::join!(
            child.wait(),
            crate::drain_bounded_output(&mut stdout, crate::DIAGNOSTIC_CAPTURE_LIMIT),
            crate::drain_bounded_output(&mut stderr, crate::DIAGNOSTIC_CAPTURE_LIMIT),
        );
        let status = status.with_context(|| {
            format!("failed to wait for local rcpd candidate {}", path.display())
        })?;
        let stdout_data = finish_local_probe_capture(path, "stdout", stdout_data, true)?;
        let stderr_data = finish_local_probe_capture(path, "stderr", stderr_data, false)?;
        Ok(Output {
            status,
            stdout: stdout_data,
            stderr: stderr_data,
        })
    };

    let outcome = tokio::select! {
        biased;
        () = preparation.cancellation.cancelled() => LocalProbeOutcome::Cancelled,
        result = tokio::time::timeout(probe_timeout, collect_output) => match result {
            Ok(output) => LocalProbeOutcome::Completed(output),
            Err(elapsed) => LocalProbeOutcome::TimedOut(elapsed),
        },
    };
    match outcome {
        LocalProbeOutcome::Completed(output) => {
            preparation.ensure_active()?;
            output
        }
        interrupted => {
            // dropping both read ends before termination prevents a descendant which inherited the
            // candidate's pipes from extending the probe lifetime after the candidate has exited.
            drop(stdout);
            drop(stderr);
            if let Err(error) = child.start_kill() {
                tracing::debug!(
                    "failed to request termination of interrupted local rcpd candidate {}: {error:#}",
                    path.display()
                );
            }
            let candidate = path.to_path_buf();
            preparation
                .cleanup
                .defer_bounded("rcp-local-candidate-reap", move |budget| {
                    reap_interrupted_local_candidate(child, &candidate, budget);
                });
            match interrupted {
                LocalProbeOutcome::Cancelled => {
                    Err(crate::PreparationContext::cancellation_error())
                }
                LocalProbeOutcome::TimedOut(elapsed) => Err(elapsed).with_context(|| {
                    format!(
                        "local rcpd candidate {} did not complete --protocol-version within {}",
                        path.display(),
                        humantime::format_duration(probe_timeout)
                    )
                }),
                LocalProbeOutcome::Completed(_) => unreachable!("handled above"),
            }
        }
    }
}

fn reap_interrupted_local_candidate(
    mut child: tokio::process::Child,
    candidate: &std::path::Path,
    budget: crate::CleanupBudget,
) {
    match crate::poll_process_exit_until_deadline(&budget, || {
        child.try_wait().map(|status| status.is_some())
    }) {
        Ok(true) => {}
        Ok(false) => tracing::debug!(
            "interrupted local rcpd candidate {} did not exit within its cleanup budget",
            candidate.display()
        ),
        Err(error) => {
            tracing::debug!(
                "failed to reap interrupted local rcpd candidate {}: {error:#}",
                candidate.display()
            );
        }
    }
}

fn finish_local_probe_capture(
    path: &Path,
    stream: &str,
    mut output: crate::CapturedOutput,
    reject_truncation: bool,
) -> anyhow::Result<Vec<u8>> {
    if let Some(error) = output.read_error.take() {
        return Err(error).with_context(|| {
            format!(
                "failed to read --protocol-version {stream} from local rcpd candidate {}",
                path.display()
            )
        });
    }
    if output.truncated && reject_truncation {
        anyhow::bail!(
            "local rcpd candidate {} --protocol-version {stream} exceeded the capture limit of {} bytes",
            path.display(),
            crate::DIAGNOSTIC_CAPTURE_LIMIT
        );
    }
    if output.truncated {
        Ok(output.rendered().into_bytes())
    } else {
        Ok(output.bytes)
    }
}

async fn run_deployment_binary_read<T, F>(
    preparation: &crate::PreparationContext,
    deadline: crate::BootstrapDeadline,
    operation: F,
) -> anyhow::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> anyhow::Result<T> + Send + 'static,
{
    let read = crate::run_disposable_blocking(
        preparation.cleanup.clone(),
        "rcp-deployment-binary-read",
        operation,
    );
    deadline
        .run("local rcpd deployment binary read", preparation.run(read))
        .await
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
/// * `preparation` - Cancellation and cleanup context for endpoint preparation
/// * `bootstrap_timeout` - Timeout applied independently to each remote bootstrap stage
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
pub(crate) async fn deploy_rcpd_with_context<S: crate::SshSessionOwner>(
    session: &S,
    local_rcpd_path: &std::path::Path,
    version: &str,
    remote_host: &str,
    preparation: &crate::PreparationContext,
    bootstrap_timeout: std::time::Duration,
) -> anyhow::Result<String> {
    tracing::info!(
        "Deploying rcpd {} to remote host '{}'",
        version,
        remote_host
    );

    preparation.ensure_active()?;

    // read the local binary outside Tokio's blocking pool so a stuck filesystem syscall cannot
    // stall runtime shutdown after bootstrap abandons it
    let local_rcpd_path_owned = local_rcpd_path.to_path_buf();
    let binary = run_deployment_binary_read(
        preparation,
        crate::BootstrapDeadline::new(bootstrap_timeout),
        move || {
            std::fs::read(&local_rcpd_path_owned).with_context(|| {
                format!(
                    "failed to read local rcpd binary from {}",
                    local_rcpd_path_owned.display()
                )
            })
        },
    )
    .await?;

    tracing::info!(
        "Read local rcpd binary ({} bytes) from {}",
        binary.len(),
        local_rcpd_path.display()
    );
    preparation.ensure_active()?;

    // compute checksum before transfer
    let expected_checksum = compute_sha256(&binary);
    tracing::debug!("Expected SHA-256: {}", hex::encode(&expected_checksum));

    // validate HOME is set and construct remote path
    let home = crate::get_remote_home_with_context(
        session,
        preparation,
        crate::BootstrapDeadline::new(bootstrap_timeout),
    )
    .await?;
    preparation.ensure_active()?;
    let remote_path = format!("{}/.cache/rcp/bin/rcpd-{}", home, version);

    // choose the temp path before starting the transaction, then give the remote shell sole cleanup
    // ownership through its EXIT trap. A transfer that fails after creating the file must not leak
    // a dotfile that `cleanup_old_versions` will never match.
    let temp_path = remote_temp_path(&remote_path)?;
    // the transfer itself has no bootstrap deadline: the configured timeout bounds establishing
    // each remote stage, not the time needed to transmit the binary. On cancellation, the local
    // channel is closed after a short grace; the remote shell's EXIT trap owns the unique temp path.
    transfer_binary_base64(
        session,
        &binary,
        &remote_path,
        &temp_path,
        &expected_checksum,
        preparation,
        crate::BootstrapDeadline::new(bootstrap_timeout),
    )
    .await?;

    Ok(remote_path)
}

/// Build the one remote transaction that stages, verifies, and publishes the binary.
///
/// The EXIT trap owns the temp path in the same process that can create it. A dropped local future,
/// a disconnected SSH channel, or a failed checksum therefore cannot race a separate cleanup task
/// and recreate the path after it was removed. Only a verified file reaches the atomic rename.
fn deployment_command(
    dir: &str,
    temp_path: &str,
    remote_path: &str,
    expected_checksum: &[u8],
) -> String {
    let temp_path_escaped = crate::shell_escape(temp_path);
    let expected_checksum = hex::encode(expected_checksum);
    format!(
        "cleanup() {{ rm -f {temp_path_escaped}; }}; \
         trap cleanup EXIT; \
         trap 'exit 1' HUP INT TERM; \
         mkdir -p {} && \
         exec 3> {} && \
         printf '{}\n' && \
         base64 -d >&3 && \
         exec 3>&- && \
         chmod 700 {} && \
         actual_checksum=$(sha256sum {}) && \
         actual_checksum=${{actual_checksum%% *}} && \
         if [ \"$actual_checksum\" != {} ]; then \
           printf 'checksum mismatch after transfer: expected %s, got %s\\n' {} \"$actual_checksum\" >&2; \
           exit 1; \
         fi && \
         mv -fT {} {}",
        crate::shell_escape(dir),
        temp_path_escaped,
        DEPLOYMENT_READY_RECORD,
        temp_path_escaped,
        temp_path_escaped,
        crate::shell_escape(&expected_checksum),
        crate::shell_escape(&expected_checksum),
        temp_path_escaped,
        crate::shell_escape(remote_path),
    )
}

/// Build this deployment's private temp path from the final path it will eventually be renamed to.
///
/// Split out so the caller can own the name for the whole deployment — see
/// [`deploy_rcpd_with_context`].
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

/// Transfer, verify, and publish a binary through one remote shell transaction.
///
/// The shell creates the target directory, decodes the binary into the private temp path, sets mode
/// 700, verifies its SHA-256 checksum, and atomically renames it to `remote_path`. Its EXIT trap
/// removes the temp path on every unpublished exit.
///
/// # Arguments
///
/// * `session` - SSH session to the remote host
/// * `binary` - Binary content to transfer
/// * `remote_path` - Final cache path published only after checksum verification
/// * `temp_path` - This deployment's private staging path, from [`remote_temp_path`]. Its parent
///   directory is created by the transaction
/// * `expected_checksum` - SHA-256 digest the staged file must match before publication
/// * `preparation` - Peer-preparation cancellation and owned-work context
///
/// # Errors
///
/// Returns an error if spawning, transfer, permission setting, verification, or publication fails.
/// The remote EXIT trap, rather than a separate local cleanup future, owns `temp_path` removal.
async fn transfer_binary_base64<S: crate::SshSessionOwner>(
    session: &S,
    binary: &[u8],
    remote_path: &str,
    temp_path: &str,
    expected_checksum: &[u8],
    preparation: &crate::PreparationContext,
    deadline: crate::BootstrapDeadline,
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

    let cmd = deployment_command(dir, temp_path, remote_path, expected_checksum);

    tracing::debug!("Running remote deployment transaction");

    let mut command = openssh::Session::to_command(session.clone(), "sh");
    command
        .arg("-c")
        .arg(&cmd)
        .stdin(openssh::Stdio::piped())
        .stdout(openssh::Stdio::piped())
        .stderr(openssh::Stdio::piped());
    let spawn = tokio::spawn(async move {
        command
            .spawn()
            .await
            .context("failed to spawn remote deployment transaction")
    });
    let mut child = preparation
        .run_abortable_with_deadline(spawn, "remote staging child spawn", deadline)
        .await?;

    // take handles for all streams
    let stdin = child
        .stdin()
        .take()
        .context("failed to get stdin for remote command")?;

    let stdout = child
        .stdout()
        .take()
        .context("failed to get stdout for remote command")?;

    let stderr = child
        .stderr()
        .take()
        .context("failed to get stderr for remote command")?;

    // collect stderr before waiting for readiness. Failures in mkdir, opening the staging file, or
    // shell startup happen before the marker and must retain their remote diagnostic.
    let stderr_drain = crate::AbortOnDropTask::new(tokio::spawn(crate::drain_bounded_output(
        stderr,
        crate::DIAGNOSTIC_CAPTURE_LIMIT,
    )));

    // spawning the local mux client does not prove that sshd has opened the exec channel. The
    // remote shell announces readiness only after installing cleanup, creating the directory, and
    // opening the staging file; deadline that record before a large write can fill the local pipe.
    let readiness = tokio::spawn(async move {
        let mut stdout = tokio::io::BufReader::new(stdout);
        let preamble = read_deployment_readiness(&mut stdout).await?;
        anyhow::Ok((child, stdin, stdout, preamble))
    });
    let readiness = preparation
        .run_abortable_with_deadline(
            readiness,
            "waiting for remote deployment readiness",
            deadline,
        )
        .await;
    let (child, mut stdin, mut stdout, preamble) = match readiness {
        Ok(ready) => ready,
        Err(error) => {
            let stderr_data = finish_deployment_stderr(stderr_drain).await;
            return Err(attach_deployment_stderr(error, &stderr_data));
        }
    };
    if !preamble.is_empty() {
        tracing::debug!(
            "remote deployment stdout before readiness:\n{}",
            String::from_utf8_lossy(&preamble)
        );
    }

    // write to stdin and close it before reading stdout/stderr
    // this ensures the child process receives EOF on stdin before we wait for it to finish
    use tokio::io::AsyncWriteExt;

    // write all base64 data to stdin, capturing errors instead of returning
    // immediately — if this fails (e.g. broken pipe), we still need to read
    // stderr to learn why the remote command failed
    let write_result = tokio::select! {
        biased;
        () = preparation.cancellation.cancelled() => None,
        result = write_deployment_payload(&mut stdin, encoded.as_bytes(), deadline) => Some(result),
    };
    let shutdown_stdin = matches!(write_result, Some(Ok(())));
    let finish_deadline = deadline.at_least(REMOTE_STAGING_FINALIZATION_MINIMUM);
    let finish = tokio::spawn(async move {
        finish_deadline
            .run("remote deployment verification", async move {
                let shutdown_result = if shutdown_stdin {
                    // shutdown stdin to send EOF to the remote `base64 -d` process
                    Some(stdin.shutdown().await)
                } else {
                    None
                };
                // dropping stdin sends EOF even after a failed or cancelled write
                drop(stdin);

                // drain both pipes to EOF while retaining only a bounded stderr tail for diagnostics
                let stdout_fut = crate::drain_bounded_output(&mut stdout, 0);
                let stderr_fut = stderr_drain.join();
                let (stdout_data, stderr_data) = tokio::join!(stdout_fut, stderr_fut);
                let stderr_data =
                    stderr_data.context("remote deployment stderr collector failed")?;
                let status = child
                    .wait()
                    .await
                    .context("failed to wait for remote deployment transaction")?;
                anyhow::Ok((shutdown_result, stdout_data, stderr_data, status))
            })
            .await
    });
    let finish_result = preparation
        .run_cancellation_owned_transaction(
            finish,
            REMOTE_STAGING_OWNER_GRACE,
            "remote deployment transaction finish",
        )
        .await;
    let (write_result, (shutdown_result, stdout_data, stderr_data, status)) =
        reconcile_transfer_finish(write_result, finish_result)?;

    // if writing to stdin failed (broken pipe), the remote command exited early — include stderr
    // so the user sees the actual cause (e.g. "Permission denied")
    preparation.ensure_active()?;

    if let Some(error) = stdout_data.read_error.as_ref() {
        tracing::debug!("failed to drain remote deployment stdout: {error:#}");
    }
    if let Some(error) = stderr_data.read_error.as_ref() {
        tracing::debug!("failed to drain remote deployment stderr: {error:#}");
    }

    validate_transfer_completion(write_result, shutdown_result, &stderr_data, &status)
}

async fn write_deployment_payload<W>(
    writer: &mut W,
    mut payload: &[u8],
    deadline: crate::BootstrapDeadline,
) -> anyhow::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;

    while !payload.is_empty() {
        let chunk_len = payload.len().min(DEPLOYMENT_WRITE_CHUNK_SIZE);
        let written = deadline
            .wait(
                "remote deployment payload write",
                writer.write(&payload[..chunk_len]),
            )
            .await?
            .context("failed to write base64 data to remote stdin")?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to make progress writing remote deployment payload",
            )
            .into());
        }
        payload = &payload[written..];
    }
    Ok(())
}

async fn finish_deployment_stderr(
    task: crate::AbortOnDropTask<crate::CapturedOutput>,
) -> crate::CapturedOutput {
    match tokio::time::timeout(REMOTE_STAGING_OWNER_GRACE, task.join()).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            tracing::debug!("remote deployment stderr collector failed: {error:#}");
            crate::CapturedOutput::default()
        }
        Err(_) => {
            tracing::debug!("remote deployment stderr collector did not finish during drain grace");
            crate::CapturedOutput::default()
        }
    }
}

fn attach_deployment_stderr(
    error: anyhow::Error,
    stderr_data: &crate::CapturedOutput,
) -> anyhow::Error {
    let stderr = stderr_data.rendered();
    let stderr = stderr.trim();
    if stderr.is_empty() {
        error
    } else {
        error.context(format!("remote deployment stderr: {stderr}"))
    }
}

async fn read_deployment_readiness<R>(reader: &mut R) -> anyhow::Result<Vec<u8>>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    use tokio::io::AsyncBufReadExt;

    let mut preamble = Vec::new();
    let mut line = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .context("failed to read remote deployment readiness")?;
        if available.is_empty() {
            preamble.extend_from_slice(&line);
            let preamble = String::from_utf8_lossy(&preamble);
            anyhow::bail!(
                "remote deployment exited before announcing readiness{}",
                if preamble.trim().is_empty() {
                    String::new()
                } else {
                    format!("; stdout before exit: {}", preamble.trim())
                }
            );
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index + 1);
        if preamble
            .len()
            .saturating_add(line.len())
            .saturating_add(take)
            > DEPLOYMENT_READY_OUTPUT_LIMIT
        {
            anyhow::bail!(
                "remote deployment output before readiness exceeds the {}-byte limit",
                DEPLOYMENT_READY_OUTPUT_LIMIT
            );
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            let record = line.strip_suffix(b"\n").unwrap_or(&line);
            let record = record.strip_suffix(b"\r").unwrap_or(record);
            if record == DEPLOYMENT_READY_RECORD.as_bytes() {
                return Ok(preamble);
            }
            preamble.extend_from_slice(&line);
            line.clear();
        }
    }
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
/// * `preparation` - Cancellation and cleanup context for endpoint preparation
/// * `bootstrap_timeout` - Deadline for the remote cleanup command
///
/// # Errors
///
/// Returns an error if the cleanup command fails (but this is not fatal)
pub(crate) async fn cleanup_old_versions_with_context<S: crate::SshSessionOwner>(
    session: &S,
    keep_count: usize,
    preparation: &crate::PreparationContext,
    bootstrap_timeout: std::time::Duration,
) -> anyhow::Result<()> {
    tracing::debug!("Cleaning up old rcpd versions (keeping {})", keep_count);

    // validate HOME is set before constructing the cache path
    // if this fails, we log and return Ok since cleanup is best-effort
    let home = match crate::get_remote_home_with_context(
        session,
        preparation,
        crate::BootstrapDeadline::new(bootstrap_timeout),
    )
    .await
    {
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

    let mut command = openssh::Session::to_command(session.clone(), "sh");
    command.arg("-c").arg(&cmd);
    let output = preparation
        .remote_output(
            command,
            "remote old-version cleanup",
            crate::BootstrapDeadline::new(bootstrap_timeout),
        )
        .await
        .context("failed to run cleanup command on remote host")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // log but don't fail - cleanup is best-effort
        // rcp-error-log-allow: stderr is already-rendered remote output, not an error chain
        tracing::warn!("cleanup of old versions failed (non-fatal): {}", stderr);
    } else {
        tracing::debug!("Old versions cleaned up successfully");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    struct TestDirectory(PathBuf);

    #[cfg(unix)]
    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "rcp-local-version-probe-{}-{:016x}",
                std::process::id(),
                rand::random::<u64>()
            ));
            std::fs::create_dir(&path).expect("failed to create version-probe test directory");
            Self(path)
        }

        fn script(&self, name: &str, contents: &str) -> PathBuf {
            use std::os::unix::fs::PermissionsExt;

            let path = self.0.join(name);
            std::fs::write(&path, contents).expect("failed to write version-probe script");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                .expect("failed to make version-probe script executable");
            path
        }

        #[cfg(target_os = "linux")]
        fn fifo(&self, name: &str) -> PathBuf {
            let path = self.0.join(name);
            let status = std::process::Command::new("mkfifo")
                .arg(&path)
                .status()
                .expect("failed to run mkfifo for version-probe test");
            assert!(status.success(), "mkfifo failed with {status}");
            path
        }
    }

    #[cfg(unix)]
    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[cfg(target_os = "linux")]
    fn process_start_time(pid: &str) -> std::io::Result<Option<String>> {
        let stat = match std::fs::read_to_string(Path::new("/proc").join(pid).join("stat")) {
            Ok(stat) => stat,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let (_, fields) = stat.rsplit_once(") ").ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid /proc stat record")
        })?;
        let start_time = fields.split_whitespace().nth(19).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing process start time in /proc stat record",
            )
        })?;
        Ok(Some(start_time.to_string()))
    }

    #[cfg(target_os = "linux")]
    async fn wait_for_process_identity_to_be_reaped(pid: &str, start_time: &str) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match process_start_time(pid).expect("failed to inspect candidate process") {
                    None => break,
                    Some(current) if current != start_time => break,
                    Some(_) => tokio::time::sleep(Duration::from_millis(5)).await,
                }
            }
        })
        .await
        .expect("cancelled candidate process identity was not reaped");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn cleanup_scope_reaps_same_directory_probe_after_peer_cancellation() {
        let test_dir = TestDirectory::new();
        let pid_file = test_dir.0.join("candidate.pid");
        let fifo = test_dir.fifo("candidate.fifo");
        let candidate = test_dir.script(
            "rcpd",
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$$\" > {}\nexec 3< {}\n",
                crate::shell_escape(&pid_file.to_string_lossy()),
                crate::shell_escape(&fifo.to_string_lossy()),
            ),
        );
        let current_exe = test_dir.0.join("rcp");
        assert_eq!(current_exe.parent(), candidate.parent());
        let cancellation = tokio_util::sync::CancellationToken::new();
        let cleanup = crate::RemoteCleanup::new().unwrap();
        let preparation = crate::PreparationContext::new(cancellation.clone(), cleanup.clone());
        let discovery = tokio::spawn(async move {
            find_local_rcpd_binary_with_context_from_current_exe(&preparation, Some(current_exe))
                .await
        });
        let (pid, candidate_start_time) = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(pid) = tokio::fs::read_to_string(&pid_file).await {
                    let pid = pid.trim();
                    if !pid.is_empty()
                        && let Some(start_time) =
                            process_start_time(pid).expect("failed to inspect candidate process")
                    {
                        break (pid.to_string(), start_time);
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocking candidate did not start");

        cancellation.cancel();
        let error = tokio::time::timeout(Duration::from_millis(1500), discovery)
            .await
            .expect("candidate discovery ignored peer cancellation")
            .expect("candidate discovery task panicked")
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("cancelled because peer preparation failed"),
            "unexpected cancellation error: {error:#}"
        );
        cleanup.finish();
        wait_for_process_identity_to_be_reaped(&pid, &candidate_start_time).await;
    }

    #[test]
    fn final_local_candidate_failure_preserves_peer_cancellation() {
        let cancellation = tokio_util::sync::CancellationToken::new();
        cancellation.cancel();
        let cleanup = crate::RemoteCleanup::new().unwrap();
        let preparation = crate::PreparationContext::new(cancellation, cleanup.clone());

        let searched_paths = ["PATH: /test/rcpd".to_string()];
        let error = final_local_candidate_failure(&preparation, &searched_paths).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("cancelled because peer preparation failed"),
            "final candidate rejection swallowed peer cancellation: {error:#}"
        );
        drop(preparation);
        cleanup.finish();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn version_probe_timeout_falls_back_after_descendant_keeps_pipes_open() {
        let test_dir = TestDirectory::new();
        let hanging = test_dir.script("hanging-rcpd", "#!/bin/sh\n(sleep 2) &\nexit 0\n");
        let local_version = common::version::ProtocolVersion::current();
        let cleanup = crate::RemoteCleanup::new().unwrap();
        let preparation = crate::PreparationContext::uncancelled(cleanup.clone());
        let compatible = test_dir.script(
            "compatible-rcpd",
            &format!(
                "#!/bin/sh\nprintf '%s\\n' '{{\"semantic\":\"{}\"}}'\n",
                local_version.semantic
            ),
        );

        let candidates = [
            (hanging.as_path(), Duration::from_millis(50)),
            (compatible.as_path(), LOCAL_RCPD_VERSION_TIMEOUT),
        ];
        let (selected, rejections) = tokio::time::timeout(Duration::from_secs(3), async {
            let mut rejections = Vec::new();
            for (candidate, probe_timeout) in candidates {
                match check_local_rcpd_version(
                    &preparation,
                    candidate,
                    &local_version,
                    probe_timeout,
                )
                .await
                {
                    Ok(()) => return (Some(candidate.to_path_buf()), rejections),
                    Err(error) => rejections.push(error),
                }
            }
            (None, rejections)
        })
        .await
        .expect("candidate fallback must complete within its watchdog");

        assert_eq!(selected.as_deref(), Some(compatible.as_path()));
        assert_eq!(rejections.len(), 1);
        assert!(
            format!("{:#}", rejections[0]).contains("did not complete --protocol-version within"),
            "timeout rejection must remain actionable: {:#}",
            rejections[0]
        );
        assert!(
            rejections[0].chain().count() >= 2,
            "timeout rejection must preserve the elapsed source error"
        );
        drop(preparation);
        cleanup.finish();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_local_version_probe_uses_grammatical_error_context() {
        let test_dir = TestDirectory::new();
        let candidate = test_dir.script(
            "failing-rcpd",
            "#!/bin/sh\nprintf '%s\\n' 'candidate refused version probe' >&2\nexit 7\n",
        );
        let local_version = common::version::ProtocolVersion::current();
        let cleanup = crate::RemoteCleanup::new().unwrap();
        let preparation = crate::PreparationContext::uncancelled(cleanup.clone());
        let error = check_local_rcpd_version(
            &preparation,
            &candidate,
            &local_version,
            Duration::from_secs(2),
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("failed to run --protocol-version with status Some(7)"),
            "unexpected probe error: {error:#}"
        );
        drop(preparation);
        cleanup.finish();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_version_probe_rejects_stdout_beyond_the_capture_limit() {
        let test_dir = TestDirectory::new();
        let candidate = test_dir.script(
            "verbose-stdout-rcpd",
            &format!(
                "#!/bin/sh\nhead -c {} /dev/zero\n",
                crate::DIAGNOSTIC_CAPTURE_LIMIT + 1
            ),
        );
        let cleanup = crate::RemoteCleanup::new().unwrap();
        let preparation = crate::PreparationContext::uncancelled(cleanup.clone());

        let error = run_local_version_probe(&preparation, &candidate, Duration::from_secs(2))
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("--protocol-version stdout exceeded the capture limit"),
            "unexpected probe error: {error:#}"
        );
        drop(preparation);
        cleanup.finish();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_version_probe_bounds_stderr_diagnostics() {
        let test_dir = TestDirectory::new();
        let candidate = test_dir.script(
            "verbose-stderr-rcpd",
            &format!(
                "#!/bin/sh\nhead -c {} /dev/zero >&2\nexit 7\n",
                crate::DIAGNOSTIC_CAPTURE_LIMIT + 1
            ),
        );
        let cleanup = crate::RemoteCleanup::new().unwrap();
        let preparation = crate::PreparationContext::uncancelled(cleanup.clone());

        let output = run_local_version_probe(&preparation, &candidate, Duration::from_secs(2))
            .await
            .unwrap();

        assert!(
            output.stderr.len() <= crate::DIAGNOSTIC_CAPTURE_LIMIT + 128,
            "stderr capture grew to {} bytes",
            output.stderr.len()
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("output truncated"),
            "truncated diagnostic needs an explicit marker"
        );
        drop(preparation);
        cleanup.finish();
    }

    #[test]
    fn path_discovery_failure_preserves_peer_cancellation() {
        let cancellation = tokio_util::sync::CancellationToken::new();
        cancellation.cancel();
        let cleanup = crate::RemoteCleanup::new().unwrap();
        let preparation = crate::PreparationContext::new(cancellation, cleanup.clone());
        let mut searched_paths = Vec::new();

        let error = path_discovery_result(
            &preparation,
            Err(anyhow::anyhow!("which task was abandoned")),
            &mut searched_paths,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("cancelled because peer preparation failed")
        );
        assert!(searched_paths.is_empty());
        drop(preparation);
        cleanup.finish();
    }

    #[cfg(unix)]
    #[test]
    fn local_path_discovery_records_an_ordinary_miss() {
        use std::os::unix::process::ExitStatusExt;

        let output = Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };
        let mut searched_paths = Vec::new();

        let candidate = path_candidate_from_output(output, &mut searched_paths);

        assert_eq!(candidate, None);
        assert_eq!(searched_paths, ["PATH (via 'command -v rcpd'): not found"]);
    }

    #[derive(Debug)]
    struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(signal) = self.0.take() {
                let _ = signal.send(());
            }
        }
    }

    #[tokio::test]
    async fn deployment_binary_read_deadline_drops_a_late_result_on_its_worker() {
        let cleanup = crate::RemoteCleanup::new().unwrap();
        let preparation = crate::PreparationContext::uncancelled(cleanup.clone());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let read = run_deployment_binary_read(
            &preparation,
            crate::BootstrapDeadline::new(Duration::from_millis(20)),
            move || {
                let _ = started_tx.send(());
                release_rx
                    .recv()
                    .expect("test must release the blocked deployment read");
                Ok(DropSignal(Some(dropped_tx)))
            },
        );
        let (started, result) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(1), started_rx),
            tokio::time::timeout(Duration::from_secs(1), read),
        );
        started
            .expect("deployment read worker did not start")
            .expect("deployment read worker dropped its start signal");
        let error = result
            .expect("blocked deployment read ignored its bootstrap deadline")
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("local rcpd deployment binary read timed out after 20ms"),
            "unexpected deployment-read deadline error: {error:#}"
        );
        release_tx
            .send(())
            .expect("blocked deployment read worker exited early");
        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("late deployment read result was not dropped")
            .expect("late deployment read result dropped its signal unexpectedly");
        drop(preparation);
        cleanup.finish();
    }

    #[tokio::test]
    async fn deployment_binary_read_observes_peer_cancellation() {
        let cleanup = crate::RemoteCleanup::new().unwrap();
        let cancellation = tokio_util::sync::CancellationToken::new();
        let preparation = crate::PreparationContext::new(cancellation.clone(), cleanup.clone());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let read = run_deployment_binary_read(
            &preparation,
            crate::BootstrapDeadline::new(Duration::from_secs(5)),
            move || {
                let _ = started_tx.send(());
                release_rx
                    .recv()
                    .expect("test must release the blocked deployment read");
                Ok(DropSignal(Some(dropped_tx)))
            },
        );
        let cancel_after_start = async move {
            tokio::time::timeout(Duration::from_secs(1), started_rx)
                .await
                .expect("deployment read worker did not start")
                .expect("deployment read worker dropped its start signal");
            cancellation.cancel();
        };
        let (_, result) = tokio::join!(
            cancel_after_start,
            tokio::time::timeout(Duration::from_secs(1), read),
        );
        let error = result
            .expect("blocked deployment read ignored peer cancellation")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cancelled because peer preparation failed"),
            "unexpected deployment-read cancellation error: {error:#}"
        );
        release_tx
            .send(())
            .expect("blocked deployment read worker exited early");
        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("late deployment read result was not dropped")
            .expect("late deployment read result dropped its signal unexpectedly");
        drop(preparation);
        cleanup.finish();
    }

    #[tokio::test]
    async fn deployment_readiness_accepts_a_bounded_stdout_preamble() {
        let input = b"remote banner\nRCP_DEPLOY_READY\npost-marker output\n";
        let mut reader = tokio::io::BufReader::new(&input[..]);

        let preamble = read_deployment_readiness(&mut reader).await.unwrap();

        assert_eq!(preamble, b"remote banner\n");
        let mut remaining = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut remaining)
            .await
            .unwrap();
        assert_eq!(remaining, "post-marker output\n");
    }

    #[tokio::test]
    async fn deployment_readiness_retains_non_utf8_stdout_preamble() {
        let input = b"\xffremote banner\nRCP_DEPLOY_READY\n";
        let mut reader = tokio::io::BufReader::new(&input[..]);

        let preamble = read_deployment_readiness(&mut reader).await.unwrap();

        assert_eq!(preamble, b"\xffremote banner\n");
    }

    #[tokio::test]
    async fn deployment_readiness_failure_retains_stdout_context() {
        let input = b"remote banner\nstartup failed\n";
        let mut reader = tokio::io::BufReader::new(&input[..]);

        let error = read_deployment_readiness(&mut reader).await.unwrap_err();

        let error = format!("{error:#}");
        assert!(
            error.contains("exited before announcing readiness"),
            "{error}"
        );
        assert!(error.contains("remote banner"), "{error}");
        assert!(error.contains("startup failed"), "{error}");
    }

    #[tokio::test]
    async fn deployment_readiness_failure_retains_unterminated_stdout_context() {
        let input = b"remote banner\nstartup failed without newline";
        let mut reader = tokio::io::BufReader::new(&input[..]);

        let error = read_deployment_readiness(&mut reader).await.unwrap_err();

        let error = format!("{error:#}");
        assert!(error.contains("remote banner"), "{error}");
        assert!(error.contains("startup failed without newline"), "{error}");
    }

    #[test]
    fn deployment_readiness_failure_retains_stderr_context() {
        let stderr = crate::CapturedOutput {
            bytes: b"mkdir: Permission denied\n".to_vec(),
            ..crate::CapturedOutput::default()
        };

        let error = attach_deployment_stderr(
            anyhow::anyhow!("remote deployment exited before announcing readiness"),
            &stderr,
        );

        let error = format!("{error:#}");
        assert!(error.contains("Permission denied"), "{error}");
        assert!(
            error.contains("exited before announcing readiness"),
            "{error}"
        );
    }

    #[test]
    fn payload_write_failure_precedes_a_later_finalization_failure() {
        let error = reconcile_transfer_finish::<()>(
            Some(Err(anyhow::anyhow!("sentinel payload write failure"))),
            Err(anyhow::anyhow!("sentinel finalization failure")),
        )
        .unwrap_err();

        let error = format!("{error:#}");
        assert!(error.contains("sentinel payload write failure"), "{error}");
        assert!(!error.contains("sentinel finalization failure"), "{error}");
    }

    #[tokio::test]
    async fn deployment_payload_write_times_out_without_progress() {
        let (mut writer, _reader) = tokio::io::duplex(1);

        let error = write_deployment_payload(
            &mut writer,
            b"payload larger than the pipe",
            crate::BootstrapDeadline::new(Duration::from_millis(20)),
        )
        .await
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("remote deployment payload write timed out after 20ms"),
            "unexpected stalled-write error: {error:#}"
        );
    }

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

    #[cfg(target_os = "linux")]
    fn run_deployment_command(
        dir: &std::path::Path,
        temp_path: &std::path::Path,
        remote_path: &std::path::Path,
        expected_checksum: &[u8],
        binary: &[u8],
    ) -> std::process::Output {
        use base64::Engine;
        use std::io::Write;

        let command = deployment_command(
            dir.to_str().unwrap(),
            temp_path.to_str().unwrap(),
            remote_path.to_str().unwrap(),
            expected_checksum,
        );
        let mut child = std::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("failed to spawn deployment shell");
        child
            .stdin
            .take()
            .unwrap()
            .write_all(
                base64::engine::general_purpose::STANDARD
                    .encode(binary)
                    .as_bytes(),
            )
            .expect("failed to write encoded test binary");
        child
            .wait_with_output()
            .expect("failed to wait for deployment shell")
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn deployment_command_verifies_then_publishes_executable_bytes() {
        use std::os::unix::fs::PermissionsExt;

        let test_dir = TestDirectory::new();
        let cache_dir = test_dir.0.join("cache dir's bin");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let temp_path = cache_dir.join(".rcpd-0.22.0.tmp.test");
        let remote_path = cache_dir.join("rcpd-0.22.0");
        std::fs::write(&remote_path, b"old binary").unwrap();
        let expected_checksum =
            hex::decode("2f17c9ffb972a6c5da72c2b3df01f7e2ccf52dad2c0059dac631232a15126d2e")
                .unwrap();

        let output = run_deployment_command(
            &cache_dir,
            &temp_path,
            &remote_path,
            &expected_checksum,
            b"new binary",
        );

        assert!(
            output.status.success(),
            "deployment failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            "RCP_DEPLOY_READY\n",
            "the shell must announce readiness before consuming the payload"
        );
        assert_eq!(std::fs::read(&remote_path).unwrap(), b"new binary");
        assert_eq!(
            std::fs::metadata(&remote_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert!(!temp_path.exists(), "published temp path must be gone");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn deployment_command_checksum_mismatch_preserves_final_and_removes_temp() {
        let test_dir = TestDirectory::new();
        let cache_dir = test_dir.0.join("cache dir's bin");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let temp_path = cache_dir.join(".rcpd-0.22.0.tmp.test");
        let remote_path = cache_dir.join("rcpd-0.22.0");
        std::fs::write(&remote_path, b"old binary").unwrap();
        let expected_checksum =
            hex::decode("2f17c9ffb972a6c5da72c2b3df01f7e2ccf52dad2c0059dac631232a15126d2e")
                .unwrap();

        let output = run_deployment_command(
            &cache_dir,
            &temp_path,
            &remote_path,
            &expected_checksum,
            b"corrupt binary",
        );

        assert!(!output.status.success(), "checksum mismatch must fail");
        assert_eq!(std::fs::read(&remote_path).unwrap(), b"old binary");
        assert!(
            !temp_path.exists(),
            "EXIT trap must remove the unpublished temp path"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("checksum mismatch after transfer"),
            "mismatch diagnostic missing: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn deployment_command_rejects_directory_target_without_leaking_temp() {
        let test_dir = TestDirectory::new();
        let cache_dir = test_dir.0.join("cache dir's bin");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let temp_path = cache_dir.join(".rcpd-0.22.0.tmp.test");
        let remote_path = cache_dir.join("rcpd-0.22.0");
        std::fs::create_dir(&remote_path).unwrap();
        let expected_checksum =
            hex::decode("2f17c9ffb972a6c5da72c2b3df01f7e2ccf52dad2c0059dac631232a15126d2e")
                .unwrap();

        let output = run_deployment_command(
            &cache_dir,
            &temp_path,
            &remote_path,
            &expected_checksum,
            b"new binary",
        );

        assert!(!output.status.success(), "directory target must fail");
        assert!(remote_path.is_dir(), "directory target must be preserved");
        assert!(
            std::fs::read_dir(&remote_path).unwrap().next().is_none(),
            "publishing must not move the temp file inside the directory target"
        );
        assert!(
            !temp_path.exists(),
            "EXIT trap must remove the unpublished temp path"
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
    fn write_error_context_with_stderr_includes_remote_output() {
        let stderr = b"mkdir: cannot create directory: Permission denied";
        let msg = format_write_error_context(stderr, &"exited with 1");
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
    fn write_error_context_without_stderr_includes_exit_status() {
        let stderr = b"";
        let msg = format_write_error_context(stderr, &"exited with 126");
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
    fn write_error_context_trims_whitespace_only_stderr() {
        let stderr = b"  \n\t  ";
        let msg = format_write_error_context(stderr, &"exited with 1");
        // whitespace-only stderr should be treated as empty
        assert!(
            msg.contains("remote stderr was empty"),
            "whitespace-only stderr should be treated as empty"
        );
    }

    #[cfg(unix)]
    #[test]
    fn payload_write_failure_preserves_its_source_and_diagnostics() {
        use std::os::unix::process::ExitStatusExt;

        let stderr = crate::CapturedOutput {
            bytes: b"base64: invalid input\n".to_vec(),
            ..crate::CapturedOutput::default()
        };
        let error = validate_transfer_completion(
            Some(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "sentinel payload write failure",
            )
            .into())),
            None,
            &stderr,
            &std::process::ExitStatus::from_raw(1 << 8),
        )
        .unwrap_err();

        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("sentinel payload write failure"),
            "{rendered}"
        );
        assert_eq!(
            rendered.matches("sentinel payload write failure").count(),
            1,
            "payload write cause was rendered more than once: {rendered}"
        );
        assert!(rendered.contains("base64: invalid input"), "{rendered}");
        assert!(rendered.contains(TRANSFER_HINTS), "{rendered}");
        let source = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<std::io::Error>())
            .expect("payload write source must remain in the error chain");
        assert_eq!(source.kind(), std::io::ErrorKind::BrokenPipe);
        assert_eq!(source.to_string(), "sentinel payload write failure");
    }

    #[cfg(unix)]
    #[test]
    fn remote_transfer_failure_precedes_stdin_shutdown_error() {
        use std::os::unix::process::ExitStatusExt;

        let stderr = crate::CapturedOutput {
            bytes: b"mv: target is a directory\n".to_vec(),
            ..crate::CapturedOutput::default()
        };
        let error = validate_transfer_completion(
            Some(Ok(())),
            Some(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "shutdown failed",
            ))),
            &stderr,
            &std::process::ExitStatus::from_raw(1 << 8),
        )
        .unwrap_err();
        let error = format!("{error:#}");
        assert!(error.contains("mv: target is a directory"), "{error}");
        assert!(!error.contains("shutdown failed"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn stdin_shutdown_error_is_reported_after_successful_remote_transfer() {
        use std::os::unix::process::ExitStatusExt;

        let error = validate_transfer_completion(
            Some(Ok(())),
            Some(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "shutdown failed",
            ))),
            &crate::CapturedOutput::default(),
            &std::process::ExitStatus::from_raw(0),
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("failed to shutdown stdin: shutdown failed"));
    }
}
