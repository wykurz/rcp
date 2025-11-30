//! Remote copy protocol and networking for distributed file operations
//!
//! This crate provides the networking layer and protocol definitions for remote file copying in the RCP tools suite.
//! It enables efficient distributed copying between remote hosts using SSH for orchestration and QUIC for high-performance data transfer.
//!
//! # Overview
//!
//! The remote copy system uses a three-node architecture:
//!
//! ```text
//! Master (rcp)
//! ├── SSH → Source Host (rcpd)
//! │   └── QUIC → Master (control)
//! │   └── QUIC Server (waits for Destination)
//! └── SSH → Destination Host (rcpd)
//!     └── QUIC → Master (control)
//!     └── QUIC Client → Source (data transfer)
//! ```
//!

//! ## Connection Flow
//!
//! 1. **Initialization**: Master starts `rcpd` processes on source and destination via SSH
//! 2. **Control Connections**: Both `rcpd` processes connect back to Master via QUIC
//! 3. **Address Exchange**: Source starts QUIC server and sends its address to Master
//! 4. **Direct Connection**: Master forwards address to Destination, which connects to Source
//! 5. **Data Transfer**: Files flow directly from Source to Destination (not through Master)
//!
//! This design ensures efficient data transfer while allowing the Master to coordinate operations and monitor progress.
//!
//! # Key Components
//!
//! ## SSH Session Management
//!
//! The [`SshSession`] type represents an SSH connection to a remote host and is used to:
//! - Launch `rcpd` daemons on remote hosts
//! - Configure connection parameters (user, host, port)
//!
//! ## QUIC Networking
//!
//! QUIC protocol provides:
//! - Multiplexed streams over a single connection
//! - Built-in encryption and authentication
//! - Efficient data transfer with congestion control
//!
//! Key functions:
//! - [`get_server_with_port_ranges`] - Create QUIC server endpoint with optional port restrictions
//! - [`get_client_with_port_ranges_and_pinning`] - Create secure QUIC client with certificate pinning
//! - [`get_endpoint_addr`] - Get the local address of an endpoint
//!
//! ## Port Range Configuration
//!
//! The [`port_ranges`] module allows restricting QUIC to specific port ranges, useful for firewall-restricted environments:
//!
//! ```rust,no_run
//! # use remote::get_server_with_port_ranges;
//! // bind to ports in the 8000-8999 range with default timeouts
//! // idle_timeout: 10 seconds, keep_alive: 1 second
//! let (endpoint, cert_fingerprint) = get_server_with_port_ranges(
//!     Some("8000-8999"),
//!     10,  // idle_timeout_sec
//!     1,   // keep_alive_interval_sec
//! )?;
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! ## Protocol Messages
//!
//! The [`protocol`] module defines the message types exchanged between nodes:
//! - `MasterHello` - Master → rcpd configuration
//! - `SourceMasterHello` - Source → Master address information
//! - `RcpdResult` - rcpd → Master operation results
//! - `TracingHello` - rcpd → Master tracing initialization
//!
//! ## Stream Communication
//!
//! The [`streams`] module provides high-level abstractions over QUIC streams:
//! - Bidirectional streams for request/response communication
//! - Unidirectional streams for tracing and logging
//! - Object serialization/deserialization using bincode
//!
//! ## Remote Tracing
//!
//! The [`tracelog`] module enables distributed logging and progress tracking:
//! - Forward tracing events from remote `rcpd` processes to Master
//! - Aggregate progress information across multiple remote operations
//! - Display unified progress for distributed operations
//!
//! # Security Model
//!
//! The remote copy system implements a defense-in-depth security model using SSH for authentication and certificate pinning for QUIC connection integrity.
//! This provides protection against man-in-the-middle (MITM) attacks while maintaining ease of deployment.
//!
//! ## Authentication & Authorization
//!
//! **SSH is the security perimeter**: All remote operations begin with SSH authentication.
//! - Initial access control is handled entirely by SSH
//! - Users must be authenticated and authorized via SSH before any QUIC connections are established
//! - SSH configuration (keys, permissions, etc.) determines who can initiate remote copies
//!
//! ## Transport Encryption & Integrity
//!
//! **QUIC with TLS 1.3**: All data transfer uses QUIC protocol built on TLS 1.3
//! - Provides encryption for data confidentiality
//! - Ensures data integrity through cryptographic authentication
//! - Built-in protection against replay attacks
//!
//! ## Trust Bootstrap via Certificate Pinning
//!
//! **Two secured QUIC connections** in every remote copy operation:
//!
//! ### 1. Master ← rcpd (Control Connection)
//! ```text
//! Master (rcp)                    Remote Host (rcpd)
//!    |                                   |
//!    | 1. SSH connection established     |
//!    |<--------------------------------->|
//!    | 2. Master generates self-signed   |
//!    |    cert, computes SHA-256         |
//!    |    fingerprint                    |
//!    |                                   |
//!    | 3. Launch rcpd via SSH with       |
//!    |    fingerprint as argument        |
//!    |---------------------------------->|
//!    |                                   |
//!    | 4. rcpd validates Master's cert   |
//!    |    against received fingerprint   |
//!    |<---(QUIC + cert pinning)----------|
//! ```
//!
//! - Master generates ephemeral self-signed certificate at startup
//! - Certificate fingerprint (SHA-256) is passed to rcpd via SSH command-line arguments
//! - rcpd validates Master's certificate by computing its fingerprint and comparing
//! - Connection fails if fingerprints don't match (MITM protection)
//!
//! ### 2. Source → Destination (Data Transfer Connection)
//! ```text
//! Source (rcpd)                   Destination (rcpd)
//!    |                                   |
//!    | 1. Source generates self-signed   |
//!    |    cert, computes SHA-256         |
//!    |    fingerprint                    |
//!    |                                   |
//!    | 2. Send fingerprint + address     |
//!    |    to Master via secure channel   |
//!    |---------------------------------->|
//!    |                    Master         |
//!    |                      |            |
//!    | 3. Master forwards   |            |
//!    |    to Destination    |            |
//!    |                      |----------->|
//!    |                                   |
//!    | 4. Destination validates Source's |
//!    |    cert against received          |
//!    |    fingerprint                    |
//!    |<---(QUIC + cert pinning)----------|
//! ```
//!
//! - Source generates ephemeral self-signed certificate
//! - Fingerprint is sent to Master over already-secured Master←Source connection
//! - Master forwards fingerprint to Destination over already-secured Master←Destination connection
//! - Destination validates Source's certificate against fingerprint
//! - Direct Source→Destination connection established only after successful validation
//!
//! ## SSH as Secure Out-of-Band Channel
//!
//! **Key insight**: SSH provides a secure, authenticated channel for bootstrapping QUIC trust
//!
//! - Certificate fingerprints are transmitted through SSH (Master→rcpd command-line arguments)
//! - SSH connection is already authenticated and encrypted
//! - This creates a "chain of trust":
//!   1. User trusts SSH (proven by successful authentication)
//!   2. SSH carries the certificate fingerprint securely
//!   3. QUIC connection validates against that fingerprint
//!   4. Therefore, QUIC connection is trustworthy
//!
//! ## Attack Resistance
//!
//! ### ✅ Protected Against
//!
//! - **Man-in-the-Middle (MITM)**: Certificate pinning prevents attackers from impersonating endpoints
//! - **Replay Attacks**: TLS 1.3 in QUIC provides built-in replay protection
//! - **Eavesdropping**: All data encrypted with TLS 1.3
//! - **Tampering**: Cryptographic integrity checks prevent data modification
//! - **Unauthorized Access**: SSH authentication is required before any operations
//!
//! ### ⚠️ Threat Model Assumptions
//!
//! - **SSH is secure**: The security model depends on SSH being properly configured and uncompromised
//! - **Certificate fingerprints are short-lived**: Ephemeral certificates are generated per-session
//! - **Trusted network for Master**: The machine running Master (rcp) should be trusted
//!
//! ## Best Practices
//!
//! 1. **Secure SSH Configuration**: Use key-based authentication, disable password auth
//! 2. **Keep Systems Updated**: Ensure SSH, TLS libraries, and QUIC implementations are current
//! 3. **Network Segmentation**: Run remote copies on trusted network segments when possible
//! 4. **Monitor Logs**: Certificate validation failures indicate potential security issues
//!
//! # Network Troubleshooting
//!
//! Common failure scenarios and their handling:
//!
//! ## SSH Connection Fails
//! - **Cause**: Host unreachable, authentication failure
//! - **Timeout**: ~30s (SSH default)
//! - **Error**: Standard SSH error messages
//!
//! ## rcpd Cannot Connect to Master
//! - **Cause**: Firewall blocks QUIC, network routing issue
//! - **Timeout**: Configurable via `--remote-copy-conn-timeout-sec` (default: 15s)
//! - **Solution**: Check firewall rules for QUIC ports
//!
//! ## Destination Cannot Connect to Source
//! - **Cause**: Firewall blocks direct connection between hosts
//! - **Timeout**: Configurable (default: 15s)
//! - **Solution**: Use `--quic-port-ranges` to specify allowed ports, configure firewall
//!
//! For detailed troubleshooting, see the repository's `docs/network_connectivity.md`.
//!
//! # Examples
//!
//! ## Starting a Remote Copy Daemon
//!
//! ```rust,no_run
//! use remote::{SshSession, protocol::RcpdConfig, start_rcpd};
//! use std::net::SocketAddr;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let session = SshSession {
//!     user: Some("user".to_string()),
//!     host: "example.com".to_string(),
//!     port: None,
//! };
//!
//! let config = RcpdConfig {
//!     verbose: 0,
//!     fail_early: false,
//!     max_workers: 4,
//!     max_blocking_threads: 512,
//!     max_open_files: None,
//!     ops_throttle: 0,
//!     iops_throttle: 0,
//!     chunk_size: 1024 * 1024,
//!     dereference: false,
//!     overwrite: false,
//!     overwrite_compare: String::new(),
//!     debug_log_prefix: None,
//!     quic_port_ranges: None,
//!     quic_idle_timeout_sec: 10,
//!     quic_keep_alive_interval_sec: 1,
//!     progress: false,
//!     progress_delay: None,
//!     remote_copy_conn_timeout_sec: 15,
//!     network_profile: remote::NetworkProfile::Datacenter,
//!     congestion_control: None, // use profile default (BBR for datacenter)
//!     quic_tuning: remote::QuicTuning::default(),
//!     master_cert_fingerprint: Vec::new(),
//!     chrome_trace_prefix: None,
//!     flamegraph_prefix: None,
//!     profile_level: Some("trace".to_string()),
//!     tokio_console: false,
//!     tokio_console_port: None,
//! };
//! let master_addr: SocketAddr = "192.168.1.100:5000".parse()?;
//! let server_name = "master-server";
//!
//! let process = start_rcpd(&config, &session, &master_addr, server_name, None, false, None, remote::protocol::RcpdRole::Source).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Creating a QUIC Server with Port Ranges
//!
//! ```rust,no_run
//! use remote::{get_server_with_port_ranges, get_endpoint_addr};
//!
//! # fn example() -> anyhow::Result<()> {
//! // create server restricted to ports 8000-8999
//! // timeouts: 10s idle, 1s keep-alive (CLI defaults)
//! let (endpoint, _cert_fingerprint) = get_server_with_port_ranges(
//!     Some("8000-8999"),
//!     10,  // idle_timeout_sec
//!     1,   // keep_alive_interval_sec
//! )?;
//! let addr = get_endpoint_addr(&endpoint)?;
//! println!("Server listening on: {}", addr);
//! # Ok(())
//! # }
//! ```
//!
//! # Module Organization
//!
//! - [`port_ranges`] - Port range parsing and UDP socket binding
//! - [`protocol`] - Protocol message definitions and serialization
//! - [`streams`] - QUIC stream wrappers with typed message passing
//! - [`tracelog`] - Remote tracing and progress aggregation

#[cfg(not(tokio_unstable))]
compile_error!("tokio_unstable cfg must be enabled; see .cargo/config.toml");

use anyhow::{anyhow, Context};
use rand::Rng;
use tracing::instrument;

pub mod deploy;
pub mod port_ranges;
pub mod protocol;
pub mod streams;
pub mod tracelog;

/// Network profile for QUIC configuration tuning
///
/// Profiles provide pre-configured settings optimized for different network environments.
/// The Datacenter profile is optimized for high-bandwidth, low-latency datacenter networks,
/// while the Internet profile uses more conservative settings suitable for internet connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum NetworkProfile {
    /// Optimized for datacenter networks: <1ms RTT, 25-100 Gbps
    /// Uses BBR congestion control and aggressive window sizes
    #[default]
    Datacenter,
    /// Conservative settings for internet connections
    /// Uses CUBIC congestion control and standard window sizes
    Internet,
}

impl std::fmt::Display for NetworkProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Datacenter => write!(f, "datacenter"),
            Self::Internet => write!(f, "internet"),
        }
    }
}

impl std::str::FromStr for NetworkProfile {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "datacenter" => Ok(Self::Datacenter),
            "internet" => Ok(Self::Internet),
            _ => Err(format!(
                "invalid network profile '{}', expected 'datacenter' or 'internet'",
                s
            )),
        }
    }
}

impl NetworkProfile {
    /// Returns the default congestion control algorithm for this profile
    pub fn default_congestion_control(&self) -> CongestionControl {
        match self {
            Self::Datacenter => CongestionControl::Bbr,
            Self::Internet => CongestionControl::Cubic,
        }
    }
    /// Returns the default buffer size for remote copy operations for this profile
    ///
    /// Datacenter profile uses a large buffer (16 MiB) matching the per-stream receive window
    /// to maximize throughput on high-bandwidth networks.
    /// Internet profile uses a smaller buffer (2 MiB) suitable for internet connections.
    pub fn default_remote_copy_buffer_size(&self) -> usize {
        match self {
            Self::Datacenter => DATACENTER_REMOTE_COPY_BUFFER_SIZE,
            Self::Internet => INTERNET_REMOTE_COPY_BUFFER_SIZE,
        }
    }
}

// ============================================================================
// Network profile constants
//
// These constants define the default QUIC tuning parameters for each profile.
// They are used in apply_quic_tuning() and NetworkProfile methods.
// ============================================================================

/// Datacenter profile: connection-level receive window (128 MiB)
pub const DATACENTER_RECEIVE_WINDOW: u64 = 128 * 1024 * 1024;
/// Datacenter profile: per-stream receive window (16 MiB)
pub const DATACENTER_STREAM_RECEIVE_WINDOW: u64 = 16 * 1024 * 1024;
/// Datacenter profile: send window (128 MiB)
pub const DATACENTER_SEND_WINDOW: u64 = 128 * 1024 * 1024;
/// Datacenter profile: initial RTT estimate in microseconds (0.3ms = 300µs)
pub const DATACENTER_INITIAL_RTT_US: u64 = 300;

/// Internet profile: connection-level receive window (8 MiB)
pub const INTERNET_RECEIVE_WINDOW: u64 = 8 * 1024 * 1024;
/// Internet profile: per-stream receive window (2 MiB)
pub const INTERNET_STREAM_RECEIVE_WINDOW: u64 = 2 * 1024 * 1024;
/// Internet profile: send window (8 MiB)
pub const INTERNET_SEND_WINDOW: u64 = 8 * 1024 * 1024;
/// Internet profile: initial RTT estimate in microseconds (100ms = 100,000µs)
pub const INTERNET_INITIAL_RTT_US: u64 = 100_000;

/// Datacenter profile: buffer size for remote copy operations.
///
/// Matches the per-stream receive window to maximize throughput on high-bandwidth
/// datacenter networks.
pub const DATACENTER_REMOTE_COPY_BUFFER_SIZE: usize = DATACENTER_STREAM_RECEIVE_WINDOW as usize;

/// Internet profile: buffer size for remote copy operations.
///
/// Matches the per-stream receive window for consistency with flow control.
pub const INTERNET_REMOTE_COPY_BUFFER_SIZE: usize = INTERNET_STREAM_RECEIVE_WINDOW as usize;

/// Congestion control algorithm selection
///
/// BBR (default) provides faster ramp-up on dedicated high-bandwidth links.
/// CUBIC is more conservative and fairer on shared networks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum CongestionControl {
    /// BBR (Bottleneck Bandwidth and RTT) - model-based, fast ramp-up
    /// Best for dedicated high-bandwidth links
    #[default]
    Bbr,
    /// CUBIC - loss-based, standard TCP congestion control
    /// Best for shared networks or internet connections
    Cubic,
}

impl std::fmt::Display for CongestionControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bbr => write!(f, "bbr"),
            Self::Cubic => write!(f, "cubic"),
        }
    }
}

impl std::str::FromStr for CongestionControl {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "bbr" => Ok(Self::Bbr),
            "cubic" => Ok(Self::Cubic),
            _ => Err(format!(
                "invalid congestion control '{}', expected 'bbr' or 'cubic'",
                s
            )),
        }
    }
}

/// Advanced QUIC tuning parameters
///
/// All fields are optional overrides. When set, they take precedence over
/// profile defaults. Use these for fine-tuning in specific environments.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct QuicTuning {
    /// Connection-level receive window in bytes (default: 128 MiB for datacenter, 8 MiB for internet)
    pub receive_window: Option<u64>,
    /// Per-stream receive window in bytes (default: 16 MiB for datacenter, 2 MiB for internet)
    pub stream_receive_window: Option<u64>,
    /// Send window in bytes (default: 128 MiB for datacenter, 8 MiB for internet)
    pub send_window: Option<u64>,
    /// Initial RTT estimate in milliseconds (default: 0.3ms for datacenter, 100ms for internet)
    /// Accepts floating point values for sub-millisecond precision (e.g., 0.3 for 300µs)
    pub initial_rtt_ms: Option<f64>,
    /// Initial MTU in bytes (default: 1200)
    pub initial_mtu: Option<u16>,
    /// Buffer size for remote copy file transfer operations.
    ///
    /// Defaults to per-stream receive window size for each profile (16 MiB for datacenter,
    /// 2 MiB for internet). This controls the buffer used when copying data between files
    /// and network streams. Larger buffers can improve throughput but use more memory
    /// per concurrent transfer.
    pub remote_copy_buffer_size: Option<usize>,
}

/// Configuration for QUIC connections
#[derive(Debug, Clone)]
pub struct QuicConfig {
    /// Port ranges to use for QUIC connections (e.g., "8000-8999,9000-9999")
    pub port_ranges: Option<String>,
    /// Maximum idle time before closing connection (seconds)
    pub idle_timeout_sec: u64,
    /// Interval for keep-alive packets (seconds)
    pub keep_alive_interval_sec: u64,
    /// Connection timeout for remote operations (seconds)
    pub conn_timeout_sec: u64,
    /// Network profile for tuning (default: Datacenter)
    pub network_profile: NetworkProfile,
    /// Congestion control algorithm override (None = use profile default)
    pub congestion_control: Option<CongestionControl>,
    /// Advanced tuning overrides
    pub tuning: QuicTuning,
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            port_ranges: None,
            idle_timeout_sec: 10,
            keep_alive_interval_sec: 1,
            conn_timeout_sec: 15,
            network_profile: NetworkProfile::default(),
            congestion_control: None, // use profile default
            tuning: QuicTuning::default(),
        }
    }
}

impl QuicConfig {
    /// Create QuicConfig with custom timeout values
    pub fn with_timeouts(
        idle_timeout_sec: u64,
        keep_alive_interval_sec: u64,
        conn_timeout_sec: u64,
    ) -> Self {
        Self {
            port_ranges: None,
            idle_timeout_sec,
            keep_alive_interval_sec,
            conn_timeout_sec,
            network_profile: NetworkProfile::default(),
            congestion_control: None,
            tuning: QuicTuning::default(),
        }
    }

    /// Set port ranges
    pub fn with_port_ranges(mut self, ranges: impl Into<String>) -> Self {
        self.port_ranges = Some(ranges.into());
        self
    }

    /// Set network profile
    pub fn with_network_profile(mut self, profile: NetworkProfile) -> Self {
        self.network_profile = profile;
        self
    }

    /// Set congestion control algorithm (overrides profile default)
    pub fn with_congestion_control(mut self, cc: CongestionControl) -> Self {
        self.congestion_control = Some(cc);
        self
    }

    /// Set advanced tuning parameters
    pub fn with_tuning(mut self, tuning: QuicTuning) -> Self {
        self.tuning = tuning;
        self
    }

    /// Get the effective congestion control algorithm (explicit or profile default)
    pub fn effective_congestion_control(&self) -> CongestionControl {
        self.congestion_control
            .unwrap_or_else(|| self.network_profile.default_congestion_control())
    }
    /// Get the effective remote copy buffer size (explicit or profile default)
    ///
    /// Returns the buffer size from tuning if set, otherwise uses the profile default
    /// (16 MiB for datacenter, 2 MiB for internet).
    pub fn effective_remote_copy_buffer_size(&self) -> usize {
        self.tuning
            .remote_copy_buffer_size
            .unwrap_or_else(|| self.network_profile.default_remote_copy_buffer_size())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct SshSession {
    pub user: Option<String>,
    pub host: String,
    pub port: Option<u16>,
}

impl SshSession {
    pub fn local() -> Self {
        Self {
            user: None,
            host: "localhost".to_string(),
            port: None,
        }
    }
}

// re-export is_localhost from common for convenience
pub use common::is_localhost;

async fn setup_ssh_session(
    session: &SshSession,
) -> anyhow::Result<std::sync::Arc<openssh::Session>> {
    let host = session.host.as_str();
    let destination = match (session.user.as_deref(), session.port) {
        (Some(user), Some(port)) => format!("ssh://{user}@{host}:{port}"),
        (None, Some(port)) => format!("ssh://{}:{}", session.host, port),
        (Some(user), None) => format!("ssh://{user}@{host}"),
        (None, None) => format!("ssh://{host}"),
    };
    tracing::debug!("Connecting to SSH destination: {}", destination);
    let session = std::sync::Arc::new(
        openssh::Session::connect(destination, openssh::KnownHosts::Accept)
            .await
            .context("Failed to establish SSH connection")?,
    );
    Ok(session)
}

#[instrument]
pub async fn get_remote_home_for_session(
    session: &SshSession,
) -> anyhow::Result<std::path::PathBuf> {
    let ssh_session = setup_ssh_session(session).await?;
    let home = get_remote_home(&ssh_session).await?;
    Ok(std::path::PathBuf::from(home))
}

#[instrument]
pub async fn wait_for_rcpd_process(
    process: openssh::Child<std::sync::Arc<openssh::Session>>,
) -> anyhow::Result<()> {
    tracing::info!("Waiting on rcpd server on: {:?}", process);
    // wait for process to exit with a timeout and capture output
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        process.wait_with_output(),
    )
    .await
    .context("Timeout waiting for rcpd process to exit")?
    .context("Failed to wait for rcpd process")?;
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::error!(
            "rcpd command failed on remote host, status code: {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            stdout,
            stderr
        );
        return Err(anyhow!(
            "rcpd command failed on remote host, status code: {:?}",
            output.status.code(),
        ));
    }
    // log stderr even on success if there's any output (might contain warnings)
    if !output.stderr.is_empty() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::debug!("rcpd stderr output:\n{}", stderr);
    }
    Ok(())
}

/// Escape a string for safe use in POSIX shell single quotes
///
/// Wraps the string in single quotes and escapes any single quotes within
pub(crate) fn shell_escape(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// Validate and retrieve HOME directory on remote host
///
/// Checks that $HOME is set and non-empty on the remote host.
/// This prevents constructing invalid paths like `/.cache/rcp/bin/rcpd-{version}`
/// when HOME is not set.
///
/// # Arguments
///
/// * `session` - SSH session to the remote host
///
/// # Returns
///
/// The value of $HOME on the remote host
///
/// # Errors
///
/// Returns an error if HOME is not set or is empty
pub async fn get_remote_home(session: &std::sync::Arc<openssh::Session>) -> anyhow::Result<String> {
    if let Ok(home_override) = std::env::var("RCP_REMOTE_HOME_OVERRIDE") {
        if !home_override.is_empty() {
            return Ok(home_override);
        }
    }
    let output = session
        .command("sh")
        .arg("-c")
        .arg("echo \"${HOME:?HOME not set}\"")
        .output()
        .await
        .context("failed to check HOME environment variable on remote host")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "HOME environment variable is not set on remote host\n\
            \n\
            stderr: {}\n\
            \n\
            The HOME environment variable is required for rcpd deployment and discovery.\n\
            Please ensure your SSH configuration preserves environment variables.",
            stderr
        );
    }

    let home = String::from_utf8_lossy(&output.stdout).trim().to_string();

    if home.is_empty() {
        anyhow::bail!(
            "HOME environment variable is empty on remote host\n\
            \n\
            The HOME environment variable is required for rcpd deployment and discovery.\n\
            Please ensure your SSH configuration sets HOME correctly."
        );
    }

    Ok(home)
}

#[cfg(test)]
mod shell_escape_tests {
    use super::*;

    #[test]
    fn test_shell_escape_simple() {
        assert_eq!(shell_escape("simple"), "'simple'");
    }

    #[test]
    fn test_shell_escape_with_spaces() {
        assert_eq!(shell_escape("path with spaces"), "'path with spaces'");
    }

    #[test]
    fn test_shell_escape_with_single_quote() {
        // single quote becomes: close quote, escaped quote, open quote
        assert_eq!(
            shell_escape("path'with'quotes"),
            r"'path'\''with'\''quotes'"
        );
    }

    #[test]
    fn test_shell_escape_injection_attempt() {
        // attempt to inject command
        assert_eq!(shell_escape("foo; rm -rf /"), "'foo; rm -rf /'");
        // the semicolon is now safely quoted and won't execute
    }

    #[test]
    fn test_shell_escape_special_chars() {
        assert_eq!(shell_escape("$PATH && echo pwned"), "'$PATH && echo pwned'");
        // special chars are safely quoted
    }
}

trait DiscoverySession {
    fn test_executable<'a>(
        &'a self,
        path: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<bool>> + Send + 'a>>;
    fn which<'a>(
        &'a self,
        binary: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<Option<String>>> + Send + 'a>,
    >;
    fn remote_home<'a>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send + 'a>>;
}

struct RealDiscoverySession<'a> {
    session: &'a std::sync::Arc<openssh::Session>,
}

impl<'a> DiscoverySession for RealDiscoverySession<'a> {
    fn test_executable<'b>(
        &'b self,
        path: &'b str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<bool>> + Send + 'b>>
    {
        Box::pin(async move {
            let output = self
                .session
                .command("sh")
                .arg("-c")
                .arg(format!("test -x {}", shell_escape(path)))
                .output()
                .await?;
            Ok(output.status.success())
        })
    }
    fn which<'b>(
        &'b self,
        binary: &'b str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<Option<String>>> + Send + 'b>,
    > {
        Box::pin(async move {
            let output = self.session.command("which").arg(binary).output().await?;
            if output.status.success() {
                let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !path.is_empty() {
                    return Ok(Some(path));
                }
            }
            Ok(None)
        })
    }
    fn remote_home<'b>(
        &'b self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send + 'b>>
    {
        Box::pin(get_remote_home(self.session))
    }
}

/// Discover rcpd binary on remote host
///
/// Searches in the following order:
/// 1. Explicit path (if provided)
/// 2. Same directory as local rcp binary
/// 3. PATH (via `which rcpd`)
/// 4. Deployed cache directory (~/.cache/rcp/bin/rcpd-{version})
///
/// The cache is checked last as it contains auto-deployed binaries and should
/// only be used as a fallback after checking user-installed locations.
///
/// Returns the path to rcpd if found, otherwise an error
async fn discover_rcpd_path(
    session: &std::sync::Arc<openssh::Session>,
    explicit_path: Option<&str>,
) -> anyhow::Result<String> {
    let real_session = RealDiscoverySession { session };
    discover_rcpd_path_internal(&real_session, explicit_path, None).await
}

async fn discover_rcpd_path_internal<S: DiscoverySession + ?Sized>(
    session: &S,
    explicit_path: Option<&str>,
    current_exe_override: Option<std::path::PathBuf>,
) -> anyhow::Result<String> {
    let local_version = common::version::ProtocolVersion::current();
    // try explicit path first
    if let Some(path) = explicit_path {
        tracing::debug!("Trying explicit rcpd path: {}", path);
        if session.test_executable(path).await? {
            tracing::info!("Found rcpd at explicit path: {}", path);
            return Ok(path.to_string());
        }
        // explicit path was provided but not found - return error immediately
        // don't fall back to other discovery methods
        return Err(anyhow::anyhow!(
            "rcpd binary not found or not executable at explicit path: {}",
            path
        ));
    }
    // try same directory as local rcp binary
    if let Ok(current_exe) = current_exe_override
        .map(Ok)
        .unwrap_or_else(std::env::current_exe)
    {
        if let Some(bin_dir) = current_exe.parent() {
            let path = bin_dir.join("rcpd").display().to_string();
            tracing::debug!("Trying same directory as rcp: {}", path);
            if session.test_executable(&path).await? {
                tracing::info!("Found rcpd in same directory as rcp: {}", path);
                return Ok(path);
            }
        }
    }
    // try PATH
    tracing::debug!("Trying to find rcpd in PATH");
    if let Some(path) = session.which("rcpd").await? {
        tracing::info!("Found rcpd in PATH: {}", path);
        return Ok(path);
    }
    // try deployed cache directory as last resort (reuse already-deployed binaries)
    // if HOME is not set, skip cache check
    let cache_path = match session.remote_home().await {
        Ok(home) => {
            let path = format!("{}/.cache/rcp/bin/rcpd-{}", home, local_version.semantic);
            tracing::debug!("Trying deployed cache path: {}", path);
            if session.test_executable(&path).await? {
                tracing::info!("Found rcpd in deployed cache: {}", path);
                return Ok(path);
            }
            Some(path)
        }
        Err(e) => {
            tracing::debug!(
                "HOME not set on remote host, skipping cache directory check: {:#}",
                e
            );
            None
        }
    };
    // build error message with what we searched
    let mut searched = vec![];
    searched.push("- Same directory as local rcp binary".to_string());
    searched.push("- PATH (via 'which rcpd')".to_string());
    if let Some(path) = cache_path.as_ref() {
        searched.push(format!("- Deployed cache: {}", path));
    } else {
        searched.push("- Deployed cache: (skipped, HOME not set)".to_string());
    }
    if let Some(path) = explicit_path {
        searched.insert(
            0,
            format!("- Explicit path: {} (not found or not executable)", path),
        );
    }
    Err(anyhow::anyhow!(
        "rcpd binary not found on remote host\n\
        \n\
        Searched in:\n\
        {}\n\
        \n\
        Options:\n\
        - Use automatic deployment: rcp --auto-deploy-rcpd ...\n\
        - Install rcpd manually: cargo install rcp-tools-rcp --version {}\n\
        - Specify explicit path: rcp --rcpd-path=/path/to/rcpd ...",
        searched.join("\n"),
        local_version.semantic
    ))
}

/// Try to discover rcpd and check version compatibility
///
/// Combines discovery and version checking into one function for cleaner error handling.
/// Returns the path to a compatible rcpd if found, or an error describing the problem.
async fn try_discover_and_check_version(
    session: &std::sync::Arc<openssh::Session>,
    explicit_path: Option<&str>,
    remote_host: &str,
) -> anyhow::Result<String> {
    // discover rcpd binary on remote host
    let rcpd_path = discover_rcpd_path(session, explicit_path).await?;
    // check version compatibility
    check_rcpd_version(session, &rcpd_path, remote_host).await?;
    Ok(rcpd_path)
}

/// Check version compatibility between local rcp and remote rcpd
///
/// Returns Ok if versions are compatible, Err with detailed message if not
async fn check_rcpd_version(
    session: &std::sync::Arc<openssh::Session>,
    rcpd_path: &str,
    remote_host: &str,
) -> anyhow::Result<()> {
    let local_version = common::version::ProtocolVersion::current();

    tracing::debug!("Checking rcpd version on remote host: {}", remote_host);

    // run rcpd --protocol-version on remote (call binary directly, no shell)
    let output = session
        .command(rcpd_path)
        .arg("--protocol-version")
        .output()
        .await
        .context("Failed to execute rcpd --protocol-version on remote host")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!(
            "rcpd --protocol-version failed on remote host '{}'\n\
            \n\
            stderr: {}\n\
            \n\
            This may indicate an old version of rcpd that does not support --protocol-version.\n\
            Please install a matching version of rcpd on the remote host:\n\
            - cargo install rcp-tools-rcp --version {}",
            remote_host,
            stderr,
            local_version.semantic
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let remote_version = common::version::ProtocolVersion::from_json(stdout.trim())
        .context("Failed to parse rcpd version JSON from remote host")?;

    tracing::info!(
        "Local version: {}, Remote version: {}",
        local_version,
        remote_version
    );

    if !local_version.is_compatible_with(&remote_version) {
        return Err(anyhow::anyhow!(
            "rcpd version mismatch\n\
            \n\
            Local:  rcp {}\n\
            Remote: rcpd {} on host '{}'\n\
            \n\
            The rcpd version on the remote host must exactly match the rcp version.\n\
            \n\
            To fix this, install the matching version on the remote host:\n\
            - ssh {} 'cargo install rcp-tools-rcp --version {}'",
            local_version,
            remote_version,
            remote_host,
            shell_escape(remote_host),
            local_version.semantic
        ));
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[instrument]
pub async fn start_rcpd(
    rcpd_config: &protocol::RcpdConfig,
    session: &SshSession,
    master_addr: &std::net::SocketAddr,
    master_server_name: &str,
    explicit_rcpd_path: Option<&str>,
    auto_deploy_rcpd: bool,
    bind_ip: Option<&str>,
    role: protocol::RcpdRole,
) -> anyhow::Result<openssh::Child<std::sync::Arc<openssh::Session>>> {
    tracing::info!("Starting rcpd server on: {:?}", session);
    let remote_host = &session.host;
    let ssh_session = setup_ssh_session(session).await?;
    // try to discover rcpd binary on remote host and check version
    let rcpd_path =
        match try_discover_and_check_version(&ssh_session, explicit_rcpd_path, remote_host).await {
            Ok(path) => {
                // found compatible rcpd
                path
            }
            Err(e) => {
                // discovery or version check failed
                if auto_deploy_rcpd {
                    tracing::info!(
                        "rcpd not found or version mismatch, attempting auto-deployment"
                    );
                    // find local rcpd binary
                    let local_rcpd = deploy::find_local_rcpd_binary()
                        .context("failed to find local rcpd binary for deployment")?;
                    tracing::info!("Found local rcpd binary at {}", local_rcpd.display());
                    // get version for deployment path
                    let local_version = common::version::ProtocolVersion::current();
                    // deploy to remote host
                    let deployed_path = deploy::deploy_rcpd(
                        &ssh_session,
                        &local_rcpd,
                        &local_version.semantic,
                        remote_host,
                    )
                    .await
                    .context("failed to deploy rcpd to remote host")?;
                    tracing::info!("Successfully deployed rcpd to {}", deployed_path);
                    // cleanup old versions (best effort, don't fail if this errors)
                    if let Err(e) = deploy::cleanup_old_versions(&ssh_session, 3).await {
                        tracing::warn!("failed to cleanup old versions (non-fatal): {:#}", e);
                    }
                    deployed_path
                } else {
                    // no auto-deploy, return original error
                    return Err(e);
                }
            }
        };
    // run rcpd command remotely
    let rcpd_args = rcpd_config.to_args();
    tracing::debug!("rcpd arguments: {:?}", rcpd_args);
    let mut cmd = ssh_session.arc_command(&rcpd_path);
    cmd.arg("--master-addr")
        .arg(master_addr.to_string())
        .arg("--server-name")
        .arg(master_server_name)
        .arg("--role")
        .arg(role.to_string())
        .args(rcpd_args);
    // add bind-ip if explicitly provided
    if let Some(ip) = bind_ip {
        tracing::debug!("passing --bind-ip {} to rcpd", ip);
        cmd.arg("--bind-ip").arg(ip);
    }
    // configure stdin/stdout/stderr
    // stdin must be piped so rcpd can monitor it for master disconnection (stdin watchdog)
    cmd.stdin(openssh::Stdio::piped());
    cmd.stdout(openssh::Stdio::piped());
    cmd.stderr(openssh::Stdio::piped());
    tracing::info!("Will run remotely: {cmd:?}");
    cmd.spawn().await.context("Failed to spawn rcpd command")
}

/// Apply QUIC configuration to a transport config
///
/// This applies network profile settings, congestion control, and any tuning overrides.
fn apply_quic_tuning(transport_config: &mut quinn::TransportConfig, config: &QuicConfig) {
    // 1. apply base profile settings from the module-level constants
    let (receive_window, stream_receive_window, send_window, initial_rtt_us) =
        match config.network_profile {
            NetworkProfile::Datacenter => (
                DATACENTER_RECEIVE_WINDOW,
                DATACENTER_STREAM_RECEIVE_WINDOW,
                DATACENTER_SEND_WINDOW,
                DATACENTER_INITIAL_RTT_US,
            ),
            NetworkProfile::Internet => (
                INTERNET_RECEIVE_WINDOW,
                INTERNET_STREAM_RECEIVE_WINDOW,
                INTERNET_SEND_WINDOW,
                INTERNET_INITIAL_RTT_US,
            ),
        };
    // apply profile defaults (will be overridden by tuning if specified)
    transport_config
        .receive_window(quinn::VarInt::from_u64(receive_window).unwrap_or(quinn::VarInt::MAX));
    transport_config.stream_receive_window(
        quinn::VarInt::from_u64(stream_receive_window).unwrap_or(quinn::VarInt::MAX),
    );
    transport_config.send_window(send_window);
    transport_config.initial_rtt(std::time::Duration::from_micros(initial_rtt_us));
    // 2. apply congestion control (explicit override or profile default)
    let effective_cc = config.effective_congestion_control();
    match effective_cc {
        CongestionControl::Bbr => {
            transport_config.congestion_controller_factory(std::sync::Arc::new(
                quinn::congestion::BbrConfig::default(),
            ));
        }
        CongestionControl::Cubic => {
            transport_config.congestion_controller_factory(std::sync::Arc::new(
                quinn::congestion::CubicConfig::default(),
            ));
        }
    }
    // 3. apply tuning overrides (take precedence over profile)
    if let Some(v) = config.tuning.receive_window {
        transport_config.receive_window(quinn::VarInt::from_u64(v).unwrap_or(quinn::VarInt::MAX));
    }
    if let Some(v) = config.tuning.stream_receive_window {
        transport_config
            .stream_receive_window(quinn::VarInt::from_u64(v).unwrap_or(quinn::VarInt::MAX));
    }
    if let Some(v) = config.tuning.send_window {
        transport_config.send_window(v);
    }
    if let Some(v) = config.tuning.initial_rtt_ms {
        // convert f64 milliseconds to Duration (supports sub-millisecond precision)
        let micros = (v * 1000.0) as u64;
        transport_config.initial_rtt(std::time::Duration::from_micros(micros));
    }
    if let Some(v) = config.tuning.initial_mtu {
        transport_config.initial_mtu(v);
    }
    tracing::info!(
        "Applied QUIC tuning: profile={}, congestion_control={}, receive_window={}, stream_receive_window={}, send_window={}",
        config.network_profile,
        effective_cc,
        config.tuning.receive_window.unwrap_or(receive_window),
        config.tuning.stream_receive_window.unwrap_or(stream_receive_window),
        config.tuning.send_window.unwrap_or(send_window),
    );
}

/// Compute SHA-256 fingerprint of a DER-encoded certificate
fn compute_cert_fingerprint(cert_der: &[u8]) -> ring::digest::Digest {
    ring::digest::digest(&ring::digest::SHA256, cert_der)
}

/// Configure QUIC server with a self-signed certificate
/// Returns the server config and the SHA-256 fingerprint of the certificate
fn configure_server(config: &QuicConfig) -> anyhow::Result<(quinn::ServerConfig, Vec<u8>)> {
    tracing::info!(
        "Configuring QUIC server (idle_timeout={}s, keep_alive={}s, profile={}, cc={})",
        config.idle_timeout_sec,
        config.keep_alive_interval_sec,
        config.network_profile,
        config.effective_congestion_control(),
    );
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let key_der = cert.serialize_private_key_der();
    let cert_der = cert.serialize_der()?;
    let fingerprint = compute_cert_fingerprint(&cert_der);
    let fingerprint_vec = fingerprint.as_ref().to_vec();
    tracing::debug!(
        "Generated certificate with fingerprint: {}",
        hex::encode(&fingerprint_vec)
    );
    let key = rustls::PrivateKey(key_der);
    let cert = rustls::Certificate(cert_der);
    let mut server_config = quinn::ServerConfig::with_single_cert(vec![cert], key)
        .context("Failed to create server config")?;
    // configure transport
    let mut transport_config = quinn::TransportConfig::default();
    // apply timeouts
    transport_config.max_idle_timeout(Some(
        std::time::Duration::from_secs(config.idle_timeout_sec)
            .try_into()
            .context("Failed to convert idle timeout to VarInt")?,
    ));
    transport_config.keep_alive_interval(Some(std::time::Duration::from_secs(
        config.keep_alive_interval_sec,
    )));
    // apply profile, congestion control, and tuning
    apply_quic_tuning(&mut transport_config, config);
    server_config.transport_config(std::sync::Arc::new(transport_config));
    Ok((server_config, fingerprint_vec))
}

/// Create a QUIC server endpoint with the full QuicConfig
#[instrument(skip(config))]
pub fn get_server_with_config(config: &QuicConfig) -> anyhow::Result<(quinn::Endpoint, Vec<u8>)> {
    let (server_config, cert_fingerprint) = configure_server(config)?;
    let socket = if let Some(ranges_str) = config.port_ranges.as_deref() {
        let ranges = port_ranges::PortRanges::parse(ranges_str)?;
        ranges.bind_udp_socket(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED))?
    } else {
        // default behavior: bind to any available port
        std::net::UdpSocket::bind("0.0.0.0:0")?
    };
    let endpoint = quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        Some(server_config),
        socket,
        std::sync::Arc::new(quinn::TokioRuntime),
    )
    .context("Failed to create QUIC endpoint")?;
    Ok((endpoint, cert_fingerprint))
}

/// Create a QUIC server endpoint (legacy API for backwards compatibility)
#[instrument]
pub fn get_server_with_port_ranges(
    port_ranges: Option<&str>,
    idle_timeout_sec: u64,
    keep_alive_interval_sec: u64,
) -> anyhow::Result<(quinn::Endpoint, Vec<u8>)> {
    let mut config = QuicConfig::with_timeouts(
        idle_timeout_sec,
        keep_alive_interval_sec,
        15, // default conn_timeout
    );
    if let Some(ranges) = port_ranges {
        config.port_ranges = Some(ranges.to_string());
    }
    get_server_with_config(&config)
}

// certificate verifier that validates against a pinned certificate fingerprint
// This prevents MITM attacks by ensuring we're connecting to the expected server
struct PinnedCertVerifier {
    expected_fingerprint: Vec<u8>,
}

impl PinnedCertVerifier {
    fn new(expected_fingerprint: Vec<u8>) -> Self {
        Self {
            expected_fingerprint,
        }
    }
}

impl rustls::client::ServerCertVerifier for PinnedCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        let received_fingerprint = compute_cert_fingerprint(&end_entity.0);
        if received_fingerprint.as_ref() == self.expected_fingerprint.as_slice() {
            tracing::debug!(
                "Certificate fingerprint validated successfully: {}",
                hex::encode(&self.expected_fingerprint)
            );
            Ok(rustls::client::ServerCertVerified::assertion())
        } else {
            tracing::error!(
                "Certificate fingerprint mismatch! Expected: {}, Got: {}",
                hex::encode(&self.expected_fingerprint),
                hex::encode(received_fingerprint)
            );
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::Other(std::sync::Arc::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Certificate fingerprint mismatch (expected {}, got {})",
                        hex::encode(&self.expected_fingerprint),
                        hex::encode(received_fingerprint)
                    ),
                ))),
            ))
        }
    }
}

fn get_local_ip(explicit_bind_ip: Option<&str>) -> anyhow::Result<std::net::IpAddr> {
    // if explicit IP provided, validate and use it
    if let Some(ip_str) = explicit_bind_ip {
        let ip = ip_str
            .parse::<std::net::IpAddr>()
            .with_context(|| format!("invalid IP address: {}", ip_str))?;
        match ip {
            std::net::IpAddr::V4(ipv4) => {
                tracing::debug!("using explicit bind IP: {}", ipv4);
                return Ok(std::net::IpAddr::V4(ipv4));
            }
            std::net::IpAddr::V6(_) => {
                anyhow::bail!(
                    "IPv6 address not supported for binding (got {}). \
                     QUIC endpoint binds to 0.0.0.0 (IPv4 only)",
                    ip
                );
            }
        }
    }

    // auto-detection: try kernel routing first
    if let Some(ipv4) = try_ipv4_via_kernel_routing()? {
        return Ok(std::net::IpAddr::V4(ipv4));
    }

    // fallback to interface enumeration
    tracing::debug!("routing-based detection failed, falling back to interface enumeration");
    let interfaces = collect_ipv4_interfaces().context("Failed to enumerate network interfaces")?;
    if let Some(ipv4) = choose_best_ipv4(&interfaces) {
        tracing::debug!("using IPv4 address from interface scan: {}", ipv4);
        return Ok(std::net::IpAddr::V4(ipv4));
    }

    anyhow::bail!("No IPv4 interfaces found (QUIC endpoint requires IPv4 as it binds to 0.0.0.0)")
}

fn try_ipv4_via_kernel_routing() -> anyhow::Result<Option<std::net::Ipv4Addr>> {
    // important: our QUIC endpoints bind to 0.0.0.0 (IPv4-only), so we must return IPv4
    // strategy: ask the kernel which interface it would use by connecting to RFC1918 targets.
    // these addresses never leave the local network but still exercise the routing table.
    let private_ips = [
        "10.0.0.1:80",    // class a private
        "172.16.0.1:80",  // class b private
        "192.168.1.1:80", // class c private
    ];
    for addr_str in &private_ips {
        let addr = addr_str
            .parse::<std::net::SocketAddr>()
            .expect("hardcoded socket addresses are valid");
        let socket = match std::net::UdpSocket::bind("0.0.0.0:0") {
            Ok(socket) => socket,
            Err(err) => {
                tracing::debug!(?err, "failed to bind UDP socket for routing detection");
                continue;
            }
        };
        if let Err(err) = socket.connect(addr) {
            tracing::debug!(?err, "connect() failed for routing target {}", addr);
            continue;
        }
        match socket.local_addr() {
            Ok(std::net::SocketAddr::V4(local_addr)) => {
                let ipv4 = *local_addr.ip();
                if !ipv4.is_loopback() && !ipv4.is_unspecified() {
                    tracing::debug!(
                        "using IPv4 address from kernel routing (via {}): {}",
                        addr,
                        ipv4
                    );
                    return Ok(Some(ipv4));
                }
            }
            Ok(_) => {
                tracing::debug!("kernel routing returned IPv6 despite IPv4 bind, ignoring");
            }
            Err(err) => {
                tracing::debug!(?err, "local_addr() failed for routing-based detection");
            }
        }
    }
    Ok(None)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InterfaceIpv4 {
    name: String,
    addr: std::net::Ipv4Addr,
}

fn collect_ipv4_interfaces() -> anyhow::Result<Vec<InterfaceIpv4>> {
    use if_addrs::get_if_addrs;
    let mut interfaces = Vec::new();
    for iface in get_if_addrs()? {
        if let std::net::IpAddr::V4(ipv4) = iface.addr.ip() {
            interfaces.push(InterfaceIpv4 {
                name: iface.name,
                addr: ipv4,
            });
        }
    }
    Ok(interfaces)
}

fn choose_best_ipv4(interfaces: &[InterfaceIpv4]) -> Option<std::net::Ipv4Addr> {
    interfaces
        .iter()
        .filter(|iface| !iface.addr.is_unspecified())
        .min_by_key(|iface| interface_priority(&iface.name, &iface.addr))
        .map(|iface| iface.addr)
}

fn interface_priority(
    name: &str,
    addr: &std::net::Ipv4Addr,
) -> (InterfaceCategory, u8, u8, std::net::Ipv4Addr) {
    (
        classify_interface(name, addr),
        if addr.is_link_local() { 1 } else { 0 },
        if addr.is_private() { 1 } else { 0 },
        *addr,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum InterfaceCategory {
    Preferred = 0,
    Normal = 1,
    Virtual = 2,
    Loopback = 3,
}

fn classify_interface(name: &str, addr: &std::net::Ipv4Addr) -> InterfaceCategory {
    if addr.is_loopback() {
        return InterfaceCategory::Loopback;
    }

    let normalized = normalize_interface_name(name);
    if is_virtual_interface(&normalized) {
        return InterfaceCategory::Virtual;
    }
    if is_preferred_physical_interface(&normalized) {
        return InterfaceCategory::Preferred;
    }
    InterfaceCategory::Normal
}

fn normalize_interface_name(original: &str) -> String {
    let mut normalized = String::with_capacity(original.len());
    for ch in original.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
        }
    }
    normalized
}

fn is_virtual_interface(name: &str) -> bool {
    const VIRTUAL_PREFIXES: &[&str] = &[
        "br",
        "docker",
        "veth",
        "virbr",
        "vmnet",
        "wg",
        "tailscale",
        "zt",
        "zerotier",
        "tap",
        "tun",
        "utun",
        "ham",
        "vpn",
        "lo",
        "lxc",
    ];
    VIRTUAL_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix))
        || name.contains("virtual")
}

fn is_preferred_physical_interface(name: &str) -> bool {
    const PHYSICAL_PREFIXES: &[&str] = &[
        "en",  // linux + macos ethernet (en0, enp3s0, eno1, etc.)
        "eth", // legacy ethernet naming
        "em",  // embedded NICs (em1, etc.)
        "eno", "ens", "enp", "wl", // wi-fi
        "ww", // wwan
        "wlan", "ethernet", // windows
        "lan", "wifi",
    ];
    PHYSICAL_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

#[instrument]
pub fn get_endpoint_addr(endpoint: &quinn::Endpoint) -> anyhow::Result<std::net::SocketAddr> {
    get_endpoint_addr_with_bind_ip(endpoint, None)
}

pub fn get_endpoint_addr_with_bind_ip(
    endpoint: &quinn::Endpoint,
    bind_ip: Option<&str>,
) -> anyhow::Result<std::net::SocketAddr> {
    // endpoint is bound to 0.0.0.0 so we need to get the local IP address
    let local_ip = get_local_ip(bind_ip).context("Failed to get local IP address")?;
    let endpoint_addr = endpoint.local_addr()?;
    Ok(std::net::SocketAddr::new(local_ip, endpoint_addr.port()))
}

#[instrument]
pub fn get_random_server_name() -> String {
    rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(20)
        .map(char::from)
        .collect()
}

/// Create a QUIC client endpoint with the full QuicConfig and certificate pinning
#[instrument(skip(config))]
pub fn get_client_with_config_and_pinning(
    config: &QuicConfig,
    cert_fingerprint: Vec<u8>,
) -> anyhow::Result<quinn::Endpoint> {
    tracing::info!(
        "Creating QUIC client (fingerprint: {}, idle_timeout={}s, keep_alive={}s, profile={}, cc={})",
        hex::encode(&cert_fingerprint),
        config.idle_timeout_sec,
        config.keep_alive_interval_sec,
        config.network_profile,
        config.effective_congestion_control(),
    );
    // create a crypto backend with certificate pinning
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(std::sync::Arc::new(PinnedCertVerifier::new(
            cert_fingerprint,
        )))
        .with_no_client_auth();
    create_client_endpoint_with_config(config, crypto)
}

/// Create a QUIC client endpoint (legacy API for backwards compatibility)
#[instrument]
pub fn get_client_with_port_ranges_and_pinning(
    port_ranges: Option<&str>,
    cert_fingerprint: Vec<u8>,
    idle_timeout_sec: u64,
    keep_alive_interval_sec: u64,
) -> anyhow::Result<quinn::Endpoint> {
    let mut config = QuicConfig::with_timeouts(
        idle_timeout_sec,
        keep_alive_interval_sec,
        15, // default conn_timeout
    );
    if let Some(ranges) = port_ranges {
        config.port_ranges = Some(ranges.to_string());
    }
    get_client_with_config_and_pinning(&config, cert_fingerprint)
}

// helper function to create client endpoint with given crypto config and QuicConfig
fn create_client_endpoint_with_config(
    config: &QuicConfig,
    crypto: rustls::ClientConfig,
) -> anyhow::Result<quinn::Endpoint> {
    // create QUIC client config
    let mut client_config = quinn::ClientConfig::new(std::sync::Arc::new(crypto));
    let mut transport_config = quinn::TransportConfig::default();
    // apply timeouts
    transport_config.max_idle_timeout(Some(
        std::time::Duration::from_secs(config.idle_timeout_sec)
            .try_into()
            .context("Failed to convert idle timeout to VarInt")?,
    ));
    transport_config.keep_alive_interval(Some(std::time::Duration::from_secs(
        config.keep_alive_interval_sec,
    )));
    // apply profile, congestion control, and tuning
    apply_quic_tuning(&mut transport_config, config);
    client_config.transport_config(std::sync::Arc::new(transport_config));
    let socket = if let Some(ranges_str) = config.port_ranges.as_deref() {
        let ranges = port_ranges::PortRanges::parse(ranges_str)?;
        ranges.bind_udp_socket(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED))?
    } else {
        // default behavior: bind to any available port
        std::net::UdpSocket::bind("0.0.0.0:0")?
    };
    // create and configure endpoint
    let mut endpoint = quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        None, // No server config for client
        socket,
        std::sync::Arc::new(quinn::TokioRuntime),
    )
    .context("Failed to create QUIC endpoint")?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

#[cfg(test)]
pub mod test_defaults {
    //! Test-only constants for QUIC timeout defaults
    //! These should not be used in production code - all production code should
    //! receive timeout values from CLI arguments

    /// Default QUIC idle timeout in seconds for tests
    pub const DEFAULT_QUIC_IDLE_TIMEOUT_SEC: u64 = 10;

    /// Default QUIC keep-alive interval in seconds for tests
    pub const DEFAULT_QUIC_KEEP_ALIVE_INTERVAL_SEC: u64 = 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    struct MockDiscoverySession {
        test_responses: HashMap<String, bool>,
        which_response: Option<String>,
        home_response: Result<String, String>,
        calls: Mutex<Vec<String>>,
    }

    impl Default for MockDiscoverySession {
        fn default() -> Self {
            Self {
                test_responses: HashMap::new(),
                which_response: None,
                home_response: Err("HOME not set".to_string()),
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl MockDiscoverySession {
        fn new() -> Self {
            Self::default()
        }

        fn with_home(mut self, home: Option<&str>) -> Self {
            self.home_response = match home {
                Some(home) => Ok(home.to_string()),
                None => Err("HOME not set".to_string()),
            };
            self
        }
        fn with_which(mut self, path: Option<&str>) -> Self {
            self.which_response = path.map(|p| p.to_string());
            self
        }
        fn set_test_response(&mut self, path: &str, exists: bool) {
            self.test_responses.insert(path.to_string(), exists);
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl DiscoverySession for MockDiscoverySession {
        fn test_executable<'a>(
            &'a self,
            path: &'a str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<bool>> + Send + 'a>>
        {
            self.calls.lock().unwrap().push(format!("test:{}", path));
            let exists = self.test_responses.get(path).copied().unwrap_or(false);
            Box::pin(async move { Ok(exists) })
        }
        fn which<'a>(
            &'a self,
            binary: &'a str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = anyhow::Result<Option<String>>> + Send + 'a>,
        > {
            self.calls.lock().unwrap().push(format!("which:{}", binary));
            let result = self.which_response.clone();
            Box::pin(async move { Ok(result) })
        }
        fn remote_home<'a>(
            &'a self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send + 'a>>
        {
            self.calls.lock().unwrap().push("home".to_string());
            let result = self.home_response.clone();
            Box::pin(async move {
                match result {
                    Ok(home) => Ok(home),
                    Err(e) => Err(anyhow::anyhow!(e)),
                }
            })
        }
    }

    fn endpoint_or_skip() -> (quinn::Endpoint, Vec<u8>) {
        get_server_with_port_ranges(
            None,
            test_defaults::DEFAULT_QUIC_IDLE_TIMEOUT_SEC,
            test_defaults::DEFAULT_QUIC_KEEP_ALIVE_INTERVAL_SEC,
        )
        .unwrap_or_else(|err| {
            panic!(
                "Failed to create QUIC endpoint for test; ensure UDP binding is permitted: {err:#}"
            )
        })
    }

    #[tokio::test]
    async fn discover_rcpd_prefers_explicit_path() {
        let mut session = MockDiscoverySession::new();
        session.set_test_response("/opt/rcpd", true);
        let path = discover_rcpd_path_internal(&session, Some("/opt/rcpd"), None)
            .await
            .expect("should return explicit path");
        assert_eq!(path, "/opt/rcpd");
        assert_eq!(session.calls(), vec!["test:/opt/rcpd"]);
    }

    #[tokio::test]
    async fn discover_rcpd_explicit_path_errors_without_fallbacks() {
        let session = MockDiscoverySession::new();
        let err = discover_rcpd_path_internal(&session, Some("/missing/rcpd"), None)
            .await
            .expect_err("should fail when explicit path is missing");
        assert!(
            err.to_string()
                .contains("rcpd binary not found or not executable"),
            "unexpected error: {err}"
        );
        assert_eq!(session.calls(), vec!["test:/missing/rcpd"]);
    }

    #[tokio::test]
    async fn discover_rcpd_uses_same_dir_first() {
        let mut session = MockDiscoverySession::new();
        session.set_test_response("/custom/bin/rcpd", true);
        let path =
            discover_rcpd_path_internal(&session, None, Some(PathBuf::from("/custom/bin/rcp")))
                .await
                .expect("should find in same directory");
        assert_eq!(path, "/custom/bin/rcpd");
        assert_eq!(session.calls(), vec!["test:/custom/bin/rcpd"]);
    }

    #[tokio::test]
    async fn discover_rcpd_falls_back_to_path_after_same_dir() {
        let mut session = MockDiscoverySession::new().with_which(Some("/usr/bin/rcpd"));
        session.set_test_response("/custom/bin/rcpd", false);
        let path =
            discover_rcpd_path_internal(&session, None, Some(PathBuf::from("/custom/bin/rcp")))
                .await
                .expect("should find in PATH after same dir miss");
        assert_eq!(path, "/usr/bin/rcpd");
        assert_eq!(session.calls(), vec!["test:/custom/bin/rcpd", "which:rcpd"]);
    }

    #[tokio::test]
    async fn discover_rcpd_uses_cache_last() {
        let mut session = MockDiscoverySession::new()
            .with_home(Some("/home/rcp"))
            .with_which(None);
        session.set_test_response("/custom/bin/rcpd", false);
        let local_version = common::version::ProtocolVersion::current();
        let cache_path = format!("/home/rcp/.cache/rcp/bin/rcpd-{}", local_version.semantic);
        session.set_test_response(&cache_path, true);
        let path =
            discover_rcpd_path_internal(&session, None, Some(PathBuf::from("/custom/bin/rcp")))
                .await
                .expect("should fall back to cache");
        assert_eq!(path, cache_path);
        assert_eq!(
            session.calls(),
            vec![
                "test:/custom/bin/rcpd".to_string(),
                "which:rcpd".to_string(),
                "home".to_string(),
                format!("test:{cache_path}")
            ]
        );
    }

    #[tokio::test]
    async fn discover_rcpd_reports_home_missing_in_error() {
        let mut session = MockDiscoverySession::new().with_which(None);
        session.set_test_response("/custom/bin/rcpd", false);
        let err =
            discover_rcpd_path_internal(&session, None, Some(PathBuf::from("/custom/bin/rcp")))
                .await
                .expect_err("should fail when nothing is found");
        let msg = err.to_string();
        assert!(
            msg.contains("Deployed cache: (skipped, HOME not set)"),
            "expected searched list to mention skipped cache, got: {msg}"
        );
        assert_eq!(
            session.calls(),
            vec!["test:/custom/bin/rcpd", "which:rcpd", "home"]
        );
    }

    /// verify that tokio_unstable is enabled
    ///
    /// this test ensures that the tokio_unstable cfg flag is properly set, which is required
    /// for console-subscriber (used in common/src/lib.rs) to function correctly.
    ///
    /// the compile_error! at the top of this file prevents compilation without tokio_unstable,
    /// but this test provides additional verification that the cfg flag is properly configured
    /// and catches cases where someone might remove the compile_error! macro.
    #[test]
    fn test_tokio_unstable_enabled() {
        // compile-time check: this will cause a test failure if tokio_unstable is not set
        #[cfg(not(tokio_unstable))]
        {
            panic!(
                "tokio_unstable cfg flag is not enabled! \
                 This is required for console-subscriber support. \
                 Check .cargo/config.toml"
            );
        }

        // runtime verification: if we get here, tokio_unstable is enabled
        #[cfg(tokio_unstable)]
        {
            // test passes - verify we can access tokio unstable features
            // tokio::task::JoinSet is an example of a type that uses unstable features
            let _join_set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        }
    }

    fn iface(name: &str, addr: [u8; 4]) -> InterfaceIpv4 {
        InterfaceIpv4 {
            name: name.to_string(),
            addr: std::net::Ipv4Addr::new(addr[0], addr[1], addr[2], addr[3]),
        }
    }

    #[test]
    fn choose_best_ipv4_prefers_physical_interfaces() {
        let interfaces = vec![
            iface("docker0", [172, 17, 0, 1]),
            iface("enp3s0", [192, 168, 1, 44]),
            iface("tailscale0", [100, 115, 92, 5]),
        ];
        assert_eq!(
            choose_best_ipv4(&interfaces),
            Some(std::net::Ipv4Addr::new(192, 168, 1, 44))
        );
    }

    #[test]
    fn choose_best_ipv4_deprioritizes_link_local() {
        let interfaces = vec![
            iface("enp0s8", [169, 254, 10, 2]),
            iface("wlan0", [10, 0, 0, 23]),
        ];
        assert_eq!(
            choose_best_ipv4(&interfaces),
            Some(std::net::Ipv4Addr::new(10, 0, 0, 23))
        );
    }

    #[test]
    fn choose_best_ipv4_falls_back_to_loopback() {
        let interfaces = vec![iface("lo", [127, 0, 0, 1]), iface("docker0", [0, 0, 0, 0])];
        assert_eq!(
            choose_best_ipv4(&interfaces),
            Some(std::net::Ipv4Addr::new(127, 0, 0, 1))
        );
    }

    #[tokio::test]
    async fn test_get_endpoint_addr_returns_ipv4() {
        // verify that get_endpoint_addr also returns IPv4 only
        // create a test endpoint
        let (endpoint, _fingerprint) = endpoint_or_skip();
        let addr = get_endpoint_addr(&endpoint).expect("should get endpoint address");
        assert!(
            addr.is_ipv4(),
            "get_endpoint_addr must return IPv4 address (got {addr})"
        );
    }

    #[test]
    fn test_get_local_ip_with_explicit_ipv4() {
        // test that providing a valid IPv4 address works
        let result = get_local_ip(Some("192.168.1.100"));
        assert!(result.is_ok(), "should accept valid IPv4 address");
        let ip = result.unwrap();
        assert_eq!(
            ip,
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 168, 1, 100))
        );
    }

    #[test]
    fn test_get_local_ip_with_explicit_loopback() {
        // test that providing loopback address works
        let result = get_local_ip(Some("127.0.0.1"));
        assert!(result.is_ok(), "should accept loopback address");
        let ip = result.unwrap();
        assert_eq!(
            ip,
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))
        );
    }

    #[test]
    fn test_get_local_ip_rejects_ipv6() {
        // test that providing an IPv6 address fails with a good error message
        let result = get_local_ip(Some("::1"));
        assert!(result.is_err(), "should reject IPv6 address");
        let err = result.unwrap_err();
        let err_msg = format!("{err:#}");
        assert!(
            err_msg.contains("IPv6 address not supported"),
            "error should mention IPv6 not supported, got: {err_msg}"
        );
        assert!(
            err_msg.contains("0.0.0.0"),
            "error should mention IPv4-only binding, got: {err_msg}"
        );
    }

    #[test]
    fn test_get_local_ip_rejects_ipv6_full() {
        // test that providing a full IPv6 address fails
        let result = get_local_ip(Some("2001:db8::1"));
        assert!(result.is_err(), "should reject IPv6 address");
        let err = result.unwrap_err();
        let err_msg = format!("{err:#}");
        assert!(
            err_msg.contains("IPv6 address not supported"),
            "error should mention IPv6 not supported, got: {err_msg}"
        );
    }

    #[test]
    fn test_get_local_ip_rejects_invalid_ip() {
        // test that providing an invalid IP format fails with a good error message
        let result = get_local_ip(Some("not-an-ip"));
        assert!(result.is_err(), "should reject invalid IP format");
        let err = result.unwrap_err();
        let err_msg = format!("{err:#}");
        assert!(
            err_msg.contains("invalid IP address"),
            "error should mention invalid IP address, got: {err_msg}"
        );
    }

    #[test]
    fn test_get_local_ip_rejects_invalid_ipv4() {
        // test that providing an invalid IPv4 format fails
        let result = get_local_ip(Some("999.999.999.999"));
        assert!(result.is_err(), "should reject invalid IPv4 address");
        let err = result.unwrap_err();
        let err_msg = format!("{err:#}");
        assert!(
            err_msg.contains("invalid IP address"),
            "error should mention invalid IP address, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_get_endpoint_addr_with_bind_ip_explicit() {
        // test that get_endpoint_addr_with_bind_ip works with explicit IP
        let (endpoint, _fingerprint) = endpoint_or_skip();
        let addr = get_endpoint_addr_with_bind_ip(&endpoint, Some("127.0.0.1"))
            .expect("should get endpoint address with bind IP");
        assert_eq!(
            addr.ip(),
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))
        );
        // port should come from the endpoint
        assert_eq!(addr.port(), endpoint.local_addr().unwrap().port());
    }

    #[tokio::test]
    async fn test_get_endpoint_addr_with_bind_ip_auto() {
        // test that get_endpoint_addr_with_bind_ip works with auto-detection (None)
        let (endpoint, _fingerprint) = endpoint_or_skip();
        let addr =
            get_endpoint_addr_with_bind_ip(&endpoint, None).expect("should get endpoint address");
        assert!(addr.is_ipv4(), "should return IPv4 address");
        // port should come from the endpoint
        assert_eq!(addr.port(), endpoint.local_addr().unwrap().port());
    }
}

#[cfg(test)]
mod quic_tuning_tests {
    use super::*;
    // helper to create a QuicConfig with defaults
    fn default_quic_config() -> QuicConfig {
        QuicConfig {
            port_ranges: None,
            idle_timeout_sec: 10,
            keep_alive_interval_sec: 1,
            conn_timeout_sec: 15,
            network_profile: NetworkProfile::Datacenter,
            congestion_control: None,
            tuning: QuicTuning::default(),
        }
    }
    // Datacenter profile constants (from apply_quic_tuning implementation)
    // note: network profile constants are now defined at module level (DATACENTER_RECEIVE_WINDOW, etc.)
    // and used both in apply_quic_tuning() and NetworkProfile methods.
    // quinn's TransportConfig doesn't expose getter methods, so we can't directly
    // verify the values that were set. Instead, we test that apply_quic_tuning doesn't
    // panic with various inputs and test the logic of QuicConfig separately.
    #[test]
    fn test_apply_quic_tuning_datacenter_profile_does_not_panic() {
        let config = default_quic_config();
        let mut transport = quinn::TransportConfig::default();
        // should not panic
        apply_quic_tuning(&mut transport, &config);
    }
    #[test]
    fn test_apply_quic_tuning_internet_profile_does_not_panic() {
        let mut config = default_quic_config();
        config.network_profile = NetworkProfile::Internet;
        let mut transport = quinn::TransportConfig::default();
        // should not panic
        apply_quic_tuning(&mut transport, &config);
    }
    #[test]
    fn test_apply_quic_tuning_with_all_overrides_does_not_panic() {
        let mut config = default_quic_config();
        config.tuning = QuicTuning {
            receive_window: Some(64 * 1024 * 1024),
            stream_receive_window: Some(8 * 1024 * 1024),
            send_window: Some(32 * 1024 * 1024),
            initial_rtt_ms: Some(50.0),
            initial_mtu: Some(1400),
            remote_copy_buffer_size: Some(512 * 1024),
        };
        let mut transport = quinn::TransportConfig::default();
        // should not panic
        apply_quic_tuning(&mut transport, &config);
    }
    #[test]
    fn test_apply_quic_tuning_with_partial_overrides_does_not_panic() {
        let mut config = default_quic_config();
        // only override some values
        config.tuning.receive_window = Some(256 * 1024 * 1024);
        config.tuning.initial_rtt_ms = Some(10.0);
        let mut transport = quinn::TransportConfig::default();
        // should not panic
        apply_quic_tuning(&mut transport, &config);
    }
    #[test]
    fn test_apply_quic_tuning_bbr_congestion_control_does_not_panic() {
        let mut config = default_quic_config();
        config.congestion_control = Some(CongestionControl::Bbr);
        let mut transport = quinn::TransportConfig::default();
        // should not panic
        apply_quic_tuning(&mut transport, &config);
    }
    #[test]
    fn test_apply_quic_tuning_cubic_congestion_control_does_not_panic() {
        let mut config = default_quic_config();
        config.congestion_control = Some(CongestionControl::Cubic);
        let mut transport = quinn::TransportConfig::default();
        // should not panic
        apply_quic_tuning(&mut transport, &config);
    }
    #[test]
    fn test_varint_overflow_handling_receive_window_does_not_panic() {
        let mut config = default_quic_config();
        // VarInt max is 2^62 - 1; u64::MAX exceeds this and should trigger the
        // unwrap_or(VarInt::MAX) fallback instead of panicking
        config.tuning.receive_window = Some(u64::MAX);
        let mut transport = quinn::TransportConfig::default();
        // should not panic - the fallback should handle the overflow
        apply_quic_tuning(&mut transport, &config);
    }
    #[test]
    fn test_varint_overflow_handling_stream_receive_window_does_not_panic() {
        let mut config = default_quic_config();
        config.tuning.stream_receive_window = Some(u64::MAX);
        let mut transport = quinn::TransportConfig::default();
        // should not panic - the fallback should handle the overflow
        apply_quic_tuning(&mut transport, &config);
    }
    #[test]
    fn test_varint_at_max_boundary_does_not_panic() {
        let mut config = default_quic_config();
        // VarInt::MAX is 2^62 - 1 = 4611686018427387903
        // test values just at and around the boundary
        config.tuning.receive_window = Some(quinn::VarInt::MAX.into_inner());
        let mut transport = quinn::TransportConfig::default();
        // should not panic
        apply_quic_tuning(&mut transport, &config);
    }
    #[test]
    fn test_varint_just_over_max_does_not_panic() {
        let mut config = default_quic_config();
        // VarInt::MAX + 1 should trigger the overflow handling
        let over_max: u64 = quinn::VarInt::MAX.into_inner() + 1;
        config.tuning.receive_window = Some(over_max);
        let mut transport = quinn::TransportConfig::default();
        // should not panic - fallback to VarInt::MAX
        apply_quic_tuning(&mut transport, &config);
    }
    #[test]
    fn test_sub_millisecond_rtt_does_not_panic() {
        let mut config = default_quic_config();
        // test sub-millisecond RTT (0.3ms = 300 microseconds, typical for datacenter)
        config.tuning.initial_rtt_ms = Some(0.3);
        let mut transport = quinn::TransportConfig::default();
        // should not panic
        apply_quic_tuning(&mut transport, &config);
    }
    #[test]
    fn test_fractional_rtt_precision_does_not_panic() {
        let mut config = default_quic_config();
        // test various fractional millisecond values
        for rtt in [0.1, 0.25, 0.5, 1.5, 10.75, 100.0] {
            config.tuning.initial_rtt_ms = Some(rtt);
            let mut transport = quinn::TransportConfig::default();
            // should not panic
            apply_quic_tuning(&mut transport, &config);
        }
    }
    #[test]
    fn test_datacenter_profile_uses_bbr_by_default() {
        let config = default_quic_config();
        assert_eq!(config.network_profile, NetworkProfile::Datacenter);
        assert_eq!(config.congestion_control, None);
        // effective congestion control should be BBR for datacenter
        assert_eq!(
            config.effective_congestion_control(),
            CongestionControl::Bbr,
            "datacenter profile should default to BBR"
        );
    }
    #[test]
    fn test_internet_profile_uses_cubic_by_default() {
        let mut config = default_quic_config();
        config.network_profile = NetworkProfile::Internet;
        assert_eq!(config.congestion_control, None);
        // effective congestion control should be CUBIC for internet
        assert_eq!(
            config.effective_congestion_control(),
            CongestionControl::Cubic,
            "internet profile should default to CUBIC"
        );
    }
    #[test]
    fn test_congestion_control_override_on_datacenter() {
        let mut config = default_quic_config();
        config.network_profile = NetworkProfile::Datacenter;
        config.congestion_control = Some(CongestionControl::Cubic);
        // explicit override should take precedence
        assert_eq!(
            config.effective_congestion_control(),
            CongestionControl::Cubic,
            "explicit CUBIC should override datacenter's default BBR"
        );
    }
    #[test]
    fn test_congestion_control_override_on_internet() {
        let mut config = default_quic_config();
        config.network_profile = NetworkProfile::Internet;
        config.congestion_control = Some(CongestionControl::Bbr);
        // explicit override should take precedence
        assert_eq!(
            config.effective_congestion_control(),
            CongestionControl::Bbr,
            "explicit BBR should override internet's default CUBIC"
        );
    }
    #[test]
    fn test_network_profile_default_congestion_control() {
        assert_eq!(
            NetworkProfile::Datacenter.default_congestion_control(),
            CongestionControl::Bbr
        );
        assert_eq!(
            NetworkProfile::Internet.default_congestion_control(),
            CongestionControl::Cubic
        );
    }
    #[test]
    fn test_network_profile_display() {
        assert_eq!(format!("{}", NetworkProfile::Datacenter), "datacenter");
        assert_eq!(format!("{}", NetworkProfile::Internet), "internet");
    }
    #[test]
    fn test_congestion_control_display() {
        assert_eq!(format!("{}", CongestionControl::Bbr), "bbr");
        assert_eq!(format!("{}", CongestionControl::Cubic), "cubic");
    }
    #[test]
    fn test_network_profile_from_str() {
        assert_eq!(
            "datacenter".parse::<NetworkProfile>().unwrap(),
            NetworkProfile::Datacenter
        );
        assert_eq!(
            "DATACENTER".parse::<NetworkProfile>().unwrap(),
            NetworkProfile::Datacenter
        );
        assert_eq!(
            "internet".parse::<NetworkProfile>().unwrap(),
            NetworkProfile::Internet
        );
        assert_eq!(
            "INTERNET".parse::<NetworkProfile>().unwrap(),
            NetworkProfile::Internet
        );
        assert!("invalid".parse::<NetworkProfile>().is_err());
    }
    #[test]
    fn test_network_profile_from_str_error_message() {
        let err = "invalid".parse::<NetworkProfile>().unwrap_err();
        assert!(
            err.to_string().contains("invalid network profile"),
            "error should mention invalid profile: {}",
            err
        );
    }
    #[test]
    fn test_congestion_control_from_str() {
        assert_eq!(
            "bbr".parse::<CongestionControl>().unwrap(),
            CongestionControl::Bbr
        );
        assert_eq!(
            "BBR".parse::<CongestionControl>().unwrap(),
            CongestionControl::Bbr
        );
        assert_eq!(
            "cubic".parse::<CongestionControl>().unwrap(),
            CongestionControl::Cubic
        );
        assert_eq!(
            "CUBIC".parse::<CongestionControl>().unwrap(),
            CongestionControl::Cubic
        );
        assert!("invalid".parse::<CongestionControl>().is_err());
    }
    #[test]
    fn test_congestion_control_from_str_error_message() {
        let err = "invalid".parse::<CongestionControl>().unwrap_err();
        assert!(
            err.to_string().contains("invalid congestion control"),
            "error should mention invalid cc: {}",
            err
        );
    }
    #[test]
    fn test_quic_tuning_default() {
        let tuning = QuicTuning::default();
        assert!(tuning.receive_window.is_none());
        assert!(tuning.stream_receive_window.is_none());
        assert!(tuning.send_window.is_none());
        assert!(tuning.initial_rtt_ms.is_none());
        assert!(tuning.initial_mtu.is_none());
    }
    #[test]
    fn test_quic_config_default_values() {
        let config = default_quic_config();
        assert_eq!(config.network_profile, NetworkProfile::Datacenter);
        assert_eq!(config.congestion_control, None);
        assert!(config.tuning.receive_window.is_none());
        assert!(config.tuning.remote_copy_buffer_size.is_none());
    }
    #[test]
    fn test_profile_window_sizes_match_documentation() {
        // verify that the constants match what's documented in docs/quic_performance_tuning.md
        // datacenter: 128 MiB receive, 16 MiB stream, 128 MiB send, 0.3ms RTT
        assert_eq!(DATACENTER_RECEIVE_WINDOW, 128 * 1024 * 1024);
        assert_eq!(DATACENTER_STREAM_RECEIVE_WINDOW, 16 * 1024 * 1024);
        assert_eq!(DATACENTER_SEND_WINDOW, 128 * 1024 * 1024);
        assert_eq!(DATACENTER_INITIAL_RTT_US, 300); // 0.3ms = 300 microseconds
                                                    // internet: 8 MiB receive, 2 MiB stream, 8 MiB send, 100ms RTT
        assert_eq!(INTERNET_RECEIVE_WINDOW, 8 * 1024 * 1024);
        assert_eq!(INTERNET_STREAM_RECEIVE_WINDOW, 2 * 1024 * 1024);
        assert_eq!(INTERNET_SEND_WINDOW, 8 * 1024 * 1024);
        assert_eq!(INTERNET_INITIAL_RTT_US, 100_000); // 100ms = 100,000 microseconds
    }
    #[test]
    fn test_network_profile_default_is_datacenter() {
        // verify that NetworkProfile::default() returns Datacenter as documented
        assert_eq!(NetworkProfile::default(), NetworkProfile::Datacenter);
    }
    #[test]
    fn test_network_profile_default_remote_copy_buffer_size() {
        // datacenter profile should use buffer matching per-stream receive window
        assert_eq!(
            NetworkProfile::Datacenter.default_remote_copy_buffer_size(),
            DATACENTER_STREAM_RECEIVE_WINDOW as usize
        );
        // internet profile should use buffer matching per-stream receive window
        assert_eq!(
            NetworkProfile::Internet.default_remote_copy_buffer_size(),
            INTERNET_STREAM_RECEIVE_WINDOW as usize
        );
    }
    #[test]
    fn test_quic_config_effective_remote_copy_buffer_size_uses_profile_default() {
        // when no override is set, should use profile default
        let config = default_quic_config();
        assert_eq!(config.network_profile, NetworkProfile::Datacenter);
        assert_eq!(
            config.effective_remote_copy_buffer_size(),
            DATACENTER_REMOTE_COPY_BUFFER_SIZE
        );
        // internet profile should return internet default
        let mut internet_config = default_quic_config();
        internet_config.network_profile = NetworkProfile::Internet;
        assert_eq!(
            internet_config.effective_remote_copy_buffer_size(),
            INTERNET_REMOTE_COPY_BUFFER_SIZE
        );
    }
    #[test]
    fn test_quic_config_effective_remote_copy_buffer_size_uses_override() {
        // explicit override should take precedence over profile default
        let mut config = default_quic_config();
        config.tuning.remote_copy_buffer_size = Some(1024 * 1024); // 1 MiB override
        assert_eq!(config.effective_remote_copy_buffer_size(), 1024 * 1024);
        // should work on internet profile too
        let mut internet_config = default_quic_config();
        internet_config.network_profile = NetworkProfile::Internet;
        internet_config.tuning.remote_copy_buffer_size = Some(512 * 1024); // 512 KiB override
        assert_eq!(
            internet_config.effective_remote_copy_buffer_size(),
            512 * 1024
        );
    }
    #[test]
    fn test_remote_copy_buffer_size_constants() {
        // verify buffer size constants match stream receive windows
        assert_eq!(
            DATACENTER_REMOTE_COPY_BUFFER_SIZE,
            DATACENTER_STREAM_RECEIVE_WINDOW as usize
        );
        assert_eq!(
            INTERNET_REMOTE_COPY_BUFFER_SIZE,
            INTERNET_STREAM_RECEIVE_WINDOW as usize
        );
    }
}
