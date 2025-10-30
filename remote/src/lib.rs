//! Remote copy protocol and networking for distributed file operations
//!
//! This crate provides the networking layer and protocol definitions for remote file copying
//! in the RCP tools suite. It enables efficient distributed copying between remote hosts using
//! SSH for orchestration and QUIC for high-performance data transfer.
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
//! This design ensures efficient data transfer while allowing the Master to coordinate
//! operations and monitor progress.
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
//! The [`port_ranges`] module allows restricting QUIC to specific port ranges, useful for
//! firewall-restricted environments:
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
//! The remote copy system implements a defense-in-depth security model using SSH for authentication
//! and certificate pinning for QUIC connection integrity. This provides protection against
//! man-in-the-middle (MITM) attacks while maintaining ease of deployment.
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
//!     master_cert_fingerprint: Vec::new(),
//! };
//! let master_addr: SocketAddr = "192.168.1.100:5000".parse()?;
//! let server_name = "master-server";
//!
//! let process = start_rcpd(&config, &session, &master_addr, server_name, None).await?;
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

use anyhow::{anyhow, Context};
use rand::Rng;
use tracing::instrument;

pub mod port_ranges;
pub mod protocol;
pub mod streams;
pub mod tracelog;

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
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            port_ranges: None,
            idle_timeout_sec: 10,
            keep_alive_interval_sec: 1,
            conn_timeout_sec: 15,
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
        }
    }

    /// Set port ranges
    pub fn with_port_ranges(mut self, ranges: impl Into<String>) -> Self {
        self.port_ranges = Some(ranges.into());
        self
    }
}

#[derive(Debug, PartialEq)]
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
fn shell_escape(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
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

/// Discover rcpd binary on remote host
///
/// Searches in the following order:
/// 1. Explicit path (if provided)
/// 2. Same directory as local rcp binary
/// 3. PATH (via `which rcpd`)
///
/// Returns the path to rcpd if found, otherwise an error
async fn discover_rcpd_path(
    session: &std::sync::Arc<openssh::Session>,
    explicit_path: Option<&str>,
) -> anyhow::Result<String> {
    let local_version = common::version::ProtocolVersion::current();

    // try explicit path first
    if let Some(path) = explicit_path {
        tracing::debug!("Trying explicit rcpd path: {}", path);
        let output = session
            .command("sh")
            .arg("-c")
            .arg(format!("test -x {}", shell_escape(path)))
            .output()
            .await?;
        if output.status.success() {
            tracing::info!("Found rcpd at explicit path: {}", path);
            return Ok(path.to_string());
        }
        tracing::warn!("Explicit rcpd path not found or not executable: {}", path);
    }

    // try same directory as local rcp binary
    if let Ok(current_exe) = std::env::current_exe() {
        if let Some(bin_dir) = current_exe.parent() {
            let path = bin_dir.join("rcpd").display().to_string();
            tracing::debug!("Trying same directory as rcp: {}", path);
            let output = session
                .command("sh")
                .arg("-c")
                .arg(format!("test -x {}", shell_escape(&path)))
                .output()
                .await?;
            if output.status.success() {
                tracing::info!("Found rcpd in same directory as rcp: {}", path);
                return Ok(path);
            }
        }
    }

    // try PATH
    tracing::debug!("Trying to find rcpd in PATH");
    let output = session.command("which").arg("rcpd").output().await?;
    if output.status.success() {
        let path = String::from_utf8_lossy(&output.stdout);
        let path = path.trim();
        if !path.is_empty() {
            tracing::info!("Found rcpd in PATH: {}", path);
            return Ok(path.to_string());
        }
    }

    // build error message with what we searched
    let mut searched = vec![
        "- Same directory as local rcp binary".to_string(),
        "- PATH (via 'which rcpd')".to_string(),
    ];
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
        Please install rcpd on the remote host and ensure it's in PATH:\n\
        - cargo install rcp-tools-rcp --version {}\n\
        Or specify the path explicitly:\n\
        - rcp --rcpd-path=/path/to/rcpd ...",
        searched.join("\n"),
        local_version.semantic
    ))
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

#[instrument]
pub async fn start_rcpd(
    rcpd_config: &protocol::RcpdConfig,
    session: &SshSession,
    master_addr: &std::net::SocketAddr,
    master_server_name: &str,
    explicit_rcpd_path: Option<&str>,
) -> anyhow::Result<openssh::Child<std::sync::Arc<openssh::Session>>> {
    tracing::info!("Starting rcpd server on: {:?}", session);
    let remote_host = &session.host;
    let ssh_session = setup_ssh_session(session).await?;

    // discover rcpd binary on remote host
    let rcpd_path = discover_rcpd_path(&ssh_session, explicit_rcpd_path).await?;

    // check version compatibility
    check_rcpd_version(&ssh_session, &rcpd_path, remote_host).await?;

    // run rcpd command remotely
    let rcpd_args = rcpd_config.to_args();
    tracing::debug!("rcpd arguments: {:?}", rcpd_args);
    let mut cmd = ssh_session.arc_command(&rcpd_path);
    cmd.arg("--master-addr")
        .arg(master_addr.to_string())
        .arg("--server-name")
        .arg(master_server_name)
        .args(rcpd_args);
    // capture stdout and stderr so we can read them later
    cmd.stdout(openssh::Stdio::piped());
    cmd.stderr(openssh::Stdio::piped());
    tracing::info!("Will run remotely: {cmd:?}");
    cmd.spawn().await.context("Failed to spawn rcpd command")
}

/// Compute SHA-256 fingerprint of a DER-encoded certificate
fn compute_cert_fingerprint(cert_der: &[u8]) -> ring::digest::Digest {
    ring::digest::digest(&ring::digest::SHA256, cert_der)
}

/// Configure QUIC server with a self-signed certificate
/// Returns the server config and the SHA-256 fingerprint of the certificate
fn configure_server(
    idle_timeout_sec: u64,
    keep_alive_interval_sec: u64,
) -> anyhow::Result<(quinn::ServerConfig, Vec<u8>)> {
    tracing::info!(
        "Configuring QUIC server (idle_timeout={}s, keep_alive={}s)",
        idle_timeout_sec,
        keep_alive_interval_sec
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
    // configure transport timeouts for connection liveness detection
    let mut transport_config = quinn::TransportConfig::default();
    transport_config.max_idle_timeout(Some(
        std::time::Duration::from_secs(idle_timeout_sec)
            .try_into()
            .context("Failed to convert idle timeout to VarInt")?,
    ));
    transport_config.keep_alive_interval(Some(std::time::Duration::from_secs(
        keep_alive_interval_sec,
    )));
    server_config.transport_config(std::sync::Arc::new(transport_config));
    Ok((server_config, fingerprint_vec))
}

#[instrument]
pub fn get_server_with_port_ranges(
    port_ranges: Option<&str>,
    idle_timeout_sec: u64,
    keep_alive_interval_sec: u64,
) -> anyhow::Result<(quinn::Endpoint, Vec<u8>)> {
    let (server_config, cert_fingerprint) =
        configure_server(idle_timeout_sec, keep_alive_interval_sec)?;
    let socket = if let Some(ranges_str) = port_ranges {
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

fn get_local_ip() -> anyhow::Result<std::net::IpAddr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;
    Ok(socket.local_addr()?.ip())
}

#[instrument]
pub fn get_endpoint_addr(endpoint: &quinn::Endpoint) -> anyhow::Result<std::net::SocketAddr> {
    // endpoint is bound to 0.0.0.0 so we need to get the local IP address
    let local_ip = get_local_ip().context("Failed to get local IP address")?;
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

#[instrument]
pub fn get_client_with_port_ranges_and_pinning(
    port_ranges: Option<&str>,
    cert_fingerprint: Vec<u8>,
    idle_timeout_sec: u64,
    keep_alive_interval_sec: u64,
) -> anyhow::Result<quinn::Endpoint> {
    tracing::info!(
        "Creating QUIC client with certificate pinning (fingerprint: {}, idle_timeout={}s, keep_alive={}s)",
        hex::encode(&cert_fingerprint),
        idle_timeout_sec,
        keep_alive_interval_sec
    );
    // create a crypto backend with certificate pinning
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(std::sync::Arc::new(PinnedCertVerifier::new(
            cert_fingerprint,
        )))
        .with_no_client_auth();
    create_client_endpoint(
        port_ranges,
        crypto,
        idle_timeout_sec,
        keep_alive_interval_sec,
    )
}

// helper function to create client endpoint with given crypto config
fn create_client_endpoint(
    port_ranges: Option<&str>,
    crypto: rustls::ClientConfig,
    idle_timeout_sec: u64,
    keep_alive_interval_sec: u64,
) -> anyhow::Result<quinn::Endpoint> {
    // create QUIC client config with timeouts
    let mut client_config = quinn::ClientConfig::new(std::sync::Arc::new(crypto));
    let mut transport_config = quinn::TransportConfig::default();
    transport_config.max_idle_timeout(Some(
        std::time::Duration::from_secs(idle_timeout_sec)
            .try_into()
            .context("Failed to convert idle timeout to VarInt")?,
    ));
    transport_config.keep_alive_interval(Some(std::time::Duration::from_secs(
        keep_alive_interval_sec,
    )));
    client_config.transport_config(std::sync::Arc::new(transport_config));
    let socket = if let Some(ranges_str) = port_ranges {
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
