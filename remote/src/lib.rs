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
//! - [`get_client`] - Create QUIC client endpoint
//! - [`get_endpoint_addr`] - Get the local address of an endpoint
//!
//! ## Port Range Configuration
//!
//! The [`port_ranges`] module allows restricting QUIC to specific port ranges, useful for
//! firewall-restricted environments:
//!
//! ```rust,no_run
//! # use remote::get_server_with_port_ranges;
//! // Bind to ports in the 8000-8999 range
//! let endpoint = get_server_with_port_ranges(Some("8000-8999"))?;
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
//! # Security Considerations
//!
//! - **SSH Authentication**: Uses SSH for initial authentication and authorization
//! - **Self-Signed Certificates**: QUIC connections use self-signed certificates (trusted implicitly)
//! - **No Certificate Validation**: Accepts any server certificate (development mode)
//!
//! ⚠️ **Note**: The current implementation prioritizes ease of deployment over strict security.
//! For production use in untrusted networks, consider enhancing certificate validation.
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
//! let config = RcpdConfig::default();
//! let master_addr: SocketAddr = "192.168.1.100:5000".parse()?;
//! let server_name = "master-server";
//!
//! let process = start_rcpd(&config, &session, &master_addr, server_name).await?;
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
//! // Create server restricted to ports 8000-8999
//! let endpoint = get_server_with_port_ranges(Some("8000-8999"))?;
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

#[instrument]
pub async fn start_rcpd(
    rcpd_config: &protocol::RcpdConfig,
    session: &SshSession,
    master_addr: &std::net::SocketAddr,
    master_server_name: &str,
) -> anyhow::Result<openssh::Child<std::sync::Arc<openssh::Session>>> {
    tracing::info!("Starting rcpd server on: {:?}", session);
    let session = setup_ssh_session(session).await?;
    // Run rcpd command remotely
    let current_exe = std::env::current_exe().context("Failed to get current executable path")?;
    let bin_dir = current_exe
        .parent()
        .context("Failed to get parent directory of current executable")?;
    tracing::debug!("Running rcpd from: {:?}", bin_dir);
    let rcpd_args = rcpd_config.to_args();
    tracing::debug!("rcpd arguments: {:?}", rcpd_args);
    let mut cmd = session.arc_command(format!("{}/rcpd", bin_dir.display()));
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

fn configure_server() -> anyhow::Result<quinn::ServerConfig> {
    tracing::info!("Configuring QUIC server");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let key_der = cert.serialize_private_key_der();
    let cert_der = cert.serialize_der()?;
    let key = rustls::PrivateKey(key_der);
    let cert = rustls::Certificate(cert_der);
    let server_config = quinn::ServerConfig::with_single_cert(vec![cert], key)
        .context("Failed to create server config")?;
    Ok(server_config)
}

#[instrument]
pub fn get_server_with_port_ranges(port_ranges: Option<&str>) -> anyhow::Result<quinn::Endpoint> {
    let server_config = configure_server()?;
    let socket = if let Some(ranges_str) = port_ranges {
        let ranges = port_ranges::PortRanges::parse(ranges_str)?;
        ranges.bind_udp_socket(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED))?
    } else {
        // default behavior: bind to any available port
        std::net::UdpSocket::bind("0.0.0.0:0")?
    };
    quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        Some(server_config),
        socket,
        std::sync::Arc::new(quinn::TokioRuntime),
    )
    .context("Failed to create QUIC endpoint")
}

// certificate verifier that accepts any server certificate
struct AcceptAnyCertificate;

impl rustls::client::ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
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
pub fn get_client() -> anyhow::Result<quinn::Endpoint> {
    get_client_with_port_ranges(None)
}

#[instrument]
pub fn get_client_with_port_ranges(port_ranges: Option<&str>) -> anyhow::Result<quinn::Endpoint> {
    // create a crypto backend that accepts any server certificate (for development only)
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(std::sync::Arc::new(AcceptAnyCertificate))
        .with_no_client_auth();
    // create QUIC client config
    let client_config = quinn::ClientConfig::new(std::sync::Arc::new(crypto));
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
