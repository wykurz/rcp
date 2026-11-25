//! OpenSSH multiplexing port forwarding (`-L`/`-R`) and SSH config resolution for SSH data transport.
//!
//! Talks directly to an existing OpenSSH `ControlMaster` Unix domain socket using the OpenSSH
//! multiplex protocol v4 (`MUX_C_OPEN_FWD` / `MUX_C_CLOSE_FWD`), supporting both local (`-L`) and
//! dynamically-allocated remote (`-R 127.0.0.1:0:...` returning `MUX_S_REMOTE_PORT`) port forwards
//! without spawning extra subprocesses or depending on external crates.

use anyhow::Context as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SSHMUX_VER: u32 = 4;
const MUX_MSG_HELLO: u32 = 0x0000_0001;
const MUX_C_OPEN_FWD: u32 = 0x1000_0006;
const MUX_C_CLOSE_FWD: u32 = 0x1000_0007;
const MUX_S_OK: u32 = 0x8000_0001;
const MUX_S_PERMISSION_DENIED: u32 = 0x8000_0002;
const MUX_S_FAILURE: u32 = 0x8000_0003;
const MUX_S_REMOTE_PORT: u32 = 0x8000_0007;
const MUX_FWD_LOCAL: u32 = 1;
const MUX_FWD_REMOTE: u32 = 2;
const MAX_MUX_PACKET_LEN: usize = 256 * 1024;
const LOCAL_PORT_BIND_ATTEMPTS: usize = 16;
const PRIMARY_FORWARD_CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(1);
// optional parallel tunnels (2..=K) only optimize throughput once tunnel #1 is ready; cap how long
// one stalled or rate-limited extra tunnel can delay copy startup
const EXTRA_TUNNEL_MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Direction of an OpenSSH TCP port forward.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForwardDirection {
    /// Local port forward (`ssh -L 127.0.0.1:<local_port>:<target>`): listens on the SSH client
    /// host and forwards connections through SSH to `target` on the SSH server host.
    Local,
    /// Remote port forward (`ssh -R 127.0.0.1:0:<target>`): asks the SSH server to allocate a
    /// loopback port and forwards connections from the SSH server host back to `target` on the
    /// SSH client host.
    Remote,
}

impl ForwardDirection {
    const fn mux_fwd_type(self) -> u32 {
        match self {
            Self::Local => MUX_FWD_LOCAL,
            Self::Remote => MUX_FWD_REMOTE,
        }
    }
}

/// Effective SSH connection settings resolved locally via `ssh -G`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedSshConfig {
    /// Effective `(user, host, port)` suitable for passing to a remote host that does not share
    /// the master's `~/.ssh/config` aliases.
    pub target: crate::protocol::SshTarget,
    /// True when the local `~/.ssh/config` enables `ForwardAgent` (any value other than `no`).
    pub forward_agent_enabled: bool,
}

/// Parse the `key value` output of `ssh -G` into a [`ResolvedSshConfig`].
pub(crate) fn parse_ssh_g_output(
    output: &str,
    fallback: &crate::SshSession,
) -> anyhow::Result<ResolvedSshConfig> {
    let mut hostname: Option<String> = None;
    let mut user: Option<String> = None;
    let mut port: Option<u16> = None;
    let mut forward_agent_enabled = false;
    let mut saw_forward_agent = false;
    for raw_line in output.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once(char::is_whitespace) else {
            continue;
        };
        let value = value.trim();
        if key.eq_ignore_ascii_case("hostname") && hostname.is_none() && !value.is_empty() {
            hostname = Some(value.to_owned());
        } else if key.eq_ignore_ascii_case("user") && user.is_none() && !value.is_empty() {
            user = Some(value.to_owned());
        } else if key.eq_ignore_ascii_case("port") && port.is_none() && !value.is_empty() {
            let parsed = value
                .parse::<u16>()
                .with_context(|| format!("invalid port in `ssh -G` output: {value}"))?;
            if parsed > 0 {
                port = Some(parsed);
            }
        } else if key.eq_ignore_ascii_case("forwardagent") && !saw_forward_agent {
            saw_forward_agent = true;
            forward_agent_enabled = !value.eq_ignore_ascii_case("no")
                && !value.eq_ignore_ascii_case("none")
                && !value.eq_ignore_ascii_case("false");
        }
    }
    let host = hostname.unwrap_or_else(|| fallback.openssh_host().to_owned());
    let target = crate::protocol::SshTarget {
        user: user.or_else(|| fallback.user.clone()),
        host,
        port: port.or(fallback.port),
    };
    Ok(ResolvedSshConfig {
        target,
        forward_agent_enabled,
    })
}

/// Resolve a session's effective SSH target and `ForwardAgent` setting locally using `ssh -G`.
///
/// Runs purely locally without opening any network connection.
pub async fn resolve_ssh_config(
    session: &crate::SshSession,
    timeout: std::time::Duration,
    cleanup: &crate::RemoteCleanup,
) -> anyhow::Result<ResolvedSshConfig> {
    resolve_ssh_config_with_program(session, std::path::Path::new("ssh"), timeout, cleanup).await
}

async fn resolve_ssh_config_with_program(
    session: &crate::SshSession,
    ssh_program: &std::path::Path,
    timeout: std::time::Duration,
    cleanup: &crate::RemoteCleanup,
) -> anyhow::Result<ResolvedSshConfig> {
    let mut cmd = crate::ssh_master_launcher_command(ssh_program);
    cmd.kill_on_drop(true)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .arg("-G");
    if let Some(port) = session.port {
        cmd.arg("-p").arg(port.to_string());
    }
    if let Some(user) = session.user.as_deref() {
        cmd.arg("-l").arg(user);
    }
    cmd.arg("--").arg(session.openssh_host());
    let raw_child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn `ssh -G` for '{}'", session.host))?;
    let mut child = crate::ManagedChildProcess::new(raw_child, cleanup.clone());
    let stdout = child
        .child_mut()
        .stdout
        .take()
        .context("`ssh -G` stdout was not piped")?;
    let stderr = child
        .child_mut()
        .stderr
        .take()
        .context("`ssh -G` stderr was not piped")?;
    let output = tokio::time::timeout(timeout, async {
        let (status, stdout, stderr) = tokio::join!(
            child.wait(),
            crate::drain_bounded_output(stdout, crate::DIAGNOSTIC_CAPTURE_LIMIT),
            crate::drain_bounded_output(stderr, crate::DIAGNOSTIC_CAPTURE_LIMIT),
        );
        let status = status.context("failed to wait for `ssh -G`")?;
        anyhow::Ok((status, stdout, stderr))
    })
    .await
    .with_context(|| {
        format!(
            "`ssh -G` for '{}' timed out after {}",
            session.host,
            humantime::format_duration(timeout)
        )
    })??;
    let (status, stdout, stderr) = output;
    if !status.success() {
        anyhow::bail!(
            "`ssh -G` for '{}' failed with {status}: {}",
            session.host,
            stderr.rendered().trim()
        );
    }
    let stdout_bytes = crate::finish_remote_output_capture("`ssh -G`", "stdout", stdout, true)?;
    let stdout = String::from_utf8(stdout_bytes)
        .with_context(|| format!("`ssh -G` output for '{}' was not valid UTF-8", session.host))?;
    parse_ssh_g_output(&stdout, session)
}

fn write_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_be_bytes());
}

fn write_ssh_string(buf: &mut Vec<u8>, value: &[u8]) {
    let len = u32::try_from(value.len()).expect("SSH mux string length fits u32");
    write_u32(buf, len);
    buf.extend_from_slice(value);
}

fn write_tcp_endpoint(buf: &mut Vec<u8>, host: &str, port: u32) {
    write_ssh_string(buf, host.as_bytes());
    write_u32(buf, port);
}

fn read_u32(input: &mut &[u8]) -> anyhow::Result<u32> {
    if input.len() < 4 {
        anyhow::bail!("truncated u32 in OpenSSH mux packet");
    }
    let (head, rest) = input.split_at(4);
    *input = rest;
    Ok(u32::from_be_bytes(head.try_into().unwrap()))
}

fn read_ssh_string(input: &mut &[u8]) -> anyhow::Result<String> {
    let len = read_u32(input)? as usize;
    if input.len() < len {
        anyhow::bail!("truncated string in OpenSSH mux packet (expected {len} bytes)");
    }
    let (head, rest) = input.split_at(len);
    *input = rest;
    Ok(String::from_utf8_lossy(head).into_owned())
}

fn frame_mux_payload(payload: &[u8]) -> Vec<u8> {
    let len = u32::try_from(payload.len()).expect("SSH mux packet length fits u32");
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(payload);
    framed
}

fn encode_hello_packet() -> Vec<u8> {
    let mut payload = Vec::with_capacity(8);
    write_u32(&mut payload, MUX_MSG_HELLO);
    write_u32(&mut payload, SSHMUX_VER);
    frame_mux_payload(&payload)
}

fn decode_hello_packet(mut payload: &[u8]) -> anyhow::Result<u32> {
    let msg_type = read_u32(&mut payload)?;
    if msg_type != MUX_MSG_HELLO {
        anyhow::bail!("expected MUX_MSG_HELLO ({MUX_MSG_HELLO:#x}), got {msg_type:#x}");
    }
    let version = read_u32(&mut payload)?;
    if version != SSHMUX_VER {
        anyhow::bail!("unsupported OpenSSH mux protocol version {version} (expected {SSHMUX_VER})");
    }
    Ok(version)
}

fn encode_fwd_packet(
    msg_type: u32,
    request_id: u32,
    direction: ForwardDirection,
    listen_host: &str,
    listen_port: u32,
    connect_host: &str,
    connect_port: u32,
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(64);
    write_u32(&mut payload, msg_type);
    write_u32(&mut payload, request_id);
    write_u32(&mut payload, direction.mux_fwd_type());
    write_tcp_endpoint(&mut payload, listen_host, listen_port);
    write_tcp_endpoint(&mut payload, connect_host, connect_port);
    frame_mux_payload(&payload)
}

#[derive(Debug, PartialEq, Eq)]
enum MuxFwdReply {
    Ok,
    RemotePort(u16),
    PermissionDenied(String),
    Failure(String),
}

fn decode_fwd_reply(mut payload: &[u8], expected_request_id: u32) -> anyhow::Result<MuxFwdReply> {
    let msg_type = read_u32(&mut payload)?;
    let request_id = read_u32(&mut payload)?;
    if request_id != expected_request_id {
        anyhow::bail!(
            "mismatched OpenSSH mux reply request_id (expected {expected_request_id}, got {request_id})"
        );
    }
    match msg_type {
        MUX_S_OK => Ok(MuxFwdReply::Ok),
        MUX_S_REMOTE_PORT => {
            let port = read_u32(&mut payload)?;
            let port = u16::try_from(port)
                .ok()
                .filter(|p| *p != 0)
                .with_context(|| format!("invalid remote port allocated by sshd: {port}"))?;
            Ok(MuxFwdReply::RemotePort(port))
        }
        MUX_S_PERMISSION_DENIED => Ok(MuxFwdReply::PermissionDenied(read_ssh_string(
            &mut payload,
        )?)),
        MUX_S_FAILURE => Ok(MuxFwdReply::Failure(read_ssh_string(&mut payload)?)),
        other => anyhow::bail!("unexpected OpenSSH mux reply type {other:#x}"),
    }
}

struct MuxClient<S> {
    stream: S,
    next_request_id: u32,
}

impl MuxClient<tokio::net::UnixStream> {
    async fn connect(control_path: &std::path::Path) -> anyhow::Result<Self> {
        let stream = tokio::net::UnixStream::connect(control_path)
            .await
            .with_context(|| {
                format!(
                    "failed to connect to SSH control socket {}",
                    control_path.display()
                )
            })?;
        Self::handshake(stream).await
    }
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> MuxClient<S> {
    async fn handshake(stream: S) -> anyhow::Result<Self> {
        let mut client = Self {
            stream,
            next_request_id: 1,
        };
        let hello = encode_hello_packet();
        client
            .stream
            .write_all(&hello)
            .await
            .context("failed to write OpenSSH mux HELLO")?;
        client
            .stream
            .flush()
            .await
            .context("failed to flush OpenSSH mux HELLO")?;
        let reply = client.read_packet().await?;
        decode_hello_packet(&reply)?;
        Ok(client)
    }

    async fn read_packet(&mut self) -> anyhow::Result<Vec<u8>> {
        let mut len_buf = [0u8; 4];
        self.stream
            .read_exact(&mut len_buf)
            .await
            .context("failed to read OpenSSH mux packet length")?;
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 || len > MAX_MUX_PACKET_LEN {
            anyhow::bail!("invalid OpenSSH mux packet length: {len}");
        }
        let mut payload = vec![0u8; len];
        self.stream
            .read_exact(&mut payload)
            .await
            .context("failed to read OpenSSH mux packet payload")?;
        Ok(payload)
    }

    fn alloc_request_id(&mut self) -> u32 {
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        id
    }

    async fn send_fwd_request(
        &mut self,
        msg_type: u32,
        direction: ForwardDirection,
        listen_host: &str,
        listen_port: u32,
        target: std::net::SocketAddr,
    ) -> anyhow::Result<MuxFwdReply> {
        let request_id = self.alloc_request_id();
        let target_host = target.ip().to_string();
        let packet = encode_fwd_packet(
            msg_type,
            request_id,
            direction,
            listen_host,
            listen_port,
            &target_host,
            u32::from(target.port()),
        );
        self.stream
            .write_all(&packet)
            .await
            .context("failed to send OpenSSH mux forwarding request")?;
        self.stream
            .flush()
            .await
            .context("failed to flush OpenSSH mux forwarding request")?;
        let reply = self.read_packet().await?;
        decode_fwd_reply(&reply, request_id)
    }

    async fn open_remote_forward(
        &mut self,
        target: std::net::SocketAddr,
    ) -> anyhow::Result<std::net::SocketAddr> {
        match self
            .send_fwd_request(
                MUX_C_OPEN_FWD,
                ForwardDirection::Remote,
                "127.0.0.1",
                0,
                target,
            )
            .await?
        {
            MuxFwdReply::RemotePort(port) => Ok(std::net::SocketAddr::from((
                std::net::Ipv4Addr::LOCALHOST,
                port,
            ))),
            MuxFwdReply::Ok => {
                anyhow::bail!("sshd returned MUX_S_OK without an allocated remote port")
            }
            MuxFwdReply::PermissionDenied(reason) => anyhow::bail!(
                "remote SSH server denied reverse port forwarding (-R) to {target}: {reason}. \
                 Ensure `AllowTcpForwarding` is enabled (yes or remote) in sshd_config on the remote host."
            ),
            MuxFwdReply::Failure(reason) => anyhow::bail!(
                "remote SSH server failed to open reverse port forward (-R) to {target}: {reason}. \
                 Ensure `AllowTcpForwarding` is enabled (yes or remote) in sshd_config on the remote host."
            ),
        }
    }

    async fn open_local_forward(
        &mut self,
        target: std::net::SocketAddr,
    ) -> anyhow::Result<std::net::SocketAddr> {
        let mut last_failure = None;
        for _ in 0..LOCAL_PORT_BIND_ATTEMPTS {
            let probe = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .context("failed to reserve an ephemeral loopback port for SSH -L forwarding")?;
            let port = probe
                .local_addr()
                .context("failed to inspect reserved loopback port")?
                .port();
            drop(probe);
            match self
                .send_fwd_request(
                    MUX_C_OPEN_FWD,
                    ForwardDirection::Local,
                    "127.0.0.1",
                    u32::from(port),
                    target,
                )
                .await?
            {
                MuxFwdReply::Ok => {
                    return Ok(std::net::SocketAddr::from((
                        std::net::Ipv4Addr::LOCALHOST,
                        port,
                    )));
                }
                MuxFwdReply::RemotePort(_) => {
                    anyhow::bail!("unexpected MUX_S_REMOTE_PORT reply to local (-L) forward")
                }
                MuxFwdReply::PermissionDenied(reason) => anyhow::bail!(
                    "SSH multiplex master denied local port forwarding (-L) to {target}: {reason}"
                ),
                MuxFwdReply::Failure(reason) => {
                    tracing::debug!(
                        port,
                        "SSH -L bind collision or failure on 127.0.0.1:{port}: {reason}; retrying"
                    );
                    last_failure = Some(reason);
                }
            }
        }
        anyhow::bail!(
            "failed to open local SSH port forward (-L) to {target} after {LOCAL_PORT_BIND_ATTEMPTS} attempts: {}",
            last_failure.unwrap_or_else(|| "unknown mux error".to_owned())
        )
    }

    async fn open_forward(
        &mut self,
        direction: ForwardDirection,
        target: std::net::SocketAddr,
    ) -> anyhow::Result<std::net::SocketAddr> {
        match direction {
            ForwardDirection::Local => self.open_local_forward(target).await,
            ForwardDirection::Remote => self.open_remote_forward(target).await,
        }
    }

    async fn close_forward(
        &mut self,
        direction: ForwardDirection,
        listen_addr: std::net::SocketAddr,
        target: std::net::SocketAddr,
    ) -> anyhow::Result<()> {
        match self
            .send_fwd_request(
                MUX_C_CLOSE_FWD,
                direction,
                "127.0.0.1",
                u32::from(listen_addr.port()),
                target,
            )
            .await?
        {
            MuxFwdReply::Ok => Ok(()),
            MuxFwdReply::RemotePort(_) => Ok(()),
            MuxFwdReply::PermissionDenied(reason) | MuxFwdReply::Failure(reason) => {
                anyhow::bail!(
                    "failed to close SSH port forward {listen_addr} -> {target}: {reason}"
                )
            }
        }
    }
}

#[derive(Debug)]
struct PrimaryForward {
    control_path: std::path::PathBuf,
    direction: ForwardDirection,
    listen_addr: std::net::SocketAddr,
    target_addr: std::net::SocketAddr,
}

/// Active set of 1..=K SSH port forwards carrying source↔destination control and data traffic.
///
/// Dropping this value releases any owned SSH sessions through [`crate::RemoteCleanup`]. Prefer
/// calling [`Self::close`] on graceful teardown so port forwards opened on a shared primary
/// control socket are also closed politely via `MUX_C_CLOSE_FWD`.
pub struct SshTunnelSet {
    control_addr: std::net::SocketAddr,
    data_addrs: Vec<std::net::SocketAddr>,
    requested_tunnels: usize,
    primary_forwards: Vec<PrimaryForward>,
    owned_sessions: Vec<crate::ManagedSshSession>,
}

impl std::fmt::Debug for SshTunnelSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SshTunnelSet")
            .field("control_addr", &self.control_addr)
            .field("data_addrs", &self.data_addrs)
            .field("requested_tunnels", &self.requested_tunnels)
            .field("owned_sessions", &self.owned_sessions.len())
            .finish()
    }
}

impl SshTunnelSet {
    /// Forwarded loopback address for the source↔destination control stream.
    #[must_use]
    pub fn control_addr(&self) -> std::net::SocketAddr {
        self.control_addr
    }

    /// Forwarded loopback addresses for pooled data streams (one per active SSH tunnel, or empty
    /// for `--dry-run`).
    #[must_use]
    pub fn data_addrs(&self) -> &[std::net::SocketAddr] {
        &self.data_addrs
    }

    /// Number of active independent SSH tunnel connections backing this set (always >= 1).
    #[must_use]
    fn tunnel_count(&self) -> usize {
        self.data_addrs.len().max(1)
    }

    /// Human-readable tunnel count description for slow-path notices.
    #[must_use]
    pub fn format_tunnel_summary(&self) -> String {
        let active = self.tunnel_count();
        if active < self.requested_tunnels {
            format!(
                "{active} of {} requested parallel tunnels",
                self.requested_tunnels
            )
        } else if active == 1 {
            "1 tunnel".to_owned()
        } else {
            format!("{active} parallel tunnels")
        }
    }

    /// Open `1..=requested_tunnels` port forwards reusing an existing `PreparedRcpd` session as
    /// tunnel #1 and spawning up to `requested_tunnels - 1` extra independent SSH connections in
    /// parallel for data striping.
    ///
    /// Extra tunnels degrade gracefully down to 1 if `sshd` or a firewall rate-limits additional
    /// concurrent SSH connections.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_on_existing_session(
        direction: ForwardDirection,
        primary_control_path: &std::path::Path,
        session: &crate::SshSession,
        target_control: std::net::SocketAddr,
        target_data: Option<std::net::SocketAddr>,
        requested_tunnels: usize,
        keepalive_sec: u64,
        timeout: std::time::Duration,
        cleanup: &crate::RemoteCleanup,
        local_operand_roots: &[std::path::PathBuf],
    ) -> anyhow::Result<Self> {
        let requested_tunnels = if target_data.is_some() {
            requested_tunnels.max(1)
        } else {
            1
        };
        let deadline = checked_deadline(timeout);
        let (control_addr, primary_data_addr, primary_forwards) =
            open_primary_forwards_with_timeout(
                primary_control_path,
                direction,
                target_control,
                target_data,
                timeout,
                true,
            )
            .await?;
        let mut data_addrs = Vec::with_capacity(requested_tunnels);
        if let Some(addr) = primary_data_addr {
            data_addrs.push(addr);
        }
        let mut owned_sessions = Vec::new();
        if let Some(target_data) = target_data
            && requested_tunnels > 1
        {
            let extra = open_extra_data_tunnels(
                direction,
                session,
                target_data,
                requested_tunnels - 1,
                keepalive_sec,
                deadline,
                cleanup,
                local_operand_roots,
            )
            .await;
            for (extra_session, addr) in extra {
                owned_sessions.push(extra_session);
                data_addrs.push(addr);
            }
        }
        Ok(Self {
            control_addr,
            data_addrs,
            requested_tunnels,
            primary_forwards,
            owned_sessions,
        })
    }

    /// Open `1..=requested_tunnels` direct `-L` tunnels from a destination `rcpd` directly to the
    /// source host (3-host mode, zero master relay).
    ///
    /// Tunnel #1 is required (failing with actionable 3-host guidance if SSH cannot authenticate
    /// or connect); extra tunnels `2..=K` degrade gracefully down to 1.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_direct_local_tunnels(
        source_ssh: &crate::protocol::SshTarget,
        target_control: std::net::SocketAddr,
        target_data: Option<std::net::SocketAddr>,
        requested_tunnels: usize,
        keepalive_sec: u64,
        timeout: std::time::Duration,
        cleanup: &crate::RemoteCleanup,
        local_operand_roots: &[std::path::PathBuf],
    ) -> anyhow::Result<Self> {
        let requested_tunnels = if target_data.is_some() {
            requested_tunnels.max(1)
        } else {
            1
        };
        let deadline = checked_deadline(timeout);
        let session = crate::SshSession::from(source_ssh);
        let exclusions: std::sync::Arc<[std::path::PathBuf]> = local_operand_roots.to_vec().into();
        let preparation = crate::PreparationContext::uncancelled(cleanup.clone())
            .with_control_directory_exclusions(exclusions);
        let primary_session = crate::setup_ssh_session(
            &session,
            &preparation,
            crate::BootstrapDeadline::new(timeout),
            keepalive_sec,
            crate::AgentForwardMode::Off,
        )
        .await
        .with_context(|| format_three_host_ssh_guidance(&session.host))?;
        let remaining = remaining_budget(deadline);
        let (control_addr, primary_data_addr, _) = open_primary_forwards_with_timeout(
            &primary_session.control_path(),
            ForwardDirection::Local,
            target_control,
            target_data,
            remaining,
            false,
        )
        .await?;
        let mut owned_sessions = Vec::with_capacity(requested_tunnels);
        owned_sessions.push(primary_session);
        let mut data_addrs = Vec::with_capacity(requested_tunnels);
        if let Some(addr) = primary_data_addr {
            data_addrs.push(addr);
        }
        if let Some(target_data) = target_data
            && requested_tunnels > 1
        {
            let extra = open_extra_data_tunnels(
                ForwardDirection::Local,
                &session,
                target_data,
                requested_tunnels - 1,
                keepalive_sec,
                deadline,
                cleanup,
                local_operand_roots,
            )
            .await;
            for (extra_session, addr) in extra {
                owned_sessions.push(extra_session);
                data_addrs.push(addr);
            }
        }
        Ok(Self {
            control_addr,
            data_addrs,
            requested_tunnels,
            primary_forwards: Vec::new(),
            owned_sessions,
        })
    }

    /// Politely close any forwards opened on a caller-owned primary SSH session and tear down
    /// owned tunnel sessions.
    pub async fn close(mut self) {
        let forwards = std::mem::take(&mut self.primary_forwards);
        if !forwards.is_empty() {
            close_primary_forwards_with(forwards, |path| {
                let path = path.to_path_buf();
                async move { MuxClient::connect(&path).await }
            })
            .await;
        }
        drop(self.owned_sessions);
    }
}

fn format_three_host_ssh_guidance(source_host: &str) -> String {
    format!(
        "failed to establish direct SSH tunnel from destination to source '{source_host}'. \
         In 3-host mode rcp does not relay file data through the master host; to fix this:\n\
         - configure passwordless SSH authentication from the destination host to '{source_host}', or\n\
         - pass --ssh-forward-agent (or enable ForwardAgent in ~/.ssh/config for the destination host), or\n\
         - open firewall ports between source and destination and use --port-ranges with direct TCP, or\n\
         - run rcp directly on either the source or destination host"
    )
}

fn checked_deadline(timeout: std::time::Duration) -> Option<tokio::time::Instant> {
    tokio::time::Instant::now().checked_add(timeout)
}

fn remaining_budget(deadline: Option<tokio::time::Instant>) -> std::time::Duration {
    match deadline {
        Some(deadline) => deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .max(std::time::Duration::from_millis(100)),
        None => std::time::Duration::MAX,
    }
}

async fn close_primary_forwards_with<S, F, Fut>(forwards: Vec<PrimaryForward>, connect: F)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: Fn(&std::path::Path) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<MuxClient<S>>>,
{
    let _ = tokio::time::timeout(PRIMARY_FORWARD_CLOSE_GRACE, async {
        for fwd in forwards {
            match connect(&fwd.control_path).await {
                Ok(mut mux) => {
                    if let Err(error) = mux
                        .close_forward(fwd.direction, fwd.listen_addr, fwd.target_addr)
                        .await
                    {
                        tracing::debug!("failed to close SSH port forward cleanly: {error:#}");
                    }
                }
                Err(error) => {
                    tracing::debug!(
                        "failed to connect to SSH control socket to close port forward: {error:#}"
                    );
                }
            }
        }
    })
    .await;
}

async fn open_primary_forwards_with_timeout(
    control_path: &std::path::Path,
    direction: ForwardDirection,
    target_control: std::net::SocketAddr,
    target_data: Option<std::net::SocketAddr>,
    timeout: std::time::Duration,
    record_primary: bool,
) -> anyhow::Result<(
    std::net::SocketAddr,
    Option<std::net::SocketAddr>,
    Vec<PrimaryForward>,
)> {
    open_primary_forwards_with_connector(
        control_path,
        direction,
        target_control,
        target_data,
        timeout,
        record_primary,
        |path| {
            let path = path.to_path_buf();
            async move { MuxClient::connect(&path).await }
        },
    )
    .await
}

async fn open_primary_forwards_with_connector<S, F, Fut>(
    control_path: &std::path::Path,
    direction: ForwardDirection,
    target_control: std::net::SocketAddr,
    target_data: Option<std::net::SocketAddr>,
    timeout: std::time::Duration,
    record_primary: bool,
    connect: F,
) -> anyhow::Result<(
    std::net::SocketAddr,
    Option<std::net::SocketAddr>,
    Vec<PrimaryForward>,
)>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: Fn(&std::path::Path) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<MuxClient<S>>>,
{
    let deadline = checked_deadline(timeout);
    let timeout_context = || {
        format!(
            "timed out opening SSH port forwards after {}",
            humantime::format_duration(timeout)
        )
    };
    let (mut mux, control_addr) = tokio::time::timeout(timeout, async {
        let mut mux = connect(control_path).await?;
        let control_addr = mux.open_forward(direction, target_control).await?;
        anyhow::Ok((mux, control_addr))
    })
    .await
    .with_context(timeout_context)??;
    let mut primary_forwards = Vec::new();
    if record_primary {
        primary_forwards.push(PrimaryForward {
            control_path: control_path.to_path_buf(),
            direction,
            listen_addr: control_addr,
            target_addr: target_control,
        });
    }
    let Some(target_data) = target_data else {
        return Ok((control_addr, None, primary_forwards));
    };
    let remaining = match deadline {
        Some(deadline) => deadline.saturating_duration_since(tokio::time::Instant::now()),
        None => std::time::Duration::MAX,
    };
    let data_result = if remaining.is_zero() {
        Err(anyhow::anyhow!(timeout_context()))
    } else {
        match tokio::time::timeout(remaining, mux.open_forward(direction, target_data)).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(timeout_context())),
        }
    };
    match data_result {
        Ok(addr) => {
            if record_primary {
                primary_forwards.push(PrimaryForward {
                    control_path: control_path.to_path_buf(),
                    direction,
                    listen_addr: addr,
                    target_addr: target_data,
                });
            }
            Ok((control_addr, Some(addr), primary_forwards))
        }
        Err(error) => {
            // drop the possibly-stalled or broken mux stream and roll back the already-opened
            // control forward on a fresh mux connection bounded by PRIMARY_FORWARD_CLOSE_GRACE
            drop(mux);
            if !primary_forwards.is_empty() {
                close_primary_forwards_with(primary_forwards, connect).await;
            }
            Err(error)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn open_extra_data_tunnels(
    direction: ForwardDirection,
    session: &crate::SshSession,
    target_data: std::net::SocketAddr,
    count: usize,
    keepalive_sec: u64,
    deadline: Option<tokio::time::Instant>,
    cleanup: &crate::RemoteCleanup,
    local_operand_roots: &[std::path::PathBuf],
) -> Vec<(crate::ManagedSshSession, std::net::SocketAddr)> {
    let extra_cap = checked_deadline(EXTRA_TUNNEL_MAX_WAIT);
    let extra_deadline = match (deadline, extra_cap) {
        (Some(overall), Some(cap)) => Some(overall.min(cap)),
        (Some(overall), None) => Some(overall),
        (None, Some(cap)) => Some(cap),
        (None, None) => None,
    };
    let exclusions: std::sync::Arc<[std::path::PathBuf]> = local_operand_roots.to_vec().into();
    let futures = (0..count).map(|idx| {
        let session = session.clone();
        let cleanup = cleanup.clone();
        let exclusions = exclusions.clone();
        async move {
            let remaining = match extra_deadline {
                Some(deadline) => deadline.saturating_duration_since(tokio::time::Instant::now()),
                None => EXTRA_TUNNEL_MAX_WAIT,
            };
            if remaining.is_zero() {
                tracing::debug!(
                    tunnel_index = idx + 2,
                    "skipping extra SSH tunnel: setup deadline already elapsed"
                );
                return None;
            }
            let preparation = crate::PreparationContext::uncancelled(cleanup)
                .with_control_directory_exclusions(exclusions);
            let extra_session = match crate::setup_ssh_session(
                &session,
                &preparation,
                crate::BootstrapDeadline::new(remaining),
                keepalive_sec,
                crate::AgentForwardMode::Off,
            )
            .await
            {
                Ok(session) => session,
                Err(error) => {
                    tracing::debug!(
                        tunnel_index = idx + 2,
                        "extra SSH tunnel connection failed (degrading gracefully): {error:#}"
                    );
                    return None;
                }
            };
            let fwd_remaining = match extra_deadline {
                Some(deadline) => deadline.saturating_duration_since(tokio::time::Instant::now()),
                None => EXTRA_TUNNEL_MAX_WAIT,
            };
            if fwd_remaining.is_zero() {
                tracing::debug!(
                    tunnel_index = idx + 2,
                    "skipping extra SSH tunnel forward: setup deadline elapsed"
                );
                return None;
            }
            let control_path = extra_session.control_path();
            match tokio::time::timeout(fwd_remaining, async {
                let mut mux = MuxClient::connect(&control_path).await?;
                mux.open_forward(direction, target_data).await
            })
            .await
            {
                Ok(Ok(addr)) => Some((extra_session, addr)),
                Ok(Err(error)) => {
                    tracing::debug!(
                        tunnel_index = idx + 2,
                        "extra SSH tunnel port forward failed (degrading gracefully): {error:#}"
                    );
                    None
                }
                Err(_) => {
                    tracing::debug!(
                        tunnel_index = idx + 2,
                        "extra SSH tunnel port forward timed out (degrading gracefully)"
                    );
                    None
                }
            }
        }
    });
    futures::future::join_all(futures)
        .await
        .into_iter()
        .flatten()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ssh_g_output_resolves_alias_fields_and_forward_agent() {
        let fallback = crate::SshSession {
            user: None,
            host: "prod-alias".to_owned(),
            port: None,
        };
        let output = "\
user deploy
hostname 10.20.30.40
port 2222
forwardagent yes
";
        let resolved = parse_ssh_g_output(output, &fallback).unwrap();
        assert_eq!(
            resolved,
            ResolvedSshConfig {
                target: crate::protocol::SshTarget {
                    user: Some("deploy".to_owned()),
                    host: "10.20.30.40".to_owned(),
                    port: Some(2222),
                },
                forward_agent_enabled: true,
            }
        );
    }

    #[test]
    fn parse_ssh_g_output_recognizes_forward_agent_socket_path_and_no() {
        let fallback = crate::SshSession::local();
        let with_socket = "hostname 127.0.0.1\nuser alice\nport 22\nforwardagent /tmp/agent.sock\n";
        assert!(
            parse_ssh_g_output(with_socket, &fallback)
                .unwrap()
                .forward_agent_enabled
        );
        for disabled in [
            "hostname 127.0.0.1\nuser alice\nport 22\nforwardagent no\n",
            "hostname 127.0.0.1\nuser alice\nport 22\nforwardagent none\n",
            "hostname 127.0.0.1\nuser alice\nport 22\nforwardagent false\n",
        ] {
            assert!(
                !parse_ssh_g_output(disabled, &fallback)
                    .unwrap()
                    .forward_agent_enabled,
                "ForwardAgent disabled form should parse as false: {disabled:?}"
            );
        }
    }

    #[test]
    fn parse_ssh_g_output_falls_back_when_fields_missing() {
        let fallback = crate::SshSession {
            user: Some("bob".to_owned()),
            host: "[::1]".to_owned(),
            port: Some(2200),
        };
        let resolved = parse_ssh_g_output("", &fallback).unwrap();
        assert_eq!(
            resolved,
            ResolvedSshConfig {
                target: crate::protocol::SshTarget {
                    user: Some("bob".to_owned()),
                    host: "::1".to_owned(),
                    port: Some(2200),
                },
                forward_agent_enabled: false,
            }
        );
    }

    #[test]
    fn mux_hello_round_trip_ignores_server_extensions() {
        let mut payload = Vec::new();
        write_u32(&mut payload, MUX_MSG_HELLO);
        write_u32(&mut payload, SSHMUX_VER);
        write_ssh_string(&mut payload, b"ext-name");
        write_ssh_string(&mut payload, b"ext-val");
        assert_eq!(decode_hello_packet(&payload).unwrap(), SSHMUX_VER);
        let client_framed = encode_hello_packet();
        assert_eq!(client_framed.len(), 12);
        assert_eq!(
            decode_hello_packet(&client_framed[4..]).unwrap(),
            SSHMUX_VER
        );
    }

    #[test]
    fn mux_fwd_reply_decodes_ok_remote_port_and_errors() {
        let mut ok_payload = Vec::new();
        write_u32(&mut ok_payload, MUX_S_OK);
        write_u32(&mut ok_payload, 7);
        assert_eq!(decode_fwd_reply(&ok_payload, 7).unwrap(), MuxFwdReply::Ok);
        let mut port_payload = Vec::new();
        write_u32(&mut port_payload, MUX_S_REMOTE_PORT);
        write_u32(&mut port_payload, 8);
        write_u32(&mut port_payload, 41234);
        assert_eq!(
            decode_fwd_reply(&port_payload, 8).unwrap(),
            MuxFwdReply::RemotePort(41234)
        );
        let mut denied_payload = Vec::new();
        write_u32(&mut denied_payload, MUX_S_PERMISSION_DENIED);
        write_u32(&mut denied_payload, 9);
        write_ssh_string(&mut denied_payload, b"administratively prohibited");
        assert_eq!(
            decode_fwd_reply(&denied_payload, 9).unwrap(),
            MuxFwdReply::PermissionDenied("administratively prohibited".to_owned())
        );
        let mut fail_payload = Vec::new();
        write_u32(&mut fail_payload, MUX_S_FAILURE);
        write_u32(&mut fail_payload, 10);
        write_ssh_string(&mut fail_payload, b"port forwarding failed");
        assert_eq!(
            decode_fwd_reply(&fail_payload, 10).unwrap(),
            MuxFwdReply::Failure("port forwarding failed".to_owned())
        );
        // mismatched request id must fail
        assert!(decode_fwd_reply(&ok_payload, 99).is_err());
    }

    #[tokio::test]
    async fn mux_client_round_trips_remote_port_allocation_over_duplex() {
        let (client_io, mut server_io) = tokio::io::duplex(4096);
        let server = tokio::spawn(async move {
            // read client HELLO
            let mut len = [0u8; 4];
            server_io.read_exact(&mut len).await.unwrap();
            let mut payload = vec![0u8; u32::from_be_bytes(len) as usize];
            server_io.read_exact(&mut payload).await.unwrap();
            assert_eq!(decode_hello_packet(&payload).unwrap(), SSHMUX_VER);
            // send server HELLO
            server_io.write_all(&encode_hello_packet()).await.unwrap();
            // read MUX_C_OPEN_FWD
            server_io.read_exact(&mut len).await.unwrap();
            let mut req = vec![0u8; u32::from_be_bytes(len) as usize];
            server_io.read_exact(&mut req).await.unwrap();
            let mut cursor = req.as_slice();
            assert_eq!(read_u32(&mut cursor).unwrap(), MUX_C_OPEN_FWD);
            let req_id = read_u32(&mut cursor).unwrap();
            assert_eq!(read_u32(&mut cursor).unwrap(), MUX_FWD_REMOTE);
            assert_eq!(read_ssh_string(&mut cursor).unwrap(), "127.0.0.1");
            assert_eq!(read_u32(&mut cursor).unwrap(), 0);
            assert_eq!(read_ssh_string(&mut cursor).unwrap(), "127.0.0.1");
            assert_eq!(read_u32(&mut cursor).unwrap(), 54321);
            // reply with MUX_S_REMOTE_PORT
            let mut reply = Vec::new();
            write_u32(&mut reply, MUX_S_REMOTE_PORT);
            write_u32(&mut reply, req_id);
            write_u32(&mut reply, 39876);
            server_io
                .write_all(&frame_mux_payload(&reply))
                .await
                .unwrap();
            server_io.flush().await.unwrap();
        });
        let mut client = MuxClient::handshake(client_io).await.unwrap();
        let allocated = client
            .open_remote_forward("127.0.0.1:54321".parse().unwrap())
            .await
            .unwrap();
        assert_eq!(
            allocated,
            "127.0.0.1:39876".parse::<std::net::SocketAddr>().unwrap()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn open_primary_forwards_rolls_back_control_forward_on_data_timeout_over_fresh_mux() {
        let (c1, mut s1) = tokio::io::duplex(4096);
        let (c2, mut s2) = tokio::io::duplex(4096);
        let connections =
            std::sync::Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from([
                c1, c2,
            ])));
        let first_server = tokio::spawn(async move {
            let mut len = [0u8; 4];
            s1.read_exact(&mut len).await.unwrap();
            let mut payload = vec![0u8; u32::from_be_bytes(len) as usize];
            s1.read_exact(&mut payload).await.unwrap();
            s1.write_all(&encode_hello_packet()).await.unwrap();
            // 1st MUX_C_OPEN_FWD (control): reply with allocated remote port
            s1.read_exact(&mut len).await.unwrap();
            let mut req1 = vec![0u8; u32::from_be_bytes(len) as usize];
            s1.read_exact(&mut req1).await.unwrap();
            let mut cursor = req1.as_slice();
            assert_eq!(read_u32(&mut cursor).unwrap(), MUX_C_OPEN_FWD);
            let req1_id = read_u32(&mut cursor).unwrap();
            let mut reply1 = Vec::new();
            write_u32(&mut reply1, MUX_S_REMOTE_PORT);
            write_u32(&mut reply1, req1_id);
            write_u32(&mut reply1, 41111);
            s1.write_all(&frame_mux_payload(&reply1)).await.unwrap();
            s1.flush().await.unwrap();
            // 2nd MUX_C_OPEN_FWD (data): read request and stall until client drops on timeout
            s1.read_exact(&mut len).await.unwrap();
            let mut req2 = vec![0u8; u32::from_be_bytes(len) as usize];
            s1.read_exact(&mut req2).await.unwrap();
            let mut eof = [0u8; 1];
            let _ = s1.read(&mut eof).await;
        });
        let rollback_server = tokio::spawn(async move {
            let mut len = [0u8; 4];
            s2.read_exact(&mut len).await.unwrap();
            let mut payload = vec![0u8; u32::from_be_bytes(len) as usize];
            s2.read_exact(&mut payload).await.unwrap();
            s2.write_all(&encode_hello_packet()).await.unwrap();
            // expect MUX_C_CLOSE_FWD for the rolled-back control forward (port 41111)
            s2.read_exact(&mut len).await.unwrap();
            let mut close_req = vec![0u8; u32::from_be_bytes(len) as usize];
            s2.read_exact(&mut close_req).await.unwrap();
            let mut cursor = close_req.as_slice();
            assert_eq!(read_u32(&mut cursor).unwrap(), MUX_C_CLOSE_FWD);
            let close_id = read_u32(&mut cursor).unwrap();
            assert_eq!(read_u32(&mut cursor).unwrap(), MUX_FWD_REMOTE);
            assert_eq!(read_ssh_string(&mut cursor).unwrap(), "127.0.0.1");
            assert_eq!(read_u32(&mut cursor).unwrap(), 41111);
            assert_eq!(read_ssh_string(&mut cursor).unwrap(), "127.0.0.1");
            assert_eq!(read_u32(&mut cursor).unwrap(), 50001);
            let mut ok = Vec::new();
            write_u32(&mut ok, MUX_S_OK);
            write_u32(&mut ok, close_id);
            s2.write_all(&frame_mux_payload(&ok)).await.unwrap();
            s2.flush().await.unwrap();
        });
        let err = open_primary_forwards_with_connector(
            std::path::Path::new("/tmp/mock-mux"),
            ForwardDirection::Remote,
            "127.0.0.1:50001".parse().unwrap(),
            Some("127.0.0.1:50002".parse().unwrap()),
            std::time::Duration::from_millis(150),
            true,
            move |_path| {
                let connections = connections.clone();
                async move {
                    let stream = connections
                        .lock()
                        .unwrap()
                        .pop_front()
                        .context("no mock mux connection left")?;
                    MuxClient::handshake(stream).await
                }
            },
        )
        .await
        .expect_err("stalled data forward must time out");
        assert!(
            format!("{err:#}").contains("timed out opening SSH port forwards"),
            "unexpected timeout error: {err:#}"
        );
        first_server.await.unwrap();
        rollback_server.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resolve_ssh_config_rejects_truncated_stdout() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let fake_ssh = dir.path().join("fake-ssh");
        // emit > DIAGNOSTIC_CAPTURE_LIMIT (64 KiB) of output so lines 1..4 would be truncated
        std::fs::write(
            &fake_ssh,
            "#!/bin/sh\nprintf 'hostname 10.0.0.5\\n'\ndd if=/dev/zero bs=70000 count=1 2>/dev/null | tr '\\000' 'a'\nprintf '\\n'\n",
        )
        .unwrap();
        std::fs::set_permissions(&fake_ssh, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cleanup = crate::RemoteCleanup::new().unwrap();
        let err = resolve_ssh_config_with_program(
            &crate::SshSession::local(),
            &fake_ssh,
            std::time::Duration::from_secs(5),
            &cleanup,
        )
        .await
        .expect_err("truncated ssh -G stdout must fail closed");
        assert!(
            format!("{err:#}").contains("exceeded the"),
            "unexpected truncation error: {err:#}"
        );
        cleanup.finish();
    }

    #[test]
    fn format_tunnel_summary_reports_single_parallel_and_degraded_counts() {
        let make = |active_data: usize, requested: usize| SshTunnelSet {
            control_addr: "127.0.0.1:40000".parse().unwrap(),
            data_addrs: (0..active_data)
                .map(|i| {
                    std::net::SocketAddr::from((
                        std::net::Ipv4Addr::LOCALHOST,
                        40001 + u16::try_from(i).unwrap(),
                    ))
                })
                .collect(),
            requested_tunnels: requested,
            primary_forwards: Vec::new(),
            owned_sessions: Vec::new(),
        };
        assert_eq!(make(0, 1).format_tunnel_summary(), "1 tunnel");
        assert_eq!(make(1, 1).format_tunnel_summary(), "1 tunnel");
        assert_eq!(make(4, 4).format_tunnel_summary(), "4 parallel tunnels");
        assert_eq!(
            make(1, 4).format_tunnel_summary(),
            "1 of 4 requested parallel tunnels"
        );
        assert_eq!(
            make(2, 4).format_tunnel_summary(),
            "2 of 4 requested parallel tunnels"
        );
    }
}
