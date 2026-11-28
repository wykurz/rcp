//! Master-side remote copy orchestration and transport planning.
//!
//! Coordinates peer-identity checks, daemon spawn and readiness validation, control-channel
//! handshakes (`MasterHello`, `SourceMasterHello`, `DirectConnectFailed`, `SshFallbackEndpoints`,
//! `RcpdResult`), master-managed SSH tunnel setup/fallback, graceful teardown, and summary
//! merging. Production `rcp` runs this module over [`RemoteBackend`] backed by OpenSSH; tests can
//! supply an in-process or simulated-network backend without duplicating any orchestration logic.

use anyhow::{Context, anyhow};
use futures::future::BoxFuture;
use tracing::Instrument as _;

/// Transport mode for source↔destination control and data connections in remote copies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum RemoteTransport {
    /// Direct TCP between source and destination (default; fastest).
    #[default]
    Tcp,
    /// Always tunnel source↔destination traffic over SSH (no extra firewall ports needed).
    /// Same-host copies (`host:/a host:/b`) still use direct loopback.
    Ssh,
    /// Try direct TCP first; if the control connection fails within
    /// `--remote-direct-probe-timeout-sec`, automatically fall back to SSH tunnels.
    Auto,
}

const DEFAULT_MAX_SSH_TUNNELS: usize = 16;

/// Default direct-TCP probe timeout (in seconds) before falling back to SSH in
/// `--remote-transport=auto`.
pub const DEFAULT_REMOTE_DIRECT_PROBE_TIMEOUT_SEC: u64 = 3;

/// Whether `source` and `destination` daemons need ephemeral TLS certificates at startup.
///
/// Explicit `--remote-transport=ssh` authenticates loopback forwards via a 1-RTT mutual
/// HMAC-SHA256 handshake instead of double-encrypting inside SSH, so neither daemon needs to
/// generate a TLS certificate or initialize rustls.
#[must_use]
pub const fn daemons_need_tls_certs(no_encryption: bool, transport: RemoteTransport) -> bool {
    !no_encryption && !matches!(transport, RemoteTransport::Ssh)
}

/// Resolve the number of parallel SSH tunnels to request for a transfer.
///
/// Dry runs always use 1 tunnel (control only). When `--ssh-tunnels` is omitted, the automatic
/// default tracks `effective_max_connections` (the CPU-derived remote copy concurrency) up to
/// `DEFAULT_MAX_SSH_TUNNELS`. An explicit `--ssh-tunnels=N` overrides that cap (still bounded by
/// `effective_max_connections`).
#[must_use]
fn resolve_requested_ssh_tunnels(
    explicit: Option<std::num::NonZeroUsize>,
    effective_max_connections: std::num::NonZeroUsize,
    preview_only: bool,
) -> usize {
    if preview_only {
        return 1;
    }
    let desired = match explicit {
        Some(count) => count.get(),
        None => effective_max_connections.get().min(DEFAULT_MAX_SSH_TUNNELS),
    };
    desired.min(effective_max_connections.get()).max(1)
}

/// Whether `source_bind_ip` lets SSH port forwards dial `source rcpd` over loopback.
///
/// Mirrors [`remote::get_tcp_listener_tunnel_target_ip`] + [`remote::tls::is_loopback_peer`]:
/// unspecified (`0.0.0.0` / `::`), omitted (`None`), and loopback bind addresses all accept SSH
/// forwards on loopback, whereas an explicit concrete non-loopback `--bind-ip` delivers a
/// non-loopback peer IP and therefore stays on mutual TLS 1.3 during `--remote-transport=auto`
/// SSH fallback.
fn source_bind_ip_supports_loopback_ssh_auth(source_bind_ip: Option<&str>) -> bool {
    match source_bind_ip {
        None => true,
        Some(bind_ip) => bind_ip
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_unspecified() || remote::tls::is_loopback_peer(ip)),
    }
}

/// Derive matching `(source_peer_auth, destination_peer_auth)` policies for `MasterHello`.
fn plan_peer_auth(
    no_encryption: bool,
    transport: RemoteTransport,
    topology: CopyTopology,
    source_bind_ip: Option<&str>,
    source_fingerprint: Option<remote::protocol::CertFingerprint>,
    destination_fingerprint: Option<remote::protocol::CertFingerprint>,
) -> anyhow::Result<(remote::protocol::PeerAuth, remote::protocol::PeerAuth)> {
    if no_encryption {
        return Ok((
            remote::protocol::PeerAuth::None,
            remote::protocol::PeerAuth::None,
        ));
    }
    if transport == RemoteTransport::Ssh {
        let secret = remote::protocol::SshAuthSecret::generate();
        return Ok((
            remote::protocol::PeerAuth::SshSharedSecret {
                secret: secret.clone(),
            },
            remote::protocol::PeerAuth::SshSharedSecret { secret },
        ));
    }
    let src_fp =
        source_fingerprint.context("source rcpd did not report a TLS certificate fingerprint")?;
    let dst_fp = destination_fingerprint
        .context("destination rcpd did not report a TLS certificate fingerprint")?;
    if transport == RemoteTransport::Auto
        && topology != CopyTopology::SameHost
        && source_bind_ip_supports_loopback_ssh_auth(source_bind_ip)
    {
        let secret = remote::protocol::SshAuthSecret::generate();
        Ok((
            remote::protocol::PeerAuth::AutoTlsOrSshSecret {
                peer_fingerprint: dst_fp,
                secret: secret.clone(),
            },
            remote::protocol::PeerAuth::AutoTlsOrSshSecret {
                peer_fingerprint: src_fp,
                secret,
            },
        ))
    } else {
        Ok((
            remote::protocol::PeerAuth::Tls {
                peer_fingerprint: dst_fp,
            },
            remote::protocol::PeerAuth::Tls {
                peer_fingerprint: src_fp,
            },
        ))
    }
}

/// Extract an IPv4 literal from a host string for explicit source binding.
fn extract_bind_ip_from_host(host: &str) -> Option<String> {
    if host.parse::<std::net::Ipv4Addr>().is_ok() {
        Some(host.to_string())
    } else {
        None
    }
}

/// Select the source `rcpd` bind IP, giving explicit `--bind-ip` precedence over an IPv4
/// literal inferred from the source operand host.
#[must_use]
fn resolve_source_bind_ip(explicit_bind_ip: Option<&str>, source_host: &str) -> Option<String> {
    explicit_bind_ip
        .map(str::to_owned)
        .or_else(|| extract_bind_ip_from_host(source_host))
}

/// Select the loopback address family `source rcpd` should bind for SSH forwarding.
#[must_use]
fn resolve_source_target_address_family(
    source_is_local: bool,
    client_listener_af: remote::ssh_tunnel::SshAddressFamily,
    resolved_src_ssh: Option<&remote::ssh_tunnel::ResolvedSshConfig>,
    source_ssh_endpoint: Option<std::net::SocketAddr>,
) -> remote::ssh_tunnel::SshAddressFamily {
    if source_is_local {
        // upload (-R): local `ssh` dials local `source rcpd`, so the local client's AddressFamily
        // governs the loopback family `source rcpd` binds
        return client_listener_af;
    }
    // download (-L) / 3-host / same-host: remote `source sshd` dials `source rcpd`'s loopback
    // listener. Follow the forwarding server's actual address family observed from `SSH_CONNECTION`
    // (handles both client `any` + server `inet6` and client `inet6` + ProxyCommand -> IPv4
    // server). Only fall back to client SSH config when the server endpoint probe is unavailable
    if let Some(ep) = source_ssh_endpoint {
        return if ep.is_ipv6() {
            remote::ssh_tunnel::SshAddressFamily::Inet6
        } else {
            remote::ssh_tunnel::SshAddressFamily::Inet
        };
    }
    if resolved_src_ssh.is_some_and(|c| {
        c.address_family == remote::ssh_tunnel::SshAddressFamily::Inet6
            || c.target
                .host
                .trim_matches(['[', ']'])
                .split('%')
                .next()
                .is_some_and(|host| host.parse::<std::net::Ipv6Addr>().is_ok())
    }) {
        return remote::ssh_tunnel::SshAddressFamily::Inet6;
    }
    resolved_src_ssh.map_or(remote::ssh_tunnel::SshAddressFamily::Any, |c| {
        c.address_family
    })
}

/// Select the listening loopback address family for a master-managed `-L` or `-R` tunnel.
#[must_use]
fn resolve_tunnel_listen_address_family(
    direction: remote::ssh_tunnel::ForwardDirection,
    client_address_family: remote::ssh_tunnel::SshAddressFamily,
    server_ssh_endpoint: Option<std::net::SocketAddr>,
) -> remote::ssh_tunnel::SshAddressFamily {
    match direction {
        // download (-L): the local `ssh` client opens the listening socket, so keep the client's
        // configured AddressFamily
        remote::ssh_tunnel::ForwardDirection::Local => client_address_family,
        // upload (-R): the remote `sshd` server opens the listening socket. Prefer the server's
        // ingress address family from `SSH_CONNECTION` (falling back once to the alternate literal
        // loopback inside `MuxClient::open_forward` if the server rejects it, e.g. under
        // `PermitListen 127.0.0.1:*` on an IPv6 session)
        remote::ssh_tunnel::ForwardDirection::Remote => match server_ssh_endpoint {
            Some(ep) if ep.is_ipv6() => remote::ssh_tunnel::SshAddressFamily::Inet6,
            Some(_) => remote::ssh_tunnel::SshAddressFamily::Inet,
            None => remote::ssh_tunnel::SshAddressFamily::Any,
        },
    }
}

/// Resolve the source `rcpd` `--bind-ip` value for the selected remote transport mode.
#[must_use]
fn resolve_source_bind_ip_for_transport(
    transport: RemoteTransport,
    explicit_bind_ip: Option<&str>,
    source_host: &str,
    address_family: remote::ssh_tunnel::SshAddressFamily,
) -> Option<String> {
    match transport {
        RemoteTransport::Ssh => Some(address_family.loopback_ip().to_string()),
        // in `auto` mode, do not infer `--bind-ip` from a literal IPv4 operand: cloud/NAT public
        // IPs are not bound on the host's local interfaces and would fail at bind before `auto`
        // can probe or fall back to SSH. Under `AddressFamily inet6` without an explicit
        // `--bind-ip`, bind dual-stack `::` so IPv4 direct TCP still works while `::1` stays
        // reachable for IPv6 SSH `-L`/`-R` fallback
        RemoteTransport::Auto => explicit_bind_ip.map(str::to_owned).or_else(|| {
            (address_family == remote::ssh_tunnel::SshAddressFamily::Inet6)
                .then(|| "::".to_string())
        }),
        RemoteTransport::Tcp => resolve_source_bind_ip(explicit_bind_ip, source_host),
    }
}

/// Resolved remote copy topology used to choose direct vs tunneled data transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyTopology {
    /// Both operands are on the same remote host (identical host and port).
    SameHost,
    /// Remote source → local destination (master opens `-L` tunnels to source).
    Download,
    /// Local source → remote destination (master opens `-R` tunnels to destination).
    Upload,
    /// Two distinct remote hosts (destination opens `-L` tunnels directly to source).
    ThreeHost,
}

/// Preliminary topology before live peer identity verification runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitialTopology {
    /// Identical `(user, host, port)` reuses one SSH master session, guaranteeing co-location.
    SameHostSharedSession,
    /// Two independently opened remote sessions whose `ssh -G` resolves to the same endpoint;
    /// requires live `PeerIdentity::matches` verification before taking the `SameHost` shortcut.
    SameEndpointNeedsPeerCheck,
    /// Remote source → local destination (master opens `-L` tunnels to source).
    Download,
    /// Local source → remote destination (master opens `-R` tunnels to destination).
    Upload,
    /// Two distinct remote hosts (destination opens `-L` tunnels directly to source).
    ThreeHost,
}

impl InitialTopology {
    /// Finalize the copy topology using optional live peer identities for co-location checks.
    #[must_use]
    pub fn resolve(
        self,
        verified_peers: Option<(
            &remote::protocol::PeerIdentity,
            &remote::protocol::PeerIdentity,
        )>,
    ) -> CopyTopology {
        match self {
            Self::SameHostSharedSession => CopyTopology::SameHost,
            Self::SameEndpointNeedsPeerCheck => match verified_peers {
                Some((src, dst)) if src.matches(dst) => CopyTopology::SameHost,
                _ => CopyTopology::ThreeHost,
            },
            Self::Download => CopyTopology::Download,
            Self::Upload => CopyTopology::Upload,
            Self::ThreeHost => CopyTopology::ThreeHost,
        }
    }
}

/// Compute how long `source rcpd` waits for destination to connect, accounting for direct-TCP
/// probes and SSH tunnel bring-up budgets.
#[must_use]
fn source_accept_timeout_sec(
    transport: RemoteTransport,
    topology: CopyTopology,
    conn_timeout_sec: u64,
    probe_timeout_sec: u64,
) -> u64 {
    match (transport, topology) {
        (RemoteTransport::Tcp, _) | (_, CopyTopology::SameHost) => conn_timeout_sec,
        (
            RemoteTransport::Ssh,
            CopyTopology::Download | CopyTopology::Upload | CopyTopology::ThreeHost,
        ) => conn_timeout_sec.saturating_mul(2),
        (
            RemoteTransport::Auto,
            CopyTopology::Download | CopyTopology::Upload | CopyTopology::ThreeHost,
        ) => {
            let effective_probe = probe_timeout_sec.min(conn_timeout_sec);
            effective_probe.saturating_add(conn_timeout_sec.saturating_mul(2))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectionCeilingOrigin {
    Default,
    Explicit,
}

/// Configured upper bound on remote data stream concurrency and whether the user set it explicitly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionCeiling {
    value: std::num::NonZeroUsize,
    origin: ConnectionCeilingOrigin,
}

impl ConnectionCeiling {
    /// Construct a connection ceiling from an optional `--max-connections` CLI argument.
    #[must_use]
    pub fn from_arg(value: Option<std::num::NonZeroUsize>) -> Self {
        match value {
            Some(value) => Self {
                value,
                origin: ConnectionCeilingOrigin::Explicit,
            },
            None => Self {
                value: std::num::NonZeroUsize::new(remote::DEFAULT_MAX_CONNECTIONS)
                    .expect("the default connection ceiling is nonzero"),
                origin: ConnectionCeilingOrigin::Default,
            },
        }
    }

    /// Return the numeric stream upper bound.
    #[must_use]
    pub const fn value(self) -> std::num::NonZeroUsize {
        self.value
    }

    /// Return true when the user passed `--max-connections` explicitly.
    #[must_use]
    pub const fn is_explicit(self) -> bool {
        matches!(self.origin, ConnectionCeilingOrigin::Explicit)
    }

    fn description(self) -> String {
        match self.origin {
            ConnectionCeilingOrigin::Default => {
                format!("the default connection ceiling of {}", self.value)
            }
            ConnectionCeilingOrigin::Explicit => format!("--max-connections={}", self.value),
        }
    }
}

/// Provenance of a master-authoritative `--max-files-in-flight` / `--max-open-files` setting.
#[derive(Clone, Copy, Debug)]
pub enum MasterAuthoritativeFilesInFlight {
    Explicit,
    Deprecated,
}

impl MasterAuthoritativeFilesInFlight {
    fn source(self) -> common::FilesInFlightSource {
        match self {
            Self::Explicit => common::FilesInFlightSource::Explicit,
            Self::Deprecated => common::FilesInFlightSource::DeprecatedMaxOpenFiles,
        }
    }

    fn to_rcpd(self, limit: common::ConcurrencyLimit) -> remote::protocol::RcpdFilesInFlight {
        match self {
            Self::Explicit => remote::protocol::RcpdFilesInFlight::Explicit(limit),
            Self::Deprecated => remote::protocol::RcpdFilesInFlight::DeprecatedMaxOpenFiles(limit),
        }
    }
}

/// Whether file/stream concurrency is resolved by the source daemon or fixed by the master.
#[derive(Clone, Copy, Debug)]
pub enum RemoteConcurrencyPolicy {
    Automatic,
    MasterAuthoritative {
        files_in_flight: MasterAuthoritativeFilesInFlight,
        concurrency: remote::ResolvedRemoteConcurrency,
    },
}

impl RemoteConcurrencyPolicy {
    fn notice_file_limit(
        self,
        source_files_in_flight: common::ConcurrencyLimit,
    ) -> (common::FilesInFlightSource, common::ConcurrencyLimit) {
        match self {
            Self::Automatic => (
                common::FilesInFlightSource::Automatic,
                source_files_in_flight,
            ),
            Self::MasterAuthoritative {
                files_in_flight,
                concurrency,
            } => (files_in_flight.source(), concurrency.files_in_flight()),
        }
    }
}

/// Validated remote copy parameters shared across source and destination daemon launches.
#[derive(Debug)]
pub struct MasterRemoteRequest {
    pub tcp: remote::TcpConfig,
    pub rcpd: remote::protocol::RcpdConfig,
    pub filter: Option<common::filter::FilterSettings>,
    pub connection_ceiling: ConnectionCeiling,
    pub pending_writes_multiplier: std::num::NonZeroUsize,
    pub concurrency_policy: RemoteConcurrencyPolicy,
}

/// Format an optional user-facing notice when the effective remote stream count was clamped.
#[must_use]
pub fn remote_stream_clamp_notice(
    policy: RemoteConcurrencyPolicy,
    source_files_in_flight: common::ConcurrencyLimit,
    connection_ceiling: ConnectionCeiling,
    effective_connections: std::num::NonZeroUsize,
) -> Option<String> {
    let (source, file_limit) = policy.notice_file_limit(source_files_in_flight);
    let common::ConcurrencyLimit::Limited(file_limit) = file_limit else {
        return None;
    };
    if file_limit > effective_connections {
        let option = match source {
            common::FilesInFlightSource::DeprecatedMaxOpenFiles => "--max-open-files",
            common::FilesInFlightSource::Explicit => "--max-files-in-flight",
            common::FilesInFlightSource::Automatic => {
                if !connection_ceiling.is_explicit() {
                    return None;
                }
                return Some(format!(
                    "The source's automatic file ceiling of {file_limit} was reduced to {effective_connections} remote data streams by {}",
                    connection_ceiling.description()
                ));
            }
        };
        let connection_ceiling = connection_ceiling.description();
        return Some(format!(
            "Requested {option}={file_limit}, but {connection_ceiling} reduced remote data streams to {effective_connections}"
        ));
    }
    if !connection_ceiling.is_explicit() || connection_ceiling.value() <= effective_connections {
        return None;
    }
    let file_ceiling = match source {
        common::FilesInFlightSource::Automatic => {
            format!("the source's automatic file ceiling of {file_limit}")
        }
        common::FilesInFlightSource::Explicit => {
            format!("--max-files-in-flight={file_limit}")
        }
        common::FilesInFlightSource::DeprecatedMaxOpenFiles => {
            format!("--max-open-files={file_limit}")
        }
    };
    let connection_ceiling = connection_ceiling.description();
    Some(format!(
        "Requested {connection_ceiling}, but {file_ceiling} reduced remote data streams to {effective_connections}"
    ))
}

fn endpoint_config(
    request: &MasterRemoteRequest,
    files_in_flight: remote::protocol::RcpdFilesInFlight,
    concurrency: Option<remote::ResolvedRemoteConcurrency>,
) -> remote::protocol::RcpdConfig {
    let mut rcpd = request.rcpd.clone();
    rcpd.files_in_flight = files_in_flight;
    if let Some(concurrency) = concurrency {
        rcpd.max_connections = concurrency.max_connections().get();
    }
    rcpd
}

/// Build the source daemon's [`remote::protocol::RcpdConfig`] from the master request.
#[must_use]
pub fn build_source_remote_config(request: &MasterRemoteRequest) -> remote::protocol::RcpdConfig {
    match request.concurrency_policy {
        RemoteConcurrencyPolicy::MasterAuthoritative {
            files_in_flight,
            concurrency,
        } => endpoint_config(
            request,
            files_in_flight.to_rcpd(concurrency.files_in_flight()),
            Some(concurrency),
        ),
        RemoteConcurrencyPolicy::Automatic => endpoint_config(
            request,
            remote::protocol::RcpdFilesInFlight::Automatic,
            None,
        ),
    }
}

/// Validate source readiness concurrency limits and build the matching destination daemon config.
pub fn build_destination_remote_config(
    request: &MasterRemoteRequest,
    source: &remote::RcpdReadiness,
) -> anyhow::Result<remote::protocol::RcpdConfig> {
    let (concurrency, files_in_flight) = match request.concurrency_policy {
        RemoteConcurrencyPolicy::Automatic => {
            let concurrency = remote::resolve_remote_concurrency(
                source.files_in_flight,
                request.connection_ceiling.value(),
                request.pending_writes_multiplier,
            )?;
            if concurrency.max_connections() != source.max_connections {
                anyhow::bail!(
                    "source rcpd reported {} effective streams, expected {} from its source-owned file and connection ceilings",
                    source.max_connections,
                    concurrency.max_connections()
                );
            }
            let files_in_flight = match source.files_in_flight {
                common::ConcurrencyLimit::Limited(value) => {
                    remote::protocol::RcpdFilesInFlight::ResolvedAutomatic(value)
                }
                common::ConcurrencyLimit::Unlimited => {
                    anyhow::bail!("an automatic source rcpd reported an unlimited file ceiling")
                }
            };
            (concurrency, files_in_flight)
        }
        RemoteConcurrencyPolicy::MasterAuthoritative {
            files_in_flight,
            concurrency,
        } => {
            if source.files_in_flight != concurrency.files_in_flight() {
                anyhow::bail!(
                    "source rcpd reported file ceiling {}, expected {}",
                    source.files_in_flight,
                    concurrency.files_in_flight()
                );
            }
            if concurrency.max_connections() != source.max_connections {
                anyhow::bail!(
                    "source rcpd reported {} effective streams, expected the master-authoritative value {}",
                    source.max_connections,
                    concurrency.max_connections()
                );
            }
            (
                concurrency,
                files_in_flight.to_rcpd(concurrency.files_in_flight()),
            )
        }
    };
    Ok(endpoint_config(request, files_in_flight, Some(concurrency)))
}

fn validate_destination_readiness(
    source: &remote::RcpdReadiness,
    destination: &remote::RcpdReadiness,
) -> anyhow::Result<()> {
    if destination.files_in_flight != source.files_in_flight
        || destination.max_connections != source.max_connections
    {
        anyhow::bail!(
            "destination rcpd reported file/stream limits {}/{}, but source negotiated {}/{}",
            destination.files_in_flight,
            destination.max_connections,
            source.files_in_flight,
            source.max_connections,
        );
    }
    Ok(())
}

fn validate_readiness_encryption(
    role: &str,
    expected_encryption: bool,
    readiness: &remote::RcpdReadiness,
) -> anyhow::Result<()> {
    if readiness.fingerprint.is_some() != expected_encryption {
        anyhow::bail!(
            "TLS configuration mismatch: {role} rcpd and master must both use encryption or both disable it"
        );
    }
    Ok(())
}

/// The diagnostic for an `rcpd` whose connection FAILED before it said what happened.
///
/// User-facing explanation for a failed read from a remote `rcpd`'s SSH stdout control stream.
///
/// A dying `rcpd` or severed SSH channel can end the master's stdout demux two ways: a framing or
/// transport `Err` (truncated frame, pipe error) or a clean `Ok(None)` EOF (daemon exited/killed or
/// SSH mux closed cleanly). Both mean the daemon did not deliver the expected control message, so
/// both are reported in terms of what the master did not receive. This is the `Err` half, which
/// keeps its underlying transport/framing error in the chain. See [`rcpd_closed_quietly`] for the
/// clean-EOF half.
fn rcpd_went_quiet(role: &str, host: &str, expected: &str) -> String {
    format!(
        "{role} rcpd on '{host}' did not {expected} (the process likely died - check the remote \
         host for crashes or OOM kills)"
    )
}

/// The [`rcpd_went_quiet`] situation reached through a clean end-of-stream instead: the daemon's
/// SSH stdout closed cleanly with no further control frames, so there is no transport error.
///
/// Distinct wording rather than a shared one, for two reasons. A clean silent close points at a
/// daemon that exited or was killed (`SIGKILL`, OOM) or an SSH session torn down by `ServerAlive`
/// without sending its message, whereas a read/decode error points at truncation or framing
/// corruption. And it makes the branch observable: this is the `Ok(None)` that the master used to
/// `.expect()`, which under `panic = "abort"` turned a diagnosable error into `SIGABRT` with its
/// output discarded, so the regression test for it can tell the two branches apart.
fn rcpd_closed_quietly(role: &str, host: &str, expected: &str) -> String {
    format!(
        "{role} rcpd on '{host}' closed its control connection cleanly but did not {expected} (the \
         process likely died - check the remote host for crashes or OOM kills)"
    )
}

const TRACING_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

async fn finish_tracing_receiver(
    task: remote::AbortOnDropTask<()>,
    rcpd_type: remote::tracelog::RcpdType,
    deadline: tokio::time::Instant,
) {
    match tokio::time::timeout_at(deadline, task.join()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::debug!("{rcpd_type} tracing receiver task failed: {error:#}");
        }
        Err(_) => {
            tracing::debug!("{rcpd_type} tracing receiver did not finish during drain grace");
        }
    }
}

async fn finish_tracing_receivers(
    receivers: Vec<(remote::AbortOnDropTask<()>, remote::tracelog::RcpdType)>,
) {
    let deadline = tokio::time::Instant::now() + TRACING_DRAIN_GRACE;
    futures::future::join_all(
        receivers
            .into_iter()
            .map(|(task, rcpd_type)| finish_tracing_receiver(task, rcpd_type, deadline)),
    )
    .await;
}

/// Handle to a running `rcpd` daemon owned by the master session until teardown.
///
/// Dropping a handle without calling [`DaemonHandle::wait`] must release any owned daemon channels
/// and background collectors so cancellation or panic does not leak resources.
pub trait DaemonHandle: Send {
    /// Wait for the daemon process or task to exit and surface any non-zero exit or failure.
    fn wait(self: Box<Self>) -> BoxFuture<'static, anyhow::Result<()>>;
}

/// Handle to master-managed SSH tunnels owned by the master session until teardown.
///
/// Dropping a handle without calling [`TunnelHandle::close`] must tear down tunnel resources.
pub trait TunnelHandle: Send {
    /// Gracefully close all tunnels in the set.
    fn close(self: Box<Self>) -> BoxFuture<'static, ()>;
}

/// A spawned `rcpd` daemon after its stderr startup readiness record has been parsed.
pub struct SpawnedDaemon {
    pub readiness: remote::RcpdReadiness,
    pub stdin: remote::streams::BoxedWrite,
    pub stdout: remote::streams::BoxedRead,
    pub handle: Box<dyn DaemonHandle>,
}

/// Parameters for opening master-managed SSH port forwards (`-L` download or `-R` upload).
#[derive(Debug, Clone)]
pub struct TunnelRequest {
    pub direction: remote::ssh_tunnel::ForwardDirection,
    pub target_control: std::net::SocketAddr,
    pub target_data: Option<std::net::SocketAddr>,
    pub requested_tunnels: usize,
    pub listen_address_family: remote::ssh_tunnel::SshAddressFamily,
    pub expected_peer: Option<remote::protocol::PeerIdentity>,
    pub node_endpoint: Option<std::net::SocketAddr>,
    pub timeout: std::time::Duration,
}

/// Active master-managed SSH tunnels returned by [`RemoteBackend::open_tunnels`].
pub struct OpenedTunnels {
    pub control_addr: std::net::SocketAddr,
    pub data_addrs: Vec<std::net::SocketAddr>,
    pub summary: String,
    pub handle: Box<dyn TunnelHandle>,
}

/// Backend boundary separating master orchestration from OS/SSH process and tunnel mechanics.
pub trait RemoteBackend: Send + Sync {
    /// Probe the peer identity and concrete `SSH_CONNECTION` server endpoint for `role`.
    fn probe_peer_identity(
        &self,
        role: remote::protocol::RcpdRole,
        timeout: std::time::Duration,
    ) -> BoxFuture<'_, anyhow::Result<(remote::protocol::PeerIdentity, Option<std::net::SocketAddr>)>>;

    /// Disable agent forwarding on the destination endpoint when peer verification confirms
    /// co-location on the same host.
    fn disable_destination_agent_forward(&mut self);

    /// Spawn an `rcpd` daemon for `role`, read its startup readiness record, and return its
    /// framed stdio streams and process handle.
    fn spawn_rcpd<'a>(
        &'a self,
        role: remote::protocol::RcpdRole,
        config: &'a remote::protocol::RcpdConfig,
        bind_ip: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<SpawnedDaemon>>;

    /// Open `1..=request.requested_tunnels` master-managed tunnels for the requested direction.
    fn open_tunnels(&self, request: TunnelRequest) -> BoxFuture<'_, anyhow::Result<OpenedTunnels>>;
}

async fn wait_for_daemons(
    daemons: Vec<Box<dyn DaemonHandle>>,
    report_failures: bool,
) -> anyhow::Result<()> {
    let results = futures::future::join_all(daemons.into_iter().map(DaemonHandle::wait)).await;
    let mut first_error = None;
    for result in results {
        if let Err(error) = result {
            if report_failures {
                tracing::error!("Failed to wait for rcpd process: {error:#}");
            } else {
                tracing::debug!("rcpd process failed while unwinding startup: {error:#}");
            }
            first_error.get_or_insert(error);
        }
    }
    tracing::info!("All rcpd processes finished");
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn finish_remote_teardown<F>(
    process_wait: F,
    tracing_receivers: Vec<(remote::AbortOnDropTask<()>, remote::tracelog::RcpdType)>,
) -> F::Output
where
    F: std::future::Future,
{
    // rcpd can spend several seconds flushing its tracing sender before exit. Keep receivers live
    // while every daemon finishes, then give all receiver tasks one shared final drain deadline
    let result = process_wait.await;
    finish_tracing_receivers(tracing_receivers).await;
    result
}

#[derive(Default)]
struct SessionTeardown {
    daemons: Vec<Box<dyn DaemonHandle>>,
    tracing_receivers: Vec<(remote::AbortOnDropTask<()>, remote::tracelog::RcpdType)>,
    tunnels: Option<Box<dyn TunnelHandle>>,
}

impl SessionTeardown {
    fn retain_daemon(&mut self, daemon: Box<dyn DaemonHandle>) {
        self.daemons.push(daemon);
    }

    fn retain_tracing_receiver(
        &mut self,
        task: remote::AbortOnDropTask<()>,
        rcpd_type: remote::tracelog::RcpdType,
    ) {
        self.tracing_receivers.push((task, rcpd_type));
    }

    fn retain_tunnels(&mut self, tunnels: Box<dyn TunnelHandle>) {
        self.tunnels = Some(tunnels);
    }

    async fn finish(mut self, report_process_failures: bool) -> anyhow::Result<()> {
        if let Some(tunnels) = self.tunnels.take() {
            tunnels.close().await;
        }
        finish_remote_teardown(
            wait_for_daemons(std::mem::take(&mut self.daemons), report_process_failures),
            std::mem::take(&mut self.tracing_receivers),
        )
        .await
    }
}

impl Drop for SessionTeardown {
    fn drop(&mut self) {
        // graceful exits consume all fields in finish(). Cancellation or panic still drops extra
        // SSH tunnel sessions and aborts receiver tasks; dropping each DaemonHandle releases
        // daemon channels and aborts its collectors
        drop(self.tunnels.take());
        for (task, _) in self.tracing_receivers.drain(..) {
            drop(task);
        }
    }
}

const PEER_FAILURE_RESULT_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

fn decode_daemon_result(
    result: anyhow::Result<Option<remote::protocol::RcpdResult>>,
    role: &str,
    host: &str,
) -> anyhow::Result<remote::protocol::RcpdResult> {
    let went_quiet = rcpd_went_quiet(role, host, "report a result");
    let closed_quietly = rcpd_closed_quietly(role, host, "report a result");
    result
        .with_context(|| went_quiet)?
        .ok_or_else(|| anyhow!(closed_quietly))
}

fn decode_destination_event(
    event: anyhow::Result<Option<remote::stdio::DestinationControlEvent>>,
    host: &str,
) -> anyhow::Result<remote::stdio::DestinationControlEvent> {
    let went_quiet = rcpd_went_quiet("destination", host, "report a result");
    let closed_quietly = rcpd_closed_quietly("destination", host, "report a result");
    event
        .with_context(|| went_quiet)?
        .ok_or_else(|| anyhow!(closed_quietly))
}

/// How the master's wait for one `rcpd` ended.
///
/// One failure reaches the master as several events: the result of the daemon that failed, or the
/// end of its control channel if it died, and the result of the peer that noticed. Which of them
/// arrives first is not something either daemon controls, so nothing is decided from the first one
/// alone: each daemon's side is waited out into one of these, and [`settle_daemon_ends`] reads the
/// pair.
enum DaemonEnd {
    /// The daemon reported its result.
    Reported(Box<remote::protocol::RcpdResult>),
    /// Its control channel ended, cleanly or with a transport error, before a result arrived. Holds
    /// the [`rcpd_closed_quietly`] or [`rcpd_went_quiet`] diagnostic.
    Lost(anyhow::Error),
    /// The master stopped waiting: after how the peer ended, this daemon's result was not coming in
    /// time, or not at all.
    Abandoned,
}

impl From<anyhow::Result<remote::protocol::RcpdResult>> for DaemonEnd {
    fn from(decoded: anyhow::Result<remote::protocol::RcpdResult>) -> Self {
        match decoded {
            Ok(result) => Self::Reported(Box::new(result)),
            Err(diagnostic) => Self::Lost(diagnostic),
        }
    }
}

impl DaemonEnd {
    /// The result the daemon reported, if it reported one.
    fn reported(&self) -> Option<&remote::protocol::RcpdResult> {
        match self {
            Self::Reported(result) => Some(result),
            Self::Lost(_) | Self::Abandoned => None,
        }
    }

    /// How long the peer gets to end as well once this daemon has, `None` for as long as it takes.
    fn peer_wait(&self, grace: std::time::Duration) -> Option<std::time::Duration> {
        match self.reported() {
            // the peer holds the other half of the counts and, on an established copy, sees this
            // daemon's failure on its own connections: finalizing partial files and reporting may
            // take it longer than the grace (bounded by SSH keepalive)
            Some(
                remote::protocol::RcpdResult::Success { .. }
                | remote::protocol::RcpdResult::Failure {
                    peer_connected: true,
                    ..
                },
            ) => None,
            // failed before the daemons met, or gone without a word: the peer may be waiting for a
            // connection that never comes, so it gets the time to say what it saw and no more
            Some(remote::protocol::RcpdResult::Failure {
                peer_connected: false,
                ..
            })
            | None => Some(grace),
        }
    }
}

/// Decide a remote copy's outcome from how its two daemons ended.
///
/// A function of the pair alone: which daemon the master heard from first is not an input. With
/// both results the outcome is the caller's to merge. Short of that the copy failed, and the error
/// lists what is known of each daemon, source first: the failure it reported, or the diagnostic of
/// a control channel that ended without a result. A daemon that reported success, or that was not
/// waited for, adds nothing.
fn settle_daemon_ends(
    source: DaemonEnd,
    dest: DaemonEnd,
) -> anyhow::Result<(remote::protocol::RcpdResult, remote::protocol::RcpdResult)> {
    let (source, dest) = match (source, dest) {
        (DaemonEnd::Reported(source), DaemonEnd::Reported(dest)) => return Ok((*source, *dest)),
        ends => ends,
    };
    let mut findings = Vec::with_capacity(2);
    let mut lost = Vec::with_capacity(2);
    // at most one daemon reported from here on, so at most one summary is known
    let mut failed_summary = None;
    for (role, end) in [("Source", source), ("Destination", dest)] {
        match end {
            DaemonEnd::Reported(result) => {
                if let remote::protocol::RcpdResult::Failure { error, summary, .. } = *result {
                    // rcp-error-log-allow: RcpdResult::Failure.error is a String off the wire, not a chain
                    tracing::error!("{role} rcpd failed: {error}");
                    findings.push(format!("{role}: {error}"));
                    failed_summary = Some(summary);
                }
            }
            DaemonEnd::Lost(diagnostic) => {
                findings.push(format!("{diagnostic:#}"));
                lost.push(diagnostic);
            }
            DaemonEnd::Abandoned => {}
        }
    }
    // a lost daemon with nothing beside it keeps its diagnostic as it is, chain included
    if findings.len() == 1
        && let Some(diagnostic) = lost.pop()
    {
        return Err(diagnostic);
    }
    // two findings stand side by side: a chain would make one the cause of the other, and which
    // of them is the cause is not known here
    let findings = findings.join("; ");
    Err(match failed_summary {
        Some(summary) => {
            common::copy::Error::new(anyhow!("rcpd operation(s) failed: {findings}"), summary)
                .into()
        }
        // not reached from the waits in this file: a daemon is abandoned only after its peer
        // failed or was lost
        None if findings.is_empty() => {
            anyhow!("remote copy ended without a result from both rcpd processes")
        }
        None => anyhow!("{findings}"),
    })
}

/// Wait out the source once the destination has ended, and settle.
async fn finish_after_dest_terminal<W, SourceFut>(
    dest: DaemonEnd,
    source_fut: std::pin::Pin<&mut SourceFut>,
    source_stdin: &mut Option<remote::stdio::MasterStdinSender<W>>,
    source_host: &str,
    peer_failure_grace: std::time::Duration,
) -> anyhow::Result<(remote::protocol::RcpdResult, remote::protocol::RcpdResult)>
where
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    SourceFut: std::future::Future<Output = anyhow::Result<Option<remote::protocol::RcpdResult>>>,
{
    let source = match dest.peer_wait(peer_failure_grace) {
        None => DaemonEnd::from(decode_daemon_result(
            source_fut.await,
            "source",
            source_host,
        )),
        Some(grace) => match tokio::time::timeout(grace, source_fut).await {
            Ok(result) => DaemonEnd::from(decode_daemon_result(result, "source", source_host)),
            Err(_) => DaemonEnd::Abandoned,
        },
    };
    // a source that has not reported is not waited for any longer
    if !matches!(source, DaemonEnd::Reported(_))
        && let Some(stdin) = source_stdin.take()
    {
        stdin.close().await;
    }
    settle_daemon_ends(source, dest)
}

/// Wait out the destination once the source has ended.
async fn dest_end_after_source(
    source: &DaemonEnd,
    dest_control_rx: &mut remote::stdio::MasterControlReceiver,
    dest_host: &str,
    peer_failure_grace: std::time::Duration,
) -> DaemonEnd {
    let event = match source.peer_wait(peer_failure_grace) {
        None => dest_control_rx.recv_destination_event().await,
        Some(grace) => {
            match tokio::time::timeout(grace, dest_control_rx.recv_destination_event()).await {
                Ok(event) => event,
                Err(_) => return DaemonEnd::Abandoned,
            }
        }
    };
    match decode_destination_event(event, dest_host) {
        Ok(remote::stdio::DestinationControlEvent::Result(dest_result)) => {
            DaemonEnd::Reported(dest_result)
        }
        Ok(remote::stdio::DestinationControlEvent::DirectConnectFailed { reason }) => {
            if matches!(
                source.reported(),
                Some(remote::protocol::RcpdResult::Success { .. })
            ) {
                DaemonEnd::Lost(anyhow!(
                    "destination rcpd on '{dest_host}' reported DirectConnectFailed ({reason}) after source already succeeded"
                ))
            } else {
                // still looking for a way to a source that has ended: it has nothing to report
                tracing::debug!(
                    "destination on '{dest_host}' reported DirectConnectFailed ({reason}) after source ended; closing destination stdin"
                );
                DaemonEnd::Abandoned
            }
        }
        Err(diagnostic) => DaemonEnd::Lost(diagnostic),
    }
}

#[allow(clippy::too_many_arguments)]
async fn await_daemon_results_with_fallback<W, FallbackOpener, FallbackFut>(
    source_control_rx: &mut remote::stdio::MasterControlReceiver,
    dest_control_rx: &mut remote::stdio::MasterControlReceiver,
    source_stdin: &mut Option<remote::stdio::MasterStdinSender<W>>,
    dest_stdin: &mut Option<remote::stdio::MasterStdinSender<W>>,
    mut open_fallback: Option<FallbackOpener>,
    source_host: &str,
    dest_host: &str,
    peer_failure_grace: std::time::Duration,
) -> anyhow::Result<(remote::protocol::RcpdResult, remote::protocol::RcpdResult)>
where
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    FallbackOpener: FnOnce(String) -> FallbackFut,
    FallbackFut: std::future::Future<
            Output = anyhow::Result<(std::net::SocketAddr, Vec<std::net::SocketAddr>)>,
        >,
{
    let source_fut = source_control_rx.recv_result();
    tokio::pin!(source_fut);
    loop {
        tokio::select! {
            // biased: `source_fut` resolves at most once (final RcpdResult) and immediately exits,
            // so preferring it cannot starve destination events and avoids launching SSH fallback
            // if source has already failed
            biased;
            src_res = &mut source_fut => {
                let source =
                    DaemonEnd::from(decode_daemon_result(src_res, "source", source_host));
                let dest =
                    dest_end_after_source(&source, dest_control_rx, dest_host, peer_failure_grace)
                        .await;
                // a destination that has not reported is not waited for any longer
                if !matches!(dest, DaemonEnd::Reported(_))
                    && let Some(stdin) = dest_stdin.take()
                {
                    stdin.close().await;
                }
                return settle_daemon_ends(source, dest);
            }
            dst_event = dest_control_rx.recv_destination_event() => {
                match decode_destination_event(dst_event, dest_host) {
                    Ok(remote::stdio::DestinationControlEvent::DirectConnectFailed { reason }) => {
                        let Some(opener) = open_fallback.take() else {
                            anyhow::bail!(
                                "destination rcpd on '{dest_host}' requested unexpected SSH fallback: {reason}"
                            );
                        };
                        enum FallbackSelect {
                            Opened(
                                anyhow::Result<(std::net::SocketAddr, Vec<std::net::SocketAddr>)>,
                            ),
                            SourceFinished(anyhow::Result<Option<remote::protocol::RcpdResult>>),
                            DestEvent(
                                anyhow::Result<Option<remote::stdio::DestinationControlEvent>>,
                            ),
                        }
                        let selected = {
                            let open_fut = opener(reason.clone());
                            tokio::pin!(open_fut);
                            tokio::select! {
                                biased;
                                src_res = &mut source_fut => FallbackSelect::SourceFinished(src_res),
                                dst_event = dest_control_rx.recv_destination_event() => {
                                    FallbackSelect::DestEvent(dst_event)
                                }
                                opened = &mut open_fut => FallbackSelect::Opened(opened),
                            }
                        };
                        let (control_addr, data_addrs) = match selected {
                            FallbackSelect::SourceFinished(src_res) => {
                                // the destination waits for the tunnels this would have opened:
                                // nothing more comes from it
                                if let Some(stdin) = dest_stdin.take() {
                                    stdin.close().await;
                                }
                                let source = DaemonEnd::from(decode_daemon_result(
                                    src_res,
                                    "source",
                                    source_host,
                                ));
                                if matches!(
                                    source.reported(),
                                    Some(remote::protocol::RcpdResult::Success { .. })
                                ) {
                                    anyhow::bail!(
                                        "source rcpd on '{source_host}' completed while destination on '{dest_host}' had not connected ({reason})"
                                    );
                                }
                                return settle_daemon_ends(source, DaemonEnd::Abandoned);
                            }
                            FallbackSelect::DestEvent(dst_event) => {
                                let dest = match decode_destination_event(dst_event, dest_host) {
                                    Ok(remote::stdio::DestinationControlEvent::Result(res)) => {
                                        DaemonEnd::Reported(res)
                                    }
                                    Ok(
                                        remote::stdio::DestinationControlEvent::DirectConnectFailed {
                                            reason: duplicate,
                                        },
                                    ) => DaemonEnd::Lost(anyhow!(
                                        "destination rcpd on '{dest_host}' sent duplicate DirectConnectFailed: {duplicate}"
                                    )),
                                    Err(diagnostic) => DaemonEnd::Lost(diagnostic),
                                };
                                return finish_after_dest_terminal(
                                    dest,
                                    source_fut.as_mut(),
                                    source_stdin,
                                    source_host,
                                    peer_failure_grace,
                                )
                                .await;
                            }
                            FallbackSelect::Opened(opened) => opened?,
                        };
                        dest_stdin
                            .as_ref()
                            .expect("destination stdin remains open until operation teardown")
                            .send_ssh_fallback_endpoints(control_addr, data_addrs)
                            .await
                            .context("failed to send SSH fallback endpoints to destination rcpd")?;
                    }
                    Ok(remote::stdio::DestinationControlEvent::Result(dest_result)) => {
                        return finish_after_dest_terminal(
                            DaemonEnd::Reported(dest_result),
                            source_fut.as_mut(),
                            source_stdin,
                            source_host,
                            peer_failure_grace,
                        )
                        .await;
                    }
                    Err(diagnostic) => {
                        return finish_after_dest_terminal(
                            DaemonEnd::Lost(diagnostic),
                            source_fut.as_mut(),
                            source_stdin,
                            source_host,
                            peer_failure_grace,
                        )
                        .await;
                    }
                }
            }
        }
    }
}

struct MasterTunnelPlan {
    direction: remote::ssh_tunnel::ForwardDirection,
    target_control: std::net::SocketAddr,
    target_data: Option<std::net::SocketAddr>,
    requested_tunnels: usize,
    client_address_family: remote::ssh_tunnel::SshAddressFamily,
    expected_peer: Option<remote::protocol::PeerIdentity>,
    node_endpoint: Option<std::net::SocketAddr>,
    timeout: std::time::Duration,
}

async fn open_master_tunnels(
    backend: &dyn RemoteBackend,
    plan: &MasterTunnelPlan,
) -> anyhow::Result<OpenedTunnels> {
    let mut expected_peer = plan.expected_peer.clone();
    let mut node_endpoint = plan.node_endpoint;
    // for uploads (-R), probe the destination over its retained control master so the remote `-R`
    // listener follows the forwarding server's actual address family (independent of the client's
    // AddressFamily) and multi-tunnel uploads can pin/verify extra tunnels 2..=K
    if plan.direction == remote::ssh_tunnel::ForwardDirection::Remote
        && (expected_peer.is_none() || node_endpoint.is_none())
    {
        match backend
            .probe_peer_identity(
                remote::protocol::RcpdRole::Destination,
                plan.timeout.min(std::time::Duration::from_secs(5)),
            )
            .await
        {
            Ok((peer, endpoint)) => {
                if expected_peer.is_none() {
                    expected_peer = Some(peer);
                }
                if node_endpoint.is_none() {
                    node_endpoint = endpoint;
                }
            }
            Err(error) => {
                tracing::debug!(
                    "could not probe destination peer identity/endpoint for upload tunnels: {error:#}"
                );
            }
        }
    }
    let listen_address_family = resolve_tunnel_listen_address_family(
        plan.direction,
        plan.client_address_family,
        node_endpoint,
    );
    backend
        .open_tunnels(TunnelRequest {
            direction: plan.direction,
            target_control: plan.target_control,
            target_data: plan.target_data,
            requested_tunnels: plan.requested_tunnels,
            listen_address_family,
            expected_peer,
            node_endpoint,
            timeout: plan.timeout,
        })
        .await
}

fn merge_summaries(
    source: common::copy::Summary,
    dest: common::copy::Summary,
    is_dry_run: bool,
) -> common::copy::Summary {
    // for copy/create/unchanged/remove: destination in normal mode, source in dry-run
    let primary = if is_dry_run { &source } else { &dest };
    common::copy::Summary {
        bytes_copied: primary.bytes_copied,
        files_copied: primary.files_copied,
        symlinks_created: primary.symlinks_created,
        directories_created: primary.directories_created,
        files_unchanged: primary.files_unchanged,
        symlinks_unchanged: primary.symlinks_unchanged,
        directories_unchanged: primary.directories_unchanged,
        // skip counts are always source-only
        files_skipped: source.files_skipped,
        symlinks_skipped: source.symlinks_skipped,
        directories_skipped: source.directories_skipped,
        specials_skipped: source.specials_skipped,
        rm_summary: common::rm::Summary {
            bytes_removed: primary.rm_summary.bytes_removed,
            files_removed: primary.rm_summary.files_removed,
            symlinks_removed: primary.rm_summary.symlinks_removed,
            directories_removed: primary.rm_summary.directories_removed,
            files_skipped: primary.rm_summary.files_skipped,
            symlinks_skipped: primary.rm_summary.symlinks_skipped,
            directories_skipped: primary.rm_summary.directories_skipped,
        },
    }
}

/// Complete session inputs required to run [`run_master_session`].
pub struct MasterSessionPlan {
    pub src_host: String,
    pub src_path: std::path::PathBuf,
    pub dst_host: String,
    pub dst_path: std::path::PathBuf,
    pub source_is_local: bool,
    pub initial_topology: InitialTopology,
    pub transport: RemoteTransport,
    pub no_encryption: bool,
    pub bind_ip: Option<String>,
    pub port_ranges_configured: bool,
    pub ssh_tunnels: Option<std::num::NonZeroUsize>,
    pub remote_direct_probe_timeout_sec: u64,
    pub dry_run: Option<common::config::DryRunMode>,
    pub preserve: common::preserve::Settings,
    pub resolved_src_ssh: Option<remote::ssh_tunnel::ResolvedSshConfig>,
    pub resolved_dst_ssh: Option<remote::ssh_tunnel::ResolvedSshConfig>,
    pub request: MasterRemoteRequest,
}

/// Result of [`run_master_session`], returning remote runtime stats separately instead of mutating
/// process-global display state.
pub struct MasterSessionOutcome {
    pub result: anyhow::Result<common::copy::Summary>,
    pub runtime_stats: Option<common::RemoteRuntimeStats>,
}

impl MasterSessionOutcome {
    fn err(error: anyhow::Error) -> Self {
        Self {
            result: Err(error),
            runtime_stats: None,
        }
    }
}

/// Run the full master orchestration protocol against `backend`.
#[tracing::instrument(
    skip(plan, backend),
    fields(src_host = %plan.src_host, dst_host = %plan.dst_host, transport = ?plan.transport)
)]
pub async fn run_master_session(
    plan: MasterSessionPlan,
    backend: &mut dyn RemoteBackend,
) -> MasterSessionOutcome {
    tracing::debug!("running rcpd src/dst");
    let MasterSessionPlan {
        src_host,
        src_path,
        dst_host,
        dst_path,
        source_is_local,
        initial_topology,
        transport,
        no_encryption,
        bind_ip,
        port_ranges_configured,
        ssh_tunnels,
        remote_direct_probe_timeout_sec,
        dry_run,
        preserve,
        resolved_src_ssh,
        resolved_dst_ssh,
        request,
    } = plan;
    let bootstrap_timeout = std::time::Duration::from_secs(request.tcp.conn_timeout_sec);
    let keepalive_sec = request.tcp.keepalive_sec;
    let mut teardown = SessionTeardown::default();
    let source_config = build_source_remote_config(&request);
    let probe_timeout = bootstrap_timeout.min(std::time::Duration::from_secs(5));
    let (topology, source_ssh_endpoint) = if transport == RemoteTransport::Tcp {
        (initial_topology.resolve(None), None)
    } else {
        match initial_topology {
            InitialTopology::SameEndpointNeedsPeerCheck => {
                // independently opened sessions (e.g., alice@login-pool vs bob@login-pool) may
                // land on different load-balanced backends even though `ssh -G` resolves to the
                // same endpoint; verify their actual peers before taking the SameHost shortcut
                let (src_probe, dst_probe) = tokio::join!(
                    backend.probe_peer_identity(remote::protocol::RcpdRole::Source, probe_timeout),
                    backend.probe_peer_identity(
                        remote::protocol::RcpdRole::Destination,
                        probe_timeout
                    ),
                );
                match (src_probe, dst_probe) {
                    (Ok((src_peer, src_ep)), Ok((dst_peer, _dst_ep))) => {
                        let resolved = initial_topology.resolve(Some((&src_peer, &dst_peer)));
                        if resolved == CopyTopology::SameHost {
                            tracing::debug!(
                                host = %src_host,
                                "verified co-located peer identity across independent SSH sessions; using SameHost direct transport"
                            );
                            backend.disable_destination_agent_forward();
                        } else {
                            tracing::info!(
                                src_host = %src_host,
                                dst_host = %dst_host,
                                src_peer = %src_peer.hostname,
                                dst_peer = %dst_peer.hostname,
                                "independent SSH sessions resolved to the same endpoint but landed on different peers; using ThreeHost transport"
                            );
                        }
                        (resolved, src_ep)
                    }
                    (src_res, dst_res) => {
                        let src_ep = match src_res {
                            Ok((_, ep)) => ep,
                            Err(error) => {
                                tracing::debug!(
                                    host = %src_host,
                                    "could not verify source peer identity for same-endpoint check (using ThreeHost): {error:#}"
                                );
                                None
                            }
                        };
                        if let Err(error) = dst_res {
                            tracing::debug!(
                                host = %dst_host,
                                "could not verify destination peer identity for same-endpoint check (using ThreeHost): {error:#}"
                            );
                        }
                        (CopyTopology::ThreeHost, src_ep)
                    }
                }
            }
            other => {
                let topology = other.resolve(None);
                // whenever source is remote, probe the live SSH connection's `SSH_CONNECTION`
                // server endpoint so `source rcpd` follows the forwarding server's actual address
                // family (handling both client `any` + server `inet6` and client `inet6` +
                // ProxyCommand -> IPv4 server)
                let src_ep = if !source_is_local {
                    match backend
                        .probe_peer_identity(remote::protocol::RcpdRole::Source, probe_timeout)
                        .await
                    {
                        Ok((_, ep)) => ep,
                        Err(error) => {
                            tracing::debug!(
                                host = %src_host,
                                "could not probe source SSH endpoint address family: {error:#}"
                            );
                            None
                        }
                    }
                } else {
                    None
                };
                (topology, src_ep)
            }
        }
    };
    let backend: &dyn RemoteBackend = &*backend;
    let client_listener_af = match topology {
        CopyTopology::Download => resolved_src_ssh
            .as_ref()
            .map_or(remote::ssh_tunnel::SshAddressFamily::Any, |c| {
                c.address_family
            }),
        CopyTopology::Upload => resolved_dst_ssh
            .as_ref()
            .map_or(remote::ssh_tunnel::SshAddressFamily::Any, |c| {
                c.address_family
            }),
        CopyTopology::ThreeHost | CopyTopology::SameHost => resolved_src_ssh
            .as_ref()
            .map_or(remote::ssh_tunnel::SshAddressFamily::Any, |c| {
                c.address_family
            }),
    };
    let source_target_af = resolve_source_target_address_family(
        source_is_local,
        client_listener_af,
        resolved_src_ssh.as_ref(),
        source_ssh_endpoint,
    );
    let source_bind_ip = resolve_source_bind_ip_for_transport(
        transport,
        bind_ip.as_deref(),
        &src_host,
        source_target_af,
    );
    if transport == RemoteTransport::Ssh {
        if port_ranges_configured {
            tracing::warn!(
                target: common::NOTICE_TARGET,
                "--port-ranges is ignored when --remote-transport=ssh is set; source listens on an ephemeral loopback port"
            );
        }
        if bind_ip.is_some() {
            let bound = source_bind_ip.as_deref().unwrap_or("127.0.0.1");
            tracing::warn!(
                target: common::NOTICE_TARGET,
                "--bind-ip is ignored when --remote-transport=ssh is set; source binds to {bound}"
            );
        }
    }
    let three_host_source_ssh =
        if topology == CopyTopology::ThreeHost && transport != RemoteTransport::Tcp {
            match resolved_src_ssh.as_ref() {
                Some(cfg) => Some(cfg.target.clone()),
                None => {
                    return MasterSessionOutcome::err(anyhow!(
                        "three-host SSH/auto transport requires resolved source SSH config"
                    ));
                }
            }
        } else {
            None
        };
    let SpawnedDaemon {
        readiness: source_readiness,
        stdin: raw_source_stdin,
        stdout: raw_source_stdout,
        handle: source_handle,
    } = match backend
        .spawn_rcpd(
            remote::protocol::RcpdRole::Source,
            &source_config,
            source_bind_ip.as_deref(),
        )
        .instrument(tracing::trace_span!(
            "start_rcpd",
            host = %src_host,
            role = ?remote::protocol::RcpdRole::Source
        ))
        .await
    {
        Ok(daemon) => daemon,
        Err(error) => return MasterSessionOutcome::err(error),
    };
    teardown.retain_daemon(source_handle);
    let mut source_stdin = Some(remote::stdio::MasterStdinSender::new(
        raw_source_stdin,
        keepalive_sec,
    ));
    let (mut source_control_rx, source_stdout_demux) = remote::stdio::spawn_master_stdout_demux(
        remote::streams::RecvStream::new(raw_source_stdout),
        remote::tracelog::RcpdType::Source,
    );
    // start draining immediately. Source startup notices are already queued by this point and must
    // remain visible even if any part of destination bring-up fails
    teardown.retain_tracing_receiver(source_stdout_demux, remote::tracelog::RcpdType::Source);
    tracing::info!(
        "Source rcpd ready (tls_cert={})",
        source_readiness.fingerprint.is_some()
    );
    let requested_tunnels = resolve_requested_ssh_tunnels(
        ssh_tunnels,
        source_readiness.max_connections,
        request.rcpd.preview_only,
    );
    let mut dest_stdin: Option<remote::stdio::MasterStdinSender> = None;
    let destination_setup = async {
        validate_readiness_encryption("source", request.rcpd.encryption, &source_readiness)?;
        let mut destination_config = build_destination_remote_config(&request, &source_readiness)?;
        if topology == CopyTopology::ThreeHost
            && transport != RemoteTransport::Tcp
            && !request.rcpd.preview_only
        {
            destination_config.ssh_tunnels = requested_tunnels;
        }
        if let Some(notice) = remote_stream_clamp_notice(
            request.concurrency_policy,
            source_readiness.files_in_flight,
            request.connection_ceiling,
            source_readiness.max_connections,
        ) {
            tracing::warn!(target: common::NOTICE_TARGET, "{notice}");
        }
        tracing::info!(
            "Effective remote connection count: {}",
            source_readiness.max_connections
        );
        let SpawnedDaemon {
            readiness: destination_readiness,
            stdin: raw_dest_stdin,
            stdout: raw_dest_stdout,
            handle: dest_handle,
        } = backend
            .spawn_rcpd(
                remote::protocol::RcpdRole::Destination,
                &destination_config,
                None,
            )
            .instrument(tracing::trace_span!(
                "start_rcpd",
                host = %dst_host,
                role = ?remote::protocol::RcpdRole::Destination
            ))
            .await?;
        // retain process ownership before validation introduces another exit
        teardown.retain_daemon(dest_handle);
        dest_stdin = Some(remote::stdio::MasterStdinSender::new(
            raw_dest_stdin,
            keepalive_sec,
        ));
        let (dest_control_rx, dest_stdout_demux) = remote::stdio::spawn_master_stdout_demux(
            remote::streams::RecvStream::new(raw_dest_stdout),
            remote::tracelog::RcpdType::Destination,
        );
        // retain destination stdout demux before validation so destination startup notices drain
        // symmetrically with source even if readiness validation rejects the destination
        teardown
            .retain_tracing_receiver(dest_stdout_demux, remote::tracelog::RcpdType::Destination);
        validate_destination_readiness(&source_readiness, &destination_readiness)?;
        validate_readiness_encryption(
            "destination",
            request.rcpd.encryption,
            &destination_readiness,
        )?;
        tracing::info!(
            "Destination rcpd ready (tls_cert={})",
            destination_readiness.fingerprint.is_some()
        );
        anyhow::Ok((destination_readiness, dest_control_rx))
    }
    .await;
    let (destination_readiness, mut dest_control_rx) = match destination_setup {
        Ok(setup) => setup,
        Err(error) => {
            if let Some(stdin) = source_stdin.take() {
                stdin.close().await;
            }
            if let Some(stdin) = dest_stdin.take() {
                stdin.close().await;
            }
            drop(source_control_rx);
            let _ = teardown.finish(false).await;
            return MasterSessionOutcome::err(error);
        }
    };
    tracing::info!("Connected to all rcpd processes");
    let effective_probe_timeout_sec =
        remote_direct_probe_timeout_sec.min(request.tcp.conn_timeout_sec);
    let accept_timeout_sec = source_accept_timeout_sec(
        transport,
        topology,
        request.tcp.conn_timeout_sec,
        effective_probe_timeout_sec,
    );
    let operation_result = async {
        let filter = request.filter;
        let (source_peer_auth, destination_peer_auth) = plan_peer_auth(
            no_encryption,
            transport,
            topology,
            source_bind_ip.as_deref(),
            source_readiness.fingerprint,
            destination_readiness.fingerprint,
        )?;
        // send MasterHello to source rcpd with stream authentication policy
        source_stdin
            .as_ref()
            .expect("source stdin remains open until operation teardown")
            .send_hello(remote::protocol::MasterHello::Source {
                src: src_path.clone(),
                dst: dst_path.clone(),
                peer_auth: source_peer_auth,
                filter,
                dry_run,
                // derived from the SAME `preserve` the destination is handed below, so what
                // the source reads and what the destination applies cannot drift apart
                capture: remote::protocol::ExtendedMetadataCapture::for_preserve(&preserve),
                accept_timeout_sec,
            })
            .instrument(tracing::trace_span!("send_master_hello_to_source"))
            .await?;
        tracing::debug!("Waiting for source rcpd to send hello");
        let source_hello = async {
            // built once and used by BOTH arms, so the two cannot drift into describing the same
            // event differently — which is the whole point of reporting them alike
            let went_quiet = rcpd_went_quiet("source", &src_host, "send its hello");
            let closed_quietly = rcpd_closed_quietly("source", &src_host, "send its hello");
            match source_control_rx
                .recv_source_hello()
                .await
                .with_context(|| went_quiet)?
                .ok_or_else(|| anyhow!(closed_quietly))?
            {
                remote::stdio::SourceHelloOutcome::Hello(hello) => Ok(hello),
                remote::stdio::SourceHelloOutcome::Result(
                    remote::protocol::RcpdResult::Failure { error, summary, .. },
                ) => {
                    // rcp-error-log-allow: RcpdResult::Failure.error is a String off the wire, not a chain
                    tracing::error!("Source rcpd failed before starting listeners: {error}");
                    Err(common::copy::Error::new(
                        anyhow!("Source rcpd failed: {error}"),
                        summary,
                    )
                    .into())
                }
                remote::stdio::SourceHelloOutcome::Result(
                    remote::protocol::RcpdResult::Success { message, .. },
                ) => {
                    anyhow::bail!(
                        "source rcpd completed unexpectedly before sending its hello: {message}"
                    );
                }
            }
        }
        .instrument(tracing::trace_span!("recv_source_hello"))
        .await?;
        let build_master_tunnel_plan = |direction| {
            let (expected_peer, node_endpoint) = match direction {
                remote::ssh_tunnel::ForwardDirection::Local => (
                    Some(source_hello.peer_identity.clone()),
                    source_hello.ssh_node_endpoint,
                ),
                remote::ssh_tunnel::ForwardDirection::Remote => (None, None),
            };
            let target_ip = source_hello.tunnel_target_ip;
            MasterTunnelPlan {
                direction,
                target_control: std::net::SocketAddr::new(
                    target_ip,
                    source_hello.control_addr.port(),
                ),
                target_data: (!request.rcpd.preview_only).then(|| {
                    std::net::SocketAddr::new(target_ip, source_hello.data_addr.port())
                }),
                requested_tunnels,
                client_address_family: client_listener_af,
                expected_peer,
                node_endpoint,
                timeout: bootstrap_timeout,
            }
        };
        let direct_data_addrs = if request.rcpd.preview_only {
            Vec::new()
        } else {
            vec![source_hello.data_addr]
        };
        let (dest_control_addr, dest_data_addrs, dest_transport, master_fallback) =
            match (transport, topology) {
                (RemoteTransport::Tcp, _) | (_, CopyTopology::SameHost) => (
                    source_hello.control_addr,
                    direct_data_addrs,
                    remote::protocol::DestinationTransport::Direct,
                    None,
                ),
                (RemoteTransport::Ssh, CopyTopology::Download | CopyTopology::Upload) => {
                    let direction = match topology {
                        CopyTopology::Download => remote::ssh_tunnel::ForwardDirection::Local,
                        CopyTopology::Upload => remote::ssh_tunnel::ForwardDirection::Remote,
                        _ => unreachable!("matched Download or Upload above"),
                    };
                    let plan = build_master_tunnel_plan(direction);
                    let tunnels = open_master_tunnels(backend, &plan).await?;
                    tracing::warn!(
                        target: common::NOTICE_TARGET,
                        "Transferring file data over SSH ({}); throughput will be lower than direct TCP",
                        tunnels.summary
                    );
                    let control_addr = tunnels.control_addr;
                    let data_addrs = tunnels.data_addrs;
                    teardown.retain_tunnels(tunnels.handle);
                    (
                        control_addr,
                        data_addrs,
                        remote::protocol::DestinationTransport::MasterSshTunnel,
                        None,
                    )
                }
                (RemoteTransport::Auto, CopyTopology::Download | CopyTopology::Upload) => {
                    let direction = match topology {
                        CopyTopology::Download => remote::ssh_tunnel::ForwardDirection::Local,
                        CopyTopology::Upload => remote::ssh_tunnel::ForwardDirection::Remote,
                        _ => unreachable!("matched Download or Upload above"),
                    };
                    (
                        source_hello.control_addr,
                        direct_data_addrs,
                        remote::protocol::DestinationTransport::AutoMasterSsh {
                            probe_timeout_sec: effective_probe_timeout_sec,
                        },
                        Some(build_master_tunnel_plan(direction)),
                    )
                }
                (RemoteTransport::Ssh, CopyTopology::ThreeHost) => (
                    source_hello.control_addr,
                    direct_data_addrs,
                    remote::protocol::DestinationTransport::DirectSsh {
                        source_ssh: three_host_source_ssh
                            .expect("three-host SSH resolves source SSH config before spawn"),
                        tunnel_target_ip: source_hello.tunnel_target_ip,
                        ssh_tunnels: requested_tunnels,
                        dest_root: dst_path.clone(),
                        source_peer: source_hello.peer_identity.clone(),
                        source_node_endpoint: source_hello.ssh_node_endpoint,
                    },
                    None,
                ),
                (RemoteTransport::Auto, CopyTopology::ThreeHost) => (
                    source_hello.control_addr,
                    direct_data_addrs,
                    remote::protocol::DestinationTransport::AutoDirectSsh {
                        probe_timeout_sec: effective_probe_timeout_sec,
                        source_ssh: three_host_source_ssh
                            .expect("three-host auto resolves source SSH config before spawn"),
                        tunnel_target_ip: source_hello.tunnel_target_ip,
                        ssh_tunnels: requested_tunnels,
                        dest_root: dst_path.clone(),
                        source_peer: source_hello.peer_identity.clone(),
                        source_node_endpoint: source_hello.ssh_node_endpoint,
                    },
                    None,
                ),
            };
        // send MasterHello to destination rcpd with stream authentication policy
        dest_stdin
            .as_ref()
            .expect("destination stdin remains open until operation teardown")
            .send_hello(remote::protocol::MasterHello::Destination {
                source_control_addr: dest_control_addr,
                source_data_addrs: dest_data_addrs,
                server_name: source_hello.server_name.clone(),
                preserve,
                peer_auth: destination_peer_auth,
                dry_run: request.rcpd.preview_only,
                transport: Box::new(dest_transport),
            })
            .instrument(tracing::trace_span!("send_master_hello_to_dest"))
            .await?;
        tracing::info!("Forwarded source connection info to destination");
        let open_fallback = master_fallback.map(|plan| {
            let teardown = &mut teardown;
            move |reason: String| async move {
                let tunnels = open_master_tunnels(backend, &plan)
                    .await
                    .with_context(|| {
                        format!("SSH fallback failed after direct TCP connection failed ({reason})")
                    })?;
                tracing::warn!(
                    target: common::NOTICE_TARGET,
                    "Direct TCP connection failed ({reason}); falling back to SSH ({}); throughput will be lower than direct TCP",
                    tunnels.summary
                );
                let control_addr = tunnels.control_addr;
                let data_addrs = tunnels.data_addrs;
                teardown.retain_tunnels(tunnels.handle);
                anyhow::Ok((control_addr, data_addrs))
            }
        });
        await_daemon_results_with_fallback(
            &mut source_control_rx,
            &mut dest_control_rx,
            &mut source_stdin,
            &mut dest_stdin,
            open_fallback,
            &src_host,
            &dst_host,
            PEER_FAILURE_RESULT_GRACE,
        )
        .await
    }
    .await;
    if let Some(stdin) = source_stdin.take() {
        stdin.close().await;
    }
    if let Some(stdin) = dest_stdin.take() {
        stdin.close().await;
    }
    drop(source_control_rx);
    drop(dest_control_rx);
    let teardown_result = teardown.finish(operation_result.is_ok()).await;
    let (source_result, dest_result) = match operation_result {
        Ok(results) => results,
        Err(error) => return MasterSessionOutcome::err(error),
    };
    tracing::debug!("Received RcpdResult from both source and destination rcpds");
    // check for failures and collect error details + runtime stats
    let mut errors = Vec::new();
    let (source_summary, source_runtime_stats) = match source_result {
        remote::protocol::RcpdResult::Success {
            message,
            summary,
            runtime_stats,
        } => {
            tracing::info!("Source rcpd completed successfully: {message}");
            (summary, runtime_stats)
        }
        remote::protocol::RcpdResult::Failure {
            error,
            summary,
            runtime_stats,
            peer_connected: _,
        } => {
            // rcp-error-log-allow: RcpdResult::Failure.error is a String off the wire, not a chain
            tracing::error!("Source rcpd failed: {error}");
            errors.push(format!("Source: {error}"));
            (summary, runtime_stats)
        }
    };
    let (dest_summary, dest_runtime_stats) = match dest_result {
        remote::protocol::RcpdResult::Success {
            message,
            summary,
            runtime_stats,
        } => {
            tracing::info!("Destination rcpd completed successfully: {message}");
            (summary, runtime_stats)
        }
        remote::protocol::RcpdResult::Failure {
            error,
            summary,
            runtime_stats,
            peer_connected: _,
        } => {
            // rcp-error-log-allow: RcpdResult::Failure.error is a String off the wire, not a chain
            tracing::error!("Destination rcpd failed: {error}");
            errors.push(format!("Destination: {error}"));
            (summary, runtime_stats)
        }
    };
    let runtime_stats = common::RemoteRuntimeStats {
        source_host: src_host,
        source_stats: source_runtime_stats,
        dest_host: dst_host,
        dest_stats: dest_runtime_stats,
    };
    // merge source and destination summaries:
    // - in dry-run mode the source does all the counting (destination is idle)
    // - in normal mode the destination is authoritative for copy/create/unchanged/remove
    //   counts and the source is authoritative for skip counts
    let summary = merge_summaries(source_summary, dest_summary, dry_run.is_some());
    // preserve copy failures ahead of daemon shutdown failures
    if !errors.is_empty() {
        let combined_error = errors.join("; ");
        // rcp-error-log-allow: already-rendered messages joined into a String, not a chain
        tracing::error!("rcpd operation(s) failed: {combined_error}");
        return MasterSessionOutcome {
            result: Err(common::copy::Error::new(
                anyhow::anyhow!("rcpd operation(s) failed: {combined_error}"),
                summary,
            )
            .into()),
            runtime_stats: Some(runtime_stats),
        };
    }
    let result = match teardown_result {
        Ok(()) => Ok(summary),
        Err(error) => Err(common::copy::Error::new(error, summary).into()),
    };
    MasterSessionOutcome {
        result,
        runtime_stats: Some(runtime_stats),
    }
}

#[cfg(test)]
mod tests;
