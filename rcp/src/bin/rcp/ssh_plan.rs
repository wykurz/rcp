//! Master-side SSH transport, topology, address-family, and tunnel planning.

use anyhow::{Context, anyhow};
use rcp_tools_rcp::path;

/// Transport mode for source↔destination control and data connections in remote copies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub(super) enum RemoteTransport {
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

pub(super) const DEFAULT_SSH_TUNNELS: usize = 4;
pub(super) const DEFAULT_REMOTE_DIRECT_PROBE_TIMEOUT_SEC: u64 = 3;

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
pub(super) fn resolve_source_target_address_family(
    source_is_local: bool,
    client_listener_af: remote::ssh_tunnel::SshAddressFamily,
    resolved_src_ssh: Option<&remote::ssh_tunnel::ResolvedSshConfig>,
    source_ssh_endpoint: Option<std::net::SocketAddr>,
) -> remote::ssh_tunnel::SshAddressFamily {
    if source_is_local {
        // upload (-R): local `ssh` dials local `source rcpd`, so the local client's AddressFamily
        // governs the loopback family `source rcpd` binds.
        return client_listener_af;
    }
    // download (-L) / 3-host / same-host: remote `source sshd` dials `source rcpd`'s loopback
    // listener. Follow the forwarding server's actual address family observed from `SSH_CONNECTION`
    // (handles both client `any` + server `inet6` and client `inet6` + ProxyCommand -> IPv4
    // server). Only fall back to client SSH config when the server endpoint probe is unavailable.
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
        // configured AddressFamily.
        remote::ssh_tunnel::ForwardDirection::Local => client_address_family,
        // upload (-R): the remote `sshd` server opens the listening socket. Prefer the server's
        // ingress address family from `SSH_CONNECTION` (falling back once to the alternate literal
        // loopback inside `MuxClient::open_forward` if the server rejects it, e.g. under
        // `PermitListen 127.0.0.1:*` on an IPv6 session).
        remote::ssh_tunnel::ForwardDirection::Remote => match server_ssh_endpoint {
            Some(ep) if ep.is_ipv6() => remote::ssh_tunnel::SshAddressFamily::Inet6,
            Some(_) => remote::ssh_tunnel::SshAddressFamily::Inet,
            None => remote::ssh_tunnel::SshAddressFamily::Any,
        },
    }
}

/// Resolve the source `rcpd` `--bind-ip` value for the selected remote transport mode.
#[must_use]
pub(super) fn resolve_source_bind_ip_for_transport(
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
        // reachable for IPv6 SSH `-L`/`-R` fallback.
        RemoteTransport::Auto => explicit_bind_ip.map(str::to_owned).or_else(|| {
            (address_family == remote::ssh_tunnel::SshAddressFamily::Inet6)
                .then(|| "::".to_string())
        }),
        RemoteTransport::Tcp => resolve_source_bind_ip(explicit_bind_ip, source_host),
    }
}

/// Resolved remote copy topology used to choose direct vs tunneled data transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CopyTopology {
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
pub(super) enum InitialTopology {
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
    pub(super) fn resolve(
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

/// Parsed remote copy operands and local path exclusions for SSH control sockets.
#[derive(Debug)]
pub(super) struct RemoteCopyOperands {
    pub(super) source: path::RemotePath,
    pub(super) destination: path::RemotePath,
    pub(super) source_is_local: bool,
    destination_is_local: bool,
    pub(super) local_operand_roots: Vec<std::path::PathBuf>,
}

impl RemoteCopyOperands {
    /// Classify preliminary topology from operand locality and optional `ssh -G` configs.
    #[must_use]
    pub(super) fn initial_topology(
        &self,
        resolved_src: Option<&remote::ssh_tunnel::ResolvedSshConfig>,
        resolved_dst: Option<&remote::ssh_tunnel::ResolvedSshConfig>,
    ) -> InitialTopology {
        match (self.source_is_local, self.destination_is_local) {
            (false, true) => InitialTopology::Download,
            (true, false) => InitialTopology::Upload,
            (false, false) if self.source.session() == self.destination.session() => {
                // identical (user, host, port) shares one SSH session, guaranteeing the same host
                InitialTopology::SameHostSharedSession
            }
            (false, false) => match (resolved_src, resolved_dst) {
                (Some(src), Some(dst)) if src.same_endpoint(dst) => {
                    InitialTopology::SameEndpointNeedsPeerCheck
                }
                _ => InitialTopology::ThreeHost,
            },
            (true, true) => unreachable!("remote_copy_operands never accepts two local paths"),
        }
    }
}

/// Determine whether the destination SSH master should forward the local SSH agent.
#[must_use]
fn destination_agent_forward_mode(
    operands: &RemoteCopyOperands,
    transport: RemoteTransport,
    cli_forward_agent: bool,
    resolved_dst_ssh: Option<&remote::ssh_tunnel::ResolvedSshConfig>,
) -> remote::AgentForwardMode {
    if transport == RemoteTransport::Tcp
        || operands.source_is_local
        || operands.destination_is_local
        || operands.source.session() == operands.destination.session()
    {
        return remote::AgentForwardMode::Off;
    }
    if cli_forward_agent {
        remote::AgentForwardMode::ForceOn
    } else if resolved_dst_ssh.is_some_and(|dst_cfg| dst_cfg.forward_agent_enabled) {
        remote::AgentForwardMode::InheritConfig
    } else {
        remote::AgentForwardMode::Off
    }
}

/// Master-side SSH configuration, agent-forwarding policy, and any pre-authenticated destination
/// master session prepared before endpoint bootstrap.
pub(super) struct ResolvedMasterSshPlan {
    pub(super) resolved_src_ssh: Option<remote::ssh_tunnel::ResolvedSshConfig>,
    pub(super) resolved_dst_ssh: Option<remote::ssh_tunnel::ResolvedSshConfig>,
    pub(super) dest_agent_forward_mode: remote::AgentForwardMode,
    pub(super) preauthenticated_destination: Option<remote::PreauthenticatedDestination>,
}

impl ResolvedMasterSshPlan {
    /// Resolve local `ssh -G` configs, authenticate any agent-forwarded destination up front
    /// under its configured `StrictHostKeyChecking` policy before any source connection can touch
    /// `known_hosts`, expand any remote `~` operands, and validate absolute paths.
    pub(super) async fn prepare(
        operands: &mut RemoteCopyOperands,
        transport: RemoteTransport,
        cli_forward_agent: bool,
        bootstrap_timeout: std::time::Duration,
        keepalive_sec: u64,
        cleanup: &remote::RemoteCleanup,
    ) -> anyhow::Result<Self> {
        let (resolved_src_ssh, resolved_dst_ssh) =
            resolve_ssh_configs_for_operands(operands, transport, bootstrap_timeout, cleanup)
                .await?;
        let dest_agent_forward_mode = destination_agent_forward_mode(
            operands,
            transport,
            cli_forward_agent,
            resolved_dst_ssh.as_ref(),
        );
        let preauthenticated_destination =
            if dest_agent_forward_mode != remote::AgentForwardMode::Off {
                Some(
                    remote::PreauthenticatedDestination::connect(
                        operands.destination.session(),
                        cleanup,
                        bootstrap_timeout,
                        keepalive_sec,
                        &operands.local_operand_roots,
                        dest_agent_forward_mode,
                    )
                    .await?,
                )
            } else {
                None
            };
        {
            let remote_src = &mut operands.source;
            let remote_dst = &mut operands.destination;
            // expand remote '~' using remote HOME if needed
            let same_session = remote_src.session() == remote_dst.session();
            if same_session && (remote_src.needs_remote_home() || remote_dst.needs_remote_home()) {
                let home = remote::get_remote_home_for_session_with_timeout(
                    remote_src.session(),
                    cleanup,
                    bootstrap_timeout,
                    keepalive_sec,
                    &operands.local_operand_roots,
                )
                .await?;
                remote_src.apply_remote_home(&home);
                remote_dst.apply_remote_home(&home);
            } else {
                if remote_dst.needs_remote_home() {
                    let home = if let Some(preauth) = preauthenticated_destination.as_ref() {
                        preauth
                            .get_remote_home_with_timeout(bootstrap_timeout)
                            .await?
                    } else {
                        remote::get_remote_home_for_session_with_timeout(
                            remote_dst.session(),
                            cleanup,
                            bootstrap_timeout,
                            keepalive_sec,
                            &operands.local_operand_roots,
                        )
                        .await?
                    };
                    remote_dst.apply_remote_home(&home);
                }
                if remote_src.needs_remote_home() {
                    let home = remote::get_remote_home_for_session_with_timeout(
                        remote_src.session(),
                        cleanup,
                        bootstrap_timeout,
                        keepalive_sec,
                        &operands.local_operand_roots,
                    )
                    .await?;
                    remote_src.apply_remote_home(&home);
                }
            }
        }
        if !operands.source.path().is_absolute() || !operands.destination.path().is_absolute() {
            return Err(anyhow!(
                "Remote paths must be absolute after expansion: src={:?}, dst={:?}",
                operands.source.path(),
                operands.destination.path()
            ));
        }
        Ok(Self {
            resolved_src_ssh,
            resolved_dst_ssh,
            dest_agent_forward_mode,
            preauthenticated_destination,
        })
    }
}

async fn resolve_ssh_configs_for_operands(
    operands: &RemoteCopyOperands,
    transport: RemoteTransport,
    bootstrap_timeout: std::time::Duration,
    cleanup: &remote::RemoteCleanup,
) -> anyhow::Result<(
    Option<remote::ssh_tunnel::ResolvedSshConfig>,
    Option<remote::ssh_tunnel::ResolvedSshConfig>,
)> {
    if transport == RemoteTransport::Tcp {
        return Ok((None, None));
    }
    let src = &operands.source;
    let dst = &operands.destination;
    let resolved = match (operands.source_is_local, operands.destination_is_local) {
        (false, true) => {
            let resolved_src = match remote::ssh_tunnel::resolve_ssh_config(
                src.session(),
                bootstrap_timeout,
                cleanup,
            )
            .await
            {
                Ok(resolved) => Some(resolved),
                Err(error) => {
                    tracing::debug!(
                        host = %src.session().host,
                        "could not resolve source SSH config (defaulting AddressFamily to any): {error:#}"
                    );
                    None
                }
            };
            (resolved_src, None)
        }
        (true, false) => {
            let resolved_dst = match remote::ssh_tunnel::resolve_ssh_config(
                dst.session(),
                bootstrap_timeout,
                cleanup,
            )
            .await
            {
                Ok(resolved) => Some(resolved),
                Err(error) => {
                    tracing::debug!(
                        host = %dst.session().host,
                        "could not resolve destination SSH config (defaulting AddressFamily to any): {error:#}"
                    );
                    None
                }
            };
            (None, resolved_dst)
        }
        (false, false) if src.session() == dst.session() => {
            let resolved_src = match remote::ssh_tunnel::resolve_ssh_config(
                src.session(),
                bootstrap_timeout,
                cleanup,
            )
            .await
            {
                Ok(resolved) => Some(resolved),
                Err(error) => {
                    tracing::debug!(
                        host = %src.session().host,
                        "could not resolve same-host SSH config (defaulting AddressFamily to any): {error:#}"
                    );
                    None
                }
            };
            (resolved_src, None)
        }
        (false, false) => {
            let resolve_src = async {
                remote::ssh_tunnel::resolve_ssh_config(src.session(), bootstrap_timeout, cleanup)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to resolve SSH configuration for source host '{}'",
                            src.session().host
                        )
                    })
            };
            let resolve_dst = async {
                remote::ssh_tunnel::resolve_ssh_config(dst.session(), bootstrap_timeout, cleanup)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to resolve SSH configuration for destination host '{}'",
                            dst.session().host
                        )
                    })
            };
            let (resolved_src, resolved_dst) = tokio::try_join!(resolve_src, resolve_dst)?;
            (Some(resolved_src), Some(resolved_dst))
        }
        (true, true) => unreachable!("remote_copy_operands never accepts two local paths"),
    };
    Ok(resolved)
}

/// Compute how long `source rcpd` waits for destination to connect, accounting for direct-TCP
/// probes and SSH tunnel bring-up budgets.
#[must_use]
pub(super) fn source_accept_timeout_sec(
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

/// Collect canonical local operand roots so SSH control directories avoid overlapping copy paths.
pub(super) fn collect_local_operand_roots(
    sources: &[path::PathType],
    destination: &path::PathType,
) -> anyhow::Result<Vec<std::path::PathBuf>> {
    sources
        .iter()
        .chain(std::iter::once(destination))
        .filter_map(|operand| match operand {
            path::PathType::Local(path) => Some(path),
            path::PathType::Remote(_) => None,
        })
        .map(|path| path::RemotePath::from_local(path).map(|path| path.path().to_path_buf()))
        .collect()
}

/// Normalize a source/destination operand pair into [`RemoteCopyOperands`] when at least one side
/// is remote, returning `Ok(None)` for purely local copies.
pub(super) fn remote_copy_operands(
    source: path::PathType,
    destination: path::PathType,
    local_operand_roots: Vec<std::path::PathBuf>,
) -> anyhow::Result<Option<RemoteCopyOperands>> {
    let operands = match (source, destination) {
        (path::PathType::Remote(source), path::PathType::Remote(destination)) => {
            RemoteCopyOperands {
                source,
                destination,
                source_is_local: false,
                destination_is_local: false,
                local_operand_roots,
            }
        }
        (path::PathType::Remote(source), path::PathType::Local(destination)) => {
            let destination = path::RemotePath::from_local(&destination)?;
            RemoteCopyOperands {
                source,
                destination,
                source_is_local: false,
                destination_is_local: true,
                local_operand_roots,
            }
        }
        (path::PathType::Local(source), path::PathType::Remote(destination)) => {
            let source = path::RemotePath::from_local(&source)?;
            RemoteCopyOperands {
                source,
                destination,
                source_is_local: true,
                destination_is_local: false,
                local_operand_roots,
            }
        }
        (path::PathType::Local(_), path::PathType::Local(_)) => return Ok(None),
    };
    Ok(Some(operands))
}

/// Deferred plan for opening master-managed `-L` (download) or `-R` (upload) SSH tunnels.
pub(super) struct MasterSshTunnelPlan<'a> {
    pub(super) direction: remote::ssh_tunnel::ForwardDirection,
    pub(super) endpoint: &'a remote::PreparedRcpd,
    pub(super) target_control: std::net::SocketAddr,
    pub(super) target_data: Option<std::net::SocketAddr>,
    pub(super) requested_tunnels: usize,
    pub(super) client_address_family: remote::ssh_tunnel::SshAddressFamily,
    pub(super) expected_peer: Option<remote::protocol::PeerIdentity>,
    pub(super) node_endpoint: Option<std::net::SocketAddr>,
    pub(super) keepalive_sec: u64,
    pub(super) timeout: std::time::Duration,
    pub(super) cleanup: &'a remote::RemoteCleanup,
    pub(super) local_operand_roots: &'a [std::path::PathBuf],
}

impl MasterSshTunnelPlan<'_> {
    /// Open `1..=requested_tunnels` master-managed SSH port forwards on demand.
    pub(super) async fn open(&self) -> anyhow::Result<remote::ssh_tunnel::SshTunnelSet> {
        let mut expected_peer = self.expected_peer.clone();
        let mut node_endpoint = self.node_endpoint;
        // for uploads (-R), probe the destination over its retained control master so the remote
        // `-R` listener follows the forwarding server's actual address family (independent of the
        // client's AddressFamily) and multi-tunnel uploads can pin/verify extra tunnels 2..=K.
        if self.direction == remote::ssh_tunnel::ForwardDirection::Remote
            && (expected_peer.is_none() || node_endpoint.is_none())
        {
            match self
                .endpoint
                .probe_peer_identity(self.timeout.min(std::time::Duration::from_secs(5)))
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
            self.direction,
            self.client_address_family,
            node_endpoint,
        );
        remote::ssh_tunnel::SshTunnelSet::open_on_existing_session(
            self.direction,
            &self.endpoint.control_path(),
            self.endpoint.remote(),
            self.target_control,
            self.target_data,
            self.requested_tunnels,
            listen_address_family,
            expected_peer.as_ref(),
            node_endpoint,
            self.keepalive_sec,
            self.timeout,
            self.cleanup,
            self.local_operand_roots,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssh_control_exclusions_follow_local_operand_origin() {
        let remote_source = path::parse_path("host-a:/source").unwrap();
        let remote_destination = path::parse_path("host-b:/destination").unwrap();
        let local_source = path::parse_path("/tmp/local-source").unwrap();
        let local_destination = path::parse_path("/tmp/local-destination").unwrap();
        let default_localhost = path::parse_path("localhost:/tmp/default-localhost").unwrap();
        let forced_localhost =
            path::parse_path_force_remote("localhost:/tmp/forced-localhost").unwrap();
        assert_eq!(
            collect_local_operand_roots(std::slice::from_ref(&local_source), &remote_destination)
                .unwrap(),
            vec![std::path::PathBuf::from("/tmp/local-source")]
        );
        assert_eq!(
            collect_local_operand_roots(std::slice::from_ref(&remote_source), &local_destination)
                .unwrap(),
            vec![std::path::PathBuf::from("/tmp/local-destination")]
        );
        assert!(
            collect_local_operand_roots(std::slice::from_ref(&remote_source), &remote_destination)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            collect_local_operand_roots(
                std::slice::from_ref(&default_localhost),
                &remote_destination,
            )
            .unwrap(),
            vec![std::path::PathBuf::from("/tmp/default-localhost")]
        );
        assert!(
            collect_local_operand_roots(
                std::slice::from_ref(&forced_localhost),
                &remote_destination,
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn resolve_source_bind_ip_prefers_explicit_flag_and_falls_back_to_ipv4_operand() {
        assert_eq!(
            resolve_source_bind_ip(Some("127.0.0.2"), "localhost"),
            Some("127.0.0.2".to_string())
        );
        assert_eq!(
            resolve_source_bind_ip(Some("127.0.0.2"), "127.0.0.1"),
            Some("127.0.0.2".to_string())
        );
        assert_eq!(
            resolve_source_bind_ip(None, "127.0.0.1"),
            Some("127.0.0.1".to_string())
        );
        assert_eq!(resolve_source_bind_ip(None, "localhost"), None);
    }

    #[test]
    fn remote_copy_operands_classify_copy_topology() {
        let remote_a = path::parse_path("host-a:/src").unwrap();
        let remote_a_user = path::parse_path("deploy@host-a:/dst").unwrap();
        let remote_b = path::parse_path("host-b:/dst").unwrap();
        let local_src = path::parse_path("/tmp/src").unwrap();
        let local_dst = path::parse_path("/tmp/dst").unwrap();
        let forced_localhost_src = path::parse_path_force_remote("localhost:/src").unwrap();
        let forced_localhost_dst = path::parse_path_force_remote("localhost:/dst").unwrap();
        let forced_ip_src = path::parse_path_force_remote("127.0.0.1:/src").unwrap();
        let resolved = |user: Option<&str>, host: &str, port: Option<u16>| {
            remote::ssh_tunnel::ResolvedSshConfig {
                original_host: host.to_owned(),
                target: remote::protocol::SshTarget {
                    user: user.map(str::to_owned),
                    host: host.to_owned(),
                    port,
                },
                forward_agent_enabled: false,
                address_family: remote::ssh_tunnel::SshAddressFamily::Any,
                proxy_jump: None,
                proxy_command: None,
            }
        };
        // identical (user, host, port) takes SameHost directly without needing ssh -G or peer probe
        assert_eq!(
            remote_copy_operands(remote_a.clone(), remote_a.clone(), vec![])
                .unwrap()
                .unwrap()
                .initial_topology(None, None),
            InitialTopology::SameHostSharedSession
        );
        assert_eq!(
            InitialTopology::SameHostSharedSession.resolve(None),
            CopyTopology::SameHost
        );
        // different user on the same alias requires live peer verification before taking SameHost
        let alice_cfg = resolved(Some("alice"), "10.0.0.1", Some(22));
        let bob_same_cfg = resolved(Some("deploy"), "10.0.0.1", Some(22));
        let bob_other_host_cfg = resolved(Some("deploy"), "10.0.0.2", Some(22));
        let bob_other_port_cfg = resolved(Some("deploy"), "10.0.0.1", Some(2202));
        let shared_alias = remote_copy_operands(remote_a.clone(), remote_a_user, vec![])
            .unwrap()
            .unwrap();
        let peer_a = remote::protocol::PeerIdentity {
            hostname: "node-a".to_owned(),
            boot_id: Some("boot-1".to_owned()),
            net_ns: Some("net:[1]".to_owned()),
            ssh_server_port: Some(22),
        };
        let peer_b = remote::protocol::PeerIdentity {
            hostname: "node-b".to_owned(),
            ..peer_a.clone()
        };
        let same_endpoint_initial =
            shared_alias.initial_topology(Some(&alice_cfg), Some(&bob_same_cfg));
        assert_eq!(
            same_endpoint_initial,
            InitialTopology::SameEndpointNeedsPeerCheck
        );
        assert_eq!(
            same_endpoint_initial.resolve(Some((&peer_a, &peer_a))),
            CopyTopology::SameHost
        );
        assert_eq!(
            same_endpoint_initial.resolve(Some((&peer_a, &peer_b))),
            CopyTopology::ThreeHost
        );
        assert_eq!(same_endpoint_initial.resolve(None), CopyTopology::ThreeHost);
        assert_eq!(
            shared_alias.initial_topology(Some(&alice_cfg), Some(&bob_other_host_cfg)),
            InitialTopology::ThreeHost
        );
        assert_eq!(
            shared_alias.initial_topology(Some(&alice_cfg), Some(&bob_other_port_cfg)),
            InitialTopology::ThreeHost
        );
        assert_eq!(
            shared_alias.initial_topology(None, None),
            InitialTopology::ThreeHost
        );
        assert_eq!(
            remote_copy_operands(remote_a.clone(), remote_b.clone(), vec![])
                .unwrap()
                .unwrap()
                .initial_topology(Some(&alice_cfg), Some(&bob_other_host_cfg)),
            InitialTopology::ThreeHost
        );
        assert_eq!(
            remote_copy_operands(remote_a, local_dst.clone(), vec![])
                .unwrap()
                .unwrap()
                .initial_topology(None, None),
            InitialTopology::Download
        );
        assert_eq!(
            remote_copy_operands(local_src.clone(), remote_b, vec![])
                .unwrap()
                .unwrap()
                .initial_topology(None, None),
            InitialTopology::Upload
        );
        // --force-remote distinguishes Download (-L), Upload (-R), SameHost, and ThreeHost locally
        assert_eq!(
            remote_copy_operands(forced_localhost_src.clone(), local_dst, vec![])
                .unwrap()
                .unwrap()
                .initial_topology(None, None),
            InitialTopology::Download
        );
        assert_eq!(
            remote_copy_operands(local_src, forced_localhost_dst.clone(), vec![])
                .unwrap()
                .unwrap()
                .initial_topology(None, None),
            InitialTopology::Upload
        );
        assert_eq!(
            remote_copy_operands(forced_localhost_src, forced_localhost_dst.clone(), vec![])
                .unwrap()
                .unwrap()
                .initial_topology(None, None),
            InitialTopology::SameHostSharedSession
        );
        assert_eq!(
            remote_copy_operands(forced_ip_src, forced_localhost_dst, vec![])
                .unwrap()
                .unwrap()
                .initial_topology(None, None),
            InitialTopology::ThreeHost
        );
    }

    #[test]
    fn resolve_source_target_address_family_separates_server_from_client_listener() {
        let any = remote::ssh_tunnel::SshAddressFamily::Any;
        let inet = remote::ssh_tunnel::SshAddressFamily::Inet;
        let inet6 = remote::ssh_tunnel::SshAddressFamily::Inet6;
        let cfg = |host: &str, af: remote::ssh_tunnel::SshAddressFamily| {
            remote::ssh_tunnel::ResolvedSshConfig {
                original_host: host.to_owned(),
                target: remote::protocol::SshTarget {
                    user: None,
                    host: host.to_owned(),
                    port: Some(22),
                },
                forward_agent_enabled: false,
                address_family: af,
                proxy_jump: None,
                proxy_command: None,
            }
        };
        let v6_ep: std::net::SocketAddr = "[::1]:2222".parse().unwrap();
        let v4_ep: std::net::SocketAddr = "127.0.0.1:2222".parse().unwrap();
        // mixed client AddressFamily any + server sshd AddressFamily inet6 (accepted IPv6):
        // download/3-host binds IPv6 on source independently of client listener family
        assert_eq!(
            resolve_source_target_address_family(false, any, Some(&cfg("srv", any)), Some(v6_ep)),
            inet6
        );
        // cross-family proxy: client AddressFamily inet6 reaching IPv4-only sshd via ProxyCommand
        // follows the forwarding server's actual IPv4 family rather than forcing ::1
        assert_eq!(
            resolve_source_target_address_family(
                false,
                inet6,
                Some(&cfg("srv", inet6)),
                Some(v4_ep),
            ),
            inet
        );
        // cross-family proxy in the opposite direction: client AddressFamily inet reaching IPv6
        // sshd follows the server's actual IPv6 family
        assert_eq!(
            resolve_source_target_address_family(false, inet, Some(&cfg("srv", inet)), Some(v6_ep)),
            inet6
        );
        // fallback when probe was unavailable: honor client AddressFamily inet6 or literal IPv6
        assert_eq!(
            resolve_source_target_address_family(false, any, Some(&cfg("::1", any)), None),
            inet6
        );
        assert_eq!(
            resolve_source_target_address_family(false, inet6, Some(&cfg("srv", inet6)), None),
            inet6
        );
        // IPv4 SSH_CONNECTION selects IPv4 on the server side
        assert_eq!(
            resolve_source_target_address_family(false, any, Some(&cfg("srv", any)), Some(v4_ep)),
            inet
        );
        // upload (-R): local source follows local client listener family regardless of remote sshd
        assert_eq!(
            resolve_source_target_address_family(true, any, None, Some(v6_ep)),
            any
        );
        assert_eq!(
            resolve_source_target_address_family(true, inet6, None, Some(v4_ep)),
            inet6
        );
        assert_eq!(
            resolve_source_target_address_family(true, inet, None, None),
            inet
        );
    }

    #[test]
    fn resolve_tunnel_listen_address_family_separates_client_local_from_server_remote() {
        let any = remote::ssh_tunnel::SshAddressFamily::Any;
        let inet = remote::ssh_tunnel::SshAddressFamily::Inet;
        let inet6 = remote::ssh_tunnel::SshAddressFamily::Inet6;
        let v6_ep: std::net::SocketAddr = "[::1]:2222".parse().unwrap();
        let v4_ep: std::net::SocketAddr = "127.0.0.1:2222".parse().unwrap();
        // download (-L): local ssh client opens the listening socket -> keep client AddressFamily
        assert_eq!(
            resolve_tunnel_listen_address_family(
                remote::ssh_tunnel::ForwardDirection::Local,
                inet6,
                Some(v4_ep),
            ),
            inet6
        );
        assert_eq!(
            resolve_tunnel_listen_address_family(
                remote::ssh_tunnel::ForwardDirection::Local,
                inet,
                Some(v6_ep),
            ),
            inet
        );
        assert_eq!(
            resolve_tunnel_listen_address_family(
                remote::ssh_tunnel::ForwardDirection::Local,
                any,
                Some(v6_ep),
            ),
            any
        );
        // upload (-R): remote sshd opens the listening socket -> follow server's actual family
        // (and allow loopback IPv4->IPv6 fallback if the server endpoint could not be probed)
        assert_eq!(
            resolve_tunnel_listen_address_family(
                remote::ssh_tunnel::ForwardDirection::Remote,
                inet6,
                Some(v4_ep),
            ),
            inet
        );
        assert_eq!(
            resolve_tunnel_listen_address_family(
                remote::ssh_tunnel::ForwardDirection::Remote,
                any,
                Some(v6_ep),
            ),
            inet6
        );
        assert_eq!(
            resolve_tunnel_listen_address_family(
                remote::ssh_tunnel::ForwardDirection::Remote,
                inet6,
                Some(v6_ep),
            ),
            inet6
        );
        assert_eq!(
            resolve_tunnel_listen_address_family(
                remote::ssh_tunnel::ForwardDirection::Remote,
                inet6,
                None,
            ),
            any
        );
    }

    #[test]
    fn source_accept_timeout_widens_for_ssh_and_auto_non_same_host() {
        assert_eq!(
            source_accept_timeout_sec(RemoteTransport::Tcp, CopyTopology::Download, 15, 3),
            15
        );
        assert_eq!(
            source_accept_timeout_sec(RemoteTransport::Ssh, CopyTopology::SameHost, 15, 3),
            15
        );
        assert_eq!(
            source_accept_timeout_sec(RemoteTransport::Auto, CopyTopology::SameHost, 15, 3),
            15
        );
        assert_eq!(
            source_accept_timeout_sec(RemoteTransport::Ssh, CopyTopology::Download, 15, 3),
            30
        );
        assert_eq!(
            source_accept_timeout_sec(RemoteTransport::Ssh, CopyTopology::ThreeHost, 15, 3),
            30
        );
        assert_eq!(
            source_accept_timeout_sec(RemoteTransport::Auto, CopyTopology::Upload, 15, 3),
            33
        );
        // probe timeout is clamped at conn_timeout_sec
        assert_eq!(
            source_accept_timeout_sec(RemoteTransport::Auto, CopyTopology::Download, 2, 10),
            6
        );
        // huge values saturate instead of overflowing
        assert_eq!(
            source_accept_timeout_sec(RemoteTransport::Auto, CopyTopology::Download, u64::MAX, 3),
            u64::MAX
        );
    }

    #[test]
    fn resolve_source_bind_ip_for_transport_avoids_inferred_ip_in_auto_mode() {
        let any = remote::ssh_tunnel::SshAddressFamily::Any;
        let inet6 = remote::ssh_tunnel::SshAddressFamily::Inet6;
        // tcp mode retains inferred IPv4 literal behavior
        assert_eq!(
            resolve_source_bind_ip_for_transport(RemoteTransport::Tcp, None, "54.1.2.3", any),
            Some("54.1.2.3".to_string())
        );
        // auto mode avoids inferring unbindable NAT/public IPs when --bind-ip was not passed
        assert_eq!(
            resolve_source_bind_ip_for_transport(RemoteTransport::Auto, None, "54.1.2.3", any),
            None
        );
        // auto mode under AddressFamily inet6 binds dual-stack :: so IPv6 fallback targets ::1
        assert_eq!(
            resolve_source_bind_ip_for_transport(RemoteTransport::Auto, None, "54.1.2.3", inet6),
            Some("::".to_string())
        );
        // auto mode still honors an explicit --bind-ip
        assert_eq!(
            resolve_source_bind_ip_for_transport(
                RemoteTransport::Auto,
                Some("10.0.0.5"),
                "54.1.2.3",
                inet6
            ),
            Some("10.0.0.5".to_string())
        );
        // ssh mode always binds matching loopback regardless of explicit --bind-ip or host literal
        assert_eq!(
            resolve_source_bind_ip_for_transport(
                RemoteTransport::Ssh,
                Some("10.0.0.5"),
                "54.1.2.3",
                any
            ),
            Some("127.0.0.1".to_string())
        );
        assert_eq!(
            resolve_source_bind_ip_for_transport(
                RemoteTransport::Ssh,
                Some("10.0.0.5"),
                "54.1.2.3",
                inet6
            ),
            Some("::1".to_string())
        );
    }
}
