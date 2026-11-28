//! Master-side SSH configuration resolution, operand classification, and agent-forwarding plan.

use anyhow::{Context, anyhow};
use rcp_tools_rcp::master_session::{InitialTopology, RemoteTransport};
use rcp_tools_rcp::path;

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

#[cfg(test)]
mod tests {
    use super::*;
    use rcp_tools_rcp::master_session::CopyTopology;

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
                strict_host_key_checking: remote::ssh_tunnel::SshStrictHostKeyChecking::Ask,
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
}
