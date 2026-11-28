use super::*;

#[test]
fn run_master_session_future_is_send() {
    fn assert_send<T: Send>(_value: T) {}
    fn check(plan: MasterSessionPlan, backend: &mut dyn RemoteBackend) {
        assert_send(run_master_session(plan, backend));
    }
    let _ = check;
}

fn readiness(
    files_in_flight: common::ConcurrencyLimit,
    max_connections: usize,
) -> remote::RcpdReadiness {
    remote::RcpdReadiness {
        fingerprint: None,
        files_in_flight,
        max_connections: std::num::NonZeroUsize::new(max_connections).unwrap(),
    }
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
            strict_host_key_checking: remote::ssh_tunnel::SshStrictHostKeyChecking::Ask,
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
        resolve_source_target_address_family(false, inet6, Some(&cfg("srv", inet6)), Some(v4_ep)),
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

#[test]
fn resolve_requested_ssh_tunnels_matches_streams_with_overridable_default_cap() {
    let nz = |n| std::num::NonZeroUsize::new(n).unwrap();
    // automatic default follows effective streams up to DEFAULT_MAX_SSH_TUNNELS (16)
    assert_eq!(resolve_requested_ssh_tunnels(None, nz(4), false), 4);
    assert_eq!(resolve_requested_ssh_tunnels(None, nz(8), false), 8);
    assert_eq!(resolve_requested_ssh_tunnels(None, nz(16), false), 16);
    assert_eq!(
        resolve_requested_ssh_tunnels(None, nz(32), false),
        DEFAULT_MAX_SSH_TUNNELS
    );
    // explicit --ssh-tunnels overrides the default cap both above and below 16
    assert_eq!(
        resolve_requested_ssh_tunnels(Some(nz(24)), nz(32), false),
        24
    );
    assert_eq!(resolve_requested_ssh_tunnels(Some(nz(2)), nz(16), false), 2);
    // still clamped to effective data streams
    assert_eq!(
        resolve_requested_ssh_tunnels(Some(nz(64)), nz(32), false),
        32
    );
    // preview/dry-run always uses 1 tunnel
    assert_eq!(resolve_requested_ssh_tunnels(None, nz(16), true), 1);
    assert_eq!(resolve_requested_ssh_tunnels(Some(nz(24)), nz(32), true), 1);
}

#[test]
fn plan_peer_auth_selects_expected_policy_for_every_transport_and_topology() {
    let src_fp = [0x11; 32];
    let dst_fp = [0x22; 32];
    assert!(daemons_need_tls_certs(false, RemoteTransport::Tcp));
    assert!(daemons_need_tls_certs(false, RemoteTransport::Auto));
    assert!(!daemons_need_tls_certs(false, RemoteTransport::Ssh));
    assert!(!daemons_need_tls_certs(true, RemoteTransport::Tcp));
    assert!(!daemons_need_tls_certs(true, RemoteTransport::Auto));
    assert!(!daemons_need_tls_certs(true, RemoteTransport::Ssh));
    // --no-encryption disables auth everywhere
    for transport in [
        RemoteTransport::Tcp,
        RemoteTransport::Ssh,
        RemoteTransport::Auto,
    ] {
        let (s, d) =
            plan_peer_auth(true, transport, CopyTopology::Download, None, None, None).unwrap();
        assert_eq!(s, remote::protocol::PeerAuth::None);
        assert_eq!(d, remote::protocol::PeerAuth::None);
    }
    // explicit SSH uses shared-secret loopback HMAC without TLS certs
    for topology in [
        CopyTopology::Download,
        CopyTopology::Upload,
        CopyTopology::ThreeHost,
        CopyTopology::SameHost,
    ] {
        let (s, d) = plan_peer_auth(
            false,
            RemoteTransport::Ssh,
            topology,
            Some("127.0.0.1"),
            None,
            None,
        )
        .unwrap();
        match (s, d) {
            (
                remote::protocol::PeerAuth::SshSharedSecret { secret: s_sec },
                remote::protocol::PeerAuth::SshSharedSecret { secret: d_sec },
            ) => assert_eq!(s_sec, d_sec),
            other => {
                panic!("expected matching SshSharedSecret for {topology:?}, got {other:?}")
            }
        }
    }
    // direct TCP, SameHost auto, and auto with an explicit non-loopback --bind-ip stay on
    // pure mutual TLS so SSH fallback over a non-loopback listener never trips the strict
    // loopback peer guard
    for (transport, topology, bind_ip) in [
        (RemoteTransport::Tcp, CopyTopology::Download, None),
        (RemoteTransport::Auto, CopyTopology::SameHost, None),
        (
            RemoteTransport::Auto,
            CopyTopology::Download,
            Some("10.0.0.5"),
        ),
        (
            RemoteTransport::Auto,
            CopyTopology::ThreeHost,
            Some("2001:db8::1"),
        ),
    ] {
        let (s, d) = plan_peer_auth(
            false,
            transport,
            topology,
            bind_ip,
            Some(src_fp),
            Some(dst_fp),
        )
        .unwrap();
        assert_eq!(
            s,
            remote::protocol::PeerAuth::Tls {
                peer_fingerprint: dst_fp
            }
        );
        assert_eq!(
            d,
            remote::protocol::PeerAuth::Tls {
                peer_fingerprint: src_fp
            }
        );
    }
    // non-same-host auto with wildcard/omitted/loopback bind pairs TLS fingerprints with one
    // shared fallback secret
    for topology in [
        CopyTopology::Download,
        CopyTopology::Upload,
        CopyTopology::ThreeHost,
    ] {
        for bind_ip in [
            None,
            Some("0.0.0.0"),
            Some("::"),
            Some("127.0.0.1"),
            Some("::1"),
            Some("::ffff:127.0.0.1"),
        ] {
            let (s, d) = plan_peer_auth(
                false,
                RemoteTransport::Auto,
                topology,
                bind_ip,
                Some(src_fp),
                Some(dst_fp),
            )
            .unwrap();
            match (s, d) {
                (
                    remote::protocol::PeerAuth::AutoTlsOrSshSecret {
                        peer_fingerprint: s_peer,
                        secret: s_sec,
                    },
                    remote::protocol::PeerAuth::AutoTlsOrSshSecret {
                        peer_fingerprint: d_peer,
                        secret: d_sec,
                    },
                ) => {
                    assert_eq!(s_peer, dst_fp);
                    assert_eq!(d_peer, src_fp);
                    assert_eq!(s_sec, d_sec);
                }
                other => {
                    panic!(
                        "expected AutoTlsOrSshSecret for ({topology:?}, {bind_ip:?}), got {other:?}"
                    )
                }
            }
        }
    }
}

#[test]
fn master_rejects_readiness_encryption_mismatch() {
    let plain = readiness(common::ConcurrencyLimit::Unlimited, 4);
    let encrypted = remote::RcpdReadiness {
        fingerprint: Some([0xabu8; 32]),
        ..plain.clone()
    };
    assert!(validate_readiness_encryption("source", false, &plain).is_ok());
    assert!(validate_readiness_encryption("source", true, &encrypted).is_ok());
    assert!(validate_readiness_encryption("source", true, &plain).is_err());
    assert!(validate_readiness_encryption("destination", false, &encrypted).is_err());
}

#[test]
fn master_rejects_destination_readiness_that_differs_from_source() {
    let source = readiness(
        common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(32).unwrap()),
        32,
    );
    let wrong_files = readiness(
        common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(16).unwrap()),
        32,
    );
    let wrong_streams = readiness(
        common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(32).unwrap()),
        16,
    );
    assert!(validate_destination_readiness(&source, &wrong_files).is_err());
    assert!(validate_destination_readiness(&source, &wrong_streams).is_err());
}

#[tokio::test(start_paused = true)]
async fn tracing_receivers_share_one_teardown_deadline() {
    let receivers = vec![
        (
            remote::AbortOnDropTask::new(tokio::spawn(std::future::pending())),
            remote::tracelog::RcpdType::Source,
        ),
        (
            remote::AbortOnDropTask::new(tokio::spawn(std::future::pending())),
            remote::tracelog::RcpdType::Destination,
        ),
    ];
    let started = tokio::time::Instant::now();
    finish_tracing_receivers(receivers).await;
    assert_eq!(started.elapsed(), TRACING_DRAIN_GRACE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracing_receiver_drain_does_not_wait_forever_after_abort() {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let receiver = remote::AbortOnDropTask::new(tokio::spawn(async move {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    }));
    started_rx.await.unwrap();
    let drain = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        finish_tracing_receiver(
            receiver,
            remote::tracelog::RcpdType::Source,
            tokio::time::Instant::now() + std::time::Duration::from_millis(20),
        ),
    )
    .await;
    release_tx.send(()).unwrap();
    assert!(
        drain.is_ok(),
        "an uncooperative task must be abandoned after the drain deadline"
    );
}

#[tokio::test(start_paused = true)]
async fn tracing_receivers_remain_live_until_processes_finish() {
    let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
    let receiver = remote::AbortOnDropTask::new(tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(4)).await;
        let _ = completed_tx.send(());
    }));
    let process_wait = async {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    };
    let started = tokio::time::Instant::now();
    finish_remote_teardown(
        process_wait,
        vec![(receiver, remote::tracelog::RcpdType::Source)],
    )
    .await;
    assert_eq!(started.elapsed(), std::time::Duration::from_secs(5));
    completed_rx
        .await
        .expect("receiver must remain live while the daemon is still finishing");
}

#[tokio::test(start_paused = true)]
async fn remote_teardown_retains_process_failure_after_draining_receivers() {
    let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
    let receiver = remote::AbortOnDropTask::new(tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let _ = completed_tx.send(());
    }));
    let process_wait = async {
        Err::<(), _>(std::io::Error::from(std::io::ErrorKind::StorageFull))
            .context("daemon could not flush timing output")
    };
    let error = finish_remote_teardown(
        process_wait,
        vec![(receiver, remote::tracelog::RcpdType::Source)],
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::StorageFull
    );
    completed_rx
        .await
        .expect("a daemon failure must still drain its final tracing messages");
}

#[tokio::test]
async fn dropping_remote_teardown_aborts_retained_receivers() {
    struct NotifyDrop(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for NotifyDrop {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let receiver = remote::AbortOnDropTask::new(tokio::spawn(async move {
        let _notify_drop = NotifyDrop(Some(dropped_tx));
        let _ = started_tx.send(());
        std::future::pending::<()>().await;
    }));
    started_rx.await.unwrap();
    let mut teardown = SessionTeardown::default();
    teardown.retain_tracing_receiver(receiver, remote::tracelog::RcpdType::Source);
    drop(teardown);
    tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
        .await
        .expect("teardown drop must abort retained tracing receivers")
        .unwrap();
}

#[tokio::test]
async fn cancelling_remote_teardown_aborts_owned_receivers() {
    struct NotifyDrop(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for NotifyDrop {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let receiver = remote::AbortOnDropTask::new(tokio::spawn(async move {
        let _notify_drop = NotifyDrop(Some(dropped_tx));
        let _ = started_tx.send(());
        std::future::pending::<()>().await;
    }));
    started_rx.await.unwrap();
    let teardown = tokio::spawn(finish_remote_teardown(
        std::future::pending::<()>(),
        vec![(receiver, remote::tracelog::RcpdType::Source)],
    ));
    tokio::task::yield_now().await;
    teardown.abort();
    let _ = teardown.await;
    tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
        .await
        .expect("cancelling teardown must abort receivers it already took")
        .unwrap();
}

async fn spawn_test_daemon_channels() -> (
    remote::stdio::DaemonControlSender,
    remote::stdio::MasterControlReceiver,
    remote::stdio::MasterStdinSender<tokio::io::DuplexStream>,
    remote::streams::RecvStream<tokio::io::DuplexStream>,
    tokio_util::sync::CancellationToken,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    remote::AbortOnDropTask<()>,
) {
    let (daemon_out_w, master_out_r) = tokio::io::duplex(4096);
    let (master_in_w, daemon_in_r) = tokio::io::duplex(4096);
    let (control_tx, control_rx) = remote::stdio::daemon_control_channel();
    let (_tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let writer_task = tokio::spawn(remote::stdio::run_daemon_stdout_writer(
        control_rx,
        tracing_rx,
        remote::streams::SendStream::new(daemon_out_w),
        shutdown.clone(),
    ));
    let (master_control_rx, demux_task) = remote::stdio::spawn_master_stdout_demux(
        remote::streams::RecvStream::new(master_out_r),
        remote::tracelog::RcpdType::Destination,
    );
    let stdin_sender = remote::stdio::MasterStdinSender::new(master_in_w, 0);
    let daemon_stdin_recv = remote::streams::RecvStream::new(daemon_in_r);
    (
        control_tx,
        master_control_rx,
        stdin_sender,
        daemon_stdin_recv,
        shutdown,
        writer_task,
        demux_task,
    )
}

type TestFallbackOpener = fn(
    String,
) -> std::future::Ready<
    anyhow::Result<(std::net::SocketAddr, Vec<std::net::SocketAddr>)>,
>;

#[tokio::test]
async fn await_daemon_results_preserves_source_failure_when_dest_sends_direct_connect_failed() {
    let (src_tx, mut src_rx, src_stdin, _src_in_r, src_stop, src_w, src_d) =
        spawn_test_daemon_channels().await;
    let (dst_tx, mut dst_rx, dst_stdin, mut dst_in_r, dst_stop, dst_w, dst_d) =
        spawn_test_daemon_channels().await;
    let mut src_stdin = Some(src_stdin);
    let mut dst_stdin = Some(dst_stdin);
    let expected_summary = common::copy::Summary {
        files_copied: 3,
        ..Default::default()
    };
    src_tx
        .send_result(remote::protocol::RcpdResult::Failure {
            error: "source read denied".to_owned(),
            summary: expected_summary,
            runtime_stats: Default::default(),
            peer_connected: false,
        })
        .await
        .unwrap();
    dst_tx
        .send_direct_connect_failed("connection refused".to_owned())
        .await
        .unwrap();
    drop(src_tx);
    drop(dst_tx);
    src_stop.cancel();
    dst_stop.cancel();
    // join both demuxers so both channels already hold their frames before select! starts
    src_w.await.unwrap().unwrap();
    dst_w.await.unwrap().unwrap();
    src_d.join().await.unwrap();
    dst_d.join().await.unwrap();
    let opener: Option<TestFallbackOpener> = None;
    let err = await_daemon_results_with_fallback(
        &mut src_rx,
        &mut dst_rx,
        &mut src_stdin,
        &mut dst_stdin,
        opener,
        "src-host",
        "dst-host",
        std::time::Duration::from_millis(100),
    )
    .await
    .expect_err("source failure must return Err");
    let copy_err = err
        .downcast_ref::<common::copy::Error>()
        .expect("source failure must preserve common::copy::Error");
    assert_eq!(copy_err.summary.files_copied, 3);
    assert!(format!("{err:#}").contains("source read denied"));
    // destination stdin must have been closed cleanly (EOF)
    assert!(
        dst_in_r
            .recv_object::<remote::protocol::MasterToRcpd>()
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn await_daemon_results_applies_grace_on_source_failure_without_master_fallback() {
    let (src_tx, mut src_rx, src_stdin, _src_in_r, src_stop, src_w, src_d) =
        spawn_test_daemon_channels().await;
    let (_dst_tx, mut dst_rx, dst_stdin, mut dst_in_r, _dst_stop, _dst_w, _dst_d) =
        spawn_test_daemon_channels().await;
    let mut src_stdin = Some(src_stdin);
    let mut dst_stdin = Some(dst_stdin);
    src_tx
        .send_result(remote::protocol::RcpdResult::Failure {
            error: "source vanished".to_owned(),
            summary: common::copy::Summary {
                files_copied: 2,
                ..Default::default()
            },
            runtime_stats: Default::default(),
            peer_connected: false,
        })
        .await
        .unwrap();
    drop(src_tx);
    src_stop.cancel();
    src_w.await.unwrap().unwrap();
    src_d.join().await.unwrap();
    let opener: Option<TestFallbackOpener> = None;
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        await_daemon_results_with_fallback(
            &mut src_rx,
            &mut dst_rx,
            &mut src_stdin,
            &mut dst_stdin,
            opener,
            "src-host",
            "dst-host",
            std::time::Duration::from_millis(80),
        ),
    )
    .await
    .expect("source failure must bound waiting on stalled destination even without master fallback")
    .expect_err("source failure must return Err");
    let copy_err = err
        .downcast_ref::<common::copy::Error>()
        .expect("source failure must preserve common::copy::Error");
    assert_eq!(copy_err.summary.files_copied, 2);
    assert!(format!("{err:#}").contains("source vanished"));
    assert!(
        dst_in_r
            .recv_object::<remote::protocol::MasterToRcpd>()
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn await_daemon_results_cancels_in_flight_fallback_when_source_fails() {
    struct DropFlag(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let (src_tx, mut src_rx, src_stdin, _src_in_r, src_stop, src_w, _src_d) =
        spawn_test_daemon_channels().await;
    let (dst_tx, mut dst_rx, dst_stdin, mut dst_in_r, _dst_stop, _dst_w, _dst_d) =
        spawn_test_daemon_channels().await;
    let mut src_stdin = Some(src_stdin);
    let mut dst_stdin = Some(dst_stdin);
    dst_tx
        .send_direct_connect_failed("timed out".to_owned())
        .await
        .unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dropped_clone = dropped.clone();
    let open_fallback = Some(move |_reason: String| async move {
        let _flag = DropFlag(dropped_clone);
        let _ = started_tx.send(());
        std::future::pending::<anyhow::Result<(std::net::SocketAddr, Vec<std::net::SocketAddr>)>>()
            .await
    });
    let source_sender = tokio::spawn(async move {
        started_rx.await.unwrap();
        src_tx
            .send_result(remote::protocol::RcpdResult::Failure {
                error: "source died during tunnel setup".to_owned(),
                summary: common::copy::Summary {
                    files_copied: 7,
                    ..Default::default()
                },
                runtime_stats: Default::default(),
                peer_connected: false,
            })
            .await
            .unwrap();
        drop(src_tx);
        src_stop.cancel();
    });
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        await_daemon_results_with_fallback(
            &mut src_rx,
            &mut dst_rx,
            &mut src_stdin,
            &mut dst_stdin,
            open_fallback,
            "src-host",
            "dst-host",
            std::time::Duration::from_millis(80),
        ),
    )
    .await
    .expect("source failure must interrupt pending fallback tunnel setup")
    .expect_err("source failure must return Err");
    source_sender.await.unwrap();
    assert!(
        dropped.load(std::sync::atomic::Ordering::SeqCst),
        "pending fallback future must be dropped when source fails"
    );
    let copy_err = err
        .downcast_ref::<common::copy::Error>()
        .expect("source failure must preserve common::copy::Error");
    assert_eq!(copy_err.summary.files_copied, 7);
    assert!(format!("{err:#}").contains("source died during tunnel setup"));
    assert!(
        dst_in_r
            .recv_object::<remote::protocol::MasterToRcpd>()
            .await
            .unwrap()
            .is_none()
    );
    src_w.await.unwrap().unwrap();
}

#[tokio::test]
async fn await_daemon_results_delivers_fallback_endpoints_and_completes() {
    let (src_tx, mut src_rx, src_stdin, _src_in_r, src_stop, src_w, _src_d) =
        spawn_test_daemon_channels().await;
    let (dst_tx, mut dst_rx, dst_stdin, mut dst_in_r, dst_stop, dst_w, _dst_d) =
        spawn_test_daemon_channels().await;
    let mut src_stdin = Some(src_stdin);
    let mut dst_stdin = Some(dst_stdin);
    let fallback_ctrl: std::net::SocketAddr = "127.0.0.1:46001".parse().unwrap();
    let fallback_data = vec![
        "127.0.0.1:46002".parse().unwrap(),
        "127.0.0.1:46003".parse().unwrap(),
    ];
    let fallback_data_expected = fallback_data.clone();
    let driver = tokio::spawn(async move {
        dst_tx
            .send_direct_connect_failed("firewall dropped SYN".to_owned())
            .await
            .unwrap();
        match dst_in_r
            .recv_object::<remote::protocol::MasterToRcpd>()
            .await
            .unwrap()
        {
            Some(remote::protocol::MasterToRcpd::SshFallbackEndpoints {
                control_addr,
                data_addrs,
            }) => {
                assert_eq!(control_addr, fallback_ctrl);
                assert_eq!(data_addrs, fallback_data_expected);
            }
            other => panic!("expected SshFallbackEndpoints, got {other:?}"),
        }
        src_tx
            .send_result(remote::protocol::RcpdResult::Success {
                message: "src ok".to_owned(),
                summary: Default::default(),
                runtime_stats: Default::default(),
            })
            .await
            .unwrap();
        dst_tx
            .send_result(remote::protocol::RcpdResult::Success {
                message: "dst ok".to_owned(),
                summary: Default::default(),
                runtime_stats: Default::default(),
            })
            .await
            .unwrap();
        drop(src_tx);
        drop(dst_tx);
        src_stop.cancel();
        dst_stop.cancel();
    });
    let (src_res, dst_res) = await_daemon_results_with_fallback(
        &mut src_rx,
        &mut dst_rx,
        &mut src_stdin,
        &mut dst_stdin,
        Some(move |reason: String| async move {
            assert_eq!(reason, "firewall dropped SYN");
            anyhow::Ok((fallback_ctrl, fallback_data))
        }),
        "src-host",
        "dst-host",
        std::time::Duration::from_millis(80),
    )
    .await
    .unwrap();
    driver.await.unwrap();
    assert!(matches!(
        src_res,
        remote::protocol::RcpdResult::Success { .. }
    ));
    assert!(matches!(
        dst_res,
        remote::protocol::RcpdResult::Success { .. }
    ));
    src_w.await.unwrap().unwrap();
    dst_w.await.unwrap().unwrap();
}

#[tokio::test]
async fn await_daemon_results_preserves_destination_partial_summary_on_established_source_failure()
{
    let (src_tx, mut src_rx, src_stdin, _src_in_r, src_stop, src_w, src_d) =
        spawn_test_daemon_channels().await;
    let (dst_tx, mut dst_rx, dst_stdin, _dst_in_r, dst_stop, dst_w, _dst_d) =
        spawn_test_daemon_channels().await;
    let mut src_stdin = Some(src_stdin);
    let mut dst_stdin = Some(dst_stdin);
    src_tx
        .send_result(remote::protocol::RcpdResult::Failure {
            error: "source permission denied on one file".to_owned(),
            summary: common::copy::Summary {
                files_skipped: 4,
                ..Default::default()
            },
            runtime_stats: Default::default(),
            peer_connected: true,
        })
        .await
        .unwrap();
    drop(src_tx);
    src_stop.cancel();
    src_w.await.unwrap().unwrap();
    src_d.join().await.unwrap();
    let grace = std::time::Duration::from_millis(60);
    // delay destination terminal result well past the pre-connection peer-failure grace
    let delayed_dest = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        dst_tx
            .send_result(remote::protocol::RcpdResult::Success {
                message: "destination finalized partial files".to_owned(),
                summary: common::copy::Summary {
                    files_copied: 12,
                    bytes_copied: 8192,
                    ..Default::default()
                },
                runtime_stats: Default::default(),
            })
            .await
            .unwrap();
        drop(dst_tx);
        dst_stop.cancel();
        dst_w.await.unwrap().unwrap();
    });
    let opener: Option<TestFallbackOpener> = None;
    let (src_res, dst_res) = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        await_daemon_results_with_fallback(
            &mut src_rx,
            &mut dst_rx,
            &mut src_stdin,
            &mut dst_stdin,
            opener,
            "src-host",
            "dst-host",
            grace,
        ),
    )
    .await
    .expect("established source failure must wait for destination terminal result")
    .expect("established failure must return both results for summary merging");
    delayed_dest.await.unwrap();
    match src_res {
        remote::protocol::RcpdResult::Failure {
            error,
            summary,
            peer_connected,
            ..
        } => {
            assert!(peer_connected);
            assert!(error.contains("source permission denied on one file"));
            assert_eq!(summary.files_skipped, 4);
        }
        other => panic!("expected source Failure, got {other:?}"),
    }
    match dst_res {
        remote::protocol::RcpdResult::Success { summary, .. } => {
            assert_eq!(summary.files_copied, 12);
            assert_eq!(summary.bytes_copied, 8192);
        }
        other => panic!("expected destination Success with partial counts, got {other:?}"),
    }
}

#[tokio::test]
async fn await_daemon_results_handles_established_and_pre_connect_destination_failures() {
    // 1. destination failure after connection establishment (peer_connected: true) must wait
    // past peer_failure_grace so source can report its skip counts
    let (src_tx, mut src_rx, src_stdin, _src_in_r, src_stop, src_w, _src_d) =
        spawn_test_daemon_channels().await;
    let (dst_tx, mut dst_rx, dst_stdin, _dst_in_r, dst_stop, dst_w, dst_d) =
        spawn_test_daemon_channels().await;
    let mut src_stdin = Some(src_stdin);
    let mut dst_stdin = Some(dst_stdin);
    dst_tx
        .send_result(remote::protocol::RcpdResult::Failure {
            error: "destination disk full".to_owned(),
            summary: common::copy::Summary {
                files_copied: 7,
                bytes_copied: 4096,
                ..Default::default()
            },
            runtime_stats: Default::default(),
            peer_connected: true,
        })
        .await
        .unwrap();
    drop(dst_tx);
    dst_stop.cancel();
    dst_w.await.unwrap().unwrap();
    dst_d.join().await.unwrap();
    let grace = std::time::Duration::from_millis(60);
    let delayed_src = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        src_tx
            .send_result(remote::protocol::RcpdResult::Failure {
                error: "destination closed connection".to_owned(),
                summary: common::copy::Summary {
                    files_skipped: 3,
                    ..Default::default()
                },
                runtime_stats: Default::default(),
                peer_connected: true,
            })
            .await
            .unwrap();
        drop(src_tx);
        src_stop.cancel();
        src_w.await.unwrap().unwrap();
    });
    let opener: Option<TestFallbackOpener> = None;
    let (src_res, dst_res) = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        await_daemon_results_with_fallback(
            &mut src_rx,
            &mut dst_rx,
            &mut src_stdin,
            &mut dst_stdin,
            opener,
            "src-host",
            "dst-host",
            grace,
        ),
    )
    .await
    .expect("established destination failure must wait for source terminal result")
    .expect("established failure must return both results for summary merging");
    delayed_src.await.unwrap();
    match src_res {
        remote::protocol::RcpdResult::Failure { summary, .. } => {
            assert_eq!(summary.files_skipped, 3);
        }
        other => panic!("expected source Failure with skip counts, got {other:?}"),
    }
    match dst_res {
        remote::protocol::RcpdResult::Failure { summary, .. } => {
            assert_eq!(summary.files_copied, 7);
            assert_eq!(summary.bytes_copied, 4096);
        }
        other => panic!("expected destination Failure with partial counts, got {other:?}"),
    }
    // 2. destination failure before connecting (peer_connected: false) must cancel source
    // stdin after peer_failure_grace instead of stalling on source's accept timeout
    let (_src_tx2, mut src_rx2, src_stdin2, mut src_in_r2, _src_stop2, _src_w2, _src_d2) =
        spawn_test_daemon_channels().await;
    let (dst_tx2, mut dst_rx2, dst_stdin2, _dst_in_r2, dst_stop2, dst_w2, dst_d2) =
        spawn_test_daemon_channels().await;
    let mut src_stdin2 = Some(src_stdin2);
    let mut dst_stdin2 = Some(dst_stdin2);
    dst_tx2
        .send_result(remote::protocol::RcpdResult::Failure {
            error: "could not establish 3-host SSH tunnel".to_owned(),
            summary: common::copy::Summary::default(),
            runtime_stats: Default::default(),
            peer_connected: false,
        })
        .await
        .unwrap();
    drop(dst_tx2);
    dst_stop2.cancel();
    dst_w2.await.unwrap().unwrap();
    dst_d2.join().await.unwrap();
    let opener: Option<TestFallbackOpener> = None;
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        await_daemon_results_with_fallback(
            &mut src_rx2,
            &mut dst_rx2,
            &mut src_stdin2,
            &mut dst_stdin2,
            opener,
            "src-host",
            "dst-host",
            grace,
        ),
    )
    .await
    .expect("pre-connection destination failure must time out after peer_failure_grace")
    .expect_err("pre-connection destination failure must return destination error");
    assert!(format!("{err:#}").contains("could not establish 3-host SSH tunnel"));
    assert!(
        src_stdin2.is_none(),
        "source stdin must be closed on grace timeout"
    );
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            src_in_r2.recv_object::<remote::protocol::MasterToRcpd>(),
        )
        .await
        .unwrap()
        .unwrap()
        .is_none(),
        "closing source stdin must deliver EOF to source rcpd"
    );
}

#[tokio::test]
async fn await_daemon_results_cancels_in_flight_fallback_when_dest_dies() {
    struct DropFlag(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let (_src_tx, mut src_rx, src_stdin, mut src_in_r, _src_stop, _src_w, _src_d) =
        spawn_test_daemon_channels().await;
    let (dst_tx, mut dst_rx, dst_stdin, _dst_in_r, dst_stop, dst_w, _dst_d) =
        spawn_test_daemon_channels().await;
    let mut src_stdin = Some(src_stdin);
    let mut dst_stdin = Some(dst_stdin);
    dst_tx
        .send_direct_connect_failed("timed out".to_owned())
        .await
        .unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dropped_clone = dropped.clone();
    let open_fallback = Some(move |_reason: String| async move {
        let _flag = DropFlag(dropped_clone);
        let _ = started_tx.send(());
        std::future::pending::<anyhow::Result<(std::net::SocketAddr, Vec<std::net::SocketAddr>)>>()
            .await
    });
    let dest_crasher = tokio::spawn(async move {
        started_rx.await.unwrap();
        // close destination stdout cleanly without sending a result (models daemon crash/exit)
        drop(dst_tx);
        dst_stop.cancel();
        dst_w.await.unwrap().unwrap();
    });
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        await_daemon_results_with_fallback(
            &mut src_rx,
            &mut dst_rx,
            &mut src_stdin,
            &mut dst_stdin,
            open_fallback,
            "src-host",
            "dst-host",
            std::time::Duration::from_millis(80),
        ),
    )
    .await
    .expect("destination death must interrupt pending fallback tunnel setup")
    .expect_err("destination death must return Err");
    dest_crasher.await.unwrap();
    assert!(
        dropped.load(std::sync::atomic::Ordering::SeqCst),
        "pending fallback future must be dropped when destination dies"
    );
    assert!(
        format!("{err:#}").contains("destination rcpd"),
        "unexpected error on destination death: {err:#}"
    );
    // source stdin must have been closed after grace elapsed
    assert!(
        src_in_r
            .recv_object::<remote::protocol::MasterToRcpd>()
            .await
            .unwrap()
            .is_none()
    );
}

/// The master's ends of one daemon whose stdout the test writes itself.
struct ScriptedDaemon {
    stdout: tokio::io::DuplexStream,
    control_rx: remote::stdio::MasterControlReceiver,
    // aborted when dropped: the daemon's stdout is read for as long as this lives
    _demux: remote::AbortOnDropTask<()>,
    stdin: Option<remote::stdio::MasterStdinSender<tokio::io::DuplexStream>>,
    stdin_reader: remote::streams::RecvStream<tokio::io::DuplexStream>,
}

fn scripted_daemon(rcpd_type: remote::tracelog::RcpdType) -> ScriptedDaemon {
    let (stdout, master_reader) = tokio::io::duplex(4096);
    let (control_rx, demux) = remote::stdio::spawn_master_stdout_demux(
        remote::streams::RecvStream::new(master_reader),
        rcpd_type,
    );
    let (stdin_writer, stdin_reader) = tokio::io::duplex(4096);
    ScriptedDaemon {
        stdout,
        control_rx,
        _demux: demux,
        stdin: Some(remote::stdio::MasterStdinSender::new(stdin_writer, 0)),
        stdin_reader: remote::streams::RecvStream::new(stdin_reader),
    }
}

/// How a scripted daemon's control channel ends.
#[derive(Clone, Copy, Debug)]
enum Ending {
    /// The daemon reports this failure on an established copy and exits.
    Fails(&'static str),
    /// Its stdout closes cleanly with no result, as when the process is killed.
    ClosesQuietly,
    /// Its stdout ends inside a frame, as when the SSH channel is cut.
    BreaksOff,
}

impl Ending {
    /// Play the ending on a daemon's stdout, which closes with it.
    async fn play(self, mut stdout: tokio::io::DuplexStream, files_skipped: usize) {
        use tokio::io::AsyncWriteExt as _;
        match self {
            Self::Fails(error) => {
                let result = remote::protocol::RcpdResult::Failure {
                    error: error.to_owned(),
                    summary: common::copy::Summary {
                        files_skipped,
                        ..Default::default()
                    },
                    runtime_stats: Default::default(),
                    peer_connected: true,
                };
                remote::streams::SendStream::new(&mut stdout)
                    .send_control_message(&remote::protocol::RcpdToMaster::Result(result))
                    .await
                    .unwrap();
            }
            Self::ClosesQuietly => {}
            // a length prefix that promises more than follows
            Self::BreaksOff => stdout.write_all(&[0, 0, 0, 16, 1]).await.unwrap(),
        }
    }

    /// What the master's report says of a daemon that ended this way.
    fn reported_as(self, role: &str, title: &str, host: &str) -> String {
        match self {
            Self::Fails(error) => format!("{title}: {error}"),
            Self::ClosesQuietly => rcpd_closed_quietly(role, host, "report a result"),
            Self::BreaksOff => rcpd_went_quiet(role, host, "report a result"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Role {
    Source,
    Destination,
}

/// The text of a report, once: an error that carries a summary shows its chain in its own
/// `Display` and again as its source.
fn report_text(error: &anyhow::Error) -> String {
    match error.downcast_ref::<common::copy::Error>() {
        Some(with_summary) => format!("{:#}", with_summary.source),
        None => format!("{error:#}"),
    }
}

const SOURCE_FILES_SKIPPED: usize = 2;
const DEST_FILES_SKIPPED: usize = 4;

/// The master's report for a pair of endings, when `first`'s ending is there as the master
/// starts to look and the other daemon's arrives while it waits. For a paused clock, which
/// makes "while it waits" exact.
async fn report_when_first(first: Role, source: Ending, dest: Ending) -> anyhow::Error {
    let mut source_daemon = scripted_daemon(remote::tracelog::RcpdType::Source);
    let mut dest_daemon = scripted_daemon(remote::tracelog::RcpdType::Destination);
    let play_source = source.play(source_daemon.stdout, SOURCE_FILES_SKIPPED);
    let play_dest = dest.play(dest_daemon.stdout, DEST_FILES_SKIPPED);
    let late = std::time::Duration::from_millis(50);
    let late_ending = match first {
        Role::Source => {
            play_source.await;
            tokio::spawn(async move {
                tokio::time::sleep(late).await;
                play_dest.await;
            })
        }
        Role::Destination => {
            play_dest.await;
            tokio::spawn(async move {
                tokio::time::sleep(late).await;
                play_source.await;
            })
        }
    };
    let opener: Option<TestFallbackOpener> = None;
    // a grace far beyond the late ending, so that it is never what ends a wait here
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        await_daemon_results_with_fallback(
            &mut source_daemon.control_rx,
            &mut dest_daemon.control_rx,
            &mut source_daemon.stdin,
            &mut dest_daemon.stdin,
            opener,
            "src-host",
            "dst-host",
            std::time::Duration::from_secs(20),
        ),
    )
    .await
    .expect("the master must settle once both daemons have ended");
    late_ending.await.unwrap();
    result.expect_err("a daemon that ended without a result fails the copy")
}

#[tokio::test(start_paused = true)]
async fn await_daemon_results_report_is_the_same_whichever_daemon_ends_first() {
    let source_failure = Ending::Fails("destination closed control before DestinationDone");
    let dest_failure = Ending::Fails("data stream closed before the transfer completed");
    for (source, dest) in [
        (source_failure, Ending::ClosesQuietly),
        (source_failure, Ending::BreaksOff),
        (Ending::ClosesQuietly, dest_failure),
        (Ending::BreaksOff, dest_failure),
        (Ending::ClosesQuietly, Ending::ClosesQuietly),
        (Ending::BreaksOff, Ending::ClosesQuietly),
    ] {
        let source_first = report_when_first(Role::Source, source, dest).await;
        let dest_first = report_when_first(Role::Destination, source, dest).await;
        let report = report_text(&source_first);
        assert_eq!(
            report,
            report_text(&dest_first),
            "source {source:?}, destination {dest:?}: the report depends on which daemon ended first"
        );
        for (ending, role, title, host) in [
            (source, "source", "Source", "src-host"),
            (dest, "destination", "Destination", "dst-host"),
        ] {
            let expected = ending.reported_as(role, title, host);
            assert!(
                report.contains(&expected),
                "source {source:?}, destination {dest:?}: no {expected:?} in {report:?}"
            );
        }
        // the counts of the one daemon that reported are kept in either order
        let expected_counts = match (source, dest) {
            (Ending::Fails(_), _) => Some(SOURCE_FILES_SKIPPED),
            (_, Ending::Fails(_)) => Some(DEST_FILES_SKIPPED),
            _ => None,
        };
        for error in [&source_first, &dest_first] {
            let counts = error
                .downcast_ref::<common::copy::Error>()
                .map(|error| error.summary.files_skipped);
            assert_eq!(
                counts, expected_counts,
                "source {source:?}, destination {dest:?}: {error:#}"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn await_daemon_results_lists_what_is_known_of_each_daemon_source_first() {
    let lost = "(the process likely died - check the remote host for crashes or OOM kills)";
    let killed_destination = report_when_first(
        Role::Source,
        Ending::Fails("destination closed control before DestinationDone"),
        Ending::ClosesQuietly,
    )
    .await;
    assert_eq!(
        report_text(&killed_destination),
        format!(
            "rcpd operation(s) failed: Source: destination closed control before \
             DestinationDone; destination rcpd on 'dst-host' closed its control connection \
             cleanly but did not report a result {lost}"
        )
    );
    let killed_source = report_when_first(
        Role::Destination,
        Ending::ClosesQuietly,
        Ending::Fails("data stream closed before the transfer completed"),
    )
    .await;
    assert_eq!(
        report_text(&killed_source),
        format!(
            "rcpd operation(s) failed: source rcpd on 'src-host' closed its control \
             connection cleanly but did not report a result {lost}; Destination: data stream \
             closed before the transfer completed"
        )
    );
    let both_killed =
        report_when_first(Role::Source, Ending::ClosesQuietly, Ending::ClosesQuietly).await;
    assert_eq!(
        report_text(&both_killed),
        format!(
            "source rcpd on 'src-host' closed its control connection cleanly but did not \
             report a result {lost}; destination rcpd on 'dst-host' closed its control \
             connection cleanly but did not report a result {lost}"
        )
    );
}

#[tokio::test(start_paused = true)]
async fn await_daemon_results_gives_the_peer_of_a_lost_daemon_the_grace_and_no_more() {
    use tokio::io::AsyncWriteExt as _;
    let grace = std::time::Duration::from_millis(100);
    for lost in [Role::Source, Role::Destination] {
        let mut source = scripted_daemon(remote::tracelog::RcpdType::Source);
        let mut dest = scripted_daemon(remote::tracelog::RcpdType::Destination);
        // one daemon is gone, and its peer stays up and says nothing
        let (gone, expected) = match lost {
            Role::Source => (
                &mut source,
                rcpd_closed_quietly("source", "src-host", "report a result"),
            ),
            Role::Destination => (
                &mut dest,
                rcpd_closed_quietly("destination", "dst-host", "report a result"),
            ),
        };
        gone.stdout.shutdown().await.unwrap();
        let opener: Option<TestFallbackOpener> = None;
        let started = tokio::time::Instant::now();
        let error = await_daemon_results_with_fallback(
            &mut source.control_rx,
            &mut dest.control_rx,
            &mut source.stdin,
            &mut dest.stdin,
            opener,
            "src-host",
            "dst-host",
            grace,
        )
        .await
        .expect_err("a daemon that ended without a result fails the copy");
        assert_eq!(
            started.elapsed(),
            grace,
            "{lost:?} lost: its peer gets the grace to report, and no more"
        );
        assert_eq!(report_text(&error), expected, "{lost:?} lost");
        // the peer that was not heard from is told to stop
        let silent = match lost {
            Role::Source => &mut dest,
            Role::Destination => &mut source,
        };
        assert!(silent.stdin.is_none(), "{lost:?} lost");
        assert!(
            silent
                .stdin_reader
                .recv_object::<remote::protocol::MasterToRcpd>()
                .await
                .unwrap()
                .is_none(),
            "{lost:?} lost"
        );
    }
}
