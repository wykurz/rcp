//! Deterministic 3-host simulated-network tests exercising the full `rcp` master (`master_session`),
//! `rcpd` daemon (`daemon_session`), bootstrap readiness parsing (`read_rcpd_startup_record`),
//! `source` / `destination` pipelines, master-managed SSH fallback relays, watchdogs, teardown, and
//! chaos across `"master"`, `"source"`, and `"destination"` over `turmoil::net` without SSH or
//! Docker.

use std::collections::{BTreeMap, HashMap};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{Context as _, anyhow};
use futures::future::BoxFuture;
use rcp_tools_rcp::daemon_session::{self, DaemonSessionConfig};
use rcp_tools_rcp::master_session::{
    ConnectionCeiling, DaemonHandle, InitialTopology, MasterAuthoritativeFilesInFlight,
    MasterRemoteRequest, MasterSessionOutcome, MasterSessionPlan, OpenedTunnels, RemoteBackend,
    RemoteConcurrencyPolicy, RemoteTransport, SpawnedDaemon, TunnelHandle, TunnelRequest,
    daemons_need_tls_certs, run_master_session,
};
use remote::protocol::{PeerIdentity, RcpdConfig, RcpdFilesInFlight, RcpdResult, RcpdRole};
use remote::ssh_tunnel::{ResolvedSshConfig, SshAddressFamily, SshStrictHostKeyChecking};
use remote::streams::{BoxedRead, BoxedWrite};
use remote::tls::generate_self_signed_cert;
use remote::transport::{BoxedTransportStream, Transport, TransportListener, TransportStream};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, ReadBuf};
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;
use turmoil::net::{TcpListener, TcpStream};

const MASTER_HOST: &str = "master";
const SOURCE_HOST: &str = "source";
const DEST_HOST: &str = "destination";
const SSH_STDIN_PORT: u16 = 2200;
const SSH_STDOUT_PORT: u16 = 2201;
const SSH_STDERR_PORT: u16 = 2202;

type ByteHookFn = Arc<dyn Fn(usize) + Send + Sync>;

fn install_crypto_provider() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
}

/// Captures `rcp::notice` tracing events emitted on the simulation thread without mutating global
/// subscriber state.
#[derive(Clone, Default)]
struct NoticeCollector {
    notices: Arc<Mutex<Vec<String>>>,
}

impl NoticeCollector {
    fn snapshot(&self) -> Vec<String> {
        self.notices.lock().unwrap().clone()
    }
}

struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.0 = value.to_owned();
        }
    }
}

struct NoticeSubscriber {
    collector: NoticeCollector,
    next_id: AtomicU64,
}

impl NoticeSubscriber {
    fn new(collector: NoticeCollector) -> Self {
        Self {
            collector,
            next_id: AtomicU64::new(1),
        }
    }
}

impl tracing::Subscriber for NoticeSubscriber {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == common::NOTICE_TARGET
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().target() == common::NOTICE_TARGET {
            let mut visitor = MessageVisitor(String::new());
            event.record(&mut visitor);
            self.collector.notices.lock().unwrap().push(visitor.0);
        }
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// Shared simulated link-health state for one edge in the 3-host topology.
///
/// Tracks both live impairment (`impaired` / `lossy`) and a monotonic `drop_epoch` incremented on
/// packet-dropping faults (`partition` / `fail_rate > 0`). Because `turmoil::net::TcpStream` does
/// not implement TCP retransmission, a socket that lived across a packet-drop event can wedge
/// permanently even after the link is repaired; comparing the socket's `created_epoch` against
/// `drop_epoch` ensures wedged pre-repair sockets still time out on keepalive while newly opened
/// post-repair sockets remain healthy.
struct TurmoilLinkHealth {
    impaired: AtomicBool,
    lossy: AtomicBool,
    drop_epoch: AtomicU64,
}

impl TurmoilLinkHealth {
    fn healthy() -> Arc<Self> {
        Arc::new(Self {
            impaired: AtomicBool::new(false),
            lossy: AtomicBool::new(false),
            drop_epoch: AtomicU64::new(0),
        })
    }

    fn set_impaired(&self, impaired: bool) {
        self.impaired.store(impaired, Ordering::Release);
    }

    fn set_partitioned(&self, partitioned: bool) {
        // advance drop_epoch on both entering and leaving a packet-dropping window so sockets
        // opened before OR during the fault window still time out after repair if Turmoil dropped
        // one of their segments without retransmission
        self.drop_epoch.fetch_add(1, Ordering::AcqRel);
        self.impaired.store(partitioned, Ordering::Release);
    }

    fn set_lossy(&self, lossy: bool) {
        self.drop_epoch.fetch_add(1, Ordering::AcqRel);
        self.lossy.store(lossy, Ordering::Release);
    }

    fn current_drop_epoch(&self) -> u64 {
        self.drop_epoch.load(Ordering::Acquire)
    }

    fn can_ack_keepalive(&self, created_epoch: u64) -> bool {
        !self.lossy.load(Ordering::Acquire)
            && !self.impaired.load(Ordering::Acquire)
            && self.drop_epoch.load(Ordering::Acquire) == created_epoch
    }
}

/// Per-edge link health across the 3 simulated links: `master ↔ source`, `master ↔ destination`,
/// and `source ↔ destination`.
#[derive(Clone)]
struct ThreeEdgeHealth {
    master_src: Arc<TurmoilLinkHealth>,
    master_dst: Arc<TurmoilLinkHealth>,
    src_dst: Arc<TurmoilLinkHealth>,
}

impl ThreeEdgeHealth {
    fn all_healthy() -> Self {
        Self {
            master_src: TurmoilLinkHealth::healthy(),
            master_dst: TurmoilLinkHealth::healthy(),
            src_dst: TurmoilLinkHealth::healthy(),
        }
    }

    fn for_role(&self, role: RcpdRole) -> Arc<TurmoilLinkHealth> {
        match role {
            RcpdRole::Source => Arc::clone(&self.master_src),
            RcpdRole::Destination => Arc::clone(&self.master_dst),
        }
    }

    fn for_source_accept(
        &self,
        peer_ip: std::net::IpAddr,
        master_ip: std::net::IpAddr,
    ) -> Arc<TurmoilLinkHealth> {
        if peer_ip == master_ip {
            Arc::clone(&self.master_src)
        } else {
            Arc::clone(&self.src_dst)
        }
    }

    fn for_dest_connect(
        &self,
        target_ip: std::net::IpAddr,
        master_ip: std::net::IpAddr,
    ) -> Arc<TurmoilLinkHealth> {
        if target_ip == master_ip {
            Arc::clone(&self.master_dst)
        } else {
            Arc::clone(&self.src_dst)
        }
    }
}

/// Simulated TCP stream modeling virtual-time keepalive / `ServerAlive` timeouts gated on edge
/// health, plus optional loopback peer-IP rewriting for master-relayed SSH tunnels.
struct TurmoilStream {
    inner: TcpStream,
    peer_ip_override: Option<std::net::IpAddr>,
    read_idle_timeout: Option<Duration>,
    write_idle_timeout: Option<Duration>,
    read_sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    write_sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    link_health: Arc<TurmoilLinkHealth>,
    created_drop_epoch: u64,
    on_write_bytes: Option<ByteHookFn>,
}

impl TurmoilStream {
    fn new(
        inner: TcpStream,
        peer_ip_override: Option<std::net::IpAddr>,
        read_idle_timeout: Option<Duration>,
        write_idle_timeout: Option<Duration>,
        link_health: Arc<TurmoilLinkHealth>,
        on_write_bytes: Option<ByteHookFn>,
    ) -> Self {
        let created_drop_epoch = link_health.current_drop_epoch();
        Self {
            inner,
            peer_ip_override,
            read_idle_timeout,
            write_idle_timeout,
            read_sleep: None,
            write_sleep: None,
            link_health,
            created_drop_epoch,
            on_write_bytes,
        }
    }

    fn poll_keepalive_timer(
        sleep_slot: &mut Option<Pin<Box<tokio::time::Sleep>>>,
        timeout: Option<Duration>,
        link_health: &TurmoilLinkHealth,
        created_drop_epoch: u64,
        cx: &mut Context<'_>,
        label: &str,
    ) -> Option<std::io::Error> {
        let timeout = timeout?;
        let sleep = sleep_slot.get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
        if sleep.as_mut().poll(cx).is_ready() {
            if link_health.can_ack_keepalive(created_drop_epoch) {
                let next = Box::pin(tokio::time::sleep(timeout));
                *sleep_slot = Some(next);
                let _ = sleep_slot.as_mut().unwrap().as_mut().poll(cx);
                return None;
            }
            *sleep_slot = None;
            return Some(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("simulated {label} timeout after {}s", timeout.as_secs()),
            ));
        }
        None
    }
}

impl AsyncRead for TurmoilStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(result) => {
                self.read_sleep = None;
                Poll::Ready(result)
            }
            Poll::Pending => {
                let this = self.get_mut();
                if let Some(err) = Self::poll_keepalive_timer(
                    &mut this.read_sleep,
                    this.read_idle_timeout,
                    &this.link_health,
                    this.created_drop_epoch,
                    cx,
                    "keepalive",
                ) {
                    return Poll::Ready(Err(err));
                }
                Poll::Pending
            }
        }
    }
}

impl AsyncWrite for TurmoilStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                self.write_sleep = None;
                if n > 0
                    && let Some(hook) = &self.on_write_bytes
                {
                    hook(n);
                }
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(err)) => {
                self.write_sleep = None;
                Poll::Ready(Err(err))
            }
            Poll::Pending => {
                let this = self.get_mut();
                if let Some(err) = Self::poll_keepalive_timer(
                    &mut this.write_sleep,
                    this.write_idle_timeout,
                    &this.link_health,
                    this.created_drop_epoch,
                    cx,
                    "keepalive",
                ) {
                    return Poll::Ready(Err(err));
                }
                Poll::Pending
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl TransportStream for TurmoilStream {
    fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        let mut addr = self.inner.peer_addr()?;
        if let Some(ip) = self.peer_ip_override {
            addr.set_ip(ip);
        }
        Ok(addr)
    }

    fn into_split(self: Box<Self>) -> (BoxedRead, BoxedWrite) {
        let (read_half, write_half) = tokio::io::split(*self);
        (
            Box::new(read_half) as BoxedRead,
            Box::new(write_half) as BoxedWrite,
        )
    }
}

struct ThreeHostListener {
    inner: TcpListener,
    host_name: &'static str,
    loopback_only: bool,
    edges: ThreeEdgeHealth,
    on_data_write_bytes: Option<ByteHookFn>,
}

impl ThreeHostListener {
    async fn accept_matching(
        &self,
        idle_timeout: Option<Duration>,
        on_write_bytes: Option<ByteHookFn>,
    ) -> std::io::Result<(BoxedTransportStream, std::net::SocketAddr)> {
        let master_ip = turmoil::lookup(MASTER_HOST);
        loop {
            let (stream, addr) = self.inner.accept().await?;
            let via_master_tunnel = addr.ip() == master_ip;
            if self.loopback_only && !via_master_tunnel {
                drop(stream);
                continue;
            }
            let peer_ip_override =
                via_master_tunnel.then_some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            let health = self.edges.for_source_accept(addr.ip(), master_ip);
            let stream = TurmoilStream::new(
                stream,
                peer_ip_override,
                idle_timeout,
                idle_timeout,
                health,
                on_write_bytes,
            );
            let reported_addr = stream.peer_addr()?;
            return Ok((BoxedTransportStream::new(stream), reported_addr));
        }
    }
}

impl TransportListener for ThreeHostListener {
    fn advertised_addr(&self, _bind_ip: Option<&str>) -> anyhow::Result<std::net::SocketAddr> {
        let port = self.inner.local_addr()?.port();
        Ok(std::net::SocketAddr::new(
            turmoil::lookup(self.host_name),
            port,
        ))
    }

    fn tunnel_target_ip(&self) -> anyhow::Result<std::net::IpAddr> {
        Ok(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
    }

    fn accept_control<'a>(
        &'a self,
        config: &'a remote::TcpConfig,
    ) -> BoxFuture<'a, std::io::Result<(BoxedTransportStream, std::net::SocketAddr)>> {
        Box::pin(async move {
            let idle =
                (config.keepalive_sec > 0).then(|| Duration::from_secs(config.keepalive_sec));
            self.accept_matching(idle, None).await
        })
    }

    fn accept_data<'a>(
        &'a self,
        _profile: remote::NetworkProfile,
        keepalive_sec: u64,
    ) -> BoxFuture<'a, std::io::Result<(BoxedTransportStream, std::net::SocketAddr)>> {
        Box::pin(async move {
            let idle =
                (keepalive_sec > 0).then(|| Duration::from_secs(keepalive_sec.saturating_mul(2)));
            self.accept_matching(idle, self.on_data_write_bytes.clone())
                .await
        })
    }
}

struct ThreeHostTransport {
    host_name: &'static str,
    edges: ThreeEdgeHealth,
    on_data_write_bytes: Option<ByteHookFn>,
    inject_connect_refused_once: Option<Arc<AtomicBool>>,
}

impl Transport for ThreeHostTransport {
    fn bind_control<'a>(
        &'a self,
        _config: &'a remote::TcpConfig,
        bind_ip: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Box<dyn TransportListener>>> {
        Box::pin(async move {
            let loopback_only = bind_ip
                .and_then(|ip| ip.parse::<std::net::IpAddr>().ok())
                .is_some_and(remote::tls::is_loopback_peer);
            let listener = TcpListener::bind(("0.0.0.0", 0)).await?;
            Ok(Box::new(ThreeHostListener {
                inner: listener,
                host_name: self.host_name,
                loopback_only,
                edges: self.edges.clone(),
                on_data_write_bytes: self.on_data_write_bytes.clone(),
            }) as Box<dyn TransportListener>)
        })
    }

    fn bind_data<'a>(
        &'a self,
        _config: &'a remote::TcpConfig,
        bind_ip: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Box<dyn TransportListener>>> {
        Box::pin(async move {
            let loopback_only = bind_ip
                .and_then(|ip| ip.parse::<std::net::IpAddr>().ok())
                .is_some_and(remote::tls::is_loopback_peer);
            let listener = TcpListener::bind(("0.0.0.0", 0)).await?;
            Ok(Box::new(ThreeHostListener {
                inner: listener,
                host_name: self.host_name,
                loopback_only,
                edges: self.edges.clone(),
                on_data_write_bytes: self.on_data_write_bytes.clone(),
            }) as Box<dyn TransportListener>)
        })
    }

    fn connect_control<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        config: &'a remote::TcpConfig,
    ) -> BoxFuture<'a, anyhow::Result<BoxedTransportStream>> {
        self.connect_control_with_timeout(
            addr,
            config,
            Duration::from_secs(config.conn_timeout_sec),
        )
    }

    fn connect_control_with_timeout<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        config: &'a remote::TcpConfig,
        timeout: Duration,
    ) -> BoxFuture<'a, anyhow::Result<BoxedTransportStream>> {
        Box::pin(async move {
            if let Some(flag) = &self.inject_connect_refused_once
                && flag.swap(false, Ordering::AcqRel)
            {
                return Err(anyhow::Error::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "injected pre-connect refusal",
                )));
            }
            let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
                .await
                .map_err(|_| anyhow!("connection timeout after {}s", timeout.as_secs()))?
                .with_context(|| format!("failed to connect control stream to {addr}"))?;
            let master_ip = turmoil::lookup(MASTER_HOST);
            let via_master_tunnel = addr.ip() == master_ip;
            let peer_ip_override =
                via_master_tunnel.then_some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            let health = self.edges.for_dest_connect(addr.ip(), master_ip);
            let idle =
                (config.keepalive_sec > 0).then(|| Duration::from_secs(config.keepalive_sec));
            Ok(BoxedTransportStream::new(TurmoilStream::new(
                stream,
                peer_ip_override,
                idle,
                idle,
                health,
                None,
            )))
        })
    }

    fn connect_data<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        _profile: remote::NetworkProfile,
        keepalive_sec: u64,
    ) -> BoxFuture<'a, std::io::Result<BoxedTransportStream>> {
        Box::pin(async move {
            let stream = TcpStream::connect(addr).await?;
            let master_ip = turmoil::lookup(MASTER_HOST);
            let via_master_tunnel = addr.ip() == master_ip;
            let peer_ip_override =
                via_master_tunnel.then_some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            let health = self.edges.for_dest_connect(addr.ip(), master_ip);
            let idle =
                (keepalive_sec > 0).then(|| Duration::from_secs(keepalive_sec.saturating_mul(2)));
            Ok(BoxedTransportStream::new(TurmoilStream::new(
                stream,
                peer_ip_override,
                idle,
                idle,
                health,
                None,
            )))
        })
    }
}

/// Fault modes injected during simulated `rcpd` startup readiness on a target host.
#[derive(Clone)]
enum BootstrapFault {
    None,
    Refusal(String),
    RawStderr(Vec<u8>),
    StallReadiness,
    TamperedReadiness {
        fingerprint: Option<remote::protocol::CertFingerprint>,
        files_in_flight: common::ConcurrencyLimit,
        max_connections: NonZeroUsize,
    },
}

/// Per-daemon test customization attached to simulated `"source"` or `"destination"` hosts.
#[derive(Clone)]
struct DaemonFaultConfig {
    bootstrap: BootstrapFault,
    startup_notices: Vec<String>,
    tracing_burst_count: usize,
    override_preview_only: Option<bool>,
    post_copy_exit_error: Option<String>,
    on_data_write_bytes: Option<ByteHookFn>,
    inject_connect_refused_once: Option<Arc<AtomicBool>>,
    auto_files_in_flight: NonZeroUsize,
}

impl Default for DaemonFaultConfig {
    fn default() -> Self {
        Self {
            bootstrap: BootstrapFault::None,
            startup_notices: Vec::new(),
            tracing_burst_count: 0,
            override_preview_only: None,
            post_copy_exit_error: None,
            on_data_write_bytes: None,
            inject_connect_refused_once: None,
            auto_files_in_flight: NonZeroUsize::new(4).unwrap(),
        }
    }
}

struct SpawnSpec {
    role: RcpdRole,
    config: RcpdConfig,
    bind_ip: Option<String>,
    exit_tx: oneshot::Sender<anyhow::Result<()>>,
}

#[derive(Default)]
struct PendingChannels {
    stdin: Option<TcpStream>,
    stdout: Option<TcpStream>,
    stderr: Option<TcpStream>,
}

/// Shared coordination between `"master"`'s [`TurmoilBackend`] and a simulated host's SSHD loop.
#[derive(Clone)]
struct HostDaemonRegistry {
    specs: Arc<Mutex<HashMap<u64, SpawnSpec>>>,
    faults: Arc<Mutex<DaemonFaultConfig>>,
    active_daemons: Arc<AtomicUsize>,
    exited_notify: Arc<Notify>,
    recorded_outcomes: Arc<Mutex<Vec<(RcpdRole, RcpdResult)>>>,
}

impl HostDaemonRegistry {
    fn new(
        faults: DaemonFaultConfig,
        active_daemons: Arc<AtomicUsize>,
        exited_notify: Arc<Notify>,
        recorded_outcomes: Arc<Mutex<Vec<(RcpdRole, RcpdResult)>>>,
    ) -> Self {
        Self {
            specs: Arc::new(Mutex::new(HashMap::new())),
            faults: Arc::new(Mutex::new(faults)),
            active_daemons,
            exited_notify,
            recorded_outcomes,
        }
    }

    fn register(&self, id: u64, spec: SpawnSpec) {
        self.specs.lock().unwrap().insert(id, spec);
    }

    fn take_spec(&self, id: u64) -> Option<SpawnSpec> {
        self.specs.lock().unwrap().remove(&id)
    }
}

struct ActiveCounterGuard {
    counter: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl ActiveCounterGuard {
    fn enter(counter: Arc<AtomicUsize>, notify: Arc<Notify>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self { counter, notify }
    }
}

impl Drop for ActiveCounterGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

fn build_daemon_session_config(
    role: RcpdRole,
    config: &RcpdConfig,
    bind_ip: Option<String>,
    faults: &DaemonFaultConfig,
) -> anyhow::Result<(DaemonSessionConfig, remote::RcpdReadiness)> {
    let files_in_flight = match config.files_in_flight {
        RcpdFilesInFlight::Automatic => {
            common::ResolvedFilesInFlight::automatic_with(faults.auto_files_in_flight)
        }
        RcpdFilesInFlight::ResolvedAutomatic(n) => common::ResolvedFilesInFlight::automatic_with(n),
        RcpdFilesInFlight::Explicit(common::ConcurrencyLimit::Limited(n)) => {
            common::ResolvedFilesInFlight::explicit(n)
        }
        RcpdFilesInFlight::Explicit(common::ConcurrencyLimit::Unlimited) => {
            common::ResolvedFilesInFlight::unlimited()
        }
        RcpdFilesInFlight::DeprecatedMaxOpenFiles(limit) => {
            let legacy = match limit {
                common::ConcurrencyLimit::Limited(n) => n.get(),
                common::ConcurrencyLimit::Unlimited => 0,
            };
            common::ResolvedFilesInFlight::forwarded_legacy(legacy)
        }
    };
    let max_connections =
        NonZeroUsize::new(config.max_connections).context("max_connections must be nonzero")?;
    let pending_mult = NonZeroUsize::new(config.pending_writes_multiplier)
        .context("pending_writes_multiplier must be nonzero")?;
    let concurrency =
        remote::resolve_remote_concurrency(files_in_flight.limit(), max_connections, pending_mult)?;
    let admission = if config.preview_only {
        common::EndpointAdmission::Disabled
    } else {
        let leaf_ceiling = match concurrency.files_in_flight() {
            common::ConcurrencyLimit::Limited(n) => n,
            common::ConcurrencyLimit::Unlimited => NonZeroUsize::new(16).unwrap(),
        };
        let ssh_tunnels = if role == RcpdRole::Destination {
            config.ssh_tunnels
        } else {
            0
        };
        common::EndpointAdmission::Remote(common::RemoteResources::for_limits(
            Some(NonZeroU64::new(1024).unwrap()),
            leaf_ceiling,
            concurrency.max_connections(),
            concurrency.max_pending_files(),
            ssh_tunnels,
        )?)
    };
    let tcp_config = remote::TcpConfig {
        port_ranges: config.port_ranges.clone(),
        conn_timeout_sec: config.remote_copy_conn_timeout_sec,
        network_profile: config.network_profile,
        buffer_size: config.buffer_size,
        buffer_retention_limit: config.buffer_retention_limit,
        keepalive_sec: config.remote_keepalive_sec,
    };
    let overwrite_compare = common::parse_metadata_cmp_settings(&config.overwrite_compare)?;
    let copy_settings_base = common::copy::Settings {
        reflink: Default::default(),
        local_copy_handoff: Default::default(),
        local_copy_buffer_retention_limit: Default::default(),
        dereference: config.dereference,
        fail_early: config.fail_early,
        overwrite: config.overwrite,
        overwrite_compare,
        overwrite_filter: None,
        ignore_existing: config.ignore_existing,
        chunk_size: config.chunk_size as u64,
        skip_specials: config.skip_specials,
        remote_copy_buffer_size: tcp_config.effective_buffer_size(),
        filter: None,
        dry_run: None,
        delete: None,
    };
    let cert_key = if config.encryption {
        Some(generate_self_signed_cert()?)
    } else {
        None
    };
    let readiness = remote::RcpdReadiness {
        fingerprint: cert_key.as_ref().map(|c| c.fingerprint),
        files_in_flight: concurrency.files_in_flight(),
        max_connections: concurrency.max_connections(),
    };
    let session_config = DaemonSessionConfig {
        preview_only: faults.override_preview_only.unwrap_or(config.preview_only),
        overwrite_manifest_max_entries: config.overwrite_manifest_max_entries,
        bind_ip,
        tcp_config,
        concurrency,
        admission,
        files_source: files_in_flight.source(),
        cert_key,
        copy_settings_base,
    };
    Ok((session_config, readiness))
}

async fn run_simulated_daemon_task(
    host_name: &'static str,
    spec: SpawnSpec,
    mut stdin_stream: TcpStream,
    stdout_stream: TcpStream,
    mut stderr_stream: TcpStream,
    edges: ThreeEdgeHealth,
    registry: HostDaemonRegistry,
) {
    let _active = ActiveCounterGuard::enter(
        Arc::clone(&registry.active_daemons),
        Arc::clone(&registry.exited_notify),
    );
    let faults = registry.faults.lock().unwrap().clone();
    match &faults.bootstrap {
        BootstrapFault::Refusal(msg) => {
            let _ = stderr_stream
                .write_all(format!("RCP_ERROR {msg}\n").as_bytes())
                .await;
            let _ = stderr_stream.shutdown().await;
            let _ = spec.exit_tx.send(Ok(()));
            return;
        }
        BootstrapFault::RawStderr(bytes) => {
            let _ = stderr_stream.write_all(bytes).await;
            let _ = stderr_stream.shutdown().await;
            let _ = spec.exit_tx.send(Ok(()));
            return;
        }
        BootstrapFault::StallReadiness => {
            // hold stderr open until master drops stdin on bootstrap timeout
            let mut buf = [0u8; 64];
            let _ = stdin_stream.read(&mut buf).await;
            let _ = spec.exit_tx.send(Ok(()));
            return;
        }
        BootstrapFault::None | BootstrapFault::TamperedReadiness { .. } => {}
    }
    let (session_config, _computed_readiness) =
        match build_daemon_session_config(spec.role, &spec.config, spec.bind_ip, &faults) {
            Ok(built) => built,
            Err(err) => {
                let _ = stderr_stream
                    .write_all(format!("RCP_ERROR {err:#}\n").as_bytes())
                    .await;
                let _ = stderr_stream.shutdown().await;
                let _ = spec.exit_tx.send(Err(err));
                return;
            }
        };
    let ready_line = match &faults.bootstrap {
        BootstrapFault::TamperedReadiness {
            fingerprint,
            files_in_flight,
            max_connections,
        } => {
            let fp = fingerprint
                .as_ref()
                .map_or_else(|| "-".to_owned(), remote::tls::fingerprint_to_hex);
            let files = match files_in_flight {
                common::ConcurrencyLimit::Unlimited => "unlimited".to_owned(),
                common::ConcurrencyLimit::Limited(v) => v.to_string(),
            };
            format!("RCP_READY {fp} {files} {max_connections}")
        }
        _ => remote::format_rcpd_readiness(
            session_config.cert_key.as_ref().map(|c| &c.fingerprint),
            session_config.concurrency,
        ),
    };
    if stderr_stream
        .write_all(format!("{ready_line}\n").as_bytes())
        .await
        .is_err()
    {
        let _ = spec.exit_tx.send(Ok(()));
        return;
    }
    let _ = stderr_stream.shutdown().await;
    let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
    for notice in &faults.startup_notices {
        let _ = tracing_tx.send(common::remote_tracing::TracingMessage::Log {
            timestamp: std::time::SystemTime::now(),
            level: "WARN".to_owned(),
            target: common::NOTICE_TARGET.to_owned(),
            message: notice.clone(),
        });
    }
    let burst_task = if faults.tracing_burst_count > 0 {
        let tx = tracing_tx.clone();
        let count = faults.tracing_burst_count;
        let role = spec.role;
        Some(tokio::spawn(async move {
            for idx in 0..count {
                let _ = tx.send(common::remote_tracing::TracingMessage::Log {
                    timestamp: std::time::SystemTime::now(),
                    level: "WARN".to_owned(),
                    target: common::NOTICE_TARGET.to_owned(),
                    message: format!("burst-{role:?}-{idx}"),
                });
                if idx % 10 == 0 {
                    let _ = tx.send(common::remote_tracing::TracingMessage::Progress(
                        common::progress::SerializableProgress {
                            bytes_copied: (idx as u64) * 1024,
                            files_copied: idx as u64,
                            ..Default::default()
                        },
                    ));
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }))
    } else {
        None
    };
    drop(tracing_tx);
    // give daemon's own `IdleTimeoutReader` (1 * keepalive_sec) precedence over transport-level
    // timeout so control-link partitions genuinely exercise daemon_session's KeepaliveTimedOut.
    // Separate stdin and stdout sockets mirror OS/SSH pipes: closing master stdin delivers instant
    // EOF while stdout remains open to drain queued tracing frames
    let keepalive = session_config.tcp_config.keepalive_sec;
    let daemon_stdio_timeout =
        (keepalive > 0).then(|| Duration::from_secs(keepalive.saturating_mul(2)));
    let stdin_reader = TurmoilStream::new(
        stdin_stream,
        None,
        daemon_stdio_timeout,
        None,
        edges.for_role(spec.role),
        None,
    );
    let stdout_writer = TurmoilStream::new(
        stdout_stream,
        None,
        None,
        daemon_stdio_timeout,
        edges.for_role(spec.role),
        None,
    );
    let remote_cleanup = if spec.role == RcpdRole::Destination {
        Some(remote::RemoteCleanup::new().expect("RemoteCleanup::new should succeed"))
    } else {
        None
    };
    let transport: Arc<dyn Transport> = Arc::new(ThreeHostTransport {
        host_name,
        edges: edges.clone(),
        on_data_write_bytes: faults.on_data_write_bytes.clone(),
        inject_connect_refused_once: faults.inject_connect_refused_once.clone(),
    });
    let progress = Arc::new(common::progress::Progress::new());
    let result = common::task_scope::scope_progress(
        progress,
        daemon_session::run_daemon_session(
            session_config,
            stdin_reader,
            stdout_writer,
            tracing_rx,
            remote_cleanup.clone(),
            transport,
        ),
    )
    .await;
    if let Some(cleanup) = remote_cleanup {
        cleanup.finish();
    }
    if let Some(task) = burst_task {
        let _ = task.await;
    }
    let exit_result = match faults.post_copy_exit_error {
        Some(err_msg) => Err(anyhow!("{err_msg}")),
        None => match &result {
            RcpdResult::Success { .. } => Ok(()),
            RcpdResult::Failure { error, .. } => Err(anyhow!("rcpd operation failed: {error}")),
        },
    };
    registry
        .recorded_outcomes
        .lock()
        .unwrap()
        .push((spec.role, result));
    let _ = spec.exit_tx.send(exit_result);
}

/// Runs the simulated SSHD daemon launcher on a Turmoil host (`"source"` or `"destination"`).
async fn run_simulated_sshd(
    host_name: &'static str,
    edges: ThreeEdgeHealth,
    registry: HostDaemonRegistry,
) -> Result<(), Box<dyn std::error::Error>> {
    let stdin_listener = TcpListener::bind(("0.0.0.0", SSH_STDIN_PORT)).await?;
    let stdout_listener = TcpListener::bind(("0.0.0.0", SSH_STDOUT_PORT)).await?;
    let stderr_listener = TcpListener::bind(("0.0.0.0", SSH_STDERR_PORT)).await?;
    let pending: Arc<Mutex<HashMap<u64, PendingChannels>>> = Arc::new(Mutex::new(HashMap::new()));
    let maybe_spawn = |id: u64,
                       pending: &Arc<Mutex<HashMap<u64, PendingChannels>>>,
                       edges: &ThreeEdgeHealth,
                       registry: &HostDaemonRegistry| {
        let ready = {
            let mut map = pending.lock().unwrap();
            let ready = map
                .get(&id)
                .is_some_and(|ch| ch.stdin.is_some() && ch.stdout.is_some() && ch.stderr.is_some());
            if ready { map.remove(&id) } else { None }
        };
        if let Some(PendingChannels {
            stdin: Some(stdin),
            stdout: Some(stdout),
            stderr: Some(stderr),
        }) = ready
            && let Some(spec) = registry.take_spec(id)
        {
            tokio::spawn(run_simulated_daemon_task(
                host_name,
                spec,
                stdin,
                stdout,
                stderr,
                edges.clone(),
                registry.clone(),
            ));
        }
    };
    loop {
        tokio::select! {
            accepted = stdin_listener.accept() => {
                let (mut stream, _) = accepted?;
                let pending = Arc::clone(&pending);
                let edges = edges.clone();
                let registry = registry.clone();
                tokio::spawn(async move {
                    let mut id_buf = [0u8; 8];
                    if stream.read_exact(&mut id_buf).await.is_err() {
                        return;
                    }
                    let id = u64::from_le_bytes(id_buf);
                    pending.lock().unwrap().entry(id).or_default().stdin = Some(stream);
                    maybe_spawn(id, &pending, &edges, &registry);
                });
            }
            accepted = stdout_listener.accept() => {
                let (mut stream, _) = accepted?;
                let pending = Arc::clone(&pending);
                let edges = edges.clone();
                let registry = registry.clone();
                tokio::spawn(async move {
                    let mut id_buf = [0u8; 8];
                    if stream.read_exact(&mut id_buf).await.is_err() {
                        return;
                    }
                    let id = u64::from_le_bytes(id_buf);
                    pending.lock().unwrap().entry(id).or_default().stdout = Some(stream);
                    maybe_spawn(id, &pending, &edges, &registry);
                });
            }
            accepted = stderr_listener.accept() => {
                let (mut stream, _) = accepted?;
                let pending = Arc::clone(&pending);
                let edges = edges.clone();
                let registry = registry.clone();
                tokio::spawn(async move {
                    let mut id_buf = [0u8; 8];
                    if stream.read_exact(&mut id_buf).await.is_err() {
                        return;
                    }
                    let id = u64::from_le_bytes(id_buf);
                    pending.lock().unwrap().entry(id).or_default().stderr = Some(stream);
                    maybe_spawn(id, &pending, &edges, &registry);
                });
            }
        }
    }
}

struct TurmoilDaemonHandle {
    exit_rx: oneshot::Receiver<anyhow::Result<()>>,
}

impl DaemonHandle for TurmoilDaemonHandle {
    fn wait(self: Box<Self>) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
            match self.exit_rx.await {
                Ok(res) => res,
                Err(_) => Err(anyhow!(
                    "simulated rcpd task terminated without reporting exit status"
                )),
            }
        })
    }
}

struct TurmoilTunnelHandle {
    cancel: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    _active_guard: ActiveCounterGuard,
}

impl Drop for TurmoilTunnelHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl TunnelHandle for TurmoilTunnelHandle {
    fn close(mut self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            self.cancel.cancel();
            for task in self.tasks.drain(..) {
                let _ = task.await;
            }
        })
    }
}

/// Configurable behavior for [`TurmoilBackend::open_tunnels`].
#[derive(Clone, Default)]
enum TunnelBehavior {
    #[default]
    Normal,
    StallUntilCancelled {
        started: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    },
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Shared observation counters recorded by [`TurmoilBackend`].
#[derive(Default)]
struct BackendObservations {
    source_probe_calls: AtomicUsize,
    dest_probe_calls: AtomicUsize,
    dest_agent_forward_disabled: AtomicBool,
    tunnel_requests: Mutex<Vec<TunnelRequest>>,
    spawned_configs: Mutex<Vec<(RcpdRole, RcpdConfig, Option<String>)>>,
}

struct TurmoilBackend {
    edges: ThreeEdgeHealth,
    source_registry: HostDaemonRegistry,
    dest_registry: HostDaemonRegistry,
    bootstrap_timeout: Duration,
    keepalive_sec: u64,
    next_spawn_id: AtomicU64,
    source_peer: Result<(PeerIdentity, Option<std::net::SocketAddr>), String>,
    dest_peer: Result<(PeerIdentity, Option<std::net::SocketAddr>), String>,
    tunnel_behavior: TunnelBehavior,
    active_tunnels: Arc<AtomicUsize>,
    tunnels_closed_notify: Arc<Notify>,
    observations: Arc<BackendObservations>,
}

impl TurmoilBackend {
    fn spawn_relay_listener(
        listener: TcpListener,
        target_port: u16,
        edges: ThreeEdgeHealth,
        keepalive_sec: u64,
        cancel: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let idle =
                (keepalive_sec > 0).then(|| Duration::from_secs(keepalive_sec.saturating_mul(2)));
            let connect_timeout = Duration::from_secs(keepalive_sec.max(2));
            // JoinSet aborts every child relay automatically if this listener task is dropped
            let mut relays = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    accepted = listener.accept() => {
                        let Ok((inbound, _)) = accepted else {
                            break;
                        };
                        let edges = edges.clone();
                        let relay_cancel = cancel.clone();
                        relays.spawn(async move {
                            let target = std::net::SocketAddr::new(
                                turmoil::lookup(SOURCE_HOST),
                                target_port,
                            );
                            let outbound = tokio::select! {
                                _ = relay_cancel.cancelled() => return,
                                res = tokio::time::timeout(
                                    connect_timeout,
                                    TcpStream::connect(target),
                                ) => match res {
                                    Ok(Ok(stream)) => stream,
                                    _ => return,
                                },
                            };
                            let mut in_stream = TurmoilStream::new(
                                inbound,
                                None,
                                idle,
                                idle,
                                Arc::clone(&edges.master_dst),
                                None,
                            );
                            let mut out_stream = TurmoilStream::new(
                                outbound,
                                None,
                                idle,
                                idle,
                                Arc::clone(&edges.master_src),
                                None,
                            );
                            tokio::select! {
                                _ = relay_cancel.cancelled() => {}
                                _ = tokio::io::copy_bidirectional(&mut in_stream, &mut out_stream) => {}
                            }
                        });
                    }
                }
            }
            relays.abort_all();
        })
    }
}

impl RemoteBackend for TurmoilBackend {
    fn probe_peer_identity(
        &self,
        role: RcpdRole,
        _timeout: Duration,
    ) -> BoxFuture<'_, anyhow::Result<(PeerIdentity, Option<std::net::SocketAddr>)>> {
        let outcome = match role {
            RcpdRole::Source => {
                self.observations
                    .source_probe_calls
                    .fetch_add(1, Ordering::SeqCst);
                self.source_peer.clone()
            }
            RcpdRole::Destination => {
                self.observations
                    .dest_probe_calls
                    .fetch_add(1, Ordering::SeqCst);
                self.dest_peer.clone()
            }
        };
        Box::pin(async move { outcome.map_err(|msg| anyhow!("{msg}")) })
    }

    fn disable_destination_agent_forward(&mut self) {
        self.observations
            .dest_agent_forward_disabled
            .store(true, Ordering::SeqCst);
    }

    fn spawn_rcpd<'a>(
        &'a self,
        role: RcpdRole,
        config: &'a RcpdConfig,
        bind_ip: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<SpawnedDaemon>> {
        self.observations.spawned_configs.lock().unwrap().push((
            role,
            config.clone(),
            bind_ip.map(str::to_owned),
        ));
        let (target_host, registry) = match role {
            RcpdRole::Source => (SOURCE_HOST, self.source_registry.clone()),
            RcpdRole::Destination => (DEST_HOST, self.dest_registry.clone()),
        };
        let spawn_id = self.next_spawn_id.fetch_add(1, Ordering::SeqCst);
        let bootstrap_timeout = self.bootstrap_timeout;
        let keepalive_sec = self.keepalive_sec;
        let edges = self.edges.clone();
        Box::pin(async move {
            let (exit_tx, exit_rx) = oneshot::channel();
            registry.register(
                spawn_id,
                SpawnSpec {
                    role,
                    config: config.clone(),
                    bind_ip: bind_ip.map(str::to_owned),
                    exit_tx,
                },
            );
            let connect_triple = async {
                let mut stdin = TcpStream::connect((target_host, SSH_STDIN_PORT))
                    .await
                    .with_context(|| {
                        format!("failed to open simulated SSH stdin to {target_host}")
                    })?;
                let mut stdout = TcpStream::connect((target_host, SSH_STDOUT_PORT))
                    .await
                    .with_context(|| {
                        format!("failed to open simulated SSH stdout to {target_host}")
                    })?;
                let mut stderr = TcpStream::connect((target_host, SSH_STDERR_PORT))
                    .await
                    .with_context(|| {
                        format!("failed to open simulated SSH stderr to {target_host}")
                    })?;
                stdin.write_all(&spawn_id.to_le_bytes()).await?;
                stdout.write_all(&spawn_id.to_le_bytes()).await?;
                stderr.write_all(&spawn_id.to_le_bytes()).await?;
                anyhow::Ok((stdin, stdout, stderr))
            };
            let bootstrap_deadline = tokio::time::Instant::now() + bootstrap_timeout;
            let (stdin_stream, stdout_stream, stderr_stream) =
                match tokio::time::timeout_at(bootstrap_deadline, connect_triple).await {
                    Ok(Ok(triple)) => triple,
                    Ok(Err(err)) => {
                        let _ = registry.take_spec(spawn_id);
                        return Err(err);
                    }
                    Err(_) => {
                        let _ = registry.take_spec(spawn_id);
                        anyhow::bail!(
                            "timed out after {}s connecting simulated SSH to {target_host}",
                            bootstrap_timeout.as_secs()
                        );
                    }
                };
            let mut stderr_reader = tokio::io::BufReader::new(stderr_stream);
            let readiness = match tokio::time::timeout_at(
                bootstrap_deadline,
                remote::read_rcpd_startup_record(&mut stderr_reader),
            )
            .await
            {
                Ok(Ok(readiness)) => readiness,
                Ok(Err(err)) => {
                    let _ = registry.take_spec(spawn_id);
                    drop(stderr_reader);
                    drop(stdin_stream);
                    drop(stdout_stream);
                    let _ = tokio::time::timeout(Duration::from_secs(2), exit_rx).await;
                    return Err(err);
                }
                Err(_) => {
                    let _ = registry.take_spec(spawn_id);
                    drop(stderr_reader);
                    drop(stdin_stream);
                    drop(stdout_stream);
                    let _ = tokio::time::timeout(Duration::from_secs(2), exit_rx).await;
                    anyhow::bail!(
                        "timed out after {}s waiting for {role:?} rcpd readiness on '{target_host}'",
                        bootstrap_timeout.as_secs()
                    );
                }
            };
            drop(stderr_reader);
            // master-side SSH ServerAlive budget matches production (~1 * keepalive_sec)
            let master_alive_timeout =
                (keepalive_sec > 0).then(|| Duration::from_secs(keepalive_sec));
            let stdin = TurmoilStream::new(
                stdin_stream,
                None,
                None,
                master_alive_timeout,
                edges.for_role(role),
                None,
            );
            let stdout = TurmoilStream::new(
                stdout_stream,
                None,
                master_alive_timeout,
                None,
                edges.for_role(role),
                None,
            );
            Ok(SpawnedDaemon {
                readiness,
                stdin: Box::new(stdin) as BoxedWrite,
                stdout: Box::new(stdout) as BoxedRead,
                handle: Box::new(TurmoilDaemonHandle { exit_rx }),
            })
        })
    }

    fn open_tunnels(&self, request: TunnelRequest) -> BoxFuture<'_, anyhow::Result<OpenedTunnels>> {
        self.observations
            .tunnel_requests
            .lock()
            .unwrap()
            .push(request.clone());
        let behavior = self.tunnel_behavior.clone();
        let edges = self.edges.clone();
        let keepalive_sec = self.keepalive_sec;
        let active_tunnels = Arc::clone(&self.active_tunnels);
        let tunnels_closed_notify = Arc::clone(&self.tunnels_closed_notify);
        Box::pin(async move {
            if let TunnelBehavior::StallUntilCancelled { started, dropped } = behavior {
                let _flag = DropFlag(dropped);
                started.store(true, Ordering::Release);
                return std::future::pending::<anyhow::Result<OpenedTunnels>>().await;
            }
            let master_ip = turmoil::lookup(MASTER_HOST);
            let cancel = CancellationToken::new();
            let mut tasks = Vec::new();
            let control_listener = TcpListener::bind(("0.0.0.0", 0)).await?;
            let control_addr =
                std::net::SocketAddr::new(master_ip, control_listener.local_addr()?.port());
            tasks.push(Self::spawn_relay_listener(
                control_listener,
                request.target_control.port(),
                edges.clone(),
                keepalive_sec,
                cancel.clone(),
            ));
            let mut data_addrs = Vec::new();
            if let Some(target_data) = request.target_data {
                let count = request.requested_tunnels.max(1);
                for _ in 0..count {
                    let data_listener = TcpListener::bind(("0.0.0.0", 0)).await?;
                    let addr =
                        std::net::SocketAddr::new(master_ip, data_listener.local_addr()?.port());
                    data_addrs.push(addr);
                    tasks.push(Self::spawn_relay_listener(
                        data_listener,
                        target_data.port(),
                        edges.clone(),
                        keepalive_sec,
                        cancel.clone(),
                    ));
                }
            }
            let tunnel_count = data_addrs.len().max(1);
            let active_guard = ActiveCounterGuard::enter(active_tunnels, tunnels_closed_notify);
            Ok(OpenedTunnels {
                control_addr,
                data_addrs,
                summary: format!("{tunnel_count} simulated SSH tunnel(s)"),
                handle: Box::new(TurmoilTunnelHandle {
                    cancel,
                    tasks,
                    _active_guard: active_guard,
                }),
            })
        })
    }
}

fn default_peer_identity(hostname: &str) -> PeerIdentity {
    PeerIdentity {
        hostname: hostname.to_owned(),
        boot_id: Some(format!("boot-{hostname}")),
        net_ns: Some(format!("net:[{hostname}]")),
        ssh_server_port: Some(22),
    }
}

fn default_resolved_ssh(host: &str) -> ResolvedSshConfig {
    ResolvedSshConfig {
        original_host: host.to_owned(),
        target: remote::protocol::SshTarget {
            user: None,
            host: host.to_owned(),
            port: Some(22),
        },
        forward_agent_enabled: false,
        address_family: SshAddressFamily::Inet,
        strict_host_key_checking: SshStrictHostKeyChecking::No,
        proxy_jump: None,
        proxy_command: None,
    }
}

struct PlanBuilder {
    src_path: PathBuf,
    dst_path: PathBuf,
    initial_topology: InitialTopology,
    transport: RemoteTransport,
    no_encryption: bool,
    dry_run: Option<common::config::DryRunMode>,
    filter: Option<common::filter::FilterSettings>,
    dereference: bool,
    fail_early: bool,
    conn_timeout_sec: u64,
    keepalive_sec: u64,
    probe_timeout_sec: u64,
    max_connections: usize,
    automatic_concurrency: bool,
    ssh_tunnels: Option<NonZeroUsize>,
    buffer_size: usize,
    resolved_src_ssh: Option<ResolvedSshConfig>,
    resolved_dst_ssh: Option<ResolvedSshConfig>,
}

impl PlanBuilder {
    fn new(src_path: PathBuf, dst_path: PathBuf) -> Self {
        Self {
            src_path,
            dst_path,
            initial_topology: InitialTopology::ThreeHost,
            transport: RemoteTransport::Tcp,
            no_encryption: false,
            dry_run: None,
            filter: None,
            dereference: false,
            fail_early: false,
            conn_timeout_sec: 10,
            keepalive_sec: 2,
            probe_timeout_sec: 2,
            max_connections: 4,
            automatic_concurrency: false,
            ssh_tunnels: None,
            buffer_size: 16 * 1024,
            resolved_src_ssh: Some(default_resolved_ssh(SOURCE_HOST)),
            resolved_dst_ssh: Some(default_resolved_ssh(DEST_HOST)),
        }
    }

    fn build(self) -> MasterSessionPlan {
        let conns = NonZeroUsize::new(self.max_connections).unwrap();
        let mult = NonZeroUsize::new(4).unwrap();
        let (files_in_flight, concurrency_policy) = if self.automatic_concurrency {
            (
                RcpdFilesInFlight::Automatic,
                RemoteConcurrencyPolicy::Automatic,
            )
        } else {
            let concurrency = remote::resolve_remote_concurrency(
                common::ConcurrencyLimit::Limited(conns),
                conns,
                mult,
            )
            .unwrap();
            (
                RcpdFilesInFlight::Explicit(common::ConcurrencyLimit::Limited(conns)),
                RemoteConcurrencyPolicy::MasterAuthoritative {
                    files_in_flight: MasterAuthoritativeFilesInFlight::Explicit,
                    concurrency,
                },
            )
        };
        let preview_only = self.dry_run.is_some();
        let encryption = daemons_need_tls_certs(self.no_encryption, self.transport);
        let tcp = remote::TcpConfig {
            port_ranges: None,
            conn_timeout_sec: self.conn_timeout_sec,
            network_profile: remote::NetworkProfile::Internet,
            buffer_size: Some(self.buffer_size),
            buffer_retention_limit: None,
            keepalive_sec: self.keepalive_sec,
        };
        let rcpd = RcpdConfig {
            verbose: 0,
            fail_early: self.fail_early,
            preview_only,
            max_workers: 0,
            max_blocking_threads: 0,
            files_in_flight,
            ops_throttle: 0,
            iops_throttle: 0,
            chunk_size: self.buffer_size,
            auto_meta: None,
            auto_meta_histogram: false,
            auto_meta_histogram_log: None,
            auto_meta_histogram_interval: Duration::from_secs(1),
            dereference: self.dereference,
            require_toctou_safe: false,
            overwrite: false,
            overwrite_compare: "size,mtime".to_owned(),
            overwrite_manifest_max_entries: 1024,
            overwrite_filter: None,
            ignore_existing: false,
            skip_specials: false,
            debug_log_prefix: None,
            port_ranges: None,
            progress: false,
            progress_delay: None,
            remote_copy_conn_timeout_sec: self.conn_timeout_sec,
            remote_keepalive_sec: self.keepalive_sec,
            network_profile: remote::NetworkProfile::Internet,
            buffer_size: Some(self.buffer_size),
            buffer_retention_limit: None,
            max_connections: self.max_connections,
            pending_writes_multiplier: mult.get(),
            chrome_trace_prefix: None,
            timings_prefix: None,
            timings_detail: false,
            flamegraph_prefix: None,
            profile_level: None,
            tokio_console: false,
            tokio_console_port: None,
            encryption,
            ssh_tunnels: 0,
        };
        MasterSessionPlan {
            src_host: SOURCE_HOST.to_owned(),
            src_path: self.src_path,
            dst_host: DEST_HOST.to_owned(),
            dst_path: self.dst_path,
            source_is_local: matches!(self.initial_topology, InitialTopology::Upload),
            initial_topology: self.initial_topology,
            transport: self.transport,
            no_encryption: self.no_encryption,
            bind_ip: None,
            port_ranges_configured: false,
            ssh_tunnels: self.ssh_tunnels,
            remote_direct_probe_timeout_sec: self.probe_timeout_sec,
            dry_run: self.dry_run,
            preserve: common::preserve::preserve_none(),
            resolved_src_ssh: self.resolved_src_ssh,
            resolved_dst_ssh: self.resolved_dst_ssh,
            request: MasterRemoteRequest {
                tcp,
                rcpd,
                filter: self.filter,
                connection_ceiling: ConnectionCeiling::from_arg(Some(conns)),
                pending_writes_multiplier: mult,
                concurrency_policy,
            },
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum TreeEntry {
    Dir,
    File(Vec<u8>),
    Symlink(PathBuf),
}

fn snapshot_tree(root: &Path) -> BTreeMap<PathBuf, TreeEntry> {
    fn walk(base: &Path, cur: &Path, out: &mut BTreeMap<PathBuf, TreeEntry>) {
        let mut entries: Vec<_> = std::fs::read_dir(cur)
            .unwrap()
            .map(|e| e.unwrap())
            .collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            let rel = path.strip_prefix(base).unwrap().to_path_buf();
            let meta = std::fs::symlink_metadata(&path).unwrap();
            let ft = meta.file_type();
            if ft.is_dir() {
                out.insert(rel, TreeEntry::Dir);
                walk(base, &path, out);
            } else if ft.is_symlink() {
                let target = std::fs::read_link(&path).unwrap();
                out.insert(rel, TreeEntry::Symlink(target));
            } else if ft.is_file() {
                let bytes = std::fs::read(&path).unwrap();
                out.insert(rel, TreeEntry::File(bytes));
            }
        }
    }
    let mut out = BTreeMap::new();
    if root.exists() {
        walk(root, root, &mut out);
    }
    out
}

fn populate_sample_tree(root: &Path, file_count: usize, file_size: usize) {
    std::fs::create_dir_all(root.join("alpha/nested")).unwrap();
    std::fs::create_dir_all(root.join("empty_dir")).unwrap();
    std::fs::write(root.join("zero.bin"), b"").unwrap();
    for i in 0..file_count {
        let dir = if i % 2 == 0 {
            root.join("alpha")
        } else {
            root.join("alpha/nested")
        };
        let payload: Vec<u8> = (0..file_size)
            .map(|j| ((i * 31 + j * 17 + 7) & 0xff) as u8)
            .collect();
        std::fs::write(dir.join(format!("file_{i:02}.bin")), payload).unwrap();
    }
    std::os::unix::fs::symlink("file_00.bin", root.join("alpha/link_to_00")).unwrap();
}

struct ThreeHostConfig {
    plan: MasterSessionPlan,
    source_faults: DaemonFaultConfig,
    dest_faults: DaemonFaultConfig,
    source_peer: Result<(PeerIdentity, Option<std::net::SocketAddr>), String>,
    dest_peer: Result<(PeerIdentity, Option<std::net::SocketAddr>), String>,
    tunnel_behavior: TunnelBehavior,
    sim_duration: Duration,
    seed: Option<u64>,
    min_latency: Duration,
    max_latency: Duration,
}

impl ThreeHostConfig {
    fn new(plan: MasterSessionPlan) -> Self {
        Self {
            plan,
            source_faults: DaemonFaultConfig::default(),
            dest_faults: DaemonFaultConfig::default(),
            source_peer: Ok((default_peer_identity(SOURCE_HOST), None)),
            dest_peer: Ok((default_peer_identity(DEST_HOST), None)),
            tunnel_behavior: TunnelBehavior::Normal,
            sim_duration: Duration::from_secs(120),
            seed: Some(42),
            min_latency: Duration::from_millis(1),
            max_latency: Duration::from_millis(5),
        }
    }
}

struct ThreeHostRun {
    master_outcome: Option<MasterSessionOutcome>,
    observations: Arc<BackendObservations>,
    daemon_outcomes: Vec<(RcpdRole, RcpdResult)>,
    notices: Vec<String>,
    elapsed: Duration,
    active_daemons: usize,
    active_tunnels: usize,
}

/// Run a full 3-host Turmoil simulation (`"master"`, `"source"`, `"destination"`) with a caller
/// step callback that can inspect shared state and inject partitions, holds, loss, or host crashes.
fn run_three_host_sim<StepFn>(cfg: ThreeHostConfig, mut on_step: StepFn) -> ThreeHostRun
where
    StepFn: FnMut(&mut turmoil::Sim<'_>, &ThreeEdgeHealth, Duration),
{
    install_crypto_provider();
    let mut builder = turmoil::Builder::new();
    builder
        .simulation_duration(cfg.sim_duration)
        .min_message_latency(cfg.min_latency)
        .max_message_latency(cfg.max_latency);
    if let Some(seed) = cfg.seed {
        builder.rng_seed(seed);
    }
    let mut sim = builder.build();
    let edges = ThreeEdgeHealth::all_healthy();
    let active_daemons = Arc::new(AtomicUsize::new(0));
    let active_tunnels = Arc::new(AtomicUsize::new(0));
    let exited_notify = Arc::new(Notify::new());
    let tunnels_closed_notify = Arc::new(Notify::new());
    let recorded_outcomes = Arc::new(Mutex::new(Vec::new()));
    let observations = Arc::new(BackendObservations::default());
    let source_registry = HostDaemonRegistry::new(
        cfg.source_faults,
        Arc::clone(&active_daemons),
        Arc::clone(&exited_notify),
        Arc::clone(&recorded_outcomes),
    );
    let dest_registry = HostDaemonRegistry::new(
        cfg.dest_faults,
        Arc::clone(&active_daemons),
        Arc::clone(&exited_notify),
        Arc::clone(&recorded_outcomes),
    );
    {
        let edges = edges.clone();
        let reg = source_registry.clone();
        sim.host(SOURCE_HOST, move || {
            let edges = edges.clone();
            let reg = reg.clone();
            async move { run_simulated_sshd(SOURCE_HOST, edges, reg).await }
        });
    }
    {
        let edges = edges.clone();
        let reg = dest_registry.clone();
        sim.host(DEST_HOST, move || {
            let edges = edges.clone();
            let reg = reg.clone();
            async move { run_simulated_sshd(DEST_HOST, edges, reg).await }
        });
    }
    let bootstrap_timeout = Duration::from_secs(cfg.plan.request.tcp.conn_timeout_sec);
    let keepalive_sec = cfg.plan.request.tcp.keepalive_sec;
    let master_plan = Arc::new(Mutex::new(Some(cfg.plan)));
    let master_outcome: Arc<Mutex<Option<MasterSessionOutcome>>> = Arc::new(Mutex::new(None));
    let master_done = Arc::new(AtomicBool::new(false));
    let master_done_notify = Arc::new(Notify::new());
    let master_backend_slot = Arc::new(Mutex::new(Some(TurmoilBackend {
        edges: edges.clone(),
        source_registry: source_registry.clone(),
        dest_registry: dest_registry.clone(),
        bootstrap_timeout,
        keepalive_sec,
        next_spawn_id: AtomicU64::new(1),
        source_peer: cfg.source_peer,
        dest_peer: cfg.dest_peer,
        tunnel_behavior: cfg.tunnel_behavior,
        active_tunnels: Arc::clone(&active_tunnels),
        tunnels_closed_notify: Arc::clone(&tunnels_closed_notify),
        observations: Arc::clone(&observations),
    })));
    {
        let plan_slot = Arc::clone(&master_plan);
        let backend_slot = Arc::clone(&master_backend_slot);
        let outcome_slot = Arc::clone(&master_outcome);
        let done_flag = Arc::clone(&master_done);
        let done_notify = Arc::clone(&master_done_notify);
        sim.host(MASTER_HOST, move || {
            let plan_slot = Arc::clone(&plan_slot);
            let backend_slot = Arc::clone(&backend_slot);
            let outcome_slot = Arc::clone(&outcome_slot);
            let done_flag = Arc::clone(&done_flag);
            let done_notify = Arc::clone(&done_notify);
            async move {
                let Some(plan) = plan_slot.lock().unwrap().take() else {
                    return Ok(());
                };
                let Some(mut backend) = backend_slot.lock().unwrap().take() else {
                    return Ok(());
                };
                let outcome = run_master_session(plan, &mut backend).await;
                *outcome_slot.lock().unwrap() = Some(outcome);
                done_flag.store(true, Ordering::Release);
                done_notify.notify_waiters();
                Ok(())
            }
        });
    }
    let observer_done_flag = Arc::clone(&master_done);
    let observer_done_notify = Arc::clone(&master_done_notify);
    let observer_active_daemons = Arc::clone(&active_daemons);
    let observer_exited_notify = Arc::clone(&exited_notify);
    let observer_active_tunnels = Arc::clone(&active_tunnels);
    let observer_tunnels_notify = Arc::clone(&tunnels_closed_notify);
    sim.client("observer", async move {
        // wait until master finishes (or until outer loop crashes master and marks done via
        // active_daemons draining to zero after crash)
        while !observer_done_flag.load(Ordering::Acquire) {
            observer_done_notify.notified().await;
        }
        while observer_active_daemons.load(Ordering::SeqCst) > 0 {
            observer_exited_notify.notified().await;
        }
        while observer_active_tunnels.load(Ordering::SeqCst) > 0 {
            observer_tunnels_notify.notified().await;
        }
        Ok(())
    });
    let notices = NoticeCollector::default();
    let subscriber = NoticeSubscriber::new(notices.clone());
    tracing::subscriber::with_default(subscriber, || {
        loop {
            let elapsed = sim.elapsed();
            on_step(&mut sim, &edges, elapsed);
            // if a step crashed `"master"`, mark master_done so the observer waits exclusively
            // for all surviving daemons and tunnels to clean up and exit
            if !sim.is_host_running(MASTER_HOST) && !master_done.load(Ordering::Acquire) {
                master_done.store(true, Ordering::Release);
                master_done_notify.notify_waiters();
            }
            match sim.step() {
                Ok(true) => break,
                Ok(false) => {}
                Err(err) => {
                    panic!(
                        "turmoil simulation failed ({err:?}) at {:?} (seed={:?}): master_done={}, active_daemons={}, active_tunnels={}, recorded_outcomes={}",
                        sim.elapsed(),
                        cfg.seed,
                        master_done.load(Ordering::Acquire),
                        active_daemons.load(Ordering::SeqCst),
                        active_tunnels.load(Ordering::SeqCst),
                        recorded_outcomes.lock().unwrap().len(),
                    );
                }
            }
        }
    });
    let elapsed = sim.elapsed();
    ThreeHostRun {
        master_outcome: master_outcome.lock().unwrap().take(),
        observations,
        daemon_outcomes: std::mem::take(&mut *recorded_outcomes.lock().unwrap()),
        notices: notices.snapshot(),
        elapsed,
        active_daemons: active_daemons.load(Ordering::SeqCst),
        active_tunnels: active_tunnels.load(Ordering::SeqCst),
    }
}

#[test]
fn three_host_mtls_happy_path_and_dry_run_summary_merging() {
    // 1. normal 3-host mTLS copy with an exclude filter: destination owns copy counts, source owns
    // skip counts, both runtime stats hostnames are populated, and trees match byte-for-byte
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    populate_sample_tree(src_dir.path(), 6, 12 * 1024);
    std::fs::write(src_dir.path().join("alpha/skip_me.tmp"), b"excluded data").unwrap();
    let filter =
        common::filter::FilterSettings::from_args(None, &[], &["*.tmp".to_owned()]).unwrap();
    let mut plan = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
    plan.filter = filter;
    let run = run_three_host_sim(ThreeHostConfig::new(plan.build()), |_, _, _| {});
    assert_eq!(run.active_daemons, 0);
    assert_eq!(run.active_tunnels, 0);
    let outcome = run.master_outcome.expect("master must produce outcome");
    let stats = outcome
        .runtime_stats
        .expect("runtime stats must be populated");
    assert_eq!(stats.source_host, SOURCE_HOST);
    assert_eq!(stats.dest_host, DEST_HOST);
    let summary = outcome.result.expect("3-host mTLS copy must succeed");
    assert_eq!(summary.files_copied, 7); // 6 files + zero.bin
    assert_eq!(summary.files_skipped, 1); // skip_me.tmp
    assert_eq!(summary.symlinks_created, 1);
    let mut expected = snapshot_tree(src_dir.path());
    expected.remove(Path::new("alpha/skip_me.tmp"));
    assert_eq!(snapshot_tree(&dst_dir.path().join("out")), expected);
    // 2. dry-run over 3-host mTLS: source owns copy counts, destination writes nothing on disk
    let dry_dst = tempfile::tempdir().unwrap();
    let mut dry_plan = PlanBuilder::new(src_dir.path().to_path_buf(), dry_dst.path().join("out"));
    dry_plan.dry_run = Some(common::config::DryRunMode::Brief);
    dry_plan.filter =
        common::filter::FilterSettings::from_args(None, &[], &["*.tmp".to_owned()]).unwrap();
    let dry_run = run_three_host_sim(ThreeHostConfig::new(dry_plan.build()), |_, _, _| {});
    assert_eq!(dry_run.active_daemons, 0);
    assert_eq!(dry_run.active_tunnels, 0);
    let dry_summary = dry_run
        .master_outcome
        .unwrap()
        .result
        .expect("3-host dry-run must succeed");
    assert_eq!(dry_summary.files_copied, 7);
    assert_eq!(dry_summary.files_skipped, 1);
    assert!(
        !dry_dst.path().join("out").exists(),
        "dry-run must not create destination files"
    );
}

#[test]
fn three_host_bootstrap_readiness_faults_and_source_unwinding() {
    let src_dir = tempfile::tempdir().unwrap();
    populate_sample_tree(src_dir.path(), 2, 1024);
    let expected = snapshot_tree(src_dir.path());
    // (a) RCP_ERROR refusal on destination unwinds already-running source and drains source notice
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut cfg = ThreeHostConfig::new(
            PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out")).build(),
        );
        cfg.source_faults.startup_notices = vec!["source-boot-notice-alpha".to_owned()];
        cfg.dest_faults.bootstrap = BootstrapFault::Refusal("lockfile busy".to_owned());
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let err = run.master_outcome.unwrap().result.unwrap_err();
        let text = format!("{err:#}");
        assert!(
            text.contains("rcpd refused startup"),
            "unexpected error: {text}"
        );
        assert!(text.contains("lockfile busy"), "unexpected error: {text}");
        assert!(
            run.notices
                .iter()
                .any(|n| n.contains("source-boot-notice-alpha")),
            "source startup notice must drain on destination refusal; got {:?}",
            run.notices
        );
    }
    // (b) unexpected garbage stderr line on destination
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut cfg = ThreeHostConfig::new(
            PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out")).build(),
        );
        cfg.dest_faults.bootstrap = BootstrapFault::RawStderr(b"Welcome to Ubuntu MOTD\n".to_vec());
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let err = format!("{:#}", run.master_outcome.unwrap().result.unwrap_err());
        assert!(
            err.contains("unexpected output from rcpd"),
            "unexpected garbage-stderr error: {err}"
        );
    }
    // (c) stalled destination readiness hits virtual-time bootstrap_timeout and unwinds source
    // (heartbeats keep source alive past keepalive_sec=2 until master closes source stdin at 3s)
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.conn_timeout_sec = 3;
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.dest_faults.bootstrap = BootstrapFault::StallReadiness;
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let err = format!("{:#}", run.master_outcome.unwrap().result.unwrap_err());
        assert!(err.contains("timed out"), "unexpected timeout error: {err}");
        assert!(
            run.elapsed >= Duration::from_secs(3) && run.elapsed < Duration::from_secs(10),
            "stalled readiness should time out near 3s virtual time, elapsed={:?}",
            run.elapsed
        );
        let src_res = run
            .daemon_outcomes
            .iter()
            .find(|(role, _)| *role == RcpdRole::Source)
            .expect("source daemon outcome must be recorded");
        assert!(
            matches!(
                &src_res.1,
                RcpdResult::Failure { error, .. } if error.contains("closed its stdin control channel")
            ),
            "heartbeats must keep source alive until master closes stdin at 3s, got {:?}",
            src_res.1
        );
    }
    // (d) mismatched F/E limits in destination RCP_READY unwinds both and drains dest notice too
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut cfg = ThreeHostConfig::new(
            PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out")).build(),
        );
        cfg.dest_faults.startup_notices = vec!["dest-boot-notice-mismatch".to_owned()];
        cfg.dest_faults.bootstrap = BootstrapFault::TamperedReadiness {
            fingerprint: Some([0x33; 32]),
            files_in_flight: common::ConcurrencyLimit::Limited(NonZeroUsize::new(99).unwrap()),
            max_connections: NonZeroUsize::new(4).unwrap(),
        };
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let err = format!("{:#}", run.master_outcome.unwrap().result.unwrap_err());
        assert!(
            err.contains("destination rcpd reported file/stream limits"),
            "unexpected limit-mismatch error: {err}"
        );
        assert!(
            run.notices
                .iter()
                .any(|n| n.contains("dest-boot-notice-mismatch")),
            "destination startup notice must drain even when readiness validation rejects it; got {:?}",
            run.notices
        );
    }
    // (e) missing TLS fingerprint on encrypted destination readiness
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut cfg = ThreeHostConfig::new(
            PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out")).build(),
        );
        cfg.dest_faults.bootstrap = BootstrapFault::TamperedReadiness {
            fingerprint: None,
            files_in_flight: common::ConcurrencyLimit::Limited(NonZeroUsize::new(4).unwrap()),
            max_connections: NonZeroUsize::new(4).unwrap(),
        };
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let err = format!("{:#}", run.master_outcome.unwrap().result.unwrap_err());
        assert!(
            err.contains("TLS configuration mismatch"),
            "unexpected encryption-mismatch error: {err}"
        );
    }
    // (f) oversized (>64 KiB) startup readiness record rejected by read_rcpd_startup_record
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut cfg = ThreeHostConfig::new(
            PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out")).build(),
        );
        cfg.dest_faults.bootstrap = BootstrapFault::RawStderr(vec![b'X'; 70 * 1024]);
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let err = format!("{:#}", run.master_outcome.unwrap().result.unwrap_err());
        assert!(
            err.contains("exceeds the 65536-byte limit"),
            "unexpected oversized-readiness error: {err}"
        );
    }
    // (g) held/blackholed master<->destination during bootstrap times out at bootstrap_timeout and
    // unwinds already-running source while draining its startup notice
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.conn_timeout_sec = 3;
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.source_faults.startup_notices = vec!["source-notice-dest-held".to_owned()];
        let mut held = false;
        let run = run_three_host_sim(cfg, |sim, edges, _| {
            if !held {
                held = true;
                edges.master_dst.set_impaired(true);
                sim.hold(MASTER_HOST, DEST_HOST);
            }
        });
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let err = format!("{:#}", run.master_outcome.unwrap().result.unwrap_err());
        assert!(
            err.contains("timed out"),
            "unexpected hold-timeout error: {err}"
        );
        assert!(
            run.elapsed >= Duration::from_secs(3) && run.elapsed < Duration::from_secs(10),
            "blackholed destination bootstrap should time out near 3s, elapsed={:?}",
            run.elapsed
        );
        let src_res = run
            .daemon_outcomes
            .iter()
            .find(|(role, _)| *role == RcpdRole::Source)
            .expect("source daemon outcome must be recorded");
        assert!(
            matches!(
                &src_res.1,
                RcpdResult::Failure { error, .. } if error.contains("closed its stdin control channel")
            ),
            "source must unwind on master stdin EOF after destination bootstrap timeout, got {:?}",
            src_res.1
        );
        assert!(
            run.notices
                .iter()
                .any(|n| n.contains("source-notice-dest-held")),
            "source startup notice must drain when destination is unreachable; got {:?}",
            run.notices
        );
    }
    // (h1) automatic concurrency negotiation: source chooses file ceiling, master propagates
    // ResolvedAutomatic to destination and emits clamp notice when explicit --max-connections caps it
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.automatic_concurrency = true;
        pb.max_connections = 2;
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.source_faults.auto_files_in_flight = NonZeroUsize::new(6).unwrap();
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let summary = run
            .master_outcome
            .unwrap()
            .result
            .expect("automatic concurrency negotiation must succeed");
        assert_eq!(summary.files_copied, 3);
        assert_eq!(snapshot_tree(&dst_dir.path().join("out")), expected);
        let dest_cfg = run
            .observations
            .spawned_configs
            .lock()
            .unwrap()
            .iter()
            .find(|(role, _, _)| *role == RcpdRole::Destination)
            .unwrap()
            .1
            .clone();
        assert_eq!(
            dest_cfg.files_in_flight,
            RcpdFilesInFlight::ResolvedAutomatic(NonZeroUsize::new(6).unwrap())
        );
        assert_eq!(dest_cfg.max_connections, 2);
        assert!(
            run.notices
                .iter()
                .any(|n| n.contains("automatic file ceiling of 6 was reduced to 2")),
            "expected stream clamp notice, got {:?}",
            run.notices
        );
    }
    // (h2) automatic source reporting unlimited file ceiling is rejected and unwound cleanly
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.automatic_concurrency = true;
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.source_faults.bootstrap = BootstrapFault::TamperedReadiness {
            fingerprint: Some([0x55; 32]),
            files_in_flight: common::ConcurrencyLimit::Unlimited,
            max_connections: NonZeroUsize::new(4).unwrap(),
        };
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let err = format!("{:#}", run.master_outcome.unwrap().result.unwrap_err());
        assert!(
            err.contains("automatic source rcpd reported an unlimited file ceiling"),
            "unexpected automatic-unlimited error: {err}"
        );
    }
}

#[test]
fn three_host_same_endpoint_peer_identity_check_resolution() {
    let src_dir = tempfile::tempdir().unwrap();
    populate_sample_tree(src_dir.path(), 3, 4096);
    let expected = snapshot_tree(src_dir.path());
    // (a) identical PeerIdentity -> SameHost shortcut + disables destination agent forwarding
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.initial_topology = InitialTopology::SameEndpointNeedsPeerCheck;
        pb.transport = RemoteTransport::Auto;
        let mut cfg = ThreeHostConfig::new(pb.build());
        let shared_peer = default_peer_identity("shared-node");
        cfg.source_peer = Ok((shared_peer.clone(), None));
        cfg.dest_peer = Ok((shared_peer, None));
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let summary = run
            .master_outcome
            .unwrap()
            .result
            .expect("SameHost copy must succeed");
        assert_eq!(summary.files_copied, 4);
        assert_eq!(snapshot_tree(&dst_dir.path().join("out")), expected);
        assert!(
            run.observations
                .dest_agent_forward_disabled
                .load(Ordering::SeqCst),
            "matching peers must disable destination agent forwarding"
        );
        assert_eq!(
            run.observations.source_probe_calls.load(Ordering::SeqCst),
            1
        );
        assert_eq!(run.observations.dest_probe_calls.load(Ordering::SeqCst), 1);
        // SameHost sets ssh_tunnels = 0 on destination config
        let dest_cfg = run
            .observations
            .spawned_configs
            .lock()
            .unwrap()
            .iter()
            .find(|(role, _, _)| *role == RcpdRole::Destination)
            .unwrap()
            .1
            .clone();
        assert_eq!(dest_cfg.ssh_tunnels, 0);
    }
    // (b) distinct PeerIdentity -> stays ThreeHost (does NOT disable agent forward; sets ssh_tunnels)
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.initial_topology = InitialTopology::SameEndpointNeedsPeerCheck;
        pb.transport = RemoteTransport::Auto;
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.source_peer = Ok((default_peer_identity("backend-a"), None));
        cfg.dest_peer = Ok((default_peer_identity("backend-b"), None));
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let summary = run
            .master_outcome
            .unwrap()
            .result
            .expect("ThreeHost Auto copy over healthy direct link must succeed");
        assert_eq!(summary.files_copied, 4);
        assert_eq!(snapshot_tree(&dst_dir.path().join("out")), expected);
        assert_eq!(
            run.observations.source_probe_calls.load(Ordering::SeqCst),
            1
        );
        assert_eq!(run.observations.dest_probe_calls.load(Ordering::SeqCst), 1);
        assert!(
            !run.observations
                .dest_agent_forward_disabled
                .load(Ordering::SeqCst),
            "mismatched peers must keep destination agent forwarding enabled"
        );
        let dest_cfg = run
            .observations
            .spawned_configs
            .lock()
            .unwrap()
            .iter()
            .find(|(role, _, _)| *role == RcpdRole::Destination)
            .unwrap()
            .1
            .clone();
        assert!(
            dest_cfg.ssh_tunnels > 0,
            "ThreeHost Auto must reserve ssh_tunnels on destination"
        );
    }
    // (c) probe error -> fails safe to ThreeHost without disabling agent forward
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.initial_topology = InitialTopology::SameEndpointNeedsPeerCheck;
        pb.transport = RemoteTransport::Auto;
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.dest_peer = Err("simulated probe timeout".to_owned());
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let summary = run
            .master_outcome
            .unwrap()
            .result
            .expect("probe failure must fall back safely to ThreeHost");
        assert_eq!(summary.files_copied, 4);
        assert_eq!(snapshot_tree(&dst_dir.path().join("out")), expected);
        assert_eq!(
            run.observations.source_probe_calls.load(Ordering::SeqCst),
            1
        );
        assert_eq!(run.observations.dest_probe_calls.load(Ordering::SeqCst), 1);
        assert!(
            !run.observations
                .dest_agent_forward_disabled
                .load(Ordering::SeqCst)
        );
        let dest_cfg = run
            .observations
            .spawned_configs
            .lock()
            .unwrap()
            .iter()
            .find(|(role, _, _)| *role == RcpdRole::Destination)
            .unwrap()
            .1
            .clone();
        assert!(
            dest_cfg.ssh_tunnels > 0,
            "probe failure must fall back to ThreeHost (ssh_tunnels > 0)"
        );
    }
}

#[test]
fn three_host_source_fails_before_source_hello_unwinds_destination() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    populate_sample_tree(src_dir.path(), 2, 1024);
    let mut cfg = ThreeHostConfig::new(
        PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out")).build(),
    );
    // force source's daemon_session::validate_preview_mode to fail BEFORE SourceMasterHello
    cfg.source_faults.override_preview_only = Some(true);
    let run = run_three_host_sim(cfg, |_, _, _| {});
    assert_eq!(run.active_daemons, 0);
    assert_eq!(run.active_tunnels, 0);
    let err = run.master_outcome.unwrap().result.unwrap_err();
    assert!(
        err.downcast_ref::<common::copy::Error>().is_some(),
        "pre-hello source failure must preserve common::copy::Error: {err:#}"
    );
    let text = format!("{err:#}");
    assert!(
        text.contains("Source rcpd failed") && text.contains("preview"),
        "unexpected pre-hello failure message: {text}"
    );
    // destination must have exited cleanly on master stdin EOF before ever receiving MasterHello
    let dest_outcome = run
        .daemon_outcomes
        .iter()
        .find(|(role, _)| *role == RcpdRole::Destination)
        .expect("destination daemon outcome must be recorded");
    assert!(
        matches!(
            &dest_outcome.1,
            RcpdResult::Failure { error, peer_connected: false, .. }
                if error.contains("closed its stdin control channel")
        ),
        "destination should unwind on pre-hello stdin EOF, got {:?}",
        dest_outcome.1
    );
}

#[test]
fn three_host_auto_fallback_proxies_through_master_across_partitioned_data_plane() {
    let src_dir = tempfile::tempdir().unwrap();
    populate_sample_tree(src_dir.path(), 6, 16 * 1024);
    let expected = snapshot_tree(src_dir.path());
    // (a) Auto + Download (-L): direct source<->destination partitioned -> falls back through
    // master relay with 1-RTT SSH-HMAC (AutoTlsOrSshSecret)
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.initial_topology = InitialTopology::Download;
        pb.transport = RemoteTransport::Auto;
        pb.probe_timeout_sec = 2;
        pb.conn_timeout_sec = 10;
        let mut partitioned = false;
        let run = run_three_host_sim(ThreeHostConfig::new(pb.build()), |sim, edges, _| {
            if !partitioned {
                partitioned = true;
                edges.src_dst.set_partitioned(true);
                sim.partition(SOURCE_HOST, DEST_HOST);
            }
        });
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let summary = run
            .master_outcome
            .unwrap()
            .result
            .expect("Auto fallback through master proxy must succeed");
        assert_eq!(summary.files_copied, 7);
        assert_eq!(snapshot_tree(&dst_dir.path().join("out")), expected);
        let tunnel_reqs = run.observations.tunnel_requests.lock().unwrap();
        assert_eq!(
            tunnel_reqs.len(),
            1,
            "expected exactly one fallback tunnel set"
        );
        assert_eq!(
            tunnel_reqs[0].direction,
            remote::ssh_tunnel::ForwardDirection::Local
        );
        assert!(
            run.notices
                .iter()
                .any(|n| n.contains("falling back to SSH")),
            "expected SSH fallback notice, got {:?}",
            run.notices
        );
    }
    // (b) Auto + Upload (-R): probes destination peer identity before opening -R fallback tunnels
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.initial_topology = InitialTopology::Upload;
        pb.transport = RemoteTransport::Auto;
        pb.probe_timeout_sec = 2;
        pb.conn_timeout_sec = 10;
        let mut partitioned = false;
        let run = run_three_host_sim(ThreeHostConfig::new(pb.build()), |sim, edges, _| {
            if !partitioned {
                partitioned = true;
                edges.src_dst.set_partitioned(true);
                sim.partition(SOURCE_HOST, DEST_HOST);
            }
        });
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        run.master_outcome
            .unwrap()
            .result
            .expect("Upload Auto fallback must succeed");
        assert_eq!(snapshot_tree(&dst_dir.path().join("out")), expected);
        assert_eq!(
            run.observations.source_probe_calls.load(Ordering::SeqCst),
            0,
            "Upload (-R) has a local source and must not probe source over SSH"
        );
        assert_eq!(
            run.observations.dest_probe_calls.load(Ordering::SeqCst),
            1,
            "Upload (-R) must probe destination peer identity before opening tunnels"
        );
        let tunnel_reqs = run.observations.tunnel_requests.lock().unwrap();
        assert_eq!(tunnel_reqs.len(), 1);
        assert_eq!(
            tunnel_reqs[0].direction,
            remote::ssh_tunnel::ForwardDirection::Remote
        );
        assert!(
            tunnel_reqs[0].expected_peer.is_some(),
            "Upload (-R) must thread probed destination PeerIdentity into TunnelRequest"
        );
        assert!(
            run.notices
                .iter()
                .any(|n| n.contains("falling back to SSH")),
            "expected SSH fallback notice on Upload, got {:?}",
            run.notices
        );
    }
    // (c) Explicit Ssh + Download: boots without TLS certs, uses MasterSshTunnel + SshSharedSecret
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.initial_topology = InitialTopology::Download;
        pb.transport = RemoteTransport::Ssh;
        pb.conn_timeout_sec = 10;
        let mut partitioned = false;
        let run = run_three_host_sim(ThreeHostConfig::new(pb.build()), |sim, edges, _| {
            if !partitioned {
                partitioned = true;
                edges.src_dst.set_partitioned(true);
                sim.partition(SOURCE_HOST, DEST_HOST);
            }
        });
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        run.master_outcome
            .unwrap()
            .result
            .expect("Explicit SSH transport through master proxy must succeed");
        assert_eq!(snapshot_tree(&dst_dir.path().join("out")), expected);
        assert_eq!(run.observations.tunnel_requests.lock().unwrap().len(), 1);
        assert!(
            run.notices
                .iter()
                .any(|n| n.contains("Transferring file data over SSH")),
            "expected upfront SSH transfer notice, got {:?}",
            run.notices
        );
        let spawned = run.observations.spawned_configs.lock().unwrap();
        for (role, cfg, bind_ip) in spawned.iter() {
            assert!(!cfg.encryption, "explicit SSH mode must disable TLS certs");
            if *role == RcpdRole::Source {
                assert_eq!(
                    bind_ip.as_deref(),
                    Some("127.0.0.1"),
                    "explicit SSH source must bind loopback"
                );
            }
        }
    }
    // (d) Auto + Download over healthy direct link completes via direct mTLS without fallback
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.initial_topology = InitialTopology::Download;
        pb.transport = RemoteTransport::Auto;
        pb.probe_timeout_sec = 2;
        let run = run_three_host_sim(ThreeHostConfig::new(pb.build()), |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        run.master_outcome
            .unwrap()
            .result
            .expect("Auto over healthy direct link must succeed directly");
        assert_eq!(snapshot_tree(&dst_dir.path().join("out")), expected);
        assert!(
            run.observations.tunnel_requests.lock().unwrap().is_empty(),
            "healthy Auto copy must not open fallback tunnels"
        );
        assert!(
            !run.notices
                .iter()
                .any(|n| n.contains("falling back to SSH")),
            "healthy Auto copy must not emit fallback notice"
        );
    }
}

#[test]
fn three_host_source_fails_during_master_fallback_setup() {
    let src_dir = tempfile::tempdir().unwrap();
    populate_sample_tree(src_dir.path(), 2, 2048);
    // (a) source times out waiting for destination while master fallback open_tunnels is stalled
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.initial_topology = InitialTopology::Download;
        pb.transport = RemoteTransport::Auto;
        // source accept timeout = probe (2s) + 2 * conn_timeout (2 * 3s) = 8s
        pb.probe_timeout_sec = 2;
        pb.conn_timeout_sec = 3;
        let fallback_started = Arc::new(AtomicBool::new(false));
        let fallback_dropped = Arc::new(AtomicBool::new(false));
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.tunnel_behavior = TunnelBehavior::StallUntilCancelled {
            started: Arc::clone(&fallback_started),
            dropped: Arc::clone(&fallback_dropped),
        };
        let mut partitioned = false;
        let run = run_three_host_sim(cfg, |sim, edges, _| {
            if !partitioned {
                partitioned = true;
                edges.src_dst.set_partitioned(true);
                sim.partition(SOURCE_HOST, DEST_HOST);
            }
        });
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        assert!(
            fallback_started.load(Ordering::Acquire),
            "fallback open_tunnels must have been entered"
        );
        assert!(
            fallback_dropped.load(Ordering::SeqCst),
            "in-flight fallback open_tunnels future must be cancelled when source fails"
        );
        let err = run.master_outcome.unwrap().result.unwrap_err();
        let text = format!("{err:#}");
        assert!(
            text.contains("Source:") && text.contains("Timed out"),
            "expected source accept timeout failure during stalled fallback, got: {text}"
        );
        assert!(
            run.elapsed >= Duration::from_secs(8) && run.elapsed < Duration::from_secs(20),
            "expected cancellation near source's 8s accept timeout, elapsed={:?}",
            run.elapsed
        );
    }
    // (b) destination disappears silently (partition + crash) while master fallback open_tunnels
    // is stalled -> ServerAlive timeout triggers FallbackSelect::DestEvent, cancels open_tunnels,
    // applies PEER_FAILURE_RESULT_GRACE to source, and closes source stdin
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.initial_topology = InitialTopology::Download;
        pb.transport = RemoteTransport::Auto;
        pb.probe_timeout_sec = 2;
        // long source accept timeout (2 + 2*15 = 32s) proves FallbackSelect::DestEvent won
        pb.conn_timeout_sec = 15;
        pb.keepalive_sec = 2;
        let fallback_started = Arc::new(AtomicBool::new(false));
        let fallback_dropped = Arc::new(AtomicBool::new(false));
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.tunnel_behavior = TunnelBehavior::StallUntilCancelled {
            started: Arc::clone(&fallback_started),
            dropped: Arc::clone(&fallback_dropped),
        };
        let mut partitioned = false;
        let mut dest_crashed = false;
        let run = run_three_host_sim(cfg, |sim, edges, _| {
            if !partitioned {
                partitioned = true;
                edges.src_dst.set_partitioned(true);
                sim.partition(SOURCE_HOST, DEST_HOST);
            }
            if !dest_crashed && fallback_started.load(Ordering::Acquire) {
                dest_crashed = true;
                // partition BEFORE crash so Turmoil's drop-induced FIN is dropped and master
                // genuinely detects silent loss through its 2s ServerAlive timeout
                edges.master_dst.set_partitioned(true);
                sim.partition(MASTER_HOST, DEST_HOST);
                sim.crash(DEST_HOST);
            }
        });
        assert!(
            dest_crashed,
            "destination crash must fire during stalled fallback"
        );
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        assert!(
            fallback_dropped.load(Ordering::SeqCst),
            "in-flight fallback open_tunnels future must be cancelled when destination crashes"
        );
        let err = format!("{:#}", run.master_outcome.unwrap().result.unwrap_err());
        assert!(
            err.contains("destination rcpd") && err.contains("keepalive timeout"),
            "expected ServerAlive timeout diagnostic for destination, got: {err}"
        );
        assert!(
            run.elapsed >= Duration::from_secs(4) && run.elapsed < Duration::from_secs(12),
            "expected probe (~2s) + ServerAlive (~2s) + grace (~2s) well before 32s accept timeout, elapsed={:?}",
            run.elapsed
        );
    }
}

#[test]
fn three_host_master_crash_and_control_link_partition_trigger_daemon_watchdogs() {
    let src_dir = tempfile::tempdir().unwrap();
    populate_sample_tree(src_dir.path(), 8, 32 * 1024);
    // (a) partition both master<->source and master<->destination mid-transfer (holding data plane
    // briefly so the transfer does not finish before the 1*keepalive = 2s watchdog fires)
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let data_flowing = Arc::new(AtomicBool::new(false));
        let data_flowing_hook = Arc::clone(&data_flowing);
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.keepalive_sec = 2;
        pb.conn_timeout_sec = 20;
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.source_faults.on_data_write_bytes = Some(Arc::new(move |_| {
            data_flowing_hook.store(true, Ordering::Release);
        }));
        let mut injected_at: Option<Duration> = None;
        let run = run_three_host_sim(cfg, |sim, edges, elapsed| {
            if injected_at.is_none() && data_flowing.load(Ordering::Acquire) {
                injected_at = Some(elapsed);
                edges.master_src.set_partitioned(true);
                edges.master_dst.set_partitioned(true);
                sim.hold(SOURCE_HOST, DEST_HOST);
                sim.partition(MASTER_HOST, SOURCE_HOST);
                sim.partition(MASTER_HOST, DEST_HOST);
            }
        });
        assert!(
            injected_at.is_some(),
            "fault must fire after real data bytes started flowing"
        );
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        assert!(
            run.master_outcome.unwrap().result.is_err(),
            "master must fail when both control links partition"
        );
        // verify both daemons tripped daemon_session's KeepaliveTimedOut (1 * keepalive_sec = 2s)
        for role in [RcpdRole::Source, RcpdRole::Destination] {
            let outcome = run
                .daemon_outcomes
                .iter()
                .find(|(r, _)| *r == role)
                .unwrap_or_else(|| panic!("missing daemon outcome for {role:?}"));
            assert!(
                matches!(
                    &outcome.1,
                    RcpdResult::Failure { error, .. }
                        if error.contains("master control channel lost")
                            && error.contains("after 2s without a heartbeat")
                ),
                "{role:?} should trip 2s heartbeat watchdog, got {:?}",
                outcome.1
            );
        }
        let since_inject = run.elapsed.saturating_sub(injected_at.unwrap());
        // last frame (MasterHello or Heartbeat) arrived up to keepalive_sec/3 (~666ms) before
        // injection, so a 2s watchdog fires between ~1.3s and ~3s after injection
        assert!(
            since_inject >= Duration::from_millis(1200) && since_inject <= Duration::from_secs(5),
            "expected ~2s virtual watchdog timeout after partition, delta={since_inject:?}"
        );
    }
    // (b1) process-kill sim.crash("master") mid-transfer (FIN delivered immediately): both daemons
    // observe master stdin EOF and exit promptly with zero orphans
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let data_flowing = Arc::new(AtomicBool::new(false));
        let data_flowing_hook = Arc::clone(&data_flowing);
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.keepalive_sec = 2;
        pb.conn_timeout_sec = 20;
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.source_faults.on_data_write_bytes = Some(Arc::new(move |_| {
            data_flowing_hook.store(true, Ordering::Release);
        }));
        let mut crashed_at: Option<Duration> = None;
        let run = run_three_host_sim(cfg, |sim, _, elapsed| {
            if crashed_at.is_none() && data_flowing.load(Ordering::Acquire) {
                crashed_at = Some(elapsed);
                sim.hold(SOURCE_HOST, DEST_HOST);
                sim.crash(MASTER_HOST);
            }
        });
        assert!(crashed_at.is_some(), "master crash must fire mid-transfer");
        assert!(
            run.master_outcome.is_none(),
            "crashed master must not produce a normal outcome"
        );
        assert_eq!(
            run.active_daemons, 0,
            "no orphan daemons may survive master crash"
        );
        assert_eq!(run.active_tunnels, 0);
        assert_eq!(
            run.daemon_outcomes.len(),
            2,
            "both daemons must finish cleanly"
        );
        for role in [RcpdRole::Source, RcpdRole::Destination] {
            let outcome = run
                .daemon_outcomes
                .iter()
                .find(|(r, _)| *r == role)
                .unwrap();
            assert!(
                matches!(
                    &outcome.1,
                    RcpdResult::Failure { error, .. }
                        if error.contains("master control channel lost")
                            && error.contains("closed its stdin control channel")
                ),
                "{role:?} should report stdin EOF on master process kill, got {:?}",
                outcome.1
            );
        }
        let since_crash = run.elapsed.saturating_sub(crashed_at.unwrap());
        assert!(
            since_crash < Duration::from_secs(2),
            "process-kill FIN should unwind both daemons before the 2s keepalive budget, delta={since_crash:?}"
        );
    }
    // (b2) silent master host power-off (partition both control edges before crash so no FIN is
    // delivered): both daemons trip KeepaliveTimedOut at ~2s with zero orphans
    {
        let dst_dir = tempfile::tempdir().unwrap();
        let data_flowing = Arc::new(AtomicBool::new(false));
        let data_flowing_hook = Arc::clone(&data_flowing);
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.keepalive_sec = 2;
        pb.conn_timeout_sec = 20;
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.source_faults.on_data_write_bytes = Some(Arc::new(move |_| {
            data_flowing_hook.store(true, Ordering::Release);
        }));
        let mut crashed_at: Option<Duration> = None;
        let run = run_three_host_sim(cfg, |sim, edges, elapsed| {
            if crashed_at.is_none() && data_flowing.load(Ordering::Acquire) {
                crashed_at = Some(elapsed);
                edges.master_src.set_partitioned(true);
                edges.master_dst.set_partitioned(true);
                sim.hold(SOURCE_HOST, DEST_HOST);
                sim.partition(MASTER_HOST, SOURCE_HOST);
                sim.partition(MASTER_HOST, DEST_HOST);
                sim.crash(MASTER_HOST);
            }
        });
        assert!(
            crashed_at.is_some(),
            "silent master power-off must fire mid-transfer"
        );
        assert!(run.master_outcome.is_none());
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        assert_eq!(run.daemon_outcomes.len(), 2);
        for role in [RcpdRole::Source, RcpdRole::Destination] {
            let outcome = run
                .daemon_outcomes
                .iter()
                .find(|(r, _)| *r == role)
                .unwrap();
            assert!(
                matches!(
                    &outcome.1,
                    RcpdResult::Failure { error, .. }
                        if error.contains("master control channel lost")
                            && error.contains("after 2s without a heartbeat")
                ),
                "{role:?} should trip 2s heartbeat watchdog on silent master loss, got {:?}",
                outcome.1
            );
        }
        let since_crash = run.elapsed.saturating_sub(crashed_at.unwrap());
        assert!(
            since_crash >= Duration::from_millis(1200) && since_crash <= Duration::from_secs(5),
            "expected ~2s heartbeat timeout on silent master power-off, delta={since_crash:?}"
        );
    }
}

#[test]
fn three_host_pre_connect_and_post_connect_failures_and_teardown_precedence() {
    // (a) destination pre-connect failure (peer_connected: false) cancels source within
    // PEER_FAILURE_RESULT_GRACE (~2s) instead of stalling on source's 30s accept timeout
    {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        populate_sample_tree(src_dir.path(), 2, 1024);
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.conn_timeout_sec = 30;
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.dest_faults.inject_connect_refused_once = Some(Arc::new(AtomicBool::new(true)));
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let err = format!("{:#}", run.master_outcome.unwrap().result.unwrap_err());
        assert!(
            err.contains("injected pre-connect refusal") && err.contains("Destination:"),
            "expected destination pre-connect failure, got: {err}"
        );
        let dest_res = run
            .daemon_outcomes
            .iter()
            .find(|(r, _)| *r == RcpdRole::Destination)
            .expect("destination outcome must be recorded");
        assert!(
            matches!(
                dest_res.1,
                RcpdResult::Failure {
                    peer_connected: false,
                    ..
                }
            ),
            "destination failure before connect must report peer_connected=false, got {:?}",
            dest_res.1
        );
        let src_res = run
            .daemon_outcomes
            .iter()
            .find(|(r, _)| *r == RcpdRole::Source)
            .expect("source outcome must be recorded");
        assert!(
            matches!(
                &src_res.1,
                RcpdResult::Failure { error, peer_connected: false, .. }
                    if error.contains("closed its stdin control channel")
            ),
            "source must be cancelled via master stdin EOF after grace, got {:?}",
            src_res.1
        );
        assert!(
            run.elapsed >= Duration::from_secs(2) && run.elapsed < Duration::from_secs(8),
            "pre-connect failure must wait PEER_FAILURE_RESULT_GRACE (2s) rather than 30s accept timeout, elapsed={:?}",
            run.elapsed
        );
    }
    // (b) post-connect source error (dangling symlink with --dereference, works under root & non-root)
    // preserves destination's partial summary of valid copied files
    {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        std::fs::write(src_dir.path().join("ok_1.txt"), b"first").unwrap();
        std::fs::write(src_dir.path().join("ok_2.txt"), b"second").unwrap();
        std::fs::write(src_dir.path().join("ok_3.txt"), b"third").unwrap();
        std::os::unix::fs::symlink(
            "/nonexistent/dangling-target",
            src_dir.path().join("broken"),
        )
        .unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.dereference = true;
        pb.fail_early = false;
        let run = run_three_host_sim(ThreeHostConfig::new(pb.build()), |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let src_res = run
            .daemon_outcomes
            .iter()
            .find(|(r, _)| *r == RcpdRole::Source)
            .expect("source outcome must be recorded");
        assert!(
            matches!(
                src_res.1,
                RcpdResult::Failure {
                    peer_connected: true,
                    ..
                }
            ),
            "post-connect source error must report peer_connected=true, got {:?}",
            src_res.1
        );
        let err = run.master_outcome.unwrap().result.unwrap_err();
        assert!(
            format!("{err:#}").contains("Source:"),
            "expected Source failure in combined error, got: {err:#}"
        );
        let copy_err = err
            .downcast_ref::<common::copy::Error>()
            .expect("post-connect failure must preserve common::copy::Error");
        assert_eq!(
            copy_err.summary.files_copied, 3,
            "destination's copied file count must be preserved on source error"
        );
        assert_eq!(
            std::fs::read(dst_dir.path().join("out/ok_1.txt")).unwrap(),
            b"first"
        );
        assert_eq!(
            std::fs::read(dst_dir.path().join("out/ok_2.txt")).unwrap(),
            b"second"
        );
        assert_eq!(
            std::fs::read(dst_dir.path().join("out/ok_3.txt")).unwrap(),
            b"third"
        );
        assert!(
            !dst_dir.path().join("out/broken").exists(),
            "dangling dereferenced symlink must not create a destination file"
        );
    }
    // (c1) daemon nonzero exit after an otherwise Ok copy preserves full merged Summary
    {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        populate_sample_tree(src_dir.path(), 3, 2048);
        let mut cfg = ThreeHostConfig::new(
            PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out")).build(),
        );
        cfg.dest_faults.post_copy_exit_error = Some("timing flush ENOSPC".to_owned());
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let err = run.master_outcome.unwrap().result.unwrap_err();
        let copy_err = err
            .downcast_ref::<common::copy::Error>()
            .expect("teardown error after Ok copy must preserve Summary");
        assert_eq!(copy_err.summary.files_copied, 4);
        assert!(format!("{err:#}").contains("timing flush ENOSPC"));
    }
    // (c2) copy failure takes precedence over a simultaneous teardown exit failure
    {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        std::fs::write(src_dir.path().join("good.txt"), b"ok").unwrap();
        std::os::unix::fs::symlink(
            "/nonexistent/dangling-target",
            src_dir.path().join("broken"),
        )
        .unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        pb.dereference = true;
        pb.fail_early = false;
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.dest_faults.post_copy_exit_error = Some("secondary teardown failure".to_owned());
        let run = run_three_host_sim(cfg, |_, _, _| {});
        assert_eq!(run.active_daemons, 0);
        assert_eq!(run.active_tunnels, 0);
        let err = run.master_outcome.unwrap().result.unwrap_err();
        let text = format!("{err:#}");
        assert!(
            text.contains("Source:") && !text.contains("secondary teardown failure"),
            "copy failure must win over teardown failure, got: {text}"
        );
    }
}

#[test]
fn three_host_stdout_tracing_backpressure_delivers_result_and_drains_notices() {
    let src_dir = tempfile::tempdir().unwrap();
    let dst_dir = tempfile::tempdir().unwrap();
    populate_sample_tree(src_dir.path(), 4, 8 * 1024);
    let mut cfg = ThreeHostConfig::new(
        PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out")).build(),
    );
    cfg.min_latency = Duration::from_millis(5);
    cfg.max_latency = Duration::from_millis(15);
    cfg.source_faults.tracing_burst_count = 250;
    cfg.dest_faults.tracing_burst_count = 250;
    let run = run_three_host_sim(cfg, |_, _, _| {});
    assert_eq!(run.active_daemons, 0);
    assert_eq!(run.active_tunnels, 0);
    let summary = run
        .master_outcome
        .unwrap()
        .result
        .expect("copy under tracing backpressure must succeed");
    assert_eq!(summary.files_copied, 5);
    assert_eq!(
        snapshot_tree(&dst_dir.path().join("out")),
        snapshot_tree(src_dir.path())
    );
    let source_burst = run
        .notices
        .iter()
        .filter(|n| n.contains("burst-Source-"))
        .count();
    let dest_burst = run
        .notices
        .iter()
        .filter(|n| n.contains("burst-Destination-"))
        .count();
    assert_eq!(source_burst, 250, "all source tracing notices must drain");
    assert_eq!(
        dest_burst, 250,
        "all destination tracing notices must drain"
    );
}

#[test]
fn three_host_seeded_chaos_across_all_three_edges() {
    let src_dir = tempfile::tempdir().unwrap();
    populate_sample_tree(src_dir.path(), 6, 8 * 1024);
    let expected = snapshot_tree(src_dir.path());
    let mut successes = 0usize;
    let mut clean_failures = 0usize;
    for seed in 0u64..16 {
        let dst_dir = tempfile::tempdir().unwrap();
        let mut pb = PlanBuilder::new(src_dir.path().to_path_buf(), dst_dir.path().join("out"));
        match seed % 3 {
            0 => {
                pb.initial_topology = InitialTopology::ThreeHost;
                pb.transport = RemoteTransport::Tcp;
            }
            1 => {
                pb.initial_topology = InitialTopology::Download;
                pb.transport = RemoteTransport::Auto;
                pb.probe_timeout_sec = 2;
            }
            _ => {
                pb.initial_topology = InitialTopology::Download;
                pb.transport = RemoteTransport::Ssh;
            }
        }
        pb.conn_timeout_sec = 4;
        pb.keepalive_sec = 2;
        pb.max_connections = 3;
        let mut cfg = ThreeHostConfig::new(pb.build());
        cfg.seed = Some(1000 + seed);
        cfg.sim_duration = Duration::from_secs(90);
        cfg.min_latency = Duration::from_millis(2);
        cfg.max_latency = Duration::from_millis(25);
        // deterministic per-seed schedule injecting faults across all three edges
        let edge_selector = seed % 4;
        let inject_at = Duration::from_millis(15 + (seed % 5) * 40);
        let repair_at = inject_at + Duration::from_millis(80 + (seed % 4) * 150);
        let mut phase = 0u8;
        let run = run_three_host_sim(cfg, |sim, edges, elapsed| {
            if phase == 0 && elapsed >= inject_at {
                phase = 1;
                match edge_selector {
                    0 => {
                        // transient source<->destination partition (auto falls back; tcp recovers or fails cleanly)
                        edges.src_dst.set_partitioned(true);
                        sim.partition(SOURCE_HOST, DEST_HOST);
                    }
                    1 => {
                        // transient master<->source partition
                        edges.master_src.set_partitioned(true);
                        sim.partition(MASTER_HOST, SOURCE_HOST);
                    }
                    2 => {
                        // transient master<->destination partition
                        edges.master_dst.set_partitioned(true);
                        sim.partition(MASTER_HOST, DEST_HOST);
                    }
                    _ => {
                        // packet loss on data plane + brief hold on master<->source
                        edges.src_dst.set_lossy(true);
                        edges.master_src.set_impaired(true);
                        sim.set_link_fail_rate(SOURCE_HOST, DEST_HOST, 0.25);
                        sim.hold(MASTER_HOST, SOURCE_HOST);
                    }
                }
            } else if phase == 1 && elapsed >= repair_at {
                phase = 2;
                // seeds divisible by 5 keep the partition permanent so watchdogs/timeouts are also tested
                if seed % 5 != 0 {
                    match edge_selector {
                        0 => {
                            edges.src_dst.set_partitioned(false);
                            sim.repair(SOURCE_HOST, DEST_HOST);
                        }
                        1 => {
                            edges.master_src.set_partitioned(false);
                            sim.repair(MASTER_HOST, SOURCE_HOST);
                        }
                        2 => {
                            edges.master_dst.set_partitioned(false);
                            sim.repair(MASTER_HOST, DEST_HOST);
                        }
                        _ => {
                            edges.src_dst.set_lossy(false);
                            edges.master_src.set_impaired(false);
                            sim.set_link_fail_rate(SOURCE_HOST, DEST_HOST, 0.0);
                            sim.release(MASTER_HOST, SOURCE_HOST);
                        }
                    }
                }
            }
        });
        assert_eq!(run.active_daemons, 0, "seed {seed} leaked active daemon(s)");
        assert_eq!(run.active_tunnels, 0, "seed {seed} leaked active tunnel(s)");
        let outcome = run
            .master_outcome
            .unwrap_or_else(|| panic!("seed {seed}: master did not finish"));
        match outcome.result {
            Ok(summary) => {
                successes += 1;
                assert_eq!(
                    summary.files_copied, 7,
                    "seed {seed}: unexpected files_copied in summary"
                );
                assert_eq!(
                    snapshot_tree(&dst_dir.path().join("out")),
                    expected,
                    "seed {seed}: silent corruption — destination tree differs from source"
                );
            }
            Err(_) => {
                clean_failures += 1;
            }
        }
    }
    assert!(
        successes > 0 && clean_failures > 0,
        "chaos sweep should observe both recovered successes ({successes}) and clean failures ({clean_failures})"
    );
}
