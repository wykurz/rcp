//! Deterministic simulated-network tests exercising the real `source` and `destination` copy
//! pipelines (`run_source_with_transport` / `run_destination_with_transport`), mTLS 1.3 / SSH
//! loopback HMAC / `Auto` handshakes, connection pools, `directory_tracker`, and teardown logic
//! over `turmoil::net` without SSH or Docker.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use futures::future::BoxFuture;
use rcp_tools_rcp::destination::run_destination_with_transport;
use rcp_tools_rcp::source::run_source_with_transport;
use remote::protocol::{
    DestinationTransport, ExtendedMetadataCapture, PeerAuth, RcpdToMaster, SourceMasterHello,
    SshAuthSecret,
};
use remote::streams::{BoxedRead, BoxedWrite, RecvStream, SendStream};
use remote::tls::generate_self_signed_cert;
use remote::transport::{BoxedTransportStream, Transport, TransportListener, TransportStream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;
use turmoil::net::{TcpListener, TcpStream};

type EndpointResult = anyhow::Result<(String, common::copy::Summary)>;
type EndpointOutcome = Arc<Mutex<Option<EndpointResult>>>;
type HookFn = Arc<dyn Fn() + Send + Sync>;
type ByteHookFn = Arc<dyn Fn(usize) + Send + Sync>;
type FinishHookFn = Arc<dyn Fn(&EndpointResult) + Send + Sync>;

fn install_crypto_provider() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
}

/// Shared simulated link-health state used by [`TurmoilStream`] to model Linux `TCP_KEEPALIVE` and
/// `TCP_USER_TIMEOUT` faithfully.
///
/// On a healthy non-lossy link, kernel keepalive probes receive ACKs even when the application is
/// legitimately silent (for example, an idle control stream after `DiscoveryComplete` or a pooled
/// data stream between files), so [`TurmoilStream`] re-arms its timer instead of killing the
/// connection. When a test injects a partition, one-way blackhole, link hold, host crash, or
/// packet loss (`fail_rate > 0`), probes cannot complete and stalled reads/writes fail with
/// `ErrorKind::TimedOut`.
struct TurmoilLinkHealth {
    impaired: AtomicBool,
    lossy: bool,
}

impl TurmoilLinkHealth {
    fn healthy() -> Arc<Self> {
        Arc::new(Self {
            impaired: AtomicBool::new(false),
            lossy: false,
        })
    }

    fn lossy() -> Arc<Self> {
        Arc::new(Self {
            impaired: AtomicBool::new(false),
            lossy: true,
        })
    }

    fn set_impaired(&self, impaired: bool) {
        self.impaired.store(impaired, Ordering::Release);
    }

    fn can_ack_keepalive(&self) -> bool {
        !self.lossy && !self.impaired.load(Ordering::Acquire)
    }
}

/// Simulated TCP stream with optional loopback peer-IP override (for SSH tunnel modeling),
/// virtual-time `TCP_KEEPALIVE` / `TCP_USER_TIMEOUT` modeling gated on [`TurmoilLinkHealth`], and
/// an optional byte-write observer for triggering mid-transfer faults after real payload bytes
/// are flowing.
struct TurmoilStream {
    inner: TcpStream,
    peer_ip_override: Option<std::net::IpAddr>,
    read_idle_timeout: Option<Duration>,
    write_idle_timeout: Option<Duration>,
    read_sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    write_sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    link_health: Arc<TurmoilLinkHealth>,
    on_write_bytes: Option<ByteHookFn>,
}

impl TurmoilStream {
    fn new(
        inner: TcpStream,
        peer_ip_override: Option<std::net::IpAddr>,
        idle_timeout: Option<Duration>,
        link_health: Arc<TurmoilLinkHealth>,
        on_write_bytes: Option<ByteHookFn>,
    ) -> Self {
        Self {
            inner,
            peer_ip_override,
            read_idle_timeout: idle_timeout,
            write_idle_timeout: idle_timeout,
            read_sleep: None,
            write_sleep: None,
            link_health,
            on_write_bytes,
        }
    }

    fn poll_keepalive_timer(
        sleep_slot: &mut Option<Pin<Box<tokio::time::Sleep>>>,
        timeout: Option<Duration>,
        link_health: &TurmoilLinkHealth,
        cx: &mut Context<'_>,
        label: &str,
    ) -> Option<std::io::Error> {
        let timeout = timeout?;
        let sleep = sleep_slot.get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
        if sleep.as_mut().poll(cx).is_ready() {
            if link_health.can_ack_keepalive() {
                // healthy link ACKs the simulated kernel keepalive probe; re-arm and register waker
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
                    cx,
                    "TCP keepalive",
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
                    cx,
                    "TCP_USER_TIMEOUT",
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
        // use `tokio::io::split` rather than `turmoil::net::TcpStream::into_split`: Turmoil's
        // `OwnedWriteHalf::drop` sends FIN immediately and `OwnedReadHalf::drop` closes the
        // receiver channel so a peer's FIN triggers `Segment::Rst` on unidirectional data sockets
        // whose unused half is dropped after setup
        let (read_half, write_half) = tokio::io::split(*self);
        (
            Box::new(read_half) as BoxedRead,
            Box::new(write_half) as BoxedWrite,
        )
    }
}

struct TurmoilListener {
    inner: TcpListener,
    host_name: &'static str,
    peer_ip_override: Option<std::net::IpAddr>,
    link_health: Arc<TurmoilLinkHealth>,
    on_accept_control: Option<HookFn>,
    on_accept_data: Option<HookFn>,
    on_control_write_bytes: Option<ByteHookFn>,
    on_data_write_bytes: Option<ByteHookFn>,
}

impl TransportListener for TurmoilListener {
    fn advertised_addr(&self, _bind_ip: Option<&str>) -> anyhow::Result<std::net::SocketAddr> {
        let port = self.inner.local_addr()?.port();
        let host_ip = turmoil::lookup(self.host_name);
        Ok(std::net::SocketAddr::new(host_ip, port))
    }

    fn tunnel_target_ip(&self) -> anyhow::Result<std::net::IpAddr> {
        Ok(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
    }

    fn accept_control<'a>(
        &'a self,
        config: &'a remote::TcpConfig,
    ) -> BoxFuture<'a, std::io::Result<(BoxedTransportStream, std::net::SocketAddr)>> {
        Box::pin(async move {
            let (stream, addr) = self.inner.accept().await?;
            if let Some(hook) = &self.on_accept_control {
                hook();
            }
            let idle_timeout =
                (config.keepalive_sec > 0).then(|| Duration::from_secs(config.keepalive_sec));
            Ok((
                BoxedTransportStream::new(TurmoilStream::new(
                    stream,
                    self.peer_ip_override,
                    idle_timeout,
                    Arc::clone(&self.link_health),
                    self.on_control_write_bytes.clone(),
                )),
                addr,
            ))
        })
    }

    fn accept_data<'a>(
        &'a self,
        _profile: remote::NetworkProfile,
        keepalive_sec: u64,
    ) -> BoxFuture<'a, std::io::Result<(BoxedTransportStream, std::net::SocketAddr)>> {
        Box::pin(async move {
            let (stream, addr) = self.inner.accept().await?;
            if let Some(hook) = &self.on_accept_data {
                hook();
            }
            let idle_timeout =
                (keepalive_sec > 0).then(|| Duration::from_secs(keepalive_sec.saturating_mul(2)));
            Ok((
                BoxedTransportStream::new(TurmoilStream::new(
                    stream,
                    self.peer_ip_override,
                    idle_timeout,
                    Arc::clone(&self.link_health),
                    self.on_data_write_bytes.clone(),
                )),
                addr,
            ))
        })
    }
}

struct TurmoilTransport {
    host_name: &'static str,
    peer_ip_override: Option<std::net::IpAddr>,
    link_health: Arc<TurmoilLinkHealth>,
    on_accept_control: Option<HookFn>,
    on_accept_data: Option<HookFn>,
    on_control_write_bytes: Option<ByteHookFn>,
    on_data_write_bytes: Option<ByteHookFn>,
}

impl TurmoilTransport {
    fn direct(host_name: &'static str, link_health: Arc<TurmoilLinkHealth>) -> Arc<Self> {
        Arc::new(Self {
            host_name,
            peer_ip_override: None,
            link_health,
            on_accept_control: None,
            on_accept_data: None,
            on_control_write_bytes: None,
            on_data_write_bytes: None,
        })
    }

    fn simulated_ssh_loopback(
        host_name: &'static str,
        link_health: Arc<TurmoilLinkHealth>,
    ) -> Arc<Self> {
        Arc::new(Self {
            host_name,
            peer_ip_override: Some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            link_health,
            on_accept_control: None,
            on_accept_data: None,
            on_control_write_bytes: None,
            on_data_write_bytes: None,
        })
    }

    fn with_hooks(
        host_name: &'static str,
        link_health: Arc<TurmoilLinkHealth>,
        on_accept_control: Option<HookFn>,
        on_accept_data: Option<HookFn>,
        on_control_write_bytes: Option<ByteHookFn>,
        on_data_write_bytes: Option<ByteHookFn>,
    ) -> Arc<Self> {
        Arc::new(Self {
            host_name,
            peer_ip_override: None,
            link_health,
            on_accept_control,
            on_accept_data,
            on_control_write_bytes,
            on_data_write_bytes,
        })
    }
}

impl Transport for TurmoilTransport {
    fn bind_control<'a>(
        &'a self,
        _config: &'a remote::TcpConfig,
        _bind_ip: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Box<dyn TransportListener>>> {
        Box::pin(async move {
            let listener = TcpListener::bind(("0.0.0.0", 0)).await?;
            Ok(Box::new(TurmoilListener {
                inner: listener,
                host_name: self.host_name,
                peer_ip_override: self.peer_ip_override,
                link_health: Arc::clone(&self.link_health),
                on_accept_control: self.on_accept_control.clone(),
                on_accept_data: self.on_accept_data.clone(),
                on_control_write_bytes: self.on_control_write_bytes.clone(),
                on_data_write_bytes: self.on_data_write_bytes.clone(),
            }) as Box<dyn TransportListener>)
        })
    }

    fn bind_data<'a>(
        &'a self,
        _config: &'a remote::TcpConfig,
        _bind_ip: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Box<dyn TransportListener>>> {
        Box::pin(async move {
            let listener = TcpListener::bind(("0.0.0.0", 0)).await?;
            Ok(Box::new(TurmoilListener {
                inner: listener,
                host_name: self.host_name,
                peer_ip_override: self.peer_ip_override,
                link_health: Arc::clone(&self.link_health),
                on_accept_control: self.on_accept_control.clone(),
                on_accept_data: self.on_accept_data.clone(),
                on_control_write_bytes: self.on_control_write_bytes.clone(),
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
            let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
                .await
                .with_context(|| {
                    format!("simulated connection to {addr} timed out after {timeout:?}")
                })?
                .with_context(|| format!("failed to connect to {addr}"))?;
            let idle_timeout =
                (config.keepalive_sec > 0).then(|| Duration::from_secs(config.keepalive_sec));
            Ok(BoxedTransportStream::new(TurmoilStream::new(
                stream,
                self.peer_ip_override,
                idle_timeout,
                Arc::clone(&self.link_health),
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
            let idle_timeout =
                (keepalive_sec > 0).then(|| Duration::from_secs(keepalive_sec.saturating_mul(2)));
            Ok(BoxedTransportStream::new(TurmoilStream::new(
                stream,
                self.peer_ip_override,
                idle_timeout,
                Arc::clone(&self.link_health),
                None,
            )))
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum SecurityMode {
    Plaintext,
    Tls,
    AutoDirectTls,
    SshLoopbackHmac,
    AutoSshLoopbackHmac,
    /// Deliberately pairs `SshSharedSecret` with non-loopback Turmoil IPs to verify fail-closed.
    SshNonLoopbackRejected,
    /// Deliberately pairs `AutoTlsOrSshSecret` with `MasterSshTunnel` over non-loopback IPs.
    AutoSshNonLoopbackRejected,
}

#[derive(Default)]
struct EndpointHooks {
    source_peer_connected: Option<Arc<AtomicBool>>,
    on_source_finish: Option<FinishHookFn>,
    on_destination_finish: Option<FinishHookFn>,
}

struct SimCopyOutcome {
    source_result: EndpointResult,
    destination_result: EndpointResult,
    source_progress: Arc<common::progress::Progress>,
    destination_progress: Arc<common::progress::Progress>,
    source_peer_connected: bool,
    destination_peer_connected: bool,
}

fn default_copy_settings() -> common::copy::Settings {
    common::copy::Settings {
        reflink: common::copy_data::ReflinkMode::Auto,
        local_copy_handoff: common::copy::LocalCopyHandoff::default(),
        local_copy_buffer_retention_limit: common::copy_data::BufferRetentionLimit::default(),
        dereference: false,
        fail_early: false,
        overwrite: false,
        overwrite_compare: common::filecmp::MetadataCmpSettings {
            size: true,
            mtime: true,
            ..Default::default()
        },
        overwrite_filter: None,
        ignore_existing: false,
        chunk_size: 0,
        skip_specials: false,
        remote_copy_buffer_size: 8 * 1024,
        filter: None,
        dry_run: None,
        delete: None,
    }
}

fn default_tcp_config(conn_timeout_sec: u64, keepalive_sec: u64) -> remote::TcpConfig {
    remote::TcpConfig {
        port_ranges: None,
        conn_timeout_sec,
        network_profile: remote::NetworkProfile::Internet,
        buffer_size: Some(16 * 1024),
        buffer_retention_limit: None,
        keepalive_sec,
    }
}

fn remote_concurrency(
    max_connections: usize,
) -> (remote::ResolvedRemoteConcurrency, common::EndpointAdmission) {
    let conns = NonZeroUsize::new(max_connections.max(1)).unwrap();
    let max_files = NonZeroUsize::new(16).unwrap();
    let concurrency = remote::resolve_remote_concurrency(
        common::ConcurrencyLimit::Limited(conns),
        conns,
        max_files,
    )
    .unwrap();
    let admission = common::EndpointAdmission::Remote(
        common::RemoteResources::for_limits(
            Some(std::num::NonZeroU64::new(1024).unwrap()),
            conns,
            conns,
            max_files,
            0,
        )
        .unwrap(),
    );
    (concurrency, admission)
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
    std::fs::create_dir_all(root.join("beta/empty_dir")).unwrap();
    for i in 0..file_count {
        let dir = match i % 3 {
            0 => root.join("alpha"),
            1 => root.join("alpha/nested"),
            _ => root.join("beta"),
        };
        let mut content = vec![0u8; file_size];
        for (idx, byte) in content.iter_mut().enumerate() {
            *byte = ((i * 31 + idx * 17 + 7) & 0xff) as u8;
        }
        std::fs::write(dir.join(format!("file_{i:02}.bin")), content).unwrap();
    }
    std::os::unix::fs::symlink("file_00.bin", root.join("alpha/link_to_first")).unwrap();
}

#[allow(clippy::too_many_arguments)]
fn run_simulated_pair(
    mut sim: turmoil::Sim<'_>,
    src_path: PathBuf,
    dst_path: PathBuf,
    settings: common::copy::Settings,
    preserve: common::preserve::Settings,
    tcp_config: remote::TcpConfig,
    max_connections: usize,
    security: SecurityMode,
    src_transport: Arc<dyn Transport>,
    dst_transport: Arc<dyn Transport>,
    hooks: EndpointHooks,
) -> turmoil::Result<SimCopyOutcome> {
    install_crypto_provider();
    let (src_cert, dst_cert, src_auth, dst_auth, dst_mode) = match security {
        SecurityMode::Plaintext => (
            None,
            None,
            PeerAuth::None,
            PeerAuth::None,
            DestinationTransport::Direct,
        ),
        SecurityMode::Tls => {
            let sc = Arc::new(generate_self_signed_cert().unwrap());
            let dc = Arc::new(generate_self_signed_cert().unwrap());
            let sa = PeerAuth::Tls {
                peer_fingerprint: dc.fingerprint,
            };
            let da = PeerAuth::Tls {
                peer_fingerprint: sc.fingerprint,
            };
            (Some(sc), Some(dc), sa, da, DestinationTransport::Direct)
        }
        SecurityMode::AutoDirectTls => {
            let sc = Arc::new(generate_self_signed_cert().unwrap());
            let dc = Arc::new(generate_self_signed_cert().unwrap());
            let secret = SshAuthSecret::generate();
            let sa = PeerAuth::AutoTlsOrSshSecret {
                peer_fingerprint: dc.fingerprint,
                secret: secret.clone(),
            };
            let da = PeerAuth::AutoTlsOrSshSecret {
                peer_fingerprint: sc.fingerprint,
                secret,
            };
            (Some(sc), Some(dc), sa, da, DestinationTransport::Direct)
        }
        SecurityMode::SshLoopbackHmac | SecurityMode::SshNonLoopbackRejected => {
            let secret = SshAuthSecret::generate();
            (
                None,
                None,
                PeerAuth::SshSharedSecret {
                    secret: secret.clone(),
                },
                PeerAuth::SshSharedSecret { secret },
                DestinationTransport::MasterSshTunnel,
            )
        }
        SecurityMode::AutoSshLoopbackHmac | SecurityMode::AutoSshNonLoopbackRejected => {
            let sc = Arc::new(generate_self_signed_cert().unwrap());
            let dc = Arc::new(generate_self_signed_cert().unwrap());
            let secret = SshAuthSecret::generate();
            let sa = PeerAuth::AutoTlsOrSshSecret {
                peer_fingerprint: dc.fingerprint,
                secret: secret.clone(),
            };
            let da = PeerAuth::AutoTlsOrSshSecret {
                peer_fingerprint: sc.fingerprint,
                secret,
            };
            (
                Some(sc),
                Some(dc),
                sa,
                da,
                DestinationTransport::MasterSshTunnel,
            )
        }
    };
    let (hello_tx, hello_rx) = oneshot::channel::<SourceMasterHello>();
    let source_out: EndpointOutcome = Arc::new(Mutex::new(None));
    let dest_out: EndpointOutcome = Arc::new(Mutex::new(None));
    let source_progress = Arc::new(common::progress::Progress::new());
    let destination_progress = Arc::new(common::progress::Progress::new());
    let src_peer_connected = hooks
        .source_peer_connected
        .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    let dst_peer_connected = Arc::new(AtomicBool::new(false));
    let (src_concurrency, src_admission) = remote_concurrency(max_connections);
    let (dst_concurrency, dst_admission) = remote_concurrency(max_connections);
    let src_settings = settings.clone();
    let src_tcp = tcp_config.clone();
    let src_path_c = src_path.clone();
    let dst_path_c = dst_path.clone();
    let source_out_c = Arc::clone(&source_out);
    let source_progress_c = Arc::clone(&source_progress);
    let src_peer_connected_c = Arc::clone(&src_peer_connected);
    let on_source_finish = hooks.on_source_finish;
    sim.client("source", async move {
        let (master_tx, master_rx) = remote::stdio::daemon_control_channel();
        let (_trace_tx, trace_rx) = tokio::sync::mpsc::unbounded_channel();
        let (writer_end, reader_end) = tokio::io::duplex(8 * 1024);
        let writer_shutdown = CancellationToken::new();
        let writer_task = tokio::spawn(remote::stdio::run_daemon_stdout_writer(
            master_rx,
            trace_rx,
            SendStream::new(writer_end),
            writer_shutdown.clone(),
        ));
        let hello_task = tokio::spawn(async move {
            let mut recv = RecvStream::new(reader_end);
            if let Ok(Some(RcpdToMaster::SourceHello(hello))) =
                recv.recv_object::<RcpdToMaster>().await
            {
                let _ = hello_tx.send(hello);
            }
            while let Ok(Some(_)) = recv.recv_object::<RcpdToMaster>().await {}
        });
        let res = Box::pin(common::task_scope::scope_progress(
            source_progress_c,
            common::task_scope::scope_tasks(run_source_with_transport(
                &master_tx,
                &src_path_c,
                &dst_path_c,
                &src_settings,
                ExtendedMetadataCapture::default(),
                &src_tcp,
                src_concurrency,
                src_admission,
                common::FilesInFlightSource::Explicit,
                None,
                Duration::from_secs(src_tcp.conn_timeout_sec),
                src_cert.as_deref(),
                &src_auth,
                src_peer_connected_c.as_ref(),
                src_transport,
            )),
        ))
        .await;
        if let Some(hook) = &on_source_finish {
            hook(&res);
        }
        writer_shutdown.cancel();
        drop(master_tx);
        let _ = writer_task.await;
        let _ = hello_task.await;
        *source_out_c.lock().unwrap() = Some(res);
        Ok(())
    });
    let dest_out_c = Arc::clone(&dest_out);
    let destination_progress_c = Arc::clone(&destination_progress);
    let dst_peer_connected_c = Arc::clone(&dst_peer_connected);
    let on_destination_finish = hooks.on_destination_finish;
    sim.client("destination", async move {
        let hello = hello_rx
            .await
            .expect("source must emit SourceMasterHello before destination connects");
        let (master_tx, _master_rx) = remote::stdio::daemon_control_channel();
        let cleanup = remote::RemoteCleanup::new()?;
        let res = Box::pin(common::task_scope::scope_progress(
            destination_progress_c,
            common::task_scope::scope_tasks(run_destination_with_transport(
                &hello.control_addr,
                &[hello.data_addr],
                &hello.server_name,
                &settings,
                settings.dry_run.is_some(),
                10_000,
                &preserve,
                &tcp_config,
                dst_concurrency,
                dst_admission,
                dst_cert.as_deref(),
                &dst_auth,
                &dst_mode,
                &master_tx,
                None,
                &cleanup,
                &dst_peer_connected_c,
                dst_transport,
            )),
        ))
        .await;
        if let Some(hook) = &on_destination_finish {
            hook(&res);
        }
        cleanup.finish();
        *dest_out_c.lock().unwrap() = Some(res);
        Ok(())
    });
    sim.run()?;
    let source_result = source_out
        .lock()
        .unwrap()
        .take()
        .expect("source client must record its outcome");
    let destination_result = dest_out
        .lock()
        .unwrap()
        .take()
        .expect("destination client must record its outcome");
    Ok(SimCopyOutcome {
        source_result,
        destination_result,
        source_progress,
        destination_progress,
        source_peer_connected: src_peer_connected.load(Ordering::Acquire),
        destination_peer_connected: dst_peer_connected.load(Ordering::Acquire),
    })
}

#[test]
fn simulated_mtls_full_tree_copy_preserves_contents_and_isolates_summaries() -> turmoil::Result {
    let tmp = tempfile::tempdir()?;
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    populate_sample_tree(&src, 9, 12 * 1024);
    let sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .rng_seed(101)
        .build();
    let health = TurmoilLinkHealth::healthy();
    let outcome = run_simulated_pair(
        sim,
        src.clone(),
        dst.clone(),
        default_copy_settings(),
        common::preserve::preserve_none(),
        default_tcp_config(10, 5),
        4,
        SecurityMode::Tls,
        TurmoilTransport::direct("source", Arc::clone(&health)),
        TurmoilTransport::direct("destination", health),
        EndpointHooks::default(),
    )?;
    let (_, src_summary) = outcome
        .source_result
        .expect("mTLS simulated source must succeed");
    let (_, dst_summary) = outcome
        .destination_result
        .expect("mTLS simulated destination must succeed");
    assert_eq!(snapshot_tree(&src), snapshot_tree(&dst));
    // verify per-endpoint progress scoping prevented source/destination double counting
    assert_eq!(dst_summary.files_copied, 9);
    assert_eq!(dst_summary.bytes_copied, 9 * 12 * 1024);
    assert_eq!(dst_summary.symlinks_created, 1);
    assert_eq!(dst_summary.directories_created, 5);
    assert_eq!(src_summary.files_skipped, 0);
    assert_eq!(outcome.source_progress.files_copied.get(), 9);
    assert_eq!(outcome.destination_progress.files_copied.get(), 9);
    Ok(())
}

#[test]
fn simulated_ssh_hmac_and_auto_modes_enforce_loopback_and_transfer_tree() -> turmoil::Result {
    for (seed, mode, loopback, expect_ok) in [
        (201, SecurityMode::AutoDirectTls, false, true),
        (202, SecurityMode::SshLoopbackHmac, true, true),
        (203, SecurityMode::AutoSshLoopbackHmac, true, true),
        (204, SecurityMode::SshNonLoopbackRejected, false, false),
        (205, SecurityMode::AutoSshNonLoopbackRejected, false, false),
    ] {
        let tmp = tempfile::tempdir()?;
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        populate_sample_tree(&src, 4, 4096);
        let sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(60))
            .rng_seed(seed)
            .build();
        let health = TurmoilLinkHealth::healthy();
        let src_transport: Arc<dyn Transport> = if loopback {
            TurmoilTransport::simulated_ssh_loopback("source", Arc::clone(&health))
        } else {
            TurmoilTransport::direct("source", Arc::clone(&health))
        };
        let dst_transport: Arc<dyn Transport> = if loopback {
            TurmoilTransport::simulated_ssh_loopback("destination", health)
        } else {
            TurmoilTransport::direct("destination", health)
        };
        let outcome = run_simulated_pair(
            sim,
            src.clone(),
            dst.clone(),
            default_copy_settings(),
            common::preserve::preserve_none(),
            default_tcp_config(10, 5),
            2,
            mode,
            src_transport,
            dst_transport,
            EndpointHooks::default(),
        )?;
        if expect_ok {
            outcome
                .source_result
                .unwrap_or_else(|e| panic!("source failed for {mode:?}: {e:#}"));
            let (_, dst_summary) = outcome
                .destination_result
                .unwrap_or_else(|e| panic!("destination failed for {mode:?}: {e:#}"));
            assert_eq!(snapshot_tree(&src), snapshot_tree(&dst), "mode {mode:?}");
            assert_eq!(dst_summary.files_copied, 4, "mode {mode:?}");
        } else {
            let src_err = outcome
                .source_result
                .expect_err("non-loopback SSH HMAC must fail on source");
            let dst_err = outcome
                .destination_result
                .expect_err("non-loopback SSH HMAC must fail on destination");
            assert!(
                format!("{dst_err:#}").contains("non-loopback peer"),
                "expected destination non-loopback rejection for {mode:?}, got dst={dst_err:#}"
            );
            match mode {
                SecurityMode::SshNonLoopbackRejected => {
                    assert!(
                        format!("{src_err:#}").contains("non-loopback peer"),
                        "expected source non-loopback rejection, got src={src_err:#}"
                    );
                }
                SecurityMode::AutoSshNonLoopbackRejected => {
                    assert!(
                        format!("{src_err:#}").contains("non-loopback peer")
                            || format!("{src_err:#}").contains("before starting handshake"),
                        "expected source fail-closed error in Auto mode, got src={src_err:#}"
                    );
                }
                _ => unreachable!(),
            }
        }
    }
    Ok(())
}

#[test]
fn simulated_mid_transfer_partition_tears_down_cleanly_without_hang() -> turmoil::Result {
    let tmp = tempfile::tempdir()?;
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    populate_sample_tree(&src, 12, 32 * 1024);
    let health = TurmoilLinkHealth::healthy();
    let health_hook = Arc::clone(&health);
    let data_bytes_written = Arc::new(AtomicUsize::new(0));
    let data_bytes_hook = Arc::clone(&data_bytes_written);
    let partitioned = Arc::new(AtomicBool::new(false));
    let partitioned_hook = Arc::clone(&partitioned);
    // wait until >48 KiB of encrypted data traffic has left source (past the ~3 KiB mTLS
    // handshakes and squarely into file payloads) before severing the link
    let on_data_write: ByteHookFn = Arc::new(move |n| {
        let total = data_bytes_hook.fetch_add(n, Ordering::AcqRel) + n;
        if total >= 48 * 1024 && !partitioned_hook.swap(true, Ordering::AcqRel) {
            health_hook.set_impaired(true);
            turmoil::partition("source", "destination");
        }
    });
    let sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .min_message_latency(Duration::from_millis(5))
        .max_message_latency(Duration::from_millis(15))
        .rng_seed(301)
        .build();
    let outcome = run_simulated_pair(
        sim,
        src.clone(),
        dst.clone(),
        default_copy_settings(),
        common::preserve::preserve_none(),
        default_tcp_config(5, 3),
        2,
        SecurityMode::Tls,
        TurmoilTransport::with_hooks(
            "source",
            Arc::clone(&health),
            None,
            None,
            None,
            Some(on_data_write),
        ),
        TurmoilTransport::direct("destination", health),
        EndpointHooks::default(),
    )?;
    assert!(partitioned.load(Ordering::Acquire));
    assert!(
        data_bytes_written.load(Ordering::Acquire) >= 48 * 1024,
        "partition must fire mid-transfer after payload bytes were sent"
    );
    assert!(
        outcome.destination_progress.files_copied.get() < 12,
        "partition must cut off the transfer before all 12 files complete"
    );
    assert!(
        outcome.source_result.is_err(),
        "source must fail on mid-transfer partition, got: {:?}",
        outcome.source_result
    );
    assert!(
        outcome.destination_result.is_err(),
        "destination must fail on mid-transfer partition, got: {:?}",
        outcome.destination_result
    );
    Ok(())
}

#[test]
fn simulated_asymmetric_oneway_blackhole_tears_down_both_directions() -> turmoil::Result {
    for (seed, blackhole_src_to_dst) in [(401, true), (402, false)] {
        let tmp = tempfile::tempdir()?;
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        // 24 × 64 KiB (1.5 MiB) exceeds Turmoil's per-socket flow-control window so blackholing
        // destination -> source stalls source writes mid-transfer even while source -> destination
        // remains open
        populate_sample_tree(&src, 24, 64 * 1024);
        let health = TurmoilLinkHealth::healthy();
        let health_hook = Arc::clone(&health);
        let data_bytes_written = Arc::new(AtomicUsize::new(0));
        let data_bytes_hook = Arc::clone(&data_bytes_written);
        let triggered = Arc::new(AtomicBool::new(false));
        let triggered_hook = Arc::clone(&triggered);
        let on_data_write: ByteHookFn = Arc::new(move |n| {
            let total = data_bytes_hook.fetch_add(n, Ordering::AcqRel) + n;
            if total >= 48 * 1024 && !triggered_hook.swap(true, Ordering::AcqRel) {
                health_hook.set_impaired(true);
                if blackhole_src_to_dst {
                    turmoil::partition_oneway("source", "destination");
                } else {
                    turmoil::partition_oneway("destination", "source");
                }
            }
        });
        let sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(60))
            .min_message_latency(Duration::from_millis(5))
            .max_message_latency(Duration::from_millis(10))
            .rng_seed(seed)
            .build();
        let outcome = run_simulated_pair(
            sim,
            src.clone(),
            dst.clone(),
            default_copy_settings(),
            common::preserve::preserve_none(),
            default_tcp_config(5, 3),
            2,
            SecurityMode::Tls,
            TurmoilTransport::with_hooks(
                "source",
                Arc::clone(&health),
                None,
                None,
                None,
                Some(on_data_write),
            ),
            TurmoilTransport::direct("destination", health),
            EndpointHooks::default(),
        )?;
        assert!(triggered.load(Ordering::Acquire));
        assert!(
            data_bytes_written.load(Ordering::Acquire) >= 48 * 1024,
            "blackhole must fire after payload bytes were sent"
        );
        assert!(
            outcome.destination_progress.files_copied.get() < 24,
            "blackhole (src->dst={blackhole_src_to_dst}) must cut off transfer before all 24 files complete"
        );
        assert!(
            outcome.source_result.is_err() && outcome.destination_result.is_err(),
            "asymmetric blackhole (src->dst={blackhole_src_to_dst}) must fail both endpoints: src={:?} dst={:?}",
            outcome.source_result,
            outcome.destination_result
        );
    }
    Ok(())
}

#[test]
fn simulated_stalled_control_and_data_handshakes_time_out_in_virtual_time() -> turmoil::Result {
    let wall_start = Instant::now();
    // 1. stall control handshake immediately after TCP accept -> 10s conn_timeout fires in virtual time
    {
        let tmp = tempfile::tempdir()?;
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        populate_sample_tree(&src, 3, 2048);
        let health = TurmoilLinkHealth::healthy();
        let health_hook = Arc::clone(&health);
        let on_ctrl_accept: HookFn = Arc::new(move || {
            health_hook.set_impaired(true);
            turmoil::hold("destination", "source");
        });
        let sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(60))
            .rng_seed(501)
            .build();
        let outcome = run_simulated_pair(
            sim,
            src,
            dst,
            default_copy_settings(),
            common::preserve::preserve_none(),
            // disable simulated keepalive so the 10s accept_control_bounded handshake timeout itself fires
            default_tcp_config(10, 0),
            2,
            SecurityMode::Tls,
            TurmoilTransport::with_hooks(
                "source",
                Arc::clone(&health),
                Some(on_ctrl_accept),
                None,
                None,
                None,
            ),
            TurmoilTransport::direct("destination", health),
            EndpointHooks::default(),
        )?;
        let src_err = outcome
            .source_result
            .expect_err("stalled control TLS handshake must fail source");
        let dst_err = outcome
            .destination_result
            .expect_err("stalled control TLS handshake must fail destination");
        assert!(
            format!("{src_err:#}").contains("TLS handshake timed out for control connection"),
            "expected source control handshake timeout: {src_err:#}"
        );
        assert!(
            format!("{dst_err:#}").contains("TLS handshake timed out for control connection")
                || format!("{dst_err:#}").contains("TLS handshake"),
            "expected destination control handshake error: {dst_err:#}"
        );
    }
    // 2. allow control handshake, stall all data handshakes -> bounded 8s DataConnectionPool timeout
    {
        let tmp = tempfile::tempdir()?;
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        populate_sample_tree(&src, 3, 2048);
        let health = TurmoilLinkHealth::healthy();
        let health_hook = Arc::clone(&health);
        let held = Arc::new(AtomicBool::new(false));
        let held_hook = Arc::clone(&held);
        let on_data_accept: HookFn = Arc::new(move || {
            if !held_hook.swap(true, Ordering::AcqRel) {
                health_hook.set_impaired(true);
                turmoil::hold("destination", "source");
            }
        });
        let sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(60))
            .rng_seed(502)
            .build();
        let outcome = run_simulated_pair(
            sim,
            src,
            dst,
            default_copy_settings(),
            common::preserve::preserve_none(),
            // keepalive (20s) > conn_timeout (8s) so destination's DataConnectionPool timeout fires first
            default_tcp_config(8, 20),
            1,
            SecurityMode::Tls,
            TurmoilTransport::with_hooks(
                "source",
                Arc::clone(&health),
                None,
                Some(on_data_accept),
                None,
                None,
            ),
            TurmoilTransport::direct("destination", health),
            EndpointHooks::default(),
        )?;
        assert!(held.load(Ordering::Acquire));
        let src_err = outcome
            .source_result
            .expect_err("stalled data handshake must fail source");
        let dst_err = outcome
            .destination_result
            .expect_err("stalled data handshake must fail destination");
        assert!(
            format!("{dst_err:#}").contains("timed out"),
            "expected destination data handshake timeout: {dst_err:#}"
        );
        let _ = src_err;
    }
    assert!(
        wall_start.elapsed() < Duration::from_secs(10),
        "virtual handshake timeouts should complete well under 10s wall time, took {:?}",
        wall_start.elapsed()
    );
    Ok(())
}

#[test]
fn simulated_transient_link_hold_pauses_and_recovers_byte_identically() -> turmoil::Result {
    let tmp = tempfile::tempdir()?;
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    populate_sample_tree(&src, 10, 24 * 1024);
    let health = TurmoilLinkHealth::healthy();
    let health_hook = Arc::clone(&health);
    let health_chaos = Arc::clone(&health);
    let data_bytes_written = Arc::new(AtomicUsize::new(0));
    let data_bytes_hook = Arc::clone(&data_bytes_written);
    let data_started = Arc::new(Notify::new());
    let data_started_hook = Arc::clone(&data_started);
    let held_once = Arc::new(AtomicBool::new(false));
    let held_once_hook = Arc::clone(&held_once);
    // hold the link after >48 KiB of data payload is already in flight through mTLS
    let on_data_write: ByteHookFn = Arc::new(move |n| {
        let total = data_bytes_hook.fetch_add(n, Ordering::AcqRel) + n;
        if total >= 48 * 1024 && !held_once_hook.swap(true, Ordering::AcqRel) {
            health_hook.set_impaired(true);
            turmoil::hold("source", "destination");
            data_started_hook.notify_one();
        }
    });
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .min_message_latency(Duration::from_millis(2))
        .max_message_latency(Duration::from_millis(8))
        .rng_seed(601)
        .build();
    // separate chaos controller releases the link after 250ms virtual hold (< 10s keepalive)
    sim.client("chaos", async move {
        data_started.notified().await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        health_chaos.set_impaired(false);
        turmoil::release("source", "destination");
        Ok(())
    });
    let outcome = run_simulated_pair(
        sim,
        src.clone(),
        dst.clone(),
        default_copy_settings(),
        common::preserve::preserve_none(),
        default_tcp_config(15, 10),
        3,
        SecurityMode::Tls,
        TurmoilTransport::with_hooks(
            "source",
            Arc::clone(&health),
            None,
            None,
            None,
            Some(on_data_write),
        ),
        TurmoilTransport::direct("destination", health),
        EndpointHooks::default(),
    )?;
    assert!(held_once.load(Ordering::Acquire));
    assert!(data_bytes_written.load(Ordering::Acquire) >= 48 * 1024);
    outcome
        .source_result
        .expect("source must recover after transient hold");
    let (_, dst_summary) = outcome
        .destination_result
        .expect("destination must recover after transient hold");
    assert_eq!(snapshot_tree(&src), snapshot_tree(&dst));
    assert_eq!(dst_summary.files_copied, 10);
    Ok(())
}

#[test]
fn simulated_overwrite_manifest_skips_unchanged_and_updates_changed_under_latency()
-> turmoil::Result {
    let tmp = tempfile::tempdir()?;
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    std::fs::create_dir_all(src.join("sub")).unwrap();
    std::fs::create_dir_all(dst.join("sub")).unwrap();
    // unchanged file: identical size and mtime
    let fixed_mtime = filetime::FileTime::from_unix_time(1_700_000_000, 0);
    std::fs::write(src.join("sub/same.txt"), b"identical bytes").unwrap();
    std::fs::write(dst.join("sub/same.txt"), b"identical bytes").unwrap();
    filetime::set_file_mtime(src.join("sub/same.txt"), fixed_mtime).unwrap();
    filetime::set_file_mtime(dst.join("sub/same.txt"), fixed_mtime).unwrap();
    // changed file: different size and content (27 bytes replacing 3 bytes)
    std::fs::write(src.join("sub/changed.txt"), b"new updated content payload").unwrap();
    std::fs::write(dst.join("sub/changed.txt"), b"old").unwrap();
    // new file: missing on destination (14 bytes)
    std::fs::write(src.join("sub/fresh.txt"), b"brand new file").unwrap();
    let mut settings = default_copy_settings();
    settings.overwrite = true;
    let sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .min_message_latency(Duration::from_millis(15))
        .max_message_latency(Duration::from_millis(40))
        .rng_seed(701)
        .build();
    let health = TurmoilLinkHealth::healthy();
    let outcome = run_simulated_pair(
        sim,
        src.clone(),
        dst.clone(),
        settings,
        common::preserve::preserve_all(),
        default_tcp_config(15, 10),
        2,
        SecurityMode::AutoDirectTls,
        TurmoilTransport::direct("source", Arc::clone(&health)),
        TurmoilTransport::direct("destination", health),
        EndpointHooks::default(),
    )?;
    outcome
        .source_result
        .expect("overwrite manifest source must succeed");
    let (_, dst_summary) = outcome
        .destination_result
        .expect("overwrite manifest destination must succeed");
    assert_eq!(snapshot_tree(&src), snapshot_tree(&dst));
    // verify overwrite manifest skipped `sub/same.txt` on the source side (only 2 files / 41 bytes sent)
    assert_eq!(outcome.source_progress.files_copied.get(), 2);
    assert_eq!(outcome.source_progress.bytes_copied.get(), 27 + 14);
    assert_eq!(dst_summary.files_unchanged, 1);
    assert_eq!(dst_summary.files_copied, 2);
    assert_eq!(dst_summary.rm_summary.files_removed, 1);
    assert_eq!(dst_summary.rm_summary.bytes_removed, 3);
    // verify remove_existing_dst updated the scoped destination counters without touching globals
    assert_eq!(common::get_progress().files_removed.get(), 0);
    assert_eq!(common::get_progress().bytes_removed.get(), 0);
    Ok(())
}

#[test]
fn simulated_unreadable_source_file_best_effort_vs_fail_early() -> turmoil::Result {
    // SAFETY: `geteuid` is an always-safe POSIX syscall taking no pointers.
    if unsafe { libc::geteuid() } == 0 {
        return Ok(());
    }
    for fail_early in [false, true] {
        let tmp = tempfile::tempdir()?;
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        std::fs::create_dir_all(src.join("dir")).unwrap();
        std::fs::write(src.join("dir/ok_a.txt"), b"alpha").unwrap();
        std::fs::write(src.join("dir/ok_b.txt"), b"beta").unwrap();
        let blocked = src.join("dir/unreadable.txt");
        std::fs::write(&blocked, b"secret").unwrap();
        std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o000)).unwrap();
        let mut settings = default_copy_settings();
        settings.fail_early = fail_early;
        let sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(60))
            .rng_seed(if fail_early { 802 } else { 801 })
            .build();
        let health = TurmoilLinkHealth::healthy();
        let outcome = run_simulated_pair(
            sim,
            src.clone(),
            dst.clone(),
            settings,
            common::preserve::preserve_none(),
            default_tcp_config(10, 5),
            2,
            SecurityMode::Tls,
            TurmoilTransport::direct("source", Arc::clone(&health)),
            TurmoilTransport::direct("destination", health),
            EndpointHooks::default(),
        )?;
        // restore permissions so TempDir cleanup always succeeds cleanly
        let _ = std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o644));
        let src_err = outcome
            .source_result
            .expect_err("unreadable source file must report error");
        let src_err_text = format!("{src_err:#}");
        assert!(
            src_err_text.contains("Permission denied") || src_err_text.contains("unreadable.txt"),
            "expected permission error for unreadable.txt (fail_early={fail_early}), got: {src_err_text}"
        );
        if !fail_early {
            let (_, dst_summary) = outcome
                .destination_result
                .expect("best-effort destination must complete remaining files cleanly");
            assert_eq!(dst_summary.files_copied, 2);
            assert_eq!(std::fs::read(dst.join("dir/ok_a.txt")).unwrap(), b"alpha");
            assert_eq!(std::fs::read(dst.join("dir/ok_b.txt")).unwrap(), b"beta");
            assert!(!dst.join("dir/unreadable.txt").exists());
        } else {
            assert!(
                outcome.destination_result.is_err(),
                "fail-early must abort destination instead of completing cleanly"
            );
        }
    }
    Ok(())
}

#[test]
fn simulated_dry_run_completes_and_aborts_cleanly_on_partition() -> turmoil::Result {
    // 1. clean dry-run preview reports accurate counts and leaves destination untouched
    {
        let tmp = tempfile::tempdir()?;
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        populate_sample_tree(&src, 5, 1024);
        let mut settings = default_copy_settings();
        settings.dry_run = Some(common::config::DryRunMode::Brief);
        let sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(60))
            .rng_seed(901)
            .build();
        let health = TurmoilLinkHealth::healthy();
        let outcome = run_simulated_pair(
            sim,
            src,
            dst.clone(),
            settings,
            common::preserve::preserve_none(),
            default_tcp_config(10, 5),
            2,
            SecurityMode::Tls,
            TurmoilTransport::direct("source", Arc::clone(&health)),
            TurmoilTransport::direct("destination", health),
            EndpointHooks::default(),
        )?;
        let (_, src_summary) = outcome.source_result.expect("dry-run source must succeed");
        outcome
            .destination_result
            .expect("dry-run destination must succeed");
        assert_eq!(src_summary.files_copied, 5);
        assert_eq!(src_summary.bytes_copied, 5 * 1024);
        assert_eq!(src_summary.directories_created, 5);
        assert_eq!(src_summary.symlinks_created, 1);
        assert!(!dst.exists(), "dry-run must not create destination");
    }
    // 2. partition after control mTLS handshake succeeds (during active dry-run session) aborts
    //    cleanly without hanging
    {
        let tmp = tempfile::tempdir()?;
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        populate_sample_tree(&src, 5, 1024);
        let mut settings = default_copy_settings();
        settings.dry_run = Some(common::config::DryRunMode::Brief);
        let health = TurmoilLinkHealth::healthy();
        let health_hook = Arc::clone(&health);
        let src_peer_connected = Arc::new(AtomicBool::new(false));
        let src_peer_connected_hook = Arc::clone(&src_peer_connected);
        let partitioned = Arc::new(AtomicBool::new(false));
        let partitioned_hook = Arc::clone(&partitioned);
        // partition only once `source` has completed control mTLS (`peer_connected == true`) and
        // starts writing dry-run control frames (`DryRunReady` / `DiscoveryComplete`)
        let on_ctrl_write: ByteHookFn = Arc::new(move |_| {
            if src_peer_connected_hook.load(Ordering::Acquire)
                && !partitioned_hook.swap(true, Ordering::AcqRel)
            {
                health_hook.set_impaired(true);
                turmoil::partition("source", "destination");
            }
        });
        let sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(60))
            .min_message_latency(Duration::from_millis(5))
            .max_message_latency(Duration::from_millis(10))
            .rng_seed(902)
            .build();
        let outcome = run_simulated_pair(
            sim,
            src,
            dst.clone(),
            settings,
            common::preserve::preserve_none(),
            default_tcp_config(5, 3),
            2,
            SecurityMode::Tls,
            TurmoilTransport::with_hooks(
                "source",
                Arc::clone(&health),
                None,
                None,
                Some(on_ctrl_write),
                None,
            ),
            TurmoilTransport::direct("destination", health),
            EndpointHooks {
                source_peer_connected: Some(src_peer_connected),
                ..EndpointHooks::default()
            },
        )?;
        assert!(partitioned.load(Ordering::Acquire));
        assert!(
            outcome.source_peer_connected && outcome.destination_peer_connected,
            "partition must happen after control mTLS handshake succeeded on both endpoints"
        );
        assert!(outcome.source_result.is_err() && outcome.destination_result.is_err());
        assert!(!dst.exists());
    }
    Ok(())
}

/// Wrapper stream that injects a deterministic mid-body data truncation on the destination's first
/// data connection once `remaining_before_eof` bytes have been read, recording protocol events for
/// the 5-event full-pipeline teardown subset (`DataPrefix`, `ShortData`, `ControlEOF`,
/// `DestinationError`, `SourceError`).
struct TruncatingDataRead {
    inner: BoxedRead,
    remaining_before_eof: usize,
    truncated: bool,
    trace: Arc<Mutex<Vec<&'static str>>>,
}

impl AsyncRead for TruncatingDataRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.truncated {
            return Poll::Ready(Ok(()));
        }
        if self.remaining_before_eof == 0 {
            self.truncated = true;
            self.trace.lock().unwrap().push("ShortData");
            return Poll::Ready(Ok(()));
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let max_to_read = buf.remaining().min(self.remaining_before_eof);
        let mut limited_storage = vec![0u8; max_to_read];
        let mut limited_buf = ReadBuf::new(&mut limited_storage);
        match Pin::new(&mut self.inner).poll_read(cx, &mut limited_buf) {
            Poll::Ready(Ok(())) => {
                let n = limited_buf.filled().len();
                if n > 0 {
                    buf.put_slice(limited_buf.filled());
                    self.remaining_before_eof -= n;
                    if self.remaining_before_eof == 0 {
                        self.trace.lock().unwrap().push("DataPrefix");
                    }
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Control read wrapper on the source side that records `ControlEOF` when the destination closes
/// control without sending `DestinationDone`.
struct ObservingControlRead {
    inner: BoxedRead,
    recorded_eof: bool,
    trace: Arc<Mutex<Vec<&'static str>>>,
}

impl AsyncRead for ObservingControlRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if buf.filled().len() == before && !self.recorded_eof {
                    self.recorded_eof = true;
                    self.trace.lock().unwrap().push("ControlEOF");
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

/// Plaintext-only transport stream wrapper that attaches [`ObservingControlRead`] or
/// [`TruncatingDataRead`] when split.
struct TracingTurmoilStream {
    inner: BoxedTransportStream,
    mode: TracingStreamMode,
}

enum TracingStreamMode {
    ControlSourceObserveEof(Arc<Mutex<Vec<&'static str>>>),
    DataDestinationTruncate {
        bytes_before_eof: usize,
        trace: Arc<Mutex<Vec<&'static str>>>,
    },
}

impl AsyncRead for TracingTurmoilStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        unreachable!("TracingTurmoilStream is Plaintext-only and must be split via into_split")
    }
}

impl AsyncWrite for TracingTurmoilStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        unreachable!("TracingTurmoilStream is Plaintext-only and must be split via into_split")
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        unreachable!("TracingTurmoilStream is Plaintext-only and must be split via into_split")
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        unreachable!("TracingTurmoilStream is Plaintext-only and must be split via into_split")
    }
}

impl TransportStream for TracingTurmoilStream {
    fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        Ok(std::net::SocketAddr::from(([127, 0, 0, 1], 1)))
    }

    fn into_split(self: Box<Self>) -> (BoxedRead, BoxedWrite) {
        let (read, write) = tokio::io::split(self.inner);
        let read = Box::new(read) as BoxedRead;
        let write = Box::new(write) as BoxedWrite;
        match self.mode {
            TracingStreamMode::ControlSourceObserveEof(trace) => (
                Box::new(ObservingControlRead {
                    inner: read,
                    recorded_eof: false,
                    trace,
                }) as BoxedRead,
                write,
            ),
            TracingStreamMode::DataDestinationTruncate {
                bytes_before_eof,
                trace,
            } => (
                Box::new(TruncatingDataRead {
                    inner: read,
                    remaining_before_eof: bytes_before_eof,
                    truncated: false,
                    trace,
                }) as BoxedRead,
                write,
            ),
        }
    }
}

struct TeardownFaultTransport {
    inner: Arc<TurmoilTransport>,
    trace: Arc<Mutex<Vec<&'static str>>>,
    truncate_data_bytes: Option<usize>,
    truncated_first_data: AtomicBool,
    observe_control_eof: bool,
}

struct TeardownFaultListener {
    inner: Box<dyn TransportListener>,
    trace: Arc<Mutex<Vec<&'static str>>>,
    observe_control_eof: bool,
}

impl TransportListener for TeardownFaultListener {
    fn advertised_addr(&self, bind_ip: Option<&str>) -> anyhow::Result<std::net::SocketAddr> {
        self.inner.advertised_addr(bind_ip)
    }

    fn tunnel_target_ip(&self) -> anyhow::Result<std::net::IpAddr> {
        self.inner.tunnel_target_ip()
    }

    fn accept_control<'a>(
        &'a self,
        config: &'a remote::TcpConfig,
    ) -> BoxFuture<'a, std::io::Result<(BoxedTransportStream, std::net::SocketAddr)>> {
        Box::pin(async move {
            let (stream, addr) = self.inner.accept_control(config).await?;
            if self.observe_control_eof {
                Ok((
                    BoxedTransportStream::new(TracingTurmoilStream {
                        inner: stream,
                        mode: TracingStreamMode::ControlSourceObserveEof(Arc::clone(&self.trace)),
                    }),
                    addr,
                ))
            } else {
                Ok((stream, addr))
            }
        })
    }

    fn accept_data<'a>(
        &'a self,
        profile: remote::NetworkProfile,
        keepalive_sec: u64,
    ) -> BoxFuture<'a, std::io::Result<(BoxedTransportStream, std::net::SocketAddr)>> {
        self.inner.accept_data(profile, keepalive_sec)
    }
}

impl Transport for TeardownFaultTransport {
    fn bind_control<'a>(
        &'a self,
        config: &'a remote::TcpConfig,
        bind_ip: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Box<dyn TransportListener>>> {
        Box::pin(async move {
            let inner = self.inner.bind_control(config, bind_ip).await?;
            Ok(Box::new(TeardownFaultListener {
                inner,
                trace: Arc::clone(&self.trace),
                observe_control_eof: self.observe_control_eof,
            }) as Box<dyn TransportListener>)
        })
    }

    fn bind_data<'a>(
        &'a self,
        config: &'a remote::TcpConfig,
        bind_ip: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Box<dyn TransportListener>>> {
        self.inner.bind_data(config, bind_ip)
    }

    fn connect_control<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        config: &'a remote::TcpConfig,
    ) -> BoxFuture<'a, anyhow::Result<BoxedTransportStream>> {
        self.inner.connect_control(addr, config)
    }

    fn connect_control_with_timeout<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        config: &'a remote::TcpConfig,
        timeout: Duration,
    ) -> BoxFuture<'a, anyhow::Result<BoxedTransportStream>> {
        self.inner
            .connect_control_with_timeout(addr, config, timeout)
    }

    fn connect_data<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        profile: remote::NetworkProfile,
        keepalive_sec: u64,
    ) -> BoxFuture<'a, std::io::Result<BoxedTransportStream>> {
        Box::pin(async move {
            let stream = self
                .inner
                .connect_data(addr, profile, keepalive_sec)
                .await?;
            if let Some(bytes_before_eof) = self.truncate_data_bytes
                && !self.truncated_first_data.swap(true, Ordering::AcqRel)
            {
                Ok(BoxedTransportStream::new(TracingTurmoilStream {
                    inner: stream,
                    mode: TracingStreamMode::DataDestinationTruncate {
                        bytes_before_eof,
                        trace: Arc::clone(&self.trace),
                    },
                }))
            } else {
                Ok(stream)
            }
        })
    }
}

/// Validates the 5-event full-pipeline teardown subset (`DataPrefix` -> `ShortData` ->
/// `{ControlEOF -> SourceError, DestinationError}`), recorded live inside the simulated tasks as
/// each wire event and endpoint error occurs. The full 6-event `RemoteTeardown.tla` trace
/// including the `WaitB` gate lives in `rcp/src/source/discovery/tests/trace_teardown.rs`.
fn assert_matches_teardown_order(events: &[&'static str]) {
    let mut seen = std::collections::BTreeSet::new();
    for &event in events {
        assert!(
            !seen.contains(event),
            "duplicate teardown trace event {event:?} in {events:?}"
        );
        let prereqs: &[&str] = match event {
            "DataPrefix" => &[],
            "ShortData" => &["DataPrefix"],
            "ControlEOF" | "DestinationError" => &["ShortData"],
            "SourceError" => &["ControlEOF"],
            other => panic!("unexpected teardown trace event {other:?}"),
        };
        for prereq in prereqs {
            assert!(
                seen.contains(prereq),
                "event {event:?} violated teardown prerequisite {prereq:?} in trace {events:?}"
            );
        }
        seen.insert(event);
    }
    let expected: std::collections::BTreeSet<&str> = [
        "DataPrefix",
        "ShortData",
        "ControlEOF",
        "SourceError",
        "DestinationError",
    ]
    .into_iter()
    .collect();
    assert_eq!(seen, expected, "incomplete teardown trace: {events:?}");
}

#[test]
fn simulated_short_data_teardown_closes_control_without_done_and_fails_both_endpoints()
-> turmoil::Result {
    let tmp = tempfile::tempdir()?;
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    std::fs::create_dir_all(src.join("a")).unwrap();
    std::fs::create_dir_all(src.join("b")).unwrap();
    // 16 KiB body guarantees that truncating at 1024 bytes lands strictly inside file payload
    // after the serialized `File` frame header
    std::fs::write(src.join("a/file.bin"), vec![0xab; 16 * 1024]).unwrap();
    std::fs::write(src.join("b/file.bin"), vec![0xcd; 16 * 1024]).unwrap();
    let trace: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    let health = TurmoilLinkHealth::healthy();
    let src_transport: Arc<dyn Transport> = Arc::new(TeardownFaultTransport {
        inner: TurmoilTransport::direct("source", Arc::clone(&health)),
        trace: Arc::clone(&trace),
        truncate_data_bytes: None,
        truncated_first_data: AtomicBool::new(false),
        observe_control_eof: true,
    });
    let dst_transport: Arc<dyn Transport> = Arc::new(TeardownFaultTransport {
        inner: TurmoilTransport::direct("destination", health),
        trace: Arc::clone(&trace),
        truncate_data_bytes: Some(1024),
        truncated_first_data: AtomicBool::new(false),
        observe_control_eof: false,
    });
    let trace_for_src = Arc::clone(&trace);
    let trace_for_dst = Arc::clone(&trace);
    let sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .rng_seed(1001)
        .build();
    let outcome = run_simulated_pair(
        sim,
        src,
        dst,
        default_copy_settings(),
        common::preserve::preserve_none(),
        default_tcp_config(10, 5),
        1,
        SecurityMode::Plaintext,
        src_transport,
        dst_transport,
        EndpointHooks {
            source_peer_connected: None,
            on_source_finish: Some(Arc::new(move |res| {
                if res.is_err() {
                    trace_for_src.lock().unwrap().push("SourceError");
                }
            })),
            on_destination_finish: Some(Arc::new(move |res| {
                if res.is_err() {
                    trace_for_dst.lock().unwrap().push("DestinationError");
                }
            })),
        },
    )?;
    let dst_err = outcome
        .destination_result
        .expect_err("truncated data stream must fail destination");
    assert!(
        format!("{dst_err:#}").contains("unexpected EOF"),
        "expected destination unexpected EOF error, got: {dst_err:#}"
    );
    let src_err = outcome
        .source_result
        .expect_err("control EOF without DestinationDone must fail source");
    assert!(
        format!("{src_err:#}").contains("destination closed control before DestinationDone"),
        "expected source control EOF error without DestinationDone, got: {src_err:#}"
    );
    let recorded = trace.lock().unwrap().clone();
    assert_matches_teardown_order(&recorded);
    Ok(())
}

#[test]
fn simulated_seeded_chaos_upholds_no_hang_and_no_silent_corruption_invariants() -> turmoil::Result {
    // 16 deterministic seeds sweeping healthy high-jitter transfers and random TCP link breaks
    // across mTLS 1.3, Auto, SSH-HMAC, and plaintext with varied connection pool sizes
    let cases: &[(u64, f64, f64, usize, SecurityMode)] = &[
        (1, 0.0, 1.0, 1, SecurityMode::Tls),
        (2, 0.0, 1.0, 4, SecurityMode::Tls),
        (3, 0.0, 1.0, 2, SecurityMode::AutoDirectTls),
        (4, 0.0, 1.0, 3, SecurityMode::Plaintext),
        (5, 0.03, 0.5, 1, SecurityMode::Tls),
        (6, 0.03, 0.5, 2, SecurityMode::Tls),
        (7, 0.05, 0.4, 4, SecurityMode::Tls),
        (8, 0.08, 0.3, 2, SecurityMode::Tls),
        (9, 0.12, 0.2, 1, SecurityMode::Tls),
        (10, 0.12, 0.2, 4, SecurityMode::AutoDirectTls),
        (11, 0.05, 0.4, 2, SecurityMode::Plaintext),
        (12, 0.10, 0.25, 3, SecurityMode::Plaintext),
        (13, 0.04, 0.5, 2, SecurityMode::SshLoopbackHmac),
        (14, 0.08, 0.3, 4, SecurityMode::SshLoopbackHmac),
        (15, 0.06, 0.4, 2, SecurityMode::AutoSshLoopbackHmac),
        (16, 0.15, 0.2, 4, SecurityMode::Tls),
    ];
    let mut completed_ok = 0usize;
    let mut failed_cleanly = 0usize;
    for &(seed, fail_rate, repair_rate, max_conns, mode) in cases {
        let tmp = tempfile::tempdir()?;
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        populate_sample_tree(&src, 6, 8 * 1024);
        let expected_tree = snapshot_tree(&src);
        let sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(60))
            .min_message_latency(Duration::from_millis(2))
            .max_message_latency(Duration::from_millis(25))
            .fail_rate(fail_rate)
            .repair_rate(repair_rate)
            .rng_seed(seed)
            .build();
        let health = if fail_rate > 0.0 {
            TurmoilLinkHealth::lossy()
        } else {
            TurmoilLinkHealth::healthy()
        };
        let loopback = matches!(
            mode,
            SecurityMode::SshLoopbackHmac | SecurityMode::AutoSshLoopbackHmac
        );
        let src_transport: Arc<dyn Transport> = if loopback {
            TurmoilTransport::simulated_ssh_loopback("source", Arc::clone(&health))
        } else {
            TurmoilTransport::direct("source", Arc::clone(&health))
        };
        let dst_transport: Arc<dyn Transport> = if loopback {
            TurmoilTransport::simulated_ssh_loopback("destination", health)
        } else {
            TurmoilTransport::direct("destination", health)
        };
        let outcome = run_simulated_pair(
            sim,
            src,
            dst.clone(),
            default_copy_settings(),
            common::preserve::preserve_none(),
            default_tcp_config(6, 3),
            max_conns,
            mode,
            src_transport,
            dst_transport,
            EndpointHooks::default(),
        )
        .unwrap_or_else(|e| {
            panic!(
                "simulation hung or failed harness (seed={seed}, fail_rate={fail_rate}, mode={mode:?}): {e}"
            )
        });
        match (&outcome.source_result, &outcome.destination_result) {
            (Ok(_), Ok((_, dst_summary))) => {
                completed_ok += 1;
                assert_eq!(
                    snapshot_tree(&dst),
                    expected_tree,
                    "silent corruption on seed={seed}, fail_rate={fail_rate}, mode={mode:?}"
                );
                assert_eq!(
                    dst_summary.files_copied, 6,
                    "wrong summary count on seed={seed}, mode={mode:?}"
                );
            }
            (Err(_), Ok((_, dst_summary))) => {
                // destination may only report Ok if it received and wrote every entry intact
                assert_eq!(
                    snapshot_tree(&dst),
                    expected_tree,
                    "destination returned Ok with incomplete/corrupted tree on seed={seed}, fail_rate={fail_rate}, mode={mode:?} (source_err={:?})",
                    outcome.source_result
                );
                assert_eq!(
                    dst_summary.files_copied, 6,
                    "wrong summary count on seed={seed}, mode={mode:?}"
                );
                failed_cleanly += 1;
            }
            (Ok(_), Err(_)) => {
                // source only returns Ok after receiving DestinationDone, so destination must have
                // written the complete byte-identical tree before any post-Done control tear error
                assert_eq!(
                    snapshot_tree(&dst),
                    expected_tree,
                    "source returned Ok with incomplete/corrupted destination tree on seed={seed}, fail_rate={fail_rate}, mode={mode:?} (dst_err={:?})",
                    outcome.destination_result
                );
                failed_cleanly += 1;
            }
            (Err(_), Err(_)) => {
                failed_cleanly += 1;
            }
        }
        if fail_rate == 0.0 {
            assert!(
                outcome.source_result.is_ok() && outcome.destination_result.is_ok(),
                "zero-loss seed={seed} ({mode:?}) must succeed: src={:?} dst={:?}",
                outcome.source_result,
                outcome.destination_result
            );
        }
    }
    assert!(
        completed_ok >= 4,
        "expected at least the 4 zero-loss cases to succeed, got {completed_ok}"
    );
    assert!(
        failed_cleanly >= 1,
        "expected fault-injected seeds to exercise failure paths, got {failed_cleanly}"
    );
    Ok(())
}

#[test]
fn simulated_source_host_crash_fails_destination_cleanly() -> turmoil::Result {
    install_crypto_provider();
    let tmp = tempfile::tempdir()?;
    let src = tmp.path().join("src");
    let dst = tmp.path().join("dst");
    populate_sample_tree(&src, 12, 32 * 1024);
    let src_cert = Arc::new(generate_self_signed_cert().unwrap());
    let dst_cert = Arc::new(generate_self_signed_cert().unwrap());
    let src_auth = PeerAuth::Tls {
        peer_fingerprint: dst_cert.fingerprint,
    };
    let dst_auth = PeerAuth::Tls {
        peer_fingerprint: src_cert.fingerprint,
    };
    let (hello_tx, hello_rx) = oneshot::channel::<SourceMasterHello>();
    let hello_tx = Arc::new(Mutex::new(Some(hello_tx)));
    let health = TurmoilLinkHealth::healthy();
    let dst_progress = Arc::new(common::progress::Progress::new());
    let dest_out: EndpointOutcome = Arc::new(Mutex::new(None));
    let (src_concurrency, src_admission) = remote_concurrency(2);
    let (dst_concurrency, dst_admission) = remote_concurrency(2);
    let settings = default_copy_settings();
    let preserve = common::preserve::preserve_none();
    let tcp_config = default_tcp_config(5, 3);
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .min_message_latency(Duration::from_millis(5))
        .max_message_latency(Duration::from_millis(15))
        .rng_seed(1101)
        .build();
    let src_c = src.clone();
    let dst_c = dst.clone();
    let src_settings = settings.clone();
    let src_tcp = tcp_config.clone();
    let src_cert_c = Arc::clone(&src_cert);
    let src_auth_c = src_auth.clone();
    let health_src = Arc::clone(&health);
    sim.host("source", move || {
        let src_c = src_c.clone();
        let dst_c = dst_c.clone();
        let src_settings = src_settings.clone();
        let src_tcp = src_tcp.clone();
        let src_cert_c = Arc::clone(&src_cert_c);
        let src_auth_c = src_auth_c.clone();
        let hello_tx = Arc::clone(&hello_tx);
        let health_src = Arc::clone(&health_src);
        async move {
            let (master_tx, master_rx) = remote::stdio::daemon_control_channel();
            let (_trace_tx, trace_rx) = tokio::sync::mpsc::unbounded_channel();
            let (writer_end, reader_end) = tokio::io::duplex(8 * 1024);
            let writer_shutdown = CancellationToken::new();
            let writer_task = tokio::spawn(remote::stdio::run_daemon_stdout_writer(
                master_rx,
                trace_rx,
                SendStream::new(writer_end),
                writer_shutdown.clone(),
            ));
            let hello_task = tokio::spawn(async move {
                let mut recv = RecvStream::new(reader_end);
                if let Ok(Some(RcpdToMaster::SourceHello(hello))) =
                    recv.recv_object::<RcpdToMaster>().await
                    && let Some(tx) = hello_tx.lock().unwrap().take()
                {
                    let _ = tx.send(hello);
                }
                while let Ok(Some(_)) = recv.recv_object::<RcpdToMaster>().await {}
            });
            let peer_connected = AtomicBool::new(false);
            let _ = Box::pin(common::task_scope::scope_progress(
                Arc::new(common::progress::Progress::new()),
                common::task_scope::scope_tasks(run_source_with_transport(
                    &master_tx,
                    &src_c,
                    &dst_c,
                    &src_settings,
                    ExtendedMetadataCapture::default(),
                    &src_tcp,
                    src_concurrency,
                    src_admission,
                    common::FilesInFlightSource::Explicit,
                    None,
                    Duration::from_secs(src_tcp.conn_timeout_sec),
                    Some(src_cert_c.as_ref()),
                    &src_auth_c,
                    &peer_connected,
                    TurmoilTransport::direct("source", health_src),
                )),
            ))
            .await;
            writer_shutdown.cancel();
            drop(master_tx);
            let _ = writer_task.await;
            let _ = hello_task.await;
            Ok(())
        }
    });
    let dest_out_c = Arc::clone(&dest_out);
    let dst_progress_c = Arc::clone(&dst_progress);
    let health_dst = Arc::clone(&health);
    sim.client("destination", async move {
        let hello = hello_rx.await.expect("source hello");
        let (master_tx, _master_rx) = remote::stdio::daemon_control_channel();
        let cleanup = remote::RemoteCleanup::new()?;
        let peer_connected = Arc::new(AtomicBool::new(false));
        let res = Box::pin(common::task_scope::scope_progress(
            dst_progress_c,
            common::task_scope::scope_tasks(run_destination_with_transport(
                &hello.control_addr,
                &[hello.data_addr],
                &hello.server_name,
                &settings,
                false,
                10_000,
                &preserve,
                &tcp_config,
                dst_concurrency,
                dst_admission,
                Some(dst_cert.as_ref()),
                &dst_auth,
                &DestinationTransport::Direct,
                &master_tx,
                None,
                &cleanup,
                &peer_connected,
                TurmoilTransport::direct("destination", health_dst),
            )),
        ))
        .await;
        cleanup.finish();
        *dest_out_c.lock().unwrap() = Some(res);
        Ok(())
    });
    let mut crashed_after_files = None;
    while !sim.step()? {
        let received = dst_progress.files_copied.get();
        if crashed_after_files.is_none() && received >= 2 {
            health.set_impaired(true);
            sim.crash("source");
            crashed_after_files = Some(received);
        }
    }
    let received_at_crash = crashed_after_files
        .expect("source host should have been crashed after destination received >=2 files");
    assert!(
        received_at_crash < 12,
        "crash should happen mid-transfer before all 12 files finish, happened at {received_at_crash}"
    );
    let dst_res = dest_out
        .lock()
        .unwrap()
        .take()
        .expect("destination must finish after source crash");
    assert!(
        dst_res.is_err(),
        "destination must fail cleanly when source host crashes mid-transfer, got: {dst_res:?}"
    );
    Ok(())
}
