//! Protocol conformance at the real copy endpoints, with a gated loopback relay.
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use anyhow::{Context as _, ensure};
use remote::protocol::{DestinationMessage, DestinationTransport, DirectoryLimits, File};
use remote::streams::{BoxedRead, BoxedWrite, RecvStream, SendStream};
use tokio::io::AsyncWriteExt as _;
use tokio::net::TcpListener;
use tokio::sync::Notify;

use super::{Fatal, HANG, HOOKS, Hooks, SrcDst, run, settings};

#[derive(Clone, Default)]
struct Trace(Arc<Mutex<Vec<&'static str>>>);

impl Trace {
    fn record(&self, event: &'static str) {
        self.0.lock().unwrap().push(event);
    }
}

#[tokio::test]
async fn remote_teardown_trace() -> anyhow::Result<()> {
    let temp = tempfile::tempdir()?;
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    for name in ["a", "b"] {
        std::fs::create_dir_all(src.join(name))?;
        std::fs::write(src.join(name).join("file"), b"abcd")?;
    }
    let control = TcpListener::bind("127.0.0.1:0").await?;
    let data = TcpListener::bind("127.0.0.1:0").await?;
    let control_addr = control.local_addr()?;
    let data_addr = data.local_addr()?;
    let (source_control, relay_control) = tokio::io::duplex(64 * 1024);
    let (source_read, source_write) = tokio::io::split(source_control);
    let (mut relay_read, relay_write) = tokio::io::split(relay_control);
    let (source_data, relay_data) = tokio::io::duplex(64 * 1024);
    let (send, receive) = async_channel::bounded(1);
    send.try_send(SendStream::new(Box::new(source_data) as BoxedWrite))
        .expect("one free stream slot");
    let pool = Arc::new(super::super::super::AcceptingSendStreamPool {
        recv: receive,
        return_tx: send,
    });
    let trace = Trace::default();
    let b_waiting = Arc::new(Notify::new());
    let hooks = Hooks::new({
        let waiting = b_waiting.clone();
        let trace = trace.clone();
        let b_file = src.join("b/file");
        move |event, path, _| {
            if event == "wait_ready" && path == b_file {
                trace.record("WaitB");
                waiting.notify_one();
            }
            Box::pin(async { Ok(()) })
        }
    });
    let one = NonZeroUsize::MIN;
    let eight = NonZeroUsize::new(8).unwrap();
    let default_admission = common::EndpointAdmission::Remote(common::RemoteResources::for_limits(
        Some(std::num::NonZeroU64::new(1024).unwrap()),
        one,
        one,
        eight,
        0,
    )?);
    let concurrency =
        remote::resolve_remote_concurrency(common::ConcurrencyLimit::Limited(one), one, eight)?;
    let (master_control, _unused_master) = remote::stdio::daemon_control_channel();
    let cleanup = remote::RemoteCleanup::new()?;
    let configuration = settings(false);
    let preserve = common::preserve::preserve_none();
    let tcp = remote::TcpConfig::default();
    let peer_connected = Arc::default();
    let limits = tokio::sync::oneshot::channel();
    let source = async {
        let (recv, limits) = limits
            .1
            .await
            .context("no directory limits from destination")?;
        let result = HOOKS
            .scope(
                hooks,
                run(
                    configuration.clone(),
                    Default::default(),
                    SrcDst {
                        src,
                        dst: dst.clone(),
                    },
                    Arc::new(tokio::sync::Mutex::new(SendStream::new(
                        Box::new(source_write) as BoxedWrite,
                    ))),
                    recv,
                    pool,
                    Arc::new(Fatal::new(Default::default())),
                    1,
                    8,
                    default_admission,
                    common::FilesInFlightSource::Automatic,
                    limits,
                    Arc::default(),
                ),
            )
            .await;
        let error = result.expect_err("source falsely reported success after short data");
        ensure!(
            format!("{error:#}").contains("destination closed control before DestinationDone"),
            "source did not preserve the control EOF: {error:#}"
        );
        trace.record("SourceError");
        Ok::<_, anyhow::Error>(())
    };
    let destination = async {
        let error = crate::destination::run_destination(
            &control_addr,
            &[data_addr],
            "source",
            &configuration,
            false,
            0,
            &preserve,
            &tcp,
            concurrency,
            default_admission,
            None,
            &remote::protocol::PeerAuth::None,
            &DestinationTransport::Direct,
            &master_control,
            None,
            &cleanup,
            &peer_connected,
        )
        .await
        .expect_err("destination falsely reported success after short data");
        let error = error.downcast::<common::copy::Error>()?;
        ensure!(
            format!("{:#}", error.source).contains("unexpected EOF: expected 4 bytes, got 2"),
            "destination did not preserve the short body: {:#}",
            error.source
        );
        trace.record("DestinationError");
        Ok::<_, anyhow::Error>(())
    };
    let control_relay = async {
        let (stream, _) = remote::accept_tcp_control(&control, &tcp).await?;
        let (read, mut write) = stream.into_split();
        let mut destination = RecvStream::new(read);
        let mut to_source = SendStream::new(relay_write);
        let advertised = destination
            .recv_object::<DirectoryLimits>()
            .await?
            .context("missing limits")?;
        limits
            .0
            .send((
                RecvStream::new(Box::new(source_read) as BoxedRead),
                advertised,
            ))
            .ok()
            .context("source dropped the limits channel")?;
        let uplink = async {
            tokio::io::copy(&mut relay_read, &mut write).await?;
            write.shutdown().await
        };
        let downlink = async {
            let mut held_b_ready = None;
            while let Some(message) = destination.recv_object::<DestinationMessage>().await? {
                match &message {
                    DestinationMessage::DirectoryReady { dst: path, .. }
                        if path == &dst.join("b") =>
                    {
                        ensure!(
                            held_b_ready.replace(message).is_none(),
                            "duplicate Ready for B"
                        );
                        continue;
                    }
                    DestinationMessage::DestinationDone => {
                        anyhow::bail!("destination sent Done after short data")
                    }
                    _ => {}
                }
                to_source.send_control_message(&message).await?;
            }
            trace.record("ControlEOF");
            to_source.close().await?;
            Ok::<_, anyhow::Error>(())
        };
        tokio::try_join!(
            async { uplink.await.map_err(anyhow::Error::from) },
            downlink
        )?;
        Ok::<_, anyhow::Error>(())
    };
    let data_relay = async {
        let (mut stream, _) =
            remote::accept_tcp_data(&data, tcp.network_profile, tcp.keepalive_sec).await?;
        let mut from_source = RecvStream::new(relay_data);
        let header = from_source
            .recv_object::<File>()
            .await?
            .context("missing file header")?;
        ensure!(
            header.dst == dst.join("a/file") && header.size == 4,
            "wrong data file"
        );
        let mut payload = Vec::new();
        from_source
            .copy_exact_to_buffered(&mut payload, header.size, 4)
            .await?;
        ensure!(
            payload == b"abcd",
            "source did not send full payload to the relay"
        );
        SendStream::new(&mut stream)
            .send_control_message(&header)
            .await?;
        // Recover the writer after the flushed header, then deliver only the first two bytes.
        stream.write_all(&payload[..2]).await?;
        trace.record("DataPrefix");
        b_waiting.notified().await;
        trace.record("ShortData");
        stream.shutdown().await?;
        // Keep the source data socket healthy until the real source has exited.
        Ok::<_, anyhow::Error>(from_source)
    };
    let (_source, _destination, _control, _data) = tokio::time::timeout(
        HANG,
        common::task_scope::scope_tasks(async {
            tokio::try_join!(source, destination, control_relay, data_relay)
        }),
    )
    .await
    .context("fatal teardown did not finish")??;
    let events = trace.0.lock().unwrap();
    assert_teardown_trace_order(&events);
    if let Some(path) = std::env::var_os("RCP_TEARDOWN_TRACE") {
        std::fs::write(path, events.join("\n") + "\n")?;
    }
    Ok(())
}

fn assert_teardown_trace_order(events: &[&str]) {
    assert_eq!(events.len(), 6, "{events:?}");
    let mut seen = std::collections::BTreeSet::new();
    for &event in events {
        assert!(
            !seen.contains(event),
            "duplicate event {event:?} in {events:?}"
        );
        let prereqs: &[&str] = match event {
            "DataPrefix" | "WaitB" => &[],
            "ShortData" => &["DataPrefix", "WaitB"],
            "ControlEOF" | "DestinationError" => &["ShortData"],
            "SourceError" => &["ControlEOF"],
            other => panic!("unexpected event {other:?}"),
        };
        for prereq in prereqs {
            assert!(
                seen.contains(prereq),
                "event {event:?} violated RemoteTeardown.tla prerequisite {prereq:?} in {events:?}"
            );
        }
        seen.insert(event);
    }
}

struct TurmoilTraceStream(turmoil::net::TcpStream);

impl tokio::io::AsyncRead for TurmoilTraceStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for TurmoilTraceStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl remote::transport::TransportStream for TurmoilTraceStream {
    fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.0.peer_addr()
    }

    fn into_split(self: Box<Self>) -> (BoxedRead, BoxedWrite) {
        let (read_half, write_half) = tokio::io::split(*self);
        (
            Box::new(read_half) as BoxedRead,
            Box::new(write_half) as BoxedWrite,
        )
    }
}

struct TurmoilTraceTransport;

impl remote::transport::Transport for TurmoilTraceTransport {
    fn bind_control<'a>(
        &'a self,
        _config: &'a remote::TcpConfig,
        _bind_ip: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, anyhow::Result<Box<dyn remote::transport::TransportListener>>>
    {
        Box::pin(async { anyhow::bail!("unused in relay test") })
    }

    fn bind_data<'a>(
        &'a self,
        _config: &'a remote::TcpConfig,
        _bind_ip: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, anyhow::Result<Box<dyn remote::transport::TransportListener>>>
    {
        Box::pin(async { anyhow::bail!("unused in relay test") })
    }

    fn connect_control<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        _config: &'a remote::TcpConfig,
    ) -> futures::future::BoxFuture<'a, anyhow::Result<remote::transport::BoxedTransportStream>>
    {
        Box::pin(async move {
            let stream = turmoil::net::TcpStream::connect(addr).await?;
            Ok(remote::transport::BoxedTransportStream::new(
                TurmoilTraceStream(stream),
            ))
        })
    }

    fn connect_control_with_timeout<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        config: &'a remote::TcpConfig,
        _timeout: std::time::Duration,
    ) -> futures::future::BoxFuture<'a, anyhow::Result<remote::transport::BoxedTransportStream>>
    {
        self.connect_control(addr, config)
    }

    fn connect_data<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        _profile: remote::NetworkProfile,
        _keepalive_sec: u64,
    ) -> futures::future::BoxFuture<'a, std::io::Result<remote::transport::BoxedTransportStream>>
    {
        Box::pin(async move {
            let stream = turmoil::net::TcpStream::connect(addr).await?;
            Ok(remote::transport::BoxedTransportStream::new(
                TurmoilTraceStream(stream),
            ))
        })
    }
}

#[test]
fn remote_teardown_trace_over_turmoil() -> turmoil::Result {
    let temp = tempfile::tempdir()?;
    let src = temp.path().join("src");
    let dst = temp.path().join("dst");
    for name in ["a", "b"] {
        std::fs::create_dir_all(src.join(name))?;
        std::fs::write(src.join(name).join("file"), b"abcd")?;
    }
    let trace = Trace::default();
    let b_waiting = Arc::new(Notify::new());
    let (addrs_tx, addrs_rx) =
        tokio::sync::oneshot::channel::<(std::net::SocketAddr, std::net::SocketAddr)>();
    let mut sim = turmoil::Builder::new()
        .simulation_duration(std::time::Duration::from_secs(30))
        .min_message_latency(std::time::Duration::from_millis(2))
        .max_message_latency(std::time::Duration::from_millis(8))
        .rng_seed(42)
        .build();
    let src_for_source = src.clone();
    let dst_for_source = dst.clone();
    let trace_for_source = trace.clone();
    let b_waiting_for_source = Arc::clone(&b_waiting);
    sim.client("source", async move {
        let control = turmoil::net::TcpListener::bind(("0.0.0.0", 0)).await?;
        let data = turmoil::net::TcpListener::bind(("0.0.0.0", 0)).await?;
        let host_ip = turmoil::lookup("source");
        let control_addr = std::net::SocketAddr::new(host_ip, control.local_addr()?.port());
        let data_addr = std::net::SocketAddr::new(host_ip, data.local_addr()?.port());
        addrs_tx
            .send((control_addr, data_addr))
            .map_err(|_| anyhow::anyhow!("destination dropped address channel"))?;
        let (source_control, relay_control) = tokio::io::duplex(64 * 1024);
        let (source_read, source_write) = tokio::io::split(source_control);
        let (mut relay_read, relay_write) = tokio::io::split(relay_control);
        let (source_data, relay_data) = tokio::io::duplex(64 * 1024);
        let (send, receive) = async_channel::bounded(1);
        send.try_send(SendStream::new(Box::new(source_data) as BoxedWrite))
            .expect("one free stream slot");
        let pool = Arc::new(super::super::super::AcceptingSendStreamPool {
            recv: receive,
            return_tx: send,
        });
        let hooks = Hooks::new({
            let waiting = Arc::clone(&b_waiting_for_source);
            let trace = trace_for_source.clone();
            let b_file = src_for_source.join("b/file");
            move |event, path, _| {
                if event == "wait_ready" && path == b_file {
                    trace.record("WaitB");
                    waiting.notify_one();
                }
                Box::pin(async { Ok(()) })
            }
        });
        let one = NonZeroUsize::MIN;
        let eight = NonZeroUsize::new(8).unwrap();
        let default_admission =
            common::EndpointAdmission::Remote(common::RemoteResources::for_limits(
                Some(std::num::NonZeroU64::new(1024).unwrap()),
                one,
                one,
                eight,
                0,
            )?);
        let configuration = settings(false);
        let limits = tokio::sync::oneshot::channel();
        let source = async {
            let (recv, limits) = limits
                .1
                .await
                .context("no directory limits from destination")?;
            let result = HOOKS
                .scope(
                    hooks,
                    run(
                        configuration.clone(),
                        Default::default(),
                        SrcDst {
                            src: src_for_source,
                            dst: dst_for_source.clone(),
                        },
                        Arc::new(tokio::sync::Mutex::new(SendStream::new(
                            Box::new(source_write) as BoxedWrite,
                        ))),
                        recv,
                        pool,
                        Arc::new(Fatal::new(Default::default())),
                        1,
                        8,
                        default_admission,
                        common::FilesInFlightSource::Automatic,
                        limits,
                        Arc::default(),
                    ),
                )
                .await;
            let error = result.expect_err("source falsely reported success after short data");
            ensure!(
                format!("{error:#}").contains("destination closed control before DestinationDone"),
                "source did not preserve the control EOF: {error:#}"
            );
            trace_for_source.record("SourceError");
            Ok::<_, anyhow::Error>(())
        };
        let trace_for_ctrl = trace_for_source.clone();
        let dst_for_ctrl = dst_for_source.clone();
        let control_relay = async move {
            let (stream, _) = control.accept().await?;
            let (read, mut write) = tokio::io::split(stream);
            let mut destination = RecvStream::new(read);
            let mut to_source = SendStream::new(relay_write);
            let advertised = destination
                .recv_object::<DirectoryLimits>()
                .await?
                .context("missing limits")?;
            limits
                .0
                .send((
                    RecvStream::new(Box::new(source_read) as BoxedRead),
                    advertised,
                ))
                .ok()
                .context("source dropped the limits channel")?;
            let uplink = async {
                tokio::io::copy(&mut relay_read, &mut write).await?;
                write.shutdown().await
            };
            let downlink = async {
                let mut held_b_ready = None;
                while let Some(message) = destination.recv_object::<DestinationMessage>().await? {
                    match &message {
                        DestinationMessage::DirectoryReady { dst: path, .. }
                            if path == &dst_for_ctrl.join("b") =>
                        {
                            ensure!(
                                held_b_ready.replace(message).is_none(),
                                "duplicate Ready for B"
                            );
                            continue;
                        }
                        DestinationMessage::DestinationDone => {
                            anyhow::bail!("destination sent Done after short data")
                        }
                        _ => {}
                    }
                    to_source.send_control_message(&message).await?;
                }
                trace_for_ctrl.record("ControlEOF");
                to_source.close().await?;
                Ok::<_, anyhow::Error>(())
            };
            tokio::try_join!(
                async { uplink.await.map_err(anyhow::Error::from) },
                downlink
            )?;
            Ok::<_, anyhow::Error>(())
        };
        let trace_for_data = trace_for_source.clone();
        let dst_for_data = dst_for_source.clone();
        let data_relay = async move {
            let (mut stream, _) = data.accept().await?;
            let mut from_source = RecvStream::new(relay_data);
            let header = from_source
                .recv_object::<File>()
                .await?
                .context("missing file header")?;
            ensure!(
                header.dst == dst_for_data.join("a/file") && header.size == 4,
                "wrong data file"
            );
            let mut payload = Vec::new();
            from_source
                .copy_exact_to_buffered(&mut payload, header.size, 4)
                .await?;
            ensure!(
                payload == b"abcd",
                "source did not send full payload to the relay"
            );
            SendStream::new(&mut stream)
                .send_control_message(&header)
                .await?;
            stream.write_all(&payload[..2]).await?;
            trace_for_data.record("DataPrefix");
            b_waiting_for_source.notified().await;
            trace_for_data.record("ShortData");
            stream.shutdown().await?;
            Ok::<_, anyhow::Error>(from_source)
        };
        Box::pin(common::task_scope::scope_progress(
            Arc::new(common::progress::Progress::new()),
            common::task_scope::scope_tasks(async {
                tokio::try_join!(source, control_relay, data_relay)
            }),
        ))
        .await?;
        Ok(())
    });
    let trace_for_dst = trace.clone();
    sim.client("destination", async move {
        let (control_addr, data_addr) = addrs_rx.await?;
        let one = NonZeroUsize::MIN;
        let eight = NonZeroUsize::new(8).unwrap();
        let default_admission =
            common::EndpointAdmission::Remote(common::RemoteResources::for_limits(
                Some(std::num::NonZeroU64::new(1024).unwrap()),
                one,
                one,
                eight,
                0,
            )?);
        let concurrency =
            remote::resolve_remote_concurrency(common::ConcurrencyLimit::Limited(one), one, eight)?;
        let (master_control, _unused_master) = remote::stdio::daemon_control_channel();
        let cleanup = remote::RemoteCleanup::new()?;
        let configuration = settings(false);
        let preserve = common::preserve::preserve_none();
        let tcp = remote::TcpConfig::default();
        let peer_connected = Arc::default();
        let error = Box::pin(common::task_scope::scope_progress(
            Arc::new(common::progress::Progress::new()),
            common::task_scope::scope_tasks(crate::destination::run_destination_with_transport(
                &control_addr,
                &[data_addr],
                "source",
                &configuration,
                false,
                0,
                &preserve,
                &tcp,
                concurrency,
                default_admission,
                None,
                &remote::protocol::PeerAuth::None,
                &DestinationTransport::Direct,
                &master_control,
                None,
                &cleanup,
                &peer_connected,
                Arc::new(TurmoilTraceTransport),
            )),
        ))
        .await
        .expect_err("destination falsely reported success after short data");
        cleanup.finish();
        let error = error.downcast::<common::copy::Error>()?;
        assert!(
            format!("{:#}", error.source).contains("unexpected EOF: expected 4 bytes, got 2"),
            "destination did not preserve the short body: {:#}",
            error.source
        );
        trace_for_dst.record("DestinationError");
        Ok(())
    });
    sim.run()?;
    let events = trace.0.lock().unwrap();
    assert_teardown_trace_order(&events);
    if let Some(path) = std::env::var_os("RCP_TURMOIL_TEARDOWN_TRACE") {
        std::fs::write(path, events.join("\n") + "\n")?;
    }
    Ok(())
}
