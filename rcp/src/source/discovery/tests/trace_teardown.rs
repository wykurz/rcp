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
    assert_eq!(events.len(), 6, "{events:?}");
    if let Some(path) = std::env::var_os("RCP_TEARDOWN_TRACE") {
        std::fs::write(path, events.join("\n") + "\n")?;
    }
    Ok(())
}
