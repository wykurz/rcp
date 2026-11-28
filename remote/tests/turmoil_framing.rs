//! Exercise the real stream codecs over simulated TCP, without daemon or filesystem setup.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use remote::protocol::{DestinationMessage, File, Metadata, PeerAuth, SshAuthSecret, WireAcls};
use remote::streams::{BoxedRead, BoxedWrite, RecvStream, SendStream};
use remote::tls::{
    ControlStreamAcceptor, SERVER_NAME_SOURCE, StreamAcceptor, StreamConnector, StreamKind,
    accept_bounded, accept_control_bounded, connect_bounded, generate_self_signed_cert,
};
use remote::transport::{BoxedTransportStream, TransportStream};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::Notify;
use turmoil::net::{TcpListener, TcpStream};

struct TurmoilStream {
    inner: TcpStream,
    peer_ip_override: Option<std::net::IpAddr>,
}

impl TurmoilStream {
    fn direct(inner: TcpStream) -> BoxedTransportStream {
        BoxedTransportStream::new(Self {
            inner,
            peer_ip_override: None,
        })
    }

    fn simulated_loopback(inner: TcpStream) -> BoxedTransportStream {
        BoxedTransportStream::new(Self {
            inner,
            peer_ip_override: Some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        })
    }
}

impl AsyncRead for TurmoilStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TurmoilStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
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

fn install_crypto_provider() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
}

fn file(name: &str, size: u64) -> File {
    File {
        src: format!("/src/{name}").into(),
        dst: format!("/dst/{name}").into(),
        size,
        metadata: Metadata {
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            atime_nsec: 0,
            mtime_nsec: 0,
            acls: WireAcls::Unknown,
        },
        is_root: false,
    }
}

#[test]
fn held_payload_resumes_at_next_file_boundary() -> turmoil::Result {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(2))
        .build();

    // Order fault injection explicitly: the source queues the tail only after the link is held.
    let send_tail = Arc::new(Notify::new());
    let source_gate = Arc::clone(&send_tail);
    sim.client("source", async move {
        let listener = TcpListener::bind(("0.0.0.0", 1234)).await?;
        let (mut data, _) = listener.accept().await?;
        SendStream::new(&mut data)
            .send_batch_message(&file("first", 4))
            .await?;
        data.write_all(b"ab").await?;
        source_gate.notified().await;
        data.write_all(b"cd").await?;
        SendStream::new(&mut data)
            .send_batch_message(&file("second", 1))
            .await?;
        data.write_all(b"e").await?;
        data.shutdown().await?;
        Ok(())
    });

    sim.client("destination", async move {
        let mut data = RecvStream::new(TcpStream::connect(("source", 1234)).await?);
        let first = data.recv_object::<File>().await?.expect("first header");
        assert_eq!(first.dst, std::path::Path::new("/dst/first"));
        let mut output = Vec::new();
        {
            let mut body = std::pin::pin!(data.copy_exact_to_buffered(&mut output, first.size, 2));
            turmoil::hold("source", "destination");
            send_tail.notify_one();
            assert!(
                tokio::time::timeout(Duration::from_millis(15), body.as_mut())
                    .await
                    .is_err(),
                "the file must not complete while its tail is held"
            );
            turmoil::release("source", "destination");
            assert_eq!(body.await?, first.size);
        }
        assert_eq!(output, b"abcd");

        let second = data.recv_object::<File>().await?.expect("second header");
        assert_eq!(second.dst, std::path::Path::new("/dst/second"));
        output.clear();
        assert_eq!(
            data.copy_exact_to_buffered(&mut output, second.size, 2)
                .await?,
            second.size
        );
        assert_eq!(output, b"e");
        assert!(data.recv_object::<File>().await?.is_none());
        Ok(())
    });

    sim.run()
}

#[test]
fn truncated_data_does_not_close_the_control_stream() -> turmoil::Result {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(2))
        .build();

    sim.client("source", async {
        let data_listener = TcpListener::bind(("0.0.0.0", 1234)).await?;
        let control_listener = TcpListener::bind(("0.0.0.0", 1235)).await?;
        let (mut data, _) = data_listener.accept().await?;
        let (control, _) = control_listener.accept().await?;
        SendStream::new(&mut data)
            .send_batch_message(&file("first", 4))
            .await?;
        data.write_all(b"ab").await?;
        data.shutdown().await?;

        let mut control = RecvStream::new(control);
        assert!(matches!(
            control.recv_object::<DestinationMessage>().await?,
            Some(DestinationMessage::DirectoryReady { dst, .. })
                if dst == std::path::Path::new("/dst/other")
        ));
        Ok(())
    });

    sim.client("destination", async {
        let mut data = RecvStream::new(TcpStream::connect(("source", 1234)).await?);
        let mut control = SendStream::new(TcpStream::connect(("source", 1235)).await?);
        let header = data.recv_object::<File>().await?.expect("file header");
        let mut output = Vec::new();
        let error = data
            .copy_exact_to_buffered(&mut output, header.size, 2)
            .await
            .expect_err("clean TCP EOF halfway through the declared body must be rejected");
        assert!(format!("{error:#}").contains("unexpected EOF: expected 4 bytes, got 2"));
        assert_eq!(output, b"ab");

        // Only the data socket was closed; prove this was not an accidental link-wide fault.
        control
            .send_control_message(&DestinationMessage::DirectoryReady {
                src: "/src/other".into(),
                dst: "/dst/other".into(),
            })
            .await?;
        Ok(())
    });

    sim.run()
}

#[test]
fn mtls_auto_handshake_round_trips_and_stalled_handshake_times_out_in_virtual_time()
-> turmoil::Result {
    install_crypto_provider();
    let wall_start = Instant::now();
    let src_cert = Arc::new(generate_self_signed_cert().unwrap());
    let dst_cert = Arc::new(generate_self_signed_cert().unwrap());
    let secret = SshAuthSecret::generate();
    let src_auth = PeerAuth::AutoTlsOrSshSecret {
        peer_fingerprint: dst_cert.fingerprint,
        secret: secret.clone(),
    };
    let dst_auth = PeerAuth::AutoTlsOrSshSecret {
        peer_fingerprint: src_cert.fingerprint,
        secret,
    };
    let stall_accepted = Arc::new(Notify::new());
    let stall_done = Arc::new(Notify::new());
    let prefix_done = Arc::new(Notify::new());
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(90))
        .rng_seed(7)
        .build();
    let src_cert_clone = Arc::clone(&src_cert);
    let stall_accepted_src = Arc::clone(&stall_accepted);
    let stall_done_src = Arc::clone(&stall_done);
    let prefix_done_src = Arc::clone(&prefix_done);
    sim.client("source", async move {
        let control_listener = TcpListener::bind(("0.0.0.0", 2001)).await?;
        let data_listener = TcpListener::bind(("0.0.0.0", 2002)).await?;
        let control_acceptor =
            ControlStreamAcceptor::from_peer_auth(Some(&src_cert_clone), &src_auth)?;
        // 1a. first connection has its ClientHello held in flight before byte 0: a 15s handshake
        //     timeout must fire in virtual time without wedging subsequent connections
        let (stalled, _) = control_listener.accept().await?;
        stall_accepted_src.notify_one();
        let sim_start = tokio::time::Instant::now();
        let err = match accept_control_bounded(
            &control_acceptor,
            TurmoilStream::direct(stalled),
            Duration::from_secs(15),
            "control",
        )
        .await
        {
            Ok(_) => panic!("held client handshake must time out"),
            Err(err) => err,
        };
        assert!(
            sim_start.elapsed() >= Duration::from_secs(15),
            "expected at least 15s of virtual time to elapse, got {:?}",
            sim_start.elapsed()
        );
        assert!(
            format!("{err:#}").contains("timed out"),
            "expected handshake timeout error, got: {err:#}"
        );
        stall_done_src.notify_one();
        // 1b. second connection delivers the 0x16 TLS record byte (entering PrefixedStream +
        //     accept_tls_stream) and then stalls: the same 15s bound must still fire in virtual time
        let (stalled_after_prefix, _) = control_listener.accept().await?;
        let sim_start = tokio::time::Instant::now();
        let err = match accept_control_bounded(
            &control_acceptor,
            TurmoilStream::direct(stalled_after_prefix),
            Duration::from_secs(15),
            "control",
        )
        .await
        {
            Ok(_) => panic!("post-prefix stalled TLS handshake must time out"),
            Err(err) => err,
        };
        assert!(
            sim_start.elapsed() >= Duration::from_secs(15),
            "expected at least 15s of virtual time to elapse after 0x16 prefix, got {:?}",
            sim_start.elapsed()
        );
        assert!(
            format!("{err:#}").contains("timed out"),
            "expected post-prefix TLS handshake timeout error, got: {err:#}"
        );
        prefix_done_src.notify_one();
        // 2. third connection completes mTLS 1.3 via Auto first-byte dispatch (PrefixedStream)
        //    and locks subsequent data streams to mTLS
        let (control_stream, _) = control_listener.accept().await?;
        let (mut ctrl_send, mut ctrl_recv, data_acceptor) = accept_control_bounded(
            &control_acceptor,
            TurmoilStream::direct(control_stream),
            Duration::from_secs(15),
            "control",
        )
        .await?;
        assert!(
            matches!(data_acceptor, StreamAcceptor::Tls(_)),
            "Auto control mTLS handshake must lock data acceptor to Tls"
        );
        let msg = ctrl_recv.recv_object::<DestinationMessage>().await?;
        assert!(matches!(
            msg,
            Some(DestinationMessage::DirectoryReady { .. })
        ));
        ctrl_send.send_batch_message(&file("ctrl", 0)).await?;
        let (data_stream, _) = data_listener.accept().await?;
        let (mut data_send, _) = accept_bounded(
            &data_acceptor,
            TurmoilStream::direct(data_stream),
            Duration::from_secs(15),
            StreamKind::Data,
            "data",
        )
        .await?;
        let guard = throttle::open_file_permit().await;
        let sent = data_send
            .send_message_with_file(
                &file("payload", 5),
                std::io::Cursor::new(b"hello".to_vec()),
                5,
                8,
                &guard,
            )
            .await?;
        assert_eq!(sent, 5);
        data_send.close().await?;
        Ok(())
    });
    sim.client("destination", async move {
        let connector = StreamConnector::from_peer_auth(Some(&dst_cert), &dst_auth, false)?;
        // 1a. establish TCP, hold destination -> source, then start a real ClientHello so
        //     turmoil::hold genuinely stalls handshake packets in flight until source times out
        let stalled = TcpStream::connect(("source", 2001)).await?;
        stall_accepted.notified().await;
        turmoil::hold("destination", "source");
        let stalled_connect = connect_bounded(
            &connector,
            SERVER_NAME_SOURCE,
            TurmoilStream::direct(stalled),
            Duration::from_secs(30),
            StreamKind::Control,
            "control",
        );
        tokio::select! {
            _ = stall_done.notified() => {}
            res = stalled_connect => {
                panic!("held client handshake unexpectedly completed: {res:?}");
            }
        }
        turmoil::release("destination", "source");
        // 1b. send only the 0x16 TLS handshake record byte so source enters PrefixedStream +
        //     accept_tls_stream, then hold the link so the rest of the ClientHello never arrives
        let mut stalled_after_prefix = TcpStream::connect(("source", 2001)).await?;
        stalled_after_prefix.write_all(&[0x16]).await?;
        stalled_after_prefix.flush().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        turmoil::hold("destination", "source");
        let _ = stalled_after_prefix
            .write_all(&[0x03, 0x01, 0x00, 0x05])
            .await;
        prefix_done.notified().await;
        drop(stalled_after_prefix);
        turmoil::release("destination", "source");
        // 2. connect cleanly over Auto -> mTLS 1.3 on control and data
        let control_stream = TcpStream::connect(("source", 2001)).await?;
        let (mut ctrl_send, mut ctrl_recv) = connect_bounded(
            &connector,
            SERVER_NAME_SOURCE,
            TurmoilStream::direct(control_stream),
            Duration::from_secs(15),
            StreamKind::Control,
            "control",
        )
        .await?;
        ctrl_send
            .send_control_message(&DestinationMessage::DirectoryReady {
                src: "/src/dir".into(),
                dst: "/dst/dir".into(),
            })
            .await?;
        let ack = ctrl_recv.recv_object::<File>().await?.expect("control ack");
        assert_eq!(ack.dst, std::path::Path::new("/dst/ctrl"));
        let data_stream = TcpStream::connect(("source", 2002)).await?;
        let (_, mut data_recv) = connect_bounded(
            &connector,
            SERVER_NAME_SOURCE,
            TurmoilStream::direct(data_stream),
            Duration::from_secs(15),
            StreamKind::Data,
            "data",
        )
        .await?;
        let header = data_recv.recv_object::<File>().await?.expect("data header");
        let mut out = Vec::new();
        data_recv
            .copy_exact_to_buffered(&mut out, header.size, 8)
            .await?;
        assert_eq!(out, b"hello");
        Ok(())
    });
    sim.run()?;
    assert!(
        wall_start.elapsed() < Duration::from_secs(10),
        "30s of virtual handshake timeouts should complete well under 10s wall time, took {:?}",
        wall_start.elapsed()
    );
    Ok(())
}

#[test]
fn auto_control_rejects_non_loopback_ssh_auth_and_accepts_simulated_loopback() -> turmoil::Result {
    install_crypto_provider();
    let src_cert = Arc::new(generate_self_signed_cert().unwrap());
    let dst_cert = Arc::new(generate_self_signed_cert().unwrap());
    let secret = SshAuthSecret::generate();
    let src_auth = PeerAuth::AutoTlsOrSshSecret {
        peer_fingerprint: dst_cert.fingerprint,
        secret: secret.clone(),
    };
    let dst_auth = PeerAuth::AutoTlsOrSshSecret {
        peer_fingerprint: src_cert.fingerprint,
        secret,
    };
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(30))
        .rng_seed(11)
        .build();
    let src_cert_clone = Arc::clone(&src_cert);
    sim.client("source", async move {
        let control_listener = TcpListener::bind(("0.0.0.0", 2101)).await?;
        let data_listener = TcpListener::bind(("0.0.0.0", 2102)).await?;
        let control_acceptor =
            ControlStreamAcceptor::from_peer_auth(Some(&src_cert_clone), &src_auth)?;
        // 1a. direct non-loopback Turmoil IP (192.168.0.x) attempting SSH HMAC must be refused
        let (non_loopback, _) = control_listener.accept().await?;
        let err = match accept_control_bounded(
            &control_acceptor,
            TurmoilStream::direct(non_loopback),
            Duration::from_secs(5),
            "control",
        )
        .await
        {
            Ok(_) => panic!("non-loopback SSH auth must fail closed"),
            Err(err) => err,
        };
        assert!(
            format!("{err:#}").contains("non-loopback peer"),
            "unexpected non-loopback rejection error: {err:#}"
        );
        // 1b. when destination enforces client-side non-loopback rejection, it closes before
        //     sending any SSH HMAC bytes
        let (client_rejected, _) = control_listener.accept().await?;
        let err = match accept_control_bounded(
            &control_acceptor,
            TurmoilStream::direct(client_rejected),
            Duration::from_secs(5),
            "control",
        )
        .await
        {
            Ok(_) => panic!("aborted client-side non-loopback connection must not succeed"),
            Err(err) => err,
        };
        assert!(
            format!("{err:#}").contains("before starting handshake"),
            "expected EOF before handshake after client-side rejection, got: {err:#}"
        );
        // 2. simulated SSH loopback peer dispatches Auto -> SshAuth on control and data
        let (loopback_ctrl, _) = control_listener.accept().await?;
        let (mut ctrl_send, mut ctrl_recv, data_acceptor) = accept_control_bounded(
            &control_acceptor,
            TurmoilStream::simulated_loopback(loopback_ctrl),
            Duration::from_secs(5),
            "control",
        )
        .await?;
        assert!(
            matches!(data_acceptor, StreamAcceptor::SshAuth(_)),
            "Auto control SSH-HMAC handshake must lock data acceptor to SshAuth"
        );
        assert!(matches!(
            ctrl_recv.recv_object::<DestinationMessage>().await?,
            Some(DestinationMessage::DirectoryReady { .. })
        ));
        ctrl_send.send_batch_message(&file("hmac-ok", 0)).await?;
        let (loopback_data, _) = data_listener.accept().await?;
        let (mut data_send, _) = accept_bounded(
            &data_acceptor,
            TurmoilStream::simulated_loopback(loopback_data),
            Duration::from_secs(5),
            StreamKind::Data,
            "data",
        )
        .await?;
        let guard = throttle::open_file_permit().await;
        let sent = data_send
            .send_message_with_file(
                &file("hmac-data", 4),
                std::io::Cursor::new(b"hmac".to_vec()),
                4,
                8,
                &guard,
            )
            .await?;
        assert_eq!(sent, 4);
        data_send.close().await?;
        Ok(())
    });
    sim.client("destination", async move {
        // 1a. send SSH auth magic over real non-loopback Turmoil link: source must refuse it and
        //     close the socket before replying
        let mut raw = TcpStream::connect(("source", 2101)).await?;
        raw.write_all(b"RCPA").await?;
        raw.write_all(&[0u8; 32]).await?;
        let mut reply = [0u8; 1];
        match raw.read(&mut reply).await {
            Ok(0) | Err(_) => {}
            Ok(n) => panic!("source unexpectedly replied with {n} bytes to non-loopback SSH auth"),
        }
        drop(raw);
        // 1b. client-side SshAuth connector must also refuse a non-loopback peer before writing
        let connector = StreamConnector::from_peer_auth(Some(&dst_cert), &dst_auth, true)?;
        let direct_ctrl = TcpStream::connect(("source", 2101)).await?;
        let err = match connect_bounded(
            &connector,
            SERVER_NAME_SOURCE,
            TurmoilStream::direct(direct_ctrl),
            Duration::from_secs(5),
            StreamKind::Control,
            "control",
        )
        .await
        {
            Ok(_) => panic!("client SshAuth connector must refuse non-loopback peer"),
            Err(err) => err,
        };
        assert!(
            format!("{err:#}").contains("non-loopback peer"),
            "unexpected client-side non-loopback rejection error: {err:#}"
        );
        // 2. connect using via_ssh_tunnel = true over simulated loopback peer address
        let ctrl_stream = TcpStream::connect(("source", 2101)).await?;
        let (mut ctrl_send, mut ctrl_recv) = connect_bounded(
            &connector,
            SERVER_NAME_SOURCE,
            TurmoilStream::simulated_loopback(ctrl_stream),
            Duration::from_secs(5),
            StreamKind::Control,
            "control",
        )
        .await?;
        ctrl_send
            .send_control_message(&DestinationMessage::DirectoryReady {
                src: "/src/hmac".into(),
                dst: "/dst/hmac".into(),
            })
            .await?;
        let ack = ctrl_recv.recv_object::<File>().await?.expect("hmac ack");
        assert_eq!(ack.dst, std::path::Path::new("/dst/hmac-ok"));
        let data_stream = TcpStream::connect(("source", 2102)).await?;
        let (_, mut data_recv) = connect_bounded(
            &connector,
            SERVER_NAME_SOURCE,
            TurmoilStream::simulated_loopback(data_stream),
            Duration::from_secs(5),
            StreamKind::Data,
            "data",
        )
        .await?;
        let header = data_recv.recv_object::<File>().await?.expect("hmac data");
        let mut out = Vec::new();
        data_recv
            .copy_exact_to_buffered(&mut out, header.size, 8)
            .await?;
        assert_eq!(out, b"hmac");
        Ok(())
    });
    sim.run()
}
