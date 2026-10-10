use super::*;
use tokio::io::AsyncReadExt as _;

struct HeaderMutation {
    stream: tokio::io::DuplexStream,
    mutation: Option<Box<dyn FnOnce() -> std::io::Result<()> + Send>>,
}

impl tokio::io::AsyncWrite for HeaderMutation {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.stream).poll_write(context, bytes)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // the first flush belongs to the file header, after the actual data fd was opened
        if let Some(mutation) = self.mutation.take() {
            mutation()?;
        }
        std::pin::Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // failure cleanup must discard a stream without awaiting its shutdown
        std::task::Poll::Pending
    }
}

fn settings(dereference: bool) -> common::copy::Settings {
    common::copy::Settings {
        reflink: Default::default(),
        local_copy_handoff: Default::default(),
        dereference,
        fail_early: false,
        overwrite: false,
        overwrite_compare: Default::default(),
        overwrite_filter: None,
        ignore_existing: false,
        chunk_size: 0,
        skip_specials: false,
        remote_copy_buffer_size: 4096,
        filter: None,
        dry_run: None,
        delete: None,
    }
}

#[tokio::test]
async fn dry_run_requires_destination_done() -> anyhow::Result<()> {
    for acknowledge in [false, true] {
        let source = tempfile::tempdir()?;
        let destination = source.path().join("preview");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let tcp_config = remote::TcpConfig::default();
        let (accepted, peer) = tokio::try_join!(
            async {
                remote::accept_tcp_control(&listener, &tcp_config)
                    .await
                    .map_err(anyhow::Error::from)
            },
            remote::connect_tcp_control(listener.local_addr()?, &tcp_config),
        )?;
        let settings = settings(false);
        let source_operation = handle_dry_run_connection(
            accepted.0,
            &settings,
            source.path(),
            &destination,
            common::config::DryRunMode::Brief,
            None,
            5,
        );
        let destination_operation = async {
            let (mut send, mut receive) = remote::tls::connect_bounded(
                None,
                remote::tls::SERVER_NAME_SOURCE,
                peer,
                std::time::Duration::from_secs(5),
                "dry-run test control",
            )
            .await?;
            assert!(matches!(
                receive
                    .recv_object::<remote::protocol::SourceMessage>()
                    .await?,
                Some(remote::protocol::SourceMessage::DiscoveryComplete {
                    has_root_item: false
                })
            ));
            if acknowledge {
                send.send_control_message(&remote::protocol::DestinationMessage::DestinationDone)
                    .await?;
            }
            send.close().await?;
            anyhow::Ok(())
        };
        let (result, peer_result) =
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::join!(source_operation, destination_operation)
            })
            .await?;
        peer_result?;
        if acknowledge {
            result?;
        } else {
            let error = result.expect_err("control EOF cannot acknowledge a dry run");
            assert!(
                format!("{error:#}").contains("DestinationDone"),
                "{error:#}"
            );
        }
        assert!(!destination.exists());
    }
    Ok(())
}

fn pool(writer: remote::streams::BoxedWrite) -> Arc<AcceptingSendStreamPool> {
    let (send, receive) = async_channel::bounded(1);
    send.try_send(remote::streams::SendStream::new(writer))
        .unwrap();
    Arc::new(AcceptingSendStreamPool {
        recv: receive,
        return_tx: send,
    })
}

async fn send(
    pool: Arc<AcceptingSendStreamPool>,
    path: &std::path::Path,
    settings: &common::copy::Settings,
) -> anyhow::Result<()> {
    let read = if settings.dereference {
        FileRead::Path
    } else {
        let (parent, name) = open_root_parent(path).await?;
        FileRead::Hardened(parent, name)
    };
    let fatal = discovery::Fatal::new(Default::default());
    let result = send_file_tcp(
        settings,
        Default::default(),
        path,
        std::path::Path::new("/destination/file"),
        0,
        true,
        pool,
        &Default::default(),
        Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
        ))),
        read,
        &fatal,
    )
    .await;
    match fatal.take() {
        Some(error) => Err(error),
        None => result,
    }
}

struct PayloadRequests {
    header_flushed: bool,
    sizes: Arc<std::sync::Mutex<Vec<usize>>>,
}

impl tokio::io::AsyncWrite for PayloadRequests {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.header_flushed {
            self.sizes.lock().unwrap().push(bytes.len());
        }
        std::task::Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.header_flushed = true;
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn source_adapters_use_configured_file_read_limits() -> anyhow::Result<()> {
    static PROGRESS: std::sync::LazyLock<common::progress::Progress> =
        std::sync::LazyLock::new(common::progress::Progress::new);
    for configured in [
        remote::TcpConfig::default().effective_buffer_size(),
        32 * 1024 * 1024,
    ] {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("larger-than-chunk");
        let size = configured + 17;
        common::filegen::write_file(&PROGRESS, path.clone(), size, 65536, 0).await?;
        for dereference in [false, true] {
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let pool = pool(Box::new(PayloadRequests {
                header_flushed: false,
                sizes: requests.clone(),
            }));
            let settings = common::copy::Settings {
                remote_copy_buffer_size: configured,
                ..settings(dereference)
            };
            send(pool.clone(), &path, &settings).await?;
            let requests = requests.lock().unwrap();
            assert_eq!(requests.iter().sum::<usize>(), size);
            assert_eq!(requests.iter().copied().max(), Some(configured));
            assert_eq!(pool.recv.len(), 1);
        }
    }
    Ok(())
}

#[tokio::test]
async fn shrinking_file_discards_its_incomplete_data_frame() -> anyhow::Result<()> {
    static PROGRESS: std::sync::LazyLock<common::progress::Progress> =
        std::sync::LazyLock::new(common::progress::Progress::new);
    let replacement = b"new";
    let file_size = remote::TcpConfig::default().effective_buffer_size() + 1;
    for dereference in [false, true] {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("changing-file");
        common::filegen::write_file(&PROGRESS, path.clone(), file_size, 65536, 0).await?;
        let settings = common::copy::Settings {
            remote_copy_buffer_size: remote::TcpConfig::default().effective_buffer_size(),
            ..settings(dereference)
        };
        let (writer, mut reader) = tokio::io::duplex(4096);
        let pool = pool(Box::new(HeaderMutation {
            stream: writer,
            mutation: Some(Box::new({
                let path = path.clone();
                move || std::fs::write(path, replacement)
            })),
        }));
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            send(pool.clone(), &path, &settings),
        )
        .await?
        .expect_err("a changed payload was reported as a successful file transfer");
        assert!(format!("{error:#}").contains("changing-file"), "{error:#}");
        assert_eq!(
            pool.recv.len(),
            0,
            "a mismatched frame was returned to the pool"
        );
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await?;
        let header_size = u32::from_be_bytes(bytes[..4].try_into()?) as usize;
        let header = remote::streams::RecvStream::new(bytes.as_slice())
            .recv_object::<remote::protocol::File>()
            .await?
            .unwrap();
        assert_eq!(header.size, file_size as u64);
        let payload = &bytes[4 + header_size..];
        assert!(
            payload.len() <= file_size,
            "bytes beyond the header size reached the wire"
        );
        assert_eq!(payload, replacement);
        assert_eq!(
            error
                .root_cause()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::UnexpectedEof
        );
    }
    Ok(())
}

#[tokio::test]
async fn growing_file_sends_its_snapshot_length_and_reuses_the_stream() -> anyhow::Result<()> {
    for dereference in [false, true] {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("growing-file");
        std::fs::write(&path, b"original")?;
        let next = temp.path().join("next-file");
        std::fs::write(&next, b"next")?;
        let (writer, reader) = tokio::io::duplex(4096);
        let pool = pool(Box::new(HeaderMutation {
            stream: writer,
            mutation: Some(Box::new({
                let path = path.clone();
                move || std::fs::write(path, b"replacement grows")
            })),
        }));
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            send(pool.clone(), &path, &settings(dereference)),
        )
        .await??;
        assert_eq!(pool.recv.len(), 1);
        send(pool.clone(), &next, &settings(dereference)).await?;
        let mut receive = remote::streams::RecvStream::new(reader);
        for (path, expected) in [(&path, b"replacem".as_slice()), (&next, b"next")] {
            let header = receive
                .recv_object::<remote::protocol::File>()
                .await?
                .unwrap();
            assert_eq!(&header.src, path);
            assert_eq!(header.size, expected.len() as u64);
            let mut bytes = Vec::new();
            receive
                .copy_exact_to_buffered(&mut bytes, header.size, 32)
                .await?;
            assert_eq!(bytes, expected);
        }
    }
    Ok(())
}

#[tokio::test]
async fn size_zero_proc_file_sends_an_empty_frame_and_reuses_the_stream() -> anyhow::Result<()> {
    let path = std::path::Path::new("/proc/self/status");
    assert_eq!(std::fs::metadata(path)?.len(), 0);
    for dereference in [false, true] {
        let temp = tempfile::tempdir()?;
        let next = temp.path().join("next-file");
        std::fs::write(&next, b"next")?;
        let (writer, reader) = tokio::io::duplex(16384);
        let pool = pool(Box::new(writer));
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            send(pool.clone(), path, &settings(dereference)),
        )
        .await??;
        assert_eq!(pool.recv.len(), 1);
        send(pool.clone(), &next, &settings(dereference)).await?;
        let mut receive = remote::streams::RecvStream::new(reader);
        let empty = receive
            .recv_object::<remote::protocol::File>()
            .await?
            .unwrap();
        assert_eq!(empty.src, path);
        assert_eq!(empty.size, 0);
        let following = receive
            .recv_object::<remote::protocol::File>()
            .await?
            .unwrap();
        assert_eq!(following.src, next);
        assert_eq!(following.size, 4);
        let mut bytes = Vec::new();
        receive.copy_exact_to_buffered(&mut bytes, 4, 32).await?;
        assert_eq!(bytes, b"next");
    }
    Ok(())
}

#[tokio::test]
async fn unchanged_files_reuse_one_stream_with_exact_frame_boundaries() -> anyhow::Result<()> {
    for dereference in [false, true] {
        let temp = tempfile::tempdir()?;
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        std::fs::write(&first, b"first payload")?;
        std::fs::write(&second, b"second")?;
        let (writer, reader) = tokio::io::duplex(4096);
        let pool = pool(Box::new(writer));
        send(pool.clone(), &first, &settings(dereference)).await?;
        assert_eq!(pool.recv.len(), 1);
        send(pool.clone(), &second, &settings(dereference)).await?;
        assert_eq!(pool.recv.len(), 1);
        let mut receive = remote::streams::RecvStream::new(reader);
        for (path, expected) in [(&first, b"first payload".as_slice()), (&second, b"second")] {
            let header = receive
                .recv_object::<remote::protocol::File>()
                .await?
                .unwrap();
            assert_eq!(&header.src, path);
            assert_eq!(header.size, expected.len() as u64);
            let mut bytes = Vec::new();
            receive
                .copy_exact_to_buffered(&mut bytes, header.size, 32)
                .await?;
            assert_eq!(bytes, expected);
        }
    }
    Ok(())
}

struct CloseControlOnDrop {
    close_peer: Option<tokio::sync::oneshot::Sender<()>>,
    published: std::sync::mpsc::Receiver<()>,
    panic_on_write: bool,
}

impl tokio::io::AsyncWrite for CloseControlOnDrop {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        assert!(!self.panic_on_write, "original payload writer panic");
        std::task::Poll::Ready(Err(std::io::Error::from_raw_os_error(libc::EIO)))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Pending
    }
}

impl Drop for CloseControlOnDrop {
    fn drop(&mut self) {
        if let Some(close_peer) = self.close_peer.take() {
            let _ = close_peer.send(());
            // another runtime worker processes the peer close before this destructor returns.
            // a timeout panics the test instead of permitting a timing-dependent false pass.
            self.published
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("stream destruction waited without a published fatal cause");
        }
    }
}

async fn payload_failure_precedes_stream_drop(panic_on_write: bool) -> anyhow::Result<()> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("file");
    std::fs::write(&path, b"payload")?;
    let (close_peer, close_requested) = tokio::sync::oneshot::channel();
    let (published, observed) = std::sync::mpsc::channel();
    let pool = pool(Box::new(CloseControlOnDrop {
        close_peer: Some(close_peer),
        published: observed,
        panic_on_write,
    }));
    let (source_control, peer_control) = tokio::io::duplex(4096);
    let (read, write) = tokio::io::split(source_control);
    let shutdown = PoolShutdownToken::new();
    let fatal = Arc::new(discovery::Fatal::new(shutdown.clone()));
    let source = discovery::run(
        settings(false),
        Default::default(),
        remote::protocol::SrcDst {
            src: path.clone(),
            dst: "/destination/file".into(),
        },
        Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(write) as remote::streams::BoxedWrite,
        ))),
        remote::streams::RecvStream::new(Box::new(read) as remote::streams::BoxedRead),
        pool,
        fatal,
        1,
        1,
        common::EndpointAdmission::Disabled,
        common::FilesInFlightSource::Automatic,
        remote::protocol::DirectoryLimits {
            normal: std::num::NonZeroUsize::new(2).unwrap(),
            reserve: std::num::NonZeroUsize::MIN,
        },
        Default::default(),
    );
    let peer = async move {
        close_requested.await?;
        drop(peer_control);
        shutdown.cancelled().await;
        published.send(())?;
        anyhow::Ok(())
    };
    let (source, peer) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        common::task_scope::scope_tasks(async { tokio::join!(source, peer) }),
    )
    .await?;
    peer?;
    let error = source.expect_err("payload failure must fail the source");
    if panic_on_write {
        assert!(
            format!("{error:#}").contains("original payload writer panic"),
            "{error:#}"
        );
    } else {
        assert_eq!(
            error
                .root_cause()
                .downcast_ref::<std::io::Error>()
                .and_then(std::io::Error::raw_os_error),
            Some(libc::EIO),
            "the control close replaced the original payload error: {error:#}"
        );
        assert!(format!("{error:#}").contains(path.to_str().unwrap()));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payload_error_is_published_before_stream_drop_closes_control() -> anyhow::Result<()> {
    payload_failure_precedes_stream_drop(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payload_panic_is_published_before_stream_drop_closes_control() -> anyhow::Result<()> {
    payload_failure_precedes_stream_drop(true).await
}

#[tokio::test]
async fn completion_panic_is_published_before_the_poisoned_stream_drops() -> anyhow::Result<()> {
    use tracing::instrument::WithSubscriber as _;

    struct PanicAtSendFinish;
    impl tracing::Subscriber for PanicAtSendFinish {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, attributes: &tracing::span::Attributes<'_>) -> tracing::Id {
            tracing::Id::from_u64(if attributes.metadata().name() == "source.file.send" {
                1
            } else {
                2
            })
        }
        fn record(&self, span: &tracing::Id, _: &tracing::span::Record<'_>) {
            assert_ne!(span.into_u64(), 1, "original send completion panic");
        }
        fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {}
        fn enter(&self, _: &tracing::Id) {}
        fn exit(&self, _: &tracing::Id) {}
    }
    struct ObserveDrop {
        published: PoolShutdownToken,
        observed: Arc<std::sync::atomic::AtomicBool>,
    }
    impl tokio::io::AsyncWrite for ObserveDrop {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
    impl Drop for ObserveDrop {
        fn drop(&mut self) {
            self.observed.store(
                self.published.is_cancelled(),
                std::sync::atomic::Ordering::SeqCst,
            );
        }
    }
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("file");
    std::fs::write(&path, b"payload")?;
    let published = PoolShutdownToken::new();
    let observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fatal = discovery::Fatal::new(published.clone());
    let pool = pool(Box::new(ObserveDrop {
        published,
        observed: observed.clone(),
    }));
    let (parent, name) = open_root_parent(&path).await?;
    let settings = settings(false);
    let errors = Default::default();
    let send = send_file_tcp(
        &settings,
        Default::default(),
        &path,
        std::path::Path::new("/destination/file"),
        0,
        true,
        pool,
        &errors,
        Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
        ))),
        FileRead::Hardened(parent, name),
        &fatal,
    );
    assert!(
        discovery::supervise(&fatal, send)
            .with_subscriber(PanicAtSendFinish)
            .await
            .is_err()
    );
    let error = fatal.take().expect("completion panic was not published");
    assert!(
        format!("{error:#}").contains("original send completion panic"),
        "{error:#}"
    );
    assert!(
        observed.load(std::sync::atomic::Ordering::SeqCst),
        "stream dropped before completion panic was published"
    );
    Ok(())
}
