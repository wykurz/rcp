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
    dereference: bool,
) -> anyhow::Result<()> {
    let read = if dereference {
        FileRead::Path
    } else {
        let (parent, name) = open_root_parent(path).await?;
        FileRead::Hardened(parent, name)
    };
    let fatal = discovery::Fatal::new(Default::default());
    let result = send_file_tcp(
        &settings(dereference),
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

async fn rejects_changed_payload(replacement: &'static [u8]) -> anyhow::Result<()> {
    for dereference in [false, true] {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("changing-file");
        std::fs::write(&path, b"original")?;
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
            send(pool.clone(), &path, dereference),
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
        assert_eq!(header.size, 8);
        let payload = &bytes[4 + header_size..];
        assert!(
            payload.len() <= 8,
            "bytes beyond the header size reached the wire"
        );
        assert_eq!(payload, &replacement[..replacement.len().min(8)]);
    }
    Ok(())
}

#[tokio::test]
async fn shrinking_file_discards_its_incomplete_data_frame() -> anyhow::Result<()> {
    rejects_changed_payload(b"new").await
}

#[tokio::test]
async fn growing_file_never_sends_bytes_beyond_its_header() -> anyhow::Result<()> {
    rejects_changed_payload(b"replacement grows").await
}

#[tokio::test]
async fn size_zero_proc_file_with_content_is_rejected() -> anyhow::Result<()> {
    let path = std::path::Path::new("/proc/self/status");
    assert_eq!(std::fs::metadata(path)?.len(), 0);
    for dereference in [false, true] {
        let (writer, mut reader) = tokio::io::duplex(16384);
        let pool = pool(Box::new(writer));
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            send(pool.clone(), path, dereference),
        )
        .await?
        .expect_err("a size-zero proc file's content was sent as an unframed payload");
        assert!(
            format!("{error:#}").contains("/proc/self/status"),
            "{error:#}"
        );
        assert_eq!(pool.recv.len(), 0);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await?;
        let header_size = u32::from_be_bytes(bytes[..4].try_into()?) as usize;
        assert_eq!(
            bytes.len(),
            4 + header_size,
            "size-zero frame carried payload bytes"
        );
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
        send(pool.clone(), &first, dereference).await?;
        assert_eq!(pool.recv.len(), 1);
        send(pool.clone(), &second, dereference).await?;
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
}

impl tokio::io::AsyncWrite for CloseControlOnDrop {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payload_error_is_published_before_stream_drop_closes_control() -> anyhow::Result<()> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("file");
    std::fs::write(&path, b"payload")?;
    let (close_peer, close_requested) = tokio::sync::oneshot::channel();
    let (published, observed) = std::sync::mpsc::channel();
    let pool = pool(Box::new(CloseControlOnDrop {
        close_peer: Some(close_peer),
        published: observed,
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
    assert_eq!(
        error
            .root_cause()
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::raw_os_error),
        Some(libc::EIO),
        "the control close replaced the original payload error: {error:#}"
    );
    assert!(format!("{error:#}").contains(path.to_str().unwrap()));
    Ok(())
}
