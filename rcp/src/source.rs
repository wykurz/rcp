use async_recursion::async_recursion;
use tracing::{instrument, Instrument};

fn progress() -> &'static common::progress::Progress {
    common::get_progress()
}

#[instrument(skip(error_occurred))]
#[async_recursion]
async fn send_directories_and_symlinks(
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    is_root: bool,
    control_send_stream: &remote::streams::SharedSendStream,
    error_occurred: &std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    tracing::debug!("Sending data from {:?} to {:?}", &src, dst);
    let src_metadata = match if settings.dereference {
        tokio::fs::metadata(&src).await
    } else {
        tokio::fs::symlink_metadata(&src).await
    } {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("Failed reading metadata from src {src:?}: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            // for root items, failing to read metadata is fatal - we can't proceed
            // and the protocol would hang waiting for root completion
            if settings.fail_early || is_root {
                return Err(e.into());
            }
            return Ok(());
        }
    };
    if src_metadata.is_file() {
        return Ok(());
    }
    if src_metadata.is_symlink() {
        let target = match tokio::fs::read_link(&src).await {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Failed reading symlink {src:?}: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                // notify destination that this symlink was skipped
                // for root symlinks, this also signals root completion (even if failed)
                let skip_msg = remote::protocol::SourceMessage::SymlinkSkipped {
                    src_dst: remote::protocol::SrcDst {
                        src: src.to_path_buf(),
                        dst: dst.to_path_buf(),
                    },
                    is_root,
                };
                control_send_stream
                    .lock()
                    .await
                    .send_batch_message(&skip_msg)
                    .await?;
                if settings.fail_early {
                    return Err(e.into());
                }
                return Ok(());
            }
        };
        let symlink = remote::protocol::SourceMessage::Symlink {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            target: target.clone(),
            metadata: remote::protocol::Metadata::from(&src_metadata),
            is_root,
        };
        return control_send_stream
            .lock()
            .await
            .send_batch_message(&symlink)
            .await;
    }
    if !src_metadata.is_dir() {
        assert!(
            src_metadata.is_file(),
            "Encountered fs object that's not a directory, symlink or a file? {src:?}"
        );
        return Ok(());
    }
    // send Directory message with metadata (no entry count - that comes later with files)
    let dir = remote::protocol::SourceMessage::Directory {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        metadata: remote::protocol::Metadata::from(&src_metadata),
        is_root,
    };
    tracing::debug!("Sending directory: {:?} -> {:?}", &src, dst);
    control_send_stream
        .lock()
        .await
        .send_batch_message(&dir)
        .await?;
    // recurse into children
    let mut entries = match tokio::fs::read_dir(&src).await {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("Cannot open directory {src:?} for reading: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            if settings.fail_early {
                return Err(e.into());
            }
            return Ok(());
        }
    };
    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) => {
                let entry_path = entry.path();
                let entry_name = entry_path.file_name().unwrap();
                let dst_path = dst.join(entry_name);
                if let Err(e) = send_directories_and_symlinks(
                    settings,
                    &entry_path,
                    &dst_path,
                    false,
                    control_send_stream,
                    error_occurred,
                )
                .await
                {
                    tracing::error!("Failed to send directory/symlink {entry_path:?}: {e}");
                    error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                    if settings.fail_early {
                        return Err(e);
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::error!("Failed traversing src directory {src:?}: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e.into());
                }
                break;
            }
        }
    }
    Ok(())
}

#[instrument(skip(error_occurred, stream_pool))]
#[async_recursion]
async fn send_fs_objects(
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    control_send_stream: remote::streams::SharedSendStream,
    stream_pool: std::sync::Arc<remote::streams::SendStreamPool>,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    tracing::info!("Sending data from {:?} to {:?}", src, dst);
    let src_metadata = match if settings.dereference {
        tokio::fs::metadata(src).await
    } else {
        tokio::fs::symlink_metadata(src).await
    } {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("Failed reading metadata from src {src:?}: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            return Err(e.into());
        }
    };
    if !src_metadata.is_file() {
        if let Err(e) = send_directories_and_symlinks(
            settings,
            src,
            dst,
            true,
            &control_send_stream,
            &error_occurred,
        )
        .await
        {
            tracing::error!("Failed to send directories and symlinks: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            if settings.fail_early {
                return Err(e);
            }
        }
    }
    let mut stream = control_send_stream.lock().await;
    stream
        .send_control_message(&remote::protocol::SourceMessage::DirStructureComplete)
        .await?;
    drop(stream);
    if src_metadata.is_file() {
        // root file - send with dir_total_files=1 (itself is the only file)
        if let Err(e) = send_file(
            settings,
            src,
            dst,
            &src_metadata,
            true,
            1, // root file is the only file
            stream_pool,
            &error_occurred,
            control_send_stream.clone(),
        )
        .await
        {
            tracing::error!("Failed to send root file: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            // always return error for root file failures -
            // there's nothing else to transfer and the protocol would hang
            return Err(e);
        }
    }
    Ok(())
}

#[instrument(skip(error_occurred, control_send_stream, stream_pool))]
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn send_file(
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    src_metadata: &std::fs::Metadata,
    is_root: bool,
    dir_total_files: usize,
    stream_pool: std::sync::Arc<remote::streams::SendStreamPool>,
    error_occurred: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    control_send_stream: remote::streams::SharedSendStream,
) -> anyhow::Result<()> {
    let prog = progress();
    let _ops_guard = prog.ops.guard();
    let _open_file_guard = throttle::open_file_permit()
        .instrument(tracing::trace_span!("open_file_permit"))
        .await;
    tracing::debug!("Sending file content for {:?}", src);
    throttle::get_file_iops_tokens(settings.chunk_size, src_metadata.len())
        .instrument(tracing::trace_span!(
            "iops_throttle",
            size = src_metadata.len()
        ))
        .await;
    // open the file BEFORE borrowing a stream to avoid leaving destination waiting
    let file = match tokio::fs::File::open(src)
        .instrument(tracing::trace_span!("file_open"))
        .await
    {
        Ok(f) => f,
        Err(e) => {
            tracing::error!("Failed to open file {src:?}: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            // for root file copies, failing to open the file is a fatal error -
            // there's nothing else to transfer and the protocol would hang
            if is_root {
                return Err(e.into());
            }
            // notify destination that this file was skipped (for directory tracking)
            let skip_msg = remote::protocol::SourceMessage::FileSkipped {
                src: src.to_path_buf(),
                dst: dst.to_path_buf(),
                dir_total_files,
            };
            control_send_stream
                .lock()
                .await
                .send_batch_message(&skip_msg)
                .await?;
            if settings.fail_early {
                return Err(e.into());
            }
            return Ok(());
        }
    };
    // wrap file in a buffered reader for better network throughput
    // buffer size is set by quic_config.effective_remote_copy_buffer_size() based on network profile,
    // but capped at file size to avoid over-allocation for small files
    let file_size = src_metadata.len().min(usize::MAX as u64) as usize;
    let buffer_size = settings.remote_copy_buffer_size.min(file_size).max(1);
    let mut buffered_file = tokio::io::BufReader::with_capacity(buffer_size, file);
    let metadata = remote::protocol::Metadata::from(src_metadata);
    let file_header = remote::protocol::File {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        size: src_metadata.len(),
        metadata,
        is_root,
        dir_total_files,
    };
    // borrow a stream from the pool instead of opening a new one
    let mut pooled_stream = stream_pool
        .borrow()
        .instrument(tracing::trace_span!("borrow_stream"))
        .await?;
    let send_result = pooled_stream
        .stream_mut()
        .send_message_with_data_buffered(&file_header, &mut buffered_file)
        .instrument(tracing::trace_span!(
            "send_data",
            size = src_metadata.len(),
            buffer_size
        ))
        .await;
    match send_result {
        Ok(_bytes_sent) => {
            // stream is returned to pool when pooled_stream is dropped
            prog.files_copied.inc();
            prog.bytes_copied.add(src_metadata.len());
            tracing::info!("Sent file: {:?} -> {:?}", src, dst);
            Ok(())
        }
        Err(e) => {
            tracing::error!("Failed to send file content for {src:?}: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            // don't return stream to pool on error - it's in a bad state and would
            // corrupt subsequent files if reused. take it out and close it.
            if let Some(mut bad_stream) = pooled_stream.take_and_discard() {
                // best effort close; ignore errors since stream is already broken
                let _ = bad_stream.close().await;
            }
            // replenish the pool with a new stream to prevent pool exhaustion
            if let Err(replenish_err) = stream_pool.replenish().await {
                tracing::warn!("Failed to replenish stream pool: {:#}", replenish_err);
            }
            if settings.fail_early {
                Err(e)
            } else {
                Ok(())
            }
        }
    }
}

#[instrument(skip(error_occurred, control_send_stream, stream_pool))]
async fn send_files_in_directory(
    settings: common::copy::Settings,
    src: std::path::PathBuf,
    dst: std::path::PathBuf,
    stream_pool: std::sync::Arc<remote::streams::SendStreamPool>,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
    control_send_stream: remote::streams::SharedSendStream,
) -> anyhow::Result<()> {
    tracing::info!("Sending files from {src:?}");
    // first pass: count files and collect their paths
    let mut file_entries: Vec<(std::path::PathBuf, std::path::PathBuf, std::fs::Metadata)> =
        Vec::new();
    let mut entries = match tokio::fs::read_dir(&src).await {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("Cannot open directory {src:?} for reading: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            if settings.fail_early {
                return Err(e.into());
            }
            return Ok(());
        }
    };
    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) => {
                let entry_path = entry.path();
                let entry_name = entry_path.file_name().unwrap();
                let dst_path = dst.join(entry_name);
                let entry_metadata = match if settings.dereference {
                    tokio::fs::metadata(&entry_path).await
                } else {
                    tokio::fs::symlink_metadata(&entry_path).await
                } {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::error!("Failed reading metadata from {entry_path:?}: {e}");
                        error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                        if settings.fail_early {
                            return Err(e.into());
                        }
                        continue;
                    }
                };
                if entry_metadata.is_file() {
                    file_entries.push((entry_path, dst_path, entry_metadata));
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::error!("Failed traversing src directory {src:?}: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e.into());
                }
                break;
            }
        }
    }
    drop(entries);
    let dir_total_files = file_entries.len();
    tracing::info!("Directory {:?} has {} files to send", src, dir_total_files);
    // if directory is empty, send DirectoryEmpty message
    if dir_total_files == 0 {
        let empty_msg = remote::protocol::SourceMessage::DirectoryEmpty {
            src: src.clone(),
            dst: dst.clone(),
        };
        control_send_stream
            .lock()
            .await
            .send_control_message(&empty_msg)
            .await?;
        tracing::info!("Sent DirectoryEmpty for {:?}", src);
        return Ok(());
    }
    // second pass: send files with the known total count
    let mut join_set = tokio::task::JoinSet::new();
    for (entry_path, dst_path, entry_metadata) in file_entries {
        throttle::get_ops_token().await;
        let pool = stream_pool.clone();
        let error_flag = error_occurred.clone();
        let control_stream = control_send_stream.clone();
        let total = dir_total_files;
        join_set.spawn(async move {
            send_file(
                &settings,
                &entry_path,
                &dst_path,
                &entry_metadata,
                false,
                total,
                pool,
                &error_flag,
                control_stream,
            )
            .await
        });
    }
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!("Failed to send file from {src:?}: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e);
                }
            }
            Err(e) => {
                tracing::error!("Task panicked while sending file from {src:?}: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e.into());
                }
            }
        }
    }
    Ok(())
}

/// Result of receiving a message from the control stream
enum RecvResult {
    Message(remote::protocol::DestinationMessage),
    StreamClosed,
    Error(anyhow::Error),
}

#[instrument(skip(error_occurred, stream_pool))]
async fn dispatch_control_messages(
    settings: common::copy::Settings,
    mut control_recv_stream: remote::streams::RecvStream,
    control_send_stream: remote::streams::SharedSendStream,
    stream_pool: std::sync::Arc<remote::streams::SendStreamPool>,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    let mut join_set = tokio::task::JoinSet::new();
    // flag to track when graceful shutdown has been initiated (DestinationDone received).
    // after this, task errors (like "unknown stream") are expected and should be ignored.
    let mut shutdown_initiated = false;
    // spawn a separate task to receive messages from destination.
    // this is needed because recv_object is NOT cancel-safe (it reads length-prefixed messages),
    // so we can't use it directly in select!. channel recv IS cancel-safe.
    let (msg_tx, mut msg_rx) = tokio::sync::mpsc::channel::<RecvResult>(16);
    let recv_task = tokio::spawn(async move {
        loop {
            match control_recv_stream
                .recv_object::<remote::protocol::DestinationMessage>()
                .await
            {
                Ok(Some(msg)) => {
                    if msg_tx.send(RecvResult::Message(msg)).await.is_err() {
                        break; // receiver dropped
                    }
                }
                Ok(None) => {
                    let _ = msg_tx.send(RecvResult::StreamClosed).await;
                    break;
                }
                Err(e) => {
                    let _ = msg_tx.send(RecvResult::Error(e)).await;
                    break;
                }
            }
        }
        control_recv_stream.close().await;
    });
    // main loop - select between task completions and messages (both are cancel-safe)
    let result = loop {
        tokio::select! {
            // biased ensures we check tasks first, giving priority to error detection
            biased;
            // check for task completions/failures
            task_result = join_set.join_next(), if !join_set.is_empty() => {
                if let Some(result) = task_result {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            tracing::error!("Task failed: {e}");
                            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                            if settings.fail_early {
                                break Err(e);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Task panicked: {e}");
                            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                            if settings.fail_early {
                                break Err(e.into());
                            }
                        }
                    }
                }
            }
            // receive message from destination (via channel - cancel safe)
            recv_result = msg_rx.recv() => {
                let message = match recv_result {
                    Some(RecvResult::Message(m)) => m,
                    Some(RecvResult::StreamClosed) | None => break Ok(()), // stream closed
                    Some(RecvResult::Error(e)) => break Err(e),
                };
                match message {
                    remote::protocol::DestinationMessage::DirectoryCreated(confirmation) => {
                        tracing::info!(
                            "Received directory creation confirmation for: {:?} -> {:?}",
                            confirmation.src,
                            confirmation.dst
                        );
                        let error_flag = error_occurred.clone();
                        join_set.spawn(send_files_in_directory(
                            settings,
                            confirmation.src.clone(),
                            confirmation.dst.clone(),
                            stream_pool.clone(),
                            error_flag,
                            control_send_stream.clone(),
                        ));
                    }
                    remote::protocol::DestinationMessage::DestinationDone => {
                        tracing::info!("Received DestinationDone message");
                        // set shutdown flag - we'll drain remaining tasks and close.
                        // any task errors after this point are expected (destination is done).
                        shutdown_initiated = true;
                        break Ok(());
                    }
                }
            }
        }
    };
    // if we're exiting with an error, abort the recv task immediately
    // (otherwise it would block waiting for more messages from destination)
    if result.is_err() {
        recv_task.abort();
    }
    // drain remaining tasks.
    // if shutdown was initiated (DestinationDone received), ignore task errors.
    // these are expected when the connection is closing (e.g., "unknown stream").
    while let Some(task_result) = join_set.join_next().await {
        match task_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if shutdown_initiated {
                    tracing::debug!("Task failed during shutdown (expected): {e}");
                } else {
                    tracing::error!("Task failed: {e}");
                    error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                    if settings.fail_early && result.is_ok() {
                        // only override if we don't already have an error
                        recv_task.abort();
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                if shutdown_initiated {
                    tracing::debug!("Task panicked during shutdown: {e}");
                } else {
                    tracing::error!("Task panicked: {e}");
                    error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                    if settings.fail_early && result.is_ok() {
                        recv_task.abort();
                        return Err(e.into());
                    }
                }
            }
        }
    }
    // close send stream after all tasks complete
    if shutdown_initiated {
        tracing::info!("All file send tasks completed, closing send stream");
        let mut stream = control_send_stream.lock().await;
        if let Err(e) = stream.close().await {
            tracing::debug!("Failed to close control stream: {e}");
        }
    }
    // wait for recv task to finish (it will close the stream)
    let _ = recv_task.await;
    tracing::info!("Finished dispatching control messages");
    result
}

async fn handle_connection(
    conn: quinn::Connecting,
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    pool_size: usize,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    let connection = conn.await?;
    tracing::info!("Destination connection established");
    let connection = remote::streams::Connection::new(connection);
    let (control_send_stream, control_recv_stream) = connection.open_bi().await?;
    tracing::info!("Opened streams for directory transfer");
    // create the stream pool for file transfers (pool owns a clone of connection for replenishment)
    let stream_pool = std::sync::Arc::new(
        remote::streams::SendStreamPool::new(connection.clone(), pool_size).await?,
    );
    tracing::info!("Created stream pool with {} streams", pool_size);
    let dispatch_task = tokio::spawn(dispatch_control_messages(
        *settings,
        control_recv_stream,
        control_send_stream.clone(),
        stream_pool.clone(),
        error_occurred.clone(),
    ));
    let send_result = send_fs_objects(
        settings,
        src,
        dst,
        control_send_stream,
        stream_pool.clone(),
        error_occurred.clone(),
    )
    .await;
    // if sending failed, close connection to unblock destination immediately
    if send_result.is_err() {
        connection.close();
    }
    // wait for dispatch task to complete - this releases its stream_pool reference
    let dispatch_result = dispatch_task.await;
    // close all streams in the pool - do this before propagating errors to ensure cleanup
    match std::sync::Arc::try_unwrap(stream_pool) {
        Ok(pool) => {
            if let Err(e) = pool.close_all().await {
                tracing::warn!("failed to close stream pool: {:#}", e);
            }
        }
        Err(_) => tracing::warn!("stream pool still has references, cannot close cleanly"),
    }
    // propagate errors after cleanup
    send_result?;
    dispatch_result??;
    tracing::info!("Data sent successfully");
    // close the connection and wait for clean shutdown.
    connection.close();
    let close_reason = connection.closed().await;
    tracing::debug!("connection closed: {close_reason}");
    Ok(())
}

#[instrument]
pub async fn run_source(
    master_send_stream: remote::streams::SharedSendStream,
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &common::copy::Settings,
    quic_config: &remote::QuicConfig,
    bind_ip: Option<&str>,
) -> anyhow::Result<(String, common::copy::Summary)> {
    let (server_endpoint, cert_fingerprint) = remote::get_server_with_config(quic_config)?;
    let server_addr = remote::get_endpoint_addr_with_bind_ip(&server_endpoint, bind_ip)?;
    tracing::info!("Source server listening on {}", server_addr);
    let master_hello = remote::protocol::SourceMasterHello {
        source_addr: server_addr,
        server_name: remote::get_random_server_name(),
        cert_fingerprint,
    };
    tracing::info!("Sending master hello: {:?}", master_hello);
    master_send_stream
        .lock()
        .await
        .send_control_message(&master_hello)
        .await?;
    tracing::info!("Waiting for connection from destination");
    // wait for destination to connect with a timeout
    // destination should connect within a reasonable time after receiving the source address
    let error_occurred = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let accept_timeout = std::time::Duration::from_secs(quic_config.conn_timeout_sec);
    // pool size must not exceed QUIC's max_concurrent_uni_streams setting
    // quinn's default is 100, so we use that when not explicitly configured
    let pool_size = quic_config
        .tuning
        .max_concurrent_streams
        .filter(|&v| v > 0)
        .unwrap_or(100) as usize;
    match tokio::time::timeout(accept_timeout, server_endpoint.accept()).await {
        Ok(Some(conn)) => {
            tracing::info!("New destination connection incoming");
            handle_connection(conn, settings, src, dst, pool_size, error_occurred.clone()).await?;
        }
        Ok(None) => {
            tracing::error!("Server endpoint closed unexpectedly");
            return Err(anyhow::anyhow!("Server endpoint closed unexpectedly"));
        }
        Err(_) => {
            tracing::error!(
                "Timed out waiting for destination to connect after {:?}. \
                This usually means the destination cannot reach the source. \
                Check network connectivity and firewall rules.",
                accept_timeout
            );
            return Err(anyhow::anyhow!(
                "Timed out waiting for destination to connect after {:?}",
                accept_timeout
            ));
        }
    }
    tracing::info!("Source is done");
    server_endpoint.wait_idle().await;
    // source doesn't track summary - destination is authoritative
    if error_occurred.load(std::sync::atomic::Ordering::Relaxed) {
        Err(common::copy::Error {
            source: anyhow::anyhow!("Some operations failed during remote copy"),
            summary: common::copy::Summary::default(),
        }
        .into())
    } else {
        Ok(("source OK".to_string(), common::copy::Summary::default()))
    }
}
