use anyhow::Context;
use async_recursion::async_recursion;
use tracing::{instrument, Instrument};

fn progress() -> &'static common::progress::Progress {
    common::get_progress()
}

#[instrument(skip(error_occurred, control_send_stream))]
#[async_recursion]
async fn send_directories_and_symlinks(
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    is_root: bool,
    control_send_stream: &remote::streams::BoxedSharedSendStream,
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

#[instrument(skip(error_occurred, stream_pool, control_send_stream))]
#[async_recursion]
async fn send_fs_objects_tcp(
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    control_send_stream: remote::streams::BoxedSharedSendStream,
    stream_pool: std::sync::Arc<AcceptingSendStreamPool>,
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
        if let Err(e) = send_file_tcp(
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
async fn send_file_tcp(
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    src_metadata: &std::fs::Metadata,
    is_root: bool,
    dir_total_files: usize,
    stream_pool: std::sync::Arc<AcceptingSendStreamPool>,
    error_occurred: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    control_send_stream: remote::streams::BoxedSharedSendStream,
) -> anyhow::Result<()> {
    let prog = progress();
    let _ops_guard = prog.ops.guard();
    tracing::debug!("Sending file content for {:?}", src);
    // borrow a stream FIRST to provide backpressure. files are only opened after we have
    // a stream available, which limits memory usage when destination is slow.
    let mut pooled_stream = stream_pool
        .borrow()
        .instrument(tracing::trace_span!("borrow_stream"))
        .await?;
    // now that we have a stream, acquire file-related resources
    let _open_file_guard = throttle::open_file_permit()
        .instrument(tracing::trace_span!("open_file_permit"))
        .await;
    throttle::get_file_iops_tokens(settings.chunk_size, src_metadata.len())
        .instrument(tracing::trace_span!(
            "iops_throttle",
            size = src_metadata.len()
        ))
        .await;
    // open the file AFTER borrowing a stream for backpressure
    let file = match tokio::fs::File::open(src)
        .instrument(tracing::trace_span!("file_open"))
        .await
    {
        Ok(f) => f,
        Err(e) => {
            tracing::error!("Failed to open file {src:?}: {e:#}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            // stream is returned to pool via Drop when pooled_stream goes out of scope
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
    // buffer size is set by tcp_config.effective_remote_copy_buffer_size() based on network profile,
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
            // don't return stream to pool on error - it's in a bad state.
            // close it immediately.
            if let Some(mut bad_stream) = pooled_stream.take_and_discard() {
                // best effort close; ignore errors since stream is already broken
                let _ = bad_stream.close().await;
            }
            // transport failure is always fatal - destination is waiting on this connection
            // and we can't recover from stream corruption.
            Err(e)
        }
    }
}

#[instrument(skip(error_occurred, control_send_stream, stream_pool, pending_limit))]
async fn send_files_in_directory_tcp(
    settings: common::copy::Settings,
    src: std::path::PathBuf,
    dst: std::path::PathBuf,
    stream_pool: std::sync::Arc<AcceptingSendStreamPool>,
    pending_limit: std::sync::Arc<tokio::sync::Semaphore>,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
    control_send_stream: remote::streams::BoxedSharedSendStream,
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
    // acquire permit from pending_limit before spawning to provide backpressure
    let mut join_set = tokio::task::JoinSet::new();
    for (entry_path, dst_path, entry_metadata) in file_entries {
        throttle::get_ops_token().await;
        // wait for a pending slot - this is the main backpressure point
        let permit = pending_limit
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("pending limit semaphore closed"))?;
        let pool = stream_pool.clone();
        let error_flag = error_occurred.clone();
        let control_stream = control_send_stream.clone();
        let total = dir_total_files;
        join_set.spawn(async move {
            let result = send_file_tcp(
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
            .await;
            drop(permit); // release permit when file is done
            result
        });
    }
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                // transport errors from send_file_tcp are always fatal regardless of fail_early.
                // file-level errors (permission denied, etc.) are handled inside send_file_tcp
                // by sending FileSkipped and returning Ok(()).
                tracing::error!("Transport failure sending file from {src:?}: {e:#}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                return Err(e);
            }
            Err(e) => {
                tracing::error!("Task panicked while sending file from {src:?}: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                return Err(e.into());
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

/// Dispatches control messages from destination and coordinates file sending.
///
/// # Shutdown Flow
///
/// This function must signal pool shutdown before draining tasks to prevent deadlock.
/// The flow differs between graceful and unexpected shutdown:
///
/// ## Graceful Shutdown (DestinationDone received)
/// 1. Receive `DestinationDone` message from destination
/// 2. Set `shutdown_initiated = true`, break main loop
/// 3. Signal pool shutdown via `pool_shutdown.cancel()` - this closes the pool's
///    send channel, causing any `borrow()` calls to return error
/// 4. Drain remaining tasks in `join_set` - they complete quickly since pool is closed
/// 5. Close control stream, return Ok
///
/// ## Unexpected Shutdown (StreamClosed without DestinationDone)
/// This happens when destination fails (e.g., fail-early error) and closes connections
/// without sending DestinationDone:
/// 1. Receive `StreamClosed` from control stream
/// 2. Break main loop with Ok (stream closed is not an error)
/// 3. **Critical**: Signal pool shutdown via `pool_shutdown.cancel()` BEFORE draining
/// 4. Drain remaining tasks - they now return error from `borrow()` instead of hanging
/// 5. Return Ok (errors during unexpected shutdown are logged but not propagated)
///
/// ## Deadlock Prevention
/// Without step 3 in unexpected shutdown, tasks waiting on `stream_pool.borrow()` would
/// hang forever because:
/// - The pool's recv channel waits for streams from accept loop
/// - Accept loop waits for connections from destination
/// - Destination has already closed and won't connect
/// - Pool shutdown only happens AFTER this function returns (in handle_connection)
/// - Deadlock: this function waits for tasks, tasks wait for pool, pool waits for shutdown
#[instrument(skip(
    error_occurred,
    stream_pool,
    control_recv_stream,
    control_send_stream,
    pool_shutdown
))]
async fn dispatch_control_messages_tcp(
    settings: common::copy::Settings,
    mut control_recv_stream: remote::streams::BoxedRecvStream,
    control_send_stream: remote::streams::BoxedSharedSendStream,
    stream_pool: std::sync::Arc<AcceptingSendStreamPool>,
    max_pending_files: usize,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pool_shutdown: PoolShutdownToken,
) -> anyhow::Result<()> {
    // create semaphore to limit pending file tasks for backpressure
    let pending_limit = std::sync::Arc::new(tokio::sync::Semaphore::new(max_pending_files));
    tracing::info!(
        "Created pending file limiter with {} permits",
        max_pending_files
    );
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
            // check for task completions/failures.
            // transport errors are always fatal - they indicate stream corruption or connection
            // failure, leaving the destination waiting for files that will never arrive.
            // file-level errors are handled inside send_files_in_directory_tcp by returning Ok(()).
            task_result = join_set.join_next(), if !join_set.is_empty() => {
                if let Some(result) = task_result {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            tracing::error!("Transport failure in directory send task: {e:#}");
                            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                            break Err(e);
                        }
                        Err(e) => {
                            tracing::error!("Task panicked: {e}");
                            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                            break Err(e.into());
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
                        join_set.spawn(send_files_in_directory_tcp(
                            settings,
                            confirmation.src.clone(),
                            confirmation.dst.clone(),
                            stream_pool.clone(),
                            pending_limit.clone(),
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
    // CRITICAL: Signal pool shutdown BEFORE draining tasks to prevent deadlock.
    // Without this, tasks waiting on `stream_pool.borrow()` would hang forever because:
    // - borrow() waits on the pool's recv channel
    // - recv channel waits for streams from accept loop
    // - accept loop waits for connections from destination
    // - destination has already closed (or will never connect)
    // Cancelling the token signals the accept loop to close and close the channel,
    // causing borrow() to return an error immediately.
    pool_shutdown.cancel();
    // drain remaining tasks.
    // since we called pool_shutdown.cancel() above, any tasks waiting on borrow()
    // will get "pool closed" errors. these are expected and should be logged but
    // not propagated, unless the main loop already returned an error (result.is_err()).
    //
    // error handling during drain:
    // - shutdown_initiated=true (DestinationDone received): all errors expected, log debug
    // - shutdown_initiated=false, result=Ok (unexpected close): pool errors expected, log debug
    // - result=Err: we already have an error, just log additional errors
    let pool_shutdown_errors_expected = result.is_ok(); // pool was just cancelled
    while let Some(task_result) = join_set.join_next().await {
        match task_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if shutdown_initiated || pool_shutdown_errors_expected {
                    tracing::debug!("Task failed during shutdown (expected): {e}");
                } else {
                    // transport errors are always fatal - we can't recover
                    tracing::error!("Transport failure in file send task: {e:#}");
                    error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                    // don't return error here - result already has an error
                }
            }
            Err(e) => {
                if shutdown_initiated || pool_shutdown_errors_expected {
                    tracing::debug!("Task panicked during shutdown: {e}");
                } else {
                    tracing::error!("Task panicked: {e}");
                    error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                    // don't return error here - result already has an error
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

/// Cancellation token alias for pool shutdown signaling.
///
/// Uses `CancellationToken` instead of oneshot because:
/// - Clonable: Multiple places can hold a reference (dispatch task, handle_connection)
/// - Idempotent: Can call `cancel()` multiple times safely
/// - Check-able: Can check `is_cancelled()` without consuming
type PoolShutdownToken = tokio_util::sync::CancellationToken;

/// Accepts data connections and provides SendStreams for file transfer.
///
/// The source accepts incoming TCP connections from the destination on its data port,
/// wraps them as SendStreams, and provides them via a channel for file sending tasks.
/// Connections are reused for multiple files - the `size` field in file headers delimits
/// file boundaries within a connection.
struct AcceptingSendStreamPool {
    recv: async_channel::Receiver<remote::streams::BoxedSendStream>,
    return_tx: async_channel::Sender<remote::streams::BoxedSendStream>,
}

impl AcceptingSendStreamPool {
    /// Create a new pool that accepts connections from the given listener.
    /// Returns the pool, a shutdown token, and the accept task handle.
    ///
    /// The shutdown token should be cancelled to signal the pool to close. It can be
    /// cloned and shared between multiple tasks - any clone can trigger shutdown.
    fn new(
        data_listener: tokio::net::TcpListener,
        pool_size: usize,
        profile: remote::NetworkProfile,
        tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    ) -> (Self, PoolShutdownToken, tokio::task::JoinHandle<()>) {
        let (send_tx, recv) = async_channel::bounded(pool_size);
        let (return_tx, return_rx) =
            async_channel::bounded::<remote::streams::BoxedSendStream>(pool_size);
        let shutdown_token = PoolShutdownToken::new();
        let shutdown_token_clone = shutdown_token.clone();
        // spawn task to accept data connections and manage pool
        let accept_task = tokio::spawn(async move {
            // wrap the main loop so we can handle shutdown
            tokio::select! {
                _ = async {
                    loop {
                        tokio::select! {
                            // accept new connections from destination
                            result = data_listener.accept() => {
                                match result {
                                    Ok((stream, addr)) => {
                                        tracing::debug!("Accepted data connection from {}", addr);
                                        stream.set_nodelay(true).ok();
                                        remote::configure_tcp_buffers(&stream, profile);
                                        // wrap with TLS if configured
                                        let send_stream = if let Some(ref acceptor) = tls_acceptor {
                                            match acceptor.accept(stream).await {
                                                Ok(tls_stream) => {
                                                    let (_read_half, write_half) = tokio::io::split(tls_stream);
                                                    remote::streams::SendStream::new(
                                                        Box::new(write_half) as remote::streams::BoxedWrite
                                                    )
                                                }
                                                Err(e) => {
                                                    tracing::warn!("TLS handshake failed for data connection: {}", e);
                                                    continue;
                                                }
                                            }
                                        } else {
                                            let (_read_half, write_half) = stream.into_split();
                                            remote::streams::SendStream::new(
                                                Box::new(write_half) as remote::streams::BoxedWrite
                                            )
                                        };
                                        if send_tx.send(send_stream).await.is_err() {
                                            tracing::debug!("Pool closed, stopping accept loop");
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        tracing::debug!("Data listener accept error: {}", e);
                                        break;
                                    }
                                }
                            }
                            // re-queue returned streams for reuse
                            result = return_rx.recv() => {
                                match result {
                                    Ok(stream) => {
                                        // return stream to pool for reuse by another file transfer.
                                        // file boundaries are delimited by length-prefixed headers
                                        // and the size field, so streams can be safely reused.
                                        if send_tx.send(stream).await.is_err() {
                                            tracing::debug!("Pool closed while returning stream");
                                            break;
                                        }
                                    }
                                    Err(_) => break, // return channel closed
                                }
                            }
                        }
                    }
                } => {}
                // shutdown signal received - close all streams
                _ = shutdown_token_clone.cancelled() => {
                    tracing::debug!("Pool shutdown signal received");
                }
            }
            // drain and close all streams in the pool so destination sees EOF
            // close the sender to stop any pending borrows
            send_tx.close();
            // drain streams from the return channel (streams being returned by workers)
            while let Ok(mut stream) = return_rx.try_recv() {
                let _ = stream.close().await;
            }
            return_rx.close();
            tracing::debug!("Pool accept task completed, all streams closed");
        });
        (Self { recv, return_tx }, shutdown_token, accept_task)
    }

    /// Borrow a SendStream from the pool (waits for a connection from destination).
    async fn borrow(&self) -> anyhow::Result<PooledAcceptedSendStream> {
        let stream = self
            .recv
            .recv()
            .await
            .map_err(|_| anyhow::anyhow!("data connection pool closed"))?;
        Ok(PooledAcceptedSendStream {
            stream: Some(stream),
            return_tx: self.return_tx.clone(),
        })
    }
}

/// RAII guard that returns the connection to the pool on drop.
/// Connections are reused for multiple files via length-prefixed framing.
struct PooledAcceptedSendStream {
    stream: Option<remote::streams::BoxedSendStream>,
    return_tx: async_channel::Sender<remote::streams::BoxedSendStream>,
}

impl PooledAcceptedSendStream {
    fn stream_mut(&mut self) -> &mut remote::streams::BoxedSendStream {
        self.stream.as_mut().expect("stream already taken")
    }

    fn take_and_discard(&mut self) -> Option<remote::streams::BoxedSendStream> {
        self.stream.take()
    }
}

impl Drop for PooledAcceptedSendStream {
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            // best effort return for cleanup
            let _ = self.return_tx.try_send(stream);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    control_stream: tokio::net::TcpStream,
    data_listener: tokio::net::TcpListener,
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    pool_size: usize,
    max_pending_files: usize,
    network_profile: remote::NetworkProfile,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
    tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
) -> anyhow::Result<()> {
    tracing::info!("Destination control connection established");
    // configure TCP buffers for high throughput
    remote::configure_tcp_buffers(&control_stream, network_profile);
    // wrap control connection with TLS if configured
    let (control_send_stream, control_recv_stream) = if let Some(ref acceptor) = tls_acceptor {
        let tls_stream = acceptor
            .accept(control_stream)
            .await
            .context("TLS handshake failed for control connection")?;
        let (read_half, write_half) = tokio::io::split(tls_stream);
        let recv_stream =
            remote::streams::RecvStream::new(Box::new(read_half) as remote::streams::BoxedRead);
        let send_stream =
            remote::streams::SendStream::new(Box::new(write_half) as remote::streams::BoxedWrite);
        (send_stream, recv_stream)
    } else {
        let (read_half, write_half) = control_stream.into_split();
        let recv_stream =
            remote::streams::RecvStream::new(Box::new(read_half) as remote::streams::BoxedRead);
        let send_stream =
            remote::streams::SendStream::new(Box::new(write_half) as remote::streams::BoxedWrite);
        (send_stream, recv_stream)
    };
    // wrap in Arc<Mutex<>> for shared access
    let control_send_stream = std::sync::Arc::new(tokio::sync::Mutex::new(control_send_stream));
    tracing::info!("Created control streams for directory transfer");
    // create a pool that accepts data connections from destination and provides SendStreams
    let (stream_pool, pool_shutdown, accept_task) =
        AcceptingSendStreamPool::new(data_listener, pool_size, network_profile, tls_acceptor);
    let stream_pool = std::sync::Arc::new(stream_pool);
    tracing::info!(
        "Created accepting send stream pool with {} slots",
        pool_size
    );
    // pass a clone of the shutdown token to dispatch - it will signal shutdown before
    // draining its tasks to prevent deadlock when destination closes unexpectedly.
    // see dispatch_control_messages_tcp doc comment for detailed shutdown flow.
    let dispatch_task = tokio::spawn(dispatch_control_messages_tcp(
        *settings,
        control_recv_stream,
        control_send_stream.clone(),
        stream_pool.clone(),
        max_pending_files,
        error_occurred.clone(),
        pool_shutdown.clone(),
    ));
    // send files to destination. returns Err only for fatal errors (e.g., root file failure).
    // individual file failures with fail_early=false return Ok but set error_occurred flag,
    // and destination is notified via FileSkipped messages on the control channel.
    let send_result = send_fs_objects_tcp(
        settings,
        src,
        dst,
        control_send_stream,
        stream_pool,
        error_occurred.clone(),
    )
    .await;
    // if send failed, we need to close the pool FIRST so destination's data connections
    // see EOF and can complete. Otherwise destination hangs waiting for file data and
    // never sends DestinationDone, causing dispatch_task to hang forever.
    if send_result.is_err() {
        tracing::info!("Send failed, shutting down data pool to unblock destination");
        // signal pool to shutdown (closes all streams so destination sees EOF)
        // note: cancel() is idempotent, safe to call even if dispatch already called it
        pool_shutdown.cancel();
        // abort dispatch task since we're not going to get a clean shutdown
        dispatch_task.abort();
        // wait for accept task to finish closing all streams
        let _ = accept_task.await;
        return send_result;
    }
    // send succeeded - wait for dispatch task to complete (handles destination responses).
    // note: dispatch_control_messages_tcp always calls pool_shutdown.cancel() before
    // returning, so the pool will be shut down when dispatch_task completes.
    let dispatch_result = dispatch_task.await;
    // wait for accept task to finish (pool shutdown was signaled by dispatch)
    let _ = accept_task.await;
    // propagate dispatch errors after cleanup
    dispatch_result??;
    tracing::info!("Data sent successfully");
    Ok(())
}

#[instrument(skip(master_send_stream, cert_key))]
#[allow(clippy::too_many_arguments)]
pub async fn run_source<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
    master_send_stream: remote::streams::SharedSendStream<W>,
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &common::copy::Settings,
    tcp_config: &remote::TcpConfig,
    bind_ip: Option<&str>,
    cert_key: Option<&remote::tls::CertifiedKey>,
    dest_cert_fingerprint: Option<remote::protocol::CertFingerprint>,
) -> anyhow::Result<(String, common::copy::Summary)> {
    // create TLS acceptor if encryption is enabled (requires both cert and dest fingerprint)
    let tls_acceptor = match (cert_key, dest_cert_fingerprint) {
        (Some(cert), Some(dest_fp)) => {
            // create server config with client certificate verification
            let server_config = remote::tls::create_server_config_with_client_auth(cert, dest_fp)
                .context("failed to create TLS server config with client auth")?;
            Some(std::sync::Arc::new(tokio_rustls::TlsAcceptor::from(
                server_config,
            )))
        }
        _ => None,
    };
    tracing::info!(
        "Source TLS encryption: {}",
        if tls_acceptor.is_some() {
            "enabled (mutual TLS)"
        } else {
            "disabled"
        }
    );
    // create TCP listeners for control and data connections
    let control_listener = remote::create_tcp_control_listener(tcp_config, bind_ip).await?;
    let data_listener = remote::create_tcp_data_listener(tcp_config, bind_ip).await?;
    let control_addr = remote::get_tcp_listener_addr(&control_listener, bind_ip)?;
    let data_addr = remote::get_tcp_listener_addr(&data_listener, bind_ip)?;
    tracing::info!(
        "Source TCP listeners: control={}, data={}",
        control_addr,
        data_addr
    );
    let master_hello = remote::protocol::SourceMasterHello {
        control_addr,
        data_addr,
        server_name: remote::get_random_server_name(),
    };
    tracing::info!("Sending master hello: {:?}", master_hello);
    master_send_stream
        .lock()
        .await
        .send_control_message(&master_hello)
        .await?;
    tracing::info!("Waiting for connection from destination");
    // wait for destination to connect with a timeout
    let error_occurred = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let accept_timeout = std::time::Duration::from_secs(tcp_config.conn_timeout_sec);
    let pool_size = tcp_config.max_connections;
    let max_pending_files = pool_size * tcp_config.pending_writes_multiplier;
    match tokio::time::timeout(accept_timeout, control_listener.accept()).await {
        Ok(Ok((stream, addr))) => {
            tracing::info!("Destination control connection from {}", addr);
            stream.set_nodelay(true)?;
            handle_connection(
                stream,
                data_listener,
                settings,
                src,
                dst,
                pool_size,
                max_pending_files,
                tcp_config.network_profile,
                error_occurred.clone(),
                tls_acceptor,
            )
            .await?;
        }
        Ok(Err(e)) => {
            tracing::error!("Failed to accept control connection: {:#}", e);
            return Err(e.into());
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
