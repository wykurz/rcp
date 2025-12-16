use anyhow::Context;
use tokio::io::AsyncWriteExt;
use tracing::{instrument, Instrument};

use super::directory_tracker;

fn progress() -> &'static common::progress::Progress {
    common::get_progress()
}

/// Pool of outbound TCP connections to source's data port.
///
/// Destination opens connections to source's data port to receive file data.
/// Each connection receives one file then is closed (no reuse since there's
/// no framing for multiple files on same connection).
struct DataConnectionPool {
    data_addr: std::net::SocketAddr,
    network_profile: remote::NetworkProfile,
    /// Semaphore to limit concurrent connections
    semaphore: std::sync::Arc<tokio::sync::Semaphore>,
    /// Optional TLS connector for encrypted connections
    tls_connector: Option<std::sync::Arc<tokio_rustls::TlsConnector>>,
}

impl DataConnectionPool {
    fn new(
        data_addr: std::net::SocketAddr,
        max_connections: usize,
        network_profile: remote::NetworkProfile,
        tls_connector: Option<std::sync::Arc<tokio_rustls::TlsConnector>>,
    ) -> Self {
        Self {
            data_addr,
            network_profile,
            semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(max_connections)),
            tls_connector,
        }
    }
    /// Open a new connection to the source's data port.
    /// Returns a RecvStream for reading file data.
    async fn connect(
        &self,
    ) -> anyhow::Result<(
        remote::streams::BoxedRecvStream,
        tokio::sync::OwnedSemaphorePermit,
    )> {
        // acquire semaphore permit (limits concurrent connections)
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("data pool closed"))?;
        // connect to source
        let stream = tokio::net::TcpStream::connect(self.data_addr).await?;
        stream.set_nodelay(true)?;
        remote::configure_tcp_buffers(&stream, self.network_profile);
        // wrap with TLS if configured
        let recv_stream = if let Some(ref connector) = self.tls_connector {
            let server_name =
                rustls::pki_types::ServerName::try_from("rcp").expect("'rcp' is a valid DNS name");
            let tls_stream = connector
                .connect(server_name, stream)
                .await
                .context("TLS handshake failed for data connection")?;
            let (read_half, _write_half) = tokio::io::split(tls_stream);
            remote::streams::RecvStream::new(Box::new(read_half) as remote::streams::BoxedRead)
        } else {
            let (read_half, _write_half) = stream.into_split();
            remote::streams::RecvStream::new(Box::new(read_half) as remote::streams::BoxedRead)
        };
        Ok((recv_stream, permit))
    }
    fn close(&self) {
        self.semaphore.close();
    }
}

/// Stream state after a file processing error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamState {
    /// No data was read yet - drain `file_header.size` bytes to recover.
    NeedsDrain,
    /// All data was consumed successfully (e.g., metadata error after full read).
    /// Stream is at a clean boundary and can continue with the next file.
    DataConsumed,
    /// Stream is corrupted (mid-read error) - position unknown, must close.
    Corrupted,
}

/// Error from processing a single file, with stream recovery information.
struct ProcessFileError {
    /// The underlying error.
    source: anyhow::Error,
    /// Stream state after this error - determines how caller should proceed.
    stream_state: StreamState,
}

/// Process a single file from the stream.
///
/// On success, all `file_header.size` bytes have been consumed.
/// On error, check `stream_state`:
/// - `NeedsDrain`: no data was read yet, drain `file_header.size` bytes to recover
/// - `DataConsumed`: all data consumed, stream at clean boundary, can continue
/// - `Corrupted`: mid-read error, stream position unknown, must close
#[instrument(skip(file_recv_stream))]
async fn process_single_file(
    settings: &common::copy::Settings,
    preserve: &common::preserve::Settings,
    file_recv_stream: &mut remote::streams::BoxedRecvStream,
    file_header: &remote::protocol::File,
) -> Result<(), ProcessFileError> {
    let prog = progress();
    // errors before we start reading data - stream can be recovered by draining
    let err_needs_drain = |e: anyhow::Error| ProcessFileError {
        source: e,
        stream_state: StreamState::NeedsDrain,
    };
    // errors during data transfer - stream position unknown, corrupted
    let err_corrupted = |e: anyhow::Error| ProcessFileError {
        source: e,
        stream_state: StreamState::Corrupted,
    };
    // errors after all data consumed (e.g., metadata) - stream at clean boundary
    let err_data_consumed = |e: anyhow::Error| ProcessFileError {
        source: e,
        stream_state: StreamState::DataConsumed,
    };
    // check if destination exists and handle overwrite logic
    let dst_exists = tokio::fs::symlink_metadata(&file_header.dst).await.is_ok();
    if dst_exists {
        if settings.overwrite {
            tracing::debug!("file exists, check if it's identical");
            let dst_metadata = tokio::fs::symlink_metadata(&file_header.dst)
                .await
                .map_err(|e| err_needs_drain(e.into()))?;
            let is_file = dst_metadata.is_file();
            if is_file {
                let src_file_metadata = remote::protocol::FileMetadata {
                    metadata: &file_header.metadata,
                    size: file_header.size,
                };
                let same_metadata = common::filecmp::metadata_equal(
                    &settings.overwrite_compare,
                    &src_file_metadata,
                    &dst_metadata,
                );
                if same_metadata {
                    tracing::debug!("file is identical, skipping");
                    prog.files_unchanged.inc();
                    // drain this file's data without writing - if this fails, stream is corrupted
                    let mut sink = tokio::io::sink();
                    file_recv_stream
                        .copy_exact_to_buffered(&mut sink, file_header.size, 8192)
                        .await
                        .map_err(err_corrupted)?;
                    return Ok(());
                }
                tracing::debug!("file exists but is different, removing");
                tokio::fs::remove_file(&file_header.dst)
                    .await
                    .map_err(|e| err_needs_drain(e.into()))?;
            } else {
                tracing::info!("destination is not a file, removing");
                common::rm::rm(
                    common::get_progress(),
                    &file_header.dst,
                    &common::rm::Settings {
                        fail_early: settings.fail_early,
                    },
                )
                .await
                .map_err(|err| {
                    err_needs_drain(anyhow::anyhow!("Failed to remove destination: {err}"))
                })?;
            }
        } else {
            return Err(err_needs_drain(anyhow::anyhow!(
                "destination {:?} already exists, did you intend to specify --overwrite?",
                file_header.dst
            )));
        }
    }
    throttle::get_file_iops_tokens(settings.chunk_size, file_header.size)
        .instrument(tracing::trace_span!(
            "iops_throttle",
            size = file_header.size
        ))
        .await;
    let mut file = tokio::fs::File::create(&file_header.dst)
        .instrument(tracing::trace_span!("file_create"))
        .await
        .map_err(|e| err_needs_drain(e.into()))?;
    // buffer size is set by tcp_config.effective_remote_copy_buffer_size() based on network profile,
    // but capped at file size to avoid over-allocation for small files
    let file_size = file_header.size.min(usize::MAX as u64) as usize;
    let buffer_size = settings.remote_copy_buffer_size.min(file_size).max(1);
    // once we start reading from the stream, any error means the stream is corrupted
    let copied = file_recv_stream
        .copy_exact_to_buffered(&mut file, file_header.size, buffer_size)
        .instrument(tracing::trace_span!(
            "recv_data",
            size = file_header.size,
            buffer_size
        ))
        .await
        .map_err(err_corrupted)?;
    if copied != file_header.size {
        return Err(err_corrupted(anyhow::anyhow!(
            "File size mismatch: expected {} bytes, copied {} bytes",
            file_header.size,
            copied
        )));
    }
    // flush before drop to ensure all data reaches the kernel before we set metadata.
    // tokio::fs::File hands writes to a threadpool - without flush, the threadpool
    // may complete after we set mtime, causing the file to appear modified.
    file.flush()
        .await
        .map_err(|e| err_data_consumed(e.into()))?;
    drop(file);
    tracing::info!(
        "File {} -> {} created, size: {} bytes, setting metadata...",
        file_header.src.display(),
        file_header.dst.display(),
        file_header.size
    );
    // metadata errors happen after all bytes consumed - stream is at clean boundary
    common::preserve::set_file_metadata(preserve, &file_header.metadata, &file_header.dst)
        .await
        .map_err(err_data_consumed)?;
    prog.files_copied.inc();
    prog.bytes_copied.add(file_header.size);
    Ok(())
}

/// Handle a stream that may contain multiple files.
///
/// Loops until the stream is closed (EOF on header read).
#[instrument(skip(error_occurred, file_recv_stream, directory_tracker))]
async fn handle_file_stream(
    settings: common::copy::Settings,
    preserve: common::preserve::Settings,
    mut file_recv_stream: remote::streams::BoxedRecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    let prog = progress();
    tracing::info!("Processing file stream (may contain multiple files)");
    // loop until stream closes (EOF on header read)
    loop {
        // try to receive next file header
        let file_header = match file_recv_stream
            .recv_object::<remote::protocol::File>()
            .await?
        {
            Some(h) => h,
            None => {
                // stream closed by source, no more files
                tracing::debug!("Stream closed, no more files");
                break;
            }
        };
        tracing::info!(
            "Received file: {:?} -> {:?} (dir_total_files={})",
            file_header.src,
            file_header.dst,
            file_header.dir_total_files
        );
        // acquire throttle permits for this file
        let _open_file_guard = throttle::open_file_permit()
            .instrument(tracing::trace_span!("open_file_permit"))
            .await;
        throttle::get_ops_token().await;
        let _ops_guard = prog.ops.guard();
        // process this file
        let file_result =
            process_single_file(&settings, &preserve, &mut file_recv_stream, &file_header).await;
        // track whether we need to close the stream and exit early
        let mut stream_corrupted = false;
        let mut fail_early_error: Option<anyhow::Error> = None;
        if let Err(e) = file_result {
            tracing::error!(
                "Failed to handle file {}: {:#}",
                file_header.dst.display(),
                e.source
            );
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            match e.stream_state {
                StreamState::NeedsDrain => {
                    // no data was read yet, drain the file's data to stay in sync
                    let mut sink = tokio::io::sink();
                    if let Err(drain_err) = file_recv_stream
                        .copy_exact_to_buffered(&mut sink, file_header.size, 8192)
                        .await
                    {
                        tracing::error!("Failed to drain file data: {:#}", drain_err);
                        // drain failed, stream is now corrupted
                        stream_corrupted = true;
                    }
                }
                StreamState::DataConsumed => {
                    // all data consumed successfully (e.g., metadata error after full read)
                    // stream is at a clean boundary, can continue with next file
                    tracing::debug!("Error after data consumed, stream still usable");
                }
                StreamState::Corrupted => {
                    // mid-read error, stream position unknown, must close
                    tracing::debug!("Stream corrupted, will close after tracking update");
                    stream_corrupted = true;
                }
            }
            if settings.fail_early {
                fail_early_error = Some(e.source);
            }
        }
        // ALWAYS update directory tracker, even on error
        // this prevents hangs waiting for file counts
        {
            let mut tracker = directory_tracker.lock().await;
            if file_header.is_root {
                tracing::info!(
                    "Root file processed (success={})",
                    fail_early_error.is_none() && !stream_corrupted
                );
                tracker.set_root_complete();
            } else {
                // get parent directory
                let parent_dir = file_header.dst.parent().ok_or_else(|| {
                    anyhow::anyhow!("file {:?} has no parent directory", file_header.dst)
                })?;
                tracker
                    .process_file(parent_dir, file_header.dir_total_files)
                    .await
                    .context("Failed to update directory tracker after receiving file")?;
            }
            // check if we're done after each file - this may send DestinationDone
            if tracker.is_done() {
                tracing::info!(
                    "All operations complete after file processing, sending DestinationDone"
                );
                tracker.send_destination_done().await?;
            }
        }
        // now handle stream corruption or fail-early after tracking is updated
        if stream_corrupted {
            file_recv_stream.close().await;
            // always return error for corrupted stream - protocol is out of sync and
            // remaining files on this stream are lost without tracker updates.
            return Err(fail_early_error.unwrap_or_else(|| {
                anyhow::anyhow!("stream corrupted, remaining files on this stream lost")
            }));
        }
        if let Some(err) = fail_early_error {
            file_recv_stream.close().await;
            return Err(err);
        }
    }
    file_recv_stream.close().await;
    tracing::info!("File stream processing complete");
    Ok(())
}

/// Process incoming files over TCP data connections.
///
/// Opens connections to source's data port and reads file data.
/// Each connection handles multiple files until source closes it (EOF).
#[instrument(skip(error_occurred, data_pool, directory_tracker))]
async fn process_incoming_file_streams_tcp(
    settings: common::copy::Settings,
    preserve: common::preserve::Settings,
    data_pool: std::sync::Arc<DataConnectionPool>,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    let mut join_set = tokio::task::JoinSet::new();
    // spawn worker tasks that open connections and receive files.
    // we spawn exactly N workers for N permits - all workers can be active simultaneously,
    // each handling one file at a time. this is intentional: the semaphore limits concurrent
    // *connections* (and thus concurrent file transfers), not workers. each worker loops:
    // acquire permit -> connect -> receive files until EOF -> release permit.
    for _ in 0..data_pool.semaphore.available_permits() {
        let pool = data_pool.clone();
        let tracker = directory_tracker.clone();
        let error_flag = error_occurred.clone();
        join_set.spawn(async move {
            loop {
                // try to connect to source's data port
                let (recv_stream, _permit) = match pool.connect().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        // pool closed or connection failed
                        tracing::debug!("Data connection ended: {e}");
                        break;
                    }
                };
                // handle one file on this connection
                if let Err(e) = handle_file_stream(
                    settings,
                    preserve,
                    recv_stream,
                    tracker.clone(),
                    error_flag.clone(),
                )
                .await
                {
                    tracing::debug!("File stream handling ended: {e}");
                    break;
                }
                // permit is released when _permit is dropped
            }
            Ok::<(), anyhow::Error>(())
        });
    }
    // wait for all workers to complete
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!("File stream handling task failed: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e);
                }
            }
            Err(e) => {
                tracing::error!("File stream handling task panicked: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e.into());
                }
            }
        }
    }
    join_set.shutdown().await;
    tracing::info!("All file streams completed");
    Ok(())
}

/// Create a directory, handling overwrite logic.
/// Returns Ok(true) if directory was created/exists, Ok(false) if failed.
async fn create_directory(
    settings: &common::copy::Settings,
    dst: &std::path::Path,
) -> anyhow::Result<bool> {
    let prog = progress();
    match tokio::fs::create_dir(dst).await {
        Ok(()) => {
            prog.directories_created.inc();
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            // something exists at destination - check what it is
            let dst_metadata = tokio::fs::symlink_metadata(dst).await?;
            if dst_metadata.is_dir() {
                // directory already exists - reuse it (no overwrite needed for directories)
                tracing::debug!("destination directory already exists, reusing it");
                prog.directories_unchanged.inc();
                Ok(true)
            } else if settings.overwrite {
                // not a directory but overwrite is enabled - remove and create
                tracing::info!("destination is not a directory, removing and creating a new one");
                common::rm::rm(
                    common::get_progress(),
                    dst,
                    &common::rm::Settings {
                        fail_early: settings.fail_early,
                    },
                )
                .await?;
                tokio::fs::create_dir(dst).await?;
                prog.directories_created.inc();
                Ok(true)
            } else {
                // not a directory and overwrite disabled
                tracing::error!(
                    "Destination {dst:?} exists and is not a directory, use --overwrite to replace"
                );
                Ok(false)
            }
        }
        Err(error) => {
            tracing::error!("Failed to create directory {dst:?}: {error}");
            Err(error.into())
        }
    }
}

/// Create a symlink, handling overwrite logic.
async fn create_symlink(
    settings: &common::copy::Settings,
    preserve: &common::preserve::Settings,
    dst: &std::path::Path,
    target: &std::path::Path,
    metadata: &remote::protocol::Metadata,
) -> anyhow::Result<()> {
    let prog = progress();
    match tokio::fs::symlink(target, dst).await {
        Ok(()) => {
            common::preserve::set_symlink_metadata(preserve, metadata, dst).await?;
            prog.symlinks_created.inc();
            Ok(())
        }
        Err(error) if settings.overwrite && error.kind() == std::io::ErrorKind::AlreadyExists => {
            let dst_metadata = tokio::fs::symlink_metadata(dst)
                .await
                .with_context(|| format!("failed reading metadata from dst: {dst:?}"))?;
            if dst_metadata.is_symlink() {
                let dst_link = tokio::fs::read_link(dst)
                    .await
                    .with_context(|| format!("failed reading dst symlink: {dst:?}"))?;
                if *target == dst_link {
                    tracing::debug!(
                        "destination is a symlink and points to the same location as source"
                    );
                    if preserve.symlink.any() {
                        if common::filecmp::metadata_equal(
                            &settings.overwrite_compare,
                            metadata,
                            &dst_metadata,
                        ) {
                            tracing::debug!("destination symlink is identical, skipping");
                            prog.symlinks_unchanged.inc();
                        } else {
                            tracing::debug!("destination metadata is different, updating");
                            common::preserve::set_symlink_metadata(preserve, metadata, dst).await?;
                            prog.symlinks_removed.inc();
                            prog.symlinks_created.inc();
                        }
                    } else {
                        tracing::debug!("destination symlink is identical, skipping");
                        prog.symlinks_unchanged.inc();
                    }
                } else {
                    tracing::info!(
                        "destination is a symlink but points to a different location, removing"
                    );
                    tokio::fs::remove_file(dst).await?;
                    tokio::fs::symlink(target, dst).await?;
                    common::preserve::set_symlink_metadata(preserve, metadata, dst).await?;
                    prog.symlinks_removed.inc();
                    prog.symlinks_created.inc();
                }
            } else {
                tracing::info!("destination is not a symlink, removing");
                common::rm::rm(
                    common::get_progress(),
                    dst,
                    &common::rm::Settings {
                        fail_early: settings.fail_early,
                    },
                )
                .await
                .map_err(|err| anyhow::anyhow!("Failed to remove destination: {err}"))?;
                tokio::fs::symlink(target, dst).await?;
                common::preserve::set_symlink_metadata(preserve, metadata, dst).await?;
                prog.symlinks_created.inc();
            }
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

#[instrument(skip(error_occurred, control_recv_stream, directory_tracker))]
async fn process_control_stream(
    settings: &common::copy::Settings,
    preserve: &common::preserve::Settings,
    mut control_recv_stream: remote::streams::BoxedRecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    while let Some(source_message) = control_recv_stream
        .recv_object::<remote::protocol::SourceMessage>()
        .await
        .context("Failed to receive source message")?
    {
        throttle::get_ops_token().await;
        tracing::debug!("Received source message: {:?}", source_message);
        let prog = progress();
        match source_message {
            remote::protocol::SourceMessage::Directory {
                ref src,
                ref dst,
                ref metadata,
                is_root,
            } => {
                let _ops_guard = prog.ops.guard();
                // check for failed ancestor
                {
                    let tracker = directory_tracker.lock().await;
                    if tracker.has_failed_ancestor(dst) {
                        tracing::warn!("Skipping directory {:?} - ancestor failed to create", dst);
                        continue;
                    }
                }
                // try to create directory
                let created = match create_directory(settings, dst).await {
                    Ok(created) => created,
                    Err(e) => {
                        tracing::error!("Failed to create directory {:?}: {:#}", dst, e);
                        error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                        if settings.fail_early {
                            return Err(e);
                        }
                        false
                    }
                };
                if created {
                    // add to tracker (sends DirectoryCreated)
                    // tracker handles root directory tracking internally
                    directory_tracker
                        .lock()
                        .await
                        .add_directory(src, dst, metadata.clone(), is_root)
                        .await
                        .context("Failed to add directory to tracker")?;
                } else {
                    // mark as failed - descendants will be skipped
                    error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                    let mut tracker = directory_tracker.lock().await;
                    tracker.mark_directory_failed(dst);
                    // if root directory failed, mark root as complete to avoid hang
                    if is_root {
                        tracker.set_root_complete();
                    }
                    if settings.fail_early {
                        return Err(anyhow::anyhow!("Failed to create directory {:?}", dst));
                    }
                }
            }
            remote::protocol::SourceMessage::Symlink {
                ref src,
                ref dst,
                ref target,
                ref metadata,
                is_root,
            } => {
                let _ops_guard = prog.ops.guard();
                // check for failed ancestor
                {
                    let tracker = directory_tracker.lock().await;
                    if tracker.has_failed_ancestor(dst) {
                        tracing::warn!("Skipping symlink {:?} - ancestor failed to create", dst);
                        continue;
                    }
                }
                // create symlink
                let result = create_symlink(settings, preserve, dst, target, metadata).await;
                if let Err(e) = result {
                    tracing::error!("Failed to create symlink {:?} -> {:?}: {:#}", src, dst, e);
                    error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                    if settings.fail_early {
                        return Err(e);
                    }
                }
                // mark root symlink complete
                if is_root {
                    directory_tracker.lock().await.set_root_complete();
                }
            }
            remote::protocol::SourceMessage::DirStructureComplete => {
                tracing::info!("Received DirStructureComplete");
                directory_tracker.lock().await.set_structure_complete();
            }
            remote::protocol::SourceMessage::FileSkipped {
                ref src,
                ref dst,
                dir_total_files,
            } => {
                tracing::info!("File was skipped by source: {:?} -> {:?}", src, dst);
                // get parent directory and update tracker
                let parent_dir = dst
                    .parent()
                    .ok_or_else(|| anyhow::anyhow!("skipped file {:?} has no parent", dst))?;
                directory_tracker
                    .lock()
                    .await
                    .process_file(parent_dir, dir_total_files)
                    .await
                    .context("Failed to update tracker for skipped file")?;
            }
            remote::protocol::SourceMessage::SymlinkSkipped {
                ref src_dst,
                is_root,
            } => {
                tracing::info!(
                    "Symlink was skipped by source: {:?} -> {:?}",
                    src_dst.src,
                    src_dst.dst
                );
                // symlinks don't affect file counts - just log
                // if root symlink failed, mark root as complete to avoid hang
                if is_root {
                    directory_tracker.lock().await.set_root_complete();
                }
            }
            remote::protocol::SourceMessage::DirectoryEmpty { ref src, ref dst } => {
                tracing::info!("Directory is empty: {:?} -> {:?}", src, dst);
                // mark_directory_empty handles both root and non-root directories
                // complete_directory is called internally and will set root_complete if needed
                directory_tracker
                    .lock()
                    .await
                    .mark_directory_empty(dst)
                    .await
                    .context("Failed to mark directory as empty")?;
            }
        }
        // check if we're done after each message
        let mut tracker = directory_tracker.lock().await;
        if tracker.is_done() {
            tracing::info!("All operations complete, sending DestinationDone");
            tracker.send_destination_done().await?;
            break;
        }
    }
    // close recv stream
    control_recv_stream.close().await;
    tracing::info!("Control stream processing completed");
    Ok(())
}

#[instrument(skip(cert_key))]
#[allow(clippy::too_many_arguments)]
pub async fn run_destination(
    src_control_addr: &std::net::SocketAddr,
    src_data_addr: &std::net::SocketAddr,
    _src_server_name: &str,
    settings: &common::copy::Settings,
    preserve: &common::preserve::Settings,
    tcp_config: &remote::TcpConfig,
    cert_key: Option<&remote::tls::CertifiedKey>,
    source_cert_fingerprint: Option<remote::protocol::CertFingerprint>,
) -> anyhow::Result<(String, common::copy::Summary)> {
    // create TLS connector if encryption is enabled (requires both cert and source fingerprint)
    let tls_connector = match (cert_key, source_cert_fingerprint) {
        (Some(cert), Some(source_fp)) => {
            // create client config with client certificate for mutual TLS
            let client_config = remote::tls::create_client_config_with_cert(cert, source_fp)
                .context("failed to create TLS client config")?;
            Some(std::sync::Arc::new(tokio_rustls::TlsConnector::from(
                client_config,
            )))
        }
        _ => None,
    };
    tracing::info!(
        "Destination TLS encryption: {}",
        if tls_connector.is_some() {
            "enabled (mutual TLS)"
        } else {
            "disabled"
        }
    );
    tracing::info!(
        "Connecting to source: control={}, data={}",
        src_control_addr,
        src_data_addr
    );
    // connect to source's control port
    let control_stream =
        remote::connect_tcp_control(*src_control_addr, tcp_config.conn_timeout_sec).await?;
    tracing::info!("Connected to source control port");
    remote::configure_tcp_buffers(&control_stream, tcp_config.network_profile);
    // wrap control connection with TLS if configured
    let (control_send_stream, control_recv_stream) = if let Some(ref connector) = tls_connector {
        // use a dummy server name - we verify via fingerprint, not hostname
        let server_name =
            rustls::pki_types::ServerName::try_from("rcp").expect("'rcp' is a valid DNS name");
        let tls_stream = connector
            .connect(server_name, control_stream)
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
    tracing::info!("Created control streams");
    let directory_tracker = directory_tracker::make_shared(control_send_stream, *preserve);
    let error_occurred = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    // create a pool of data connections to source
    let data_pool = std::sync::Arc::new(DataConnectionPool::new(
        *src_data_addr,
        tcp_config.max_connections,
        tcp_config.network_profile,
        tls_connector,
    ));
    let file_handler_future = process_incoming_file_streams_tcp(
        *settings,
        *preserve,
        data_pool.clone(),
        directory_tracker.clone(),
        error_occurred.clone(),
    );
    let control_future = process_control_stream(
        settings,
        preserve,
        control_recv_stream,
        directory_tracker.clone(),
        error_occurred.clone(),
    );
    tokio::pin!(file_handler_future);
    tokio::pin!(control_future);
    // race both futures - if either completes first, handle it and then wait for the other.
    // CANCEL SAFETY: the "cancelled" future is NOT dropped - it's awaited after select!
    // completes. Both futures are pinned and the winning branch awaits the other, ensuring
    // both run to completion. No work is lost due to cancellation.
    let select_result: anyhow::Result<()> = async {
        tokio::select! {
            file_result = &mut file_handler_future => {
                file_result.context("Failed to process incoming file streams")?;
                control_future.await.context("Failed to process control stream")?;
            }
            control_result = &mut control_future => {
                control_result.context("Failed to process control stream")?;
                file_handler_future.await.context("Failed to process incoming file streams")?;
            }
        }
        Ok(())
    }
    .await;
    // if there was an error, close the control stream properly before returning.
    // this is important for TLS streams which need explicit shutdown to send close_notify.
    if let Err(e) = select_result {
        tracing::info!("Error during operation, closing streams for cleanup");
        directory_tracker.lock().await.close_stream().await;
        data_pool.close();
        return Err(e);
    }
    tracing::info!("Destination is done");
    data_pool.close();
    // build summary from progress counters
    let prog = progress();
    let summary = common::copy::Summary {
        bytes_copied: prog.bytes_copied.get(),
        files_copied: prog.files_copied.get() as usize,
        symlinks_created: prog.symlinks_created.get() as usize,
        directories_created: prog.directories_created.get() as usize,
        files_unchanged: prog.files_unchanged.get() as usize,
        symlinks_unchanged: prog.symlinks_unchanged.get() as usize,
        directories_unchanged: prog.directories_unchanged.get() as usize,
        rm_summary: common::rm::Summary {
            files_removed: prog.files_removed.get() as usize,
            symlinks_removed: prog.symlinks_removed.get() as usize,
            directories_removed: prog.directories_removed.get() as usize,
        },
    };
    if error_occurred.load(std::sync::atomic::Ordering::Relaxed) {
        Err(common::copy::Error {
            source: anyhow::anyhow!("Some operations failed during remote copy"),
            summary,
        }
        .into())
    } else {
        Ok(("destination OK".to_string(), summary))
    }
}
