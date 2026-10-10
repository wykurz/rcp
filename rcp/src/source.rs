mod discovery;
#[cfg(test)]
mod tests;
use anyhow::Context;
use async_recursion::async_recursion;
use futures::TryFutureExt as _;
// trait-only import: brings FileMeta::size()/uid()/... into scope without shadowing std::fs::Metadata
use common::preserve::Metadata as _;
use common::safedir::Dir;
use remote::protocol::ExtendedMetadataCapture;
use std::os::fd::AsFd as _;
use std::sync::Arc;
use tokio::io::AsyncReadExt as _;
use tracing::{Instrument, instrument};

fn progress() -> &'static common::progress::Progress {
    common::get_progress()
}

/// Open the trusted parent prefix of a root operand and return it as a hardened `Dir` plus the
/// operand's final component, so the root file/symlink can be read fd-relative (`O_NOFOLLOW`) — the
/// same trusted-parent + hardened-final-component model the local copy uses. `open_parent_dir`
/// follows symlinks in the prefix (the caller's trust responsibility, per docs/tocttou.md), then
/// the final component is opened/classified `O_NOFOLLOW` below it, so a swap of the root entry in a
/// writable parent is caught at open. This hardens the remote source root the same way nested
/// entries are already hardened by held parent descriptors.
async fn open_root_parent(src: &std::path::Path) -> anyhow::Result<(Arc<Dir>, std::ffi::OsString)> {
    let operand = common::walk::split_root_operand(src).await?;
    let parent = Dir::open_parent_dir(&operand.parent, common::Side::Source)
        .await
        .with_context(|| format!("cannot open parent directory of root operand {src:?}"))?
        .into_tree();
    Ok((Arc::new(parent), operand.name))
}

/// Selects the held parent or explicit dereferencing adapter for a file data open.
enum FileRead {
    /// Open fd-relative from the directory's held fd via `open_file_read(name)`
    /// (TOCTOU-safe: `O_NOFOLLOW` + `S_ISREG`, no path re-resolution).
    Hardened(Arc<Dir>, std::ffi::OsString),
    /// Path-based `File::open(src)`: the `-L`/`--dereference` walk (follows symlinks by design).
    Path,
}

fn source_buffer_capacity(file: &tokio::fs::File, size: u64, configured: usize) -> usize {
    // a buffered fill performs one file read; capacity beyond that read's limit cannot be used
    let file_size = size.min(usize::MAX as u64) as usize;
    configured.min(file_size).min(file.max_buf_size()).max(1)
}

#[instrument(skip(error_collector, control_send_stream, stream_pool, file_read, fatal))]
#[allow(clippy::too_many_arguments)]
async fn send_file_tcp(
    settings: &common::copy::Settings,
    capture: ExtendedMetadataCapture,
    src: &std::path::Path,
    dst: &std::path::Path,
    size: u64,
    is_root: bool,
    stream_pool: std::sync::Arc<AcceptingSendStreamPool>,
    error_collector: &std::sync::Arc<common::error_collector::ErrorCollector>,
    control_send_stream: remote::streams::BoxedSharedSendStream,
    // how to open this file's data: fd-relative (hardened) or by path (`-L`).
    file_read: FileRead,
    fatal: &discovery::Fatal,
) -> anyhow::Result<()> {
    let prog = progress();
    let _ops_guard = prog.ops.guard();
    tracing::debug!("Sending file content for {:?}", src);
    // borrow a stream FIRST to provide backpressure. files are only opened after we have
    // a stream available, which limits memory usage when destination is slow.
    let mut pooled_stream = common::timing_scope!(trace, "source.file.wait_stream")
        .measure(
            stream_pool
                .borrow()
                .instrument(tracing::trace_span!("borrow_stream")),
        )
        .await?;
    // now that we have a stream, acquire file-related resources
    let open_file_guard = common::timing_scope!(trace, "source.file.wait_open")
        .measure(throttle::open_file_permit().instrument(tracing::trace_span!("open_file_permit")))
        .await;
    let admission = open_file_guard.admission();
    // keep the large file future in one pinned place. Async admission/supervision wrappers borrow
    // it instead of embedding it again and pushing each spawned file task over Tokio's boxing limit.
    let transfer = std::pin::pin!(async {
        let _open_file_guard = open_file_guard;
        common::timing_scope!(trace, "source.file.wait_iops")
            .measure(
                throttle::get_file_iops_tokens(settings.chunk_size, size)
                    .instrument(tracing::trace_span!("iops_throttle", size)),
            )
            .await;
        // open the file AFTER borrowing a stream for backpressure. on the hardened path
        // open fd-relative (O_NOFOLLOW + S_ISREG, no path re-resolution) so a concurrent
        // symlink swap can't redirect the read; the path-based open is only for the
        // `-L`/`--dereference` walk (which follows symlinks by design).
        let file_open = common::timing_scope!(trace, "source.file.open");
        let open_result = match &file_read {
            FileRead::Hardened(dir, name) => dir
                .open_file_read(name)
                .instrument(tracing::trace_span!("file_open"))
                .await
                .map(|(file, meta)| {
                    (
                        tokio::fs::File::from_std(file),
                        meta.size(),
                        remote::protocol::Metadata::from(&meta),
                    )
                }),
            FileRead::Path => {
                let src = src.to_owned();
                common::safedir::run_metadata_probed_blocking(
                    common::Side::Source,
                    common::MetadataOp::Stat,
                    move || {
                        let file = std::fs::File::open(src)?; // rcp-toctou-allow: -L path (dereference, documented not hardened)
                        let metadata = file.metadata()?;
                        if !metadata.is_file() {
                            return Err(std::io::Error::other(
                                "source data descriptor is not a regular file",
                            ));
                        }
                        Ok((file, metadata))
                    },
                )
                .instrument(tracing::trace_span!("file_open"))
                .await
                .map(|(file, meta)| {
                    (
                        tokio::fs::File::from_std(file),
                        meta.len(),
                        remote::protocol::Metadata::from(&meta),
                    )
                })
            }
        };
        file_open.finish();
        // read the source ACL from the SAME fd whose bytes are about to be sent (read-side fidelity,
        // docs/tocttou.md): a probe by path could be answered by a different inode than the one being
        // transferred, pairing one file's permissions with another's contents. Files have no default
        // ACL, so only the access one is asked for. Only when the master asked at all — with `f:acl`
        // off this issues no xattr syscall, which is the whole reason the capture field is on the wire.
        // Folded into `open_result` so a failure takes the same accounted path as a failed open: the
        // header has not been sent, so the destination is still owed exactly one entry for this file.
        let open_result = match open_result {
            Ok((file, size, meta)) if capture.file_acl => {
                common::safedir::read_acls_fd(file.as_fd(), common::Side::Source, false)
                    .await
                    .map(|acls| (file, size, meta, Some(acls)))
            }
            Ok((file, size, meta)) => Ok((file, size, meta, None)),
            Err(e) => Err(e),
        };
        let (file, size, metadata, src_acls) = match open_result {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("Failed to read file {src:?} for sending: {e:#}");
                // stream is returned to pool via Drop when pooled_stream goes out of scope
                // for root file copies, failing to open the file is a fatal error -
                // there's nothing else to transfer and the protocol would hang
                let error = anyhow::Error::from(e)
                    .context(format!("failed to read source file {src:?} for sending"));
                if is_root {
                    return Err(error);
                }
                if settings.fail_early {
                    return Err(error);
                }
                let skip_msg = remote::protocol::SourceMessage::FileSkipped {
                    src: src.to_path_buf(),
                    dst: dst.to_path_buf(),
                };
                if let Err(skip_error) = control_send_stream
                    .lock()
                    .await
                    .send_control_message(&skip_msg)
                    .await
                {
                    return Err(error.context(format!(
                        "failed to report skipped source file {src:?}: {skip_error:#}"
                    )));
                }
                // destination completion drains this task before the collector is inspected, so its
                // owned filesystem cause survives a failed skip without borrowing an older error.
                error_collector.push(error);
                return Ok(());
            }
        };
        // both adapters derive the header from the opened data descriptor, so a compatible
        // replacement contributes its own size, permissions, ownership, and timestamps
        // attach the ACLs read above (both branches: they came from the data fd either way).
        let metadata = match &src_acls {
            Some(acls) => metadata.with_acls(acls),
            None => metadata,
        };
        let buffer_size = source_buffer_capacity(&file, size, settings.remote_copy_buffer_size);
        let mut buffered_file = tokio::io::BufReader::with_capacity(buffer_size, file);
        let file_header = remote::protocol::File {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            size,
            metadata,
            is_root,
        };
        let send = common::timing_scope!(trace, "source.file.send");
        let send_result = async {
            let bytes_sent = pooled_stream
                .stream_mut()
                .send_message_with_data_buffered(&file_header, &mut (&mut buffered_file).take(size))
                .await?;
            if bytes_sent != size {
                let error = std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "source payload ended after {bytes_sent} bytes; header promised {size}"
                    ),
                );
                return Err(error.into());
            }
            anyhow::Ok(())
        }
        .instrument(tracing::trace_span!("send_data", size, buffer_size))
        .map_err(|error| {
            let error = error.context(format!("failed to send source file {src:?} to {dst:?}"));
            tracing::error!("Failed to send file content for {src:?}: {error:#}");
            error
        })
        .await;
        send.finish();
        send_result?;
        pooled_stream.finish_message();
        // stream is returned to pool when pooled_stream is dropped
        prog.files_copied.inc();
        prog.bytes_copied.add(size);
        tracing::info!("Sent file: {:?} -> {:?}", src, dst);
        Ok(())
    });
    // the caught body borrows the stream guard. Publish every error or panic before dropping
    // a poisoned stream can make the peer close control and replace the original cause.
    let admitted = std::pin::pin!(common::safedir::with_fd_admission(admission, transfer));
    discovery::supervise(fatal, admitted).await
}

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
    /// The caller owns its shutdown token, fatal state, and tracked accept task.
    ///
    /// The shutdown token should be cancelled to signal the pool to close. It can be
    /// cloned and shared between multiple tasks - any clone can trigger shutdown.
    #[allow(clippy::too_many_arguments)]
    fn new(
        data_listener: tokio::net::TcpListener,
        pool_size: usize,
        profile: remote::NetworkProfile,
        keepalive_sec: u64,
        conn_timeout_sec: u64,
        tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
        shutdown_token: PoolShutdownToken,
        fatal: Arc<discovery::Fatal>,
        tasks: &mut tokio::task::JoinSet<anyhow::Result<()>>,
    ) -> Self {
        let (send_tx, recv) = async_channel::bounded(pool_size);
        let (return_tx, return_rx) =
            async_channel::bounded::<remote::streams::BoxedSendStream>(pool_size);
        let shutdown_token_clone = shutdown_token;
        // bound each data-connection TLS accept: this handshake runs INLINE in the accept loop below,
        // so a destination that connects TCP then stalls the handshake would otherwise block ALL
        // further data connections (a hang, not just a lost connection).
        let accept_tls_timeout = std::time::Duration::from_secs(conn_timeout_sec);
        // spawn task to accept data connections and manage pool
        common::task_scope::spawn_tracked(tasks, async move {
            // retain channels outside the supervised body: panic publication must precede a
            // sender drop that wakes blocked borrowers with a generic pool-closed error
            discovery::supervise(&fatal, async {
            // wrap the main loop so we can handle shutdown
            tokio::select! {
                result = async {
                    loop {
                        tokio::select! {
                            // accept new connections from destination (the helper applies the
                            // Data socket options — no TCP_USER_TIMEOUT, see configure_tcp_socket)
                            result = remote::accept_tcp_data(&data_listener, profile, keepalive_sec) => {
                                match result {
                                    Ok((stream, addr)) => {
                                        tracing::debug!("Accepted data connection from {}", addr);
                                        // Wrap with TLS if configured. This handshake runs INLINE in
                                        // the accept loop, so its bound is what stops one stalled
                                        // peer from blocking every further data connection. Only the
                                        // write half is kept (the destination never sends here), and
                                        // a failure drops just this connection.
                                        let send_stream = match remote::tls::accept_bounded(
                                            tls_acceptor.as_deref(),
                                            stream,
                                            accept_tls_timeout,
                                            "data",
                                        ).await {
                                            Ok((send_stream, _recv_stream)) => send_stream,
                                            Err(e) => {
                                                tracing::warn!("Dropping data connection: {:#}", &e);
                                                continue;
                                            }
                                        };
                                        if send_tx.send(send_stream).await.is_err() {
                                            tracing::debug!("Pool closed, stopping accept loop");
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        return Err(e.into());
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
                    anyhow::Ok(())
                } => { result?; }
                // shutdown signal received - close all streams
                _ = shutdown_token_clone.cancelled() => {
                    tracing::debug!("Pool shutdown signal received");
                }
            }
            // drain and close all streams in the pool so destination sees EOF
            // close the sender to stop any pending borrows
            send_tx.close();
            // drain streams from the return channel (streams being returned by workers)
            while let Ok(stream) = return_rx.try_recv() {
                drop(stream);
            }
            return_rx.close();
            tracing::debug!("Pool accept task completed, all streams closed");
            Ok(())
            }).await
        });
        Self { recv, return_tx }
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
            reusable: true,
        })
    }
}

/// RAII guard that returns the connection to the pool on drop.
/// Connections are reused for multiple files via length-prefixed framing.
struct PooledAcceptedSendStream {
    stream: Option<remote::streams::BoxedSendStream>,
    reusable: bool,
    return_tx: async_channel::Sender<remote::streams::BoxedSendStream>,
}

impl PooledAcceptedSendStream {
    fn stream_mut(&mut self) -> &mut remote::streams::BoxedSendStream {
        // any failure or cancellation must discard its partial frame; only successful completion
        // below makes the stream reusable again
        self.reusable = false;
        self.stream.as_mut().expect("stream already taken")
    }

    fn finish_message(&mut self) {
        self.reusable = true;
    }
}

impl Drop for PooledAcceptedSendStream {
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take()
            && self.reusable
        {
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
    capture: ExtendedMetadataCapture,
    src: &std::path::Path,
    dst: &std::path::Path,
    tcp_config: &remote::TcpConfig,
    concurrency: remote::ResolvedRemoteConcurrency,
    admission: common::EndpointAdmission,
    files_source: common::FilesInFlightSource,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
    tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
) -> anyhow::Result<()> {
    tracing::info!("Destination control connection established");
    let pool_size = concurrency.max_connections().get();
    let max_pending_files = concurrency.max_pending_files().get();
    // the control stream's socket options (no-delay, buffers, liveness) were applied by the caller
    // when it accepted the connection
    // wrap control connection with TLS if configured; the handshake is bounded because a peer that
    // establishes TCP then stalls it would otherwise hang the source here indefinitely, before any
    // teardown state exists
    let (control_send_stream, mut control_recv_stream) = remote::tls::accept_bounded(
        tls_acceptor.as_deref(),
        control_stream,
        std::time::Duration::from_secs(tcp_config.conn_timeout_sec),
        "control",
    )
    .await?;
    let receiver_limits = tokio::time::timeout(
        std::time::Duration::from_secs(tcp_config.conn_timeout_sec),
        control_recv_stream.recv_object::<remote::protocol::DirectoryLimits>(),
    )
    .await
    .context("timed out receiving destination directory limits")??
    .context("destination closed before directory limits")?;
    // wrap in Arc<Mutex<>> for shared access
    let control_send_stream = std::sync::Arc::new(tokio::sync::Mutex::new(control_send_stream));
    tracing::info!("Created control streams for directory transfer");
    // create a pool that accepts data connections from destination and provides SendStreams
    let mut accept_tasks = tokio::task::JoinSet::new();
    let pool_shutdown = PoolShutdownToken::new();
    let fatal = Arc::new(discovery::Fatal::new(pool_shutdown.clone()));
    let stream_pool = AcceptingSendStreamPool::new(
        data_listener,
        pool_size,
        tcp_config.network_profile,
        tcp_config.keepalive_sec,
        tcp_config.conn_timeout_sec,
        tls_acceptor,
        pool_shutdown.clone(),
        fatal.clone(),
        &mut accept_tasks,
    );
    let _pool_guard = pool_shutdown.clone().drop_guard();
    let stream_pool = std::sync::Arc::new(stream_pool);
    tracing::info!(
        "Created accepting send stream pool with {} slots",
        pool_size
    );
    let mut result = discovery::run(
        settings.clone(),
        capture,
        remote::protocol::SrcDst {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
        },
        control_send_stream,
        control_recv_stream,
        stream_pool,
        fatal.clone(),
        pool_size,
        max_pending_files,
        admission,
        files_source,
        receiver_limits,
        error_collector,
    )
    .await;
    pool_shutdown.cancel();
    accept_tasks.abort_all();
    while let Some(accepted) = accept_tasks.join_next().await {
        let error = match accepted {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(error) if !error.is_cancelled() => Some(error.into()),
            Err(_) => None,
        };
        if result.is_ok()
            && let Some(error) = error
        {
            result = Err(fatal.take().unwrap_or(error));
        }
    }
    result
}

/// Traverse filesystem and report dry-run entries via tracing — `-L`/`--dereference` ONLY.
/// This path-based reporter follows symlinks by request (documented not hardened); the
/// default path uses the fd-relative [`dry_run_traverse_fd`].
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn dry_run_traverse(
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    source_root: &std::path::Path,
    is_root: bool,
    dry_run_mode: common::config::DryRunMode,
    summary: &mut common::copy::Summary,
) -> anyhow::Result<()> {
    let src_metadata = match common::walk::run_metadata_probed(
        common::Side::Source,
        common::MetadataOp::Stat,
        async {
            if settings.dereference {
                tokio::fs::metadata(src).await
            } else {
                tokio::fs::symlink_metadata(src).await
            }
        },
    )
    .await
    {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("Failed reading metadata from src {src:?}: {e:#}");
            if settings.fail_early || is_root {
                return Err(e.into());
            }
            return Ok(());
        }
    };
    let is_dir = src_metadata.is_dir();
    // apply filter - use should_include_root_item for root items
    // (anchored patterns match paths inside the source, not the source itself)
    let filter_result = if let Some(ref filter) = settings.filter {
        if is_root {
            let file_name = src.file_name().map(std::path::Path::new).unwrap_or(src);
            filter.should_include_root_item(file_name, is_dir)
        } else {
            let relative_path = src.strip_prefix(source_root).unwrap_or(src);
            filter.should_include(relative_path, is_dir)
        }
    } else {
        common::filter::FilterResult::Included
    };
    let should_process = matches!(filter_result, common::filter::FilterResult::Included);
    let skip_reason = common::dry_run::format_skip_reason(&filter_result);
    // determine if we should report this entry based on dry-run mode
    let should_report = match dry_run_mode {
        common::config::DryRunMode::Brief => should_process,
        common::config::DryRunMode::All | common::config::DryRunMode::Explain => true,
    };
    // helper to format status for output
    let format_status = |process: bool, reason: &Option<String>| -> String {
        if process {
            "would copy".to_string()
        } else if matches!(dry_run_mode, common::config::DryRunMode::Explain) {
            format!("skip ({})", reason.as_deref().unwrap_or("filtered"))
        } else {
            "skip".to_string()
        }
    };
    if src_metadata.is_file() {
        if should_report {
            let size = src_metadata.len();
            tracing::info!(
                target: "dry_run",
                "{}: {:?} -> {:?} [file ({})]",
                format_status(should_process, &skip_reason),
                src,
                dst,
                bytesize::ByteSize(size)
            );
        }
        if should_process {
            summary.files_copied += 1;
            summary.bytes_copied += src_metadata.len();
        } else {
            summary.files_skipped += 1;
            progress().files_skipped.inc();
        }
        return Ok(());
    }
    if src_metadata.is_symlink() {
        let target = match common::walk::run_metadata_probed(
            common::Side::Source,
            common::MetadataOp::ReadLink,
            tokio::fs::read_link(src), // rcp-toctou-allow: -L path (dereference, documented not hardened)
        )
        .await
        {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Failed reading symlink {src:?}: {e:#}");
                if settings.fail_early {
                    return Err(e.into());
                }
                return Ok(());
            }
        };
        if should_report {
            tracing::info!(
                target: "dry_run",
                "{}: {:?} -> {:?} [symlink -> {:?}]",
                format_status(should_process, &skip_reason),
                src,
                dst,
                target
            );
        }
        if should_process {
            summary.symlinks_created += 1;
        } else {
            summary.symlinks_skipped += 1;
            progress().symlinks_skipped.inc();
        }
        return Ok(());
    }
    if !src_metadata.is_dir() {
        // special file (socket, FIFO, device)
        if !should_process {
            // filtered out by include/exclude - count as files_skipped (matching local copy)
            summary.files_skipped += 1;
            progress().files_skipped.inc();
        } else if settings.skip_specials {
            if should_report {
                tracing::info!(
                    target: "dry_run",
                    "skip (special file): {:?} -> {:?} [type: {:?}]",
                    src,
                    dst,
                    src_metadata.file_type()
                );
            }
            summary.specials_skipped += 1;
            progress().specials_skipped.inc();
        } else {
            // without --skip-specials, real copy would error on this file type
            let err = anyhow::anyhow!(
                "dry-run: {:?} -> {:?} unsupported file type: {:?}",
                src,
                dst,
                src_metadata.file_type()
            );
            tracing::error!("{:#}", &err);
            if settings.fail_early {
                return Err(err);
            }
        }
        return Ok(());
    }
    // directory
    if should_report {
        tracing::info!(
            target: "dry_run",
            "{}: {:?} -> {:?} [dir]",
            format_status(should_process, &skip_reason),
            src,
            dst
        );
    }
    // if filtered out, check whether to stop or still traverse
    if !should_process {
        match &filter_result {
            // explicitly excluded by pattern - never traverse (excludes are absolute)
            common::filter::FilterResult::ExcludedByPattern(_) => {
                summary.directories_skipped += 1;
                progress().directories_skipped.inc();
                return Ok(());
            }
            // no include pattern matched - traverse only if could contain matches
            common::filter::FilterResult::ExcludedByDefault => {
                if let Some(ref filter) = settings.filter {
                    let relative_path = if is_root {
                        src.file_name().map(std::path::Path::new).unwrap_or(src)
                    } else {
                        src.strip_prefix(source_root).unwrap_or(src)
                    };
                    let mut should_traverse = false;
                    for pattern in &filter.includes {
                        if filter.could_contain_matches(relative_path, pattern) {
                            should_traverse = true;
                            break;
                        }
                    }
                    if !should_traverse {
                        summary.directories_skipped += 1;
                        progress().directories_skipped.inc();
                        return Ok(());
                    }
                    // will traverse looking for matches - defer created/skipped decision
                } else {
                    summary.directories_skipped += 1;
                    progress().directories_skipped.inc();
                    return Ok(());
                }
            }
            // included - will be processed, continue to recurse
            common::filter::FilterResult::Included => {}
        }
    }
    // save current counts before recursing to detect if anything was added
    let before_files = summary.files_copied;
    let before_symlinks = summary.symlinks_created;
    let before_dirs = summary.directories_created;
    if should_process {
        summary.directories_created += 1;
    }
    // recurse into children
    let mut entries = match tokio::fs::read_dir(src).await {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("Cannot open directory {src:?} for reading: {e:#}");
            if settings.fail_early {
                return Err(e.into());
            }
            return Ok(());
        }
    };
    loop {
        match common::walk::next_entry_probed(&mut entries, common::Side::Source, || {
            format!("failed traversing src directory {:?}", &src)
        })
        .await
        {
            Ok(Some((entry, _file_type))) => {
                let entry_path = entry.path();
                let entry_name = entry_path.file_name().unwrap();
                let dst_path = dst.join(entry_name);
                if let Err(e) = dry_run_traverse(
                    settings,
                    &entry_path,
                    &dst_path,
                    source_root,
                    false,
                    dry_run_mode,
                    summary,
                )
                .await
                {
                    tracing::error!("Failed to traverse {entry_path:?}: {e:#}");
                    if settings.fail_early {
                        return Err(e);
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::error!("Failed traversing src directory {src:?}: {e:#}");
                if settings.fail_early {
                    return Err(e);
                }
                break;
            }
        }
    }
    // after recursing, check if anything was added inside this directory.
    // if nothing was added AND this directory doesn't directly match an include pattern,
    // we should not count it (it was only traversed to look for potential matches).
    // the root directory is never uncounted — it's the user-specified source.
    if !is_root {
        let child_content_added = summary.files_copied > before_files
            || summary.symlinks_created > before_symlinks
            || summary.directories_created > before_dirs + if should_process { 1 } else { 0 };
        if should_process {
            // directly matched directory: un-count if nothing was added and not
            // directly matched by an include pattern
            if !child_content_added && let Some(filter) = &settings.filter {
                let relative_path = src.strip_prefix(source_root).unwrap_or(src);
                if !filter.directly_matches_include(relative_path, true) {
                    summary.directories_created -= 1;
                }
            }
        } else {
            // traversed-only directory: promote to created if descendants matched,
            // otherwise count as skipped
            if child_content_added {
                summary.directories_created += 1;
            } else {
                summary.directories_skipped += 1;
                progress().directories_skipped.inc();
            }
        }
    }
    Ok(())
}

/// Where [`dry_run_traverse_fd`] classifies and opens the CURRENT entry from.
///
/// Nested entries and the strict-mode root are always `Opened` (fd-relative, the hardened
/// shape). The default-path root is `Lazy`: classified by path stat, its parent opened only
/// when traversal actually proceeds past the root filter — so an excluded root under an
/// execute-only (0111, searchable-not-readable) parent skips cleanly (the O_RDONLY parent
/// open would fail EACCES there), matching the local copy's root-filter behavior and the
/// historical path-based dry run. An INCLUDED root still requires the parent open, exactly
/// like the real remote copy this dry run previews.
enum DryRunDirSource {
    Opened(Arc<Dir>),
    /// The operand's parent path, opened on demand (default-path root only).
    Lazy(std::path::PathBuf),
}

/// Traverse and report dry-run entries via tracing, fd-relative — the default (non-`-L`) path.
///
/// The hardened twin of [`dry_run_traverse`]: the entry is classified via its parent's held fd
/// (`child(name)`, `O_NOFOLLOW`), a symlink's target is read inode-exact off the classified
/// handle, and directories are enumerated through their own opened fd — the walk never
/// re-resolves a multi-component path, so a concurrent swap cannot make the dry run report
/// names, sizes, or targets from outside the source tree (the same shape as the real copy's
/// hardened walk; this one only *reports*). The sole exception is the default-path root
/// (see [`DryRunDirSource::Lazy`]). `src` is the display path for reporting/filters.
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn dry_run_traverse_fd(
    settings: &common::copy::Settings,
    parent: &DryRunDirSource,
    name: &std::ffi::OsStr,
    src: &std::path::Path,
    dst: &std::path::Path,
    source_root: &std::path::Path,
    is_root: bool,
    dry_run_mode: common::config::DryRunMode,
    summary: &mut common::copy::Summary,
) -> anyhow::Result<()> {
    // classify the entry: fd-relative via the held parent fd, or — for the default-path
    // root only (`Lazy`) — by path stat, so a root the filter excludes never requires
    // read permission on its parent
    let (kind, size, handle) = match parent {
        DryRunDirSource::Opened(dir) => match dir.child(name).await {
            Ok(handle) => (handle.kind(), handle.meta().size(), Some(handle)),
            Err(e) => {
                tracing::error!("Failed reading metadata from src {src:?}: {e:#}");
                if settings.fail_early || is_root {
                    return Err(e.into());
                }
                return Ok(());
            }
        },
        DryRunDirSource::Lazy(_) => {
            match common::walk::run_metadata_probed(
                common::Side::Source,
                common::MetadataOp::Stat,
                tokio::fs::symlink_metadata(src),
            )
            .await
            {
                Ok(md) => (common::walk::EntryKind::from_metadata(&md), md.len(), None),
                Err(e) => {
                    tracing::error!("Failed reading metadata from src {src:?}: {e:#}");
                    if settings.fail_early || is_root {
                        return Err(e.into());
                    }
                    return Ok(());
                }
            }
        }
    };
    let is_dir = kind == common::walk::EntryKind::Dir;
    // apply filter - use should_include_root_item for root items
    // (anchored patterns match paths inside the source, not the source itself)
    let filter_result = if let Some(ref filter) = settings.filter {
        if is_root {
            let file_name = src.file_name().map(std::path::Path::new).unwrap_or(src);
            filter.should_include_root_item(file_name, is_dir)
        } else {
            let relative_path = src.strip_prefix(source_root).unwrap_or(src);
            filter.should_include(relative_path, is_dir)
        }
    } else {
        common::filter::FilterResult::Included
    };
    let should_process = matches!(filter_result, common::filter::FilterResult::Included);
    let skip_reason = common::dry_run::format_skip_reason(&filter_result);
    // determine if we should report this entry based on dry-run mode
    let should_report = match dry_run_mode {
        common::config::DryRunMode::Brief => should_process,
        common::config::DryRunMode::All | common::config::DryRunMode::Explain => true,
    };
    // helper to format status for output
    let format_status = |process: bool, reason: &Option<String>| -> String {
        if process {
            "would copy".to_string()
        } else if matches!(dry_run_mode, common::config::DryRunMode::Explain) {
            format!("skip ({})", reason.as_deref().unwrap_or("filtered"))
        } else {
            "skip".to_string()
        }
    };
    // A default-path Lazy ROOT that will be COPIED (INCLUDED) opens its parent HERE — for every
    // kind (file/symlink/special/dir) — matching the real remote copy, which opens the source
    // parent to read the root. So an execute-only (0111) parent, or a nonexistent one, fails the
    // dry run identically (exit 1) instead of succeeding where the real copy would not. An EXCLUDED
    // root was never materialized, preserving the skip-without-parent-read behavior; nested entries
    // and the strict-mode root are already Opened. Held for the Dir arm's enumeration below.
    let opened_parent: Option<Arc<Dir>> = match parent {
        DryRunDirSource::Opened(dir) => Some(dir.clone()),
        DryRunDirSource::Lazy(parent_path) if should_process => Some(Arc::new(
            Dir::open_parent_dir(parent_path, common::Side::Source)
                .await
                .with_context(|| {
                    format!("cannot open parent directory of dry-run root {parent_path:?}")
                })?
                .into_tree(),
        )),
        DryRunDirSource::Lazy(_) => None,
    };
    match kind {
        common::walk::EntryKind::File => {
            if should_report {
                tracing::info!(
                    target: "dry_run",
                    "{}: {:?} -> {:?} [file ({})]",
                    format_status(should_process, &skip_reason),
                    src,
                    dst,
                    bytesize::ByteSize(size)
                );
            }
            if should_process {
                summary.files_copied += 1;
                summary.bytes_copied += size;
            } else {
                summary.files_skipped += 1;
                progress().files_skipped.inc();
            }
            Ok(())
        }
        common::walk::EntryKind::Symlink => {
            // target read inode-exact off the classified handle (read-side fidelity); the
            // default-path ROOT (`Lazy`, no handle) reads by path like the historical dry
            // run — the operand itself sits at the trusted boundary, and every nested
            // entry is read via the fd walk
            let target = match &handle {
                Some(handle) => handle
                    .read_symlink(common::Side::Source)
                    .await
                    .map(|(target, _meta)| target),
                None => {
                    common::walk::run_metadata_probed(
                        common::Side::Source,
                        common::MetadataOp::ReadLink,
                        tokio::fs::read_link(src), // rcp-toctou-allow: default-path dry-run ROOT only (Lazy operand at the trusted boundary); nested entries read inode-exact via the fd walk
                    )
                    .await
                }
            };
            let target = match target {
                Ok(target) => target,
                Err(e) => {
                    tracing::error!("Failed reading symlink {src:?}: {e:#}");
                    if settings.fail_early {
                        return Err(e.into());
                    }
                    return Ok(());
                }
            };
            if should_report {
                tracing::info!(
                    target: "dry_run",
                    "{}: {:?} -> {:?} [symlink -> {:?}]",
                    format_status(should_process, &skip_reason),
                    src,
                    dst,
                    target
                );
            }
            if should_process {
                summary.symlinks_created += 1;
            } else {
                summary.symlinks_skipped += 1;
                progress().symlinks_skipped.inc();
            }
            Ok(())
        }
        common::walk::EntryKind::Special => {
            if !should_process {
                // filtered out by include/exclude - count as files_skipped (matching local copy)
                summary.files_skipped += 1;
                progress().files_skipped.inc();
            } else if settings.skip_specials {
                if should_report {
                    tracing::info!(
                        target: "dry_run",
                        "skip (special file): {:?} -> {:?}",
                        src,
                        dst
                    );
                }
                summary.specials_skipped += 1;
                progress().specials_skipped.inc();
            } else {
                // without --skip-specials, real copy would error on this file type — so the dry
                // run must too. A root special is always fatal (matches the real copy's exit 1);
                // a nested one respects --fail-early.
                let err = anyhow::anyhow!(
                    "dry-run: {:?} -> {:?} unsupported (special) file type",
                    src,
                    dst
                );
                tracing::error!("{:#}", &err);
                if settings.fail_early || is_root {
                    return Err(err);
                }
            }
            Ok(())
        }
        common::walk::EntryKind::Dir => {
            if should_report {
                tracing::info!(
                    target: "dry_run",
                    "{}: {:?} -> {:?} [dir]",
                    format_status(should_process, &skip_reason),
                    src,
                    dst
                );
            }
            // if filtered out, check whether to stop or still traverse
            if !should_process {
                match &filter_result {
                    // explicitly excluded by pattern - never traverse (excludes are absolute)
                    common::filter::FilterResult::ExcludedByPattern(_) => {
                        summary.directories_skipped += 1;
                        progress().directories_skipped.inc();
                        return Ok(());
                    }
                    // no include pattern matched - traverse only if could contain matches
                    common::filter::FilterResult::ExcludedByDefault => {
                        if let Some(ref filter) = settings.filter {
                            let relative_path = if is_root {
                                src.file_name().map(std::path::Path::new).unwrap_or(src)
                            } else {
                                src.strip_prefix(source_root).unwrap_or(src)
                            };
                            let mut should_traverse = false;
                            for pattern in &filter.includes {
                                if filter.could_contain_matches(relative_path, pattern) {
                                    should_traverse = true;
                                    break;
                                }
                            }
                            if !should_traverse {
                                summary.directories_skipped += 1;
                                progress().directories_skipped.inc();
                                return Ok(());
                            }
                            // will traverse looking for matches - defer created/skipped decision
                        } else {
                            summary.directories_skipped += 1;
                            progress().directories_skipped.inc();
                            return Ok(());
                        }
                    }
                    // included - will be processed, continue to recurse
                    common::filter::FilterResult::Included => {}
                }
            }
            // save current counts before recursing to detect if anything was added
            let before_files = summary.files_copied;
            let before_symlinks = summary.symlinks_created;
            let before_dirs = summary.directories_created;
            if should_process {
                summary.directories_created += 1;
            }
            // open the directory through the held parent fd (O_NOFOLLOW) and enumerate it
            // via its own fd — a swapped-in symlink fails closed here, never redirects. An
            // INCLUDED root and every Opened source already hold the parent fd
            // (`opened_parent`); a traversed-but-excluded Lazy dir opens its parent here (it is
            // being traversed to look for matches, so parent read is needed, matching the real
            // copy).
            let dir = match &opened_parent {
                Some(parent_dir) => parent_dir.open_dir(name).await,
                None => match parent {
                    DryRunDirSource::Lazy(parent_path) => {
                        match Dir::open_parent_dir(parent_path, common::Side::Source).await {
                            Ok(trusted) => trusted.into_tree().open_dir(name).await,
                            Err(e) => Err(e),
                        }
                    }
                    // Opened always populates opened_parent above; unreachable
                    DryRunDirSource::Opened(_) => unreachable!("Opened sets opened_parent"),
                },
            };
            let enumeration = async {
                let dir = Arc::new(dir?);
                let mut cursor = dir.entries().await?;
                let dir_source = DryRunDirSource::Opened(dir);
                loop {
                    let batch = cursor
                        .next_batch(std::num::NonZeroUsize::new(64).unwrap())
                        .await?;
                    if batch.is_empty() {
                        break;
                    }
                    for entry in batch {
                        let entry_src = src.join(&entry.name);
                        let entry_dst = dst.join(&entry.name);
                        if let Err(error) = dry_run_traverse_fd(
                            settings,
                            &dir_source,
                            &entry.name,
                            &entry_src,
                            &entry_dst,
                            source_root,
                            false,
                            dry_run_mode,
                            summary,
                        )
                        .await
                        {
                            tracing::error!("Failed to traverse {entry_src:?}: {error:#}");
                            if settings.fail_early {
                                return Err(error);
                            }
                        }
                    }
                }
                anyhow::Ok(())
            }
            .await;
            if let Err(error) = enumeration {
                tracing::error!("Cannot enumerate directory {src:?}: {error:#}");
                if settings.fail_early || is_root {
                    return Err(error);
                }
                return Ok(());
            }
            // after recursing, check if anything was added inside this directory.
            // if nothing was added AND this directory doesn't directly match an include pattern,
            // we should not count it (it was only traversed to look for potential matches).
            // the root directory is never uncounted — it's the user-specified source.
            if !is_root {
                let child_content_added = summary.files_copied > before_files
                    || summary.symlinks_created > before_symlinks
                    || summary.directories_created
                        > before_dirs + if should_process { 1 } else { 0 };
                if should_process {
                    // directly matched directory: un-count if nothing was added and not
                    // directly matched by an include pattern
                    if !child_content_added && let Some(filter) = &settings.filter {
                        let relative_path = src.strip_prefix(source_root).unwrap_or(src);
                        if !filter.directly_matches_include(relative_path, true) {
                            summary.directories_created -= 1;
                        }
                    }
                } else {
                    // traversed-only directory: promote to created if descendants matched,
                    // otherwise count as skipped
                    if child_content_added {
                        summary.directories_created += 1;
                    } else {
                        summary.directories_skipped += 1;
                        progress().directories_skipped.inc();
                    }
                }
            }
            Ok(())
        }
    }
}

/// Handle a dry-run connection: traverse, log entries, and complete without transferring data.
/// Destination completes after receiving the empty discovery marker.
async fn handle_dry_run_connection(
    stream: tokio::net::TcpStream,
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    dry_run_mode: common::config::DryRunMode,
    tls_acceptor: Option<std::sync::Arc<tokio_rustls::TlsAcceptor>>,
    conn_timeout_sec: u64,
) -> anyhow::Result<(String, common::copy::Summary)> {
    tracing::info!("Handling dry-run connection");
    // set up TLS if needed; the handshake is bounded like every other (a peer that establishes TCP
    // then stalls would otherwise hang this connection indefinitely)
    let (control_send_stream, mut control_recv_stream) = remote::tls::accept_bounded(
        tls_acceptor.as_deref(),
        stream,
        std::time::Duration::from_secs(conn_timeout_sec),
        "dry-run control",
    )
    .await?;
    let control_send_stream: remote::streams::BoxedSharedSendStream =
        std::sync::Arc::new(tokio::sync::Mutex::new(control_send_stream));
    // traverse and log dry-run entries (output goes via tracing). The default path uses the
    // fd-relative walker — the same hardened shape as the real copy, so a concurrent swap
    // cannot make the dry run report content from outside the source tree, and under strict
    // operand resolution a symlinked operand prefix fails closed at the parent open. `-L`
    // keeps the path-based reporter (follows symlinks by request; documented not hardened).
    let mut summary = common::copy::Summary::default();
    if settings.dereference {
        dry_run_traverse(settings, src, dst, src, true, dry_run_mode, &mut summary).await?;
    } else {
        let operand = common::walk::split_root_operand(src).await?;
        let display = operand.display.clone();
        // strict operand resolution: open the parent EAGERLY (openat2 RESOLVE_NO_SYMLINKS)
        // so a symlinked operand prefix fails closed before anything is reported. On the
        // default path the parent is opened lazily — only if the walk proceeds past the
        // root filter — so an excluded root under an execute-only (0111) parent skips
        // cleanly, matching the local copy's root-filter behavior (see DryRunDirSource).
        let parent = if common::safedir::strict_operand_resolution() {
            let parent = Dir::open_parent_dir(&operand.parent, common::Side::Source)
                .await
                .with_context(|| format!("cannot open parent directory of dry-run source {src:?}"))?
                .into_tree();
            DryRunDirSource::Opened(Arc::new(parent))
        } else {
            DryRunDirSource::Lazy(operand.parent.clone())
        };
        dry_run_traverse_fd(
            settings,
            &parent,
            &operand.name,
            &display,
            dst,
            &display,
            true,
            dry_run_mode,
            &mut summary,
        )
        .await?;
    }
    // tell destination we're done with directory structure (nothing was sent in dry-run)
    {
        let mut stream = control_send_stream.lock().await;
        stream
            .send_control_message(&remote::protocol::SourceMessage::DiscoveryComplete {
                has_root_item: false,
            })
            .await?;
    }
    tracing::info!("Sent DiscoveryComplete, waiting for DestinationDone");
    // wait for destination to acknowledge it's done
    loop {
        match control_recv_stream
            .recv_object::<remote::protocol::DestinationMessage>()
            .await?
        {
            Some(remote::protocol::DestinationMessage::DestinationDone) => {
                tracing::info!("Received DestinationDone");
                break;
            }
            Some(other) => {
                tracing::debug!("Ignoring message during dry-run: {:?}", other);
            }
            None => {
                anyhow::bail!("destination closed control before DestinationDone during dry run");
            }
        }
    }
    // close streams
    control_send_stream.lock().await.close().await.ok();
    tracing::info!("Dry-run complete");
    // print summary
    tracing::info!(
        target: "dry_run",
        "Summary: {} files ({} bytes), {} directories, {} symlinks would be copied",
        summary.files_copied,
        summary.bytes_copied,
        summary.directories_created,
        summary.symlinks_created
    );
    Ok(("dry-run complete".to_string(), summary))
}

#[instrument(skip(master_send_stream, cert_key))]
#[allow(clippy::too_many_arguments)]
pub async fn run_source<W: tokio::io::AsyncWrite + Unpin + Send + 'static>(
    master_send_stream: remote::streams::SharedSendStream<W>,
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &common::copy::Settings,
    // what extended per-entry metadata (ACLs) the master wants read; all-false means this source
    // issues no xattr syscall at all.
    capture: ExtendedMetadataCapture,
    tcp_config: &remote::TcpConfig,
    concurrency: remote::ResolvedRemoteConcurrency,
    admission: common::EndpointAdmission,
    files_source: common::FilesInFlightSource,
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
    let error_collector = std::sync::Arc::new(common::error_collector::ErrorCollector::default());
    let accept_timeout = std::time::Duration::from_secs(tcp_config.conn_timeout_sec);
    // the accept helper applies the Control socket options before returning, so the dry-run path
    // below gets the same configuration as the normal one
    match tokio::time::timeout(
        accept_timeout,
        remote::accept_tcp_control(&control_listener, tcp_config),
    )
    .await
    {
        Ok(Ok((stream, addr))) => {
            tracing::info!("Destination control connection from {}", addr);
            // in dry-run mode, do simplified flow: traverse, log, and tell destination we're done
            if let Some(dry_run_mode) = settings.dry_run {
                return handle_dry_run_connection(
                    stream,
                    settings,
                    src,
                    dst,
                    dry_run_mode,
                    tls_acceptor,
                    tcp_config.conn_timeout_sec,
                )
                .await;
            }
            // normal flow
            handle_connection(
                stream,
                data_listener,
                settings,
                capture,
                src,
                dst,
                tcp_config,
                concurrency,
                admission,
                files_source,
                error_collector.clone(),
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
    // destination is authoritative for copy/unchanged/removed counts, but
    // skip counts are source-side only (destination never encounters skipped items)
    let summary = common::copy::Summary {
        files_skipped: progress().files_skipped.get() as usize,
        symlinks_skipped: progress().symlinks_skipped.get() as usize,
        directories_skipped: progress().directories_skipped.get() as usize,
        specials_skipped: progress().specials_skipped.get() as usize,
        ..Default::default()
    };
    match error_collector.take_error() {
        Some(err) => Err(common::copy::Error {
            source: err,
            summary,
        }
        .into()),
        None => Ok(("source OK".to_string(), summary)),
    }
}
