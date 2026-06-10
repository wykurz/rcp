use anyhow::Context;
use common::safedir::Dir;
use std::ffi::OsStr;
use std::os::fd::AsFd;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tracing::{Instrument, instrument};

use super::directory_tracker;

fn progress() -> &'static common::progress::Progress {
    common::get_progress()
}

/// Resolve the open `Dir` of `dst`'s parent for a fd-relative destination write.
///
/// The destination tracks every created directory's `Dir` in the fd-map, top-down
/// (a parent's `DirectoryCreated` precedes any message for its children), so for a
/// non-root entry the parent is always already tracked. For the root entry — whose
/// parent is the trusted user-specified destination parent and is itself never a
/// tracked directory — the parent is opened once via `open_parent_dir` and cached in
/// the tracker as `root_parent_dir` (so a root *directory* and its later empty-dir
/// cleanup share the same pinned parent fd).
///
/// Returns the parent `Dir` plus the entry's final-component name (validated to be a
/// single component by the fd-relative `Dir` methods). Fails closed if a non-root
/// parent is not tracked (it should always be) — never falls back to a path-based
/// open that a concurrent symlink swap could redirect.
async fn resolve_parent_dir(
    directory_tracker: &directory_tracker::SharedDirectoryTracker,
    dst: &std::path::Path,
    is_root: bool,
) -> anyhow::Result<(Arc<Dir>, std::ffi::OsString)> {
    let parent_path = dst
        .parent()
        .ok_or_else(|| anyhow::anyhow!("destination {:?} has no parent directory", dst))?;
    let name = dst
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("destination {:?} has no file name", dst))?
        .to_owned();
    if is_root {
        // the root's parent is the trusted user-specified destination parent. Open it
        // once and cache it; reuse on a subsequent call (root dir create + cleanup).
        {
            let tracker = directory_tracker.lock().await;
            if let Some(parent) = tracker.root_parent_dir() {
                return Ok((parent, name));
            }
        }
        // the root's parent is the TRUSTED user-specified destination parent prefix; resolve it
        // following symlinks normally (a symlinked destination container must be followed into the
        // real dir). Only entries strictly below the named root are O_NOFOLLOW-hardened.
        let parent = Dir::open_parent_dir(parent_path, common::Side::Destination)
            .await
            .with_context(|| {
                format!("failed opening destination root parent directory {parent_path:?}")
            })?;
        // cross from the trusted parent prefix into the hardened tree (O_NOFOLLOW below here).
        let parent = Arc::new(parent.into_tree());
        directory_tracker
            .lock()
            .await
            .set_root_parent_dir(parent.clone());
        Ok((parent, name))
    } else {
        // non-root: the parent must already be tracked (top-down creation guarantees
        // it). Fail closed if it is missing rather than re-resolving the path.
        let parent = {
            let tracker = directory_tracker.lock().await;
            tracker.get_dir(parent_path)
        };
        let parent = parent.ok_or_else(|| {
            anyhow::anyhow!(
                "parent directory {:?} of {:?} is not tracked (fd-map miss)",
                parent_path,
                dst
            )
        })?;
        Ok((parent, name))
    }
}

/// Pool of outbound TCP connections to source's data port.
///
/// Destination opens connections to source's data port to receive file data.
/// A connection carries MULTIPLE files: each file is length-prefixed by its
/// `File` header (the `size` field delimits its bytes), and a worker keeps
/// reading files from the connection until the source closes the stream (EOF).
/// See `handle_file_stream` and the source-side reuse note in `rcp::source`.
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

/// Drain `size` bytes of a file's data off the stream into a sink, without writing it.
///
/// Used when a file is skipped (already exists, identical, dest-newer) so the next file's header
/// lands at a clean stream boundary. A failure here means the stream position is now unknown.
async fn drain_file_data(
    stream: &mut remote::streams::BoxedRecvStream,
    size: u64,
) -> anyhow::Result<()> {
    let mut sink = tokio::io::sink();
    stream.copy_exact_to_buffered(&mut sink, size, 8192).await?;
    Ok(())
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
#[instrument(skip(file_recv_stream, dst_parent))]
async fn process_single_file(
    settings: &common::copy::Settings,
    preserve: &common::preserve::Settings,
    file_recv_stream: &mut remote::streams::BoxedRecvStream,
    file_header: &remote::protocol::File,
    dst_parent: &Arc<Dir>,
    dst_name: &OsStr,
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
    // classify any existing destination entry through the parent's pinned fd (O_NOFOLLOW),
    // never re-resolving file_header.dst by path. handle overwrite/--ignore-existing.
    if let Ok(dst_handle) = dst_parent.child(dst_name).await {
        if settings.ignore_existing {
            tracing::debug!("destination exists, skipping (--ignore-existing)");
            prog.files_unchanged.inc();
            drain_file_data(file_recv_stream, file_header.size)
                .await
                .map_err(err_corrupted)?;
            return Ok(());
        }
        if !settings.overwrite {
            return Err(err_needs_drain(anyhow::anyhow!(
                "destination {:?} already exists, did you intend to specify --overwrite?",
                file_header.dst
            )));
        }
        tracing::debug!("file exists, check if it's identical");
        if dst_handle.kind() == common::walk::EntryKind::File {
            let src_file_metadata = remote::protocol::FileMetadata {
                metadata: &file_header.metadata,
                size: file_header.size,
            };
            if common::filecmp::metadata_equal(
                &settings.overwrite_compare,
                &src_file_metadata,
                dst_handle.meta(),
            ) {
                tracing::debug!("file is identical, skipping");
                prog.files_unchanged.inc();
                drain_file_data(file_recv_stream, file_header.size)
                    .await
                    .map_err(err_corrupted)?;
                return Ok(());
            }
            if let Some(common::copy::OverwriteFilter::Newer) = settings.overwrite_filter
                && common::filecmp::dest_is_newer(&src_file_metadata, dst_handle.meta())
            {
                tracing::debug!("dest is newer than source, skipping");
                prog.files_unchanged.inc();
                drain_file_data(file_recv_stream, file_header.size)
                    .await
                    .map_err(err_corrupted)?;
                return Ok(());
            }
        }
        tracing::debug!("destination differs, removing existing entry");
        // recheck-guarded, fd-relative removal contained to dst_parent (mirrors copy.rs:1.3).
        remove_existing_dst(
            dst_parent,
            dst_name,
            &file_header.dst,
            &dst_handle,
            settings,
        )
        .await
        .map_err(err_needs_drain)?;
    }
    throttle::get_file_iops_tokens(settings.chunk_size, file_header.size)
        .instrument(tracing::trace_span!(
            "iops_throttle",
            size = file_header.size
        ))
        .await;
    // create the destination file fresh through the parent's pinned fd (O_CREAT|O_EXCL|
    // O_NOFOLLOW): never follows a symlink, never escapes dst_parent. the creation mode
    // matches the metadata applier's chmod target, mirroring copy.rs.
    let create_mode = common::preserve::masked_mode(preserve.file.mode_mask, &file_header.metadata);
    let std_file = dst_parent
        .create_file(dst_name, create_mode)
        .await
        .with_context(|| format!("failed creating {:?}", file_header.dst))
        .map_err(err_needs_drain)?;
    // wrap the std file for async writes; the underlying fd is retained so its metadata
    // can be applied through the held fd (no path re-open).
    let mut file = tokio::fs::File::from_std(std_file);
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
    // flush before metadata to ensure all data reaches the kernel before we set mtime.
    // tokio::fs::File hands writes to a threadpool - without flush, the threadpool
    // may complete after we set mtime, causing the file to appear modified.
    file.flush()
        .await
        .map_err(|e| err_data_consumed(e.into()))?;
    tracing::info!(
        "File {} -> {} created, size: {} bytes, setting metadata...",
        file_header.src.display(),
        file_header.dst.display(),
        file_header.size
    );
    // metadata errors happen after all bytes consumed - stream is at clean boundary.
    // apply through the file's OWN fd (fd-relative): no path re-resolution of dst.
    common::safedir::set_file_metadata_fd(
        preserve,
        &file_header.metadata,
        file.as_fd(),
        common::Side::Destination,
    )
    .await
    .with_context(|| format!("failed setting metadata on {:?}", file_header.dst))
    .map_err(err_data_consumed)?;
    drop(file);
    prog.files_copied.inc();
    prog.bytes_copied.add(file_header.size);
    Ok(())
}

/// Remove an existing destination entry (file / symlink / directory) so a fresh entry can take
/// its place, fd-relative and recheck-guarded — the destination counterpart of
/// [`common::copy::remove_existing`].
///
/// The entry was already classified into `dst_handle` (via `dst_parent.child(name)`). Removal is:
/// 1. [`Dir::recheck`] re-opens `name` and confirms it is STILL the same inode (`dev`/`ino`). If a
///    concurrent symlink swap changed the entry's identity, `recheck` returns `ESTALE` and we fail
///    closed, removing nothing.
/// 2. The entry is removed through the held `dst_parent` fd by kind: file/symlink/special via
///    `unlink_at` (never follows a symlink), empty directory via `rmdir_at`, and a non-empty
///    directory subtree via [`common::rm::rm_child`] (fd-relative recursive removal on the held
///    parent). All removal is contained to `dst_parent` — it cannot escape the destination tree.
async fn remove_existing_dst(
    dst_parent: &Arc<Dir>,
    dst_name: &OsStr,
    dst_path: &std::path::Path,
    dst_handle: &common::safedir::Handle,
    settings: &common::copy::Settings,
) -> anyhow::Result<()> {
    let prog = progress();
    // recheck: confirm the entry is still the same inode we classified; fail closed on a swap.
    dst_parent
        .recheck(dst_name, dst_handle)
        .await
        .with_context(|| {
            format!(
                "destination {dst_path:?} changed identity before removal (possible TOCTOU swap)"
            )
        })?;
    match dst_handle.kind() {
        common::walk::EntryKind::File
        | common::walk::EntryKind::Symlink
        | common::walk::EntryKind::Special => {
            let removed_size = {
                use common::preserve::Metadata as _;
                dst_handle.meta().size()
            };
            dst_parent
                .unlink_at(dst_name)
                .await
                .with_context(|| format!("failed removing existing destination {dst_path:?}"))?;
            let is_symlink = dst_handle.kind() == common::walk::EntryKind::Symlink;
            if is_symlink {
                prog.symlinks_removed.inc();
            } else {
                prog.files_removed.inc();
                prog.bytes_removed.add(removed_size);
            }
            Ok(())
        }
        common::walk::EntryKind::Dir => {
            // fast path: an empty directory removes cleanly via rmdir_at.
            match dst_parent.rmdir_at(dst_name).await {
                Ok(()) => {
                    prog.directories_removed.inc();
                    Ok(())
                }
                // POSIX permits either ENOTEMPTY or EEXIST for a non-empty directory.
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::ENOTEMPTY) | Some(libc::EEXIST)
                    ) =>
                {
                    // fd-relative recursive removal of the subtree on the held parent.
                    common::rm::rm_child(
                        common::get_progress(),
                        dst_parent,
                        dst_name,
                        dst_path,
                        &common::rm::Settings {
                            fail_early: settings.fail_early,
                            filter: None,
                            dry_run: None,
                            time_filter: None,
                        },
                    )
                    .await
                    .map(|_summary| ())
                    .map_err(|err| {
                        err.source
                            .context(format!("failed removing existing directory {dst_path:?}"))
                    })
                }
                Err(error) => Err(anyhow::Error::new(error)
                    .context(format!("failed removing existing directory {dst_path:?}"))),
            }
        }
    }
}

/// Handle a stream that may contain multiple files.
///
/// Loops until the stream is closed (EOF on header read).
#[instrument(skip(error_collector, file_recv_stream, directory_tracker))]
async fn handle_file_stream(
    settings: common::copy::Settings,
    preserve: common::preserve::Settings,
    mut file_recv_stream: remote::streams::BoxedRecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
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
            "Received file: {:?} -> {:?}",
            file_header.src,
            file_header.dst
        );
        // acquire throttle permits for this file
        let _open_file_guard = throttle::open_file_permit()
            .instrument(tracing::trace_span!("open_file_permit"))
            .await;
        throttle::get_ops_token().await;
        let _ops_guard = prog.ops.guard();
        // resolve the destination parent directory's held fd from the tracker (for the
        // root file, open the trusted parent via open_parent_dir). all writes for this
        // file are then fd-relative on that pinned parent. a resolution failure is a
        // pre-data error: the stream can be recovered by draining this file's bytes.
        let file_result =
            match resolve_parent_dir(&directory_tracker, &file_header.dst, file_header.is_root)
                .await
            {
                Ok((dst_parent, dst_name)) => {
                    process_single_file(
                        &settings,
                        &preserve,
                        &mut file_recv_stream,
                        &file_header,
                        &dst_parent,
                        &dst_name,
                    )
                    .await
                }
                Err(e) => Err(ProcessFileError {
                    source: e.context("failed resolving destination parent directory"),
                    stream_state: StreamState::NeedsDrain,
                }),
            };
        // track whether we need to close the stream and exit early
        let mut stream_corrupted = false;
        let mut fail_early_error: Option<anyhow::Error> = None;
        if let Err(e) = file_result {
            tracing::error!(
                "Failed to handle file {}: {:#}",
                file_header.dst.display(),
                e.source
            );
            match e.stream_state {
                StreamState::NeedsDrain => {
                    // no data was read yet, drain the file's data to stay in sync
                    if let Err(drain_err) =
                        drain_file_data(&mut file_recv_stream, file_header.size).await
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
            } else {
                error_collector.push(e.source);
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
                    .process_file(parent_dir)
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
#[instrument(skip(error_collector, data_pool, directory_tracker))]
async fn process_incoming_file_streams_tcp(
    settings: common::copy::Settings,
    preserve: common::preserve::Settings,
    data_pool: std::sync::Arc<DataConnectionPool>,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
) -> anyhow::Result<()> {
    let mut join_set = tokio::task::JoinSet::new();
    // spawn worker tasks that open connections and receive files.
    // we spawn exactly N workers for N permits - all workers can be active simultaneously,
    // each handling one file at a time. this is intentional: the semaphore limits concurrent
    // *connections* (and thus concurrent file transfers), not workers. each worker loops:
    // acquire permit -> connect -> receive files until EOF -> release permit.
    let fail_early = settings.fail_early;
    let settings = std::sync::Arc::new(settings);
    let preserve = std::sync::Arc::new(preserve);
    for _ in 0..data_pool.semaphore.available_permits() {
        let pool = data_pool.clone();
        let tracker = directory_tracker.clone();
        let collector = error_collector.clone();
        let settings = settings.clone();
        let preserve = preserve.clone();
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
                // receive files from this connection until the source closes it (EOF)
                if let Err(e) = handle_file_stream(
                    (*settings).clone(),
                    *preserve,
                    recv_stream,
                    tracker.clone(),
                    collector.clone(),
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
                if fail_early {
                    return Err(e);
                }
                error_collector.push(e);
            }
            Err(e) => {
                tracing::error!("File stream handling task panicked: {e}");
                if fail_early {
                    return Err(e.into());
                }
                error_collector.push(e.into());
            }
        }
    }
    join_set.shutdown().await;
    tracing::info!("All file streams completed");
    Ok(())
}

/// Result of directory creation attempt.
///
/// The `Created`/`AlreadyExisted` variants carry the open `Dir` fd for the resolved
/// directory so the caller can store it in the tracker's fd-map (children's writes
/// then resolve relative to it).
enum DirectoryCreateResult {
    /// directory was created by us (new), with its open fd
    Created(Arc<Dir>),
    /// directory already existed (reused), with its open fd
    AlreadyExisted(Arc<Dir>),
    /// skipped due to --ignore-existing (destination is not a directory)
    Skipped,
    /// failed to create directory
    Failed,
}

/// Enumerate a reused destination directory (fd-relative on its pinned `O_NOFOLLOW` handle) into
/// a manifest of pre-existing entries, so the source can skip transferring identical files.
///
/// Returns an empty manifest (no `child()` stats performed) when the entry count exceeds
/// `max_entries` — the large-directory safeguard: that directory falls back to today's
/// transfer-and-drain. Entries that cannot be enumerated/stat'd are omitted (conservative: the
/// source will send them).
async fn build_existing_manifest(
    dir: &Arc<Dir>,
    max_entries: usize,
) -> Vec<remote::protocol::ExistingEntry> {
    use common::preserve::Metadata as _;
    // a cap of 0 disables the optimization for every non-empty directory; short-circuit before
    // the readdir so the disable case pays nothing.
    if max_entries == 0 {
        return Vec::new();
    }
    let entries = match dir.read_entries().await {
        Ok(entries) => entries,
        Err(e) => {
            tracing::debug!("manifest: cannot enumerate destination directory: {:#}", e);
            return Vec::new();
        }
    };
    if entries.len() > max_entries {
        tracing::debug!(
            "manifest: {} entries exceeds cap {}, skipping manifest (files will transfer)",
            entries.len(),
            max_entries
        );
        return Vec::new();
    }
    let mut manifest = Vec::with_capacity(entries.len());
    for (name, _hint) in entries {
        match dir.child(&name).await {
            Ok(handle) => {
                let meta = handle.meta();
                manifest.push(remote::protocol::ExistingEntry {
                    name: std::path::PathBuf::from(name),
                    is_file: handle.kind() == common::walk::EntryKind::File,
                    metadata: remote::protocol::Metadata::from(meta),
                    size: meta.size(),
                });
            }
            Err(e) => {
                let e: anyhow::Error = e.into();
                tracing::debug!("manifest: cannot stat child {:?}: {:#}", name, e);
            }
        }
    }
    manifest
}

/// Create a directory fd-relative on the PARENT's held `Dir`, handling overwrite logic.
///
/// All operations resolve relative to `dst_parent`'s pinned fd: classify an existing entry via
/// `dst_parent.child(dst_name)`; create via `dst_parent.make_dir(dst_name, mode)` (`mkdirat`);
/// reuse an existing directory via `dst_parent.open_dir(dst_name)` (`O_NOFOLLOW|O_DIRECTORY` — a
/// directory→symlink swap fails closed with ELOOP/ENOTDIR); replace a non-directory via the
/// recheck-guarded [`remove_existing_dst`] then `make_dir`. A privileged destination therefore
/// cannot be redirected by a concurrent symlink swap of the parent into creating a directory
/// outside the destination tree. The new directory is created mode `0o700` (writable so children
/// can be populated); its real source mode is applied later by `complete_directory_single`,
/// mirroring the path-based / local-copy behavior.
///
/// Returns the result; does NOT increment progress counters — the caller defers the increment
/// until completion (when it knows whether the directory is kept).
async fn create_directory(
    settings: &common::copy::Settings,
    dst_parent: &Arc<Dir>,
    dst_name: &OsStr,
    dst: &std::path::Path,
) -> anyhow::Result<DirectoryCreateResult> {
    let prog = progress();
    match dst_parent.make_dir(dst_name, 0o700).await {
        Ok(dir) => {
            // don't increment counter here - will be done in complete_directory
            // when we know we're keeping this directory
            Ok(DirectoryCreateResult::Created(Arc::new(dir)))
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            // something exists at destination - classify it via the parent fd (O_NOFOLLOW).
            let dst_handle = dst_parent
                .child(dst_name)
                .await
                .with_context(|| format!("failed reading metadata from dst: {dst:?}"))?;
            if dst_handle.kind() == common::walk::EntryKind::Dir {
                // directory already exists - reuse it (no overwrite needed for directories).
                // open_dir is O_NOFOLLOW|O_DIRECTORY, so a swap to a symlink fails closed here.
                tracing::debug!("destination directory already exists, reusing it");
                let dir = dst_parent
                    .open_dir(dst_name)
                    .await
                    .with_context(|| format!("cannot open existing directory {dst:?}"))?;
                prog.directories_unchanged.inc();
                Ok(DirectoryCreateResult::AlreadyExisted(Arc::new(dir)))
            } else if settings.ignore_existing {
                // not a directory but ignore_existing is set - skip the subtree
                tracing::debug!(
                    "destination exists but is not a directory, skipping subtree (--ignore-existing)"
                );
                prog.directories_unchanged.inc();
                Ok(DirectoryCreateResult::Skipped)
            } else if settings.overwrite {
                // not a directory but overwrite is enabled - remove (recheck-guarded, fd-relative)
                // and create.
                tracing::info!("destination is not a directory, removing and creating a new one");
                remove_existing_dst(dst_parent, dst_name, dst, &dst_handle, settings).await?;
                let dir = dst_parent
                    .make_dir(dst_name, 0o700)
                    .await
                    .with_context(|| format!("cannot create directory {dst:?}"))?;
                // don't increment counter here - will be done in complete_directory
                Ok(DirectoryCreateResult::Created(Arc::new(dir)))
            } else {
                // not a directory and overwrite disabled
                tracing::error!(
                    "Destination {dst:?} exists and is not a directory, use --overwrite to replace"
                );
                Ok(DirectoryCreateResult::Failed)
            }
        }
        Err(error) => {
            tracing::error!("Failed to create directory {dst:?}: {error:#}");
            Err(anyhow::Error::new(error).context(format!("cannot create directory {dst:?}")))
        }
    }
}

/// Create a symlink fd-relative on the PARENT's held `Dir`, handling overwrite logic, and apply
/// its metadata through the created link's own pinned handle.
///
/// Creation goes through `dst_parent.symlink_at(dst_name, target)` (`symlinkat` relative to the
/// pinned parent fd), which fails with `EEXIST` on any pre-existing entry (never following it);
/// the returned handle pins the link inode for race-free metadata application. Overwrite removal
/// is recheck-guarded and fd-relative via [`remove_existing_dst`]. A privileged destination
/// therefore cannot be redirected by a concurrent symlink swap of the parent into creating a link
/// outside the destination tree.
async fn create_symlink(
    settings: &common::copy::Settings,
    preserve: &common::preserve::Settings,
    dst_parent: &Arc<Dir>,
    dst_name: &OsStr,
    dst: &std::path::Path,
    target: &std::path::Path,
    metadata: &remote::protocol::Metadata,
) -> anyhow::Result<()> {
    let prog = progress();
    // fast path: the destination slot is empty, create the link directly.
    match dst_parent.symlink_at(dst_name, target).await {
        Ok(link_handle) => {
            common::safedir::set_symlink_metadata_fd(
                preserve,
                metadata,
                &link_handle,
                common::Side::Destination,
            )
            .await
            .with_context(|| format!("failed setting symlink metadata on {dst:?}"))?;
            prog.symlinks_created.inc();
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if settings.ignore_existing {
                tracing::debug!("destination exists, skipping symlink (--ignore-existing)");
                prog.symlinks_unchanged.inc();
                return Ok(());
            }
            if !settings.overwrite {
                return Err(
                    anyhow::Error::new(error).context(format!("failed creating symlink {dst:?}"))
                );
            }
            // classify the existing entry through the parent fd (O_NOFOLLOW).
            let dst_handle = dst_parent
                .child(dst_name)
                .await
                .with_context(|| format!("failed reading metadata from dst: {dst:?}"))?;
            if dst_handle.kind() == common::walk::EntryKind::Symlink {
                let dst_link = dst_parent
                    .read_link_at(dst_name)
                    .await
                    .with_context(|| format!("failed reading dst symlink: {dst:?}"))?;
                if *target == dst_link {
                    tracing::debug!(
                        "destination is a symlink and points to the same location as source"
                    );
                    if preserve.symlink.any()
                        && !common::filecmp::metadata_equal(
                            &settings.overwrite_compare,
                            metadata,
                            dst_handle.meta(),
                        )
                    {
                        tracing::debug!("destination metadata is different, updating");
                        common::safedir::set_symlink_metadata_fd(
                            preserve,
                            metadata,
                            &dst_handle,
                            common::Side::Destination,
                        )
                        .await
                        .with_context(|| format!("failed setting symlink metadata on {dst:?}"))?;
                        prog.symlinks_removed.inc();
                        prog.symlinks_created.inc();
                        return Ok(());
                    }
                    tracing::debug!("destination symlink is identical, skipping");
                    prog.symlinks_unchanged.inc();
                    return Ok(());
                }
                tracing::info!(
                    "destination is a symlink but points to a different location, removing"
                );
            } else {
                tracing::info!("destination is not a symlink, removing");
            }
            // remove the conflicting entry (recheck-guarded, fd-relative) then create the link.
            remove_existing_dst(dst_parent, dst_name, dst, &dst_handle, settings).await?;
            let link_handle = dst_parent
                .symlink_at(dst_name, target)
                .await
                .with_context(|| format!("failed creating symlink {dst:?}"))?;
            common::safedir::set_symlink_metadata_fd(
                preserve,
                metadata,
                &link_handle,
                common::Side::Destination,
            )
            .await
            .with_context(|| format!("failed setting symlink metadata on {dst:?}"))?;
            prog.symlinks_created.inc();
            Ok(())
        }
        Err(error) => {
            Err(anyhow::Error::new(error).context(format!("failed creating symlink {dst:?}")))
        }
    }
}

#[instrument(skip(error_collector, control_recv_stream, directory_tracker))]
async fn process_control_stream(
    settings: &common::copy::Settings,
    overwrite_manifest_max_entries: usize,
    preserve: &common::preserve::Settings,
    mut control_recv_stream: remote::streams::BoxedRecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
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
                entry_count,
                keep_if_empty,
            } => {
                let _ops_guard = prog.ops.guard();
                // check for failed ancestor
                let has_failed_ancestor = {
                    let tracker = directory_tracker.lock().await;
                    tracker.has_failed_ancestor(dst)
                };
                if has_failed_ancestor {
                    tracing::warn!("Skipping directory {:?} - ancestor failed to create", dst);
                    // nack so the source releases this directory's held fd (it was
                    // never created, so no files will be requested for it).
                    {
                        let tracker = directory_tracker.lock().await;
                        tracker
                            .send_directory_skipped(src, dst)
                            .await
                            .context("Failed to send DirectorySkipped for skipped directory")?;
                    }
                    // still count as a processed child entry for the parent
                    if !is_root && let Some(parent) = dst.parent() {
                        directory_tracker
                            .lock()
                            .await
                            .process_child_entry(parent)
                            .await
                            .context("Failed to update parent tracker for skipped directory")?;
                    }
                    continue;
                }
                // resolve the destination parent directory's held fd (for the root, open
                // the trusted parent via open_parent_dir). all creation is then fd-relative
                // on that pinned parent.
                let create_result = match resolve_parent_dir(&directory_tracker, dst, is_root).await
                {
                    Ok((dst_parent, dst_name)) => {
                        create_directory(settings, &dst_parent, &dst_name, dst).await
                    }
                    Err(e) => Err(e.context("failed resolving destination parent directory")),
                };
                // try to create directory
                let (create_result, error_already_pushed) = match create_result {
                    Ok(result) => (result, false),
                    Err(e) => {
                        tracing::error!("Failed to create directory {:?}: {:#}", dst, e);
                        if settings.fail_early {
                            return Err(e);
                        }
                        error_collector.push(e);
                        (DirectoryCreateResult::Failed, true)
                    }
                };
                // classify the outcome before the match consumes it: whether we created the
                // directory (vs reused an existing one) and whether create reported a hard
                // failure (vs an --ignore-existing skip).
                let was_created = matches!(create_result, DirectoryCreateResult::Created(_));
                let create_failed = matches!(create_result, DirectoryCreateResult::Failed);
                match create_result {
                    DirectoryCreateResult::Created(dir)
                    | DirectoryCreateResult::AlreadyExisted(dir) => {
                        // build the manifest only for a REUSED dir under overwrite/ignore-existing;
                        // a freshly-created dir is empty and feature-off needs no manifest.
                        let existing =
                            if !was_created && (settings.overwrite || settings.ignore_existing) {
                                build_existing_manifest(&dir, overwrite_manifest_max_entries).await
                            } else {
                                Vec::new()
                            };
                        // add to tracker (sends DirectoryCreated, stores the dir fd in the fd-map)
                        // tracker handles root directory tracking internally
                        directory_tracker
                            .lock()
                            .await
                            .add_directory(
                                src,
                                dst,
                                dir,
                                metadata.clone(),
                                is_root,
                                was_created,
                                entry_count,
                                keep_if_empty,
                                existing,
                            )
                            .await
                            .context("Failed to add directory to tracker")?;
                    }
                    DirectoryCreateResult::Skipped | DirectoryCreateResult::Failed => {
                        // mark as failed - descendants will be skipped.
                        // for Skipped (--ignore-existing), this is intentional and not an error.
                        // for Failed, push the synthetic "not a directory" error when
                        // create_directory returned Ok(Failed). when it returned
                        // Err(e), the real error (e.g. EACCES) was already pushed.
                        if create_failed && !error_already_pushed {
                            error_collector.push(anyhow::anyhow!(
                                "destination {dst:?} exists and is not a directory, use --overwrite to replace"
                            ));
                        }
                        let mut tracker = directory_tracker.lock().await;
                        tracker.mark_directory_failed(dst);
                        // nack so the source releases this directory's held fd: it was
                        // not created and no files will be requested for it. Without this
                        // a no-ack subtree larger than the source's dir-fd budget hangs
                        // the copy. (Sent even on the fail_early return path below — the
                        // source's Pass 1 may still be mid-walk when the failure races
                        // its DestinationDone; an extra nack is harmless there.)
                        tracker
                            .send_directory_skipped(src, dst)
                            .await
                            .context("Failed to send DirectorySkipped for failed directory")?;
                        // if root directory failed, mark root as complete to avoid hang
                        if is_root {
                            tracker.set_root_complete();
                        }
                        // failed directory won't go through complete_directory, so
                        // notify parent immediately
                        if !is_root && let Some(parent) = dst.parent() {
                            tracker
                                .process_child_entry(parent)
                                .await
                                .context("Failed to update parent tracker for failed directory")?;
                        }
                        if create_failed && settings.fail_early {
                            return Err(anyhow::anyhow!(
                                "destination {dst:?} exists and is not a directory, use --overwrite to replace"
                            ));
                        }
                    }
                }
                // note: successfully created directories notify their parent when
                // they complete (in complete_directory), not here at creation time
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
                let has_failed_ancestor = {
                    let tracker = directory_tracker.lock().await;
                    tracker.has_failed_ancestor(dst)
                };
                if has_failed_ancestor {
                    tracing::warn!("Skipping symlink {:?} - ancestor failed to create", dst);
                    // still count as a processed child entry for the parent
                    if !is_root && let Some(parent) = dst.parent() {
                        directory_tracker
                            .lock()
                            .await
                            .process_child_entry(parent)
                            .await
                            .context("Failed to update parent tracker for skipped symlink")?;
                    }
                    continue;
                }
                // resolve the destination parent's held fd (for the root, open the trusted
                // parent via open_parent_dir), then create the symlink fd-relative on it.
                let result = match resolve_parent_dir(&directory_tracker, dst, is_root).await {
                    Ok((dst_parent, dst_name)) => {
                        create_symlink(
                            settings,
                            preserve,
                            &dst_parent,
                            &dst_name,
                            dst,
                            target,
                            metadata,
                        )
                        .await
                    }
                    Err(e) => Err(e.context("failed resolving destination parent directory")),
                };
                if let Err(e) = result {
                    tracing::error!("Failed to create symlink {:?} -> {:?}: {:#}", src, dst, e);
                    if settings.fail_early {
                        return Err(e);
                    }
                    error_collector.push(e);
                }
                // mark root symlink complete
                if is_root {
                    directory_tracker.lock().await.set_root_complete();
                }
                // count this symlink as a processed child entry for its parent
                if !is_root && let Some(parent) = dst.parent() {
                    directory_tracker
                        .lock()
                        .await
                        .process_child_entry(parent)
                        .await
                        .context("Failed to update parent tracker for symlink")?;
                }
            }
            remote::protocol::SourceMessage::DirStructureComplete { has_root_item } => {
                tracing::info!(
                    "Received DirStructureComplete (has_root_item={})",
                    has_root_item
                );
                directory_tracker
                    .lock()
                    .await
                    .set_structure_complete(has_root_item);
            }
            remote::protocol::SourceMessage::FileSkipped { ref src, ref dst } => {
                tracing::info!("File was skipped by source: {:?} -> {:?}", src, dst);
                // get parent directory and update tracker
                let parent_dir = dst
                    .parent()
                    .ok_or_else(|| anyhow::anyhow!("skipped file {:?} has no parent", dst))?;
                directory_tracker
                    .lock()
                    .await
                    .process_file(parent_dir)
                    .await
                    .context("Failed to update tracker for skipped file")?;
            }
            remote::protocol::SourceMessage::FileUnchanged { ref src, ref dst } => {
                tracing::info!(
                    "File unchanged, source skipped transfer: {:?} -> {:?}",
                    src,
                    dst
                );
                // destination is authoritative for files_unchanged (matches the drain path
                // in process_single_file).
                prog.files_unchanged.inc();
                let parent_dir = dst
                    .parent()
                    .ok_or_else(|| anyhow::anyhow!("unchanged file {:?} has no parent", dst))?;
                directory_tracker
                    .lock()
                    .await
                    .process_file(parent_dir)
                    .await
                    .context("Failed to update tracker for unchanged file")?;
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
                // if root symlink failed, mark root as complete to avoid hang
                if is_root {
                    directory_tracker.lock().await.set_root_complete();
                }
                // count this skipped symlink as a processed child entry for its parent
                if !is_root && let Some(parent) = src_dst.dst.parent() {
                    directory_tracker
                        .lock()
                        .await
                        .process_child_entry(parent)
                        .await
                        .context("Failed to update parent tracker for skipped symlink")?;
                }
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
    overwrite_manifest_max_entries: usize,
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
    let error_collector = std::sync::Arc::new(common::error_collector::ErrorCollector::default());
    let directory_tracker = directory_tracker::make_shared(
        control_send_stream,
        *preserve,
        settings.fail_early,
        error_collector.clone(),
    );
    // create a pool of data connections to source
    let data_pool = std::sync::Arc::new(DataConnectionPool::new(
        *src_data_addr,
        tcp_config.max_connections,
        tcp_config.network_profile,
        tls_connector,
    ));
    let file_handler_future = process_incoming_file_streams_tcp(
        settings.clone(),
        *preserve,
        data_pool.clone(),
        directory_tracker.clone(),
        error_collector.clone(),
    );
    let control_future = process_control_stream(
        settings,
        overwrite_manifest_max_entries,
        preserve,
        control_recv_stream,
        directory_tracker.clone(),
        error_collector.clone(),
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
        // filtering is applied on the source side, so destination skipped counts are always 0
        files_skipped: 0,
        symlinks_skipped: 0,
        directories_skipped: 0,
        specials_skipped: 0,
        rm_summary: common::rm::Summary {
            bytes_removed: prog.bytes_removed.get(),
            files_removed: prog.files_removed.get() as usize,
            symlinks_removed: prog.symlinks_removed.get() as usize,
            directories_removed: prog.directories_removed.get() as usize,
            // filtering is applied on the source side, so destination skipped counts are always 0
            files_skipped: 0,
            symlinks_skipped: 0,
            directories_skipped: 0,
        },
    };
    match error_collector.take_error() {
        Some(err) => Err(common::copy::Error {
            source: err,
            summary,
        }
        .into()),
        None => Ok(("destination OK".to_string(), summary)),
    }
}

#[cfg(test)]
mod manifest_tests {
    use super::*;

    async fn open_dir(path: &std::path::Path) -> Arc<Dir> {
        Arc::new(
            common::safedir::Dir::open_root_dir(path, false, common::Side::Destination)
                .await
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn manifest_lists_files_dirs_symlinks() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a.txt"), "hello").unwrap(); // 5 bytes
        std::fs::create_dir(tmp.path().join("sub")).unwrap();
        std::os::unix::fs::symlink("a.txt", tmp.path().join("link")).unwrap();
        let dir = open_dir(tmp.path()).await;

        let manifest = build_existing_manifest(&dir, usize::MAX).await;

        assert_eq!(manifest.len(), 3);
        let file = manifest
            .iter()
            .find(|e| e.name == std::path::Path::new("a.txt"))
            .unwrap();
        assert!(file.is_file);
        assert_eq!(file.size, 5);
        let sub = manifest
            .iter()
            .find(|e| e.name == std::path::Path::new("sub"))
            .unwrap();
        assert!(!sub.is_file);
        let link = manifest
            .iter()
            .find(|e| e.name == std::path::Path::new("link"))
            .unwrap();
        assert!(!link.is_file);
    }

    #[tokio::test]
    async fn manifest_capped_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a.txt"), "x").unwrap();
        std::fs::write(tmp.path().join("b.txt"), "y").unwrap();
        let dir = open_dir(tmp.path()).await;

        // 2 entries, cap 1 => fall back to empty manifest (no stats, transfer-and-drain)
        let manifest = build_existing_manifest(&dir, 1).await;
        assert!(manifest.is_empty());
    }

    #[tokio::test]
    async fn manifest_zero_cap_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a.txt"), "x").unwrap();
        let dir = open_dir(tmp.path()).await;

        // cap 0 disables the optimization (short-circuits before the readdir)
        let manifest = build_existing_manifest(&dir, 0).await;
        assert!(manifest.is_empty());
    }
}
