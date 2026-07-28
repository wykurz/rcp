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
/// Outcome of [`DataConnectionPool::connect`]. Distinguishing a teardown-induced close (`PoolClosed`)
/// from a genuine connection failure (`Failed`) AT THE ERROR SOURCE (not by later timing) is what lets
/// the worker record only genuine failures — a benign late reconnect during teardown is `PoolClosed`
/// and is never mistaken for a cause.
enum ConnectOutcome {
    Connected(
        remote::streams::BoxedRecvStream,
        tokio::sync::OwnedSemaphorePermit,
    ),
    /// The pool was closed / cancelled by teardown — a benign end, not a failure to report.
    PoolClosed,
    /// A genuine connect failure (refused, timed out, TLS fault) whose cause is worth surfacing if the
    /// transfer turns out incomplete.
    Failed(anyhow::Error),
}

/// The stashed connect cause and the teardown latch, under ONE mutex.
///
/// They must be one state, not two: a worker that checked a separate "are we tearing down" flag and
/// only then locked the slot could be cancelled in between, and would stash a teardown artifact that
/// `choose_final_result` then prefers over the real control failure. Sharing the mutex makes the
/// transition atomic — [`DataConnectionPool::close`] latches `tearing_down` under the lock BEFORE
/// cancelling, so every recorder either wins the lock first (its failure genuinely predates
/// teardown) or sees the latch and drops its error.
#[derive(Default)]
struct ConnectErrors {
    tearing_down: bool,
    first: Option<anyhow::Error>,
}

struct DataConnectionPool {
    data_addr: std::net::SocketAddr,
    network_profile: remote::NetworkProfile,
    /// Semaphore to limit concurrent connections
    semaphore: std::sync::Arc<tokio::sync::Semaphore>,
    /// Optional TLS connector for encrypted connections
    tls_connector: Option<std::sync::Arc<tokio_rustls::TlsConnector>>,
    /// Upper bound on a single TCP-connect + TLS-handshake, so a worker already past the semaphore
    /// but stuck mid-handshake cannot block the pool drain forever (it would otherwise leave the
    /// file-handler future — and thus `run_destination`'s teardown — waiting indefinitely).
    conn_timeout: std::time::Duration,
    /// Cancelled by [`Self::close`] so an IN-FLIGHT `connect()` is released immediately on teardown,
    /// not only bounded by `conn_timeout`.
    cancel: tokio_util::sync::CancellationToken,
    /// The FIRST genuine (non-teardown) connect failure, first-writer-wins. Surfaced as the cause when
    /// the completion gate fires, so a premature connect failure names e.g. "connection refused"
    /// rather than only a synthetic "incomplete transfer".
    connect_errors: std::sync::Mutex<ConnectErrors>,
}

impl DataConnectionPool {
    fn new(
        data_addr: std::net::SocketAddr,
        max_connections: usize,
        network_profile: remote::NetworkProfile,
        tls_connector: Option<std::sync::Arc<tokio_rustls::TlsConnector>>,
        conn_timeout_sec: u64,
    ) -> Self {
        Self {
            data_addr,
            network_profile,
            semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(max_connections)),
            tls_connector,
            conn_timeout: std::time::Duration::from_secs(conn_timeout_sec),
            cancel: tokio_util::sync::CancellationToken::new(),
            connect_errors: std::sync::Mutex::new(ConnectErrors::default()),
        }
    }
    /// Open a new connection to the source's data port.
    async fn connect(&self) -> ConnectOutcome {
        // acquire a permit; a closed semaphore means teardown (not a failure to report).
        let permit = match self.semaphore.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => return ConnectOutcome::PoolClosed,
        };
        // Race the TCP connect + TLS handshake against teardown cancellation and a hard timeout. A
        // worker already PAST the semaphore, stuck mid-handshake on an unresponsive peer, would
        // otherwise never finish — leaving the file-handler future (and thus `run_destination`'s
        // teardown) waiting for it forever. A cancel is a benign teardown (`PoolClosed`).
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => ConnectOutcome::PoolClosed,
            result = tokio::time::timeout(self.conn_timeout, self.connect_and_handshake()) => {
                match result {
                    Ok(Ok(recv_stream)) => ConnectOutcome::Connected(recv_stream, permit),
                    Ok(Err(e)) => ConnectOutcome::Failed(e),
                    Err(_elapsed) => ConnectOutcome::Failed(anyhow::anyhow!(
                        "data connection to source timed out after {}s",
                        self.conn_timeout.as_secs()
                    )),
                }
            }
        }
    }
    /// Record the FIRST genuine (non-teardown) connect failure (first-writer-wins).
    ///
    /// Drops the error once teardown has begun. `connect()` classifies at the error source, but a
    /// connect that had ALREADY resolved to `Failed` when `close()` fired still reaches here — and
    /// that failure is a teardown artifact (the source is closing its data listener), not a cause.
    /// Recording it would let `choose_final_result` prefer it over the real error. The latch is read
    /// under the SAME lock that guards the slot, so the check cannot be overtaken by a concurrent
    /// `close()` between testing it and writing.
    fn record_first_connect_error(&self, e: anyhow::Error) {
        let mut errs = self.connect_errors.lock().unwrap();
        if errs.tearing_down {
            tracing::debug!("ignoring connect failure observed after teardown began: {e:#}");
            return;
        }
        if errs.first.is_none() {
            errs.first = Some(e);
        }
    }
    /// Whether teardown has begun, observable WITHOUT the tracker mutex.
    ///
    /// Set by [`Self::close`], which `signal_source_teardown` invokes first precisely so this is
    /// true from the instant teardown starts. The data workers' end-of-stream gate relies on that:
    /// the tracker's own `is_closing()` is only set behind OUTER and can still read false while a
    /// suspended future holds it. This reads the cancellation token rather than
    /// [`ConnectErrors::tearing_down`] so the gate stays lock-free — `close()` latches the flag
    /// before cancelling, so a cancelled token always implies the flag is already set.
    fn is_tearing_down(&self) -> bool {
        self.cancel.is_cancelled()
    }
    fn take_first_connect_error(&self) -> Option<anyhow::Error> {
        self.connect_errors.lock().unwrap().first.take()
    }
    /// The blocking-on-I/O part of [`Self::connect`], factored out so it can be bounded/cancelled.
    ///
    /// The handshake bound here is `conn_timeout`, the same deadline [`Self::connect`] applies to
    /// the whole TCP-connect + handshake; only the read half is kept (this side never sends on a
    /// data connection).
    async fn connect_and_handshake(&self) -> anyhow::Result<remote::streams::BoxedRecvStream> {
        let stream = tokio::net::TcpStream::connect(self.data_addr).await?;
        stream.set_nodelay(true)?;
        remote::configure_tcp_buffers(&stream, self.network_profile);
        let (_send_stream, recv_stream) = remote::tls::connect_bounded(
            self.tls_connector.as_deref(),
            remote::tls::SERVER_NAME_SOURCE,
            stream,
            self.conn_timeout,
            "data",
        )
        .await?;
        Ok(recv_stream)
    }
    fn close(&self) {
        // Latch teardown BEFORE cancelling, under the slot's own lock: a recorder either commits its
        // cause before this point (so it genuinely predates teardown) or observes the latch and
        // drops it. Ordering it before `cancel()` also keeps `is_tearing_down()` — which reads the
        // token lock-free — from ever being true while the latch is still false.
        self.connect_errors.lock().unwrap().tearing_down = true;
        self.semaphore.close();
        // release any worker currently blocked in `connect_and_handshake` (semaphore close only
        // stops NEW permit acquisitions; it does not touch a connect already in flight).
        self.cancel.cancel();
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
    // Count the file BEFORE applying metadata: its bytes are already on disk, so a metadata
    // failure below must not erase it from the summary. This mirrors the local path
    // (`common::copy`, which increments its progress counters before `set_file_metadata_fd`) —
    // the remote summary is built from these counters, so incrementing after would report
    // "files copied: 0" for a tree whose data transferred completely and only failed to be
    // chowned. The metadata error is still recorded and still fails the copy.
    prog.files_copied.inc();
    prog.bytes_copied.add(file_header.size);
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

/// Whether a peer closure at a file-header boundary is benign.
///
/// THE single completion gate for end-of-stream on a data connection, consulted by EVERY shape the
/// stream can end in — a clean framed EOF (`Ok(None)`) and a transport-level peer closure alike.
/// Keeping one gate is the point: the two shapes are indistinguishable in meaning (the peer stopped
/// between two headers) and differ only in whether a `close_notify`/FIN arrived before the socket
/// died, which is timing, not semantics. Gating only one of them is what let the hang below survive.
///
/// A closure is benign ONLY if the transfer already completed (`is_done()`: the source closes data
/// connections only after consuming our `DestinationDone`) or teardown has begun. Otherwise it is a
/// mid-transfer truncation: tolerating it would make the worker reconnect and block on an idle
/// socket while the source waits for a `DestinationDone` that can never come — an indefinite hang,
/// with the completion gate unreachable because neither future completes.
///
/// Teardown is read from the POOL (lock-free) as well as the tracker: the tracker's `is_closing()`
/// is set behind OUTER, which a suspended future can hold for an unbounded window, so relying on it
/// alone lets a worker record a spurious truncation that then masks the real cause.
async fn peer_close_is_benign(
    directory_tracker: &directory_tracker::SharedDirectoryTracker,
    data_pool: &DataConnectionPool,
) -> bool {
    if data_pool.is_tearing_down() {
        return true;
    }
    let t = directory_tracker.lock().await;
    t.is_done() || t.is_closing()
}

/// The error for a data stream that ended before the transfer completed.
///
/// A FIXED message (never the transport error) so `ErrorCollector` dedups several concurrent
/// truncations to one cause and cannot mask a real error; the transport kind is logged at the call
/// site.
fn truncated_stream_error() -> anyhow::Error {
    anyhow::anyhow!(
        "data stream closed before the transfer completed (truncated header or dropped link)"
    )
}

/// Handle a stream that may contain multiple files.
///
/// Loops until the stream is closed (EOF on header read).
#[instrument(skip(error_collector, file_recv_stream, directory_tracker, data_pool))]
async fn handle_file_stream(
    settings: common::copy::Settings,
    preserve: common::preserve::Settings,
    mut file_recv_stream: remote::streams::BoxedRecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
    data_pool: std::sync::Arc<DataConnectionPool>,
) -> anyhow::Result<()> {
    let prog = progress();
    tracing::info!("Processing file stream (may contain multiple files)");
    // loop until stream closes (EOF on header read)
    loop {
        // try to receive next file header
        let file_header = match file_recv_stream
            .recv_object::<remote::protocol::File>()
            .await
        {
            Ok(Some(h)) => h,
            Ok(None) => {
                // A CLEAN framed EOF at a header boundary. This is NOT self-evidently benign: the
                // source closes a data stream gracefully while it keeps running (a send failure
                // discards the stream with `close()`, and the pool drain closes returning streams),
                // and on plain TCP a graceful FIN is the ordinary shape anyway. So it goes through
                // the SAME completion gate as a transport-level closure — see `peer_close_is_benign`.
                if !peer_close_is_benign(&directory_tracker, &data_pool).await {
                    tracing::error!("Data stream closed cleanly before the transfer completed");
                    return Err(truncated_stream_error());
                }
                tracing::debug!("Stream closed, no more files");
                break;
            }
            Err(e) => {
                // Distinguish a benign end-of-transfer close from a mid-transfer TRUNCATION, and both
                // from a framing/decode fault.
                //
                // A peer closure reaches us as a TRANSPORT error whenever the socket died without a
                // clean `close_notify`/FIN — `UnexpectedEof` (TLS: rustls's missing `close_notify`)
                // or `ConnectionReset`, or any other "the peer closed" kind depending on timing. The
                // clean-EOF shape arrives as `Ok(None)` above; both mean the same thing and share one
                // gate.
                let peer_closed = e.downcast_ref::<std::io::Error>().is_some_and(|io| {
                    use std::io::ErrorKind::{
                        BrokenPipe, ConnectionAborted, ConnectionReset, NotConnected, UnexpectedEof,
                    };
                    matches!(
                        io.kind(),
                        UnexpectedEof
                            | ConnectionReset
                            | ConnectionAborted
                            | BrokenPipe
                            | NotConnected
                    )
                });
                if peer_closed {
                    // The kind alone is ambiguous — a truncated header ending in a reset looks
                    // identical to a benign close — so COMPLETION STATE decides, via the same gate
                    // the clean-EOF arm uses. A PRE-completion closure is FATAL: it propagates, the
                    // worker aborts, the join loop signals teardown, and the copy fails with this
                    // cause.
                    if peer_close_is_benign(&directory_tracker, &data_pool).await {
                        tracing::debug!(
                            "Data stream ended at header boundary (peer closed): {e:#}"
                        );
                        break;
                    }
                    tracing::error!(
                        "Data stream closed before the transfer completed (truncation): {e:#}"
                    );
                    return Err(truncated_stream_error());
                }
                // A framing/decode fault — an oversized length prefix (`InvalidData`), a TLS protocol
                // fault, or a frame that does not decode to a `File` — is always fatal.
                return Err(e).context("transport or decode fault reading file header");
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
            // check if we're done after each file - this may send DestinationDone. We send it even
            // when THIS file failed: the failure is recorded (in the collector) so the destination
            // still reports Failure, but DestinationDone is what lets the source shut down cleanly on
            // the COMPLETION path. (On a non-completion abort the source is instead signaled by
            // `signal_source_teardown` closing the control stream — see `run_destination` and
            // docs/remote_protocol.md.)
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

/// Signal the source to tear down, and stop the local data pool.
///
/// This is the ONE abort funnel for the destination. Closing the control send stream makes the
/// source observe the close, release its dir-fd budget, and tear down — so a source that is idle (an
/// all-empty-file transfer, whose header-only sends never trip a broken pipe) or parked on a
/// saturated dir-fd budget stops, instead of leaving the destination waiting on `control_future`
/// forever (an infinite hang). Closing the data pool fails the workers' reconnects so they exit.
///
/// Both operations are idempotent, which is what lets every caller invoke it unconditionally: on the
/// happy path `send_destination_done` already closed the stream (a no-op close here) and the copy is
/// done with the pool. There is no armed/disarmed state to get wrong — the invariant "the source is
/// always signaled before the destination waits on it" holds because this is called on every path.
///
/// ORDER MATTERS: the pool is closed FIRST. `close()` is synchronous and lock-free, so it lands on
/// this future's very first poll, whereas `close_stream()` must first acquire the tracker mutex
/// (OUTER) — which the loser future can hold while SUSPENDED mid-send (see `run_destination`). Doing
/// the awaited close first would leave, for that whole unbounded window, a teardown that no worker
/// can observe: workers would keep reconnecting into an idle socket, and a connect that failed in
/// the window would stash a teardown ARTIFACT that `choose_final_result` then prefers over the real
/// cause. Closing the pool first makes "teardown has begun" observable immediately and lock-free —
/// it cancels in-flight connects (`ConnectOutcome::PoolClosed`) and is the signal
/// [`DataConnectionPool::is_tearing_down`] exposes to the data workers' end-of-stream gate.
async fn signal_source_teardown(
    directory_tracker: &directory_tracker::SharedDirectoryTracker,
    data_pool: &DataConnectionPool,
) {
    data_pool.close();
    directory_tracker.lock().await.close_stream().await;
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
                // Connect to the source's data port. A `PoolClosed` outcome is teardown (benign) — stop
                // silently. A `Failed` outcome is a GENUINE connect failure (refused / timed out / TLS
                // fault): stash the first such cause so the completion gate can name it if the transfer
                // turns out incomplete, then stop. Whether it actually mattered is decided centrally by
                // whether the transfer completed — a benign late reconnect that fails during teardown is
                // dropped because the gate never fires. The worker carries no signaling responsibility.
                let (recv_stream, _permit) = match pool.connect().await {
                    ConnectOutcome::Connected(recv_stream, permit) => (recv_stream, permit),
                    ConnectOutcome::PoolClosed => break,
                    ConnectOutcome::Failed(e) => {
                        tracing::debug!("Data connection failed: {e:#}");
                        pool.record_first_connect_error(e);
                        break;
                    }
                };
                // Receive files until the source closes this connection (EOF). A returned error is a
                // genuine abort — a --fail-early file/metadata failure, or a corrupted stream whose
                // lost files can never let the tracker reach is_done() — so propagate it OUT of the
                // task (the join loop records it and signals the source ONCE). Individual
                // non-fail-early errors never return here: handle_file_stream records them into the
                // collector and keeps draining.
                handle_file_stream(
                    (*settings).clone(),
                    *preserve,
                    recv_stream,
                    tracker.clone(),
                    collector.clone(),
                    pool.clone(),
                )
                .await?;
                // permit is released when _permit is dropped
            }
            Ok::<(), anyhow::Error>(())
        });
    }
    // Drain the workers. A worker returns Err (or panics) ONLY on a genuine abort — a --fail-early
    // file/metadata failure, a corrupted stream, or a panic — never on a limpable individual error
    // (handle_file_stream records those and keeps draining). Record every abort so
    // `run_destination`'s `take_error` reports the real cause, and on the FIRST abort signal the
    // source ONCE, EAGERLY: the abort must reach the source now, not after the pool finishes
    // draining, because the other workers stay parked reading their data streams until the source is
    // told to stop (and the source only closes the data connections once it has torn down). Deferring
    // the signal to after the loop would therefore deadlock. `signal_source_teardown` is idempotent,
    // and `run_destination` calls it again unconditionally.
    let mut signaled = false;
    while let Some(result) = join_set.join_next().await {
        let aborted = match result {
            Ok(Ok(())) => false,
            Ok(Err(e)) => {
                tracing::error!("File stream worker aborted: {e:#}");
                error_collector.push(e);
                true
            }
            Err(e) => {
                tracing::error!("File stream worker panicked: {e:#}");
                error_collector.push(e.into());
                true
            }
        };
        if aborted && !signaled {
            signaled = true;
            signal_source_teardown(&directory_tracker, &data_pool).await;
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
    /// directory already existed (reused), with its open fd and, under strict operand resolution,
    /// the original `(uid, gid)` to restore at completion (`None` in the default path — see
    /// [`common::safedir::lockdown_reused_dir`])
    AlreadyExisted(Arc<Dir>, Option<(u32, u32)>),
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
                // strict-only lockdown: take over the reused directory and restrict it to 0o700 for
                // the copy's duration, restoring the original owner at completion (no-op / None in
                // the default path). A recheck or EPERM failure propagates as this directory's
                // create error — the caller marks it Failed (or aborts under --fail-early), so no
                // child is written into an unsecured directory.
                let restore_owner = common::safedir::lockdown_reused_dir(&dir, &dst_handle)
                    .await
                    .with_context(|| {
                        format!("cannot secure reused destination directory {dst:?}")
                    })?;
                prog.directories_unchanged.inc();
                Ok(DirectoryCreateResult::AlreadyExisted(
                    Arc::new(dir),
                    restore_owner,
                ))
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
                // the original owner to restore at completion — Some only for a strict-mode locked
                // reused directory (see create_directory / lockdown_reused_dir); None otherwise.
                let restore_owner = match &create_result {
                    DirectoryCreateResult::AlreadyExisted(_, restore_owner) => *restore_owner,
                    _ => None,
                };
                match create_result {
                    DirectoryCreateResult::Created(dir)
                    | DirectoryCreateResult::AlreadyExisted(dir, _) => {
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
                                restore_owner,
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
    // the handshake is bounded because a peer that establishes TCP then stalls it would otherwise
    // hang here indefinitely, BEFORE any teardown state exists (only the TCP connect above was
    // timed out)
    let (control_send_stream, control_recv_stream) = remote::tls::connect_bounded(
        tls_connector.as_deref(),
        remote::tls::SERVER_NAME_SOURCE,
        control_stream,
        std::time::Duration::from_secs(tcp_config.conn_timeout_sec),
        "control",
    )
    .await?;
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
        tcp_config.conn_timeout_sec,
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
    // Race both futures to first completion, then ALWAYS drive BOTH to completion before choosing an
    // error — the loser is never cancelled (a `?` on the winner would drop a data worker mid-record,
    // degrading the reported cause to a teardown symptom).
    //
    // The source is signaled to tear down (`signal_source_teardown` → `close_stream`) CONCURRENTLY
    // with the loser via `tokio::join!`, NOT inline before awaiting it. This avoids a held-but-unpolled
    // deadlock: `close_stream` takes the tracker mutex (OUTER), and the loser (`process_control_stream`
    // mid-`add_directory`) can be SUSPENDED holding OUTER across a control-stream send. Awaiting the
    // signal inline would park it on OUTER while the loser — the very next statement — is never polled
    // to release it. `join!` polls both, so the loser drains its send, releases OUTER, and the signal
    // then acquires it. This is deadlock-free because the lock order is strictly OUTER ≺ INNER (the
    // `control_send_stream` mutex; INNER is only ever taken through a `&mut DirectoryTracker` method,
    // which holds OUTER first), so no cycle is possible, and the source drains the control stream
    // continuously so the loser's send always completes. DO NOT reintroduce an inline `signal.await`
    // here, and DO NOT acquire OUTER while holding INNER anywhere, or this deadlock returns.
    let (file_result, control_result) = tokio::select! {
        file_result = &mut file_handler_future => {
            let ((), control_result) = tokio::join!(
                signal_source_teardown(&directory_tracker, &data_pool),
                &mut control_future,
            );
            (file_result, control_result)
        }
        control_result = &mut control_future => {
            let ((), file_result) = tokio::join!(
                signal_source_teardown(&directory_tracker, &data_pool),
                &mut file_handler_future,
            );
            (file_result, control_result)
        }
    };
    // `file_handler_future` returns Ok even when workers aborted (they record into the collector),
    // so a stream-level error here is almost always the control future's; either way `take_error`
    // below prefers the recorded operation cause.
    let select_result: anyhow::Result<()> = file_result
        .context("Failed to process incoming file streams")
        .and(control_result.context("Failed to process control stream"));
    // build summary from progress counters (used by every exit path below; the counters are
    // final now that the select! above has driven both futures to completion).
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
    // Choose the final result. Both futures have completed and the control stream + data pool were
    // already closed by `signal_source_teardown`. Read completion ONCE — the tracker is quiescent
    // (both futures done), so `is_done()` is stable, and it is monotonic once true.
    let completed = directory_tracker.lock().await.is_done();
    let recorded = error_collector.take_error();
    let connect_cause = data_pool.take_first_connect_error();
    choose_final_result(recorded, select_result, completed, connect_cause, summary)
}

/// Decide the destination's final result from the three teardown signals. Extracted from
/// [`run_destination`] so the decision matrix — which is easy to get subtly wrong — can be unit
/// tested directly (see `teardown_tests`).
///
/// Priority: (1) a recorded per-operation error is the real user-facing cause; (2) an INCOMPLETE
/// tracker must never report success even with nothing recorded (a data worker's `pool.connect()`
/// failing before completion leaves queued files unsent, and the source reads our control-stream
/// close as graceful) — name the actual cause, preferring the stashed `connect_cause` (e.g.
/// "connection refused" / "TLS handshake timed out") over a `select_result` teardown symptom, over a
/// synthetic message; (3) once complete, a late `select_result` error (and any stashed
/// `connect_cause` from a benign late reconnect) is only a teardown symptom and is swallowed.
fn choose_final_result(
    recorded: Option<anyhow::Error>,
    select_result: anyhow::Result<()>,
    completed: bool,
    connect_cause: Option<anyhow::Error>,
    summary: common::copy::Summary,
) -> anyhow::Result<(String, common::copy::Summary)> {
    if let Some(err) = recorded {
        return Err(common::copy::Error {
            source: err,
            summary,
        }
        .into());
    }
    if !completed {
        let cause = connect_cause
            .or_else(|| select_result.err())
            .unwrap_or_else(|| {
                anyhow::anyhow!(
                    "destination did not receive all expected entries (transfer incomplete)"
                )
            });
        return Err(common::copy::Error {
            source: cause.context("incomplete transfer"),
            summary,
        }
        .into());
    }
    if let Err(e) = select_result {
        tracing::debug!("ignoring teardown symptom after successful completion: {e:#}");
    }
    tracing::info!("Destination is done");
    Ok(("destination OK".to_string(), summary))
}

#[cfg(test)]
mod teardown_tests {
    use super::*;

    fn summary() -> common::copy::Summary {
        common::copy::Summary::default()
    }

    // ── choose_final_result: the completion-gate decision matrix ──

    #[test]
    fn recorded_error_is_reported_even_when_completed() {
        let r = choose_final_result(
            Some(anyhow::anyhow!("permission denied writing file")),
            Ok(()),
            true,
            None,
            summary(),
        );
        let e = format!("{:#}", r.unwrap_err());
        assert!(e.contains("permission denied writing file"), "{e}");
    }

    #[test]
    fn incomplete_without_a_recorded_error_fails_synthetically() {
        // a premature closure records nothing and the streams close cleanly, but the tracker never
        // reached is_done() — must NOT report success.
        let r = choose_final_result(None, Ok(()), false, None, summary());
        let e = format!("{:#}", r.unwrap_err());
        assert!(e.to_lowercase().contains("incomplete"), "{e}");
    }

    #[test]
    fn incomplete_prefers_the_specific_stream_cause() {
        let r = choose_final_result(
            None,
            Err(anyhow::anyhow!("Permission denied creating /dst/foo")),
            false,
            None,
            summary(),
        );
        let e = format!("{:#}", r.unwrap_err());
        assert!(e.contains("Permission denied creating /dst/foo"), "{e}");
        assert!(e.contains("incomplete transfer"), "{e}");
    }

    #[test]
    fn incomplete_prefers_the_connect_cause_over_stream_symptom() {
        // Finding #5: a premature connect failure stashes its cause; the gate names it (over a
        // generic message or a select_result teardown symptom).
        let r = choose_final_result(
            None,
            Err(anyhow::anyhow!("peer closed connection")), // teardown symptom
            false,
            Some(anyhow::anyhow!("connection refused")), // the real connect cause
            summary(),
        );
        let e = format!("{:#}", r.unwrap_err());
        assert!(e.contains("connection refused"), "{e}");
        assert!(e.contains("incomplete transfer"), "{e}");
    }

    #[test]
    fn completed_ignores_a_stashed_connect_cause_from_a_benign_reconnect() {
        // Finding #5: a worker that looped to connect() as the source stopped accepting after
        // DestinationDone stashes "connection refused", but the transfer COMPLETED — must be success.
        let r = choose_final_result(
            None,
            Ok(()),
            true,
            Some(anyhow::anyhow!("connection refused")),
            summary(),
        );
        assert!(
            r.is_ok(),
            "a completed transfer must not fail on a benign late-reconnect connect error"
        );
    }

    #[test]
    fn completed_with_no_error_is_success() {
        assert!(choose_final_result(None, Ok(()), true, None, summary()).is_ok());
    }

    #[test]
    fn completed_swallows_a_late_teardown_symptom() {
        // once complete, a late control error is only a teardown symptom (e.g. a control send that
        // lost the race with the stream close) — success must not flip to failure.
        let r = choose_final_result(
            None,
            Err(anyhow::anyhow!("peer closed connection")),
            true,
            None,
            summary(),
        );
        assert!(
            r.is_ok(),
            "a completed transfer must not fail on a teardown symptom"
        );
    }

    // ── the deadlock-freedom property of the teardown combinator ──

    /// Reproduces the shape that deadlocked before the fix: a "signal" that needs a mutex (the
    /// tracker OUTER lock) and a "loser" future that is SUSPENDED holding that mutex across an await.
    /// The old inline form (`signal.await; loser.await`) parks the signal on the mutex while the
    /// never-polled loser holds it → deadlock. `tokio::join!(signal, loser)` polls both, so the loser
    /// makes progress, releases the lock, and the signal completes. This pins the async-ordering
    /// property the real `run_destination` combinator relies on.
    #[tokio::test(start_paused = true)]
    async fn join_of_signal_and_loser_does_not_deadlock_when_loser_holds_the_lock() {
        let lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
        let release = std::sync::Arc::new(tokio::sync::Notify::new());

        // the loser acquires the lock and then suspends WHILE STILL HOLDING it (mirrors
        // process_control_stream suspended mid-`add_directory` holding the tracker lock).
        let loser = {
            let lock = lock.clone();
            let release = release.clone();
            async move {
                let _guard = lock.lock().await;
                release.notified().await;
            }
        };
        // the signal parks on the same lock (mirrors signal_source_teardown → close_stream).
        let signal = {
            let lock = lock.clone();
            async move {
                let _g = lock.lock().await;
            }
        };
        // release the loser shortly after both are being polled — reachable ONLY because `join!`
        // polls the loser concurrently with the parked signal.
        let trigger = {
            let release = release.clone();
            async move {
                tokio::task::yield_now().await;
                release.notify_one();
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(signal, loser, trigger);
        })
        .await
        .expect("join! must not deadlock when the loser holds the lock across an await");
    }

    // ── #1: a header-boundary peer-closure is fatal ONLY before completion ──

    /// A reader that immediately fails at the first `poll_read` with a given "peer closed" kind —
    /// reproducing a header-boundary transport drop (a truncated header looks identical to a benign
    /// close). This covers the TRANSPORT-error shape of an end-of-stream; the clean shape is covered
    /// separately with `tokio::io::empty()`, which yields `Ok(0)` on an empty decode buffer and so
    /// arrives as `Ok(None)`. (Only a PARTIAL frame produces `ErrorKind::Other` — "bytes remaining
    /// on stream" — which is the always-fatal decode arm.)
    struct FailingReader(std::io::ErrorKind);
    impl tokio::io::AsyncRead for FailingReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::new(self.0, "simulated peer closure")))
        }
    }

    // handle_file_stream fails at the first recv here, so the settings values are immaterial.
    fn test_copy_settings() -> common::copy::Settings {
        common::copy::Settings {
            dereference: false,
            fail_early: false,
            overwrite: false,
            overwrite_compare: Default::default(),
            overwrite_filter: None,
            ignore_existing: false,
            chunk_size: 0,
            skip_specials: false,
            remote_copy_buffer_size: 0,
            filter: None,
            dry_run: None,
            delete: None,
        }
    }

    fn tracker_over_sink() -> directory_tracker::SharedDirectoryTracker {
        let send = remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite
        );
        directory_tracker::make_shared(
            std::sync::Arc::new(tokio::sync::Mutex::new(send)),
            common::preserve::Settings::default(),
            false,
            std::sync::Arc::new(common::error_collector::ErrorCollector::default()),
        )
    }

    /// An OPEN pool (teardown not begun), so the gate is decided by the tracker alone.
    fn test_pool() -> std::sync::Arc<DataConnectionPool> {
        std::sync::Arc::new(DataConnectionPool::new(
            "127.0.0.1:1".parse().unwrap(),
            1,
            remote::NetworkProfile::default(),
            None,
            1,
        ))
    }

    async fn run_over_reader(
        tracker: directory_tracker::SharedDirectoryTracker,
        pool: std::sync::Arc<DataConnectionPool>,
        reader: remote::streams::BoxedRead,
    ) -> anyhow::Result<()> {
        let recv = remote::streams::RecvStream::new(reader);
        handle_file_stream(
            test_copy_settings(),
            common::preserve::Settings::default(),
            recv,
            tracker,
            std::sync::Arc::new(common::error_collector::ErrorCollector::default()),
            pool,
        )
        .await
    }

    async fn run_handle_file_stream(
        tracker: directory_tracker::SharedDirectoryTracker,
        kind: std::io::ErrorKind,
    ) -> anyhow::Result<()> {
        run_over_reader(
            tracker,
            test_pool(),
            Box::new(FailingReader(kind)) as remote::streams::BoxedRead,
        )
        .await
    }

    /// A CLEAN framed EOF (an empty reader) before completion must be fatal, exactly like a
    /// transport-level peer closure. This is the arm that previously broke out un-gated and left the
    /// worker reconnecting into an idle socket while the source waited for `DestinationDone`.
    #[tokio::test]
    async fn pre_completion_clean_eof_is_fatal() {
        let r = run_over_reader(
            tracker_over_sink(),
            test_pool(),
            Box::new(tokio::io::empty()) as remote::streams::BoxedRead,
        )
        .await;
        let e = format!(
            "{:#}",
            r.expect_err("a pre-completion clean EOF must be fatal")
        );
        assert!(e.contains("before the transfer completed"), "{e}");
    }

    /// A connect failure recorded BEFORE teardown is a genuine cause and is kept; one recorded
    /// after `close()` is a teardown artifact and must be dropped, or `choose_final_result` would
    /// prefer it over the real control failure.
    #[tokio::test]
    async fn connect_errors_recorded_after_teardown_are_dropped() {
        let pool = test_pool();
        pool.record_first_connect_error(anyhow::anyhow!("genuine: connection refused"));
        pool.close();
        pool.record_first_connect_error(anyhow::anyhow!("artifact: listener gone"));
        let cause = format!("{:#}", pool.take_first_connect_error().expect("a cause"));
        assert!(cause.contains("genuine"), "{cause}");

        let torn_down = test_pool();
        torn_down.close();
        torn_down.record_first_connect_error(anyhow::anyhow!("artifact: listener gone"));
        assert!(
            torn_down.take_first_connect_error().is_none(),
            "an error observed only after teardown must not become the reported cause"
        );
    }

    /// The same clean EOF is BENIGN once teardown has begun — and the pool alone must establish
    /// that, without the tracker mutex, since a suspended future can hold it for an unbounded window.
    #[tokio::test]
    async fn clean_eof_during_teardown_is_benign() {
        let pool = test_pool();
        pool.close();
        run_over_reader(
            tracker_over_sink(),
            pool,
            Box::new(tokio::io::empty()) as remote::streams::BoxedRead,
        )
        .await
        .expect("a clean EOF during teardown is benign");
    }

    #[tokio::test]
    async fn pre_completion_peer_closure_is_fatal() {
        // incomplete tracker (is_done()==false, is_closing()==false) + a whitelisted peer-closure →
        // FATAL. Before the fix this returned Ok (treated as clean EOF) → the hang.
        let r =
            run_handle_file_stream(tracker_over_sink(), std::io::ErrorKind::ConnectionReset).await;
        let e = format!(
            "{:#}",
            r.expect_err("a pre-completion peer closure must be fatal")
        );
        assert!(e.contains("before the transfer completed"), "{e}");
    }

    #[tokio::test]
    async fn peer_closure_after_completion_is_benign() {
        let t = tracker_over_sink();
        // has_root_item=false sets structure_complete AND root_complete → is_done() == true.
        t.lock().await.set_structure_complete(false);
        assert!(t.lock().await.is_done());
        let r = run_handle_file_stream(t, std::io::ErrorKind::UnexpectedEof).await;
        assert!(
            r.is_ok(),
            "a peer closure after completion is a normal end-of-transfer"
        );
    }

    #[tokio::test]
    async fn peer_closure_during_our_teardown_is_benign() {
        let t = tracker_over_sink();
        t.lock().await.close_stream().await; // sets is_closing()
        assert!(t.lock().await.is_closing());
        let r = run_handle_file_stream(t, std::io::ErrorKind::ConnectionReset).await;
        assert!(
            r.is_ok(),
            "a peer closure during an abort we initiated is benign"
        );
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
