use anyhow::Context;
use common::safedir::{Dir, RemovalSnapshot};
use futures::{FutureExt, StreamExt};
use std::ffi::OsStr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tracing::{Instrument, instrument};

use super::directory_tracker;
use crate::receiver_shutdown::ReceiverShutdown;

#[cfg(test)]
mod file_tests;
mod resources;
#[cfg(test)]
mod testutils;

fn progress() -> &'static common::progress::Progress {
    common::get_progress()
}

/// Resolve the open `Dir` of `dst`'s parent for a fd-relative destination write.
///
/// The destination tracks every created directory's `Dir` in the fd-map, top-down
/// (a parent's Begin precedes any message for its children), so for a
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
        if let Some(parent) = directory_tracker.with_state(|state| state.root_parent_dir()) {
            return Ok((parent, name));
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
        directory_tracker.with_state(|state| state.set_root_parent_dir(parent.clone()));
        Ok((parent, name))
    } else {
        // non-root: the parent must already be tracked (top-down creation guarantees
        // it). Fail closed if it is missing rather than re-resolving the path.
        let parent = directory_tracker.with_state(|state| state.get_dir(parent_path));
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

struct DataConnectionPool {
    data_addr: std::net::SocketAddr,
    network_profile: remote::NetworkProfile,
    /// Liveness budget applied to each data connection (see `remote::configure_tcp_socket`).
    /// These are `ConnectionKind::Data`: keepalive only, no `TCP_USER_TIMEOUT` — this side stops
    /// reading for as long as its iops reservation makes it wait, and a user timeout cannot tell
    /// that from a dead peer.
    keepalive_sec: u64,
    copy_buffer_retention_limit: usize,
    /// Semaphore to limit concurrent connections
    semaphore: std::sync::Arc<tokio::sync::Semaphore>,
    /// Optional TLS connector for encrypted connections
    tls_connector: Option<std::sync::Arc<tokio_rustls::TlsConnector>>,
    /// Upper bound on a single TCP-connect + TLS-handshake, so a worker already past the semaphore
    /// but stuck mid-handshake cannot block the pool drain forever (it would otherwise leave the
    /// file-handler future — and thus `run_destination`'s teardown — waiting indefinitely).
    conn_timeout: std::time::Duration,
    shutdown: ReceiverShutdown,
}

impl DataConnectionPool {
    fn new(
        data_addr: std::net::SocketAddr,
        max_connections: usize,
        tcp_config: &remote::TcpConfig,
        tls_connector: Option<std::sync::Arc<tokio_rustls::TlsConnector>>,
        shutdown: ReceiverShutdown,
    ) -> Self {
        Self {
            data_addr,
            network_profile: tcp_config.network_profile,
            keepalive_sec: tcp_config.keepalive_sec,
            copy_buffer_retention_limit: tcp_config.effective_buffer_retention_limit(),
            semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(max_connections)),
            tls_connector,
            conn_timeout: std::time::Duration::from_secs(tcp_config.conn_timeout_sec),
            shutdown,
        }
    }
    /// Open a new connection to the source's data port.
    async fn connect(&self) -> ConnectOutcome {
        // every shutdown owner releases both admission and in-flight connection waits; the timeout
        // bounds TCP/TLS setup after admission, without charging time spent waiting for a permit
        tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => ConnectOutcome::PoolClosed,
            result = async {
                let permit = self.semaphore.clone().acquire_owned().await
                    .expect("connection admission semaphore is never closed");
                match tokio::time::timeout(self.conn_timeout, self.connect_and_handshake()).await {
                    Ok(Ok(recv_stream)) => ConnectOutcome::Connected(recv_stream, permit),
                    Ok(Err(e)) => ConnectOutcome::Failed(e),
                    Err(_elapsed) => ConnectOutcome::Failed(anyhow::anyhow!(
                        "data connection to source timed out after {}s",
                        self.conn_timeout.as_secs()
                    )),
                }
            } => result,
        }
    }
    /// The blocking-on-I/O part of [`Self::connect`], factored out so it can be bounded/cancelled.
    ///
    /// The handshake bound here is `conn_timeout`, the same deadline [`Self::connect`] applies to
    /// the whole TCP-connect + handshake; only the read half is kept (this side never sends on a
    /// data connection).
    async fn connect_and_handshake(&self) -> anyhow::Result<remote::streams::BoxedRecvStream> {
        let stream =
            remote::connect_tcp_data(self.data_addr, self.network_profile, self.keepalive_sec)
                .await?;
        let (_send_stream, recv_stream) = remote::tls::connect_bounded(
            self.tls_connector.as_deref(),
            remote::tls::SERVER_NAME_SOURCE,
            stream,
            self.conn_timeout,
            "data",
        )
        .await?;
        let recv_stream =
            recv_stream.with_copy_buffer_retention_limit(self.copy_buffer_retention_limit);
        tracing::debug!(
            copy_buffer_retention_limit = recv_stream.copy_buffer_retention_limit(),
            "configured receiver data stream"
        );
        Ok(recv_stream)
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
    common::timing_scope!(trace, "destination.file.drain")
        .measure(stream.copy_exact_to_buffered(&mut sink, size, 8192))
        .await?;
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
    // PLAN, then acquire, then MUTATE — the same split as `plan_dst_file`/`execute_dst_plan` in
    // common/src/copy.rs. Here it buys less than it does locally: there is no source open to fail,
    // only the iops reservation to wait for, so what the ordering avoids is unlinking the destination
    // and then sitting on `--iops-throttle` for seconds before putting anything back. It is NOT
    // cancellation safety — `create_file` waits on the ops-throttle internally, so a cancelled
    // transfer can still land between the removal and the create — and it does not try to be; see
    // `common::copy::copy_file_fd` for why rcp accepts that instead of staging and renaming.
    let plan = common::timing_scope!(trace, "destination.file.plan")
        .measure(plan_dst_file(settings, file_header, dst_parent, dst_name))
        .await
        .map_err(err_needs_drain)?;
    // a skipped file's bytes are already on the wire and must come off it before the next header.
    if matches!(plan, DstFilePlan::Skip) {
        drain_file_data(file_recv_stream, file_header.size)
            .await
            .map_err(err_corrupted)?;
        return Ok(());
    }
    // logged between deciding and waiting, so it marks the point where this side has committed to a
    // plan but has not yet acted on it — the interval a competing writer can still slip into, and the
    // one a throttle-bound transfer spends all its time in.
    tracing::debug!("destination slot classified, reserving iops budget");
    common::timing_scope!(trace, "destination.file.wait_iops")
        .measure(
            throttle::get_file_iops_tokens(settings.chunk_size, file_header.size).instrument(
                tracing::trace_span!("iops_throttle", size = file_header.size),
            ),
        )
        .await;
    // the reservation is held, so the occupied entry can go through the pinned parent.
    if let DstFilePlan::Replace(dst_snapshot) = &plan {
        tracing::debug!("destination differs, removing existing entry");
        remove_existing_dst(
            dst_parent,
            dst_name,
            &file_header.dst,
            *dst_snapshot,
            settings,
        )
        .await
        .map_err(err_needs_drain)?;
    }
    // create the destination file fresh through the parent's pinned fd (O_CREAT|O_EXCL|
    // O_NOFOLLOW): never follows a symlink, never escapes dst_parent. it is created owner-only
    // (`DST_FILE_CREATE_MODE`) and only widened to the source mode by `set_file_metadata_owned`
    // below, after the last byte, mirroring copy.rs.
    let std_file = match common::timing_scope!(trace, "destination.file.create")
        .measure(dst_parent.create_file(dst_name))
        .await
    {
        Ok(std_file) => std_file,
        // the slot is occupied: a writer filled it between the classification above — or during the
        // `--iops-throttle` wait after it, which can be seconds — and now. `create_file`'s `O_EXCL`
        // is the only way this side finds out. Resolve it here and honor --overwrite /
        // --ignore-existing rather than failing the file on EEXIST regardless of either, exactly as
        // the local copy does (common/src/copy.rs). Planning and executing are back-to-back on this
        // route, unlike the one above: the reservation is already held, so nothing is left to wait
        // for between the removal and the retry.
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            tracing::debug!("destination appeared after classification, re-planning");
            let plan = common::timing_scope!(trace, "destination.file.plan")
                .measure(plan_dst_file(settings, file_header, dst_parent, dst_name))
                .await
                .map_err(err_needs_drain)?;
            if matches!(plan, DstFilePlan::Skip) {
                drain_file_data(file_recv_stream, file_header.size)
                    .await
                    .map_err(err_corrupted)?;
                return Ok(());
            }
            if let DstFilePlan::Replace(dst_snapshot) = &plan {
                remove_existing_dst(
                    dst_parent,
                    dst_name,
                    &file_header.dst,
                    *dst_snapshot,
                    settings,
                )
                .await
                .map_err(err_needs_drain)?;
            }
            // retry exactly once. the slot was cleared just above, so a second EEXIST means yet
            // another writer refilled it — report that rather than looping, which against a live
            // competing writer would never terminate.
            common::timing_scope!(trace, "destination.file.create")
                .measure(dst_parent.create_file(dst_name))
                .await
                .with_context(|| format!("failed creating {:?}", file_header.dst))
                .map_err(err_needs_drain)?
        }
        Err(error) => {
            return Err(err_needs_drain(
                anyhow::Error::new(error).context(format!("failed creating {:?}", file_header.dst)),
            ));
        }
    };
    // wrap the std file for async writes; the underlying fd is retained so its metadata
    // can be applied through the held fd (no path re-open).
    let mut file = tokio::fs::File::from_std(std_file);
    // buffer size is set by tcp_config.effective_remote_copy_buffer_size() based on network profile,
    // but capped at file size to avoid over-allocation for small files
    let file_size = file_header.size.min(usize::MAX as u64) as usize;
    let buffer_size = settings.remote_copy_buffer_size.min(file_size).max(1);
    // once we start reading from the stream, any error means the stream is corrupted
    let copied = common::timing_scope!(trace, "destination.file.receive")
        .measure(
            file_recv_stream
                .copy_exact_to_buffered(&mut file, file_header.size, buffer_size)
                .instrument(tracing::trace_span!(
                    "recv_data",
                    size = file_header.size,
                    buffer_size
                )),
        )
        .await
        .map_err(err_corrupted)?;
    if copied != file_header.size {
        return Err(err_corrupted(anyhow::anyhow!(
            "File size mismatch: expected {} bytes, copied {} bytes",
            file_header.size,
            copied
        )));
    }
    finalize_received_file(file, file_header, preserve)
        .await
        .map_err(err_data_consumed)
}

/// Finish an already-consumed payload before applying metadata and recording copied counts.
async fn finalize_received_file(
    mut file: tokio::fs::File,
    file_header: &remote::protocol::File,
    preserve: &common::preserve::Settings,
) -> anyhow::Result<()> {
    let prog = progress();
    // flush before metadata to ensure all data reaches the kernel before we set mtime.
    // tokio::fs::File hands writes to a threadpool - without flush, the threadpool
    // may complete after we set mtime, causing the file to appear modified.
    common::timing_scope!(trace, "destination.file.flush")
        .measure(file.flush())
        .await
        .with_context(|| format!("failed flushing {:?}", file_header.dst))?;
    // conversion preserves the descriptor, but cannot report delayed write errors: flush first
    let file = file.try_into_std().map_err(|_| {
        anyhow::anyhow!(
            "failed converting flushed destination {:?}: Tokio still retains a file reference",
            file_header.dst
        )
    })?;
    tracing::info!(
        "File {} -> {} created, size: {} bytes, setting metadata...",
        file_header.src.display(),
        file_header.dst.display(),
        file_header.size
    );
    // Count the file BEFORE applying metadata: its bytes are already on disk, so a metadata
    // failure below must not erase it from the summary. This mirrors the local path
    // (`common::copy`, which increments its progress counters before `set_file_metadata_owned`) —
    // the remote summary is built from these counters, so incrementing after would report
    // "files copied: 0" for a tree whose data transferred completely and only failed to be
    // chowned. The metadata error is still recorded and still fails the copy.
    prog.files_copied.inc();
    prog.bytes_copied.add(file_header.size);
    // metadata errors happen after all bytes consumed - stream is at clean boundary.
    // apply through the file's OWN fd (fd-relative): no path re-resolution of dst.
    // The source's ACLs travel in the wire header. A `Captured` all-`None` value means the source
    // had none and the destination's must be CLEARED, not left alone (an inherited default ACL
    // would otherwise widen this file past its source); `Unknown` (capture off) hands the applier
    // `None`, which it ignores with `f:acl` off — and refuses, fail-closed, if `f:acl` were on,
    // which cannot happen: the master derives capture from the same `preserve` this applier uses.
    // (A source that cannot READ a file's ACL sends `FileSkipped`, never a `File` header, so an
    // `Unknown` here is never a degraded read.)
    let src_acls = file_header.metadata.captured_acls();
    common::timing_scope!(trace, "destination.file.metadata")
        .measure(common::safedir::set_file_metadata_owned(
            preserve,
            &file_header.metadata,
            src_acls.as_ref(),
            file.into(),
            common::Side::Destination,
        ))
        .await
        .with_context(|| format!("failed setting metadata on {:?}", file_header.dst))?;
    Ok(())
}

/// What to do about whatever occupies the destination's slot — the destination counterpart of
/// `common::copy::FilePlan`.
enum DstFilePlan {
    /// Nothing occupies the slot; create the file directly.
    Vacant,
    /// An entry must be removed before the create (`--overwrite`).
    Replace(RemovalSnapshot),
    /// This file is not copied (`--ignore-existing`, or an identical / newer destination under
    /// `--overwrite`). Its bytes are still on the wire, so the caller must drain them.
    Skip,
}

/// Classify whatever occupies `dst_name` and decide what to do about it.
///
/// **Non-mutating.** It classifies and decides; the caller removes. Keep it that way — the separation
/// is what lets the caller finish acquiring the iops reservation before the destination stops
/// existing.
///
/// The mirror of `common::copy::plan_dst_file`, and it keeps that function's two rules: the lookup
/// goes through the parent's pinned fd (`O_NOFOLLOW`) rather than re-resolving `file_header.dst` by
/// path, and only `NotFound` is taken to mean the slot is empty.
///
/// [`process_single_file`] calls this from both of its routes: up front, and again when `create_file`
/// reports `EEXIST` because a writer filled the slot after the first look. A skip decision counts
/// itself here; draining the skipped file's bytes is the caller's job, since only it holds the stream.
async fn plan_dst_file(
    settings: &common::copy::Settings,
    file_header: &remote::protocol::File,
    dst_parent: &Arc<Dir>,
    dst_name: &OsStr,
) -> anyhow::Result<DstFilePlan> {
    let prog = progress();
    let dst_handle = match dst_parent.child(dst_name).await {
        Ok(dst_handle) => dst_handle,
        // `NotFound` is the ONE error that means the slot is empty. Every other failure (EACCES on
        // the parent, EMFILE, ESTALE, EIO) says we could not look, not that there is nothing there,
        // and must keep its own cause — mirroring `plan_dst_file`. Treating them as "vacant" made
        // the create's error stand in for the real one, and `--ignore-existing` fail rather than
        // skip.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DstFilePlan::Vacant);
        }
        Err(error) => {
            return Err(anyhow::Error::new(error).context(format!(
                "failed looking up destination {:?}",
                file_header.dst
            )));
        }
    };
    if settings.ignore_existing {
        tracing::debug!("destination exists, skipping (--ignore-existing)");
        prog.files_unchanged.inc();
        return Ok(DstFilePlan::Skip);
    }
    if !settings.overwrite {
        return Err(anyhow::anyhow!(
            "destination {:?} already exists, did you intend to specify --overwrite?",
            file_header.dst
        ));
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
            return Ok(DstFilePlan::Skip);
        }
        if let Some(common::copy::OverwriteFilter::Newer) = settings.overwrite_filter
            && common::filecmp::dest_is_newer(&src_file_metadata, dst_handle.meta())
        {
            tracing::debug!("dest is newer than source, skipping");
            prog.files_unchanged.inc();
            return Ok(DstFilePlan::Skip);
        }
    }
    Ok(DstFilePlan::Replace(dst_handle.into_removal_snapshot()))
}

/// Adapt the shared local/link overwrite removal to the remote destination's error type.
async fn remove_existing_dst(
    dst_parent: &Arc<Dir>,
    dst_name: &OsStr,
    dst_path: &std::path::Path,
    dst_snapshot: RemovalSnapshot,
    settings: &common::copy::Settings,
) -> anyhow::Result<()> {
    common::copy::remove_existing(
        progress(),
        dst_parent,
        dst_name,
        dst_path,
        dst_snapshot,
        settings,
    )
    .await
    .map(|_summary| ())
    .map_err(|err| err.source)
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
/// Shared cancellation is observable before control cleanup can wait on the stream lock.
fn peer_close_is_benign(directory_tracker: &directory_tracker::SharedDirectoryTracker) -> bool {
    directory_tracker.shutdown().is_cancelled()
        || directory_tracker.with_state(|state| state.is_done())
}

/// Recognize transport closure independently of framing, decoding, and protocol failures.
fn is_peer_closure(error: &anyhow::Error) -> bool {
    error.downcast_ref::<std::io::Error>().is_some_and(|io| {
        use std::io::ErrorKind::{
            BrokenPipe, ConnectionAborted, ConnectionReset, NotConnected, UnexpectedEof,
        };
        matches!(
            io.kind(),
            UnexpectedEof | ConnectionReset | ConnectionAborted | BrokenPipe | NotConnected
        )
    })
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
            .await
        {
            Ok(Some(h)) => h,
            Ok(None) => {
                // A CLEAN framed EOF at a header boundary. This is NOT self-evidently benign: the
                // source closes a data stream gracefully while it keeps running (a send failure
                // discards the stream with `close()`, and the pool drain closes returning streams),
                // and on plain TCP a graceful FIN is the ordinary shape anyway. So it goes through
                // the SAME completion gate as a transport-level closure — see `peer_close_is_benign`.
                if !peer_close_is_benign(&directory_tracker) {
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
                if is_peer_closure(&e) {
                    // The kind alone is ambiguous — a truncated header ending in a reset looks
                    // identical to a benign close — so COMPLETION STATE decides, via the same gate
                    // the clean-EOF arm uses. A PRE-completion closure is FATAL: it propagates, the
                    // worker aborts, the join loop signals teardown, and the copy fails with this
                    // cause.
                    if peer_close_is_benign(&directory_tracker) {
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
        if file_header.is_root {
            directory_tracker.with_state(|state| state.observe_root())?;
        }
        tracing::info!(
            "Received file: {:?} -> {:?}",
            file_header.src,
            file_header.dst
        );
        // acquire throttle permits for this file
        let _open_file_guard = common::timing_scope!(trace, "destination.file.wait_open")
            .measure(
                throttle::open_file_permit().instrument(tracing::trace_span!("open_file_permit")),
            )
            .await;
        common::timing_scope!(trace, "destination.file.wait_rate")
            .measure(throttle::get_ops_token())
            .await;
        let _ops_guard = prog.ops.guard();
        // resolve the destination parent directory's held fd from the tracker (for the
        // root file, open the trusted parent via open_parent_dir). all writes for this
        // file are then fd-relative on that pinned parent. a resolution failure is a
        // pre-data error: the stream can be recovered by draining this file's bytes.
        let file_result = common::safedir::with_fd_admission(_open_file_guard.admission(), async {
            match common::timing_scope!(trace, "destination.file.parent")
                .measure(resolve_parent_dir(
                    &directory_tracker,
                    &file_header.dst,
                    file_header.is_root,
                ))
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
            }
        })
        .await;
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
        common::timing_scope!(trace, "destination.file.complete")
            .measure(async {
                if fail_early_error.is_some() || stream_corrupted {
                    directory_tracker.begin_close();
                }
                if file_header.is_root {
                    tracing::info!(
                        "Root file processed (success={})",
                        fail_early_error.is_none() && !stream_corrupted
                    );
                    directory_tracker.with_state(|state| state.set_root_complete());
                } else {
                    // get parent directory
                    let parent_dir = file_header.dst.parent().ok_or_else(|| {
                        anyhow::anyhow!("file {:?} has no parent directory", file_header.dst)
                    })?;
                    directory_tracker
                        .process_file(parent_dir)
                        .await
                        .context("Failed to update directory tracker after receiving file")?;
                }
                anyhow::Ok(())
            })
            .await?;
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
    common::task_scope::scope_tasks(async {
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
            common::task_scope::spawn_tracked(&mut join_set, async move {
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
                            pool.shutdown.record_first_connect_error(e);
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
        // the signal to after the loop would therefore deadlock. Stream cleanup is idempotent,
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
                directory_tracker.close_stream().await;
            }
        }
        join_set.shutdown().await;
        tracing::info!("All file streams completed");
        Ok(())
    })
    .await
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
    /// what the lockdown must undo at completion — the original owner and the original ACLs
    /// (`None` in the default path — see [`common::safedir::lockdown_reused_dir`])
    AlreadyExisted(Arc<Dir>, Option<common::safedir::ReusedDirLock>),
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
    lookup_concurrency: std::num::NonZeroUsize,
) -> Vec<remote::protocol::ExistingEntry> {
    common::timing_scope!("destination.manifest.inventory")
        .measure(async {
            // a cap of 0 disables the optimization for every non-empty directory; short-circuit before
            // the readdir so the disable case pays nothing.
            if max_entries == 0 {
                return Vec::new();
            }
            // capped enumeration: an over-cap directory stops at cap+1 instead of being read in full
            // only to be discarded — the full read was unbounded, uncancellable work on the blocking pool
            let entries = match dir.read_entries_capped(max_entries).await {
                Ok(Some(entries)) => entries,
                Ok(None) => {
                    tracing::debug!(
                        "manifest: directory exceeds cap {}, skipping manifest (files will transfer)",
                        max_entries
                    );
                    return Vec::new();
                }
                Err(e) => {
                    tracing::debug!("manifest: cannot enumerate destination directory: {:#}", e);
                    return Vec::new();
                }
            };
            let mut manifest = Vec::with_capacity(entries.len());
            let mut lookups = futures::stream::iter(entries)
                .map(|(name, _hint)| lookup_manifest_entry(dir, name))
                .buffer_unordered(lookup_concurrency.get());
            while let Some(entry) = lookups.next().await {
                if let Some(entry) = entry {
                    manifest.push(entry);
                }
            }
            manifest
        })
        .await
}

/// Classify one manifest entry while admission covers its transient handle and blocking owners.
async fn lookup_manifest_entry(
    dir: &Dir,
    name: std::ffi::OsString,
) -> Option<remote::protocol::ExistingEntry> {
    use common::preserve::Metadata as _;
    let guard = throttle::pending_meta_permit().await;
    common::safedir::with_fd_admission(guard.admission(), async {
        match dir.child(&name).await {
            Ok(handle) => {
                let meta = handle.meta();
                let entry = remote::protocol::ExistingEntry {
                    name: std::path::PathBuf::from(name),
                    is_file: handle.kind() == common::walk::EntryKind::File,
                    metadata: remote::protocol::Metadata::from(meta),
                    size: meta.size(),
                };
                drop(handle);
                Some(entry)
            }
            Err(error) => {
                let error: anyhow::Error = error.into();
                tracing::debug!("manifest: cannot stat child {:?}: {error:#}", name);
                None
            }
        }
    })
    .await
}

async fn make_admitted_directory(
    parent: &Dir,
    name: &OsStr,
    credit: Option<Arc<resources::DirectoryLease>>,
) -> std::io::Result<Dir> {
    match credit {
        Some(credit) => {
            parent
                .make_dir_admitted(name, common::safedir::DST_DIR_CREATE_MODE, credit)
                .await
        }
        None => {
            parent
                .make_dir(name, common::safedir::DST_DIR_CREATE_MODE)
                .await
        }
    }
}

/// Create a directory fd-relative on the PARENT's held `Dir`, handling overwrite logic.
///
/// All operations resolve relative to `dst_parent`'s pinned fd: classify an existing entry via
/// `dst_parent.child(dst_name)`; create via `dst_parent.make_dir(dst_name, mode)` (`mkdirat`);
/// reuse an existing directory via `dst_parent.open_dir(dst_name)` (`O_NOFOLLOW|O_DIRECTORY` — a
/// directory→symlink swap fails closed with ELOOP/ENOTDIR); replace a non-directory via
/// fd-relative [`remove_existing_dst`] then `make_dir`. A privileged destination therefore cannot
/// be redirected by a concurrent symlink swap of the parent into creating a directory outside the
/// destination tree. The new directory is created at
/// [`common::safedir::DST_DIR_CREATE_MODE`] (writable so children can be populated); its real
/// source mode is applied later by `DirectoryFinalization::execute`, mirroring the path-based /
/// local-copy behavior.
///
/// Returns the result; does NOT increment progress counters — the caller defers the increment
/// until completion (when it knows whether the directory is kept).
async fn create_directory_admitted(
    settings: &common::copy::Settings,
    dst_parent: &Arc<Dir>,
    dst_name: &OsStr,
    dst: &std::path::Path,
    credit: Option<Arc<resources::DirectoryLease>>,
) -> anyhow::Result<DirectoryCreateResult> {
    #[cfg(test)]
    teardown_tests::gate_preparation(dst).await?;
    let prog = progress();
    match make_admitted_directory(dst_parent, dst_name, credit.clone()).await {
        Ok(dir) => {
            // don't increment counter here - will be done during directory finalization
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
                let dir = match credit {
                    Some(credit) => dst_parent.open_dir_admitted(dst_name, credit).await,
                    None => dst_parent.open_dir(dst_name).await,
                }
                .with_context(|| format!("cannot open existing directory {dst:?}"))?;
                // strict-only lockdown: take over the reused directory, restrict it to 0o700 and
                // remove its default ACL for the copy's duration. The mode masks access ACLs;
                // finalization restores metadata (no-op / None in the default path). A recheck, EPERM or ACL
                // failure propagates as this directory's create error — the caller marks it Failed
                // (or aborts under --fail-early), so no child is written into an unsecured
                // directory, nor one whose default ACL children would still inherit.
                let reused_lock = common::safedir::lockdown_reused_dir(&dir, &dst_handle)
                    .await
                    .with_context(|| {
                        format!("cannot secure reused destination directory {dst:?}")
                    })?;
                prog.directories_unchanged.inc();
                Ok(DirectoryCreateResult::AlreadyExisted(
                    Arc::new(dir),
                    reused_lock,
                ))
            } else if settings.ignore_existing {
                // not a directory but ignore_existing is set - skip the subtree
                tracing::debug!(
                    "destination exists but is not a directory, skipping subtree (--ignore-existing)"
                );
                prog.directories_unchanged.inc();
                Ok(DirectoryCreateResult::Skipped)
            } else if settings.overwrite {
                // not a directory but overwrite is enabled - remove fd-relatively and create.
                tracing::info!("destination is not a directory, removing and creating a new one");
                remove_existing_dst(
                    dst_parent,
                    dst_name,
                    dst,
                    dst_handle.into_removal_snapshot(),
                    settings,
                )
                .await?;
                let dir = make_admitted_directory(dst_parent, dst_name, credit)
                    .await
                    .with_context(|| format!("cannot create directory {dst:?}"))?;
                // don't increment counter here - will be done during directory finalization
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
/// is fd-relative via [`remove_existing_dst`]. A privileged destination therefore cannot be
/// redirected by a concurrent symlink swap of the parent into creating a link outside the
/// destination tree.
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
            // remove the conflicting entry fd-relatively, then create the link.
            remove_existing_dst(
                dst_parent,
                dst_name,
                dst,
                dst_handle.into_removal_snapshot(),
                settings,
            )
            .await?;
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

/// Flush a directory's manifest and Ready contiguously, then evaluate finalization.
/// Release the send lock before recording Ready and awaiting any resulting finalization.
/// Finalization publishes logical completion for the control receiver's terminal send.
async fn announce_directory_ready(
    directory_tracker: &directory_tracker::SharedDirectoryTracker,
    control_send: &remote::streams::BoxedSharedSendStream,
    data_pool: &DataConnectionPool,
    src: &std::path::Path,
    dst: &std::path::Path,
    existing: Vec<remote::protocol::ExistingEntry>,
) -> anyhow::Result<()> {
    let manifest_entries = existing.len();
    common::timing_scope!("destination.manifest.flush")
        .measure(async {
            let chunks = remote::protocol::chunk_manifest(
                existing,
                remote::protocol::MANIFEST_CHUNK_BYTE_BUDGET,
            );
            {
                let mut stream = control_send.lock().await;
                for entries in chunks {
                    let chunk_msg = remote::protocol::DestinationMessage::DirectoryManifestChunk {
                        dst: dst.to_path_buf(),
                        entries,
                    };
                    stream
                        .send_batch_message(&chunk_msg)
                        .await
                        .map_err(|error| classify_control_reply_error(error, data_pool))?;
                }
                let message = remote::protocol::DestinationMessage::DirectoryReady {
                    src: src.to_path_buf(),
                    dst: dst.to_path_buf(),
                };
                stream
                    .send_control_message(&message)
                    .await
                    .map_err(|error| classify_control_reply_error(error, data_pool))
                    .context("Failed to send DirectoryReady")?;
            }
            anyhow::Ok(())
        })
        .await?;
    tracing::info!(
        "Sent DirectoryReady: {:?} -> {:?} (manifest={})",
        src,
        dst,
        manifest_entries
    );
    directory_tracker
        .mark_announced(dst)
        .await
        .context("Failed to complete announced directory")?;
    Ok(())
}

fn destination_child_name(dst: &std::path::Path) -> anyhow::Result<std::ffi::OsString> {
    dst.file_name()
        .map(std::ffi::OsStr::to_os_string)
        .ok_or_else(|| anyhow::anyhow!("destination {dst:?} has no file name"))
}

async fn wait_for_directory_parent(
    tracker: &directory_tracker::SharedDirectoryTracker,
    pool: &DataConnectionPool,
    dst: &std::path::Path,
) -> anyhow::Result<Option<Arc<Dir>>> {
    let mut creation = tracker.with_state(|state| state.parent_creation(dst))?;
    common::timing_scope!("destination.directory.wait_parent")
        .measure(async {
            loop {
                match creation.borrow_and_update().clone() {
                    directory_tracker::DirectoryCreation::Created(dir) => return Ok(Some(dir)),
                    directory_tracker::DirectoryCreation::Rejected => return Ok(None),
                    directory_tracker::DirectoryCreation::Pending => {}
                }
                tokio::select! {
                    biased;
                    _ = pool.shutdown.cancelled() => return Err(ControlReplyDuringTeardown.into()),
                    result = creation.changed() => {
                        // an abandoned creator owns the real cause; dependent cancellation must not
                        // race it into the error collector with a synthetic missing-parent error
                        if result.is_err() { return Err(ControlReplyDuringTeardown.into()); }
                    }
                }
            }
        })
        .await
}

#[allow(clippy::too_many_arguments)]
async fn prepare_and_announce_directory(
    settings: &common::copy::Settings,
    tracker: &directory_tracker::SharedDirectoryTracker,
    send: &remote::streams::BoxedSharedSendStream,
    pool: &DataConnectionPool,
    errors: &common::error_collector::ErrorCollector,
    admission: &mut Option<directory_tracker::DirectoryAdmission>,
    src: &std::path::Path,
    dst: &std::path::Path,
    metadata: remote::protocol::Metadata,
    is_root: bool,
    keep_if_empty: bool,
    slots: Arc<tokio::sync::Semaphore>,
    build_slot: Arc<tokio::sync::Semaphore>,
    manifest_cap: usize,
    lookup_concurrency: std::num::NonZeroUsize,
    credit: Option<Arc<resources::DirectoryLease>>,
) -> anyhow::Result<()> {
    let _ops_guard = progress().ops.guard();
    let parent = if is_root {
        None
    } else {
        wait_for_directory_parent(tracker, pool, dst).await?
    };
    if !is_root && parent.is_none() {
        tracker
            .reject_directory(admission.take().expect("owned directory admission"), None)
            .await?;
        tracker
            .send_directory_skipped(src, dst)
            .await
            .map_err(|error| classify_control_reply_error(error, pool))?;
        return Ok(());
    }
    let permit = common::timing_scope!("destination.directory.wait_prepare")
        .measure(slots.acquire_owned())
        .await
        .context("directory preparation admission closed")?;
    let result = common::timing_scope!("destination.directory.prepare")
        .measure(async {
            let (parent, name) = match parent {
                Some(parent) => (parent, destination_child_name(dst)?),
                None => resolve_parent_dir(tracker, dst, is_root)
                    .await
                    .context("failed resolving destination parent directory")?,
            };
            create_directory_admitted(settings, &parent, &name, dst, credit.clone()).await
        })
        .await;
    let result = match result {
        Ok(DirectoryCreateResult::Failed) => Err(anyhow::anyhow!(
            "destination {dst:?} exists and is not a directory, use --overwrite to replace"
        )),
        result => result,
    };
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            tracing::error!("Failed to create directory {dst:?}: {error:#}");
            if settings.fail_early {
                return Err(error);
            }
            errors.push(error);
            DirectoryCreateResult::Failed
        }
    };
    let was_created = matches!(result, DirectoryCreateResult::Created(_));
    let resolved = match result {
        DirectoryCreateResult::Created(dir) => Some((dir, None)),
        DirectoryCreateResult::AlreadyExisted(dir, lockdown) => Some((dir, lockdown)),
        DirectoryCreateResult::Skipped | DirectoryCreateResult::Failed => None,
    };
    if let Some((dir, lockdown)) = resolved {
        tracker
            .register_directory(
                admission.take().expect("owned directory admission"),
                dir.clone(),
                metadata,
                was_created,
                keep_if_empty,
                lockdown,
            )
            .map_err(|error| {
                if error.is::<directory_tracker::DirectoryRegistrationDuringTeardown>() {
                    error.context(ControlReplyDuringTeardown)
                } else {
                    error
                }
            })?;
        drop(permit);
        if !was_created && (settings.overwrite || settings.ignore_existing) {
            let _slot = common::timing_scope!("destination.manifest.wait_build")
                .measure(build_slot.acquire_owned())
                .await
                .context("manifest build admission closed")?;
            let existing = build_existing_manifest(&dir, manifest_cap, lookup_concurrency).await;
            announce_directory_ready(tracker, send, pool, src, dst, existing).await?;
        } else {
            announce_directory_ready(tracker, send, pool, src, dst, Vec::new()).await?;
        }
    } else {
        tracker
            .reject_directory(
                admission.take().expect("owned directory admission"),
                Some(permit),
            )
            .await?;
        tracker
            .send_directory_skipped(src, dst)
            .await
            .map_err(|error| classify_control_reply_error(error, pool))?;
    }
    Ok(())
}

/// Identifies a failure to decode or apply a control message, independent of stream teardown.
#[derive(Debug, thiserror::Error)]
#[error("source control message failed")]
struct ControlMessageFailure;

/// Identifies a reply transport failure observed after destination teardown began.
#[derive(Debug, thiserror::Error)]
#[error("control reply interrupted by destination teardown")]
struct ControlReplyDuringTeardown;

fn classify_control_reply_error(error: anyhow::Error, pool: &DataConnectionPool) -> anyhow::Error {
    // classify at the send site: a filesystem failure with the same errno must stay fatal.
    if pool.shutdown.is_cancelled() && is_peer_closure(&error) {
        error.context(ControlReplyDuringTeardown)
    } else {
        error
    }
}

const DIRECTORY_RELEASE_BATCH_SIZE: usize = 64;

async fn flush_directory_releases(
    lifetimes: &mut resources::DirectoryLifetimes,
    released: resources::Released,
    send: &remote::streams::BoxedSharedSendStream,
    pool: &DataConnectionPool,
) -> anyhow::Result<()> {
    // bound writer ownership while draining only notifications that are already available
    let mut releases = Vec::with_capacity(DIRECTORY_RELEASE_BATCH_SIZE);
    releases.push(released);
    while releases.len() < DIRECTORY_RELEASE_BATCH_SIZE {
        let Some(released) = lifetimes.try_next_release() else {
            break;
        };
        releases.push(released);
    }
    let messages: Vec<_> = releases
        .iter()
        .map(
            |released| remote::protocol::DestinationMessage::DirectoryReleased {
                src: released.pair.src.clone(),
                dst: released.pair.dst.clone(),
            },
        )
        .collect();
    send.lock()
        .await
        .send_control_messages(&messages)
        .await
        .map_err(|error| classify_control_reply_error(error, pool))
        .context("failed to release destination directory capacity")
        .map_err(|error| {
            if is_peer_closure(&error) || error.is::<ControlReplyDuringTeardown>() {
                error
            } else {
                error.context(ControlMessageFailure)
            }
        })?;
    // no lifetime retires until the entire batch has been flushed successfully
    for released in &releases {
        lifetimes
            .acknowledged(released)
            .context(ControlMessageFailure)?;
    }
    Ok(())
}

fn destination_done_eligible(
    directory_tracker: &directory_tracker::SharedDirectoryTracker,
    directory_lifetimes: Option<&resources::DirectoryLifetimes>,
) -> bool {
    directory_tracker.with_state(|state| state.is_done())
        && directory_lifetimes.is_none_or(resources::DirectoryLifetimes::is_empty)
}

#[instrument(skip(
    error_collector,
    control_recv_stream,
    directory_tracker,
    control_send_stream,
    data_pool
))]
#[allow(clippy::too_many_arguments)]
async fn process_control_stream(
    directory_limits: Option<remote::protocol::DirectoryLimits>,
    settings: &common::copy::Settings,
    overwrite_manifest_max_entries: usize,
    manifest_lookup_concurrency: std::num::NonZeroUsize,
    max_directory_jobs: std::num::NonZeroUsize,
    preserve: &common::preserve::Settings,
    mut control_recv_stream: remote::streams::BoxedRecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
    control_send_stream: remote::streams::BoxedSharedSendStream,
    data_pool: std::sync::Arc<DataConnectionPool>,
    error_collector: std::sync::Arc<common::error_collector::ErrorCollector>,
) -> anyhow::Result<()> {
    let control = async {
        // membership covers parent waits, preparation, replies, and eligible finalization, including
        // finished tasks until reaped. Source credits alone do not bound post-Ready finalization.
        let mut directory_jobs = tokio::task::JoinSet::new();
        let mut directory_lifetimes = directory_limits.map(resources::DirectoryLifetimes::new);
        let preparation_slots = Arc::new(tokio::sync::Semaphore::new(
            manifest_lookup_concurrency.get(),
        ));
        let manifest_build_slot = Arc::new(tokio::sync::Semaphore::new(1));
        let receiving = async {
            loop {
                // release notifications can unblock discovery without another source frame. Keep the
                // same receive future across them: abandoning a partial frame would corrupt decoding.
                let source_message = {
                    let receive =
                        control_recv_stream.recv_object::<remote::protocol::SourceMessage>();
                    tokio::pin!(receive);
                    loop {
                        let releases_drained = directory_lifetimes
                            .as_ref()
                            .is_none_or(resources::DirectoryLifetimes::is_empty);
                        let watch_completion = releases_drained
                            || !directory_tracker.with_state(|state| state.is_done());
                        tokio::select! {
                            biased;
                            message = &mut receive => break message,
                            _ = data_pool.shutdown.cancelled() => return Ok(()),
                            released = async { directory_lifetimes.as_mut().expect("enabled directory lifetimes").next_release().await },
                                if directory_lifetimes.is_some() => {
                                flush_directory_releases(
                                    directory_lifetimes.as_mut().unwrap(),
                                    released,
                                    &control_send_stream,
                                    &data_pool,
                                ).await?;
                                if destination_done_eligible(&directory_tracker, directory_lifetimes.as_ref()) {
                                    directory_tracker.send_destination_done().await.context(ControlMessageFailure)?;
                                    return Ok(());
                                }
                            },
                            _ = directory_tracker.wait_for_completion(), if watch_completion => {
                                if destination_done_eligible(&directory_tracker, directory_lifetimes.as_ref()) {
                                    directory_tracker.send_destination_done().await.context(ControlMessageFailure)?;
                                    return Ok(());
                                }
                            },
                        }
                    }
                };
                let source_message = source_message
                    .map_err(|error| {
                        if is_peer_closure(&error) {
                            error
                        } else {
                            error.context(ControlMessageFailure)
                        }
                    })
                    .context("Failed to receive source message")?;
                let Some(source_message) = source_message else {
                    break;
                };
                // preserve a ready framing error above, but never admit buffered work after teardown
                if data_pool.shutdown.is_cancelled() {
                    break;
                }
                // reap announce tasks that already finished: without this the set grows by one JoinHandle
                // per reused directory for the whole copy, and a join failure would surface only after
                // the loop ends
                while let Some(joined) = directory_jobs.try_join_next() {
                    reap_announce_task(joined, &directory_tracker, &error_collector).await;
                }
                // protocol bookkeeping must not queue behind filesystem rate admission; filesystem
                // operations performed by the handlers consume their own tokens
                tracing::debug!("Received source message: {:?}", source_message);
                let prog = progress();
                let handling = async {
                    match source_message {
                        remote::protocol::SourceMessage::DirectoryBegin {
                            admission: directory_class,
                            ref src,
                            ref dst,
                            ref metadata,
                            is_root,
                            keep_if_empty,
                        } => {
                            while directory_jobs.len() >= max_directory_jobs.get() {
                                let joined = tokio::select! {
                                    biased;
                                    _ = data_pool.shutdown.cancelled() => return Ok(()),
                                    joined = common::timing_scope!("destination.directory.wait_job")
                                        .measure(directory_jobs.join_next()) => joined,
                                };
                                if let Some(joined) = joined {
                                    reap_announce_task(
                                        joined,
                                        &directory_tracker,
                                        &error_collector,
                                    )
                                    .await;
                                }
                            }
                            let directory_credit = directory_lifetimes
                                .as_mut()
                                .map(|lifetimes| lifetimes.admit(src, dst, directory_class))
                                .transpose()?;
                            let admission = directory_tracker
                                .with_state(|state| state.admit_directory(dst, is_root))?;
                            let tracker = directory_tracker.clone();
                            let send = control_send_stream.clone();
                            let pool = data_pool.clone();
                            let errors = error_collector.clone();
                            let settings = settings.clone();
                            let slots = preparation_slots.clone();
                            let build_slot = manifest_build_slot.clone();
                            let (src, dst, metadata) = (src.clone(), dst.clone(), metadata.clone());
                            common::task_scope::spawn_tracked(&mut directory_jobs, async move {
                                // retain the creation claim outside the fallible/unwind future so the
                                // primary cause is recorded before an abandoned publication wakes children
                                let mut admission = Some(admission);
                                let announcement = async {
                                    tokio::select! {
                                        biased;
                                        _ = pool.shutdown.cancelled() => Err(ControlReplyDuringTeardown.into()),
                                        result = prepare_and_announce_directory(&settings, &tracker, &send, &pool, &errors,
                                            &mut admission, &src, &dst, metadata, is_root, keep_if_empty, slots, build_slot,
                                            overwrite_manifest_max_entries, manifest_lookup_concurrency, directory_credit) => result,
                                    }
                                };
                                publish_announce_failure(&tracker, &errors, announcement).await;
                            });
                        }

                        remote::protocol::SourceMessage::DirectoryEnd {
                            src,
                            dst,
                            entry_count,
                        } => {
                            directory_tracker.seal_directory(&dst, entry_count).await?;
                            if let Some(lifetimes) = directory_lifetimes.as_mut() {
                                lifetimes.ended(&src, &dst)?;
                            }
                        }
                        remote::protocol::SourceMessage::Symlink {
                            ref src,
                            ref dst,
                            ref target,
                            ref metadata,
                            is_root,
                        } => {
                            directory_tracker.with_state(|state| {
                                state.ensure_discovering()?;
                                if is_root {
                                    state.observe_root()?;
                                }
                                anyhow::Ok(())
                            })?;
                            let _ops_guard = prog.ops.guard();
                            let parent = if is_root {
                                None
                            } else {
                                wait_for_directory_parent(&directory_tracker, &data_pool, dst)
                                    .await?
                            };
                            let has_failed_ancestor = !is_root && parent.is_none();
                            if has_failed_ancestor {
                                tracing::warn!(
                                    "Skipping symlink {:?} - ancestor failed to create",
                                    dst
                                );
                                // still count as a processed child entry for the parent
                                if !is_root && let Some(parent) = dst.parent() {
                                    directory_tracker
                                        .process_child_entry(parent)
                                        .await
                                        .context(
                                            "Failed to update parent tracker for skipped symlink",
                                        )?;
                                }
                                return Ok(());
                            }
                            // resolve the destination parent's held fd (for the root, open the trusted
                            // parent via open_parent_dir), then create the symlink fd-relative on it.
                            let resolved = match parent {
                                Some(parent) => {
                                    destination_child_name(dst).map(|name| (parent, name))
                                }
                                None => resolve_parent_dir(&directory_tracker, dst, is_root).await,
                            };
                            let result = match resolved {
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
                                Err(e) => {
                                    Err(e.context("failed resolving destination parent directory"))
                                }
                            };
                            if let Err(e) = result {
                                tracing::error!(
                                    "Failed to create symlink {:?} -> {:?}: {:#}",
                                    src,
                                    dst,
                                    e
                                );
                                if settings.fail_early {
                                    return Err(e);
                                }
                                error_collector.push(e);
                            }
                            // mark root symlink complete
                            if is_root {
                                directory_tracker.with_state(|state| state.set_root_complete());
                            }
                            // count this symlink as a processed child entry for its parent
                            if !is_root && let Some(parent) = dst.parent() {
                                directory_tracker
                                    .process_child_entry(parent)
                                    .await
                                    .context("Failed to update parent tracker for symlink")?;
                            }
                        }
                        remote::protocol::SourceMessage::DiscoveryComplete { has_root_item } => {
                            tracing::info!(
                                "Received DiscoveryComplete (has_root_item={})",
                                has_root_item
                            );
                            directory_tracker
                                .with_state(|state| state.finish_discovery(has_root_item))?;
                        }
                        remote::protocol::SourceMessage::FileSkipped { ref src, ref dst } => {
                            tracing::info!("File was skipped by source: {:?} -> {:?}", src, dst);
                            // get parent directory and update tracker
                            let parent_dir = dst.parent().ok_or_else(|| {
                                anyhow::anyhow!("skipped file {:?} has no parent", dst)
                            })?;
                            directory_tracker
                                .process_child_entry(parent_dir)
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
                            let parent_dir = dst.parent().ok_or_else(|| {
                                anyhow::anyhow!("unchanged file {:?} has no parent", dst)
                            })?;
                            directory_tracker
                                .process_child_entry(parent_dir)
                                .await
                                .context("Failed to update tracker for unchanged file")?;
                        }
                    }
                    anyhow::Ok(())
                };
                handling.await.map_err(|error| {
                    if error.is::<ControlReplyDuringTeardown>() {
                        error
                    } else {
                        error.context(ControlMessageFailure)
                    }
                })?;
                // ready control traffic must not starve already queued lifetime releases. Flush a
                // bounded batch after a valid message, then let the receive-first select check
                // buffered framing/protocol errors before those releases can make Done eligible.
                if !data_pool.shutdown.is_cancelled()
                    && let Some(lifetimes) = directory_lifetimes.as_mut()
                    && let Some(released) = lifetimes.try_next_release()
                {
                    flush_directory_releases(lifetimes, released, &control_send_stream, &data_pool)
                        .await?;
                    continue;
                }
                // check if we're done after each message
                if destination_done_eligible(&directory_tracker, directory_lifetimes.as_ref()) {
                    directory_tracker
                        .send_destination_done()
                        .await
                        .context(ControlMessageFailure)?;
                    break;
                }
            }
            anyhow::Ok(())
        };
        let result = std::panic::AssertUnwindSafe(receiving).catch_unwind().await;
        let result = match result {
            Ok(result) => result,
            Err(payload) => {
                let message = payload
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("non-string panic payload");
                Err(anyhow::anyhow!("control receiver panicked: {message}")
                    .context(ControlMessageFailure))
            }
        };
        // retain the initiating control failure before cancelling work and collecting teardown effects
        // local joining is unconditional, including when an enclosing rcpd task scope is already active
        if result.is_ok() && directory_tracker.with_state(|state| state.transfer_complete()) {
            while let Some(joined) = directory_jobs.join_next().await {
                reap_announce_task(joined, &directory_tracker, &error_collector).await;
            }
        } else {
            directory_tracker.begin_close();
            directory_jobs.shutdown().await;
        }
        // close recv stream
        control_recv_stream.close().await;
        tracing::info!("Control stream processing completed");
        result
    };
    common::task_scope::scope_tasks(control).await
}

/// Publish an announcer's error or unwind panic before awaiting teardown.
async fn publish_announce_failure(
    tracker: &directory_tracker::SharedDirectoryTracker,
    errors: &common::error_collector::ErrorCollector,
    announce: impl std::future::Future<Output = anyhow::Result<()>>,
) {
    // an abandoned finalizer can cancel shared work while this task is being polled. Publish its
    // cause before the next await: Tokio abort cannot interrupt this poll, and the receiver joins
    // every directory job before reading the collector or returning to its caller.
    let result = std::panic::AssertUnwindSafe(announce).catch_unwind().await;
    let error = match result {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error,
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic payload");
            anyhow::anyhow!("directory announcer panicked: {message}")
        }
    };
    if error.is::<ControlReplyDuringTeardown>() {
        tracing::debug!("directory announcer stopped during teardown: {error:#}");
        return;
    }
    tracing::error!("directory announcer failed: {error:#}");
    errors.push(error);
    tracker.close_stream().await;
}

/// Reap owned announcers and report unexpected task-join failures.
/// Returned errors and unwind panics are published eagerly by the announcer itself.
async fn reap_announce_task(
    joined: Result<(), tokio::task::JoinError>,
    directory_tracker: &directory_tracker::SharedDirectoryTracker,
    error_collector: &std::sync::Arc<common::error_collector::ErrorCollector>,
) {
    if let Err(e) = joined {
        tracing::error!("directory announce task failed to join: {e:#}");
        error_collector.push(e.into());
        directory_tracker.close_stream().await;
    }
}

#[instrument(skip(cert_key))]
#[allow(clippy::too_many_arguments)]
pub async fn run_destination(
    src_control_addr: &std::net::SocketAddr,
    src_data_addr: &std::net::SocketAddr,
    _src_server_name: &str,
    settings: &common::copy::Settings,
    dry_run: bool,
    overwrite_manifest_max_entries: usize,
    preserve: &common::preserve::Settings,
    tcp_config: &remote::TcpConfig,
    concurrency: remote::ResolvedRemoteConcurrency,
    admission: common::EndpointAdmission,
    cert_key: Option<&remote::tls::CertifiedKey>,
    source_cert_fingerprint: Option<remote::protocol::CertFingerprint>,
) -> anyhow::Result<(String, common::copy::Summary)> {
    let directory_work = match admission {
        common::EndpointAdmission::Remote(resources) => resources.leaf_capacity,
        _ => concurrency.max_connections(),
    };
    let directory_limits = if dry_run {
        None
    } else {
        Some(remote::protocol::DirectoryLimits::for_endpoint(
            admission,
            concurrency.max_connections().get(),
            concurrency.max_pending_files().get(),
        )?)
    };
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
    // connect to source's control port (socket options applied by the connect helper)
    let control_stream = remote::connect_tcp_control(*src_control_addr, tcp_config).await?;
    tracing::info!("Connected to source control port");
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
    if let Some(limits) = directory_limits {
        control_send_stream
            .lock()
            .await
            .send_control_message(&limits)
            .await
            .context("failed to advertise destination directory capacity")?;
    }
    tracing::info!("Created control streams");
    let error_collector = std::sync::Arc::new(common::error_collector::ErrorCollector::default());
    let directory_tracker = directory_tracker::SharedDirectoryTracker::new(
        control_send_stream.clone(),
        *preserve,
        settings.fail_early,
        error_collector.clone(),
    );
    // create a pool of data connections to source
    let data_pool = std::sync::Arc::new(DataConnectionPool::new(
        *src_data_addr,
        concurrency.max_connections().get(),
        tcp_config,
        tls_connector,
        directory_tracker.shutdown().clone(),
    ));
    // one operation-level indirection bounds the nested receiver layout in rcpd's watchdog
    // future; the same owner still drives control work through completion and teardown
    let mut control_future = Box::pin(process_control_stream(
        directory_limits,
        settings,
        overwrite_manifest_max_entries,
        directory_work,
        concurrency.max_pending_files(),
        preserve,
        control_recv_stream,
        directory_tracker.clone(),
        control_send_stream,
        data_pool.clone(),
        error_collector.clone(),
    ));
    // ordinary copies drive both receiver futures to completion before choosing an error; dropping
    // the loser could discard a file mid-record and replace its cause with a teardown symptom
    // cleanup runs beside that future because it may own the control send lock across I/O
    let (file_result, control_result) = if dry_run {
        // the master explicitly disables data work; a slow preview has no data handshake deadline
        let control_result = control_future.await;
        directory_tracker.close_stream().await;
        (Ok(()), control_result)
    } else {
        let file_handler_future = process_incoming_file_streams_tcp(
            settings.clone(),
            *preserve,
            data_pool.clone(),
            directory_tracker.clone(),
            error_collector.clone(),
        );
        tokio::pin!(file_handler_future);
        tokio::select! {
            file_result = &mut file_handler_future => {
                let ((), control_result) = tokio::join!(
                    directory_tracker.close_stream(),
                    &mut control_future,
                );
                (file_result, control_result)
            }
            control_result = &mut control_future => {
                let ((), file_result) = tokio::join!(
                    directory_tracker.close_stream(),
                    &mut file_handler_future,
                );
                (file_result, control_result)
            }
        }
    };
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
    // already closed by shared cancellation and stream cleanup. Read completion ONCE — the tracker is quiescent
    // (both futures done). Logical completion and a successfully sent Done must both hold.
    let completed = directory_tracker.with_state(|state| state.transfer_complete());
    let recorded = error_collector.take_error();
    let connect_cause = data_pool.shutdown.take_first_connect_error();
    choose_final_result(
        recorded,
        file_result,
        control_result,
        completed,
        connect_cause,
        summary,
    )
}

/// Preserve a fatal control cause ahead of collected operation errors and teardown effects.
/// Stream teardown and late connection failures are benign only after successful completion.
fn choose_final_result(
    recorded: Option<anyhow::Error>,
    file_result: anyhow::Result<()>,
    control_result: anyhow::Result<()>,
    completed: bool,
    connect_cause: Option<anyhow::Error>,
    summary: common::copy::Summary,
) -> anyhow::Result<(String, common::copy::Summary)> {
    let control_error = control_result
        .context("Failed to process control stream")
        .err();
    let file_error = file_result
        .context("Failed to process incoming file streams")
        .err();
    let (message_failure, stream_error) = match control_error {
        Some(error) if error.is::<ControlMessageFailure>() => (Some(error), file_error),
        control_error => (None, file_error.or(control_error)),
    };
    if let Some(err) = message_failure.or(recorded) {
        return Err(common::copy::Error {
            source: err,
            summary,
        }
        .into());
    }
    if !completed {
        let cause = connect_cause.or(stream_error).unwrap_or_else(|| {
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
    if let Some(e) = stream_error {
        tracing::debug!("ignoring teardown symptom after successful completion: {e:#}");
    }
    tracing::info!("Destination is done");
    Ok(("destination OK".to_string(), summary))
}

#[cfg(test)]
mod teardown_tests {
    use std::os::fd::AsFd as _;
    mod resource_tests;

    use super::*;

    #[tokio::test]
    async fn control_completion_progresses_while_manifest_lookups_wait_for_ops_tokens() {
        const TEST_NAME: &str = "destination::teardown_tests::control_completion_progresses_while_manifest_lookups_wait_for_ops_tokens";
        if crate::test_process::run_in_child(TEST_NAME) {
            return;
        }
        let tmp = tempfile::tempdir().unwrap();
        for index in 0..4 {
            std::fs::write(tmp.path().join(format!("file-{index}")), b"contents").unwrap();
        }
        let dir = Dir::open_root_dir(tmp.path(), false, common::Side::Destination)
            .await
            .unwrap();
        throttle::init_ops_tokens(1);
        throttle::get_ops_token().await;
        let mut lookups =
            Box::pin(futures::future::join_all((0..4).map(|index| {
                lookup_manifest_entry(&dir, format!("file-{index}").into())
            })));
        assert!(lookups.as_mut().now_or_never().is_none());
        let mut frame = Vec::new();
        remote::streams::SendStream::new(&mut frame)
            .send_control_message(&remote::protocol::SourceMessage::DiscoveryComplete {
                has_root_item: false,
            })
            .await
            .unwrap();
        let (writer, replies) = tokio::io::duplex(4096);
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(writer) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        process_control_stream(
            None,
            &testutils::copy_settings(false, 0),
            10,
            std::num::NonZeroUsize::new(4).unwrap(),
            std::num::NonZeroUsize::new(8).unwrap(),
            &common::preserve::preserve_none(),
            remote::streams::RecvStream::new(
                Box::new(std::io::Cursor::new(frame)) as remote::streams::BoxedRead
            ),
            tracker.clone(),
            send,
            test_pool(&tracker),
            errors.clone(),
        )
        .now_or_never()
        .expect("control completion waited for filesystem ops tokens")
        .unwrap();
        assert!(tracker.with_state(|state| state.transfer_complete()));
        assert!(errors.take_error().is_none());
        let mut replies = remote::streams::RecvStream::new(replies);
        assert!(matches!(
            replies
                .recv_object::<remote::protocol::DestinationMessage>()
                .await
                .unwrap(),
            Some(remote::protocol::DestinationMessage::DestinationDone)
        ));
        assert!(
            replies
                .recv_object::<remote::protocol::DestinationMessage>()
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            lookups.as_mut().now_or_never().is_none(),
            "control progress must not disable filesystem throttling"
        );
        throttle::init_ops_tokens(4);
        for entry in lookups.await {
            let entry = entry.expect("admitted manifest lookup must classify its real file");
            assert!(entry.is_file);
            assert_eq!(entry.size, 8);
        }
        crate::test_process::completed(TEST_NAME);
    }

    #[tokio::test]
    async fn directory_creation_progresses_while_parent_ready_is_blocked() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let child = root.join("child");
        let metadata = remote::protocol::Metadata::from(&std::fs::metadata(tmp.path()).unwrap());
        let (source, destination) = tokio::io::duplex(4096);
        let mut source = remote::streams::SendStream::new(source);
        for (dst, is_root) in [(&root, true), (&child, false)] {
            source
                .send_control_message(&remote::protocol::SourceMessage::DirectoryBegin {
                    admission: remote::protocol::DirectoryClass::Normal,
                    src: dst.clone(),
                    dst: dst.clone(),
                    metadata: metadata.clone(),
                    is_root,
                    keep_if_empty: true,
                })
                .await
                .unwrap();
        }
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        let held = send.lock().await;
        let settings = testutils::copy_settings(false, 0);
        let preserve = common::preserve::preserve_none();
        let mut control = Box::pin(process_control_stream(
            None,
            &settings,
            0,
            std::num::NonZeroUsize::new(2).unwrap(),
            std::num::NonZeroUsize::new(8).unwrap(),
            &preserve,
            remote::streams::RecvStream::new(Box::new(destination) as remote::streams::BoxedRead),
            tracker.clone(),
            send.clone(),
            test_pool(&tracker),
            errors,
        ));
        tokio::select! {
            result = &mut control => panic!("control ended prematurely: {result:?}"),
            result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while tracker.with_state(|state| state.get_dir(&child)).is_none() {
                    tokio::task::yield_now().await;
                }
            }) => assert!(result.is_ok(), "child creation must not await its parent's Ready"),
        }
        drop(control);
        drop(held);
    }

    #[tokio::test]
    async fn cancellation_stops_admission_even_with_buffered_control_frames() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let mut receiver = ReceiverFixture::new(tmp.path(), 1, 8, false);
        receiver.pool.shutdown.cancel();
        receiver.begin(&root, true).await;
        for index in 0..16 {
            receiver
                .begin(&root.join(format!("child-{index}")), false)
                .await;
        }
        receiver.stopped().await;
        assert!(
            receiver
                .tracker
                .with_state(|state| state.observe_root())
                .is_ok(),
            "cancelled receiver must not reserve buffered Begins"
        );
        assert!(!root.exists());
    }
    struct PreparationGate {
        path: std::path::PathBuf,
        state: Arc<PreparationGateState>,
    }
    struct PreparationGateState {
        started: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
        active: std::sync::atomic::AtomicUsize,
        failure: u8,
    }
    struct PreparationActivity(Arc<PreparationGateState>);
    impl Drop for PreparationActivity {
        fn drop(&mut self) {
            self.0
                .active
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    static PREPARATION_GATES: std::sync::LazyLock<
        std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, Arc<PreparationGateState>>>,
    > = std::sync::LazyLock::new(Default::default);
    impl PreparationGate {
        fn new(path: &std::path::Path, failure: u8) -> Self {
            let state = Arc::new(PreparationGateState {
                started: tokio::sync::Semaphore::new(0),
                release: tokio::sync::Semaphore::new(0),
                active: std::sync::atomic::AtomicUsize::new(0),
                failure,
            });
            assert!(
                PREPARATION_GATES
                    .lock()
                    .unwrap()
                    .insert(path.to_owned(), state.clone())
                    .is_none()
            );
            Self {
                path: path.to_owned(),
                state,
            }
        }
        async fn started(&self) {
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                self.state.started.acquire(),
            )
            .await
            .unwrap()
            .unwrap()
            .forget();
        }
        fn release(&self) {
            self.state.release.add_permits(1);
        }
    }
    impl Drop for PreparationGate {
        fn drop(&mut self) {
            PREPARATION_GATES.lock().unwrap().remove(&self.path);
            self.state.release.close();
        }
    }
    pub(super) async fn gate_preparation(path: &std::path::Path) -> anyhow::Result<()> {
        let state = PREPARATION_GATES.lock().unwrap().get(path).cloned();
        if let Some(state) = state {
            state
                .active
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _activity = PreparationActivity(state.clone());
            state.started.add_permits(1);
            state.release.acquire().await?.forget();
            match state.failure {
                1 => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "original directory creation cause",
                    )
                    .into());
                }
                2 => panic!("original directory creation panic"),
                _ => {}
            }
        }
        Ok(())
    }
    struct ReceiverFixture {
        source: remote::streams::SendStream<tokio::io::DuplexStream>,
        replies: remote::streams::RecvStream<tokio::io::DuplexStream>,
        send: remote::streams::BoxedSharedSendStream,
        tracker: directory_tracker::SharedDirectoryTracker,
        pool: Arc<DataConnectionPool>,
        errors: Arc<common::error_collector::ErrorCollector>,
        task: tokio::task::JoinHandle<anyhow::Result<()>>,
        metadata: remote::protocol::Metadata,
    }
    impl ReceiverFixture {
        fn new(tmp: &std::path::Path, e: usize, p: usize, fail_early: bool) -> Self {
            let (source, receive) = tokio::io::duplex(65536);
            let (writer, replies) = tokio::io::duplex(65536);
            let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                Box::new(writer) as remote::streams::BoxedWrite,
            )));
            let errors = Arc::new(common::error_collector::ErrorCollector::default());
            let tracker = directory_tracker::SharedDirectoryTracker::new(
                send.clone(),
                common::preserve::preserve_none(),
                fail_early,
                errors.clone(),
            );
            let pool = test_pool(&tracker);
            let (job_tracker, job_send, job_pool, job_errors) =
                (tracker.clone(), send.clone(), pool.clone(), errors.clone());
            let task = tokio::spawn(async move {
                let mut settings = testutils::copy_settings(false, 0);
                settings.fail_early = fail_early;
                process_control_stream(
                    None,
                    &settings,
                    0,
                    std::num::NonZeroUsize::new(e).unwrap(),
                    std::num::NonZeroUsize::new(p).unwrap(),
                    &common::preserve::preserve_none(),
                    remote::streams::RecvStream::new(
                        Box::new(receive) as remote::streams::BoxedRead
                    ),
                    job_tracker,
                    job_send,
                    job_pool,
                    job_errors,
                )
                .await
            });
            Self {
                source: remote::streams::SendStream::new(source),
                replies: remote::streams::RecvStream::new(replies),
                send,
                tracker,
                pool,
                errors,
                task,
                metadata: remote::protocol::Metadata::from(&std::fs::metadata(tmp).unwrap()),
            }
        }
        async fn begin(&mut self, path: &std::path::Path, is_root: bool) {
            self.source
                .send_control_message(&remote::protocol::SourceMessage::DirectoryBegin {
                    admission: remote::protocol::DirectoryClass::Normal,
                    src: path.to_owned(),
                    dst: path.to_owned(),
                    metadata: self.metadata.clone(),
                    is_root,
                    keep_if_empty: true,
                })
                .await
                .unwrap();
        }
        async fn end(&mut self, path: &std::path::Path, entry_count: usize) {
            self.source
                .send_control_message(&remote::protocol::SourceMessage::DirectoryEnd {
                    src: path.to_owned(),
                    dst: path.to_owned(),
                    entry_count,
                })
                .await
                .unwrap();
        }
        async fn discovery_complete(&mut self) {
            self.source
                .send_control_message(&remote::protocol::SourceMessage::DiscoveryComplete {
                    has_root_item: true,
                })
                .await
                .unwrap();
        }
        async fn reply(&mut self) -> remote::protocol::DestinationMessage {
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                self.replies.recv_object(),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        }
        async fn ready(&mut self, path: &std::path::Path) {
            assert!(
                matches!(self.reply().await, remote::protocol::DestinationMessage::DirectoryReady { dst, .. } if dst == path)
            );
        }
        async fn stopped(&mut self) {
            tokio::time::timeout(std::time::Duration::from_secs(2), &mut self.task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }
    impl Drop for ReceiverFixture {
        fn drop(&mut self) {
            self.pool.shutdown.cancel();
            self.task.abort();
        }
    }
    #[tokio::test]
    async fn capacity_one_directory_chain_finishes_without_future_end_dependencies() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let child = root.join("child");
        let leaf = child.join("leaf");
        let mut receiver = ReceiverFixture::new(tmp.path(), 1, 1, false);
        for (path, is_root) in [(&root, true), (&child, false), (&leaf, false)] {
            receiver.begin(path, is_root).await;
        }
        for path in [&root, &child, &leaf] {
            receiver.ready(path).await;
        }
        receiver.end(&root, 1).await;
        receiver.end(&child, 1).await;
        receiver.end(&leaf, 0).await;
        receiver.discovery_complete().await;
        assert!(matches!(
            receiver.reply().await,
            remote::protocol::DestinationMessage::DestinationDone
        ));
        receiver.stopped().await;
        assert!(leaf.is_dir());
    }
    #[tokio::test]
    async fn siblings_overlap_with_two_preparations_and_waiting_children_use_no_slot() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let first = root.join("first");
        let waiting = first.join("waiting");
        let second = root.join("second");
        let third = root.join("third");
        let first_gate = PreparationGate::new(&first, 0);
        let second_gate = PreparationGate::new(&second, 0);
        let third_gate = PreparationGate::new(&third, 0);
        let mut receiver = ReceiverFixture::new(tmp.path(), 2, 8, false);
        receiver.begin(&root, true).await;
        receiver.ready(&root).await;
        receiver.begin(&first, false).await;
        first_gate.started().await;
        receiver.begin(&waiting, false).await;
        receiver.begin(&second, false).await;
        second_gate.started().await;
        assert!(!waiting.exists());
        receiver.begin(&third, false).await;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(40),
                third_gate.state.started.acquire()
            )
            .await
            .is_err(),
            "E=2 must bound physical preparation"
        );
        second_gate.release();
        receiver.ready(&second).await;
        third_gate.started().await;
        third_gate.release();
        receiver.ready(&third).await;
        first_gate.release();
        let mut ready = std::collections::HashSet::new();
        for _ in 0..2 {
            if let remote::protocol::DestinationMessage::DirectoryReady { dst, .. } =
                receiver.reply().await
            {
                ready.insert(dst);
            }
        }
        assert_eq!(
            ready,
            [first.clone(), waiting.clone()].into_iter().collect()
        );
        for (path, count) in [
            (&root, 3),
            (&first, 1),
            (&waiting, 0),
            (&second, 0),
            (&third, 0),
        ] {
            receiver.end(path, count).await;
        }
        receiver.discovery_complete().await;
        assert!(matches!(
            receiver.reply().await,
            remote::protocol::DestinationMessage::DestinationDone
        ));
        receiver.stopped().await;
    }
    #[tokio::test]
    async fn ready_jobs_retain_capacity_until_eligible_finalization_finishes() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let first = root.join("first");
        let second = root.join("second");
        let third = root.join("third");
        let mut receiver = ReceiverFixture::new(tmp.path(), 1, 2, false);
        receiver.begin(&root, true).await;
        receiver.ready(&root).await;
        let finalization = Arc::new(tokio::sync::Semaphore::new(0));
        receiver.tracker.gate_finalization(finalization.clone());
        let send = receiver.send.clone();
        let held = send.lock().await;
        for path in [&first, &second] {
            receiver.begin(path, false).await;
            receiver.end(path, 0).await;
        }
        // wait until both Begins have been read; their blocked replies keep End handling inline
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while receiver
                .tracker
                .with_state(|state| state.get_dir(&second))
                .is_none()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(held);
        let mut ready = std::collections::HashSet::new();
        for _ in 0..2 {
            if let remote::protocol::DestinationMessage::DirectoryReady { dst, .. } =
                receiver.reply().await
            {
                ready.insert(dst);
            }
        }
        assert_eq!(ready, [first.clone(), second.clone()].into_iter().collect());
        let third_gate = PreparationGate::new(&third, 0);
        receiver.begin(&third, false).await;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(40),
                third_gate.state.started.acquire()
            )
            .await
            .is_err(),
            "P includes post-Ready finalization"
        );
        finalization.add_permits(2);
        third_gate.started().await;
        third_gate.release();
        receiver.ready(&third).await;
        finalization.add_permits(2);
        receiver.end(&third, 0).await;
        receiver.end(&root, 3).await;
        receiver.discovery_complete().await;
        assert!(matches!(
            receiver.reply().await,
            remote::protocol::DestinationMessage::DestinationDone
        ));
        receiver.stopped().await;
    }
    #[tokio::test]
    async fn inline_symlink_waits_for_secured_parent_without_blocking_publication() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let link = root.join("link");
        let gate = PreparationGate::new(&root, 0);
        let mut receiver = ReceiverFixture::new(tmp.path(), 1, 2, false);
        receiver.begin(&root, true).await;
        gate.started().await;
        receiver
            .source
            .send_control_message(&remote::protocol::SourceMessage::Symlink {
                src: link.clone(),
                dst: link.clone(),
                target: "target".into(),
                metadata: receiver.metadata.clone(),
                is_root: false,
            })
            .await
            .unwrap();
        receiver.end(&root, 1).await;
        receiver.discovery_complete().await;
        assert_eq!(
            link.symlink_metadata().unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
        gate.release();
        receiver.ready(&root).await;
        assert!(matches!(
            receiver.reply().await,
            remote::protocol::DestinationMessage::DestinationDone
        ));
        receiver.stopped().await;
        assert_eq!(
            std::fs::read_link(link).unwrap(),
            std::path::Path::new("target")
        );
    }
    #[tokio::test]
    async fn creation_failure_and_panic_publish_original_cause_before_waiting_descendants() {
        for failure in [1, 2] {
            let tmp = tempfile::tempdir().unwrap();
            let root = tmp.path().join("root");
            let child = root.join("child");
            let gate = PreparationGate::new(&root, failure);
            let mut receiver = ReceiverFixture::new(tmp.path(), 1, 8, true);
            receiver.begin(&root, true).await;
            gate.started().await;
            receiver.begin(&child, false).await;
            receiver
                .source
                .send_control_message(&remote::protocol::SourceMessage::Symlink {
                    src: child.join("link"),
                    dst: child.join("link"),
                    target: "target".into(),
                    metadata: receiver.metadata.clone(),
                    is_root: false,
                })
                .await
                .unwrap();
            tokio::task::yield_now().await;
            gate.release();
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                receiver.pool.shutdown.cancelled(),
            )
            .await
            .unwrap();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), &mut receiver.task)
                .await
                .unwrap()
                .unwrap();
            let error = receiver
                .errors
                .take_error()
                .expect("creator records its cause before cancellation");
            if failure == 1 {
                assert_eq!(
                    error.downcast_ref::<std::io::Error>().unwrap().kind(),
                    std::io::ErrorKind::PermissionDenied,
                    "derivative failures must not replace the original error chain"
                );
            }
            assert!(format!("{error:#}").contains(if failure == 1 {
                "original directory creation cause"
            } else {
                "original directory creation panic"
            }));
            assert!(!child.exists());
            assert!(
                !receiver
                    .tracker
                    .with_state(|state| state.transfer_complete())
            );
        }
    }
    #[tokio::test]
    async fn early_eof_cancels_pending_creation_without_done() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let child = root.join("child");
        let gate = PreparationGate::new(&root, 0);
        let mut receiver = ReceiverFixture::new(tmp.path(), 1, 8, false);
        receiver.begin(&root, true).await;
        gate.started().await;
        receiver.begin(&child, false).await;
        receiver.end(&root, 1).await;
        receiver.end(&child, 0).await;
        receiver.discovery_complete().await;
        receiver.source.close().await.unwrap();
        receiver.stopped().await;
        assert!(
            !receiver
                .tracker
                .with_state(|state| state.transfer_complete())
        );
        assert!(receiver.tracker.with_state(|state| state.is_closing()));
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn rejection_after_discovery_settles_admitted_descendants_and_parent_once() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let child = root.join("child");
        let leaf = child.join("leaf");
        let gate = PreparationGate::new(&child, 1);
        let mut receiver = ReceiverFixture::new(tmp.path(), 1, 8, false);
        receiver.begin(&root, true).await;
        receiver.ready(&root).await;
        receiver.begin(&child, false).await;
        gate.started().await;
        receiver.begin(&leaf, false).await;
        receiver.end(&root, 1).await;
        receiver.end(&child, 1).await;
        receiver.end(&leaf, 0).await;
        receiver.discovery_complete().await;
        tokio::task::yield_now().await;
        gate.release();
        let mut skipped = std::collections::HashSet::new();
        for _ in 0..2 {
            if let remote::protocol::DestinationMessage::DirectorySkipped { dst, .. } =
                receiver.reply().await
            {
                skipped.insert(dst);
            }
        }
        assert_eq!(skipped, [child.clone(), leaf.clone()].into_iter().collect());
        assert!(matches!(
            receiver.reply().await,
            remote::protocol::DestinationMessage::DestinationDone
        ));
        receiver.stopped().await;
        assert!(!child.exists());
        let error = receiver.errors.take_error().unwrap();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }
    #[cfg_attr(rcp_nix_sandbox, ignore = "Nix sandbox cannot write POSIX ACL xattrs")]
    #[tokio::test]
    async fn early_eof_restores_secured_reused_directory_acl_on_the_held_inode() {
        if !common::safedir::openat2_available() {
            return;
        }
        const TEST_NAME: &str = "destination::teardown_tests::early_eof_restores_secured_reused_directory_acl_on_the_held_inode";
        if crate::test_process::run_in_child(TEST_NAME) {
            return;
        }
        common::safedir::enable_strict_operand_resolution();
        let tmp = tempfile::tempdir().unwrap();
        let canonical_tmp = tmp.path().canonicalize().unwrap();
        let root = canonical_tmp.join("root");
        std::fs::create_dir(&root).unwrap();
        let mut acl = 2u32.to_le_bytes().to_vec();
        for (tag, permission) in [(1u16, 7u16), (4, 5), (32, 0)] {
            acl.extend_from_slice(&tag.to_le_bytes());
            acl.extend_from_slice(&permission.to_le_bytes());
            acl.extend_from_slice(&u32::MAX.to_le_bytes());
        }
        let fd = std::fs::File::open(&root).unwrap();
        common::safedir::apply_acls_fd(
            fd.as_fd(),
            common::Side::Destination,
            &common::safedir::Acls {
                access: None,
                default: Some(acl.clone()),
            },
            true,
        )
        .await
        .unwrap();
        let mut receiver = ReceiverFixture::new(tmp.path(), 1, 8, false);
        let send = receiver.send.clone();
        let held_send = send.lock().await;
        receiver.begin(&root, true).await;
        let dir = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(dir) = receiver.tracker.with_state(|state| state.get_dir(&root)) {
                    break dir;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(dir.read_acls().await.unwrap().default, None);
        std::fs::rename(&root, tmp.path().join("held-inode")).unwrap();
        std::fs::create_dir(&root).unwrap();
        receiver.end(&root, 0).await;
        receiver.discovery_complete().await;
        receiver.source.close().await.unwrap();
        receiver.stopped().await;
        assert!(
            !receiver
                .tracker
                .with_state(|state| state.transfer_complete())
        );
        drop(receiver);
        drop(held_send);
        assert_eq!(dir.read_acls().await.unwrap().default, Some(acl));
        let replacement = Dir::open_root_dir(&root, false, common::Side::Destination)
            .await
            .unwrap();
        assert_eq!(replacement.read_acls().await.unwrap().default, None);
        crate::test_process::completed(TEST_NAME);
    }

    #[tokio::test]
    async fn directory_completion_does_not_restart_a_fragmented_control_read() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let gate = PreparationGate::new(&root, 0);
        let (mut source, receive) = tokio::io::duplex(4096);
        let (writer, replies) = tokio::io::duplex(4096);
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(writer) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        let task = tokio::spawn(async move {
            process_control_stream(
                None,
                &testutils::copy_settings(false, 0),
                0,
                std::num::NonZeroUsize::MIN,
                std::num::NonZeroUsize::new(2).unwrap(),
                &common::preserve::preserve_none(),
                remote::streams::RecvStream::new(Box::new(receive) as remote::streams::BoxedRead),
                tracker.clone(),
                send,
                test_pool(&tracker),
                errors,
            )
            .await
        });
        remote::streams::SendStream::new(&mut source)
            .send_control_message(&remote::protocol::SourceMessage::DirectoryBegin {
                admission: remote::protocol::DirectoryClass::Normal,
                src: root.clone(),
                dst: root.clone(),
                metadata: remote::protocol::Metadata::from(&std::fs::metadata(tmp.path()).unwrap()),
                is_root: true,
                keep_if_empty: true,
            })
            .await
            .unwrap();
        gate.started().await;
        let mut frame = Vec::new();
        remote::streams::SendStream::new(&mut frame)
            .send_control_message(&remote::protocol::SourceMessage::DirectoryEnd {
                src: root.clone(),
                dst: root.clone(),
                entry_count: 0,
            })
            .await
            .unwrap();
        source.write_all(&frame[..5]).await.unwrap();
        gate.release();
        let mut replies = remote::streams::RecvStream::new(replies);
        assert!(matches!(
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                replies.recv_object::<remote::protocol::DestinationMessage>()
            )
            .await
            .unwrap()
            .unwrap(),
            Some(remote::protocol::DestinationMessage::DirectoryReady { .. })
        ));
        source.write_all(&frame[5..]).await.unwrap();
        remote::streams::SendStream::new(&mut source)
            .send_control_message(&remote::protocol::SourceMessage::DiscoveryComplete {
                has_root_item: true,
            })
            .await
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                replies.recv_object::<remote::protocol::DestinationMessage>()
            )
            .await
            .unwrap()
            .unwrap(),
            Some(remote::protocol::DestinationMessage::DestinationDone)
        ));
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn control_decode_failure_joins_directory_jobs_before_returning() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let gate = PreparationGate::new(&root, 0);
        let (mut source, destination) = tokio::io::duplex(4096);
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        common::task_scope::scope_tasks(async {
            let settings = testutils::copy_settings(false, 0);
            let preserve = common::preserve::preserve_none();
            let control = process_control_stream(
                None,
                &settings,
                0,
                std::num::NonZeroUsize::MIN,
                std::num::NonZeroUsize::new(2).unwrap(),
                &preserve,
                remote::streams::RecvStream::new(
                    Box::new(destination) as remote::streams::BoxedRead
                ),
                tracker.clone(),
                send,
                test_pool(&tracker),
                errors,
            );
            let corrupt_source = async {
                remote::streams::SendStream::new(&mut source)
                    .send_control_message(&remote::protocol::SourceMessage::DirectoryBegin {
                        admission: remote::protocol::DirectoryClass::Normal,
                        src: root.clone(),
                        dst: root.clone(),
                        metadata: remote::protocol::Metadata::from(
                            &std::fs::metadata(tmp.path()).unwrap(),
                        ),
                        is_root: true,
                        keep_if_empty: true,
                    })
                    .await
                    .unwrap();
                gate.started().await;
                source.write_all(&[0, 0, 0, 0]).await.unwrap();
            };
            let (result, ()) = tokio::join!(control, corrupt_source);
            assert!(result.unwrap_err().is::<ControlMessageFailure>());
            assert_eq!(
                gate.state.active.load(std::sync::atomic::Ordering::SeqCst),
                0,
                "control error returned before its directory job dropped"
            );
            assert!(!tracker.with_state(|state| state.transfer_complete()));
        })
        .await;
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn sibling_registration_during_finalizer_teardown_preserves_original_cause() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let failed = root.join("failed");
        let sibling = root.join("sibling");
        std::fs::create_dir_all(&failed).unwrap();
        std::fs::create_dir(&sibling).unwrap();
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let mut preserve = common::preserve::preserve_none();
        preserve.dir.user_and_time.time = true;
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            preserve,
            true,
            errors.clone(),
        );
        let metadata = remote::protocol::Metadata::from(&std::fs::metadata(&root).unwrap());
        for (path, is_root) in [(&root, true), (&failed, false)] {
            let admission = tracker
                .with_state(|state| state.admit_directory(path, is_root))
                .unwrap();
            let dir = Arc::new(
                Dir::open_root_dir(path, false, common::Side::Destination)
                    .await
                    .unwrap(),
            );
            let mut entry_metadata = metadata.clone();
            if !is_root {
                entry_metadata.mtime_nsec = 1_000_000_000;
            }
            tracker
                .register_directory(admission, dir, entry_metadata, false, true, None)
                .unwrap();
            tracker.mark_announced(path).await.unwrap();
        }
        tracker.seal_directory(&root, 2).await.unwrap();
        let mut admission = Some(
            tracker
                .with_state(|state| state.admit_directory(&sibling, false))
                .unwrap(),
        );
        // retain the actual finalizer failure before its worker publishes it; shared cancellation
        // must make a sibling's registration refusal benign without losing this original cause
        let original = tracker.seal_directory(&failed, 0).await.unwrap_err();
        assert_eq!(
            original
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(libc::EINVAL)
        );
        assert!(tracker.with_state(|state| state.is_closing()));
        let pool = test_pool(&tracker);
        publish_announce_failure(
            &tracker,
            &errors,
            prepare_and_announce_directory(
                &testutils::copy_settings(false, 0),
                &tracker,
                &send,
                &pool,
                &errors,
                &mut admission,
                &sibling,
                &sibling,
                metadata,
                false,
                true,
                Arc::new(tokio::sync::Semaphore::new(1)),
                Arc::new(tokio::sync::Semaphore::new(1)),
                0,
                std::num::NonZeroUsize::MIN,
                None,
            ),
        )
        .await;
        assert!(
            !errors.has_errors(),
            "a derivative registration refusal must not publish a competing cause"
        );
        publish_announce_failure(&tracker, &errors, async { Err(original) }).await;
        assert!(pool.shutdown.is_cancelled());
        let reported = errors.take_error().unwrap();
        assert_eq!(
            reported
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(libc::EINVAL)
        );
        assert!(format!("{reported:#}").contains("failed to set metadata on directory"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finalizer_failure_is_published_before_its_cancelled_job_finishes() {
        let tmp = tempfile::tempdir().unwrap();
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let mut preserve = common::preserve::preserve_none();
        preserve.dir.user_and_time.time = true;
        let tracker =
            directory_tracker::SharedDirectoryTracker::new(send, preserve, true, errors.clone());
        let path = tmp.path().to_owned();
        let admission = tracker
            .with_state(|state| state.admit_directory(&path, true))
            .unwrap();
        let directory = Arc::new(
            Dir::open_root_dir(&path, false, common::Side::Destination)
                .await
                .unwrap(),
        );
        let mut metadata = remote::protocol::Metadata::from(&std::fs::metadata(&path).unwrap());
        metadata.mtime_nsec = 1_000_000_000;
        tracker
            .register_directory(admission, directory, metadata, false, true, None)
            .unwrap();
        tracker.seal_directory(&path, 0).await.unwrap();
        let (failed, failure) = tokio::sync::oneshot::channel();
        let (release, paused) = std::sync::mpsc::channel();
        common::task_scope::scope_tasks(async {
            let mut jobs = tokio::task::JoinSet::new();
            let (job_tracker, job_errors) = (tracker.clone(), errors.clone());
            common::task_scope::spawn_tracked(&mut jobs, async move {
                let finalizer = job_tracker.clone();
                publish_announce_failure(&job_tracker, &job_errors, async move {
                    let error = finalizer.mark_announced(&path).await.unwrap_err();
                    failed.send(()).unwrap();
                    // pause this poll after the claim cancels, before the wrapper publishes its
                    // error. A concurrent abort must wait for this poll to publish and finish.
                    paused
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    Err(error)
                })
                .await;
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), failure)
                .await
                .unwrap()
                .unwrap();
            assert!(tracker.shutdown().is_cancelled());
            assert!(!errors.has_errors());
            let mut joining = Box::pin(jobs.shutdown());
            assert!(joining.as_mut().now_or_never().is_none());
            release.send(()).unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), joining)
                .await
                .expect("shutdown must join the error publisher");
        })
        .await;
        let error = errors
            .take_error()
            .expect("cancelled job must retain its cause");
        assert_eq!(
            error
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(libc::EINVAL)
        );
        assert!(format!("{error:#}").contains("failed to set metadata on directory"));
        assert!(!tracker.with_state(|state| state.transfer_complete()));
    }

    fn summary() -> common::copy::Summary {
        common::copy::Summary::default()
    }

    // ── choose_final_result: the completion-gate decision matrix ──

    async fn assert_control_failure_survives_completion(
        message: remote::protocol::SourceMessage,
        completes_before_rejection: bool,
        file_failed: bool,
    ) {
        let (source, destination) = tokio::io::duplex(4096);
        let mut source = remote::streams::SendStream::new(source);
        source.send_control_message(&message).await.unwrap();
        source.close().await.unwrap();
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        tracker
            .with_state(|state| state.finish_discovery(true))
            .unwrap();
        if completes_before_rejection {
            tracker.with_state(|state| state.set_root_complete());
        }
        let control_result = process_control_stream(
            None,
            &testutils::copy_settings(false, 0),
            10,
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::new(8).unwrap(),
            &common::preserve::preserve_none(),
            remote::streams::RecvStream::new(Box::new(destination) as remote::streams::BoxedRead),
            tracker.clone(),
            send,
            test_pool(&tracker),
            errors.clone(),
        )
        .await;
        assert!(
            control_result.is_err(),
            "the receiver must reject the control message"
        );
        if !completes_before_rejection {
            tracker.with_state(|state| state.set_root_complete());
        }
        assert!(
            tracker.with_state(|state| state.is_done()),
            "exercise final selection after logical completion"
        );
        let completed = tracker.with_state(|state| state.transfer_complete());
        assert!(!completed, "a rejected control message cannot send Done");
        let file_result = if file_failed {
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "file stream closed during teardown",
            )
            .into())
        } else {
            Ok(())
        };
        let error = choose_final_result(
            errors.take_error(),
            file_result,
            control_result,
            completed,
            None,
            summary(),
        )
        .expect_err("completion must not turn a rejected control message into success");
        assert!(format!("{error:#}").contains("structural message after DiscoveryComplete"));
    }
    struct FailingDoneWriter {
        fail_close: bool,
    }
    #[derive(Clone, Copy, Debug)]
    enum DoneFailure {
        Write,
        Flush,
        Close,
    }
    struct FailAfterReadyWriter {
        failure: DoneFailure,
        done_frame: Vec<u8>,
        done_started: bool,
        io_calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl tokio::io::AsyncWrite for FailAfterReadyWriter {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.io_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let is_done = bytes == self.done_frame.as_slice();
            if (is_done || self.done_started) && matches!(self.failure, DoneFailure::Write) {
                if self.done_started {
                    return std::task::Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "original partial Done write cause",
                    )));
                }
                self.done_started = true;
                return std::task::Poll::Ready(Ok(bytes.len().min(1)));
            }
            self.done_started |= is_done;
            std::task::Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            self.io_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.done_started && matches!(self.failure, DoneFailure::Flush) {
                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "original Done flush cause",
                )));
            }
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            self.io_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.done_started && matches!(self.failure, DoneFailure::Close) {
                std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "original Done close cause",
                )))
            } else {
                std::task::Poll::Ready(Ok(()))
            }
        }
    }
    #[tokio::test]
    async fn directory_completion_done_failure_preserves_one_original_cause() {
        for failure in [DoneFailure::Write, DoneFailure::Flush, DoneFailure::Close] {
            let tmp = tempfile::tempdir().unwrap();
            let mut done_frame = Vec::new();
            remote::streams::SendStream::new(&mut done_frame)
                .send_control_message(&remote::protocol::DestinationMessage::DestinationDone)
                .await
                .unwrap();
            let io_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                Box::new(FailAfterReadyWriter {
                    failure,
                    done_frame,
                    done_started: false,
                    io_calls: io_calls.clone(),
                }) as remote::streams::BoxedWrite,
            )));
            let errors = Arc::new(common::error_collector::ErrorCollector::default());
            let tracker = directory_tracker::SharedDirectoryTracker::new(
                send.clone(),
                common::preserve::preserve_none(),
                false,
                errors.clone(),
            );
            let admission = tracker
                .with_state(|state| state.admit_directory(tmp.path(), true))
                .unwrap();
            tracker
                .register_directory(
                    admission,
                    Arc::new(
                        Dir::open_root_dir(tmp.path(), false, common::Side::Destination)
                            .await
                            .unwrap(),
                    ),
                    remote::protocol::Metadata::from(&std::fs::metadata(tmp.path()).unwrap()),
                    false,
                    true,
                    None,
                )
                .unwrap();
            tracker.seal_directory(tmp.path(), 0).await.unwrap();
            let (source, destination) = tokio::io::duplex(4096);
            let mut source = remote::streams::SendStream::new(source);
            source
                .send_control_message(&remote::protocol::SourceMessage::DiscoveryComplete {
                    has_root_item: true,
                })
                .await
                .unwrap();
            let pool = test_pool(&tracker);
            let settings = testutils::copy_settings(false, 0);
            let preserve = common::preserve::preserve_none();
            let mut control = Box::pin(process_control_stream(
                None,
                &settings,
                0,
                std::num::NonZeroUsize::MIN,
                std::num::NonZeroUsize::new(2).unwrap(),
                &preserve,
                remote::streams::RecvStream::new(
                    Box::new(destination) as remote::streams::BoxedRead
                ),
                tracker.clone(),
                send.clone(),
                pool.clone(),
                errors.clone(),
            ));
            assert!(control.as_mut().now_or_never().is_none());
            let announce = publish_announce_failure(
                &tracker,
                &errors,
                announce_directory_ready(
                    &tracker,
                    &send,
                    &pool,
                    tmp.path(),
                    tmp.path(),
                    Vec::new(),
                ),
            );
            let (control_result, ()) =
                tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    tokio::join!(control, announce)
                })
                .await
                .unwrap();
            let calls_after_failure = io_calls.load(std::sync::atomic::Ordering::SeqCst);
            tracker.close_stream().await;
            assert_eq!(
                io_calls.load(std::sync::atomic::Ordering::SeqCst),
                calls_after_failure,
                "cleanup retried I/O after the terminal write failed"
            );
            let error = choose_final_result(
                errors.take_error(),
                Ok(()),
                control_result,
                tracker.with_state(|state| state.transfer_complete()),
                None,
                summary(),
            )
            .unwrap_err();
            assert_eq!(
                error
                    .root_cause()
                    .downcast_ref::<std::io::Error>()
                    .map(std::io::Error::kind),
                Some(std::io::ErrorKind::PermissionDenied),
                "Done failure lost its original typed cause: {failure:?}: {error:#}"
            );
            assert!(!format!("{error:#}").contains("DestinationDone failed"));
        }
    }
    #[tokio::test]
    async fn control_framing_failure_precedes_earlier_recoverable_file_error() {
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        errors.push(
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "earlier recoverable file error",
            )
            .into(),
        );
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
        )));
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        let control_result = process_control_stream(
            None,
            &testutils::copy_settings(false, 0),
            0,
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::new(2).unwrap(),
            &common::preserve::preserve_none(),
            remote::streams::RecvStream::new(
                Box::new(std::io::Cursor::new(vec![0u8, 0, 0, 0])) as remote::streams::BoxedRead
            ),
            tracker.clone(),
            send,
            test_pool(&tracker),
            errors.clone(),
        )
        .await;
        assert!(
            control_result
                .as_ref()
                .unwrap_err()
                .is::<ControlMessageFailure>()
        );
        let error = choose_final_result(
            errors.take_error(),
            Ok(()),
            control_result,
            tracker.with_state(|state| state.transfer_complete()),
            None,
            summary(),
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("Failed to receive source message"),
            "{error:#}"
        );
    }
    #[derive(Default)]
    struct ReleaseWriterState {
        bytes: Vec<u8>,
        flushes: usize,
        block_flush: bool,
        fail_flush: bool,
        waker: Option<std::task::Waker>,
    }
    struct ReleaseWriter(Arc<std::sync::Mutex<ReleaseWriterState>>);
    impl tokio::io::AsyncWrite for ReleaseWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.0.lock().unwrap().bytes.extend_from_slice(bytes);
            std::task::Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let mut state = self.0.lock().unwrap();
            if state.block_flush {
                state.waker = Some(cx.waker().clone());
                return std::task::Poll::Pending;
            }
            state.flushes += 1;
            if state.fail_flush {
                std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "original directory release flush cause",
                )))
            } else {
                std::task::Poll::Ready(Ok(()))
            }
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
    fn queued_directory_releases(count: usize) -> resources::DirectoryLifetimes {
        let mut lifetimes = resources::DirectoryLifetimes::new(remote::protocol::DirectoryLimits {
            normal: std::num::NonZeroUsize::new(count).unwrap(),
            reserve: std::num::NonZeroUsize::MIN,
        });
        for index in 0..count {
            let path = std::path::PathBuf::from(format!("/directory-{index}"));
            let lease = lifetimes
                .admit(&path, &path, remote::protocol::DirectoryClass::Normal)
                .unwrap();
            lifetimes.ended(&path, &path).unwrap();
            drop(lease);
        }
        lifetimes
    }
    async fn released_paths(bytes: Vec<u8>) -> Vec<std::path::PathBuf> {
        let mut reader = remote::streams::RecvStream::new(std::io::Cursor::new(bytes));
        let mut paths = Vec::new();
        while let Some(message) = reader
            .recv_object::<remote::protocol::DestinationMessage>()
            .await
            .unwrap()
        {
            let remote::protocol::DestinationMessage::DirectoryReleased { src, dst } = message
            else {
                panic!("expected directory release, got {message:?}");
            };
            assert_eq!(src, dst);
            paths.push(dst);
        }
        paths
    }
    #[tokio::test]
    async fn queued_directory_releases_share_a_bounded_flush() {
        let output = Arc::new(std::sync::Mutex::new(ReleaseWriterState::default()));
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(ReleaseWriter(output.clone())) as remote::streams::BoxedWrite,
        )));
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            Arc::new(common::error_collector::ErrorCollector::default()),
        );
        let pool = test_pool(&tracker);
        let mut lifetimes = queued_directory_releases(65);
        let first = lifetimes.next_release().await;
        flush_directory_releases(&mut lifetimes, first, &send, &pool)
            .now_or_never()
            .expect("queued releases must not wait to fill a batch")
            .unwrap();
        let bytes = {
            let output = output.lock().unwrap();
            assert_eq!(output.flushes, 1);
            output.bytes.clone()
        };
        assert_eq!(
            released_paths(bytes).await,
            (0..64)
                .map(|index| std::path::PathBuf::from(format!("/directory-{index}")))
                .collect::<Vec<_>>()
        );
        assert!(!lifetimes.is_empty(), "the next batch must remain charged");
        let last = lifetimes.try_next_release().unwrap();
        assert_eq!(last.pair.dst, std::path::Path::new("/directory-64"));
        assert!(lifetimes.try_next_release().is_none());
        flush_directory_releases(&mut lifetimes, last, &send, &pool)
            .now_or_never()
            .expect("a partial batch must flush immediately")
            .unwrap();
        assert!(lifetimes.is_empty(), "every flushed release must retire");
        let bytes = {
            let output = output.lock().unwrap();
            assert_eq!(output.flushes, 2);
            output.bytes.clone()
        };
        assert_eq!(
            released_paths(bytes).await,
            (0..65)
                .map(|index| std::path::PathBuf::from(format!("/directory-{index}")))
                .collect::<Vec<_>>()
        );
    }
    #[tokio::test]
    async fn failed_directory_release_flush_keeps_every_lifetime_charged() {
        let output = Arc::new(std::sync::Mutex::new(ReleaseWriterState {
            block_flush: true,
            fail_flush: true,
            ..Default::default()
        }));
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(ReleaseWriter(output.clone())) as remote::streams::BoxedWrite,
        )));
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            Arc::new(common::error_collector::ErrorCollector::default()),
        );
        let pool = test_pool(&tracker);
        let mut lifetimes = queued_directory_releases(2);
        let first = lifetimes.next_release().await;
        let mut flush = Box::pin(flush_directory_releases(
            &mut lifetimes,
            first,
            &send,
            &pool,
        ));
        assert!(flush.as_mut().now_or_never().is_none());
        let bytes = {
            let mut output = output.lock().unwrap();
            output.block_flush = false;
            output.waker.take().unwrap().wake();
            output.bytes.clone()
        };
        let error = flush.await.unwrap_err();
        assert!(error.is::<ControlMessageFailure>());
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            released_paths(bytes).await,
            ["/directory-0", "/directory-1"].map(std::path::PathBuf::from)
        );
        for path in ["/directory-0", "/directory-1"] {
            let path = std::path::Path::new(path);
            assert!(
                lifetimes
                    .admit(path, path, remote::protocol::DirectoryClass::Normal)
                    .is_err(),
                "a failed flush retired {path:?} before acknowledgement"
            );
        }
        assert!(!lifetimes.is_empty());
    }
    #[tokio::test]
    async fn peer_release_send_closure_preserves_collected_destination_error() {
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        errors.push(
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "destination entry permission failure",
            )
            .into(),
        );
        let (writer, peer) = tokio::io::duplex(64);
        drop(peer);
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(writer) as remote::streams::BoxedWrite,
        )));
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        let pool = test_pool(&tracker);
        let mut lifetimes = resources::DirectoryLifetimes::new(remote::protocol::DirectoryLimits {
            normal: std::num::NonZeroUsize::MIN,
            reserve: std::num::NonZeroUsize::MIN,
        });
        let path = std::path::Path::new("/directory");
        drop(
            lifetimes
                .admit(path, path, remote::protocol::DirectoryClass::Normal)
                .unwrap(),
        );
        let released = lifetimes.next_release().await;
        let result = flush_directory_releases(&mut lifetimes, released, &send, &pool).await;
        assert!(result.is_err(), "a lost release must fail the transfer");
        let error =
            choose_final_result(errors.take_error(), Ok(()), result, false, None, summary())
                .unwrap_err();
        assert_eq!(
            error
                .root_cause()
                .downcast_ref::<std::io::Error>()
                .map(std::io::Error::kind),
            Some(std::io::ErrorKind::PermissionDenied),
            "{error:#}"
        );
    }
    #[tokio::test]
    async fn peer_control_closure_preserves_collected_destination_error() {
        for kind in [
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::ConnectionReset,
        ] {
            let errors = Arc::new(common::error_collector::ErrorCollector::default());
            errors.push(
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "destination entry permission failure",
                )
                .into(),
            );
            let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
            )));
            let tracker = directory_tracker::SharedDirectoryTracker::new(
                send.clone(),
                common::preserve::preserve_none(),
                false,
                errors.clone(),
            );
            let result = process_control_stream(
                None,
                &testutils::copy_settings(false, 0),
                0,
                std::num::NonZeroUsize::MIN,
                std::num::NonZeroUsize::new(2).unwrap(),
                &common::preserve::preserve_none(),
                remote::streams::RecvStream::new(
                    Box::new(FailingReader(kind)) as remote::streams::BoxedRead
                ),
                tracker.clone(),
                send,
                test_pool(&tracker),
                errors.clone(),
            )
            .await;
            assert!(
                result.is_err(),
                "peer closure must fail an incomplete transfer"
            );
            let error =
                choose_final_result(errors.take_error(), Ok(()), result, false, None, summary())
                    .unwrap_err();
            assert_eq!(
                error
                    .root_cause()
                    .downcast_ref::<std::io::Error>()
                    .map(std::io::Error::kind),
                Some(std::io::ErrorKind::PermissionDenied),
                "{error:#}"
            );
        }
    }
    #[tokio::test]
    async fn control_tls_closure_is_benign_only_after_done_was_sent() {
        for sent in [false, true] {
            let errors = Arc::new(common::error_collector::ErrorCollector::default());
            let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
            )));
            let tracker = directory_tracker::SharedDirectoryTracker::new(
                send.clone(),
                common::preserve::preserve_none(),
                false,
                errors.clone(),
            );
            if sent {
                receive_test_root_control(
                    tracker.clone(),
                    send,
                    errors.clone(),
                    vec![remote::protocol::SourceMessage::DiscoveryComplete {
                        has_root_item: false,
                    }],
                )
                .await
                .unwrap();
            } else {
                tracker
                    .with_state(|state| state.finish_discovery(false))
                    .unwrap();
            }
            let closure =
                remote::streams::RecvStream::new(FailingReader(std::io::ErrorKind::UnexpectedEof))
                    .recv_object::<remote::protocol::SourceMessage>()
                    .await
                    .unwrap_err();
            assert!(tracker.with_state(|state| state.is_done()));
            assert_eq!(tracker.with_state(|state| state.transfer_complete()), sent);
            let result = choose_final_result(
                errors.take_error(),
                Ok(()),
                Err(closure),
                tracker.with_state(|state| state.transfer_complete()),
                None,
                summary(),
            );
            assert_eq!(result.is_ok(), sent);
        }
    }
    impl tokio::io::AsyncWrite for FailingDoneWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if self.fail_close {
                std::task::Poll::Ready(Ok(bytes.len()))
            } else {
                std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "original Done write cause",
                )))
            }
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
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "original Done close cause",
            )))
        }
    }
    #[tokio::test]
    async fn done_write_and_close_failures_return_the_original_cause() {
        for fail_close in [false, true] {
            let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                Box::new(FailingDoneWriter { fail_close }) as remote::streams::BoxedWrite,
            )));
            let errors = Arc::new(common::error_collector::ErrorCollector::default());
            let tracker = directory_tracker::SharedDirectoryTracker::new(
                send.clone(),
                common::preserve::preserve_none(),
                false,
                errors.clone(),
            );
            let control_result = receive_test_root_control(
                tracker.clone(),
                send,
                errors.clone(),
                vec![remote::protocol::SourceMessage::DiscoveryComplete {
                    has_root_item: false,
                }],
            )
            .await;
            assert!(
                control_result.is_err(),
                "the failed Done owner must exit its receiver"
            );
            assert!(!tracker.send_destination_done().await.unwrap());
            assert!(!tracker.with_state(|state| state.transfer_complete()));
            assert!(tracker.with_state(|state| state.is_done()));
            assert!(
                !errors.has_errors(),
                "the control owner returns its error once"
            );
            assert_eq!(
                control_result
                    .as_ref()
                    .unwrap_err()
                    .downcast_ref::<std::io::Error>()
                    .unwrap()
                    .kind(),
                std::io::ErrorKind::PermissionDenied
            );
            let error = choose_final_result(
                None,
                Ok(()),
                control_result,
                tracker.with_state(|state| state.transfer_complete()),
                None,
                summary(),
            )
            .unwrap_err();

            assert!(format!("{error:#}").contains(if fail_close {
                "original Done close cause"
            } else {
                "original Done write cause"
            }));
        }
    }
    #[tokio::test]
    async fn cancelled_control_done_leaves_the_transfer_incomplete() {
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        let mut frame = Vec::new();
        remote::streams::SendStream::new(&mut frame)
            .send_control_message(&remote::protocol::SourceMessage::DiscoveryComplete {
                has_root_item: false,
            })
            .await
            .unwrap();
        let held = send.lock().await;
        let settings = testutils::copy_settings(false, 0);
        let preserve = common::preserve::preserve_none();
        let mut control = Box::pin(process_control_stream(
            None,
            &settings,
            10,
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::new(8).unwrap(),
            &preserve,
            remote::streams::RecvStream::new(
                Box::new(std::io::Cursor::new(frame)) as remote::streams::BoxedRead
            ),
            tracker.clone(),
            send.clone(),
            test_pool(&tracker),
            errors.clone(),
        ));
        assert!(
            control.as_mut().now_or_never().is_none(),
            "the control owner must wait for its writer"
        );
        drop(control);
        drop(held);
        assert!(tracker.with_state(|state| state.is_done()));
        assert!(!tracker.send_destination_done().await.unwrap());
        let error = choose_final_result(
            errors.take_error(),
            Ok(()),
            Ok(()),
            tracker.with_state(|state| state.transfer_complete()),
            None,
            summary(),
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("incomplete transfer"));
    }

    #[tokio::test]
    async fn duplicate_discovery_marker_remains_fatal_after_completion() {
        for (completes_before_rejection, file_failed) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            assert_control_failure_survives_completion(
                remote::protocol::SourceMessage::DiscoveryComplete {
                    has_root_item: true,
                },
                completes_before_rejection,
                file_failed,
            )
            .await;
        }
    }
    #[tokio::test]
    async fn late_structural_message_remains_fatal_after_completion() {
        for (completes_before_rejection, file_failed) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            assert_control_failure_survives_completion(
                remote::protocol::SourceMessage::DirectoryEnd {
                    src: "/src".into(),
                    dst: "/dst".into(),
                    entry_count: 0,
                },
                completes_before_rejection,
                file_failed,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn control_read_failure_remains_fatal_with_only_logical_completion() {
        for (malformed, tearing_down) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let reader: remote::streams::BoxedRead = if malformed {
                Box::new(std::io::Cursor::new(vec![0u8, 0, 0, 0]))
            } else {
                Box::new(FailingReader(std::io::ErrorKind::ConnectionReset))
            };
            let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
            )));
            let errors = Arc::new(common::error_collector::ErrorCollector::default());
            let tracker = directory_tracker::SharedDirectoryTracker::new(
                send.clone(),
                common::preserve::preserve_none(),
                false,
                errors.clone(),
            );
            tracker
                .with_state(|state| state.finish_discovery(false))
                .unwrap();
            let pool = test_pool(&tracker);
            if tearing_down {
                pool.shutdown.cancel();
            }
            let control_result = process_control_stream(
                None,
                &testutils::copy_settings(false, 0),
                10,
                std::num::NonZeroUsize::MIN,
                std::num::NonZeroUsize::new(8).unwrap(),
                &common::preserve::preserve_none(),
                remote::streams::RecvStream::new(reader),
                tracker.clone(),
                send,
                pool,
                errors.clone(),
            )
            .await;
            assert!(control_result.is_err());
            let result = choose_final_result(
                errors.take_error(),
                Ok(()),
                control_result,
                tracker.with_state(|state| state.transfer_complete()),
                None,
                summary(),
            );
            assert!(
                result.is_err(),
                "logical completion without Sent cannot authorize success"
            );
        }
    }

    #[test]
    fn recorded_error_is_reported_even_when_completed() {
        let r = choose_final_result(
            Some(anyhow::anyhow!("permission denied writing file")),
            Ok(()),
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
        let r = choose_final_result(None, Ok(()), Ok(()), false, None, summary());
        let e = format!("{:#}", r.unwrap_err());
        assert!(e.to_lowercase().contains("incomplete"), "{e}");
    }

    #[test]
    fn incomplete_prefers_the_specific_stream_cause() {
        let r = choose_final_result(
            None,
            Ok(()),
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
            Ok(()),
            Err(anyhow::anyhow!("peer closed connection")), // teardown symptom
            false,
            Some(anyhow::anyhow!("connection refused")), // the real connect cause
            summary(),
        );
        let e = format!("{:#}", r.unwrap_err());
        assert!(e.contains("connection refused"), "{e}");
        assert!(e.contains("incomplete transfer"), "{e}");
    }

    #[tokio::test]
    async fn control_reply_failure_after_teardown_preserves_data_connect_cause() {
        for tearing_down in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let (source, destination) = tokio::io::duplex(4096);
            let mut source = remote::streams::SendStream::new(source);
            source
                .send_control_message(&remote::protocol::SourceMessage::DirectoryBegin {
                    admission: remote::protocol::DirectoryClass::Normal,
                    src: "/src".into(),
                    dst: tmp.path().to_owned(),
                    metadata: remote::protocol::Metadata::from(
                        &std::fs::metadata(tmp.path()).unwrap(),
                    ),
                    is_root: true,
                    keep_if_empty: true,
                })
                .await
                .unwrap();
            let (writer, closed_peer) = tokio::io::duplex(4096);
            drop(closed_peer);
            let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                Box::new(writer) as remote::streams::BoxedWrite,
            )));
            let errors = Arc::new(common::error_collector::ErrorCollector::default());
            let tracker = directory_tracker::SharedDirectoryTracker::new(
                send.clone(),
                common::preserve::preserve_none(),
                false,
                errors.clone(),
            );
            let pool = test_pool(&tracker);
            pool.shutdown.record_first_connect_error(
                std::io::Error::from_raw_os_error(libc::ECONNREFUSED).into(),
            );
            if tearing_down {
                tracker.close_stream().await;
            }
            let control = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                process_control_stream(
                    None,
                    &testutils::copy_settings(false, 0),
                    10,
                    std::num::NonZeroUsize::MIN,
                    std::num::NonZeroUsize::new(8).unwrap(),
                    &common::preserve::preserve_none(),
                    remote::streams::RecvStream::new(
                        Box::new(destination) as remote::streams::BoxedRead
                    ),
                    tracker.clone(),
                    send,
                    pool.clone(),
                    errors.clone(),
                ),
            )
            .await
            .expect("failed acknowledgement must not hang");
            let error = choose_final_result(
                errors.take_error(),
                Ok(()),
                control,
                tracker.with_state(|state| state.is_done()),
                pool.shutdown.take_first_connect_error(),
                summary(),
            )
            .unwrap_err();
            assert_eq!(
                error
                    .root_cause()
                    .downcast_ref::<std::io::Error>()
                    .unwrap()
                    .kind(),
                if tearing_down {
                    std::io::ErrorKind::ConnectionRefused
                } else {
                    std::io::ErrorKind::BrokenPipe
                },
                "tearing_down={tearing_down}: {error:#}"
            );
        }
    }

    #[tokio::test]
    async fn announcer_teardown_does_not_hide_independent_filesystem_failure() {
        for failed_reply in [false, true] {
            let (writer, closed_peer) = tokio::io::duplex(4096);
            drop(closed_peer);
            let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                Box::new(writer) as remote::streams::BoxedWrite,
            )));
            let errors = Arc::new(common::error_collector::ErrorCollector::default());
            let tracker = directory_tracker::SharedDirectoryTracker::new(
                send.clone(),
                common::preserve::preserve_none(),
                false,
                errors.clone(),
            );
            let pool = test_pool(&tracker);
            pool.shutdown.record_first_connect_error(
                std::io::Error::from_raw_os_error(libc::ECONNREFUSED).into(),
            );
            pool.shutdown.cancel();
            if failed_reply {
                publish_announce_failure(
                    &tracker,
                    &errors,
                    announce_directory_ready(
                        &tracker,
                        &send,
                        &pool,
                        std::path::Path::new("/src"),
                        std::path::Path::new("/dst"),
                        Vec::new(),
                    ),
                )
                .await;
            } else {
                publish_announce_failure(&tracker, &errors, async {
                    Err(std::io::Error::from_raw_os_error(libc::EPIPE).into())
                })
                .await;
            }
            let error = choose_final_result(
                errors.take_error(),
                Ok(()),
                Ok(()),
                false,
                pool.shutdown.take_first_connect_error(),
                summary(),
            )
            .unwrap_err();
            assert_eq!(
                error
                    .root_cause()
                    .downcast_ref::<std::io::Error>()
                    .unwrap()
                    .kind(),
                if failed_reply {
                    std::io::ErrorKind::ConnectionRefused
                } else {
                    std::io::ErrorKind::BrokenPipe
                },
                "failed_reply={failed_reply}: {error:#}"
            );
        }
    }

    #[test]
    fn completed_ignores_a_stashed_connect_cause_from_a_benign_reconnect() {
        // Finding #5: a worker that looped to connect() as the source stopped accepting after
        // DestinationDone stashes "connection refused", but the transfer COMPLETED — must be success.
        let r = choose_final_result(
            None,
            Ok(()),
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
        assert!(choose_final_result(None, Ok(()), Ok(()), true, None, summary()).is_ok());
    }

    #[test]
    fn completed_swallows_a_late_teardown_symptom() {
        // once complete, a late control error is only a teardown symptom (e.g. a control send that
        // lost the race with the stream close) — success must not flip to failure.
        let r = choose_final_result(
            None,
            Ok(()),
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
    /// control send lock) and a "loser" future that is SUSPENDED holding that mutex across an await.
    /// The old inline form (`signal.await; loser.await`) parks the signal on the mutex while the
    /// never-polled loser holds it → deadlock. `tokio::join!(signal, loser)` polls both, so the loser
    /// makes progress, releases the lock, and the signal completes. This pins the async-ordering
    /// property the real `run_destination` combinator relies on.
    #[tokio::test(start_paused = true)]
    async fn join_of_signal_and_loser_does_not_deadlock_when_loser_holds_the_lock() {
        let lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
        let release = std::sync::Arc::new(tokio::sync::Notify::new());

        // the loser acquires the lock and then suspends WHILE STILL HOLDING it (mirrors
        // process_control_stream suspended mid-`send_directory_skipped` holding the control send lock).
        let loser = {
            let lock = lock.clone();
            let release = release.clone();
            async move {
                let _guard = lock.lock().await;
                release.notified().await;
            }
        };
        // the signal parks on the same lock (mirrors tracker.close_stream).
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

    fn tracker_over_sink() -> directory_tracker::SharedDirectoryTracker {
        let send = remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite
        );
        directory_tracker::SharedDirectoryTracker::new(
            std::sync::Arc::new(tokio::sync::Mutex::new(send)),
            common::preserve::Settings::default(),
            false,
            std::sync::Arc::new(common::error_collector::ErrorCollector::default()),
        )
    }

    /// A data pool sharing the tracker's shutdown transition.
    fn test_pool(
        tracker: &directory_tracker::SharedDirectoryTracker,
    ) -> std::sync::Arc<DataConnectionPool> {
        std::sync::Arc::new(DataConnectionPool::new(
            "127.0.0.1:1".parse().unwrap(),
            1,
            &remote::TcpConfig {
                keepalive_sec: remote::DEFAULT_REMOTE_KEEPALIVE_SEC,
                conn_timeout_sec: 1,
                ..Default::default()
            },
            None,
            tracker.shutdown().clone(),
        ))
    }

    #[tokio::test]
    async fn data_pool_applies_buffer_retention_to_connected_receiver() {
        for (chunk, limit, expected) in [
            (None, None, 2 * 1024 * 1024),
            (Some(4096), None, 4096),
            (None, Some(0), 0),
            (None, Some(262144), 262144),
            (None, Some(16 * 1024 * 1024), 16 * 1024 * 1024),
        ] {
            let tcp = remote::TcpConfig {
                buffer_size: chunk,
                buffer_retention_limit: limit,
                ..Default::default()
            };
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let tracker = tracker_over_sink();
            let pool = DataConnectionPool::new(
                listener.local_addr().unwrap(),
                1,
                &tcp,
                None,
                tracker.shutdown().clone(),
            );
            let (receiver, peer) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::join!(
                    pool.connect_and_handshake(),
                    remote::accept_tcp_data(&listener, tcp.network_profile, tcp.keepalive_sec)
                )
            })
            .await
            .unwrap();
            let _peer = peer.unwrap();
            assert_eq!(receiver.unwrap().copy_buffer_retention_limit(), expected);
        }
    }

    #[tokio::test]
    async fn abandoned_directory_claim_releases_connection_admission_waiters() {
        let tracker = tracker_over_sink();
        let pool = test_pool(&tracker);
        let _held = pool.semaphore.acquire().await.unwrap();
        let admission = tracker
            .with_state(|state| state.admit_directory(std::path::Path::new("/dst"), true))
            .unwrap();
        let mut connecting = Box::pin(pool.connect());
        assert!(connecting.as_mut().now_or_never().is_none());
        drop(admission);
        assert!(matches!(
            connecting.now_or_never(),
            Some(ConnectOutcome::PoolClosed)
        ));
    }

    #[tokio::test]
    async fn abandoned_directory_claim_cancels_an_in_flight_tls_handshake() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let certificate = remote::tls::generate_self_signed_cert().unwrap();
        let config =
            remote::tls::create_client_config_with_cert(&certificate, certificate.fingerprint)
                .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tracker = tracker_over_sink();
        let pool = DataConnectionPool::new(
            listener.local_addr().unwrap(),
            1,
            &remote::TcpConfig {
                keepalive_sec: 0,
                conn_timeout_sec: 60,
                ..Default::default()
            },
            Some(Arc::new(tokio_rustls::TlsConnector::from(config))),
            tracker.shutdown().clone(),
        );
        let admission = tracker
            .with_state(|state| state.admit_directory(std::path::Path::new("/dst"), true))
            .unwrap();
        let mut connecting = Box::pin(pool.connect());
        let (peer, _) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::select! {
                _ = &mut connecting => panic!("handshake completed before the peer replied"),
                accepted = remote::accept_tcp_data(&listener, pool.network_profile, pool.keepalive_sec) => accepted.unwrap(),
            }
        })
        .await
        .unwrap();
        assert!(connecting.as_mut().now_or_never().is_none());
        drop(admission);
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(5), connecting)
                .await
                .expect("shared shutdown must release a stalled handshake"),
            ConnectOutcome::PoolClosed
        ));
        drop(peer);
    }

    async fn receive_test_root_control(
        tracker: directory_tracker::SharedDirectoryTracker,
        send: remote::streams::BoxedSharedSendStream,
        errors: Arc<common::error_collector::ErrorCollector>,
        messages: Vec<remote::protocol::SourceMessage>,
    ) -> anyhow::Result<()> {
        let (source, destination) = tokio::io::duplex(4096);
        let mut source = remote::streams::SendStream::new(source);
        for message in messages {
            source.send_control_message(&message).await?;
        }
        source.close().await?;
        process_control_stream(
            None,
            &testutils::copy_settings(false, 0),
            10,
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::new(8).unwrap(),
            &common::preserve::preserve_none(),
            remote::streams::RecvStream::new(Box::new(destination) as remote::streams::BoxedRead),
            tracker.clone(),
            send,
            test_pool(&tracker),
            errors,
        )
        .await
    }
    #[tokio::test]
    async fn admitted_directory_root_excludes_file_and_symlink_before_registration() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let file = tmp.path().join("file");
        let link = tmp.path().join("link");
        let metadata = remote::protocol::Metadata::from(&std::fs::metadata(tmp.path()).unwrap());
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        let admission = tracker
            .with_state(|state| state.admit_directory(&root, true))
            .unwrap();
        let (source, destination) = tokio::io::duplex(4096);
        let mut source = remote::streams::SendStream::new(source);
        source
            .send_control_message(&remote::protocol::File {
                src: "/src".into(),
                dst: file.clone(),
                size: 0,
                metadata: metadata.clone(),
                is_root: true,
            })
            .await
            .unwrap();
        source.close().await.unwrap();
        let file_receive = run_over_reader(tracker.clone(), Box::new(destination));
        let link_receive = receive_test_root_control(
            tracker.clone(),
            send,
            errors,
            vec![remote::protocol::SourceMessage::Symlink {
                src: "/src".into(),
                dst: link.clone(),
                target: "target".into(),
                metadata: metadata.clone(),
                is_root: true,
            }],
        );
        let (file_result, link_result) = tokio::join!(file_receive, link_receive);
        for result in [file_result, link_result] {
            assert!(format!("{:#}", result.unwrap_err()).contains("duplicate root item"));
        }
        for path in [&root, &file, &link] {
            assert!(path.symlink_metadata().is_err());
        }
        drop(admission);
        assert!(tracker.with_state(|state| state.is_closing()));
        assert!(tracker.with_state(|state| state.get_dir(&root)).is_none());
    }

    #[tokio::test]
    async fn duplicate_root_symlink_is_rejected_before_creating_another_link() {
        let tmp = tempfile::tempdir().unwrap();
        let first = tmp.path().join("first");
        let second = tmp.path().join("second");
        let metadata = remote::protocol::Metadata::from(&std::fs::metadata(tmp.path()).unwrap());
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        let messages = [&first, &second]
            .into_iter()
            .map(|dst| remote::protocol::SourceMessage::Symlink {
                src: "/src".into(),
                dst: dst.clone(),
                target: "target".into(),
                metadata: metadata.clone(),
                is_root: true,
            })
            .collect();
        let result =
            receive_test_root_control(tracker.clone(), send, errors.clone(), messages).await;
        assert_eq!(
            std::fs::read_link(first).unwrap(),
            std::path::Path::new("target")
        );
        assert!(
            std::fs::symlink_metadata(&second).is_err(),
            "a duplicate root must be rejected before mutation"
        );
        tracker
            .with_state(|state| state.finish_discovery(true))
            .unwrap();
        let error = choose_final_result(
            errors.take_error(),
            Ok(()),
            result,
            tracker.with_state(|state| state.is_done()),
            None,
            summary(),
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("duplicate root item"));
    }
    #[tokio::test]
    async fn duplicate_root_file_is_recorded_even_after_root_completion() {
        for first_is_symlink in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let first = tmp.path().join("first");
            let second = tmp.path().join("second");
            let metadata =
                remote::protocol::Metadata::from(&std::fs::metadata(tmp.path()).unwrap());
            let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
            )));
            let errors = Arc::new(common::error_collector::ErrorCollector::default());
            let tracker = directory_tracker::SharedDirectoryTracker::new(
                send.clone(),
                common::preserve::preserve_none(),
                false,
                errors.clone(),
            );
            if first_is_symlink {
                receive_test_root_control(
                    tracker.clone(),
                    send,
                    errors.clone(),
                    vec![
                        remote::protocol::SourceMessage::Symlink {
                            src: "/src".into(),
                            dst: first.clone(),
                            target: "target".into(),
                            metadata: metadata.clone(),
                            is_root: true,
                        },
                        remote::protocol::SourceMessage::DiscoveryComplete {
                            has_root_item: true,
                        },
                    ],
                )
                .await
                .unwrap();
            } else {
                // model discovery while the root file is still in flight; control EOF would
                // already cancel the shared receiver and prevent opening a new data stream
                tracker
                    .with_state(|state| state.finish_discovery(true))
                    .unwrap();
            }
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let pool = Arc::new(DataConnectionPool::new(
                listener.local_addr().unwrap(),
                1,
                &remote::TcpConfig {
                    keepalive_sec: remote::DEFAULT_REMOTE_KEEPALIVE_SEC,
                    conn_timeout_sec: 1,
                    ..Default::default()
                },
                None,
                tracker.shutdown().clone(),
            ));
            let source = async {
                let (stream, _) = remote::accept_tcp_data(
                    &listener,
                    remote::NetworkProfile::default(),
                    remote::DEFAULT_REMOTE_KEEPALIVE_SEC,
                )
                .await
                .unwrap();
                drop(listener);
                let mut source = remote::streams::SendStream::new(stream);
                let paths = if first_is_symlink {
                    vec![&second]
                } else {
                    vec![&first, &second]
                };
                for dst in paths {
                    source
                        .send_control_message(&remote::protocol::File {
                            src: "/src".into(),
                            dst: dst.clone(),
                            size: 0,
                            metadata: metadata.clone(),
                            is_root: true,
                        })
                        .await
                        .unwrap();
                }
                source.close().await.unwrap();
            };
            let receiver = process_incoming_file_streams_tcp(
                testutils::copy_settings(false, 0),
                common::preserve::preserve_none(),
                pool,
                tracker.clone(),
                errors.clone(),
            );
            let (file_result, ()) =
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    tokio::join!(receiver, source)
                })
                .await
                .expect("duplicate root must terminate the data receiver");
            assert!(
                first.symlink_metadata().is_ok(),
                "the first root must be accepted"
            );
            assert!(
                second.symlink_metadata().is_err(),
                "a duplicate root must be rejected before mutation"
            );
            let completed = tracker.with_state(|state| state.is_done());
            assert!(
                completed,
                "verify the error collector defeats completed-result suppression"
            );
            let error = choose_final_result(
                errors.take_error(),
                file_result,
                Ok(()),
                completed,
                None,
                summary(),
            )
            .unwrap_err();
            assert!(format!("{error:#}").contains("duplicate root item"));
        }
    }

    #[tokio::test]
    async fn final_ready_flushes_before_destination_done_exactly_once() {
        let tmp = tempfile::tempdir().unwrap();
        let (writer, reader) = tokio::io::duplex(4096);
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(writer) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        {
            let dir = Arc::new(
                Dir::open_root_dir(tmp.path(), false, common::Side::Destination)
                    .await
                    .unwrap(),
            );
            let admission = tracker
                .with_state(|state| state.admit_directory(tmp.path(), true))
                .unwrap();
            tracker
                .register_directory(
                    admission,
                    dir,
                    remote::protocol::Metadata::from(&std::fs::metadata(tmp.path()).unwrap()),
                    false,
                    true,
                    None,
                )
                .unwrap();
            tracker.seal_directory(tmp.path(), 0).await.unwrap();
        }
        let (source, destination) = tokio::io::duplex(4096);
        let mut source = remote::streams::SendStream::new(source);
        source
            .send_control_message(&remote::protocol::SourceMessage::DiscoveryComplete {
                has_root_item: true,
            })
            .await
            .unwrap();
        let pool = test_pool(&tracker);
        let settings = testutils::copy_settings(false, 0);
        let preserve = common::preserve::preserve_none();
        let control = process_control_stream(
            None,
            &settings,
            0,
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::new(2).unwrap(),
            &preserve,
            remote::streams::RecvStream::new(Box::new(destination) as remote::streams::BoxedRead),
            tracker.clone(),
            send.clone(),
            pool.clone(),
            errors,
        );
        let announce = announce_directory_ready(
            &tracker,
            &send,
            &pool,
            std::path::Path::new("/src"),
            tmp.path(),
            Vec::new(),
        );
        let (control, announce) = tokio::join!(control, announce);
        control.unwrap();
        announce.unwrap();
        assert!(!tracker.send_destination_done().await.unwrap());
        let mut reader = remote::streams::RecvStream::new(reader);
        assert!(matches!(
            reader
                .recv_object::<remote::protocol::DestinationMessage>()
                .await
                .unwrap(),
            Some(remote::protocol::DestinationMessage::DirectoryReady { .. })
        ));
        assert!(matches!(
            reader
                .recv_object::<remote::protocol::DestinationMessage>()
                .await
                .unwrap(),
            Some(remote::protocol::DestinationMessage::DestinationDone)
        ));
        assert!(
            reader
                .recv_object::<remote::protocol::DestinationMessage>()
                .await
                .unwrap()
                .is_none()
        );
    }
    #[tokio::test]
    async fn rejected_root_and_submitted_descendant_each_receive_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("file");
        std::fs::write(&root, "existing").unwrap();
        let child = root.join("child");
        let metadata = remote::protocol::Metadata::from(&std::fs::metadata(tmp.path()).unwrap());
        let (source, destination) = tokio::io::duplex(4096);
        let mut source = remote::streams::SendStream::new(source);
        for message in [
            remote::protocol::SourceMessage::DirectoryBegin {
                admission: remote::protocol::DirectoryClass::Normal,
                src: "/src".into(),
                dst: root.clone(),
                metadata: metadata.clone(),
                is_root: true,
                keep_if_empty: true,
            },
            remote::protocol::SourceMessage::DirectoryEnd {
                src: "/src".into(),
                dst: root.clone(),
                entry_count: 1,
            },
            remote::protocol::SourceMessage::DirectoryBegin {
                admission: remote::protocol::DirectoryClass::Normal,
                src: "/src/child".into(),
                dst: child.clone(),
                metadata,
                is_root: false,
                keep_if_empty: true,
            },
            remote::protocol::SourceMessage::DirectoryEnd {
                src: "/src/child".into(),
                dst: child.clone(),
                entry_count: 0,
            },
            remote::protocol::SourceMessage::DiscoveryComplete {
                has_root_item: true,
            },
        ] {
            source.send_control_message(&message).await.unwrap();
        }
        let (writer, reader) = tokio::io::duplex(4096);
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(writer) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        let mut settings = testutils::copy_settings(false, 0);
        settings.ignore_existing = true;
        process_control_stream(
            None,
            &settings,
            10,
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::new(8).unwrap(),
            &common::preserve::preserve_none(),
            remote::streams::RecvStream::new(Box::new(destination) as remote::streams::BoxedRead),
            tracker.clone(),
            send,
            test_pool(&tracker),
            errors.clone(),
        )
        .await
        .unwrap();
        let mut reader = remote::streams::RecvStream::new(reader);
        for expected in [root.clone(), child] {
            assert!(
                matches!(reader.recv_object::<remote::protocol::DestinationMessage>().await.unwrap(), Some(remote::protocol::DestinationMessage::DirectorySkipped { dst, .. }) if dst == expected)
            );
        }
        assert!(matches!(
            reader
                .recv_object::<remote::protocol::DestinationMessage>()
                .await
                .unwrap(),
            Some(remote::protocol::DestinationMessage::DestinationDone)
        ));
        assert!(
            reader
                .recv_object::<remote::protocol::DestinationMessage>()
                .await
                .unwrap()
                .is_none()
        );
        assert!(errors.take_error().is_none());
        assert_eq!(std::fs::read_to_string(root).unwrap(), "existing");
    }

    #[tokio::test]
    async fn discovery_marker_rejects_late_structural_messages_before_filesystem_work() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("late");
        let metadata = remote::protocol::Metadata::from(&std::fs::metadata(tmp.path()).unwrap());
        let messages = [
            remote::protocol::SourceMessage::DirectoryBegin {
                admission: remote::protocol::DirectoryClass::Normal,
                src: "/src".into(),
                dst: dst.clone(),
                metadata: metadata.clone(),
                is_root: true,
                keep_if_empty: true,
            },
            remote::protocol::SourceMessage::DirectoryEnd {
                src: "/src".into(),
                dst: dst.clone(),
                entry_count: 0,
            },
            remote::protocol::SourceMessage::Symlink {
                src: "/src".into(),
                dst: dst.clone(),
                target: "target".into(),
                metadata,
                is_root: true,
            },
        ];
        for message in messages {
            let (source, destination) = tokio::io::duplex(4096);
            let mut source = remote::streams::SendStream::new(source);
            source
                .send_control_message(&remote::protocol::SourceMessage::DiscoveryComplete {
                    has_root_item: true,
                })
                .await
                .unwrap();
            source.send_control_message(&message).await.unwrap();
            source.close().await.unwrap();
            let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
                Box::new(tokio::io::sink()) as remote::streams::BoxedWrite,
            )));
            let errors = Arc::new(common::error_collector::ErrorCollector::default());
            let tracker = directory_tracker::SharedDirectoryTracker::new(
                send.clone(),
                common::preserve::preserve_none(),
                false,
                errors.clone(),
            );
            let result = process_control_stream(
                None,
                &testutils::copy_settings(false, 0),
                10,
                std::num::NonZeroUsize::MIN,
                std::num::NonZeroUsize::new(8).unwrap(),
                &common::preserve::preserve_none(),
                remote::streams::RecvStream::new(
                    Box::new(destination) as remote::streams::BoxedRead
                ),
                tracker.clone(),
                send,
                test_pool(&tracker),
                errors,
            )
            .await;
            let error = result.expect_err("structural traffic must stop at DiscoveryComplete");
            assert!(format!("{error:#}").contains("structural message after DiscoveryComplete"));
            assert!(std::fs::symlink_metadata(&dst).is_err());
        }
    }

    #[cfg(panic = "unwind")]
    #[tokio::test]
    async fn announcer_panic_cancels_pool_while_source_waits_for_ready() {
        struct PanicOnce {
            panicked: bool,
            stream: tokio::io::DuplexStream,
        }
        impl tokio::io::AsyncWrite for PanicOnce {
            fn poll_write(
                mut self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                bytes: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                if !self.panicked {
                    self.panicked = true;
                    panic!("announcer test panic");
                }
                std::task::Poll::Ready(Ok(bytes.len()))
            }
            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
            fn poll_shutdown(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let (source_receive, destination_send) = tokio::io::duplex(4096);
        let mut source_receive = remote::streams::RecvStream::new(source_receive);
        let send = Arc::new(tokio::sync::Mutex::new(remote::streams::SendStream::new(
            Box::new(PanicOnce {
                panicked: false,
                stream: destination_send,
            }) as remote::streams::BoxedWrite,
        )));
        let errors = Arc::new(common::error_collector::ErrorCollector::default());
        let tracker = directory_tracker::SharedDirectoryTracker::new(
            send.clone(),
            common::preserve::preserve_none(),
            false,
            errors.clone(),
        );
        let pool = test_pool(&tracker);
        let (source, destination) = tokio::io::duplex(4096);
        let mut source = remote::streams::SendStream::new(source);
        source
            .send_control_message(&remote::protocol::SourceMessage::DirectoryBegin {
                admission: remote::protocol::DirectoryClass::Normal,
                src: "/src".into(),
                dst: tmp.path().to_owned(),
                metadata: remote::protocol::Metadata::from(&std::fs::metadata(tmp.path()).unwrap()),
                is_root: true,
                keep_if_empty: true,
            })
            .await
            .unwrap();
        let mut settings = testutils::copy_settings(false, 0);
        settings.overwrite = true;
        let preserve = common::preserve::preserve_none();
        let receive = process_control_stream(
            None,
            &settings,
            10,
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroUsize::new(8).unwrap(),
            &preserve,
            remote::streams::RecvStream::new(Box::new(destination) as remote::streams::BoxedRead),
            tracker,
            send,
            pool.clone(),
            errors.clone(),
        );
        tokio::pin!(receive);
        let cancelled = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::select! {
                result = &mut receive => {
                    assert!(pool.shutdown.is_cancelled(), "receiver exited before publishing cancellation");
                    Some(result)
                },
                () = pool.shutdown.cancelled() => None,
            }
        }).await;
        assert!(
            cancelled.is_ok(),
            "an announcer panic must publish cancellation without another source message"
        );
        let error = errors
            .take_error()
            .expect("panic must be recorded before cancellation");
        assert!(format!("{error:#}").contains("announcer test panic"));
        let closed = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            source_receive.recv_object::<remote::protocol::DestinationMessage>(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            closed.is_none(),
            "source waiting for Ready must observe destination teardown"
        );
        source.close().await.unwrap();
        if let Some(result) = cancelled.unwrap() {
            result.unwrap();
        } else {
            receive.await.unwrap();
        }
    }

    async fn run_over_reader(
        tracker: directory_tracker::SharedDirectoryTracker,
        reader: remote::streams::BoxedRead,
    ) -> anyhow::Result<()> {
        let recv = remote::streams::RecvStream::new(reader);
        handle_file_stream(
            testutils::copy_settings(false, 0),
            common::preserve::Settings::default(),
            recv,
            tracker,
            std::sync::Arc::new(common::error_collector::ErrorCollector::default()),
        )
        .await
    }

    async fn run_handle_file_stream(
        tracker: directory_tracker::SharedDirectoryTracker,
        kind: std::io::ErrorKind,
    ) -> anyhow::Result<()> {
        run_over_reader(
            tracker,
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
            Box::new(tokio::io::empty()) as remote::streams::BoxedRead,
        )
        .await;
        let e = format!(
            "{:#}",
            r.expect_err("a pre-completion clean EOF must be fatal")
        );
        assert!(e.contains("before the transfer completed"), "{e}");
    }

    #[tokio::test]
    async fn clean_eof_during_teardown_is_benign() {
        let tracker = tracker_over_sink();
        tracker.begin_close();
        run_over_reader(
            tracker,
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
    async fn cancelled_receiver_drops_data_workers_before_its_scope_returns() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tracker = tracker_over_sink();
        let pool = Arc::new(DataConnectionPool::new(
            listener.local_addr().unwrap(),
            1,
            &remote::TcpConfig {
                keepalive_sec: 0,
                conn_timeout_sec: 5,
                ..Default::default()
            },
            None,
            tracker.shutdown().clone(),
        ));
        let worker_owner = Arc::downgrade(&pool);
        let peer = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            common::task_scope::scope_tasks(async move {
                let receiver = process_incoming_file_streams_tcp(
                    testutils::copy_settings(false, 0),
                    common::preserve::preserve_none(),
                    pool,
                    tracker,
                    Arc::new(common::error_collector::ErrorCollector::default()),
                );
                tokio::pin!(receiver);
                tokio::select! {
                    result = &mut receiver => panic!("idle receiver ended early: {result:?}"),
                    accepted = remote::accept_tcp_data(&listener, remote::NetworkProfile::default(), 0) => accepted.unwrap().0,
                }
            }),
        )
        .await
        .expect("cancelled worker ownership must drain");
        // keep the peer open: only cancellation, not EOF, can release the worker's captures
        assert!(
            worker_owner.upgrade().is_none(),
            "the receiver scope returned before its data worker released ownership"
        );
        drop(peer);
    }

    #[tokio::test]
    async fn peer_closure_after_completion_is_benign() {
        let t = tracker_over_sink();
        // has_root_item=false sets structure_complete AND root_complete → is_done() == true.
        t.with_state(|state| state.finish_discovery(false)).unwrap();
        assert!(t.with_state(|state| state.is_done()));
        let r = run_handle_file_stream(t, std::io::ErrorKind::UnexpectedEof).await;
        assert!(
            r.is_ok(),
            "a peer closure after completion is a normal end-of-transfer"
        );
    }

    #[tokio::test]
    async fn peer_closure_during_our_teardown_is_benign() {
        let t = tracker_over_sink();
        t.close_stream().await; // sets is_closing()
        assert!(t.with_state(|state| state.is_closing()));
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

        for width in [1, 2] {
            let manifest = build_existing_manifest(
                &dir,
                usize::MAX,
                std::num::NonZeroUsize::new(width).unwrap(),
            )
            .await;
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
    }

    #[tokio::test]
    async fn manifest_capped_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a.txt"), "x").unwrap();
        std::fs::write(tmp.path().join("b.txt"), "y").unwrap();
        let dir = open_dir(tmp.path()).await;

        // 2 entries, cap 1 => fall back to empty manifest (no stats, transfer-and-drain)
        let manifest = build_existing_manifest(&dir, 1, std::num::NonZeroUsize::MIN).await;
        assert!(manifest.is_empty());
    }

    #[tokio::test]
    async fn manifest_at_cap_keeps_every_entry() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a.txt"), "x").unwrap();
        std::fs::write(tmp.path().join("b.txt"), "yz").unwrap();
        let dir = open_dir(tmp.path()).await;
        let manifest =
            build_existing_manifest(&dir, 2, std::num::NonZeroUsize::new(2).unwrap()).await;
        assert_eq!(manifest.len(), 2);
        let sizes: std::collections::BTreeMap<_, _> = manifest
            .into_iter()
            .map(|entry| (entry.name, entry.size))
            .collect();
        assert_eq!(
            sizes,
            [
                (std::path::PathBuf::from("a.txt"), 1),
                (std::path::PathBuf::from("b.txt"), 2)
            ]
            .into_iter()
            .collect()
        );
    }

    #[tokio::test]
    async fn manifest_lookup_omits_missing_child_and_keeps_existing_sibling() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("present"), "data").unwrap();
        let dir = open_dir(tmp.path()).await;
        let (missing, present) = futures::join!(
            lookup_manifest_entry(&dir, std::ffi::OsString::from("missing")),
            lookup_manifest_entry(&dir, std::ffi::OsString::from("present")),
        );
        assert!(missing.is_none());
        let present = present.unwrap();
        assert_eq!(present.name, std::path::Path::new("present"));
        assert!(present.is_file);
        assert_eq!(present.size, 4);
    }

    #[tokio::test]
    async fn manifest_lookup_releases_admission_after_completion_or_cancellation() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("present"), "data").unwrap();
        let dir = open_dir(tmp.path()).await;
        let _reset = testutils::ResetAdmission;
        throttle::set_admission_limits(Some(std::num::NonZeroUsize::MIN));
        let held = throttle::pending_meta_permit().await;
        // an invalid component would finish synchronously inside Dir::child without admission.
        // now_or_never drops the waiting lookup, exercising cancellation before submission.
        assert!(
            lookup_manifest_entry(&dir, std::ffi::OsString::from("invalid/name"))
                .now_or_never()
                .is_none()
        );
        drop(held);
        let entry = lookup_manifest_entry(&dir, std::ffi::OsString::from("present"))
            .await
            .unwrap();
        let next = throttle::pending_meta_permit().now_or_never();
        assert!(
            next.is_some(),
            "owned metadata must not retain fd admission"
        );
        assert_eq!(entry.size, 4);
    }

    #[tokio::test]
    async fn manifest_zero_cap_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a.txt"), "x").unwrap();
        let dir = open_dir(tmp.path()).await;

        // cap 0 disables the optimization (short-circuits before the readdir)
        let manifest = build_existing_manifest(&dir, 0, std::num::NonZeroUsize::MIN).await;
        assert!(manifest.is_empty());
    }
}

#[cfg(test)]
mod removal_tests {
    use super::*;

    #[tokio::test]
    async fn overwrite_removes_a_replacement_in_the_contained_destination_slot()
    -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let victim_path = tmp.path().join("victim.txt");
        tokio::fs::write(&victim_path, "original").await?;
        let dst_parent =
            Arc::new(Dir::open_root_dir(tmp.path(), false, common::Side::Destination).await?);
        let victim = OsStr::new("victim.txt");
        let planned = dst_parent.child(victim).await?;

        // a competing writer changes the final component after destination classification.
        tokio::fs::remove_file(&victim_path).await?;
        tokio::fs::write(&victim_path, "replacement").await?;

        remove_existing_dst(
            &dst_parent,
            victim,
            &victim_path,
            planned.into_removal_snapshot(),
            &testutils::copy_settings(true, 0),
        )
        .await?;

        assert!(
            !victim_path.exists(),
            "overwrite removes the entry currently occupying the contained slot"
        );
        Ok(())
    }
}
