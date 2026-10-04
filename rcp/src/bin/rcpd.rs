use anyhow::Context;
use clap::Parser;
use tokio::io::AsyncReadExt;
use tracing::instrument;

use rcp_tools_rcp::{destination, source};

#[derive(Parser, Debug, Clone)]
#[command(
    name = "rcpd",
    version,
    about = "Remote copy daemon - used by `rcp` for performing remote data copies",
    long_about = "`rcpd` is used by the `rcp` command for performing remote data copies.

This daemon is automatically started by `rcp` on remote hosts via SSH and should not typically be invoked manually. Please see `rcp --help` for more information about remote copy operations."
)]
struct Args {
    /// Role of this rcpd instance (source or destination)
    ///
    /// This is set by the master (rcp) to distinguish between source and destination
    /// rcpd processes, especially for same-host copies
    #[arg(long, value_name = "ROLE")]
    role: remote::protocol::RcpdRole,

    /// Preview-only startup admission; the master's role hello must agree before work begins.
    #[arg(long, hide = true)]
    preview_only: bool,

    // Copy options
    /// Overwrite existing files/directories
    #[arg(short, long, help_heading = "Copy options")]
    overwrite: bool,

    /// File attributes to compare when deciding if files are identical (used with --overwrite)
    ///
    /// Comma-separated list. Available: uid, gid, mode, size, mtime, ctime
    #[arg(
        long,
        default_value = "size,mtime",
        value_name = "OPTIONS",
        help_heading = "Copy options"
    )]
    overwrite_compare: String,

    /// Max pre-existing destination entries per directory eligible for skip-without-transfer.
    #[arg(
        long,
        default_value_t = remote::protocol::DEFAULT_OVERWRITE_MANIFEST_MAX_ENTRIES,
        value_name = "N",
        help_heading = "Copy options"
    )]
    overwrite_manifest_max_entries: usize,

    /// Skip overwriting files that match a condition (used with --overwrite)
    #[arg(
        long,
        value_name = "FILTER",
        requires = "overwrite",
        help_heading = "Copy options"
    )]
    overwrite_filter: Option<common::copy::OverwriteFilter>,

    /// Do not overwrite existing files
    #[arg(long, conflicts_with = "overwrite", help_heading = "Copy options")]
    ignore_existing: bool,

    /// Skip special files (sockets, FIFOs, devices) without error
    #[arg(long, help_heading = "Copy options")]
    skip_specials: bool,

    /// Exit on first error
    #[arg(short = 'e', long = "fail-early", help_heading = "Copy options")]
    fail_early: bool,

    /// Always follow symbolic links in source
    #[arg(short = 'L', long, help_heading = "Copy options")]
    dereference: bool,

    /// Mirror of master's --require-toctou-safe flag
    ///
    /// Arms strict operand resolution on this rcpd instance: the operand
    /// root/parent opens resolve with openat2(RESOLVE_NO_SYMLINKS). The operand
    /// paths themselves arrive via the master (which validated their strict
    /// form), so no operands are linted here.
    #[arg(long, help_heading = "Copy options")]
    require_toctou_safe: bool,

    /// Quiet mode, don't report errors
    #[arg(short = 'q', long = "quiet", help_heading = "Progress & output")]
    quiet: bool,

    /// Chunk size for calculating I/O operations per file
    ///
    /// Required when using --iops-throttle (must be > 0)
    #[arg(
        long,
        default_value = "0",
        value_name = "SIZE",
        help_heading = "Performance & throttling"
    )]
    chunk_size: u64,

    // note: rcpd never reads --progress-type from the master CLI (master sets
    // progress mode out-of-band via control messages). The flag is accepted as
    // a no-op via CommonArgs to keep the shared definition simple.
    #[command(flatten)]
    common: common::cli::CommonArgs,

    #[arg(
        long,
        hide = true,
        value_parser = common::cli::parse_positive_usize,
        conflicts_with_all = [
            "max_files_in_flight",
            "max_open_files",
            "forwarded_legacy_files_in_flight"
        ]
    )]
    resolved_automatic_files_in_flight: Option<std::num::NonZeroUsize>,

    #[arg(
        long,
        hide = true,
        value_name = "N",
        conflicts_with_all = [
            "max_files_in_flight",
            "max_open_files",
            "resolved_automatic_files_in_flight"
        ]
    )]
    forwarded_legacy_files_in_flight: Option<usize>,

    // Remote copy options
    /// IP address to bind TCP server to (set by master, internal use only)
    #[arg(long, value_name = "IP", help_heading = "Remote copy options")]
    bind_ip: Option<String>,

    /// Restrict TCP to specific port ranges (e.g., "8000-8999,10000-10999")
    ///
    /// Both `-` and `:` are accepted as range separators (e.g., "8000-8999" or "8000:8999").
    /// Defaults to dynamic port allocation if not specified.
    #[arg(long, value_name = "RANGES", help_heading = "Remote copy options")]
    port_ranges: Option<String>,

    /// Disable TLS encryption and authentication for all connections
    ///
    /// WARNING: Disables both encryption and authentication. Data is sent in plaintext
    /// and connections are accepted from anyone. Only use on isolated, trusted networks.
    #[arg(long, help_heading = "Remote copy options")]
    no_encryption: bool,

    /// Master's certificate fingerprint for client authentication (internal use)
    ///
    /// When TLS is enabled, rcpd will verify that connecting clients present a certificate
    /// with this fingerprint. This prevents unauthorized connections to the rcpd port.
    #[arg(long, value_name = "FINGERPRINT", help_heading = "Remote copy options")]
    master_cert_fp: Option<String>,

    /// Connection timeout for remote copy operations in seconds
    ///
    /// Applies to: rcpd→master connection, destination→source connection. Must be at least 1.
    #[arg(
        long,
        default_value = "15",
        value_name = "N",
        value_parser = common::cli::parse_positive_u64,
        help_heading = "Remote copy options"
    )]
    remote_copy_conn_timeout_sec: u64,

    /// Liveness budget for every rcp TCP connection in seconds, 0 disables (set by rcp master)
    ///
    /// Mirrors the master's --remote-keepalive-sec. Without it this rcpd would keep hanging on
    /// a peer whose host vanished while the master recovered on its own. Keepalive applies to
    /// every connection; TCP_USER_TIMEOUT only to control connections (see
    /// remote::configure_tcp_socket).
    #[arg(
        long,
        value_name = "N",
        default_value_t = remote::DEFAULT_REMOTE_KEEPALIVE_SEC,
        help_heading = "Remote copy options"
    )]
    remote_keepalive_sec: u64,

    /// Network profile for TCP tuning
    #[arg(
        long,
        default_value = "datacenter",
        value_name = "PROFILE",
        help_heading = "Remote copy options"
    )]
    network_profile: remote::NetworkProfile,

    /// Buffer size for remote copy file transfer operations in bytes.
    ///
    /// Controls the buffer used when copying data between files and network streams.
    /// Larger buffers can improve throughput but use more memory per concurrent transfer.
    ///
    /// Default: 16 MiB for datacenter, 2 MiB for internet profile.
    #[arg(long, value_name = "BYTES", help_heading = "Remote copy options")]
    buffer_size: Option<usize>,

    /// Maximum receiver scratch retained per data connection, in bytes.
    ///
    /// Defaults to min(effective buffer size, 2 MiB). Allocated lazily; zero disables retention.
    /// Does not change I/O chunk sizes: larger chunks use temporary storage per file.
    #[arg(long, value_name = "BYTES", help_heading = "Remote copy options")]
    buffer_retention_limit: Option<usize>,

    /// Maximum concurrent data connections (default: 100)
    ///
    /// This separately configurable ceiling defaults to 100. Effective data streams are
    /// min(--max-files-in-flight, --max-connections). Remote automatic limits resolve on the
    /// source rcpd and are adopted by the destination. To raise remote parallelism above the
    /// CPU-derived file default, increase both ceilings.
    #[arg(
        long,
        default_value_t = std::num::NonZeroUsize::new(remote::DEFAULT_MAX_CONNECTIONS).unwrap(),
        value_name = "N",
        value_parser = common::cli::parse_positive_usize,
        help_heading = "Remote copy options"
    )]
    max_connections: std::num::NonZeroUsize,

    /// Multiplier for pending file writes (default: 4)
    ///
    /// Pending capacity is effective data streams × pending-writes-multiplier.
    #[arg(
        long,
        default_value_t = std::num::NonZeroUsize::new(
            remote::DEFAULT_PENDING_WRITES_MULTIPLIER
        ).unwrap(),
        value_name = "N",
        value_parser = common::cli::parse_positive_usize,
        help_heading = "Remote copy options"
    )]
    pending_writes_multiplier: std::num::NonZeroUsize,

    /// Enable file-based debug logging
    ///
    /// Example: /tmp/rcpd-log creates /tmp/rcpd-log-YYYY-MM-DDTHH-MM-SS-RANDOM
    #[arg(long, value_name = "PREFIX", help_heading = "Remote copy options")]
    debug_log_prefix: Option<String>,

    // Profiling options
    /// Enable Chrome tracing output for profiling (set by rcp master)
    ///
    /// Produces JSON file viewable in Perfetto UI (ui.perfetto.dev) or chrome://tracing.
    #[arg(long, value_name = "PREFIX", help_heading = "Profiling")]
    chrome_trace: Option<String>,

    /// Enable flamegraph output for profiling (set by rcp master)
    ///
    /// Produces folded stack file convertible to SVG with `inferno-flamegraph`.
    #[arg(long, value_name = "PREFIX", help_heading = "Profiling")]
    flamegraph: Option<String>,

    /// Log level for profiling (chrome-trace, flamegraph)
    ///
    /// Controls which spans are captured. Only spans from rcp crates are recorded.
    #[arg(
        long,
        value_name = "LEVEL",
        default_value = "trace",
        help_heading = "Profiling"
    )]
    profile_level: String,

    /// Enable tokio-console for live async debugging
    #[arg(long, help_heading = "Profiling")]
    tokio_console: bool,

    /// Port for tokio-console server
    #[arg(long, value_name = "PORT", help_heading = "Profiling")]
    tokio_console_port: Option<u16>,

    /// Print protocol version information as JSON and exit
    ///
    /// Used by rcp to verify version compatibility before launching remote operations
    #[arg(long)]
    protocol_version: bool,
}

impl Args {
    fn resolve_files_in_flight(&self) -> common::ResolvedFilesInFlight {
        if let Some(value) = self.resolved_automatic_files_in_flight {
            common::ResolvedFilesInFlight::automatic_with(value)
        } else if let Some(value) = self.forwarded_legacy_files_in_flight {
            common::ResolvedFilesInFlight::forwarded_legacy(value)
        } else {
            self.common.resolve_files_in_flight()
        }
    }
    fn resolve_remote_concurrency(
        &self,
        files_in_flight: common::ResolvedFilesInFlight,
    ) -> anyhow::Result<remote::ResolvedRemoteConcurrency> {
        remote::resolve_remote_concurrency(
            files_in_flight.limit(),
            self.max_connections,
            self.pending_writes_multiplier,
        )
    }
    fn throttle_config(
        &self,
        files_in_flight: common::ResolvedFilesInFlight,
        concurrency: remote::ResolvedRemoteConcurrency,
    ) -> common::ThrottleConfig {
        let mut throttle = self
            .common
            .throttle_config(files_in_flight, self.chunk_size);
        if !self.preview_only {
            throttle.admission_policy = common::AdmissionPolicy::Remote {
                streams: concurrency.max_connections(),
                pending: concurrency.max_pending_files(),
            };
        }
        throttle
    }
    fn validate_preview_mode(&self, hello: &remote::protocol::MasterHello) -> anyhow::Result<()> {
        let preview = match hello {
            remote::protocol::MasterHello::Source { dry_run, .. } => dry_run.is_some(),
            remote::protocol::MasterHello::Destination { dry_run, .. } => *dry_run,
        };
        anyhow::ensure!(
            preview == self.preview_only,
            "master hello preview mode does not match daemon startup admission"
        );
        Ok(())
    }
    /// Build the remote TCP config from CLI args. Shared by the listener setup in `async_main`
    /// and the source/destination operations in `run_operation`.
    fn to_tcp_config(&self) -> remote::TcpConfig {
        remote::TcpConfig {
            port_ranges: self.port_ranges.clone(),
            conn_timeout_sec: self.remote_copy_conn_timeout_sec,
            network_profile: self.network_profile,
            buffer_size: self.buffer_size,
            buffer_retention_limit: self.buffer_retention_limit,
            keepalive_sec: self.remote_keepalive_sec,
        }
    }
    /// Build the copy settings shared by the source and destination arms. `filter`/`dry_run` come
    /// from the `MasterHello`; the destination passes `None` for both (filtering happens at source).
    fn to_copy_settings(
        &self,
        filter: Option<common::filter::FilterSettings>,
        dry_run: Option<common::config::DryRunMode>,
        tcp_config: &remote::TcpConfig,
    ) -> anyhow::Result<common::copy::Settings> {
        Ok(common::copy::Settings {
            reflink: Default::default(),
            dereference: self.dereference,
            fail_early: self.fail_early,
            overwrite: self.overwrite,
            overwrite_compare: common::parse_metadata_cmp_settings(&self.overwrite_compare)?,
            overwrite_filter: self.overwrite_filter,
            ignore_existing: self.ignore_existing,
            chunk_size: self.chunk_size,
            skip_specials: self.skip_specials,
            remote_copy_buffer_size: tcp_config.effective_buffer_size(),
            filter,
            dry_run,
            delete: None,
        })
    }
}

/// monitor stdin for EOF to detect master disconnection
/// when SSH connection dies, stdin is closed and we should exit immediately
async fn stdin_monitor() {
    let mut stdin = tokio::io::stdin();
    let mut buf = [0u8; 1];
    loop {
        match stdin.read(&mut buf).await {
            Ok(0) => {
                // EOF - stdin closed, master disconnected
                tracing::warn!(
                    "stdin closed (EOF), master (rcp) connection lost - initiating shutdown"
                );
                return;
            }
            Ok(_) => {
                // ignore any data sent to stdin
            }
            Err(e) => {
                // distinguish between transient and permanent errors
                match e.kind() {
                    std::io::ErrorKind::Interrupted => {
                        // signal interrupted the read, retry
                        tracing::debug!("stdin read interrupted by signal, retrying");
                        continue;
                    }
                    std::io::ErrorKind::WouldBlock => {
                        // resource temporarily unavailable, retry
                        tracing::debug!("stdin read would block, retrying");
                        continue;
                    }
                    _ => {
                        // other errors are likely permanent - treat as disconnect
                        tracing::warn!("stdin read error ({:#}), treating as master disconnect", e);
                        return;
                    }
                }
            }
        }
    }
}

/// Map an operation's outcome to the `RcpdResult` sent to the master, extracting the real
/// summary from a `common::copy::Error` when one is carried.
fn rcpd_result_from(
    result: anyhow::Result<(String, common::copy::Summary)>,
) -> remote::protocol::RcpdResult {
    let runtime_stats = common::collect_runtime_stats();
    match result {
        Ok((message, summary)) => remote::protocol::RcpdResult::Success {
            message,
            summary,
            runtime_stats,
        },
        Err(error) => {
            // try to extract the real summary from common::copy::Error
            let (error_msg, summary) = match error.downcast::<common::copy::Error>() {
                Ok(copy_error) => (format!("{:#}", copy_error.source), copy_error.summary),
                Err(other_error) => (format!("{other_error:#}"), common::copy::Summary::default()),
            };
            remote::protocol::RcpdResult::Failure {
                error: error_msg,
                summary,
                runtime_stats,
            }
        }
    }
}

/// async operation for rcpd - runs the actual source or destination logic
///
/// `result_committed` is flipped true the moment the final `RcpdResult` has been sent to the
/// master — the outer stdin watchdog consults it so a master that consumes the result and then
/// closes the SSH channel (stdin EOF) is not misread as a mid-operation disconnect.
#[allow(clippy::too_many_arguments)]
async fn run_operation<W, R>(
    args: Args,
    tcp_config: &remote::TcpConfig,
    concurrency: remote::ResolvedRemoteConcurrency,
    admission: common::EndpointAdmission,
    files_source: common::FilesInFlightSource,
    master_send_stream: remote::streams::SendStream<W>,
    mut master_recv_stream: remote::streams::RecvStream<R>,
    cert_key: Option<remote::tls::CertifiedKey>,
    result_committed: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<remote::protocol::RcpdResult>
where
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    // run source or destination
    let master_hello = master_recv_stream
        .recv_object::<remote::protocol::MasterHello>()
        .await
        .context("Failed to receive hello message from master")?
        // a clean EOF here means the master went away before saying which role we play (it may
        // have failed between spawning us and sending our hello); report it rather than abort.
        .ok_or_else(|| {
            anyhow::anyhow!("master closed the control connection before sending its hello")
        })?;
    tracing::info!("Received side: {:?}", master_hello);
    args.validate_preview_mode(&master_hello)?;
    // The master sends NOTHING further on this connection after `MasterHello` (see
    // docs/remote_protocol.md §2.1: it holds the connection open to await our `RcpdResult`), so
    // the only signals this read can produce are an EOF — the master exited, finished or killed —
    // and a transport error, which is how the keepalive + TCP_USER_TIMEOUT configured on this
    // accepted socket surface a VANISHED master host. Keeping a reader on the socket is
    // load-bearing, not tidiness: the stdin watchdog in `async_main` fires only when SSH itself
    // notices the death, which a network partition can delay far beyond `--remote-keepalive-sec`
    // (SSH has its own, often slower, liveness settings) — and when stdin is unavailable it never
    // fires at all. Without this reader the kernel marks the socket dead and nothing observes it:
    // both rcpds keep copying to each other, or waiting on each other, indefinitely. The
    // `select!`s below cancel the operation the moment this fires; dropping the operation future
    // aborts its owned task sets. `run_with_stdin_watchdog`'s surviving task scope waits for tracked
    // source and destination descendants to release their captures before daemon teardown
    let master_watchdog = async move {
        loop {
            match master_recv_stream
                .recv_object::<remote::protocol::MasterHello>()
                .await
            {
                // Any frame after the hello is a protocol violation. One that happens to decode
                // as a MasterHello is logged and ignored; anything ELSE fails the decode and is
                // treated as a failed connection by the arm below — deliberately, not as a gap:
                // binaries are version-matched (remote_copy.md), so no legitimate master sends
                // post-hello traffic, and an undecodable frame means a confused or corrupted
                // peer, which is exactly what this watchdog exists to stop.
                Ok(Some(unexpected)) => tracing::warn!(
                    "ignoring an unexpected message from the master after its hello: {unexpected:?}"
                ),
                Ok(None) => return "master closed its control connection".to_string(),
                Err(error) => return format!("master control connection failed: {error:#}"),
            }
        }
    };
    tokio::pin!(master_watchdog);
    let master_vanished = |cause: String| {
        tracing::error!(
            "Master (rcp) control connection lost mid-operation - {cause}. This usually means \
             the master process was killed or its host became unreachable. Shutting down."
        );
        remote::protocol::RcpdResult::Failure {
            error: format!("master control connection lost: {cause}"),
            summary: common::copy::Summary::default(),
            runtime_stats: common::collect_runtime_stats(),
        }
    };
    let rcpd_result = match master_hello {
        remote::protocol::MasterHello::Source {
            src,
            dst,
            dest_cert_fingerprint,
            filter,
            dry_run,
            capture,
        } => {
            // build settings with filter from MasterHello
            let settings = args.to_copy_settings(filter, dry_run, tcp_config)?;
            tracing::info!("Starting source");
            let shared_send = std::sync::Arc::new(tokio::sync::Mutex::new(master_send_stream));
            let operation = async {
                let result = rcpd_result_from(
                    source::run_source(
                        shared_send.clone(),
                        &src,
                        &dst,
                        &settings,
                        capture,
                        tcp_config,
                        concurrency,
                        admission,
                        files_source,
                        args.bind_ip.as_deref(),
                        cert_key.as_ref(),
                        dest_cert_fingerprint,
                    )
                    .await,
                );
                // send the result back to master — once this returns it is COMMITTED: the
                // master can consume it regardless of when the close below lands
                shared_send
                    .lock()
                    .await
                    .send_control_message(&result)
                    .await?;
                result_committed.store(true, std::sync::atomic::Ordering::SeqCst);
                anyhow::Ok(result)
            };
            // no result-send on the watchdog branch: the master is gone, so there is no one to
            // send to — attempting it would only replace the real cause with a send error
            let (rcpd_result, committed) = tokio::select! {
                // biased: the operation branch is POLLED first, so whenever both branches are
                // ready in the same poll the finished operation wins. The watchdog guards the
                // COPY and the result send ONLY — the select ends the moment the result is
                // committed, so the master consuming it and closing its side (EOF on the
                // watchdog's reader) can no longer be misread as a master loss and fail a
                // successful copy. An EOF that genuinely precedes the committed result IS a
                // master loss and is reported.
                biased;
                result = operation => (result?, true),
                cause = &mut master_watchdog => (master_vanished(cause), false),
            };
            if committed {
                // best-effort close, OUTSIDE the watchdog's race: the master may already have
                // consumed the result and closed the connection, and a close error carries no
                // information the master does not already hold
                if let Err(error) = shared_send.lock().await.close().await {
                    tracing::debug!(
                        "closing the master control stream after the result: {error:#}"
                    );
                }
            }
            rcpd_result
        }
        remote::protocol::MasterHello::Destination {
            source_control_addr,
            source_data_addr,
            server_name,
            preserve,
            source_cert_fingerprint,
            dry_run,
        } => {
            // destination doesn't use filter (filtering happens at source).
            // empty directory cleanup decisions are communicated per-directory
            // via keep_if_empty in the Directory message.
            let settings = args.to_copy_settings(None, None, tcp_config)?;
            tracing::info!("Starting destination");
            // same Arc<Mutex> shape as the source arm: the best-effort close runs AFTER the
            // select, so the stream must outlive the (dropped) operation future
            let shared_send = std::sync::Arc::new(tokio::sync::Mutex::new(master_send_stream));
            let operation = async {
                let result = rcpd_result_from(
                    destination::run_destination(
                        &source_control_addr,
                        &source_data_addr,
                        &server_name,
                        &settings,
                        dry_run,
                        args.overwrite_manifest_max_entries,
                        &preserve,
                        tcp_config,
                        concurrency,
                        admission,
                        cert_key.as_ref(),
                        source_cert_fingerprint,
                    )
                    .await,
                );
                // send the result back to master — once this returns it is COMMITTED, as above
                shared_send
                    .lock()
                    .await
                    .send_control_message(&result)
                    .await?;
                result_committed.store(true, std::sync::atomic::Ordering::SeqCst);
                anyhow::Ok(result)
            };
            // no result-send on the watchdog branch, as above: the master is gone
            let (rcpd_result, committed) = tokio::select! {
                // biased, and the watchdog guards only through the result commit — see the
                // source arm for the full rationale
                biased;
                result = operation => (result?, true),
                cause = &mut master_watchdog => (master_vanished(cause), false),
            };
            if committed {
                // best-effort close outside the watchdog's race, as above
                if let Err(error) = shared_send.lock().await.close().await {
                    tracing::debug!(
                        "closing the master control stream after the result: {error:#}"
                    );
                }
            }
            rcpd_result
        }
    };
    Ok(rcpd_result)
}

async fn run_with_stdin_watchdog<Operation, Watchdog>(
    operation: Operation,
    watchdog: Option<Watchdog>,
    result_committed: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> remote::protocol::RcpdResult
where
    Operation: std::future::Future<Output = anyhow::Result<remote::protocol::RcpdResult>>,
    Watchdog: std::future::Future<Output = ()>,
{
    // watchdog cancellation drops the losing operation inside this surviving scope, so aborted
    // tracked descendants release their captures before daemon teardown can finish
    common::task_scope::scope_tasks(async move {
        let map_result = |result: anyhow::Result<remote::protocol::RcpdResult>| match result {
            Ok(result) => result,
            Err(error) => remote::protocol::RcpdResult::Failure {
                error: format!("{error:#}"),
                summary: common::copy::Summary::default(),
                runtime_stats: common::collect_runtime_stats(),
            },
        };
        if let Some(watchdog) = watchdog {
            tokio::pin!(operation);
            tokio::select! {
                biased;
                result = &mut operation => map_result(result),
                () = watchdog => {
                    if result_committed.load(std::sync::atomic::Ordering::SeqCst) {
                        // stdin EOF after result commitment belongs to normal master shutdown
                        map_result(operation.await)
                    } else {
                        tracing::error!("Master (rcp) disconnected - stdin closed. Shutting down.");
                        remote::protocol::RcpdResult::Failure {
                            error: "master (rcp) disconnected - stdin closed".to_string(),
                            summary: common::copy::Summary::default(),
                            runtime_stats: common::collect_runtime_stats(),
                        }
                    }
                }
            }
        } else {
            map_result(operation.await)
        }
    })
    .await
}

#[instrument]
async fn async_main(
    args: Args,
    tracing_receiver: tokio::sync::mpsc::UnboundedReceiver<common::remote_tracing::TracingMessage>,
    files_in_flight: common::ResolvedFilesInFlight,
    concurrency: remote::ResolvedRemoteConcurrency,
    admission: common::EndpointAdmission,
) -> anyhow::Result<String> {
    // install rustls crypto provider (ring) before any TLS operations
    if !args.no_encryption {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok(); // ignore if already installed
    }
    // build TCP config for listener creation
    let tcp_config = args.to_tcp_config();
    tracing::info!(
        "Effective remote connection count: {}",
        concurrency.max_connections()
    );
    // generate TLS certificate and create server config (if encryption enabled)
    let (cert_key, tls_acceptor) = if !args.no_encryption {
        let cert_key = remote::tls::generate_self_signed_cert()
            .context("failed to generate TLS certificate")?;
        // if master fingerprint provided, require client authentication
        let server_config = if let Some(ref fp_hex) = args.master_cert_fp {
            let master_fingerprint = remote::tls::fingerprint_from_hex(fp_hex)
                .context("invalid master certificate fingerprint")?;
            remote::tls::create_server_config_with_client_auth(&cert_key, master_fingerprint)
                .context("failed to create TLS server config with client auth")?
        } else {
            // encryption enabled but no master fingerprint - this is a security risk
            anyhow::bail!(
                "TLS encryption is enabled but --master-cert-fp was not provided. \
                 This would allow any client to connect. Either provide --master-cert-fp \
                 or use --no-encryption for trusted networks."
            );
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
        (Some(cert_key), Some(acceptor))
    } else {
        (None, None)
    };
    // create TCP listener for master connections
    let listener =
        remote::create_tcp_control_listener(&tcp_config, args.bind_ip.as_deref()).await?;
    let listen_addr = remote::get_tcp_listener_addr(&listener, args.bind_ip.as_deref())?;
    // output connection info to stderr (read by master via SSH)
    // we use stderr because stdout is reserved for logs per project convention
    // (rcpd doesn't display progress bars locally - it sends progress data over the network)
    // format: "RCP_TLS <addr> <fingerprint> <F> <E>" or "RCP_TCP <addr> <F> <E>"
    eprintln!(
        "{}",
        remote::format_rcpd_readiness(
            listen_addr,
            cert_key.as_ref().map(|cert| &cert.fingerprint),
            concurrency,
        )
    );
    // flush stderr to ensure master receives the line immediately
    use std::io::Write;
    std::io::stderr()
        .flush()
        .context("failed to flush stderr")?;
    tracing::info!("Listening for master connections on {}", listen_addr);
    // helper to accept a connection and optionally wrap with TLS
    //
    // BOTH the TCP accept and the TLS handshake are bounded. These accepts are sequential and the
    // handshake runs inline, so a peer that completes TCP and then sends no TLS bytes would
    // otherwise block the legitimate master from ever connecting, leaving an orphaned rcpd holding
    // the port. The handshake bound lives in `remote::tls::accept_bounded`.
    async fn accept_connection(
        listener: &tokio::net::TcpListener,
        tls_acceptor: Option<&tokio_rustls::TlsAcceptor>,
        tcp_config: &remote::TcpConfig,
        purpose: &str,
    ) -> anyhow::Result<(
        remote::streams::BoxedSendStream,
        remote::streams::BoxedRecvStream,
    )> {
        let timeout = std::time::Duration::from_secs(tcp_config.conn_timeout_sec);
        // the accept helper applies the Control socket options before returning
        let (stream, addr) =
            tokio::time::timeout(timeout, remote::accept_tcp_control(listener, tcp_config))
                .await
                .with_context(|| format!("timeout waiting for master {} connection", purpose))?
                .with_context(|| format!("failed to accept {} connection", purpose))?;
        tracing::info!("Accepted {} connection from {}", purpose, addr);
        remote::tls::accept_bounded(tls_acceptor, stream, timeout, purpose).await
    }
    // accept control connection (TCP + TLS handshake, both bounded)
    let (master_send_stream, master_recv_stream) =
        accept_connection(&listener, tls_acceptor.as_ref(), &tcp_config, "control").await?;
    // accept tracing connection (TCP + TLS handshake, both bounded)
    let (tracing_send_stream, _tracing_recv_stream) =
        accept_connection(&listener, tls_acceptor.as_ref(), &tcp_config, "tracing").await?;
    tracing::info!(
        "Master connections established (encryption={})",
        !args.no_encryption
    );
    // spawn tracing sender task to forward progress/logs to master
    let tracing_cancel = tokio_util::sync::CancellationToken::new();
    let tracing_task = {
        let cancel = tracing_cancel.clone();
        tokio::spawn(async move {
            if let Err(e) =
                remote::tracelog::run_sender(tracing_receiver, tracing_send_stream, cancel).await
            {
                tracing::warn!("Tracing sender failed: {e:#}");
            }
        })
    };
    // check if stdin is available for monitoring
    // SSH with -T closes stdin immediately, so we only monitor if it's actually open
    let stdin_available = {
        let mut stdin = tokio::io::stdin();
        let mut buf = [0u8; 1];
        // try a non-blocking peek - if stdin is EOF immediately, don't monitor it
        match tokio::time::timeout(std::time::Duration::from_millis(1), stdin.read(&mut buf)).await
        {
            Ok(Ok(0)) => false,  // EOF - stdin closed
            Ok(Ok(_)) => true,   // has data - stdin open
            Ok(Err(_)) => false, // error - treat as closed
            Err(_) => true,      // timeout - stdin open (waiting for data)
        }
    };
    tracing::debug!(
        "stdin monitoring: {}",
        if stdin_available {
            "enabled"
        } else {
            "disabled (stdin closed)"
        }
    );
    let result_committed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let operation = run_operation(
        args.clone(),
        &tcp_config,
        concurrency,
        admission,
        files_in_flight.source(),
        master_send_stream,
        master_recv_stream,
        cert_key.clone(),
        result_committed.clone(),
    );
    let rcpd_result = run_with_stdin_watchdog(
        operation,
        stdin_available.then(stdin_monitor),
        result_committed,
    )
    .await;
    // cancel tracing task and wait for it to finish — BOUNDED: the sender polls its cancellation
    // token only between messages, so one suspended mid-send on a partitioned (or
    // stopped-reading) master would otherwise hold this await for a whole transport timeout
    // (another keepalive budget, or the kernel's ~15min retransmission limit with keepalive
    // disabled), defeating the watchdog's prompt teardown. On the ordinary path the final flush
    // completes in milliseconds; a sender that cannot flush within the bound has no one left
    // reading it — stop waiting and let process exit reclaim the socket.
    tracing_cancel.cancel();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), tracing_task).await;
    match rcpd_result {
        remote::protocol::RcpdResult::Success {
            message,
            summary: _,
            runtime_stats: _,
        } => Ok(message),
        remote::protocol::RcpdResult::Failure {
            error,
            summary: _,
            runtime_stats: _,
        } => {
            // rcp-error-log-allow: RcpdResult::Failure.error is a String off the wire, not a chain
            tracing::error!("rcpd operation failed: {error}");
            Err(anyhow::anyhow!("rcpd operation failed: {error}"))
        }
    }
}

fn exit_with_startup_refusal(diagnostic: &str, code: i32) -> ! {
    eprintln!(
        "{}",
        common::format_startup_diagnostic(Some(remote::RCPD_STARTUP_ERROR_PREFIX), diagnostic)
    );
    std::process::exit(code);
}

fn main() -> Result<(), anyhow::Error> {
    // handle --protocol-version flag before parsing full arguments
    // this allows it to work without required arguments
    // respect -- separator: only check args before -- to allow files named --protocol-version
    let args: Vec<String> = std::env::args().collect();
    let separator_pos = args.iter().position(|arg| arg == "--");
    let args_to_check = if let Some(pos) = separator_pos {
        &args[..pos]
    } else {
        &args[..]
    };
    if args_to_check.iter().any(|arg| arg == "--protocol-version") {
        let version = common::version::ProtocolVersion::current();
        let json = version.to_json()?;
        println!("{}", json);
        return Ok(());
    }

    let args = match Args::try_parse() {
        Ok(args) => args,
        Err(error) if error.use_stderr() => {
            exit_with_startup_refusal(&error.to_string(), error.exit_code());
        }
        Err(error) => error.exit(),
    };
    let files_in_flight = args.resolve_files_in_flight();
    let concurrency = match args.resolve_remote_concurrency(files_in_flight) {
        Ok(concurrency) => concurrency,
        Err(error) => exit_with_startup_refusal(&format!("{error:#}"), 1),
    };
    // TOCTOU linter: arms strict operand resolution when the master passed
    // --require-toctou-safe, and fail-closes on this host if the invocation
    // cannot be hardened (e.g. -L, or a pre-openat2 kernel). Operands arrive
    // via the master — which already linted their strict form — so none are
    // passed here. Unlike the tools' `enforce_or_exit` (stdout), a refusal is
    // printed to STDERR as a typed single-line refusal: the master reads rcpd's
    // first stderr line for the startup handshake, so this is what surfaces in its error —
    // on stdout the reason would drown in the log drain and the user would only
    // see a generic handshake failure.
    match common::toctou_check::run_linter(args.dereference, false, args.require_toctou_safe, &[]) {
        common::toctou_check::LinterAction::Exit { output, code } => {
            exit_with_startup_refusal(&output, code);
        }
        common::toctou_check::LinterAction::Proceed => {}
    }
    let (tracing_layer, tracing_sender, tracing_receiver) =
        common::remote_tracing::RemoteTracingLayer::new();
    let func = {
        let args = args.clone();
        |admission| {
            async_main(
                args,
                tracing_receiver,
                files_in_flight,
                concurrency,
                admission,
            )
        }
    };
    let debug_log_file = args.debug_log_prefix.as_ref().map(|prefix| {
        let filename = common::generate_debug_log_filename(prefix);
        println!("rcpd: Debug logging to file: {filename}");
        filename
    });
    // rcpd never prints a user-facing summary (results stream to master).
    let mut output = args.common.output_config(args.quiet, false);
    output.startup_error_prefix = Some(remote::RCPD_STARTUP_ERROR_PREFIX);
    let runtime = args.common.runtime_config();
    let throttle = args.throttle_config(files_in_flight, concurrency);
    let tracing = common::TracingConfig {
        remote_layer: Some(tracing_layer),
        debug_log_file,
        chrome_trace_prefix: args.chrome_trace.clone(),
        timings_prefix: args.common.timings.clone(),
        timings_detail: args.common.timings_detail,
        flamegraph_prefix: args.flamegraph.clone(),
        trace_identifier: format!("rcpd-{}", args.role),
        profile_level: Some(args.profile_level.clone()),
        tokio_console: args.tokio_console,
        tokio_console_port: args.tokio_console_port,
    };
    // rcpd's progress is always Remote (streamed to master), regardless of
    // --progress-type — that flag is ignored on this binary. The master
    // controls progress mode by setting --progress (and optionally
    // --progress-delay) together; --progress-delay alone does not enable
    // remote progress here because the master never sends it without
    // --progress (see RcpdConfig::to_args). This intentionally diverges from
    // CommonArgs's "--progress-delay implies --progress" doc, which targets
    // user-facing tools.
    let progress = if args.common.progress {
        Some(common::ProgressSettings {
            progress_type: common::GeneralProgressType::Remote(tracing_sender),
            progress_delay: args.common.progress_delay,
        })
    } else {
        None
    };
    let res = common::run(progress, output, runtime, throttle, tracing, func);
    if res.is_none() {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct HeldCapture(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for HeldCapture {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    async fn operation_holding_children(
        dropped: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        cancel: tokio::sync::oneshot::Sender<()>,
    ) -> anyhow::Result<remote::protocol::RcpdResult> {
        let mut children = tokio::task::JoinSet::new();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let started_capture = HeldCapture(dropped.clone());
        common::task_scope::spawn_tracked(&mut children, async move {
            let _capture = started_capture;
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.unwrap();
        let unpolled_capture = HeldCapture(dropped);
        common::task_scope::spawn_tracked(&mut children, async move {
            let _capture = unpolled_capture;
            std::future::pending::<()>().await;
        });
        let _ = cancel.send(());
        std::future::pending().await
    }
    #[tokio::test]
    async fn stdin_eof_waits_for_started_and_unpolled_captures() {
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        let operation =
            common::task_scope::scope_tasks(operation_holding_children(dropped.clone(), cancel));
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_with_stdin_watchdog(
                operation,
                Some(async move {
                    let _ = cancelled.await;
                }),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            ),
        )
        .await
        .expect("stdin cancellation must drain child captures");
        assert!(
            matches!(result, remote::protocol::RcpdResult::Failure { error, .. } if error.contains("stdin closed"))
        );
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn master_disconnect_waits_for_started_and_unpolled_captures() {
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        let work =
            common::task_scope::scope_tasks(operation_holding_children(dropped.clone(), cancel));
        let operation = async move {
            tokio::select! {
                biased;
                result = work => result,
                _ = cancelled => Err(anyhow::anyhow!("master control connection lost")),
            }
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_with_stdin_watchdog(
                operation,
                None::<std::future::Pending<()>>,
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            ),
        )
        .await
        .expect("master cancellation must drain child captures");
        assert!(
            matches!(result, remote::protocol::RcpdResult::Failure { error, .. } if error == "master control connection lost")
        );
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn committed_result_survives_stdin_eof_during_cleanup() {
        let committed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mark_committed = committed.clone();
        let operation = async move {
            mark_committed.store(true, std::sync::atomic::Ordering::SeqCst);
            tokio::task::yield_now().await;
            Ok(remote::protocol::RcpdResult::Success {
                message: "copy complete".to_string(),
                summary: common::copy::Summary::default(),
                runtime_stats: common::collect_runtime_stats(),
            })
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_with_stdin_watchdog(operation, Some(std::future::ready(())), committed),
        )
        .await
        .expect("committed operation must finish cleanup");
        assert!(matches!(
            result,
            remote::protocol::RcpdResult::Success { .. }
        ));
    }

    fn daemon_args(extra: &[&str]) -> Args {
        let mut argv = vec!["rcpd", "--role=source"];
        argv.extend_from_slice(extra);
        Args::try_parse_from(argv).unwrap()
    }

    #[test]
    fn daemon_resolves_buffer_retention_without_changing_copy_chunks() {
        for role in ["--role=source", "--role=destination"] {
            for (extra, chunk, limit) in [
                (vec![], 16 * 1024 * 1024, 2 * 1024 * 1024),
                (
                    vec!["--network-profile=internet"],
                    2 * 1024 * 1024,
                    2 * 1024 * 1024,
                ),
                (vec!["--buffer-size=12345"], 12345, 12345),
                (
                    vec!["--buffer-size=33554432"],
                    32 * 1024 * 1024,
                    2 * 1024 * 1024,
                ),
                (
                    vec!["--buffer-retention-limit=16777216"],
                    16 * 1024 * 1024,
                    16 * 1024 * 1024,
                ),
                (
                    vec!["--buffer-size=12345", "--buffer-retention-limit=8192"],
                    12345,
                    8192,
                ),
                (vec!["--buffer-retention-limit=0"], 16 * 1024 * 1024, 0),
                (
                    vec!["--buffer-size=12345", "--buffer-retention-limit=32768"],
                    12345,
                    32768,
                ),
            ] {
                let mut argv = vec!["rcpd", role];
                argv.extend(extra);
                let args = Args::try_parse_from(argv).unwrap();
                let tcp = args.to_tcp_config();
                assert_eq!(tcp.effective_buffer_size(), chunk);
                assert_eq!(tcp.effective_buffer_retention_limit(), limit);
                assert_eq!(
                    args.to_copy_settings(None, None, &tcp)
                        .unwrap()
                        .remote_copy_buffer_size,
                    chunk
                );
            }
        }
    }

    #[test]
    fn daemon_rejects_invalid_buffer_retention_limits() {
        for value in ["-1", "nonsense", "18446744073709551616"] {
            let error = Args::try_parse_from([
                "rcpd",
                "--role=destination",
                &format!("--buffer-retention-limit={value}"),
            ])
            .unwrap_err();
            assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
            assert!(error.to_string().contains("--buffer-retention-limit"));
        }
    }

    #[test]
    fn preview_startup_avoids_reserving_copy_streams_on_both_roles() {
        for role in ["--role=source", "--role=destination"] {
            for preview in [false, true] {
                let mut argv = vec![
                    "rcpd",
                    role,
                    "--max-files-in-flight=200",
                    "--max-connections=100",
                ];
                if preview {
                    argv.push("--preview-only");
                }
                let args = Args::try_parse_from(argv).unwrap();
                let files = args.resolve_files_in_flight();
                let concurrency = args.resolve_remote_concurrency(files).unwrap();
                let throttle = args.throttle_config(files, concurrency);
                assert_eq!(
                    throttle.admission_policy,
                    if preview {
                        common::AdmissionPolicy::Local
                    } else {
                        common::AdmissionPolicy::Remote {
                            streams: std::num::NonZeroUsize::new(100).unwrap(),
                            pending: std::num::NonZeroUsize::new(400).unwrap(),
                        }
                    },
                );
            }
        }
    }

    #[test]
    fn hello_preview_mode_must_match_startup_admission() {
        for preview in [false, true] {
            let args = daemon_args(if preview { &["--preview-only"] } else { &[] });
            for dry_run in [false, true] {
                let source = remote::protocol::MasterHello::Source {
                    src: "/source".into(),
                    dst: "/destination".into(),
                    dest_cert_fingerprint: None,
                    filter: None,
                    dry_run: dry_run.then_some(common::DryRunMode::Brief),
                    capture: Default::default(),
                };
                let destination = remote::protocol::MasterHello::Destination {
                    source_control_addr: "127.0.0.1:10001".parse().unwrap(),
                    source_data_addr: "127.0.0.1:10002".parse().unwrap(),
                    server_name: "source".to_owned(),
                    preserve: Default::default(),
                    source_cert_fingerprint: None,
                    dry_run,
                };
                for hello in [&source, &destination] {
                    assert_eq!(
                        args.validate_preview_mode(hello).is_ok(),
                        preview == dry_run
                    );
                }
            }
        }
    }

    #[test]
    fn direct_daemon_uses_automatic_file_limit_for_connections() {
        let args = daemon_args(&[]);
        let files_in_flight = args.resolve_files_in_flight();
        assert_eq!(
            files_in_flight.source(),
            common::FilesInFlightSource::Automatic
        );
        let expected = match files_in_flight.limit() {
            common::ConcurrencyLimit::Limited(value) => value.get().min(args.max_connections.get()),
            common::ConcurrencyLimit::Unlimited => panic!("automatic policy must be finite"),
        };
        let concurrency = args.resolve_remote_concurrency(files_in_flight).unwrap();
        assert_eq!(concurrency.max_connections().get(), expected);
    }

    #[test]
    fn resolved_automatic_override_changes_only_file_limit_provenance() {
        let direct = daemon_args(&["--max-files-in-flight=7"]);
        let resolved_automatic = daemon_args(&["--resolved-automatic-files-in-flight=7"]);
        let direct_limit = direct.resolve_files_in_flight();
        let automatic_limit = resolved_automatic.resolve_files_in_flight();
        assert_eq!(direct_limit.limit(), automatic_limit.limit());
        assert_eq!(direct_limit.source(), common::FilesInFlightSource::Explicit);
        assert_eq!(
            automatic_limit.source(),
            common::FilesInFlightSource::Automatic
        );
        let direct_concurrency = direct.resolve_remote_concurrency(direct_limit).unwrap();
        let automatic_concurrency = resolved_automatic
            .resolve_remote_concurrency(automatic_limit)
            .unwrap();
        assert_eq!(
            direct_concurrency.max_connections(),
            automatic_concurrency.max_connections()
        );
    }

    #[test]
    fn direct_daemon_accepts_explicit_unlimited_file_admission() {
        let args = daemon_args(&["--max-files-in-flight=unlimited"]);
        let files_in_flight = args.resolve_files_in_flight();
        assert_eq!(files_in_flight.limit(), common::ConcurrencyLimit::Unlimited);
        assert_eq!(
            files_in_flight.source(),
            common::FilesInFlightSource::Explicit
        );
        let throttle = args
            .common
            .throttle_config(files_in_flight, args.chunk_size);
        assert_eq!(throttle.deprecated_max_open_files_warning(), None);
    }

    #[test]
    fn forwarded_legacy_finite_retains_provenance_without_a_duplicate_warning() {
        let args = daemon_args(&["--forwarded-legacy-files-in-flight=7"]);
        let files_in_flight = args.resolve_files_in_flight();
        assert_eq!(
            files_in_flight.limit(),
            common::ConcurrencyLimit::Limited(std::num::NonZeroUsize::new(7).unwrap())
        );
        assert_eq!(
            files_in_flight.source(),
            common::FilesInFlightSource::DeprecatedMaxOpenFiles
        );
        let throttle = args
            .common
            .throttle_config(files_in_flight, args.chunk_size);
        assert_eq!(throttle.deprecated_max_open_files_warning(), None);
    }

    #[test]
    fn forwarded_legacy_unlimited_retains_provenance_without_a_duplicate_warning() {
        let args = daemon_args(&["--forwarded-legacy-files-in-flight=0"]);
        let files_in_flight = args.resolve_files_in_flight();
        assert_eq!(files_in_flight.limit(), common::ConcurrencyLimit::Unlimited);
        assert_eq!(
            files_in_flight.source(),
            common::FilesInFlightSource::DeprecatedMaxOpenFiles
        );
        let throttle = args
            .common
            .throttle_config(files_in_flight, args.chunk_size);
        assert_eq!(throttle.deprecated_max_open_files_warning(), None);
    }
}
