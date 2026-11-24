use anyhow::Context;
use clap::Parser;
use tracing::instrument;

use rcp_tools_rcp::{destination, source};

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

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

    /// Disable TLS encryption and authentication for source↔destination TCP connections
    ///
    /// WARNING: Disables both encryption and authentication on source↔destination TCP connections.
    /// Master↔rcpd control and tracing continue to run over SSH. Data is sent in plaintext and
    /// TCP connections are accepted from anyone. Only use on isolated, trusted networks.
    #[arg(long, help_heading = "Remote copy options")]
    no_encryption: bool,

    /// Connection timeout for remote copy operations in seconds
    ///
    /// Applies to: destination→source connection and TLS handshake. Must be at least 1.
    #[arg(
        long,
        default_value = "15",
        value_name = "N",
        value_parser = common::cli::parse_positive_u64,
        help_heading = "Remote copy options"
    )]
    remote_copy_conn_timeout_sec: u64,

    /// Liveness budget for remote connections in seconds, 0 disables (set by rcp master)
    ///
    /// Mirrors the master's --remote-keepalive-sec. Applies to source↔destination TCP connections
    /// (keepalive on every connection; TCP_USER_TIMEOUT only on control connections — see
    /// remote::configure_tcp_socket) and to master↔rcpd SSH stdin heartbeat timeouts.
    #[arg(
        long,
        value_name = "N",
        default_value_t = remote::DEFAULT_REMOTE_KEEPALIVE_SEC,
        help_heading = "Remote copy options"
    )]
    remote_keepalive_sec: u64,

    /// Network profile for TCP tuning
    ///
    /// A profile selects one buffer size (16 MiB for datacenter, 2 MiB for internet): the TCP
    /// socket buffer size wanted on data connections, requested from the kernel only where that
    /// yields more than its own sizing, and the default for the copy buffer and for the buffer
    /// kept between files.
    #[arg(
        long,
        default_value = "datacenter",
        value_name = "PROFILE",
        help_heading = "Remote copy options"
    )]
    network_profile: remote::NetworkProfile,

    /// Maximum transfer buffer and file I/O request size, in bytes.
    ///
    /// Sets the size of the source's read buffer and of the destination's receive buffer, which
    /// is also the size of its file writes.
    /// The configured value must fit usize on the master and each daemon; otherwise
    /// argument validation or daemon startup fails. Accepted per-file limits are capped
    /// to the file size and the host's isize::MAX, with a one-byte minimum.
    /// These are representability limits, not available-memory checks; oversized settings
    /// can still exhaust memory and abort a daemon. Individual disk/network operations can be shorter;
    /// this does not set TCP packet or TLS record sizes.
    /// Each active transfer holds one buffer of up to this size on each side, so larger limits
    /// allow more working memory. Keeping buffers between files is controlled separately.
    ///
    /// Default: the network profile's buffer size, 16 MiB for datacenter and 2 MiB for internet.
    #[arg(long, value_name = "BYTES", help_heading = "Remote copy options")]
    buffer_size: Option<usize>,

    /// Maximum transfer buffer kept per data connection between files, in bytes.
    ///
    /// The source keeps its read buffer and the destination its receive buffer.
    /// Defaults to min(effective buffer size, the network profile's buffer size). Allocated
    /// lazily; zero disables retention.
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
            local_copy_handoff: Default::default(),
            local_copy_buffer_retention_limit: Default::default(),
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

#[derive(Debug)]
struct KeepaliveIdleTimeout(std::time::Duration);

impl std::fmt::Display for KeepaliveIdleTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "master control channel idle timeout after {}s",
            self.0.as_secs()
        )
    }
}

impl std::error::Error for KeepaliveIdleTimeout {}

fn find_keepalive_idle_timeout(error: &anyhow::Error) -> Option<std::time::Duration> {
    for cause in error.chain() {
        if let Some(timeout) = cause.downcast_ref::<KeepaliveIdleTimeout>() {
            return Some(timeout.0);
        }
        if let Some(io_err) = cause.downcast_ref::<std::io::Error>()
            && let Some(timeout) = io_err
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<KeepaliveIdleTimeout>())
        {
            return Some(timeout.0);
        }
    }
    None
}

/// Async reader adapter that bounds continuous read idleness by `budget`, refreshing the idle
/// deadline whenever new bytes arrive so a slowly-streaming large frame (such as `MasterHello`
/// carrying a large filter file) stays alive as long as read progress continues.
struct IdleTimeoutReader<R> {
    inner: R,
    budget: Option<std::time::Duration>,
    deadline_armed: bool,
    sleep: std::pin::Pin<Box<tokio::time::Sleep>>,
}

impl<R> IdleTimeoutReader<R> {
    fn new(inner: R, keepalive_sec: u64) -> Self {
        Self {
            inner,
            budget: (keepalive_sec > 0).then(|| std::time::Duration::from_secs(keepalive_sec)),
            deadline_armed: false,
            sleep: Box::pin(tokio::time::sleep(std::time::Duration::ZERO)),
        }
    }
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for IdleTimeoutReader<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before_len = buf.filled().len();
        match std::pin::Pin::new(&mut this.inner).poll_read(cx, buf) {
            std::task::Poll::Ready(Ok(())) => {
                if buf.filled().len() > before_len {
                    // forward progress arrived; the next wait gets a fresh idle budget
                    this.deadline_armed = false;
                }
                std::task::Poll::Ready(Ok(()))
            }
            std::task::Poll::Ready(Err(error)) => {
                this.deadline_armed = false;
                std::task::Poll::Ready(Err(error))
            }
            std::task::Poll::Pending => {
                if let Some(budget) = this.budget {
                    if !this.deadline_armed
                        && let Some(deadline) = tokio::time::Instant::now().checked_add(budget)
                    {
                        this.sleep.as_mut().reset(deadline);
                        this.deadline_armed = true;
                    }
                    if this.deadline_armed
                        && std::future::Future::poll(this.sleep.as_mut(), cx).is_ready()
                    {
                        this.deadline_armed = false;
                        return std::task::Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            KeepaliveIdleTimeout(budget),
                        )));
                    }
                }
                std::task::Poll::Pending
            }
        }
    }
}

enum MasterReadOutcome {
    Message(remote::protocol::MasterToRcpd),
    Eof,
    KeepaliveTimedOut(std::time::Duration),
    Error(anyhow::Error),
}

struct MasterStdinStream<R> {
    framed: remote::streams::RecvStream<IdleTimeoutReader<R>>,
}

impl<R: tokio::io::AsyncRead + Unpin> MasterStdinStream<R> {
    fn new(reader: R, keepalive_sec: u64) -> Self {
        Self {
            framed: remote::streams::RecvStream::new(IdleTimeoutReader::new(reader, keepalive_sec)),
        }
    }

    async fn recv_message(&mut self) -> MasterReadOutcome {
        match self
            .framed
            .recv_object::<remote::protocol::MasterToRcpd>()
            .await
        {
            Ok(Some(message)) => MasterReadOutcome::Message(message),
            Ok(None) => MasterReadOutcome::Eof,
            Err(error) => {
                if let Some(budget) = find_keepalive_idle_timeout(&error) {
                    MasterReadOutcome::KeepaliveTimedOut(budget)
                } else {
                    MasterReadOutcome::Error(error)
                }
            }
        }
    }
}

async fn recv_master_hello<R>(
    master_stdin: &mut MasterStdinStream<R>,
) -> anyhow::Result<remote::protocol::MasterHello>
where
    R: tokio::io::AsyncRead + Unpin,
{
    loop {
        match master_stdin.recv_message().await {
            MasterReadOutcome::Message(remote::protocol::MasterToRcpd::Hello(hello)) => {
                return Ok(hello);
            }
            MasterReadOutcome::Message(remote::protocol::MasterToRcpd::Heartbeat) => {
                // master sends periodic heartbeats from spawn while bringing up the peer daemon
                continue;
            }
            MasterReadOutcome::Eof => {
                anyhow::bail!("master closed its stdin control channel before sending its hello");
            }
            MasterReadOutcome::KeepaliveTimedOut(budget) => {
                anyhow::bail!(
                    "master control channel timed out after {}s waiting for hello",
                    budget.as_secs()
                );
            }
            MasterReadOutcome::Error(error) => {
                return Err(error).context("Failed to receive hello message from master");
            }
        }
    }
}

/// Monitor the master's SSH stdin control stream for disconnect or missed heartbeats.
///
/// The master sends periodic [`remote::protocol::MasterToRcpd::Heartbeat`] frames while alive
/// (whenever `keepalive_sec > 0`) and closes stdin when tearing down. Returning ends the active
/// operation.
async fn master_stdin_watchdog<R>(master_stdin: &mut MasterStdinStream<R>) -> String
where
    R: tokio::io::AsyncRead + Unpin,
{
    loop {
        match master_stdin.recv_message().await {
            MasterReadOutcome::Message(remote::protocol::MasterToRcpd::Heartbeat) => continue,
            MasterReadOutcome::Message(remote::protocol::MasterToRcpd::Hello(unexpected)) => {
                tracing::warn!(
                    "ignoring an unexpected hello message from the master after startup: {unexpected:?}"
                );
            }
            MasterReadOutcome::Eof => {
                return "master closed its stdin control channel".to_string();
            }
            MasterReadOutcome::KeepaliveTimedOut(budget) => {
                return format!(
                    "master control channel timed out after {}s without a heartbeat",
                    budget.as_secs()
                );
            }
            MasterReadOutcome::Error(error) => {
                return format!("master control channel failed: {error:#}");
            }
        }
    }
}

/// Outcome of running an `rcpd` operation and attempting to deliver its final `RcpdResult`.
struct OperationOutcome {
    result: remote::protocol::RcpdResult,
    /// True once `result` was acknowledged by the stdout writer, meaning `async_main` should
    /// drain trailing best-effort traces for [`TRACING_DRAIN_GRACE`]. False when the master was
    /// lost or result delivery failed, so `async_main` aborts the stdout writer immediately.
    drain_stdout: bool,
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

/// Async operation for rcpd - runs the actual source or destination logic under the unified
/// master stdin watchdog, then delivers the final `RcpdResult` under the same liveness policy.
#[allow(clippy::too_many_arguments)]
async fn run_operation<R>(
    args: Args,
    tcp_config: &remote::TcpConfig,
    concurrency: remote::ResolvedRemoteConcurrency,
    admission: common::EndpointAdmission,
    files_source: common::FilesInFlightSource,
    master_control: remote::stdio::DaemonControlSender,
    mut master_stdin: MasterStdinStream<R>,
    cert_key: Option<remote::tls::CertifiedKey>,
) -> OperationOutcome
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let master_hello = match recv_master_hello(&mut master_stdin).await {
        Ok(hello) => hello,
        Err(error) => {
            return OperationOutcome {
                result: remote::protocol::RcpdResult::Failure {
                    error: format!("{error:#}"),
                    summary: common::copy::Summary::default(),
                    runtime_stats: common::collect_runtime_stats(),
                },
                drain_stdout: false,
            };
        }
    };
    tracing::info!("Received side: {:?}", master_hello);
    let control_for_source = master_control.clone();
    let operation = async move {
        args.validate_preview_mode(&master_hello)?;
        match master_hello {
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
                source::run_source(
                    &control_for_source,
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
                .await
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
                .await
            }
        }
    };
    run_guarded_operation_and_send_result(operation, &mut master_stdin, &master_control).await
}

enum WatchdogOutcome<T> {
    Completed(T),
    MasterLost(String),
}

async fn run_with_master_watchdog<Operation, Watchdog, T>(
    operation: Operation,
    watchdog: Watchdog,
) -> WatchdogOutcome<T>
where
    Operation: std::future::Future<Output = T>,
    Watchdog: std::future::Future<Output = String>,
{
    // watchdog cancellation drops the losing operation inside this surviving scope, so aborted
    // tracked descendants release their captures before daemon teardown can finish
    common::task_scope::scope_tasks(async move {
        tokio::pin!(operation);
        tokio::select! {
            // biased: the operation branch is polled first, so whenever both branches are ready
            // in the same poll the finished operation wins
            biased;
            result = &mut operation => WatchdogOutcome::Completed(result),
            cause = watchdog => WatchdogOutcome::MasterLost(cause),
        }
    })
    .await
}

/// Brief grace after `stdin` EOF for an already-written `RcpdResult` acknowledgement to arrive.
///
/// On normal completion the master closes daemon `stdin` right after decoding `RcpdResult`, so the
/// writer task has already pushed the frame into stdout and its ack resolves immediately. On a real
/// disconnect stdout breaks with `EPIPE` immediately as well. This grace only bounds a half-closed
/// peer that closes `stdin` while leaving `stdout` open and unread.
const STDIN_EOF_RESULT_ACK_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// Deliver the authoritative `RcpdResult` while monitoring `master_stdin` under the configured
/// liveness policy instead of capping delivery to [`TRACING_DRAIN_GRACE`].
///
/// While the master remains alive (heartbeats keep arriving, or `keepalive_sec == 0`), this waits
/// for stdout backpressure (for example an in-flight trace frame on a slow SSH link) to clear. On
/// disconnect or keepalive timeout it returns `drain_stdout = false` so the caller aborts the
/// stdout writer instead of later flushing an unacknowledged result.
async fn deliver_result<R>(
    master_stdin: &mut MasterStdinStream<R>,
    master_control: &remote::stdio::DaemonControlSender,
    result: remote::protocol::RcpdResult,
) -> OperationOutcome
where
    R: tokio::io::AsyncRead + Unpin,
{
    let send = master_control.send_result(result.clone());
    tokio::pin!(send);
    loop {
        tokio::select! {
            // biased: if the writer already flushed RcpdResult and sent its ack, always report
            // that consistent delivery even if master stdin EOF arrived in the same poll
            biased;
            send_res = &mut send => {
                return match send_res {
                    Ok(()) => OperationOutcome {
                        result,
                        drain_stdout: true,
                    },
                    Err(error) => {
                        tracing::error!("failed to send result to master: {error:#}");
                        OperationOutcome {
                            result: remote::protocol::RcpdResult::Failure {
                                error: format!("failed to send result to master: {error:#}"),
                                summary: common::copy::Summary::default(),
                                runtime_stats: common::collect_runtime_stats(),
                            },
                            drain_stdout: false,
                        }
                    }
                };
            }
            outcome = master_stdin.recv_message() => {
                match outcome {
                    MasterReadOutcome::Message(remote::protocol::MasterToRcpd::Heartbeat) => {
                        continue;
                    }
                    MasterReadOutcome::Message(remote::protocol::MasterToRcpd::Hello(unexpected)) => {
                        tracing::warn!(
                            "ignoring an unexpected hello message from the master while sending result: {unexpected:?}"
                        );
                    }
                    MasterReadOutcome::Eof => {
                        return match tokio::time::timeout(STDIN_EOF_RESULT_ACK_GRACE, &mut send).await {
                            Ok(Ok(())) => OperationOutcome {
                                result,
                                drain_stdout: true,
                            },
                            Ok(Err(error)) => {
                                tracing::error!("failed to send result to master: {error:#}");
                                OperationOutcome {
                                    result: remote::protocol::RcpdResult::Failure {
                                        error: format!("failed to send result to master: {error:#}"),
                                        summary: common::copy::Summary::default(),
                                        runtime_stats: common::collect_runtime_stats(),
                                    },
                                    drain_stdout: false,
                                }
                            }
                            Err(_) => {
                                tracing::error!(
                                    "master closed stdin before result delivery completed"
                                );
                                OperationOutcome {
                                    result: remote::protocol::RcpdResult::Failure {
                                        error: "master closed stdin before result delivery completed"
                                            .to_string(),
                                        summary: common::copy::Summary::default(),
                                        runtime_stats: common::collect_runtime_stats(),
                                    },
                                    drain_stdout: false,
                                }
                            }
                        };
                    }
                    MasterReadOutcome::KeepaliveTimedOut(budget) => {
                        tracing::error!(
                            "master control channel timed out after {}s while sending result",
                            budget.as_secs()
                        );
                        return OperationOutcome {
                            result: remote::protocol::RcpdResult::Failure {
                                error: format!(
                                    "master control channel timed out after {}s while sending result",
                                    budget.as_secs()
                                ),
                                summary: common::copy::Summary::default(),
                                runtime_stats: common::collect_runtime_stats(),
                            },
                            drain_stdout: false,
                        };
                    }
                    MasterReadOutcome::Error(error) => {
                        tracing::error!(
                            "master control channel failed while sending result: {error:#}"
                        );
                        return OperationOutcome {
                            result: remote::protocol::RcpdResult::Failure {
                                error: format!(
                                    "master control channel failed while sending result: {error:#}"
                                ),
                                summary: common::copy::Summary::default(),
                                runtime_stats: common::collect_runtime_stats(),
                            },
                            drain_stdout: false,
                        };
                    }
                }
            }
        }
    }
}

/// Run an operation under the master stdin watchdog, then deliver `RcpdResult` tied to master
/// disconnect or the configured keepalive policy.
async fn run_guarded_operation_and_send_result<Operation, R>(
    operation: Operation,
    master_stdin: &mut MasterStdinStream<R>,
    master_control: &remote::stdio::DaemonControlSender,
) -> OperationOutcome
where
    Operation: std::future::Future<Output = anyhow::Result<(String, common::copy::Summary)>>,
    R: tokio::io::AsyncRead + Unpin,
{
    let outcome =
        match run_with_master_watchdog(operation, master_stdin_watchdog(master_stdin)).await {
            WatchdogOutcome::Completed(outcome) => outcome,
            WatchdogOutcome::MasterLost(cause) => {
                tracing::error!(
                    "Master (rcp) control channel lost mid-operation - {cause}. This usually \
                     means the master process was killed or its host became unreachable. \
                     Shutting down."
                );
                return OperationOutcome {
                    result: remote::protocol::RcpdResult::Failure {
                        error: format!("master control channel lost: {cause}"),
                        summary: common::copy::Summary::default(),
                        runtime_stats: common::collect_runtime_stats(),
                    },
                    drain_stdout: false,
                };
            }
        };
    let result = rcpd_result_from(outcome);
    deliver_result(master_stdin, master_control, result).await
}

const TRACING_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

fn write_stderr_best_effort(args: std::fmt::Arguments<'_>) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr(), "{args}");
}

async fn finish_stdout_writer(
    task: remote::AbortOnDropTask<()>,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    // the same grace bounds an in-flight frame and the remaining queue. The task guard aborts
    // on expiry or caller cancellation instead of detaching a writer blocked on the master
    cancellation_token.cancel();
    match tokio::time::timeout(TRACING_DRAIN_GRACE, task.join()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            write_stderr_best_effort(format_args!("Master stdout writer task failed: {error:#}"));
        }
        Err(_) => {
            write_stderr_best_effort(format_args!(
                "Timed out draining remote master stdout stream"
            ));
        }
    }
}

#[instrument(skip(tracing_receiver))]
async fn async_main(
    args: Args,
    debug_log_file: Option<String>,
    tracing_receiver: tokio::sync::mpsc::UnboundedReceiver<common::remote_tracing::TracingMessage>,
    files_in_flight: common::ResolvedFilesInFlight,
    concurrency: remote::ResolvedRemoteConcurrency,
    admission: common::EndpointAdmission,
) -> anyhow::Result<String> {
    if let Some(filename) = debug_log_file {
        tracing::info!("rcpd: Debug logging to file: {filename}");
    }
    // install rustls crypto provider (ring) before any TLS operations
    if !args.no_encryption {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok(); // ignore if already installed
    }
    let tcp_config = args.to_tcp_config();
    tracing::info!(
        "Effective remote connection count: {}",
        concurrency.max_connections()
    );
    // generate ephemeral TLS certificate for source↔destination mutual TLS (if encryption enabled)
    let cert_key = if !args.no_encryption {
        Some(
            remote::tls::generate_self_signed_cert()
                .context("failed to generate TLS certificate")?,
        )
    } else {
        None
    };
    // output readiness record to stderr (read by master via SSH)
    // stdout is owned by the framed binary master↔rcpd control and tracing protocol
    // format: "RCP_READY <hex_fp|-> <F> <E>"
    use std::io::Write;
    writeln!(
        std::io::stderr(),
        "{}",
        remote::format_rcpd_readiness(cert_key.as_ref().map(|cert| &cert.fingerprint), concurrency)
    )
    .context("failed to write readiness record to stderr")?;
    // flush stderr to ensure master receives the readiness record immediately
    std::io::stderr()
        .flush()
        .context("failed to flush stderr")?;
    let (stdin_reader, stdout_writer) = remote::stdio::take_protocol_stdio()?;
    let master_stdin = MasterStdinStream::new(stdin_reader, tcp_config.keepalive_sec);
    let master_send_stream = remote::streams::SendStream::new(stdout_writer);
    let (control_sender, control_receiver) = remote::stdio::daemon_control_channel();
    let stdout_cancel = tokio_util::sync::CancellationToken::new();
    let stdout_writer_task = {
        let cancel = stdout_cancel.clone();
        remote::AbortOnDropTask::new(tokio::spawn(async move {
            if let Err(error) = remote::stdio::run_daemon_stdout_writer(
                control_receiver,
                tracing_receiver,
                master_send_stream,
                cancel,
            )
            .await
            {
                // its receiver is gone, so reporting through tracing would lose this diagnostic
                write_stderr_best_effort(format_args!("Master stdout writer failed: {error:#}"));
            }
        }))
    };
    tracing::info!(
        "Master SSH stdio channel ready (source↔destination encryption={})",
        !args.no_encryption
    );
    let outcome = run_operation(
        args,
        &tcp_config,
        concurrency,
        admission,
        files_in_flight.source(),
        control_sender,
        master_stdin,
        cert_key,
    )
    .await;
    if outcome.drain_stdout {
        // control_sender was dropped when run_operation returned and RcpdResult is already
        // delivered; drain queued trailing traces within TRACING_DRAIN_GRACE
        finish_stdout_writer(stdout_writer_task, stdout_cancel).await;
    } else {
        // abort immediately when master was lost or result delivery failed so we never hang
        // waiting on a dead pipe or flush an unacknowledged frame after returning failure
        drop(stdout_writer_task);
    }
    match outcome.result {
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
    write_stderr_best_effort(format_args!(
        "{}",
        common::format_startup_diagnostic(Some(remote::RCPD_STARTUP_ERROR_PREFIX), diagnostic)
    ));
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
        let version = remote::version::ProtocolVersion::current();
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
    // first stderr line for the startup handshake, so this is what surfaces in its error.
    match common::toctou_check::run_linter(args.dereference, false, args.require_toctou_safe, &[]) {
        common::toctou_check::LinterAction::Exit { output, code } => {
            exit_with_startup_refusal(&output, code);
        }
        common::toctou_check::LinterAction::Proceed => {}
    }
    let (tracing_layer, tracing_sender, tracing_receiver) =
        common::remote_tracing::RemoteTracingLayer::new();
    let debug_log_file = args
        .debug_log_prefix
        .as_ref()
        .map(|prefix| common::generate_debug_log_filename(prefix));
    let func = {
        let args = args.clone();
        let debug_log_file = debug_log_file.clone();
        |admission| {
            async_main(
                args,
                debug_log_file,
                tracing_receiver,
                files_in_flight,
                concurrency,
                admission,
            )
        }
    };
    // rcpd never prints a user-facing summary to stdout (stdout carries framed master protocol)
    let mut output = args
        .common
        .output_config(args.quiet, false, common::OutputFormat::Text);
    output.startup_error_prefix = Some(remote::RCPD_STARTUP_ERROR_PREFIX);
    output.result_output = common::ResultOutput::StderrErrorsOnly;
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
    use tokio::io::AsyncReadExt;

    fn blocked_stdout_writer() -> (
        remote::AbortOnDropTask<()>,
        tokio_util::sync::CancellationToken,
        tokio::io::DuplexStream,
    ) {
        let (control_tx, control_rx) = remote::stdio::daemon_control_channel();
        drop(control_tx);
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        sender
            .send(common::remote_tracing::TracingMessage::Log {
                timestamp: std::time::UNIX_EPOCH,
                level: "INFO".to_owned(),
                target: "rcp::source".to_owned(),
                message: "queued preview".to_owned(),
            })
            .unwrap();
        let (writer, reader) = tokio::io::duplex(1);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let token = shutdown.clone();
        let task = remote::AbortOnDropTask::new(tokio::spawn(async move {
            remote::stdio::run_daemon_stdout_writer(
                control_rx,
                receiver,
                remote::streams::SendStream::new(writer),
                token,
            )
            .await
            .unwrap();
        }));
        (task, shutdown, reader)
    }

    async fn assert_stdout_writer_was_aborted(mut reader: tokio::io::DuplexStream) {
        let mut remaining = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            reader.read_to_end(&mut remaining),
        )
        .await
        .expect("the writer must release its stream after shutdown ends")
        .unwrap();
        // only the byte already buffered may remain. A detached writer resumes once we read
        // and sends the rest of the frame, which would make an EOF-only assertion pass
        assert!(
            remaining.len() <= 1,
            "writer continued writing after shutdown: {} bytes",
            remaining.len()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stdout_writer_timeout_aborts_a_blocked_write() {
        let (task, shutdown, mut reader) = blocked_stdout_writer();
        reader.read_u8().await.unwrap();
        let started = tokio::time::Instant::now();
        finish_stdout_writer(task, shutdown).await;
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(5));
        assert_stdout_writer_was_aborted(reader).await;
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_stdout_writer_shutdown_aborts_a_blocked_write() {
        let (task, shutdown, mut reader) = blocked_stdout_writer();
        reader.read_u8().await.unwrap();
        let mut finish = Box::pin(finish_stdout_writer(task, shutdown));
        assert!(futures::poll!(&mut finish).is_pending());
        drop(finish);
        assert_stdout_writer_was_aborted(reader).await;
    }

    #[tokio::test(start_paused = true)]
    async fn stdout_writer_shutdown_waits_for_queued_output() -> anyhow::Result<()> {
        let (task, shutdown, reader) = blocked_stdout_writer();
        let mut stream = remote::streams::RecvStream::new(reader);
        let receive = async {
            let message = stream
                .recv_object::<remote::protocol::RcpdToMaster>()
                .await?;
            let Some(remote::protocol::RcpdToMaster::Tracing(
                common::remote_tracing::TracingMessage::Log { message, .. },
            )) = message
            else {
                panic!("shutdown must forward the queued preview");
            };
            assert_eq!(message, "queued preview");
            assert!(
                stream
                    .recv_object::<remote::protocol::RcpdToMaster>()
                    .await?
                    .is_none()
            );
            Ok::<_, anyhow::Error>(())
        };
        let ((), received) = tokio::join!(finish_stdout_writer(task, shutdown), receive);
        received
    }

    struct HeldCapture(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for HeldCapture {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    async fn operation_holding_children(
        dropped: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        cancel: tokio::sync::oneshot::Sender<()>,
    ) -> anyhow::Result<(String, common::copy::Summary)> {
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
        let (master_stdin_tx, daemon_stdin_rx) = tokio::io::duplex(4096);
        let mut master_stdin = MasterStdinStream::new(daemon_stdin_rx, 0);
        let closer = tokio::spawn(async move {
            let _ = cancelled.await;
            drop(master_stdin_tx);
        });
        let (control_tx, _control_rx) = remote::stdio::daemon_control_channel();
        let operation =
            common::task_scope::scope_tasks(operation_holding_children(dropped.clone(), cancel));
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_guarded_operation_and_send_result(operation, &mut master_stdin, &control_tx),
        )
        .await
        .expect("stdin cancellation must drain child captures");
        closer.await.unwrap();
        assert!(!outcome.drain_stdout);
        assert!(
            matches!(outcome.result, remote::protocol::RcpdResult::Failure { error, .. } if error.contains("master closed its stdin control channel"))
        );
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn completed_operation_delivers_result_even_when_master_closes_stdin_immediately() {
        let (master_stdin_tx, daemon_stdin_rx) = tokio::io::duplex(4096);
        let (daemon_stdout_tx, master_stdout_rx) = tokio::io::duplex(4096);
        let (control_tx, control_rx) = remote::stdio::daemon_control_channel();
        let (_tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        let stdout_cancel = tokio_util::sync::CancellationToken::new();
        let writer_task = {
            let cancel = stdout_cancel.clone();
            remote::AbortOnDropTask::new(tokio::spawn(async move {
                remote::stdio::run_daemon_stdout_writer(
                    control_rx,
                    tracing_rx,
                    remote::streams::SendStream::new(daemon_stdout_tx),
                    cancel,
                )
                .await
                .unwrap();
            }))
        };
        let master = tokio::spawn(async move {
            let mut stdout = remote::streams::RecvStream::new(master_stdout_rx);
            let frame = stdout
                .recv_object::<remote::protocol::RcpdToMaster>()
                .await
                .unwrap()
                .expect("master must receive RcpdResult");
            // close daemon stdin the instant RcpdResult is decoded
            drop(master_stdin_tx);
            frame
        });
        let mut master_stdin = MasterStdinStream::new(daemon_stdin_rx, 5);
        let outcome = run_guarded_operation_and_send_result(
            async {
                Ok((
                    "copy complete".to_string(),
                    common::copy::Summary::default(),
                ))
            },
            &mut master_stdin,
            &control_tx,
        )
        .await;
        drop(control_tx);
        assert!(outcome.drain_stdout);
        finish_stdout_writer(writer_task, stdout_cancel).await;
        assert!(matches!(
            outcome.result,
            remote::protocol::RcpdResult::Success { ref message, .. } if message == "copy complete"
        ));
        assert!(matches!(
            master.await.unwrap(),
            remote::protocol::RcpdToMaster::Result(remote::protocol::RcpdResult::Success {
                message,
                ..
            }) if message == "copy complete"
        ));
    }
    #[tokio::test(start_paused = true)]
    async fn deliver_result_waits_past_trace_grace_while_master_heartbeats_continue() {
        let (master_stdin_tx, daemon_stdin_rx) = tokio::io::duplex(4096);
        let (daemon_stdout_tx, master_stdout_rx) = tokio::io::duplex(64);
        let (control_tx, control_rx) = remote::stdio::daemon_control_channel();
        let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        let stdout_cancel = tokio_util::sync::CancellationToken::new();
        // enqueue an in-flight trace larger than the 64-byte duplex buffer so stdout blocks
        tracing_tx
            .send(common::remote_tracing::TracingMessage::Log {
                timestamp: std::time::SystemTime::UNIX_EPOCH,
                level: "INFO".to_owned(),
                target: "rcpd".to_owned(),
                message: "b".repeat(512),
            })
            .unwrap();
        let writer_task = {
            let cancel = stdout_cancel.clone();
            remote::AbortOnDropTask::new(tokio::spawn(async move {
                remote::stdio::run_daemon_stdout_writer(
                    control_rx,
                    tracing_rx,
                    remote::streams::SendStream::new(daemon_stdout_tx),
                    cancel,
                )
                .await
                .unwrap();
            }))
        };
        // let the writer start writing the in-flight trace and park on stdout backpressure
        tokio::task::yield_now().await;
        let master = tokio::spawn(async move {
            let mut stdin = remote::streams::SendStream::new(master_stdin_tx);
            // keep heartbeating across more than 2x TRACING_DRAIN_GRACE (12s > 5s) before reading
            for _ in 0..3 {
                tokio::time::sleep(std::time::Duration::from_secs(4)).await;
                stdin
                    .send_control_message(&remote::protocol::MasterToRcpd::Heartbeat)
                    .await
                    .unwrap();
            }
            let mut stdout = remote::streams::RecvStream::new(master_stdout_rx);
            let mut final_result = None;
            while let Some(frame) = stdout
                .recv_object::<remote::protocol::RcpdToMaster>()
                .await
                .unwrap()
            {
                if let remote::protocol::RcpdToMaster::Result(res) = frame {
                    final_result = Some(res);
                    drop(stdin);
                    break;
                }
            }
            final_result.expect("master must receive RcpdResult after resuming reads")
        });
        let started = tokio::time::Instant::now();
        let mut master_stdin = MasterStdinStream::new(daemon_stdin_rx, 30);
        let outcome = run_guarded_operation_and_send_result(
            async {
                Ok((
                    "copy complete".to_string(),
                    common::copy::Summary::default(),
                ))
            },
            &mut master_stdin,
            &control_tx,
        )
        .await;
        assert!(started.elapsed() >= std::time::Duration::from_secs(12));
        assert!(outcome.drain_stdout);
        drop(control_tx);
        finish_stdout_writer(writer_task, stdout_cancel).await;
        assert!(matches!(
            outcome.result,
            remote::protocol::RcpdResult::Success { ref message, .. } if message == "copy complete"
        ));
        assert!(matches!(
            master.await.unwrap(),
            remote::protocol::RcpdResult::Success { message, .. } if message == "copy complete"
        ));
    }
    #[tokio::test(start_paused = true)]
    async fn deliver_result_with_zero_keepalive_waits_past_trace_grace_until_reader_resumes() {
        let (master_stdin_tx, daemon_stdin_rx) = tokio::io::duplex(4096);
        let (daemon_stdout_tx, master_stdout_rx) = tokio::io::duplex(64);
        let (control_tx, control_rx) = remote::stdio::daemon_control_channel();
        let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        let stdout_cancel = tokio_util::sync::CancellationToken::new();
        tracing_tx
            .send(common::remote_tracing::TracingMessage::Log {
                timestamp: std::time::SystemTime::UNIX_EPOCH,
                level: "INFO".to_owned(),
                target: "rcpd".to_owned(),
                message: "z".repeat(512),
            })
            .unwrap();
        let writer_task = {
            let cancel = stdout_cancel.clone();
            remote::AbortOnDropTask::new(tokio::spawn(async move {
                remote::stdio::run_daemon_stdout_writer(
                    control_rx,
                    tracing_rx,
                    remote::streams::SendStream::new(daemon_stdout_tx),
                    cancel,
                )
                .await
                .unwrap();
            }))
        };
        tokio::task::yield_now().await;
        let master = tokio::spawn(async move {
            let _keep_stdin_open = master_stdin_tx;
            // with keepalive disabled (0), no heartbeats are sent and reads pause for 60s
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            let mut stdout = remote::streams::RecvStream::new(master_stdout_rx);
            while let Some(frame) = stdout
                .recv_object::<remote::protocol::RcpdToMaster>()
                .await
                .unwrap()
            {
                if let remote::protocol::RcpdToMaster::Result(res) = frame {
                    return res;
                }
            }
            panic!("master must receive RcpdResult when keepalive is disabled");
        });
        let started = tokio::time::Instant::now();
        let mut master_stdin = MasterStdinStream::new(daemon_stdin_rx, 0);
        let outcome = run_guarded_operation_and_send_result(
            async {
                Ok((
                    "copy complete".to_string(),
                    common::copy::Summary::default(),
                ))
            },
            &mut master_stdin,
            &control_tx,
        )
        .await;
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(60));
        assert!(outcome.drain_stdout);
        drop(control_tx);
        finish_stdout_writer(writer_task, stdout_cancel).await;
        assert!(matches!(
            outcome.result,
            remote::protocol::RcpdResult::Success { ref message, .. } if message == "copy complete"
        ));
        assert!(matches!(
            master.await.unwrap(),
            remote::protocol::RcpdResult::Success { message, .. } if message == "copy complete"
        ));
    }
    #[tokio::test(start_paused = true)]
    async fn deliver_result_honors_keepalive_timeout_and_aborts_without_leaking_success() {
        let (_master_stdin_tx, daemon_stdin_rx) = tokio::io::duplex(4096);
        let (daemon_stdout_tx, master_stdout_rx) = tokio::io::duplex(64);
        let (control_tx, control_rx) = remote::stdio::daemon_control_channel();
        let (tracing_tx, tracing_rx) = tokio::sync::mpsc::unbounded_channel();
        let stdout_cancel = tokio_util::sync::CancellationToken::new();
        tracing_tx
            .send(common::remote_tracing::TracingMessage::Log {
                timestamp: std::time::SystemTime::UNIX_EPOCH,
                level: "INFO".to_owned(),
                target: "rcpd".to_owned(),
                message: "s".repeat(512),
            })
            .unwrap();
        let writer_task = {
            let cancel = stdout_cancel.clone();
            remote::AbortOnDropTask::new(tokio::spawn(async move {
                let _ = remote::stdio::run_daemon_stdout_writer(
                    control_rx,
                    tracing_rx,
                    remote::streams::SendStream::new(daemon_stdout_tx),
                    cancel,
                )
                .await;
            }))
        };
        tokio::task::yield_now().await;
        let started = tokio::time::Instant::now();
        let mut master_stdin = MasterStdinStream::new(daemon_stdin_rx, 30);
        let outcome = run_guarded_operation_and_send_result(
            async {
                Ok((
                    "copy complete".to_string(),
                    common::copy::Summary::default(),
                ))
            },
            &mut master_stdin,
            &control_tx,
        )
        .await;
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(30));
        assert!(!outcome.drain_stdout);
        assert!(
            matches!(outcome.result, remote::protocol::RcpdResult::Failure { ref error, .. } if error.contains("timed out after 30s while sending result")),
            "unexpected outcome: {:?}",
            outcome.result
        );
        // match async_main: abort the writer immediately when drain_stdout is false
        drop(control_tx);
        drop(writer_task);
        // draining whatever was in the pipe must never yield Success
        let mut stdout = remote::streams::RecvStream::new(master_stdout_rx);
        while let Ok(Some(frame)) = stdout.recv_object::<remote::protocol::RcpdToMaster>().await {
            assert!(
                !matches!(
                    frame,
                    remote::protocol::RcpdToMaster::Result(
                        remote::protocol::RcpdResult::Success { .. }
                    )
                ),
                "aborted delivery must never leak RcpdResult::Success"
            );
        }
    }
    #[tokio::test(start_paused = true)]
    async fn zero_keepalive_never_times_out_and_exits_on_eof() {
        let (writer, reader) = tokio::io::duplex(4096);
        let mut send_stream = remote::streams::SendStream::new(writer);
        let mut master_stdin = MasterStdinStream::new(reader, 0);
        let sender = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(600)).await;
            send_stream
                .send_control_message(&remote::protocol::MasterToRcpd::Hello(
                    remote::protocol::MasterHello::Source {
                        src: "/src".into(),
                        dst: "/dst".into(),
                        dest_cert_fingerprint: None,
                        filter: None,
                        dry_run: None,
                        capture: Default::default(),
                    },
                ))
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(600)).await;
            drop(send_stream);
        });
        let started = tokio::time::Instant::now();
        let _hello = recv_master_hello(&mut master_stdin).await.unwrap();
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(600));
        let cause = master_stdin_watchdog(&mut master_stdin).await;
        sender.await.unwrap();
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(1200));
        assert_eq!(cause, "master closed its stdin control channel");
    }
    #[tokio::test(start_paused = true)]
    async fn recv_master_hello_keeps_advancing_large_frame_alive_and_times_out_if_stalled() {
        use tokio::io::AsyncWriteExt as _;
        let mut filter = common::filter::FilterSettings::default();
        for i in 0..200 {
            filter.add_include(&format!("dir_{i}/**/*.txt")).unwrap();
            filter.add_exclude(&format!("tmp_{i}/**/*.log")).unwrap();
        }
        let expected_hello = remote::protocol::MasterHello::Source {
            src: "/src".into(),
            dst: "/dst".into(),
            dest_cert_fingerprint: None,
            filter: Some(filter),
            dry_run: None,
            capture: Default::default(),
        };
        let mut encoded = Vec::new();
        remote::streams::SendStream::new(&mut encoded)
            .send_control_message(&remote::protocol::MasterToRcpd::Hello(
                expected_hello.clone(),
            ))
            .await
            .unwrap();
        assert!(encoded.len() > 400);
        // drip the single MasterHello frame in 4 chunks spaced 3s apart under a 5s keepalive
        // budget (total 9s > 5s): read progress must refresh the idle deadline
        let (mut writer, reader) = tokio::io::duplex(4096);
        let mut master_stdin = MasterStdinStream::new(reader, 5);
        let encoded_chunks = encoded.clone();
        let sender = tokio::spawn(async move {
            let chunk_size = encoded_chunks.len().div_ceil(4);
            for (idx, chunk) in encoded_chunks.chunks(chunk_size).enumerate() {
                if idx > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                }
                writer.write_all(chunk).await.unwrap();
                writer.flush().await.unwrap();
            }
            writer
        });
        let started = tokio::time::Instant::now();
        let received = recv_master_hello(&mut master_stdin).await.unwrap();
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(9));
        assert_eq!(format!("{received:?}"), format!("{expected_hello:?}"));
        let _keep_open = sender.await.unwrap();
        // a peer that stalls mid-frame for the full keepalive budget must time out
        let (mut stalled_writer, stalled_reader) = tokio::io::duplex(4096);
        let mut stalled_stdin = MasterStdinStream::new(stalled_reader, 5);
        let stalled_sender = tokio::spawn(async move {
            stalled_writer.write_all(&encoded[..16]).await.unwrap();
            stalled_writer.flush().await.unwrap();
            stalled_writer
        });
        let stalled_start = tokio::time::Instant::now();
        let error = recv_master_hello(&mut stalled_stdin)
            .await
            .expect_err("mid-frame stall must time out");
        assert_eq!(stalled_start.elapsed(), std::time::Duration::from_secs(5));
        assert!(
            format!("{error:#}").contains("timed out after 5s waiting for hello"),
            "unexpected stall error: {error:#}"
        );
        let _keep_stalled = stalled_sender.await.unwrap();
        // a completely silent peer must also time out after one budget
        let (_silent_writer, silent_reader) = tokio::io::duplex(4096);
        let mut silent_stdin = MasterStdinStream::new(silent_reader, 5);
        let silent_start = tokio::time::Instant::now();
        let error = recv_master_hello(&mut silent_stdin)
            .await
            .expect_err("silent peer must time out");
        assert_eq!(silent_start.elapsed(), std::time::Duration::from_secs(5));
        assert!(
            format!("{error:#}").contains("timed out after 5s waiting for hello"),
            "unexpected silent error: {error:#}"
        );
    }
    #[tokio::test(start_paused = true)]
    async fn recv_master_hello_ignores_heartbeats_and_honors_keepalive_timeout() {
        let (writer, reader) = tokio::io::duplex(4096);
        let mut send_stream = remote::streams::SendStream::new(writer);
        let mut master_stdin = MasterStdinStream::new(reader, 5);
        let expected_hello = remote::protocol::MasterHello::Source {
            src: "/src".into(),
            dst: "/dst".into(),
            dest_cert_fingerprint: None,
            filter: None,
            dry_run: None,
            capture: Default::default(),
        };
        let hello_to_send = expected_hello.clone();
        let sender = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            send_stream
                .send_control_message(&remote::protocol::MasterToRcpd::Heartbeat)
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            send_stream
                .send_control_message(&remote::protocol::MasterToRcpd::Hello(hello_to_send))
                .await
                .unwrap();
            send_stream
        });
        let received = recv_master_hello(&mut master_stdin).await.unwrap();
        assert_eq!(format!("{received:?}"), format!("{expected_hello:?}"));
        let _keep_open = sender.await.unwrap();
        // silence longer than keepalive_sec triggers timeout in the post-hello watchdog
        let started = tokio::time::Instant::now();
        let cause = master_stdin_watchdog(&mut master_stdin).await;
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(5));
        assert!(
            cause.contains("timed out after 5s without a heartbeat"),
            "unexpected watchdog cause: {cause}"
        );
    }
    #[tokio::test(start_paused = true)]
    async fn master_stdin_watchdog_refreshes_on_heartbeats_and_exits_on_eof() {
        let (writer, reader) = tokio::io::duplex(4096);
        let mut send_stream = remote::streams::SendStream::new(writer);
        let mut master_stdin = MasterStdinStream::new(reader, 3);
        let sender = tokio::spawn(async move {
            for _ in 0..3 {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                send_stream
                    .send_control_message(&remote::protocol::MasterToRcpd::Heartbeat)
                    .await
                    .unwrap();
            }
            drop(send_stream);
        });
        let started = tokio::time::Instant::now();
        let cause = master_stdin_watchdog(&mut master_stdin).await;
        sender.await.unwrap();
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(6));
        assert_eq!(cause, "master closed its stdin control channel");
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
                (vec![], 16 * 1024 * 1024, 16 * 1024 * 1024),
                (
                    vec!["--network-profile=internet"],
                    2 * 1024 * 1024,
                    2 * 1024 * 1024,
                ),
                (vec!["--buffer-size=12345"], 12345, 12345),
                (
                    vec!["--buffer-size=33554432"],
                    32 * 1024 * 1024,
                    16 * 1024 * 1024,
                ),
                (
                    vec!["--network-profile=internet", "--buffer-size=16777216"],
                    16 * 1024 * 1024,
                    2 * 1024 * 1024,
                ),
                (
                    vec!["--buffer-retention-limit=2097152"],
                    16 * 1024 * 1024,
                    2 * 1024 * 1024,
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
    fn daemon_rejects_buffer_sizes_outside_its_usize_range() {
        let too_large = (usize::MAX as u128 + 1).to_string();
        for role in ["--role=source", "--role=destination"] {
            for value in ["-1", "nonsense", too_large.as_str()] {
                let error = Args::try_parse_from(["rcpd", role, &format!("--buffer-size={value}")])
                    .unwrap_err();
                assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
                assert!(error.to_string().contains("--buffer-size"));
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
