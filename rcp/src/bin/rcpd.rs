use anyhow::Context;
use clap::Parser;
use tracing::instrument;

use rcp_tools_rcp::daemon_session;

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

    /// Requested 3-host destination SSH tunnel count for descriptor headroom accounting (set by rcp
    /// master on destination daemons only).
    #[arg(long, hide = true, default_value_t = 0)]
    ssh_tunnels: usize,

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
                ssh_tunnels: if self.role == remote::protocol::RcpdRole::Destination {
                    self.ssh_tunnels
                } else {
                    0
                },
            };
        }
        throttle
    }
    /// Build the remote TCP config from CLI args for [`daemon_session::DaemonSessionConfig`].
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
    /// Build the base copy settings for [`daemon_session::DaemonSessionConfig`]. `filter` and
    /// `dry_run` are applied inside `daemon_session` once `MasterHello` arrives.
    fn to_copy_settings_base(
        &self,
        overwrite_compare: common::filecmp::MetadataCmpSettings,
        tcp_config: &remote::TcpConfig,
    ) -> common::copy::Settings {
        common::copy::Settings {
            reflink: Default::default(),
            local_copy_handoff: Default::default(),
            local_copy_buffer_retention_limit: Default::default(),
            dereference: self.dereference,
            fail_early: self.fail_early,
            overwrite: self.overwrite,
            overwrite_compare,
            overwrite_filter: self.overwrite_filter,
            ignore_existing: self.ignore_existing,
            chunk_size: self.chunk_size,
            skip_specials: self.skip_specials,
            remote_copy_buffer_size: tcp_config.effective_buffer_size(),
            filter: None,
            dry_run: None,
            delete: None,
        }
    }
}

fn write_stderr_best_effort(args: std::fmt::Arguments<'_>) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr(), "{args}");
}

#[allow(clippy::too_many_arguments)]
#[instrument(skip(tracing_receiver, remote_cleanup))]
async fn async_main(
    args: Args,
    debug_log_file: Option<String>,
    tracing_receiver: tokio::sync::mpsc::UnboundedReceiver<common::remote_tracing::TracingMessage>,
    files_in_flight: common::ResolvedFilesInFlight,
    concurrency: remote::ResolvedRemoteConcurrency,
    overwrite_compare: common::filecmp::MetadataCmpSettings,
    admission: common::EndpointAdmission,
    remote_cleanup: Option<remote::RemoteCleanup>,
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
    let copy_settings_base = args.to_copy_settings_base(overwrite_compare, &tcp_config);
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
    tracing::info!(
        "Master SSH stdio channel ready (source↔destination TLS cert={})",
        !args.no_encryption
    );
    let session_config = daemon_session::DaemonSessionConfig {
        preview_only: args.preview_only,
        overwrite_manifest_max_entries: args.overwrite_manifest_max_entries,
        bind_ip: args.bind_ip,
        tcp_config,
        concurrency,
        admission,
        files_source: files_in_flight.source(),
        cert_key,
        copy_settings_base,
    };
    let result = daemon_session::run_daemon_session(
        session_config,
        stdin_reader,
        stdout_writer,
        tracing_receiver,
        remote_cleanup,
        remote::transport::tcp(),
    )
    .await;
    match result {
        remote::protocol::RcpdResult::Success {
            message,
            summary: _,
            runtime_stats: _,
        } => Ok(message),
        remote::protocol::RcpdResult::Failure {
            error,
            summary: _,
            runtime_stats: _,
            peer_connected: _,
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
    let overwrite_compare = match common::parse_metadata_cmp_settings(&args.overwrite_compare) {
        Ok(overwrite_compare) => overwrite_compare,
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
    let remote_cleanup = match (args.role == remote::protocol::RcpdRole::Destination)
        .then(remote::RemoteCleanup::new)
        .transpose()
    {
        Ok(cleanup) => cleanup,
        Err(error) => {
            exit_with_startup_refusal(
                &format!("failed to start remote cleanup supervisor: {error:#}"),
                1,
            );
        }
    };
    let func = {
        let args = args.clone();
        let debug_log_file = debug_log_file.clone();
        let remote_cleanup = remote_cleanup.clone();
        |admission| {
            async_main(
                args,
                debug_log_file,
                tracing_receiver,
                files_in_flight,
                concurrency,
                overwrite_compare,
                admission,
                remote_cleanup,
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
    if let Some(remote_cleanup) = remote_cleanup {
        remote_cleanup.finish();
    }
    if res.is_none() {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
                let overwrite_compare =
                    common::parse_metadata_cmp_settings(&args.overwrite_compare).unwrap();
                assert_eq!(
                    args.to_copy_settings_base(overwrite_compare, &tcp)
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
                            ssh_tunnels: 0,
                        }
                    },
                );
            }
        }
    }

    #[test]
    fn preview_only_flag_sets_startup_admission_mode() {
        assert!(!daemon_args(&[]).preview_only);
        assert!(daemon_args(&["--preview-only"]).preview_only);
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
