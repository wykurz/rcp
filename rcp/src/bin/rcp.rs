use anyhow::{anyhow, Context};
use clap::Parser;
use tracing::instrument;

use rcp_tools_rcp::path;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "rcp",
    version,
    about = "Copy files efficiently - similar to `cp` but generally MUCH faster when dealing with large filesets",
    long_about = "`rcp` is a tool for copying files similar to `cp` but generally MUCH faster when dealing with a large number of files.

Supports both local and remote copying using `host:/path` syntax (similar to `scp`).

Inspired by tools like `dsync`(1) and `pcp`(2).

EXAMPLES:
    # Basic local copy with progress
    rcp /source /dest --progress --summary

    # Copy with metadata preservation and overwrite
    rcp /source /dest --preserve --overwrite --progress

    # Remote copy from one host to another
    rcp user@host1:/path/to/source user@host2:/path/to/dest --progress

    # Copy from remote to local
    rcp host:/remote/path /local/path --progress

    # Copy from local to remote
    rcp /local/path host:/remote/path --preserve --progress

1) https://mpifileutils.readthedocs.io/en/v0.11.1/dsync.1.html
2) https://github.com/wtsi-ssg/pcp"
)]
struct Args {
    // Copy options (core behavior + metadata preservation)
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

    /// Exit on first error
    #[arg(short = 'e', long = "fail-early", help_heading = "Copy options")]
    fail_early: bool,

    /// Always follow symbolic links in source
    #[arg(short = 'L', long, help_heading = "Copy options")]
    dereference: bool,

    /// Preserve file metadata: file owner, group, setuid, setgid, mtime, atime and mode
    #[arg(short, long, help_heading = "Copy options")]
    preserve: bool,

    /// Specify exactly what attributes to preserve
    ///
    /// If specified, the --preserve flag is ignored. Format: "`<type1>:<attributes1> <type2>:<attributes2>` ..." where `<type>` is one of f (file), d (directory), l (symlink), and `<attributes>` is a comma-separated list of uid, gid, time, or a 4-digit octal mode mask.
    ///
    /// Example: "f:uid,gid,time,0777 d:uid,gid,time,0777 l:uid,gid,time"
    #[arg(long, value_name = "SETTINGS", help_heading = "Copy options")]
    preserve_settings: Option<String>,

    // Progress & output
    /// Show progress
    #[arg(long, help_heading = "Progress & output")]
    progress: bool,

    /// Set the type of progress display
    ///
    /// If specified, --progress flag is implied.
    #[arg(long, value_name = "TYPE", help_heading = "Progress & output")]
    progress_type: Option<common::ProgressType>,

    /// Set delay between progress updates
    ///
    /// Default is 200ms for interactive mode (`ProgressBar`) and 10s for non-interactive mode (`TextUpdates`). If specified, --progress flag is implied. Accepts human-readable durations like "200ms", "10s", "5min".
    #[arg(long, value_name = "DELAY", help_heading = "Progress & output")]
    progress_delay: Option<String>,

    /// Print summary at the end
    #[arg(long, help_heading = "Progress & output")]
    summary: bool,

    /// Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count, help_heading = "Progress & output")]
    verbose: u8,

    /// Quiet mode, don't report errors
    #[arg(short = 'q', long = "quiet", help_heading = "Progress & output")]
    quiet: bool,

    // Performance & throttling
    /// Maximum number of open files (0 = no limit, unspecified = 80% of system limit)
    #[arg(long, value_name = "N", help_heading = "Performance & throttling")]
    max_open_files: Option<usize>,

    /// Throttle the number of operations per second, 0 means no throttle
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Performance & throttling"
    )]
    ops_throttle: usize,

    /// Limit I/O operations per second (0 = no throttle)
    ///
    /// Requires --chunk-size to calculate I/O operations per file: ((`file_size` - 1) / `chunk_size`) + 1
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Performance & throttling"
    )]
    iops_throttle: usize,

    /// Chunk size for calculating I/O operations per file
    ///
    /// Required when using --iops-throttle (must be > 0)
    #[arg(
        long,
        default_value = "0",
        value_name = "SIZE",
        help_heading = "Performance & throttling"
    )]
    chunk_size: bytesize::ByteSize,

    // Advanced settings
    /// Number of worker threads (0 = number of CPU cores)
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Advanced settings"
    )]
    max_workers: usize,

    /// Number of blocking worker threads (0 = Tokio default of 512)
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Advanced settings"
    )]
    max_blocking_threads: usize,

    // Remote copy options
    /// IP address to bind the master QUIC server to
    ///
    /// By default, the best available network interface is automatically selected.
    /// Use this option to explicitly bind to a specific IP address (e.g., "192.168.1.5").
    /// This is useful for multi-homed hosts or when you want to control which network
    /// is used for QUIC traffic. Only IPv4 addresses are supported.
    ///
    /// When the source path uses an IP address (e.g., "192.168.1.100:/path"), that IP
    /// is automatically passed to the source rcpd so it binds explicitly to that address.
    #[arg(long, value_name = "IP", help_heading = "Remote copy options")]
    bind_ip: Option<String>,

    /// Restrict QUIC to specific port ranges (e.g., "8000-8999,10000-10999")
    ///
    /// Defaults to dynamic port allocation if not specified
    #[arg(long, value_name = "RANGES", help_heading = "Remote copy options")]
    quic_port_ranges: Option<String>,

    /// QUIC idle timeout in seconds
    ///
    /// Maximum time a QUIC connection can be idle before being closed
    #[arg(
        long,
        default_value = "10",
        value_name = "N",
        help_heading = "Remote copy options"
    )]
    quic_idle_timeout_sec: u64,

    /// QUIC keep-alive interval in seconds
    ///
    /// Interval for sending QUIC keep-alive packets to detect dead connections
    #[arg(
        long,
        default_value = "1",
        value_name = "N",
        help_heading = "Remote copy options"
    )]
    quic_keep_alive_interval_sec: u64,

    /// Connection timeout for remote copy operations in seconds
    ///
    /// Applies to: rcpd→master connection, destination→source connection
    #[arg(
        long,
        default_value = "15",
        value_name = "N",
        help_heading = "Remote copy options"
    )]
    remote_copy_conn_timeout_sec: u64,

    /// Network profile for QUIC tuning
    ///
    /// 'lan' (default): Optimized for datacenter networks (<1ms RTT, 25-100 Gbps).
    /// Uses BBR congestion control and aggressive window sizes.
    /// 'wan': Conservative settings for internet/WAN connections.
    /// Uses CUBIC congestion control and standard window sizes.
    #[arg(
        long,
        default_value = "lan",
        value_name = "PROFILE",
        help_heading = "Remote copy options"
    )]
    network_profile: remote::NetworkProfile,

    /// Congestion control algorithm for QUIC (overrides profile default)
    ///
    /// 'bbr': Model-based, fast ramp-up. Best for dedicated high-bandwidth links.
    /// 'cubic': Loss-based, standard TCP congestion control. Best for shared networks.
    /// Default: 'bbr' for LAN profile, 'cubic' for WAN profile.
    #[arg(long, value_name = "ALGORITHM", help_heading = "Remote copy options")]
    congestion_control: Option<remote::CongestionControl>,

    /// QUIC connection-level receive window (overrides profile default)
    ///
    /// Accepts byte sizes like "128MiB", "1GiB", or plain numbers in bytes.
    #[arg(long, value_name = "SIZE", help_heading = "Remote copy options")]
    quic_receive_window: Option<bytesize::ByteSize>,

    /// QUIC per-stream receive window (overrides profile default)
    ///
    /// Accepts byte sizes like "16MiB", "256MiB", or plain numbers in bytes.
    #[arg(long, value_name = "SIZE", help_heading = "Remote copy options")]
    quic_stream_receive_window: Option<bytesize::ByteSize>,

    /// QUIC send window (overrides profile default)
    ///
    /// Accepts byte sizes like "128MiB", "1GiB", or plain numbers in bytes.
    #[arg(long, value_name = "SIZE", help_heading = "Remote copy options")]
    quic_send_window: Option<bytesize::ByteSize>,

    /// Initial RTT estimate in milliseconds (overrides profile default)
    ///
    /// Accepts floating point values for sub-millisecond precision (e.g., 0.3 for 300µs).
    #[arg(long, value_name = "MS", help_heading = "Remote copy options")]
    quic_initial_rtt_ms: Option<f64>,

    /// Initial MTU in bytes (default: 1200)
    #[arg(long, value_name = "BYTES", help_heading = "Remote copy options")]
    quic_initial_mtu: Option<u16>,

    /// Enable file-based debug logging for rcpd processes
    ///
    /// Example: /tmp/rcpd-log creates /tmp/rcpd-log-YYYY-MM-DDTHH-MM-SS-RANDOM
    #[arg(long, value_name = "PREFIX", help_heading = "Remote copy options")]
    rcpd_debug_log_prefix: Option<String>,

    // Profiling options
    /// Enable Chrome tracing output for profiling
    ///
    /// Produces JSON files viewable in Perfetto UI (ui.perfetto.dev) or chrome://tracing.
    /// Accepts a path prefix; full filename includes tool name, role, hostname, PID, and timestamp.
    /// For remote operations, tracing is automatically enabled on rcpd processes too.
    /// Example: --chrome-trace=/tmp/trace produces:
    ///   /tmp/trace-rcp-myhost-12345-2025-01-15T10:30:45.json
    ///   /tmp/trace-rcpd-source-host1-23456-2025-01-15T10:30:46.json (remote)
    ///   /tmp/trace-rcpd-destination-host2-34567-2025-01-15T10:30:46.json (remote)
    #[arg(long, value_name = "PREFIX", help_heading = "Profiling")]
    chrome_trace: Option<String>,

    /// Enable flamegraph output for profiling
    ///
    /// Produces folded stack files convertible to SVG with `inferno-flamegraph`.
    /// Accepts a path prefix; full filename includes tool name, role, hostname, PID, and timestamp.
    /// For remote operations, tracing is automatically enabled on rcpd processes too.
    /// Example: --flamegraph=/tmp/flame produces .folded files.
    /// Convert to SVG: cat *.folded | inferno-flamegraph > flamegraph.svg
    #[arg(long, value_name = "PREFIX", help_heading = "Profiling")]
    flamegraph: Option<String>,

    /// Log level for profiling (chrome-trace, flamegraph)
    ///
    /// Controls which spans are captured. Only spans from rcp crates are recorded.
    /// Values: trace, debug, info, warn, error (default: trace)
    #[arg(
        long,
        value_name = "LEVEL",
        default_value = "trace",
        help_heading = "Profiling"
    )]
    profile_level: String,

    /// Enable tokio-console for live async debugging
    ///
    /// Starts a tokio-console server for real-time async task inspection.
    /// Connect with: `tokio-console http://127.0.0.1:PORT`
    #[arg(long, help_heading = "Profiling")]
    tokio_console: bool,

    /// Port for tokio-console server (default: 6669)
    #[arg(long, value_name = "PORT", help_heading = "Profiling")]
    tokio_console_port: Option<u16>,

    /// Print protocol version information as JSON and exit
    ///
    /// Used to verify version compatibility with rcpd
    #[arg(long, help_heading = "Remote copy options")]
    protocol_version: bool,

    /// Path to rcpd binary on remote hosts
    ///
    /// If not specified, rcp will search for rcpd in standard locations
    #[arg(long, value_name = "PATH", help_heading = "Remote copy options")]
    rcpd_path: Option<String>,

    /// Automatically deploy rcpd binary to remote hosts if missing or version mismatch
    ///
    /// When enabled, rcp will transfer the local rcpd binary to remote hosts
    /// at ~/.cache/rcp/bin/rcpd-{version} if not found or if version doesn't match.
    /// The binary is transferred securely via SSH and verified with SHA-256 checksum.
    #[arg(long, help_heading = "Remote copy options")]
    auto_deploy_rcpd: bool,

    /// Force remote copy mode even for local-to-local paths
    ///
    /// Normally, when both source and destination are local paths (including paths
    /// with `localhost:` prefix), rcp performs a local copy. This flag forces the
    /// use of the remote copy protocol (rcpd) instead, which is useful for testing
    /// or when you want consistent behavior across local and remote operations.
    ///
    /// Requires paths to use the `localhost:` prefix (e.g., `localhost:/path/to/file`).
    #[arg(long, help_heading = "Remote copy options")]
    force_remote: bool,

    // ARGUMENTS
    /// Source path(s) and destination path
    #[arg()]
    paths: Vec<String>,
}

/// extract IP from host if it's an IPv4 address (for explicit binding)
fn extract_bind_ip_from_host(host: &str) -> Option<String> {
    // try parsing as IPv4
    if host.parse::<std::net::Ipv4Addr>().is_ok() {
        Some(host.to_string())
    } else {
        None
    }
}

#[instrument]
async fn run_rcpd_master(
    args: &Args,
    preserve: &common::preserve::Settings,
    src: &path::RemotePath,
    dst: &path::RemotePath,
) -> anyhow::Result<common::copy::Summary> {
    tracing::debug!("running rcpd src/dst");
    // build QUIC tuning from byte sizes
    let quic_tuning = remote::QuicTuning {
        receive_window: args.quic_receive_window.map(|b| b.0),
        stream_receive_window: args.quic_stream_receive_window.map(|b| b.0),
        send_window: args.quic_send_window.map(|b| b.0),
        initial_rtt_ms: args.quic_initial_rtt_ms,
        initial_mtu: args.quic_initial_mtu,
    };
    // build QUIC config with profile and tuning settings
    let quic_config = remote::QuicConfig {
        port_ranges: args.quic_port_ranges.clone(),
        idle_timeout_sec: args.quic_idle_timeout_sec,
        keep_alive_interval_sec: args.quic_keep_alive_interval_sec,
        conn_timeout_sec: args.remote_copy_conn_timeout_sec,
        network_profile: args.network_profile,
        congestion_control: args.congestion_control,
        tuning: quic_tuning.clone(),
    };
    // open a port and wait from server & client hello, respond to client with server port
    let (server_endpoint, master_cert_fingerprint) = remote::get_server_with_config(&quic_config)?;
    let server_addr =
        remote::get_endpoint_addr_with_bind_ip(&server_endpoint, args.bind_ip.as_deref())?;
    let server_name = remote::get_random_server_name();
    let mut rcpds = vec![];
    let rcpd_config = remote::protocol::RcpdConfig {
        verbose: args.verbose,
        fail_early: args.fail_early,
        max_workers: args.max_workers,
        max_blocking_threads: args.max_blocking_threads,
        max_open_files: args.max_open_files,
        ops_throttle: args.ops_throttle,
        iops_throttle: args.iops_throttle,
        chunk_size: args.chunk_size.0 as usize,
        dereference: args.dereference,
        overwrite: args.overwrite,
        overwrite_compare: args.overwrite_compare.clone(),
        debug_log_prefix: args.rcpd_debug_log_prefix.clone(),
        quic_port_ranges: args.quic_port_ranges.clone(),
        quic_idle_timeout_sec: args.quic_idle_timeout_sec,
        quic_keep_alive_interval_sec: args.quic_keep_alive_interval_sec,
        progress: args.progress,
        progress_delay: args.progress_delay.clone(),
        remote_copy_conn_timeout_sec: args.remote_copy_conn_timeout_sec,
        network_profile: args.network_profile,
        congestion_control: args.congestion_control,
        quic_tuning,
        master_cert_fingerprint,
        chrome_trace_prefix: args.chrome_trace.clone(),
        flamegraph_prefix: args.flamegraph.clone(),
        profile_level: Some(args.profile_level.clone()),
        tokio_console: args.tokio_console,
        tokio_console_port: args.tokio_console_port,
    };
    // deduplicate sessions if src and dst are the same host
    // this avoids deploying rcpd twice to the same location
    let sessions = if src.session() == dst.session() {
        vec![src.session()]
    } else {
        vec![src.session(), dst.session()]
    };
    for session in sessions {
        // determine bind IP: source uses host IP if available, destination uses None
        let (bind_ip, role) = if session == src.session() {
            (
                extract_bind_ip_from_host(&session.host),
                remote::protocol::RcpdRole::Source,
            )
        } else {
            (None, remote::protocol::RcpdRole::Destination)
        };
        let rcpd = remote::start_rcpd(
            &rcpd_config,
            session,
            &server_addr,
            &server_name,
            args.rcpd_path.as_deref(),
            args.auto_deploy_rcpd,
            bind_ip.as_deref(),
            role,
        )
        .await?;
        rcpds.push(rcpd);
    }
    // if src and dst are the same, we need to start rcpd twice even though we only deployed once
    if src.session() == dst.session() && rcpds.len() == 1 {
        let bind_ip = extract_bind_ip_from_host(&src.session().host);
        let rcpd = remote::start_rcpd(
            &rcpd_config,
            src.session(),
            &server_addr,
            &server_name,
            args.rcpd_path.as_deref(),
            args.auto_deploy_rcpd,
            bind_ip.as_deref(),
            remote::protocol::RcpdRole::Destination,
        )
        .await?;
        rcpds.push(rcpd);
    }
    tracing::info!("Waiting for connections from rcpd processes...");
    let rcpd_connect_timeout = std::time::Duration::from_secs(args.remote_copy_conn_timeout_sec);
    // helper to accept a connection and read its role
    let accept_rcpd_connection = |endpoint: quinn::Endpoint, timeout: std::time::Duration| async move {
        let conn = {
            let connecting = match tokio::time::timeout(timeout, endpoint.accept()).await {
                Ok(Some(conn)) => conn,
                Ok(None) => return Err(anyhow!("Server endpoint closed before rcpd connected")),
                Err(_) => {
                    return Err(anyhow!(
                        "Timed out waiting for rcpd to connect after {:?}. \
                            Check if hosts are reachable and rcpd can be executed.",
                        timeout
                    ))
                }
            };
            remote::streams::Connection::new(connecting.await?)
        };
        let mut tracing_stream = conn
            .accept_uni()
            .await
            .context("Failed to open unidirectional stream with rcpd")?;
        let hello = tracing_stream
            .recv_object::<remote::protocol::TracingHello>()
            .await
            .context("Failed to receive tracing hello from rcpd")?
            .context("Expected TracingHello from rcpd")?;
        Ok::<_, anyhow::Error>((conn, tracing_stream, hello))
    };

    // accept both connections
    tracing::info!("Waiting for first rcpd connection...");
    let (conn1, conn1_tracing_stream, conn1_hello) =
        accept_rcpd_connection(server_endpoint.clone(), rcpd_connect_timeout).await?;
    tracing::info!("First rcpd connected with role: {}", conn1_hello.role);

    tracing::info!("Waiting for second rcpd connection...");
    let (conn2, conn2_tracing_stream, conn2_hello) =
        accept_rcpd_connection(server_endpoint.clone(), rcpd_connect_timeout).await?;
    tracing::info!("Second rcpd connected with role: {}", conn2_hello.role);

    // match connections by role
    let (source_connection, source_tracing_stream, dest_connection, dest_tracing_stream) =
        if conn1_hello.role == remote::protocol::RcpdRole::Source {
            (conn1, conn1_tracing_stream, conn2, conn2_tracing_stream)
        } else {
            (conn2, conn2_tracing_stream, conn1, conn1_tracing_stream)
        };
    let source_tracing_task = {
        tokio::spawn(async move {
            if let Err(e) = remote::tracelog::run_receiver(
                source_tracing_stream,
                remote::tracelog::RcpdType::Source,
            )
            .await
            {
                tracing::warn!("Source remote tracing receiver failed: {}", e);
            }
        })
    };
    let dest_tracing_task = {
        tokio::spawn(async move {
            if let Err(e) = remote::tracelog::run_receiver(
                dest_tracing_stream,
                remote::tracelog::RcpdType::Destination,
            )
            .await
            {
                tracing::warn!("Destination remote tracing receiver failed: {}", e);
            }
        })
    };
    // send MasterHello to source rcpd
    let (source_send_stream, mut source_recv_stream) = source_connection
        .open_bi()
        .await
        .context("Failed to open bidirectional stream with source rcpd")?;
    {
        let mut source_send_stream = source_send_stream.lock().await;
        source_send_stream
            .send_control_message(&remote::protocol::MasterHello::Source {
                src: src.path().to_path_buf(),
                dst: dst.path().to_path_buf(),
            })
            .await?;
        source_send_stream.close().await?;
    }
    tracing::debug!("Waiting for source rcpd to send hello");
    let source_hello = source_recv_stream
        .recv_object::<remote::protocol::SourceMasterHello>()
        .await?
        .expect("Failed to receive source hello from source rcpd");
    // send MasterHello to destination rcpd
    let (dest_send_stream, mut dest_recv_stream) = dest_connection
        .open_bi()
        .await
        .context("Failed to open bidirectional stream with destination rcpd")?;
    {
        let mut dest_send_stream = dest_send_stream.lock().await;
        dest_send_stream
            .send_control_message(&remote::protocol::MasterHello::Destination {
                source_addr: source_hello.source_addr,
                server_name: source_hello.server_name.clone(),
                source_cert_fingerprint: source_hello.cert_fingerprint.clone(),
                preserve: *preserve,
            })
            .await?;
        dest_send_stream.close().await?;
    }
    tracing::info!("Forwarded source connection info to destination");
    let source_result = source_recv_stream
        .recv_object::<remote::protocol::RcpdResult>()
        .await?
        .expect("Failed to receive RcpdResult from source rcpd");
    let dest_result = dest_recv_stream
        .recv_object::<remote::protocol::RcpdResult>()
        .await?
        .expect("Failed to receive RcpdResult from destination rcpd");
    tracing::debug!("Received RcpdResult from both source and destination rcpds");
    // check for failures and collect error details + runtime stats
    let mut errors = Vec::new();
    let (_source_summary, source_runtime_stats) = match source_result {
        remote::protocol::RcpdResult::Success {
            message,
            summary,
            runtime_stats,
        } => {
            tracing::info!("Source rcpd completed successfully: {message}");
            (summary, runtime_stats)
        }
        remote::protocol::RcpdResult::Failure {
            error,
            summary,
            runtime_stats,
        } => {
            tracing::error!("Source rcpd failed: {error}");
            errors.push(format!("Source: {error}"));
            (summary, runtime_stats)
        }
    };
    let (dest_summary, dest_runtime_stats) = match dest_result {
        remote::protocol::RcpdResult::Success {
            message,
            summary,
            runtime_stats,
        } => {
            tracing::info!("Destination rcpd completed successfully: {message}");
            (summary, runtime_stats)
        }
        remote::protocol::RcpdResult::Failure {
            error,
            summary,
            runtime_stats,
        } => {
            tracing::error!("Destination rcpd failed: {error}");
            errors.push(format!("Destination: {error}"));
            (summary, runtime_stats)
        }
    };
    // store remote runtime stats for display at the end
    common::set_remote_runtime_stats(common::RemoteRuntimeStats {
        source_host: src.session().host.clone(),
        source_stats: source_runtime_stats,
        dest_host: dst.session().host.clone(),
        dest_stats: dest_runtime_stats,
    });
    // close connections which will cause rcpd processes to exit and tracing tasks to finish
    source_connection.close();
    dest_connection.close();
    // wait for endpoint to become idle with a timeout to avoid blocking too long
    tokio::time::timeout(
        std::time::Duration::from_millis(500),
        server_endpoint.wait_idle(),
    )
    .await
    .ok();
    // wait for rcpd processes to fully exit and capture any error output
    for rcpd in rcpds {
        if let Err(e) = remote::wait_for_rcpd_process(rcpd).await {
            tracing::error!("Failed to wait for rcpd process: {e}");
        }
    }
    // wait for tracing tasks to complete (they should finish when streams close)
    // we ignore errors here since connection loss is expected during shutdown
    let _ = source_tracing_task.await;
    let _ = dest_tracing_task.await;
    tracing::info!("All rcpd processes finished");
    // propagate any errors from rcpd processes
    if !errors.is_empty() {
        let combined_error = errors.join("; ");
        tracing::error!("rcpd operation(s) failed: {combined_error}");
        return Err(common::copy::Error::new(
            anyhow::anyhow!("rcpd operation(s) failed: {combined_error}"),
            dest_summary,
        )
        .into());
    }
    // return summary from destination (source summary is empty/unused)
    Ok(dest_summary)
}

#[instrument]
async fn async_main(args: Args) -> anyhow::Result<common::copy::Summary> {
    if args.paths.len() < 2 {
        return Err(anyhow!(
            "You must specify at least one source path and one destination path!"
        ));
    }
    let src_strings = &args.paths[0..args.paths.len() - 1];
    for src in src_strings {
        if src == "." || src.ends_with("/.") {
            return Err(anyhow!(
                "expanding source directory ({:?}) using dot operator ('.') is not supported, please use absolute \
                path or '*' instead",
                std::path::PathBuf::from(src))
            );
        }
    }
    // choose parser based on --force-remote flag
    let parse_fn = if args.force_remote {
        path::parse_path_force_remote
    } else {
        path::parse_path
    };
    let parsed_srcs: Vec<path::PathType> = src_strings
        .iter()
        .map(|src| parse_fn(src))
        .collect::<anyhow::Result<Vec<_>>>()?;
    // pick the path type of the first source in the list and ensure all other sources match
    let first_src_path_type = parsed_srcs[0].clone();
    for path_type in &parsed_srcs[1..] {
        if *path_type != first_src_path_type {
            return Err(anyhow!(
                "Cannot mix different path types in the source list: {:?} and {:?}",
                first_src_path_type,
                path_type
            ));
        }
    }
    let dst_string = args.paths.last().unwrap();
    // validate destination path for problematic patterns (applies to both local and remote)
    path::validate_destination_path(dst_string)?;
    let dst_parsed = parse_fn(dst_string)?;
    // check if we have remote paths
    let has_remote_paths = match first_src_path_type {
        path::PathType::Remote(_) => true,
        path::PathType::Local(_) => matches!(dst_parsed, path::PathType::Remote(_)),
    };
    // for remote paths, we only support single source
    if has_remote_paths && src_strings.len() > 1 {
        return Err(anyhow!(
            "Multiple sources are currently not supported when using remote paths!"
        ));
    }
    // if any of the src/dst paths are remote, we'll be using the rcpd
    let remote_src_dst = if has_remote_paths {
        // resolve destination path with trailing slash logic for remote case
        let resolved_dst_string = path::resolve_destination_path(&src_strings[0], dst_string)?;
        let resolved_dst_path_type = parse_fn(&resolved_dst_string)?;
        match (first_src_path_type.clone(), resolved_dst_path_type) {
            (path::PathType::Remote(src_remote), path::PathType::Remote(dst_remote)) => {
                Some((src_remote, dst_remote))
            }
            (path::PathType::Remote(src_remote), path::PathType::Local(dst_local)) => {
                Some((src_remote, path::RemotePath::from_local(&dst_local)))
            }
            (path::PathType::Local(src_local), path::PathType::Remote(dst_remote)) => {
                Some((path::RemotePath::from_local(&src_local), dst_remote))
            }
            (path::PathType::Local(_), path::PathType::Local(_)) => None,
        }
    } else {
        None
    };
    if args.preserve_settings.is_some() && args.preserve {
        tracing::warn!("The --preserve flag is ignored when --preserve-settings is specified!");
    }
    let preserve = if let Some(preserve_settings) = &args.preserve_settings {
        common::parse_preserve_settings(preserve_settings)
            .map_err(|err| common::copy::Error::new(err, Default::default()))?
    } else if args.preserve {
        common::preserve::preserve_all()
    } else {
        common::preserve::preserve_default()
    };
    tracing::debug!("preserve settings: {:?}", &preserve);
    if let Some((mut remote_src, mut remote_dst)) = remote_src_dst {
        // expand remote '~' using remote HOME if needed
        let same_session = remote_src.session() == remote_dst.session();
        if same_session && (remote_src.needs_remote_home() || remote_dst.needs_remote_home()) {
            let home = remote::get_remote_home_for_session(remote_src.session()).await?;
            remote_src.apply_remote_home(&home);
            remote_dst.apply_remote_home(&home);
        } else {
            if remote_src.needs_remote_home() {
                let home = remote::get_remote_home_for_session(remote_src.session()).await?;
                remote_src.apply_remote_home(&home);
            }
            if remote_dst.needs_remote_home() {
                let home = remote::get_remote_home_for_session(remote_dst.session()).await?;
                remote_dst.apply_remote_home(&home);
            }
        }
        if !remote_src.path().is_absolute() || !remote_dst.path().is_absolute() {
            return Err(anyhow!(
                "Remote paths must be absolute after expansion: src={:?}, dst={:?}",
                remote_src.path(),
                remote_dst.path()
            ));
        }
        return match run_rcpd_master(&args, &preserve, &remote_src, &remote_dst).await {
            Ok(summary) => Ok(summary),
            Err(error) => {
                if let Some(copy_error) = error.downcast_ref::<common::copy::Error>() {
                    if args.summary {
                        return Err(anyhow!("{}\n\n{}", copy_error, &copy_error.summary));
                    }
                }
                Err(error)
            }
        };
    }
    // warn if paths had localhost: prefix but we're doing a local copy
    // (only check when not using --force-remote, since that's the opt-in for remote behavior)
    if !args.force_remote {
        let any_localhost_prefix = src_strings.iter().any(|s| path::has_localhost_prefix(s))
            || path::has_localhost_prefix(dst_string);
        if any_localhost_prefix {
            tracing::warn!(
                "Paths with 'localhost:' prefix are treated as local. \
                Use --force-remote to force remote copy via SSH."
            );
        }
    }
    // handle multiple sources only when destination ends with '/'
    if src_strings.len() > 1 && !dst_string.ends_with('/') {
        return Err(anyhow!(
            "Multiple sources can only be copied INTO a directory; if this is your intent - follow the \
            destination path with a trailing slash"
        ));
    }
    let src_dst: Vec<(std::path::PathBuf, std::path::PathBuf)> = src_strings
        .iter()
        .zip(parsed_srcs.iter())
        .map(|(src_str, parsed_src)| {
            let resolved_dst = path::resolve_destination_path(src_str, dst_string)?;
            // parse the resolved destination to handle localhost: prefix correctly
            let dst_path = match parse_fn(&resolved_dst)? {
                path::PathType::Local(p) => p,
                path::PathType::Remote(_) => {
                    return Err(anyhow!(
                        "Internal error: unexpected remote path in local copy branch"
                    ))
                }
            };
            let src_path = match parsed_src {
                path::PathType::Local(p) => p.clone(),
                path::PathType::Remote(_) => {
                    return Err(anyhow!(
                        "Internal error: unexpected remote path in local copy branch"
                    ))
                }
            };
            // check for existing destination only when not using trailing slash (single source case)
            if src_strings.len() == 1 && !dst_string.ends_with('/') && dst_path.exists() && !args.overwrite {
                return Err(anyhow!(
                    "Destination path {dst_path:?} already exists! \n\
                    If you want to copy INTO it, then follow the destination path with a trailing slash (/). Use \
                    --overwrite if you want to overwrite it"
                ));
            }
            Ok((src_path, dst_path))
        })
        .collect::<anyhow::Result<Vec<(std::path::PathBuf, std::path::PathBuf)>>>()?;
    let settings = common::copy::Settings {
        dereference: args.dereference,
        fail_early: args.fail_early,
        overwrite: args.overwrite,
        overwrite_compare: common::parse_metadata_cmp_settings(&args.overwrite_compare)
            .map_err(|err| common::copy::Error::new(err, Default::default()))?,
        chunk_size: args.chunk_size.0,
    };
    tracing::debug!("copy settings: {:?}", &settings);
    let mut join_set = tokio::task::JoinSet::new();
    for (src_path, dst_path) in src_dst {
        let do_copy =
            || async move { common::copy(&src_path, &dst_path, &settings, &preserve).await };
        join_set.spawn(do_copy());
    }
    let mut success = true;
    let mut copy_summary = common::copy::Summary::default();
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(result) => match result {
                Ok(summary) => copy_summary = copy_summary + summary,
                Err(error) => {
                    tracing::error!("{:#}", &error);
                    copy_summary = copy_summary + error.summary;
                    if args.fail_early {
                        if args.summary {
                            return Err(anyhow!("{}\n\n{}", error, &copy_summary));
                        }
                        return Err(anyhow!("{}", error));
                    }
                    success = false;
                }
            },
            Err(error) => {
                if settings.fail_early {
                    if args.summary {
                        return Err(anyhow!("{}\n\n{}", error, &copy_summary));
                    }
                    return Err(anyhow!("{}", error));
                }
            }
        }
    }
    if !success {
        if args.summary {
            return Err(anyhow!("rcp encountered errors\n\n{}", &copy_summary));
        }
        return Err(anyhow!("rcp encountered errors"));
    }
    Ok(copy_summary)
}

fn has_remote_paths(args: &Args) -> bool {
    for path in &args.paths {
        if matches!(path::parse_path(path), Ok(path::PathType::Remote(_))) {
            return true;
        }
    }
    false
}

fn main() -> Result<(), anyhow::Error> {
    // handle --protocol-version flag before parsing full arguments
    // this allows it to work without required arguments (paths)
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

    let args = Args::parse();
    let is_remote_operation = has_remote_paths(&args);
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    let output = common::OutputConfig {
        quiet: args.quiet,
        verbose: args.verbose,
        print_summary: args.summary,
    };
    let runtime = common::RuntimeConfig {
        max_workers: args.max_workers,
        max_blocking_threads: args.max_blocking_threads,
    };
    let throttle = common::ThrottleConfig {
        max_open_files: args.max_open_files,
        ops_throttle: args.ops_throttle,
        iops_throttle: args.iops_throttle,
        chunk_size: args.chunk_size.0,
    };
    let tracing = common::TracingConfig {
        remote_layer: None,
        debug_log_file: None,
        chrome_trace_prefix: args.chrome_trace.clone(),
        flamegraph_prefix: args.flamegraph.clone(),
        trace_identifier: "rcp-master".to_string(),
        profile_level: Some(args.profile_level.clone()),
        tokio_console: args.tokio_console,
        tokio_console_port: args.tokio_console_port,
    };
    let res = common::run(
        if args.progress || args.progress_type.is_some() {
            Some(common::ProgressSettings {
                progress_type: if is_remote_operation {
                    common::GeneralProgressType::RemoteMaster {
                        progress_type: args.progress_type.unwrap_or_default(),
                        get_progress_snapshot: Box::new(
                            remote::tracelog::get_latest_progress_snapshot,
                        ),
                    }
                } else {
                    common::GeneralProgressType::User(args.progress_type.unwrap_or_default())
                },
                progress_delay: args.progress_delay,
            })
        } else {
            None
        },
        output,
        runtime,
        throttle,
        tracing,
        func,
    );
    if res.is_none() {
        std::process::exit(1);
    }
    Ok(())
}
