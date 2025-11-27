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
    /// The master (rcp) address to connect to
    #[arg(long)]
    master_addr: std::net::SocketAddr,

    /// The server name to use for the QUIC connection
    #[arg(long)]
    server_name: String,

    /// SHA-256 fingerprint of the Master's TLS certificate (hex-encoded)
    ///
    /// Used for certificate pinning to prevent MITM attacks
    #[arg(long)]
    master_cert_fingerprint: String,

    /// Role of this rcpd instance (source or destination)
    ///
    /// This is set by the master (rcp) to distinguish between source and destination
    /// rcpd processes, especially for same-host copies
    #[arg(long, value_name = "ROLE")]
    role: remote::protocol::RcpdRole,

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

    /// Exit on first error
    #[arg(short = 'e', long = "fail-early", help_heading = "Copy options")]
    fail_early: bool,

    /// Always follow symbolic links in source
    #[arg(short = 'L', long, help_heading = "Copy options")]
    dereference: bool,

    // Progress & output
    /// Show progress
    #[arg(long, help_heading = "Progress & output")]
    progress: bool,

    /// Set delay between progress updates
    ///
    /// Default is 200ms for interactive mode (`ProgressBar`) and 10s for non-interactive mode (`TextUpdates`). If specified, --progress flag is implied. Accepts human-readable durations like "200ms", "10s", "5min".
    #[arg(long, value_name = "DELAY", help_heading = "Progress & output")]
    progress_delay: Option<String>,

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

    /// Throttle the number of operations per second (0 = no throttle)
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
    chunk_size: u64,

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
    /// IP address to bind QUIC server to (set by master, internal use only)
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
    #[arg(
        long,
        default_value = "lan",
        value_name = "PROFILE",
        help_heading = "Remote copy options"
    )]
    network_profile: remote::NetworkProfile,

    /// Congestion control algorithm for QUIC (overrides profile default)
    #[arg(long, value_name = "ALGORITHM", help_heading = "Remote copy options")]
    congestion_control: Option<remote::CongestionControl>,

    /// QUIC connection-level receive window in bytes (overrides profile default)
    #[arg(long, value_name = "BYTES", help_heading = "Remote copy options")]
    quic_receive_window: Option<u64>,

    /// QUIC per-stream receive window in bytes (overrides profile default)
    #[arg(long, value_name = "BYTES", help_heading = "Remote copy options")]
    quic_stream_receive_window: Option<u64>,

    /// QUIC send window in bytes (overrides profile default)
    #[arg(long, value_name = "BYTES", help_heading = "Remote copy options")]
    quic_send_window: Option<u64>,

    /// Initial RTT estimate in milliseconds (overrides profile default)
    #[arg(long, value_name = "MS", help_heading = "Remote copy options")]
    quic_initial_rtt_ms: Option<u64>,

    /// Initial MTU in bytes (default: 1200)
    #[arg(long, value_name = "BYTES", help_heading = "Remote copy options")]
    quic_initial_mtu: Option<u16>,

    /// Enable file-based debug logging
    ///
    /// Example: /tmp/rcpd-log creates /tmp/rcpd-log-YYYY-MM-DDTHH-MM-SS-RANDOM
    #[arg(long, value_name = "PREFIX", help_heading = "Remote copy options")]
    debug_log_prefix: Option<String>,

    /// Print protocol version information as JSON and exit
    ///
    /// Used by rcp to verify version compatibility before launching remote operations
    #[arg(long)]
    protocol_version: bool,
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
                        tracing::warn!("stdin read error ({}), treating as master disconnect", e);
                        return;
                    }
                }
            }
        }
    }
}

/// async operation for rcpd - runs the actual source or destination logic
async fn run_operation(
    args: Args,
    master_connection: remote::streams::Connection,
) -> anyhow::Result<remote::protocol::RcpdResult> {
    // run source or destination
    let (master_send_stream, mut master_recv_stream) = master_connection.accept_bi().await?;
    let master_hello = master_recv_stream
        .recv_object::<remote::protocol::MasterHello>()
        .await
        .context("Failed to receive hello message from master")?
        .unwrap();
    tracing::info!("Received side: {:?}", master_hello);
    let settings = common::copy::Settings {
        dereference: args.dereference,
        fail_early: args.fail_early,
        overwrite: args.overwrite,
        overwrite_compare: common::parse_metadata_cmp_settings(&args.overwrite_compare)?,
        chunk_size: args.chunk_size,
    };
    let quic_config = remote::QuicConfig {
        port_ranges: args.quic_port_ranges.clone(),
        idle_timeout_sec: args.quic_idle_timeout_sec,
        keep_alive_interval_sec: args.quic_keep_alive_interval_sec,
        conn_timeout_sec: args.remote_copy_conn_timeout_sec,
        network_profile: args.network_profile,
        congestion_control: args.congestion_control,
        tuning: remote::QuicTuning {
            receive_window: args.quic_receive_window,
            stream_receive_window: args.quic_stream_receive_window,
            send_window: args.quic_send_window,
            initial_rtt_ms: args.quic_initial_rtt_ms,
            initial_mtu: args.quic_initial_mtu,
        },
    };
    let rcpd_result = match master_hello {
        remote::protocol::MasterHello::Source { src, dst } => {
            tracing::info!("Starting source");
            match source::run_source(
                master_send_stream.clone(),
                &src,
                &dst,
                &settings,
                &quic_config,
                args.bind_ip.as_deref(),
            )
            .await
            {
                Ok((message, summary)) => {
                    let runtime_stats = common::collect_runtime_stats();
                    remote::protocol::RcpdResult::Success {
                        message,
                        summary,
                        runtime_stats,
                    }
                }
                Err(error) => {
                    let runtime_stats = common::collect_runtime_stats();
                    remote::protocol::RcpdResult::Failure {
                        error: format!("{error:#}"),
                        summary: common::copy::Summary::default(),
                        runtime_stats,
                    }
                }
            }
        }
        remote::protocol::MasterHello::Destination {
            source_addr,
            server_name,
            source_cert_fingerprint,
            preserve,
        } => {
            tracing::info!("Starting destination");
            match destination::run_destination(
                &source_addr,
                &server_name,
                &source_cert_fingerprint,
                &settings,
                &preserve,
                &quic_config,
            )
            .await
            {
                Ok((message, summary)) => {
                    let runtime_stats = common::collect_runtime_stats();
                    remote::protocol::RcpdResult::Success {
                        message,
                        summary,
                        runtime_stats,
                    }
                }
                Err(error) => {
                    let runtime_stats = common::collect_runtime_stats();
                    remote::protocol::RcpdResult::Failure {
                        error: format!("{error:#}"),
                        summary: common::copy::Summary::default(),
                        runtime_stats,
                    }
                }
            }
        }
    };
    tracing::debug!("Closing master send stream");
    {
        let mut master_send_stream = master_send_stream.lock().await;
        master_send_stream
            .send_control_message(&rcpd_result)
            .await?;
        master_send_stream.close().await?;
    }
    Ok(rcpd_result)
}

#[instrument]
async fn async_main(
    args: Args,
    tracing_receiver: tokio::sync::mpsc::UnboundedReceiver<common::remote_tracing::TracingMessage>,
) -> anyhow::Result<String> {
    tracing::info!(
        "Connecting to master {} (server name: {})",
        args.master_addr,
        args.server_name
    );
    // decode hex-encoded master cert fingerprint
    let master_cert_fingerprint =
        hex::decode(&args.master_cert_fingerprint).with_context(|| {
            format!(
                "Failed to decode master cert fingerprint: {}",
                args.master_cert_fingerprint
            )
        })?;
    // build QUIC config with profile and tuning settings
    let quic_config = remote::QuicConfig {
        port_ranges: args.quic_port_ranges.clone(),
        idle_timeout_sec: args.quic_idle_timeout_sec,
        keep_alive_interval_sec: args.quic_keep_alive_interval_sec,
        conn_timeout_sec: args.remote_copy_conn_timeout_sec,
        network_profile: args.network_profile,
        congestion_control: args.congestion_control,
        tuning: remote::QuicTuning {
            receive_window: args.quic_receive_window,
            stream_receive_window: args.quic_stream_receive_window,
            send_window: args.quic_send_window,
            initial_rtt_ms: args.quic_initial_rtt_ms,
            initial_mtu: args.quic_initial_mtu,
        },
    };
    // use certificate pinning for Master→rcpd connection
    let client = remote::get_client_with_config_and_pinning(&quic_config, master_cert_fingerprint)?;
    let master_connection = {
        let master_connection = client
            .connect(args.master_addr, &args.server_name)?
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to master at {}. \
                    This usually means the master is unreachable from this host. \
                    Check network connectivity and firewall rules.",
                    args.master_addr
                )
            })?;
        remote::streams::Connection::new(master_connection)
    };
    tracing::info!("Connected to master");
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
    // only start monitoring stdin if it's actually available
    let stdin_watchdog = if stdin_available {
        Some(tokio::spawn(stdin_monitor()))
    } else {
        None
    };
    let mut tracing_stream = master_connection.open_uni().await?;
    tracing_stream
        .send_control_message(&remote::protocol::TracingHello { role: args.role })
        .await?;
    // setup tracing
    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let tracing_sender_task = tokio::spawn(remote::tracelog::run_sender(
        tracing_receiver,
        tracing_stream,
        cancellation_token.clone(),
    ));
    // run operation with stdin monitoring (if available)
    // if stdin closes while running, abort immediately
    let rcpd_result = if let Some(watchdog) = stdin_watchdog {
        // stdin is available - monitor for disconnection
        tokio::select! {
            result = run_operation(args.clone(), master_connection.clone()) => {
                match result {
                    Ok(r) => r,
                    Err(e) => {
                        let runtime_stats = common::collect_runtime_stats();
                        remote::protocol::RcpdResult::Failure {
                            error: format!("{e:#}"),
                            summary: common::copy::Summary::default(),
                            runtime_stats,
                        }
                    }
                }
            }
            _ = watchdog => {
                // stdin closed - master disconnected, exit immediately
                // no point in cleanup since master is dead and can't receive results
                tracing::error!(
                    "Master (rcp) disconnected - stdin closed. \
                     This usually means the master process was killed or the SSH connection was terminated. \
                     Exiting immediately."
                );
                std::process::exit(1);
            }
        }
    } else {
        // stdin not available - rely on QUIC timeouts only
        match run_operation(args.clone(), master_connection.clone()).await {
            Ok(r) => r,
            Err(e) => {
                let runtime_stats = common::collect_runtime_stats();
                remote::protocol::RcpdResult::Failure {
                    error: format!("{e:#}"),
                    summary: common::copy::Summary::default(),
                    runtime_stats,
                }
            }
        }
    };
    // shutdown tracing sender with timeout to handle dead connections
    cancellation_token.cancel();
    tracing::debug!("Cancelling tracing sender");
    match tokio::time::timeout(std::time::Duration::from_secs(2), tracing_sender_task).await {
        Ok(Ok(Ok(_))) => tracing::debug!("Tracing sender shut down cleanly"),
        Ok(Ok(Err(e))) => tracing::warn!("Tracing sender task failed: {e}"),
        Ok(Err(e)) => tracing::warn!("Tracing sender task panicked: {e}"),
        Err(_) => tracing::warn!("Tracing sender shutdown timed out (master likely disconnected)"),
    }
    master_connection.close();
    // wait for client to become idle with timeout
    match tokio::time::timeout(std::time::Duration::from_secs(2), client.wait_idle()).await {
        Ok(_) => tracing::debug!("QUIC client became idle"),
        Err(_) => tracing::warn!("QUIC client idle timeout (master likely disconnected)"),
    }
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
            tracing::error!("rcpd operation failed: {error}");
            Err(anyhow::anyhow!("rcpd operation failed: {error}"))
        }
    }
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

    let args = Args::parse();
    let (tracing_layer, tracing_sender, tracing_receiver) =
        common::remote_tracing::RemoteTracingLayer::new();
    let func = {
        let args = args.clone();
        || async_main(args, tracing_receiver)
    };
    let debug_log_file = args.debug_log_prefix.as_ref().map(|prefix| {
        let filename = common::generate_debug_log_filename(prefix);
        println!("rcpd: Debug logging to file: {filename}");
        filename
    });
    let output = common::OutputConfig {
        quiet: args.quiet,
        verbose: args.verbose,
        print_summary: false,
    };
    let runtime = common::RuntimeConfig {
        max_workers: args.max_workers,
        max_blocking_threads: args.max_blocking_threads,
    };
    let throttle = common::ThrottleConfig {
        max_open_files: args.max_open_files,
        ops_throttle: args.ops_throttle,
        iops_throttle: args.iops_throttle,
        chunk_size: args.chunk_size,
    };
    let tracing = common::TracingConfig {
        remote_layer: Some(tracing_layer),
        debug_log_file,
    };
    let res = common::run(
        if args.progress {
            Some(common::ProgressSettings {
                progress_type: common::GeneralProgressType::Remote(tracing_sender),
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
