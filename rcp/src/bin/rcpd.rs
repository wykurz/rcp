use anyhow::Context;
use clap::Parser;
use tokio::io::AsyncReadExt;
use tracing::instrument;

use rcp_tools_rcp::{destination, source};

fn parse_nonzero_usize(s: &str) -> Result<usize, String> {
    let val: usize = s.parse().map_err(|e| format!("{e}"))?;
    if val == 0 {
        return Err("value must be at least 1".to_string());
    }
    Ok(val)
}

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
    /// IP address to bind TCP server to (set by master, internal use only)
    #[arg(long, value_name = "IP", help_heading = "Remote copy options")]
    bind_ip: Option<String>,

    /// Restrict TCP to specific port ranges (e.g., "8000-8999,10000-10999")
    ///
    /// Defaults to dynamic port allocation if not specified
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
    /// Applies to: rcpd→master connection, destination→source connection
    #[arg(
        long,
        default_value = "15",
        value_name = "N",
        help_heading = "Remote copy options"
    )]
    remote_copy_conn_timeout_sec: u64,

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

    /// Maximum concurrent TCP connections for file transfers (default: 100)
    #[arg(
        long,
        default_value = "100",
        value_name = "N",
        help_heading = "Remote copy options"
    )]
    max_connections: usize,

    /// Multiplier for pending file writes (default: 4)
    ///
    /// Controls backpressure by limiting pending file transfers to
    /// max_connections × multiplier.
    #[arg(
        long,
        default_value = "4",
        value_name = "N",
        value_parser = parse_nonzero_usize,
        help_heading = "Remote copy options"
    )]
    pending_writes_multiplier: usize,

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
async fn run_operation<W, R>(
    args: Args,
    mut master_send_stream: remote::streams::SendStream<W>,
    mut master_recv_stream: remote::streams::RecvStream<R>,
    cert_key: Option<remote::tls::CertifiedKey>,
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
        .unwrap();
    tracing::info!("Received side: {:?}", master_hello);
    // build tcp_config first so we can use its effective_buffer_size()
    let tcp_config = remote::TcpConfig {
        port_ranges: args.port_ranges.clone(),
        conn_timeout_sec: args.remote_copy_conn_timeout_sec,
        network_profile: args.network_profile,
        buffer_size: args.buffer_size,
        max_connections: args.max_connections,
        pending_writes_multiplier: args.pending_writes_multiplier,
    };
    let rcpd_result = match master_hello {
        remote::protocol::MasterHello::Source {
            src,
            dst,
            dest_cert_fingerprint,
            filter,
            dry_run,
        } => {
            // build settings with filter from MasterHello
            let settings = common::copy::Settings {
                dereference: args.dereference,
                fail_early: args.fail_early,
                overwrite: args.overwrite,
                overwrite_compare: common::parse_metadata_cmp_settings(&args.overwrite_compare)?,
                chunk_size: args.chunk_size,
                remote_copy_buffer_size: tcp_config.effective_buffer_size(),
                filter,
                dry_run,
            };
            tracing::info!("Starting source");
            let shared_send = std::sync::Arc::new(tokio::sync::Mutex::new(master_send_stream));
            let result = match source::run_source(
                shared_send.clone(),
                &src,
                &dst,
                &settings,
                &tcp_config,
                args.bind_ip.as_deref(),
                cert_key.as_ref(),
                dest_cert_fingerprint,
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
            };
            // send result back to master
            {
                let mut send = shared_send.lock().await;
                send.send_control_message(&result).await?;
                send.close().await?;
            }
            result
        }
        remote::protocol::MasterHello::Destination {
            source_control_addr,
            source_data_addr,
            server_name,
            preserve,
            source_cert_fingerprint,
        } => {
            // destination doesn't use filter (filtering happens at source).
            // empty directory cleanup decisions are communicated per-directory
            // via keep_if_empty in the Directory message.
            let settings = common::copy::Settings {
                dereference: args.dereference,
                fail_early: args.fail_early,
                overwrite: args.overwrite,
                overwrite_compare: common::parse_metadata_cmp_settings(&args.overwrite_compare)?,
                chunk_size: args.chunk_size,
                remote_copy_buffer_size: tcp_config.effective_buffer_size(),
                filter: None,
                dry_run: None,
            };
            tracing::info!("Starting destination");
            match destination::run_destination(
                &source_control_addr,
                &source_data_addr,
                &server_name,
                &settings,
                &preserve,
                &tcp_config,
                cert_key.as_ref(),
                source_cert_fingerprint,
            )
            .await
            {
                Ok((message, summary)) => {
                    // send result to master
                    master_send_stream
                        .send_control_message(&remote::protocol::RcpdResult::Success {
                            message: message.clone(),
                            summary,
                            runtime_stats: common::collect_runtime_stats(),
                        })
                        .await?;
                    master_send_stream.close().await?;
                    let runtime_stats = common::collect_runtime_stats();
                    remote::protocol::RcpdResult::Success {
                        message,
                        summary,
                        runtime_stats,
                    }
                }
                Err(error) => {
                    let runtime_stats = common::collect_runtime_stats();
                    let result = remote::protocol::RcpdResult::Failure {
                        error: format!("{error:#}"),
                        summary: common::copy::Summary::default(),
                        runtime_stats,
                    };
                    master_send_stream.send_control_message(&result).await?;
                    master_send_stream.close().await?;
                    result
                }
            }
        }
    };
    Ok(rcpd_result)
}

#[instrument]
async fn async_main(
    args: Args,
    tracing_receiver: tokio::sync::mpsc::UnboundedReceiver<common::remote_tracing::TracingMessage>,
) -> anyhow::Result<String> {
    // install rustls crypto provider (ring) before any TLS operations
    if !args.no_encryption {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok(); // ignore if already installed
    }
    // build TCP config for listener creation
    let tcp_config = remote::TcpConfig {
        port_ranges: args.port_ranges.clone(),
        conn_timeout_sec: args.remote_copy_conn_timeout_sec,
        network_profile: args.network_profile,
        buffer_size: args.buffer_size,
        max_connections: args.max_connections,
        pending_writes_multiplier: args.pending_writes_multiplier,
    };
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
    // format: "RCP_TLS <addr> <fingerprint>" or "RCP_TCP <addr>"
    if let Some(ref cert) = cert_key {
        let fingerprint_hex = remote::tls::fingerprint_to_hex(&cert.fingerprint);
        eprintln!("RCP_TLS {} {}", listen_addr, fingerprint_hex);
    } else {
        eprintln!("RCP_TCP {}", listen_addr);
    }
    // flush stderr to ensure master receives the line immediately
    use std::io::Write;
    std::io::stderr()
        .flush()
        .context("failed to flush stderr")?;
    tracing::info!("Listening for master connections on {}", listen_addr);
    let conn_timeout = std::time::Duration::from_secs(args.remote_copy_conn_timeout_sec);
    // helper to accept a connection and optionally wrap with TLS
    async fn accept_connection(
        listener: &tokio::net::TcpListener,
        tls_acceptor: Option<&tokio_rustls::TlsAcceptor>,
        timeout: std::time::Duration,
        purpose: &str,
    ) -> anyhow::Result<(
        remote::streams::BoxedSendStream,
        remote::streams::BoxedRecvStream,
    )> {
        let (stream, addr) = tokio::time::timeout(timeout, listener.accept())
            .await
            .with_context(|| format!("timeout waiting for master {} connection", purpose))?
            .with_context(|| format!("failed to accept {} connection", purpose))?;
        tracing::info!("Accepted {} connection from {}", purpose, addr);
        stream.set_nodelay(true)?;
        if let Some(acceptor) = tls_acceptor {
            let tls_stream = acceptor
                .accept(stream)
                .await
                .with_context(|| format!("TLS handshake failed for {} connection", purpose))?;
            let (read_half, write_half) = tokio::io::split(tls_stream);
            let recv_stream =
                remote::streams::RecvStream::new(Box::new(read_half) as remote::streams::BoxedRead);
            let send_stream = remote::streams::SendStream::new(
                Box::new(write_half) as remote::streams::BoxedWrite
            );
            Ok((send_stream, recv_stream))
        } else {
            let (read_half, write_half) = stream.into_split();
            let recv_stream =
                remote::streams::RecvStream::new(Box::new(read_half) as remote::streams::BoxedRead);
            let send_stream = remote::streams::SendStream::new(
                Box::new(write_half) as remote::streams::BoxedWrite
            );
            Ok((send_stream, recv_stream))
        }
    }
    // accept control connection (TCP + TLS handshake immediately)
    let (master_send_stream, master_recv_stream) =
        accept_connection(&listener, tls_acceptor.as_ref(), conn_timeout, "control").await?;
    // accept tracing connection (TCP + TLS handshake immediately)
    let (tracing_send_stream, _tracing_recv_stream) =
        accept_connection(&listener, tls_acceptor.as_ref(), conn_timeout, "tracing").await?;
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
                tracing::warn!("Tracing sender failed: {e}");
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
    // only start monitoring stdin if it's actually available
    let stdin_watchdog = if stdin_available {
        Some(tokio::spawn(stdin_monitor()))
    } else {
        None
    };
    // run operation with stdin monitoring (if available)
    // if stdin closes while running, abort immediately
    let rcpd_result = if let Some(watchdog) = stdin_watchdog {
        // stdin is available - monitor for disconnection
        // CANCEL SAFETY: both branches are cancel-safe. `run_operation` is a
        // high-level future that can be dropped safely. When the watchdog
        // branch wins (stdin closed), we exit(1) immediately so there's no
        // concern about partial state from the cancelled `run_operation`.
        tokio::select! {
            result = run_operation(args.clone(), master_send_stream, master_recv_stream, cert_key.clone()) => {
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
        // stdin not available - rely on TCP timeouts only
        match run_operation(
            args.clone(),
            master_send_stream,
            master_recv_stream,
            cert_key.clone(),
        )
        .await
        {
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
    // cancel tracing task and wait for it to finish
    tracing_cancel.cancel();
    let _ = tracing_task.await;
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
        chrome_trace_prefix: args.chrome_trace.clone(),
        flamegraph_prefix: args.flamegraph.clone(),
        trace_identifier: format!("rcpd-{}", args.role),
        profile_level: Some(args.profile_level.clone()),
        tokio_console: args.tokio_console,
        tokio_console_port: args.tokio_console_port,
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
