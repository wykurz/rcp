use anyhow::Context;
use structopt::StructOpt;
use tracing::instrument;

mod destination;
mod directory_tracker;
mod source;

#[derive(structopt::StructOpt, std::fmt::Debug, std::clone::Clone)]
#[structopt(
    name = "rcpd",
    about = "`rcpd` is used by the `rcp` command for performing remote data copies. Please see `rcp` for more \
information."
)]
struct Args {
    /// The master (rcp) address to connect to
    #[structopt(long, required = true)]
    master_addr: std::net::SocketAddr,

    /// The server name to use for the QUIC connection
    #[structopt(long, required = true)]
    server_name: String,

    /// Overwrite existing files/directories
    #[structopt(short, long)]
    overwrite: bool,

    /// Comma separated list of file attributes to compare when when deciding if files are "identical", used with
    /// --overwrite flag.
    /// Options are: uid, gid, mode, size, mtime, ctime
    #[structopt(long, default_value = "size,mtime")]
    overwrite_compare: String,

    /// Exit on first error
    #[structopt(short = "-e", long = "fail-early")]
    fail_early: bool,

    /// Show progress
    #[structopt(long)]
    progress: bool,

    /// Sets the delay between progress updates.
    ///
    /// - For the interactive (--progress-type=ProgressBar), the default is 200ms.
    /// - For the non-interactive (--progress-type=TextUpdates), the default is 10s.
    ///
    /// If specified, --progress flag is implied.
    ///
    /// This option accepts a human readable duration, e.g. "200ms", "10s", "5min" etc.
    #[structopt(long)]
    progress_delay: Option<String>,

    /// Always follow symbolic links in source
    #[structopt(short = "-L", long)]
    dereference: bool,

    /// Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: u8,

    /// Quiet mode, don't report errors
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// Number of worker threads, 0 means number of cores
    #[structopt(long, default_value = "0")]
    max_workers: usize,

    /// Number of blocking worker threads, 0 means Tokio runtime default (512)
    #[structopt(long, default_value = "0")]
    max_blocking_threads: usize,

    /// Maximum number of open files, 0 means no limit, leaving unspecified means using 80% of max open files system
    /// limit
    #[structopt(long)]
    max_open_files: Option<usize>,

    /// Throttle the number of operations per second, 0 means no throttle
    #[structopt(long, default_value = "0")]
    ops_throttle: usize,

    /// Throttle the number of I/O operations per second, 0 means no throttle.
    ///
    /// I/O is calculated based on provided chunk size -- number of I/O operations for a file is calculated as:
    /// ((file size - 1) / chunk size) + 1
    #[structopt(long, default_value = "0")]
    iops_throttle: usize,

    /// Chunk size used to calculate number of I/O per file.
    ///
    /// Modifying this setting to a value > 0 is REQUIRED when using --iops-throttle.
    #[structopt(long, default_value = "0")]
    chunk_size: u64,

    /// Throttle the number of bytes per second, 0 means no throttle
    #[structopt(long, default_value = "0")]
    tput_throttle: usize,

    /// Enable file-based debug logging with given prefix
    #[structopt(long)]
    debug_log_prefix: Option<String>,

    /// Restrict QUIC binding to specific port ranges (e.g., "8000-8999,10000-10999")
    /// If not specified, uses dynamic port allocation (default behavior)
    #[structopt(long)]
    quic_port_ranges: Option<String>,
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
    let client = remote::get_client_with_port_ranges(args.quic_port_ranges.as_deref())?;
    let master_connection = {
        let master_connection = client.connect(args.master_addr, &args.server_name)?.await?;
        remote::streams::Connection::new(master_connection)
    };
    tracing::info!("Connected to master");
    let mut tracing_stream = master_connection.open_uni().await?;
    tracing_stream
        .send_control_message(&remote::protocol::TracingHello {})
        .await?;
    // setup tracing
    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let tracing_sender_task = tokio::spawn(remote::tracelog::run_sender(
        tracing_receiver,
        tracing_stream,
        cancellation_token.clone(),
    ));
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
    let rcpd_result = match master_hello {
        remote::protocol::MasterHello::Source { src, dst } => {
            tracing::info!("Starting source");
            match source::run_source(
                master_send_stream.clone(),
                &src,
                &dst,
                &settings,
                args.quic_port_ranges.as_deref(),
            )
            .await
            {
                Ok(message) => remote::protocol::RcpdResult::Success { message },
                Err(error) => remote::protocol::RcpdResult::Failure {
                    error: format!("{error:#}"),
                },
            }
        }
        remote::protocol::MasterHello::Destination {
            source_addr,
            server_name,
            preserve,
        } => {
            tracing::info!("Starting destination");
            match destination::run_destination(&source_addr, &server_name, &settings, &preserve)
                .await
            {
                Ok(message) => remote::protocol::RcpdResult::Success { message },
                Err(error) => remote::protocol::RcpdResult::Failure {
                    error: format!("{error:#}"),
                },
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
    // shutdown tracing sender
    cancellation_token.cancel();
    tracing::debug!("Cancelling tracing sender");
    tracing_sender_task.await??;
    master_connection.close();
    client.wait_idle().await;
    match rcpd_result {
        remote::protocol::RcpdResult::Success { message } => Ok(message),
        remote::protocol::RcpdResult::Failure { error } => {
            tracing::error!("rcpd operation failed: {error}");
            Err(anyhow::anyhow!("rcpd operation failed: {error}"))
        }
    }
}

fn main() -> Result<(), anyhow::Error> {
    let args = Args::from_args();
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
    let res = common::run(
        if args.progress {
            Some(common::ProgressSettings {
                progress_type: common::GeneralProgressType::Remote(tracing_sender),
                progress_delay: args.progress_delay,
            })
        } else {
            None
        },
        args.quiet,
        args.verbose,
        false,
        args.max_workers,
        args.max_blocking_threads,
        args.max_open_files,
        args.ops_throttle,
        args.iops_throttle,
        args.chunk_size,
        args.tput_throttle,
        Some(tracing_layer),
        debug_log_file,
        func,
    );
    if res.is_none() {
        std::process::exit(1);
    }
    Ok(())
}
