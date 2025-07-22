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
}

#[instrument]
async fn async_main(
    args: Args,
    tracing_receiver: tokio::sync::mpsc::UnboundedReceiver<common::remote_tracing::TracingMessage>,
) -> anyhow::Result<String> {
    let client = remote::get_client()?;
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
    let tracing_sender_task = tokio::spawn(remote::run_remote_tracing_sender(
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
    let result = match master_hello {
        remote::protocol::MasterHello::Source {
            src,
            dst,
            source_config,
            rcpd_config,
        } => {
            tracing::info!("Starting source");
            source::run_source(
                master_send_stream.clone(),
                &src,
                &dst,
                &source_config,
                &rcpd_config,
            )
            .await?
        }
        remote::protocol::MasterHello::Destination {
            source_addr,
            server_name,
            destination_config,
            rcpd_config,
        } => {
            tracing::info!("Starting destination");
            destination::run_destination(
                &source_addr,
                &server_name,
                &destination_config,
                &rcpd_config,
            )
            .await?
        }
    };
    tracing::debug!("Closing master send stream");
    {
        let mut master_send_stream = master_send_stream.lock().await;
        master_send_stream
            .send_control_message(&remote::protocol::RcpdGoodBye {})
            .await?;
        master_send_stream.close().await?;
    }
    // shutdown tracing sender
    cancellation_token.cancel();
    tracing::debug!("Cancelling tracing sender");
    tracing_sender_task.await??;
    master_connection.close();
    client.wait_idle().await;
    Ok(result)
}

fn main() -> Result<(), anyhow::Error> {
    let args = Args::from_args();
    // let master_connection = client.connect(args.master_addr, &args.server_name)?.await?;
    let (tracing_layer, tracing_receiver) = common::remote_tracing::RemoteTracingLayer::new();
    // TODO: signal cancellation_token when the process is about to exit
    let func = {
        let args = args.clone();
        || async_main(args, tracing_receiver)
    };
    let res = common::run(
        None,
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
        func,
    );
    if res.is_none() {
        std::process::exit(1);
    }
    Ok(())
}
