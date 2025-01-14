use anyhow::{Context, Result};
use structopt::StructOpt;
use tokio::net::TcpListener;
use tracing::{event, instrument, Level};

#[derive(StructOpt, Debug, Clone)]
#[structopt(
    name = "rcpd",
    about = "`rcpd` is used by the `rcp` command for performing remote data copies. Please see `rcp` for more \
information."
)]
struct Args {
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

    /// Throttle the number of opearations per second, 0 means no throttle
    #[structopt(long, default_value = "0")]
    ops_throttle: usize,
}

async fn handle_connection(socket: &mut tokio::net::TcpStream) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    // Send a simple OK response
    socket
        .write_all(b"OK\n")
        .await
        .context("Failed to write to socket")?;
    Ok(())
}

#[instrument]
async fn async_main(args: Args) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:8080")
        .await
        .context("Failed to bind to port 8080")?;
    event!(Level::INFO, "Server listening on 127.0.0.1:8080");
    loop {
        let (mut socket, addr) = listener
            .accept()
            .await
            .context("Failed to accept connection")?;
        event!(Level::INFO, "New connection from {}", addr);
        // Spawn a new task for each connection
        tokio::spawn(async move {
            if let Err(e) = handle_connection(&mut socket).await {
                event!(Level::ERROR, "Error handling connection: {}", e);
            }
        });
    }
}

fn main() -> Result<(), anyhow::Error> {
    let args = Args::from_args();
    let func = {
        let args = args.clone();
        || async_main(args)
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
        func,
    );
    match res {
        Ok(_) => std::process::exit(0),
        Err(_) => std::process::exit(1),
    }
}
