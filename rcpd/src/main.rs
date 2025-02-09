use anyhow::Context;
use rand::{distributions::Alphanumeric, Rng};
use structopt::StructOpt;
use tracing::{event, instrument, Level};

#[derive(structopt::StructOpt, std::fmt::Debug, std::clone::Clone)]
#[structopt(
    name = "rcpd",
    about = "`rcpd` is used by the `rcp` command for performing remote data copies. Please see `rcp` for more \
information."
)]
struct Args {
    /// Which side of the connection this daemon represents (source/destination)
    #[structopt(long, required = true)]
    side: remote::Side,

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

    /// Maximum number of concurrent QUIC streams, default is 1000
    #[structopt(long, default_value = "1000")]
    max_concurrent_streams: u32,
}

#[instrument]
async fn handle_connection(socket: &mut tokio::net::TcpStream) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    // Send a simple OK response
    socket
        .write_all(b"OK\n")
        .await
        .context("Failed to write to socket")?;
    Ok(())
}

async fn handle_quic_connection(conn: quinn::Connecting) -> anyhow::Result<()> {
    let connection = conn.await?;
    event!(Level::INFO, "QUIC connection established");

    // Open a unidirectional stream for sending data
    let mut send_stream = connection.open_uni().await?;
    event!(Level::INFO, "Opened unidirectional stream");

    // Send some test data
    match send_stream.write_all(b"Hello from QUIC server!\n").await {
        Ok(()) => {
            event!(Level::INFO, "Data sent successfully");
            // Properly finish the stream
            send_stream.finish().await?;
        }
        Err(quinn::WriteError::ConnectionLost(e)) => {
            return Err(anyhow::anyhow!("Connection lost: {}", e));
        }
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to send data: {}", e));
        }
    }

    Ok(())
}

fn configure_server(args: &Args) -> anyhow::Result<quinn::ServerConfig> {
    // Generate a self-signed certificate for testing
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let key_der = cert.serialize_private_key_der();
    let cert_der = cert.serialize_der()?;

    let key = rustls::PrivateKey(key_der);
    let cert = rustls::Certificate(cert_der);

    let mut server_config = quinn::ServerConfig::with_single_cert(vec![cert], key)
        .context("Failed to create server config")?;

    // Configure the server for better performance
    std::sync::Arc::get_mut(&mut server_config.transport)
        .expect("Failed to get transport config")
        .max_concurrent_uni_streams(args.max_concurrent_streams.into())
        .max_idle_timeout(Some(tokio::time::Duration::from_secs(30).try_into()?));

    Ok(server_config)
}

async fn async_main(args: Args) -> anyhow::Result<String> {
    match &args.side {
        remote::Side::Source => {
            // Configure QUIC server
            let server_config = configure_server(&args)?;

            // Bind to a random port by using port 0
            let addr = "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap();
            let endpoint = quinn::Endpoint::server(server_config, addr)
                .context("Failed to create QUIC endpoint")?;

            let bound_addr = endpoint
                .local_addr()
                .context("Failed to get local address")?;

            event!(Level::INFO, "QUIC server listening on {}", bound_addr);

            // Generate random server name
            let server_name: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(20)
                .map(char::from)
                .collect();

            // Print port and server name to stdout for the client to use
            println!("{} {}", bound_addr.port(), server_name);

            // Keep accepting connections
            if let Some(conn) = endpoint.accept().await {
                event!(Level::INFO, "New QUIC connection incoming");
                handle_quic_connection(conn).await?;
            }

            event!(Level::INFO, "QUIC server is done",);

            Ok("whee".to_string())
        }
        remote::Side::Destination {
            src_endpoint,
            server_name,
        } => {
            let dst_endpoint = "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap();
            let endpoint =
                quinn::Endpoint::client(dst_endpoint).context("Failed to create QUIC endpoint")?;

            let connection = endpoint.connect(*src_endpoint, server_name)?.await?;
            tracing::event!(tracing::Level::INFO, "Connected to QUIC server");

            // Accept incoming unidirectional streams
            while let Ok(mut recv_stream) = connection.accept_uni().await {
                tracing::event!(tracing::Level::INFO, "Received new unidirectional stream");

                // Read the incoming data
                let mut buf = Vec::new();
                match recv_stream.read_to_end(1024).await {
                    Ok(data) => {
                        buf.extend_from_slice(&data);
                        tracing::event!(
                            tracing::Level::INFO,
                            "Received data: {}",
                            String::from_utf8_lossy(&buf)
                        );
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("Failed to read from stream: {}", e));
                    }
                }
            }

            tracing::event!(tracing::Level::INFO, "QUIC client finished");
            Ok("QUIC client done".to_string())
        }
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
