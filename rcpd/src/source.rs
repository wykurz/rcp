use anyhow::Context;
use rand::{distributions::Alphanumeric, Rng};
use tracing::{instrument, Level};

// FIXME
#[allow(dead_code)]
async fn handle_quic_connection(conn: quinn::Connecting) -> anyhow::Result<()> {
    let connection = conn.await?;
    tracing::event!(Level::INFO, "QUIC connection established");

    // Open a unidirectional stream for sending data
    let mut send_stream = connection.open_uni().await?;
    tracing::event!(Level::INFO, "Opened unidirectional stream");

    // Send some test data
    match send_stream.write_all(b"Hello from QUIC server!\n").await {
        Ok(()) => {
            tracing::event!(Level::INFO, "Data sent successfully");
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

#[instrument]
pub async fn run_source(max_concurrent_streams: u32) -> anyhow::Result<String> {
    let server_config = remote::configure_server(max_concurrent_streams)?;
    // Bind to a random port by using port 0
    let addr = "0.0.0.0:0".parse::<std::net::SocketAddr>().unwrap();
    let endpoint =
        quinn::Endpoint::server(server_config, addr).context("Failed to create QUIC endpoint")?;
    let bound_addr = endpoint
        .local_addr()
        .context("Failed to get local address")?;

    tracing::event!(Level::INFO, "QUIC server listening on {}", bound_addr);

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
        tracing::event!(Level::INFO, "New QUIC connection incoming");
        handle_quic_connection(conn).await?;
    }

    tracing::event!(Level::INFO, "QUIC server is done",);

    Ok("whee".to_string())
}
