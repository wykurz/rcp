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
pub async fn run_source(
    master_connection: &quinn::Connection,
    max_concurrent_streams: u32,
) -> anyhow::Result<String> {
    let server_endpoint = remote::get_server(max_concurrent_streams)?;
    let server_addr = remote::get_endpoint_addr(&server_endpoint)?;
    tracing::event!(Level::INFO, "QUIC server listening on {}", server_addr);
    let server_name = remote::get_random_server_name();
    let master_hello = remote::protocol::SourceMasterHello {
        source_addr: server_addr,
        server_name: server_name.clone(),
    };
    tracing::event!(Level::INFO, "Sending master hello: {:?}", master_hello);
    let master_hello = bincode::serialize(&master_hello)?;
    master_connection.send_datagram(bytes::Bytes::from(master_hello))?;
    // start accepting connections from destination
    if let Some(conn) = server_endpoint.accept().await {
        tracing::event!(Level::INFO, "New QUIC connection incoming");
        handle_quic_connection(conn).await?;
    }
    tracing::event!(Level::INFO, "QUIC server is done",);
    Ok("whee".to_string())
}
