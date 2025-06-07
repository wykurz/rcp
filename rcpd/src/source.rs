use tracing::{instrument, Level};

async fn handle_connection(conn: quinn::Connecting) -> anyhow::Result<()> {
    let connection = conn.await?;
    tracing::event!(Level::INFO, "Destination connection established");
    let mut send_stream = connection.open_uni().await?;
    tracing::event!(Level::INFO, "Opened unidirectional stream");
    // TODO: send some test data
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
    _src: &std::path::Path,
    _source_config: &remote::protocol::SourceConfig,
    _rcpd_config: &remote::protocol::RcpdConfig,
) -> anyhow::Result<String> {
    let server_endpoint = remote::get_server(max_concurrent_streams)?;
    let server_addr = remote::get_endpoint_addr(&server_endpoint)?;
    tracing::event!(Level::INFO, "Source server listening on {}", server_addr);
    let master_hello = remote::protocol::SourceMasterHello {
        source_addr: server_addr,
        server_name: remote::get_random_server_name(),
    };
    tracing::event!(Level::INFO, "Sending master hello: {:?}", master_hello);
    let master_hello = bincode::serialize(&master_hello)?;
    master_connection.send_datagram(bytes::Bytes::from(master_hello))?;
    tracing::event!(Level::INFO, "Waiting for connection from destination");
    if let Some(conn) = server_endpoint.accept().await {
        tracing::event!(Level::INFO, "New destination connection incoming");
        handle_connection(conn).await?;
    } else {
        tracing::event!(Level::ERROR, "Timed out waiting for destination to connect");
        return Err(anyhow::anyhow!(
            "Timed out waiting for destination to connect"
        ));
    }
    tracing::event!(Level::INFO, "Source is done",);
    Ok("source OK".to_string())
}
