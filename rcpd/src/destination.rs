use anyhow::Context;
use tracing::{instrument, Level};

#[instrument]
pub async fn run_destination(
    src_endpoint: &std::net::SocketAddr,
    src_server_name: &str,
) -> anyhow::Result<String> {
    // master_endpoint
    let dst_endpoint = "0.0.0.0:0".parse::<std::net::SocketAddr>().unwrap();
    let endpoint =
        quinn::Endpoint::client(dst_endpoint).context("Failed to create QUIC endpoint")?;

    let connection = endpoint.connect(*src_endpoint, src_server_name)?.await?;
    tracing::event!(Level::INFO, "Connected to QUIC server");

    // Accept incoming unidirectional streams
    while let Ok(mut recv_stream) = connection.accept_uni().await {
        tracing::event!(Level::INFO, "Received new unidirectional stream");

        // Read the incoming data
        let mut buf = Vec::new();
        match recv_stream.read_to_end(1024).await {
            Ok(data) => {
                buf.extend_from_slice(&data);
                tracing::event!(
                    Level::INFO,
                    "Received data: {}",
                    String::from_utf8_lossy(&buf)
                );
            }
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to read from stream: {}", e));
            }
        }
    }

    tracing::event!(Level::INFO, "QUIC client finished");
    Ok("QUIC client done".to_string())
}
