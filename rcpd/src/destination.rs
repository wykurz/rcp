use tracing::{instrument, Level};

#[instrument]
pub async fn run_destination(
    src_endpoint: &std::net::SocketAddr,
    src_server_name: &str,
) -> anyhow::Result<String> {
    let client = remote::get_client()?;
    let connection = client.connect(*src_endpoint, src_server_name)?.await?;
    tracing::event!(Level::INFO, "Connected to Source");
    while let Ok(mut recv_stream) = connection.accept_uni().await {
        tracing::event!(Level::INFO, "Received new unidirectional stream");
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
    tracing::event!(Level::INFO, "Destination is done");
    Ok("destination OK".to_string())
}
