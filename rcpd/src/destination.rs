use tracing::{instrument, Level};

#[instrument]
pub async fn run_destination(
    src_endpoint: &std::net::SocketAddr,
    src_server_name: &str,
    _destination_config: &remote::protocol::DestinationConfig,
    _rcpd_config: &remote::protocol::RcpdConfig,
) -> anyhow::Result<String> {
    let client = remote::get_client()?;
    let connection = client.connect(*src_endpoint, src_server_name)?.await?;
    tracing::event!(Level::INFO, "Connected to Source");
    while let Ok(mut recv_stream) = connection.accept_uni().await {
        tracing::event!(Level::INFO, "Received new unidirectional stream");
        while let Some(chunk) = recv_stream.read_chunk(usize::MAX, true).await? {
            match bincode::deserialize::<remote::protocol::FsObject>(&chunk.bytes)? {
                ref fs_object @ remote::protocol::FsObject::Directory {
                    ref src, ref dst, ..
                } => {
                    tracing::event!(Level::INFO, "Received directory: {:?} -> {:?}", src, dst);
                    tokio::fs::create_dir(&dst).await?;
                    let settings = common::preserve::preserve_all();
                    common::preserve::set_dir_metadata(&settings, fs_object, dst).await?;
                }
                whatever => {
                    return Err(anyhow::anyhow!(
                        "Received unsupported FsObject type {:?}",
                        whatever
                    ));
                }
            }
        }
    }
    tracing::event!(Level::INFO, "Destination is done");
    Ok("destination OK".to_string())
}
