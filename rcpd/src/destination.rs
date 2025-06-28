use tokio::io::AsyncWriteExt;
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
        let mut framed = tokio_util::codec::FramedRead::new(
            &mut recv_stream,
            tokio_util::codec::LengthDelimitedCodec::new(),
        );
        while let Some(frame) = futures::StreamExt::next(&mut framed).await {
            let chunk = frame?;
            // throttle::get_ops_token().await;
            match bincode::deserialize::<remote::protocol::FsObject>(&chunk)? {
                remote::protocol::FsObject::DirStub { ref src, ref dst } => {
                    tracing::event!(Level::INFO, "Received directory: {:?} -> {:?}", src, dst);
                    // TODO: spawn a task
                    tokio::fs::create_dir(&dst).await?;
                    // let settings = common::preserve::preserve_all();
                    // common::preserve::set_dir_metadata(&settings, fs_object, dst).await?;
                }
                remote::protocol::FsObject::File {
                    ref src,
                    ref dst,
                    size,
                    ref metadata,
                } => {
                    tracing::event!(Level::INFO, "Received file: {:?} -> {:?}", src, dst);
                    // TODO:
                    // let _open_file_guard = throttle::open_file_permit().await;
                    // throttle::get_iops_tokens(tokens as u32).await;
                    let mut file = tokio::fs::File::create(&dst).await?;
                    // TODO: can we use tokio::io::copy_buf instead?
                    let read_buffer = framed.read_buffer();
                    let buffer_size = read_buffer.len() as u64;
                    file.write_all(read_buffer).await?;
                    let mut data_stream = framed.into_inner();
                    let stream_bytes = tokio::io::copy(&mut data_stream, &mut file).await?;
                    if buffer_size + stream_bytes != size {
                        return Err(anyhow::anyhow!(
                            "File size mismatch: expected {} bytes, copied {} (read buffer) + {} (stream) (= {} total bytes)",
                            size,
                            buffer_size,
                            stream_bytes,
                            buffer_size + stream_bytes,
                        ));
                    }
                    let settings = common::preserve::preserve_all();
                    common::preserve::set_file_metadata(&settings, &metadata, dst).await?;
                    break;
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
