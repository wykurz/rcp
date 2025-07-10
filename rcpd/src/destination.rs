use tokio::io::AsyncWriteExt;
use tracing::{instrument, Level};

#[instrument]
async fn handle_file_stream(mut recv_stream: quinn::RecvStream) -> anyhow::Result<()> {
    tracing::event!(Level::INFO, "Processing file stream");
    let mut framed = tokio_util::codec::FramedRead::new(
        &mut recv_stream,
        tokio_util::codec::LengthDelimitedCodec::new(),
    );
    if let Some(frame) = futures::StreamExt::next(&mut framed).await {
        let chunk = frame?;
        match bincode::deserialize::<remote::protocol::FsObject>(&chunk)? {
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
            }
            remote::protocol::FsObject::Symlink {
                ref src,
                ref dst,
                ref target,
                ref metadata,
            } => {
                tracing::event!(
                    Level::INFO,
                    "Received symlink: {:?} -> {:?} (target: {:?})",
                    src,
                    dst,
                    target
                );
                tokio::fs::symlink(target, dst).await?;
                let settings = common::preserve::preserve_all();
                common::preserve::set_symlink_metadata(&settings, metadata, dst).await?;
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected file or symlink on unidirectional stream"
                ));
            }
        }
    }
    Ok(())
}

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
    // Spawn task to handle unidirectional file streams
    let connection_clone = connection.clone();
    let file_handler_task = tokio::spawn(async move {
        let mut join_set = tokio::task::JoinSet::new();
        while let Ok(recv_stream) = connection_clone.accept_uni().await {
            tracing::event!(Level::INFO, "Received new unidirectional stream for file");
            join_set.spawn(handle_file_stream(recv_stream));
        }
        // Wait for all file tasks to complete
        while let Some(res) = join_set.join_next().await {
            if let Err(e) = res {
                tracing::event!(Level::ERROR, "File handler task failed: {:?}", e);
            }
        }
        tracing::event!(Level::INFO, "File handler task completed");
    });
    // Handle bidirectional stream for directory structure
    while let Ok((mut send_stream, mut recv_stream)) = connection.accept_bi().await {
        tracing::event!(Level::INFO, "Received new bidirectional stream");
        let mut framed = tokio_util::codec::FramedRead::new(
            &mut recv_stream,
            tokio_util::codec::LengthDelimitedCodec::new(),
        );
        while let Some(frame) = futures::StreamExt::next(&mut framed).await {
            let chunk = frame?;
            // throttle::get_ops_token().await;
            match bincode::deserialize::<remote::protocol::FsObject>(&chunk)? {
                remote::protocol::FsObject::DirStub { ref src, ref dst } => {
                    tracing::event!(
                        Level::INFO,
                        "Received directory stub: {:?} -> {:?}",
                        src,
                        dst
                    );
                    tokio::fs::create_dir_all(&dst).await?;
                }
                remote::protocol::FsObject::Directory {
                    ref src,
                    ref dst,
                    ref metadata,
                } => {
                    tracing::event!(Level::INFO, "Received directory: {:?} -> {:?}", src, dst);
                    tokio::fs::create_dir_all(&dst).await?;
                    let settings = common::preserve::preserve_all();
                    common::preserve::set_dir_metadata(&settings, metadata, dst).await?;
                    // Send directory creation confirmation
                    let confirmation = remote::protocol::DirectoryCreated {
                        src: src.clone(),
                        dst: dst.clone(),
                    };
                    let confirmation_bytes = bincode::serialize(&confirmation)?;
                    let mut framed_confirmation = tokio_util::codec::FramedWrite::new(
                        &mut send_stream,
                        tokio_util::codec::LengthDelimitedCodec::new(),
                    );
                    futures::SinkExt::send(
                        &mut framed_confirmation,
                        bytes::Bytes::from(confirmation_bytes),
                    )
                    .await?;
                    tracing::event!(
                        Level::INFO,
                        "Sent directory creation confirmation for: {:?}",
                        dst
                    );
                }
                remote::protocol::FsObject::File { .. }
                | remote::protocol::FsObject::Symlink { .. } => {
                    tracing::event!(
                        Level::WARN,
                        "Received file/symlink on bidirectional stream, ignoring"
                    );
                }
            }
        }
    }
    // Wait for file handler task to complete
    let _ = file_handler_task.await;
    tracing::event!(Level::INFO, "Destination is done");
    Ok("destination OK".to_string())
}
