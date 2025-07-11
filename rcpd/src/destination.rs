use crate::directory_tracker::DirectoryTracker;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tracing::{instrument, Level};

#[instrument]
async fn handle_file_stream(
    mut file_recv_stream: quinn::RecvStream,
    directory_tracker: Arc<DirectoryTracker>,
) -> anyhow::Result<()> {
    tracing::event!(Level::INFO, "Processing file stream");
    let mut file_recv_stream = tokio_util::codec::FramedRead::new(
        &mut file_recv_stream,
        tokio_util::codec::LengthDelimitedCodec::new(),
    );
    if let Some(frame) = futures::StreamExt::next(&mut file_recv_stream).await {
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
                let read_buffer = file_recv_stream.read_buffer();
                let buffer_size = read_buffer.len() as u64;
                file.write_all(read_buffer).await?;
                let mut data_stream = file_recv_stream.into_inner();
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
                // Decrement directory entry count
                directory_tracker.decrement_entry(src, dst).await?;
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
                // Decrement directory entry count
                directory_tracker.decrement_entry(src, dst).await?;
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

/// Spawns a task that continuously accepts unidirectional streams for file/symlink transfers.
/// Each stream is handled in its own task for maximum parallelism.
#[instrument]
async fn spawn_file_handler_task(
    connection: quinn::Connection,
    directory_tracker: Arc<DirectoryTracker>,
) -> anyhow::Result<()> {
    while let Ok(file_recv_stream) = connection.accept_uni().await {
        tracing::event!(Level::INFO, "Received new unidirectional stream for file");
        tokio::spawn(handle_file_stream(
            file_recv_stream,
            directory_tracker.clone(),
        ));
    }
    tracing::event!(Level::INFO, "File handler task completed");
    Ok(())
}

/// Handles the directory structure creation phase by processing DirStub and Directory messages.
/// Fails hard on receiving any unexpected message type.
#[instrument]
async fn create_directory_structure(
    mut dir_stub_recv_stream: tokio_util::codec::FramedRead<
        quinn::RecvStream,
        tokio_util::codec::LengthDelimitedCodec,
    >,
    directory_tracker: Arc<DirectoryTracker>,
) -> anyhow::Result<()> {
    while let Some(frame) = futures::StreamExt::next(&mut dir_stub_recv_stream).await {
        let chunk = frame?;
        // throttle::get_ops_token().await;
        match bincode::deserialize::<remote::protocol::FsObject>(&chunk)? {
            remote::protocol::FsObject::DirStub {
                ref src,
                ref dst,
                num_entries,
            } => {
                tracing::event!(
                    Level::INFO,
                    "Received directory stub: {:?} -> {:?} (entries: {})",
                    src,
                    dst,
                    num_entries
                );
                tokio::fs::create_dir_all(&dst).await?;
                directory_tracker
                    .add_directory(src, dst, num_entries)
                    .await?;
            }
            _ => {
                return Err(anyhow::anyhow!("Expected DirStub, got: {:?}", chunk));
            }
        }
    }
    tracing::event!(Level::INFO, "Directory structure creation completed");
    Ok(())
}

#[instrument]
async fn update_directory_metadata(
    mut dir_metadata_recv_stream: tokio_util::codec::FramedRead<
        quinn::RecvStream,
        tokio_util::codec::LengthDelimitedCodec,
    >,
) -> anyhow::Result<()> {
    while let Some(frame) = futures::StreamExt::next(&mut dir_metadata_recv_stream).await {
        let chunk = frame?;
        // throttle::get_ops_token().await;
        match bincode::deserialize::<remote::protocol::FsObject>(&chunk)? {
            remote::protocol::FsObject::Directory {
                ref src,
                ref dst,
                ref metadata,
            } => {
                tracing::event!(
                    Level::INFO,
                    "Received directory metadata: {:?} -> {:?}",
                    src,
                    dst
                );
                // Apply metadata changes now that directory is complete
                let settings = common::preserve::preserve_all();
                common::preserve::set_dir_metadata(&settings, metadata, dst).await?;
                tracing::event!(
                    Level::INFO,
                    "Applied metadata for completed directory: {:?}",
                    dst
                );
            }
            _ => {
                return Err(anyhow::anyhow!("Expected Directory, got: {:?}", chunk));
            }
        }
    }
    tracing::event!(Level::INFO, "Directory structure creation completed");
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
    // Handle bidirectional stream for directory structure
    let (dir_created_send_stream, dir_stub_recv_stream) = connection.accept_bi().await?;
    let dir_created_send_stream = tokio_util::codec::FramedWrite::new(
        dir_created_send_stream,
        tokio_util::codec::LengthDelimitedCodec::new(),
    );
    let dir_stub_recv_stream = tokio_util::codec::FramedRead::new(
        dir_stub_recv_stream,
        tokio_util::codec::LengthDelimitedCodec::new(),
    );
    let dir_metadata_recv_stream = connection.accept_uni().await?;
    let dir_metadata_recv_stream = tokio_util::codec::FramedRead::new(
        dir_metadata_recv_stream,
        tokio_util::codec::LengthDelimitedCodec::new(),
    );
    tracing::event!(Level::INFO, "Received directory creation streams");
    let directory_tracker = Arc::new(DirectoryTracker::new(dir_created_send_stream));
    let file_handler_task = tokio::spawn(spawn_file_handler_task(
        connection,
        directory_tracker.clone(),
    ));
    let update_metadata_task = tokio::spawn(update_directory_metadata(dir_metadata_recv_stream));
    create_directory_structure(dir_stub_recv_stream, directory_tracker.clone()).await?;
    directory_tracker.finish().await?;
    file_handler_task.await??;
    update_metadata_task.await??;
    tracing::event!(Level::INFO, "Destination is done");
    Ok("destination OK".to_string())
}
