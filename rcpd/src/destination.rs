use tokio::io::AsyncWriteExt;
use tracing::{event, instrument, Level};

use crate::directory_tracker;
use crate::streams;

#[instrument]
async fn handle_file_stream(
    mut file_recv_stream: streams::RecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
) -> anyhow::Result<()> {
    event!(Level::INFO, "Processing file stream");
    if let Some(fs_obj) = file_recv_stream
        .recv_object::<remote::protocol::FsObject>()
        .await?
    {
        match fs_obj {
            remote::protocol::FsObject::File {
                ref src,
                ref dst,
                size,
                ref metadata,
                is_root,
            } => {
                event!(Level::INFO, "Received file: {:?} -> {:?}", src, dst);
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
                drop(file); // Ensure file is closed before setting metadata
                event!(
                    Level::INFO,
                    "File {} -> {} created, size: {} bytes, setting metadata...",
                    src.display(),
                    dst.display(),
                    size
                );
                let settings = common::preserve::preserve_all();
                common::preserve::set_file_metadata(&settings, &metadata, dst).await?;
                if !is_root {
                    directory_tracker
                        .lock()
                        .await
                        .decrement_entry(src, dst)
                        .await?;
                } else {
                    event!(
                        Level::INFO,
                        "Root file {} -> {} processed",
                        src.display(),
                        dst.display()
                    );
                }
            }
            remote::protocol::FsObject::Symlink {
                ref src,
                ref dst,
                ref target,
                ref metadata,
                is_root,
            } => {
                event!(
                    Level::INFO,
                    "Received symlink: {:?} -> {:?} (target: {:?})",
                    src,
                    dst,
                    target
                );
                tokio::fs::symlink(target, dst).await?;
                let settings = common::preserve::preserve_all();
                common::preserve::set_symlink_metadata(&settings, metadata, dst).await?;
                if !is_root {
                    directory_tracker
                        .lock()
                        .await
                        .decrement_entry(src, dst)
                        .await?;
                } else {
                    event!(
                        Level::INFO,
                        "Root symlink {} -> {} processed",
                        src.display(),
                        dst.display()
                    );
                }
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

/// Handles unidirectional streams for file/symlink transfers with proper task tracking.
/// Processes new streams and completes existing ones concurrently for maximum parallelism.
#[instrument]
async fn process_incoming_file_streams(
    connection: streams::Connection,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
) -> anyhow::Result<()> {
    let mut join_set = tokio::task::JoinSet::new();
    // TODO: we're accumulating unbounded number of spawned tasks here
    while let Ok(file_recv_stream) = connection.accept_uni().await {
        event!(Level::INFO, "Received new unidirectional stream for file");
        let tracker = directory_tracker.clone();
        join_set.spawn(handle_file_stream(file_recv_stream, tracker.clone()));
    }
    // Handle completion of existing file streams
    while let Some(result) = join_set.join_next().await {
        result??;
    }
    join_set.shutdown().await;
    event!(Level::INFO, "All file streams completed");
    Ok(())
}

/// Handles the directory structure creation phase by processing DirStub and Directory messages.
/// Fails hard on receiving any unexpected message type.
#[instrument]
async fn create_directory_structure(
    mut dir_stub_recv_stream: streams::RecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
) -> anyhow::Result<()> {
    while let Some(fs_obj) = dir_stub_recv_stream
        .recv_object::<remote::protocol::FsObject>()
        .await?
    {
        // throttle::get_ops_token().await;
        match fs_obj {
            remote::protocol::FsObject::DirStub {
                ref src,
                ref dst,
                num_entries,
            } => {
                event!(
                    Level::INFO,
                    "Received directory stub: {:?} -> {:?} (entries: {})",
                    src,
                    dst,
                    num_entries
                );
                tokio::fs::create_dir_all(&dst).await?;
                directory_tracker
                    .lock()
                    .await
                    .add_directory(src, dst, num_entries)
                    .await?;
            }
            _ => {
                return Err(anyhow::anyhow!("Expected DirStub, got: {:?}", fs_obj));
            }
        }
    }
    event!(Level::INFO, "Directory structure creation completed");
    directory_tracker
        .lock()
        .await
        .done_creating_directories()
        .await?;
    Ok(())
}

#[instrument]
async fn update_directory_metadata(
    mut dir_metadata_recv_stream: streams::RecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
) -> anyhow::Result<()> {
    while let Some(fs_obj) = dir_metadata_recv_stream
        .recv_object::<remote::protocol::FsObject>()
        .await?
    {
        // throttle::get_ops_token().await;
        match fs_obj {
            remote::protocol::FsObject::Directory {
                ref src,
                ref dst,
                ref metadata,
                is_root,
            } => {
                event!(
                    Level::INFO,
                    "Received directory metadata: {:?} -> {:?}",
                    src,
                    dst
                );
                // Apply metadata changes now that directory is complete
                let settings = common::preserve::preserve_all();
                common::preserve::set_dir_metadata(&settings, metadata, dst).await?;
                event!(
                    Level::INFO,
                    "Applied metadata for completed directory: {:?}",
                    dst
                );
                if !is_root {
                    directory_tracker
                        .lock()
                        .await
                        .decrement_entry(src, dst)
                        .await?;
                } else {
                    event!(
                        Level::INFO,
                        "Root directory {} -> {} processed",
                        src.display(),
                        dst.display()
                    );
                }
            }
            _ => {
                return Err(anyhow::anyhow!("Expected Directory, got: {:?}", fs_obj));
            }
        }
    }
    event!(Level::INFO, "Directory metadata update completed");
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
    event!(Level::INFO, "Connected to Source");
    let connection = streams::Connection::new(connection);
    // Always accept the directory streams first (even for single files)
    let (dir_created_send_stream, dir_stub_recv_stream) = connection.accept_bi().await?;
    let dir_metadata_recv_stream = connection.accept_uni().await?;
    event!(Level::INFO, "Received directory creation streams");
    let directory_tracker = directory_tracker::make_shared(dir_created_send_stream);
    let file_handler_task = tokio::spawn(process_incoming_file_streams(
        connection.clone(),
        directory_tracker.clone(),
    ));
    let update_metadata_task = tokio::spawn(update_directory_metadata(
        dir_metadata_recv_stream,
        directory_tracker.clone(),
    ));
    create_directory_structure(dir_stub_recv_stream, directory_tracker).await?;
    file_handler_task.await??;
    update_metadata_task.await??;
    event!(Level::INFO, "Destination is done");
    Ok("destination OK".to_string())
}
