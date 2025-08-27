use anyhow::Context;
use tracing::instrument;

use crate::directory_tracker;

async fn send_root_done(
    control_send_stream: remote::streams::SharedSendStream,
) -> anyhow::Result<()> {
    let mut stream = control_send_stream.lock().await;
    stream
        .send_control_message(&remote::protocol::DestinationMessage::DestinationDone)
        .await?;
    stream.close().await?;
    tracing::info!("Sent destination done message");
    Ok(())
}

#[instrument]
async fn handle_file_stream(
    destination_config: remote::protocol::DestinationConfig,
    control_send_stream: remote::streams::SharedSendStream,
    mut file_recv_stream: remote::streams::RecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
) -> anyhow::Result<()> {
    tracing::info!("Processing file stream");
    let file_header = file_recv_stream
        .recv_object::<remote::protocol::File>()
        .await?
        .expect("No file data sent over uni-stream?!");
    tracing::info!(
        "Received file: {:?} -> {:?}",
        file_header.src,
        file_header.dst
    );
    // TODO:
    // let _open_file_guard = throttle::open_file_permit().await;
    // throttle::get_iops_tokens(tokens as u32).await;
    let mut file = tokio::fs::File::create(&file_header.dst).await?;
    let copied = file_recv_stream.copy_to(&mut file).await?;
    if copied != file_header.size {
        return Err(anyhow::anyhow!(
            "File size mismatch: expected {} bytes, copied {} bytes",
            file_header.size,
            copied
        ));
    }
    drop(file); // Ensure file is closed before setting metadata
    tracing::info!(
        "File {} -> {} created, size: {} bytes, setting metadata...",
        file_header.src.display(),
        file_header.dst.display(),
        file_header.size
    );
    common::preserve::set_file_metadata(
        &destination_config.preserve,
        &file_header.metadata,
        &file_header.dst,
    )
    .await?;
    if file_header.is_root {
        tracing::info!("Root file processed");
        send_root_done(control_send_stream).await?;
    } else {
        directory_tracker
            .lock()
            .await
            .decrement_entry(&file_header.src, &file_header.dst)
            .await
            .context("Failed to decrement directory entry count after receiving a file")?;
    }
    Ok(())
}

#[instrument]
async fn process_incoming_file_streams(
    destination_config: remote::protocol::DestinationConfig,
    control_send_stream: remote::streams::SharedSendStream,
    connection: remote::streams::Connection,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
) -> anyhow::Result<()> {
    let mut join_set = tokio::task::JoinSet::new();
    // TODO: we're accumulating unbounded number of spawned tasks here
    while let Ok(file_recv_stream) = connection.accept_uni().await {
        tracing::info!("Received new unidirectional stream for file");
        let tracker = directory_tracker.clone();
        join_set.spawn(handle_file_stream(
            destination_config.clone(),
            control_send_stream.clone(),
            file_recv_stream,
            tracker.clone(),
        ));
    }
    // Handle completion of existing file streams
    while let Some(result) = join_set.join_next().await {
        result??;
    }
    join_set.shutdown().await;
    tracing::info!("All file streams completed");
    Ok(())
}

#[instrument]
async fn create_directory_structure(
    destination_config: &remote::protocol::DestinationConfig,
    control_send_stream: remote::streams::SharedSendStream,
    mut control_recv_stream: remote::streams::RecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
) -> anyhow::Result<()> {
    while let Some(source_message) = control_recv_stream
        .recv_object::<remote::protocol::SourceMessage>()
        .await
        .context("Failed to receive FS object message")?
    {
        // throttle::get_ops_token().await;
        tracing::debug!("Received source message: {:?}", source_message);
        let mut directory_tracker = directory_tracker.lock().await;
        match source_message {
            remote::protocol::SourceMessage::DirStub {
                ref src,
                ref dst,
                num_entries,
            } => {
                tokio::fs::create_dir(&dst).await?;
                directory_tracker
                    .add_directory(src, dst, num_entries)
                    .await
                    .context("Failed to add directory to tracker")?;
            }
            remote::protocol::SourceMessage::Directory {
                ref src,
                ref dst,
                ref metadata,
                is_root,
            } => {
                // apply metadata changes now that directory is complete
                common::preserve::set_dir_metadata(&destination_config.preserve, metadata, dst)
                    .await?;
                if is_root {
                    tracing::info!("Root directory processed");
                    send_root_done(control_send_stream).await?;
                    break;
                } else {
                    directory_tracker
                        .decrement_entry(src, dst)
                        .await
                        .context(format!("Failed to decrement directory entry count after receiving directory metadata src: {src:?}, dst: {dst:?}"))?;
                }
            }
            remote::protocol::SourceMessage::Symlink {
                ref src,
                ref dst,
                ref target,
                ref metadata,
                is_root,
            } => {
                tokio::fs::symlink(target, dst).await?;
                common::preserve::set_symlink_metadata(&destination_config.preserve, metadata, dst)
                    .await?;
                if is_root {
                    tracing::info!("Root symlink processed");
                    send_root_done(control_send_stream).await?;
                    break;
                } else {
                    directory_tracker.decrement_entry(src, dst).await.context(
                        "Failed to decrement directory entry count after receiving a symlink",
                    )?;
                }
            }
            remote::protocol::SourceMessage::DirStructureComplete => {
                tracing::info!("All directories creation completed");
            }
            remote::protocol::SourceMessage::SourceDone => {
                tracing::info!("Received source done message received");
                break;
            }
        }
    }
    tracing::info!("Directory structure creation completed");
    Ok(())
}

#[instrument]
pub async fn run_destination(
    src_endpoint: &std::net::SocketAddr,
    src_server_name: &str,
    destination_config: &remote::protocol::DestinationConfig,
    _rcpd_config: &remote::protocol::RcpdConfig,
) -> anyhow::Result<String> {
    let client = remote::get_client()?;
    let connection = client.connect(*src_endpoint, src_server_name)?.await?;
    tracing::info!("Connected to Source");
    let connection = remote::streams::Connection::new(connection);
    // Always accept the directory streams first (even for single files)
    let (control_send_stream, control_recv_stream) = connection.accept_bi().await?;
    tracing::info!("Received directory creation streams");
    let directory_tracker = directory_tracker::make_shared(control_send_stream.clone());
    let file_handler_task = tokio::spawn(process_incoming_file_streams(
        destination_config.clone(),
        control_send_stream.clone(),
        connection.clone(),
        directory_tracker.clone(),
    ));
    create_directory_structure(
        destination_config,
        control_send_stream,
        control_recv_stream,
        directory_tracker,
    )
    .await
    .context("Failed to create directory structure")?;
    file_handler_task
        .await
        .context("Failed to process incoming file streams")??;
    tracing::info!("Destination is done");
    connection.close();
    client.wait_idle().await;
    Ok("destination OK".to_string())
}
