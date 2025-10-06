use anyhow::Context;
use tracing::instrument;

use crate::directory_tracker;

fn progress() -> &'static common::progress::Progress {
    common::get_progress()
}

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

#[instrument(skip(_open_file_guard))]
async fn handle_file_stream(
    _open_file_guard: throttle::OpenFileGuard,
    settings: common::copy::Settings,
    preserve: common::preserve::Settings,
    control_send_stream: remote::streams::SharedSendStream,
    mut file_recv_stream: remote::streams::RecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
) -> anyhow::Result<()> {
    let prog = progress();
    let _ops_guard = prog.ops.guard();
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
    throttle::get_file_iops_tokens(settings.chunk_size, file_header.size as u64).await;
    let mut file = tokio::fs::File::create(&file_header.dst).await?;
    let copied = file_recv_stream.copy_to(&mut file).await?;
    if copied != file_header.size {
        return Err(anyhow::anyhow!(
            "File size mismatch: expected {} bytes, copied {} bytes",
            file_header.size,
            copied
        ));
    }
    file_recv_stream.close().await;
    drop(file); // Ensure file is closed before setting metadata
    tracing::info!(
        "File {} -> {} created, size: {} bytes, setting metadata...",
        file_header.src.display(),
        file_header.dst.display(),
        file_header.size
    );
    common::preserve::set_file_metadata(&preserve, &file_header.metadata, &file_header.dst).await?;
    prog.files_copied.inc();
    prog.bytes_copied.add(file_header.size);
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
    settings: common::copy::Settings,
    preserve: common::preserve::Settings,
    control_send_stream: remote::streams::SharedSendStream,
    connection: remote::streams::Connection,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
) -> anyhow::Result<()> {
    let mut join_set = tokio::task::JoinSet::new();
    while let Ok(file_recv_stream) = connection.accept_uni().await {
        tracing::info!("Received new unidirectional stream for file");
        let open_file_guard = throttle::open_file_permit().await;
        throttle::get_ops_token().await;
        let tracker = directory_tracker.clone();
        join_set.spawn(handle_file_stream(
            open_file_guard,
            settings,
            preserve,
            control_send_stream.clone(),
            file_recv_stream,
            tracker.clone(),
        ));
        // opportunistically cleanup finished tasks
        while let Some(result) = join_set.try_join_next() {
            result??;
        }
    }
    // handle completion of existing file streams
    while let Some(result) = join_set.join_next().await {
        result??;
    }
    join_set.shutdown().await;
    tracing::info!("All file streams completed");
    Ok(())
}

#[instrument]
async fn create_directory_structure(
    settings: &common::copy::Settings,
    preserve: &common::preserve::Settings,
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
        let prog = progress();
        match source_message {
            remote::protocol::SourceMessage::DirStub {
                ref src,
                ref dst,
                num_entries,
            } => {
                let _ops_guard = prog.ops.guard();
                tokio::fs::create_dir(&dst).await?;
                prog.directories_created.inc();
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
                let _ops_guard = prog.ops.guard();
                // apply metadata changes now that directory is complete
                common::preserve::set_dir_metadata(preserve, metadata, dst).await?;
                if is_root {
                    tracing::info!("Root directory processed");
                    send_root_done(control_send_stream).await?;
                    break;
                } else {
                    directory_tracker
                        .decrement_entry(src, dst)
                        .await
                        .with_context(|| format!("Failed to decrement directory entry count after receiving directory metadata src: {src:?}, dst: {dst:?}"))?;
                }
            }
            remote::protocol::SourceMessage::Symlink {
                ref src,
                ref dst,
                ref target,
                ref metadata,
                is_root,
            } => {
                let _ops_guard = prog.ops.guard();
                tokio::fs::symlink(target, dst).await?;
                common::preserve::set_symlink_metadata(preserve, metadata, dst).await?;
                prog.symlinks_created.inc();
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
    tracing::info!("Closing control recv stream");
    control_recv_stream.close().await;
    tracing::info!("Directory structure creation completed");
    Ok(())
}

#[instrument]
pub async fn run_destination(
    src_endpoint: &std::net::SocketAddr,
    src_server_name: &str,
    settings: &common::copy::Settings,
    preserve: &common::preserve::Settings,
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
        *settings,
        *preserve,
        control_send_stream.clone(),
        connection.clone(),
        directory_tracker.clone(),
    ));
    create_directory_structure(
        settings,
        preserve,
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
