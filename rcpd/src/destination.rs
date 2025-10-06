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
    // check if destination exists and handle overwrite logic
    let dst_exists = tokio::fs::symlink_metadata(&file_header.dst).await.is_ok();
    if dst_exists {
        if settings.overwrite {
            tracing::debug!("file exists, check if it's identical");
            let dst_metadata = tokio::fs::symlink_metadata(&file_header.dst).await?;
            // check if file type is the same (both must be files)
            let same_type = dst_metadata.is_file();
            if same_type {
                // create wrapper that includes size for comparison
                let src_file_metadata = remote::protocol::FileMetadata {
                    metadata: &file_header.metadata,
                    size: file_header.size,
                };
                // check metadata fields including size
                let same_metadata = common::filecmp::metadata_equal(
                    &settings.overwrite_compare,
                    &src_file_metadata,
                    &dst_metadata,
                );
                if same_metadata {
                    tracing::debug!("file is identical, skipping");
                    prog.files_unchanged.inc();
                    // drain the stream without writing
                    let mut sink = tokio::io::sink();
                    file_recv_stream.copy_to(&mut sink).await?;
                    file_recv_stream.close().await;
                    if file_header.is_root {
                        tracing::info!("Root file processed (unchanged)");
                        send_root_done(control_send_stream).await?;
                    } else {
                        directory_tracker
                            .lock()
                            .await
                            .decrement_entry(&file_header.src, &file_header.dst)
                            .await
                            .context("Failed to decrement directory entry count after skipping unchanged file")?;
                    }
                    return Ok(());
                }
                // file exists but is different, remove it
                tracing::debug!("file exists but is different, removing");
                tokio::fs::remove_file(&file_header.dst).await?;
            } else {
                // destination is not a file (could be symlink or directory), remove it
                tracing::info!("destination is not a file, removing");
                common::rm::rm(
                    common::get_progress(),
                    &file_header.dst,
                    &common::rm::Settings {
                        fail_early: settings.fail_early,
                    },
                )
                .await
                .map_err(|err| anyhow::anyhow!("Failed to remove destination: {err}"))?;
            }
        } else {
            return Err(anyhow::anyhow!(
                "destination {:?} already exists, did you intend to specify --overwrite?",
                file_header.dst
            ));
        }
    }
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
        throttle::get_ops_token().await;
        tracing::debug!("Received source message: {:?}", source_message);
        let prog = progress();
        match source_message {
            remote::protocol::SourceMessage::DirStub {
                ref src,
                ref dst,
                num_entries,
            } => {
                let _ops_guard = prog.ops.guard();
                if let Err(error) = tokio::fs::create_dir(&dst).await {
                    if settings.overwrite && error.kind() == std::io::ErrorKind::AlreadyExists {
                        // check if the destination is a directory - if so, leave it
                        // use symlink_metadata to not follow symlinks
                        let dst_metadata =
                            tokio::fs::symlink_metadata(dst).await.with_context(|| {
                                format!("failed reading metadata from dst: {dst:?}")
                            })?;
                        if dst_metadata.is_dir() {
                            tracing::debug!("destination is a directory, leaving it as is");
                            prog.directories_unchanged.inc();
                        } else {
                            tracing::info!(
                                "destination is not a directory, removing and creating a new one"
                            );
                            // do NOT hold directory_tracker lock while calling rm::rm() as it might deadlock
                            // with file streams trying to decrement entries
                            common::rm::rm(
                                common::get_progress(),
                                dst,
                                &common::rm::Settings {
                                    fail_early: settings.fail_early,
                                },
                            )
                            .await
                            .map_err(|err| {
                                anyhow::anyhow!("Failed to remove destination: {err}")
                            })?;
                            tokio::fs::create_dir(&dst)
                                .await
                                .with_context(|| format!("cannot create directory {dst:?}"))?;
                            prog.directories_created.inc();
                        }
                    } else {
                        return Err(error.into());
                    }
                } else {
                    prog.directories_created.inc();
                }
                directory_tracker
                    .lock()
                    .await
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
                        .lock()
                        .await
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
                if let Err(error) = tokio::fs::symlink(target, dst).await {
                    if settings.overwrite && error.kind() == std::io::ErrorKind::AlreadyExists {
                        let dst_metadata =
                            tokio::fs::symlink_metadata(dst).await.with_context(|| {
                                format!("failed reading metadata from dst: {dst:?}")
                            })?;
                        if dst_metadata.is_symlink() {
                            let dst_link = tokio::fs::read_link(dst)
                                .await
                                .with_context(|| format!("failed reading dst symlink: {dst:?}"))?;
                            if target == &dst_link {
                                tracing::debug!("destination is a symlink and points to the same location as source");
                                if preserve.symlink.any() {
                                    // check if we need to update the metadata for this symlink
                                    if !common::filecmp::metadata_equal(
                                        &settings.overwrite_compare,
                                        metadata,
                                        &dst_metadata,
                                    ) {
                                        tracing::debug!(
                                            "destination metadata is different, updating"
                                        );
                                        common::preserve::set_symlink_metadata(
                                            preserve, metadata, dst,
                                        )
                                        .await?;
                                        prog.symlinks_removed.inc();
                                        prog.symlinks_created.inc();
                                    } else {
                                        tracing::debug!(
                                            "destination symlink is identical, skipping"
                                        );
                                        prog.symlinks_unchanged.inc();
                                    }
                                } else {
                                    tracing::debug!("destination symlink is identical, skipping");
                                    prog.symlinks_unchanged.inc();
                                }
                            } else {
                                tracing::info!("destination is a symlink but points to a different location, removing");
                                tokio::fs::remove_file(dst).await?;
                                tokio::fs::symlink(target, dst).await?;
                                common::preserve::set_symlink_metadata(preserve, metadata, dst)
                                    .await?;
                                prog.symlinks_removed.inc();
                                prog.symlinks_created.inc();
                            }
                        } else {
                            tracing::info!("destination is not a symlink, removing");
                            // do NOT hold directory_tracker lock while calling rm::rm()
                            common::rm::rm(
                                common::get_progress(),
                                dst,
                                &common::rm::Settings {
                                    fail_early: settings.fail_early,
                                },
                            )
                            .await
                            .map_err(|err| {
                                anyhow::anyhow!("Failed to remove destination: {err}")
                            })?;
                            tokio::fs::symlink(target, dst).await?;
                            common::preserve::set_symlink_metadata(preserve, metadata, dst).await?;
                            prog.symlinks_created.inc();
                        }
                    } else {
                        return Err(error.into());
                    }
                } else {
                    common::preserve::set_symlink_metadata(preserve, metadata, dst).await?;
                    prog.symlinks_created.inc();
                }
                if is_root {
                    tracing::info!("Root symlink processed");
                    send_root_done(control_send_stream).await?;
                    break;
                } else {
                    directory_tracker
                        .lock()
                        .await
                        .decrement_entry(src, dst)
                        .await
                        .context(
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
