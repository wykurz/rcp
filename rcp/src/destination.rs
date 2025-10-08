use anyhow::Context;
use tracing::instrument;

use super::directory_tracker;

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

#[instrument(skip(_open_file_guard, error_occurred))]
async fn handle_file_stream(
    _open_file_guard: throttle::OpenFileGuard,
    settings: common::copy::Settings,
    preserve: common::preserve::Settings,
    control_send_stream: remote::streams::SharedSendStream,
    mut file_recv_stream: remote::streams::RecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
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
    // wrap file handling logic to ensure stream is always cleaned up
    let file_result = async {
        // check if destination exists and handle overwrite logic
        let dst_exists = tokio::fs::symlink_metadata(&file_header.dst).await.is_ok();
        if dst_exists {
            if settings.overwrite {
                tracing::debug!("file exists, check if it's identical");
                let dst_metadata = tokio::fs::symlink_metadata(&file_header.dst).await?;
                let same_type = dst_metadata.is_file();
                if same_type {
                    let src_file_metadata = remote::protocol::FileMetadata {
                        metadata: &file_header.metadata,
                        size: file_header.size,
                    };
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
                        return Ok(());
                    }
                    tracing::debug!("file exists but is different, removing");
                    tokio::fs::remove_file(&file_header.dst).await?;
                } else {
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
        drop(file);
        tracing::info!(
            "File {} -> {} created, size: {} bytes, setting metadata...",
            file_header.src.display(),
            file_header.dst.display(),
            file_header.size
        );
        common::preserve::set_file_metadata(&preserve, &file_header.metadata, &file_header.dst)
            .await?;
        prog.files_copied.inc();
        prog.bytes_copied.add(file_header.size);
        Ok(())
    }
    .await;
    // handle result: log error if failed, drain stream if not already drained
    match file_result {
        Ok(()) => {}
        Err(e) => {
            tracing::error!("Failed to handle file {}: {}", file_header.dst.display(), e);
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            // ensure stream is drained to avoid blocking source
            let mut sink = tokio::io::sink();
            if let Err(drain_err) = file_recv_stream.copy_to(&mut sink).await {
                tracing::error!("Failed to drain file stream: {}", drain_err);
            }
            file_recv_stream.close().await;
            if settings.fail_early {
                return Err(e);
            }
        }
    }
    // always decrement directory tracker or send root done, even on failure
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

#[instrument(skip(error_occurred))]
async fn process_incoming_file_streams(
    settings: common::copy::Settings,
    preserve: common::preserve::Settings,
    control_send_stream: remote::streams::SharedSendStream,
    connection: remote::streams::Connection,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    let mut join_set = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            // check if any spawned task completed (potentially with error)
            Some(result) = join_set.join_next() => {
                tracing::debug!("File handling task completed");
                match result {
                    Ok(Ok(())) => {},
                    Ok(Err(e)) => {
                        tracing::error!("File handling task failed: {e}");
                        error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                        if settings.fail_early {
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        tracing::error!("File handling task panicked: {e}");
                        error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                        if settings.fail_early {
                            return Err(e.into());
                        }
                    }
                }
            }
            // accept new file streams
            stream_result = connection.accept_uni() => {
                match stream_result {
                    Ok(file_recv_stream) => {
                        tracing::info!("Received new unidirectional stream for file");
                        let open_file_guard = throttle::open_file_permit().await;
                        throttle::get_ops_token().await;
                        let tracker = directory_tracker.clone();
                        let error_flag = error_occurred.clone();
                        join_set.spawn(handle_file_stream(
                            open_file_guard,
                            settings,
                            preserve,
                            control_send_stream.clone(),
                            file_recv_stream,
                            tracker.clone(),
                            error_flag,
                        ));
                    }
                    Err(e) => {
                        // connection closed, wait for remaining tasks to complete
                        tracing::debug!("Connection closed: {e}");
                        break;
                    }
                }
            }
        }
    }
    tracing::debug!("Waiting for remaining file handling tasks to complete");
    // handle completion of remaining file streams
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!("File handling task failed: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e);
                }
            }
            Err(e) => {
                tracing::error!("File handling task panicked: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e.into());
                }
            }
        }
    }
    join_set.shutdown().await;
    tracing::info!("All file streams completed");
    Ok(())
}

#[instrument(skip(error_occurred))]
async fn create_directory_structure(
    settings: &common::copy::Settings,
    preserve: &common::preserve::Settings,
    control_send_stream: remote::streams::SharedSendStream,
    mut control_recv_stream: remote::streams::RecvStream,
    directory_tracker: directory_tracker::SharedDirectoryTracker,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
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
                let dir_failed = match tokio::fs::create_dir(&dst).await {
                    Ok(()) => {
                        prog.directories_created.inc();
                        false
                    }
                    Err(error)
                        if settings.overwrite
                            && error.kind() == std::io::ErrorKind::AlreadyExists =>
                    {
                        // check if the destination is a directory - if so, leave it
                        // use symlink_metadata to not follow symlinks
                        match tokio::fs::symlink_metadata(dst).await {
                            Ok(dst_metadata) if dst_metadata.is_dir() => {
                                tracing::debug!("destination is a directory, leaving it as is");
                                prog.directories_unchanged.inc();
                                false
                            }
                            Ok(_) => {
                                tracing::info!(
                                    "destination is not a directory, removing and creating a new one"
                                );
                                // do NOT hold directory_tracker lock while calling rm::rm() as it might deadlock
                                // with file streams trying to decrement entries
                                match common::rm::rm(
                                    common::get_progress(),
                                    dst,
                                    &common::rm::Settings {
                                        fail_early: settings.fail_early,
                                    },
                                )
                                .await
                                {
                                    Ok(_) => match tokio::fs::create_dir(&dst).await {
                                        Ok(()) => {
                                            prog.directories_created.inc();
                                            false
                                        }
                                        Err(e) => {
                                            tracing::error!("Failed to create directory {dst:?} after removing: {e}");
                                            error_occurred
                                                .store(true, std::sync::atomic::Ordering::Relaxed);
                                            if settings.fail_early {
                                                return Err(e.into());
                                            }
                                            true
                                        }
                                    },
                                    Err(err) => {
                                        tracing::error!(
                                            "Failed to remove destination {dst:?}: {err}"
                                        );
                                        error_occurred
                                            .store(true, std::sync::atomic::Ordering::Relaxed);
                                        if settings.fail_early {
                                            return Err(anyhow::anyhow!(
                                                "Failed to remove destination: {err}"
                                            ));
                                        }
                                        true
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to read metadata from {dst:?}: {e}");
                                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                                if settings.fail_early {
                                    return Err(e.into());
                                }
                                true
                            }
                        }
                    }
                    Err(error) => {
                        tracing::error!("Failed to create directory {dst:?}: {error}");
                        error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                        if settings.fail_early {
                            return Err(error.into());
                        }
                        true
                    }
                };
                directory_tracker
                    .lock()
                    .await
                    .add_directory(src, dst, num_entries, dir_failed)
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
                // wrap entire symlink creation logic to handle errors gracefully
                let symlink_result = async {
                    match tokio::fs::symlink(target, dst).await {
                        Ok(()) => {
                            common::preserve::set_symlink_metadata(preserve, metadata, dst).await?;
                            prog.symlinks_created.inc();
                            Ok(())
                        }
                        Err(error) if settings.overwrite && error.kind() == std::io::ErrorKind::AlreadyExists => {
                            let dst_metadata = tokio::fs::symlink_metadata(dst).await.with_context(|| {
                                format!("failed reading metadata from dst: {dst:?}")
                            })?;
                            if dst_metadata.is_symlink() {
                                let dst_link = tokio::fs::read_link(dst).await.with_context(|| format!("failed reading dst symlink: {dst:?}"))?;
                                if target == &dst_link {
                                    tracing::debug!("destination is a symlink and points to the same location as source");
                                    if preserve.symlink.any() {
                                        if !common::filecmp::metadata_equal(
                                            &settings.overwrite_compare,
                                            metadata,
                                            &dst_metadata,
                                        ) {
                                            tracing::debug!("destination metadata is different, updating");
                                            common::preserve::set_symlink_metadata(preserve, metadata, dst).await?;
                                            prog.symlinks_removed.inc();
                                            prog.symlinks_created.inc();
                                        } else {
                                            tracing::debug!("destination symlink is identical, skipping");
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
                                    common::preserve::set_symlink_metadata(preserve, metadata, dst).await?;
                                    prog.symlinks_removed.inc();
                                    prog.symlinks_created.inc();
                                }
                            } else {
                                tracing::info!("destination is not a symlink, removing");
                                common::rm::rm(
                                    common::get_progress(),
                                    dst,
                                    &common::rm::Settings {
                                        fail_early: settings.fail_early,
                                    },
                                )
                                .await
                                .map_err(|err| anyhow::anyhow!("Failed to remove destination: {err}"))?;
                                tokio::fs::symlink(target, dst).await?;
                                common::preserve::set_symlink_metadata(preserve, metadata, dst).await?;
                                prog.symlinks_created.inc();
                            }
                            Ok(())
                        }
                        Err(error) => Err(error.into()),
                    }
                }.await;
                if let Err(e) = symlink_result {
                    tracing::error!("Failed to create symlink {src:?} -> {dst:?}: {e}");
                    error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                    if settings.fail_early {
                        return Err(e);
                    }
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
    _conn_timeout_sec: u64,
) -> anyhow::Result<(String, common::copy::Summary)> {
    let client = remote::get_client()?;
    tracing::info!("Connecting to source at {}", src_endpoint);
    let connection = client
        .connect(*src_endpoint, src_server_name)?
        .await
        .with_context(|| {
            format!(
                "Failed to connect to source at {src_endpoint}. \
                This usually means the source is unreachable from the destination. \
                Check network connectivity and firewall rules."
            )
        })?;
    tracing::info!("Connected to Source");
    let connection = remote::streams::Connection::new(connection);
    // Always accept the directory streams first (even for single files)
    let (control_send_stream, control_recv_stream) = connection.accept_bi().await?;
    tracing::info!("Received directory creation streams");
    let directory_tracker = directory_tracker::make_shared(control_send_stream.clone());
    let error_occurred = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let file_handler_future = process_incoming_file_streams(
        *settings,
        *preserve,
        control_send_stream.clone(),
        connection.clone(),
        directory_tracker.clone(),
        error_occurred.clone(),
    );
    let directory_future = create_directory_structure(
        settings,
        preserve,
        control_send_stream,
        control_recv_stream,
        directory_tracker,
        error_occurred.clone(),
    );
    tokio::pin!(file_handler_future);
    tokio::pin!(directory_future);
    // race both futures - if either completes first, handle it and then wait for the other
    // if either fails, close connection to unblock the other
    tokio::select! {
        file_result = &mut file_handler_future => {
            if file_result.is_err() {
                connection.close();
            }
            file_result.context("Failed to process incoming file streams")?;
            directory_future.await.context("Failed to create directory structure")?;
        }
        dir_result = &mut directory_future => {
            if dir_result.is_err() {
                connection.close();
            }
            dir_result.context("Failed to create directory structure")?;
            file_handler_future.await.context("Failed to process incoming file streams")?;
        }
    }
    tracing::info!("Destination is done");
    connection.close();
    client.wait_idle().await;
    // build summary from progress counters
    let prog = progress();
    let summary = common::copy::Summary {
        bytes_copied: prog.bytes_copied.get(),
        files_copied: prog.files_copied.get() as usize,
        symlinks_created: prog.symlinks_created.get() as usize,
        directories_created: prog.directories_created.get() as usize,
        files_unchanged: prog.files_unchanged.get() as usize,
        symlinks_unchanged: prog.symlinks_unchanged.get() as usize,
        directories_unchanged: prog.directories_unchanged.get() as usize,
        rm_summary: common::rm::Summary {
            files_removed: prog.files_removed.get() as usize,
            symlinks_removed: prog.symlinks_removed.get() as usize,
            directories_removed: prog.directories_removed.get() as usize,
        },
    };
    if error_occurred.load(std::sync::atomic::Ordering::Relaxed) {
        Err(common::copy::Error {
            source: anyhow::anyhow!("Some operations failed during remote copy"),
            summary,
        }
        .into())
    } else {
        Ok(("destination OK".to_string(), summary))
    }
}
