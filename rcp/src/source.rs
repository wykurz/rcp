use async_recursion::async_recursion;
use tracing::instrument;

fn progress() -> &'static common::progress::Progress {
    common::get_progress()
}

#[instrument(skip(error_occurred))]
#[async_recursion]
async fn send_directories_and_symlinks(
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    is_root: bool,
    control_send_stream: &remote::streams::SharedSendStream,
    connection: &remote::streams::Connection,
    error_occurred: &std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    tracing::debug!("Sending data from {:?} to {:?}", &src, dst);
    let src_metadata = match if settings.dereference {
        tokio::fs::metadata(&src).await
    } else {
        tokio::fs::symlink_metadata(&src).await
    } {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("Failed reading metadata from src {src:?}: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            if settings.fail_early {
                return Err(e.into());
            }
            return Ok(());
        }
    };
    if src_metadata.is_file() {
        return Ok(());
    }
    if src_metadata.is_symlink() {
        let target = match tokio::fs::read_link(&src).await {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Failed reading symlink {src:?}: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                // notify destination that this symlink was skipped (for directory tracking)
                if !is_root {
                    let skip_msg =
                        remote::protocol::SourceMessage::SymlinkSkipped(remote::protocol::SrcDst {
                            src: src.to_path_buf(),
                            dst: dst.to_path_buf(),
                        });
                    control_send_stream
                        .lock()
                        .await
                        .send_batch_message(&skip_msg)
                        .await?;
                }
                if settings.fail_early {
                    return Err(e.into());
                }
                return Ok(());
            }
        };
        let symlink = remote::protocol::SourceMessage::Symlink {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            target: target.clone(),
            metadata: remote::protocol::Metadata::from(&src_metadata),
            is_root,
        };
        return control_send_stream
            .lock()
            .await
            .send_batch_message(&symlink)
            .await;
    }
    if !src_metadata.is_dir() {
        assert!(
            src_metadata.is_file(),
            "Encountered fs object that's not a directory, symlink or a file? {src:?}"
        );
        return Ok(());
    }
    // we do one more read_dir to count entries; this could be avoided by e.g. modifying
    // the protocol to send the entry count at a later time
    let mut entry_count = 0;
    let mut entries = match tokio::fs::read_dir(&src).await {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("Cannot open directory {src:?} for reading: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            if settings.fail_early {
                return Err(e.into());
            }
            return Ok(());
        }
    };
    loop {
        match entries.next_entry().await {
            Ok(Some(_entry)) => {
                entry_count += 1;
            }
            Ok(None) => break,
            Err(e) => {
                tracing::error!("Failed traversing src directory {src:?}: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e.into());
                }
                break;
            }
        }
    }
    let dir = remote::protocol::SourceMessage::DirStub {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        num_entries: entry_count,
    };
    tracing::debug!(
        "Sending directory stub: {:?} -> {:?}, with {} entries",
        &src,
        dst,
        entry_count
    );
    control_send_stream
        .lock()
        .await
        .send_batch_message(&dir)
        .await?;
    let mut entries = match tokio::fs::read_dir(&src).await {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("Cannot open directory {src:?} for reading: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            if settings.fail_early {
                return Err(e.into());
            }
            return Ok(());
        }
    };
    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) => {
                assert!(
                    entry_count > 0,
                    "Entry count for {src:?} is out of sync, was it modified during the copy?"
                );
                entry_count -= 1;
                let entry_path = entry.path();
                let entry_name = entry_path.file_name().unwrap();
                let dst_path = dst.join(entry_name);
                if let Err(e) = send_directories_and_symlinks(
                    settings,
                    &entry_path,
                    &dst_path,
                    false,
                    control_send_stream,
                    connection,
                    error_occurred,
                )
                .await
                {
                    tracing::error!("Failed to send directory/symlink {entry_path:?}: {e}");
                    error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                    if settings.fail_early {
                        return Err(e);
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::error!("Failed traversing src directory {src:?}: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e.into());
                }
                break;
            }
        }
    }
    assert!(
        entry_count == 0,
        "Entry count for {src:?} is out of sync, was it modified during the copy?"
    );
    Ok(())
}

#[instrument(skip(error_occurred))]
#[async_recursion]
async fn send_fs_objects(
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    control_send_stream: remote::streams::SharedSendStream,
    connection: remote::streams::Connection,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    tracing::info!("Sending data from {:?} to {:?}", src, dst);
    let src_metadata = match if settings.dereference {
        tokio::fs::metadata(src).await
    } else {
        tokio::fs::symlink_metadata(src).await
    } {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("Failed reading metadata from src {src:?}: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            return Err(e.into());
        }
    };
    if !src_metadata.is_file() {
        if let Err(e) = send_directories_and_symlinks(
            settings,
            src,
            dst,
            true,
            &control_send_stream,
            &connection,
            &error_occurred,
        )
        .await
        {
            tracing::error!("Failed to send directories and symlinks: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            if settings.fail_early {
                return Err(e);
            }
        }
    }
    let mut stream = control_send_stream.lock().await;
    stream
        .send_control_message(&remote::protocol::SourceMessage::DirStructureComplete)
        .await?;
    if src_metadata.is_file() {
        if let Err(e) = send_file(
            settings,
            src,
            dst,
            &src_metadata,
            true,
            connection,
            &error_occurred,
            control_send_stream.clone(),
        )
        .await
        {
            tracing::error!("Failed to send root file: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            if settings.fail_early {
                return Err(e);
            }
        }
    }
    return Ok(());
}

#[instrument(skip(error_occurred, control_send_stream))]
#[async_recursion]
#[allow(clippy::too_many_arguments)]
async fn send_file(
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    src_metadata: &std::fs::Metadata,
    is_root: bool,
    connection: remote::streams::Connection,
    error_occurred: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    control_send_stream: remote::streams::SharedSendStream,
) -> anyhow::Result<()> {
    let prog = progress();
    let _ops_guard = prog.ops.guard();
    let _open_file_guard = throttle::open_file_permit().await;
    tracing::debug!("Sending file content for {:?}", src);
    throttle::get_file_iops_tokens(settings.chunk_size, src_metadata.len()).await;
    // open the file BEFORE opening the stream to avoid leaving destination waiting
    let mut file = match tokio::fs::File::open(src).await {
        Ok(f) => f,
        Err(e) => {
            tracing::error!("Failed to open file {src:?}: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            // notify destination that this file was skipped (for directory tracking)
            if !is_root {
                let skip_msg =
                    remote::protocol::SourceMessage::FileSkipped(remote::protocol::SrcDst {
                        src: src.to_path_buf(),
                        dst: dst.to_path_buf(),
                    });
                control_send_stream
                    .lock()
                    .await
                    .send_batch_message(&skip_msg)
                    .await?;
            }
            if settings.fail_early {
                return Err(e.into());
            }
            return Ok(());
        }
    };
    let metadata = remote::protocol::Metadata::from(src_metadata);
    let file_header = remote::protocol::File {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        size: src_metadata.len(),
        metadata,
        is_root,
    };
    let mut file_send_stream = connection.open_uni().await?;
    match file_send_stream
        .send_message_with_data(&file_header, &mut file)
        .await
    {
        Ok(_bytes_sent) => {
            file_send_stream.close().await?;
            prog.files_copied.inc();
            prog.bytes_copied.add(src_metadata.len());
            tracing::info!("Sent file: {:?} -> {:?}", src, dst);
            Ok(())
        }
        Err(e) => {
            tracing::error!("Failed to send file content for {src:?}: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            file_send_stream.close().await?;
            if settings.fail_early {
                Err(e)
            } else {
                Ok(())
            }
        }
    }
}

#[instrument(skip(error_occurred, control_send_stream))]
async fn send_files_in_directory(
    settings: common::copy::Settings,
    src: std::path::PathBuf,
    dst: std::path::PathBuf,
    connection: remote::streams::Connection,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
    control_send_stream: remote::streams::SharedSendStream,
) -> anyhow::Result<()> {
    tracing::info!("Sending files from {src:?}");
    let mut entries = match tokio::fs::read_dir(&src).await {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("Cannot open directory {src:?} for reading: {e}");
            error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
            if settings.fail_early {
                return Err(e.into());
            }
            return Ok(());
        }
    };
    let mut join_set = tokio::task::JoinSet::new();
    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) => {
                let entry_path = entry.path();
                let entry_name = entry_path.file_name().unwrap();
                let dst_path = dst.join(entry_name);
                let entry_metadata = match if settings.dereference {
                    tokio::fs::metadata(&entry_path).await
                } else {
                    tokio::fs::symlink_metadata(&entry_path).await
                } {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::error!("Failed reading metadata from {entry_path:?}: {e}");
                        error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                        if settings.fail_early {
                            return Err(e.into());
                        }
                        continue;
                    }
                };
                if !entry_metadata.is_file() {
                    continue;
                }
                throttle::get_ops_token().await;
                let connection = connection.clone();
                let error_flag = error_occurred.clone();
                let control_stream = control_send_stream.clone();
                join_set.spawn(async move {
                    send_file(
                        &settings,
                        &entry_path,
                        &dst_path,
                        &entry_metadata,
                        false,
                        connection,
                        &error_flag,
                        control_stream,
                    )
                    .await
                });
            }
            Ok(None) => break,
            Err(e) => {
                tracing::error!("Failed traversing src directory {src:?}: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e.into());
                }
                break;
            }
        }
    }
    drop(entries);
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!("Failed to send file from {src:?}: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e);
                }
            }
            Err(e) => {
                tracing::error!("Task panicked while sending file from {src:?}: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e.into());
                }
            }
        }
    }
    Ok(())
}

#[instrument(skip(error_occurred))]
async fn dispatch_control_messages(
    settings: common::copy::Settings,
    mut control_recv_stream: remote::streams::RecvStream,
    control_send_stream: remote::streams::SharedSendStream,
    connection: remote::streams::Connection,
    src_root: std::path::PathBuf,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    let mut join_set = tokio::task::JoinSet::new();
    while let Some(message) = control_recv_stream
        .recv_object::<remote::protocol::DestinationMessage>()
        .await?
    {
        match message {
            remote::protocol::DestinationMessage::DirectoryCreated(confirmation) => {
                tracing::info!(
                    "Received directory creation confirmation for: {:?} -> {:?}",
                    confirmation.src,
                    confirmation.dst
                );
                let error_flag = error_occurred.clone();
                join_set.spawn(send_files_in_directory(
                    settings,
                    confirmation.src.clone(),
                    confirmation.dst.clone(),
                    connection.clone(),
                    error_flag,
                    control_send_stream.clone(),
                ));
            }
            remote::protocol::DestinationMessage::DirectoryFailed(failure) => {
                tracing::warn!(
                    "Received directory creation failure for: {:?} -> {:?}, skipping its contents",
                    failure.src,
                    failure.dst
                );
            }
            remote::protocol::DestinationMessage::DirectoryComplete(completion) => {
                tracing::info!(
                    "Received directory completion for: {:?} -> {:?}",
                    completion.src,
                    completion.dst
                );
                // Send directory metadata
                match if settings.dereference {
                    tokio::fs::metadata(&completion.src).await
                } else {
                    tokio::fs::symlink_metadata(&completion.src).await
                } {
                    Ok(src_metadata) => {
                        let metadata = remote::protocol::Metadata::from(&src_metadata);
                        let is_root = completion.src == src_root;
                        let dir_metadata = remote::protocol::SourceMessage::Directory {
                            src: completion.src,
                            dst: completion.dst,
                            metadata,
                            is_root,
                        };
                        tracing::debug!("Before sending directory metadata");
                        {
                            let mut stream = control_send_stream.lock().await;
                            stream.send_control_message(&dir_metadata).await?;
                        }
                        tracing::debug!("Sent directory metadata");
                    }
                    Err(e) => {
                        tracing::error!("Failed to read metadata from {:?}: {e}", completion.src);
                        error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                        if settings.fail_early {
                            return Err(e.into());
                        }
                    }
                }
            }
            remote::protocol::DestinationMessage::DestinationDone => {
                tracing::info!("Received destination done message");
                let mut stream = control_send_stream.lock().await;
                stream
                    .send_control_message(&remote::protocol::SourceMessage::SourceDone)
                    .await?;
                tracing::info!("Closing control send stream");
                stream.close().await?;
                tracing::info!("Sent source done message");
                break;
            }
        }
        // opportunistically cleanup finished tasks
        while let Some(result) = join_set.try_join_next() {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::error!("Task failed: {e}");
                    error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                    if settings.fail_early {
                        return Err(e);
                    }
                }
                Err(e) => {
                    tracing::error!("Task panicked: {e}");
                    error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                    if settings.fail_early {
                        return Err(e.into());
                    }
                }
            }
        }
    }
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!("Task failed: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e);
                }
            }
            Err(e) => {
                tracing::error!("Task panicked: {e}");
                error_occurred.store(true, std::sync::atomic::Ordering::Relaxed);
                if settings.fail_early {
                    return Err(e.into());
                }
            }
        }
    }
    tracing::info!("Closing control recv stream");
    control_recv_stream.close().await;
    tracing::info!("Finished dispatching control messages");
    Ok(())
}

async fn handle_connection(
    conn: quinn::Connecting,
    settings: &common::copy::Settings,
    src: &std::path::Path,
    dst: &std::path::Path,
    error_occurred: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()> {
    let connection = conn.await?;
    tracing::info!("Destination connection established");
    let connection = remote::streams::Connection::new(connection);
    let (control_send_stream, control_recv_stream) = connection.open_bi().await?;
    tracing::info!("Opened streams for directory transfer");
    let dispatch_task = tokio::spawn(dispatch_control_messages(
        *settings,
        control_recv_stream,
        control_send_stream.clone(),
        connection.clone(),
        src.to_path_buf(),
        error_occurred.clone(),
    ));
    let send_result = send_fs_objects(
        settings,
        src,
        dst,
        control_send_stream,
        connection.clone(),
        error_occurred.clone(),
    )
    .await;
    // if sending failed, close connection to unblock destination immediately
    if send_result.is_err() {
        connection.close();
    }
    send_result?;
    dispatch_task.await??;
    tracing::info!("Data sent successfully");
    Ok(())
}

#[instrument]
pub async fn run_source(
    master_send_stream: remote::streams::SharedSendStream,
    src: &std::path::Path,
    dst: &std::path::Path,
    settings: &common::copy::Settings,
    quic_config: &remote::QuicConfig,
    bind_ip: Option<&str>,
) -> anyhow::Result<(String, common::copy::Summary)> {
    let (server_endpoint, cert_fingerprint) = remote::get_server_with_port_ranges(
        quic_config.port_ranges.as_deref(),
        quic_config.idle_timeout_sec,
        quic_config.keep_alive_interval_sec,
    )?;
    let server_addr = remote::get_endpoint_addr_with_bind_ip(&server_endpoint, bind_ip)?;
    tracing::info!("Source server listening on {}", server_addr);
    let master_hello = remote::protocol::SourceMasterHello {
        source_addr: server_addr,
        server_name: remote::get_random_server_name(),
        cert_fingerprint,
    };
    tracing::info!("Sending master hello: {:?}", master_hello);
    master_send_stream
        .lock()
        .await
        .send_control_message(&master_hello)
        .await?;
    tracing::info!("Waiting for connection from destination");
    // wait for destination to connect with a timeout
    // destination should connect within a reasonable time after receiving the source address
    let error_occurred = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let accept_timeout = std::time::Duration::from_secs(quic_config.conn_timeout_sec);
    match tokio::time::timeout(accept_timeout, server_endpoint.accept()).await {
        Ok(Some(conn)) => {
            tracing::info!("New destination connection incoming");
            handle_connection(conn, settings, src, dst, error_occurred.clone()).await?;
        }
        Ok(None) => {
            tracing::error!("Server endpoint closed unexpectedly");
            return Err(anyhow::anyhow!("Server endpoint closed unexpectedly"));
        }
        Err(_) => {
            tracing::error!(
                "Timed out waiting for destination to connect after {:?}. \
                This usually means the destination cannot reach the source. \
                Check network connectivity and firewall rules.",
                accept_timeout
            );
            return Err(anyhow::anyhow!(
                "Timed out waiting for destination to connect after {:?}",
                accept_timeout
            ));
        }
    }
    tracing::info!("Source is done");
    server_endpoint.wait_idle().await;
    // source doesn't track summary - destination is authoritative
    if error_occurred.load(std::sync::atomic::Ordering::Relaxed) {
        Err(common::copy::Error {
            source: anyhow::anyhow!("Some operations failed during remote copy"),
            summary: common::copy::Summary::default(),
        }
        .into())
    } else {
        Ok(("source OK".to_string(), common::copy::Summary::default()))
    }
}
