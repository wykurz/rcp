use anyhow::Context;
use async_recursion::async_recursion;
use tracing::instrument;

#[instrument]
#[async_recursion]
async fn send_directories_and_symlinks(
    source_config: &remote::protocol::SourceConfig,
    src: &std::path::Path,
    dst: &std::path::Path,
    is_root: bool,
    control_send_stream: &remote::streams::SharedSendStream,
    connection: &remote::streams::Connection,
) -> anyhow::Result<()> {
    tracing::debug!("Sending data from {:?} to {:?}", &src, dst);
    let src_metadata = if source_config.dereference {
        tokio::fs::metadata(&src).await
    } else {
        tokio::fs::symlink_metadata(&src).await
    }
    .with_context(|| format!("failed reading metadata from src: {src:?}"))?;
    if src_metadata.is_file() {
        return Ok(());
    }
    if src_metadata.is_symlink() {
        let symlink = remote::protocol::SourceMessage::Symlink {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            target: tokio::fs::read_link(&src).await?.to_path_buf(),
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
    let mut entries = tokio::fs::read_dir(&src).await?;
    while let Some(_entry) = entries.next_entry().await? {
        entry_count += 1;
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
    let mut entries = tokio::fs::read_dir(&src)
        .await
        .with_context(|| format!("cannot open directory {src:?} for reading"))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing src directory {:?}", &src))?
    {
        assert!(
            entry_count > 0,
            "Entry count for {src:?} is out of sync, was it modified during the copy?"
        );
        entry_count -= 1;
        let entry_path = entry.path();
        let entry_name = entry_path.file_name().unwrap();
        let dst_path = dst.join(entry_name);
        send_directories_and_symlinks(
            source_config,
            &entry_path,
            &dst_path,
            false,
            control_send_stream,
            connection,
        )
        .await?;
    }
    assert!(
        entry_count == 0,
        "Entry count for {src:?} is out of sync, was it modified during the copy?"
    );
    Ok(())
}

#[instrument]
#[async_recursion]
async fn send_fs_objects(
    source_config: &remote::protocol::SourceConfig,
    src: &std::path::Path,
    dst: &std::path::Path,
    control_send_stream: remote::streams::SharedSendStream,
    connection: remote::streams::Connection,
) -> anyhow::Result<()> {
    tracing::info!("Sending data from {:?} to {:?}", src, dst);
    let src_metadata = if source_config.dereference {
        tokio::fs::metadata(src).await
    } else {
        tokio::fs::symlink_metadata(src).await
    }
    .with_context(|| format!("failed reading metadata from src: {src:?}"))?;
    if !src_metadata.is_file() {
        send_directories_and_symlinks(
            source_config,
            src,
            dst,
            true,
            &control_send_stream,
            &connection,
        )
        .await?;
    }
    let mut stream = control_send_stream.lock().await;
    stream
        .send_control_message(&remote::protocol::SourceMessage::DirStructureComplete)
        .await?;
    if src_metadata.is_file() {
        send_file(src, dst, &src_metadata, true, connection).await?;
    }
    return Ok(());
}

#[instrument]
#[async_recursion]
async fn send_file(
    src: &std::path::Path,
    dst: &std::path::Path,
    src_metadata: &std::fs::Metadata,
    is_root: bool,
    connection: remote::streams::Connection,
) -> anyhow::Result<()> {
    let metadata = remote::protocol::Metadata::from(src_metadata);
    let file_header = remote::protocol::File {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        size: src_metadata.len(),
        metadata,
        is_root,
    };
    let mut file_send_stream = connection.open_uni().await?;
    tracing::debug!("Sending file content for {:?}", src);
    file_send_stream
        .send_message_with_data(&file_header, &mut tokio::fs::File::open(src).await?)
        .await
        .with_context(|| format!("failed sending file content: {:?}", &src))?;
    file_send_stream.close().await?;
    tracing::info!("Sent file: {:?} -> {:?}", src, dst);
    Ok(())
}

#[instrument]
async fn send_files_in_directory(
    source_config: &remote::protocol::SourceConfig,
    src: &std::path::Path,
    dst: &std::path::Path,
    connection: remote::streams::Connection,
) -> anyhow::Result<()> {
    tracing::info!("Sending files from {src:?}");
    let mut entries = tokio::fs::read_dir(src)
        .await
        .with_context(|| format!("cannot open directory {src:?} for reading"))?;
    let mut join_set = tokio::task::JoinSet::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing src directory {:?}", &src))?
    {
        let entry_path = entry.path();
        let entry_name = entry_path.file_name().unwrap();
        let dst_path = dst.join(entry_name);
        let entry_metadata = if source_config.dereference {
            tokio::fs::metadata(&entry_path).await
        } else {
            tokio::fs::symlink_metadata(&entry_path).await
        }
        .with_context(|| format!("failed reading metadata from src: {src:?}"))?;
        if !entry_metadata.is_file() {
            continue;
        }
        let connection = connection.clone();
        join_set.spawn(async move {
            send_file(&entry_path, &dst_path, &entry_metadata, false, connection).await
        });
    }
    drop(entries);
    while let Some(res) = join_set.join_next().await {
        res.with_context(|| format!("send_files_in_directory: {src:?} -> {dst:?} failed"))??;
    }
    Ok(())
}

#[instrument]
async fn dispatch_control_messages(
    source_config: remote::protocol::SourceConfig,
    mut control_recv_stream: remote::streams::RecvStream,
    control_send_stream: remote::streams::SharedSendStream,
    connection: remote::streams::Connection,
    src_root: std::path::PathBuf,
) -> anyhow::Result<()> {
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
                // TODO: we should spawn here
                send_files_in_directory(
                    &source_config,
                    &confirmation.src,
                    &confirmation.dst,
                    connection.clone(),
                )
                .await?;
            }
            remote::protocol::DestinationMessage::DirectoryComplete(completion) => {
                tracing::info!(
                    "Received directory completion for: {:?} -> {:?}",
                    completion.src,
                    completion.dst
                );
                // Send directory metadata
                let src_metadata = if source_config.dereference {
                    tokio::fs::metadata(&completion.src).await
                } else {
                    tokio::fs::symlink_metadata(&completion.src).await
                }
                .with_context(|| {
                    format!("failed reading metadata from src: {:?}", &completion.src)
                })?;
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
    }
    tracing::info!("Closing control recv stream");
    control_recv_stream.close().await;
    tracing::info!("Finished dispatching control messages");
    Ok(())
}

async fn handle_connection(
    conn: quinn::Connecting,
    source_config: &remote::protocol::SourceConfig,
    src: &std::path::Path,
    dst: &std::path::Path,
) -> anyhow::Result<()> {
    let connection = conn.await?;
    tracing::info!("Destination connection established");
    let connection = remote::streams::Connection::new(connection);
    let (control_send_stream, control_recv_stream) = connection.open_bi().await?;
    tracing::info!("Opened streams for directory transfer");
    let dispatch_task = tokio::spawn(dispatch_control_messages(
        *source_config,
        control_recv_stream,
        control_send_stream.clone(),
        connection.clone(),
        src.to_path_buf(),
    ));
    send_fs_objects(source_config, src, dst, control_send_stream, connection).await?;
    dispatch_task.await??;
    tracing::info!("Data sent successfully");
    Ok(())
}

#[instrument]
pub async fn run_source(
    master_send_stream: remote::streams::SharedSendStream,
    src: &std::path::Path,
    dst: &std::path::Path,
    source_config: &remote::protocol::SourceConfig,
) -> anyhow::Result<String> {
    let server_endpoint = remote::get_server()?;
    let server_addr = remote::get_endpoint_addr(&server_endpoint)?;
    tracing::info!("Source server listening on {}", server_addr);
    let master_hello = remote::protocol::SourceMasterHello {
        source_addr: server_addr,
        server_name: remote::get_random_server_name(),
    };
    tracing::info!("Sending master hello: {:?}", master_hello);
    master_send_stream
        .lock()
        .await
        .send_control_message(&master_hello)
        .await?;
    tracing::info!("Waiting for connection from destination");
    if let Some(conn) = server_endpoint.accept().await {
        tracing::info!("New destination connection incoming");
        handle_connection(conn, source_config, src, dst).await?;
    } else {
        tracing::error!("Timed out waiting for destination to connect");
        return Err(anyhow::anyhow!(
            "Timed out waiting for destination to connect"
        ));
    }
    tracing::info!("Source is done");
    server_endpoint.wait_idle().await;
    Ok("source OK".to_string())
}
