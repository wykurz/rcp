use anyhow::Context;
use async_recursion::async_recursion;
use std::os::unix::fs::MetadataExt;
use tracing::{event, instrument, Level};

use crate::streams;

#[instrument]
#[async_recursion]
async fn send_directories_and_symlinks(
    src: &std::path::Path,
    dst: &std::path::Path,
    control_send_stream: &streams::SharedSendStream,
    connection: &streams::Connection,
    is_root: bool,
) -> anyhow::Result<()> {
    event!(Level::INFO, "Sending data from {:?} to {:?}", src, dst);
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from src: {:?}", &src))?;
    if src_metadata.is_symlink() {
        // TODO: handle dereferencing symlinks
        let symlink = remote::protocol::FsObjectMessage::Symlink {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            target: tokio::fs::read_link(src).await?.to_path_buf(),
            metadata: remote::protocol::Metadata::from(&src_metadata),
            is_root,
        };
        return control_send_stream.lock().await.send_object(&symlink).await;
    }
    if !src_metadata.is_dir() {
        assert!(
            src_metadata.is_file(),
            "Encountered fs object that's not a directory, symlink or a file? {src:?}"
        );
        // handle files separately
        return Ok(());
    }
    // we do one more read_dir to count entries; this could be avoided by e.g. modifying
    // the protocol to send the entry count at a later time
    let mut entry_count = 0;
    let mut entries = tokio::fs::read_dir(src).await?;
    while let Some(_entry) = entries.next_entry().await? {
        entry_count += 1;
    }
    let dir = remote::protocol::FsObjectMessage::DirStub {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        num_entries: entry_count,
    };
    event!(
        Level::DEBUG,
        "Sending directory stub: {:?} -> {:?}, with {} entries",
        src,
        dst,
        entry_count
    );
    control_send_stream.lock().await.send_object(&dir).await?;
    let mut entries = tokio::fs::read_dir(src)
        .await
        .with_context(|| format!("cannot open directory {src:?} for reading"))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing src directory {:?}", &src))?
    {
        let entry_path = entry.path();
        let entry_name = entry_path.file_name().unwrap();
        let dst_path = dst.join(entry_name);
        send_directories_and_symlinks(
            &entry_path,
            &dst_path,
            control_send_stream,
            connection,
            false,
        )
        .await?;
    }
    Ok(())
}

#[instrument]
#[async_recursion]
async fn send_fs_objects(
    src: &std::path::Path,
    dst: &std::path::Path,
    control_send_stream: streams::SharedSendStream,
    connection: streams::Connection,
) -> anyhow::Result<()> {
    event!(Level::INFO, "Sending data from {:?} to {:?}", src, dst);
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from src: {:?}", &src))?;
    if !src_metadata.is_file() {
        send_directories_and_symlinks(src, dst, &control_send_stream, &connection, true).await?;
    }
    let mut stream = control_send_stream.lock().await;
    stream
        .send_object(&remote::protocol::FsObjectMessage::DirsAndSymlinksComplete)
        .await?;
    stream.flush().await?;
    if src_metadata.is_file() {
        send_file(src, dst, true, connection).await?;
    }
    return Ok(());
}

#[instrument]
#[async_recursion]
async fn send_file(
    src: &std::path::Path,
    dst: &std::path::Path,
    is_root: bool,
    connection: streams::Connection,
) -> anyhow::Result<()> {
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from src: {:?}", &src))?;
    assert!(
        src_metadata.is_file(),
        "Expected src to be a file, got {src:?}"
    );
    let metadata = remote::protocol::Metadata::from(&src_metadata);
    let file_header = remote::protocol::File {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        size: src_metadata.len(),
        metadata,
        is_root,
    };
    let mut file_send_stream = connection.open_uni().await?;
    file_send_stream
        .send_object(&file_header)
        .await
        .with_context(|| format!("failed sending file metadata: {:?}", &src))?;
    event!(Level::INFO, "Sending file content for {:?}", src);
    file_send_stream
        .copy_from(&mut tokio::fs::File::open(src).await?)
        .await
        .with_context(|| format!("failed sending file content: {:?}", &src))?;
    file_send_stream.close().await?;
    // Stream will be closed automatically when dropped
    event!(Level::INFO, "Sent file/symlink: {:?} -> {:?}", src, dst);
    Ok(())
}

#[instrument]
async fn send_files_in_directory(
    src: &std::path::Path,
    dst: &std::path::Path,
    connection: streams::Connection,
) -> anyhow::Result<()> {
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
        let connection = connection.clone();
        join_set.spawn(async move { send_file(&entry_path, &dst_path, false, connection).await });
    }
    drop(entries);
    while let Some(res) = join_set.join_next().await {
        res.with_context(|| format!("send_files_in_directory: {src:?} -> {dst:?} failed"))??;
    }
    Ok(())
}

#[instrument]
async fn dispatch_control_messages(
    mut control_recv_stream: streams::RecvStream,
    control_send_stream: streams::SharedSendStream,
    connection: streams::Connection,
    src_root: std::path::PathBuf,
) -> anyhow::Result<()> {
    while let Some(message) = control_recv_stream
        .recv_object::<remote::protocol::DestinationMessage>()
        .await?
    {
        match message {
            remote::protocol::DestinationMessage::DirectoryCreated(confirmation) => {
                event!(
                    Level::INFO,
                    "Received directory creation confirmation for: {:?} -> {:?}",
                    confirmation.src,
                    confirmation.dst
                );
                send_files_in_directory(&confirmation.src, &confirmation.dst, connection.clone())
                    .await?;
            }
            remote::protocol::DestinationMessage::DirectoryComplete(completion) => {
                event!(
                    Level::INFO,
                    "Received directory completion for: {:?} -> {:?}",
                    completion.src,
                    completion.dst
                );
                // Send directory metadata
                let src_metadata = tokio::fs::symlink_metadata(&completion.src)
                    .await
                    .with_context(|| {
                        format!("failed reading metadata from src: {:?}", &completion.src)
                    })?;
                let metadata = remote::protocol::Metadata {
                    mode: src_metadata.mode(),
                    uid: src_metadata.uid(),
                    gid: src_metadata.gid(),
                    atime: src_metadata.atime(),
                    mtime: src_metadata.mtime(),
                    atime_nsec: src_metadata.atime_nsec(),
                    mtime_nsec: src_metadata.mtime_nsec(),
                };
                let is_root = completion.src == src_root;
                let dir_metadata = remote::protocol::FsObjectMessage::Directory {
                    src: completion.src,
                    dst: completion.dst,
                    metadata,
                    is_root,
                };
                let mut stream = control_send_stream.lock().await;
                stream.send_object(&dir_metadata).await?;
                stream.flush().await?;
            }
            remote::protocol::DestinationMessage::RootDone => {
                event!(Level::INFO, "Received root completion message");
                break;
            }
        }
    }
    event!(Level::INFO, "Finished dispatching control messages");
    Ok(())
}

async fn handle_connection(
    conn: quinn::Connecting,
    src: &std::path::Path,
    dst: &std::path::Path,
) -> anyhow::Result<()> {
    let connection = conn.await?;
    event!(Level::INFO, "Destination connection established");
    let connection = streams::Connection::new(connection);
    let (control_send_stream, control_recv_stream) = connection.open_bi().await?;
    event!(Level::INFO, "Opened streams for directory transfer");
    let src_root = src.to_path_buf();
    let dispatch_task = tokio::spawn(dispatch_control_messages(
        control_recv_stream,
        control_send_stream.clone(),
        connection.clone(),
        src_root.clone(),
    ));
    send_fs_objects(src, dst, control_send_stream, connection).await?;
    dispatch_task.await??;
    event!(Level::INFO, "Data sent successfully");
    Ok(())
}

#[instrument]
pub async fn run_source(
    master_connection: &quinn::Connection,
    src: &std::path::Path,
    dst: &std::path::Path,
    _source_config: &remote::protocol::SourceConfig,
    _rcpd_config: &remote::protocol::RcpdConfig,
) -> anyhow::Result<String> {
    let server_endpoint = remote::get_server()?;
    let server_addr = remote::get_endpoint_addr(&server_endpoint)?;
    event!(Level::INFO, "Source server listening on {}", server_addr);
    let master_hello = remote::protocol::SourceMasterHello {
        source_addr: server_addr,
        server_name: remote::get_random_server_name(),
    };
    event!(Level::INFO, "Sending master hello: {:?}", master_hello);
    let master_hello = bincode::serialize(&master_hello)?;
    // TODO: replace send_datagram with setting up a bi-directional stream
    master_connection.send_datagram(bytes::Bytes::from(master_hello))?;
    event!(Level::INFO, "Waiting for connection from destination");
    if let Some(conn) = server_endpoint.accept().await {
        event!(Level::INFO, "New destination connection incoming");
        handle_connection(conn, src, dst).await?;
    } else {
        event!(Level::ERROR, "Timed out waiting for destination to connect");
        return Err(anyhow::anyhow!(
            "Timed out waiting for destination to connect"
        ));
    }
    event!(Level::INFO, "Source is done",);
    Ok("source OK".to_string())
}
