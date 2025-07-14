use anyhow::Context;
use async_recursion::async_recursion;
use std::os::unix::fs::MetadataExt;
use tracing::{event, instrument, Level};

use crate::streams;

#[instrument]
#[async_recursion]
async fn send_directory_structure_helper(
    src: &std::path::Path,
    dst: &std::path::Path,
    dir_stub_send_stream: &mut streams::SendStream,
) -> anyhow::Result<()> {
    event!(
        Level::INFO,
        "Sending directory structure from {:?} to {:?}",
        src,
        dst
    );
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from src: {:?}", &src))?;
    if !src_metadata.is_dir() {
        // TODO: handle dereferencing symlinks
        return Ok(());
    }
    // Count the number of entries in the directory
    let mut entry_count = 0;
    let mut entries = tokio::fs::read_dir(src).await?;
    while let Some(_entry) = entries.next_entry().await? {
        entry_count += 1;
    }
    let dir = remote::protocol::FsObject::DirStub {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        num_entries: entry_count,
    };
    event!(
        Level::DEBUG,
        "Sending directory stub: {:?} -> {:?} with {} entries",
        src,
        dst,
        entry_count
    );
    dir_stub_send_stream
        .send_object(&dir)
        .await
        .with_context(|| format!("failed sending directory: {:?}", &src))?;
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
        send_directory_structure_helper(&entry_path, &dst_path, dir_stub_send_stream).await?;
    }
    Ok(())
}

async fn send_directory_structure(
    src: &std::path::Path,
    dst: &std::path::Path,
    mut dir_stub_send_stream: streams::SendStream,
) -> anyhow::Result<()> {
    send_directory_structure_helper(src, dst, &mut dir_stub_send_stream).await?;
    // Stream will be closed automatically when dropped
    event!(Level::INFO, "Finished sending directory structure");
    Ok(())
}

#[instrument]
#[async_recursion]
async fn send_file_or_symlink(
    src: &std::path::Path,
    dst: &std::path::Path,
    connection: &quinn::Connection,
) -> anyhow::Result<()> {
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from src: {:?}", &src))?;
    if src_metadata.is_dir() {
        return Ok(());
    }
    let metadata = remote::protocol::Metadata {
        mode: src_metadata.mode(),
        uid: src_metadata.uid(),
        gid: src_metadata.gid(),
        atime: src_metadata.atime(),
        mtime: src_metadata.mtime(),
        atime_nsec: src_metadata.atime_nsec(),
        mtime_nsec: src_metadata.mtime_nsec(),
    };
    let fs_obj = if src_metadata.is_file() {
        remote::protocol::FsObject::File {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            size: src_metadata.len(),
            metadata,
        }
    } else {
        assert!(
            src_metadata.is_symlink(),
            "Expected src to be a file or symlink, got {src:?}"
        );
        remote::protocol::FsObject::Symlink {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            target: tokio::fs::read_link(src).await?.to_path_buf(),
            metadata,
        }
    };
    let connection = streams::Connection::new(connection.clone());
    let mut file_send_stream = connection.open_uni().await?;
    file_send_stream
        .send_object(&fs_obj)
        .await
        .with_context(|| format!("failed sending file metadata: {:?}", &src))?;
    if src_metadata.is_file() {
        event!(Level::INFO, "Sending file content for {:?}", src);
        let data_stream = file_send_stream.get_mut();
        tokio::io::copy(&mut tokio::fs::File::open(src).await?, data_stream).await?;
    }
    // Stream will be closed automatically when dropped
    event!(Level::INFO, "Sent file/symlink: {:?} -> {:?}", src, dst);
    Ok(())
}

#[instrument]
async fn send_files_in_directory(
    src: &std::path::Path,
    dst: &std::path::Path,
    connection: &quinn::Connection,
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
        join_set
            .spawn(async move { send_file_or_symlink(&entry_path, &dst_path, &connection).await });
    }
    drop(entries);
    while let Some(res) = join_set.join_next().await {
        res.with_context(|| format!("send_files_in_directory: {src:?} -> {dst:?} failed"))??;
    }
    Ok(())
}

#[instrument]
async fn wait_for_directory_creation_and_send_files(
    mut dir_created_recv_stream: streams::RecvStream,
    mut dir_metadata_send_stream: streams::SendStream,
    connection: &quinn::Connection,
) -> anyhow::Result<()> {
    // Wait for directory creation confirmations and completions
    while let Some(message) = dir_created_recv_stream
        .recv_object::<remote::protocol::DirectoryMessage>()
        .await?
    {
        match message {
            remote::protocol::DirectoryMessage::Created(confirmation) => {
                event!(
                    Level::INFO,
                    "Received directory creation confirmation for: {:?} -> {:?}",
                    confirmation.src,
                    confirmation.dst
                );
                // Send files in this directory
                send_files_in_directory(&confirmation.src, &confirmation.dst, connection).await?;
            }
            remote::protocol::DirectoryMessage::Complete(completion) => {
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
                let dir_metadata = remote::protocol::FsObject::Directory {
                    src: completion.src,
                    dst: completion.dst,
                    metadata,
                };
                dir_metadata_send_stream.send_object(&dir_metadata).await?;
            }
        }
    }
    // Stream will be closed automatically when dropped
    event!(
        Level::INFO,
        "Finished waiting for directory creation confirmations"
    );
    Ok(())
}

async fn handle_connection(
    conn: quinn::Connecting,
    src: &std::path::Path,
    dst: &std::path::Path,
) -> anyhow::Result<()> {
    let connection = conn.await?;
    event!(Level::INFO, "Destination connection established");
    let connection = streams::Connection::new(connection.clone());
    // Always open directory streams first (even for single files)
    let (dir_stub_send_stream, dir_created_recv_stream) = connection.open_bi().await?;
    let dir_metadata_send_stream = connection.open_uni().await?;
    event!(Level::INFO, "Opened streams for directory transfer");
    if src.is_dir() {
        // Start directory confirmation receiver task
        let confirmation_task = tokio::spawn(async move {
            wait_for_directory_creation_and_send_files(
                dir_created_recv_stream,
                dir_metadata_send_stream,
                connection.inner(),
            )
            .await
        });
        send_directory_structure(src, dst, dir_stub_send_stream).await?;
        confirmation_task.await??;
    } else {
        // For single files, just drop the directory streams immediately
        drop(dir_stub_send_stream);
        drop(dir_metadata_send_stream);
        drop(dir_created_recv_stream);
        send_file_or_symlink(src, dst, connection.inner()).await?;
    }
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
    if !src.is_absolute() {
        return Err(anyhow::anyhow!(
            "Source path must be absolute: {}",
            src.display()
        ));
    }
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
