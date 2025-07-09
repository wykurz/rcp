use anyhow::Context;
use async_recursion::async_recursion;
use std::os::unix::fs::MetadataExt;
use tracing::{event, instrument, Level};

#[instrument]
#[async_recursion]
async fn send_directory_structure(
    src: &std::path::Path,
    dst: &std::path::Path,
    send_stream: std::sync::Arc<tokio::sync::Mutex<quinn::SendStream>>,
) -> anyhow::Result<()> {
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from src: {:?}", &src))?;
    if !src_metadata.is_dir() {
        return Ok(());
    }
    let dir = remote::protocol::FsObject::DirStub {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
    };
    let mut locked_stream = send_stream.lock().await;
    let mut framed = tokio_util::codec::FramedWrite::new(
        &mut *locked_stream,
        tokio_util::codec::LengthDelimitedCodec::new(),
    );
    futures::SinkExt::send(&mut framed, bytes::Bytes::from(bincode::serialize(&dir)?))
        .await
        .with_context(|| format!("failed sending directory: {:?}", &src))?;
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
        let send_stream = send_stream.clone();
        let rec =
            || async move { send_directory_structure(&entry_path, &dst_path, send_stream).await };
        join_set.spawn(rec());
    }
    // unfortunately ReadDir is opening file-descriptors and there's not a good way to limit this,
    // one thing we CAN do however is to drop it as soon as we're done with it
    drop(entries);
    while let Some(res) = join_set.join_next().await {
        res.with_context(|| format!("send_directory_structure: {src:?} -> {dst:?} failed"))??;
    }
    Ok(())
}

#[instrument]
#[async_recursion]
async fn send_file_or_symlink(
    src: &std::path::Path,
    dst: &std::path::Path,
    send_stream: std::sync::Arc<tokio::sync::Mutex<quinn::SendStream>>,
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
    let mut locked_stream = send_stream.lock().await;
    let mut framed = tokio_util::codec::FramedWrite::new(
        &mut *locked_stream,
        tokio_util::codec::LengthDelimitedCodec::new(),
    );
    futures::SinkExt::send(
        &mut framed,
        bytes::Bytes::from(bincode::serialize(&fs_obj)?),
    )
    .await
    .with_context(|| format!("failed sending file metadata: {:?}", &src))?;
    if src_metadata.is_file() {
        event!(Level::INFO, "Sending file content for {:?}", src);
        let mut data_stream = framed.into_inner();
        tokio::io::copy(&mut tokio::fs::File::open(src).await?, &mut data_stream).await?;
    }
    Ok(())
}

async fn handle_connection(
    conn: quinn::Connecting,
    src: &std::path::Path,
    dst: &std::path::Path,
) -> anyhow::Result<()> {
    let connection = conn.await?;
    event!(Level::INFO, "Destination connection established");
    let send_stream = connection.open_uni().await?;
    let send_stream = std::sync::Arc::new(tokio::sync::Mutex::new(send_stream));
    event!(Level::INFO, "Opened unidirectional stream");
    if src.is_dir() {
        // TODO: start a directory completion receiver task
        send_directory_structure(src, dst, send_stream.clone()).await?;
    } else {
        send_file_or_symlink(src, dst, send_stream.clone()).await?;
    }
    event!(Level::INFO, "Data sent successfully");
    send_stream.lock().await.finish().await?;
    Ok(())
}

#[instrument]
pub async fn run_source(
    master_connection: &quinn::Connection,
    max_concurrent_streams: u32,
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
    let server_endpoint = remote::get_server(max_concurrent_streams)?;
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
