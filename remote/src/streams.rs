use bytes::Buf;
use futures::SinkExt;
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncWriteExt};
use tracing::instrument;

#[derive(Debug)]
pub struct SendStream {
    framed:
        tokio_util::codec::FramedWrite<quinn::SendStream, tokio_util::codec::LengthDelimitedCodec>,
}

impl SendStream {
    pub async fn new(stream: quinn::SendStream) -> anyhow::Result<Self> {
        let framed = tokio_util::codec::FramedWrite::new(
            stream,
            tokio_util::codec::LengthDelimitedCodec::new(),
        );
        Ok(Self { framed })
    }

    pub async fn send_batch_message<T: serde::Serialize>(&mut self, obj: &T) -> anyhow::Result<()> {
        let bytes = bincode::serialize(obj)?;
        self.framed.send(bytes::Bytes::from(bytes)).await?;
        Ok(())
    }

    pub async fn send_control_message<T: serde::Serialize>(
        &mut self,
        obj: &T,
    ) -> anyhow::Result<()> {
        self.send_batch_message(obj).await?;
        self.framed.flush().await?;
        Ok(())
    }

    /// Sends an object followed by data from a buffered reader.
    ///
    /// This method uses `copy_buf` which avoids internal buffer allocation by using
    /// the reader's existing buffer. Wrap your reader in `BufReader::with_capacity(size, reader)`
    /// to control the buffer size.
    #[instrument(level = "trace", skip(self, obj, reader))]
    pub async fn send_message_with_data_buffered<T: serde::Serialize, R: AsyncBufRead + Unpin>(
        &mut self,
        obj: &T,
        reader: &mut R,
    ) -> anyhow::Result<u64> {
        self.send_batch_message(obj).await?;
        let data_stream = self.framed.get_mut();
        let bytes_copied = tokio::io::copy_buf(reader, data_stream).await?;
        Ok(bytes_copied)
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        self.framed.close().await?;
        Ok(())
    }
}

pub type SharedSendStream = std::sync::Arc<tokio::sync::Mutex<SendStream>>;

#[derive(Debug)]
pub struct RecvStream {
    framed:
        tokio_util::codec::FramedRead<quinn::RecvStream, tokio_util::codec::LengthDelimitedCodec>,
}

impl RecvStream {
    pub async fn new(stream: quinn::RecvStream) -> anyhow::Result<Self> {
        let framed = tokio_util::codec::FramedRead::new(
            stream,
            tokio_util::codec::LengthDelimitedCodec::new(),
        );
        Ok(Self { framed })
    }

    pub async fn recv_object<T: serde::de::DeserializeOwned>(
        &mut self,
    ) -> anyhow::Result<Option<T>> {
        if let Some(frame) = futures::StreamExt::next(&mut self.framed).await {
            let bytes = frame?;
            let obj = bincode::deserialize(&bytes)?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }

    /// Copies data to a writer using the default buffer size (8 KiB).
    ///
    /// For better performance with large files, use [`Self::copy_to_buffered`] instead.
    #[instrument(level = "trace", skip(self, writer))]
    pub async fn copy_to<W: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        writer: &mut W,
    ) -> anyhow::Result<u64> {
        let read_buffer = self.framed.read_buffer();
        let buffer_size = read_buffer.len() as u64;
        writer.write_all(read_buffer).await?;
        let data_stream = self.framed.get_mut();
        let stream_bytes = tokio::io::copy(data_stream, writer).await?;
        Ok(buffer_size + stream_bytes)
    }

    /// Copies data to a writer using a custom buffer size.
    ///
    /// Uses a buffered reader around the QUIC stream with the specified capacity.
    /// This avoids the default 8 KiB buffer in `tokio::io::copy` and can significantly
    /// improve throughput on high-bandwidth networks.
    #[instrument(level = "trace", skip(self, writer))]
    pub async fn copy_to_buffered<W: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        writer: &mut W,
        buffer_size: usize,
    ) -> anyhow::Result<u64> {
        let read_buffer = self.framed.read_buffer();
        let buffered_bytes = read_buffer.len() as u64;
        writer.write_all(read_buffer).await?;
        let data_stream = self.framed.get_mut();
        // wrap the QUIC recv stream in a BufReader to control the buffer size
        let mut buffered_stream = tokio::io::BufReader::with_capacity(buffer_size, data_stream);
        let stream_bytes = tokio::io::copy_buf(&mut buffered_stream, writer).await?;
        Ok(buffered_bytes + stream_bytes)
    }

    /// Copies exactly `size` bytes to a writer using a custom buffer size.
    ///
    /// Unlike [`Self::copy_to_buffered`], this does NOT read until EOF. It reads
    /// exactly the specified number of bytes, leaving the stream open for
    /// reading subsequent messages.
    #[instrument(level = "trace", skip(self, writer))]
    pub async fn copy_exact_to_buffered<W: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        writer: &mut W,
        size: u64,
        buffer_size: usize,
    ) -> anyhow::Result<u64> {
        if size == 0 {
            return Ok(0);
        }
        // first drain any buffered data from the framed reader
        let read_buffer = self.framed.read_buffer_mut();
        let buffered = (read_buffer.len() as u64).min(size);
        if buffered > 0 {
            writer.write_all(&read_buffer[..buffered as usize]).await?;
            read_buffer.advance(buffered as usize);
        }
        let remaining = size - buffered;
        if remaining == 0 {
            return Ok(size);
        }
        // read exactly `remaining` bytes from the underlying stream
        let data_stream = self.framed.get_mut();
        let mut limited = data_stream.take(remaining);
        let mut buf = vec![0u8; buffer_size.min(remaining as usize)];
        let mut total_copied = buffered;
        loop {
            let bytes_to_read = buf.len().min((size - total_copied) as usize);
            if bytes_to_read == 0 {
                break;
            }
            let n = limited.read(&mut buf[..bytes_to_read]).await?;
            if n == 0 {
                break;
            }
            writer.write_all(&buf[..n]).await?;
            total_copied += n as u64;
        }
        if total_copied != size {
            anyhow::bail!(
                "unexpected EOF: expected {} bytes, got {}",
                size,
                total_copied
            );
        }
        Ok(size)
    }

    pub async fn close(&mut self) {
        let recv_stream = self.framed.get_mut();
        // copied from QUIC documentation: https://docs.rs/quinn/0.10.2/quinn/struct.RecvStream.html
        if recv_stream.read_to_end(0).await.is_err() {
            // discard unexpected data and notify the peer to stop sending it
            let _ = recv_stream.stop(0u8.into());
        }
    }
}

/// Connection wrapper that provides framed stream creation
#[derive(Clone, Debug)]
pub struct Connection {
    inner: quinn::Connection,
}

impl Connection {
    pub fn new(conn: quinn::Connection) -> Self {
        Self { inner: conn }
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn open_bi(&self) -> anyhow::Result<(SharedSendStream, RecvStream)> {
        let (send_stream, recv_stream) = self.inner.open_bi().await?;
        let send_stream = SendStream::new(send_stream).await?;
        let recv_stream = RecvStream::new(recv_stream).await?;
        Ok((
            std::sync::Arc::new(tokio::sync::Mutex::new(send_stream)),
            recv_stream,
        ))
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn open_uni(&self) -> anyhow::Result<SendStream> {
        let send_stream = self.inner.open_uni().await?;
        SendStream::new(send_stream).await
    }

    pub async fn accept_bi(&self) -> anyhow::Result<(SharedSendStream, RecvStream)> {
        let (send_stream, recv_stream) = self.inner.accept_bi().await?;
        let send_stream = SendStream::new(send_stream).await?;
        let recv_stream = RecvStream::new(recv_stream).await?;
        Ok((
            std::sync::Arc::new(tokio::sync::Mutex::new(send_stream)),
            recv_stream,
        ))
    }

    pub async fn accept_uni(&self) -> anyhow::Result<RecvStream> {
        let recv_stream = self.inner.accept_uni().await?;
        RecvStream::new(recv_stream).await
    }

    pub fn close(&self) {
        self.inner.close(0u32.into(), b"done");
    }

    /// Waits for the connection to be closed by peer.
    ///
    /// This should be called after all streams are closed to ensure the peer
    /// has finished processing before the connection is dropped. Returns the
    /// reason the connection was closed.
    pub async fn closed(&self) -> quinn::ConnectionError {
        self.inner.closed().await
    }
}

/// A pool of reusable unidirectional send streams.
///
/// The pool pre-opens N streams at creation time. Tasks borrow streams
/// to send files, and streams are automatically returned to the pool
/// when the guard is dropped. If a stream is discarded due to errors,
/// a new one can be opened to replenish the pool.
pub struct SendStreamPool {
    available: async_channel::Receiver<SendStream>,
    return_tx: async_channel::Sender<SendStream>,
    connection: Connection,
}

impl SendStreamPool {
    /// Creates a new pool with `pool_size` pre-opened unidirectional streams.
    #[instrument(level = "debug", skip(connection))]
    pub async fn new(connection: Connection, pool_size: usize) -> anyhow::Result<Self> {
        let (return_tx, available) = async_channel::bounded(pool_size);
        for _ in 0..pool_size {
            let stream = connection.open_uni().await?;
            return_tx.send(stream).await?;
        }
        tracing::debug!("created stream pool with {} streams", pool_size);
        Ok(Self {
            available,
            return_tx,
            connection,
        })
    }

    /// Borrows a stream from the pool. Waits if none available.
    pub async fn borrow(&self) -> anyhow::Result<PooledSendStream> {
        let stream = self
            .available
            .recv()
            .await
            .map_err(|_| anyhow::anyhow!("stream pool closed"))?;
        Ok(PooledSendStream {
            stream: Some(stream),
            return_tx: self.return_tx.clone(),
        })
    }

    /// Opens a new stream to replenish the pool after one was discarded.
    ///
    /// This should be called when a stream is discarded due to errors to
    /// prevent pool exhaustion. The new stream is added directly to the pool.
    pub async fn replenish(&self) -> anyhow::Result<()> {
        let stream = self.connection.open_uni().await?;
        // try_send because pool might be full if multiple replenish calls race
        if let Err(e) = self.return_tx.try_send(stream) {
            tracing::debug!("pool already full, closing replenished stream");
            // close explicitly to avoid leaking QUIC stream resources
            let mut stream = e.into_inner();
            if let Err(e) = stream.close().await {
                tracing::warn!("failed to close excess replenished stream: {:#}", e);
            }
        }
        Ok(())
    }

    /// Closes all streams in the pool.
    ///
    /// This should be called after all file transfers are complete.
    pub async fn close_all(self) -> anyhow::Result<()> {
        self.available.close();
        while let Ok(mut stream) = self.available.recv().await {
            stream.close().await?;
        }
        Ok(())
    }
}

/// Guard that returns stream to pool on drop.
///
/// Use [`Self::stream_mut`] to access the underlying [`SendStream`].
pub struct PooledSendStream {
    stream: Option<SendStream>,
    return_tx: async_channel::Sender<SendStream>,
}

impl PooledSendStream {
    /// Returns a mutable reference to the underlying stream.
    pub fn stream_mut(&mut self) -> &mut SendStream {
        self.stream.as_mut().expect("stream already taken")
    }

    /// Takes the stream out of the pool permanently, preventing it from being returned.
    ///
    /// Use this when the stream is in an error state and should not be reused.
    /// The caller is responsible for closing the stream if needed.
    pub fn take_and_discard(&mut self) -> Option<SendStream> {
        self.stream.take()
    }
}

impl Drop for PooledSendStream {
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            // best effort return to pool; if channel is closed, stream is dropped
            let _ = self.return_tx.try_send(stream);
        }
    }
}
