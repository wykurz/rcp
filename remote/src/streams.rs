use bytes::Buf;
use futures::SinkExt;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tracing::instrument;

/// Framed send stream for length-delimited messages.
///
/// Generic over the underlying writer type - works with TCP, TLS, or any AsyncWrite.
#[derive(Debug)]
pub struct SendStream<W = OwnedWriteHalf> {
    framed: tokio_util::codec::FramedWrite<W, tokio_util::codec::LengthDelimitedCodec>,
}

impl<W: AsyncWrite + Unpin> SendStream<W> {
    pub fn new(stream: W) -> Self {
        let framed = tokio_util::codec::FramedWrite::new(
            stream,
            tokio_util::codec::LengthDelimitedCodec::new(),
        );
        Self { framed }
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

pub type SharedSendStream<W = OwnedWriteHalf> = std::sync::Arc<tokio::sync::Mutex<SendStream<W>>>;

/// Type alias for boxed write stream (supports both TLS and plain TCP)
pub type BoxedWrite = Box<dyn AsyncWrite + Unpin + Send>;
/// Type alias for boxed read stream (supports both TLS and plain TCP)
pub type BoxedRead = Box<dyn AsyncRead + Unpin + Send>;
/// Send stream over boxed writer
pub type BoxedSendStream = SendStream<BoxedWrite>;
/// Recv stream over boxed reader
pub type BoxedRecvStream = RecvStream<BoxedRead>;
/// Shared send stream over boxed writer
pub type BoxedSharedSendStream = SharedSendStream<BoxedWrite>;

/// Framed receive stream for length-delimited messages.
///
/// Generic over the underlying reader type - works with TCP, TLS, or any AsyncRead.
#[derive(Debug)]
pub struct RecvStream<R = OwnedReadHalf> {
    framed: tokio_util::codec::FramedRead<R, tokio_util::codec::LengthDelimitedCodec>,
}

impl<R: AsyncRead + Unpin> RecvStream<R> {
    pub fn new(stream: R) -> Self {
        let framed = tokio_util::codec::FramedRead::new(
            stream,
            tokio_util::codec::LengthDelimitedCodec::new(),
        );
        Self { framed }
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
    /// Uses a buffered reader around the TCP stream with the specified capacity.
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
        // wrap the TCP recv stream in a BufReader to control the buffer size
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
        // for TCP, we just let the stream drop - no special cleanup needed
    }
}

/// Connection wrapper for control channel (bidirectional TCP connection)
#[derive(Debug)]
pub struct ControlConnection {
    send: SendStream,
    recv: RecvStream,
}

impl ControlConnection {
    /// Create a control connection from a TCP stream
    pub fn new(stream: TcpStream) -> Self {
        let (read_half, write_half) = stream.into_split();
        Self {
            send: SendStream::new(write_half),
            recv: RecvStream::new(read_half),
        }
    }

    /// Split into send and recv halves for independent use
    pub fn into_split(self) -> (SharedSendStream, RecvStream) {
        (
            std::sync::Arc::new(tokio::sync::Mutex::new(self.send)),
            self.recv,
        )
    }

    /// Get mutable access to send stream
    pub fn send_mut(&mut self) -> &mut SendStream {
        &mut self.send
    }

    /// Get mutable access to recv stream
    pub fn recv_mut(&mut self) -> &mut RecvStream {
        &mut self.recv
    }
}
