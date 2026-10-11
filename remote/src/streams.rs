use anyhow::Context as _;
use bytes::Buf;
use futures::SinkExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tracing::instrument;

/// Bound a transfer chunk by configuration, snapshot length, and representable byte lengths.
pub fn file_buffer_size(size: u64, configured: usize) -> usize {
    // clamp before narrowing; this is a representability bound, not an available-memory limit
    configured.min(size.min(isize::MAX as u64) as usize).max(1)
}

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
        let bytes = bitcode::serialize(obj).map_err(anyhow::Error::from)?;
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

    /// Sends individual control frames with a shared final flush.
    pub async fn send_control_messages<'a, T: serde::Serialize + 'a>(
        &mut self,
        objects: impl IntoIterator<Item = &'a T>,
    ) -> anyhow::Result<()> {
        for object in objects {
            let bytes = bitcode::serialize(object).map_err(anyhow::Error::from)?;
            self.framed.feed(bytes::Bytes::from(bytes)).await?;
        }
        self.framed.flush().await?;
        Ok(())
    }

    /// Send a header and at most its snapshot byte count from an owned source descriptor.
    ///
    /// After the header flush, one initialized payload allocation moves through admitted blocking
    /// reads, then stays borrowed until each network write finishes. Borrowing the caller's guard
    /// retains admission through this future; each read also retains admission for detached work.
    /// The caller must discard the stream on failure or a returned count below `size`.
    #[instrument(level = "trace", skip(self, obj, file, admission))]
    pub async fn send_message_with_file<T: serde::Serialize, R: std::io::Read + Send + 'static>(
        &mut self,
        obj: &T,
        mut file: R,
        size: u64,
        configured: usize,
        admission: &throttle::OpenFileGuard,
    ) -> anyhow::Result<u64> {
        common::safedir::with_fd_admission(admission.admission(), async {
            self.send_batch_message(obj).await?;
            let buffer_size = file_buffer_size(size, configured);
            let mut buffer = if size == 0 {
                Vec::new()
            } else {
                vec![0; buffer_size]
            };
            let mut sent = 0;
            while sent < size {
                let read_size = (size - sent).min(buffer_size as u64) as usize;
                let (read_result, returned_file, returned_buffer) =
                    common::safedir::run_fd_admitted_blocking(move || {
                        let result = loop {
                            match file.read(&mut buffer[..read_size]) {
                                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                                result => break result,
                            }
                        };
                        Ok((result, file, buffer))
                    })
                    .await?;
                file = returned_file;
                buffer = returned_buffer;
                #[cfg(test)]
                owned_source_tests::record_buffer(&buffer);
                let count = read_result.context("failed to read source payload")?;
                if count == 0 {
                    break;
                }
                self.framed
                    .get_mut()
                    .write_all(&buffer[..count])
                    .await
                    .context("failed to write source payload")?;
                sent += count as u64;
            }
            // copy_buf flushed on completion, including premature EOF and empty payloads. preserve
            // that ordering so a flush error precedes the caller's snapshot-length check.
            self.framed
                .get_mut()
                .flush()
                .await
                .context("failed to flush source payload")?;
            Ok(sent)
        })
        .await
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
pub struct RecvStream<R = OwnedReadHalf> {
    framed: tokio_util::codec::FramedRead<R, tokio_util::codec::LengthDelimitedCodec>,
    // a boxed slice has no spare capacity: the retention bound covers the whole allocation
    copy_buffer: Box<[u8]>,
    copy_buffer_retention_limit: usize,
}

impl<R> std::fmt::Debug for RecvStream<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // neither the reader nor the framing and copy buffers may expose payload bytes
        f.debug_struct("RecvStream")
            .field("copy_buffer_len", &self.copy_buffer.len())
            .field(
                "copy_buffer_retention_limit",
                &self.copy_buffer_retention_limit,
            )
            .finish_non_exhaustive()
    }
}

impl<R: AsyncRead + Unpin> RecvStream<R> {
    pub fn new(stream: R) -> Self {
        let framed = tokio_util::codec::FramedRead::new(
            stream,
            tokio_util::codec::LengthDelimitedCodec::new(),
        );
        Self {
            framed,
            copy_buffer: Box::default(),
            copy_buffer_retention_limit: 0,
        }
    }

    /// Configure the maximum initialized copy scratch retained by this connection.
    ///
    /// Retention is disabled until configured. Zero disables it; chunks exceeding the limit
    /// use temporary storage without shrinking I/O requests. No storage is allocated here.
    pub fn with_copy_buffer_retention_limit(mut self, limit: usize) -> Self {
        self.copy_buffer_retention_limit = limit;
        if self.copy_buffer.len() > limit {
            self.copy_buffer = Box::default();
        }
        self
    }

    /// Return the retention limit configured on this receive stream.
    pub fn copy_buffer_retention_limit(&self) -> usize {
        self.copy_buffer_retention_limit
    }

    pub async fn recv_object<T: serde::de::DeserializeOwned>(
        &mut self,
    ) -> anyhow::Result<Option<T>> {
        if let Some(frame) = futures::StreamExt::next(&mut self.framed).await {
            let bytes = frame?;
            let obj = bitcode::deserialize(&bytes).map_err(anyhow::Error::from)?;
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
    /// reading subsequent messages. Initialized scratch storage up to the configured
    /// retention limit is allocated lazily and reused by this connection. Larger chunks
    /// use temporary storage without reducing the requested buffer size.
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
        let buffer_size = buffer_size.min(remaining.min(usize::MAX as u64) as usize);
        let mut temporary;
        let buf = if buffer_size <= self.copy_buffer_retention_limit {
            if self.copy_buffer.len() < buffer_size {
                self.copy_buffer = vec![0; buffer_size].into_boxed_slice();
            }
            &mut self.copy_buffer[..buffer_size]
        } else {
            // chunks above the configured retention limit use temporary storage
            temporary = vec![0; buffer_size];
            temporary.as_mut_slice()
        };
        let mut total_copied = buffered;
        loop {
            let bytes_to_read = (buf.len() as u64).min(size - total_copied) as usize;
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

#[cfg(test)]
mod coalescing_tests;

#[cfg(test)]
mod copy_buffer_tests;

#[cfg(test)]
mod owned_source_tests;
