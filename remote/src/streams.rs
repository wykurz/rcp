use futures::SinkExt;
use tokio::io::{AsyncBufRead, AsyncWriteExt};
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
}
