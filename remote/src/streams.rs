use futures::SinkExt;
use tokio::io::AsyncWriteExt;

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

    pub async fn send_message_with_data<T: serde::Serialize, R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        obj: &T,
        reader: &mut R,
    ) -> anyhow::Result<u64> {
        self.send_batch_message(obj).await?;
        let mut data_stream = self.framed.get_mut();
        let bytes_copied = tokio::io::copy(reader, &mut data_stream).await?;
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

    pub async fn open_bi(&self) -> anyhow::Result<(SharedSendStream, RecvStream)> {
        let (send_stream, recv_stream) = self.inner.open_bi().await?;
        let send_stream = SendStream::new(send_stream).await?;
        let recv_stream = RecvStream::new(recv_stream).await?;
        Ok((
            std::sync::Arc::new(tokio::sync::Mutex::new(send_stream)),
            recv_stream,
        ))
    }

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
