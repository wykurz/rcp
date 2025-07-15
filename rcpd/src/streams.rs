use futures::SinkExt;

/// Hello message to prevent QUIC stream hanging
const HELLO_MESSAGE: &[u8] = b"HELLO_STREAM";

/// A wrapper around quinn::SendStream with framed codec support
#[derive(Debug)]
pub struct SendStream {
    framed:
        tokio_util::codec::FramedWrite<quinn::SendStream, tokio_util::codec::LengthDelimitedCodec>,
}

impl SendStream {
    /// Create a new SendStream from a quinn::SendStream and send hello message
    pub async fn new(mut stream: quinn::SendStream) -> anyhow::Result<Self> {
        // Send hello message to notify peer that stream is ready
        stream.write_all(HELLO_MESSAGE).await?;

        let framed = tokio_util::codec::FramedWrite::new(
            stream,
            tokio_util::codec::LengthDelimitedCodec::new(),
        );
        Ok(Self { framed })
    }

    /// Send a serialized object
    pub async fn send_object<T: serde::Serialize>(&mut self, obj: &T) -> anyhow::Result<()> {
        let bytes = bincode::serialize(obj)?;
        self.framed.send(bytes::Bytes::from(bytes)).await?;
        Ok(())
    }

    /// Get mutable reference to the underlying stream for raw data transfer
    pub fn get_mut(&mut self) -> &mut quinn::SendStream {
        self.framed.get_mut()
    }
}

impl Drop for SendStream {
    fn drop(&mut self) {
        // Close the underlying stream when SendStream is dropped
        std::mem::drop(self.framed.close());
    }
}

/// A wrapper around quinn::RecvStream with framed codec support
#[derive(Debug)]
pub struct RecvStream {
    framed:
        tokio_util::codec::FramedRead<quinn::RecvStream, tokio_util::codec::LengthDelimitedCodec>,
}

impl RecvStream {
    /// Create a new RecvStream from a quinn::RecvStream and read hello message
    pub async fn new(mut stream: quinn::RecvStream) -> anyhow::Result<Self> {
        // Read hello message to confirm stream is ready
        let mut hello_buf = vec![0u8; HELLO_MESSAGE.len()];
        stream.read_exact(&mut hello_buf).await?;

        if hello_buf != HELLO_MESSAGE {
            return Err(anyhow::anyhow!("Invalid hello message received"));
        }

        let framed = tokio_util::codec::FramedRead::new(
            stream,
            tokio_util::codec::LengthDelimitedCodec::new(),
        );
        Ok(Self { framed })
    }

    /// Receive a serialized object
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

    /// Get the read buffer from the framed reader
    pub fn read_buffer(&self) -> &[u8] {
        self.framed.read_buffer()
    }

    /// Convert into the underlying stream
    pub fn into_inner(self) -> quinn::RecvStream {
        self.framed.into_inner()
    }
}

/// Connection wrapper that provides framed stream creation
#[derive(Clone, Debug)]
pub struct Connection {
    inner: quinn::Connection,
}

impl Connection {
    /// Create a new Connection wrapper
    pub fn new(conn: quinn::Connection) -> Self {
        Self { inner: conn }
    }

    /// Open a bidirectional stream and return wrapped send/recv streams
    pub async fn open_bi(&self) -> anyhow::Result<(SendStream, RecvStream)> {
        let (send_stream, recv_stream) = self.inner.open_bi().await?;
        let send_stream = SendStream::new(send_stream).await?;
        let recv_stream = RecvStream::new(recv_stream).await?;
        Ok((send_stream, recv_stream))
    }

    /// Open a unidirectional stream and return wrapped send stream
    pub async fn open_uni(&self) -> anyhow::Result<SendStream> {
        let send_stream = self.inner.open_uni().await?;
        SendStream::new(send_stream).await
    }

    /// Accept a bidirectional stream and return wrapped send/recv streams
    pub async fn accept_bi(&self) -> anyhow::Result<(SendStream, RecvStream)> {
        let (send_stream, recv_stream) = self.inner.accept_bi().await?;
        let send_stream = SendStream::new(send_stream).await?;
        let recv_stream = RecvStream::new(recv_stream).await?;
        Ok((send_stream, recv_stream))
    }

    /// Accept a unidirectional stream and return wrapped recv stream
    pub async fn accept_uni(&self) -> anyhow::Result<RecvStream> {
        let recv_stream = self.inner.accept_uni().await?;
        RecvStream::new(recv_stream).await
    }
}

impl std::ops::Deref for Connection {
    type Target = quinn::Connection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
