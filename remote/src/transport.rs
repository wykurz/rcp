//! Pluggable network transport for source↔destination control and data streams.
//!
//! Production code uses [`tcp()`], which routes every socket through [`crate`]'s configured TCP
//! funnel (`configure_tcp_socket`). Tests can supply simulated transports (such as Turmoil)
//! without changing daemon orchestration or weakening production socket-option invariants.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Bidirectional byte stream carrying a remote protocol connection.
pub trait TransportStream: AsyncRead + AsyncWrite + Unpin + Send + 'static {
    /// Remote socket address reported by this connection.
    fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr>;
    /// Split the stream into independent owned read and write halves.
    fn into_split(self: Box<Self>) -> (crate::streams::BoxedRead, crate::streams::BoxedWrite);
}

impl TransportStream for tokio::net::TcpStream {
    fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        tokio::net::TcpStream::peer_addr(self)
    }

    fn into_split(self: Box<Self>) -> (crate::streams::BoxedRead, crate::streams::BoxedWrite) {
        let (read_half, write_half) = (*self).into_split();
        (
            Box::new(read_half) as crate::streams::BoxedRead,
            Box::new(write_half) as crate::streams::BoxedWrite,
        )
    }
}

/// Type-erased [`TransportStream`] used across handshake and daemon connection boundaries.
pub struct BoxedTransportStream(Box<dyn TransportStream>);

impl std::fmt::Debug for BoxedTransportStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoxedTransportStream")
            .field("peer_addr", &self.0.peer_addr())
            .finish()
    }
}

impl BoxedTransportStream {
    /// Wrap any [`TransportStream`] implementation in a type-erased stream handle.
    pub fn new<S: TransportStream>(stream: S) -> Self {
        Self(Box::new(stream))
    }

    /// Remote socket address reported by the underlying stream.
    pub(crate) fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.0.peer_addr()
    }

    /// Split the stream into independent owned read and write halves.
    pub(crate) fn into_split(self) -> (crate::streams::BoxedRead, crate::streams::BoxedWrite) {
        self.0.into_split()
    }
}

impl From<tokio::net::TcpStream> for BoxedTransportStream {
    fn from(stream: tokio::net::TcpStream) -> Self {
        Self::new(stream)
    }
}

impl AsyncRead for BoxedTransportStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for BoxedTransportStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut *self.0).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut *self.0).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.0).poll_shutdown(cx)
    }
}

/// Bound listener accepting control or data connections on a [`Transport`].
pub trait TransportListener: Send + Sync + 'static {
    /// Address advertised to the remote peer (resolving wildcard bindings).
    fn advertised_addr(&self, bind_ip: Option<&str>) -> anyhow::Result<std::net::SocketAddr>;
    /// Target IP on this host that SSH port forwards (`-L`/`-R`) should reach.
    fn tunnel_target_ip(&self) -> anyhow::Result<std::net::IpAddr>;
    /// Accept one control connection with its configured transport options.
    fn accept_control<'a>(
        &'a self,
        config: &'a crate::TcpConfig,
    ) -> BoxFuture<'a, std::io::Result<(BoxedTransportStream, std::net::SocketAddr)>>;
    /// Accept one data connection with its configured transport options.
    ///
    /// Implementors must be cancel-safe across `tokio::select!` polls (matching
    /// `tokio::net::TcpListener::accept`) so dropping a pending future never discards an accepted
    /// connection.
    fn accept_data<'a>(
        &'a self,
        profile: crate::NetworkProfile,
        keepalive_sec: u64,
    ) -> BoxFuture<'a, std::io::Result<(BoxedTransportStream, std::net::SocketAddr)>>;
}

/// Factory for source↔destination control and data listeners and connections.
pub trait Transport: Send + Sync + 'static {
    /// Bind a listener for the control connection.
    fn bind_control<'a>(
        &'a self,
        config: &'a crate::TcpConfig,
        bind_ip: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Box<dyn TransportListener>>>;
    /// Bind a listener for pooled data connections.
    fn bind_data<'a>(
        &'a self,
        config: &'a crate::TcpConfig,
        bind_ip: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Box<dyn TransportListener>>>;
    /// Connect to the source's control listener under the default `conn_timeout_sec`.
    fn connect_control<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        config: &'a crate::TcpConfig,
    ) -> BoxFuture<'a, anyhow::Result<BoxedTransportStream>>;
    /// Connect to the source's control listener under an explicit timeout.
    fn connect_control_with_timeout<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        config: &'a crate::TcpConfig,
        timeout: std::time::Duration,
    ) -> BoxFuture<'a, anyhow::Result<BoxedTransportStream>>;
    /// Connect one data stream to the source's data listener.
    fn connect_data<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        profile: crate::NetworkProfile,
        keepalive_sec: u64,
    ) -> BoxFuture<'a, std::io::Result<BoxedTransportStream>>;
}

struct ConfiguredTcpListener(tokio::net::TcpListener);

impl TransportListener for ConfiguredTcpListener {
    fn advertised_addr(&self, bind_ip: Option<&str>) -> anyhow::Result<std::net::SocketAddr> {
        crate::get_tcp_listener_addr(&self.0, bind_ip)
    }

    fn tunnel_target_ip(&self) -> anyhow::Result<std::net::IpAddr> {
        crate::get_tcp_listener_tunnel_target_ip(&self.0)
    }

    fn accept_control<'a>(
        &'a self,
        config: &'a crate::TcpConfig,
    ) -> BoxFuture<'a, std::io::Result<(BoxedTransportStream, std::net::SocketAddr)>> {
        Box::pin(async move {
            let (stream, addr) = crate::accept_tcp_control(&self.0, config).await?;
            Ok((BoxedTransportStream::from(stream), addr))
        })
    }

    fn accept_data<'a>(
        &'a self,
        profile: crate::NetworkProfile,
        keepalive_sec: u64,
    ) -> BoxFuture<'a, std::io::Result<(BoxedTransportStream, std::net::SocketAddr)>> {
        Box::pin(async move {
            let (stream, addr) = crate::accept_tcp_data(&self.0, profile, keepalive_sec).await?;
            Ok((BoxedTransportStream::from(stream), addr))
        })
    }
}

struct ConfiguredTcpTransport;

impl Transport for ConfiguredTcpTransport {
    fn bind_control<'a>(
        &'a self,
        config: &'a crate::TcpConfig,
        bind_ip: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Box<dyn TransportListener>>> {
        Box::pin(async move {
            let listener = crate::create_tcp_control_listener(config, bind_ip).await?;
            Ok(Box::new(ConfiguredTcpListener(listener)) as Box<dyn TransportListener>)
        })
    }

    fn bind_data<'a>(
        &'a self,
        config: &'a crate::TcpConfig,
        bind_ip: Option<&'a str>,
    ) -> BoxFuture<'a, anyhow::Result<Box<dyn TransportListener>>> {
        Box::pin(async move {
            let listener = crate::create_tcp_data_listener(config, bind_ip).await?;
            Ok(Box::new(ConfiguredTcpListener(listener)) as Box<dyn TransportListener>)
        })
    }

    fn connect_control<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        config: &'a crate::TcpConfig,
    ) -> BoxFuture<'a, anyhow::Result<BoxedTransportStream>> {
        Box::pin(async move {
            let stream = crate::connect_tcp_control(addr, config).await?;
            Ok(BoxedTransportStream::from(stream))
        })
    }

    fn connect_control_with_timeout<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        config: &'a crate::TcpConfig,
        timeout: std::time::Duration,
    ) -> BoxFuture<'a, anyhow::Result<BoxedTransportStream>> {
        Box::pin(async move {
            let stream = crate::connect_tcp_control_with_timeout(addr, config, timeout).await?;
            Ok(BoxedTransportStream::from(stream))
        })
    }

    fn connect_data<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        profile: crate::NetworkProfile,
        keepalive_sec: u64,
    ) -> BoxFuture<'a, std::io::Result<BoxedTransportStream>> {
        Box::pin(async move {
            let stream = crate::connect_tcp_data(addr, profile, keepalive_sec).await?;
            Ok(BoxedTransportStream::from(stream))
        })
    }
}

/// Production TCP transport backed by [`crate`]'s configured socket funnel.
#[must_use]
pub fn tcp() -> std::sync::Arc<dyn Transport> {
    std::sync::Arc::new(ConfiguredTcpTransport)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    struct VectoredRecorder {
        vectored_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        written: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    }

    impl AsyncRead for VectoredRecorder {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for VectoredRecorder {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.written.lock().unwrap().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[std::io::IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            self.vectored_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut total = 0;
            let mut out = self.written.lock().unwrap();
            for slice in bufs {
                out.extend_from_slice(slice);
                total += slice.len();
            }
            Poll::Ready(Ok(total))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl TransportStream for VectoredRecorder {
        fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
            Ok(std::net::SocketAddr::from(([127, 0, 0, 1], 1)))
        }

        fn into_split(self: Box<Self>) -> (crate::streams::BoxedRead, crate::streams::BoxedWrite) {
            let (r, w) = tokio::io::split(*self);
            (
                Box::new(r) as crate::streams::BoxedRead,
                Box::new(w) as crate::streams::BoxedWrite,
            )
        }
    }

    #[tokio::test]
    async fn boxed_transport_stream_forwards_vectored_writes() {
        let vectored_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let written = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut stream = BoxedTransportStream::new(VectoredRecorder {
            vectored_calls: std::sync::Arc::clone(&vectored_calls),
            written: std::sync::Arc::clone(&written),
        });
        assert!(stream.is_write_vectored());
        let bufs = [
            std::io::IoSlice::new(b"hello "),
            std::io::IoSlice::new(b"vectored"),
        ];
        let n = stream.write_vectored(&bufs).await.unwrap();
        assert_eq!(n, 14);
        assert_eq!(vectored_calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(&*written.lock().unwrap(), b"hello vectored");
    }
}
