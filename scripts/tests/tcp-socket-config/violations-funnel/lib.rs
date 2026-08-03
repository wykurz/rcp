//! Violations fixture: the funnel file itself. An unconfigured helper and an impl-method accept
//! must be reported (the latter fails closed as "stray" — helpers are free functions); the
//! configured helper and the funnel body must not be. Not compiled — read only by the linter.

pub fn configure_tcp_socket(stream: &tokio::net::TcpStream) {
    stream.set_nodelay(true).ok();
    let sock_ref = socket2::SockRef::from(stream);
    sock_ref.set_send_buffer_size(1).ok();
    sock_ref.set_recv_buffer_size(1).ok();
    let keepalive = socket2::TcpKeepalive::new();
    sock_ref.set_tcp_keepalive(&keepalive).ok();
    sock_ref.set_tcp_user_timeout(Some(std::time::Duration::from_secs(1))).ok();
}

/// A helper that forgets to configure what it opened — the hole the per-file count let through.
pub async fn connect_tcp_raw(addr: std::net::SocketAddr) -> std::io::Result<tokio::net::TcpStream> {
    let stream = tokio::net::TcpStream::connect(addr).await?; // EXPECT-VIOLATION
    Ok(stream)
}

/// A configured helper: not reported.
pub async fn accept_tcp_control(
    listener: &tokio::net::TcpListener,
) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
    let (stream, addr) = listener.accept().await?;
    configure_tcp_socket(&stream);
    Ok((stream, addr))
}

pub struct Acceptor {
    listener: tokio::net::TcpListener,
}

impl Acceptor {
    /// An impl method is outside any recognized top-level fn, so its accept fails closed.
    pub async fn accept_one(&self) -> std::io::Result<tokio::net::TcpStream> {
        let (stream, _addr) = self.listener.accept().await?; // EXPECT-VIOLATION
        Ok(stream)
    }
}

/// Two opens, one configure: the count backstop must fail the second open even though the first
/// is properly paired.
pub async fn connect_tcp_pair(a: std::net::SocketAddr, b: std::net::SocketAddr) -> std::io::Result<()> {
    let first = tokio::net::TcpStream::connect(a).await?;
    configure_tcp_socket(&first);
    let second = tokio::net::TcpStream::connect(b).await?; // EXPECT-VIOLATION
    drop((first, second));
    Ok(())
}

/// Configuring an UNRELATED stream must not satisfy the opened one's pairing.
pub async fn connect_tcp_misdirected(addr: std::net::SocketAddr, other: &tokio::net::TcpStream) -> std::io::Result<()> {
    let opened = tokio::net::TcpStream::connect(addr).await?; // EXPECT-VIOLATION
    configure_tcp_socket(other);
    drop(opened);
    Ok(())
}
