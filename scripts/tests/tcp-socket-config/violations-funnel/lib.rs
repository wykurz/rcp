//! Violations fixture: the funnel file itself. An unconfigured helper, a helper that calls the
//! funnel past its typed entry points, and an impl-method accept must be reported (the last
//! fails closed as "stray" — helpers are free functions); the configured helper, the entry
//! points and the funnel body must not be. Not compiled — read only by the linter.

fn configure_tcp_socket(sock_ref: socket2::SockRef<'_>, send: Option<usize>, receive: Option<usize>) {
    sock_ref.set_tcp_nodelay(true).ok();
    sock_ref.set_send_buffer_size(send.unwrap_or(1)).ok();
    sock_ref.set_recv_buffer_size(receive.unwrap_or(1)).ok();
    let keepalive = socket2::TcpKeepalive::new();
    sock_ref.set_tcp_keepalive(&keepalive).ok();
    sock_ref.set_tcp_user_timeout(Some(std::time::Duration::from_secs(1))).ok();
}

fn configure_control_connection(stream: &tokio::net::TcpStream) {
    configure_tcp_socket(socket2::SockRef::from(stream), None, None);
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
    configure_control_connection(&stream);
    Ok((stream, addr))
}

/// A helper that calls the funnel itself. That does not count: the typed entry points decide
/// which buffer may be asked for at which moment, and this call asks for a receive buffer on a
/// connection that already exists.
pub async fn connect_tcp_direct(addr: std::net::SocketAddr) -> std::io::Result<tokio::net::TcpStream> {
    let stream = tokio::net::TcpStream::connect(addr).await?; // EXPECT-VIOLATION
    configure_tcp_socket(socket2::SockRef::from(&stream), None, Some(1 << 20));
    Ok(stream)
}

/// A socket created to connect later and never configured: the connect itself is not what the
/// linter pairs, the socket is.
pub async fn connect_tcp_unprepared(addr: std::net::SocketAddr) -> std::io::Result<tokio::net::TcpStream> {
    let socket = socket2::Socket::new(socket2::Domain::for_address(addr), socket2::Type::STREAM, None)?; // EXPECT-VIOLATION
    let socket = tokio::net::TcpSocket::from_std_stream(socket.into());
    socket.connect(addr).await
}

/// The same through the other constructor.
pub fn unprepared_raw(addr: std::net::SocketAddr) -> std::io::Result<socket2::Socket> {
    let socket = socket2::Socket::new_raw(socket2::Domain::for_address(addr), socket2::Type::STREAM, None)?; // EXPECT-VIOLATION
    Ok(socket)
}

/// The other spellings of an owned option are not allowed even beside the funnel.
pub fn hurry(stream: &tokio::net::TcpStream, socket: &tokio::net::TcpSocket) {
    stream.set_nodelay(true).ok(); // EXPECT-VIOLATION
    socket.set_keepalive(true).ok(); // EXPECT-VIOLATION
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
    configure_control_connection(&first);
    let second = tokio::net::TcpStream::connect(b).await?; // EXPECT-VIOLATION
    drop((first, second));
    Ok(())
}

/// Configuring an UNRELATED stream must not satisfy the opened one's pairing.
pub async fn connect_tcp_misdirected(addr: std::net::SocketAddr, other: &tokio::net::TcpStream) -> std::io::Result<()> {
    let opened = tokio::net::TcpStream::connect(addr).await?; // EXPECT-VIOLATION
    configure_control_connection(other);
    drop(opened);
    Ok(())
}
