//! Violations fixture: raw connections and a hand-set option OUTSIDE the funnel file. Every
//! line marked EXPECT-VIOLATION must be reported at exactly that line, and nothing else.
//! Not compiled — read only by the linter.

pub async fn open_data(addr: std::net::SocketAddr) -> std::io::Result<tokio::net::TcpStream> {
    let stream = tokio::net::TcpStream::connect(addr).await?; // EXPECT-VIOLATION
    stream.set_nodelay(true)?; // EXPECT-VIOLATION
    Ok(stream)
}

pub async fn open_prepared(addr: std::net::SocketAddr) -> std::io::Result<tokio::net::TcpStream> {
    // a socket made by hand to connect later is as raw as a direct connect, whichever way it is
    // made, and setting an owned option on it through socket2 is still setting it by hand
    let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)?; // EXPECT-VIOLATION
    socket.set_tcp_nodelay(true)?; // EXPECT-VIOLATION
    socket.set_recv_buffer_size(1 << 20)?; // EXPECT-VIOLATION
    let other = tokio::net::TcpSocket::new_v4()?; // EXPECT-VIOLATION
    drop(other);
    let sixth = tokio::net::TcpSocket::new_v6()?; // EXPECT-VIOLATION
    drop(sixth);
    tokio::net::TcpSocket::from_std_stream(socket.into()).connect(addr).await
}

pub fn open_with_deadline(addr: &std::net::SocketAddr) -> std::io::Result<std::net::TcpStream> {
    // the blocking, bounded connect is a connect
    std::net::TcpStream::connect_timeout(addr, std::time::Duration::from_secs(1)) // EXPECT-VIOLATION
}

pub fn open_raw(addr: std::net::SocketAddr) -> std::io::Result<socket2::Socket> {
    let socket = socket2::Socket::new_raw(socket2::Domain::for_address(addr), socket2::Type::STREAM, None)?; // EXPECT-VIOLATION
    socket.set_keepalive(true)?; // EXPECT-VIOLATION
    Ok(socket)
}

pub fn poll_one(listener: &tokio::net::TcpListener, cx: &mut std::task::Context<'_>) -> bool {
    // accepting by polling is accepting
    listener.poll_accept(cx).is_ready() // EXPECT-VIOLATION
}

/// A function with the funnel's name in another file is not the funnel: what it sets, it sets by
/// hand.
fn configure_tcp_socket(socket: &socket2::Socket) {
    socket.set_recv_buffer_size(1 << 20).ok(); // EXPECT-VIOLATION
    socket.set_tcp_user_timeout(None).ok(); // EXPECT-VIOLATION
}

pub async fn accept_one(
    listener: &tokio::net::TcpListener,
) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
    // configuring by hand right after does not excuse the raw accept: the pairing the old
    // per-file count accepted is exactly what per-site scoping forbids outside the funnel
    let (stream, addr) = listener.accept().await?; // EXPECT-VIOLATION
    Ok((stream, addr))
}
