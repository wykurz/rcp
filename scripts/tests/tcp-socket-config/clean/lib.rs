//! Clean funnel-file fixture for scripts/check-tcp-socket-config.sh: a well-formed funnel, its
//! typed entry points, configured helpers and a test module with throwaway sockets. Nothing here
//! may be reported. Not compiled — read only by the linter.

fn configure_tcp_socket(sock_ref: socket2::SockRef<'_>, send: Option<usize>, receive: Option<usize>) {
    sock_ref.set_tcp_nodelay(true).ok();
    sock_ref.set_send_buffer_size(send.unwrap_or(1)).ok();
    sock_ref.set_recv_buffer_size(receive.unwrap_or(1)).ok();
    let keepalive = socket2::TcpKeepalive::new();
    sock_ref.set_tcp_keepalive(&keepalive).ok();
    sock_ref.set_tcp_user_timeout(Some(std::time::Duration::from_secs(1))).ok();
}

// the typed entry points: each calls the funnel, and none of them opens anything
fn configure_control_connection(stream: &tokio::net::TcpStream) {
    configure_tcp_socket(socket2::SockRef::from(stream), None, None);
}

fn configure_accepted_data_connection(stream: &tokio::net::TcpStream, send: Option<usize>) {
    configure_tcp_socket(socket2::SockRef::from(stream), send, None);
}

fn configure_data_socket_before_connect(socket: &socket2::Socket, receive: Option<usize>) {
    configure_tcp_socket(socket2::SockRef::from(socket), None, receive);
}

/// A configured connect helper: the connect and its configuration share one body.
pub async fn connect_tcp_control(addr: std::net::SocketAddr) -> std::io::Result<tokio::net::TcpStream> {
    let stream = tokio::net::TcpStream::connect(addr).await?;
    configure_control_connection(&stream);
    Ok(stream)
}

/// A socket configured BEFORE it connects, the shape `connect_tcp_data` uses: the binding the
/// entry point has to name is the socket, and the connected stream needs nothing more.
pub async fn connect_tcp_data(addr: std::net::SocketAddr) -> std::io::Result<tokio::net::TcpStream> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM.nonblocking(),
        Some(socket2::Protocol::TCP),
    )?;
    configure_data_socket_before_connect(&socket, None);
    let socket = tokio::net::TcpSocket::from_std_stream(socket.into());
    socket.connect(addr).await
}

/// A configured accept helper.
pub async fn accept_tcp_control(
    listener: &tokio::net::TcpListener,
) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
    let (stream, addr) = listener.accept().await?;
    configure_control_connection(&stream);
    Ok((stream, addr))
}

#[cfg(test)]
mod tests {
    async fn connected_pair() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
        // raw sockets inside the funnel file's test module are throwaway fixtures, skipped by 2b
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (a, b) = tokio::join!(tokio::net::TcpStream::connect(addr), listener.accept());
        (a.unwrap(), b.unwrap().0)
    }
}

/// A multi-line open: the connect sits on a continuation line inside `timeout(...)` and is
/// attributed to the most recent `let` binding — the shape `connect_tcp_control` uses.
pub async fn connect_tcp_bounded(addr: std::net::SocketAddr) -> std::io::Result<tokio::net::TcpStream> {
    let stream = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::net::TcpStream::connect(addr),
    )
    .await??;
    configure_control_connection(&stream);
    Ok(stream)
}

/// A rustfmt multi-line configure call: the configured identifier is on the line after the
/// open-paren — the shape a long call takes.
pub async fn accept_tcp_data(
    listener: &tokio::net::TcpListener,
) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
    let (stream, addr) = listener.accept().await?;
    configure_accepted_data_connection(
        &stream,
        None,
    );
    Ok((stream, addr))
}
