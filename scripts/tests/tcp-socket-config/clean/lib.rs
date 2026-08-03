//! Clean funnel-file fixture for scripts/check-tcp-socket-config.sh: a well-formed funnel plus
//! configured helpers plus a test module with throwaway sockets. Nothing here may be reported.
//! Not compiled — read only by the linter.

pub fn configure_tcp_socket(stream: &tokio::net::TcpStream) {
    stream.set_nodelay(true).ok();
    let sock_ref = socket2::SockRef::from(stream);
    sock_ref.set_send_buffer_size(1).ok();
    sock_ref.set_recv_buffer_size(1).ok();
    let keepalive = socket2::TcpKeepalive::new();
    sock_ref.set_tcp_keepalive(&keepalive).ok();
    sock_ref.set_tcp_user_timeout(Some(std::time::Duration::from_secs(1))).ok();
}

/// A configured connect helper: the connect and its configuration share one body.
pub async fn connect_tcp_data(addr: std::net::SocketAddr) -> std::io::Result<tokio::net::TcpStream> {
    let stream = tokio::net::TcpStream::connect(addr).await?;
    configure_tcp_socket(&stream);
    Ok(stream)
}

/// A configured accept helper.
pub async fn accept_tcp_control(
    listener: &tokio::net::TcpListener,
) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
    let (stream, addr) = listener.accept().await?;
    configure_tcp_socket(&stream);
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
    configure_tcp_socket(&stream);
    Ok(stream)
}

/// A rustfmt multi-line configure call: the configured identifier is on the line after the
/// open-paren — the shape the real accept helpers use.
pub async fn accept_tcp_verbose(
    listener: &tokio::net::TcpListener,
) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
    let (stream, addr) = listener.accept().await?;
    configure_tcp_socket(
        &stream,
    );
    Ok((stream, addr))
}
