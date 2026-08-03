//! Clean consumer-file fixture: connections come from the remote:: helpers, and prose naming
//! TcpStream::connect( or .accept() in a line comment must not be mistaken for a call.
//! Not compiled — read only by the linter.

pub async fn open_control(
    addr: std::net::SocketAddr,
    cfg: &remote::TcpConfig,
) -> anyhow::Result<tokio::net::TcpStream> {
    // remote::connect_tcp_control wraps TcpStream::connect( with the standard socket options,
    // and remote::accept_tcp_control does the same for listener.accept() on the other side
    let stream = remote::connect_tcp_control(addr, cfg).await?;
    Ok(stream)
}
