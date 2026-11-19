//! Clean consumer-file fixture: connections come from the remote:: helpers, and prose naming
//! TcpStream::connect( or .accept() in a line comment must not be mistaken for a call.
//! Not compiled — read only by the linter.

pub async fn open_control(
    addr: std::net::SocketAddr,
    cfg: &remote::TcpConfig,
) -> anyhow::Result<tokio::net::TcpStream> {
    // remote::connect_tcp_control wraps TcpStream::connect( with the standard socket options,
    // and remote::accept_tcp_control does the same for listener.accept() on the other side.
    // nothing here calls set_keepalive( or Socket::new_raw( either: naming them is not using them
    let stream = remote::connect_tcp_control(addr, cfg).await?;
    Ok(stream)
}

/// Connecting something that is not a TCP socket is none of the linter's business: a bare
/// `.connect(` is the TLS connector's and the data pool's method name as well.
pub async fn open_tls(
    connector: &tokio_rustls::TlsConnector,
    name: rustls::pki_types::ServerName<'static>,
    stream: tokio::net::TcpStream,
) -> std::io::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
    connector.connect(name, stream).await
}
