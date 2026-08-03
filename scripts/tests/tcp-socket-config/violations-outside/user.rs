//! Violations fixture: raw connections and a hand-set option OUTSIDE the funnel file. Every
//! line marked EXPECT-VIOLATION must be reported at exactly that line, and nothing else.
//! Not compiled — read only by the linter.

pub async fn open_data(addr: std::net::SocketAddr) -> std::io::Result<tokio::net::TcpStream> {
    let stream = tokio::net::TcpStream::connect(addr).await?; // EXPECT-VIOLATION
    stream.set_nodelay(true)?; // EXPECT-VIOLATION
    Ok(stream)
}

pub async fn accept_one(
    listener: &tokio::net::TcpListener,
) -> std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
    // configuring by hand right after does not excuse the raw accept: the pairing the old
    // per-file count accepted is exactly what per-site scoping forbids outside the funnel
    let (stream, addr) = listener.accept().await?; // EXPECT-VIOLATION
    Ok((stream, addr))
}
