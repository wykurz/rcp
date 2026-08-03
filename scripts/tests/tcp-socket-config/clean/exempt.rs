//! Exempt-file fixture (the remote/src/tls.rs stand-in, named via TCP_CHECK_CONNECT_EXEMPT):
//! raw sockets here are tolerated by the rule-2 exemption. Not compiled — read only by the linter.

async fn handshake_fixture() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (a, b) = tokio::join!(tokio::net::TcpStream::connect(addr), listener.accept());
    (a.unwrap(), b.unwrap().0)
}
