//! Second exempt-file fixture (stand-in for a second entry in a space-separated
//! TCP_CHECK_CONNECT_EXEMPT list). Not compiled — read only by the linter.

async fn turmoil_fixture() {
    let listener = turmoil::net::TcpListener::bind(("0.0.0.0", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _ = tokio::join!(turmoil::net::TcpStream::connect(addr), listener.accept());
}
