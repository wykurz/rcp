//! Funnel stand-in for the violations-outside fixture: well-formed (all owned option-setters
//! present) so the vacuity check stays quiet — the marked violations live in user.rs.
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
