//! Vacuity fixture: a funnel whose body no longer contains one owned option-setter
//! (set_tcp_user_timeout). The linter must fail rather than pass vacuously — otherwise a rename
//! or a deleted option silently hollows out rule 1. Not compiled — read only by the linter.

fn configure_tcp_socket(socket: &impl std::os::fd::AsFd) {
    let sock_ref = socket2::SockRef::from(socket);
    sock_ref.set_tcp_nodelay(true).ok();
    sock_ref.set_send_buffer_size(1).ok();
    sock_ref.set_recv_buffer_size(1).ok();
    let keepalive = socket2::TcpKeepalive::new();
    sock_ref.set_tcp_keepalive(&keepalive).ok();
}
