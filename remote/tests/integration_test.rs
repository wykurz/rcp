use anyhow::Result;
use remote::port_ranges::PortRanges;

#[test]
fn test_remote_port_binding_with_ranges() -> Result<()> {
    // test that we can bind to a specific port range for UDP (used for IP detection)
    let ranges = PortRanges::parse("20000-20999")?;
    let socket = ranges
        .bind_udp_socket(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
        .map_err(|err| {
            anyhow::anyhow!("Failed to bind UDP socket in range 20000-20999: {err:#}")
        })?;
    let addr = socket.local_addr()?;
    // verify the port is within our specified range
    assert!(
        addr.port() >= 20000 && addr.port() <= 20999,
        "Port {} should be within range 20000-20999",
        addr.port()
    );
    Ok(())
}

#[tokio::test]
async fn test_remote_tcp_listener_creation_with_port_ranges() -> Result<()> {
    // test TCP listener creation with port ranges
    let config = remote::TcpConfig::default().with_port_ranges("21000-21999");
    let listener = remote::create_tcp_control_listener(&config, None).await?;
    let addr = listener.local_addr()?;
    // verify the port is within our specified range
    assert!(
        addr.port() >= 21000 && addr.port() <= 21999,
        "Port {} should be within range 21000-21999",
        addr.port()
    );
    Ok(())
}

#[tokio::test]
async fn test_remote_tcp_data_listener_creation_with_port_ranges() -> Result<()> {
    // test TCP data listener creation with port ranges
    let config = remote::TcpConfig::default().with_port_ranges("22000-22999");
    let listener = remote::create_tcp_data_listener(&config, None).await?;
    let addr = listener.local_addr()?;
    // verify the port is within our specified range
    assert!(
        addr.port() >= 22000 && addr.port() <= 22999,
        "Port {} should be within range 22000-22999",
        addr.port()
    );
    Ok(())
}

#[test]
fn test_remote_multiple_port_ranges() -> Result<()> {
    // test parsing and binding with multiple port ranges
    let ranges = PortRanges::parse("23000-23099,23200-23299,23500")?;
    let socket = ranges
        .bind_udp_socket(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
        .map_err(|err| {
            anyhow::anyhow!("Failed to bind UDP socket for multi-range test: {err:#}")
        })?;
    let addr = socket.local_addr()?;
    // verify the port is within one of our specified ranges
    let port = addr.port();
    let in_range =
        (23000..=23099).contains(&port) || (23200..=23299).contains(&port) || port == 23500;
    assert!(
        in_range,
        "Port {port} should be within one of the specified ranges"
    );
    Ok(())
}

#[tokio::test]
async fn test_remote_tcp_listener_address_resolution() -> Result<()> {
    // test that we can get an externally-routable address from a TCP listener
    let config = remote::TcpConfig::default().with_port_ranges("16000-16999");
    let listener = remote::create_tcp_control_listener(&config, None).await?;
    let addr = remote::get_tcp_listener_addr(&listener, None)?;
    // the address should have our local IP (not 0.0.0.0)
    assert!(
        !addr.ip().is_unspecified(),
        "Address should not be 0.0.0.0, got: {}",
        addr
    );
    // verify the port is within our specified range
    assert!(
        addr.port() >= 16000 && addr.port() <= 16999,
        "Port {} should be within range 16000-16999",
        addr.port()
    );
    Ok(())
}

#[tokio::test]
async fn test_remote_tcp_connect_with_timeout() -> Result<()> {
    // test TCP connection with a server
    let config = remote::TcpConfig::default();
    let listener = remote::create_tcp_control_listener(&config, Some("127.0.0.1")).await?;
    let server_addr = listener.local_addr()?;
    // spawn a task to accept the connection
    let accept_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        Ok::<_, std::io::Error>(())
    });
    // connect to the server
    let stream = remote::connect_tcp_control(server_addr, 5).await?;
    assert!(stream.nodelay()?, "TCP_NODELAY should be set");
    // wait for accept to complete
    accept_handle.await??;
    Ok(())
}
