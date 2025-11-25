use anyhow::Result;
use remote::port_ranges::PortRanges;

// test-only default values for QUIC timeouts
const DEFAULT_QUIC_IDLE_TIMEOUT_SEC: u64 = 10;
const DEFAULT_QUIC_KEEP_ALIVE_INTERVAL_SEC: u64 = 1;

#[test]
fn test_remote_port_binding_with_ranges() -> Result<()> {
    // Test that we can bind to a specific port range
    // Use a unique range for this test to avoid parallel test conflicts
    let ranges = PortRanges::parse("20000-20999")?;
    let socket = ranges
        .bind_udp_socket(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
        .map_err(|err| {
            anyhow::anyhow!("Failed to bind UDP socket in range 20000-20999: {err:#}")
        })?;
    let addr = socket.local_addr()?;
    // Verify the port is within our specified range
    assert!(
        addr.port() >= 20000 && addr.port() <= 20999,
        "Port {} should be within range 20000-20999",
        addr.port()
    );
    Ok(())
}

#[tokio::test]
async fn test_remote_quic_server_creation_with_port_ranges() -> Result<()> {
    // Test the complete QUIC server creation with port ranges
    let (endpoint, _cert_fingerprint) = remote::get_server_with_port_ranges(
        Some("21000-21999"),
        DEFAULT_QUIC_IDLE_TIMEOUT_SEC,
        DEFAULT_QUIC_KEEP_ALIVE_INTERVAL_SEC,
    )
    .map_err(|err| anyhow::anyhow!("Failed to bind QUIC server in range 21000-21999: {err:#}"))?;
    let addr = endpoint.local_addr()?;
    // Verify the port is within our specified range
    assert!(
        addr.port() >= 21000 && addr.port() <= 21999,
        "Port {} should be within range 21000-21999",
        addr.port()
    );
    Ok(())
}

#[tokio::test]
async fn test_remote_quic_client_creation_with_port_ranges() -> Result<()> {
    // Test the complete QUIC client creation with port ranges
    // We use a dummy fingerprint since we're only testing port binding, not actual connections
    let dummy_fingerprint = vec![0u8; 32]; // dummy SHA-256 fingerprint
    let endpoint = remote::get_client_with_port_ranges_and_pinning(
        Some("22000-22999"),
        dummy_fingerprint,
        DEFAULT_QUIC_IDLE_TIMEOUT_SEC,
        DEFAULT_QUIC_KEEP_ALIVE_INTERVAL_SEC,
    )
    .map_err(|err| anyhow::anyhow!("Failed to bind QUIC client in range 22000-22999: {err:#}"))?;
    let addr = endpoint.local_addr()?;
    // Verify the port is within our specified range
    assert!(
        addr.port() >= 22000 && addr.port() <= 22999,
        "Port {} should be within range 22000-22999",
        addr.port()
    );
    Ok(())
}

#[test]
fn test_remote_multiple_port_ranges() -> Result<()> {
    // Test parsing and binding with multiple port ranges
    let ranges = PortRanges::parse("23000-23099,23200-23299,23500")?;
    let socket = ranges
        .bind_udp_socket(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
        .map_err(|err| {
            anyhow::anyhow!("Failed to bind UDP socket for multi-range test: {err:#}")
        })?;
    let addr = socket.local_addr()?;
    // Verify the port is within one of our specified ranges
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
async fn test_remote_full_quic_endpoint_functionality() -> Result<()> {
    // Test that we can create both server and client with port ranges and they work together
    let (server, _cert_fingerprint) = remote::get_server_with_port_ranges(
        Some("16000-16999"),
        DEFAULT_QUIC_IDLE_TIMEOUT_SEC,
        DEFAULT_QUIC_KEEP_ALIVE_INTERVAL_SEC,
    )
    .map_err(|err| anyhow::anyhow!("Failed to bind QUIC server in range 16000-16999: {err:#}"))?;
    let server_addr = remote::get_endpoint_addr(&server)?;
    // Use a dummy fingerprint since we're only testing port binding, not actual connections
    let dummy_fingerprint = vec![0u8; 32];
    let client = remote::get_client_with_port_ranges_and_pinning(
        Some("17000-17999"),
        dummy_fingerprint,
        DEFAULT_QUIC_IDLE_TIMEOUT_SEC,
        DEFAULT_QUIC_KEEP_ALIVE_INTERVAL_SEC,
    )
    .map_err(|err| anyhow::anyhow!("Failed to bind QUIC client in range 17000-17999: {err:#}"))?;
    let client_addr = client.local_addr()?;
    // Verify both are in their respective ranges
    assert!(
        server_addr.port() >= 16000 && server_addr.port() <= 16999,
        "Server port {} should be within range 16000-16999",
        server_addr.port()
    );
    assert!(
        client_addr.port() >= 17000 && client_addr.port() <= 17999,
        "Client port {} should be within range 17000-17999",
        client_addr.port()
    );
    // Basic functionality test - we don't need to actually connect, just verify endpoints exist
    println!("Server bound to: {server_addr}");
    println!("Client bound to: {client_addr}");
    Ok(())
}
