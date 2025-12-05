use anyhow::{anyhow, Context};

#[derive(Debug, Clone)]
pub struct PortRanges {
    ranges: Vec<std::ops::Range<u16>>,
}

impl PortRanges {
    /// Parse port ranges from a string like "8000-8999,10000-10999"
    pub fn parse(ranges_str: &str) -> anyhow::Result<Self> {
        let mut ranges = Vec::new();
        for range_str in ranges_str.split(',') {
            let range_str = range_str.trim();
            if range_str.is_empty() {
                continue;
            }
            if let Some((start_str, end_str)) = range_str.split_once('-') {
                let start: u16 = start_str
                    .trim()
                    .parse()
                    .with_context(|| format!("Invalid start port in range: {start_str}"))?;
                let end: u16 = end_str
                    .trim()
                    .parse()
                    .with_context(|| format!("Invalid end port in range: {end_str}"))?;
                if start > end {
                    return Err(anyhow!(
                        "Invalid port range: start port {} > end port {}",
                        start,
                        end
                    ));
                }
                if start == 0 {
                    return Err(anyhow!("Port 0 is not allowed in ranges"));
                }
                ranges.push(start..end + 1); // range is exclusive of end, so add 1
            } else {
                // single port
                let port: u16 = range_str
                    .parse()
                    .with_context(|| format!("Invalid port: {range_str}"))?;
                if port == 0 {
                    return Err(anyhow!("Port 0 is not allowed"));
                }
                ranges.push(port..port + 1);
            }
        }
        if ranges.is_empty() {
            return Err(anyhow!("No valid port ranges found"));
        }
        Ok(PortRanges { ranges })
    }

    /// Try to bind to a UDP socket within the specified port ranges
    pub fn bind_udp_socket(&self, ip: std::net::IpAddr) -> anyhow::Result<std::net::UdpSocket> {
        use rand::seq::SliceRandom;
        use std::time::{Duration, Instant};
        // collect all possible ports from all ranges
        let mut all_ports: Vec<u16> = Vec::new();
        for range in &self.ranges {
            all_ports.extend(range.clone());
        }
        // randomize the order to avoid always using the same ports
        let mut rng = rand::thread_rng();
        all_ports.shuffle(&mut rng);
        let start_time = Instant::now();
        // allow overriding the timeout via environment variable
        let max_duration_secs = match std::env::var("RCP_UDP_BIND_MAX_DURATION_SECONDS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            Some(x) => {
                tracing::debug!(
                    "Using custom UDP bind timeout: {x}s (from RCP_UDP_BIND_MAX_DURATION_SECONDS)",
                );
                x
            }
            None => 5,
        };
        let max_duration = Duration::from_secs(max_duration_secs);
        let mut attempts = 0;
        let mut last_error = None;
        for port in all_ports {
            if start_time.elapsed() > max_duration {
                tracing::warn!(
                    "Port binding timeout after {} attempts in {:?}",
                    attempts,
                    start_time.elapsed()
                );
                break;
            }
            attempts += 1;
            let addr = std::net::SocketAddr::new(ip, port);
            match std::net::UdpSocket::bind(addr) {
                Ok(socket) => {
                    tracing::info!(
                        "Successfully bound to manually selected port {}:{} after {} attempts",
                        ip,
                        port,
                        attempts
                    );
                    return Ok(socket);
                }
                Err(e) => {
                    tracing::debug!("Failed to bind to {}:{}: {}", ip, port, e);
                    // add small delay on port collisions to reduce thundering herd
                    let is_addr_in_use = e.kind() == std::io::ErrorKind::AddrInUse;
                    last_error = Some(e);
                    if is_addr_in_use && attempts % 10 == 0 {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        }
        Err(anyhow!(
            "Failed to bind to any port in the specified ranges after {} attempts in {:?}: {}",
            attempts,
            start_time.elapsed(),
            last_error
                .map(|e| e.to_string())
                .unwrap_or_else(|| "no ports available".to_string())
        ))
    }

    /// Try to bind to a TCP listener within the specified port ranges
    pub async fn bind_tcp_listener(
        &self,
        ip: std::net::IpAddr,
    ) -> anyhow::Result<tokio::net::TcpListener> {
        use rand::seq::SliceRandom;
        use std::time::{Duration, Instant};
        // collect all possible ports from all ranges
        let mut all_ports: Vec<u16> = Vec::new();
        for range in &self.ranges {
            all_ports.extend(range.clone());
        }
        // randomize the order to avoid always using the same ports
        let mut rng = rand::thread_rng();
        all_ports.shuffle(&mut rng);
        let start_time = Instant::now();
        // allow overriding the timeout via environment variable
        let max_duration_secs = match std::env::var("RCP_TCP_BIND_MAX_DURATION_SECONDS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            Some(x) => {
                tracing::debug!(
                    "Using custom TCP bind timeout: {x}s (from RCP_TCP_BIND_MAX_DURATION_SECONDS)",
                );
                x
            }
            None => 5,
        };
        let max_duration = Duration::from_secs(max_duration_secs);
        let mut attempts = 0;
        let mut last_error = None;
        for port in all_ports {
            if start_time.elapsed() > max_duration {
                tracing::warn!(
                    "Port binding timeout after {} attempts in {:?}",
                    attempts,
                    start_time.elapsed()
                );
                break;
            }
            attempts += 1;
            let addr = std::net::SocketAddr::new(ip, port);
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    tracing::info!(
                        "Successfully bound TCP listener to {}:{} after {} attempts",
                        ip,
                        port,
                        attempts
                    );
                    return Ok(listener);
                }
                Err(e) => {
                    tracing::debug!("Failed to bind TCP to {}:{}: {}", ip, port, e);
                    // add small delay on port collisions to reduce thundering herd
                    let is_addr_in_use = e.kind() == std::io::ErrorKind::AddrInUse;
                    last_error = Some(e);
                    if is_addr_in_use && attempts % 10 == 0 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }
        }
        Err(anyhow!(
            "Failed to bind TCP to any port in the specified ranges after {} attempts in {:?}: {}",
            attempts,
            start_time.elapsed(),
            last_error
                .map(|e| e.to_string())
                .unwrap_or_else(|| "no ports available".to_string())
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_port() {
        let ranges = PortRanges::parse("8080").unwrap();
        assert_eq!(ranges.ranges.len(), 1);
        assert_eq!(ranges.ranges[0], 8080..8081);
    }

    #[test]
    fn test_parse_range() {
        let ranges = PortRanges::parse("8000-8999").unwrap();
        assert_eq!(ranges.ranges.len(), 1);
        assert_eq!(ranges.ranges[0], 8000..9000);
    }

    #[test]
    fn test_parse_multiple_ranges() {
        let ranges = PortRanges::parse("8000-8999,10000-10999,12345").unwrap();
        assert_eq!(ranges.ranges.len(), 3);
        assert_eq!(ranges.ranges[0], 8000..9000);
        assert_eq!(ranges.ranges[1], 10000..11000);
        assert_eq!(ranges.ranges[2], 12345..12346);
    }

    #[test]
    fn test_parse_invalid_range() {
        assert!(PortRanges::parse("9000-8000").is_err()); // start > end
        assert!(PortRanges::parse("0-100").is_err()); // port 0 not allowed
        assert!(PortRanges::parse("abc").is_err()); // non-numeric
        assert!(PortRanges::parse("").is_err()); // empty
    }
}
