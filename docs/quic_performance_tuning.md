# QUIC Performance Tuning

This document explains how to tune `rcp` remote copy performance for different network environments.

## Overview

Remote copy operations in `rcp` use the QUIC protocol, which provides reliable, encrypted data transfer. QUIC performance depends heavily on proper configuration of flow control windows and congestion control algorithms to match network characteristics.

## Network Profiles

`rcp` provides two network profiles that configure QUIC parameters for different environments:

### LAN Profile (Default)

Optimized for datacenter and local network environments with:
- Low latency (<1ms RTT)
- High bandwidth (25-100 Gbps)
- Dedicated or lightly-shared links

```bash
rcp --network-profile=lan source:/data dest:/data
```

**Parameters:**
| Setting | Value | Rationale |
|---------|-------|-----------|
| Receive window | 128 MiB | Connection-level flow control sized for 100 Gbps @ 1ms |
| Stream receive window | 16 MiB | Per-stream flow control with headroom for multiple streams |
| Send window | 128 MiB | Matches receive window for balanced bidirectional flow |
| Initial RTT estimate | 0.3ms | Low-latency assumption for aggressive pacing |
| Congestion control | BBR | Model-based algorithm for fast ramp-up |

### WAN Profile

Conservative settings for internet and shared network conditions:
- Variable latency (10-200ms RTT)
- Moderate bandwidth
- Shared infrastructure where fairness matters

```bash
rcp --network-profile=wan source:/data dest:/data
```

**Parameters:**
| Setting | Value | Rationale |
|---------|-------|-----------|
| Receive window | 8 MiB | Standard sizing for internet conditions |
| Stream receive window | 2 MiB | Conservative per-stream allocation |
| Send window | 8 MiB | Matches receive window |
| Initial RTT estimate | 100ms | Safe default for variable latency |
| Congestion control | CUBIC | Loss-based algorithm for fair sharing |

## Congestion Control

Each profile has a default congestion control algorithm, but this can be overridden:

```bash
# LAN windows with CUBIC (for shared datacenter networks)
rcp --network-profile=lan --congestion-control=cubic source:/data dest:/data

# WAN windows with BBR (for dedicated WAN links)
rcp --network-profile=wan --congestion-control=bbr source:/data dest:/data
```

### BBR (Bottleneck Bandwidth and RTT)

- **Model-based**: Estimates bandwidth and RTT rather than reacting to packet loss
- **Fast ramp-up**: Can fill available bandwidth in fewer round trips
- **Best for**: Dedicated links, shallow buffer switches (common in datacenters)
- **Caveats**: May be aggressive when competing with CUBIC flows on shared links

### CUBIC

- **Loss-based**: Reduces window by 30% on packet loss
- **Gradual ramp-up**: Slow start doubles window per RTT until loss detected
- **Best for**: Shared networks, deep buffer environments, internet traffic
- **Caveats**: Any packet loss is interpreted as congestion; slower to reach peak throughput

## Advanced Tuning

For specialized environments, individual QUIC parameters can be overridden. These accept human-readable byte values:

```bash
# Ultra-aggressive for 100 Gbps dedicated link
rcp --quic-receive-window=256MiB \
    --quic-stream-receive-window=32MiB \
    --quic-send-window=256MiB \
    source:/data dest:/data

# Conservative for constrained memory environment
rcp --quic-receive-window=16MiB \
    --quic-stream-receive-window=4MiB \
    source:/data dest:/data
```

### Available Parameters

| Flag | Description | Default (LAN) | Default (WAN) |
|------|-------------|---------------|---------------|
| `--quic-receive-window=<SIZE>` | Connection-level receive window | 128 MiB | 8 MiB |
| `--quic-stream-receive-window=<SIZE>` | Per-stream receive window | 16 MiB | 2 MiB |
| `--quic-send-window=<SIZE>` | Send window | 128 MiB | 8 MiB |
| `--quic-initial-rtt-ms=<MS>` | Initial RTT estimate (supports decimals, e.g., 0.3) | 0.3 | 100 |
| `--quic-initial-mtu=<BYTES>` | Initial MTU size | 1200 | 1200 |

Size values accept formats like: `128MiB`, `1GiB`, `16777216` (bytes).

## Understanding Flow Control

QUIC uses flow control windows to prevent senders from overwhelming receivers. The key insight is that these windows should be sized based on the **Bandwidth-Delay Product (BDP)** of the network:

```
BDP = Bandwidth × RTT
```

For example:
- **100 Gbps @ 1ms RTT**: BDP = 12.5 GB/s × 0.001s = 12.5 MiB
- **1 Gbps @ 100ms RTT**: BDP = 125 MB/s × 0.1s = 12.5 MiB

Windows should be at least as large as the BDP to fully utilize the link. The LAN profile uses 10x the BDP for headroom with multiple concurrent streams.

## Memory Considerations

Larger flow control windows require more memory:

- LAN profile: ~256 MiB per connection (128 MiB receive + 128 MiB send)
- WAN profile: ~16 MiB per connection

For servers with limited memory or many concurrent connections, consider:
1. Using the WAN profile
2. Reducing window sizes via advanced tuning flags
3. Limiting concurrent remote copy operations

## Troubleshooting

### Slow throughput on LAN

1. Verify low RTT: `ping -c 10 destination-host`
2. Check for packet loss which can trigger congestion control backoff
3. Consider if the network is truly dedicated (BBR may be aggressive on shared networks)

### High retransmission rates

1. Switch from BBR to CUBIC: `--congestion-control=cubic`
2. This is often caused by shallow switch buffers or competing traffic

### Memory pressure on remote hosts

1. Use WAN profile or reduce window sizes
2. Check system memory and adjust `--max-workers` to limit concurrency

## Configuration on rcpd

When `rcp` spawns remote `rcpd` daemons, it automatically passes the QUIC tuning configuration. The remote daemons use the same profile and settings as the master process, ensuring consistent behavior across all nodes in a multi-host copy.

## References

- [Quinn TransportConfig documentation](https://docs.rs/quinn/latest/quinn/struct.TransportConfig.html)
- [RFC 9002: QUIC Loss Detection and Congestion Control](https://datatracker.ietf.org/doc/rfc9002/)
- [BBR Congestion Control (ACM Queue)](https://queue.acm.org/detail.cfm?id=3022184)
- [When to use and not use BBR (APNIC)](https://blog.apnic.net/2020/01/10/when-to-use-and-not-use-bbr/)
