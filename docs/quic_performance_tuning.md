# QUIC Performance Tuning for High-Throughput Remote Copies

## 1. Problem Statement

### 1.1 Observed Performance

When copying a 100 GiB fileset of 4 MiB files over a low-latency LAN (~0.35ms RTT), rcp achieves:
- ~190 MiB/s average throughput
- Slow ramp-up from ~170 MiB/s to ~270 MiB/s over ~10 minutes
- Below rsync performance (~230 MiB/s)

The target environment (single datacenter with 25-100 Gbps NICs, <1ms RTT) should be capable of multi-GiB/s throughput.

### 1.2 Root Cause Analysis

The slow ramp-up and limited throughput are caused by **conservative default QUIC parameters** in quinn that are tuned for internet conditions (100 Mbps @ 100ms RTT), not datacenter LANs.

Key bottlenecks identified:

1. **Congestion control slow start**: CUBIC starts with ~14KB initial window and grows exponentially then logarithmically. On a high-bandwidth path, it takes many RTTs to fill the pipe.

2. **Conservative flow control windows**: Default `stream_receive_window` is ~1.25 MB (calculated for 100ms RTT × 100 Mbps). For a 0.35ms RTT LAN with 25+ Gbps capacity, this is drastically undersized.

3. **Default initial RTT estimate**: Quinn assumes 100ms RTT initially, causing overly conservative pacing in the first seconds.

4. **Per-stream flow control**: Each new unidirectional stream starts fresh with its own flow control window.

## 2. QUIC/Quinn Background

### 2.1 Current Configuration

The current code (`remote/src/lib.rs:878-912`) only configures:
```rust
transport_config.max_idle_timeout(Some(Duration::from_secs(idle_timeout_sec)));
transport_config.keep_alive_interval(Some(Duration::from_secs(keep_alive_interval_sec)));
```

All other parameters use quinn defaults.

### 2.2 Quinn Default Values

| Parameter | Default | Calculation/Notes |
|-----------|---------|-------------------|
| `stream_receive_window` | ~1.25 MB | 100ms × 100 Mbps |
| `receive_window` | VarInt::MAX | Connection-level (unlimited) |
| `send_window` | ~10 MB | 8 × stream_receive_window |
| `initial_rtt` | 333ms | Conservative estimate |
| Congestion controller | CUBIC | RFC 8312 |
| `initial_window` (CUBIC) | ~14.7 KB | Per RFC 9002 |
| `initial_mtu` | 1200 bytes | Safe for internet |

### 2.3 Congestion Control Algorithms

Quinn supports three congestion control algorithms:

**CUBIC**:
- Loss-based: reduces window by 30% on packet loss
- Slow start doubles window per RTT until loss
- Good for: general internet, deep buffer networks, shared infrastructure
- Issues: interprets any packet loss as congestion; slow to reach high bandwidth

**NewReno**:
- Simpler loss-based algorithm
- Similar characteristics to CUBIC but less aggressive recovery
- Generally lower performance than CUBIC

**BBR** (Bottleneck Bandwidth and Round-trip propagation time):
- Model-based: estimates bandwidth and RTT, not loss-based
- Maintains estimated bandwidth-delay product (BDP) as target
- Faster ramp-up: can fill pipe in fewer RTTs
- Good for: high-BDP networks, shallow buffers (common in datacenter switches), dedicated links
- Caveats:
  - Marked experimental in quinn
  - Can cause higher retransmission rates in some scenarios
  - May be unfair when competing with CUBIC flows on shared links
  - Best suited for dedicated/isolated networks

**Decision**: BBR is the default for rcp. The primary use case is datacenter file transfers on dedicated high-bandwidth links where BBR's characteristics (fast ramp-up, not fooled by shallow buffers) are ideal. Users on shared networks or WAN can switch to CUBIC via `--congestion-control=cubic`.

## 3. Design Decisions

Based on the target environment (25-100 Gbps NICs, <1ms RTT, datacenter):

1. **Default profile**: `lan` - optimized for datacenter use case
2. **Default congestion control**: `bbr` - fastest ramp-up for dedicated links
3. **Initial window**: 8 MB - aggressive to saturate high-bandwidth links quickly
4. **Flow control windows**: Sized for 100 Gbps @ 1ms (12.5 MB BDP)

## 4. Proposed Configuration

### 4.1 Network Profiles

Provide network profile presets via CLI:

```
--network-profile=<lan|wan>    (default: lan)
```

| Profile | Target Environment | Congestion Control | Characteristics |
|---------|-------------------|-------------------|-----------------|
| `lan` | Datacenter, <1ms RTT, 25-100 Gbps | BBR | Aggressive windows, low initial_rtt |
| `wan` | Internet, variable RTT | CUBIC | Conservative defaults |

### 4.2 LAN Profile Parameters (Default)

Target: Saturate 100 Gbps NIC within 30 seconds on <1ms RTT network.

**Bandwidth-Delay Product Calculation:**
- 100 Gbps = 12.5 GB/s = 12,500 MB/s
- 1ms RTT = 0.001s
- BDP = 12,500 MB/s × 0.001s = 12.5 MB

**With safety margin for multiple streams and bursty traffic:**
- Per-stream window: 16 MB
- Connection window: 128 MB
- Initial congestion window: 8 MB (to reach ~6 GB/s in first RTT)

```rust
// LAN profile configuration
fn configure_lan_profile(transport_config: &mut TransportConfig) {
    // Flow control: sized for 100 Gbps @ 1ms RTT
    // BDP = 12.5 MB, use 10x for headroom with multiple streams
    transport_config.receive_window(VarInt::from_u32(128 * 1024 * 1024));      // 128 MB
    transport_config.stream_receive_window(VarInt::from_u32(16 * 1024 * 1024)); // 16 MB per stream
    transport_config.send_window(128 * 1024 * 1024);                            // 128 MB

    // RTT: assume very low latency datacenter
    transport_config.initial_rtt(Duration::from_micros(300)); // 0.3ms

    // Congestion control: BBR for fast ramp-up on dedicated links
    transport_config.congestion_controller_factory(Arc::new(BbrConfig::default()));

    // Note: BBR doesn't use initial_window the same way CUBIC does,
    // but we set it for fallback/compatibility
}
```

### 4.3 WAN Profile Parameters

Conservative settings for internet/shared network conditions:

```rust
// WAN profile configuration
fn configure_wan_profile(transport_config: &mut TransportConfig) {
    // Flow control: standard sizing for internet
    transport_config.receive_window(VarInt::from_u32(8 * 1024 * 1024));        // 8 MB
    transport_config.stream_receive_window(VarInt::from_u32(2 * 1024 * 1024));  // 2 MB
    transport_config.send_window(8 * 1024 * 1024);                              // 8 MB

    // RTT: conservative estimate for internet
    transport_config.initial_rtt(Duration::from_millis(100));

    // Congestion control: CUBIC for fairness on shared networks
    transport_config.congestion_controller_factory(Arc::new(CubicConfig::default()));
}
```

### 4.4 Congestion Control Override

Allow explicit congestion control selection independent of profile:

```
--congestion-control=<bbr|cubic>    (default: bbr for lan, cubic for wan)
```

This allows combinations like:
- `--network-profile=lan --congestion-control=cubic` - LAN windows with CUBIC (for shared datacenter networks)
- `--network-profile=wan --congestion-control=bbr` - WAN windows with BBR (for dedicated WAN links)

### 4.5 Advanced Tuning Flags

For experimentation and edge cases, expose individual parameters:

```
--quic-receive-window=<bytes>         Connection-level receive window
--quic-stream-receive-window=<bytes>  Per-stream receive window
--quic-send-window=<bytes>            Send window
--quic-initial-rtt-ms=<milliseconds>  Initial RTT estimate
--quic-initial-mtu=<bytes>            Initial MTU (default: 1200)
```

These override profile settings when specified.

**Example - Ultra-aggressive for 100 Gbps dedicated link:**
```bash
rcp --quic-receive-window=268435456 \
    --quic-stream-receive-window=33554432 \
    --quic-send-window=268435456 \
    source:/data dest:/data
```

## 5. Implementation Plan

### 5.1 New Types and Structures

```rust
// In remote/src/lib.rs or new remote/src/profile.rs

/// Network profile for QUIC configuration
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NetworkProfile {
    #[default]
    Lan,
    Wan,
}

/// Congestion control algorithm selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CongestionControl {
    Bbr,
    Cubic,
}

impl Default for CongestionControl {
    fn default() -> Self {
        Self::Bbr  // BBR is default
    }
}

/// Advanced QUIC tuning parameters (all optional overrides)
#[derive(Debug, Clone, Default)]
pub struct QuicTuning {
    pub receive_window: Option<u32>,
    pub stream_receive_window: Option<u32>,
    pub send_window: Option<u32>,
    pub initial_rtt_ms: Option<u32>,
    pub initial_mtu: Option<u16>,
}

/// Extended QUIC configuration
#[derive(Debug, Clone)]
pub struct QuicConfig {
    pub port_ranges: Option<String>,
    pub idle_timeout_sec: u64,
    pub keep_alive_interval_sec: u64,
    pub conn_timeout_sec: u64,
    // New fields
    pub network_profile: NetworkProfile,
    pub congestion_control: CongestionControl,
    pub tuning: QuicTuning,
}
```

### 5.2 Configuration Application

```rust
fn apply_quic_config(transport_config: &mut TransportConfig, config: &QuicConfig) {
    // 1. Apply base profile
    match config.network_profile {
        NetworkProfile::Lan => apply_lan_profile(transport_config),
        NetworkProfile::Wan => apply_wan_profile(transport_config),
    }

    // 2. Apply congestion control (may override profile default)
    match config.congestion_control {
        CongestionControl::Bbr => {
            transport_config.congestion_controller_factory(Arc::new(BbrConfig::default()));
        }
        CongestionControl::Cubic => {
            transport_config.congestion_controller_factory(Arc::new(CubicConfig::default()));
        }
    }

    // 3. Apply individual overrides
    if let Some(v) = config.tuning.receive_window {
        transport_config.receive_window(VarInt::from_u32(v));
    }
    if let Some(v) = config.tuning.stream_receive_window {
        transport_config.receive_window(VarInt::from_u32(v));
    }
    if let Some(v) = config.tuning.send_window {
        transport_config.send_window(v as u64);
    }
    if let Some(v) = config.tuning.initial_rtt_ms {
        transport_config.initial_rtt(Duration::from_millis(v as u64));
    }
    if let Some(v) = config.tuning.initial_mtu {
        transport_config.initial_mtu(v);
    }
}
```

### 5.3 CLI Arguments

Add to both `rcp` and `rcpd`:

```rust
/// Network profile for QUIC tuning
#[arg(long, default_value = "lan", value_parser = parse_network_profile)]
network_profile: NetworkProfile,

/// Congestion control algorithm
#[arg(long, default_value = "bbr", value_parser = parse_congestion_control)]
congestion_control: CongestionControl,

/// QUIC receive window in bytes (overrides profile)
#[arg(long)]
quic_receive_window: Option<u32>,

/// QUIC per-stream receive window in bytes (overrides profile)
#[arg(long)]
quic_stream_receive_window: Option<u32>,

/// QUIC send window in bytes (overrides profile)
#[arg(long)]
quic_send_window: Option<u32>,

/// Initial RTT estimate in milliseconds (overrides profile)
#[arg(long)]
quic_initial_rtt_ms: Option<u32>,

/// Initial MTU in bytes (overrides profile)
#[arg(long)]
quic_initial_mtu: Option<u16>,
```

### 5.4 RcpdConfig Updates

Extend `RcpdConfig` in `remote/src/protocol.rs` to include the new parameters so they're passed to remote daemons.

### 5.5 Implementation Order

1. Add new types (`NetworkProfile`, `CongestionControl`, `QuicTuning`)
2. Extend `QuicConfig` with new fields
3. Implement `apply_lan_profile()` and `apply_wan_profile()`
4. Modify `configure_server()` and `create_client_endpoint()` to use profiles
5. Add CLI arguments to `rcp` and `rcpd`
6. Update `RcpdConfig` to pass settings to remote daemons
7. Update tests

## 6. Testing Strategy

### 6.1 Unit Tests

- Profile application produces expected TransportConfig values
- CLI parsing handles all argument combinations
- Default values are correct

### 6.2 Integration Tests

- Existing remote copy tests pass with new defaults
- Remote copy works with `--network-profile=wan`
- Remote copy works with `--congestion-control=cubic`
- Individual overrides work correctly

### 6.3 Performance Benchmarks

Create benchmark suite measuring:

| Scenario | Metric |
|----------|--------|
| 100 GiB of 4 MiB files | Time to complete, average throughput |
| 10 GiB of 1 KB files | Time to complete (tests overhead) |
| 100 GiB single file | Peak throughput |
| Time to 90% throughput | Ramp-up speed |

Compare:
- Before (current defaults)
- After with LAN profile + BBR
- After with WAN profile + CUBIC

### 6.4 Stress Testing

- Verify no memory exhaustion with large windows
- Test behavior when actual network is slower than configured
- Test profile mismatch between source and destination

## 7. Expected Results

### 7.1 LAN Profile + BBR

| Metric | Before | Expected After |
|--------|--------|----------------|
| Initial throughput | ~170 MiB/s | >1 GiB/s |
| Time to 90% peak | ~10 minutes | <30 seconds |
| Peak throughput | ~270 MiB/s | Limited by storage/NIC |

### 7.2 Memory Impact

LAN profile increases memory usage:
- Per-connection: ~128 MB receive buffer + ~128 MB send buffer
- For single source-destination pair: ~512 MB total
- Acceptable for datacenter servers with ample RAM

### 7.3 Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Large windows on slow networks | Provide WAN profile; document when to use |
| BBR retransmission overhead | Can switch to CUBIC; monitor in benchmarks |
| Memory pressure | Documented; can reduce windows via CLI |
| BBR experimental status | Can switch to CUBIC if issues arise |

## 8. Future Work

### 8.1 Auto-Detection (Phase 2)

Measure RTT during connection setup and automatically select profile:
- RTT < 5ms → LAN profile
- RTT >= 5ms → WAN profile

### 8.2 Stream Multiplexing (Phase 3)

For many small files, consider:
- Batching multiple files per stream to avoid per-stream overhead
- Requires protocol changes

### 8.3 Kernel Offload Optimization

Ensure UDP socket options enable:
- GSO (Generic Segmentation Offload) on send
- GRO (Generic Receive Offload) on receive

Quinn enables GSO by default, but verify GRO is active.

## 9. References

- [Quinn TransportConfig documentation](https://docs.rs/quinn/latest/quinn/struct.TransportConfig.html)
- [Quinn congestion control module](https://docs.rs/quinn-proto/latest/quinn_proto/congestion/)
- [RFC 9002: QUIC Loss Detection and Congestion Control](https://datatracker.ietf.org/doc/rfc9002/)
- [BBR Congestion Control (ACM Queue)](https://queue.acm.org/detail.cfm?id=3022184)
- [When to use and not use BBR (APNIC)](https://blog.apnic.net/2020/01/10/when-to-use-and-not-use-bbr/)
- [QUIC is not Quick Enough over Fast Internet (arXiv)](https://arxiv.org/html/2310.09423v2)
