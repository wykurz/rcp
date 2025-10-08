# Network Connectivity Handling in rcp

This document describes how rcp handles various network connectivity scenarios and provides guidance for testing.

## Architecture Overview

rcp uses a three-node architecture for remote copying:

```
Master (rcp)
├── SSH → Source Host (rcpd in source mode)
│   └── QUIC → Master
│   └── QUIC Server (waits for Destination)
└── SSH → Destination Host (rcpd in destination mode)
    └── QUIC → Master
    └── QUIC Client → Source
```

## Connection Flow

1. **Master starts rcpd processes via SSH**
   - Master SSHs to source host and starts rcpd
   - Master SSHs to destination host and starts rcpd

2. **rcpd processes connect back to Master via QUIC**
   - Source rcpd connects to Master
   - Destination rcpd connects to Master

3. **Source waits for Destination connection**
   - Source rcpd starts QUIC server
   - Source sends its address to Master
   - Master forwards address to Destination
   - Destination connects to Source via QUIC

4. **Data transfer**
   - Source sends files to Destination
   - Destination acknowledges completion

## Failure Scenarios and Handling

### 1. Master Cannot SSH to Source/Destination

**Scenario**: SSH connection fails (host unreachable, auth failure, etc.)

**Handling**:
- openssh library returns error immediately
- Master displays error and exits
- Timeout: SSH has its own connection timeout (~30s by default)

**Error Message**: Standard SSH error messages

### 2. rcpd Binary Not Found on Remote Host

**Scenario**: rcpd binary doesn't exist in expected location on remote host

**Handling**:
- SSH command execution fails
- Master waits for rcpd to connect (timeout: configurable, default 15s)
- Master returns timeout error

**Error Message**:
```
Timed out waiting for source/destination rcpd to connect after 15s.
Check if source/destination host is reachable and rcpd can be executed.
```

### 3. rcpd Cannot Connect to Master

**Scenario**: QUIC connection from rcpd to Master fails (firewall, network issue)

**Handling**:
- rcpd connection attempt times out (QUIC default timeout)
- rcpd exits with error
- Master waits for connection (timeout: configurable, default 15s)
- Master returns timeout error

**Error Message** (in rcpd logs):
```
Failed to connect to master at <addr>.
This usually means the master is unreachable from this host.
Check network connectivity and firewall rules.
```

**Error Message** (in Master):
```
Timed out waiting for source/destination rcpd to connect after 15s.
Check if source/destination host is reachable and rcpd can be executed.
```

### 4. Destination Cannot Connect to Source (**Most Common**)

**Scenario**: Destination rcpd cannot reach Source rcpd's QUIC server (firewall, routing issue)

**Handling**:
- Source waits for Destination connection (timeout: configurable, default 15s)
- Destination connection attempt times out (QUIC default timeout)
- Both return errors

**Error Message** (in Source):
```
Timed out waiting for destination to connect after 15s.
This usually means the destination cannot reach the source.
Check network connectivity and firewall rules.
```

**Error Message** (in Destination):
```
Failed to connect to source at <addr>.
This usually means the source is unreachable from the destination.
Check network connectivity and firewall rules.
```

## Timeout Values

- **SSH connection**: ~30s (openssh default, configurable via SSH config)
- **QUIC connection attempt**: ~10s (quinn library default)
- **Waiting for rcpd to connect to Master**: 15s (default, configurable via `--remote-copy-conn-timeout-sec`)
- **Waiting for Destination to connect to Source**: 15s (default, configurable via `--remote-copy-conn-timeout-sec`)

The `--remote-copy-conn-timeout-sec` argument can be used with both `rcp` and `rcpd` to customize the timeout for remote copy connections. For example:

```bash
rcp --remote-copy-conn-timeout-sec 20 source:/path dest:/path
```

## Troubleshooting

### Connection Times Out

1. **Check firewall rules**: Ensure QUIC ports (especially `--quic-port-ranges`) are open
2. **Check routing**: Ensure hosts can reach each other (use `ping`, `traceroute`)
3. **Check rcpd binary**: Ensure rcpd exists and is executable on remote hosts
4. **Check NAT**: If hosts are behind NAT, ensure proper port forwarding
5. **Use verbose logging**: Run with `-vv` to see detailed connection attempts
