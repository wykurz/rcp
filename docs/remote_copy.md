# Remote Copy Operations

This document covers the operational aspects of rcp's remote copy functionality: binary discovery, version checking, automatic deployment, and network connectivity.

For the detailed protocol specification (message types, flows, invariants), see **[remote_protocol.md](remote_protocol.md)**.

## Overview

The rcp remote copy system uses a three-node architecture:

```
         Master (rcp)
           /    \
          /      \
       SSH      SSH
        |        |
       TLS      TLS
        |        |
Source (rcpd)--TLS--Destination (rcpd)
              control + data
```

**Key components**:
- **Master (rcp)**: Coordinates the operation, runs on user's machine
- **Source (rcpd)**: Reads and sends files
- **Destination (rcpd)**: Receives and writes files

The master connects to remote hosts via SSH, spawns rcpd processes, and coordinates the transfer. All TCP connections (Master↔rcpd and Source↔Destination) are encrypted with TLS by default.

## TLS Encryption

By default, all TCP connections are encrypted using TLS 1.3 with self-signed certificates and fingerprint pinning.

### Security Properties

| Connection | Authentication | Encryption |
|------------|---------------|------------|
| Master → rcpd | Certificate fingerprint (via SSH stdout) | TLS 1.3 |
| Source ↔ Destination | Certificate fingerprint (via Master) | TLS 1.3 |

**Key features**:
- **Forward secrecy**: Ephemeral keys per session
- **Mutual authentication**: Both parties verify each other's certificates
- **No trust anchor**: Self-signed certificates with fingerprint pinning (no CA)
- **Transparent**: No configuration needed (enabled by default)

### Connection Flow

1. **Master spawns rcpd via SSH**: `ssh host "rcpd --role source"`
2. **rcpd generates ephemeral certificate** and outputs fingerprint to stdout
3. **Master reads fingerprint** before connecting (trusted via SSH channel)
4. **Master connects with TLS**, verifying rcpd's certificate fingerprint
5. **Master distributes fingerprints** to source and destination for mutual TLS

### Disabling Encryption

For performance on fully trusted networks, encryption can be disabled:

```bash
rcp --no-encryption source:/path dest:/path
```

**WARNING**: This exposes all data in plain text over the network. Only use on trusted, isolated networks.

For more details on the security model, see [security.md](security.md).

## rcpd Binary Discovery

When initiating a remote copy, rcp must locate the rcpd binary on remote hosts.

### Search Strategy

Discovery checks locations in this order:

1. **Explicit path** (`--rcpd-path` flag)
   - Highest priority, user override
   - Checked via SSH: `test -x /path/to/rcpd`

2. **Same directory as local rcp**
   - Most likely matching version (built/installed together)
   - Path derived from `std::env::current_exe()`

3. **PATH**
   - Uses `which rcpd` on remote host
   - Respects user's PATH configuration
   - Indicates intentional installation (e.g., `cargo install`)

4. **Deployed cache** (`~/.cache/rcp/bin/rcpd-{version}`)
   - Last resort for auto-deployed binaries
   - Only checked if not found elsewhere

### Error Handling

If rcpd is not found:

```
rcpd binary not found on remote host

Searched in:
- Same directory as local rcp binary
- PATH (via 'which rcpd')

Please install rcpd on the remote host and ensure it's in PATH:
- cargo install rcp-tools-rcp --version 0.22.0
Or specify the path explicitly:
- rcp --rcpd-path=/path/to/rcpd ...
```

### Graceful Degradation

If `HOME` is not set on the remote host:
- Cache directory check is skipped
- Discovery continues with same-directory and PATH checks
- Error message indicates cache was skipped

## Version Checking

rcp requires exact version match between local rcp and remote rcpd.

### Version Information

Each binary embeds version information at build time:

```rust
pub struct ProtocolVersion {
    pub semantic: String,              // e.g., "0.22.0"
    pub git_describe: Option<String>,  // e.g., "v0.21.1-7-g644da27"
    pub git_hash: Option<String>,      // Full commit hash
}
```

### Verification Process

1. **Execute version command** on remote:
   ```bash
   rcpd --protocol-version
   ```
   Returns JSON with version information.

2. **Compare versions**:
   - Policy: Exact semantic version match required
   - `rcp 0.22.0` requires `rcpd 0.22.0` exactly
   - Git info used for debugging only

3. **On mismatch**, return error with:
   - Local and remote versions
   - Remote hostname for context
   - Installation command with correct version

### Version Mismatch Error

```
rcpd version mismatch

Local:  rcp 0.22.0 (v0.21.1-7-g644da27)
Remote: rcpd 0.21.0 (v0.20.5-12-gf8a1b3c) on host 'prod-server'

The rcpd version on the remote host must exactly match the rcp version.

To fix this, install the matching version on the remote host:
- ssh prod-server 'cargo install rcp-tools-rcp --version 0.22.0'
```

### CLI Flags

```bash
# Human-readable version
rcp --version
# Output: rcp 0.22.0

# Machine-readable protocol version (JSON)
rcp --protocol-version
# Output: {"semantic":"0.22.0","git_describe":"v0.21.1-7-g644da27",...}
```

## Automatic Deployment

The `--auto-deploy-rcpd` flag enables automatic transfer and installation of rcpd to remote hosts.

### When to Use

- Version mismatch detected on remote host
- rcpd not installed on remote host
- Simplified deployment without manual installation

### Deployment Workflow

1. **Find local rcpd binary**:
   - Check same directory as rcp (ensures matching build)
   - Fall back to PATH via `which rcpd`

2. **Transfer binary to remote**:
   - Read local rcpd binary
   - Compute SHA-256 checksum
   - Base64 encode and transfer via SSH stdin
   - Atomic rename to final location

3. **Verify and set permissions**:
   - Verify SHA-256 checksum on remote
   - Set permissions to 700 (user-only execute)
   - Clean up old versions (keeps last 3)

### Transfer Mechanism

Binary transfer uses base64 encoding over SSH:

```bash
mkdir -p ~/.cache/rcp/bin && \
base64 -d > ~/.cache/rcp/bin/.rcpd-{version}.tmp.$$ && \
chmod 700 ~/.cache/rcp/bin/.rcpd-{version}.tmp.$$ && \
mv -f ~/.cache/rcp/bin/.rcpd-{version}.tmp.$$ ~/.cache/rcp/bin/rcpd-{version}
```

**Why base64**:
- Universal availability (POSIX standard)
- No external dependencies (no scp/rsync needed)
- Works with restricted shells

### Atomicity and Safety

**Unique temporary files**: Each deployment uses `.rcpd-{version}.tmp.$$` where `$$` is shell PID.

**Atomic rename**: `mv -f` is atomic on POSIX filesystems. Binary is either fully present or not present.

**Race condition handling**:
- Multiple concurrent deployments: Each uses unique temp file, final `mv` is atomic
- Interrupted deployment: Temp file left behind (harmless), final file unaffected
- Reading during deployment: Reader sees old or new inode, never corruption

### Caching

- **Location**: `~/.cache/rcp/bin/rcpd-{version}`
- **Reuse**: Deployed binary used for all subsequent operations
- **Cleanup**: Keeps last 3 versions, removes older automatically

### Error Messages

**Local binary not found**:
```
no local rcpd binary found for deployment

Searched in:
- Same directory: /path/to/rcp/../rcpd
- PATH: (via 'which rcpd')

To use auto-deployment, ensure rcpd is available:
- cargo install rcp-tools-rcp (installs to ~/.cargo/bin)
```

**Checksum mismatch**:
```
checksum mismatch after transfer

Expected: abc123...
Got:      def456...

The binary transfer may have been corrupted.
```

## Network Connectivity

### Connection Flow

1. **Master starts rcpd via SSH**
   - SSH to source host, start rcpd
   - SSH to destination host, start rcpd

2. **rcpd connects back to Master**
   - Source rcpd connects via TCP
   - Destination rcpd connects via TCP

3. **Source waits for Destination**
   - Source starts TCP listeners (control + data ports)
   - Source sends addresses to Master
   - Master forwards addresses to Destination
   - Destination connects to Source

4. **Data transfer**
   - Files sent over pooled data connections
   - Completion acknowledged via control channel

### Failure Scenarios

#### SSH Connection Failure

**Scenario**: SSH fails (host unreachable, auth failure)

**Handling**: SSH library returns error immediately, master displays error and exits.

#### rcpd Binary Not Found

**Scenario**: rcpd doesn't exist on remote host

**Handling**: SSH command fails, master waits for connection (timeout: 15s default).

**Error**:
```
Timed out waiting for source/destination rcpd to connect after 15s.
Check if source/destination host is reachable and rcpd can be executed.
```

#### rcpd Cannot Connect to Master

**Scenario**: TCP connection from rcpd to Master fails (firewall, network)

**rcpd error**:
```
Failed to connect to master at <addr>.
This usually means the master is unreachable from this host.
Check network connectivity and firewall rules.
```

#### Destination Cannot Connect to Source (Most Common)

**Scenario**: Destination cannot reach Source's TCP server

**Source error**:
```
Timed out waiting for destination to connect after 15s.
This usually means the destination cannot reach the source.
Check network connectivity and firewall rules.
```

**Destination error**:
```
Failed to connect to source at <addr>.
This usually means the source is unreachable from the destination.
Check network connectivity and firewall rules.
```

### Timeout Configuration

| Timeout | Default | Configuration |
|---------|---------|---------------|
| SSH connection | ~30s | SSH config |
| rcpd → Master | 15s | `--remote-copy-conn-timeout-sec` |
| Destination → Source | 15s | `--remote-copy-conn-timeout-sec` |

Example:
```bash
rcp --remote-copy-conn-timeout-sec 30 source:/path dest:/path
```

### Port Configuration

Use `--port-ranges` to restrict TCP ports:

```bash
rcp --port-ranges 8000-8100 source:/path dest:/path
```

Useful when:
- Firewalls only allow specific port ranges
- Avoiding conflicts with other services
- Security policies require specific ports

### Troubleshooting Checklist

1. **Check firewall rules**: Ensure TCP ports are open
2. **Check routing**: `ping`, `traceroute` between hosts
3. **Check rcpd binary**: Exists and is executable on remote hosts
4. **Check NAT**: Ensure proper port forwarding if applicable
5. **Use verbose logging**: Run with `-vv` for detailed connection info

## Static Binary Distribution

rcp builds static musl binaries by default for maximum portability.

### Configuration

From `.cargo/config.toml`:

```toml
[build]
target = "x86_64-unknown-linux-musl"
rustflags = ["--cfg", "tokio_unstable"]

[target.x86_64-unknown-linux-musl]
linker = "x86_64-unknown-linux-musl-gcc"
rustflags = ["--cfg", "tokio_unstable", "-C", "target-feature=+crt-static"]
```

### Benefits

- No dynamic library dependencies (except kernel)
- Works on all Linux distributions (glibc, musl, Alpine)
- No libc version conflicts
- Single binary deployable anywhere
- Verified with `ldd` showing "not a dynamic executable"

### Building

```bash
# Default (musl static binary)
cargo build

# Glibc (if needed)
cargo build --target x86_64-unknown-linux-gnu
```

## CLI Reference

### rcp Flags for Remote Operations

| Flag | Description |
|------|-------------|
| `--rcpd-path=PATH` | Override rcpd binary path on remote hosts |
| `--auto-deploy-rcpd` | Automatically deploy rcpd to remote hosts |
| `--remote-copy-conn-timeout-sec=N` | Connection timeout (default: 15) |
| `--port-ranges=RANGES` | Restrict TCP to specific ports (e.g., "8000-8999") |
| `--max-connections=N` | Maximum concurrent data connections (default: 100) |
| `--network-profile=PROFILE` | Buffer sizing: `datacenter` (default) or `internet` |

### Network Profiles

**Datacenter (default)**:
- Larger TCP buffers (16 MiB)
- Optimized for low-latency, high-bandwidth networks

**Internet**:
- Smaller TCP buffers (2 MiB)
- Better for higher-latency networks

## Security Considerations

### Shell Injection Prevention

- All paths passed to shell commands are escaped using `shell_escape()`
- Version checking uses direct binary execution (no shell)
- Hostnames are shell-escaped in suggested commands

### Trust Model

- SSH is the security perimeter for initial authentication
- All operations require SSH authentication first
- Data transfers are encrypted with TLS 1.3 by default (certificate fingerprint pinning)
- Use `--no-encryption` only on trusted networks where encryption overhead is undesirable

For comprehensive security analysis, see **[security.md](security.md)**.

## Design Rationale

### Exact Version Matching

**Decision**: Require exact semantic version match

**Rationale**:
- Strictest policy during active development
- Protocol changes are frequent
- Prevents subtle bugs from version skew
- Future: May relax to minor version tolerance after v1.0

### Multi-tier Discovery

**Decision**: Explicit path → same dir → PATH → cache

**Rationale**:
- Respects explicit user configuration (highest priority)
- Same directory likely has matching version
- PATH follows Unix conventions
- Cache is last resort for auto-deployed binaries

### Musl as Default Target

**Decision**: Build static musl binaries by default

**Rationale**:
- Eliminates "works on my machine" issues
- Single binary works everywhere
- Critical for deployment simplicity
- Small size increase (10-30%) acceptable

## References

- **Protocol specification**: [remote_protocol.md](remote_protocol.md)
- **Security model**: [security.md](security.md)
- **Implementation**: `remote/src/lib.rs`, `remote/src/deploy.rs`
- **Version module**: `common/src/version.rs`
