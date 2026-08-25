# Remote Copy Operations

This document covers the operational aspects of rcp's remote copy functionality: binary discovery,
version checking, automatic deployment, and network connectivity.

For the detailed protocol specification (message types, flows, invariants), see
**[remote_protocol.md](remote_protocol.md)**.

## Overview

The rcp remote copy system uses a three-node architecture:

![Three-node remote copy architecture](assets/remote_architecture.svg)

**Key components**:

- **Master (rcp)**: Coordinates the operation, runs on user's machine
- **Source (rcpd)**: Reads and sends files
- **Destination (rcpd)**: Receives and writes files

The master connects to remote hosts via SSH, spawns rcpd processes, and coordinates the transfer.
All TCP connections (Master↔rcpd and Source↔Destination) are encrypted with TLS by default.

## TLS Encryption

By default, all TCP connections are encrypted using TLS 1.3 with self-signed certificates and
fingerprint pinning.

### Security Properties

| Connection           | Authentication                                 | Encryption |
| -------------------- | ---------------------------------------------- | ---------- |
| Master → rcpd        | Mutual fingerprint pinning (exchanged via SSH) | TLS 1.3    |
| Source ↔ Destination | Mutual fingerprint pinning (via Master)        | TLS 1.3    |

**Key features**:

- **Forward secrecy**: Ephemeral keys per session
- **Mutual authentication**: Both parties verify each other's certificates
- **No trust anchor**: Self-signed certificates with fingerprint pinning (no CA)
- **Transparent**: No configuration needed (enabled by default)

### Connection Flow

1. **Master prepares compatible rcpd endpoints**, sharing one SSH preparation for a same-host copy
2. **Master spawns source rcpd via SSH**: `ssh host "rcpd --role source --master-cert-fp <fp>"`
3. **Source generates an ephemeral certificate** and reports its fingerprint plus negotiated file
   and stream limits in the first stderr readiness record
4. **Master connects to the source before spawning the destination**, then starts the destination
   with the source-negotiated limits and verifies its readiness record matches
5. **Master connects with TLS**, verifying each rcpd's certificate fingerprint; rcpd in turn
   verifies master's client certificate against `--master-cert-fp`
6. **Master distributes fingerprints** to source and destination for mutual TLS

### Disabling Encryption

For performance on fully trusted networks, encryption can be disabled:

```bash
rcp --no-encryption source:/path dest:/path
```

**WARNING**: This exposes all data in plain text over the network. Only use on trusted, isolated
networks.

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
- Deployed cache: ~/.cache/rcp/bin/rcpd-0.36.0

Options:
- Use automatic deployment: rcp --auto-deploy-rcpd ...
- Install rcpd manually: cargo install rcp-tools-rcp --version 0.36.0
- Specify explicit path: rcp --rcpd-path=/path/to/rcpd ...
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
   Returns JSON with version information. The probe has a two-second deadline; timeout errors retain
   the affected host in their error chain, and auto-deployment may recover by publishing a
   compatible local candidate.

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
# Output: rcp 0.36.0

# Machine-readable protocol version (JSON)
rcp --protocol-version
# Output: {"semantic":"0.36.0","git_describe":"v0.35.0-7-g644da27",...}
```

## Automatic Deployment

The `--auto-deploy-rcpd` flag enables automatic transfer and installation of rcpd to remote hosts.

### When to Use

- Version mismatch detected on remote host
- rcpd not installed on remote host
- Simplified deployment without manual installation

### Deployment Workflow

1. **Find local rcpd binary**:
   - Check same directory as rcp with a bounded `--protocol-version` probe
   - Fall back to later candidates such as PATH when an earlier candidate is missing, unusable, or
     incompatible; a timed-out probe has its pipes closed, receives a kill request, and gets a
     one-second reap grace before an owned background reaper permits fallback

2. **Transfer binary to a temp file**:
   - Read local rcpd binary
   - Compute SHA-256 checksum
   - Base64 encode and transfer via SSH stdin
   - Set permissions to 700 (user-only execute)

3. **Verify, then publish**:
   - Verify SHA-256 checksum **of the temp file** (a mismatch removes it, publishing nothing)
   - Atomic rename to the final location
   - Re-run a bounded `--protocol-version` probe on the published remote path before spawning it
   - Clean up old versions (keeps last 3)

### Transfer Mechanism

Binary transfer uses base64 encoding over SSH, where `{unique}` is `{pid}-{random}`, generated by
the deploying process:

```bash
# transfer
mkdir -p ~/.cache/rcp/bin && \
base64 -d > ~/.cache/rcp/bin/.rcpd-{version}.tmp.{unique} && \
chmod 700 ~/.cache/rcp/bin/.rcpd-{version}.tmp.{unique}
# verify (sha256sum on the temp file), then publish
mv -f ~/.cache/rcp/bin/.rcpd-{version}.tmp.{unique} ~/.cache/rcp/bin/rcpd-{version}
```

**Why base64**:

- Universal availability (POSIX standard)
- No external dependencies (no scp/rsync needed)
- Works with restricted shells

### Atomicity and Safety

**Unique temporary files**: Each deployment writes to `.rcpd-{version}.tmp.{pid}-{random}`. The
unique part is generated locally and **must not** be a shell expansion such as `$$`: every path sent
to the remote shell is single-quoted, so `$$` would be taken literally and every deployment would
collide on one file.

**Verify before publish**: the checksum is taken on the temp file, so a corrupt or truncated
transfer is never reachable under the name other processes execute.

**Atomic rename**: `mv -f` is atomic on POSIX filesystems. Binary is either fully present or not
present.

**Race condition handling**:

- Multiple concurrent deployments: Each uses unique temp file, final `mv` is atomic
- Interrupted deployment: Temp file left behind (harmless), final file unaffected
- Reading during deployment: Reader sees old or new inode, never corruption

### Caching

- **Location**: `~/.cache/rcp/bin/rcpd-{version}`
- **Reuse**: Deployed binary used for all subsequent operations
- **Cleanup**: Keeps last 3 versions, removes older automatically

### Error Messages

**Compatible local binary not found**:

```
no compatible local rcpd binary found for deployment

Searched in:
- Same directory: /path/to/rcp/rcpd

To use auto-deployment, ensure rcpd is available:
- cargo install rcp-tools-rcp (installs to ~/.cargo/bin)
- or add rcpd to PATH
- or build with: cargo build --release --bin rcpd
```

(A `PATH:` line is added when `which rcpd` resolves a candidate. An incompatible candidate remains
in the list with its rejection reason; a compatible one is deployed. When `which` finds nothing, no
`PATH:` line is shown — hence the common not-found output lists only the same-directory candidate.)

**Checksum mismatch**:

```
checksum mismatch after transfer

Expected: abc123...
Got:      def456...

The binary transfer may have been corrupted.
Please try again or check network connectivity.
```

## Network Connectivity

### Connection Flow

1. **Master prepares rcpd via SSH**
   - Source and destination preparation may run concurrently on distinct hosts
   - Equal SSH endpoint settings share one preparation, discovery, and optional deployment

2. **Master starts and connects to source rcpd**
   - Source creates a TCP listener and emits `RCP_TLS <addr> <fingerprint> <F> <E>` (or plaintext
     `RCP_TCP <addr> <F> <E>`) as its first stderr record
   - Master immediately opens the source control and tracing connections

3. **Master starts and connects to destination rcpd**
   - Automatic policy uses the source-selected `F`; explicit policy retains master authority
   - Destination readiness must report the same `F` and `E`, after which the master opens its
     control and tracing connections

4. **Source waits for Destination**
   - Source starts TCP listeners (control + data ports)
   - Source sends addresses to Master
   - Master forwards addresses to Destination
   - Destination connects to Source

5. **Data transfer**
   - Files sent over pooled data connections
   - Completion acknowledged via control channel

### Failure Scenarios

#### SSH Connection Failure

**Scenario**: SSH fails (host unreachable, auth failure)

**Handling**: SSH library returns error immediately, master displays error and exits.

#### rcpd Binary Not Found

**Scenario**: rcpd doesn't exist on remote host

**Handling**: Binary discovery and a version check run *before* rcpd is spawned — no connection
timeout is involved. Two distinct outcomes:

- **No binary found** (and `--auto-deploy-rcpd` is not set): the master fails immediately with the
  `rcpd binary not found on remote host` error shown in [Error Handling](#error-handling).
- **A binary is found but its version doesn't match** `rcp`, and `--auto-deploy-rcpd` is **not**
  set: the master fails with the version-mismatch error shown in
  [Version Mismatch Error](#version-mismatch-error), not the "not found" error. (With
  `--auto-deploy-rcpd`, a mismatch instead triggers deployment: the master ships its *local* rcpd
  binary — the first compatible candidate beside `rcp` or on PATH — and stores it under the local
  `rcp`'s compatibility tag. Each local candidate's own `--protocol-version` is checked before
  deployment, with stale candidates skipped in favor of later fallbacks. The published remote path
  is checked again before either rcpd role is spawned.)

#### Master Cannot Connect to rcpd

**Scenario**: TCP connection from Master to rcpd's listener fails (firewall, network)

The master opens two connections — control then tracing — to *each* rcpd. In the master's error the
`<purpose>` is therefore one of `source control`, `source tracing`, `dest control`, or
`dest tracing`; in the rcpd's own error it is the bare `control` or `tracing`.

**Master error**:

```
failed to connect to rcpd at <addr> for <purpose>
```

or, on timeout:

```
timeout connecting to rcpd for <purpose>
```

**rcpd error** (while waiting for the master to connect):

```
timeout waiting for master <purpose> connection
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
connection to <addr> timed out after 15s
```

or:

```
failed to connect to <addr>
```

### Timeout Configuration

| Timeout              | Default                             | Configuration                    |
| -------------------- | ----------------------------------- | -------------------------------- |
| SSH connection       | ~30s                                | SSH config                       |
| Master → rcpd        | 15s (60s with `--auto-deploy-rcpd`) | `--remote-copy-conn-timeout-sec` |
| Destination → Source | 15s (60s with `--auto-deploy-rcpd`) | `--remote-copy-conn-timeout-sec` |
| Dead-peer detection  | 120s (0 disables)                   | `--remote-keepalive-sec`         |

Dead-peer detection covers idle connections everywhere, and unacknowledged data on control
connections only — a data transfer to a vanished host still falls back to the kernel's
retransmission limit (~15 min). See [remote_protocol.md](remote_protocol.md#84-connection-liveness).

Example:

```bash
rcp --remote-copy-conn-timeout-sec 30 source:/path dest:/path
```

### Port Configuration

Use `--port-ranges` to restrict TCP ports:

```bash
rcp --port-ranges 8000-8999 source:/path dest:/path
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

From `.cargo/config.toml` (abridged; the file also sets `rustdocflags` per section and configures
the `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-musl` targets):

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

| Flag                               | Description                                                    |
| ---------------------------------- | -------------------------------------------------------------- |
| `--rcpd-path=PATH`                 | Override rcpd binary path on remote hosts                      |
| `--auto-deploy-rcpd`               | Automatically deploy rcpd to remote hosts                      |
| `--remote-copy-conn-timeout-sec=N` | Connection timeout (default: 15; 60 with `--auto-deploy-rcpd`) |
| `--remote-keepalive-sec=N`         | Dead-peer detection budget, 0 disables (default: 120)          |
| `--port-ranges=RANGES`             | Restrict TCP to specific ports (e.g., "8000-8999")             |
| `--max-files-in-flight=N`          | Explicit ceiling; automatic remote default is source-owned     |
| `--max-connections=N`              | Maximum concurrent data connections (default: 100)             |
| `--network-profile=PROFILE`        | Buffer sizing: `datacenter` (default) or `internet`            |

For a remote copy, let `F` be the logical file-work ceiling and `M` be `--max-connections`; the
effective stream count is `E = min(F, M)`, or `E = M` for the legacy explicit-unlimited policy. The
source owns automatic `F` selection and the destination adopts its reported `F/E`; explicit `F`
remains master-authoritative. Pending capacity is `E × --pending-writes-multiplier`. Values for `E`
or that product above Tokio's semaphore maximum are rejected rather than reaching semaphore
construction.

Explicit limits can be checked before remote `~` expansion. Automatic limits cannot be known there:
the source resolves and validates them before its readiness record and before destination spawn.
Each endpoint separately applies its local soft-RLIMIT descriptor safety when admitting file-like
work. Explicit connection or descriptor clamps produce a default-visible notice naming requested and
effective values; automatic clamps are verbose-only. Profiling and Tokio-console artifact
announcements likewise use the tracing notice target and reach master output only after the daemon
readiness handshake. These readiness and internal spawn-contract changes are wire revision 4.

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
