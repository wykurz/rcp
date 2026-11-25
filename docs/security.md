# Security Model

This document describes the security architecture of the rcp (remote copy) tool.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [TLS Certificate Authentication](#tls-certificate-authentication)
- [Trust Model](#trust-model)
- [Threat Model](#threat-model)
- [Disabling Encryption](#disabling-encryption)
- [Security Best Practices](#security-best-practices)
- [Reporting Security Issues](#reporting-security-issues)

## Overview

The rcp remote copy system provides:

1. **SSH** for authentication, authorization, rcpd deployment, and all Master↔rcpd control and
   tracing traffic
2. **TLS with certificate pinning** for encrypted and authenticated Source↔Destination data transfer

Master↔rcpd control and tracing run inside the authenticated SSH exec channel (`stdin`/`stdout`), so
daemons never open a master-facing TCP listener. Source↔Destination TCP connections are encrypted
and mutually authenticated by default using TLS 1.3 with self-signed certificates and fingerprint
pinning. Source and destination each generate an ephemeral certificate, report its fingerprint to
the master over SSH `stderr`, and receive their peer's fingerprint from the master over SSH `stdin`.

## Architecture

### Three-Node Remote Copy

A remote copy operation involves three participants:

```text
                ┌─────────────┐
                │   Master    │  (Local machine running `rcp`)
                │    (rcp)    │
                └─────────────┘
                  │         │
        ┌─────────┘         └─────────┐
    SSH │                             │ SSH
(spawn, │                             │ (spawn,
control,│                             │ control,
tracing)│                             │ tracing)
        v                             v
   ┌─────────────┐             ┌─────────────┐
   │   Source    │             │ Destination │
   │   (rcpd)    │─── TLS ────>│   (rcpd)    │
   └─────────────┘             └─────────────┘
                 (control + data)
```

### Communication Channels

| Channel                                    | Protocol | Authentication             | Encryption |
| ------------------------------------------ | -------- | -------------------------- | ---------- |
| Master ↔ Source (spawn, control, tracing)  | SSH      | SSH keys/password          | SSH        |
| Master ↔ Destination (spawn, control, ...) | SSH      | SSH keys/password          | SSH        |
| Source ↔ Destination (control)             | TLS      | Mutual fingerprint pinning | TLS 1.3    |
| Source → Destination (data)                | TLS      | Mutual fingerprint pinning | TLS 1.3    |

## TLS Certificate Authentication

### How It Works

Source and destination each generate an ephemeral self-signed certificate for their direct TCP
connections (the master communicates with both daemons over SSH and needs no TLS certificate). Each
daemon reports its certificate fingerprint in `RCP_READY` on SSH `stderr`, and the master
distributes the peer fingerprints over SSH `stdin`. For Source↔Destination connections, the source
is the TLS server and the destination connects as a client.

**Authentication Flow:**

```
PHASE 1: rcpd Spawn and Readiness (via SSH)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Master spawns rcpd processes via SSH:
  ssh host-a "rcpd --role=source ..."
  ssh host-b "rcpd --role=destination ..."

Each rcpd:
  1. Generates ephemeral self-signed certificate
  2. Computes fingerprint: SHA256(cert.to_der())
  3. Outputs to stderr: "RCP_READY <hex_fingerprint> <F> <E>"
  4. Switches SSH stdin/stdout to framed binary control + tracing

PHASE 2: Fingerprint Exchange (via SSH)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Master reads from SSH stderr for each rcpd:
  - Parses "RCP_READY <fingerprint> <F> <E>"
  - Fingerprint received via trusted SSH channel!

`F` is the negotiated logical file-work ceiling and `E` is the effective data-stream ceiling. They
carry performance policy, not secret material; the TLS fingerprint remains the value authenticated
through the SSH channel.

Master sends each peer's fingerprint over SSH stdin inside MasterHello:
  - MasterHello::Source carries destination's cert fingerprint
  - MasterHello::Destination carries source's cert fingerprint and TCP addresses

PHASE 3: Source ↔ Destination Connection
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source creates TLS server, destination connects with client cert:
  - Destination verifies source's cert fingerprint
  - Source verifies destination's client cert fingerprint
  - Mutual authentication achieved ✓
```

### Security Properties

| Property            | Guarantee                                                |
| ------------------- | -------------------------------------------------------- |
| **Confidentiality** | SSH for Master↔rcpd; AES-256-GCM or ChaCha20-Poly1305    |
| **Integrity**       | SSH MAC/AEAD and TLS AEAD                                |
| **Authentication**  | SSH plus mutual TLS certificate fingerprint verification |
| **Forward Secrecy** | Ephemeral key exchange on SSH and TLS 1.3                |

### Why Certificate Pinning?

We use self-signed certificates with fingerprint pinning because:

1. **Ephemeral**: Fresh certificate generated for each transfer session
2. **No PKI required**: No certificate authorities or trust chains
3. **Trusted channel available**: SSH provides secure fingerprint distribution
4. **Mutual authentication**: Both parties verify each other's fingerprints
5. **Standard TLS**: Well-tested, excellent library support

### Fingerprint Distribution Security

Certificate fingerprints are distributed via trusted SSH channels:

- **SSH stderr**: each rcpd outputs its fingerprint in `RCP_READY`; master reads it via SSH
- **SSH stdin**: master sends each peer's fingerprint to source and destination inside `MasterHello`
- **Per-session**: New certificates and fingerprints for each rcp invocation
- **Memory-only**: Private keys are never written to disk

Because both `RCP_READY` and `MasterHello` travel inside the SSH exec channels, neither daemon
accepts any master-facing TCP connections and both peers learn each other's fingerprint before
opening any direct TCP connection.

## Trust Model

### Trust Chain

```text
User
  │
  ├─── trusts ──→ Local machine (master)
  │
  ├─── trusts ──→ SSH infrastructure
  │                   │
  │                   ├─ supplies peer identity
  │                   └─ distributes cert fingerprints
  │
  └─── trusts ──→ TLS certificate pinning
                      │
                      ├─ authenticates all connections
                      └─ encrypts all data transfer
```

### Trusted Components

1. **Local machine**: The machine running `rcp` is fully trusted
2. **SSH infrastructure**: SSH keys, known_hosts, and SSH daemon security
3. **Remote hosts**: Hosts accessible via SSH are trusted to run rcpd

### SSH Host Verification Limitation

The current SSH launcher in [`remote/src/lib.rs`](../remote/src/lib.rs) explicitly passes
`StrictHostKeyChecking=no`. It does not enforce strict known-host verification, even if the user's
SSH configuration requests it. TLS pinning checks the fingerprint supplied through that SSH session;
it cannot independently establish that the bootstrap peer is the intended host. An attacker who can
impersonate the SSH bootstrap peer can therefore supply the fingerprints that subsequent TLS
connections trust. Do not treat TLS pinning as protection against that bootstrap attack.

### Local Deployment Candidates

When `--auto-deploy-rcpd` needs a local binary, rcp runs `--protocol-version` on candidates beside
the running `rcp` binary and then on the user's `PATH` to verify protocol compatibility before
deployment. Those locations are therefore trusted local code sources: a candidate is executed with
the invoking user's privileges. The two-second probe timeout is a liveness bound for a stalled
candidate, not a sandbox or a mitigation for candidate side effects. Environments with stricter
local execution controls should use a trusted installation location or deploy rcpd manually.

### What We Protect Against

With TLS enabled and fingerprints obtained from the intended SSH peers:

- **Network eavesdropping**: All data encrypted
- **Man-in-the-middle**: Certificate fingerprint verification prevents unauthorized parties
- **Connection hijacking**: TLS session cannot be taken over
- **Replay attacks**: TLS prevents replay of captured traffic

### What We Don't Protect Against

- **Compromised SSH keys**: If attacker has your SSH keys, they can run rcpd
- **Compromised local machine**: Master machine is fully trusted
- **Compromised remote hosts**: If host is compromised, attacker controls rcpd
- **Side-channel attacks**: No specific mitigations for timing attacks, etc.
- **TOCTTOU attacks with `--dereference`/`-L`**: Following symlinks is requested behavior and cannot
  be hardened. See [TOCTTOU Vulnerabilities](tocttou.md) for details.
- **Unsupported non-Linux builds**: RCP supports Linux only. The sources rely on Linux-specific
  filesystem APIs, so non-Linux builds receive no compatibility or security guarantees. See
  [TOCTTOU Vulnerabilities](tocttou.md).
- **Trust of the operand path's prefix**: The hardening protects everything at or below the named
  root, but by default the tools do not verify that the directories *above* it are free of
  less-privileged control. `--require-toctou-safe` enforces the hardened walk (refusing `-L` and
  unsupported non-Linux builds) and the strict operand contract: operands must be absolute and
  lexically normal, and every operand open resolves `RESOLVE_NO_SYMLINKS`, so a symlink spliced
  anywhere along the path fails closed. Path *policy* — and keeping prefix directories non-writable
  by lesser-privileged actors — remains the caller's responsibility; see the
  [Scope of TOCTOU safety](tocttou.md#scope-of-toctou-safety) section.
- **POSIX ACLs, unless requested**: a copy that does not ask for `acl` reproduces the source's mode
  and drops its ACL, and a source ACL entry narrower than `other` acts as a deny in effect — so
  dropping it grants what the source withheld. `--require-toctou-safe` does not close this; it
  contains the *destination* tree's inherited ACLs, which is a different bug. Pair it with
  `--preserve-settings=all+acl` where source ACLs are security-relevant. See [POSIX ACLs](acls.md).

On Linux, the default (non-`-L`) local hardening is implemented through a single shared safe-walk
driver (`common/src/walk_driver.rs`): `rcp` (copy), `rchm`, and `rrm` are `WalkVisitor`
implementations, so the recursive walk — and in particular the leaf-permit "drop before recursion"
deadlock invariant — lives in one place rather than being hand-maintained per tool. The
trusted/hardened boundary is type-enforced via `TrustedDir`, and `DT_UNKNOWN` filter classification
goes through the single `filter_is_dir` path. `rlink` remains dual-tree (source plus `--update`) but
shares the same substrate. See [TOCTTOU Vulnerabilities](tocttou.md) for the full mechanism.

## Threat Model

### Threats Mitigated

These TLS guarantees assume trustworthy fingerprint distribution; the
[SSH host-verification limitation](#ssh-host-verification-limitation) still applies.

| Threat                    | Mitigation                                           |
| ------------------------- | ---------------------------------------------------- |
| **Passive eavesdropping** | TLS encryption prevents reading data                 |
| **Active MITM**           | Fingerprint verification fails for wrong certificate |
| **Rogue rcpd connection** | TLS handshake fails - wrong/missing fingerprint      |
| **Connection racing**     | Attacker cannot present correct certificate          |
| **Data tampering**        | AEAD detects any modification                        |
| **Session hijacking**     | TLS session keys are connection-specific             |

### Race Condition Prevention

A potential attack vector is an attacker racing to connect to a freshly started source rcpd listener
before the legitimate destination does (master↔rcpd traffic runs over SSH stdio and exposes no TCP
listener at all). Certificate fingerprint verification prevents an attacker from *authenticating* —
it cannot establish a trusted session or impersonate the destination:

1. Attacker observes source rcpd's TCP listeners and probes its control or data port
2. Attacker attempts a TLS handshake before destination connects
3. Source rcpd requires a client certificate and verifies its fingerprint against the destination
   fingerprint delivered by master via SSH stdin
4. **Attacker's certificate has wrong fingerprint** - handshake rejected, no authenticated session
   is established (confidentiality and integrity are preserved)

**Availability caveat**: this protects authenticity, not availability. Every TLS handshake is
bounded by `--remote-copy-conn-timeout-sec`, so a peer that establishes TCP and then stalls can only
occupy a sequential accept loop until that deadline. A failed source-control handshake is still
fatal to the source daemon, however, so an attacker who can reach the source's listening port can
still deny service by initiating a bad or deliberately stalled handshake:

- **Source↔destination listeners (source rcpd):** the control listener propagates a failed handshake
  as fatal; the data listener logs and continues on failure, but a peer that stalls mid-handshake
  delays its sequential accept loop until the configured deadline.

Reaching these ports requires network access to the (typically trusted) source host; restrict it
with `--port-ranges` plus firewall rules, or use `--remote-transport=ssh` (which binds source to
`127.0.0.1` only and tunnels traffic over SSH while preserving mutual TLS 1.3 by default).
Tolerating failed source-control handshakes without aborting the daemon remains a future hardening.

### Cipher Suites

With TLS 1.3, the following cipher suites are used:

- `TLS_AES_256_GCM_SHA384` (preferred, hardware accelerated)
- `TLS_AES_128_GCM_SHA256` (fallback)
- `TLS_CHACHA20_POLY1305_SHA256` (software-only systems)

These are the TLS 1.3 suites of the rustls `ring` provider; rcp does not override cipher-suite
selection or ordering. TLS 1.3 is **pinned** — every connection is configured for TLS 1.3 only
(`builder_with_protocol_versions`), so TLS 1.2 is never negotiated. This is safe to do
unconditionally: TLS 1.3 has been available at both ends since the TLS layer was introduced (rustls
has always offered it), so pinning never excludes a legitimate rcp/rcpd peer. A peer that cannot
negotiate 1.3 simply fails the handshake — the correct outcome — rather than silently downgrading.
This defense remains independent of the compatibility checks at remote discovery and deployment
boundaries; see [Remote Copy](remote_copy.md).

## Disabling Encryption

For trusted networks where encryption overhead is undesirable, use `--no-encryption`:

```bash
rcp --no-encryption source:/path dest:/path
```

**Important**: `--no-encryption` disables **both encryption AND authentication** on
Source↔Destination TCP connections (control and data). Master↔rcpd control and tracing continue to
run inside the SSH exec channel. With this flag:

- All Source↔Destination traffic (control messages and file data) is transmitted in plaintext
- The source TCP listeners accept connections from anyone who can reach their ports (`rcpd` emits
  `RCP_READY - <F> <E>` with no fingerprint)
- SSH protects Master↔rcpd communication and spawning, not the direct Source↔Destination TCP
  connections

The security model with `--no-encryption` relies entirely on network isolation.

**When to use `--no-encryption`:**

- Isolated datacenter networks with no untrusted traffic
- Performance-critical transfers on physically secured networks
- Testing and debugging

**When NOT to use `--no-encryption`:**

- Any network with potential eavesdroppers or MITM attackers
- Cross-datacenter transfers over public internet
- Transfers containing sensitive data
- Networks where other users could connect to ephemeral ports

## Security Best Practices

### For System Administrators

1. **SSH Hardening**
   - Use key-based authentication (disable password auth)
   - Implement rate limiting (fail2ban, sshguard)
   - Keep SSH server updated
   - Use strong key types (Ed25519, RSA 4096+)
   - Configure proper file permissions on SSH keys

2. **Network Security**
   - Use firewall rules to restrict TCP ports if needed (via `--port-ranges`), or tunnel traffic
     over SSH with `--remote-transport=ssh` / `--remote-transport=auto` when only port 22 is open
   - Only pass `--ssh-forward-agent` to trusted destination hosts when 3-host SSH copies require
     agent forwarding (the flag applies solely to the destination endpoint and is off by default)
   - Monitor for unusual connection patterns
   - Log SSH and rcp operations for audit trails
   - Use network segmentation for sensitive operations

3. **System Hardening**
   - Keep rcp binaries updated
   - Verify binary integrity (checksums, signatures)
   - Run with least privilege necessary
   - Monitor system logs for security events

4. **Access Control**
   - Limit which users can run rcp
   - Use sudo policies to restrict remote copy operations
   - Implement file permission policies
   - Regular access audits

5. **Binary Deployment**
   - **Auto-deployment**: Use `--auto-deploy-rcpd` to automatically deploy rcpd binaries
     - Binaries are transferred over SSH (already authenticated)
     - SHA-256 checksums verify integrity after transfer
     - Binaries are cached at `~/.cache/rcp/bin/rcpd-{version}` on remote hosts
   - **Manual deployment**: Preferred for air-gapped environments or strict change control

### For Users

1. **Secure Workflows**
   - Use SSH agent forwarding carefully (or avoid it)
   - Keep SSH keys protected (passphrase, key agent timeout)
   - Don't use `--no-encryption` unless you understand the implications

2. **Error Handling**
   - Don't ignore connection failures or TLS errors
   - Report suspicious connection behavior
   - TLS handshake failures may indicate an attack attempt

## Reporting Security Issues

If you discover a security vulnerability in rcp, please:

1. **Do NOT open a public GitHub issue**
2. Email the maintainer directly (see repository for contact)
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

We will respond within 48 hours and work with you on a coordinated disclosure.

## Summary

The rcp security model provides:

- ✅ **Authentication**: SSH + TLS certificate pinning (mutual authentication)
- ✅ **Authorization**: SSH/filesystem permissions control access
- ✅ **Encryption**: TLS 1.3 with AES-256-GCM (enabled by default)
- ✅ **Integrity**: AEAD ensures data cannot be tampered with
- ✅ **Forward Secrecy**: TLS 1.3 ephemeral key exchange
- ✅ **Process isolation**: rcpd processes are properly lifecycle-managed

**Limitations**:

- ⚠️ **TOCTTOU with `--dereference`/`-L`**: Following symlinks is requested behavior and is not
  hardened. On supported Linux builds, all other default paths (local copy/link/chmod/rm/delete,
  remote copy source+destination) are fully TOCTOU-hardened. Use `--require-toctou-safe` in sudo
  rules to enforce the hardened walk (it refuses `-L` and unsupported non-Linux targets) plus the
  strict operand contract (absolute, lexically normal operands, resolved `RESOLVE_NO_SYMLINKS`;
  needs Linux 5.6+). Path policy is still the caller's responsibility — see the
  [Scope of TOCTOU safety](tocttou.md#scope-of-toctou-safety) section of
  [TOCTTOU Vulnerabilities](tocttou.md) for details.

Use `--no-encryption` only on trusted networks where performance is critical.
