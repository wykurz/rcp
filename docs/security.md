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

1. **SSH** for initial authentication, authorization, and rcpd deployment
2. **TLS with certificate pinning** for encrypted and authenticated data transfer

All TCP connections are encrypted and mutually authenticated by default using TLS 1.3 with
self-signed certificates and fingerprint pinning. Each party generates an ephemeral certificate, and
fingerprints are distributed via SSH (for master↔rcpd) or protocol messages (for source↔dest).

## Architecture

### Three-Node Remote Copy

A remote copy operation involves three participants:

```text
                 ┌─────────────┐
                 │   Master    │  (Local machine running `rcp`)
                 │    (rcp)    │
                 └─────────────┘
                  │ │       │ │
        ┌─────────┘ │       │ └─────────┐
        │   ┌───────┘       └───────┐   │
    SSH │   │ TLS               TLS │   │ SSH
(spawn) │   │ (control)   (control) │   │ (spawn)
        v   v                       v   v
   ┌─────────────┐             ┌─────────────┐
   │   Source    │             │ Destination │
   │   (rcpd)    │─── TLS ────>│   (rcpd)    │
   └─────────────┘             └─────────────┘
                 (control + data)
```

Each master↔rcpd TLS arrow above stands for two connections — control plus a tracing/progress
channel — listed separately in the table below.

### Communication Channels

| Channel                                  | Protocol | Authentication             | Encryption |
| ---------------------------------------- | -------- | -------------------------- | ---------- |
| Master → Source (spawn)                  | SSH      | SSH keys/password          | SSH        |
| Master → Destination (spawn)             | SSH      | SSH keys/password          | SSH        |
| Master ↔ Source (control + tracing)      | TLS      | Mutual fingerprint pinning | TLS 1.3    |
| Master ↔ Destination (control + tracing) | TLS      | Mutual fingerprint pinning | TLS 1.3    |
| Source ↔ Destination (control)           | TLS      | Mutual fingerprint pinning | TLS 1.3    |
| Source → Destination (data)              | TLS      | Mutual fingerprint pinning | TLS 1.3    |

## TLS Certificate Authentication

### How It Works

Each party generates an ephemeral self-signed certificate. The key design principle is that **for
master connections, rcpd is always the TLS server** (master connects to it as a TLS client) - this
allows master to read the certificate fingerprint from rcpd's stderr, carried over the trusted SSH
channel, before connecting. (For source↔destination connections, the source is the TLS server and
the destination connects as a client.)

**Authentication Flow:**

```
PHASE 1: rcpd Spawn and Setup (via SSH)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Master generates its own ephemeral certificate, then spawns rcpd processes via SSH:
  ssh host-a "rcpd --role=source --master-cert-fp=<master_fingerprint>"
  ssh host-b "rcpd --role=destination --master-cert-fp=<master_fingerprint>"

Each rcpd:
  1. Generates ephemeral self-signed certificate
  2. Computes fingerprint: SHA256(cert.to_der())
  3. Creates TLS server listener (requires a client cert matching --master-cert-fp)
  4. Outputs to stderr: "RCP_TLS <addr> <hex_fingerprint>"

PHASE 2: Master → rcpd Connection
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Master reads from SSH stderr for each rcpd:
  - Parses "RCP_TLS <addr> <fingerprint>"
  - Fingerprint received via trusted SSH channel!

Master connects TO rcpd as TLS client (control connection, then tracing connection):
  1. TLS handshake (rcpd is server)
  2. Master verifies rcpd's cert fingerprint matches the SSH-delivered value
  3. rcpd verifies master's client cert against --master-cert-fp
  4. Connection mutually authenticated ✓

PHASE 3: Source ↔ Destination Connection
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Master distributes fingerprints via authenticated protocol messages.
Source creates TLS server, destination connects with client cert:
  - Destination verifies source's cert fingerprint
  - Source verifies destination's client cert fingerprint
  - Mutual authentication achieved
```

### Security Properties

| Property            | Guarantee                                                |
| ------------------- | -------------------------------------------------------- |
| **Confidentiality** | All data encrypted with AES-256-GCM or ChaCha20-Poly1305 |
| **Integrity**       | AEAD (Authenticated Encryption with Associated Data)     |
| **Authentication**  | Certificate fingerprint verification                     |
| **Forward Secrecy** | TLS 1.3 ephemeral key exchange                           |

### Why Certificate Pinning?

We use self-signed certificates with fingerprint pinning because:

1. **Ephemeral**: Fresh certificate generated for each transfer session
2. **No PKI required**: No certificate authorities or trust chains
3. **Trusted channel available**: SSH provides secure fingerprint distribution
4. **Mutual authentication**: Both parties verify each other's fingerprints
5. **Standard TLS**: Well-tested, excellent library support

### Fingerprint Distribution Security

Certificate fingerprints are distributed via trusted channels:

- **SSH command line**: master passes its own fingerprint to rcpd via `--master-cert-fp`
- **SSH stderr**: rcpd outputs its fingerprint to stderr; master reads it via SSH
- **Protocol messages**: Source/dest fingerprints exchanged via authenticated TLS channels
- **Per-session**: New certificates and fingerprints for each rcp invocation
- **Memory-only**: Private keys are never written to disk

The key insight is that rcpd acts as the TLS server, allowing master to receive the fingerprint
through the trusted SSH channel BEFORE making any network connection.

## Trust Model

### Trust Chain

```text
User
  │
  ├─── trusts ──→ Local machine (master)
  │
  ├─── trusts ──→ SSH infrastructure
  │                   │
  │                   ├─ authenticates remote hosts
  │                   └─ securely distributes cert fingerprints
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

### What We Protect Against

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
- **TOCTTOU on non-Linux**: The hardened path is Linux-only; non-Linux builds use path-based
  operations. See [TOCTTOU Vulnerabilities](tocttou.md).
- **Trust of the operand path's prefix**: The hardening protects everything at or below the named
  root, but by default the tools do not verify that the directories *above* it are free of
  less-privileged control. `--require-toctou-safe` enforces the hardened walk (refusing
  `-L`/non-Linux) and the strict operand contract: operands must be absolute and lexically normal,
  and every operand open resolves `RESOLVE_NO_SYMLINKS`, so a symlink spliced anywhere along the
  path fails closed. Path *policy* — and keeping prefix directories non-writable by lesser-
  privileged actors — remains the caller's responsibility; see the
  [Scope of TOCTOU safety](tocttou.md#scope-of-toctou-safety) section.

On Linux, the default (non-`-L`) local hardening is implemented through a single shared safe-walk
driver (`common/src/walk_driver.rs`): `rcp` (copy), `rchm`, and `rrm` are `WalkVisitor`
implementations, so the recursive walk — and in particular the leaf-permit "drop before recursion"
deadlock invariant — lives in one place rather than being hand-maintained per tool. The
trusted/hardened boundary is type-enforced via `TrustedDir`, and `DT_UNKNOWN` filter classification
goes through the single `filter_is_dir` path. `rlink` remains dual-tree (source plus `--update`) but
shares the same substrate. See [TOCTTOU Vulnerabilities](tocttou.md) for the full mechanism.

## Threat Model

### Threats Mitigated

| Threat                    | Mitigation                                           |
| ------------------------- | ---------------------------------------------------- |
| **Passive eavesdropping** | TLS encryption prevents reading data                 |
| **Active MITM**           | Fingerprint verification fails for wrong certificate |
| **Rogue rcpd connection** | TLS handshake fails - wrong/missing fingerprint      |
| **Connection racing**     | Attacker cannot present correct certificate          |
| **Data tampering**        | AEAD detects any modification                        |
| **Session hijacking**     | TLS session keys are connection-specific             |

### Race Condition Prevention

A potential attack vector is an attacker racing to connect to a freshly started rcpd listener before
the legitimate master does. Certificate fingerprint verification prevents an attacker from
*authenticating* — it cannot establish a trusted session or impersonate the master:

1. Attacker observes SSH spawning rcpd and probes rcpd's TCP port
2. Attacker attempts a TLS handshake before master connects
3. rcpd requires a client certificate and verifies its fingerprint against `--master-cert-fp`
   (received via SSH)
4. **Attacker's certificate has wrong fingerprint** - handshake rejected, no authenticated session
   is established (confidentiality and integrity are preserved)

**Availability caveat**: this protects authenticity, not availability. None of the listeners bound
the TLS handshake duration, and each awaits handshakes serially, so an attacker who can reach a
listening port before the legitimate peer can deny service:

- **Master-facing listeners (rcpd):** accept the two master connections one at a time and treat a
  failed handshake as fatal — a wrong-certificate attempt aborts rcpd, and a client that stalls
  mid-handshake blocks it.
- **Source↔destination listeners (source rcpd):** the control listener likewise propagates a failed
  handshake as fatal; the data listener logs and continues on failure, but a peer that stalls
  mid-handshake still blocks the accept loop.

Reaching these ports requires network access to the (typically trusted) source/destination hosts;
restrict it with `--port-ranges` plus firewall rules. Bounding each handshake with a timeout and
tolerating failed attempts is a planned hardening.

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
(This does not depend on the version check, which `--auto-deploy-rcpd` can bypass; see
[Remote Copy](remote_copy.md).)

## Disabling Encryption

For trusted networks where encryption overhead is undesirable, use `--no-encryption`:

```bash
rcp --no-encryption source:/path dest:/path
```

**Important**: `--no-encryption` disables **both encryption AND authentication** on **all** rcp TCP
connections — master↔rcpd (control and tracing) as well as Source↔Destination (control and data).
With this flag:

- All traffic (control messages and file data) is transmitted in plaintext
- Every listener accepts connections from anyone who can reach its port (rcpd advertises
  `RCP_TCP <addr>` with no fingerprint)
- SSH authenticates only the *spawning* of rcpd — the subsequent TCP connections are made directly
  to rcpd's port and are not protected by SSH

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
   - Use firewall rules to restrict TCP ports if needed (via `--port-ranges`)
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
  hardened. On Linux, all other default paths (local copy/link/chmod/rm/delete, remote copy
  source+destination) are fully TOCTOU-hardened. Non-Linux builds are not hardened. Use
  `--require-toctou-safe` in sudo rules to enforce the hardened walk (it refuses `-L`/non-Linux)
  plus the strict operand contract (absolute, lexically normal operands, resolved
  `RESOLVE_NO_SYMLINKS`; needs Linux 5.6+). Path policy is still the caller's responsibility — see
  the [Scope of TOCTOU safety](tocttou.md#scope-of-toctou-safety) section of
  [TOCTTOU Vulnerabilities](tocttou.md) for details.

Use `--no-encryption` only on trusted networks where performance is critical.
