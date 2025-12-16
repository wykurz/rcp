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
self-signed certificates and fingerprint pinning. Each party generates an ephemeral certificate,
and fingerprints are distributed via SSH (for master↔rcpd) or protocol messages (for source↔dest).

## Architecture

### Three-Node Remote Copy

A remote copy operation involves three participants:

```text
┌─────────────┐
│   Master    │  (Local machine running `rcp`)
│    (rcp)    │
└─────────────┘
      │ │
      │ └─────────────────┐
      │                   │
      ├─── SSH ───┐       ├─── SSH ───┐
      │           │       │           │
      ▼           ▼       ▼           ▼
┌─────────────┐     ┌─────────────┐
│   Source    │     │ Destination │
│   (rcpd)    │────>│   (rcpd)    │
└─────────────┘     └─────────────┘
       │                   │
       └───────────────────┘
         TLS encrypted
         (control + data)
```

### Communication Channels

| Channel | Protocol | Authentication | Encryption |
|---------|----------|----------------|------------|
| Master → Source (spawn) | SSH | SSH keys/password | SSH |
| Master → Destination (spawn) | SSH | SSH keys/password | SSH |
| Master ↔ Source (control) | TLS | Fingerprint pinning | TLS 1.3 |
| Master ↔ Destination (control) | TLS | Fingerprint pinning | TLS 1.3 |
| Source ↔ Destination (control) | TLS | Mutual fingerprint pinning | TLS 1.3 |
| Source → Destination (data) | TLS | Mutual fingerprint pinning | TLS 1.3 |

## TLS Certificate Authentication

### How It Works

Each party generates an ephemeral self-signed certificate. The key design principle is that
**rcpd is always the TLS server** - this allows master to read the certificate fingerprint
from SSH stdout before connecting, ensuring secure authentication.

**Authentication Flow:**

```
PHASE 1: rcpd Spawn and Setup (via SSH)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Master spawns rcpd processes via SSH:
  ssh host-a "rcpd --role=source"
  ssh host-b "rcpd --role=destination"

Each rcpd:
  1. Generates ephemeral self-signed certificate
  2. Computes fingerprint: SHA256(cert.to_der())
  3. Creates TLS server listener
  4. Outputs to stdout: "RCP_TLS <addr> <hex_fingerprint>"

PHASE 2: Master → rcpd Connection
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Master reads from SSH stdout for each rcpd:
  - Parses "RCP_TLS <addr> <fingerprint>"
  - Fingerprint received via trusted SSH channel!

Master connects TO rcpd as TLS client:
  1. TLS handshake (rcpd is server)
  2. Master verifies rcpd's cert fingerprint matches SSH stdout
  3. Connection authenticated ✓

PHASE 3: Source ↔ Destination Connection
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Master distributes fingerprints via authenticated protocol messages.
Source creates TLS server, destination connects with client cert:
  - Destination verifies source's cert fingerprint
  - Source verifies destination's client cert fingerprint
  - Mutual authentication achieved
```

### Security Properties

| Property | Guarantee |
|----------|-----------|
| **Confidentiality** | All data encrypted with AES-256-GCM or ChaCha20-Poly1305 |
| **Integrity** | AEAD (Authenticated Encryption with Associated Data) |
| **Authentication** | Certificate fingerprint verification |
| **Forward Secrecy** | TLS 1.3 ephemeral key exchange |

### Why Certificate Pinning?

We use self-signed certificates with fingerprint pinning because:

1. **Ephemeral**: Fresh certificate generated for each transfer session
2. **No PKI required**: No certificate authorities or trust chains
3. **Trusted channel available**: SSH provides secure fingerprint distribution
4. **Mutual authentication**: Both parties verify each other's fingerprints
5. **Standard TLS**: Well-tested, excellent library support

### Fingerprint Distribution Security

Certificate fingerprints are distributed via trusted channels:

- **SSH stdout**: rcpd outputs its fingerprint to stdout; master reads it via SSH
- **Protocol messages**: Source/dest fingerprints exchanged via authenticated TLS channels
- **Per-session**: New certificates and fingerprints for each rcp invocation
- **Memory-only**: Private keys are never written to disk

The key insight is that rcpd acts as the TLS server, allowing master to receive the
fingerprint through the trusted SSH channel BEFORE making any network connection.

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

## Threat Model

### Threats Mitigated

| Threat | Mitigation |
|--------|------------|
| **Passive eavesdropping** | TLS encryption prevents reading data |
| **Active MITM** | Fingerprint verification fails for wrong certificate |
| **Rogue rcpd connection** | TLS handshake fails - wrong/missing fingerprint |
| **Connection racing** | Attacker cannot present correct certificate |
| **Data tampering** | AEAD detects any modification |
| **Session hijacking** | TLS session keys are connection-specific |

### Race Condition Prevention

A potential attack vector is an attacker racing to connect to master before the
legitimate rcpd process. Certificate fingerprint verification prevents this:

1. Attacker observes SSH spawning rcpd
2. Attacker tries to connect to master first
3. rcpd verifies master's certificate fingerprint (received via SSH)
4. **Attacker's certificate has wrong fingerprint** - connection rejected
5. Legitimate rcpd connects and verifies correct fingerprint

### Cipher Suites

With TLS 1.3, the following cipher suites are used:

- `TLS_AES_256_GCM_SHA384` (preferred, hardware accelerated)
- `TLS_AES_128_GCM_SHA256` (fallback)
- `TLS_CHACHA20_POLY1305_SHA256` (software-only systems)

## Disabling Encryption

For trusted networks where encryption overhead is undesirable, use `--no-encryption`:

```bash
rcp --no-encryption source:/path dest:/path
```

**When to use `--no-encryption`:**
- Isolated datacenter networks with no untrusted traffic
- Performance-critical transfers on physically secured networks
- Testing and debugging

**When NOT to use `--no-encryption`:**
- Any network with potential eavesdroppers
- Cross-datacenter transfers over public internet
- Transfers containing sensitive data

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

Use `--no-encryption` only on trusted networks where performance is critical.
