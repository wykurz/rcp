# Security Model

This document describes the security architecture of the rcp (remote copy) tool and its defense mechanisms against common network attacks.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Security Mechanisms](#security-mechanisms)
- [Trust Model](#trust-model)
- [Attack Resistance](#attack-resistance)
- [Security Best Practices](#security-best-practices)
- [Threat Model](#threat-model)
- [Insecure Mode](#insecure-mode)

## Overview

The rcp remote copy system implements a **defense-in-depth security model** that combines:

1. **SSH** for authentication and authorization
2. **TLS 1.3 (via QUIC)** for transport encryption
3. **Certificate Pinning** for connection integrity and MITM prevention

This layered approach provides strong security guarantees while maintaining ease of deployment and high performance.

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
                    Direct QUIC
                    connection
```

### Communication Channels

1. **Master ← Source (rcpd)**: QUIC control connection
2. **Master ← Destination (rcpd)**: QUIC control connection
3. **Source → Destination**: QUIC data transfer connection

**All three connections are secured with certificate pinning.**

## Security Mechanisms

### 1. SSH Authentication & Authorization

**SSH provides the security perimeter** for remote copy operations.

- **Authentication**: Users must successfully authenticate via SSH before any file operations
- **Authorization**: SSH permissions (user accounts, sudo access, file permissions) control what operations are allowed
- **Key-Based Auth Recommended**: SSH key-based authentication is more secure than passwords
- **Existing Infrastructure**: Leverages existing SSH configuration and security policies

### 2. QUIC with TLS 1.3

All data transfer uses the QUIC protocol, which is built on TLS 1.3:

- **Encryption**: All data encrypted using modern ciphers (ChaCha20-Poly1305, AES-GCM)
- **Perfect Forward Secrecy**: Session keys are ephemeral and cannot be recovered later
- **Integrity**: Cryptographic authentication prevents data tampering
- **Replay Protection**: Built-in mechanisms prevent replay attacks
- **0-RTT Disabled**: We don't use 0-RTT mode to avoid potential security issues

### 3. Certificate Pinning

Certificate pinning prevents man-in-the-middle (MITM) attacks by validating that QUIC connections are established with the expected peer.

#### How It Works

**For Master ← rcpd connections:**

1. Master generates an ephemeral self-signed certificate at startup
2. Master computes SHA-256 fingerprint of the certificate
3. When launching rcpd via SSH, Master passes the fingerprint as a command-line argument
4. rcpd receives the fingerprint through the secure SSH channel
5. When rcpd connects to Master via QUIC, it validates the certificate fingerprint
6. Connection is rejected if fingerprints don't match

**For Source → Destination connections:**

1. Source generates an ephemeral self-signed certificate
2. Source computes SHA-256 fingerprint and sends it to Master over the already-secured Master←Source connection
3. Master forwards the fingerprint to Destination over the already-secured Master←Destination connection
4. Destination validates Source's certificate against the received fingerprint when establishing the direct connection
5. Connection is rejected if fingerprints don't match

#### Why This Is Secure

The security relies on SSH as a **secure out-of-band channel** for fingerprint exchange:

```text
┌────────────────────────────────────────────────────────┐
│  SSH Connection (authenticated & encrypted)            │
│                                                         │
│  Master ─────[certificate fingerprint]────> rcpd      │
│                                                         │
│  This channel is already secured by SSH authentication │
└────────────────────────────────────────────────────────┘
                         │
                         │ Fingerprint is trustworthy because
                         │ SSH authenticated the source
                         ▼
┌────────────────────────────────────────────────────────┐
│  QUIC Connection                                       │
│                                                         │
│  rcpd validates certificate matches the fingerprint    │
│  received via SSH                                      │
│                                                         │
│  If valid → Connection is trustworthy                  │
│  If invalid → MITM attack detected, connection refused │
└────────────────────────────────────────────────────────┘
```

**Chain of Trust:**
1. User trusts SSH (proven by successful authentication)
2. SSH carries the certificate fingerprint securely
3. QUIC connection validates against that fingerprint
4. Therefore, QUIC connection is trustworthy

## Trust Model

### Trusted Components

1. **SSH Infrastructure**: SSH must be properly configured and uncompromised
2. **Master Machine**: The local machine running `rcp` is assumed to be trusted
3. **Cryptographic Libraries**: rustls, ring, quinn are assumed to implement cryptography correctly

### Trust Establishment

```text
User → SSH → Master → Certificate Fingerprint → QUIC Connection

Each arrow represents a trust relationship:
1. User trusts their SSH client and configuration
2. SSH authenticates and encrypts communication with remote hosts
3. Master uses SSH to bootstrap trust for QUIC connections
4. Certificate fingerprints validate QUIC peer identity
```

### Ephemeral Certificates

- Certificates are **generated fresh for each session**
- Private keys never leave the machine that generates them
- Fingerprints are only valid for the current operation
- No long-term certificate management or PKI required

## Attack Resistance

### ✅ Protected Attacks

| Attack Type | Protection Mechanism |
|-------------|---------------------|
| **Man-in-the-Middle (MITM)** | Certificate pinning detects and prevents endpoint impersonation |
| **Eavesdropping** | TLS 1.3 encryption protects all data in transit |
| **Data Tampering** | Cryptographic authentication (AEAD) prevents modifications |
| **Replay Attacks** | TLS 1.3 and QUIC built-in replay protection |
| **Unauthorized Access** | SSH authentication required before any operations |
| **Connection Hijacking** | Certificate pinning ensures peer identity throughout connection |
| **Downgrade Attacks** | TLS 1.3 prevents protocol downgrade |

### Attack Scenarios

#### Scenario 1: Network MITM Attack

**Attack**: Attacker intercepts network traffic and tries to impersonate Master

```text
Master ─────X─────→ [Attacker] ─────→ rcpd
             ↑
         Intercepted
```

**Defense**:
- Attacker cannot provide valid certificate matching the fingerprint sent via SSH
- rcpd computes fingerprint of attacker's certificate
- Fingerprint mismatch is detected
- Connection is refused
- **Result: Attack prevented ✅**

#### Scenario 2: Compromised Network Device

**Attack**: Attacker controls a router or switch and attempts to redirect QUIC traffic

**Defense**:
- Even with network control, attacker cannot generate a certificate with the expected fingerprint
- SHA-256 preimage resistance makes it computationally infeasible to forge certificates
- **Result: Attack prevented ✅**

#### Scenario 3: DNS Spoofing

**Attack**: Attacker modifies DNS to redirect connections

**Defense**:
- rcp uses SSH for initial connections, which establishes host identity
- Certificate fingerprints are tied to actual endpoints, not DNS names
- **Result: Attack prevented ✅**

### ❌ Out of Scope

The following attacks are **not** protected by rcp itself:

| Attack Type | Why Not Protected | Mitigation |
|-------------|-------------------|------------|
| **Compromised SSH** | SSH is the root of trust | Use SSH best practices (key-based auth, fail2ban, etc.) |
| **Compromised Master** | Master is trusted by design | Keep Master machine secure, use separate accounts |
| **Malicious rcpd Binary** | Binary authenticity not verified | Verify binary integrity, use package managers |
| **Local Privilege Escalation** | OS-level security issue | Keep systems patched, use proper permissions |
| **Timing Attacks on Crypto** | Library implementation details | Use up-to-date cryptographic libraries |

## Security Best Practices

### For System Administrators

1. **SSH Hardening**
   - Use key-based authentication (disable password auth)
   - Implement rate limiting (fail2ban, sshguard)
   - Keep SSH server updated
   - Use strong key types (Ed25519, RSA 4096+)
   - Configure proper file permissions on SSH keys

2. **Network Security**
   - Use firewall rules to restrict QUIC ports if needed (via `--quic-port-ranges`)
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

### For Users

1. **Verify Connections**
   - Check SSH host key fingerprints on first connection
   - Monitor for certificate validation failures (may indicate MITM)
   - Review rcp verbose logs (`-vv`) for security diagnostics

2. **Secure Workflows**
   - Use SSH agent forwarding carefully (or avoid it)
   - Don't disable certificate verification unless absolutely necessary
   - Keep SSH keys protected (passphrase, key agent timeout)

3. **Error Handling**
   - Investigate certificate mismatch errors immediately
   - Don't ignore connection failures
   - Report suspicious connection behavior

## Threat Model

### Assumptions

1. **SSH is secure**: The underlying SSH implementation is trustworthy and properly configured
2. **Cryptography is sound**: TLS 1.3, SHA-256, and cryptographic libraries work as designed
3. **Master is trusted**: The machine running the Master (rcp) is not compromised
4. **Physical security**: Attackers don't have physical access to machines
5. **Side channels**: Timing attacks and other side-channel attacks are out of scope

### Trust Boundaries

```text
┌─────────────────────────────────────┐
│  Trusted Zone                       │
│  - Master machine                   │
│  - SSH authentication               │
│  - Local user environment           │
└─────────────────────────────────────┘
                │
                │ SSH (authenticated)
                ▼
┌─────────────────────────────────────┐
│  Network (Untrusted)                │
│  - Can intercept traffic            │
│  - Can modify packets               │
│  - Can inject traffic               │
│  - Cannot forge certificates        │  ← Protected by certificate pinning
└─────────────────────────────────────┘
                │
                │ QUIC (cert pinned)
                ▼
┌─────────────────────────────────────┐
│  Remote Hosts                       │
│  - Authenticated via SSH            │
│  - Running authorized rcpd          │
│  - Bound by file permissions        │
└─────────────────────────────────────┘
```

### Known Limitations

1. **Session Secrets Not Persisted**: If Master is compromised during operation, current session could be affected
2. **No Forward Secrecy Between Sessions**: Each session is independent (this is actually a feature)
3. **Certificate Validation Only**: We don't validate hostname, expiry, or CA chains (not needed for ephemeral certs)
4. **No Client Certificates**: rcpd doesn't present certificates to Master (authentication is via fingerprint validation)

## Insecure Mode

For testing or completely trusted network environments, certificate verification can be disabled.

### When to Use

- **Development and testing** on localhost
- **Completely isolated networks** with physical security
- **Debugging connection issues** (temporarily)

### How to Enable

Insecure mode is used when no certificate pinning is configured:

```rust
// Explicitly skip verification (not recommended)
let client = get_client_with_port_ranges(Some("8000-8999"), true)?;
```

### Security Warning

⚠️ **CRITICAL**: Insecure mode makes connections vulnerable to man-in-the-middle attacks!

- An attacker on the network can intercept and modify all data
- No validation of peer identity is performed
- Only use in completely trusted environments

## Compliance and Standards

### Standards Compliance

- **TLS 1.3**: RFC 8446
- **QUIC**: RFC 9000
- **SSH**: OpenSSH (system SSH implementation)
- **Cryptography**:
  - SHA-256 for fingerprints (FIPS 180-4)
  - AES-GCM, ChaCha20-Poly1305 for encryption
  - ECDHE for key exchange

### Security Audits

- Regular dependency updates via cargo audit
- Static analysis via clippy
- Code review for security-sensitive changes
- Community security reports via GitHub issues

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

## Conclusion

The rcp security model provides strong protection against network-based attacks while maintaining ease of use and high performance. By combining SSH authentication, TLS 1.3 encryption, and certificate pinning, rcp ensures that remote file copy operations are:

- ✅ **Authenticated**: Only authorized users can initiate operations
- ✅ **Encrypted**: All data is protected from eavesdropping
- ✅ **Integrity-Protected**: Data cannot be modified in transit
- ✅ **MITM-Resistant**: Certificate pinning prevents endpoint impersonation

As long as SSH is properly configured and the Master machine is trusted, rcp provides a secure foundation for remote file operations.
