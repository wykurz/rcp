# Security Model

This document describes the security architecture of the rcp (remote copy) tool.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Security Mechanisms](#security-mechanisms)
- [Trust Model](#trust-model)
- [Current Limitations](#current-limitations)
- [Security Best Practices](#security-best-practices)
- [Future Work](#future-work)

## Overview

The rcp remote copy system currently relies on:

1. **SSH** for authentication, authorization, and rcpd deployment
2. **Network trust** for data transfer between source and destination

> **⚠️ SECURITY WARNING**: Data transfers between source and destination are currently **unencrypted**. Use only on trusted networks or tunnel through SSH/VPN for sensitive data.

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
                    Direct TCP
                    connection
                    (unencrypted)
```

### Communication Channels

1. **Master ↔ Source (rcpd)**: TCP control connection
2. **Master ↔ Destination (rcpd)**: TCP control connection
3. **Source → Destination**: TCP data transfer (control + data ports)

## Security Mechanisms

### SSH Authentication & Authorization

**SSH provides the security perimeter** for remote copy operations.

- **Authentication**: Users must successfully authenticate via SSH before any file operations
- **Authorization**: SSH permissions (user accounts, sudo access, file permissions) control what operations are allowed
- **Key-Based Auth Recommended**: SSH key-based authentication is more secure than passwords
- **Existing Infrastructure**: Leverages existing SSH configuration and security policies

### rcpd Lifecycle Management

- rcpd processes are spawned via SSH and connect back to master
- stdin watchdog ensures rcpd exits if master dies (no orphaned processes)
- rcpd only performs operations authorized by the SSH session

## Trust Model

### Trusted Components

1. **SSH Infrastructure**: SSH must be properly configured and uncompromised
2. **Master Machine**: The local machine running `rcp` is assumed to be trusted
3. **Network between Source and Destination**: Currently assumed to be trusted (see limitations)

### Trust Establishment

```text
User → SSH → Master → rcpd processes

Each arrow represents a trust relationship:
1. User trusts their SSH client and configuration
2. SSH authenticates and encrypts communication with remote hosts
3. Master spawns rcpd processes via authenticated SSH sessions
```

## Current Limitations

### No Encryption for Data Transfer

**The data channel between source and destination is currently unencrypted.**

| Risk | Description | Mitigation |
|------|-------------|------------|
| **Eavesdropping** | Attackers on the network can observe file contents | Use VPN or SSH tunneling |
| **MITM attacks** | Attackers could potentially intercept/modify data | Use trusted networks only |
| **Data integrity** | No cryptographic verification of data in transit | TCP checksums provide basic error detection |

### Recommended Mitigations

For sensitive data transfers:

1. **Use SSH port forwarding**:
   ```bash
   # Set up tunnel, then use localhost addresses
   ssh -L 8000:destination:8000 source-host
   ```

2. **Use VPN**: Ensure source and destination are on a VPN

3. **Use trusted networks**: Datacenter networks, private networks, etc.

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
   - Consider VPN for cross-datacenter transfers

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
   - Use VPN/tunneling for sensitive data

2. **Error Handling**
   - Don't ignore connection failures
   - Report suspicious connection behavior

## Future Work

### Planned Security Enhancements

1. **TLS encryption for data connections**
   - Add rustls-based encryption between source and destination
   - Certificate pinning via SSH-distributed fingerprints
   - Will restore confidentiality and integrity guarantees

2. **Mutual authentication**
   - Source and destination verify each other's identity
   - Prevent unauthorized data injection

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

The current rcp security model provides:

- ✅ **Authentication**: SSH authentication required before any operations
- ✅ **Authorization**: SSH/filesystem permissions control access
- ✅ **Process isolation**: rcpd processes are properly lifecycle-managed
- ⚠️ **Encryption**: Data transfers are currently unencrypted (use trusted networks)
- ⚠️ **Integrity**: No cryptographic verification (TCP checksums only)

Use rcp on trusted networks or with additional tunneling for sensitive data until TLS support is added.
