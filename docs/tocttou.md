# TOCTTOU Vulnerabilities in rcp

This document describes Time-of-Check-Time-of-Use (TOCTTOU) race condition vulnerabilities
that affect rcp when used with elevated privileges, and discusses potential mitigations.

## Table of Contents

- [Overview](#overview)
- [What is TOCTTOU?](#what-is-tocttou)
- [Attack Scenarios](#attack-scenarios)
- [Current State](#current-state)
- [Mitigation Options](#mitigation-options)
- [Recommendations](#recommendations)

## Overview

When rcp runs with elevated privileges (e.g., via sudo), an unprivileged attacker on the
same system may be able to exploit TOCTTOU race conditions to:

- Read files they shouldn't have access to
- Write to locations outside the intended destination
- Cause rcp to follow symlinks to sensitive locations

**Important**: These attacks require local access to the system and the ability to modify
files/directories in the path being copied. Remote network attackers cannot exploit these
vulnerabilities directly.

## What is TOCTTOU?

TOCTTOU (Time-of-Check-Time-of-Use) is a class of race condition that occurs when:

1. A program **checks** some property of a file (e.g., is it a regular file?)
2. Time passes (even microseconds)
3. The program **uses** the file based on that check (e.g., opens and reads it)

Between steps 1 and 3, an attacker can swap the file for something else (typically a
symlink), causing the program to operate on a different file than intended.

```
Privileged process (rcp via sudo)          Attacker (unprivileged)
─────────────────────────────────          ─────────────────────────
lstat("/data/file") → regular file
                                           mv /data/file /data/file.bak
                                           ln -s /etc/shadow /data/file
open("/data/file") → opens /etc/shadow!
read() → reads /etc/shadow contents
write to destination → leaks /etc/shadow
```

## Attack Scenarios

### Scenario 1: Symlink Race Attack (Privileged Source Read)

**Setup**: A user has a sudo rule allowing them to copy files from `/backup`:

```bash
user ALL=(root) NOPASSWD: /usr/bin/rcp /backup/* /home/user/restore/
```

**Attack**:

1. Attacker creates `/backup/myfile` (a regular file they own)
2. Attacker runs: `sudo rcp /backup/myfile /home/user/restore/`
3. In a tight loop, attacker rapidly alternates:
   - `ln -sf /etc/shadow /backup/myfile`
   - `touch /backup/myfile` (recreate regular file)
4. If the timing is right, rcp checks when it's a regular file, but opens when it's a symlink
5. Result: `/etc/shadow` is copied to `/home/user/restore/myfile`

### Scenario 2: Destination Escape Attack

**Setup**: A user can copy to a specific destination via sudo:

```bash
user ALL=(root) NOPASSWD: /usr/bin/rcp /home/user/upload/* /var/www/html/
```

**Attack**:

1. Attacker creates a directory structure: `/home/user/upload/innocent/`
2. Attacker runs: `sudo rcp /home/user/upload/innocent /var/www/html/`
3. During the copy, attacker replaces `/var/www/html/innocent` with a symlink to `/etc`
4. rcp continues writing into what it thinks is the destination directory
5. Result: Files written to `/etc/` instead of `/var/www/html/innocent/`

### Scenario 3: Directory Traversal via Symlink

**Setup**: rcp is copying a directory tree:

```bash
sudo rcp /shared/project /backup/
```

**Attack**:

1. Attacker has write access to `/shared/project/subdir/`
2. During directory traversal, attacker replaces `subdir/file` with a symlink to `/etc/passwd`
3. rcp (running as root) reads `/etc/passwd` instead of the intended file
4. Result: Sensitive file contents copied to attacker-accessible backup

### Scenario 4: Metadata Preservation Attack

**Setup**: rcp preserves ownership when run as root:

```bash
sudo rcp --preserve=ownership /data/files /backup/
```

**Attack**:

1. Attacker creates `/data/files/setuid_shell` owned by attacker
2. During copy, attacker replaces destination with symlink to `/usr/bin/`
3. rcp applies metadata (ownership) to the symlink target
4. Result: Attacker potentially modifies ownership of system binaries

## Current State

rcp currently has **no specific TOCTTOU mitigations**. File operations follow this pattern:

```rust
// common/src/copy.rs - simplified
let src_metadata = tokio::fs::symlink_metadata(&src).await?;  // CHECK
// ... time passes, file can change ...
if src_metadata.is_file() {
    tokio::fs::copy(&src, &dst).await?;  // USE
}
```

**Behaviors that expose TOCTTOU windows**:

1. **Metadata check before open**: `symlink_metadata()` followed by `copy()` or `read_dir()`
2. **Path-based operations**: All operations use path strings, not file descriptors
3. **No O_NOFOLLOW enforcement**: Standard `tokio::fs` doesn't provide this control
4. **Destination checks**: Existence and metadata checks before writing
5. **Permission preservation**: `fchownat()`/`utimensat()` applied after file creation

**What rcp does do**:

- Uses `symlink_metadata()` (lstat) instead of `metadata()` (stat) to not follow symlinks
  during checks
- Explicit `--dereference` flag required to follow symlinks
- Preserves symlinks as symlinks by default (copies link target path, not link content)

## Mitigation Options

### Option 1: libpathrs Integration

[libpathrs](https://github.com/cyphar/libpathrs) is a Rust library designed specifically
to prevent TOCTTOU attacks through safe path resolution.

**How it works**:

1. Opens a "Root" handle to the base directory
2. Resolves paths **within** that root, returning `O_PATH` file descriptors
3. All operations use file descriptors, not paths
4. Leverages Linux `openat2()` with `RESOLVE_NO_SYMLINKS` / `RESOLVE_BENEATH` flags

```rust
use pathrs::Root;

let root = Root::open("/backup")?;
let handle = root.resolve("subdir/file")?;  // Safe resolution
let file = handle.reopen(OpenFlags::O_RDONLY)?;  // fd-based open
```

**Advantages**:

- Designed for exactly this problem (used by container runtimes like runc)
- Uses kernel features when available (openat2, Linux 5.6+)
- Falls back to userspace emulation on older kernels
- Well-tested, security-focused

**Disadvantages**:

- Linux-only (no macOS/Windows support)
- Requires significant refactoring of file operations
- Older kernel fallback is less secure than kernel-native path
- Adds external dependency

**Kernel requirements for full protection**:

| Linux Version | Protection Level |
|---------------|------------------|
| < 5.6         | Userspace emulation only (limited protection) |
| 5.6+          | openat2() with RESOLVE_* flags |
| 6.8+          | Full protection including procfs safety |

### Option 2: O_NOFOLLOW and openat()

A lighter-weight approach using standard POSIX APIs:

```rust
use std::os::unix::fs::OpenOptionsExt;

OpenOptions::new()
    .read(true)
    .custom_flags(libc::O_NOFOLLOW)
    .open(&path)?;
```

**Advantages**:

- No external dependencies
- Works on all Unix systems
- Simpler to implement

**Disadvantages**:

- Only prevents following symlinks at the **final** component
- Parent directories can still be swapped for symlinks
- Must be combined with `openat()` for directory traversal safety

### Option 3: Privilege Separation

Instead of running the entire copy operation as root, separate into privileged and
unprivileged components:

```
┌─────────────────────┐         ┌─────────────────────┐
│  Privileged Helper  │────────▶│  Unprivileged rcp   │
│  (opens files)      │   fd    │  (copies data)      │
└─────────────────────┘         └─────────────────────┘
```

**Advantages**:

- Reduces attack surface
- Privileged code can be minimal and audited
- File descriptors cannot be swapped (unlike paths)

**Disadvantages**:

- Complex architecture change
- Performance overhead from IPC
- Still needs careful implementation in the helper

### Option 4: chroot/namespace Isolation

Run copy operations inside a restricted namespace:

```bash
# Using unshare to create mount namespace
unshare --mount --propagation=private -- rcp /source /dest
```

**Advantages**:

- Kernel-enforced isolation
- Works with existing rcp code

**Disadvantages**:

- Requires root/CAP_SYS_ADMIN to set up namespace
- May not work in all environments (containers, etc.)
- Doesn't solve the fundamental TOCTTOU issue, just limits scope

## Recommendations

### For Users Running rcp with Sudo

1. **Minimize privilege scope**: Use specific sudo rules instead of blanket root access

   ```bash
   # Bad: allows copying anywhere
   user ALL=(root) NOPASSWD: /usr/bin/rcp

   # Better: restricts source and destination
   user ALL=(root) NOPASSWD: /usr/bin/rcp /specific/source/* /specific/dest/
   ```

2. **Restrict paths to root-owned directories**: If the source directory is owned by root
   and not writable by the attacker, they cannot perform symlink swaps

3. **Use `--dereference` carefully**: This flag explicitly follows symlinks and increases
   the attack surface

4. **Consider alternatives for sensitive operations**:
   - For backup: Use rsync with `--safe-links` or dedicated backup tools
   - For system files: Use package managers or configuration management tools
   - For containers: Use tools designed for container filesystems (with libpathrs)

5. **Monitor for suspicious activity**: Rapid file modifications during copies may
   indicate an attack attempt

### For Developers

If implementing TOCTTOU mitigations in rcp:

1. **libpathrs is the recommended approach** for comprehensive protection on Linux
2. Start with a `--safe-resolve` flag that enables libpathrs-based resolution
3. Make it opt-in initially, with clear documentation about kernel requirements
4. Consider Linux-only for this feature rather than attempting cross-platform support
5. Add integration tests that verify TOCTTOU protection using rapid symlink swapping

### Defense in Depth

No single mitigation is perfect. Combine multiple approaches:

```
Layer 1: Restrictive sudo rules (limit paths)
       ↓
Layer 2: libpathrs safe resolution (prevent symlink following)
       ↓
Layer 3: SELinux/AppArmor policies (restrict file access)
       ↓
Layer 4: Audit logging (detect attack attempts)
```

## Further Reading

- [LWN: The difficulty of safe path traversal](https://lwn.net/Articles/1050887/)
- [libpathrs documentation](https://docs.rs/pathrs/latest/pathrs/)
- [openat2(2) man page](https://man7.org/linux/man-pages/man2/openat2.2.html)
- [CWE-367: TOCTTOU Race Condition](https://cwe.mitre.org/data/definitions/367.html)
- [CVE-2019-16884](https://nvd.nist.gov/vuln/detail/CVE-2019-16884) - runc symlink attack

## Summary

| Aspect | Current State | With libpathrs |
|--------|---------------|----------------|
| Symlink following | Uses lstat, but path-based ops | fd-based, kernel-enforced |
| Directory traversal | Path strings, TOCTTOU window | O_PATH handles, atomic |
| Cross-platform | Linux, macOS, Windows | Linux only |
| Kernel requirement | Any | 5.6+ recommended, 6.8+ ideal |
| Implementation effort | - | Significant refactoring |

TOCTTOU vulnerabilities in rcp are **real but require local access** and specific sudo
configurations to exploit. For high-security environments where rcp runs with elevated
privileges, consider the mitigations described above, particularly libpathrs for
Linux-only deployments.
