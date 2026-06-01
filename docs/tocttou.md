# TOCTTOU Vulnerabilities in the RCP tools

This document describes Time-of-Check-Time-of-Use (TOCTTOU) race condition vulnerabilities
that affect the RCP tools when used with elevated privileges, and documents the hardening
that is now implemented on Linux. The examples below use `rcp`, but the attack pattern
applies to the other tools as well.

## Table of Contents

- [Overview](#overview)
- [Scope of TOCTOU safety](#scope-of-toctou-safety)
- [What is TOCTTOU?](#what-is-tocttou)
- [Attack Scenarios](#attack-scenarios)
- [Implemented Hardening](#implemented-hardening)
- [What Is Not Hardened](#what-is-not-hardened)
- [Residual Preconditions](#residual-preconditions)
- [The Linter: --toctou-check and --require-toctou-safe](#the-linter---toctou-check-and---require-toctou-safe)
- [Summary](#summary)

## Overview

When rcp runs with elevated privileges (e.g., via sudo), an unprivileged attacker on the
same system may be able to exploit TOCTTOU race conditions to:

- Read files they shouldn't have access to
- Write to locations outside the intended destination
- Cause rcp to follow symlinks to sensitive locations

**Important**: These attacks require local access to the system and the ability to modify
files/directories in the path being copied. Remote network attackers cannot exploit these
vulnerabilities directly.

**The precise threat condition** is not merely "running as root": it is *running with more
privilege than an actor who can modify the paths being traversed.* Root operating on
trusted, root-owned trees is outside this threat; the risk arises specifically when a
more-privileged run traverses a tree that a less-privileged actor can write to.

On Linux, the local and remote default paths are now TOCTOU-hardened (see
[Implemented Hardening](#implemented-hardening) below) — for everything *at or below the
named root*. The trust of the path *above* the named root is the caller's responsibility.
The next section defines the guarantee precisely; read it before relying on TOCTOU safety
under elevated privilege.

## Scope of TOCTOU safety

TOCTOU safety in the RCP tools rests on **two guarantees** and **one delegated
responsibility**. The scope is deliberately narrow, because a general-purpose copy/chmod/rm
tool can neither freeze the filesystem while it works nor vouch for the paths it is handed. So
it guarantees only what is both achievable and security-relevant, and leans on the caller for
the rest.

### In scope — the two guarantees

On the default (non-`-L`) path, on Linux, given the operand paths as written:

**1. Containment.** No symlink or path-component swap _at or below a named root_ — anywhere in
the tree the tool traverses — can redirect a read, write, chmod, chown, or delete to an object
**outside that root's subtree.** This is the property that matters when a privileged process
operates on a tree a less-privileged actor can write *into* (the classic "root copies, chmods,
or removes over an attacker-controlled directory" hazard). It is delivered by the fd-based
safe walk described below: the named root and every entry beneath it are opened `O_NOFOLLOW`,
classified by `fstat` of the held fd, and operated on via fd-relative syscalls — and for
chmod/chown/hard-link, via the entry's pinned `O_PATH` fd through `/proc/self/fd` — so a
swapped-in symlink is never followed out of the subtree.

**2. Permission and ownership fidelity.** The destination (`rcp`/`rlink`) or the modified
entry (`rchm`) receives exactly the permissions and ownership of the source object that was
actually read; a concurrent swap can never make the tool **widen** permissions or attach the
wrong owner — e.g. it cannot write a `0600` root-owned file's contents out as world-readable.
Mode and bytes are taken from the *same* fd, and metadata is applied through the destination's
own held fd, never re-resolved by path.

**"The named root"** is the final component of the operand path — the file or directory you
name. It is opened `O_NOFOLLOW` and classified by the `fstat` of that held fd, so a swap of
the root *entry itself* (within its parent) is caught at open, and everything reachable
beneath it is the hardened tree. (Corollary: the root's immediate parent need not be
non-writable for the guarantee to hold — a swap of the root entry there is caught at open.)

### Out of scope — what we deliberately do not promise

**Freezing the tree, or pinning _which_ object is operated on.** The tree may change
concurrently; the tool operates on whatever is validly reachable within the subtree at access
time. Each child is reached with a single-component `openat(parent_fd, name, O_NOFOLLOW)`,
which re-resolves `name` — it is **not** pinned to the exact inode first classified, so a
same-name swap to *another regular file in the same hardened directory* is possible and
accepted. This is deliberate, not a gap, because it is not a security boundary: an actor who
can swap entries inside the subtree already controls that subtree's contents, so operating on
the swapped-in file grants them nothing they did not already have. Both guarantees above still
hold across such a swap — you cannot escape the subtree (Containment), and permissions are
never widened because mode and bytes come from the *same* fd (Fidelity). We do not attempt to
detect or prevent concurrent modification beyond that.

**Whether the operand path _itself_ is trustworthy.** The directories *above* the named root —
the prefix the tool follows to reach it — are resolved normally (following symlinks). The
tools do **not** verify that this prefix is free of less-privileged control, or free of
symlinks that resolve somewhere unexpected. A general-purpose tool has no way to decide
whether a given path is an acceptable privileged target: *"is `/home/alice` a legitimate place
for root to write?"* depends entirely on policy the tool does not — and cannot — know. Any
in-tool "is this prefix trusted" heuristic is either unsound (it cannot anticipate every
symlink, `..`, ownership, and mount-namespace case) or so conservative it refuses almost every
real privileged copy. So the tools do not attempt it. **The caller must pass operands it has
resolved and whose prefix it trusts.**

The guarantees are *additionally* bounded by separately-documented exceptions:
`--dereference`/`-L` and non-Linux builds (see [What Is Not Hardened](#what-is-not-hardened)),
`rcmp` (read-only; out of scope), and the kernel preconditions `fs.protected_hardlinks=1` and
no attacker-controlled bind mounts (see [Residual Preconditions](#residual-preconditions)). A
hardlink alias planted at or below the root, for instance, is a *non-swap* redirect covered by
the `protected_hardlinks` precondition rather than by the Containment guarantee.

### The contract for safe privileged (sudo) use

Safe TOCTOU use under elevated privilege is a two-layer arrangement:

1. **A layer above the RCP tools** — the `sudo` policy, or a thin vetted wrapper — decides
   which operand paths are acceptable and ensures they are fully resolved and that the
   directories above each named root are not under a less-privileged actor's control. The
   "is this path safe?" judgment lives here, because this is where the policy context exists.
2. **The RCP tool** guarantees the hardened walk for everything at and below those named
   roots (the in-scope property above).

`--require-toctou-safe` is the tool's half of this contract: it refuses to run unless the
invocation uses the hardened walk — rejecting `--dereference`/`-L` (which follows symlinks by
design) and non-Linux builds. It does **not**, and cannot, vouch for the trust of the operand
paths; that is the caller's responsibility per (1).

```bash
# The sudo policy constrains operands to vetted, resolved, trusted paths;
# --require-toctou-safe guarantees the hardened walk for what is at/below them.
user ALL=(root) NOPASSWD: /usr/bin/rcp --require-toctou-safe /vetted/source/* /vetted/dest/
```

A wildcard rule (`... --require-toctou-safe *`) enforces only the tool's half — hardened
walk, no `-L`. It does **not** make an arbitrary destination safe. Lock the paths down in the
policy.

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

These scenarios explain the classes of attack the hardening defends against. Each is now
defeated on Linux for the default (non-`-L`) path.

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

**Hardening**: The source open uses `openat(parent_fd, "myfile", O_RDONLY|O_NOFOLLOW|O_NONBLOCK)`.
A swapped-in symlink fails with `ELOOP`. A swapped-in FIFO is caught by the subsequent
`fstat`+`S_ISREG` check (the process never blocks waiting for a writer).

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

**Hardening**: Destination directories are opened `O_NOFOLLOW|O_DIRECTORY` relative to the
parent's held fd. If the entry has been swapped to a symlink, `openat` fails with `ELOOP`.
All file writes are relative to that held fd — never re-resolving the path.

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

**Hardening**: Every entry name is opened with `openat(parent_dir_fd, name, O_NOFOLLOW)`,
where `parent_dir_fd` is the already-held directory fd opened the same way. A swapped
symlink anywhere in the tree causes a fail-closed `ELOOP` or `ENOTDIR` — never a
redirect.

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

**Hardening**: All metadata operations (chown, chmod, utimes) use fd-based syscalls
(`fchown`, `fchmod`, fd-relative `utimensat`) on the held destination fd rather than
path-based calls. There is no path re-resolution after the file is created.

## Implemented Hardening

### Mechanism: fd-based safe walk

The core principle is **never re-resolve a path**. The implementation (in
`common/src/safedir.rs`) holds each directory as an open fd (`Dir`, opened
`O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`) and derives each child via a
single-component `openat(parent_fd, name, O_NOFOLLOW)`. The type of every entry is
determined by `fstat`-ing that fd — not from the `getdents` `d_type` field alone
(which is only a hint). Every operation on the child is performed on the held fd or via
fd-relative `*at()` syscalls.

Specific invariants enforced:

- Every `openat` of a non-dereferenced entry uses `O_NOFOLLOW`. A swapped-in symlink
  fails with `ELOOP` (fail closed).
- File opens include `O_NONBLOCK` to avoid blocking on a FIFO that an attacker swapped
  in. The subsequent `fstat`+`S_ISREG` check rejects any non-regular entry (FIFO,
  device, directory) with `EINVAL`.
- Metadata operations (chown, chmod, utimes, and critically symlink timestamps) use
  fd-based syscalls. File data is copied via `copy_file_range` between held fds.
- On overwrite paths, a `recheck` verifies that the `(dev, ino)` of the entry matches
  the originally classified handle before performing the unlink.
- Directory names passed to any `*at()` call are validated to be single path
  components (no `/`, `.`, `..`).
- **Source payload and metadata come from the same fd (read-side fidelity).** For each copied or
  sent source object, the data and the metadata applied/sent for it are read from one open file
  description, so a same-name swap cannot pair one inode's bytes/target with another inode's
  mode/owner/timestamps: a regular file via `open_file_read` → `(File, FileMeta)`; a symlink via the
  `O_PATH` handle's `read_symlink` (target + metadata off the one fd); a directory via the
  enumerated `Dir` fd (`read_entries` + `meta`). The remote *destination* is fidelity-safe by
  construction — it writes the received bytes and applies the received metadata to its single
  created fd, so there are no two source fds to mismatch. `scripts/check-source-read-fidelity.sh`
  (run in CI) backstops this by forbidding by-name source-payload reads (`read_link_at`,
  `File::open`) in the hardened modules, outside the `-L`/`--dereference` path.

### One shared traversal driver

The recursive safe-walk is not re-implemented per tool. `rcp` (copy), `rchm`, and `rrm`
are thin [`WalkVisitor`](../common/src/walk_driver.rs) implementations; the single shared
driver in `common/src/walk_driver.rs` owns the recursive
spawn/classify/permit/drop-before-recurse skeleton, so the security-relevant invariants
each live in exactly one place:

- **Drop-before-recurse (deadlock invariant)**: the leaf-permit "drop the permit before
  recursing into a directory" rule lives in the driver's directory branch alone, not
  hand-maintained at every recursion site. A structural lint
  (`scripts/check-walk-driver-usage.sh`) fails the build if `copy.rs`/`chmod.rs`/`rm.rs`
  reintroduce a hand-rolled walk (a `JoinSet` or `read_entries`).
- **Trusted vs hardened boundary**: the symlink-following parent-prefix open returns a
  distinct `TrustedDir` type (`common/src/safedir.rs`); crossing below the named root
  yields a hardened `Dir` whose child opens are all `O_NOFOLLOW`. The boundary is
  type-enforced — a hardened child cannot be silently used where a trusted parent is
  required, and vice versa.
- **DT_UNKNOWN classification**: every filter `is_dir` decision routes through the single
  `walk::filter_is_dir` path, which falls back to an authoritative `fstat` when the
  `getdents` hint is `DT_UNKNOWN` (never following a symlink), so a real directory's
  subtree can never be silently omitted by a hint-only check.

`rlink` is the documented exception: it walks two correlated trees (source plus
`--update`) and so keeps its own dual-tree enumeration, but it shares the same substrate —
the `TrustedDir` boundary, the `LeafPermit` lifecycle, and `filter_is_dir` — rather than
duplicating the hardening.

### Scope

The following are fully TOCTOU-hardened on Linux, against both leaf-entry and
intermediate-directory symlink/path swaps:

| Tool / path | Notes |
|---|---|
| `rcp` local copy | Files, dirs, symlinks; all overwrite branches |
| `rlink` | Hard-link walk incl. copy delegations |
| `rchm` | Recursive chmod/chgrp/chown |
| `rrm` | Recursive remove incl. read-only-dir relax |
| `--delete` pruning | fd-relative prune; enumeration + removal via held dst fd |
| `rcp` remote copy — source side | Two-pass fd-map: dirs opened `O_NOFOLLOW`, files read fd-relative |
| `rcp` remote copy — destination side | Directory tracker fd-map: dirs created/opened `O_NOFOLLOW`, files written fd-relative |

Remote `--delete` is not yet supported and is rejected by rcp before any operation begins.

### Trusted boundary

The hardening protects everything **at or below the directory named on the command line**.
It assumes the named root itself and the path components above it are not modifiable by a
less-privileged actor. More precisely:

- For `rcp /backup/foo /dst`, the identity of `/backup/foo` at open time is trusted. Every
  entry strictly below it is hardened.
- The components between a fixed sudo prefix and the named operand (e.g., the `foo` part
  of `/backup/foo` where `/backup` is the fixed prefix and `foo` is actor-supplied) are
  resolved normally when opening the root. If those intermediate components are
  actor-writable, that is the operator's responsibility to prevent — typically by using a
  fixed source path in the sudo rule, not a wildcard.

The trust of these intermediate components — the prefix the tool follows to reach the named
root — is the **caller's responsibility** and is not verified in-tool. See
[Scope of TOCTOU safety](#scope-of-toctou-safety) for why this boundary is delegated to the
caller, and [The Linter](#the-linter---toctou-check-and---require-toctou-safe) below for what
`--require-toctou-safe` does enforce (the hardened walk, not the prefix).

## What Is Not Hardened

The following are **not TOCTOU-hardened** and are reported as "not safe" by
`--toctou-check`:

- **`--dereference` / `-L`**: Following symlinks is the requested behavior. A swapped link
  is followed by design. Do not use `-L` in privileged sudo rules over attacker-writable
  trees.
- **Non-Linux builds**: The hardened path (`safedir.rs` with `O_NOFOLLOW` + fd-relative
  ops) is Linux-only. macOS and other non-Linux platforms continue to use path-based
  operations and are not hardened.
- **`rcmp`** (read-only compare): `rcmp` cannot mis-permission or destroy files. A
  concurrent swap could cause a wrong comparison result (treating an unintended file as
  equal or unequal), but no data is written. This is accepted and `rcmp` is out of scope.

## Residual Preconditions

The fd-based mechanism is sound under two Linux kernel conditions that are enabled by
default:

- **`fs.protected_hardlinks=1`** (Linux default): With this setting disabled, an actor
  who can hardlink a sensitive file into the traversed tree defeats any userspace
  hardening — the privileged process opens a real regular file via `O_NOFOLLOW`,
  `fstat` confirms `S_ISREG`, and dev/ino checks pass, because the entry *is* that
  inode. This enables unauthorized reads, privileged chmod/chown of the aliased inode,
  and similar attacks (Scenario 4). No userspace scheme can defend against a missing
  `protected_hardlinks` guard. Verify with: `sysctl fs.protected_hardlinks`
- **`/proc` mounted**: Used by `rchm` for file chmod (via `/proc/self/fd/<n>`). Standard
  on all Linux distributions.
- **Actor cannot create bind mounts or manipulate mount namespaces**: Requires privilege;
  an unprivileged actor cannot exploit this.

## The Linter: --toctou-check and --require-toctou-safe

Every RCP tool supports two security flags:

### `--toctou-check`

Prints whether the invocation is TOCTOU-safe and exits without performing any operation.
Exit code 0 = safe, 1 = not safe.

```bash
# safe invocation
$ sudo rcp --toctou-check /backup/data /restore/
TOCTOU status: SAFE
  Note: Hardening assumes the directory named on the command line (and the path
  components above it) are not modifiable by a less-privileged actor; it protects
  everything at or below the named root. Also assumes fs.protected_hardlinks=1
  (Linux default).

# not safe: -L follows symlinks
$ sudo rcp --toctou-check -L /backup/data /restore/
TOCTOU status: NOT SAFE
  Reason: --dereference/-L follows symlinks by request, so a swapped link is
  followed — not hardened under privilege asymmetry
  Note: Hardening assumes the directory named on the command line (and the path
  components above it) are not modifiable by a less-privileged actor; it protects
  everything at or below the named root. Also assumes fs.protected_hardlinks=1
  (Linux default).
```

The "safe" verdict always includes a caveat reminding that the trusted-boundary
assumption (path components above the named root are not actor-writable) cannot be
statically verified from the invocation alone.

### `--require-toctou-safe`

Refuses to run unless the invocation uses the TOCTOU-hardened walk — that is, it refuses
`--dereference`/`-L` (which follows symlinks by request) and non-Linux builds. It is the
tool's half of the safe-privileged-use contract (see
[The contract for safe privileged (sudo) use](#the-contract-for-safe-privileged-sudo-use)).

```bash
# Refused: -L is not safe
$ sudo rcp --require-toctou-safe -L /backup/data /restore/
Refusing to run: invocation is not TOCTOU-safe.
  Reason: --dereference/-L follows symlinks by request ...
```

It does **not** verify the trust of the operand path's prefix — the directories above the
named root. That judgment requires policy context the tool does not have, so it is the
caller's responsibility (see [Scope of TOCTOU safety](#scope-of-toctou-safety)): the `sudo`
policy or a vetted wrapper must pass resolved operands whose prefix it trusts.
`--require-toctou-safe` operates purely on the verdict (`--dereference`/non-Linux) and never
inspects the operand paths, so it treats local and remote (`host:/path`) operands
identically.

### Recommended sudo rule pattern

To enforce that `rcp` only runs in a TOCTOU-safe configuration under a `sudo` rule:

```bash
# rcp: only allow TOCTOU-safe invocations
user ALL=(root) NOPASSWD: /usr/bin/rcp --require-toctou-safe *

# rchm: same pattern
user ALL=(root) NOPASSWD: /usr/bin/rchm --require-toctou-safe *

# rrm: same pattern
user ALL=(root) NOPASSWD: /usr/bin/rrm --require-toctou-safe *
```

With this rule, any attempt to invoke with `--dereference`/`-L` or on a non-Linux build
is rejected by the tool itself before any filesystem operation begins. The `*` wildcard
means the user can supply any additional arguments, but because the sudo rule lists
`--require-toctou-safe` as a literal token before the `*`, sudo only authorizes invocations
where it appears first — so the user cannot omit it. (The tool itself accepts the flag in any
position; it is the sudo prefix match, not the tool's argument parser, that pins it.) The flag
enforces only the tool's half (the hardened walk); it does not verify that an arbitrary operand
path is safe. Locking the paths down in the sudo rule IS the caller's prefix-trust decision (see
[Scope of TOCTOU safety](#scope-of-toctou-safety)), so restrict source and destination paths
wherever possible:

```bash
# Better: also lock down the paths
user ALL=(root) NOPASSWD: /usr/bin/rcp --require-toctou-safe /specific/source/* /specific/dest/
```

## Summary

| Aspect | Status |
|--------|--------|
| Symlink following (leaf) | Hardened (Linux): `O_NOFOLLOW` on every entry open |
| Intermediate directory swaps | Hardened (Linux): every dir opened fd-relative from parent |
| FIFO swap (DoS/side-effect) | Hardened (Linux): `O_NONBLOCK` + `fstat`+`S_ISREG` |
| Metadata ops (chown/chmod/utimes) | Hardened (Linux): fd-based, no path re-resolution |
| File data copy | Hardened (Linux): `copy_file_range` between held fds |
| `--delete` pruning | Hardened (Linux): fd-relative enumeration and removal |
| Remote copy (source side) | Hardened (Linux): two-pass dir-fd map |
| Remote copy (destination side) | Hardened (Linux): directory tracker fd-map |
| Remote `--delete` | Not supported (rejected before operation) |
| `--dereference` / `-L` | **Not hardened** (follows symlinks by design) |
| Non-Linux builds | **Not hardened** (path-based code, documented) |
| `rcmp` | Out of scope (read-only; no mis-permissioning possible) |
| _Which_ in-subtree file a swap makes us read | Out of scope — reads are not inode-pinned; a same-directory regular-file swap can change which file is read, but cannot escape the subtree or widen permissions (see [Scope of TOCTOU safety](#scope-of-toctou-safety)) |
| Prefix trust (path above the named root) | Caller's responsibility — out of scope, not verified in-tool (see [Scope of TOCTOU safety](#scope-of-toctou-safety)) |
| `fs.protected_hardlinks=0` | **Not defended** (userspace cannot close this gap) |

TOCTTOU vulnerabilities in rcp are **real but require local access** and specific
privilege configurations to exploit. On Linux, the default (non-`-L`) paths of all
write-capable tools are now fully hardened. Use `--require-toctou-safe` in sudo rules to
enforce safe invocations automatically.

## Further Reading

- [LWN: The difficulty of safe path traversal](https://lwn.net/Articles/1050887/)
- [openat2(2) man page](https://man7.org/linux/man-pages/man2/openat2.2.html)
- [CWE-367: TOCTTOU Race Condition](https://cwe.mitre.org/data/definitions/367.html)
- [CVE-2019-16884](https://nvd.nist.gov/vuln/detail/CVE-2019-16884) - runc symlink attack
