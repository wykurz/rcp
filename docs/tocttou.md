# TOCTTOU Vulnerabilities in the RCP tools

This document describes Time-of-Check-Time-of-Use (TOCTTOU) race condition vulnerabilities that
affect the RCP tools when used with elevated privileges, and documents the hardening that is now
implemented on Linux. The examples below use `rcp`, but the attack pattern applies to the other
tools as well.

## Table of Contents

- [Overview](#overview)
- [What is TOCTTOU?](#what-is-tocttou)
- [Attack Scenarios](#attack-scenarios)
- [Scope of TOCTOU safety](#scope-of-toctou-safety)
- [Implemented Hardening](#implemented-hardening)
- [What Is Not Hardened](#what-is-not-hardened)
- [Residual Preconditions](#residual-preconditions)
- [The Linter: --toctou-check and --require-toctou-safe](#the-linter---toctou-check-and---require-toctou-safe)
- [Safe privileged use (sudo)](#safe-privileged-use-sudo)
- [Summary](#summary)

## Overview

When rcp runs with elevated privileges (e.g., via sudo), an unprivileged attacker on the same system
may be able to exploit TOCTTOU race conditions to:

- Read files they shouldn't have access to
- Write to locations outside the intended destination
- Cause rcp to follow symlinks to sensitive locations

**Important**: These attacks require local access to the system and the ability to modify
files/directories in the path being copied. Remote network attackers cannot exploit these
vulnerabilities directly.

**The precise threat condition** is not merely "running as root": it is *running with more privilege
than an actor who can modify the paths being traversed.* Root operating on trusted, root-owned trees
is outside this threat; the risk arises specifically when a more-privileged run traverses a tree
that a less-privileged actor can write to.

On Linux, the local and remote default paths are now TOCTOU-hardened — for everything *at or below
the named root*. The trust of the path *above* the named root is the caller's responsibility.
[Scope of TOCTOU safety](#scope-of-toctou-safety) defines the guarantee precisely; read it before
relying on TOCTOU safety under elevated privilege.

## What is TOCTTOU?

TOCTTOU (Time-of-Check-Time-of-Use) is a class of race condition that occurs when:

1. A program **checks** some property of a file (e.g., is it a regular file?)
2. Time passes (even microseconds)
3. The program **uses** the file based on that check (e.g., opens and reads it)

Between steps 1 and 3, an attacker can swap the file for something else (typically a symlink),
causing the program to operate on a different file than intended.

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

These scenarios explain the classes of attack the hardening defends against. Each is now defeated on
Linux for the default (non-`-L`) path.

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

**Hardening**: The source open uses `openat(parent_fd, "myfile", O_RDONLY|O_NOFOLLOW|O_NONBLOCK)`. A
swapped-in symlink fails with `ELOOP`. A swapped-in FIFO is caught by the subsequent
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

**Hardening**: Destination directories are opened `O_NOFOLLOW|O_DIRECTORY` relative to the parent's
held fd. If the entry has been swapped to a symlink, `openat` fails with `ELOOP`. All file writes
are relative to that held fd — never re-resolving the path.

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

**Hardening**: Every entry name is opened with `openat(parent_dir_fd, name, O_NOFOLLOW)`, where
`parent_dir_fd` is the already-held directory fd opened the same way. A swapped symlink anywhere in
the tree causes a fail-closed `ELOOP` or `ENOTDIR` — never a redirect.

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

**Hardening**: All metadata operations (chown, chmod, utimes) use fd-based syscalls (`fchown`,
`fchmod`, fd-relative `utimensat`) on the held destination fd rather than path-based calls. There is
no path re-resolution after the file is created.

## Scope of TOCTOU safety

TOCTOU safety in the RCP tools rests on **two guarantees** and **one delegated responsibility**. The
scope is deliberately narrow, because a general-purpose copy/chmod/rm tool can neither freeze the
filesystem while it works nor vouch for the paths it is handed. So it guarantees only what is both
achievable and security-relevant, and leans on the caller for the rest.

### In scope — the two guarantees

On the default (non-`-L`) path, on Linux, given the operand paths as written:

**1. Containment.** No symlink or path-component swap *at or below a named root* — anywhere in the
tree the tool traverses — can redirect a read, write, chmod, chown, or delete to an object **outside
that root's subtree.** This is the property that matters when a privileged process operates on a
tree a less-privileged actor can write *into* (the classic "root copies, chmods, or removes over an
attacker-controlled directory" hazard). It is delivered by the fd-based safe walk described below:
the named root and every entry beneath it are opened `O_NOFOLLOW`, classified by `fstat` of the held
fd, and operated on via fd-relative syscalls — and for chmod/chown/hard-link, via the entry's pinned
`O_PATH` fd through `/proc/self/fd` — so a swapped-in symlink is never followed out of the subtree.

**2. Permission and ownership fidelity.** The destination (`rcp`/`rlink`) or the modified entry
(`rchm`) receives exactly the permissions and ownership of the source object that was actually read;
a concurrent swap can never make the tool **widen** permissions or attach the wrong owner — e.g. it
cannot write a `0600` root-owned file's contents out as world-readable. Mode and bytes are taken
from the *same* fd, and metadata is applied through the destination's own held fd, never re-resolved
by path.

**"The named root"** is the final component of the operand path — the file or directory you name. It
is opened `O_NOFOLLOW` and classified by the `fstat` of that held fd, so a swap of the root *entry
itself* (within its parent) is caught at open, and everything reachable beneath it is the hardened
tree. (Corollary: the root's immediate parent need not be non-writable for the guarantee to hold — a
swap of the root entry there is caught at open.)

### Out of scope — what we deliberately do not promise

**Freezing the tree, or pinning *which* object is operated on.** The tree may change concurrently;
the tool operates on whatever is validly reachable within the subtree at access time. Each child is
reached with a single-component `openat(parent_fd, name, O_NOFOLLOW)`, which re-resolves `name` — it
is **not** pinned to the exact inode first classified, so a same-name swap to *another regular file
in the same hardened directory* is possible and accepted. This is deliberate, not a gap, because it
is not a security boundary: an actor who can swap entries inside the subtree already controls that
subtree's contents, so operating on the swapped-in file grants them nothing they did not already
have. Both guarantees above still hold across such a swap — you cannot escape the subtree
(Containment), and permissions are never widened because mode and bytes come from the *same* fd
(Fidelity). We do not attempt to detect or prevent concurrent modification beyond that.

**Whether the operand path *itself* is trustworthy.** The directories *above* the named root — the
prefix the tool follows to reach it — are resolved normally (following symlinks). The tools do
**not** verify that this prefix is free of less-privileged control, or free of symlinks that resolve
somewhere unexpected. A general-purpose tool has no way to decide whether a given path is an
acceptable privileged target: *"is `/home/alice` a legitimate place for root to write?"* depends
entirely on policy the tool does not — and cannot — know. Any in-tool "is this prefix trusted"
heuristic is either unsound (it cannot anticipate every symlink, `..`, ownership, and
mount-namespace case) or so conservative it refuses almost every real privileged copy. So the tools
do not attempt it. **The caller must pass operands it has resolved and whose prefix it trusts.**

The guarantees are *additionally* bounded by separately-documented exceptions: `--dereference`/`-L`
and non-Linux builds (see [What Is Not Hardened](#what-is-not-hardened)), `rcmp` (read-only; out of
scope), and the kernel preconditions `fs.protected_hardlinks=1` and no attacker-controlled bind
mounts (see [Residual Preconditions](#residual-preconditions)). A hardlink alias planted at or below
the root, for instance, is a *non-swap* redirect covered by the `protected_hardlinks` precondition
rather than by the Containment guarantee.

## Implemented Hardening

### Mechanism: fd-based safe walk

The core principle is **never re-resolve a path**. The implementation (in `common/src/safedir.rs`)
holds each directory as an open fd (`Dir`, opened `O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`) and
derives each child via a single-component `openat(parent_fd, name, O_NOFOLLOW)`. The type of every
entry is determined by `fstat`-ing that fd — not from the `getdents` `d_type` field alone (which is
only a hint). Every operation on the child is performed on the held fd or via fd-relative `*at()`
syscalls.

Specific invariants enforced:

- Every `openat` of a non-dereferenced entry uses `O_NOFOLLOW`. A swapped-in symlink fails with
  `ELOOP` (fail closed).
- File opens include `O_NONBLOCK` to avoid blocking on a FIFO that an attacker swapped in. The
  subsequent `fstat`+`S_ISREG` check rejects any non-regular entry (FIFO, device, directory) with
  `EINVAL`.
- Metadata operations (chown, chmod, utimes, and critically symlink timestamps) use fd-based
  syscalls. File data is copied via `copy_file_range` between held fds.
- On overwrite paths, a `recheck` verifies that the `(dev, ino)` of the entry matches the originally
  classified handle before performing the unlink.
- Directory names passed to any `*at()` call are validated to be single path components (no `/`,
  `.`, `..`).
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

The recursive safe-walk is not re-implemented per tool. `rcp` (copy), `rchm`, and `rrm` are thin
[`WalkVisitor`](../common/src/walk_driver.rs) implementations; the single shared driver in
`common/src/walk_driver.rs` owns the recursive spawn/classify/permit/drop-before-recurse skeleton,
so the security-relevant invariants each live in exactly one place:

- **Drop-before-recurse (deadlock invariant)**: the leaf-permit "drop the permit before recursing
  into a directory" rule lives in the driver's directory branch alone, not hand-maintained at every
  recursion site. A structural lint (`scripts/check-walk-driver-usage.sh`) fails the build if
  `copy.rs`/`chmod.rs`/`rm.rs` reintroduce a hand-rolled walk (a `JoinSet` or `read_entries`).
- **Trusted vs hardened boundary**: the symlink-following parent-prefix open returns a distinct
  `TrustedDir` type (`common/src/safedir.rs`); crossing below the named root yields a hardened `Dir`
  whose child opens are all `O_NOFOLLOW`. The boundary is type-enforced — a hardened child cannot be
  silently used where a trusted parent is required, and vice versa.
- **DT_UNKNOWN classification**: every filter `is_dir` decision routes through the single
  `walk::filter_is_dir` path, which falls back to an authoritative `fstat` when the `getdents` hint
  is `DT_UNKNOWN` (never following a symlink), so a real directory's subtree can never be silently
  omitted by a hint-only check.

`rlink` is the documented exception: it walks two correlated trees (source plus `--update`) and so
keeps its own dual-tree enumeration, but it shares the same substrate — the `TrustedDir` boundary,
the `LeafPermit` lifecycle, and `filter_is_dir` — rather than duplicating the hardening.

### Scope

The following are fully TOCTOU-hardened on Linux, against both leaf-entry and intermediate-directory
symlink/path swaps:

| Tool / path                          | Notes                                                                                 |
| ------------------------------------ | ------------------------------------------------------------------------------------- |
| `rcp` local copy                     | Files, dirs, symlinks; all overwrite branches                                         |
| `rlink`                              | Hard-link walk incl. copy delegations                                                 |
| `rchm`                               | Recursive chmod/chgrp/chown                                                           |
| `rrm`                                | Recursive remove incl. read-only-dir relax                                            |
| `--delete` pruning                   | fd-relative prune; enumeration + removal via held dst fd                              |
| `rcp` remote copy — source side      | Two-pass fd-map: dirs opened `O_NOFOLLOW`, files read fd-relative                     |
| `rcp` remote copy — destination side | Directory tracker fd-map: dirs created/opened `O_NOFOLLOW`, files written fd-relative |

Remote `--delete` is not yet supported and is rejected by rcp before any operation begins.

### Trusted boundary

The hardening protects everything **at or below the directory named on the command line**; the
identity of the named root at open time is trusted, and the prefix above it is the caller's
responsibility (see [Scope of TOCTOU safety](#scope-of-toctou-safety) for why that judgment is
delegated). Concretely, for `rcp /backup/foo /dst` under a sudo rule that fixes `/backup` but lets
the caller supply `foo`: the components between the fixed prefix and the named operand are resolved
normally when opening the root, so keeping them out of a less-privileged actor's control is the
policy's job — typically by pinning the full path in the rule rather than using a wildcard.

## What Is Not Hardened

The following are **not TOCTOU-hardened** and are reported as "not safe" by `--toctou-check`:

- **`--dereference` / `-L`**: Following symlinks is the requested behavior. A swapped link is
  followed by design. Do not use `-L` in privileged sudo rules over attacker-writable trees.
- **Non-Linux builds**: The hardened path (`safedir.rs` with `O_NOFOLLOW` + fd-relative ops) is
  Linux-only. macOS and other non-Linux platforms continue to use path-based operations and are not
  hardened.
- **`rcmp`** (read-only compare): `rcmp` cannot mis-permission or destroy files. A concurrent swap
  could cause a wrong comparison result (treating an unintended file as equal or unequal), but no
  data is written. This is accepted and `rcmp` is out of scope.

## Residual Preconditions

The fd-based mechanism is sound under two Linux kernel conditions that are enabled by default:

- **`fs.protected_hardlinks=1`** (Linux default): With this setting disabled, an actor who can
  hardlink a sensitive file into the traversed tree defeats any userspace hardening — the privileged
  process opens a real regular file via `O_NOFOLLOW`, `fstat` confirms `S_ISREG`, and dev/ino checks
  pass, because the entry *is* that inode. This enables unauthorized reads, privileged chmod/chown
  of the aliased inode, and similar attacks (Scenario 4). No userspace scheme can defend against a
  missing `protected_hardlinks` guard. Verify with: `sysctl fs.protected_hardlinks`
- **`/proc` mounted**: Used by `rchm` for file chmod (via `/proc/self/fd/<n>`). Standard on all
  Linux distributions.
- **Actor cannot create bind mounts or manipulate mount namespaces**: Requires privilege; an
  unprivileged actor cannot exploit this.

## The Linter: --toctou-check and --require-toctou-safe

Every RCP tool supports two security flags:

### `--toctou-check`

Prints whether the invocation is TOCTOU-safe and exits without performing any operation. Exit code 0
= safe, 1 = not safe.

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

The "safe" verdict always includes a caveat reminding that the trusted-boundary assumption (path
components above the named root are not actor-writable) cannot be statically verified from the
invocation alone.

### `--require-toctou-safe`

Refuses to run unless the invocation uses the TOCTOU-hardened walk — that is, it refuses
`--dereference`/`-L` (which follows symlinks by request) and non-Linux builds.

```bash
# Refused: -L is not safe
$ sudo rcp --require-toctou-safe -L /backup/data /restore/
Refusing to run: invocation is not TOCTOU-safe.
  Reason: --dereference/-L follows symlinks by request ...
```

It does **not** verify the trust of the operand path's prefix — the directories above the named
root; that judgment requires policy context the tool does not have, so it remains the caller's
responsibility (see [Scope of TOCTOU safety](#scope-of-toctou-safety)). `--require-toctou-safe`
operates purely on the verdict (`--dereference`/non-Linux) and never inspects the operand paths, so
it treats local and remote (`host:/path`) operands identically.

<a id="the-contract-for-safe-privileged-sudo-use"></a>

## Safe privileged use (sudo)

Safe TOCTOU use under elevated privilege is a two-layer arrangement:

1. **A layer above the RCP tools** — the `sudo` policy, or a thin vetted wrapper — decides which
   operand paths are acceptable and ensures they are fully resolved and that the directories above
   each named root are not under a less-privileged actor's control. The "is this path safe?"
   judgment lives here, because this is where the policy context exists.
2. **The RCP tool** guarantees the hardened walk for everything at and below those named roots (the
   in-scope property of [Scope of TOCTOU safety](#scope-of-toctou-safety)).

`--require-toctou-safe` is the tool's half of this contract: it refuses to run unless the invocation
uses the hardened walk. It does **not**, and cannot, vouch for the trust of the operand paths; that
is the caller's responsibility per (1).

### Recommended sudo policy patterns

Pin `--require-toctou-safe` and exact, policy-approved operands for direct rules whenever the
operation is fixed:

```bash
# Exact paths keep the caller from choosing a different privileged target;
# --require-toctou-safe guarantees the hardened walk for what is at/below them.
user ALL=(root) NOPASSWD: /usr/bin/rcp --require-toctou-safe /vetted/source/snapshot /vetted/dest/
user ALL=(root) NOPASSWD: /usr/bin/rrm --require-toctou-safe /specific/staging/tree
```

A rule ending in `*` can pin the literal flag position, but it enforces only the tool's half of the
contract — hardened walk, no `-L` — while delegating every trailing option and operand accepted by
that tool. It does **not** make an arbitrary destination safe; do not treat it as a safe policy for
a mutating binary. Lock the paths down in the policy, or — when callers need a limited choice of
operations or operands — expose a vetted wrapper that allowlists those choices and constructs the
final command. For `rchm`, the wrapper also needs `--no-setid` and the controls in
[Set-ID suppression under sudo (`rchm`)](#set-id-suppression-under-sudo-rchm):

```bash
# The wrapper validates arguments instead of forwarding them to rchm.
user ALL=(root) NOPASSWD: /usr/local/sbin/safe-rchm *
```

### Name resolution under sudo (`rchm`)

`rchm --owner <name>` / `--group <name>` resolve a user/group *name* to a numeric id. When the
in-process lookup misses (the static musl release binaries have no NSS, so directory-service names
from LDAP/SSSD/NIS are invisible), `rchm` spawns the host `getent` tool (directly, via an argument
vector — never through a shell). Spawning a subprocess from a privileged process is a PATH-injection
surface: `sudo` preserves the caller's `PATH` unless the policy sets `secure_path`, so a `getent`
resolved through `PATH` could be an attacker-planted binary executed as root.

`rchm` closes this without depending on the sudoers configuration:

- **Privileged (effective-root):** `PATH` is **ignored**. `getent` is located only from a fixed list
  of trusted, root-owned directories (`/usr/bin`, `/bin`, `/run/current-system/sw/bin`). If it is
  not found there, the lookup errors rather than falling back to `PATH`.
- **`--getent-path <ABSOLUTE>`:** pins the exact binary, bypassing both `PATH` and the probe. Must
  be absolute (a relative path would re-introduce a `PATH`/cwd lookup) and may be given **at most
  once** — a duplicate is rejected, because a permissive trailing-wildcard policy (`... *`) would
  otherwise let an attacker append a second `--getent-path` to override a pinned value.
- **Numeric ids** (`--owner 1000`) never invoke `getent` at all — the safest option for a sudo rule
  when the resolving environment is untrusted.

```bash
# Pin the resolver and the complete operation so name lookups never consult the caller's PATH:
user ALL=(root) NOPASSWD: /usr/bin/rchm --require-toctou-safe --no-setid --group=data --getent-path=/usr/bin/getent /vetted/root
```

This is `rchm`-specific: `rcp`/`rlink` carry numeric ids from source metadata and never resolve
names.

### Set-ID suppression under sudo (`rchm`)

TOCTTOU hardening does not make an arbitrary privileged chmod/chown policy safe. In particular, an
ownership change can turn an attacker-controlled executable with a set-ID bit into a privileged
set-ID executable. The kernel normally clears set-user-ID and set-group-ID during `chown`, but
`rchm` normally restores existing bits so an ownership-only operation preserves the requested
metadata.

`--no-setid` provides a stronger contract for a constrained privileged wrapper: for every selected
non-symlink whose type has an applicable `--mode`, `--owner`, or `--group` rule, the entry's final
mode has set-user-ID (`04000`) and set-group-ID (`02000`) cleared. The guarantee has these
deliberate consequences:

- Existing set-ID bits are removed even if the mode expression does not mention them.
- Set-group-ID is removed from selected directories as well as files; sticky (`01000`) is
  unaffected.
- Filters and `f:`/`d:`/`l:` rules retain their normal scope. An entry that is filtered out, or
  whose type has no applicable operation rule, is not changed merely because the flag is present.
  Symlinks have no settable mode on Linux and are excluded from the guarantee.
- `--no-setid` alone is not an operation; at least one of `--mode`, `--owner`, or `--group` is still
  required.
- Omitting the flag preserves the existing behavior, including preservation of set-ID bits across
  ownership changes.

The guarantee describes the mode after `rchm` successfully completes its operation on an entry.
Clearing mode bits and changing ownership require separate syscalls; `rchm` does not freeze a
concurrent owner from changing the mode between them. In particular, a set-group-ID directory can
transiently carry that bit across a group change before the final masked chmod. A privileged wrapper
must not rely on `--no-setid` for ownership changes while an adversary can concurrently chmod the
selected inode or create entries in a selected directory; it must remove that concurrent control or
otherwise quiesce the tree for the operation.

This flag is **necessary but not sufficient** for a wrapper that delegates privileged `rchm`. Such a
wrapper must validate a small, explicit interface rather than pass arbitrary arguments through to
`rchm`:

- Resolve and restrict every operand to policy-approved roots whose path prefixes are trusted.
- Allowlist the exact numeric UIDs/GIDs and mode expressions the policy needs. Clearing set-ID does
  not prevent damage from arbitrary chown/chgrp, world-writable modes, or access to a sensitive
  target.
- Always add both `--require-toctou-safe` and `--no-setid`; do not let the caller remove or override
  them.
- Prefer numeric IDs. If names are required, supply a fixed trusted `--getent-path=/usr/bin/getent`
  (or another administrator-selected absolute path) rather than accepting the resolver path from the
  caller.
- Reject unrecognized options and additional operands. In particular, do not expose
  caller-controlled file-valued options: `--filter-file` reads a file as the privileged process, and
  `--auto-meta-histogram-log` creates or truncates its target. If the policy needs either, the
  wrapper must supply a fixed, trusted path.

An administrator-selected invocation inside a vetted `safe-rchm` wrapper might resemble:

```bash
/usr/bin/rchm --require-toctou-safe --no-setid \
  --owner=1000 --group=2000 --mode='f:0644 d:0755' -- /vetted/root
```

The wrapper must construct this command from validated values; it must not interpolate or forward a
caller-provided option string.

## Summary

| Aspect                                       | Status                                                                                                                                                                                                                  |
| -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Symlink following (leaf)                     | Hardened (Linux): `O_NOFOLLOW` on every entry open                                                                                                                                                                      |
| Intermediate directory swaps                 | Hardened (Linux): every dir opened fd-relative from parent                                                                                                                                                              |
| FIFO swap (DoS/side-effect)                  | Hardened (Linux): `O_NONBLOCK` + `fstat`+`S_ISREG`                                                                                                                                                                      |
| Metadata ops (chown/chmod/utimes)            | Hardened (Linux): fd-based, no path re-resolution                                                                                                                                                                       |
| File data copy                               | Hardened (Linux): `copy_file_range` between held fds                                                                                                                                                                    |
| `--delete` pruning                           | Hardened (Linux): fd-relative enumeration and removal                                                                                                                                                                   |
| Remote copy (source side)                    | Hardened (Linux): two-pass dir-fd map                                                                                                                                                                                   |
| Remote copy (destination side)               | Hardened (Linux): directory tracker fd-map                                                                                                                                                                              |
| Remote `--delete`                            | Not supported (rejected before operation)                                                                                                                                                                               |
| `--dereference` / `-L`                       | **Not hardened** (follows symlinks by design)                                                                                                                                                                           |
| Non-Linux builds                             | **Not hardened** (path-based code, documented)                                                                                                                                                                          |
| `rcmp`                                       | Out of scope (read-only; no mis-permissioning possible)                                                                                                                                                                 |
| *Which* in-subtree file a swap makes us read | Out of scope — reads are not inode-pinned; a same-directory regular-file swap can change which file is read, but cannot escape the subtree or widen permissions (see [Scope of TOCTOU safety](#scope-of-toctou-safety)) |
| Prefix trust (path above the named root)     | Caller's responsibility — out of scope, not verified in-tool (see [Scope of TOCTOU safety](#scope-of-toctou-safety))                                                                                                    |
| `fs.protected_hardlinks=0`                   | **Not defended** (userspace cannot close this gap)                                                                                                                                                                      |

TOCTTOU vulnerabilities in rcp are **real but require local access** and specific privilege
configurations to exploit. On Linux, the default (non-`-L`) paths of all write-capable tools are now
fully hardened. Use `--require-toctou-safe` in sudo rules to enforce safe invocations automatically.

## Further Reading

- [LWN: The difficulty of safe path traversal](https://lwn.net/Articles/1050887/)
- [openat2(2) man page](https://man7.org/linux/man-pages/man2/openat2.2.html)
- [CWE-367: TOCTTOU Race Condition](https://cwe.mitre.org/data/definitions/367.html)
- [CVE-2019-16884](https://nvd.nist.gov/vuln/detail/CVE-2019-16884) - runc symlink attack
