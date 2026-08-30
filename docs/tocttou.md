# TOCTTOU Vulnerabilities in the RCP tools

This document describes Time-of-Check-Time-of-Use (TOCTTOU) race condition vulnerabilities that
affect the mutating `rcp`, `rlink`, `rchm`, and `rrm` tools when used with elevated privileges, and
documents their Linux hardening. The examples below use `rcp`; the exclusions for the read-only
`rcmp` and path-based `filegen` tools are stated under
[What Is Not Hardened](#what-is-not-hardened).

## Table of Contents

- [Overview](#overview)
- [What is TOCTTOU?](#what-is-tocttou)
- [Attack Scenarios](#attack-scenarios)
- [Scope of TOCTOU safety](#scope-of-toctou-safety)
- [Implemented Hardening](#implemented-hardening)
- [What Is Not Hardened](#what-is-not-hardened)
- [Residual Preconditions](#residual-preconditions)
- [The Linter: --toctou-check and
  --require-toctou-safe](#the-linter---toctou-check-and---require-toctou-safe)
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

On Linux, the listed local tools and remote `rcp` default paths are TOCTOU-hardened — for everything
*at or below the named root*. The trust of the path *above* the named root is the caller's
responsibility; `--require-toctou-safe` additionally machine-checks every operand's form and
enforces symlink-free resolution whenever an operation consumes that operand (see
[The Linter](#the-linter---toctou-check-and---require-toctou-safe)).
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

These scenarios explain the classes of attack the hardening defends against. The default (non-`-L`)
Linux path defeats each one.

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
`parent_dir_fd` is the already-held directory fd opened the same way. A swapped symlink is either
opened and handled as the link object or rejected by a type-specific file/directory open; it is
never followed as a redirect.

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
`fchmod`, fd-relative `utimensat`) on a held destination fd rather than path-based calls. Newly
created symlinks are the narrow exception: when owner or timestamp preservation is requested, the
final component is reopened `O_PATH|O_NOFOLLOW`, its immutable target must match the intended
target, and metadata is applied through that same reopened fd.

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

**2. Permission and ownership fidelity for copied entries.** For an entry that `rcp`, or `rlink`'s
real-copy path, actually materializes, the destination permissions and ownership come from the
source object that was **actually read**, as selected by the `--preserve` policy in force — not
necessarily from a byte-for-byte copy of the source mode. What the policy selects is a separate
question from this guarantee: the default mask is `0o0777` (special bits stripped, like `cp`), and
ownership is preserved only when `uid`/`gid` are requested, so by default the destination is owned
by the copier. A concurrent swap cannot make the tool **widen** permissions beyond what the policy
selected for the source it read, or attach another object's owner — e.g. it cannot write a `0600`
root-owned file's contents out as world-readable. Regular-file mode and bytes come from the *same*
fd. Destination metadata mutation is fd-bound once its recipient is selected; newly created symlink
metadata uses the target-bound one-component reopen documented below rather than claiming the
created name still identifies one inode.

An `rlink` hard link is the pinned source inode itself, so it has no separate metadata-copy step.
`rchm` likewise has no source object: its analogous guarantee is that the requested mode/owner
transformation is applied through the classified entry's held descriptor. `rrm` and `--delete`
materialize no metadata and provide Containment only. An `--ignore-existing` or comparison skip does
not materialize an entry either; the untouched destination is outside the fidelity guarantee.

**Fidelity holds throughout the copy, not only at the end: destination files are created owner-only
until their contents are written.** A destination file is created at `0o600` and widened only after
the last byte, by the final `fchmod` or access-ACL application — the file counterpart of the
directory split-chmod below. Creating it at the final mode instead publishes the destination's
*audience* before its contents exist. At default settings that is an exposure of partial contents:
the mode mask is `0o0777` (`setuid`/`setgid`/sticky stripped, matching `cp`), so a world-readable
source yields a world-readable destination from creation onward, and anyone who can reach the
directory can read however much has landed. No symlink swap is needed — a world-searchable
destination directory is enough. Withholding the owner execute bit is deliberate as well — a
half-written executable should not be executable.

The escalation case (Scenario 4 above) explains why creation at the final mode is unsafe when
special bits are preserved (`--preserve`, or a `--preserve-settings` mask of `7777`) and the copier
is interrupted. Root holds `CAP_FSETID` — *"don't clear set-user-ID and set-group-ID mode bits when
a file is modified"* — so writing a destination created with the final special-bit mode would not
strip `S_ISUID`. A `SIGKILL`, OOM kill, or crash after the last byte could then close the write
descriptor and leave a complete, functional, setuid-root executable whose contents the *source's*
owner authored. While the descriptor remains open, `execve` refuses the file with `ETXTBSY`;
owner-only creation ensures interruption cannot publish that executable audience.

This is unconditional, not gated behind `--require-toctou-safe`: a file the copy creates is its own
new object with no prior user-visible state to preserve, exactly like a fresh directory. The mode is
not a parameter of the creating call, so no call site can opt out. One consequence is intended: a
copy that fails before the final widening step leaves a file readable only by its **owner**. Its
ordinary permission bits remain `0o600`; the access-ACL branch can first add requested special bits
with a narrowed `fchmod`, but they are inert because no execute bit is set. An interruption before
widening has the same result. Cancellation racing the final atomic widening syscall can leave either
the owner-only state or the correctly widened source audience, never a partially applied or broader
audience. "Owner", not "copier", because the chown runs first: a copy that preserves ownership and
gets past that step has already handed the file to the source's uid, so a later failure leaves it
owner-only under *that* uid rather than the copier's. (No exposure either way — the source's owner
authored the contents and could already read them, and only a privileged copier can chown to another
uid in the first place — but the file is not necessarily one the copier can still read.) Every
fallible preparatory metadata step precedes widening. Without a source access ACL the sequence is
chown → utimens → final `fchmod`; with one it is chown → utimens → narrowed owner-only `fchmod` →
final ACL application. The most likely instance is a copy that preserves ownership whose `fchown` is
refused — a non-root copier copying a file owned by someone else gets `EPERM` at the first step. The
failure is reported and the copy exits non-zero either way, and a later run *normally* re-copies the
file: the default `--overwrite-compare` is `size,mtime`, and a partial file differs in size.

That re-copy is not guaranteed, and the exceptions are worth stating precisely: a failed metadata
application can leave the destination owner-only until something re-copies it. There are two. **A
failure at the final widening step** (`fchmod`, or source access-ACL application): the timestamps
have already been applied by then, so the destination matches its source on both size and mtime and
is skipped. The failure both strands the destination owner-only and withholds permissions the copy
did not finish earning. **The nanosecond concession**: `metadata_equal` skips the nanosecond
comparison whenever *either* side's `mtime_nsec` is zero, for filesystems that do not store
sub-second timestamps (`common/src/filecmp.rs`), so even a destination that never reached `futimens`
— and so carries its own write time — compares **equal** to its source under the default
`size,mtime` (mode is not compared) exactly when two conditions hold together: that write and the
source mtime fall in the **same whole second**, **and** the nanosecond field is zero on **either**
side. Copying a file written in that same second reaches it, as does any source carrying a
whole-second mtime (`touch -d @<seconds>`, a tar extraction, a reproducible-build epoch) or a
destination filesystem without sub-second timestamps. Adding the mode to the comparison
(`--overwrite-compare=size,mtime,mode`) detects a stranded permission-widening failure, as does
removing the destination first. The implementation neither deletes the file on a metadata failure
nor includes mode in the default comparison; either policy would change overwrite semantics.

One sharp edge of that ordering is *not* transient: `--preserve-settings="f:time,7777"` asks for the
source's mode but not its ownership, and the `fchown` is issued only when `uid` or `gid` is
preserved — so a setuid source produces a destination that is **permanently** setuid *and* owned by
the copier, which for a root copier is a setuid-root binary whose contents the *source's* owner
authored. Nothing later narrows it: that is the requested outcome, not a window. It matches
`cp --preserve=mode`, and asking for a source's mode without its ownership is an explicit choice —
but preserve both or neither when the source tree is not yours.

**"The named root"** is the final component of the operand path — the file or directory you name.
The tool opens it `O_NOFOLLOW` and classifies it from that fd, producing a point-in-time kind and
metadata snapshot without following a symlink. Work performed through an opened file or directory fd
acts on that exact object. A later single-component, by-name syscall instead acts on the compatible
object occupying the root's slot in its pinned parent when the syscall runs. The root's immediate
parent need not be non-writable for containment: a replacement there cannot redirect resolution
outside that parent, but exact final-name identity is not promised.

### Out of scope — what we deliberately do not promise

**Freezing the tree, or pinning *which* object is operated on.** The tree may change concurrently;
the tool operates on whatever is validly reachable within the subtree at access time. An fd-based
operation remains bound to its opened object. A separate single-component syscall through a pinned
parent re-resolves the final name, however, and can read, remove, or descend into a compatible
replacement rather than the object an earlier classification observed. `unlinkat` accepts any
non-directory replacement, `rmdir` accepts an empty directory, and no-follow opens enforce the type
required by their flags. This is deliberate, not a gap, because an actor who can replace entries
inside the subtree already controls that subtree's contents. Both guarantees above still hold: the
operation cannot escape the parent (Containment), and copied permissions remain paired with the
payload fd (Fidelity). Strict operand resolution hardens the prefix and symlink rules; it does not
freeze final-name identity.

**Atomic replacement of a destination, or rollback of an interrupted one.** `rcp` copy semantics are
point-in-time and non-atomic. Under `--overwrite`, a file being replaced can be left truncated (the
copy died while writing data) or absent (it died between the removal and the create), and the
previous contents are not recoverable in either case. The removal is deliberately ordered *after*
the source open, so a copy that could never have produced a single byte — an unreadable or
swapped-away source — does not destroy what it was going to replace; but that is a
failure-*ordering* property, not cancellation safety. `Ctrl-C`, a `SIGKILL`, or a `--fail-early`
abort triggered by an unrelated file can still land in the gap, and the data copy that follows the
create is a far larger window than the gap itself. Closing this would mean staging every file under
a temporary name and renaming it into place, at a cost in throughput and in orphaned staging files
that `rcp` deliberately does not pay. This is not a security boundary: the invocation authorizes
replacement of destination slots. A compatible entry occupying one of those slots when removal runs
can be removed, including an entire directory subtree. The loss is confined to the destination
subtree, and no privilege or containment property above depends on it.

**Whether the operand path *itself* is trustworthy.** The directories *above* the named root — the
prefix the tool follows to reach it — are resolved normally (following symlinks). The tools do
**not** verify that this prefix is free of less-privileged control, or free of symlinks that resolve
somewhere unexpected. A general-purpose tool has no way to decide whether a given path is an
acceptable privileged target: *"is `/home/alice` a legitimate place for root to write?"* depends
entirely on policy the tool does not — and cannot — know. Any in-tool "is this prefix trusted"
heuristic is either unsound (it cannot anticipate every symlink, `..`, ownership, and
mount-namespace case) or so conservative it refuses almost every real privileged copy. So the tools
do not attempt it. **The caller must pass operands it has resolved and whose prefix it trusts.**

**Narrowed under `--require-toctou-safe`.** The strict mode cannot decide *policy* either — it still
cannot know whether `/home/alice` is a legitimate privileged target — but it machine-checks the
mechanical half of this responsibility. Every operand must be absolute and lexically normal (no
`.`/`..` components, no `//` segments; `realpath` output always qualifies), and every operand
root/parent open resolves with `openat2(RESOLVE_NO_SYMLINKS)`, so a symlink in any *directory*
component of an operand path fails closed with `ELOOP` at the open itself. The named entry itself is
never followed either: a symlink *operand* is handled as the link object (copied/removed/chowned as
a link, `ELOOP` where a directory is required), per the tools' existing non-`-L` semantics. A
wrapper that resolves and string-validates an operand therefore validates symlink-free resolution
into the intended parent/name slot: between its check and the tool's use, no symlink can be spliced
anywhere along the path. This does not freeze exact object identity; an actor with write access to a
path directory can rename a compatible object into the slot. What such a caller still owes is
*write*-control of the directories along the path — a prefix writer can substitute content they
could already write — plus the bind-mount and `protected_hardlinks` preconditions below.

**Reused destination directories are locked down under `--require-toctou-safe`.** A privileged copy
into an existing tree *reuses* a destination directory that is already present rather than
recreating it. Fresh directories are created copier-owned at `0o700` and only widened to the source
mode at the very end, but a reused directory would otherwise be copied into as-is — a pre-existing
`0o777` directory would expose the directory and every freshly written child for the whole subtree
copy, and stay exposed if the copy is interrupted. Under strict mode each reused directory is
therefore taken over by the copier before any child is written. The compatible directory selected by
`openat(O_NOFOLLOW|O_DIRECTORY)` is accepted, and its uid, gid, and mode are captured from that fd.
Ownership is changed only when the captured uid differs, and before chmod so the prior owner cannot
open the directory back up mid-copy; the directory is then `fchmod`ed to `0o700`. The takeover is
verified by re-stat of the same fd and fails closed if it did not actually land (a filesystem that
reports a successful `chown`/`chmod` without honoring it, e.g. CIFS without unix extensions). A
reused directory that had the setgid bit keeps it through the lockdown — children created during the
copy inherit the directory's group, and finalize cannot repair the group of a child already created
— so the group is also pinned to the value captured from the opened fd: taking uid ownership freezes
the group (after takeover, only the copier or root can change it), and if a prior owner raced a
`chgrp` into the takeover window the copier resets it, so a setgid directory cannot funnel freshly
written children into an attacker-chosen group. The directory's **default ACL** is snapshotted and
removed at the same point, for the same reason the mode is restricted: a `chmod` cannot reach it, so
children created during the copy would otherwise inherit it and be granted access beyond their
`mode` (its *access* ACL needs no strip — the `fchmod(0o700)` rewrites `ACL_MASK` to `---`, so every
named entry grants nothing for the duration). At finalize — after all children and any `--delete`
prune — the directory's original uid is restored when lockdown changed it and source uid
preservation is off; source metadata is then applied through the same fd. The final owner is the
source owner for each `--preserve`d component and the original owner otherwise, and the mode is the
same masked metadata an ordinary copy applies — the source directory's, or the update tree's for
`rlink --update`. Finalize does not re-stat this state: handing the final uid/mode back also gives
that owner the ability to race the observation or change the state immediately afterward, so the
check would not establish a durable safety or fidelity property. Two side effects are accepted, not
hidden. First, a reused directory whose processing is *aborted after lockdown* — by a `--fail-early`
abort, or by any per-directory error that returns before finalize (e.g. an enumeration failure) even
*without* `--fail-early` — is left no wider than a successful copy's result: the local path leaves
it *secured* (copier-owned at `0o700`), while the remote path may instead have already restored it
to its transparent final state (source mode with the original/source owner). The lockdown restricts
the mode to `0o700` immediately after taking ownership, and the takeover is VERIFIED (uid + exactly
`0o700`) before any further step, so on a filesystem that honors these syscalls any later failure
leaves the directory secured. Three exceptions still fail closed (no child written) but leave the
directory no narrower than requested — possibly its ORIGINAL, pre-lockdown mode, which may be wider
than the `0o700` a mid-copy directory holds (though never wider than the directory already was
before rcp ran): (1) the restricting `chmod` fails but the ownership rollback SUCCEEDS — the
directory is returned to its original owner and mode (the failed `chmod` changed nothing); (2) BOTH
the `chmod` and the rollback fail (a read-only or failing backend), reported with both errnos,
leaving it copier-owned at its original mode; and (3) a backend reports `chmod`/`chown` success
without taking effect (e.g. CIFS without unix extensions), so the verification fails. rcp cannot
force a non-honoring backend narrower and does not retain the directory's original mode to
re-restrict — and it does not chown a secured directory back to the prior owner (that would
re-widen) — so it reports the true observed owner/mode and leaves repair to the operator.
Restoration is likewise deliberately *not* forced onto the normal abort paths: doing so would
re-widen the directory (chown it back to the prior owner and re-apply the source mode) while its
children may be incomplete — the opposite of failing closed. On a honoring backend the secured
`0o700` is the outcome and no abort yields a wider directory than the mid-copy state; the exceptions
above only ever return the directory toward the state it already had. The directory's snapshotted
**default ACL** is the one thing an abort must not merely leave alone: the lockdown removed it and
holds the only copy of those bytes in memory, so losing it would be permanent data destruction
rather than a permission left narrow. Its restore therefore runs from an RAII guard rather than from
finalize alone, so every path that locks a directory and then never reaches finalize — a
`--fail-early` abort that drops in-flight siblings, a per-directory error, a remote destination that
fails between locking and registering — still puts it back. A failed restore there is logged with
the directory and the ACL bytes, since a destructor cannot report an error — but as an ordinary
`warn!`, so it needs `-v` to be seen (it is per-directory and unbounded on a mass abort, which is
exactly what disqualifies it from the always-visible notice channel). See
[acls.md](acls.md#--require-toctou-safe-containment). Second, an actor holding a directory fd opened
*before* the lockdown can still read the *names* of children written afterward (each child's
contents stay protected by its own source-derived mode). This is destination-only and
strict-mode-only; the default path leaves reused directories exactly as-is (their permissions may
block writing).

**Limitation — distinct source directories merging into one destination.** The lockdown coordinates
each reused directory's lifecycle per resolved destination path, and strict *multi-source* copies
are serialized, but sibling directories *within* one tree are still copied concurrently. Two source
directories can end up sharing one destination inode in two ways. **Explicitly**, when the operands
name it: `rcp A/x B/x dst/` maps both onto `dst/x`, and the default dispatch runs the operands
concurrently. **Implicitly**, when the destination filesystem folds or normalizes distinct source
names onto one inode (e.g. `Foo/` and `foo/` on a case-insensitive mount).

Either way, two concurrent tasks share one directory's lifecycle and each finalizes it
independently, with three consequences. A restore — widening the mode, or chowning back to the
original owner — can fire while the other task is still writing children, transiently reverting that
shared directory to its un-hardened exposure. A finalize that applies a non-writable source mode
(`0o555`, say) while the other task still has children to create fails *that* task with a reported
`EACCES`. And a nested directory the other task filtered out can be removed as locally empty before
it is populated.

None of these is a Containment or Fidelity break — children are still created
`O_CREAT|O_EXCL|O_NOFOLLOW` through the task's own held fd, and mode and bytes still come from the
same source fd. Concurrently merging two source directories into one destination inode is outside
this hardening. `--require-toctou-safe` rejects duplicate destinations outright, so it does not
reach the explicit case; the folding case is not detectable lexically, so it does. Closing this
fully requires coordinating every directory lifecycle — fresh and reused — by `(dev, ino)` across
the local, `rlink`, and remote engines and failing closed on an in-flight alias.

**Limitation — concurrent privileged invocations require a single writer.** The lockdown coordinates
each reused directory's lifecycle only WITHIN a single process (strict multi-source copies in one
invocation are serialized). It does NOT coordinate across SEPARATE processes: two concurrent
`rcp`/`rlink` invocations that reuse the SAME destination directory each independently lock it down
(chown to the copier, `chmod 0o700`) and later restore it, with no cross-process lock. One
invocation's restore — chowning back to the original owner and re-applying the source mode at
finalize — can therefore fire while the other is still writing children, transiently reverting the
shared directory to its un-hardened exposure. Note what an overlap does *not* cost: both guarantees
still hold for each invocation — children are created `O_CREAT|O_EXCL|O_NOFOLLOW` through the
invocation's own held fd and mode and bytes come from the same source fd, so neither Containment nor
Fidelity is affected. What is exposed is the shared directory's *interim* mode for the overlap
(which, being the copy's final source mode, can be wider than the `0o700` a mid-copy directory would
otherwise hold).

rcp does not take a cross-process inode lock, and the reason is not deadlock avoidance. A *blocking*
`flock` held from lockdown through finalize could deadlock if two walks took directories in
different orders, but a *non-blocking* one (fail closed on contention) cannot. The decisive problem
is that `flock` is **advisory and carries no holder identity**: in exactly the situation the
lockdown exists for — a reused destination directory an unprivileged actor can open — that actor can
take `LOCK_EX` and hold it indefinitely, turning every `--require-toctou-safe` copy into a hard
failure, and rcp cannot tell that holder from a peer copy. That trades a narrow, precondition-gated
exposure for a cheap, persistent, attacker-triggerable denial of service. Secondarily, `flock` is
unreliable on the shared destinations where this would matter (NFS emulates it via POSIX locks; some
FUSE backends make it node-local), and it coordinates only rcp against rcp — never against the
adversary — so the precondition would still have to be documented.

The precondition is therefore **single-writer** — do not run concurrent privileged copies into the
same destination subtree (the general expectation that a destination tree has one writer at a time).
Running a single invocation with multiple sources is safe (it is serialized in-process).

The guarantees are *additionally* bounded by separately-documented exceptions: `--dereference`/`-L`
and non-Linux builds (see [What Is Not Hardened](#what-is-not-hardened)), `rcmp` (read-only; out of
scope), and the kernel preconditions `fs.protected_hardlinks=1` and no attacker-controlled bind
mounts (see [Residual Preconditions](#residual-preconditions)). A hardlink alias planted at or below
the root, for instance, is a *non-swap* redirect covered by the `protected_hardlinks` precondition
rather than by the Containment guarantee.

## Implemented Hardening

### Validation must bind the action

A consistency check is TOCTOU hardening only when the checked object remains structurally bound to
the action. That is true when the action uses the same held descriptor or open file description,
when two already-open descriptors are compared and subsequent work uses the verified descriptor, or
when one kernel primitive makes the condition and mutation atomic.

It is not true of a `stat`/`fstatat`/`openat`/`(dev, ino)` observation followed by a separate
by-name syscall. Another replacement can occur before `unlinkat`, `rmdir`, rename, open, or chmod
consumes the name, so such a check only moves the race. The implementation does not use
opportunistic checks to claim that a by-name mutation fails closed or affects an exact inode.
Narrowing that interval or catching most swaps is not a security property: an attacker able to run
the race repeatedly can retry until the unguarded interval wins, so those checks do not justify
extra code, descriptors, or syscalls.

A held fd binds object identity, but not a mutable metadata postcondition once an actor regains
permission to change that inode. Such an actor can race a final `fstat` or change the state
immediately after it. Verification remains meaningful when it gates later fd-bound work while the
actor is excluded; an end-of-operation observation with no bound consumer is only probabilistic
detection and is omitted.

The same rule applies to advisory warnings and user-experience preflights. Prefer a notice derived
from settings or the authoritative action result over a raceable filesystem observation. Do not read
the filesystem merely to predict what a later open/create/remove might report when the observation
cannot be carried into that action.

This rule does not prohibit point-in-time observations for behavior that is explicitly
snapshot-based. Kind, size, and metadata snapshots drive filters, overwrite planning, dispatch, and
accounting; they describe what the tool observed, not necessarily the object a later by-name syscall
affects. The implementation does not spend another fd or metadata syscall merely to make an advisory
overwrite comparison race-coherent when a writer can replace the slot again before its later by-name
action. Nor does the rule prohibit an unconditional best-effort slot cleanup after a failed create:
that cleanup makes no identity claim and accepts that a compatible replacement may be removed while
an incompatible entry or non-empty directory may survive. If an operation requires one exact object,
the action must remain fd-bound, use an atomic kernel primitive, or be declined.

### Mechanism: fd-based safe walk

The core containment principle is **never re-resolve an untrusted multi-component path**. The
implementation (in `common/src/safedir.rs`) holds each directory as an open fd (`Dir`, opened
`O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`) and derives each child via a single-component
`openat(parent_fd, name, O_NOFOLLOW)`. The type of every entry is determined by `fstat`-ing that fd
— not from the `getdents` `d_type` field alone (which is only a hint). Object-specific work uses the
opened fd. Slot operations use one-component, fd-relative `*at()` syscalls and intentionally resolve
the final name when the syscall runs.

Specific invariants enforced:

- Every `openat` of a non-dereferenced entry uses `O_NOFOLLOW`. Classification uses
  `O_PATH|O_NOFOLLOW`, which opens and pins a swapped-in symlink as the link object rather than
  following it. Type-specific file or directory opens reject a symlink instead of traversing its
  target.
- File opens include `O_NONBLOCK` to avoid blocking on a FIFO that an attacker swapped in. The
  subsequent `fstat`+`S_ISREG` check rejects any non-regular entry (FIFO, device, directory) with
  `EINVAL`.
- Metadata operations (chown, chmod, utimes, and critically symlink timestamps) use fd-based
  syscalls. Local file transfer tries `copy_file_range` and uses sparse/dense fallbacks through the
  same held file fds; remote transfer streams between held source and destination files over the
  network.
- Destination `symlink_at` is create-only. On the newly-created, post-create path with no requested
  symlink owner/time metadata, creation performs no final-name reopen or stat. Otherwise
  `set_symlink_metadata_at` opens the current link `O_PATH|O_NOFOLLOW`, reads its immutable target
  through that handle, and applies metadata through the same handle only if the target matches. This
  target-to-recipient binding accepts a compatible same-target replacement and rejects a
  different-target replacement; it does not prove created or final-name identity. Overwrite
  comparison with an existing destination uses `read_symlink_owned`, which returns the target with
  the same metadata-recipient handle without an fd duplicate.
- **Destination objects are created narrow and widened last.** A new destination directory is
  created at `0o700` and a new destination file at `0o600`; the source mode is applied only once the
  object is complete (all children written, all bytes written). Nothing is ever reachable at its
  final — possibly setuid — mode before its contents exist; see
  [Scope of TOCTOU safety](#scope-of-toctou-safety). The file creation mode is a constant rather
  than a parameter of the creating call, so no call site can opt out of it.
- Local and remote `rcp` overwrite, `rlink` overwrite, `rrm`, and `--delete` removals operate
  fd-relatively through pinned parents and never follow a symlink. Each removal syscall enforces its
  own kind constraint; it can fail on an incompatible replacement or remove the compatible final
  component present when it runs. Recursive fallback classifies the current slot before dispatch, so
  its later syscall uses that admission snapshot. No `(dev, ino)` observation can bind a separate
  by-name mutation, and exact final-name identity is outside the guarantee. Successful leaf removal
  is accounted from its planning/admission snapshot: the reported kind and bytes are not an atomic
  audit of a concurrently replaced slot.
- Before recursive removal, the shared remover used by `rrm`, local/`rlink`/remote overwrite, and
  `--delete` opens the current directory in the classified name slot with `O_NOFOLLOW|O_DIRECTORY`,
  then enumerates and removes children through that fd. A compatible same-name directory replacement
  is accepted: the directory selected by that constrained open is authoritative, and exact identity
  relative to the earlier filter/kind snapshot is not part of the removal contract. Final removal is
  a single `rmdir` relative to the pinned parent, with no identity check before or after it. It
  removes only the empty directory occupying that name when the syscall runs, so a compatible
  replacement may be removed and the walked directory may survive if another process renamed it.
  Child totals describe successful operations through the walked fd; a successful final `rmdir` adds
  one directory for the name slot, and those totals need not describe one inode under a concurrent
  replacement. `--require-toctou-safe` strengthens operand resolution but does not add atomic
  final-name identity. In the `rrm` caller specifically, a directory temporarily changed to `0o700`
  is restored through the pinned inode only when filtering deliberately retains it (an include
  traversal, time-filter skip, or expected `ENOTEMPTY` while filtering). Operation errors,
  cancellation, and unexpected `rmdir` failures do not attempt metadata rollback for a removal in
  progress.
- Directory names passed to any `*at()` call are validated to be single path components (no `/`,
  `.`, `..`).
- **Source payload and metadata come from the same fd (read-side fidelity).** For each copied or
  sent source object, the data and the metadata applied/sent for it are read from one open file
  description, so a same-name swap cannot pair one inode's bytes/target with another inode's
  mode/owner/timestamps — or, under `acl`, with another inode's ACL, which is read from that same fd
  (`read_acls_fd` on a file's data fd, `Dir::read_acls` on the enumerated directory fd): a regular
  file via `open_file_read` → `(File, FileMeta)`; a symlink via the `O_PATH` handle's `read_symlink`
  (target + metadata off the one fd); a directory via the enumerated `Dir` fd (`read_entries` +
  `meta`). On the remote *destination*, a regular-file payload and header are applied to one created
  file fd, and directory metadata is applied through the held directory fd. Symlink metadata uses
  the target-bound reopened handle described above. `scripts/check-source-read-fidelity.sh` (run in
  CI) backstops this by forbidding by-name source-payload reads (`read_link_at`, `File::open`) in
  hardened modules outside the `-L`/`--dereference` path.

### Strict operand resolution (`--require-toctou-safe`)

The operand paths themselves — the one place the fd-based walk still consults multi-component paths
(`Dir::open_root_dir` for a named root, `Dir::open_parent_dir` for its container; both in
`common/src/safedir.rs`) — get a stricter contract when `--require-toctou-safe` is in force:

- **Lexical form.** Every operand must be absolute and lexically normal: no `.` or `..` components,
  no empty (`//`) segments; a single trailing slash is allowed (it carries copy-into meaning for
  destinations). `realpath` output always qualifies. This makes string-level operand policies
  (sudoers patterns, wrapper prefix checks) sound — `/vetted/../etc` cannot pass a `/vetted/*`
  check.
- **No-symlink resolution.** Both operand opens switch from plain `openat` to
  `openat2(RESOLVE_NO_SYMLINKS)` (which implies `RESOLVE_NO_MAGICLINKS`), enforced by the kernel at
  the open itself: a symlink in any directory component of the path fails closed with `ELOOP`. This
  closes the race between a wrapper's `realpath`+policy check and the tool's open. Requires Linux
  5.6+; on older kernels `--require-toctou-safe` refuses to run rather than degrade. The named entry
  itself keeps the tools' normal non-`-L` semantics: classified `O_NOFOLLOW`, a symlink operand is
  operated on as the link object and never followed (`ELOOP` where the open requires a directory,
  e.g. the `--delete` prune reopen).
- **Opened by the consumer, then threaded.** Strict resolution is part of each authoritative operand
  open, not a separate filesystem preflight. A source parent is opened before the source root is
  classified and filtered, and that fd is transferred into dispatch. Rlink opens and classifies an
  `--update` operand when the joint source/update decision consumes it. A destination parent is
  opened only when real destination work, or an explicit dry-run feature such as `--ignore-existing`
  or `--delete`, needs it; the same fd is then used by that work. A filtered root or a dry run with
  no destination-dependent decision does not open an otherwise unused destination prefix. This is
  intentional: an up-front observation would neither bind a later action nor improve safety, and
  local and remote copies follow the same rule. The `--delete` prune preview can reopen a
  destination directory below the named root with `RESOLVE_NO_SYMLINKS`; if a symlink makes that
  preview inapplicable, it is skipped because the real run would create or replace that slot.
- **Remote operands.** The master lints the remote path parts (which must be absolute as written;
  `host:~/x` forms are rejected) and mirrors the flag onto each spawned `rcpd`, which arms the same
  strict resolution for its root opens on the remote host. The source `rcpd` opens the source parent
  before its root filter; the destination `rcpd` learns the destination path only from the source's
  directory/symlink messages (its own `MasterHello::Destination` carries no path), and opens the
  destination parent when it processes the root — so a symlinked prefix fails closed whenever an
  operand is actually accessed. **Residual:** whenever the destination `rcpd` receives no create
  message — a remote **`--dry-run`** (which writes nothing by design), a copy whose **source root is
  entirely filtered out**, or a **skipped root special** — it never opens (hence never validates)
  its destination parent. Nothing is written through the prefix, so it is benign, but unlike a local
  operation it does not separately fail closed on a symlinked destination prefix. The protocol does
  not hand the destination operand to the destination `rcpd` up front; neither the master nor the
  source can validate a path on the destination host, so this no-message case has no
  destination-side prefix open.

The switch is process-global, armed once by the linter before any filesystem work. Without the flag,
normal operand resolution applies. A consumed source under an execute-only (`0111`) parent can fail
with `EACCES` under strict mode because the retained parent fd is opened for reading; a source that
a cheap default-mode path stat can exclude may skip that open. Unused destination prefixes are not
opened in either local or remote strict operation. Systems whose legitimate paths cross symlinks
(`/home → /var/home`, automounts) must pass the resolved form whenever those paths are consumed —
that is the point, and it is why this is opt-in rather than the default.

### One shared traversal driver

The recursive safe-walk is not re-implemented per tool. `rcp` (copy), `rchm`, and `rrm` are thin
[`WalkVisitor`](../common/src/walk_driver.rs) implementations; the single shared driver in
`common/src/walk_driver.rs` owns the recursive spawn/classify/permit/drop-before-recurse skeleton,
so the security-relevant invariants each live in exactly one place:

- **Drop-before-recurse (deadlock invariant)**: the "release provisional leaf admission before
  recursing into a directory" rule lives in the driver's directory branch alone, not hand-maintained
  at every recursion site. The inner admission scope ends before `dir_pre` and descent, restoring
  any outer pool before nested work. A structural lint (`scripts/check-walk-driver-usage.sh`) fails
  the build if `copy.rs`/`chmod.rs`/`rm.rs` reintroduce a hand-rolled walk (a `JoinSet` or
  `read_entries`).
- **Admission scheduling (descriptor invariant)**: a reliable `getdents` type hint lets an ordinary
  source walk make a cheap filter decision before admission. A `DT_UNKNOWN` entry with an active
  filter acquires before spawn and is classified in its scheduled worker; an included unknown or
  known non-directory otherwise acquires immediately before spawn. A positive directory hint
  normally takes no leaf admission. For destructive or observable shared-walk filter decisions, the
  worker owns one exact classification, applies the filter to that entry, and transfers the result
  into dispatch. A skipped result has no processed/keep outcome; a selected result contributes its
  exact outcome in directory-enumeration order. This prevents stale hints from authorizing
  remove/delete work or destination protection. In rlink, `DT_UNKNOWN` source and update-only hints
  also dispatch to scheduled workers for exact type-sensitive filtering and accounting. Its
  dual-tree path admits before opening either source or update handle when an update counterpart
  needs classification, because the source hint says nothing about that separate entry. An
  authoritative directory releases any provisional permit inside the inner scope before `dir_pre` or
  descent. This bounds fd-bearing leaf fan-out without putting arbitrary recursive directory
  depth/breadth into a pool, which would recreate the deep-directory hold-and-wait deadlock.
- **Checked entry ownership**: `AdmittedEntry` binds an authoritative destructive-filter decision to
  dispatch without a second name lookup; only a checked non-directory handle can then construct
  `AdmittedLeaf`, so a directory-as-leaf state is unrepresentable in release builds. Their explicit
  `Drop` implementations close classification handles before releasing permits on every destructing
  exit, including success, error, early return, panic unwind, and cancellation. The directory
  transition is centralized in the driver and releases provisional leaf admission before recursion.
  The canonical safedir blocking runner attempts to upgrade any live ambient weak admission
  reference; if none is live, it adds no lease. If its async waiter is abandoned after an upgrade,
  any descriptor-bearing output drops before that lease.
- **Root and delegation scope**: local copy/rm/chmod and rlink root setup acquire before their
  fd-bearing parent/classification work. Delegated shared-driver and rlink entries either ensure or
  transfer admission before final classification. This statement does not cover every remote rcpd
  parent/root open; remote source and destination have separate protocol-specific setup.
- **Independent pools and recursive overwrite**: the runtime normally intersects the file-work
  ceiling with the internal soft-`RLIMIT_NOFILE` descriptor ceiling independently for the OpenFile
  and PendingMeta semaphores. If that query fails, only a finite user-supplied ceiling can recover;
  it becomes the sole admission bound and produces a visible warning. Automatic admission and legacy
  unlimited admission fail closed instead; a successful query returning a zero soft limit also fails
  closed for every policy. The pools receive the same effective numerical ceiling, not one combined
  total. Copy/link overwrite leaf work can retain OpenFile admission while recursively invoking rm,
  which draws from PendingMeta. Every OpenFile permit is budgeted for three descriptors. Transfer
  overlaps the source classification, source data, and destination data fds; the data closure drops
  the source data fd before returning the destination fd for metadata, whose blocking helper then
  overlaps the source classification, destination data fd, and one duplicate. Every concurrently
  available PendingMeta permit contributes one classification descriptor. The four-unit heuristic
  therefore covers the independent pools as three times the OpenFile limit plus one times the
  PendingMeta limit, including tasks in different phases. Metadata-only tools can transiently hold
  two descriptors per PendingMeta operation but do not use the three-descriptor OpenFile path
  concurrently. The OpenFile → PendingMeta call is the only direction between those pools;
  permission-relaxed and recursive directory handles and process-support descriptors remain outside
  them. The static `--max-files-in-flight` ceiling therefore limits applicable file-like work rather
  than every process descriptor or all possible concurrent activity.
- **Trusted vs hardened boundary**: the symlink-following parent-prefix open returns a distinct
  `TrustedDir` type (`common/src/safedir.rs`); crossing below the named root yields a hardened `Dir`
  whose child opens are all `O_NOFOLLOW`. The boundary is type-enforced — a hardened child cannot be
  silently used where a trusted parent is required, and vice versa.
- **Filter-classification errors**: ordinary hardened shared walks and rlink's `DT_UNKNOWN` source
  and update-only paths classify entries in their scheduled workers and fold those exact outcomes.
  This worker-owned rule is scoped to paths requiring type-sensitive classification; reliable
  terminal hint exclusions can still complete in the producer. Destructive remove/delete filtering
  uses the authoritative kind in its transferred `AdmittedEntry`. None of these paths follows a
  symlink, and an error propagates rather than becoming a cheap non-directory result. The path-based
  rcmp walk remains a read-only exception: a failed `DirEntry::file_type` falls back to
  non-directory for filtering. That fallback can produce an inaccurate comparison result, but it
  authorizes no filesystem mutation.
- **Copy planning**: after destination-only preflight, local regular-file copies use metadata from
  the admitted `O_PATH` handle as a point-in-time snapshot for dry-run accounting and the initial
  destination comparison. Dry runs and identical/newer decisions return without an `O_RDONLY`
  payload open. Only an actionable `Vacant` or `Replace` plan opens the current payload; if its
  `(dev, ino)` differs from the admitted snapshot, the overwrite candidate is recomputed from the
  payload fd's paired metadata before any mutation. A metadata-only skip may therefore accept a
  later same-parent, same-type replacement. Exact inode freezing remains out of scope.
- **Cancellation boundary**: repository-owned blocking jobs routed through the canonical safedir
  runner attempt to upgrade a live ambient weak admission reference and retain it when present. If
  the async waiter is cancelled after an upgrade, work that has not started is discarded; for work
  whose blocking job already won the start/cancel race, an abandoned descriptor-bearing output drops
  before the strong lease. Fail-early task scopes wait for every recursively spawned **async**
  descendant to finish dropping, but do not await an already-started blocking job: the job keeps its
  lease while the operation returns and may still contain later filesystem calls, leaving the
  runtime's bounded shutdown able to abandon work stuck indefinitely on dead storage. With no live
  ambient admission, the runner adds no lease. Local copy's synchronous data move and filegen's
  one-buffer synchronous chunks use this boundary. A weak scope alone is passive: admitted remote
  source/destination payload-leaf streaming still uses `tokio::fs::File`, whose private blocking
  jobs can retain an `Arc<StdFile>` owning the same fd after high-level cancellation without
  inheriting admission. The fd is retained, not cloned or duplicated. Covering this payload-leaf
  residual requires a bounded remote-I/O abstraction; the existing weak scope is not a claim about
  every Tokio filesystem operation and adds no wire-protocol state.

`rlink` is the documented exception: it walks two correlated trees (source plus `--update`) and so
keeps its own dual-tree enumeration, but it shares the same substrate — the `TrustedDir` boundary,
explicit entry admission, and checked leaf ownership — rather than duplicating the hardening. Its
`DT_UNKNOWN` source and update-only entries are classified in their scheduled workers; reliable
terminal hint exclusions remain a producer-side fast path.

### Scope

The following Linux paths provide Containment against leaf-entry and intermediate-directory
symlink/path swaps. Fidelity applies only where the row materializes or changes metadata. This table
does not add an exact final-name identity guarantee: fd-bound work uses its opened object, while a
by-name slot operation can affect a compatible replacement in its pinned parent.

| Tool / path                          | Containment mechanism                                | Fidelity / mutation binding                                    |
| ------------------------------------ | ---------------------------------------------------- | -------------------------------------------------------------- |
| `rcp` local copy                     | Fd walk; overwrite removes through a pinned parent   | Materialized payload and metadata use paired held fds          |
| `rlink`                              | Pinned-source hard links; contained overwrite/copy   | Hard link is source inode; delegated copies use `rcp` fidelity |
| `rchm`                               | Recursive fd walk                                    | Requested changes apply through the classified entry fd        |
| `rrm`                                | Fd-bound descent; contained leaf/final-slot removal  | Not applicable                                                 |
| `--delete` pruning                   | Enumeration/removal through the held destination dir | Not applicable                                                 |
| `rcp` remote copy — source side      | Two-pass fd map with `O_NOFOLLOW` directories        | Sent payload and metadata come from one held source object     |
| `rcp` remote copy — destination side | Directory fd map and contained slot replacement      | Transferred payload/metadata use bound fds; skips are N/A      |

Remote `--delete` is unsupported and rejected by rcp before any operation begins.

### Trusted boundary

The hardening protects operations **at or below the directory named on the command line**; the
prefix above that root is the caller's responsibility by default (see
[Scope of TOCTOU safety](#scope-of-toctou-safety) for why that judgment is delegated). The no-follow
root classification is a snapshot, not a freeze of the final name. Concretely, for
`rcp /backup/foo /dst` under a sudo rule that fixes `/backup` but lets the caller supply `foo`: the
components between the fixed prefix and the named operand are resolved normally when opening the
root, so keeping them out of a less-privileged actor's control is the policy's job — typically by
pinning the full path in the rule rather than using a wildcard.

Under `--require-toctou-safe` the boundary tightens: operands must be absolute and lexically normal,
and the full operand path — prefix included — is opened `RESOLVE_NO_SYMLINKS`, so the "resolved
normally" clause above does not apply; a symlink between the fixed prefix and the named operand
fails closed instead of being followed. See
[Strict operand resolution](#strict-operand-resolution---require-toctou-safe).

## What Is Not Hardened

The following are **not TOCTOU-hardened**, or — for POSIX ACLs, which are a fidelity and containment
concern rather than a race — not covered by default. The flag- and platform-level items (`-L`,
non-Linux) are reported as "not safe" by `--toctou-check`; the runtime filesystem-property item
(POSIX ACLs) is NOT — `--toctou-check` inspects operand form and flags, not the runtime state of the
filesystems involved, so it cannot detect it:

- **`--dereference` / `-L`**: Following symlinks is the requested behavior. A swapped link is
  followed by design. Do not use `-L` in privileged sudo rules over attacker-writable trees.
- **Non-Linux builds**: The hardened path (`safedir.rs` with `O_NOFOLLOW` + fd-relative ops) is
  Linux-only. macOS and other non-Linux platforms continue to use path-based operations and are not
  hardened.
- **`rcmp`** (read-only compare): `rcmp` cannot mis-permission or destroy files. A concurrent swap
  could cause a wrong comparison result (treating an unintended file as equal or unequal), but no
  data is written. This is accepted and `rcmp` is out of scope.
- **`filegen`** (test-data generator): `filegen` creates directories and files through path-based
  APIs and exposes neither security flag below. It is not hardened for privileged operation on an
  attacker-writable tree.
- **POSIX ACLs — the source's are not preserved unless you ask.** rcp does process POSIX ACLs, both
  locally and across the remote transport, but only when `--preserve-settings` requests `acl`
  (`all+acl`, or a per-type `f:acl`/`d:acl`). Detecting an ACL costs a syscall on every entry —
  there is no bit in `stat` for it — so the flag people reach for by default deliberately does not
  pay it. A copy that does not request `acl` therefore does not reproduce the source ACL. A newly
  created destination can instead inherit an ACL from its parent unless strict mode contains that
  inheritance. Without `d:acl`, a reused directory retains its destination access ACL; with `d:acl`,
  finalize replaces or clears it from the source. A source ACL entry narrower than `other` acts as a
  deny in effect, so dropping the source ACL can grant exactly what the source withheld.
  **`--require-toctou-safe` does not close this source-fidelity gap**; see the table below. The full
  model, both widening directions and the measured costs are in [acls.md](acls.md).

  The strict mode contains inherited destination ACLs at three creation/reuse sites. No destination
  entry rcp **creates and successfully completes** carries an ACL entry that did not come from its
  source. A *reused* directory is outside that created-entry invariant: it keeps its destination
  access ACL present but masked during lockdown, while its default ACL is suppressed so new children
  cannot inherit it. Successful finalize leaves those destination ACLs in place when `d:acl` is off,
  or replaces/clears both ACLs according to the source when `d:acl` is on.

  - Every directory rcp successfully creates has both its ACLs removed after the `mkdirat`, so
    nothing created beneath it can inherit one. Two syscalls per directory and none per file:
    stripping the default ACL stops the inheritance chain for the whole subtree. The window between
    `mkdirat` and the strip is not exploitable under the parent write-control and single-writer
    preconditions: no protected actor can replace the slot, the directory is at `0o700`, and the
    kernel intersects inherited entries with the create mode, leaving `m::---`, so every named entry
    grants nothing.
  - Every **reused** directory has its DEFAULT ACL snapshotted and removed for the copy's duration.
    An RAII guard restores it if the copy aborts; successful finalize restores it when `d:acl` is
    off or replaces/clears it from the source when `d:acl` is on. The **access** ACL is not
    stripped: the lockdown's `fchmod(0o700)` already neuters it, because on Linux `chmod` rewrites
    the `ACL_MASK` entry from the new group bits, so `0o700` sets the mask to `---` and every named
    `user:`/`group:` entry — and the owning group — grants nothing for the duration. With `d:acl`
    off, finalize `chmod` re-derives the mask from the requested mode; with `d:acl` on, the source
    access ACL replaces or clears the destination ACL. Removing the **default** ACL is what the
    `chmod` could not do for itself: without the strip, children created during the copy would
    inherit it and could gain access beyond their `mode`.
  - A direct file created in the ambient operand parent has its inherited access ACL removed in its
    creation closure. Files under created or locked directories need no per-file strip because their
    parent cannot pass an ACL on.

  If a create re-open or ACL strip fails, rcp reports that error before writing children/data or
  applying final metadata and attempts one unconditional, fd-relative cleanup of the current slot.
  This best-effort cleanup makes no inode-identity claim: it can remove a compatible replacement,
  while an incompatible entry, a non-empty directory, or a cleanup error can leave the slot
  occupied. An unsanitized file/directory rcp created remains owner-only with inherited access
  entries masked off and is never widened or descended into. See
  [acls.md](acls.md#--require-toctou-safe-containment) for the complete contract.

  Consequence worth knowing: under `--require-toctou-safe` **without** `d:acl`, a freshly created
  destination directory ends with no ACL even where its parent's default ACL would ordinarily have
  given it one. That is the intended trade — the flag's contract is that the destination reflects
  its source and nothing else, so containment beats inheritance — and `d:acl` is the escape. See
  [acls.md](acls.md#a-consequence-not-an-oversight).

  Because an ACL is a property of the filesystem at runtime (not of the operand path or flags),
  `--toctou-check` does not — and cannot — detect or report any of this.

  For entries rcp creates and successfully completes:

  |                               | dropped SOURCE ACL | inherited DESTINATION ACL     |
  | ----------------------------- | ------------------ | ----------------------------- |
  | default                       | open               | open                          |
  | `--preserve-settings=all+acl` | closed             | closed, repaired at finalize  |
  | `--require-toctou-safe`       | **open**           | closed, prevented at creation |
  | both                          | closed             | closed, prevented at creation |

  **Pair `--require-toctou-safe` with `--preserve-settings=all+acl`** where the source's ACLs are
  security-relevant. The flags close different bugs and deliberately do not imply each other:
  auto-enabling `acl` would impose per-entry ACL reads on a flag people reach for a different
  reason, and would silently override an explicit `--preserve-settings`. A syscall-free notice
  derived from the selected settings warns that source entries may carry ACLs the copy will omit; it
  does not make an opportunistic root observation.

## Residual Preconditions

The mechanism is sound given the following, none of which the tools can enforce themselves. The
first two are Linux kernel conditions that hold by default; the third is a privilege assumption; the
fourth is an operational one the caller owes:

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
- **A destination subtree has a single writer**: The reused-directory lockdown under
  `--require-toctou-safe` coordinates each directory's lifecycle only *within* one process. Do not
  run concurrent privileged copies into the same destination subtree — see
  [Limitation — concurrent privileged invocations](#out-of-scope--what-we-deliberately-do-not-promise)
  for what overlaps, and why rcp does not take a cross-process lock. A single invocation with
  multiple sources is safe (strict multi-source copies are serialized in-process).

## The Linter: --toctou-check and --require-toctou-safe

The mutating `rcp`, `rlink`, `rchm`, and `rrm` tools support two security flags. The read-only
`rcmp` and path-based `filegen` tools do not:

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

`--toctou-check` also emits a `Note:` for every operand that `--require-toctou-safe` would refuse
(non-absolute or non-normal form, or a kernel without `openat2`), without changing the exit code —
useful when preparing a wrapper or sudo rule for the strict mode.

### `--require-toctou-safe`

Refuses to run unless the invocation uses the TOCTOU-hardened walk **and** satisfies the strict
operand contract:

- refuses `--dereference`/`-L` (follows symlinks by request) and non-Linux builds;
- refuses any operand that is not absolute and lexically normal (no `.`/`..` components, no `//`
  segments; a single trailing slash is allowed — `realpath` output always qualifies);
- refuses kernels without `openat2(2)` (Linux 5.6+);
- and, having passed those checks, resolves every operand root/parent open with
  `openat2(RESOLVE_NO_SYMLINKS)`, so a symlink in any directory component of an operand path fails
  closed with `ELOOP` at use time (a symlink *operand* is never followed either — it is operated on
  as the link object, per the tools' non-`-L` semantics).

```bash
# Refused: -L is not safe
$ sudo rcp --require-toctou-safe -L /backup/data /restore/
Refusing to run: invocation is not TOCTOU-safe.
  Reason: --dereference/-L follows symlinks by request ...

# Refused: operand is not in strict form
$ sudo rcp --require-toctou-safe ./data /restore/out
Refusing to run: invocation is not TOCTOU-safe.
  Reason: operand "./data" is not absolute; --require-toctou-safe requires absolute, ...

# Fails closed (ELOOP) at the open if any directory component is — or becomes — a symlink
$ sudo rcp --require-toctou-safe /backup/linkdir/data /restore/out
```

What it still does **not** decide is *policy*: whether a given absolute path is an acceptable
privileged target remains the caller's judgment, and the directories along the operand paths must
still be kept out of a less-privileged actor's *write* control (a prefix-writer can rename real
directories, substituting content they could already place — though never redirect through a
symlink). See [Scope of TOCTOU safety](#scope-of-toctou-safety). Local and remote (`host:/path`)
operands are held to the same contract; the flag is mirrored to each `rcpd`.

<a id="the-contract-for-safe-privileged-sudo-use"></a>

## Safe privileged use (sudo)

Safe TOCTOU use under elevated privilege is a two-layer arrangement:

1. **A layer above the RCP tools** — the `sudo` policy, or a thin vetted wrapper — decides which
   operand paths are acceptable (typically: `realpath` the operand, then match the result against
   the policy's allowlist) and ensures the directories along each path are not under a
   less-privileged actor's *write* control. The "is this path safe?" judgment lives here, because
   this is where the policy context exists.
2. **The RCP tool** guarantees the hardened walk for everything at and below those named roots (the
   in-scope property of [Scope of TOCTOU safety](#scope-of-toctou-safety)).

`--require-toctou-safe` is the tool's half of this contract, and it also verifies the mechanical
half of (1): it refuses non-hardened invocations, requires an absolute and lexically unambiguous
operand spelling, and resolves it with `RESOLVE_NO_SYMLINKS`. The wrapper-validated spelling
therefore reaches the symlink-free parent/name slot that exists at open time. This does not pin the
identity of real prefix directories across renames or freeze the object in the final slot. The tool
also cannot make the *policy* judgment — which paths are acceptable — so write control of prefix
directories remains the caller's responsibility per (1).

### Recommended sudo policy patterns

Pin `--require-toctou-safe` and exact, policy-approved operands for direct rules whenever the
operation is fixed:

```bash
# Exact paths keep the caller from choosing a different privileged target;
# --require-toctou-safe guarantees the hardened walk for what is at/below them.
user ALL=(root) NOPASSWD: /usr/bin/rcp --require-toctou-safe /vetted/source/snapshot /vetted/dest/
user ALL=(root) NOPASSWD: /usr/bin/rrm --require-toctou-safe /specific/staging/tree
```

Note what a rule like this does *not* constrain: how many times the caller runs it, or when. Nothing
above stops two of these invocations overlapping, and the reused-directory lockdown assumes a
[single writer per destination subtree](#residual-preconditions). Where a policy grants a mutating
rule over a shared destination, the wrapper — not rcp — must serialize it.

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
  of trusted, root-owned directories (`/usr/bin`, `/bin`, `/run/current-system/sw/bin`). Each fixed
  pathname is executed directly; no separate `stat` predicts a later exec. An exec attempt that
  returns `ENOENT`/`ENOTDIR` falls through to the next pathname. Those codes can also mean a present
  script or ELF binary has a missing or invalid interpreter; other launch errors and every started
  process result are authoritative. Exhausting the list errors rather than consulting `PATH`.
- **`--getent-path <ABSOLUTE>`:** uses that absolute pathname verbatim and bypasses both `PATH` and
  the trusted-directory list. It does not pin an inode: every ancestor used during resolution, every
  followed symlink target, and the final entry must be outside untrusted write control. The option
  must be absolute and may be given **at most once** — a duplicate is rejected, because a permissive
  trailing-wildcard policy (`... *`) would otherwise let an attacker append a second `--getent-path`
  to override the policy's value.
- **Numeric ids** (`--owner 1000`) never invoke `getent` at all — the safest option for a sudo rule
  when the resolving environment is untrusted.

```bash
# Select an administrator-protected resolver path so lookups never consult the caller's PATH:
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
- Without the flag, ownership-only operations preserve the entry's set-ID bits.

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

| Aspect                                        | Status                                                                                                                                                                                                                                                                                                          |
| --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Symlink following (leaf)                      | Hardened (Linux): `O_NOFOLLOW` on every entry open                                                                                                                                                                                                                                                              |
| Intermediate directory swaps                  | Hardened (Linux): every dir opened fd-relative from parent                                                                                                                                                                                                                                                      |
| FIFO swap (DoS/side-effect)                   | Hardened (Linux): `O_NONBLOCK` + `fstat`+`S_ISREG`                                                                                                                                                                                                                                                              |
| Metadata ops (chown/chmod/utimes/ACLs)        | Hardened (Linux): fd-bound; create-by-name symlink metadata is target-bound on its reopened handle                                                                                                                                                                                                              |
| File data copy                                | Hardened (Linux): local `copy_file_range`/fd-bound fallbacks; remote held-file streaming                                                                                                                                                                                                                        |
| `--delete` pruning                            | Hardened (Linux): fd-relative enumeration and removal                                                                                                                                                                                                                                                           |
| Remote copy (source side)                     | Hardened (Linux): two-pass dir-fd map                                                                                                                                                                                                                                                                           |
| Remote copy (destination side)                | Hardened (Linux): directory tracker fd-map                                                                                                                                                                                                                                                                      |
| Remote `--delete`                             | Not supported (rejected before operation)                                                                                                                                                                                                                                                                       |
| `--dereference` / `-L`                        | **Not hardened** (follows symlinks by design)                                                                                                                                                                                                                                                                   |
| Non-Linux builds                              | **Not hardened** (path-based code, documented)                                                                                                                                                                                                                                                                  |
| `rcmp`                                        | Out of scope (read-only; no mis-permissioning possible)                                                                                                                                                                                                                                                         |
| `filegen`                                     | **Not hardened** for privileged use on attacker-writable paths (path-based writes; no TOCTOU security flags)                                                                                                                                                                                                    |
| POSIX ACLs                                    | Source ACLs preserved only when requested with `acl`; strict mode prevents created entries from retaining inherited destination ACLs and suppresses reused-directory inheritance during the copy. Reused ACLs remain when `d:acl` is off; source ACLs replace/clear them when it is on (see [acls.md](acls.md)) |
| *Which* in-subtree object a name selects      | Out of scope — a same-parent compatible replacement can be selected; subsequent fd-bound work stays on that opened object and applies only the requested/source-derived policy (see [Scope of TOCTOU safety](#scope-of-toctou-safety))                                                                          |
| *Which* in-parent entry a by-name action uses | Out of scope — overwrite, remove, and delete mutate through a pinned parent; a compatible same-name replacement can be affected, but the operation cannot follow it outside that parent (see [Scope of TOCTOU safety](#scope-of-toctou-safety))                                                                 |
| Removal accounting under concurrent swaps     | Point-in-time — kind/byte totals use planning or admission snapshots plus syscall outcomes; they are operational summaries, not proof of the inode a later by-name syscall removed                                                                                                                              |
| Prefix trust (path above the named root)      | Caller's responsibility by default; under `--require-toctou-safe`, operands must be absolute + lexically normal and resolve `RESOLVE_NO_SYMLINKS` — a spliced symlink fails closed; prefix-writer renames stay in scope of the caller (see [Scope of TOCTOU safety](#scope-of-toctou-safety))                   |
| `fs.protected_hardlinks=0`                    | **Not defended** (userspace cannot close this gap)                                                                                                                                                                                                                                                              |

TOCTTOU vulnerabilities in rcp are **real but require local access** and specific privilege
configurations to exploit. On Linux, the default (non-`-L`) paths of `rcp`, `rlink`, `rchm`, and
`rrm` provide Containment, with fidelity or inode-bound metadata mutation where applicable, subject
to the [Residual Preconditions](#residual-preconditions) and the limitations above. This is not a
claim of TOCTOU safety in every sense. Use `--require-toctou-safe` in sudo rules to enforce the
hardened invocations automatically.

## Further Reading

- [LWN: The difficulty of safe path traversal](https://lwn.net/Articles/1050887/)
- [openat2(2) man page](https://man7.org/linux/man-pages/man2/openat2.2.html)
- [CWE-367: TOCTTOU Race Condition](https://cwe.mitre.org/data/definitions/367.html)
- [CVE-2019-16884](https://nvd.nist.gov/vuln/detail/CVE-2019-16884) - runc symlink attack
