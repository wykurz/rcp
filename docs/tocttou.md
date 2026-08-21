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
the named root*. The trust of the path *above* the named root is the caller's responsibility;
`--require-toctou-safe` additionally machine-checks the operand form and enforces symlink-free
resolution of the whole operand path (see
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

**2. Permission and ownership fidelity.** The permissions and ownership the destination
(`rcp`/`rlink`) or the modified entry (`rchm`) ends up with are those of the source object that was
**actually read**, as selected by the `--preserve` policy in force — not necessarily a byte-for-byte
copy of the source mode. What the policy selects is a separate question from this guarantee: the
default mask is `0o0777` (special bits stripped, like `cp`), and ownership is preserved only when
`uid`/`gid` are requested, so by default the destination is owned by the copier. The guarantee is
about *fidelity to the object that was read*: a concurrent swap can never make the tool **widen**
permissions beyond what the policy selected for the source it read, or attach another object's owner
— e.g. it cannot write a `0600` root-owned file's contents out as world-readable. Mode and bytes are
taken from the *same* fd, and metadata is applied through the destination's own held fd, never
re-resolved by path.

**Fidelity holds throughout the copy, not only at the end: destination files are created owner-only
until their contents are written.** A destination file is created at `0o600` and only widened to the
source mode by the closing `fchmod`, after the last byte and after every other metadata step — the
file counterpart of the directory split-chmod below. Creating it at the final mode instead publishes
the destination's *audience* before its contents exist. At default settings that is an exposure of
partial contents: the mode mask is `0o0777` (`setuid`/`setgid`/sticky stripped, matching `cp`), so a
world-readable source yields a world-readable destination from creation onward, and anyone who can
reach the directory can read however much has landed. No symlink swap is needed — a world-searchable
destination directory is enough, which is the default for any repeat or incremental
`rcp --overwrite`. Withholding the owner execute bit is deliberate as well — a half-written
executable should not be executable.

The escalation case (Scenario 4 above) needs the special bits preserved — `--preserve`, or a
`--preserve-settings` mask of `7777` — **and** an interrupted copier. Root holds `CAP_FSETID` —
*"don't clear set-user-ID and set-group-ID mode bits when a file is modified"* — so writing does not
strip `S_ISUID` from a root copier's destination, and a `SIGKILL`, OOM kill, or crash after the last
byte left a complete, functional, setuid-root executable whose contents the *source's* owner
authored. Be precise about which window that is: the **successful** copy was never an exec window on
Linux, because the destination stays open for writing through metadata application and `execve`
refuses a file any process holds open for writing with `ETXTBSY`. It is the copier's *death* that
closes that descriptor and drops the protection, which is exactly when the old creation mode had
already published a finished-looking setuid binary.

This is unconditional, not gated behind `--require-toctou-safe`: a file the copy creates is its own
new object with no prior user-visible state to preserve, exactly like a fresh directory. The mode is
not a parameter of the creating call, so no call site can opt out. One consequence is intended: a
copy that is interrupted, or that fails anywhere before *or during* metadata application, leaves a
file readable only by its **owner**, at `0o600`. "Owner", not "copier", because the chown runs
first: a copy that preserves ownership and gets past that step has already handed the file to the
source's uid, so a later failure leaves it owner-only under *that* uid rather than the copier's. (No
exposure either way — the source's owner authored the contents and could already read them, and only
a privileged copier can chown to another uid in the first place — but the file is not necessarily
one the copier can still read.) That the mode stays `0o600` at all holds at every step because the
widening `fchmod` runs last: metadata application is chown → utimens → chmod, chown first so it
cannot clear the `setuid` bit the chmod restores, and chmod last so no later failure can publish the
final mode on a file the copy is about to report as failed. The most likely instance is a copy that
preserves ownership whose `fchown` is refused — a non-root copier copying a file owned by someone
else gets `EPERM` at the first step. The failure is reported and the copy exits non-zero either way,
and a later run *normally* re-copies the file: the default `--overwrite-compare` is `size,mtime`,
and a partial file differs in size.

That re-copy is not guaranteed, and the exceptions are worth stating precisely, because before this
change a skipped retry was harmless — a failed metadata application still left the file at its
correct final mode, whereas now it leaves the destination owner-only until something re-copies it.
There are two. **A failure at the closing `fchmod`**: the timestamps have already been applied by
then, so the destination matches its source on both size and mtime and is skipped. Something has to
run last, and under the previous chown → chmod → utimens order this was the `futimens` failure
instead; what changed is that the step which can strand a file is now also the step that no longer
publishes a mode the copy did not finish earning. **The nanosecond concession**: `metadata_equal`
skips the nanosecond comparison whenever *either* side's `mtime_nsec` is zero, for filesystems that
do not store sub-second timestamps (`common/src/filecmp.rs`), so even a destination that never
reached `futimens` — and so carries its own write time — compares **equal** to its source under the
default `size,mtime` (mode is not compared) exactly when two conditions hold together: that write
and the source mtime fall in the **same whole second**, **and** the nanosecond field is zero on
**either** side. Copying a file written in that same second reaches it, as does any source carrying
a whole-second mtime (`touch -d @<seconds>`, a tar extraction, a reproducible-build epoch) or a
destination filesystem without sub-second timestamps. Adding the mode to the comparison
(`--overwrite-compare=size,mtime,mode`) closes both — a stranded destination is `0o600` and its
source is not — as does removing the destination first. Neither deleting the file on a metadata
failure nor changing the comparison default is done here: both are behavior changes that need their
own design.

One sharp edge of that ordering is *not* transient: `--preserve-settings="f:time,7777"` asks for the
source's mode but not its ownership, and the `fchown` is issued only when `uid` or `gid` is
preserved — so a setuid source produces a destination that is **permanently** setuid *and* owned by
the copier, which for a root copier is a setuid-root binary whose contents the *source's* owner
authored. Nothing later narrows it: that is the requested outcome, not a window. It matches
`cp --preserve=mode`, and asking for a source's mode without its ownership is an explicit choice —
but preserve both or neither when the source tree is not yours.

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
that `rcp` deliberately does not pay. This is not a security boundary: the files at risk are exactly
the ones the copy was instructed to replace, the loss is confined to the destination subtree, and no
privilege or containment property above depends on it.

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
wrapper that resolves and string-validates an operand therefore validates the object the tool
operates on: between its check and the tool's use, no symlink can be spliced anywhere along the
path. What such a caller still owes is *write*-control of the directories along the path — an actor
with write access to a prefix directory can rename real directories into place, substituting only
content they could already write — plus the bind-mount and `protected_hardlinks` preconditions
below.

**Reused destination directories are locked down under `--require-toctou-safe`.** A privileged copy
into an existing tree *reuses* a destination directory that is already present rather than
recreating it. Fresh directories are created copier-owned at `0o700` and only widened to the source
mode at the very end, but a reused directory would otherwise be copied into as-is — a pre-existing
`0o777` directory would expose the directory and every freshly written child for the whole subtree
copy, and stay exposed if the copy is interrupted. Under strict mode each reused directory is
therefore taken over by the copier before any child is written: the opened directory fd is
inode-rechecked against the entry classified a moment earlier (a `rename` that swapped in a
*different* directory fails closed), then it is `fchown`ed to the copier and `fchmod`ed to `0o700` —
chown first, so the prior owner cannot `chmod` it back open mid-copy. The takeover is then verified
by re-stat and fails closed if it did not actually land (a filesystem that reports a successful
`chown`/`chmod` without honoring it, e.g. CIFS without unix extensions). A reused directory that had
the setgid bit keeps it through the lockdown — children created during the copy inherit the
directory's group, and finalize cannot repair the group of a child already created — so the group is
also pinned to the value captured at classification: taking uid ownership freezes the group (only
the copier or root can change it now), and if a prior owner raced a `chgrp` into the takeover window
the copier resets it, so a setgid directory cannot funnel freshly written children into an
attacker-chosen group. The directory's **default ACL** is snapshotted and removed at the same point,
for the same reason the mode is restricted: a `chmod` cannot reach it, so children created during
the copy would otherwise inherit it and be granted access beyond their `mode` (its *access* ACL
needs no strip — the `fchmod(0o700)` rewrites `ACL_MASK` to `---`, so every named entry grants
nothing for the duration). At finalize — after all children and any `--delete` prune — the
directory's original owner is restored and the source metadata re-applied (the final owner is the
source owner for each `--preserve`d component and the original owner otherwise, and the mode is the
same masked metadata the unhardened copy would apply — the source directory's, or the update tree's
for `rlink --update`), so a successful copy leaves the directory byte-identical to a copy without
this hardening — with one strict-mode refinement: under `--require-toctou-safe`, finalize
additionally re-stats each reused directory and fails closed if the restored owner or mode did not
take effect (catching a backend that does not honor `chown`/`chmod`); a setgid bit that the kernel
drops because the copier is not in the directory's group and is not privileged is reported as a
WARNING (narrower than the source, and not a failure — matching the best-effort behavior of a
non-strict copy). Two side effects are accepted, not hidden. First, a reused directory whose
processing is *aborted after lockdown* — by a `--fail-early` abort, or by any per-directory error
that returns before finalize (e.g. an enumeration failure) even *without* `--fail-early` — is left
no wider than a successful copy's result: the local path leaves it *secured* (copier-owned at
`0o700`), while the remote path may instead have already restored it to its transparent final state
(source mode with the original/source owner). The lockdown restricts the mode to `0o700` immediately
after taking ownership, and the takeover is VERIFIED (uid + exactly `0o700`) before any further
step, so on a filesystem that honors these syscalls any later failure leaves the directory secured.
Three exceptions still fail closed (no child written) but leave the directory no narrower than
requested — possibly its ORIGINAL, pre-lockdown mode, which may be wider than the `0o700` a mid-copy
directory holds (though never wider than the directory already was before rcp ran): (1) the
restricting `chmod` fails but the ownership rollback SUCCEEDS — the directory is returned to its
original owner and mode (the failed `chmod` changed nothing); (2) BOTH the `chmod` and the rollback
fail (a read-only or failing backend), reported with both errnos, leaving it copier-owned at its
original mode; and (3) a backend reports `chmod`/`chown` success without taking effect (e.g. CIFS
without unix extensions), so the verification fails. rcp cannot force a non-honoring backend
narrower and does not retain the directory's original mode to re-restrict — and it does not chown a
secured directory back to the prior owner (that would re-widen) — so it reports the true observed
owner/mode and leaves repair to the operator. Restoration is likewise deliberately *not* forced onto
the normal abort paths: doing so would re-widen the directory (chown it back to the prior owner and
re-apply the source mode) while its children may be incomplete — the opposite of failing closed. On
a honoring backend the secured `0o700` is the outcome and no abort yields a wider directory than the
mid-copy state; the exceptions above only ever return the directory toward the state it already had.
The directory's snapshotted **default ACL** is the one thing an abort must not merely leave alone:
the lockdown removed it and holds the only copy of those bytes in memory, so losing it would be
permanent data destruction rather than a permission left narrow. Its restore therefore runs from an
RAII guard rather than from finalize alone, so every path that locks a directory and then never
reaches finalize — a `--fail-early` abort that drops in-flight siblings, a per-directory error, a
remote destination that fails between locking and registering — still puts it back. A failed restore
there is logged with the directory and the ACL bytes, since a destructor cannot report an error —
but as an ordinary `warn!`, so it needs `-v` to be seen (it is per-directory and unbounded on a mass
abort, which is exactly what disqualifies it from the always-visible notice channel). See
[acls.md](acls.md#--require-toctou-safe-containment). Second, an actor holding a directory fd opened
*before* the lockdown can still read the *names* of children written afterward (each child's
contents stay protected by its own source-derived mode). This is destination-only and
strict-mode-only; the default path leaves reused directories exactly as-is (their permissions may
then block writing, which is the pre-existing behavior).

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
same source fd — and the underlying concurrent merge of two source directories into one destination
inode is pre-existing and independent of this hardening. `--require-toctou-safe` rejects duplicate
destinations outright, so it does not reach the explicit case; the folding case is not detectable
lexically, so it does. Closing this fully requires coordinating every directory lifecycle — fresh
*and* reused — by `(dev, ino)` across the local, `rlink`, and remote engines (fail closed on an
in-flight alias); it is tracked as a follow-up.

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
Running a single invocation with multiple sources is safe (it is serialized in-process). Overlaps
are often, but not reliably, caught: finalize re-stats each reused directory and fails closed if the
restored owner or mode did not take effect, which surfaces many interleavings as a loud error.

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
- **Destination objects are created narrow and widened last.** A new destination directory is
  created at `0o700` and a new destination file at `0o600`; the source mode is applied only once the
  object is complete (all children written, all bytes written). Nothing is ever reachable at its
  final — possibly setuid — mode before its contents exist; see
  [Scope of TOCTOU safety](#scope-of-toctou-safety). The file creation mode is a constant rather
  than a parameter of the creating call, so no call site can opt out of it.
- On overwrite paths, a `recheck` verifies that the `(dev, ino)` of the entry matches the originally
  classified handle before performing the unlink.
- Directory names passed to any `*at()` call are validated to be single path components (no `/`,
  `.`, `..`).
- **Source payload and metadata come from the same fd (read-side fidelity).** For each copied or
  sent source object, the data and the metadata applied/sent for it are read from one open file
  description, so a same-name swap cannot pair one inode's bytes/target with another inode's
  mode/owner/timestamps — or, under `acl`, with another inode's ACL, which is read from that same fd
  (`read_acls_fd` on a file's data fd, `Dir::read_acls` on the enumerated directory fd): a regular
  file via `open_file_read` → `(File, FileMeta)`; a symlink via the `O_PATH` handle's `read_symlink`
  (target + metadata off the one fd); a directory via the enumerated `Dir` fd (`read_entries` +
  `meta`). The remote *destination* is fidelity-safe by construction — it writes the received bytes
  and applies the received metadata to its single created fd, so there are no two source fds to
  mismatch. `scripts/check-source-read-fidelity.sh` (run in CI) backstops this by forbidding by-name
  source-payload reads (`read_link_at`, `File::open`) in the hardened modules, outside the
  `-L`/`--dereference` path.

### Strict operand resolution (`--require-toctou-safe`)

The operand paths themselves — the one place the fd-based walk still consults multi-component paths
(`Dir::open_root_dir` for a named root, `Dir::open_parent_dir` for its container; both in
`common/src/safedir.rs`) — get a stricter contract when `--require-toctou-safe` is in force:

- **Lexical form.** Every operand must be absolute and lexically normal: no `.` or `..` components,
  no empty (`//`) segments; a single trailing slash is allowed (it carries copy-into meaning for
  destinations). `realpath` output always qualifies. This makes string-level operand policies
  (sudoers patterns, wrapper prefix checks) sound — `/vetted/../etc` can no longer pass a
  `/vetted/*` check.
- **No-symlink resolution.** Both operand opens switch from plain `openat` to
  `openat2(RESOLVE_NO_SYMLINKS)` (which implies `RESOLVE_NO_MAGICLINKS`), enforced by the kernel at
  the open itself: a symlink in any directory component of the path fails closed with `ELOOP`. This
  closes the race between a wrapper's `realpath`+policy check and the tool's open. Requires Linux
  5.6+; on older kernels `--require-toctou-safe` refuses to run rather than degrade. The named entry
  itself keeps the tools' existing non-`-L` semantics: classified `O_NOFOLLOW`, a symlink operand is
  operated on as the link object and never followed (`ELOOP` where the open requires a directory,
  e.g. the `--delete` prune reopen).
- **Resolved up front, then threaded.** Every operand prefix is validated at the entry of the
  operation, **before any filter / dry-run / overwrite / `--update` branching** — the same
  discipline the recursive walk already uses below the named root, now extended up to the operands
  themselves. A symlinked prefix therefore fails closed before any downstream branch (a filtered-out
  source root, a trailing-slash or `--overwrite` destination, a plain `--update`, or a `--dry-run`
  that never touches the destination) can skip validation. The resulting parent fd is normally
  retained for root-filter classification, existence checks, and the walk. Rlink's optional update
  root is a documented implementation exception: it performs one up-front strict prefix/existence
  probe and later opens the update parent again for its fd-relative walk. Both operations
  independently use `openat2(RESOLVE_NO_SYMLINKS)`, so the second open cannot weaken containment;
  consolidating them into one retained parent is a follow-up rather than a security dependency.
  Where a dry run holds no persistent destination fd, the existence probes re-resolve with a
  decomposed `openat2` (parent open + `O_NOFOLLOW` child), which is atomic and distinguishes an
  intermediate-prefix symlink (fail closed) from a final component that is merely a symlink or
  non-directory (a replaceable operand, accepted). The `--delete` prune scan is the one consumer
  that, in a dry run, reopens the destination directory by its full path (`openat2`
  `RESOLVE_NO_SYMLINKS`): this path is *below* the named root, which the real run walks fd-relative
  and would replace or skip as it goes, so a symlink anywhere in it (an intermediate the real run
  would replace, or a final symlink) yields `ELOOP` and the prune preview is simply skipped —
  nothing to prune, since the real run creates a fresh directory there and exits successfully. The
  operand prefix *above* the named root is still validated up front (fatal), so this below-root skip
  does not weaken the contract; it only keeps the preview consistent with the real run.
- **Remote operands.** The master lints the remote path parts (which must be absolute as written;
  `host:~/x` forms are rejected) and mirrors the flag onto each spawned `rcpd`, which arms the same
  strict resolution for its root opens on the remote host. The source `rcpd` opens the source parent
  up front (fail closed) before its root filter; the destination `rcpd` learns the destination path
  only from the source's directory/symlink messages (its own `MasterHello::Destination` carries no
  path), and opens the destination parent when it processes the root — so a symlinked prefix fails
  closed whenever an operand is actually accessed. **Residual:** whenever the destination `rcpd`
  receives no create message — a remote **`--dry-run`** (which writes nothing by design), a copy
  whose **source root is entirely filtered out**, or a **skipped root special** — it never opens
  (hence never validates) its destination parent. Nothing is written through the prefix, so it is
  benign, but unlike a local operation it does not separately fail closed on a symlinked destination
  prefix. Closing this fully needs a remote-protocol change (handing the destination operand to the
  destination `rcpd` up front, since neither the master nor the source can open a path on the
  destination host) and is deferred.

The switch is process-global, armed once by the linter before any filesystem work; the default (no
flag) behavior is unchanged. Because the operand parents are opened for read up front, an excluded
root under an execute-only (`0111`) parent — which the default path skips via a path stat — instead
fails with `EACCES` under strict mode; this is consistent with the real copy (which already required
parent read) and with `rrm`/`rchm`. One consequence worth knowing: systems whose legitimate paths
cross symlinks (`/home → /var/home`, automounts) must pass the resolved form — that is the point,
and it is why this is opt-in rather than the default.

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
  filter acquires first because its authoritative filter probe opens a handle; an included unknown
  or known non-directory otherwise acquires immediately before spawn. A positive directory hint
  normally takes no leaf admission. Destructive remove/delete filtering is deliberately stricter:
  every hint is advisory, so the driver acquires, classifies once, and transfers that exact
  `AdmittedEntry` into dispatch. Rlink's dual-tree path similarly admits before opening either
  source or update handle when an update counterpart needs classification, because the source hint
  says nothing about that separate entry. An authoritative directory releases any provisional permit
  inside the inner scope before `dir_pre` or descent. This bounds fd-bearing leaf fan-out without
  putting arbitrary recursive directory depth/breadth into a pool, which would recreate the
  deep-directory hold-and-wait deadlock.
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
- **Independent pools and recursive overwrite**: the same configured `N` is assigned to separate
  OpenFile and PendingMeta semaphores, not one combined pool. Copy/link overwrite leaf work can
  retain OpenFile admission while recursively invoking rm, which draws from PendingMeta. That
  OpenFile → PendingMeta call is the only direction between those two descriptor-admission pools;
  recursive directory handles and process-support descriptors remain outside both `--max-open-files`
  pools and their default soft-limit heuristic.
- **Trusted vs hardened boundary**: the symlink-following parent-prefix open returns a distinct
  `TrustedDir` type (`common/src/safedir.rs`); crossing below the named root yields a hardened `Dir`
  whose child opens are all `O_NOFOLLOW`. The boundary is type-enforced — a hardened child cannot be
  silently used where a trusted parent is required, and vice versa.
- **Filter-classification errors**: ordinary hardened source walks and rlink's dual-tree driver
  route `DT_UNKNOWN` `is_dir` decisions through `walk::filter_is_dir`; destructive remove/delete
  filtering instead uses the authoritative kind in its transferred `AdmittedEntry`. Neither path
  follows a symlink, and an error propagates rather than becoming a cheap non-directory result. The
  path-based rcmp walk remains a read-only exception: a failed `DirEntry::file_type` currently falls
  back to non-directory for filtering. Making that path fail closed is a follow-up; it does not
  authorize filesystem mutation today.
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
  inheriting admission. The fd is retained, not cloned or duplicated. A separate bounded remote-I/O
  abstraction is the documented follow-up for that payload-leaf residual; it is not a claim about
  every Tokio filesystem operation, and the wire protocol is unchanged.

`rlink` is the documented exception: it walks two correlated trees (source plus `--update`) and so
keeps its own dual-tree enumeration, but it shares the same substrate — the `TrustedDir` boundary,
explicit entry admission, checked leaf ownership, and fallible `filter_is_dir` — rather than
duplicating the hardening.

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

Under `--require-toctou-safe` the boundary tightens: operands must be absolute and lexically normal,
and the full operand path — prefix included — is opened `RESOLVE_NO_SYMLINKS`, so the "resolved
normally" clause above no longer applies; a symlink between the fixed prefix and the named operand
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
- **POSIX ACLs — the source's are not preserved unless you ask.** rcp does process POSIX ACLs, both
  locally and across the remote transport, but only when `--preserve-settings` requests `acl`
  (`all+acl`, or a per-type `f:acl`/`d:acl`). Detecting an ACL costs a syscall on every entry —
  there is no bit in `stat` for it — so the flag people reach for by default deliberately does not
  pay it. A copy that does not request `acl` therefore ends up with the source's **mode** and no
  ACL, and a source ACL entry narrower than `other` acts as a deny in effect, so dropping it grants
  exactly what the source withheld. **`--require-toctou-safe` does not close this**; see the table
  below. The full model, both widening directions and the measured costs are in [acls.md](acls.md).

  Two things the strict mode *does* do, both containment rather than fidelity — no destination entry
  rcp **creates** carries an ACL entry that did not come from its source. (A *reused* directory was
  already there and keeps its own access ACL; only what the copy writes is in scope.)

  - Every directory rcp **creates** has both its ACLs removed after the `mkdirat`, so nothing
    created beneath it can inherit one. Two syscalls per directory and none per file: stripping the
    default ACL stops the inheritance chain for the whole subtree. The window between `mkdirat` and
    the strip is not exploitable — the directory is at `0o700` and the kernel intersects inherited
    entries with the create mode, leaving `m::---`, so every named entry grants nothing.
  - Every **reused** directory has its DEFAULT ACL snapshotted and removed for the copy's duration,
    and restored at finalize (or by an RAII guard if the copy aborts first). Its **access** ACL is
    deliberately left alone: the lockdown's `fchmod(0o700)` already neuters it, because on Linux
    `chmod` rewrites the `ACL_MASK` entry from the new group bits, so `0o700` sets the mask to `---`
    and every named `user:`/`group:` entry — and the owning group — grants nothing for the duration.
    The entries survive; note that "ineffective" is not "absent", and the finalize `chmod`
    re-derives the mask from the restored mode's group bits, which is the same rewrite an unhardened
    copy's directory-metadata step performs. Removing the **default** ACL is what the `chmod` could
    not do for itself: `chmod` does not touch it, so without the strip children created during the
    copy would inherit it and be granted access beyond their `mode`.

  Consequence worth knowing: under `--require-toctou-safe` **without** `d:acl`, a freshly created
  destination directory ends with no ACL even where its parent's default ACL would ordinarily have
  given it one. That is the intended trade — the flag's contract is that the destination reflects
  its source and nothing else, so containment beats inheritance — and `d:acl` is the escape. See
  [acls.md](acls.md#a-consequence-not-an-oversight).

  Because an ACL is a property of the filesystem at runtime (not of the operand path or flags),
  `--toctou-check` does not — and cannot — detect or report any of this.

  |                               | dropped SOURCE ACL | inherited DESTINATION ACL     |
  | ----------------------------- | ------------------ | ----------------------------- |
  | default                       | open               | open                          |
  | `--preserve-settings=all+acl` | closed             | closed, repaired at finalize  |
  | `--require-toctou-safe`       | **open**           | closed, prevented at creation |
  | both                          | closed             | closed, prevented at creation |

  **Pair `--require-toctou-safe` with `--preserve-settings=all+acl`** where the source's ACLs are
  security-relevant. The flags close different bugs and deliberately do not imply each other:
  auto-enabling `acl` would impose the per-entry probe on a flag people reach for a different
  reason, and would silently override an explicit `--preserve-settings`. One `listxattr` on the
  source root per run warns when that root carries an ACL the copy is about to drop — a heuristic,
  since a root without one says nothing about its children.

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
half of (1): it refuses non-hardened invocations, refuses operands that are not absolute and
lexically normal (so the string the policy validated is the only object the path can denote), and
resolves every operand `RESOLVE_NO_SYMLINKS` so the object it opens is the one at that literal path
at open time. What it cannot do is make the *policy* judgment — which paths are acceptable — or
defend against an actor who can already write a prefix directory renaming real directories; that
remains the caller's responsibility per (1).

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

| Aspect                                       | Status                                                                                                                                                                                                                                                                                        |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Symlink following (leaf)                     | Hardened (Linux): `O_NOFOLLOW` on every entry open                                                                                                                                                                                                                                            |
| Intermediate directory swaps                 | Hardened (Linux): every dir opened fd-relative from parent                                                                                                                                                                                                                                    |
| FIFO swap (DoS/side-effect)                  | Hardened (Linux): `O_NONBLOCK` + `fstat`+`S_ISREG`                                                                                                                                                                                                                                            |
| Metadata ops (chown/chmod/utimes/ACLs)       | Hardened (Linux): fd-based, no path re-resolution                                                                                                                                                                                                                                             |
| File data copy                               | Hardened (Linux): `copy_file_range` between held fds                                                                                                                                                                                                                                          |
| `--delete` pruning                           | Hardened (Linux): fd-relative enumeration and removal                                                                                                                                                                                                                                         |
| Remote copy (source side)                    | Hardened (Linux): two-pass dir-fd map                                                                                                                                                                                                                                                         |
| Remote copy (destination side)               | Hardened (Linux): directory tracker fd-map                                                                                                                                                                                                                                                    |
| Remote `--delete`                            | Not supported (rejected before operation)                                                                                                                                                                                                                                                     |
| `--dereference` / `-L`                       | **Not hardened** (follows symlinks by design)                                                                                                                                                                                                                                                 |
| Non-Linux builds                             | **Not hardened** (path-based code, documented)                                                                                                                                                                                                                                                |
| `rcmp`                                       | Out of scope (read-only; no mis-permissioning possible)                                                                                                                                                                                                                                       |
| POSIX ACLs                                   | Source ACLs preserved only with `--preserve-settings=all+acl`; `--require-toctou-safe` contains inherited *destination* ACLs but does not preserve the *source's* — pair the two (see [acls.md](acls.md))                                                                                     |
| *Which* in-subtree file a swap makes us read | Out of scope — reads are not inode-pinned; a same-directory regular-file swap can change which file is read, but cannot escape the subtree or widen permissions (see [Scope of TOCTOU safety](#scope-of-toctou-safety))                                                                       |
| Prefix trust (path above the named root)     | Caller's responsibility by default; under `--require-toctou-safe`, operands must be absolute + lexically normal and resolve `RESOLVE_NO_SYMLINKS` — a spliced symlink fails closed; prefix-writer renames stay in scope of the caller (see [Scope of TOCTOU safety](#scope-of-toctou-safety)) |
| `fs.protected_hardlinks=0`                   | **Not defended** (userspace cannot close this gap)                                                                                                                                                                                                                                            |

TOCTTOU vulnerabilities in rcp are **real but require local access** and specific privilege
configurations to exploit. On Linux, the default (non-`-L`) paths of all write-capable tools are
hardened for the two guarantees stated in [Scope of TOCTOU safety](#scope-of-toctou-safety) —
containment and permission/ownership fidelity — subject to the
[Residual Preconditions](#residual-preconditions) and the limitations above; they are not a claim of
TOCTOU safety in every sense. Use `--require-toctou-safe` in sudo rules to enforce safe invocations
automatically.

## Further Reading

- [LWN: The difficulty of safe path traversal](https://lwn.net/Articles/1050887/)
- [openat2(2) man page](https://man7.org/linux/man-pages/man2/openat2.2.html)
- [CWE-367: TOCTTOU Race Condition](https://cwe.mitre.org/data/definitions/367.html)
- [CVE-2019-16884](https://nvd.nist.gov/vuln/detail/CVE-2019-16884) - runc symlink attack
