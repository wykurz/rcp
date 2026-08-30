# POSIX ACLs in the RCP tools

This document is the single home for how the RCP tools handle POSIX.1e access control lists: what
`--preserve-settings=all+acl` does, why ACLs are opt-in rather than part of `all`, the two
independent ways a copy can end up **more permissive than its source**, and what
`--require-toctou-safe` guarantees and excludes. It covers `rcp` (local and remote) and `rlink`.

If you only read one thing: **`--preserve-settings=all` does not preserve ACLs**, and on a
destination tree that has default ACLs a copy can gain permissions its source never granted. Use
`all+acl` when either matters.

## Table of Contents

- [The model](#the-model)
- [Two ways a copy widens](#two-ways-a-copy-widens)
- [Who pays to contain inheritance](#who-pays-to-contain-inheritance)
- [Why `acl` is opt-in](#why-acl-is-opt-in)
- [The CLI surface](#the-cli-surface)
- [The ACL-preservation settings notice](#the-acl-preservation-settings-notice)
- [How ACLs are applied, and why the order matters](#how-acls-are-applied-and-why-the-order-matters)
- [Failure rules](#failure-rules)
- [Carry the bytes verbatim](#carry-the-bytes-verbatim)
- [Remote copies](#remote-copies)
- [`--require-toctou-safe` containment](#--require-toctou-safe-containment)
- [Tool coverage](#tool-coverage)
- [Known holes](#known-holes)
- [Non-goals](#non-goals)

## The model

A POSIX.1e ACL lives in two extended attributes, and the RCP tools read and write them as opaque
kernel blobs:

| attribute                  | on               | governs                                   |
| -------------------------- | ---------------- | ----------------------------------------- |
| `system.posix_acl_access`  | any inode        | that entry's own permissions              |
| `system.posix_acl_default` | directories only | what freshly created **children** inherit |

An access ACL is a list of entries: `USER_OBJ` (the owner), zero or more named `USER` entries,
`GROUP_OBJ` (the owning group), zero or more named `GROUP` entries, `MASK`, and `OTHER`.

**The mask-vs-group-bits duality is the thing to internalise.** When an entry carries an access ACL,
the mode's rwx bits and the ACL are not two independent facts — the kernel keeps them in sync:

- the mode's **owner** bits are `USER_OBJ`,
- the mode's **other** bits are `OTHER`,
- the mode's **group** bits are the ACL's **`MASK`**, *not* `GROUP_OBJ`. (A *stored* ACL always has
  a `MASK`: one without named entries needs none, and such an ACL is trivial, so Linux does not
  store it — see below.)

So `chmod` rewrites `MASK` from the new group bits, and writing an access ACL moves the mode.
Neither operation is a read-only view of the other; each is a write to the same underlying state
through a different window. Two consequences run through the whole design:

- A `chmod` **after** an ACL is applied re-derives `MASK` and destroys the ACL's fidelity. This is
  why the applier's order is what it is (see
  [below](#how-acls-are-applied-and-why-the-order-matters)).
- Restricting a directory to `0o700` sets its `MASK` to `---`, which renders every named entry
  ineffective *without removing it*. "Ineffective" is not "absent": a later `chmod` back to a wider
  mode makes those entries effective again.

**The mode and the ACL are both required; neither subsumes the other.**

- An entry with no ACL stores nothing. A so-called *trivial* ACL — one that is exactly equivalent to
  the mode — is not held as an xattr at all, so "copy the mode" already covers every entry that has
  no ACL, which is almost all of them.
- The mode expresses things the ACL cannot: `setuid`, `setgid` and the sticky bit live **only** in
  the mode. An ACL carries none of them.
- The ACL expresses things the mode cannot: any per-user or per-group entry. In particular a named
  entry that is *narrower* than `other` acts as a deny in effect. POSIX.1e has no literal deny
  entry, but the outcome is the same, and no mode can express "everyone except this uid".

## Two ways a copy widens

Both cases are reproducible on ext4. They are independent; closing one does not close the other.

### 1. The source's ACL is dropped

```
SOURCE  mode=0o755  acl = u::rwx  u:65534:---  g::---  m::r-x  o::r-x   -> uid 65534: no access
DEST    mode=0o755  acl = (none)                                        -> uid 65534: read+execute
```

Copying the mode and discarding the ACL grants uid 65534 exactly what the source withheld.

**Both modes are `0o755`: the copy does not change the mode at all.** That is the point, and it is
worth pausing on. The source's group bits `r-x` *are* its `MASK` — by the duality above, an entry
carrying this ACL cannot have any other mode — so there is no mode for rcp to get wrong and no
mode-diff to notice. The destination is more permissive purely because an entry is missing, and a
reader who expects the mode to have moved will go looking for the bug in the wrong place.

This is a **fidelity** gap, not a race. Requesting `acl` closes it.

### 2. The destination tree's default ACL is inherited

This one needs nothing unusual on the source — just a destination directory with a default ACL,
which is exactly what default ACLs are for:

```
SOURCE   mode=0o640  acl = (none)                                  -> uid 65534: no access
DEST DIR default acl = u::rwx u:65534:rwx g::r-x m::rwx o::r-x

  rcp creates the child at 0o600 -> it INHERITS  u:65534:rwx, m::---   (mask makes it inert)
  rcp's final fchmod(0o640) re-derives the mask from the mode's
  group bits                     -> m::r--, so u:65534 becomes EFFECTIVE r--

DEST     mode=0o640  acl = u::rw- u:65534:rwx g::r-x m::r-- o::---  -> uid 65534: read
```

Note the shape: the entry is inherited *inert* (the create mode leaves the mask empty) and the
finalize `chmod` is what makes it effective.

It also reaches through directories rcp creates **itself**, so this is not a matter of handling
pre-existing destination directories. A directory created with `mkdirat(0o700)` inherits **both** an
access and a default ACL from its parent, and its children then inherit from it:

```
dstroot           (pre-existing)  default = u::rwx u:65534:rwx g::r-x m::rwx o::r-x
dstroot/fresh     (mkdirat 0o700) access  = u::rwx u:65534:rwx g::r-x m::--- o::---   default = <inherited>
dstroot/fresh/f   (0o600, fchmod 0o644) access = u::rw- u:65534:rwx g::r-x m::r-- o::r--
```

**The design consequence: faithful preservation means CLEARING as well as setting.** A source with
no ACL requires an explicit `fremovexattr` on the destination, or inheritance silently widens the
copy. "Do nothing when the source has no ACL" is not correct — and that single fact is what makes
[the failure rules](#failure-rules) below counterintuitive.

## Who pays to contain inheritance

Widening direction 2 is a **privileged-copy** concern, not an everyday-correctness one, and it is
treated that way. There are three levels, and no cost is imposed on the default path:

| mode                                                | direction 2 contained?                                                        | cost                                                                                                                                          |
| --------------------------------------------------- | ----------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| default (neither `acl` nor `--require-toctou-safe`) | no — documented, see [Known holes](#known-holes)                              | none                                                                                                                                          |
| `acl` requested                                     | yes, repaired at finalize: the applier's clear step removes inherited entries | the [per-entry probe](#why-acl-is-opt-in)                                                                                                     |
| `--require-toctou-safe`                             | yes, prevented at creation                                                    | 2 syscalls per created **directory**, none per file beneath it (a reused one pays more, and a direct **file** operand pays one strip — below) |

The strict-mode containment is the cheap one because stripping a directory's ACLs once stops the
chain for everything created beneath it: a child of a stripped directory comes out with no ACL at
all, with no per-file work.

The "2 syscalls" is the cost for a directory rcp **creates** — one `fremovexattr` per ACL inside the
creation call. A **reused** directory costs more, because its own ACL has to survive the copy: a
`flistxattr`, an `fgetxattr` only when a default ACL is present, and a conditional remove and
conditional final set for that attribute. The lockdown requests only the default ACL, so a present
access ACL adds no `fgetxattr`. This work remains per-directory, and files beneath it pay nothing.
One narrow exception remains: a file created **directly in the ambient operand parent** (a direct
file operand's destination, the one directory rcp neither creates nor locks down) pays a single
`fremovexattr` inside its create, because no directory-level strip ran there. That is one syscall
per *operand*, not per file of a tree.

## Why `acl` is opt-in

There is no bit in `stat` saying whether an entry has an ACL. Detecting one costs a syscall, on
every entry. Measured on ext4, 20k files, best of 3:

| operation                                       | ns/entry |
| ----------------------------------------------- | -------- |
| `stat` (what rcp already pays per entry)        | 949      |
| `getxattr(system.posix_acl_access)` → `ENODATA` | 1057     |
| `listxattr`                                     | 591      |

ACL preservation performs one `flistxattr` per entry. File reads duplicate the held fd so blocking
work can own it; directory reads share the already-owned directory fd. It then performs one
`fgetxattr` for each requested ACL attribute actually present: at most one for a file and two for a
directory. A size race or an attribute larger than the stack buffer can require a retry. Against the
handful of metadata operations a copy already pays per entry, this is material across a tree of
millions and is paid even when no entry has an ACL. That is why `all` excludes ACLs and `all+acl`
opts in.

The read probe is `listxattr`-first because `listxattr` is materially cheaper than a `getxattr` that
misses, and misses are the overwhelming majority. A `getxattr` is issued only for a requested name
the list actually contained.

Every ACL syscall — read and write — is routed through the same metadata gate as `stat`, `chmod` and
`utimens`, so it is rate-limited, counts against the congestion window and feeds the latency probe
(see [congestion_control.md](congestion_control.md)). ACL reads are bucketed as `Stat` and ACL
writes as `Chmod`; `--auto-meta-histogram-log` shows the per-operation latency distribution if you
want to see whether that bucketing still fits. The one exception is the reused-directory lockdown's
abort-path restore, which runs from a destructor and so cannot await the gate.

That `all` really pays nothing is asserted on **syscall count**, not on outcome — an outcome-only
test cannot see the cost creep back in.

## The CLI surface

`acl` is an attribute in the existing `--preserve-settings` DSL, plus a preset modifier:

```bash
rcp --preserve-settings=all      src dst   # uid, gid, time, full mode. NO ACLs.
rcp --preserve-settings=all+acl  src dst   # ...plus ACLs on files and directories
rcp --preserve-settings="f:uid,gid,time,acl,7777 d:uid,gid,time,acl,7777 l:uid,gid,time" src dst
```

**Preset grammar.** A string is a preset if and only if its first `+`-separated token is `all` or
`none`; each remaining token is a modifier, and `acl` is the only recognized modifier. An unknown
modifier is an error naming the valid set, and repeats are idempotent (`all+acl+acl` is `all+acl`).
Any string whose first `+`-token is not `all` or `none` is parsed by the per-type DSL, where `+` is
an ordinary character.

`none+acl` is legal and means "copy ACLs but not ownership or timestamps". Odd but coherent, and
special-casing it would be arbitrary.

**Two parse-time rejections**, both deliberate — silently accepting either would be the same class
of quiet lie this feature exists to remove:

- **`l:acl` is an error.** The kernel has no symlink ACL, so the attribute could only ever be
  ignored.
- **`acl` combined with a mode mask that narrows rwx is an error.** An ACL *is* the permission
  state, so a mask saying "strip group and other" while an ACL puts them back is a contradiction.
  The rule is exactly `mask & 0o777 != 0o777`. Masks that strip only the **special** bits are fine
  and remain the default: the ACL carries no setuid/setgid/sticky, so the two are orthogonal.
  `all+acl` (mask `0o7777`) and a bare `f:acl` (default mask `0o0777`) both pass.

## The ACL-preservation settings notice

`all` does not include ACLs, so a run that asks for metadata fidelity without asking for `acl` emits
one conditional notice:

```
WARN This copy does not preserve POSIX ACLs for files and directories. If such source entries carry
     ACLs, the destination may become more permissive. Use `--preserve-settings=all+acl`.
```

The notice is derived entirely from the resolved settings. It does not inspect the source root or
any other entry and therefore adds no filesystem syscall. A point-in-time root observation would say
nothing about descendants and an attacker could change the observed ACL immediately before or after
it. That opportunistic observation would not improve the guarantee, so it does not justify either
the runtime cost or the complexity.

Under `--require-toctou-safe` the notice also explains that strict mode prevents inherited
destination ACLs but does not carry source ACLs across. See
[the two flags close different bugs](#the-two-flags-close-different-bugs).

### What arms it

Two settings arm the notice independently:

- The resolved `--preserve`/`--preserve-settings` requests anything beyond the preserve-none
  baseline — `all`, a bare `f:uid`, a `7777` mode mask, or any other metadata fidelity.
- `--require-toctou-safe`, whatever the preserve settings say. It is a request about permissions in
  its own right and receives the strict wording above.

The notice names each supported entry kind whose ACLs may be omitted. Preserving `f:acl` alone still
warns about directories; preserving `d:acl` alone still warns about files. A hard-linked rlink file
already shares the source inode and therefore its ACL, so rlink treats that file case as safe
without writing through the link. Directory ACLs can still be omitted and remain worth mentioning.

| Invocation                                | Notice                          |
| ----------------------------------------- | ------------------------------- |
| `rcp src dst`                             | silent                          |
| `rcp --preserve-settings=none src dst`    | silent                          |
| `rcp --preserve-settings=all src dst`     | warns                           |
| `rcp --preserve-settings=d:uid src dst`   | warns                           |
| `rcp --require-toctou-safe src dst`       | warns (strict wording)          |
| `rcp --preserve-settings=all+acl src dst` | silent — the ACLs are preserved |
| `rlink src dst`                           | warns — rlink defaults to `all` |

A bare `rcp` is silent because it reproduces the source's `rwx` bits like `cp` and makes no broader
metadata-fidelity claim: uid, gid, timestamps, and special mode bits are also omitted without a
notice. `rlink` defaults to `all`, so a bare rlink has made that claim. Resolved preserve-none
settings, including the equivalent `f:0777 d:0777`, silence the notice in either tool.

The notice fires at most **once per process**, independent of tree size and operand scheduling. It
is printed at the **default verbosity** through the dedicated `rcp::notice` tracing target; other
warnings still require `-v`. `--quiet` suppresses it along with all other output. The built-in
target directive is applied after `RUST_LOG`, so `RUST_LOG` cannot suppress it.

On a remote copy the source `rcpd` derives and emits the notice from settings sent over the wire;
the master re-emits it on `rcp::notice`, prefixed `remote::source::`. This is source-side advice,
but no source filesystem observation is performed.

The event is a `warn!`, not an `error!`, and does not affect the copy's exit code. It states a
possibility implied by the requested settings. When ACL preservation is requested, an unreadable
source ACL or a destination that cannot store it is an operation error instead.

## How ACLs are applied, and why the order matters

A destination file is created owner-only (`0o600`) and a destination directory at `0o700`, and
widened to the final mode only once it is complete — for a **file**, after the last byte and every
other metadata step, so the widening step is the **last fallible one** (see
[tocttou.md](tocttou.md#scope-of-toctou-safety) for why). Applying an access ACL *is* a
mode-changing operation, because `fsetxattr` drives the mode's rwx bits from the ACL's `USER_OBJ`,
`MASK` and `OTHER` entries. So the ACL and the `chmod` want the same slot, and a `chmod` after the
ACL would rewrite `MASK` and destroy the ACL's fidelity.

The resolution, verified against the kernel:

```
fchmod(0o4600)          -> mode=0o4600  acl=(none)
fsetxattr(access_acl)   -> mode=0o4755  acl=u::rwx u:65534:--- g::--- m::r-x o::r-x
```

`fsetxattr` sets the rwx bits exactly and **preserves the special bits**, so it can be the single
widening step. The invariant the file applier holds, and which both branches are tested for:

> Exactly one step widens the destination FILE from its owner-only create mode, and it is the last
> fallible step in the applier. Without an access ACL that step is the `fchmod`; with one it is the
> `fsetxattr`, and the preceding `fchmod` is narrowed to `(mode & 0o7000) | 0o600` so that it cannot
> widen.

Two facts make the other branch safe. Clearing an inherited ACL can only narrow, so it may run
*before* the widening `chmod`. And removing an access ACL is mode-neutral (a `0o600` file stays
`0o600` across `removexattr`), so the clear does not disturb the mode the `chmod` is about to set.
The **default** ACL is mode-neutral in both directions, so it rides along without constraining the
order.

**Directories take the same two-branch shape but not the "last fallible step" half**, and the
difference is deliberate. The mode step stays in the directory applier's existing chmod slot, with
`futimens` after it: a directory's mtime is bumped by every child created inside it, so its
timestamps can only be applied once its children exist, which makes the directory applier post-order
and puts `futimens` last by necessity. There is no exposure in that ordering — `futimens` does not
touch permissions — but the invariant above is a statement about files, and
[tocttou.md](tocttou.md#scope-of-toctou-safety) is careful to scope it that way too.

One asymmetry follows. A destination file is always freshly created, so the ACL branch's narrowed
`fchmod` only ever holds it at the mode it already had. A destination **directory** may be reused,
and there that same `fchmod` actively narrows it to `0o700` plus its special bits for the moment
between the chmod and the `fsetxattr`. If the `fsetxattr` fails, the copy is reported failed and a
pre-existing directory is left at `0o700` — not the mode it started at. That is fail-closed on
purpose (the alternative publishes the source's mode on a directory whose ACL never landed), but it
is why a destination directory can come back owner-only after a failed `d:acl` copy.

## Failure rules

Both directions refuse to leave a destination whose permission state is neither the source's nor
knowably safe. They are symmetric, and the second is the one that surprises people.

**A destination that cannot HOLD the source's ACL fails the entry.** `EOPNOTSUPP` on a *set* is an
error, not a shrug: the destination would be more permissive than its source, which violates
permission fidelity. By contrast `EOPNOTSUPP` or `ENODATA` on a *remove* is success — there is
nothing to clear, so nothing can have widened.

**An ACL-read failure on the SOURCE fails the entry; it never degrades.** This is the single most
important thing to internalise before touching this code. "Degrade gracefully — carry on without the
ACL" is the instinctive choice and it is **wrong here**, because absent ACLs are not "no
information", they are an instruction to **CLEAR**. A transient `EMFILE` would therefore *strip* the
destination's existing ACLs, including a directory's **default** ACL, which then governs everything
created beneath it afterwards. That can be more destructive than the source-fidelity failure
described above.

**Failing must be accounted, or a remote copy hangs.** A source-side entry that fails before its
message is sent leaves the destination waiting for an entry that never arrives, so it never sends
`DestinationDone` and both peers stay alive forever — no timeout ends that. Every ACL failure path
therefore sends the matching compensation (a `FileSkipped` for a nested entry) or fails the copy
outright (for a root). See [remote_protocol.md §2.5](remote_protocol.md) and §3.3 there.

## Carry the bytes verbatim

**The ACL blobs are never parsed, rebuilt or reordered.** POSIX.1e requires entries in canonical
order — `USER_OBJ`, named users by ascending uid, `GROUP_OBJ`, named groups by ascending gid,
`MASK`, `OTHER` — and the kernel rejects any other order with `EINVAL`. Passing the source's bytes
through unchanged sidesteps this entirely: those bytes were already validated by the source kernel.
The on-disk format is defined little-endian (`__le16`/`__le32`), so it is portable across hosts
as-is, and the destination kernel validates on `fsetxattr`.

Code that *constructs* an ACL — id remapping, a synthesized entry — must sort canonically or it will
fail at `fsetxattr`.

## Remote copies

The wire carries the same opaque bytes. `protocol::Metadata` has an `acls: WireAcls` field — an enum
distinguishing `Captured { access, default }` (authoritative, including the "has none" case) from
`Unknown` (no ACL information at all: capture off, or the source could not read them) — and
`MasterHello::Source` has a
`capture: ExtendedMetadataCapture { file_acl, dir_acl, root_acl_notice }` field so the source knows
whether to read ACLs at all. Without it the source — which is told `preserve` by nobody — would have
to read them unconditionally, landing the per-entry cost on every remote copy including ones that do
not want ACLs. The first two flags drive the per-entry reads. The historically named
`root_acl_notice` field is only the settings bit for the syscall-free
[ACL-preservation notice](#the-acl-preservation-settings-notice); it never populates a `Metadata`.

[remote_protocol.md §2.5](remote_protocol.md) is the authority: which messages carry which ACL, why
a `Captured` `None` means CLEAR while `Unknown` authorizes no source-derived ACL set or clear, and
how a failed read is accounted. Mode application and strict reused-directory lockdown can still
affect destination ACL state. Do not duplicate the protocol details here.

One case is worth naming because a false authoritative absence is destructive. The remote
`--dereference` (`-L`) directory walk does not retain its transient enumeration descriptor for the
later ACL capture, so it cannot read the ACL from the fd whose contents it enumerated. Reporting "no
ACL" would instruct the destination to CLEAR rather than merely omit the source ACL. That walk opens
the directory by path instead, after enumeration succeeds. This is the same concession `-L` makes
elsewhere (it is documented as not TOCTOU-hardened), and avoids silently stripping the destination.

## `--require-toctou-safe` containment

The invariant:

> Under `--require-toctou-safe`, no destination entry that rcp **creates and successfully
> completes** carries an ACL entry that did not come from its source. Every destination directory
> rcp creates or reuses is prevented from passing an inherited ACL on — and in the one directory rcp
> writes into without creating or reusing it (the ambient parent of a direct operand), the created
> entry itself is scrubbed instead.

Both "creates" and "successfully completes" are load-bearing. A create whose ACL cannot be sanitized
fails before rcp writes children/data or applies final metadata; its best-effort slot cleanup can
also fail. A **reused** directory was already there and is outside the created-entry invariant: it
keeps its access ACL present but masked during lockdown. At successful finalize, `d:acl` replaces or
clears both ACLs according to the source; without `d:acl`, the destination access ACL remains and
its original default ACL is restored. The invariant is about what the copy creates successfully, not
about scrubbing a destination tree or promising rollback after a failed create.

One directory kind is left: the **ambient operand parent** — the pre-existing directory a *direct*
operand is written into (`rcp file.txt existing-dir/`), which rcp neither creates nor reuses as a
copy destination. rcp does write there — exactly one entry — so containment needs three sites:

**Freshly created directories.** The `mkdirat`, the re-open, and the removal of both
`system.posix_acl_access` and `system.posix_acl_default` all run inside **one blocking closure**,
which either is reclaimed before it starts or runs to completion after it wins the start/cancel
race. No cancellation (a `--fail-early` sibling abort dropping the future) can therefore abandon a
created directory between those steps. If the re-open or strip fails, the closure attempts one
unconditional `rmdir` of the current slot through the pinned parent, then reports the original
error. This is slot cleanup, not an exact-inode check: it can remove a compatible empty replacement,
while an incompatible or populated replacement and any cleanup error leave the slot occupied. A
surviving directory created by rcp remains copier-owned at `0o700`, with inherited access entries
masked off; rcp neither descends into it nor widens it. This statement relies on the strict-mode
parent write-control and single-writer preconditions: the ambient parent is caller-trusted, and each
nested parent is already owner-only or locked down, so a protected actor cannot substitute a
compatible directory between `mkdirat` and the re-open. The re-open is not an inode-identity proof.
Two ACL syscalls per successful directory and **none per file** beneath one: stripping the default
ACL stops the inheritance chain for the whole subtree.

**Reused directories.** The lockdown that restricts a reused destination directory to `0o700` for
the copy's duration already neuters its *access* ACL (the `chmod` rewrites `MASK`), but a `chmod`
does not touch a **default** ACL, so children created during the copy would still inherit it. The
lockdown therefore also **snapshots and removes the default ACL** for the copy's duration and puts
it back at finalize when `d:acl` is off. Its access ACL is not stripped: the lockdown masks it, then
ordinary directory metadata application either leaves it in place (`d:acl` off) or replaces/clears
it from the source (`d:acl` on).

**Direct files in the ambient parent.** A file created there has no sanitized directory above it, so
`create_file` removes the inherited access ACL **inside the creation closure** (files carry no
default ACL, so one attribute suffices). Without it the inherited entries sit inert under the
owner-only create mode — and the final chmod to the source mode re-derives `ACL_MASK` from the group
bits and activates them: a strict copy of a plain `0640` file would grant a named user effective
read access its source never did. Files created beneath rcp-created or locked directories skip this
(the chain was already broken above them), so the cost is per direct operand, not per file. If the
strip fails, the closure attempts one unconditional unlink of the current slot through the pinned
parent and returns the strip error. A compatible non-directory replacement can be removed; a
directory or failed cleanup can remain. The file rcp created contains no copied data, stays at
`0o600` with inherited entries masked off, and is never widened.

At finalize a locked directory's default ACL is resolved through the lockdown guard, one of two
ways: with `d:acl` on, the **source's** default ACL is installed (nothing to write when the source
has none — the lockdown already left the directory bare, which is the correct end state); otherwise
the snapshot is restored (nothing to do when there was none). The guard stays **armed** through
every remaining fallible step and cancellation point up to the successful return, so a cancelled or
failed finalize rolls back rather than keeping the source's ACL — and armed even when the directory
originally had no default ACL, because the rollback then means *removing* whatever a partial
finalize installed, not leaving it. Every finalize write to the guarded attribute is **serialized
against the guard's rollback**. A write still queued when its waiter is dropped is reclaimed before
it starts, but one already taken by a blocking worker cannot be cancelled; without serialization,
that write could land *after* the `Drop` rollback and silently undo it. A write that finds the guard
already disarmed is skipped instead.

Restoring a snapshot runs **first**, unwinding the lockdown in the reverse of the order it was
applied — ACL removed last, so restored first; then the owner; then the mode. Two reasons, neither
of them about the mode:

- **It runs while the copier still owns the directory**, before the owner is handed back. Only an
  owner or a `CAP_FOWNER` copier may write these attributes at all.
- **It fails toward *unchanged*.** If a later step fails, the directory still holds the ACL it had
  before the copy. Deferring the restore past the chmod would fail toward *damaged* instead — a
  directory permanently stripped of an ACL it owned before rcp ran, which is the one outcome this
  path is required to treat as an error rather than produce.

The restore does not compete with the finalize `chmod` for the widening slot, because only a
**default** ACL is restored and a default ACL is mode-neutral. Any implementation that also restores
an *access* ACL must perform that restore before the chmod: the snapshot's `USER_OBJ`/`MASK`/`OTHER`
agree with the directory's original mode, not the source's, so a later restore would install the
wrong rwx bits.

Failing to *strip* at lockdown time with `EOPNOTSUPP` is tolerated (a filesystem without ACL support
cannot have had one). Failing to *restore* at finalize is an error: the directory would permanently
lose an ACL it had before the copy. The restore also runs from a `Drop` guard, so a copy that is
aborted between lockdown and finalize — by `--fail-early`, or by any per-directory error — does not
destroy the snapshot it is holding the only copy of.

### A consequence, not an oversight

Under `--require-toctou-safe` **without** `d:acl`, a freshly created destination directory ends with
no ACL **even if its parent's default ACL would ordinarily have given it one**. An administrator who
set a default ACL on a destination tree expecting new subdirectories to inherit it will not get that
under this flag.

That is the intended trade, and it should read as a decision: the flag's contract is that the
destination reflects its source and nothing else, so **containment beats inheritance**. A user who
wants the source's ACLs instead of none adds `d:acl`. A user who wants the destination tree's
inheritance policy honored should not be using this flag on that tree.

### The two flags close different bugs

|                         | 1. dropped source ACL | 2. inherited destination ACL  |
| ----------------------- | --------------------- | ----------------------------- |
| default                 | open                  | open                          |
| `all+acl`               | closed                | closed, repaired at finalize  |
| `--require-toctou-safe` | **open**              | closed, prevented at creation |
| both                    | closed                | closed, prevented at creation |

`--require-toctou-safe` alone never makes a destination *wider* than it would otherwise be — the
strip only ever narrows — but it does nothing for a source whose own ACL was restrictive, because
direction 1 is a fidelity gap present in every copy rather than a race.

**So pair `--require-toctou-safe` with `--preserve-settings=all+acl`** when the source's ACLs
matter. The pairing also buys something neither gives alone: with `acl` only, direction 2 is
repaired at finalize, so a child transiently carries inherited entries during the copy (inert,
because the create mode leaves the mask empty — but present); under strict mode they never exist at
all.

This is deliberately **not** implemented as an implication. The flags are orthogonal — containment
against races versus fidelity to the source — and auto-enabling `acl` would impose the per-entry ACL
read on a flag people reach for a different reason, while silently overriding an explicit
`--preserve-settings=none`. It remains a recommendation and a settings-derived notice.

## Tool coverage

| tool                    | ACLs                                                                                                       |
| ----------------------- | ---------------------------------------------------------------------------------------------------------- |
| `rcp`, local and remote | files and directories, both ACL kinds                                                                      |
| `rlink`                 | `d:acl` applies to directories exactly as in `rcp`; `f:acl` applies only on the real copy path — see below |
| `rcmp`                  | not compared; see [Non-goals](#non-goals)                                                                  |
| `rchm`                  | out of scope                                                                                               |

**`rlink` and the shared inode.** A hard-linked destination entry *is* the source inode, so writing
an ACL through it would mutate the **source**. `f:acl` therefore never applies to a hard link; it
applies only where `rlink` really copies a file (a changed file under `--update`). Directories have
no such constraint — `rlink` creates or reuses a real destination directory of its own — so `d:acl`
goes through the same applier as `rcp`'s, lockdown and all. It takes the source directory's ACLs, or
the `--update` tree's where that is the version being materialized, matching how `rlink` already
chooses which directory's mode and ownership to apply.

## Known holes

**`--overwrite` skips ACL-only differences.** A file that compares equal under `--overwrite-compare`
(default `size,mtime`) is not transferred and keeps its old ACL. This is the same shape as `mode`,
which the default comparison also ignores. Closing the gap requires an `acl` term in the comparison
DSL and ACL bytes in the destination manifest.

**Inherited destination ACLs are not contained on the default path.** Widening direction 2 stands
for a copy that requests neither `acl` nor `--require-toctou-safe`. This is deliberate: containing
it costs either the per-entry probe or the per-directory strip, and neither is worth imposing on
ordinary data movement for what is a privileged-copy concern.
[Who pays to contain inheritance](#who-pays-to-contain-inheritance) tabulates the alternatives.

**NFSv4 ACLs are neither handled nor detected.** A source on an NFSv4-ACL filesystem
(`system.nfs4_acl`) has no `system.posix_acl_access`, so it reads as "no ACL" — and, because absent
means CLEAR, an `acl` copy onto such a destination clears rather than preserves. NFSv4 is a
different model, not a different encoding of this one.

## Non-goals

- **Non-Linux platforms.** The whole fd-based hardened path is Linux-only, and so is this.
- **Other extended attributes.** Only `system.posix_acl_access` and `system.posix_acl_default` are
  read or written. There is no `-X`/`--xattrs` equivalent; the `+` preset grammar leaves room for
  one.
- **Interpreting, remapping or synthesizing ACL entries.** The bytes are carried verbatim; uids and
  gids inside an ACL are not translated between hosts, exactly as `uid`/`gid` preservation is not.
- **Comparing ACLs.** `rcmp` and `--overwrite-compare` have no `acl` term.
