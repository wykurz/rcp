# POSIX ACLs in the RCP tools

This document is the single home for how the RCP tools handle POSIX.1e access control lists: what
`--preserve-settings=all+acl` does, why ACLs are opt-in rather than part of `all`, the two
independent ways a copy can end up **more permissive than its source**, and what
`--require-toctou-safe` does and does not fix. It covers `rcp` (local and remote) and `rlink`.

If you only read one thing: **`--preserve-settings=all` does not preserve ACLs**, and on a
destination tree that has default ACLs a copy can gain permissions its source never granted. Use
`all+acl` when either matters.

## Table of Contents

- [The model](#the-model)
- [Two ways a copy widens](#two-ways-a-copy-widens)
- [Who pays to contain inheritance](#who-pays-to-contain-inheritance)
- [Why `acl` is opt-in](#why-acl-is-opt-in)
- [The CLI surface](#the-cli-surface)
- [The source-root warning](#the-source-root-warning)
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

Both were reproduced on ext4 while this was designed. They are independent; closing one does not
close the other.

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

This is the reported bug, and it is a **fidelity** gap present in every copy — not a race. `acl`
closes it.

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
`listxattr` plus an `fgetxattr` to snapshot the default ACL and an `fremovexattr` to strip it at
lockdown, then an `fsetxattr` to put it back at finalize. Still per-directory, and still nothing per
file — with one narrow exception: a file created **directly in the ambient operand parent** (a
direct file operand's destination, the one directory rcp neither creates nor locks down) pays a
single `fremovexattr` inside its create, because no directory-level strip ever ran there. That is
one syscall per *operand*, not per file of a tree.

## Why `acl` is opt-in

There is no bit in `stat` saying whether an entry has an ACL. Detecting one costs a syscall, on
every entry. Measured on ext4, 20k files, best of 3:

| operation                                       | ns/entry |
| ----------------------------------------------- | -------- |
| `stat` (what rcp already pays per entry)        | 949      |
| `getxattr(system.posix_acl_access)` → `ENODATA` | 1057     |
| `listxattr`                                     | 591      |

So ACL preservation adds one syscall of roughly `stat`'s cost to every file, and two to every
directory (which has both an access and a default ACL). Against the handful of metadata ops a copy
already pays per entry — a copied file costs about half a dozen — that is a fifth to a third more
metadata work, not a doubling. Small per entry; not small across a tree of millions, and paid by
every user of the flag whether or not a single entry turns out to have an ACL. That is why `all`
excludes ACLs and `all+acl` opts in.

The read probe is therefore `listxattr`-first: `listxattr` is materially cheaper than a `getxattr`
that misses, and misses are the overwhelming majority. A `getxattr` is issued only for a name the
list actually contained.

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
`none`; each remaining token is a modifier, of which `acl` is currently the only one. An unknown
modifier is an error naming the valid set, and repeats are idempotent (`all+acl+acl` is `all+acl`).
Any string whose first `+`-token is not `all` or `none` falls through to the per-type DSL exactly as
before, so `+` remains an ordinary character there and no existing settings string changed meaning.

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

## The source-root warning

`all` not preserving ACLs is only defensible if a user who has not read this page finds out. So one
`listxattr` on the **source root** per run — a constant, free at any tree size — warns once when the
root carries an ACL while `acl` is not requested, **provided the run asked for metadata fidelity in
the first place**:

```
WARN Source root "/data/project" carries a POSIX ACL that this copy will NOT preserve
     (`--preserve-settings` does not request `acl`), so the destination may end up more permissive
     than its source. Use `--preserve-settings=all+acl`. Only the root was checked - entries beneath
     it may carry ACLs this says nothing about.
```

Under `--require-toctou-safe` the same probe, at the same cost, carries a sharper message: strict
mode contains inherited destination ACLs but does **not** preserve the source's, so the copy can
still end up more permissive than what it copied — see
[the two flags close different bugs](#the-two-flags-close-different-bugs).

### What arms it

The notice is armed by the run **asking** for the fidelity a dropped ACL would undermine, not by an
ACL merely existing. Two things arm it, independently:

- The resolved `--preserve`/`--preserve-settings` requesting anything beyond the shipped default —
  `all`, a bare `f:uid`, a `7777` mode mask, anything.
- `--require-toctou-safe`, whatever the preserve settings say. It is a request about permissions on
  its own, and it is where a user is most likely to assume the source's ACLs came along; it gets the
  sharper wording above.

Everything else is silent, probe included — a copy that did not ask pays nothing at all, not even
the one `listxattr`:

| Invocation                                | Notice                          |
| ----------------------------------------- | ------------------------------- |
| `rcp src dst`                             | silent                          |
| `rcp --preserve-settings=none src dst`    | silent                          |
| `rcp --preserve-settings=all src dst`     | warns                           |
| `rcp --preserve-settings=d:uid src dst`   | warns                           |
| `rcp --require-toctou-safe src dst`       | warns (strict wording)          |
| `rcp --preserve-settings=all+acl src dst` | silent — the ACLs are preserved |
| `rlink src dst`                           | warns — rlink defaults to `all` |

The reason a bare `rcp` is silent is consistency about what the default claims. Left alone, rcp
reproduces the source's `rwx` bits like `cp` and nothing else: uid, gid, timestamps and the
setuid/setgid/sticky bits are all dropped without a word. A run that makes no claim about the
destination's metadata does not need advice about one more attribute it also does not carry, and
singling ACLs out there would be the one loud omission among several silent ones. Ask for anything —
that is the claim — and the advice becomes worth printing.

`rlink` sits on the other side of the same rule rather than being an exception to it: its default
**is** `all`, because metadata fidelity is what the tool is for, so a bare `rlink` has asked. Only
`rlink --preserve-settings=none` (or settings that resolve back to the shipped default) turns the
notice off there.

The gate reads the RESOLVED settings, and the baseline it compares against is the **shipped
default** (`preserve_none`) — not whatever the tool would have used had you passed nothing. So
`--preserve-settings=none` and its longhand spelling `f:0777 d:0777` are equivalent to each other
and both silent, in either tool. There is no separate "was the flag typed" bit.

The two baselines coincide for `rcp`, whose CLI default *is* the shipped default, so there passing
nothing and passing `none` behave alike. They do not coincide for `rlink`, whose CLI default is
`all`: `rlink src dst` warns while `rlink --preserve-settings='f:0777 d:0777' src dst` is silent.
Nothing special-cases the tool — the two invocations resolve to different settings.

**This is a heuristic, not a guarantee.** A root without an ACL says nothing about its children, and
the probe deliberately does not walk: probing enough entries to be sure *is* the per-entry cost that
made `acl` opt-in. Two more limits worth knowing:

- It fires at most **once per process** — first probe wins. The budget is claimed as late as
  possible, after the root's kind and the per-kind settings are known, so a root that could not have
  warned (a symlink, or a kind whose ACLs are being preserved) does not consume it. But **which**
  root wins is not ordered: a multi-operand run copies its operands concurrently, so with several
  eligible roots the one that gets there first is whichever the scheduler ran first, and a root that
  would have warned can be silenced by one that had nothing to say. The cap exists to bound a
  `--dereference` walk, which re-enters the copy entry point per resolved symlink; making it
  per-operand instead is a larger change than the cap is worth.
- It consults the setting for the root's own kind, so `f:acl` does not silence a warning about a
  directory root. The root is classified `O_NOFOLLOW`, so a symlink root is skipped — a symlink has
  no ACL, and under `-L`, where its target does the copying, the target is not probed either.

For `rlink` the probe covers **directory** roots only. A hard-linked destination file *is* the
source inode, so its ACL cannot be dropped — and must never be written through, which is why `f:acl`
applies only on rlink's real copy path (see [Tool coverage](#tool-coverage)).

### Where the notice shows up

It is printed at the **default verbosity** — no `-v` needed. That is the whole point: a user who
does not know `all` excludes ACLs is exactly the user who will not think to ask for more output.

It gets there without making anything else noisier. The notice carries its own tracing target,
`rcp::notice`, and that target has its own `warn` directive; a target directive is more specific
than the global level, so it wins for this one target and nothing more. Raising the global default
to `warn` instead would have unmuted every other `warn!` in the tools, 14 of which sit in per-entry
paths (`Skipping directory {:?} - ancestor failed to create` and friends), so a single failed
subtree would print thousands of lines. Every other warning still needs `-v`.

On a **remote** copy the probe runs on the source `rcpd`, where the root is. The notice keeps its
target across the wire — the master re-emits forwarded notices on `rcp::notice` rather than on the
blanket `remote` target — so it renders at the default verbosity there too, prefixed
`remote::source::` like any other forwarded line. Both ends need the directive: without it the
source `rcpd`, itself running at the default verbosity, would not even send the notice.

Turning it off:

- **`--quiet`** suppresses it, along with all other output. That is the supported way.
- **`RUST_LOG` cannot.** Like the tools' other built-in directives (`tokio`, `quinn`, `rustls`,
  `h2`, and the verbosity level itself), the `rcp::notice` directive is applied *after* `RUST_LOG`
  is read and takes precedence for that target. `RUST_LOG` still raises verbosity for targets rcp
  does not name — `RUST_LOG=common=debug` works as before.

It is a `warn!` and not an `error!`, and the copy's exit code is unchanged. Failing on it would be
wrong: the probe sees only the root, so the same tree copied one level deeper would not trigger it,
and a failure on a signal that arbitrary reads as capricious. Where rcp *does* fail — a destination
that cannot hold the ACL, or a source ACL it cannot read — the user asked for `acl` and rcp could
not deliver it. Here they did not ask.

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
error, not a shrug: the destination would be more permissive than its source, which is the bug being
fixed. By contrast `EOPNOTSUPP` or `ENODATA` on a *remove* is success — there is nothing to clear,
so nothing can have widened.

**An ACL-read failure on the SOURCE fails the entry; it never degrades.** This is the single most
important thing to internalise before touching this code. "Degrade gracefully — carry on without the
ACL" is the instinctive choice and it is **wrong here**, because absent ACLs are not "no
information", they are an instruction to **CLEAR**. A transient `EMFILE` would therefore *strip* the
destination's existing ACLs, including a directory's **default** ACL, which then governs everything
created beneath it afterwards. That is worse than the original bug.

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

Any future code that *constructs* an ACL — id remapping, a synthesized entry — must sort canonically
or it will fail at `fsetxattr`.

## Remote copies

The wire carries the same opaque bytes: `protocol::Metadata` gained an `acls: WireAcls` field — an
enum distinguishing `Captured { access, default }` (authoritative, including the "has none" case)
from `Unknown` (no ACL information at all: capture off, or the source could not read them) — and
`MasterHello::Source` gained a
`capture: ExtendedMetadataCapture { file_acl, dir_acl,
root_acl_notice }` field so the source knows
whether to probe at all. Without it the source — which is told `preserve` by nobody — would have to
probe unconditionally, landing the per-entry cost on every remote copy including ones that do not
want ACLs. The first two flags drive the per-entry reads; `root_acl_notice` only arms the
one-per-run [source-root notice](#the-source-root-warning) and never populates a `Metadata`.

[remote_protocol.md §2.5](remote_protocol.md) is the authority: which messages carry which ACL, why
a `Captured` `None` means CLEAR while `Unknown` means the destination's ACLs are left untouched, and
how a failed read is accounted. Do not duplicate it here.

One case is worth naming because getting it wrong would have been actively destructive. The remote
`--dereference` (`-L`) directory walk holds **no directory fd**, so it cannot read a directory's ACL
from the fd whose contents it enumerated. Reporting "no ACL" would not merely have failed to carry
the source's ACLs — an authoritative absence is an instruction to CLEAR, so it would have **stripped
the destination's**. So that walk opens the directory by path instead, after enumeration succeeds.
That is the same concession `-L` already makes everywhere else (it is documented as not
TOCTOU-hardened), and it is the honest answer rather than a silently destructive one.

## `--require-toctou-safe` containment

The invariant:

> Under `--require-toctou-safe`, no destination entry that rcp **creates** carries an ACL entry that
> did not come from its source. Every destination directory rcp creates or reuses is prevented from
> passing an inherited ACL on — and in the one directory rcp writes into without creating or reusing
> it (the ambient parent of a direct operand), the created entry itself is scrubbed instead.

The "that rcp creates" is load-bearing, not throat-clearing. A **reused** directory is one that was
already there, and rcp was not asked to change it: it keeps its own access ACL throughout, and the
default ACL it had before the copy is put back at the end. The invariant is about what the copy
*writes*, not about scrubbing a destination tree.

One directory kind is left: the **ambient operand parent** — the pre-existing directory a *direct*
operand is written into (`rcp file.txt existing-dir/`), which rcp neither creates nor reuses as a
copy destination. rcp does write there — exactly one entry — so containment needs three sites:

**Freshly created directories.** The `mkdirat`, the re-open, and the removal of both
`system.posix_acl_access` and `system.posix_acl_default` all run inside **one blocking closure**,
which runs to completion once submitted: no cancellation (a `--fail-early` sibling abort dropping
the future) can abandon a created-but-unsanitized directory, and a directory whose open or strip
fails is removed (best-effort, it is empty) rather than left carrying inherited ACLs an
indistinguishable rerun would then faithfully "restore". Two syscalls per directory and **none per
file** beneath one: stripping the default ACL stops the inheritance chain for the whole subtree.

**Reused directories.** The lockdown that restricts a reused destination directory to `0o700` for
the copy's duration already neuters its *access* ACL (the `chmod` rewrites `MASK`), but a `chmod`
does not touch a **default** ACL, so children created during the copy would still inherit it. The
lockdown therefore also **snapshots and removes the default ACL** for the copy's duration and puts
it back at finalize. Its access ACL is deliberately left alone: rcp was not asked to change a
directory that already existed.

**Direct files in the ambient parent.** A file created there has no sanitized directory above it, so
`create_file` removes the inherited access ACL **inside the creation closure** (files carry no
default ACL, so one attribute suffices). Without it the inherited entries sit inert under the
owner-only create mode — and the final chmod to the source mode re-derives `ACL_MASK` from the group
bits and activates them: a strict copy of a plain `0640` file would grant a named user effective
read access its source never did. Files created beneath rcp-created or locked directories skip this
(the chain was already broken above them), so the cost is per direct operand, not per file.

At finalize a locked directory's default ACL is resolved through the lockdown guard, one of two
ways: with `d:acl` on, the **source's** default ACL is installed (nothing to write when the source
has none — the lockdown already left the directory bare, which is the correct end state); otherwise
the snapshot is restored (nothing to do when there was none). The guard stays **armed** through
every remaining fallible step — including the final re-stat verification and every cancellation
point up to the successful return, so a cancelled or verify-rejected finalize rolls back rather than
keeping the source's ACL — and armed even when the directory originally had no default ACL, because
the rollback then means *removing* whatever a partial finalize installed, not leaving it. Every
finalize write to the guarded attribute is **serialized against the guard's rollback**: finalize
runs on a blocking pool that cannot be cancelled, so a dropped future detaches its write, and an
unserialized write could land *after* the `Drop` rollback and silently undo it. A write that finds
the guard already disarmed is skipped instead.

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
**default** ACL is ever restored and a default ACL is mode-neutral. That is a consequence of the
lockdown having narrowed to the default ACL alone; it was not always true. While the lockdown also
stripped the *access* ACL, the restore had to precede the chmod for a third reason — the snapshot's
`USER_OBJ`/`MASK`/`OTHER` agree with the directory's *original* mode, not the source's, so a late
restore would install the wrong rwx bits and the finalize re-stat would reject the copy. Anything
that reintroduces an access-ACL restore here inherits that constraint.

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
against races versus fidelity to the source — and auto-enabling `acl` would impose the per-entry
probe on a flag people reach for a different reason, while silently overriding an explicit
`--preserve-settings=none`. It is a recommendation, and — when the root probe has evidence for it —
a warning.

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
which the default comparison also ignores. Closing it means an `acl` term in the compare DSL and ACL
bytes in the destination manifest; that is a natural follow-up.

**Inherited destination ACLs are not contained on the default path.** Widening direction 2 stands
for a copy that requests neither `acl` nor `--require-toctou-safe`. This is deliberate: containing
it costs either the per-entry probe or the per-directory strip, and neither is worth imposing on
ordinary data movement for what is a privileged-copy concern.
[Who pays to contain inheritance](#who-pays-to-contain-inheritance) tabulates the alternatives.

**The source-root warning is a heuristic**, with the limits listed
[above](#the-source-root-warning).

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
