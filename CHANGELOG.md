# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- `--max-files-in-flight=N` now caps applicable file-like work across the tools. Explicit values are
  positive (including `1`); when omitted, the common default is
  `max(std::thread::available_parallelism(), 4)`, including for `filegen`. This is a work ceiling,
  not a process-descriptor count or a guarantee of achieved concurrency. When `RLIMIT_NOFILE` is
  available, the runtime still applies its descriptor-safety ceiling independently to OpenFile and
  PendingMeta, which have the same effective numerical limit but are not a combined pool. If the
  query fails, a finite user-supplied limit remains usable as the sole admission ceiling and emits a
  default-visible notice; automatic and unlimited admission still fail closed. A successfully
  queried zero soft limit fails closed for every policy. For `filegen`, the legacy
  `--max-open-files=0` spelling previously disabled admission entirely; it now has the common
  compatibility meaning of removing the user ceiling while retaining descriptor safety, and emits a
  deprecation notice.

- Remote automatic file concurrency is now source-owned: the source `rcpd` resolves
  `F = max(std::thread::available_parallelism(), 4)` from its own CPU availability and reports that
  logical file ceiling plus effective stream count `E = min(F, --max-connections)` in its readiness
  record; the destination is then started with those exact values. Explicit
  `--max-files-in-flight=N` and legacy `--max-open-files` inputs remain master-authoritative.
  Effective-stream and pending-task capacities above Tokio's semaphore maximum are rejected without
  panic. Explicit capacity is validated before remote `~` expansion. For automatic policy, the
  master validates the configured connection upper bound before remote side effects; the source
  validates its actual CPU-selected capacity before readiness and before the destination is spawned.
  Each endpoint still applies descriptor safety locally. The daemon spawn configuration now makes
  explicit file limits finite by type; unlimited admission is carried only with legacy provenance,
  and the unreachable daemon-only `--explicit-unlimited-files-in-flight` argument was removed.

- Remote startup now prepares same-host source/destination daemons once, starts and connects the
  source before constructing the destination, and reserves the first successful daemon stderr line
  for the extended `RCP_TCP`/`RCP_TLS` readiness record. Profiling, Tokio-console, compatibility,
  and explicit clamp announcements flow through the default-visible `rcp::notice` tracing target;
  explicit stream or descriptor clamps warn, while the ordinary automatic/default intersection
  remains quiet. Local `rcpd --protocol-version` probes are bounded at two seconds and capture only
  bounded diagnostics; remote probes, including post-deployment verification, use
  `--remote-copy-conn-timeout-sec` (15 seconds normally and 60 seconds with auto-deployment); zero
  is rejected because every bootstrap stage requires a real deadline. Every read-only remote
  bootstrap command — HOME lookup, executable checks, PATH discovery, and version probes — uses that
  required deadline, so a blocked filesystem lookup cannot stall endpoint preparation indefinitely.
  The timeout applies independently to each stage. Remote and local PATH discovery use the shell's
  POSIX `command -v` rather than the external `which`; an ordinary local PATH miss is retained in
  the deployment-candidate diagnostic. An absent remote `HOME` skips deployed-cache discovery but
  still fails remote `~` expansion or deployment, which require a home directory. Deployment waits
  for a bounded `RCP_DEPLOY_READY` marker after the remote shell has installed its cleanup trap and
  opened the staging file, then applies an idle timeout to each payload write; it does not impose a
  total transfer-time cap. Post-EOF verification has a bounded stage of at least 60 seconds. On peer
  cancellation, the transfer gets a bounded grace before its local SSH-channel task is aborted and
  transferred to the typed cleanup scope; the endpoint coordinator never awaits an uncooperative
  owner without a bound, while a raced successful result is still disposed off its Tokio worker.
  Old-version cleanup is also bounded. Local candidate execution happens behind a fixed-shell child
  so a stalled candidate filesystem remains inside the two-second probe deadline.

- Local auto-deployment candidate probes now observe peer-preparation cancellation before, during,
  and after each version check, using the same pipe-close, kill, and reap cleanup as a timeout
  without rejecting the candidate and falling through. Auto-deployment targets Linux because its
  fail-closed publication relies on GNU/BusyBox `mv -T`; stock macOS and BSD remote hosts need a
  matching `rcpd` installed manually. Reading the selected local deployment payload now uses a
  cleanup-owned disposable OS worker under the configured bootstrap deadline and peer cancellation,
  so a blocked local filesystem cannot retain Tokio runtime or remote-resource ownership.

- SSH multiplex masters now remain owned foreground processes; command-line overrides disable both
  `ForkAfterAuthentication` and `ControlPersist` even when SSH configuration enables them. The
  configured SSH executable is resolved inside a known-shell child, and subsequent commands use the
  native multiplex protocol over the retained control socket instead of synchronously spawning a
  fresh local `ssh`; the configured deadline can therefore cancel every local exec-channel setup.
  Cancellation or the configured remote setup deadline terminates the actual connecting process. A
  preparation guard owns that launcher and its private control directory together until it transfers
  both to the successful-session owner. A cleanup supervisor is started before any remote resource
  is accepted; either exit signals the process immediately and queues reaping there, and the control
  directory is removed only after the process is confirmed exited. Reapers for SSH masters and
  interrupted local candidates now require the cleanup scope's typed budget. Finalization shortens
  only jobs submitted before it began; a later job from another live cleanup owner receives a fresh
  budget instead of inheriting an expired deadline. A child that never becomes reapable therefore
  cannot leave a cleanup thread polling forever; an unconfirmed SSH master keeps its control
  directory when that budget expires. Cleanup never falls back to the original resource-owning
  submitter: worker-spawn failure stays on the supervisor, while a failed supervisor channel tries
  an isolated worker before leaking the job for process-exit reclamation. Nested cleanup remains in
  its parent worker, independent invocations cannot drain one another's workers, and the CLI gives
  the scope one bounded budget for its final resource owner, supervisor, and queued jobs. SSH
  control-socket readiness uses one disposable filesystem worker for its polling lifetime rather
  than creating a thread per poll. Daemon waits run concurrently across endpoints while tracing
  receivers remain live; any receiver tasks left afterward share one final drain deadline.
  Control-directory selection tries runtime, temporary, state, and home locations before reporting
  that no safe Unix-socket path is available. Remote daemon SSH exec-channel creation and readiness
  reads use that same configured deadline; readiness records larger than 64 KiB are rejected. The
  master threads the actual local operand roots through both remote-HOME lookup and endpoint
  preparation, so control-directory selection rejects canonical temporary-directory aliases inside a
  copied tree without treating the process working directory as an operand. Remote-to-remote copies
  therefore retain normal runtime and temporary candidates even when launched from `/`; the
  filesystem root itself is the sole non-excluding operand because every absolute candidate lies
  beneath it. Daemon configuration refusals discovered before tracing use the structured `RCP_ERROR`
  startup record, and unstructured startup failures retain captured stdout/stderr in the error
  chain. Failed-startup reaping now keeps the SSH child and both output drains lexically inside its
  bounded timeout future, so grace expiry or caller cancellation releases the managed session
  instead of detaching ownership. The source tracing receiver starts before destination bring-up and
  is drained on later startup failures, preserving queued source notices beside the destination
  error. Source connection failures also wait for daemon cleanup. Daemon stdout/stderr is forwarded
  live at debug verbosity while a bounded tail is retained and joined for completion diagnostics;
  deployment likewise drains bounded diagnostics while preferring the remote failure over a
  secondary stdin-shutdown error. An explicit connection request clamped by the source file ceiling,
  or an automatic source file ceiling clamped by an explicit connection request, now produces a
  default-visible notice; legacy file-limit notices preserve the `--max-open-files` spelling the
  user supplied.

- `remote::TcpConfig` now contains transport settings only; validated file/stream capacities are
  represented by `ResolvedRemoteConcurrency`. Its fluent setters were removed, and callers
  initialize the public fields from `TcpConfig::default()` instead. The deployment module is
  private, and all zero-caller top-level prepare/start convenience wrappers were removed. Repository
  callers use the cancellation-, timeout-, and cleanup-aware endpoint implementation directly. These
  are intentional breaking changes to an internal support crate. The internal `rcp-tools-throttle`
  API likewise removes the zero-caller `set_max_open_files` compatibility wrapper;
  `set_admission_limits` now takes one typed optional capacity and applies it to both independent
  admission pools.

- Local recursive copy, remove, chmod, and rlink walks now bound descriptor-heavy leaf work on wide
  or slow trees, preventing `EMFILE` (Too many open files) failures from unbounded leaf fan-out. On
  NFS and other filesystems that report `DT_UNKNOWN`, rlink classifies unknown source and update
  entries in their scheduled workers, so type-sensitive filtering, keep-going errors, and directory
  skip accounting use the entry those workers actually opened. Destructive or observable shared walk
  filters likewise classify the exact entry in the worker that acts on it, so stale directory hints
  cannot authorize an action or an incorrect `--delete` keep decision. Ordinary reliable-hint
  exclusions remain cheap, and hinted directories do not consume leaf admission merely to descend.

- In the shared local copy/remove/chmod walks and rlink machinery, fail-early now waits for
  recursively spawned async descendants to quiesce before returning. Rlink's source and update
  producers also reap a ready child error before newly available admission can launch more work.
  These guarantees do not extend to every `rcpd` traversal. Blocking work that has already started
  remains outside the async wait so the bounded runtime shutdown can still abandon storage that
  never responds.

- Local regular-file copies now use the admitted `O_PATH` metadata as a point-in-time snapshot for
  dry-run accounting and the initial overwrite comparison. Dry runs and identical/newer skips return
  without an `O_RDONLY` payload open; actionable `Vacant` or `Replace` plans open the current
  payload, and a changed `(dev, ino)` makes an overwrite candidate re-plan from the payload fd
  before any destination mutation. This does not freeze the source name: a later same-parent,
  same-type replacement after a metadata-only skip remains an accepted race.

- Local `--delete` exclude protection now honors keep-going when exact entry classification fails:
  the error is reported, later siblings are still processed, and completed removal totals are
  retained. With fail-early, the traversal stops after reporting only work completed before the
  failed entry.

- Recursive removal now verifies the walked directory before descent and again before `rmdir`. After
  a successful removal, a zero link count proves the pinned inode was unlinked; filesystems that
  instead report `ENOENT` or `ESTALE` are accepted as a logged removed-but-unqueryable outcome. A
  nonzero link count or any other error still fails closed.

- The source-root ACL notice now fires only when the run **asked** for the fidelity a dropped ACL
  would undermine: `--preserve`/`--preserve-settings` requesting anything beyond the shipped
  default, or `--require-toctou-safe`. A bare `rcp src dst` is silent, probe included — it pays not
  even the one `listxattr`. Left at the default, `rcp` reproduces the source's `rwx` bits like `cp`
  and drops uid, gid, timestamps and the setuid/setgid/sticky bits without a word, so warning about
  one more attribute it also does not carry was the single loud omission among several silent ones.
  The gate reads the resolved settings against the **shipped default**, so
  `--preserve-settings=none` — and any spelling that lands back on that default, such as
  `f:0777 d:0777` — counts as "did not ask" in every tool. A bare `rlink` is therefore unchanged,
  since its CLI default is `all` rather than the shipped default; `rlink --preserve-settings=none`
  goes quiet like everything else that asked for nothing.

- `WIRE_REVISION` is now 5. Revision 2 remains the historical wire-schema change: carrying the
  arming flag to a remote source added a field to `MasterHello::Source`'s `capture`, which reshaped
  the hello. Revision 3 protected the first version-sensitive rcpd file-limit spawn contract;
  revision 4 protects source-owned negotiation, typed internal override arguments, and readiness
  records that now carry `F` and `E`. Revision 5 protects the final daemon CLI contract: the
  unreachable `--explicit-unlimited-files-in-flight` argument was removed and
  `--remote-copy-conn-timeout-sec=0` is now rejected. Revisions 3–5 do not change serialized message
  schemas. Without the revision bumps, a stale same-version `rcpd` (on `PATH` or in the deploy
  cache) could pass compatibility and then misdecode the hello, receive unsupported spawn arguments,
  or emit an obsolete readiness record. Builds now advertise `0.39.0+w5`, and a cached
  `rcpd-0.39.0-w4` is no longer resolved.

## [0.38.0] - 2026-08-05

### Added

- POSIX ACL preservation, opt-in via `--preserve-settings=all+acl` or a per-type `acl` attribute
  (`f:acl` / `d:acl`), for `rcp` (local and remote) and `rlink`. Both `system.posix_acl_access` and,
  on directories, `system.posix_acl_default` are carried; the bytes cross the wire verbatim, since
  POSIX.1e requires canonical entry order and the kernel rejects anything else with `EINVAL`, so
  passing through what the source kernel already validated sidesteps the problem. This closes two
  independent ways a copy could end up **more permissive than its source**, both of which were
  reproduced on ext4. First, the reported one: a source ACL entry narrower than `other` acts as a
  deny in effect — POSIX.1e has no literal deny entry, but no mode can express "everyone except this
  uid" — so copying the mode and dropping the ACL granted exactly what the source withheld. Second,
  one found while designing the fix, which needs nothing unusual on the source: a destination
  directory carrying a **default** ACL passes it to every entry created beneath it, including the
  directories rcp creates itself, and rcp's own finalize `chmod` then re-derives the ACL mask from
  the mode's group bits and makes those inherited entries *effective*. The design consequence is
  that faithful preservation means **clearing** as well as setting — a source with no ACL requires
  an explicit removal on the destination — and that in turn is why an ACL that cannot be read on the
  source **fails** the entry rather than degrading: an absent ACL is an instruction to clear, so a
  transient `EMFILE` that degraded would *strip* the destination's ACLs, a directory's inheritance
  policy included.

  `--preserve-settings=all` deliberately does **not** include ACLs, and neither does the deprecated
  `--preserve`. There is no bit in `stat` saying whether an entry has an ACL, so merely detecting
  one costs a syscall on every entry — measured at 1057 ns against the 949 ns `stat` rcp already
  pays, roughly doubling the per-entry metadata cost of the flag most people reach for by default.
  The read probe is `listxattr`-first (591 ns, and a miss is the overwhelming majority) with a
  `getxattr` only for a name actually present, and every ACL syscall goes through the same
  congestion gate as `stat`/`chmod`. That the default path really pays nothing is asserted on
  syscall count rather than on outcome. What it *does* pay is one `listxattr` on the source **root**
  per run — a constant, free at any tree size — which warns when that root carries an ACL the copy
  is about to drop; a heuristic, since a root without one says nothing about its children.

  That notice is printed at the **default verbosity**, unlike every other warning these tools emit,
  because the user it exists for is precisely the one who will not think to pass `-v`. It gets there
  through a tracing target of its own (`rcp::notice`) with its own filter directive, so nothing else
  becomes noisier — raising the global default to `warn` instead would have unmuted 14 per-entry
  `warn!` sites, and one failed subtree would print thousands of lines. On a remote copy the notice
  keeps that target across the wire so the master renders it too; both ends need the directive,
  since a source `rcpd` at the default verbosity would otherwise not even send it. `--quiet`
  suppresses it; the copy still succeeds and its exit code is unchanged.

  Two settings combinations are rejected at parse time rather than silently ignored: `l:acl` (the
  kernel has no symlink ACL) and `acl` alongside a mode mask that narrows the rwx bits (an ACL *is*
  the permission state, so a mask stripping group and other while the ACL puts them back is a
  contradiction — masks that strip only the special bits stay legal, since the ACL carries no
  setuid/setgid/sticky). The `+` preset grammar is new: a string is a preset only when its first
  `+`-token is `all` or `none`, so `+` remains an ordinary character in the per-type DSL and no
  existing settings string changed meaning.

  Applying an access ACL is itself a mode-changing operation — `fsetxattr` drives the mode's rwx
  bits from `USER_OBJ`/`MASK`/`OTHER` — so it takes over as the single step that widens a
  destination from the owner-only mode it was created at, and the `fchmod` before it is narrowed to
  carry only the special bits. The invariant from the previous release is preserved exactly for
  files: one widening step, and it is the last fallible one. Directories take the same two-branch
  shape but keep `futimens` after it, as they did before — a directory's mtime is bumped by every
  child created in it, so its timestamps can only be applied once its children exist.

  Under `--require-toctou-safe` the *destination* side is contained as well, which is a different
  bug from the above and is closed at a different price: every directory rcp creates has both ACLs
  removed after the `mkdirat` (two syscalls per directory, none per file beneath a directory rcp
  created or locked down — stripping the default ACL stops the inheritance chain for the whole
  subtree; the one exception is a direct FILE operand, whose ambient parent rcp neither creates nor
  locks down, paying one strip per operand — see docs/acls.md), and every reused directory has its
  default ACL snapshotted and restored around the copy. The two flags deliberately do not imply each
  other — strict mode contains inherited *destination* ACLs but does not preserve the *source's* —
  so pair them where source ACLs are security-relevant; the root warning says so under that flag.
  One consequence is intended and documented: under strict mode without `d:acl`, a freshly created
  destination directory ends with no ACL even where its parent's default ACL would have given it
  one. Containment beats inheritance; `d:acl` is the escape.

  Known holes, documented rather than fixed: a file that compares equal under `--overwrite-compare`
  (default `size,mtime`) is skipped and keeps its old ACL, the same shape as `mode`, which the
  default comparison also ignores; inherited destination ACLs are not contained on the default path;
  and NFSv4 ACLs (`system.nfs4_acl`) are neither handled nor detected. `rlink`'s hard-linked files
  share the source inode, so `f:acl` never writes through one — it applies only where `rlink` really
  copies. See the new [POSIX ACLs](docs/acls.md) document for the full model.

- Add `--remote-keepalive-sec` (default 120, `0` disables): a liveness budget for every TCP
  connection a remote copy makes. A peer whose host vanishes — power loss, a severed link, a
  destroyed VM — sends neither `FIN` nor `RST`, so until now such a copy simply hung. The master
  awaits each `rcpd`'s result with no timeout of its own; the connection is idle, so nothing
  provokes a retransmission, and the same holds for the source↔destination control reads, leaving
  all three processes waiting forever. The budget arms two kernel mechanisms, because neither covers
  the other's case: TCP keepalive (`SO_KEEPALIVE` with `TCP_KEEPIDLE`/`TCP_KEEPINTVL`/`TCP_KEEPCNT`)
  probes an *idle* connection, which is the master-awaiting-a-result case and the control streams
  generally, while `TCP_USER_TIMEOUT` bounds how long *unacknowledged* data may stay outstanding —
  on **control** connections only (see the later entry in this release): on the data pool it cannot
  tell a dead peer from a throttled receiver that has legitimately stopped reading, so a data
  connection relies on keepalive when idle and on the kernel's retransmission limit (roughly 15
  minutes, `tcp_retries2`) mid-transfer. The keepalive sub-values are derived from the single budget
  rather than exposed separately (idle at half of it, probes every twelfth of it, six of them), so
  they cannot be set into an inconsistent relationship. The default detects a vanished host in about
  two minutes while surviving any stall shorter than that; widen it on a flaky WAN, or pass `0` to
  disable both mechanisms. The value is propagated to both `rcpd` processes — without that the
  master would recover while both `rcpd`s kept hanging, which looks fixed and is worse than the
  symmetric hang it replaces. Applying an option is best effort: one that a platform or a container
  policy refuses is logged rather than failing the copy.

  `TCP_USER_TIMEOUT` is applied to **control** connections only (master↔`rcpd`, and the
  source↔destination control stream) — never to the pooled data connections. It cannot distinguish a
  dead peer from a live one that has stopped reading: with the receiver's window at zero and every
  zero-window probe acknowledged, the sender is still aborted once the budget expires. The
  destination stops reading exactly that way, waiting for its per-file iops reservation between a
  file's header and its bytes, so `--iops-throttle 50` on a 10 GiB file at 1 MiB chunks leaves that
  socket unread for minutes; applying the budget there would fail a copy that previously just ran
  slow. The price is stated rather than glossed: a host that vanishes **mid-transfer** is still
  detected only by the kernel's retransmission limit (roughly 15 minutes) — unchanged from before
  this release, not a 2-minute detection — while an **idle** data connection is caught by keepalive
  after idle + retries × interval. Control connections are not strictly backpressure-free either
  (their dispatch loop takes one ops token per message, versus thousands per file on the data path),
  which is a known residual rather than a claim of immunity.

  These options join no-delay and buffer sizing in a single `remote::configure_tcp_socket` entry
  point, called at every site that establishes or accepts a connection, and taking a
  `ConnectionKind` so each site declares which of the two it is. Previously `set_nodelay` was
  applied at 7 sites and buffer sizing at 4, hand-paired at each connection, so two sites got no
  buffer sizing at all — the master↔`rcpd` control and tracing connections, and the source's dry-run
  control connection, now get the profile's buffer sizes as well. A new lint
  (`scripts/check-tcp-socket-config.sh`) fails the build on a socket option set outside that entry
  point, and on a file that opens or accepts a TCP connection without routing it through there.

### Changed

- `rcp-tools-remote` (the internal support library) has two breaking API changes from the TCP
  configuration consolidation above: `configure_tcp_buffers` is **removed** — it is subsumed by
  `configure_tcp_socket`, which additionally applies no-delay and the liveness options — and
  `connect_tcp_control` now takes `&TcpConfig` in place of a bare `timeout_sec`, so it can apply the
  full socket configuration itself instead of leaving each caller to remember the buffer call. The
  crate is documented as internal to the rcp tools and is not intended for direct use, but it is
  published, so the change is recorded here.

- A source `rcpd` whose destination peer goes away now says so in its log. End-of-stream on the
  destination's control connection was recorded as a normal end of dispatch whether or not
  `DestinationDone` ever arrived, so a log read after the fact reported a destination that aborted
  or died mid-copy as a clean finish. Behavior is deliberately unchanged, and the reason is the
  master rather than the destination: the master's read of the destination's result is mandatory, so
  a destination that dies silently still fails the copy there. Staying quiet on the source therefore
  conceals nothing, while failing there would add a second report of one event.

### Security

- Destination files are now created owner-only (`0o600`) and only widened to the source mode once
  their contents are written and every other metadata step has succeeded. Previously the destination
  was created at its *final* source mode before a single byte had been written, so a half-written
  file carried the audience its finished form was meant to have. At default settings that is an
  exposure of partial contents: the mode mask is `0o0777` (`setuid`/`setgid`/sticky are stripped,
  matching `cp`), so a world-readable source produces a world-readable destination from the moment
  it is created, and anyone who can reach the directory can read whatever has landed so far. No
  symlink race is needed — a world-searchable destination directory is enough, which is the default
  for any repeat or incremental `rcp --overwrite`.

  The sharper case needs the special bits preserved (`--preserve`, or a `--preserve-settings` mask
  of `7777`) **and** a copier that is interrupted. Root holds `CAP_FSETID` ("don't clear set-user-ID
  and set-group-ID mode bits when a file is modified"), so writing does not strip `S_ISUID` from a
  root copier's destination, and a `SIGKILL`, OOM kill, or crash after the last byte left behind a
  complete, functional, setuid-root executable whose contents the *source's* owner authored. Note
  that the **successful** copy was never an exec window on Linux: the destination stays open for
  writing through metadata application, and `execve` refuses a file any process holds open for
  writing with `ETXTBSY`. It is the interrupted copy — where the copier's death closes that
  descriptor and removes the protection — that the old creation mode exposed. (A non-root copier's
  first write clears the special bits, so there the leftover was a zero-length setuid file.)

  The fix is the file counterpart of the split-chmod destination directories already get, it costs
  no extra syscalls — only the mode argument of the creating `openat` changes — and it is
  unconditional rather than gated behind `--require-toctou-safe`, because a file the copy creates is
  its own new object with no prior user-visible state to preserve. It applies to local `rcp`/`rlink`
  (including every `rlink` fall-back-to-copy path) and to remote (`rcpd`) copies. One intended
  behavior change: a copy that is interrupted, or that fails anywhere before *or during* metadata
  application, now leaves a file readable only by its owner (`0o600`) rather than at the source
  mode. Its owner, not necessarily the copier: the chown runs before the chmod, so a copy that
  preserves ownership and got past that step has already handed the file to the source's uid. That
  holds for every step because the widening `fchmod` is applied last, after the timestamps: metadata
  application is chown → utimens → chmod, chown first so it cannot clear the `setuid` bit the chmod
  restores, and chmod last so no failure after it can publish the final mode on a file the copy is
  about to report as failed. The most likely instance is a copy that preserves ownership whose
  `fchown` is refused — a non-root copier copying a file owned by someone else gets `EPERM` at the
  first step. The copy exits non-zero naming that error either way, and a later run *normally*
  re-copies such a file: the default `--overwrite-compare` is `size,mtime`, and a partial file
  differs in size.

  "Normally" needs two exceptions stated, because before this change a skipped retry was harmless —
  a failed metadata application still left the file at its correct final mode, whereas now it leaves
  the destination owner-only until something re-copies it. First, a *data-complete* file that failed
  at the closing `fchmod` has already had its timestamps applied, so it matches its source on both
  size and mtime and is skipped. (Something has to run last; under the previous chown → chmod →
  utimens order this was the `futimens` failure instead. What changed is that the step which can
  strand a file is now also the step that no longer publishes a mode the copy did not finish
  earning.) Second, the mtime comparison skips nanoseconds whenever *either* side's `mtime_nsec` is
  zero — a concession to filesystems that do not store sub-second timestamps — so even a file that
  never reached `futimens`, and therefore carries its own write time, compares *equal* to its source
  when that write and the source mtime fall in the same whole second **and** either side's
  nanosecond field is zero. Copying a file written in that same second reaches it, as does a source
  carrying a whole-second mtime (`touch -d @<seconds>`, a tar extraction, a reproducible-build
  epoch) or a destination filesystem without sub-second timestamps. Comparing the mode too
  (`--overwrite-compare=size,mtime,mode`) closes both, since a stranded destination is `0o600` and
  its source is not; so does removing the file first.
- Under `--require-toctou-safe`, a privileged copy/link now locks down each *reused* destination
  directory for the duration of the copy. Previously a fresh destination directory was created
  copier-owned at `0o700` and only widened to the source mode at the end, but an existing directory
  that the copy reused was left exactly as-is — a pre-existing `0o777` directory exposed the
  directory and every freshly written child for the whole subtree copy, and stayed exposed if the
  copy was interrupted. Strict mode now takes over each reused directory before any child is
  written: it inode-rechecks the opened directory fd against the just-classified entry (a `rename`
  that swapped in a different directory fails closed), then `fchown`s it to the copier and `fchmod`s
  it to `0o700` (chown first, so the prior owner cannot re-widen it mid-copy). The takeover is
  verified by re-stat and fails closed if it did not land (a filesystem that does not honor
  `chown`/`chmod`, e.g. CIFS without unix extensions). A reused directory that had the setgid bit
  keeps it (children created during the copy inherit its group), and the group is pinned to the
  value captured at classification — if a prior owner raced a `chgrp` into the takeover window, the
  copier resets it so children cannot be redirected into an attacker-chosen group. At finalize the
  original owner is restored and the source metadata re-applied, so a successful copy leaves the
  directory byte-identical to before this change (final owner is the source owner for each
  `--preserve`d component and the original owner otherwise, and the mode is the same masked metadata
  the unhardened copy would apply — the source directory's mode, or under `rlink --update` the
  update tree's), and finalize re-stats each reused directory and fails closed if the restored owner
  or mode did not take effect (a dropped setgid bit the kernel strips for a non-root copier outside
  the group is a warning, not a failure). Applies to local `rcp`/`rlink` and remote (`rcpd`) copies.
  Two documented side effects. First, a reused directory whose processing is aborted after lockdown
  — by a `--fail-early` abort, or by any per-directory error that returns before finalize (e.g. an
  enumeration failure) even *without* `--fail-early` — is left no wider than a successful copy would
  leave it (the local path leaves it secured — copier-owned at `0o700`; the remote path may instead
  have already restored it to its transparent final state), except when the restricting `chmod`
  itself fails: the directory is then returned toward the state it already had before rcp ran (its
  original owner and mode if the ownership rollback succeeds, its original mode copier-owned if that
  also fails) — never wider than it already was. Second, an actor holding a directory fd opened
  before the lockdown can still observe the names of children written afterward (child contents stay
  protected by their own source-derived mode). A non-owning, non-privileged copier that cannot chown
  the reused directory fails closed for that directory. The lockdown also snapshots and removes the
  directory's POSIX **default** ACL for the copy's duration, restoring it at finalize (or from an
  RAII guard on abort), because a `chmod` cannot reach a default ACL and freshly written children
  would otherwise inherit it; its **access** ACL needs no strip, since the interim `0o700` rewrites
  the ACL mask from the group bits and leaves every named entry granting nothing. Limitation: on a
  backend that does not honor `chown`/`chmod` (e.g. CIFS without unix extensions) the takeover or
  finalize fails closed but may leave the reused directory copier-owned at its original, possibly
  permissive, mode. The default (non-strict) path is unchanged: reused directories are left as-is.

### Fixed

- Fix a long-standing indefinite hang in remote (`rcpd`) copies under `--fail-early`. When a
  directory failed to be created — out of space, or a `--require-toctou-safe` lockdown that could
  not secure a reused directory — or a transfer task failed, the source's directory walk could stay
  parked on its file-descriptor budget forever, because the budget was not released on the
  destination's abort. The source now explicitly releases that budget after every normal
  control-message dispatch result, before draining tasks. Once dispatch starts, an RAII closer
  covers cancellation and panic unwind before that point, so teardown does not hang on a parked walk
  along paths that run destructors. The failure is now also reported with its real cause (for
  example a file's `Permission denied`) rather than the internal budget-release wakeup that unblocks
  the parked walk.
- Fix two indefinite hangs in remote `rcp -L`/`--dereference` copies, on source entries that change
  while the copy walks them. Both need `-L` and nothing else — no ACLs, no `--fail-early`.

  A directory's `entry_count` is fixed when its `Directory` message is sent, and the walk descends
  into the children it counted only afterwards. A child that vanished, changed type, or stopped
  matching the filter in between produced no message at all, so the destination's parent never
  reached its expected entry count, `DestinationDone` was never sent, and the copy hung with both
  peers alive — which no keepalive or timeout ends. Such a child now gets the same `FileSkipped` the
  hardened (non-`-L`) walk has always sent for one it counted and then could not open. There were
  four such exits rather than one, so they now report through a single funnel that the compiler
  makes every exit declare itself to, instead of each having to remember the compensation on its
  own.

  The ROOT hung for a different reason: the walk re-`stat`ed a root the caller had already
  classified, so a root that was a directory at the first read and a regular file at the second
  announced `DirStructureComplete { has_root_item: true }` and then sent nothing at all, leaving the
  destination waiting on root completion forever. The walk is now handed the caller's single
  classification — the one-classification rule the hardened root already followed — so the two
  cannot disagree. A root that changes type after being classified now fails its enumeration and
  takes the committed-but-unreadable-directory route (a 0-entry `Directory` the destination can
  complete) with the error reported, so the copy exits non-zero instead of hanging. One consequence:
  a `-L` root that disappears after being classified now leaves an empty destination directory and a
  non-zero exit, where it previously failed with nothing created — matching what the hardened path
  already did for the same window.

- Fix a silent data loss when a source directory is replaced by a regular file mid-copy, in remote
  copies **both** with and without `-L`. The source's two passes could each account for the same
  name: Pass 1 counts a child as a directory and accounts for it (its own `Directory` message, or a
  `FileSkipped` when it can no longer be sent), and Pass 2 — which re-enumerates the directory
  rather than inheriting Pass 1's classification — counts it AGAIN as one of that directory's
  expected files, because it is one by then. Since the destination expects exactly the
  traversal-time entry count, the surplus was truncated away in `readdir` order, and when the
  casualty was a genuinely counted file that file was never transferred: the destination completed
  normally and the copy exited `0` with a source file quietly missing. The two passes are now
  mutually exclusive by name — the names Pass 1 counted as directories or symlinks are retained with
  the file count and skipped by Pass 2, whatever they look like by then.

  Independently of that, having more files than the traversal counted is no longer a muted warning.
  The extras cannot all be sent, so one of them is dropped and that may be a file the copy was asked
  to make; it is now recorded as an error, and such a copy exits non-zero naming the directory
  instead of reporting success. The remaining case is a file genuinely created mid-copy, which can
  still displace a counted one under that rule — reported, not silent.

  The `-L` walk also stopped reporting one of these outright: a counted child that is a regular file
  by the time the walk reaches it is not copied at all (its name belongs to Pass 1), which now
  records an error like every other entry the source cannot send, matching what the non-`-L` walk
  already did. And the `-L` walk's directory-open now takes a metadata-throttle token like every
  other metadata syscall in it — it was the one call escaping `--ops-throttle` entirely, while its
  hardened counterpart was already gated.

- Fix a silent data-loss bug in remote (`rcpd`) copies under `--fail-early`: a regular file that
  could not be written at the destination (for example into a pre-existing directory with no write
  permission) was logged and then dropped, and the destination reported success — the copy exited
  `0` with the file missing. The destination now records the failure and reports it, so the copy
  exits non-zero naming the real cause (e.g. `Permission denied`). When an operation error and a
  connection-teardown error coincide, the destination now reports the operation error (the real
  cause) rather than the teardown symptom. To surface any such abort the destination signals the
  source by closing its control stream — on every data-path abort path (a fatal stream error, a
  worker panic, or an early connection loss), so a source with no data left in flight (an
  all-empty-file transfer, whose header-only sends never trip a broken pipe) or one parked on its
  file-descriptor budget tears down promptly instead of the copy hanging indefinitely. The
  destination also drives both of its internal futures to completion before choosing the error, so a
  worker's real cause is never lost to a cancellation when the control side reports a teardown
  symptom first.
- Remote (`rcpd`) copies now treat a corrupt or malformed file-header frame — a decode failure (a
  frame received intact that does not decode), an oversized length prefix, or another framing/TLS
  protocol fault — as a fatal error instead of silently ending the data stream. An end-of-stream at
  a header boundary is tolerated as normal ONLY when the transfer has already completed or teardown
  has begun; otherwise it is a truncation and is fatal (see the incomplete-transfer entry below).
  This applies to BOTH shapes an end-of-stream takes — a transport-level peer closure
  (`UnexpectedEof` under TLS, a connection reset otherwise) and a CLEAN shutdown surfacing as a
  graceful end-of-frames. The clean shape was previously accepted unconditionally, which left the
  original hang reachable whenever the peer closed gracefully — the source does exactly that for a
  stream whose send failed, and a graceful FIN is the ordinary shape on unencrypted transports.
- Remote (`rcpd`) copies now fail closed on an incomplete transfer instead of exiting `0` with files
  missing. If the destination's completion accounting is not satisfied at teardown and no error was
  recorded — for example a data connection to the source failed transiently before all files were
  received — the copy is now reported as an "incomplete transfer" failure, naming the actual cause
  when one is available (e.g. "connection refused" / "TLS handshake timed out") rather than a
  generic message. A data-stream that closes MID-transfer (a truncated header or a dropped link
  before the transfer completed) is now treated as fatal and fails the copy promptly, instead of the
  destination reconnecting and hanging while the source waited for a completion signal that could
  never come.
- Bound every remote-copy TLS handshake by the connection timeout. Previously only the TCP connect
  was timed out; a peer that established TCP and then stalled the TLS handshake could hang a copy
  indefinitely. Every handshake now runs through one bounded helper — the data connection (both
  directions), the control connection (destination connect and source accept), each source-side
  data-accept, the source's dry-run control accept, and both master↔`rcpd` accepts — each bounded by
  `--remote-copy-conn-timeout-sec` and released on teardown. The `rcpd` accepts matter most: they
  are sequential with the handshake inline, so a peer that connected and sent no TLS bytes would
  block the legitimate master indefinitely and strand an `rcpd` holding its port. A CI check
  (`scripts/check-tls-handshake-timeout.sh`) now rejects a handshake performed outside that helper,
  since the timeout had previously been added at three call sites and missed at two.
- Remote (`rcpd`) copies now count a file whose DATA transferred but whose metadata could not be
  applied. The destination incremented its file/byte counters only after `chown`/`chmod` succeeded,
  so copying a root-owned tree as an unprivileged user reported `files copied: 0` even though every
  file's contents had landed on disk. The counters are now incremented once the data is written —
  matching the local path — while the metadata error is still recorded and still fails the copy.
- Remote (`rcpd`) copies now report the real cause when a data connector fails during teardown. The
  destination's abort funnel closed the control stream (an awaited, lock-taking operation) before
  stopping the data pool, so a connector failing in that window stashed a teardown artifact — e.g.
  "connection refused" — that was then preferred over the actual error. The pool is now stopped
  first (it is synchronous and lock-free, so it takes effect immediately), and a connect failure
  observed after teardown began is discarded rather than recorded.
- Fix `rcp`/`rlink` aborting the process (`SIGABRT`, exit 134) instead of completing a copy when two
  source operands alias the same destination directory, e.g. `rcp A/x B/x dst/` (both map to
  `dst/x`) — the default dispatch runs operands concurrently, so the second to reach `dst/x` found a
  directory the first had already created and tripped an internal assertion that treated "we just
  created this directory" as proof no other writer could have populated it. Two concurrent `rcp`
  processes into the same destination hit the same assertion. A destination directory that already
  exists is now handled like any other pre-existing entry in this case (matching how a pre-existing
  destination symlink was already handled), instead of aborting.
- Fix `rcp`/`rlink` failing a file with `File exists (os error 17)` — silently bypassing
  `--overwrite` and `--ignore-existing` — when another writer created that file first. The copy
  skips the per-file existence check inside a destination directory it just created, on the
  assumption that a directory it created moments ago is still empty; when that assumption is stale
  (a second source operand whose destination aliases this one, a concurrent `rcp`, or any other
  writer), the create failed on `O_EXCL` instead of consulting the overwrite settings, so
  `rcp --overwrite A/x B/x dst/` failed on whichever file lost the race and `--ignore-existing`
  failed on a file it had been asked to skip. Such a file is now resolved like any other
  pre-existing destination: overwritten, skipped, or — with neither flag — reported with the
  actionable "did you intend to specify `--overwrite`?" error rather than a raw `EEXIST`. The same
  recovery now also applies to a destination the copy did *not* just create — in local copies, which
  previously failed the same way if a file appeared in the moment between the copy checking the
  destination and creating it, and on the remote (`rcpd`) destination, which had no recovery at all
  and failed the file on `EEXIST` regardless of `--overwrite` or `--ignore-existing` whenever a
  writer filled the slot after the destination classified it. That window is not instantaneous on
  the remote side: it spans the whole `--iops-throttle` reservation, which can be seconds. None of
  these cases costs a copy without a conflict any extra syscall.
- Fix the remote master and `rcpd` aborting the process (`SIGABRT`, exit 134, core dump) instead of
  reporting a clean error when a peer's control connection closed without sending an expected
  message — the master reading a dead source or destination `rcpd`'s hello or result, and an `rcpd`
  reading a master that vanished before sending the hello that assigns its role. Because the
  workspace builds with `panic = "abort"`, hitting any of these four sites skipped normal unwinding:
  the tracing guards kept alive specifically so chrome traces, flamegraphs, and histogram logs flush
  never ran, silently discarding them, and the runtime summary was never printed. All four sites now
  report a descriptive error naming the affected host (e.g. "destination rcpd on '\<host\>' did not
  report a result (the process likely died - check the remote host for crashes or OOM kills)")
  through the normal error-reporting path, exiting `1` instead of aborting. A transport error on
  those same reads is now reported just as descriptively, keeping the underlying cause in the chain:
  a dying `rcpd` reaches the master as a clean EOF only when the socket's receive queue happens to
  be empty at the moment of death, and as a connection reset otherwise, and which of the two arrives
  is not something either side controls — so previously the same crash produced a clear diagnostic
  or a bare "connection reset by peer" depending on timing. The two are worded to say which happened
  ("closed its control connection cleanly but did not report a result" versus "did not report a
  result" with the transport error beneath it), because they point in different directions: a
  connection that closed cleanly but silently means the peer exited without sending its message,
  while one that failed can also mean the network went away.
- Fix `--overwrite` removing a destination file before the copy was able to replace it, which could
  leave neither the old file nor a new one. The old destination was unlinked while deciding *what to
  do* about it, and only afterwards did the copy reserve its I/O-throttle budget and open the source
  — so a source that turned out to be unreadable or had been swapped away had already destroyed data
  the copy then could not replace, for a file it never held a single byte of. Deciding and doing are
  now separate steps: the destination is classified without being touched, the source open and
  throttle wait happen next, and only then is the old entry removed — with an inode-recheck, so an
  entry swapped in since the decision fails closed instead of being deleted. One visible
  consequence: a copy that fails to read a source file no longer removes that file's destination, so
  `rm_summary.files_removed` (and `--summary`) can be lower than before for a run with read errors.
  The remote destination (`rcpd`) had the same shape — it unlinked and only then waited on the
  throttle — and got the same treatment; it has no source open to reorder, so there the reordering
  shortens the gap to the create syscall rather than removing a failure from it.

  This is not atomic replacement and does not try to be. `rcp` copies are point-in-time and
  non-atomic: an interrupted or failed `--overwrite` can still leave the files it was overwriting
  truncated or missing, and the data copy following the create is a far larger window than anything
  before it. Making replacement atomic means staging every file under a temporary name and renaming
  it into place, which costs performance `rcp` deliberately does not spend.
- Fix `rcp`/`rlink` treating a destination lookup that *failed* as a destination that does not
  exist, locally and on the remote (`rcpd`) destination alike. Only `ENOENT` means an empty slot;
  `EACCES` on the destination directory, `EMFILE`, `EIO`, or an `ESTALE` from a network filesystem
  all mean the copy could not look. Those were resolved to "nothing there, go ahead and create", so
  the real errno was replaced by whatever the subsequent create reported — and `--ignore-existing`
  failed on a file it should have skipped. Such a failure is now reported with its own cause intact.
  The same rule now also governs both `--dry-run --ignore-existing` probes, for files and for
  directories — the one place a destination is examined by path rather than through a pinned parent
  directory, since a dry run holds no destination descriptor. They previously reported "would copy"
  and exited `0` through a destination parent they could not even traverse, predicting a copy the
  real run then fails and swallowing the error that explains why. The directory probe additionally
  consulted `Path::is_dir()`, which FOLLOWS symlinks: a destination symlink — even one pointing at a
  directory — read as "a directory is already there, recurse into it", while the real
  `--ignore-existing` run classifies that same entry through the parent's descriptor (`O_NOFOLLOW`)
  as a non-directory and skips the whole subtree. So the dry run predicted copying a subtree the
  real run skips. That decision now comes from the metadata already probed, which also drops a
  second, un-throttled path-resolving syscall per directory.
- Fix local `rcp`/`rlink` reporting zero removed bytes in the summary when `--overwrite` replaced an
  existing destination file. The live progress display counted them, so the two disagreed, and the
  final summary and any error payload under-reported the work done. (The remote destination already
  accounted these correctly.)
- Fix concurrent `--auto-deploy-rcpd` deployments sharing one temporary file, and verify the
  transferred binary before publishing it rather than after. The temp name was built from the shell
  variable `$$`, but every path handed to the remote shell is single-quoted, so `$$` never expanded
  and every deployment wrote to the same literal `.rcpd-<version>.tmp.$$`. Two concurrent
  deployments to one host therefore raced on that file, and because the publishing `mv` is a rename,
  one could publish it while the other was still writing through its own descriptor — landing those
  writes directly on the inode now serving as the cached `rcpd`. The name is now generated by the
  deploying process and is unique per deployment. Separately, the SHA-256 check ran against the
  *final* path after the rename, so a truncated or corrupt transfer was briefly reachable under the
  name other processes execute, and stayed there after the deployment reported failure; it now runs
  against the temp file, and a mismatch removes it instead of publishing it. A failed deployment
  also no longer leaks its staging file: the temp path is now chosen by the caller before anything
  can create it, and every failure between choosing it and publishing exits through one cleanup
  funnel. Previously the path was picked inside the transfer and returned only on success, so a
  transfer that failed after the remote shell had created the file — broken pipe, full disk, killed
  command — left it behind, one per retry, and the old-version cleanup globs `rcpd-*`, which never
  matches these dotfiles.
- `scripts/check-error-logging.sh` now reads Rust's other literal forms, so a `tracing` call written
  near one is checked rather than misparsed. The linter does real lexing to decide where a call ends
  and to separate its format string from its argument list, and all four of those scans treated `"`
  as the only string delimiter. A **raw string** desynchronized them — raw strings have no escapes,
  so the quote inside `r#"…"#` is body text, and reading it as the terminator ended the literal
  early and left the rest of the body to be scanned as code, a `(` in it counted as one of the
  call's own parens. A `'('` **char literal** was counted as a real paren outright. Both are now
  understood, including the byte and C prefixes (`br`, `cr`) and any hash count, which is *matched*
  rather than assumed to be one — `r##"…"#…"##` is closed by the `"##` at its end, not by the `"#`
  in its body. Neither construct was ever checked *wrongly*: each wedged the collector, which the
  existing `UNPARSED` guard announces, so the old state was fragile rather than unsound — the first
  raw string written near a `tracing` call produced a false alarm someone had to diagnose.

  The tick is shared with **lifetimes** and loop labels, which are far more common than an
  unreadable char literal, so `'` opens a char literal only where exactly one character (or a
  backslash escape) is followed by a closing quote; anything else consumes the tick alone, and a
  long escape (`'\u{1F600}'`) with no closing quote inside a bounded window falls back to the
  lifetime reading. Reading `&'a str` as a char literal would consume forward to the next quote and
  swallow the code in between, turning a construct the lexer merely did not understand into a false
  alarm — the one outcome this check must never produce. A raw string spanning lines is now tracked
  the way a block comment already was, its body skipped instead of scanned as code, and one still
  open at end of file is announced by the same `UNPARSED` guard: a new way for the lexer to stop
  reading a file has to be as loud as the old one, because silence from a lexer that stopped reading
  is indistinguishable from a clean file. The fixture that pinned that guard is replaced — it was a
  raw string the lexer did not understand *yet*, so it would have gone on passing while proving
  nothing — by a call whose parens genuinely never balance before the end of the file.
- `rcpd` now watches the master control connection for the entire operation, not just until the
  hello. The master sends nothing on that connection after `MasterHello` — it holds it open to await
  the final result — so the socket's only remaining signals are an EOF (the master exited or
  aborted) and a transport error, which is how the keepalive and `TCP_USER_TIMEOUT` configured on it
  surface a master HOST that vanished. Nothing read the socket, so the kernel marking it dead
  cancelled nothing: with the master gone, the source and destination `rcpd`s kept copying to each
  other — or waiting on each other — indefinitely, because the stdin watchdog only fires when SSH
  itself notices the death, which a network partition can delay far past `--remote-keepalive-sec`
  (and with stdin unavailable it never fires at all). Either signal now cancels the operation
  through the same teardown the stdin watchdog uses, reused-directory ACL restores included. The
  watchdog stands down once the final result is committed to the master: it used to keep racing
  through the stream close as well, so a master that consumed both results and closed its side first
  could make `rcpd` report a finished copy as a master loss and exit 1 — the close now runs after
  the guarded region, best-effort.
- The remote destination no longer stalls its control receive loop while it builds a reused
  directory's `--overwrite`/`--ignore-existing` manifest. The build — enumerating and stat'ing up to
  `--overwrite-manifest-max-entries` (default 5,000,000) pre-existing entries — ran inline between
  two reads of the control socket, so for its whole duration the source's messages went unread, the
  receive window closed, and after `--remote-keepalive-sec` of zero-window stall the
  `TCP_USER_TIMEOUT` on the source's control socket terminated an otherwise healthy copy (Linux
  counts data stuck behind a zero window toward that timeout; the peer answering window probes does
  not reset it). Each manifest is now built in a per-directory task (one build at a time, so peak
  memory is unchanged) that then sends the directory's manifest chunks and its `DirectoryCreated`
  itself; the chunks stay contiguous with their trigger, other directories' messages may interleave
  around them — the source already accumulates chunks keyed by destination path — and the receive
  loop keeps draining the socket throughout. Message processing for other directories no longer
  queues behind one huge enumeration, either. A directory's completion now also waits for its own
  announce: symlink and subdirectory children arrive over the control stream without waiting for the
  Pass-2 trigger, so a reused directory holding only those could complete — and send
  `DestinationDone`, closing the control send stream — while its announce task was still
  enumerating, failing a healthy copy with a broken-pipe announce error (and handing the source a
  `DestinationDone` with a `Directory` still unanswered). And when the source tears down mid-copy,
  outstanding manifest builds are aborted rather than drained: a queued build would only scan a
  complete directory to fail its announce against the closed peer, which under `-L` (where the
  source's dir-fd budget does not bound them) could stall failure shutdown without bound. The `-L`
  walk itself now carries an outstanding-directory **credit** mirroring the hardened walk's dir-fd
  budget. Every sent `Directory` owns one credit; `DirectoryCreated` transfers it into that
  directory's Pass-2 task and releases it when the task finishes, while `DirectorySkipped` releases
  it immediately. The gate closes on dispatch exit with the same typed marker, so a slow ack,
  manifest, or direct-file Pass-2 path cannot create an unbounded backlog at that stage. Zero-file
  Pass 2 releases immediately, so destination fds retained for recursive ancestors can outlive the
  credit; holding it through destination directory completion would deadlock a directory-only chain.
  The credit is stored in the same map entry as the directory's Pass-1 contents, so an
  unreadable-directory arm or absent/duplicate acknowledgement cannot omit or invent one.
- Four corrections to the (unreleased) strict-mode ACL containment and its rollback, each closing a
  way the destination could end with ACL state that came from neither the source nor its own past: a
  **direct file operand** copied into a parent carrying a default ACL kept the inherited entries
  (inert at the owner-only create mode, activated by the final `chmod` re-deriving the ACL mask — a
  strict copy of a plain `0640` file granted a named user read access its source never did; files
  created in the one directory kind rcp neither creates nor locks down now strip the inherited ACL
  inside the create itself); a remote source directory that **could not be opened** encoded its
  unknown ACLs the same as "has none", which the destination applied as an authoritative clear,
  permanently stripping a reused destination directory's access and default ACLs on an
  already-failing copy (the wire now distinguishes `Unknown` from `Captured`, and unknown means the
  destination's ACLs are left alone, a locked directory's original default ACL restored); the
  reused-directory lockdown guard could not roll back a **partially-applied source default ACL** on
  a directory that originally had none, and a finalize ACL write detached on the blocking pool could
  land *after* the guard's abort-restore and silently undo it (the guard now has an explicit
  armed/disarmed state that stays armed through the final re-stat verification — a cancelled or
  verify-rejected finalize rolls the directory back rather than keeping the source's default ACL,
  and armed-with-no-original removes what a partial finalize installed — and every finalize write to
  the guarded attribute is serialized against the rollback, skipped if the restore already ran); and
  `make_dir`'s create → re-open → strip sequence spanned separate cancellation points, so a
  `--fail-early` sibling abort could abandon an rcp-created directory still carrying the
  destination's inherited default ACL, indistinguishable from a user directory on any later run
  (creation, open and sanitization now share one uncancellable blocking closure — which is also why
  the congestion controller now sees one `MkDir` probe per created directory rather than a `MkDir`
  and a `Stat`; a directory whose open or strip fails is removed rather than left behind).
- Same-release builds with different remote wire schemas now reject each other **in both
  directions**: `rcpd --protocol-version` carries a wire-schema revision inside the semantic version
  itself, as semver build metadata (`0.38.0+w1`), so the exact string equality every build —
  including ones predating the revision — has always used for compatibility fails closed no matter
  which side was upgraded first. The revision is bumped on any wire-visible protocol change that the
  crate version does not capture (a stale same-version `rcpd` on `PATH` or in the deploy cache
  previously passed the check and failed mid-copy), the deploy-cache filename carries it
  (`rcpd-0.38.0-w1`), and the version-mismatch error explains the `+w` component with a remedy that
  quotes the plain crate version cargo can actually install.
- `scripts/check-tcp-socket-config.sh` now confines establishing and accepting TCP connections to
  configured helpers in `remote/src/lib.rs`: raw `TcpStream::connect`/`.accept()` are forbidden
  everywhere else, and inside that file every connecting or accepting function must itself call
  `configure_tcp_socket` (a per-file mention count previously passed a file with one configured and
  one unconfigured connection — and in the funnel file, whose declaration and tests inflate the
  count, it would have admitted several). The four call sites outside the funnel moved into new
  `connect_tcp_data`/`accept_tcp_control`/`accept_tcp_data` helpers beside the existing
  `connect_tcp_control`. Also `scripts/check-error-logging.sh` now carries a **multi-line raw format
  string** into the call it is collecting, so a chain-losing `{}` in one is reported; its body was
  previously dropped wholesale (correct for classification, where literal prose must not arm
  anything, but the placeholders of a format string live in exactly that body). Fixtures pin both
  directions.
- Fix remote copies failing with `unix_listener: path "..." too long for Unix domain socket` when
  `$HOME` is long. The SSH connection-multiplexing socket lived under `$XDG_STATE_HOME` (else
  `$HOME/.local/state`), and `sockaddr_un` caps the whole path at 108 bytes — of which the suffixes
  the SSH layer appends leave only about 48 for the home directory, thin for container workspaces,
  network homes, and CI checkout paths. The error named neither `rcp` nor `HOME`, so it gave the
  user nothing to act on. The control directory is now taken from the first genuinely usable
  candidate among `$XDG_RUNTIME_DIR`, `$TMPDIR`, and `/tmp` — "usable" meaning short enough to hold
  the socket path *and* writable, since a `$XDG_RUNTIME_DIR` inherited into a `su`/`sudo -u` session
  commonly names a directory that exists but cannot be written.
- The Nix flake evaluates on Darwin again, un-breaking `nix flake check --all-systems` in CI:
  nixpkgs removed the `darwin.apple_sdk.frameworks.*` compatibility stubs the flake listed (the
  Darwin stdenv now ships the SDK itself, and the project needs no framework beyond it), and the
  `acl`/`strace` dev-shell tools are Linux-only and are now gated accordingly. The flake's systems
  are now listed explicitly and no longer include `x86_64-darwin`, which nixpkgs 26.11 dropped —
  importing nixpkgs for it throws outright; `aarch64-darwin` stays.

## [0.37.0] - 2026-07-15

### Security

- `--require-toctou-safe` now enforces a strict operand contract in addition to requiring the
  hardened walk. Every operand path (including `rlink --update` and the path part of remote
  `host:/path` operands) must be absolute and lexically normal — no `.` or `..` components, no empty
  `//` segments; `realpath` output always qualifies — and every operand root/parent open resolves
  with `openat2(RESOLVE_NO_SYMLINKS)`, so a symlink in any directory component of an operand path
  fails closed with `ELOOP` at the open itself (a symlink operand is never followed either — it is
  operated on as the link object, per the tools' non-`-L` semantics). This closes the race between a
  wrapper's `realpath`+policy validation and the tool's open, and makes string-level operand
  policies in sudo rules and vetted wrappers sound. The flag now requires Linux 5.6+ (`openat2`) and
  refuses older kernels; remote copies mirror the flag to each `rcpd`. Invocations that previously
  passed relative or unnormalized operands with `--require-toctou-safe` are now refused — pass
  `realpath`-resolved operands. `--toctou-check` output gains informational notes for operands the
  strict mode would refuse; its exit code is unchanged, as is all behavior without the flag. For
  LOCAL operations, destination and `--update` operands are validated the same way, up front, on
  every path (real copy, `--dry-run`, filtered source, `--overwrite`, trailing-slash), so a
  symlinked destination prefix fails closed regardless of flags. The remote `--dry-run` source
  traversal on the default (non-`-L`) path is now fd-relative like the real copy, so a concurrent
  swap cannot make a privileged dry run report names, sizes, or symlink targets from outside the
  source tree. One remote limitation: the destination `rcpd` validates its operand prefix only when
  it actually writes, so a fully source-filtered remote copy or a remote `--dry-run` (which write
  nothing) do not separately fail closed on a symlinked destination prefix — nothing is followed
  through it; closing this gap fully requires sending the destination operand to the destination
  `rcpd` up front (a protocol change, deferred). See `docs/tocttou.md`.

## [0.36.0] - 2026-07-10

### Added

- Add `rchm --no-setid` for constrained privileged wrappers. Every selected non-symlink covered by
  an applicable mode, owner, or group rule finishes with set-user-ID and set-group-ID cleared,
  including pre-existing bits; sticky is unaffected. The flag respects filters and per-type rules,
  is not an operation by itself, and leaves the default behavior unchanged when omitted.

### Security

- Fix a remote-copy TLS authentication bypass: the certificate verifiers accepted any
  `CertificateVerify` signature, so fingerprint pinning only proved a peer presented a known
  certificate, not that it held the private key — a replayed certificate defeated the documented
  MITM protection. Signature verification now delegates to rustls, restoring proof of possession,
  and every remote connection pins TLS 1.3.
- Update the remote transport's crypto stack (`aws-lc-rs` 1.15.1 → 1.17.0, `aws-lc-sys` 0.34.0 →
  0.41.0), resolving five high-severity `aws-lc-sys` advisories, including X.509 name-constraints
  and PKCS7 signature-validation bypasses.

## [0.35.0] - 2026-06-15

### Changed

- Pinned `sysinfo` at `0.38` and froze the minimum supported Rust version (MSRV) at 1.91.1, declared
  via `rust-version` in `Cargo.toml` and enforced by a dedicated CI job that compiles the workspace
  on that toolchain. The dev/CI toolchain (latest stable) is now tracked separately from the MSRV,
  so routine dependency or toolchain updates can't silently raise the floor. `sysinfo` 0.39 required
  Rust 1.95; 0.38 keeps the same `set_open_files_limit` API at MSRV 1.88.

## [0.34.0] - 2026-06-12

### Added

- Remote `rcp` skips re-transferring files the destination would leave untouched anyway. The
  destination sends a manifest of its existing entries; the source compares against it and sends a
  "file unchanged" notification instead of the file body. Under `--overwrite` this covers
  destination entries identical to the source (or strictly newer, with `--overwrite-filter=newer`);
  under `--ignore-existing` it covers any name already present at the destination, regardless of its
  contents. The per-directory manifest is capped by `--overwrite-manifest-max-entries` (default
  5,000,000); a directory exceeding the cap falls back to transferring files normally.

### Changed

- `rcp host:~ dst/` (a bare remote home as source with a trailing-slash destination) now errors
  instead of creating a directory literally named `~` under `dst/`: the remote home's basename
  cannot be resolved locally. Use a destination without a trailing slash to name the result
  explicitly.

### Fixed

- Fix `rcp`/`rlink` rejecting `.`/`..` source operands (`.`, `./`, `..`, `../`, `tree/..`) when the
  destination ends with a slash, e.g. `rcp . out/` or `rlink tree/.. out/` previously failed with
  "source ... does not have a basename". The source basename for the trailing-slash form is now
  resolved through the same canonicalization the copy/link operation uses, so `dst/<name>` always
  matches the entry that gets created.
- `rchm` `--owner`/`--group` name resolution now works for directory-service (LDAP/SSSD/NIS) users
  and groups when using the static release binaries: when the in-process lookup cannot see the name
  (static musl builds have no NSS and read only `/etc/passwd`/`/etc/group`), `rchm` falls back to
  the host `getent` tool, which carries full NSS. When running privileged (e.g. via `sudo`), the
  `getent` binary is located from a fixed list of trusted system directories rather than `PATH`, so
  a name lookup cannot exec an attacker-controlled binary as root; `--getent-path <ABSOLUTE>` pins
  an exact binary (intended to be baked into a sudo rule) and is rejected if given more than once.
  Numeric ids never invoke `getent`.
- Fix `rlink` silently reporting success (exit 0) when a filter
  (`--include`/`--exclude`/`--filter-file`) was active and a directory's only traversed child failed
  to link: the directory became empty, was pruned by the empty-directory cleanup, and the child's
  failure was dropped. The collected error is now surfaced, so such a run exits non-zero.

### Security

- Harden `rcp`, `rlink`, `rchm`, `rrm`, and remote copy against time-of-check-to- time-of-use
  (TOCTOU) races on Linux: traversal now uses an fd-based safe walk (`O_NOFOLLOW` + fd-relative
  `*at()` syscalls) so a concurrent symlink or path- component swap at or below a named root cannot
  redirect a read/write/chmod/chown/ delete outside that root's subtree, and mode/bytes are read
  from the same fd so a swap can never widen permissions or attach the wrong owner. Add
  `--toctou-check` and `--require-toctou-safe` to each tool to audit/enforce safe operands. See
  `docs/tocttou.md`; `--dereference`/`-L`, non-Linux builds, and `rcmp` are out of scope.

## [0.33.0] - 2026-05-28

### Added

- Add `--delete` (rsync-style mirror) to `rcp` and `rlink`: removes destination entries with no
  source counterpart. Implies `--overwrite`; supports `--delete-excluded`; honors `--dry-run`. Local
  operations only for now (remote `rcp` support planned).
- Add `rchm`: a fast recursive chmod/chgrp/chown tool for large filesets (a `dchmod` replacement),
  with a per-type `--mode`/`--group`/`--owner` DSL, no-op skipping, pre-order directory changes by
  default (so `--mode d:u+rwx` can repair an unreadable directory; `--defer-dir-changes` applies
  directories after their contents), progress, filtering, and throttling.

## [0.32.0] - 2026-05-16

### Added

- Add `--auto-meta-throttle` adaptive congestion control for metadata operations, with per-side
  (source/destination) and per-syscall controllers that dynamically tune concurrency and rate based
  on observed latency. See `docs/congestion_control.md` for design.
- Add `--auto-meta-histogram`, `--auto-meta-histogram-log <PATH>`, and
  `--auto-meta-histogram-interval <DUR>` for per-(side, op) HDR latency histograms with a live
  distribution panel and binary log file. The log carries per-tick progress snapshots (ops/s, files
  copied, bytes copied, etc.) interleaved with histogram records so offline tools can correlate
  latency distributions with throughput.
- Add `--skip-specials` flag to skip non-copyable objects (sockets, FIFOs, devices) silently
- Add age-based filtering to `rrm` via `--modified-before` / `--created-before`, applied to both
  files and directories

### Changed

- Upgrade workspace to Rust 2024 edition; modernize code to use `let` chains and 2024 idioms
- Surface remote stderr when rcpd deployment fails with broken pipe to aid diagnosis

### Fixed

- Fix `--fail-early` race in remote copy that swallowed file-level errors
- Propagate non-`AlreadyExists` errors from hard-link helper instead of silently ignoring them

## [0.31.0] - 2026-04-02

### Fixed

- Fix setuid/setgid bit preservation during file copy

## [0.30.0] - 2026-03-30

### Added

- Add `--overwrite-filter=newer` to skip overwriting newer destination files
- Add `--ignore-existing` to skip copying over existing destinations
- Accept colon as port range separator in `--port-ranges`

## [0.29.0] - 2026-03-17

### Added

- Add `--preserve-settings` to `rlink` for controlling metadata preservation on directories,
  symlinks, and copied files. Supports presets (`all`, `none`) and custom per-type format. Defaults
  to `all` (preserving backward compatibility).
- Add `--allow-lossy-update` to `rlink` as a safety guard when `--update` comparison attributes are
  not covered by `--preserve-settings`
- Add `all` and `none` presets to `--preserve-settings` in `rcp`
- Add `--expand-missing` flag to `rcmp` to report missing entries individually
- Report total bytes removed in `rrm` and sizes for compared datasets in `rcmp`

### Changed

- Change `rcmp` output format to JSON

### Deprecated

- Deprecate `--preserve` flag in `rcp` in favor of `--preserve-settings=all`

### Fixed

- Replace generic error messages with actual root causes in non-fail-early mode
- Support relative local paths in remote copy

## [0.28.0] - 2026-02-19

### Added

- Add `--include`/`--exclude` glob filters for selective file operations
- Report skipped (filtered out) entries in progress bar

### Fixed

- Fix `--include`/`--exclude` filter bugs with empty directory handling
- Fix remote copy directory completion ordering
- Fix remote copy bug where a directory could be completed while contents were still being added
- Fix destination permission errors not allowing continuation without `--fail-early`
- Fix `--summary` output by separating counts by copy/link/remove and disambiguating 'skipped'
- Fix `--dry-run` output when used with other flags

## [0.27.0] - 2026-01-23

### Fixed

- Fix docs.rs build by adding required package metadata to all crates

## [0.26.0] - 2026-01-22

### Changed

- Simplify release process configuration

### Fixed

- Fix Debian and RPM package builds

## [0.24.0] - 2026-01-21

### Changed

- Improve release process with automated package builds via GitHub Actions on tag push

## [0.23.0] - 2026-01-21

### Added

- Backpressure mechanism for remote copy sender to prevent overwhelming slow receivers
- Chaos testing infrastructure for protocol and I/O fault injection

### Changed

- `rcmp` now outputs differences to stdout when no log file is provided
- Reduce default number of parallel file writes in `filegen`
- Increase default connection timeout to 60s when `--auto-deploy-rcpd` is enabled
- Optimize TLS counters using sharded atomics with cache line padding

### Fixed

- Fix bug where directory metadata would be skipped if any child failed to copy
- Fix `filegen` progress tracking to update on all file writes, not just on completion
- Fix Debian and RHEL package builds to properly find musl toolchain
- Fix cargo publish to work with musl

## [0.22.0] - 2025-12-16

### Added

- **TLS encryption and authentication** for remote copy operations (enabled by default)
  - Mutual TLS with self-signed certificates and fingerprint pinning
  - Master distributes certificate fingerprints via SSH for secure key exchange
  - Use `--no-encryption` to disable for trusted networks (disables both encryption AND
    authentication)
- **Automatic rcpd deployment** (`--auto-deploy-rcpd` flag)
  - Automatically deploys rcpd binary to remote hosts via SSH
  - SHA-256 checksum verification for transfer integrity
  - Atomic deployment using temp files for concurrent safety
  - Version-based caching to `~/.cache/rcp/bin/rcpd-{version}`
  - Automatic cleanup of old versions (keeps last 3)
- **Protocol version checking** between rcp and rcpd to detect version mismatches
- **Docker-based multi-host integration tests** for testing actual remote scenarios
- **Support for `~` in remote paths** (e.g., `host:~/path/to/file`)
- **Connection pooling** for data streams with configurable pool size (`--max-connections`)
- **Performance tracing instrumentation** for profiling critical paths
- **Profiling support** via `--chrome-trace` and `--flamegraph` options
- **Configurable buffer sizes** for remote file copies (`--remote-copy-buffer-size`)
- **`--bind-ip` option** to specify local IP address for remote connections

### Changed

- **BREAKING**: Remote copy now uses TCP instead of QUIC for data transfer
  - Removed `--quic-idle-timeout-sec`, `--quic-keep-alive-interval-sec`, and other QUIC-specific
    options
  - Simplified protocol with better performance characteristics
- **BREAKING**: Static musl builds are now the default configuration
  - Enables automatic deployment to hosts without matching glibc versions
- Simplified remote copy protocol with stream pooling for better throughput
- Socket buffers are now maximized for high bandwidth transfers
- Improved error messages to guide users toward `--auto-deploy-rcpd` when rcpd is not found

### Fixed

- Fixed file mtime preservation - file contents are now flushed before setting mtime
- Fixed parsing of paths containing colons (e.g., `C:\path` on Windows paths in arguments)
- Fixed deadlock in source when destination fails with `--fail-early` and closes connections
- Fixed resource usage stats display showing invalid walltime values
- Fixed rcpd path discovery order
- Various test stability improvements

### Removed

- QUIC transport layer and all related configuration options
- `docs/quic_performance_tuning.md` (no longer applicable)

## [0.21.0] - 2025-10-24

### Added

- Configurable connection timeout for remote operations via `--remote-copy-conn-timeout-sec`
  (default: 15s)
- stdin watchdog in `rcpd` to detect master process disconnection immediately
- Automatic cleanup of `rcpd` processes when master (`rcp`) dies or disconnects
- Comprehensive lifecycle management tests for remote copy operations
- CI lint to detect and prevent `anyhow::Error::msg()` usage that destroys error chains
- Test coverage for error chain preservation across `rcp`, `rrm`, `rlink`, `rcmp`, and `filegen`:
  - `parent_dir_no_write_permission` - verifies permission errors are visible in rm operations
  - `test_destination_permission_error_includes_root_cause` - verifies permission errors in copy
    operations
  - `test_permission_error_includes_root_cause` - verifies permission errors in filegen and link
    operations

### Changed

- `rcpd` now automatically exits when master process dies (via stdin monitoring + connection close
  detection)
- Error types (`copy::Error`, `link::Error`, `rm::Error`, `filegen::Error`) now use
  `#[error("{source:#}")]` to automatically display full error chains
- All error logging now uses `{:#}` format consistently for better error chain visibility
- Multi-operation failures now preserve the first error with context instead of generic failure
  messages

### Fixed

- **CRITICAL**: Fixed error chain destruction in 21 locations across all tools where
  `anyhow::Error::msg()` was converting errors to strings
- `rcpd` processes no longer remain orphaned on remote hosts after master crash
- Remote copy operations now detect dead connections within seconds instead of hanging indefinitely
- Error messages now consistently show root causes (e.g., "Permission denied", "No space left on
  device", "Disk quota exceeded")
- Permission denied errors in parent directories are now properly reported with full context
- Error logging in main binaries (`rcp`, `rrm`, `rlink`) now uses consistent `{:#}` format

## [0.20.0] - 2025-01-19

### Added

- Remote copy operations now respect `--progress-type` flag (Auto/ProgressBar/TextUpdates)
- TextUpdates progress mode now includes timestamps matching log format
- `rcmp` now supports `--progress-type` flag
- Support for special file types (sockets, FIFOs, block/character devices) in `rcmp` via
  `ObjType::Other`
- Installation instructions for cargo/crates.io
- Documentation links to docs.rs for all tools

### Fixed

- Backward compatibility for `--progress-type` argument parsing (both PascalCase and kebab-case now
  work)
- `filegen` argument ordering restored to previous behavior
- Log timestamps now correctly use local time

## [0.19.0] and earlier

See git history for changes in previous versions.

[Unreleased]: https://github.com/wykurz/rcp/compare/v0.38.0...HEAD
[0.38.0]: https://github.com/wykurz/rcp/compare/v0.37.0...v0.38.0
[0.37.0]: https://github.com/wykurz/rcp/compare/v0.36.0...v0.37.0
[0.36.0]: https://github.com/wykurz/rcp/compare/v0.35.0...v0.36.0
[0.35.0]: https://github.com/wykurz/rcp/compare/v0.34.0...v0.35.0
[0.34.0]: https://github.com/wykurz/rcp/compare/v0.33.0...v0.34.0
[0.33.0]: https://github.com/wykurz/rcp/compare/v0.32.0...v0.33.0
[0.32.0]: https://github.com/wykurz/rcp/compare/v0.31.0...v0.32.0
[0.31.0]: https://github.com/wykurz/rcp/compare/v0.30.0...v0.31.0
[0.30.0]: https://github.com/wykurz/rcp/compare/v0.29.0...v0.30.0
[0.29.0]: https://github.com/wykurz/rcp/compare/v0.28.0...v0.29.0
[0.28.0]: https://github.com/wykurz/rcp/compare/v0.27.0...v0.28.0
[0.27.0]: https://github.com/wykurz/rcp/compare/v0.26.0...v0.27.0
[0.26.0]: https://github.com/wykurz/rcp/compare/v0.24.0...v0.26.0
[0.24.0]: https://github.com/wykurz/rcp/compare/v0.23.0...v0.24.0
[0.23.0]: https://github.com/wykurz/rcp/compare/v0.22.0...v0.23.0
[0.22.0]: https://github.com/wykurz/rcp/compare/v0.21.1...v0.22.0
[0.21.0]: https://github.com/wykurz/rcp/compare/v0.20.0...v0.21.0
[0.20.0]: https://github.com/wykurz/rcp/compare/v0.19.0...v0.20.0
