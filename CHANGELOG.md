# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

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
  the reused directory fails closed for that directory. Limitations: the lockdown restricts the
  directory MODE only — POSIX access/default ACLs are not snapshotted, stripped, or restored, so a
  permissive **default** ACL is still inherited by freshly written children (the interim `0o700`
  does neutralize a directory's own access ACL, because `chmod` rewrites the ACL mask from the group
  bits; ACL handling is deferred); and on a backend that does not honor `chown`/`chmod` (e.g. CIFS
  without unix extensions) the takeover or finalize fails closed but may leave the reused directory
  copier-owned at its original, possibly permissive, mode. The default (non-strict) path is
  unchanged: reused directories are left as-is.

### Fixed

- Fix a long-standing indefinite hang in remote (`rcpd`) copies under `--fail-early`. When a
  directory failed to be created — out of space, or a `--require-toctou-safe` lockdown that could
  not secure a reused directory — or a transfer task failed, the source's directory walk could stay
  parked on its file-descriptor budget forever, because the budget was not released on the
  destination's abort. The source now releases that budget whenever its control-message dispatch
  loop exits for any reason (a control-stream close, a transport-task error, or a panic), so the
  copy tears down cleanly instead of hanging. The failure is now also reported with its real cause
  (for example a file's `Permission denied`) rather than the internal budget-release wakeup that
  unblocks the parked walk.
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

[Unreleased]: https://github.com/wykurz/rcp/compare/v0.37.0...HEAD
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
