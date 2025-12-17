# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.22.0] - 2025-12-16

### Added
- **TLS encryption and authentication** for remote copy operations (enabled by default)
  - Mutual TLS with self-signed certificates and fingerprint pinning
  - Master distributes certificate fingerprints via SSH for secure key exchange
  - Use `--no-encryption` to disable for trusted networks (disables both encryption AND authentication)
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
  - Removed `--quic-idle-timeout-sec`, `--quic-keep-alive-interval-sec`, and other QUIC-specific options
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
- Configurable connection timeout for remote operations via `--remote-copy-conn-timeout-sec` (default: 15s)
- stdin watchdog in `rcpd` to detect master process disconnection immediately
- Automatic cleanup of `rcpd` processes when master (`rcp`) dies or disconnects
- Comprehensive lifecycle management tests for remote copy operations
- CI lint to detect and prevent `anyhow::Error::msg()` usage that destroys error chains
- Test coverage for error chain preservation across `rcp`, `rrm`, `rlink`, `rcmp`, and `filegen`:
  - `parent_dir_no_write_permission` - verifies permission errors are visible in rm operations
  - `test_destination_permission_error_includes_root_cause` - verifies permission errors in copy operations
  - `test_permission_error_includes_root_cause` - verifies permission errors in filegen and link operations

### Changed
- `rcpd` now automatically exits when master process dies (via stdin monitoring + connection close detection)
- Error types (`copy::Error`, `link::Error`, `rm::Error`, `filegen::Error`) now use `#[error("{source:#}")]` to automatically display full error chains
- All error logging now uses `{:#}` format consistently for better error chain visibility
- Multi-operation failures now preserve the first error with context instead of generic failure messages

### Fixed
- **CRITICAL**: Fixed error chain destruction in 21 locations across all tools where `anyhow::Error::msg()` was converting errors to strings
- `rcpd` processes no longer remain orphaned on remote hosts after master crash
- Remote copy operations now detect dead connections within seconds instead of hanging indefinitely
- Error messages now consistently show root causes (e.g., "Permission denied", "No space left on device", "Disk quota exceeded")
- Permission denied errors in parent directories are now properly reported with full context
- Error logging in main binaries (`rcp`, `rrm`, `rlink`) now uses consistent `{:#}` format

## [0.20.0] - 2025-01-19

### Added
- Remote copy operations now respect `--progress-type` flag (Auto/ProgressBar/TextUpdates)
- TextUpdates progress mode now includes timestamps matching log format
- `rcmp` now supports `--progress-type` flag
- Support for special file types (sockets, FIFOs, block/character devices) in `rcmp` via `ObjType::Other`
- Installation instructions for cargo/crates.io
- Documentation links to docs.rs for all tools

### Fixed
- Backward compatibility for `--progress-type` argument parsing (both PascalCase and kebab-case now work)
- `filegen` argument ordering restored to previous behavior
- Log timestamps now correctly use local time

## [0.19.0] and earlier

See git history for changes in previous versions.

[Unreleased]: https://github.com/wykurz/rcp/compare/v0.22.0...HEAD
[0.22.0]: https://github.com/wykurz/rcp/compare/v0.21.1...v0.22.0
[0.21.0]: https://github.com/wykurz/rcp/compare/v0.20.0...v0.21.0
[0.20.0]: https://github.com/wykurz/rcp/compare/v0.19.0...v0.20.0
