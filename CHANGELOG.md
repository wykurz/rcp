# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Configurable QUIC connection timeouts for remote operations via CLI arguments:
  - `--quic-idle-timeout-sec` (default: 10s) - maximum idle time before closing QUIC connection
  - `--quic-keep-alive-interval-sec` (default: 1s) - interval for sending keep-alive packets
  - `--remote-copy-conn-timeout-sec` (default: 15s) - connection timeout for remote operations
- stdin watchdog in `rcpd` to detect master process disconnection immediately
- Automatic cleanup of `rcpd` processes when master (`rcp`) dies or disconnects
- Comprehensive lifecycle management tests for remote copy operations
- CI lint to detect and prevent `anyhow::Error::msg()` usage that destroys error chains
- Test coverage for error chain preservation across `rcp`, `rrm`, `rlink`, `rcmp`, and `filegen`:
  - `parent_dir_no_write_permission` - verifies permission errors are visible in rm operations
  - `test_destination_permission_error_includes_root_cause` - verifies permission errors in copy operations
  - `test_permission_error_includes_root_cause` - verifies permission errors in filegen and link operations

### Changed
- `rcpd` now automatically exits when master process dies (via stdin monitoring + QUIC timeouts)
- QUIC connections now have explicit idle timeout and keep-alive configuration
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

[0.20.0]: https://github.com/wykurz/rcp/compare/v0.19.0...v0.20.0
