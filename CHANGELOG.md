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

### Changed
- `rcpd` now automatically exits when master process dies (via stdin monitoring + QUIC timeouts)
- QUIC connections now have explicit idle timeout and keep-alive configuration

### Fixed
- `rcpd` processes no longer remain orphaned on remote hosts after master crash
- Remote copy operations now detect dead connections within seconds instead of hanging indefinitely

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
