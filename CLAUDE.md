# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

- **Build all packages**: `cargo build`
- **Build specific package**: `cargo build -p <package_name>` (e.g., `cargo build -p rcp`)
- **Build for release**: `cargo build --release`
- **Test all packages**: `cargo nextest run` (recommended) or `cargo test --verbose`
- **Test with full output**: `cargo nextest run --no-capture`
- **Test specific package**: `cargo nextest run -p <package_name>` or `cargo test -p <package_name>`
- **Test specific test**: `cargo nextest run --no-capture <test_name>`
- **Format code**: `cargo fmt`
- **Check formatting**: `cargo fmt --check`
- **Lint code**: `cargo clippy`
- **Build documentation**: `cargo doc --no-deps` (checks doc examples compile)
- **Build docs with warnings**: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` (treats warnings as errors)

## Project Architecture

This is a Rust workspace containing multiple related CLI tools for efficient file operations:

### Core Tools
- **rcp**: Main file copying tool (much faster than `cp` for large filesets)
- **rrm**: File removal tool (equivalent to `rm -rf`)
- **rlink**: Hard-linking tool
- **rcmp**: File comparison tool (metadata only)
- **rcpd**: Remote copy daemon for distributed operations

### Supporting Crates
- **common**: Shared utilities and types used across all tools
- **throttle**: Rate limiting and resource management
- **remote**: Remote operation protocol definitions
- **filegen**: Test file generation utility

### Key Design Patterns

**Workspace Structure**: Uses Cargo workspace with resolver = "2" for dependency management across multiple binaries.

**Error Handling**: Tools log non-terminal errors and continue by default. Use `--fail-early` flag to fail immediately on any error.

**Progress Reporting**: Progress goes to `stderr`, logs to `stdout`, allowing output redirection while preserving interactive progress bars.

**Copy Semantics**: Unlike `cp`, paths without trailing slash are treated as final destination names, paths with trailing slash are directories to copy into.

**Throttling**: Built-in support for `--ops-throttle` and `--max-open-files` to control resource usage.

## Development Conventions

From CONVENTIONS.md:
- Use fully qualified names for functions and types (e.g., `std::net::SocketAddr`)
- Import macros and traits used in macros explicitly
- Avoid empty lines in functions or type definitions
- Specify only major.minor versions for crate dependencies, not patch versions
- Don't start comments from a capital letter and use dot only to separate multiple sentences.

## Testing

The project uses standard Cargo testing. Each tool has its own `tests/` directory with integration tests.

**Important for AI Agents**: When modifying function signatures or adding new parameters, always verify that:
1. All tests still pass: `cargo nextest run` or `cargo test`
2. Documentation examples compile: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`

This ensures doc examples stay in sync with actual function signatures.

## Remote Operations

The `rcpd` daemon enables distributed copying operations. It connects to a master process (`rcp`) and can run as either source or destination side, using QUIC protocol for communication.

### QUIC Connection Timeouts

Both `rcp` and `rcpd` accept CLI arguments to configure QUIC connection behavior:

- `--quic-idle-timeout-sec=N` (default: 10) - Maximum idle time before closing connection
- `--quic-keep-alive-interval-sec=N` (default: 1) - Interval for keep-alive packets
- `--remote-copy-conn-timeout-sec=N` (default: 15) - Connection timeout for remote operations

These can be tuned for different network environments:
- **LAN**: More aggressive values (5-8s idle timeout) for faster failure detection
- **WAN**: Higher values (15-30s idle timeout) to handle network hiccups
- **High latency**: Increase all timeouts proportionally

### rcpd Lifecycle Management

The `rcpd` daemon automatically exits when the master (`rcp`) process dies or disconnects:

1. **stdin watchdog** (primary): Monitors stdin EOF to detect master disconnection immediately
2. **QUIC idle timeout** (backup): Detects dead connections if stdin monitoring unavailable

This ensures no orphaned `rcpd` processes remain on remote hosts after the master exits unexpectedly.