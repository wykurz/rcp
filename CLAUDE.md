# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

- **Build all packages**: `cargo build`
- **Build specific package**: `cargo build -p <package_name>` (e.g., `cargo build -p rcp`)
- **Build for release**: `cargo build --release`
- **Test all packages**: `cargo test --verbose`
- **Test specific package**: `cargo test -p <package_name>`
- **Format code**: `cargo fmt`
- **Check formatting**: `cargo fmt --check`
- **Lint code**: `cargo clippy`

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

## Testing

The project uses standard Cargo testing. Each tool has its own `tests/` directory with integration tests.

## Remote Operations

The `rcpd` daemon enables distributed copying operations. It connects to a master process (`rcp`) and can run as either source or destination side, using QUIC protocol for communication.