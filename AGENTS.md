# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Build Commands

This project uses [`just`](https://github.com/casey/just) for common development tasks.

### Setup

**Using nix (recommended):**
```bash
nix develop  # Automatically includes just and all dev tools
```

**Without nix:**
```bash
cargo install just
cargo install cargo-nextest  # Optional but recommended for testing
```

### Common Commands

- **List all commands**: `just --list` or just `just`
- **Run lints**: `just lint` (fmt, clippy, error logging, package metadata)
- **Format code**: `just fmt`
- **Run tests (debug)**: `just test` (uses nextest)
- **Run tests (release)**: `just test-release` (uses nextest --release)
- **Run doctests**: `just doctest` (debug mode)
- **Run doctests (release)**: `just doctest-release`
- **Run all tests**: `just test-all` (debug + release + doctests)
- **Quick check**: `just check` (faster than build)
- **Build all**: `just build`
- **Build release**: `just build-release`
- **Check docs**: `just doc`
- **Run all CI checks**: `just ci` (lint + doc + test-all + Docker tests)

**IMPORTANT**: Always run `just ci` before committing changes to ensure:
- ✅ Code formatting is correct (`cargo fmt --check`)
- ✅ Clippy lints pass (`cargo clippy`)
- ✅ Error logging format is correct (custom script checks)
- ✅ Package metadata is consistent across workspace (docs.rs settings, lints)
- ✅ Documentation builds without warnings (`cargo doc --no-deps`)
- ✅ All tests pass in both debug and release modes (`cargo nextest run`)
- ✅ All doctests compile and run (`cargo test --doc`)

**Note**: CI workflows (GitHub Actions) run both debug and release tests in parallel to catch optimization-related bugs. The `just ci` command replicates this locally.

### Direct Cargo Commands

You can also use cargo directly:

- **Build all packages**: `cargo build`
- **Build specific package**: `cargo build -p <package_name>` (e.g., `cargo build -p rcp`)
- **Build for release**: `cargo build --release`
- **Test all packages**: `cargo nextest run` (recommended) or `cargo test --verbose`
- **Test with release optimizations**: `cargo nextest run --release`
- **Test with full output**: `cargo nextest run --no-capture`
- **Test specific package**: `cargo nextest run -p <package_name>` or `cargo test -p <package_name>`
- **Test specific test**: `cargo nextest run --no-capture <test_name>`
- **Run doctests**: `cargo test --doc`
- **Run doctests (release)**: `cargo test --doc --release`
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

### Error Logging Convention

**CRITICAL**: When logging errors (both custom `Error` types and raw errors), **ALWAYS** use alternate display format `{:#}` or debug format `{:?}` to preserve the error chain:

```rust
// ✅ CORRECT - Shows full error chain
tracing::error!("operation failed: {:#}", &error);  // Inline: "failed: Permission denied"
tracing::error!("operation failed: {:?}", &error);  // Multi-line with "Caused by:"

// ❌ WRONG - May lose root cause (will fail CI)
tracing::error!("operation failed: {}", &error);   // Only shows outer message!
```

**Rationale**: Using `{}` (Display format) may hide critical root causes like "Permission denied", "No space left on device", "Disk quota exceeded", etc. Using `{:#}` everywhere ensures consistency and guarantees users always see the underlying system error.

**Implementation**: Custom Error types in `common/src/copy.rs`, `common/src/link.rs`, `common/src/rm.rs`, and `common/src/filegen.rs` use `#[error("{source:#}")]` in their thiserror definition. While this means they work correctly with any format specifier, we use `{:#}` consistently everywhere for simplicity.

**CI Enforcement**: The `scripts/check-error-logging.sh` script automatically checks for this pattern and will fail CI if violations are found.

### Error Chain Preservation

**CRITICAL**: **NEVER** use `anyhow::Error::msg()` to wrap errors. This converts errors to strings and **destroys the error chain**, hiding root causes.

```rust
// ❌ WRONG - Destroys error chain
.map_err(|err| Error::new(anyhow::Error::msg(err), summary))

// ✅ CORRECT - Preserves error chain
// When err is already anyhow::Error (from .with_context()):
.map_err(|err| Error::new(err, summary))

// When err is JoinError:
.map_err(|err| Error::new(err.into(), summary))

// When err is custom Error type:
.map_err(|err| Error::new(err.source, summary))
```

**Rationale**: `anyhow::Error::msg()` converts the error to a string, completely losing the error chain. This is why you won't see "Permission denied" and other underlying errors in logs.

**CI Enforcement**: The `scripts/check-anyhow-error-msg.sh` script automatically detects any usage of `anyhow::Error::msg()` and will fail CI if found.

**Test Coverage**:
- `common/src/rm.rs::tests::parent_dir_no_write_permission`
- `common/src/copy.rs::copy_tests::error_message_tests::test_destination_permission_error_includes_root_cause`
- `common/src/link.rs::link_tests::test_link_destination_permission_error_includes_root_cause`
- `common/src/filegen.rs::tests::test_permission_error_includes_root_cause`

### Package Metadata Consistency

All packages in the workspace must have consistent metadata in their `Cargo.toml`:

1. **Workspace inheritance**: `version.workspace = true`, `edition.workspace = true`, `license.workspace = true`, `repository.workspace = true`
2. **Lints**: `[lints] workspace = true`
3. **docs.rs settings**: All packages must have identical `[package.metadata.docs.rs]` configuration:

```toml
[package.metadata.docs.rs]
cargo-args = ["--config", "build.rustflags=[\"--cfg\", \"tokio_unstable\"]"]
rustdoc-args = ["--cfg", "tokio_unstable"]
```

This ensures docs.rs can build documentation for all crates (since it doesn't use the local `.cargo/config.toml`).

**CI Enforcement**: The `scripts/check-package-metadata.sh` script automatically checks for these patterns.

### Comment and Doc Style

- Doc comments (`///` or `//!`) should start with a capitalized sentence and read naturally.
- Regular code comments (`//`) should start lowercase per repository conventions.

## Testing

The project uses standard Cargo testing. Each tool has its own `tests/` directory with integration tests.

**Test naming**: Name tests after observed behavior (e.g., `copies_directory_tree`, `fails_on_permission_denied`). Use `filegen` crate to create repeatable test fixtures.

**Important for AI Agents**: When modifying function signatures or adding new parameters, always verify that:
1. All tests still pass: `cargo nextest run` or `cargo test`
2. Documentation examples compile: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`

This ensures doc examples stay in sync with actual function signatures.

### Sudo-Required Tests

Some tests require passwordless sudo (e.g., to create root-owned files for testing permission errors). These tests follow a naming convention:

- **Name must contain `sudo`**: e.g., `test_remote_sudo_stream_continues_after_metadata_error`
- **Must be marked `#[ignore]`**: e.g., `#[ignore = "requires passwordless sudo"]`
- **CI runs them separately**: using `cargo nextest run --run-ignored only -E 'test(~sudo)'`

The `test(~sudo)` filter matches test names containing "sudo" anywhere. This is different from `test(/sudo/)` which matches path components.

To run sudo tests locally (requires passwordless sudo configured):
```bash
cargo nextest run --run-ignored only -E 'test(~sudo)'
```

## Remote Operations

The `rcpd` daemon enables distributed copying operations. It connects to a master process (`rcp`) and can run as either source or destination side, using TCP for data transfer.

**IMPORTANT**: Before making any changes to remote copy operations, **always read `docs/remote_protocol.md` first**. This document describes the protocol design and must be kept in sync with the implementation. Use it as the source of truth for how the protocol should behave.

**Environment requirements for remote tests:** localhost SSH must be available and usable (running sshd, accessible via `ssh localhost`). Remote integration tests assert this requirement and will **fail fast** if it is not met; they are never skipped based on environment.

### Connection Timeouts

Both `rcp` and `rcpd` accept CLI arguments to configure connection behavior:

- `--remote-copy-conn-timeout-sec=N` (default: 15) - Connection timeout for remote operations

### rcpd Lifecycle Management

The `rcpd` daemon automatically exits when the master (`rcp`) process dies or disconnects:

1. **stdin watchdog** (primary): Monitors stdin EOF to detect master disconnection immediately
2. **TCP connection close** (backup): Detects dead connections when master's TCP connection closes

This ensures no orphaned `rcpd` processes remain on remote hosts after the master exits unexpectedly.

**Security**: Avoid committing SSH config or keys to the repository.

## Agent Team Workflow

For any non-trivial coding task, create an agent team with two members:

1. **coder** — Creates and executes the task plan, writes code, runs tests, and responds to reviewer feedback.
2. **reviewer** — Reviews all changes made by the coder, checking for correctness, adherence to project conventions, and potential issues.

The coder and reviewer should iterate until both are satisfied with the solution. Only then is the task considered complete.

## Commit Guidelines

Keep commit subjects concise and imperative, following the existing history style (e.g., "Fix how we manage dependencies", "Bump version ..."). Provide context and issue links in the body when applicable.

## Shell Compatibility

**IMPORTANT**: The user's shell is `fish`, not `bash`. When running shell commands:
- Use `bash -c '...'` to wrap bash-specific syntax (like `for` loops, `$(...)` command substitution)
- Or write scripts to `/tmp/*.sh` and execute them
- Simple commands without bash-specific syntax work directly

Example:
```bash
# Instead of: for i in $(seq 1 10); do echo $i; done
# Use: bash -c 'for i in $(seq 1 10); do echo $i; done'
# Or write to a script file and execute it
```
