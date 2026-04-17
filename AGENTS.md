# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

For project overview and tool descriptions, see [README.md](README.md).
For coding conventions, see [CONVENTIONS.md](CONVENTIONS.md).
For design and reference documentation, see the [docs/](docs/) directory.

## Build & Test Commands

This project uses [`just`](https://github.com/casey/just). Always prefer `just` commands over direct `cargo` commands.

### Setup

**Using nix (recommended):**
```bash
nix develop  # automatically includes just and all dev tools
```

**Without nix:**
```bash
cargo install just
cargo install cargo-nextest  # required for `just test`, `just test-release`, `just test-all`, and `just ci`
```

### Common Commands

- `just` or `just --list` — list all available commands
- `just lint` — run lints (fmt, clippy, error logging, package metadata)
- `just fmt` — format code
- `just test` — run tests in debug mode
- `just test-release` — run tests in release mode
- `just doctest` — run doctests
- `just test-all` — all tests (debug + release + doctests)
- `just check` — quick check (faster than build)
- `just build` / `just build-release` — build
- `just doc` — check docs build
- `just ci` — full CI checks (lint + doc + test-all + Docker tests)

**IMPORTANT**: Always run `just ci` before committing changes. CI workflows run debug and release tests in parallel to catch optimization-related bugs.

## Critical Conventions

General coding conventions are in [CONVENTIONS.md](CONVENTIONS.md). Several rules below are enforced by CI scripts (each such subsection explicitly names the check); other guidance is still expected to be followed even if not CI-checked.

### Error Logging

**Always** use alternate display `{:#}` or debug `{:?}` when logging errors — never plain `{}`:

```rust
// ✅ CORRECT - shows full error chain
tracing::error!("operation failed: {:#}", &error);

// ❌ WRONG - may hide root causes like "Permission denied" (fails CI)
tracing::error!("operation failed: {}", &error);
```

Enforced by `scripts/check-error-logging.sh`.

### Error Chain Preservation

**NEVER** use `anyhow::Error::msg()` — it converts errors to strings and destroys the chain.

```rust
// ❌ WRONG - destroys error chain
.map_err(|err| Error::new(anyhow::Error::msg(err), summary))

// ✅ CORRECT - preserves error chain
// Use exactly one of the following, depending on the error type:
// Option A: err is already anyhow::Error
.map_err(|err| Error::new(err, summary))
// Option B: err is JoinError
.map_err(|err| Error::new(err.into(), summary))
// Option C: err is custom Error type
.map_err(|err| Error::new(err.source, summary))
```

Enforced by `scripts/check-anyhow-error-msg.sh`.

### Package Metadata Consistency

All workspace packages must have consistent `Cargo.toml`:

1. Workspace inheritance: `version.workspace = true`, `edition.workspace = true`, `license.workspace = true`, `repository.workspace = true`
2. Lints: `[lints] workspace = true`
3. Identical `[package.metadata.docs.rs]`:

```toml
[package.metadata.docs.rs]
cargo-args = ["--config", "build.rustflags=[\"--cfg\", \"tokio_unstable\"]"]
rustdoc-args = ["--cfg", "tokio_unstable"]
```

Enforced by `scripts/check-package-metadata.sh`.

### Comment & Doc Style

- Doc comments (`///`, `//!`): start with a capitalized sentence, read naturally.
- Regular comments (`//`): start lowercase (see CONVENTIONS.md).

## Testing

Name tests after observed behavior (e.g., `copies_directory_tree`, `fails_on_permission_denied`). Use the `filegen` crate for repeatable fixtures.

When modifying function signatures or parameters, verify:
1. Tests still pass: `just test`
2. Doc examples still compile and run: `just doctest` (and optionally `just doc` to verify generated docs with warnings denied)

For test categories, Docker multi-host tests, chaos tests, and sudo-required tests, see [docs/testing.md](docs/testing.md).

## Remote Operations

Before making any changes to remote copy operations, **always read [docs/remote_protocol.md](docs/remote_protocol.md) first**. It is the source of truth for protocol behavior and must be kept in sync with the implementation. For operational aspects (binary discovery, deployment, connectivity), see [docs/remote_copy.md](docs/remote_copy.md).

**Remote tests** require localhost SSH available and usable (`ssh localhost` must succeed). Tests fail fast if unavailable — they are never skipped based on environment.

**Security**: Never commit SSH config or keys to the repository.

## Agent Team Workflow


After creating a PR, check all review comments (including automated ones from Copilot). Evaluate each on its merit — address valid suggestions and respond to ones that aren't applicable.

## Commit Guidelines

Keep commit subjects concise and imperative, matching the existing history style (e.g., "Fix how we manage dependencies"). Provide context and issue links in the body when applicable.

## Shell Compatibility

The user's shell is `fish`, not `bash`. For bash-specific syntax (`for` loops, `$(...)` substitutions):
- Wrap in `bash -c '...'`, or
- Write to `/tmp/*.sh` and execute

Simple commands without bash-specific syntax work directly.
