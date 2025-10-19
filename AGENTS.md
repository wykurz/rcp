# Repository Guidelines

## Project Structure & Module Organization
The workspace bundles eight crates: CLI binaries (`rcp`, `rrm`, `rlink`, `rcmp`) and supporting libraries (`common`, `remote`, `throttle`, `filegen`). Binary entry points live in `<tool>/src`, with integration suites in `<tool>/tests/` (for example, `rcp/tests/remote_tests.rs`). Shared logic and transport code reside in `common/src` and `remote/src`. Documentation assets live under `assets/`, long-form references in `docs/`, and helper tooling in `scripts/`.

## Build, Test, and Development Commands
- `cargo build --workspace` compiles every crate with the pinned Rust 1.90.0 toolchain.
- `cargo test --workspace --all-features` runs unit and integration coverage, including CLI smoke tests.
- `cargo clippy --workspace --all-targets --all-features` must stay clean; warnings fail CI via workspace lints.
- `cargo fmt --all` formats code; use `cargo fmt --all --check` before submitting.
- `nix develop` enters a shell that mirrors the CI toolchain and dependencies (optional but recommended).

## Coding Style & Naming Conventions
Use `rustfmt` defaults (4-space indent) and keep functions tight—no stray blank lines. Prefer fully qualified paths for functions and types, and import macros or traits where used as documented in `CONVENTIONS.md`. Limit crate version constraints to `major.minor`. Start comments in lowercase and reserve periods for multi-sentence notes.

## Testing Guidelines
Add focused unit tests beside their modules and cross-tool scenarios under `<tool>/tests`. Name tests after observed behavior (e.g., `copies_directory_tree`). When touching transfer logic, cover both local and remote paths; leverage `filegen` for repeatable fixtures. Ensure new throttling or error-handling behaviors extend existing assertions instead of replacing them.

## Commit & Pull Request Guidelines
Keep commit subjects concise and imperative, following the existing history (`Fix how we manage dependencies`, `Bump version ...`). Provide context and issue links in the body when applicable. PRs should summarize behavior changes, list validation commands (`cargo test`, `cargo clippy`, `cargo fmt --check`), and note any documentation updates or UI captures affecting users.

## Security & Configuration Tips
Remote workflows launch `rcpd` via SSH; verify binaries exist on participating hosts and avoid committing SSH config or keys.
