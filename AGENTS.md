# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

For project overview and tool descriptions, see [README.md](README.md). For coding conventions, see
[CONVENTIONS.md](CONVENTIONS.md). For design and reference documentation, see the [docs/](docs/)
directory, and [Documentation](#documentation) for what is written where.

## Library API Scope

Workspace library crates are internal to the rcp tools, even when published. External library API
compatibility is not a project requirement. Choose visibility and signatures for in-repository
callers, updating those callers, tests, and documentation examples together. Remove unused public
entry points or make them private; do not preserve them solely for hypothetical external library
consumers.

## Build & Test Commands

This project uses [`just`](https://github.com/casey/just). Always prefer `just` commands over direct
`cargo` commands.

### Setup

**Using nix (recommended):**

```bash
nix develop  # automatically includes just and all dev tools
```

**Without nix:**

Install [actionlint 1.7.12](https://github.com/rhysd/actionlint/blob/v1.7.12/docs/install.md), `jq`,
and the Python PyYAML package (often named `python3-yaml`) with your system package manager, and
install the report dependencies from `scripts/report-site-requirements.txt` in your Python
environment. Offline AWS safety tests additionally require the pinned SDK and JWT/cryptography
closure in `infra/aws_benchmark/production-requirements.txt` (CPython 3.12 or 3.14, Linux x86_64),
or the dependencies provided by `nix develop`. These tests do not call AWS. The report and AWS
dependencies are required by `just lint`, and so is OpenSSH (`sshd`, `ssh` and `ssh-keygen`): the
benchmark harness tests start a private, unprivileged `sshd` on loopback. Then install the remaining
tools:

```bash
./scripts/cargo-install-host.sh just
./scripts/cargo-install-host.sh cargo-nextest  # required for `just test`, `just test-release`, `just test-all`, and `just ci`
./scripts/cargo-install-host.sh dprint  # required for `just fmt`, `just lint`, and `just ci`
```

### Common Commands

- `just` or `just --list` — list all available commands
- `just lint` — run lints (fmt, clippy, and the repo's CI check scripts)
- `just lint-workflows` — check GitHub workflow syntax, expressions, and action usage
- `just fmt` — format code
- `just test` — run tests in debug mode
- `just test-release` — run tests in release mode
- `just doctest` — run doctests
- `just test-all` — all tests (debug + release + doctests)
- `just check` — quick check (faster than build)
- `just build` / `just build-release` — build
- `just doc` — check docs build
- `just nix-targets` — evaluate Nix target behavior and realize native package/dev-shell smoke
  checks
- `just ci` — full CI checks (lint + Nix target checks when available + doc + test-all + Docker)

### Depot CI

Prefer Depot for remote verification when it is available. `nix develop` supplies the pinned Depot
CLI v2.102.7 and jq, and remote `depot-*` recipes require configured Depot access.

During development, run focused checks for the affected code: prefer `just depot-test` with package,
target or test filters, `just depot-lint`, or the relevant local recipe. Save full `just depot-ci`
verification for the final inputs before opening or updating a PR; use local `just ci` if Depot is
unavailable. Intermediate commits do not require full CI, and committing already-tested inputs does
not invalidate that verification. Source changes after the full run require fresh verification
before publication.

Depot runs the current worktree without a commit or push, but its local patch excludes ordinary
untracked files. Stage any ordinary untracked files needed by the check; a successful run covers
only the inputs present when it started. `depot-test` runs one x86 debug job and accepts the same
arguments as `just test`; `depot-test-all` and `depot-ci` retain the complete test configurations.

For targeted remote verification, use `just depot-test`, `just depot-test-release`,
`just depot-doctest`, `just depot-doctest-release`, `just depot-doc`, `just depot-test-all`,
`just depot-docker-test`, `just depot-docker-chaos-test`, `just depot-chaos`, or
`just depot-test-all-with-docker`. `just depot-lint` includes lint and Nix target checks. Full
`depot-ci` includes regular Docker coverage; GitHub runs chaos separately after merges to main.

## Critical Conventions

General coding conventions are in [CONVENTIONS.md](CONVENTIONS.md). Several rules below are enforced
by CI scripts (each such subsection explicitly names the check); other guidance is still expected to
be followed even if not CI-checked.

### Performance and Resource Tuning

Performance and resource-policy heuristics should have documented defaults and CLI overrides, so
users can tune them for their workload and hardware. Keep independently useful controls separate
(for example, I/O chunk size and idle buffer retention). Correctness and protocol invariants do not
need tuning knobs.

### Error Logging

**Always** use alternate display `{:#}` or debug `{:?}` when logging errors — never plain `{}`:

```rust
// ✅ CORRECT - shows full error chain
tracing::error!("operation failed: {:#}", &error);

// ❌ WRONG - may hide root causes like "Permission denied" (fails CI)
tracing::error!("operation failed: {}", &error);
```

This holds at every `tracing` level, not just `error!`; for inline captures (write `{e:#}`, never a
bare `{e}`); and for structured fields (`?error` records Debug and is fine, `%error` records Display
and is not).

`scripts/check-error-logging.sh` is a **heuristic backstop, not a proof** — it is awk, so it cannot
pair format placeholders with arguments in general and it recognises errors by name. Its rules are
deliberately sound rather than complete: a clean run means "none of the known shapes", so still read
log lines you touch. Its exact behaviour in both directions is pinned by
`scripts/test-check-error-logging.sh` against the fixtures in `scripts/tests/error-logging/` — add a
case there whenever you change a rule.

If a binding only *looks* like an error — most often a `String` carrying an already-rendered
message, where `{:#}` would add nothing — opt out on the line above, with a reason. That placement
is the only one accepted, and deliberately so: the marker must be a comment on the line
**immediately** above the macro, with no blank line between. A marker in a trailing comment, inside
the format string, or on any preceding line of code does not suppress anything — otherwise an
unrelated mention of the word could silence a real violation without a reason attached to it.

```rust
// rcp-error-log-allow: RcpdResult::Failure.error is a String off the wire, not a chain
tracing::error!("rcpd operation failed: {error}");
```

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

1. Workspace inheritance: `version.workspace = true`, `edition.workspace = true`,
   `license.workspace = true`, `repository.workspace = true`
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
- Markdown docs: formatted with dprint, prose hard-wrapped at 100 columns — `just fmt` fixes.

Markdown formatting is enforced by `scripts/check-doc-format.sh`.

### Structural Simplification

Always look for structural simplifications in the area you touch. The goal is simplicity —
de-duplication is a means to it, not an absolute rule. The most common smell: re-deriving a value
from raw input that an earlier stage already computed (e.g. re-parsing a string the CLI already
parsed into a typed value); re-derivation drifts from the source of truth and breeds inconsistency
bugs. Thread the already-computed value through when that is the simpler shape; keep a small
redundancy when *it* is. Apply low-risk simplifications as part of the change; describe larger
restructurings in the PR rather than bundling them.

### Prefer Structural Fixes Over Repeated Patching

When code — especially delicate concurrency / teardown / error-handling code — accumulates repeated
bug reports or review findings, stop patching symptom by symptom. Step back and ask whether a
**structural** change would make the whole thing simpler to reason about and its invariants
verifiable *by construction*. Reach for:

- **RAII guards** so an invariant holds on **every** exit path — early return, `break`, `?`, panic —
  instead of the same cleanup/signal being repeated (and eventually forgotten) at each call site. A
  missed exit path is the single most common source of the recurring findings here.
- **The type system** — type-state, newtypes, and enums that make illegal states unrepresentable —
  so the compiler enforces the invariant rather than a comment asking a reader to.
- **Consolidation** — move a scattered check or cleanup into one place a reader can verify once.

For any change of non-trivial size or risk, consider **launching subagents to review different areas
or aspects in parallel** (one per module or concern) before settling on an approach — an independent
read catches missed exit paths, aliasing, and invariant gaps that a single pass misses.

### TOCTOU Checks Must Bind the Action

Before changing filesystem-race hardening, read [docs/tocttou.md](docs/tocttou.md): its guarantees
are what the implementation must keep. A change that alters a guarantee changes the document with
it; any other change leaves it as it is (see [Documentation](#documentation)).

A check is security-bearing only when the action uses the same held descriptor or open file
description, later work uses the verified held descriptor, or one kernel primitive makes the check
and action atomic. A metadata or identity observation followed by a separate by-name action does not
bind that action; do not add one or describe it as fail-closed.

A by-name action through a pinned parent guarantees containment and syscall type, not exact
final-component identity; a compatible replacement may be affected. Kind, size, and metadata may
still guide filtering, planning, dispatch, and accounting when they are documented as snapshots.

## Documentation

What the project knows is written at three levels, and each fact has one home. A fact written at the
wrong level is what makes a document grow until nobody reads it.

- **Design**: [docs/remote_protocol.md](docs/remote_protocol.md), [docs/tocttou.md](docs/tocttou.md)
  and every `DESIGN.md`. What the parts are responsible for, what they promise each other (messages,
  their order, guarantees), and why the design is this way.
- **Module**: the `//!` comment at the top of a module. How this module keeps its promises: what it
  owns, how its parts work together, its ordering, limits and timeouts.
- **Item**: `///` and `//` at the code. Why this function, branch or constant is the way it is.

**The test for a design document:** a statement belongs there if it would still be true after the
implementation was rewritten to the same design. How a promise is kept belongs in the module that
keeps it. In the remote protocol, for example:

- **Design.** "The destination sends `DestinationDone` and closes its control send stream;
  `DestinationDone` triggers source shutdown." A message and its order between two parts.
- **Module.** "The stdout writer skips a queued control frame whose acknowledgement receiver was
  dropped." True of one implementation: it belongs in the module comment of that writer.
- **Both.** What a field on the wire means and why it exists is design. What its receiver does with
  it, such as which wait it shortens and by how much, is the receiver's module.

### Read the design, and usually leave it alone

Read the design documents of an area before changing it: they say what must stay true. Most changes
then need no edit to them, because most changes alter how a promise is kept and not the promise.
Edit a design document only when the change makes one of its statements false, or changes the design
itself: a message, an ordering, a guarantee, a responsibility. That is rare and deserves a second
look, so say in the pull request which statement changed and why. When no edit was needed, say that
as well ("read `docs/remote_protocol.md`: no change"): it is the expected outcome, not an omission.

A design document does not record what a change did, how a bug came about, or how something is
implemented. What changed and why goes in the commit message and the pull request. What a user will
notice goes in `CHANGELOG.md`, in two or three lines.

### Module comments

Give a module a `//!` comment when it owns behaviour that its function signatures do not show: what
the module is for, what it promises its callers, and how its parts work together. When you are about
to explain an implementation in a design document, this is where it goes. Keep the comment current:
when you change how the module works, change it in the same commit. Link the items it names
(``[`Type`]``, ``[`function`]``): `just doc` fails on a broken link in the documentation it builds,
where a name in a Markdown file goes stale unnoticed.

### Guides for users and operators

`README.md`, `docs/remote_copy.md`, `docs/acls.md`, `docs/security.md` and the guides under
`infra/aws_benchmark/` are for people who run the tools. They change when what such a person does or
sees changes: a flag, a default, an output, a step. They do not explain internals.

### When reviewing

- An addition to a design document is a finding unless the design changed. Ask for it to move into
  the module comment, or to be dropped.
- Do not ask for a design document to be updated because the implementation changed. If an
  explanation is missing, ask for a module or item comment.
- A statement in a design document that has become false is a finding. It is corrected if it is
  design, and otherwise deleted or moved into the code.

## Testing

### Nix sandbox test selection

Follow the canonical
[Nix sandbox test selection guidance](docs/testing.md#nix-sandbox-test-selection). Keep sandbox
selection source-owned through `rcp_nix_sandbox`; do not add name-based skip lists to the flake or
claim that downstream packaging has adopted this contract without verifying it.

Name tests after observed behavior (e.g., `copies_directory_tree`, `fails_on_permission_denied`).
Use the `filegen` crate for repeatable fixtures.

When modifying function signatures or parameters, verify:

1. Tests still pass: prefer `just depot-test`; otherwise run local `just test`.
2. Doc examples still compile and run: prefer `just depot-doctest`; otherwise run local
   `just doctest`. To verify generated docs with warnings denied, prefer `just depot-doc`; otherwise
   run local `just doc`.

For test categories, Docker multi-host tests, chaos tests, and sudo-required tests, see
[docs/testing.md](docs/testing.md).

## Remote Operations

Before making any changes to remote copy operations, **always read
[docs/remote_protocol.md](docs/remote_protocol.md) first**. It says what the master and the two
daemons promise each other, and a change must keep those promises or change the document with them.
Most changes need no edit to it (see [Documentation](#documentation)). For operational aspects
(binary discovery, deployment, connectivity), see [docs/remote_copy.md](docs/remote_copy.md).

**Remote tests** require localhost SSH available and usable (`ssh localhost` must succeed). Tests
fail fast if unavailable — they are never skipped based on environment.

**Security**: Never commit SSH config or keys to the repository.

## Pull Requests

Dedicated feature worktrees are always integrated through a pull request. Never merge a worktree
branch into `main` locally. Finish the change on the worktree's branch, run the required
verification, and commit and push that branch. If the branch already has an open PR, push follow-up
commits to that same source branch so the existing PR updates; do not create another PR. Keep the
worktree available for review follow-ups.

RCP permits passing pull requests whose branch is behind the current base. At publication and final
handoff, fetch the base and check for conflicts or interacting upstream changes, but do not rebase
solely because the base advanced. Rebase when needed for conflicts, when interacting changes need
verification together, or at the user's request; rerun required verification if this changes tested
inputs. This repository policy overrides generic requirements to rebase unconditionally before
publication or after every base update.

Keep **Rebase and merge** as the only allowed GitHub merge method and preserve required linear
history. GitHub replays the PR commits onto the current base when it merges the PR; a separate
branch update is unnecessary for unrelated base changes. Existing required PR checks remain in
force, and full CI on `main` verifies the combined result after merge.

When the change is in an area that has a design document, say in the PR description what reading it
led to: no edit, which is the usual case, or which statement changed and why (see
[Documentation](#documentation)).

After creating a PR, check all review comments (including automated ones from Copilot). Evaluate
each on its merit — address valid suggestions and respond to ones that aren't applicable.

## Commit Guidelines

Keep commit subjects concise and imperative, matching the existing history style (e.g., "Fix how we
manage dependencies"). Provide context and issue links in the body when applicable.

Squash work-in-progress commits before opening a PR — land clean, reviewable commits. Never commit
working specs or implementation plans (e.g. `docs/superpowers/`, which is gitignored); they are
session artifacts.

## Shell Compatibility

The user's shell is `fish`, not `bash`. For bash-specific syntax (`for` loops, `$(...)`
substitutions):

- Wrap in `bash -c '...'`, or
- Write to `/tmp/*.sh` and execute

Simple commands without bash-specific syntax work directly.
