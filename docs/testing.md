# Testing

This document covers testing approaches, infrastructure, and best practices for rcp.

## Overview

The rcp test suite includes:

- **Unit tests**: Core functionality in `common/`, `throttle/`, `remote/` crates
- **Integration tests**: Local and remote file operations
- **Docker multi-host tests**: True multi-host scenarios across separate containers

## Running Tests

On supported Linux hosts, Just and `scripts/cargo-host.sh` select the host's musl target, and the
default Nix shell supplies its matching toolchain. Raw `cargo` instead follows
`.cargo/config.toml`'s fixed `x86_64-unknown-linux-musl` distribution default. Set
`CARGO_BUILD_TARGET` explicitly when a different target is intentional; the wrapper preserves that
override.

### Quick Reference

```bash
# Using just (recommended)
just test              # Run all tests (debug mode, uses nextest)
just test-release      # Run tests in release mode
just doctest           # Run documentation tests
just test-all          # Run all tests (debug + release + doctests)
just nix-targets       # Evaluate and realize Nix target smoke checks
just ci                # Full CI checks (lint + Nix when available + doc + test-all + Docker)

# Using cargo through the host wrapper
./scripts/cargo-host.sh nextest run
./scripts/cargo-host.sh nextest run -p <package>
./scripts/cargo-host.sh nextest run --no-capture <test_name>
./scripts/cargo-host.sh test --doc
```

### Test Profiles

The project uses [cargo-nextest](https://nexte.st/) for faster test execution.

Fixtures that mutate process-global admission, congestion, hooks, or file-descriptor state are
supported through nextest's per-test process isolation or through libtest with `--test-threads=1`.
Parallel in-process libtest is unsupported for these fixtures. The standard `just test` and
`just ci` paths use nextest; the Nix libtest check phase keeps `--test-threads=1` for the same
contract. Do not add broad serialization for unrelated tests.

```bash
# Default profile (debug tests)
./scripts/cargo-host.sh nextest run

# Release profile
./scripts/cargo-host.sh nextest run --release

# Docker profile (for multi-host tests)
./scripts/cargo-host.sh nextest run --profile docker --run-ignored only
```

### Nix sandbox test selection

Builds through this repository's flake set `rcp_nix_sandbox`; ordinary Cargo and nextest runs do
not. Put an individual skip immediately before its existing test attribute, and name the exact
unavailable prerequisite:

```rust
#[cfg_attr(
    rcp_nix_sandbox,
    ignore = "Nix sandbox cannot write POSIX ACL xattrs"
)]
#[test]
```

Use a whole-target gate only when every test in that target has the same prerequisite. The flake
sets this cfg and `--test-threads=1`; it does not select tests by name.

The flake passes the same matching `target.'cfg(all())'.rustflags` Cargo configuration to its build
and check phases. Cargo joins that entry with exact-target flags supplied by the Rust package hooks,
whereas a matching target entry would suppress `build.rustflags`. Keeping the two phases identical
also prevents a rustflag-only mismatch from invalidating otherwise reusable release units during the
check. Do not use the `RUSTFLAGS` environment variable for this: it takes precedence over all target
entries and would replace the packaging flags instead of joining them.

This is a contract of the repository flake, not a claim that every downstream package recipe has
adopted it. Downstream packagers opt in by carrying the same cfg and build/check configuration.
Verify the sandbox contract with:

```bash
nix build --no-update-lock-file -L .#rcp-all
```

## Test Categories

### Unit Tests

Core functionality tests in each crate:

- `common/`: Path parsing, error handling, metadata operations
- `throttle/`: Rate limiting, resource management
- `remote/`: Protocol messages, serialization

### Integration Tests

Local file operation tests in each tool's `tests/` directory:

- **rcp**: Copy operations, metadata preservation, error handling
- **rrm**: File removal, permission handling
- **rlink**: Hard-linking operations
- **rcmp**: File comparison

**POSIX ACL tests** (`rcp/tests/acls.rs`, `rlink/tests/acls.rs`, and unit tests in
`common/src/safedir.rs`) need a temp filesystem that can hold `system.posix_acl_*` extended
attributes. Per the repo's convention they **fail rather than skip** when it cannot, so a lost
feature cannot pass unnoticed — a failure there means `TMPDIR` is on a filesystem without ACL
support, not that the code is broken. Fixtures write the xattrs directly rather than shelling out to
`setfacl`, so no runtime dependency is added; `pkgs.acl` is in the dev shell so `getfacl` is on hand
for reading an ACL by eye while debugging, but nothing in the suite uses it. A few of these tests
count syscalls with `strace(1)` (the whole point of `acl` being opt-in is that the default path
costs nothing, which no outcome-only check can show), so `strace` must be installed — it is in the
dev shell. See [POSIX ACLs](acls.md).

### Remote Integration Tests

Tests using localhost SSH (`rcp/tests/remote_tests.rs` and the real-session tests in
`remote/src/lib.rs`):

- Single file and directory copy
- Symlink handling
- Metadata preservation
- Error scenarios (unreadable files, permission errors)
- rcpd lifecycle management

**Requirements**: localhost SSH must be available and usable (running sshd, accessible via
`ssh localhost`).

Keep rcp's localhost-SSH integration tests in the `remote_tests` target and remote-crate
real-session tests in `tests::localhost_ssh_tests`. Nextest selects those two structural locations
for its serial group; test names do not control membership. Launcher fakes and socket-only protocol
tests stay outside those locations and run in parallel.

`rcp/tests/remote_non_ssh_tests.rs` instead covers localhost-as-local behavior and validation
failures that must occur before SSH setup, so it runs without that prerequisite.

### Sudo-Required Tests

Some tests require passwordless sudo (e.g., creating root-owned files):

- **Naming convention**: Test name must contain `sudo`
- **Marked with**: `#[ignore = "requires passwordless sudo"]`
- **CI runs separately**: `./scripts/cargo-host.sh nextest run --run-ignored only -E 'test(~sudo)'`

To run locally:

```bash
./scripts/cargo-host.sh nextest run --run-ignored only -E 'test(~sudo)'
```

Most of these use sudo only to plant root-owned inputs and still run `rcpd` as the normal user.
`test_remote_sudo_strict_reuse_restores_foreign_owned_dir` is the exception: it runs the whole
`rcp`+`rcpd` chain as root, so root itself must reach localhost over SSH passwordlessly. It skips
when that is unavailable, which keeps it runnable on a workstation — but CI provisions root SSH and
sets `RCP_REQUIRE_ROOT_SSH=1`, which turns the skip into a hard failure so the assertion cannot
silently stop running:

```bash
# run it with the preconditions enforced rather than skipped
RCP_REQUIRE_ROOT_SSH=1 ./scripts/cargo-host.sh nextest run --run-ignored only -E 'test(~sudo)'
```

## Docker Multi-Host Testing

Docker-based tests provide true multi-host scenarios that localhost tests cannot cover.

### Motivation

The Docker tests were created to:

- Test multi-host operations (e.g., `host-a:/src → host-b:/dst`)
- Catch connection ordering bugs (role assignment when connections arrive out of order)
- Provide deterministic testing of timing-sensitive scenarios

### Architecture

Three Alpine Linux containers simulate separate hosts:

```
┌─────────────────┐
│     master      │  Runs rcp commands (coordinator)
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
┌───v───┐ ┌───v───┐
│host-a │ │host-b │  SSH servers with rcpd
└───────┘ └───────┘
```

**Container configuration**:

- Based on Alpine Linux 3.19
- OpenSSH server configured with pre-installed SSH keys
- rcp/rcpd binaries mounted from the Linux musl target matching the container architecture
- All containers run as `testuser`
- Containers can SSH to each other by hostname

`scripts/docker-target.sh` resolves `DOCKER_DEFAULT_PLATFORM` first and otherwise queries the Docker
daemon's architecture. The Docker build, binary check, and Compose mounts all use the corresponding
Linux musl target. The helper overwrites `RCP_DOCKER_TARGET` with that resolved value when invoking
Compose; it is internal plumbing, not a user override.

Setup and no-cache rebuild each resolve that target once and do not return until `testuser` can
reach both remote hosts from the master container within one readiness deadline. Rebuild also
rebuilds the selected musl payload before starting the project. Failure diagnostics use the held
target and finite logs without another daemon architecture query. Helper-owned local lifecycles and
workflow-owned CI paths each have exactly one teardown owner.

### Running Docker Tests

**Using just commands (recommended)**:

```bash
# Full lifecycle (build → start → test → stop)
just docker-test

# Development workflow
just docker-up           # Start containers (builds binaries first)
just docker-test-only    # Run tests (containers must be running)
just docker-down         # Stop when done

# Debugging
just docker-logs         # View container logs
just docker-clean        # Clean test files from containers
```

**Using Cargo through the host wrapper**:

```bash
just docker-up
./scripts/cargo-host.sh nextest run --profile docker --run-ignored only
just docker-down
```

The wrapper target is for the runnable host-side test binary. The container payload was built by
`just docker-up` for Docker's independently selected platform.

**Using helper script**:

```bash
# From the repository root
./tests/docker/test-helpers.sh setup      # Build payload and start containers
./tests/docker/test-helpers.sh test-copy  # Quick smoke test
./tests/docker/test-helpers.sh shell      # Open shell in master
./tests/docker/test-helpers.sh stop       # Stop containers
```

### Test Coverage

**Basic multi-host operations** (`docker_multi_host.rs`):

- File copying between separate hosts
- Directory copying with cleanup
- Overwrite protection behavior
- Error handling for missing files

**Source-first startup and role assignment** (`docker_multi_host_role_ordering.rs`):

- Source readiness before destination daemon startup, including a deliberately delayed source role
- Repeated role assignment verification
- Rapid successive operations
- Bidirectional copies (A→B then B→A)

**Key technique - Delayed wrapper**:

The tests use a shell wrapper to delay source-role startup on one host:

```rust
env.exec_rcp_with_delayed_source_rcpd(
    "host-a", // source role is delayed
    "host-b", // destination role is not delayed
    2000,     // delay in ms
    &["host-a:/tmp/src.txt", "host-b:/tmp/dst.txt"],
)
```

The wrapper lets `--protocol-version` probes complete immediately so it preserves their two-second
deadline. The test asserts that source readiness is logged before destination spawn even though the
source role is delayed.

### Developer Setup (WSL2)

**Prerequisites**:

1. **Docker Desktop for Windows** with WSL2 integration:
   - Download from https://www.docker.com/products/docker-desktop/
   - Enable "Use WSL 2 instead of Hyper-V" during installation
   - In Settings → Resources → WSL Integration, enable your WSL distribution

2. **Verify installation**:
   ```bash
   docker --version          # Docker CLI
   docker compose version    # Preferred Compose v2 plugin
   docker info               # Docker daemon connection
   ```

3. **Install Compose if needed**: install the Docker Compose v2 plugin for `docker compose`. The
   helper prefers that interface and falls back to the legacy standalone `docker-compose` command
   when it is already installed.

### Manual Testing

```bash
# Start containers
just docker-up

# Get shell in master container
docker exec -it rcp-test-master /bin/bash

# Inside container, test multi-host copy
ssh host-a "echo 'test data' > /tmp/src.txt"
rcp -vv host-a:/tmp/src.txt host-b:/tmp/dst.txt
ssh host-b "cat /tmp/dst.txt"  # Should output: test data

exit

# Stop containers
just docker-down
```

### Troubleshooting

**Docker daemon not running**:

```bash
docker info  # Check connection
```

→ Start Docker Desktop application

**Permission errors on SSH keys**:

```bash
chmod 600 tests/docker/ssh_keys/id_ed25519
```

**Binaries not found**:

```bash
just docker-build
```

**Containers fail to start**:

```bash
just docker-logs  # Check for errors
just docker-down  # Clean up
just docker-up    # Restart
```

For more troubleshooting, see `tests/docker/README.md`.

## CI Integration

### GitHub Actions Workflow

The `.github/workflows/validate.yml` workflow runs:

1. **Lint, documentation, and policy checks** through the host wrapper and repository scripts
2. **Debug and release tests plus doctests** against the x86_64-musl CI target
3. **Sudo-gated debug and release tests** in their corresponding test jobs
4. **Non-chaos Docker tests** in a separate multi-host job
5. **MSRV, glibc release, and Nix shell/package checks** in dedicated jobs

**Docker job details**:

- Sets up the x86_64-musl toolchain and lets `just docker-up` build the Docker-selected payload
- Starts the Compose project through the repository helper (Compose v2 with legacy fallback) and
  waits for both remote SSH hosts
- Runs `just docker-test-only` with chaos excluded
- Shows finite container logs on failure
- Always cleans up containers (even on failure)

### Running CI Locally

```bash
just ci  # lint + Nix targets when available + doc + test-all (debug + release + doctests) + Docker
```

`just ci` is the primary local "is this ready to push?" gate — it runs the same lint, docs, debug +
release + doctest + Docker checks CI does, plus full Nix target checks when Nix is available. It is
the one command to run before pushing. It's a close proxy for the CI matrix rather than a
byte-for-byte match; a few CI steps live outside it:

- **Sudo-gated tests** (`test(~sudo)`, which need passwordless sudo). CI runs these in a separate
  step, in both debug and release. Run them yourself when a change touches sudo-only behavior:

  ```bash
  ./scripts/cargo-host.sh nextest run --run-ignored only -E 'test(~sudo)'
  ```

- **glibc release build.** CI also builds the workspace for `x86_64-unknown-linux-gnu`. On supported
  x86_64 and AArch64 Linux hosts, `just ci` selects the host architecture's musl target. Raw Cargo's
  fixed x86_64-musl distribution default does not control these wrapped commands. Docker payloads
  independently use `DOCKER_DEFAULT_PLATFORM` first, then the daemon architecture.

- **Chaos tests.** `just ci`'s Docker step runs the full `docker` profile *including* chaos (the
  compose containers are privileged, so they actually run), whereas CI excludes chaos from its main
  Docker job and runs it in a separate `chaos-tests.yml` workflow. So `just ci` does cover chaos
  locally — CI just schedules it separately.

## Chaos Testing

Chaos tests verify rcp's behavior under adverse conditions. They run in Docker containers with
special capabilities and are designed to be reproducible in CI.

### Running Chaos Tests

```bash
# Full lifecycle
just docker-chaos-test

# Development workflow
just docker-up
just docker-chaos-test-only
just docker-down
```

Chaos tests run separately from regular Docker tests in CI (see
`.github/workflows/chaos-tests.yml`).

### Test Categories

**Network condition tests** (`docker_chaos_network.rs`):

- High latency (200ms) - verifies timeouts and protocol resilience
- Bandwidth limits (1 Mbit/s) - verifies throttled transfer completion
- Directory copy under latency - verifies multi-RTT protocol handling

**Process chaos tests** (`docker_chaos_process.rs`):

- Kill rcpd early (before connections established)
- Kill rcpd mid-transfer (tests TCP failure detection)
- Blackhole the source after startup - verifies transport liveness timeout behavior
- Master killed - verifies rcpd cleanup via stdin watchdog

**I/O chaos tests** (`docker_chaos_io.rs`):

- Disk full (ENOSPC) via small tmpfs mount
- Permission denied on destination directory
- Permission denied on source file
- Verifies error chain preservation (root cause visible in stderr)

**Filesystem chaos tests** (`docker_chaos_filesystem.rs`):

- These are **best-effort liveness smoke tests**, not rigorous assertions. A detached background
  process (`docker exec -d` — fire-and-forget, its launch and completion are not checked) *attempts*
  to mutate the source tree (delete files, add files, remove a directory, or mutate under an active
  `--include` filter) around the time a copy runs. Timing is best-effort (a short `sleep` on small
  fixtures), so a given run may not actually overlap traversal/transfer, and the mutation is not
  guaranteed to have happened — the tests do not prove a specific race was exercised.
- The assertion is a proxy for "rcp did not crash": it checks that the outer `docker exec` exit
  status is `0` or `1`. That catches a gross crash — the build sets `panic = "abort"`, so a panic
  raises SIGABRT, and both that and a segfault surface through `docker exec` as `128+n` (`134` /
  `139`), outside the accepted range. Its limits: `docker exec` status `1` is not distinguishable
  from a docker-level failure (the check observes docker's status, not rcp's directly), and a true
  **hang** is not detected at all (`docker exec` has no per-command timeout, so a hang stalls the CI
  job). Proving rcp actually ran and reading its true inner status would require an in-container
  completion marker.

**Protocol stress tests** (`docker_chaos_protocol.rs`):

- Backpressure with slow destination/source (64 kbit/s bandwidth limit)
- Many files (150 files to stress connection pool)
- Limited connections (`--max-connections=10`)
- Large file transfer (10MB to test chunking)
- Combined stress (files + bandwidth + limited connections)

**Note**: Packet loss tests are disabled because `tc netem loss` affects all traffic including SSH,
causing hangs before the copy starts.

### Implementation Details

- **Network simulation**: Linux `tc` (traffic control) with `netem` and `tbf` qdiscs
- **Process control**: `pkill` with SIGKILL, SIGSTOP, SIGCONT signals
- **I/O simulation**: tmpfs mounts for disk full, chmod for permission errors
- **Required capabilities**: `CAP_NET_ADMIN` (tc), `CAP_SYS_ADMIN` (mount)

---

## Design Decisions

**Why Docker over alternatives?**

- **vs. Network namespaces**: Keeps the required Linux network setup inside the test containers
- **vs. VMs**: Faster startup, easier to manage, better CI integration
- **vs. Mock transport**: More realistic, tests actual SSH/TCP stack

**Why Alpine Linux?**

- Small image size (~50MB vs ~150MB for Debian/Ubuntu)
- Fast container startup
- OpenSSH available in package manager

**Why mount binaries instead of COPY?**

- No container rebuild needed for code changes
- Faster iteration during development
- Ensures tests use exact same binary as local builds

**Why musl target?**

- Both supported musl configurations enable static CRT linking for their matching architecture
- Avoids glibc version incompatibilities
- Matches Alpine without making Docker payload selection depend on the developer's host target

## References

- **Docker setup details**: `tests/docker/README.md`
- **Test implementations**: `rcp/tests/` directory
- **Nextest configuration**: `.config/nextest.toml`
