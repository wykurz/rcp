# Testing

This document covers testing approaches, infrastructure, and best practices for rcp.

## Overview

The rcp test suite includes:

- **Unit tests**: Core functionality in `common/`, `throttle/`, `remote/` crates
- **Integration tests**: Local and remote file operations
- **Docker multi-host tests**: True multi-host scenarios across separate containers

## Running Tests

### Quick Reference

```bash
# Using just (recommended)
just test              # Run all tests (debug mode, uses nextest)
just test-release      # Run tests in release mode
just doctest           # Run documentation tests
just test-all          # Run all tests (debug + release + doctests)
just ci                # Full CI checks (lint + doc + test-all + Docker tests)

# Using cargo directly
cargo nextest run                           # All tests
cargo nextest run -p <package>              # Specific package
cargo nextest run --no-capture <test_name>  # Specific test with output
cargo test --doc                            # Documentation tests
```

### Test Profiles

The project uses [cargo-nextest](https://nexte.st/) for faster test execution:

```bash
# Default profile (debug tests)
cargo nextest run

# Release profile
cargo nextest run --release

# Docker profile (for multi-host tests)
cargo nextest run --profile docker --run-ignored only
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

### Remote Integration Tests

Tests using localhost SSH (`rcp/tests/remote_tests.rs`):

- Single file and directory copy
- Symlink handling
- Metadata preservation
- Error scenarios (unreadable files, permission errors)
- rcpd lifecycle management

**Requirements**: localhost SSH must be available and usable (running sshd, accessible via
`ssh localhost`).

### Sudo-Required Tests

Some tests require passwordless sudo (e.g., creating root-owned files):

- **Naming convention**: Test name must contain `sudo`
- **Marked with**: `#[ignore = "requires passwordless sudo"]`
- **CI runs separately**: `cargo nextest run --run-ignored only -E 'test(~sudo)'`

To run locally:

```bash
cargo nextest run --run-ignored only -E 'test(~sudo)'
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
- rcp/rcpd binaries mounted from `target/x86_64-unknown-linux-musl/debug/`
- All containers run as `testuser`
- Containers can SSH to each other by hostname

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

**Using cargo directly**:

```bash
just docker-up
cargo nextest run --profile docker --run-ignored only
just docker-down
```

**Using helper script**:

```bash
cd tests/docker
./test-helpers.sh start      # Start containers
./test-helpers.sh test-copy  # Quick smoke test
./test-helpers.sh shell      # Open shell in master
./test-helpers.sh stop       # Stop containers
```

### Test Coverage

**Basic multi-host operations** (`docker_multi_host.rs`):

- File copying between separate hosts
- Directory copying with cleanup
- Overwrite protection behavior
- Error handling for missing files

**Connection ordering scenarios** (`docker_multi_host_role_ordering.rs`):

- Role assignment verification regardless of connection timing
- Delayed rcpd connection tests (forces specific connection order)
- Rapid successive operations
- Bidirectional copies (A→B then B→A)

**Key technique - Delayed wrapper**:

The tests use a shell wrapper to delay rcpd startup on one host:

```rust
env.exec_rcp_with_delayed_rcpd(
    "host-a",      // source (delayed)
    "host-b",      // destination (connects first)
    2000,          // delay in ms
    &["host-a:/tmp/src.txt", "host-b:/tmp/dst.txt"]
)
```

This deterministically reproduces timing scenarios that caused the original role-matching bug.

### Developer Setup (WSL2)

**Prerequisites**:

1. **Docker Desktop for Windows** with WSL2 integration:
   - Download from https://www.docker.com/products/docker-desktop/
   - Enable "Use WSL 2 instead of Hyper-V" during installation
   - In Settings → Resources → WSL Integration, enable your WSL distribution

2. **Verify installation**:
   ```bash
   docker --version          # Should show: Docker version 24.x.x
   docker-compose --version  # Should show: Docker Compose version v2.x.x
   docker info               # Should connect without errors
   ```

3. **Install docker-compose if needed**:
   ```bash
   sudo apt update
   sudo apt install docker-compose
   ```

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
cargo build  # Builds to musl target by default
```

**Containers fail to start**:

```bash
cd tests/docker
docker-compose logs     # Check for errors
docker-compose down     # Clean up
docker-compose up -d    # Restart
```

For more troubleshooting, see `tests/docker/README.md`.

## CI Integration

### GitHub Actions Workflow

The `.github/workflows/validate.yml` workflow runs:

1. **Debug tests**: `cargo nextest run`
2. **Release tests**: `cargo nextest run --release` (catches optimization-related bugs)
3. **Docker tests**: Multi-host tests in parallel with other jobs
4. **Sudo tests**: `cargo nextest run --run-ignored only -E 'test(~sudo)'`

**Docker job details**:

- Sets up musl toolchain and builds binaries
- Starts Docker containers with `docker-compose`
- Runs tests using nextest Docker profile
- Shows container logs on failure
- Always cleans up containers (even on failure)

### Running CI Locally

```bash
just ci  # lint + doc + test-all (debug + release + doctests) + Docker tests
```

`just ci` is the primary local "is this ready to push?" gate — it runs the same lint, docs, and
debug + release + doctest + Docker checks CI does, and is the one command to run before pushing.
It's a close proxy for the CI matrix rather than a byte-for-byte match; a few CI steps live outside
it:

- **Sudo-gated tests** (`test(~sudo)`, which need passwordless sudo). CI runs these in a separate
  step, in both debug and release. Run them yourself when a change touches sudo-only behavior:

  ```bash
  cargo nextest run --run-ignored only -E 'test(~sudo)'   # add --release to also cover release
  ```

- **glibc release build.** CI also builds the workspace for `x86_64-unknown-linux-gnu`; `just ci`
  builds and tests only the default `x86_64-unknown-linux-musl` target.

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
cargo nextest run --profile docker --run-ignored only -E 'test(~chaos)'
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
- Pause rcpd (SIGSTOP) to simulate hangs - verifies timeout behavior
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

- **vs. Network namespaces**: More portable, works on macOS/Windows
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

- Static linking ensures binaries work in Alpine containers
- Avoids glibc version incompatibilities
- Project default target anyway

## References

- **Docker setup details**: `tests/docker/README.md`
- **Test implementations**: `rcp/tests/` directory
- **Nextest configuration**: `.config/nextest.toml`
