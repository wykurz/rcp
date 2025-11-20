# Testing Strategy

This document outlines testing approaches for rcp, including future improvements.

## Current Test Coverage

### Unit Tests
- Core functionality in `common/`, `throttle/`, `remote/` crates
- CLI parsing tests for all binaries
- Path parsing and validation

### Integration Tests
- Local file operations (copy, link, remove, compare)
- Remote operations using localhost SSH
- Permission handling and error cases
- Auto-deployment of rcpd

### Docker-Based Multi-Host Tests
- Multi-host file operations across separate containers
- Directory copying between hosts
- Overwrite protection and error handling
- **Role ordering tests**: Deterministic testing of connection order scenarios
- Delayed rcpd connection tests to verify role-matching bug fix

See `tests/docker/README.md` for setup and usage instructions.

### Remaining Limitations
- No testing of cross-network scenarios (latency, packet loss, etc.)
- Network simulation with `tc` not yet implemented

## Docker-Based Multi-Host Integration Tests (Implemented)

### Motivation
The role-matching fix addressed a bug where source/destination roles could be swapped if rcpd connections arrived out of order. The Docker-based tests provide deterministic test coverage for:
- Connection order races between different physical hosts
- Multi-host scenarios where source and destination are truly separate
- Role assignment verification regardless of connection timing

### Implementation

#### Infrastructure Setup
**Location**: `tests/docker/`

**Docker Compose configuration** with 3 containers:
- `master`: Runs rcp commands (Alpine Linux with SSH client)
- `host-a`: SSH server with rcpd available
- `host-b`: SSH server with rcpd available

**Container configuration**:
- Based on Alpine Linux 3.19
- OpenSSH server and client configured
- Pre-configured SSH keys for passwordless authentication (test-only keys checked into repo)
- rcp/rcpd binaries mounted from `target/x86_64-unknown-linux-musl/debug/`
- All containers run as `testuser` for consistent permissions

**Network setup**:
- All containers on same Docker network
- Containers can SSH to each other by hostname

#### Test Implementation

**Test infrastructure** (`rcp/tests/support/docker_env.rs`) provides:
- `DockerEnv::new()` - Test environment setup
- `exec_rcp()` - Execute rcp commands from master container
- File operations: `write_file()`, `read_file()`, `file_exists()`, `remove_file()`
- Connection timing manipulation: `exec_rcp_with_delayed_rcpd()` to force specific connection order

**Test coverage**:

The test suite includes two main categories:

**Basic multi-host operations**: File and directory copying between separate hosts, overwrite protection, error handling, and verbose output validation.

**Connection ordering scenarios**: Tests that deterministically verify the role-matching bug fix by controlling which rcpd instance connects first. These use a delayed wrapper mechanism to force specific connection timing and validate that source/destination roles are correctly assigned regardless of connection order.

**Key technique - Delayed wrapper**:
The tests use a shell wrapper script that delays rcpd startup on the source host while letting the destination connect immediately. This forces the exact scenario that caused the original bug:
```rust
env.exec_rcp_with_delayed_rcpd(
    "host-a",      // source (delayed)
    "host-b",      // destination (connects first)
    2000,          // delay in ms
    &["host-a:/tmp/src.txt", "host-b:/tmp/dst.txt"]
)
```

#### Running Docker Tests

For detailed instructions on running Docker tests locally, see `docs/multi_host_testing_setup.md`.

**Quick reference**:
```bash
just docker-test              # Full lifecycle (start → test → stop)
just docker-up                # Start containers
cargo nextest run --profile docker --run-ignored only
just docker-down              # Stop containers
```

#### CI Integration

Docker tests run automatically in GitHub Actions on all PRs and pushes to main.

**Workflow**: `.github/workflows/validate.yml` includes a `test-docker` job that:
- Runs in parallel with other test jobs (debug, release)
- Sets up musl toolchain and builds binaries
- Starts Docker containers with `docker-compose`
- Runs tests using nextest Docker profile
- Shows container logs on failure
- Always cleans up containers (even on failure)

## Future Improvements

### Network Simulation with Traffic Control

Add realistic network conditions using `tc` (traffic control) in Docker containers:

**Potential test scenarios**:
- High latency (50-500ms)
- Packet loss (1-10%)
- Variable jitter
- Bandwidth constraints
- Connection drops during transfer

**Implementation approach**:
1. Update Dockerfile to include `iproute2` package
2. Add test helpers to configure `tc qdisc` rules
3. Write tests that verify RCP behavior under adverse conditions
4. Test connection recovery and timeout handling

**Value**: Would catch network-related bugs that clean Docker networks won't reveal.

### Additional Test Scenarios

Expand coverage for edge cases not yet tested:

**Potential additions**:
- Three-way copies (A→B→C chain operations)
- Simultaneous bidirectional transfers (A↔B)
- Large file transfers (multi-GB stress testing)
- Rcpd auto-deployment testing in Docker environment
- Permission error propagation across hosts
- Graceful handling of container restarts mid-transfer

**Implementation**: Can be added incrementally as needed.

### Alternative Approaches Considered

**Lightweight network simulation** (not chosen):
- Network namespaces (Linux only, less portable)
- Virtual interfaces with veth pairs (more complex setup)
- Mock QUIC transport (less realistic)

Current Docker approach provides good balance of realism and simplicity.
