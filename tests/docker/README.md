# RCP Multi-Host Docker Test Environment

This directory contains a Docker-based setup for testing RCP operations across multiple hosts.

## Overview

The setup creates 3 Alpine Linux containers with SSH servers:

- **master**: Where you run `rcp` commands from
- **host-a**: First remote host (source or destination)
- **host-b**: Second remote host (source or destination)

All containers have the RCP binaries (`rcp`, `rcpd`, `rrm`, `rlink`, `rcmp`) available and can SSH
to each other using pre-configured keys.

## Quick Start: Running Tests

### Option 1: Using `just` (Recommended)

The easiest way to run Docker tests with automatic setup and cleanup:

```bash
# From repo root - Full lifecycle (builds binaries, starts containers, runs tests, stops containers)
just docker-test

# Or keep containers running for development
just docker-test-keep    # Builds binaries, starts containers, runs tests (keeps running)
just docker-test-only    # Run tests again without setup (containers must be running)
just docker-down         # Stop containers when done
```

**Note**: `just docker-test` automatically builds the required binaries for Docker's selected musl
target before starting containers. No manual build step is needed.

### Option 2: Using Nextest Through the Host Wrapper

```bash
# From repo root
# 1. Start containers (builds binaries automatically)
just docker-up

# 2. Run tests using nextest docker profile
./scripts/cargo-host.sh nextest run --profile docker --run-ignored only

# 3. Stop containers when done
just docker-down
```

The wrapper selects a runnable host target for the test process. The binaries mounted into the
containers are separate payloads: `just docker-up` follows `DOCKER_DEFAULT_PLATFORM` first and the
Docker daemon architecture otherwise.

`just docker-up` returns only after `testuser` can reach both `host-a` and `host-b` from the master
container within one bounded readiness deadline. A readiness failure captures the current logs
without following them; full lifecycle commands then preserve the failure status while stopping the
Compose project once.

### Option 3: Using Libtest Through the Host Wrapper

```bash
# From repo root
# 1. Start containers (builds binaries automatically)
just docker-up

# 2. Run with --ignored flag to include Docker tests
./scripts/cargo-host.sh test --test docker_multi_host -- --ignored
./scripts/cargo-host.sh test --test docker_multi_host_role_ordering -- --ignored

# 3. Stop containers
just docker-down
```

## Development Workflow

For active development where you run tests multiple times:

```bash
# Start containers once (builds binaries on first run)
just docker-up

# Run tests repeatedly (no rebuild needed if only changing tests)
just docker-test-only
# ... make code changes to RCP source ...
just docker-build  # Rebuild if you changed rcp/rcpd source
just docker-test-only
# ... make more changes ...
just docker-test-only

# Clean test files if needed (keeps containers running)
just docker-clean

# When done
just docker-down
```

**Tip**: If you only change test code (in `rcp/tests/`), you don't need to rebuild binaries. If you
change RCP source code, run `just docker-build` before running tests again.

## Available `just` Commands

```bash
just docker-build        # Build binaries for Docker's selected musl target
just docker-up           # Build binaries + start containers
just docker-down         # Stop containers
just docker-test         # Full cycle: build → start → test → stop
just docker-test-keep    # Build → start → test (keep containers running)
just docker-test-only    # Run tests (containers must be running)
just docker-clean        # Clean test files (keeps containers running)
just docker-logs         # View container logs
just docker-logs-once    # Capture current logs without following
```

## Low-Level Helper Script

For more control, run `test-helpers.sh` from the repository root:

```bash
# Container lifecycle
./tests/docker/test-helpers.sh setup      # Build payload and start containers
./tests/docker/test-helpers.sh stop       # Stop containers
./tests/docker/test-helpers.sh restart    # Restart containers
./tests/docker/test-helpers.sh status     # Show container status

# Testing and debugging
./tests/docker/test-helpers.sh test-copy  # Quick copy test
./tests/docker/test-helpers.sh test-ssh   # Test SSH connectivity
./tests/docker/test-helpers.sh cleanup    # Remove test files
./tests/docker/test-helpers.sh logs       # View logs (follow mode)
./tests/docker/test-helpers.sh logs-once  # Capture current logs without following
./tests/docker/test-helpers.sh shell      # Open shell in master

# Maintenance
./tests/docker/test-helpers.sh rebuild    # Rebuild images/payloads and wait for both SSH hosts
./tests/docker/test-helpers.sh help       # Show all commands
```

## Prerequisites

### WSL2 (Your Environment)

1. **Install Docker Desktop for Windows**:
   - Download from: https://www.docker.com/products/docker-desktop/
   - During installation, ensure "Use WSL 2 instead of Hyper-V" is checked
   - After installation, open Docker Desktop settings:
     - Go to Resources → WSL Integration
     - Enable integration with your WSL distro

2. **Verify Docker in WSL**:
   ```bash
   docker --version
   docker compose version
   docker info
   ```

3. **Install Compose if needed**: install the Docker Compose v2 plugin for `docker compose`. The
   helper prefers v2 and falls back to the legacy standalone `docker-compose` command when it is
   already installed.

**Note**: Binaries are automatically built when you run `just docker-test` or `just docker-up`.
`DOCKER_DEFAULT_PLATFORM` wins when it is set; otherwise the Docker daemon architecture selects the
Linux musl target. The same target path is passed to Compose for the bind mounts.

## Manual Testing Scenarios

### Basic Remote Copy

```bash
# Inside master container
docker exec -it rcp-test-master /bin/bash

# Copy file from host-a to host-b
ssh host-a "echo 'test data' > /tmp/src.txt"
rcp -vv host-a:/tmp/src.txt host-b:/tmp/dst.txt
ssh host-b "cat /tmp/dst.txt"
```

### Directory Copy

```bash
# Inside master container
ssh host-a "mkdir -p /tmp/src && echo 'file1' > /tmp/src/file1.txt && echo 'file2' > /tmp/src/file2.txt"
rcp -vv host-a:/tmp/src/ host-b:/tmp/dst/
ssh host-b "ls -la /tmp/dst"
```

### Test rcpd Auto-Deployment

RCP should automatically deploy `rcpd` to remote hosts via SSH:

```bash
# Inside master container

# Remove rcpd from host-a to test auto-deployment
ssh host-a "rm -f /usr/local/bin/rcpd"

# Run copy - rcp should auto-deploy rcpd
rcp -vv host-a:/tmp/test.txt host-b:/tmp/test2.txt

# Verify rcpd was deployed
ssh host-a "ls -la ~/.cache/rcp/bin/"
```

### Test with Verbose Logging

```bash
# Inside master container
rcp -vv host-a:/tmp/src.txt host-b:/tmp/dst.txt
```

Look for log lines showing:

- SSH connections to each host
- rcpd deployment (if needed)
- TCP connection establishment
- File transfer progress
- Role assignment (source vs destination)

## Debugging

### View Container Logs

```bash
# All containers
just docker-logs

# Unattended diagnostics (capture once and return)
just docker-logs-once

# Specific container
./tests/docker/test-helpers.sh logs master
./tests/docker/test-helpers.sh logs host-a
```

### SSH Directly from Host (WSL)

Each container exposes SSH on a unique port:

```bash
# From WSL (not inside container)
ssh -p 2220 -i tests/docker/ssh_keys/id_ed25519 testuser@localhost  # master
ssh -p 2221 -i tests/docker/ssh_keys/id_ed25519 testuser@localhost  # host-a
ssh -p 2222 -i tests/docker/ssh_keys/id_ed25519 testuser@localhost  # host-b
```

### Exec into Container

```bash
docker exec -it rcp-test-master /bin/bash
docker exec -it rcp-test-host-a /bin/bash
docker exec -it rcp-test-host-b /bin/bash
```

### Rebuild Containers

If you modify the Dockerfile:

```bash
./tests/docker/test-helpers.sh rebuild
```

Rebuild resolves the Docker architecture once, rebuilds the images and matching musl payloads, then
waits for SSH readiness on both remote hosts before returning.

### Check Network Connectivity

```bash
# Inside master container
ping host-a
ping host-b

# Check if SSH port is open
nc -zv host-a 22
nc -zv host-b 22
```

## Common Issues

### "Cannot connect to the Docker daemon"

**Solution**: Ensure Docker Desktop is running and WSL integration is enabled.

```bash
# Check Docker daemon status
docker info
```

### "Permission denied" when accessing SSH key

**Solution**: Ensure correct permissions on SSH keys:

```bash
chmod 600 tests/docker/ssh_keys/id_ed25519
chmod 644 tests/docker/ssh_keys/id_ed25519.pub
chmod 644 tests/docker/ssh_keys/config
```

### "No such file or directory" for binaries

**Solution**: Build the binaries first:

```bash
just docker-build
```

### Containers fail to start

**Solution**: Check logs and rebuild:

```bash
just docker-logs-once
just docker-down
just docker-up
```

### "Connection refused" when SSH-ing between containers

**Solution**: Ensure SSH server is running in containers:

```bash
docker exec -it rcp-test-host-a ps aux | grep sshd
```

## File Structure

```
tests/docker/
├── Dockerfile.ssh-host       # Alpine + SSH server image
├── docker-compose.yml         # 3-container setup
├── ssh_keys/
│   ├── id_ed25519            # Private key (TEST ONLY - not for production!)
│   ├── id_ed25519.pub        # Public key
│   └── config                # SSH client config
└── README.md                  # This file
```

## Security Note

**WARNING**: The SSH keys in this directory are FOR TESTING ONLY. They are:

- Checked into version control
- Publicly visible
- Should NEVER be used for production systems
- Should NEVER be used on real servers

## Test Coverage

The Docker environment supports automated integration tests in:

### `tests/docker_multi_host.rs`

- Basic multi-host file copy
- Overwrite protection behavior
- Directory copying (with cleanup before/after to prevent hangs)
- Error handling for missing files

**Note**: The verbose logging test is disabled because docker exec doesn't capture stderr.

### `tests/docker_multi_host_role_ordering.rs`

- **Baseline tests**: Verify basic multi-host functionality
- **Rapid operations**: Stress-test role assignment with quick successive copies
- **Bidirectional copies**: Test A→B then B→A scenarios
- **Destination connects first**: THE critical test for the role-matching bug fix
  - Uses delayed wrapper to force destination rcpd to connect before source
  - Verifies correct role assignment regardless of connection order
- **Consistent role assignment**: Multiple iterations to catch timing-dependent issues

All tests are safe to run in parallel and include proper cleanup.
