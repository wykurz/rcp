# RCP Multi-Host Docker Test Environment

This directory contains a Docker-based setup for testing RCP operations across multiple hosts.

## Overview

The setup creates 3 Alpine Linux containers with SSH servers:
- **master**: Where you run `rcp` commands from
- **host-a**: First remote host (source or destination)
- **host-b**: Second remote host (source or destination)

All containers have the RCP binaries (`rcp`, `rcpd`, `rrm`, `rlink`, `rcmp`) available and can SSH to each other using pre-configured keys.

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

**Note**: `just docker-test` automatically builds the required binaries (musl target) before starting containers. No manual build step needed!

### Option 2: Using `cargo nextest` Directly

```bash
# From repo root
# 1. Start containers (builds binaries automatically)
just docker-up

# 2. Run tests using nextest docker profile
cargo nextest run --profile docker --run-ignored only

# 3. Stop containers when done
just docker-down
```

### Option 3: Using Standard `cargo test`

```bash
# From repo root
# 1. Start containers (builds binaries automatically)
just docker-up

# 2. Run with --ignored flag to include Docker tests
cargo test --test docker_multi_host -- --ignored
cargo test --test docker_multi_host_role_ordering -- --ignored

# 3. Stop containers
just docker-down
```

## Development Workflow

For active development where you run tests multiple times:

```bash
# Start containers once (builds binaries on first run)
just docker-up

# Run tests repeatedly (no rebuild needed if only changing tests)
cargo nextest run --profile docker --run-ignored only
# ... make code changes to RCP source ...
cargo build  # Rebuild if you changed rcp/rcpd source
cargo nextest run --profile docker --run-ignored only
# ... make more changes ...
cargo nextest run --profile docker --run-ignored only

# Clean test files if needed (keeps containers running)
just docker-clean

# When done
just docker-down
```

**Tip**: If you only change test code (in `rcp/tests/`), you don't need to rebuild binaries. If you change RCP source code, run `cargo build` before running tests again.

## Available `just` Commands

```bash
just docker-build        # Build binaries for Docker (musl target)
just docker-up           # Build binaries + start containers
just docker-down         # Stop containers
just docker-test         # Full cycle: build → start → test → stop
just docker-test-keep    # Build → start → test (keep containers running)
just docker-test-only    # Run tests (containers must be running)
just docker-clean        # Clean test files (keeps containers running)
just docker-logs         # View container logs
```

## Low-Level Helper Script

For more control, use `test-helpers.sh` directly:

```bash
cd tests/docker

# Container lifecycle
./test-helpers.sh start      # Start containers
./test-helpers.sh stop       # Stop containers
./test-helpers.sh restart    # Restart containers
./test-helpers.sh status     # Show container status

# Testing and debugging
./test-helpers.sh test-copy  # Quick copy test
./test-helpers.sh test-ssh   # Test SSH connectivity
./test-helpers.sh cleanup    # Remove test files
./test-helpers.sh logs       # View logs (follow mode)
./test-helpers.sh shell      # Open shell in master

# Maintenance
./test-helpers.sh rebuild    # Rebuild from scratch
./test-helpers.sh help       # Show all commands
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
   docker-compose --version
   ```

3. **Install docker-compose if needed**:
   ```bash
   # If docker-compose is not available:
   sudo apt update
   sudo apt install docker-compose
   ```

**Note**: Binaries are automatically built when you run `just docker-test` or `just docker-up`. This project uses the musl target by default, and binaries are mounted from `target/x86_64-unknown-linux-musl/debug/` into the containers.

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
docker-compose logs

# Specific container
docker-compose logs master
docker-compose logs host-a
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
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

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
cd /home/mateusz/projects/rcp
cargo build
```

### Containers fail to start

**Solution**: Check logs and rebuild:

```bash
docker-compose logs
docker-compose down
docker-compose up --build
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
