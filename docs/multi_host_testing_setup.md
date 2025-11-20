# Multi-Host Testing Setup

This document describes the Docker-based multi-host testing infrastructure for RCP.

## Problem and Solution

**Problem**: Testing multi-host RCP operations (e.g., `host-a:/src → host-b:/dst`) requires multiple separate machines. The existing test suite only tests `localhost → localhost`, which doesn't exercise true multi-host scenarios or detect connection ordering bugs.

**Solution**: Docker container infrastructure that simulates multiple hosts with SSH connectivity, allowing deterministic testing of multi-host operations and connection timing scenarios.

## Architecture

### Container Setup

Three Alpine Linux containers simulate separate hosts:
- **master**: Runs `rcp` commands (acts as the coordinator)
- **host-a**: SSH server with rcpd binary (source or destination)
- **host-b**: SSH server with rcpd binary (source or destination)

All containers:
- Run OpenSSH server with pre-configured keys
- Have RCP binaries mounted from `target/x86_64-unknown-linux-musl/debug/`
- Can SSH to each other by hostname
- Share a Docker bridge network for connectivity

### Integration Points

**Build System**:
- `just docker-build`: Builds musl binaries required for containers
- `just docker-up`: Starts containers (builds binaries first)
- `just docker-test`: Full lifecycle (start → test → stop)
- `just docker-down`: Stops and removes containers

**Test Suite**:
- Integration tests in `rcp/tests/` cover basic multi-host operations and connection timing scenarios
- Test helpers in `rcp/tests/support/` provide Docker environment management
- Tests are marked `#[ignore]` and run with `--run-ignored only` via nextest Docker profile

**CI Integration**:
- `.github/workflows/validate.yml`: Includes `test-docker` job
- Runs in parallel with debug/release test jobs
- Automatically builds binaries and manages container lifecycle

**Nextest Profile**:
```toml
[profile.docker]
default-filter = 'binary(~docker_multi_host)'
```

## Developer Setup (WSL2)

### Prerequisites

**Docker Desktop for Windows** with WSL2 integration:

1. Download from https://www.docker.com/products/docker-desktop/
2. During installation, ensure **"Use WSL 2 instead of Hyper-V"** is checked
3. After installation, open Docker Desktop settings:
   - Navigate to **Settings → Resources → WSL Integration**
   - Enable integration with your WSL distribution
   - Apply and restart

**Verify installation**:
```bash
docker --version          # Should show: Docker version 24.x.x
docker-compose --version  # Should show: Docker Compose version v2.x.x
docker info               # Should connect without errors
```

If `docker-compose` is missing:
```bash
sudo apt update
sudo apt install docker-compose
```

### Running Tests Locally

**Using just commands (recommended)**:
```bash
# Full test lifecycle
just docker-test

# Development workflow
just docker-up           # Start containers (keeps running)
just docker-test-only    # Run tests without setup
just docker-test-only    # Run again after code changes
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

## Key Design Decisions

**Why Docker over alternatives?**:
- **vs. Network namespaces**: More portable, works on macOS/Windows
- **vs. VMs**: Faster startup, easier to manage, better CI integration
- **vs. Mock transport**: More realistic, tests actual SSH/QUIC stack

**Why Alpine Linux?**:
- Small image size (~50MB vs ~150MB for Debian/Ubuntu)
- Fast container startup
- OpenSSH available in package manager

**Why mount binaries instead of COPY?**:
- No container rebuild needed for code changes
- Faster iteration during development
- Ensures tests use exact same binary as local builds

**Why test-only SSH keys checked into git?**:
- Simplifies setup (no key generation step)
- Reproducible across environments
- Keys are clearly marked as test-only in multiple places
- Never used for production systems

**Why musl target?**:
- Static linking ensures binaries work in Alpine containers
- Avoids glibc version incompatibilities
- Project default target anyway

## Troubleshooting

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
cargo build --workspace  # Builds to musl target by default
```

**Containers fail to start**:
```bash
cd tests/docker
docker-compose logs     # Check for errors
docker-compose down     # Clean up
docker-compose up -d    # Restart
```

**Tests can't connect to containers**:
→ Tests require containers to be running: `just docker-up` first

For complete troubleshooting guide, see `tests/docker/README.md`.

## References

- **Detailed setup guide**: `tests/docker/README.md`
- **Testing strategy**: `docs/testing_strategy.md`
- **Test implementation**: `rcp/tests/` (Docker multi-host tests)
