# rcp development task runner
# See https://github.com/casey/just for more info

# Expose recipe arguments to the shell as "$@" rather than only as `{{...}}` interpolation, which
# is textual and so splits a value containing spaces into several arguments. No recipe here used
# shell positionals before this, so nothing else is affected.
set positional-arguments

# List available commands
default:
    @just --list

# Run the build-tool-backed lint steps without recursively running the full lint suite.
_lint-build:
    @echo "🔍 Checking formatting..."
    ./scripts/cargo-host.sh fmt --check
    @echo "🔍 Checking markdown formatting..."
    ./scripts/check-doc-format.sh
    @echo "🔍 Running clippy..."
    ./scripts/cargo-host.sh clippy --workspace --all-targets -- -D warnings

# Run every target check when Nix is available, while retaining the documented non-Nix lint path.
_lint-targets:
    @set -e; if command -v "${NIX:-nix}" >/dev/null 2>&1; then \
        echo "🔍 Testing build target consistency checker..."; \
        ./scripts/test-check-build-targets.sh; \
        echo "🔍 Checking build target consistency..."; \
        ./scripts/check-build-targets.sh; \
        echo "🔍 Testing Nix target selection..."; \
        ./scripts/test-nix-targets.sh; \
    else \
        echo "⚠️  Nix is unavailable; running all non-Nix target checks."; \
        RCP_BUILD_TARGET_TEST_SKIP_NIX_EVAL=1 ./scripts/test-check-build-targets.sh; \
        ./scripts/check-build-targets.sh --skip-nix-eval; \
        echo "⚠️  Skipped Nix evaluation and realization checks; CI runs them with Nix."; \
    fi

# Run all lints (formatting, clippy, policy checks, and helper behavior tests)
lint: _lint-build _lint-targets
    @echo "🔍 Testing the Depot CI helper..."
    ./scripts/test-depot-ci.sh
    @echo "🔍 Testing the Docker helper..."
    ./scripts/test-docker-helpers.sh
    @echo "🔍 Testing the error logging linter..."
    ./scripts/test-check-error-logging.sh
    @echo "🔍 Checking error logging format..."
    ./scripts/check-error-logging.sh
    @echo "🔍 Checking anyhow::Error::msg usage..."
    @echo "🔍 Testing the anyhow::Error::msg linter..."
    ./scripts/test-check-anyhow-error-msg.sh
    ./scripts/check-anyhow-error-msg.sh
    @echo "🔍 Checking rust version consistency..."
    ./scripts/check-rust-version.sh
    @echo "🔍 Testing build entrypoint inventory..."
    ./scripts/test-check-build-entrypoints.sh
    @echo "🔍 Checking build entrypoint inventory..."
    ./scripts/check-build-entrypoints.sh
    @echo "🔍 Checking package metadata consistency..."
    ./scripts/check-package-metadata.sh
    @echo "🔍 Checking walk-driver usage..."
    ./scripts/check-walk-driver-usage.sh
    @echo "🔍 Checking source-read fidelity..."
    ./scripts/check-source-read-fidelity.sh
    @echo "🔍 Checking TLS handshake timeouts..."
    ./scripts/check-tls-handshake-timeout.sh
    @echo "🔍 Testing the TCP socket configuration linter..."
    ./scripts/test-check-tcp-socket-config.sh
    @echo "🔍 Checking TCP socket configuration..."
    ./scripts/check-tcp-socket-config.sh
    @echo "🔍 Testing the update-deps MSRV report filter..."
    ./scripts/test-update-deps.sh
    @if [ "${CARGO_HOST_TEST_INTEGRATION:-}" != 1 ]; then \
        echo "🔍 Testing host Cargo selection..."; \
        ./scripts/test-cargo-host.sh; \
    fi
    @echo "🔍 Testing Docker target selection..."
    ./scripts/test-docker-target.sh
    @echo "✅ All lints passed!"

# Format code and markdown docs
fmt:
    ./scripts/cargo-host.sh fmt
    dprint fmt

# Run tests (debug mode, using nextest by default)
test:
    ./scripts/cargo-host.sh nextest run

# Run tests in release mode
test-release:
    ./scripts/cargo-host.sh nextest run --release

# Run doctests (debug mode)
doctest:
    ./scripts/cargo-host.sh test --doc

# Run doctests in release mode
doctest-release:
    ./scripts/cargo-host.sh test --doc --release

# Run all tests (both debug and release)
test-all: test doctest test-release doctest-release
    @echo "✅ All tests passed!"

# Quick compilation check (faster than full build)
check:
    ./scripts/cargo-host.sh check --workspace

# Verify the workspace builds on the minimum supported Rust version (MSRV), for
# both shipped targets (gnu + musl). Separate from `just ci`: needs the nix
# devShell's `msrv-check` wrapper. GitHub CI enforces the same via the `msrv` job.
msrv:
    msrv-check

# Upgrade dependencies, staying within the MSRV. Both halves read `rust-version`:
# `cargo upgrade` for the manifests, resolver 3 for the lockfile. Anything held
# back is reported rather than silently skipped. The monthly `cargo-update`
# workflow runs exactly this; pass `-n` to preview without touching the tree.
update-deps *ARGS:
    ./scripts/update-deps.sh "$@"

# Build all packages
build:
    ./scripts/cargo-host.sh build --workspace

# Build release binaries
build-release:
    ./scripts/cargo-host.sh build --workspace --release

# Build and check documentation
doc:
    RUSTDOCFLAGS="--cfg tokio_unstable -D warnings" ./scripts/cargo-host.sh doc --no-deps --workspace

# Run the standard CI checks locally before pushing (lint, docs, tests + Docker).
# The MSRV check is intentionally separate — run `just msrv` (it needs the nix
# devShell's pinned toolchain); GitHub CI runs it as the dedicated `msrv` job.
ci: lint doc test-all-with-docker
    @echo "✅ All CI checks passed! (run 'just msrv' for the separate MSRV check)"

# Depot CI
# ========

# Run selected CI jobs remotely on Depot.
_depot-ci-run *JOBS:
    ./scripts/depot-ci.sh "$@"

# Run debug tests remotely on Depot.
depot-test: (_depot-ci-run "test")

# Run release tests remotely on Depot.
depot-test-release: (_depot-ci-run "test-release")

# Run debug doctests remotely on Depot.
depot-doctest: (_depot-ci-run "doctest")

# Run release doctests remotely on Depot.
depot-doctest-release: (_depot-ci-run "doctest-release")

# Build and check documentation remotely on Depot.
depot-doc: (_depot-ci-run "doc")

# Run debug and release tests and doctests remotely on Depot.
depot-test-all: (_depot-ci-run "test" "doctest" "test-release" "doctest-release")

# Run Docker integration tests remotely on Depot.
depot-docker-test: (_depot-ci-run "docker-test")

# Run Docker chaos tests remotely on Depot.
depot-docker-chaos-test: (_depot-ci-run "docker-chaos-test")

# Run Docker chaos tests remotely on Depot.
depot-chaos: (_depot-ci-run "docker-chaos-test")

# Run all tests and Docker integration tests remotely on Depot.
depot-test-all-with-docker: (_depot-ci-run "test" "doctest" "test-release" "doctest-release" "docker-test")

# Run the standard CI jobs remotely on Depot.
depot-ci: (_depot-ci-run "lint" "doc" "test" "doctest" "test-release" "doctest-release" "docker-test")

# Clean build artifacts
clean:
    ./scripts/cargo-host.sh clean

# Docker multi-host integration tests
# =====================================

# Build binaries for Docker tests (musl target required)
docker-build:
    @echo "🔨 Building binaries for Docker tests (musl target)..."
    ./tests/docker/test-helpers.sh build

# Start Docker containers for multi-host tests
docker-up:
    @echo "🐳 Starting Docker test containers..."
    ./tests/docker/test-helpers.sh setup

# Stop Docker containers
docker-down:
    @echo "🐳 Stopping Docker test containers..."
    cd tests/docker && ./test-helpers.sh stop

# Clean test files from containers (keeps containers running)
docker-clean:
    @echo "🧹 Cleaning test files from containers..."
    cd tests/docker && ./test-helpers.sh cleanup

# View logs from all containers
docker-logs:
    @echo "📋 Container logs:"
    cd tests/docker && ./test-helpers.sh logs

# Show current container logs without following (for unattended diagnostics)
docker-logs-once:
    @echo "📋 Container logs:"
    cd tests/docker && ./test-helpers.sh logs-once

# Run Docker tests (requires containers already running)
docker-test-only *ARGS:
    @echo "🧪 Running Docker multi-host tests..."
    ./scripts/cargo-host.sh nextest run --profile docker --run-ignored only "$@"

# Run Docker tests with full lifecycle (setup -> test -> cleanup)
docker-test:
    ./tests/docker/test-helpers.sh lifecycle just docker-test-only
    @echo "✅ Docker tests completed!"

# Run Docker tests but keep containers running (useful for development)
docker-test-keep:
    ./tests/docker/test-helpers.sh lifecycle-keep just docker-test-only
    @echo "✅ Docker tests completed (containers still running)"
    @echo "💡 Run 'just docker-down' when finished"

# Run all tests including Docker integration tests
test-all-with-docker: test-all docker-test
    @echo "✅ All tests (including Docker) passed!"

# Chaos testing (network simulation, failure injection)
# =====================================================

# Verify container capabilities (mount, tc) are available
docker-verify-caps:
    @echo "🔍 Verifying container capabilities..."
    @bash -c 'for host in rcp-test-host-a rcp-test-host-b; do \
        echo "Checking SYS_ADMIN (mount) on $host..."; \
        docker exec $host mkdir -p /tmp/cap-test && \
        docker exec $host mount -t tmpfs -o size=1k tmpfs /tmp/cap-test && \
        docker exec $host umount /tmp/cap-test && \
        docker exec $host rmdir /tmp/cap-test && \
        echo "  ✅ SYS_ADMIN verified on $host"; \
    done'
    @echo "✅ All container capabilities verified!"

# Run chaos tests only (requires containers already running)
docker-chaos-test-only: docker-verify-caps
    @echo "🌪️  Running chaos tests..."
    ./scripts/cargo-host.sh nextest run --profile docker --run-ignored only -E 'test(~chaos)'

# Run chaos tests with full lifecycle
docker-chaos-test:
    ./tests/docker/test-helpers.sh lifecycle just docker-chaos-test-only
    @echo "✅ Chaos tests completed!"

# Run chaos tests but keep containers running (useful for development)
docker-chaos-test-keep:
    ./tests/docker/test-helpers.sh lifecycle-keep just docker-chaos-test-only
    @echo "✅ Chaos tests completed (containers still running)"
    @echo "💡 Run 'just docker-down' when finished"

# Shorthand for chaos tests (equivalent to GitHub Actions chaos-tests.yml workflow)
chaos: docker-chaos-test

# Release management
# ==================

# Interactive release helper - detects state and guides through release process
release:
    @./scripts/release.sh
