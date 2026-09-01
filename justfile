# rcp development task runner
# See https://github.com/casey/just for more info

# Expose recipe arguments to the shell as "$@" rather than only as `{{...}}` interpolation, which
# is textual and so splits a value containing spaces into several arguments. No recipe here used
# shell positionals before this, so nothing else is affected.
set positional-arguments

# List available commands
default:
    @just --list

# Run all lints (fmt, markdown formatting, clippy, error logging, anyhow error msg, rust version, package metadata, walk-driver usage, source-read fidelity, TLS handshake timeouts, TCP socket configuration)
lint:
    @echo "🔍 Checking formatting..."
    cargo fmt --check
    @echo "🔍 Checking markdown formatting..."
    ./scripts/check-doc-format.sh
    @echo "🔍 Running clippy..."
    cargo clippy --workspace --all-targets -- -D warnings
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
    @echo "✅ All lints passed!"

# Format code and markdown docs
fmt:
    cargo fmt
    dprint fmt

# Run tests (debug mode, using nextest by default)
test:
    cargo nextest run

# Run tests in release mode
test-release:
    cargo nextest run --release

# Run doctests (debug mode)
doctest:
    cargo test --doc

# Run doctests in release mode
doctest-release:
    cargo test --doc --release

# Run all tests (both debug and release)
test-all: test doctest test-release doctest-release
    @echo "✅ All tests passed!"

# Quick compilation check (faster than full build)
check:
    cargo check --workspace

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
    cargo build --workspace

# Build release binaries
build-release:
    cargo build --workspace --release

# Build and check documentation
doc:
    RUSTDOCFLAGS="--cfg tokio_unstable -D warnings" cargo doc --no-deps --workspace

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
    cargo clean

# Docker multi-host integration tests
# =====================================

# Build binaries for Docker tests (musl target required)
docker-build:
    @echo "🔨 Building binaries for Docker tests (musl target)..."
    cargo build --workspace

# Start Docker containers for multi-host tests
docker-up: docker-build
    @echo "🐳 Starting Docker test containers..."
    cd tests/docker && ./test-helpers.sh start

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

# Run Docker tests (requires containers already running)
docker-test-only:
    @echo "🧪 Running Docker multi-host tests..."
    cargo nextest run --profile docker --run-ignored only

# Run Docker tests with full lifecycle (setup -> test -> cleanup)
docker-test: docker-up docker-test-only docker-down
    @echo "✅ Docker tests completed!"

# Run Docker tests but keep containers running (useful for development)
docker-test-keep: docker-up docker-test-only
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
    cargo nextest run --profile docker --run-ignored only -E 'test(~chaos)'

# Run chaos tests with full lifecycle
docker-chaos-test: docker-up docker-chaos-test-only docker-down
    @echo "✅ Chaos tests completed!"

# Run chaos tests but keep containers running (useful for development)
docker-chaos-test-keep: docker-up docker-chaos-test-only
    @echo "✅ Chaos tests completed (containers still running)"
    @echo "💡 Run 'just docker-down' when finished"

# Shorthand for chaos tests (equivalent to GitHub Actions chaos-tests.yml workflow)
chaos: docker-chaos-test

# Release management
# ==================

# Interactive release helper - detects state and guides through release process
release:
    @./scripts/release.sh
