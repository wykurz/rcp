# rcp development task runner
# See https://github.com/casey/just for more info

# List available commands
default:
    @just --list

# Run all lints (fmt, clippy, error logging, anyhow error msg, rust version, remote test naming)
lint:
    @echo "🔍 Checking formatting..."
    cargo fmt --check
    @echo "🔍 Running clippy..."
    cargo clippy --workspace --all-targets -- -D warnings
    @echo "🔍 Checking error logging format..."
    ./scripts/check-error-logging.sh
    @echo "🔍 Checking anyhow::Error::msg usage..."
    ./scripts/check-anyhow-error-msg.sh
    @echo "🔍 Checking rust version consistency..."
    ./scripts/check-rust-version.sh
    @echo "🔍 Checking remote test naming..."
    ./scripts/check-remote-test-naming.sh
    @echo "✅ All lints passed!"

# Format code
fmt:
    cargo fmt

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

# Build all packages
build:
    cargo build --workspace

# Build release binaries
build-release:
    cargo build --workspace --release

# Build and check documentation
doc:
    RUSTDOCFLAGS="--cfg tokio_unstable -D warnings" cargo doc --no-deps --workspace

# Run all CI checks locally before pushing (matches GitHub Actions)
ci: lint doc test-all-with-docker
    @echo "✅ All CI checks passed! Safe to push."

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
