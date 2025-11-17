# rcp development task runner
# See https://github.com/casey/just for more info

# List available commands
default:
    @just --list

# Run all lints (fmt, clippy, error logging, anyhow error msg, rust version)
lint:
    @echo "🔍 Checking formatting..."
    cargo fmt --check
    @echo "🔍 Running clippy..."
    cargo clippy --workspace -- -D warnings
    @echo "🔍 Checking error logging format..."
    ./scripts/check-error-logging.sh
    @echo "🔍 Checking anyhow::Error::msg usage..."
    ./scripts/check-anyhow-error-msg.sh
    @echo "🔍 Checking rust version consistency..."
    ./scripts/check-rust-version.sh
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
ci: lint doc test-all
    @echo "✅ All CI checks passed! Safe to push."

# Clean build artifacts
clean:
    cargo clean
