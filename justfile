# rcp development task runner
# See https://github.com/casey/just for more info

# List available commands
default:
    @just --list

# Run all lints (fmt, clippy, error logging, anyhow error msg)
lint:
    @echo "🔍 Checking formatting..."
    cargo fmt --check
    @echo "🔍 Running clippy..."
    cargo clippy --workspace -- -D warnings
    @echo "🔍 Checking error logging format..."
    ./scripts/check-error-logging.sh
    @echo "🔍 Checking anyhow::Error::msg usage..."
    ./scripts/check-anyhow-error-msg.sh
    @echo "✅ All lints passed!"

# Format code
fmt:
    cargo fmt

# Run tests (using nextest by default)
test:
    cargo nextest run

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
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace

# Run all CI checks locally before pushing
ci: lint test doc
    @echo "✅ All CI checks passed! Safe to push."

# Clean build artifacts
clean:
    cargo clean
