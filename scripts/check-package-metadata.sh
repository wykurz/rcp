#!/bin/bash
# Package Metadata Consistency Linter
# Ensures all workspace packages have consistent metadata settings
#
# This script uses only standard Unix tools (grep, sed, awk) available in GitHub CI

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🔍 Checking package metadata consistency..."

VIOLATIONS_FOUND=0

# Dynamically discover workspace members from root Cargo.toml
PACKAGES=$(sed -n '/^members = \[/,/^\]/p' Cargo.toml | grep -oE '"[^"]+"' | tr -d '"')

if [ -z "$PACKAGES" ]; then
    echo -e "${RED}ERROR: Could not parse workspace members from Cargo.toml${NC}"
    exit 1
fi

# Expected docs.rs metadata (normalized for comparison)
EXPECTED_CARGO_ARGS='cargo-args = ["--config", "build.rustflags=[\"--cfg\", \"tokio_unstable\"]"]'
EXPECTED_RUSTDOC_ARGS='rustdoc-args = ["--cfg", "tokio_unstable"]'

# Check each package
for pkg in $PACKAGES; do
    cargo_toml="$pkg/Cargo.toml"

    if [ ! -f "$cargo_toml" ]; then
        echo -e "${RED}ERROR: $cargo_toml not found${NC}"
        VIOLATIONS_FOUND=1
        continue
    fi

    # Check for [lints] section with workspace = true (may have blank lines or comments between)
    if ! awk '
        /^\[lints\]/ { in_lints = 1; next }
        in_lints && /^\[/ { in_lints = 0 }
        in_lints && /workspace[[:space:]]*=[[:space:]]*true/ { found = 1; exit }
        END { exit !found }
    ' "$cargo_toml"; then
        echo -e "${RED}ERROR: $cargo_toml missing '[lints] workspace = true'${NC}"
        VIOLATIONS_FOUND=1
    fi

    # Check for workspace-inherited fields
    for field in "version.workspace" "edition.workspace" "license.workspace" "repository.workspace"; do
        if ! grep -q "$field = true" "$cargo_toml"; then
            echo -e "${RED}ERROR: $cargo_toml missing '$field = true'${NC}"
            VIOLATIONS_FOUND=1
        fi
    done

    # Check for [package.metadata.docs.rs] section
    if ! grep -q '^\[package\.metadata\.docs\.rs\]' "$cargo_toml"; then
        echo -e "${RED}ERROR: $cargo_toml missing '[package.metadata.docs.rs]' section${NC}"
        VIOLATIONS_FOUND=1
        continue
    fi

    # Check cargo-args
    if ! grep -q 'cargo-args = \["--config", "build.rustflags=\[\\"--cfg\\", \\"tokio_unstable\\"\]"\]' "$cargo_toml"; then
        echo -e "${RED}ERROR: $cargo_toml has incorrect or missing 'cargo-args' in [package.metadata.docs.rs]${NC}"
        echo -e "${YELLOW}  Expected: $EXPECTED_CARGO_ARGS${NC}"
        VIOLATIONS_FOUND=1
    fi

    # Check rustdoc-args
    if ! grep -q 'rustdoc-args = \["--cfg", "tokio_unstable"\]' "$cargo_toml"; then
        echo -e "${RED}ERROR: $cargo_toml has incorrect or missing 'rustdoc-args' in [package.metadata.docs.rs]${NC}"
        echo -e "${YELLOW}  Expected: $EXPECTED_RUSTDOC_ARGS${NC}"
        VIOLATIONS_FOUND=1
    fi
done

if [ $VIOLATIONS_FOUND -eq 1 ]; then
    echo ""
    echo -e "${RED}=================================================${NC}"
    echo -e "${RED}ERROR: Package metadata inconsistencies found!${NC}"
    echo ""
    echo "All packages should have:"
    echo "  1. [lints] workspace = true"
    echo "  2. version.workspace = true, edition.workspace = true, etc."
    echo "  3. [package.metadata.docs.rs] with:"
    echo "     $EXPECTED_CARGO_ARGS"
    echo "     $EXPECTED_RUSTDOC_ARGS"
    echo -e "${RED}=================================================${NC}"
    exit 1
fi

echo -e "${GREEN}Package metadata consistency check passed!${NC}"
exit 0
