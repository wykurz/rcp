#!/bin/bash
# Rust Version Consistency Linter
# Ensures all hardcoded Rust toolchain versions match across the repository
#
# This script checks that rust-toolchain.toml, flake.nix, and default.nix
# all reference the same Rust version

set -euo pipefail

# colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # no color

echo "🔍 checking rust version consistency..."

# extract version from rust-toolchain.toml (source of truth)
EXPECTED_VERSION=$(grep -E '^channel\s*=\s*"' rust-toolchain.toml | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/')

if [ -z "$EXPECTED_VERSION" ]; then
    echo -e "${RED}✗ failed to extract version from rust-toolchain.toml${NC}"
    exit 1
fi

echo "expected version: $EXPECTED_VERSION"

VIOLATIONS=0

# check flake.nix
FLAKE_VERSION=$(grep -E 'rust-bin\.stable\."[0-9]+\.[0-9]+\.[0-9]+"' flake.nix | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/' || true)
if [ -z "$FLAKE_VERSION" ]; then
    echo -e "${RED}✗ failed to extract version from flake.nix (pattern not found)${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
elif [ "$FLAKE_VERSION" != "$EXPECTED_VERSION" ]; then
    echo -e "${RED}✗ flake.nix has version $FLAKE_VERSION (expected $EXPECTED_VERSION)${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
fi

# check default.nix
DEFAULT_VERSION=$(grep -E 'rust-bin\.stable\."[0-9]+\.[0-9]+\.[0-9]+"' default.nix | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)".*/\1/' || true)
if [ -z "$DEFAULT_VERSION" ]; then
    echo -e "${RED}✗ failed to extract version from default.nix (pattern not found)${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
elif [ "$DEFAULT_VERSION" != "$EXPECTED_VERSION" ]; then
    echo -e "${RED}✗ default.nix has version $DEFAULT_VERSION (expected $EXPECTED_VERSION)${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
fi

if [ $VIOLATIONS -eq 0 ]; then
    echo -e "${GREEN}✓ all rust versions are consistent: $EXPECTED_VERSION${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}found $VIOLATIONS version mismatch(es)${NC}"
    echo ""
    echo "to fix: update all files to use version $EXPECTED_VERSION from rust-toolchain.toml"
    exit 1
fi
