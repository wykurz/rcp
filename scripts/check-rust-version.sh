#!/bin/bash
# Rust Version Consistency Linter
#
# The project deliberately tracks TWO separate Rust versions:
#
#   1. DEV toolchain — the (latest) stable used for day-to-day builds and CI.
#      Source of truth: `channel` in rust-toolchain.toml.
#      Must match the `.default` rust-overlay toolchains in flake.nix / default.nix.
#
#   2. MSRV (minimum supported Rust version) — the floor the project builds on.
#      Source of truth: `rust-version` in [workspace.package] of Cargo.toml.
#      Must match: the `toolchain:` pin of the `msrv` job in
#      .github/workflows/validate.yml, and the `.minimal` rust-overlay toolchains
#      in flake.nix / default.nix.
#
# The dedicated CI `msrv` job actually compiles the workspace on the MSRV
# toolchain; this script ensures every file agrees on what that version is, so an
# accidental bump can't slip through unnoticed.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # no color

FAIL=0
fail() { echo -e "${RED}✗ $1${NC}"; FAIL=1; }

# Extract the first dotted version (2 or 3 components) from a line on stdin.
extract_ver() { sed -n -E 's/.*"([0-9]+\.[0-9]+(\.[0-9]+)?)".*/\1/p'; }
# Same, but anchored to the rust-overlay stable."..." token, so a trailing quoted
# token on the same line (e.g. a comment) cannot be matched by mistake.
extract_nix_ver() { sed -n -E 's/.*stable\."([0-9]+\.[0-9]+(\.[0-9]+)?)".*/\1/p'; }

echo "🔍 checking rust version consistency..."

# --- 1. DEV toolchain ---------------------------------------------------------
DEV=$(grep -E '^channel[[:space:]]*=' rust-toolchain.toml | head -n1 | extract_ver || true)
if [ -z "$DEV" ]; then
    fail "could not read dev toolchain 'channel' from rust-toolchain.toml"
fi

for file in flake.nix default.nix; do
    v=$(grep -E 'rust-bin\.stable\."[0-9.]+"\.default' "$file" | head -n1 | extract_nix_ver || true)
    if [ -z "$v" ]; then
        fail "$file: could not find dev toolchain (rust-bin.stable.\"…\".default)"
    elif [ "$v" != "$DEV" ]; then
        fail "$file: dev toolchain is $v (expected $DEV from rust-toolchain.toml)"
    fi
done

# --- 2. MSRV ------------------------------------------------------------------
MSRV=$(grep -E '^rust-version[[:space:]]*=' Cargo.toml | head -n1 | extract_ver || true)
if [ -z "$MSRV" ]; then
    fail "could not read 'rust-version' (MSRV) from [workspace.package] in Cargo.toml"
fi

for file in flake.nix default.nix; do
    v=$(grep -E 'rust-bin\.stable\."[0-9.]+"\.minimal' "$file" | head -n1 | extract_nix_ver || true)
    if [ -z "$v" ]; then
        fail "$file: could not find MSRV toolchain (rust-bin.stable.\"…\".minimal)"
    elif [ "$v" != "$MSRV" ]; then
        fail "$file: MSRV toolchain is $v (expected $MSRV from Cargo.toml rust-version)"
    fi
done

CI_MSRV=$(grep -E '^[[:space:]]*toolchain:[[:space:]]*"?[0-9]+\.[0-9]+' .github/workflows/validate.yml \
    | head -n1 | sed -E 's/.*toolchain:[[:space:]]*"?([0-9]+\.[0-9]+(\.[0-9]+)?).*/\1/' || true)
if [ -z "$CI_MSRV" ]; then
    fail ".github/workflows/validate.yml: could not find the msrv job 'toolchain:' pin"
elif [ "$CI_MSRV" != "$MSRV" ]; then
    fail ".github/workflows/validate.yml: msrv job pins $CI_MSRV (expected $MSRV)"
fi

# --- result -------------------------------------------------------------------
if [ "$FAIL" -eq 0 ]; then
    echo -e "${GREEN}✓ rust versions consistent — dev: $DEV, msrv: $MSRV${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}rust version inconsistencies found (see above)${NC}"
    echo "dev toolchain source of truth:  rust-toolchain.toml (channel)"
    echo "msrv source of truth:           Cargo.toml ([workspace.package] rust-version)"
    exit 1
fi
