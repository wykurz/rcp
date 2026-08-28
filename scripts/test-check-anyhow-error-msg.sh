#!/bin/bash
# tests for scripts/check-anyhow-error-msg.sh.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LINTER="$SCRIPT_DIR/check-anyhow-error-msg.sh"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT

echo "🔍 Testing check-anyhow-error-msg.sh..."

expect_failure() {
    local expected="$1"
    shift
    set +e
    local output
    output=$("$@" 2>&1)
    local status=$?
    set -e
    if [ "$status" -eq 0 ] || ! grep -Fq "$expected" <<< "$output"; then
        echo -e "${RED}FAIL: expected failure containing: $expected${NC}"
        echo "  got status $status, output:"
        sed 's/^/    /' <<< "$output"
        exit 1
    fi
}

SPACED_FIXTURE="$TEMP_DIR/fixture with spaces"
mkdir -p "$SPACED_FIXTURE"
printf '%s\n' 'fn violation() { let _ = anyhow::Error::msg("chain lost"); }' \
    > "$SPACED_FIXTURE/violation.rs"
expect_failure "Found violation in $SPACED_FIXTURE/violation.rs" "$LINTER" "$SPACED_FIXTURE"

expect_failure "nothing was checked" "$LINTER" "$TEMP_DIR/does-not-exist"

EMPTY_FIXTURE="$TEMP_DIR/empty"
mkdir -p "$EMPTY_FIXTURE"
expect_failure "nothing was checked" "$LINTER" "$EMPTY_FIXTURE"

mkdir -p "$TEMP_DIR/bin"
printf '%s\n' '#!/bin/sh' 'exit 37' > "$TEMP_DIR/bin/find"
chmod +x "$TEMP_DIR/bin/find"
expect_failure "failed to enumerate Rust files" env "PATH=$TEMP_DIR/bin:$PATH" bash -c \
    'cd "$1" && "$2"' -- "$TEMP_DIR" "$LINTER"

echo -e "${GREEN}✅ check-anyhow-error-msg.sh tests passed!${NC}"
