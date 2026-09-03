#!/bin/bash
# tests action-pin validation across GitHub and Depot workflow trees.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECKER="$SCRIPT_DIR/check-action-pins.py"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT
ROOT="$TEMP_DIR/repository"

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    exit 1
}

write_fixtures() {
    local github_pin="$1"
    local depot_workflow_pin="$2"
    local depot_action_pin="$3"
    mkdir -p "$ROOT/.github/workflows" "$ROOT/.depot/workflows" \
        "$ROOT/.depot/actions/fixture"
    cat > "$ROOT/.github/workflows/validate.yml" <<FIXTURE
jobs:
  test:
    steps:
      - uses: actions/checkout@$github_pin
      - uses: taiki-e/install-action@$github_pin
      - uses: ./local-action
      - uses: docker://alpine:3.23
      # uses: actions/cache@v5
FIXTURE
    cat > "$ROOT/.depot/workflows/ci.yml" <<FIXTURE
jobs:
  test:
    steps:
      - uses: actions/checkout@$depot_workflow_pin
FIXTURE
    cat > "$ROOT/.depot/actions/fixture/action.yml" <<FIXTURE
runs:
  using: composite
  steps:
    - uses: taiki-e/install-action@$depot_action_pin
FIXTURE
}

expect_success() {
    local output
    if ! output=$("$CHECKER" --root "$ROOT" 2>&1); then
        fail "matching action pins were rejected: $output"
    fi
}

expect_failure() { # $1 = expected diagnostic
    local expected="$1"
    local output status
    set +e
    output=$("$CHECKER" --root "$ROOT" 2>&1)
    status=$?
    set -e
    if [ "$status" -eq 0 ] || [[ "$output" != *"$expected"* ]]; then
        fail "expected failure containing '$expected'; got status $status: $output"
    fi
}

PIN=3d3c42e5aac5ba805825da76410c181273ba90b1

write_fixtures "$PIN" "$PIN" "$PIN"
expect_success

rm -rf "$ROOT"
write_fixtures v7 "$PIN" "$PIN"
expect_failure 'must use a full 40-character commit SHA'

rm -rf "$ROOT"
write_fixtures "$PIN" aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa "$PIN"
expect_failure 'uses inconsistent pins'

rm -rf "$ROOT"
write_fixtures "$PIN" "$PIN" aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
expect_failure 'uses inconsistent pins'

printf 'Action pin tests passed\n'
