#!/bin/bash
# Remote Test Naming Convention Linter
# Ensures all tests that use localhost: (run rcpd remotely) are named test_remote_*
#
# Tests using localhost: must start with test_remote_* to ensure:
# 1. They are skipped in nix sandbox (--skip=test_remote)
# 2. They run serially (nextest filter 'test(remote)')

set -euo pipefail

# colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # no color

echo "🔍 checking remote test naming conventions..."

VIOLATIONS=0
TEMP_FILE=$(mktemp)
trap 'rm -f "$TEMP_FILE"' EXIT

# find all test functions in remote_tests.rs
TEST_FILE="rcp/tests/remote_tests.rs"

if [ ! -f "$TEST_FILE" ]; then
    echo -e "${RED}✗ $TEST_FILE not found${NC}"
    exit 1
fi

# use grep to find test functions that contain localhost:
# then check if they start with test_remote_
grep -n '^fn test_[a-zA-Z0-9_]*(' "$TEST_FILE" | while IFS=: read -r line_num func_line; do
    # extract function name
    test_name=$(echo "$func_line" | sed -E 's/^fn ([a-zA-Z0-9_]+)\(.*/\1/')

    # find the end of this test function (next function or end of file)
    next_func_line=$(grep -n '^fn test_[a-zA-Z0-9_]*(' "$TEST_FILE" | grep -A1 "^${line_num}:" | tail -1 | cut -d: -f1)
    if [ -z "$next_func_line" ] || [ "$next_func_line" = "$line_num" ]; then
        # this is the last function, check until end of file
        test_body=$(sed -n "${line_num},\$p" "$TEST_FILE")
    else
        # extract lines from current function to next function
        end_line=$((next_func_line - 1))
        test_body=$(sed -n "${line_num},${end_line}p" "$TEST_FILE")
    fi

    # check if test body contains localhost:
    if echo "$test_body" | grep -q 'localhost:'; then
        # check if test name starts with test_remote_
        if ! echo "$test_name" | grep -q '^test_remote_'; then
            echo "$test_name"
        fi
    fi
done > "$TEMP_FILE"

# check if any violations were found
if [ -s "$TEMP_FILE" ]; then
    VIOLATIONS=$(wc -l < "$TEMP_FILE")
    echo -e "${RED}✗ found $VIOLATIONS test(s) using localhost: without test_remote_ prefix:${NC}"
    while IFS= read -r test_name; do
        echo -e "${RED}  - $test_name${NC}"
    done < "$TEMP_FILE"
    echo ""
    echo "tests using localhost: must be named test_remote_* to ensure:"
    echo "  1. they are skipped in nix sandbox (--skip=test_remote)"
    echo "  2. they run serially (nextest filter 'test(remote)')"
    exit 1
else
    echo -e "${GREEN}✓ all tests using localhost: are properly named test_remote_*${NC}"
    exit 0
fi
