#!/bin/bash
# Tests for scripts/check-error-logging.sh
#
# That linter is awk/regex based, so its blind spots are invisible from the outside: a rule that
# quietly stops matching still reports "check passed". These fixtures pin both directions — every
# call marked EXPECT-VIOLATION must be reported at that exact line, and nothing in the clean
# fixture may be reported at all.
#
# The linter itself is invoked, with its search directories pointed at the fixtures, so the rules
# under test are the shipped ones rather than a copy that can drift.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LINTER="$SCRIPT_DIR/check-error-logging.sh"
FIXTURES="$SCRIPT_DIR/tests/error-logging"

echo "🔍 Testing check-error-logging.sh..."

FAILED=0

# ── the violations fixture: every marked call must be reported, and nothing else ────────────────
VIOLATIONS_FIXTURE="$FIXTURES/violations/violations.rs"
# anchored at end-of-line so the fixture's own header, which names the marker in prose, is not
# mistaken for one. `sort` is lexical rather than numeric because `comm` below requires that order.
expected=$(grep -n 'EXPECT-VIOLATION$' "$VIOLATIONS_FIXTURE" | cut -d: -f1 | sort)

# the linter exits 1 when it finds anything, which is the expected outcome here
set +e
output=$("$LINTER" "$FIXTURES/violations" 2>&1)
status=$?
set -e

if [ "$status" -ne 1 ]; then
    echo -e "${RED}FAIL: the linter must exit 1 on the violations fixture, got $status${NC}"
    FAILED=1
fi

reported=$(echo "$output" | grep -oE '^  Line [0-9]+' | grep -oE '[0-9]+' | sort)

if [ "$expected" != "$reported" ]; then
    echo -e "${RED}FAIL: reported violations do not match the EXPECT-VIOLATION markers${NC}"
    echo "  missed (marked, not reported):"
    comm -23 <(echo "$expected") <(echo "$reported") | sed 's/^/    line /'
    echo "  unexpected (reported, not marked):"
    comm -13 <(echo "$expected") <(echo "$reported") | sed 's/^/    line /'
    FAILED=1
else
    echo -e "${GREEN}  ✓ all $(echo "$expected" | wc -l | tr -d ' ') marked violations reported, no others${NC}"
fi

# ── the clean fixture: nothing may be reported ──────────────────────────────────────────────────
set +e
clean_output=$("$LINTER" "$FIXTURES/clean" 2>&1)
clean_status=$?
set -e

if [ "$clean_status" -ne 0 ]; then
    echo -e "${RED}FAIL: the linter must exit 0 on the clean fixture, got $clean_status${NC}"
    echo "$clean_output" | grep -E '^  Line [0-9]+' | sed 's/^/    false positive: /'
    FAILED=1
else
    echo -e "${GREEN}  ✓ no false positives on the clean fixture${NC}"
fi

# ── the unparsed fixtures: text the lexer stops reading must be ANNOUNCED ───────────────────────
# Once the lexer stops reading a file the linter checks no more of it, so the one outcome it must never
# produce is "passed". Pinned separately because such a file swallows any marker placed below it, and
# in one directory because the two shapes reach end of file differently: a call whose parens never
# balance, and a raw string that never closes. Each is asserted by its own message — a single grep for
# `UNPARSED` would let one shape cover for the other going quiet.
set +e
unparsed_output=$("$LINTER" "$FIXTURES/unparsed" 2>&1)
unparsed_status=$?
set -e

if [ "$unparsed_status" -ne 1 ] ||
    ! echo "$unparsed_output" | grep -q 'UNPARSED: could not find where this call ends' ||
    ! echo "$unparsed_output" | grep -q 'UNPARSED: a raw string opened here and never closed'; then
    echo -e "${RED}FAIL: an unterminated call and an unterminated raw string must each be reported as UNPARSED, exiting 1${NC}"
    echo "  got status $unparsed_status, output:"
    echo "$unparsed_output" | sed 's/^/    /'
    FAILED=1
else
    echo -e "${GREEN}  ✓ text the lexer cannot get through is announced instead of silently skipping the file${NC}"
fi

# ── an empty sweep must fail rather than report success ──────────────────────────────────────────
# Scanning nothing and printing "check passed" is indistinguishable from a clean codebase, so a
# directory list that matches nothing (a typo, a moved crate, a path word-split on a space) is an error.
set +e
empty_output=$("$LINTER" "$FIXTURES/does-not-exist" 2>&1)
empty_status=$?
set -e

if [ "$empty_status" -ne 1 ] || ! echo "$empty_output" | grep -q 'nothing was checked'; then
    echo -e "${RED}FAIL: a run matching no directory must fail, not report success${NC}"
    echo "  got status $empty_status, output:"
    echo "$empty_output" | sed 's/^/    /'
    FAILED=1
else
    echo -e "${GREEN}  ✓ a run that scans nothing fails instead of reporting success${NC}"
fi

if [ "$FAILED" -eq 1 ]; then
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}ERROR: check-error-logging.sh does not behave as specified${NC}"
    echo "Fixtures live in scripts/tests/error-logging/."
    echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    exit 1
fi

echo -e "${GREEN}✅ check-error-logging.sh tests passed!${NC}"
exit 0
