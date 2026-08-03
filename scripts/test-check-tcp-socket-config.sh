#!/bin/bash
# Tests for scripts/check-tcp-socket-config.sh
#
# That linter is awk/grep based, so its blind spots are invisible from the outside: a rule that
# quietly stops matching still reports "check passed". These fixtures pin both directions — every
# line marked EXPECT-VIOLATION must be reported at that exact line, and nothing in the clean
# fixture may be reported at all. Each violations fixture keeps its markers in a SINGLE file, so
# comparing bare line numbers stays unambiguous.
#
# The linter itself is invoked, with its search directories and funnel/exempt paths pointed at the
# fixtures, so the rules under test are the shipped ones rather than a copy that can drift.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LINTER="$SCRIPT_DIR/check-tcp-socket-config.sh"
FIXTURES="$SCRIPT_DIR/tests/tcp-socket-config"

echo "🔍 Testing check-tcp-socket-config.sh..."

FAILED=0

# run_violation_fixture <dir> <marked-file>: the linter must exit 1 and report exactly the
# marked lines, no others.
run_violation_fixture() {
    local dir="$1" marked="$2"
    local expected reported output status
    # anchored at end-of-line so a fixture header naming the marker in prose is not one.
    # sort is lexical because comm requires that order.
    expected=$(grep -n 'EXPECT-VIOLATION$' "$dir/$marked" | cut -d: -f1 | sort)
    set +e
    output=$(TCP_CHECK_FUNNEL_FILE="$dir/lib.rs" "$LINTER" "$dir" 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 1 ]; then
        echo -e "${RED}FAIL: the linter must exit 1 on $(basename "$dir"), got $status${NC}"
        FAILED=1
    fi
    reported=$(echo "$output" | grep -oE '^  Line [0-9]+' | grep -oE '[0-9]+' | sort)
    if [ "$expected" != "$reported" ]; then
        echo -e "${RED}FAIL: $(basename "$dir"): reported violations do not match the EXPECT-VIOLATION markers${NC}"
        echo "  missed (marked, not reported):"
        comm -23 <(echo "$expected") <(echo "$reported") | sed 's/^/    line /'
        echo "  unexpected (reported, not marked):"
        comm -13 <(echo "$expected") <(echo "$reported") | sed 's/^/    line /'
        FAILED=1
    else
        echo -e "${GREEN}  ✓ $(basename "$dir"): all $(echo "$expected" | wc -l | tr -d ' ') marked violations reported, no others${NC}"
    fi
}

# ── raw connections / hand-set options outside the funnel file (rules 1 + 2a) ───────────────────
run_violation_fixture "$FIXTURES/violations-outside" "user.rs"

# ── unconfigured helper and stray impl-method accept inside the funnel file (rule 2b) ───────────
run_violation_fixture "$FIXTURES/violations-funnel" "lib.rs"

# ── the clean fixture: nothing may be reported (exempt file included via override) ──────────────
set +e
clean_output=$(TCP_CHECK_FUNNEL_FILE="$FIXTURES/clean/lib.rs" \
    TCP_CHECK_CONNECT_EXEMPT="$FIXTURES/clean/exempt.rs" \
    "$LINTER" "$FIXTURES/clean" 2>&1)
clean_status=$?
set -e
if [ "$clean_status" -ne 0 ]; then
    echo -e "${RED}FAIL: the linter must exit 0 on the clean fixture, got $clean_status${NC}"
    echo "$clean_output" | grep -E '^  Line [0-9]+' | sed 's/^/    false positive: /'
    FAILED=1
else
    echo -e "${GREEN}  ✓ no false positives on the clean fixture${NC}"
fi

# ── the vacuous fixture: a funnel missing one owned option must FAIL, not pass vacuously ────────
set +e
vacuous_output=$(TCP_CHECK_FUNNEL_FILE="$FIXTURES/vacuous/lib.rs" "$LINTER" "$FIXTURES/vacuous" 2>&1)
vacuous_status=$?
set -e
if [ "$vacuous_status" -ne 1 ] || ! echo "$vacuous_output" | grep -q "no longer sets"; then
    echo -e "${RED}FAIL: a funnel missing set_tcp_user_timeout must fail the vacuity check (exit \
$vacuous_status)${NC}"
    FAILED=1
else
    echo -e "${GREEN}  ✓ a hollowed-out funnel fails instead of passing vacuously${NC}"
fi

if [ "$FAILED" -ne 0 ]; then
    echo -e "${RED}❌ check-tcp-socket-config.sh behaviour has drifted from its fixtures${NC}"
    exit 1
fi
echo -e "${GREEN}✅ check-tcp-socket-config.sh matches its fixtures${NC}"
