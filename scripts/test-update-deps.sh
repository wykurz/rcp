#!/bin/bash
# Tests for the MSRV report filter in scripts/update-deps.sh
#
# That filter is a single sed substitution over `cargo update --verbose` output, which is a human
# format and not a stable machine interface. If cargo reflows the line, the filter silently stops
# matching and every MSRV hold-back goes unreported -- the run still looks healthy, because nothing
# it guards actually fails. These fixtures are what makes that visible.
#
# Both directions are pinned: `msrv-holdbacks` must report exactly the MSRV-blocked packages, and
# the two negative fixtures must report nothing at all. In particular a hold-back with no
# `requires Rust` suffix is held for an unrelated reason and must NOT be reported as an MSRV one.
#
# The shipped script is invoked via its --report-from hook, so the rules under test are the real
# ones rather than a copy that can drift.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$SCRIPT_DIR/update-deps.sh"
FIXTURES="$SCRIPT_DIR/tests/update-deps"

echo "🔍 Testing the update-deps MSRV report filter..."

FAILED=0
CASES=0

for fixture in "$FIXTURES"/*.txt; do
    name="$(basename "$fixture" .txt)"
    expected_file="${fixture%.txt}.expected"
    CASES=$((CASES + 1))

    if [ ! -f "$expected_file" ]; then
        echo -e "${RED}FAIL: $name has no .expected file${NC}"
        FAILED=1
        continue
    fi

    actual=$("$SCRIPT" --report-from "$fixture")
    expected=$(cat "$expected_file")

    if [ "$actual" != "$expected" ]; then
        echo -e "${RED}FAIL: $name${NC}"
        echo "  expected:"
        echo "$expected" | sed 's/^/    /'
        echo "  actual:"
        echo "$actual" | sed 's/^/    /'
        FAILED=1
    fi
done

if [ "$CASES" -eq 0 ]; then
    echo -e "${RED}FAIL: no fixtures found in $FIXTURES${NC}"
    echo "An empty fixture directory would make this script pass vacuously."
    exit 1
fi

# A filter that matched everything would satisfy the positive fixture while being useless. Prove it
# discriminates: the negative fixture has hold-back lines, just none for MSRV reasons.
if ! grep -q "Unchanged" "$FIXTURES/other-holdbacks-only.txt"; then
    echo -e "${RED}FAIL: the negative fixture no longer contains any hold-back lines,${NC}"
    echo "so it can no longer show that non-MSRV hold-backs are excluded."
    FAILED=1
fi

# ── the pull-request body fragment ─────────────────────────────────────────────────────────────
# This is what the workflow pastes into the PR, so a broken rendering is invisible until a real
# monthly run produces a mangled description.
body_out="$(mktemp)"


"$SCRIPT" --report-from "$FIXTURES/msrv-holdbacks.txt" --report-out "$body_out" > /dev/null
if ! grep -q '^### Held back by the MSRV$' "$body_out"; then
    echo -e "${RED}FAIL: the report body is missing its heading${NC}"
    FAILED=1
fi
if ! grep -q 'enum-map` 2.7.3 → 3.1.0 (needs Rust 1.95)' "$body_out"; then
    echo -e "${RED}FAIL: the report body is missing the hold-back lines${NC}"
    FAILED=1
fi

# ── option-looking values ──────────────────────────────────────────────────────────────────────
# `--report-out --dry-run` used to consume the flag as the path, leaving DRY_RUN=0 -- so a command
# that reads as a preview performed a real update. Both option-taking flags must reject a value
# that looks like another option, and reject a missing one.
#
# The status and the message are both asserted, because "exited nonzero" is far too weak a signal
# here: in the CI lint job cargo-edit is not installed, so the OLD, broken parser would swallow the
# flag, reach the tool check, and fail there with status 1 -- satisfying a bare "did it fail?" test
# while the bug it is meant to catch is fully present.
for bad in "--report-out --dry-run" "--report-out" "--report-from -n" "--report-from"; do
    set +e
    # shellcheck disable=SC2086  # deliberately word-split: these are argv fixtures
    out=$("$SCRIPT" $bad 2>&1)
    status=$?
    set -e

    if [ "$status" -ne 2 ]; then
        echo -e "${RED}FAIL: '$bad' exited $status, expected 2 (argument rejected)${NC}"
        echo "$out" | sed 's/^/    /'
        FAILED=1
    elif ! echo "$out" | grep -q "needs a path"; then
        echo -e "${RED}FAIL: '$bad' exited 2 but not from the argument check${NC}"
        echo "$out" | sed 's/^/    /'
        FAILED=1
    fi
done

# Nothing held back must write an EMPTY file rather than no file: the workflow tests -s to choose
# its wording, so a missing file and an empty one must not be confused.
"$SCRIPT" --report-from "$FIXTURES/nothing-held-back.txt" --report-out "$body_out" > /dev/null
if [ ! -f "$body_out" ]; then
    echo -e "${RED}FAIL: --report-out must still create the file when nothing is held back${NC}"
    FAILED=1
elif [ -s "$body_out" ]; then
    echo -e "${RED}FAIL: the report body must be empty when nothing is held back, got:${NC}"
    sed 's/^/    /' "$body_out"
    FAILED=1
fi

# ── live paths, against a mock cargo ───────────────────────────────────────────────────────────
# Everything above stops at --report-from, which returns before any of the machinery that has
# actually carried bugs: the lockfile guard, the colour flag, and the cross-check against cargo's
# raw output. Fixtures alone cannot see those -- an earlier, broken revision of this script passed
# every one of them.
#
# So drive the real code path with a fake cargo on PATH. The script anchors itself to its own
# repository root, so it is reached through a symlink inside a throwaway tree: `dirname
# $BASH_SOURCE/..` then resolves there and the run cannot touch the real workspace. Symlinked
# rather than copied so the code under test is always the shipped file.
mock_root=""
cleanup_mock() { [ -n "$mock_root" ] && rm -rf "$mock_root"; }
trap 'rm -f "$body_out"; cleanup_mock' EXIT

setup_mock() { # $1 = canned `cargo update --verbose` output
    mock_root="$(mktemp -d)"
    mkdir -p "$mock_root/repo/scripts" "$mock_root/bin" "$mock_root/elsewhere"
    ln -s "$SCRIPT" "$mock_root/repo/scripts/update-deps.sh"
    printf '%s\n' "$1" > "$mock_root/report.txt"

    # The mock writes to $PWD/Cargo.lock and records its own working directory, rather than writing
    # to the repo path it could just as well hardcode. That is the difference between a test that
    # notices the script forgetting to `cd` to its own workspace and one that does not: with a
    # hardcoded path, deleting that `cd` still leaves every assertion here passing, because the
    # mock would obligingly write where the test was looking.
    cat > "$mock_root/bin/cargo" <<MOCK
#!/bin/bash
echo "\$*" >> "$mock_root/calls.txt"
pwd >> "$mock_root/cwds.txt"
if [ "\$1" = "upgrade" ]; then
    # cargo-edit resolves workspace metadata before honouring --dry-run, and that rewrites a stale
    # lockfile. Reproduce that here: it is the whole reason the guard exists.
    echo "mutated by cargo upgrade" >> "\$PWD/Cargo.lock"
    exit 0
fi
case "\$*" in
    *--verbose*) cat "$mock_root/report.txt" ;;
    *) echo "(mock) cargo \$*" ;;
esac
MOCK
    cp "$mock_root/bin/cargo" "$mock_root/bin/cargo-upgrade"
    chmod +x "$mock_root/bin/cargo" "$mock_root/bin/cargo-upgrade"
}

# Invoked by absolute path from a DIFFERENT directory, which is the shape that used to update the
# caller's project instead of this one.
run_mock() { (cd "$mock_root/elsewhere" && PATH="$mock_root/bin:$PATH" "$mock_root/repo/scripts/update-deps.sh" "$@" 2>&1); }

assert_mock_ran_in_repo() {
    if [ ! -s "$mock_root/cwds.txt" ]; then
        echo -e "${RED}FAIL: the mock cargo was never invoked${NC}"; FAILED=1; return
    fi
    while read -r dir; do
        if [ "$dir" != "$mock_root/repo" ]; then
            echo -e "${RED}FAIL: cargo ran in '$dir', expected the script's own repo root${NC}"
            echo "    (the script must cd to its workspace, or it updates the caller's project)"
            FAILED=1
            return
        fi
    done < "$mock_root/cwds.txt"
    if [ -e "$mock_root/elsewhere/Cargo.lock" ]; then
        echo -e "${RED}FAIL: a Cargo.lock was created in the caller's directory${NC}"; FAILED=1
    fi
}

HELD='   Unchanged sysinfo v0.38.4 (available: v0.39.6, requires Rust 1.95)'
SELECTED='   Unchanged enum-map v3.1.0 (requires Rust 1.95)'

# 1. an existing lockfile must come back byte-identical
setup_mock "$HELD
$SELECTED"
printf 'original lockfile\n' > "$mock_root/repo/Cargo.lock"
out=$(run_mock -n) || {
    echo -e "${RED}FAIL: dry run exited nonzero${NC}"; echo "$out" | sed 's/^/    /'; FAILED=1
}
if [ "$(cat "$mock_root/repo/Cargo.lock")" != "original lockfile" ]; then
    echo -e "${RED}FAIL: dry run did not restore the lockfile it mutated${NC}"
    FAILED=1
fi

# The selected-but-incompatible line has no `available:`; counting it would abort the run in
# exactly the resolver-fallback case that most needs to reach the MSRV gate.
if ! echo "$out" | grep -q 'sysinfo` 0.38.4 → 0.39.6'; then
    echo -e "${RED}FAIL: the held-back package was not reported${NC}"; FAILED=1
fi
if echo "$out" | grep -q "does not match cargo's output"; then
    echo -e "${RED}FAIL: the fallback-selected line triggered a false drift failure${NC}"; FAILED=1
fi

# The report scan must disable colour, or the anchored substitution silently matches nothing.
if ! grep -q -- "--color never" "$mock_root/calls.txt"; then
    echo -e "${RED}FAIL: the report scan did not pass --color never${NC}"
    sed 's/^/    /' "$mock_root/calls.txt"
    FAILED=1
fi
assert_mock_ran_in_repo
cleanup_mock

# 2. no lockfile to begin with: cargo creates one, and the preview must not leave it behind
setup_mock "$HELD"
set +e
out=$(run_mock -n)
status=$?
set -e
if [ "$status" -ne 0 ]; then
    echo -e "${RED}FAIL: dry run with no lockfile exited $status${NC}"
    echo "$out" | sed 's/^/    /'
    FAILED=1
fi
if [ -e "$mock_root/repo/Cargo.lock" ]; then
    echo -e "${RED}FAIL: dry run left a lockfile behind where none existed${NC}"
    FAILED=1
fi
assert_mock_ran_in_repo
cleanup_mock

# 2b. a symlinked lockfile must be refused rather than saved and restored: `-f` follows the link,
# so a dangling one reads as absent and the cleanup would delete the link the user created.
setup_mock "$HELD"
ln -s "$mock_root/nonexistent-target" "$mock_root/repo/Cargo.lock"
set +e
out=$(run_mock -n)
status=$?
set -e
if [ "$status" -eq 0 ] || ! echo "$out" | grep -q "is a symlink"; then
    echo -e "${RED}FAIL: a dangling lockfile symlink was not refused (exit $status)${NC}"
    echo "$out" | sed 's/^/    /'
    FAILED=1
fi
if [ ! -L "$mock_root/repo/Cargo.lock" ]; then
    echo -e "${RED}FAIL: the lockfile symlink was destroyed${NC}"; FAILED=1
fi
cleanup_mock

# 3. genuine drift must still fail loudly. Trailing text keeps both markers on the line -- so the
# raw count sees it -- while breaking the anchored pattern the report keys on.
setup_mock '   Unchanged sysinfo v0.38.4 (available: v0.39.6, requires Rust 1.95) [reflowed]'
printf 'original lockfile\n' > "$mock_root/repo/Cargo.lock"
set +e
out=$(run_mock -n 2>&1)
status=$?
set -e
if [ "$status" -eq 0 ] || ! echo "$out" | grep -q "does not match cargo's output"; then
    echo -e "${RED}FAIL: a reflowed cargo line did not trigger the drift check (exit $status)${NC}"
    echo "$out" | sed 's/^/    /'
    FAILED=1
fi
# Cleanup has to survive the failure path too -- that is the one where an abandoned lockfile is
# easiest to leave behind, since the script exits through `set -e` rather than the bottom.
if [ "$(cat "$mock_root/repo/Cargo.lock")" != "original lockfile" ]; then
    echo -e "${RED}FAIL: the lockfile was not restored when the run failed${NC}"
    FAILED=1
fi
cleanup_mock

if [ "$FAILED" -ne 0 ]; then
    echo -e "${RED}❌ update-deps report filter tests failed${NC}"
    exit 1
fi

echo -e "${GREEN}✅ update-deps tests passed ($CASES fixtures + live paths against a mock cargo)${NC}"
