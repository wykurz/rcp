#!/bin/bash

set -euo pipefail

invoked_as="${0##*/}"

if [[ "$invoked_as" == "git" ]]; then
    if [[ "$*" == "ls-files --others --exclude-standard -z" ]]; then
        case "$DEPOT_TEST_SCENARIO" in
            git-scan-failure)
                echo "fake untracked-file scan failed" >&2
                exit 42
                ;;
            untracked)
                printf 'needed.txt\0nested/file with spaces.txt\0'
                ;;
            depot-untracked)
                printf '.depot/generated.yml\0'
                ;;
        esac
        exit 0
    fi

    echo "unexpected fake git invocation: $*" >&2
    exit 26
fi

if [[ "$invoked_as" == "depot" ]]; then
    {
        printf 'CALL'
        printf '\t%s' "$@"
        printf '\n'
    } >>"$DEPOT_TEST_LOG"

    if [[ "$1 $2" == "ci run" && "${3:-}" != "show" ]]; then
        if [[ "$DEPOT_TEST_SCENARIO" == "launch-failure" ]]; then
            echo "fake launch failed" >&2
            exit 23
        fi

        echo "fake launch output"
        if [[ "$DEPOT_TEST_SCENARIO" != "missing-run-id" ]]; then
            echo "Run: test-run-id"
        fi
        exit 0
    fi

    if [[ "$1 $2 ${3:-}" == "ci run show" ]]; then
        case "$DEPOT_TEST_SCENARIO" in
            failed)
                printf '{"status":"failed"}\n'
                ;;
            cancelled)
                printf '{"status":"cancelled"}\n'
                ;;
            malformed)
                echo "not JSON"
                ;;
            status-query-failure)
                echo "fake status query failed" >&2
                exit 24
                ;;
            status-query-transient)
                poll_count=0
                if [[ -f "$DEPOT_TEST_STATE" ]]; then
                    poll_count="$(<"$DEPOT_TEST_STATE")"
                fi
                if [[ "$poll_count" -eq 0 ]]; then
                    echo 1 >"$DEPOT_TEST_STATE"
                    echo "fake transient status query failure" >&2
                    exit 24
                fi
                printf '{"status":"finished"}\n'
                ;;
            status-query-four-transient)
                poll_count=0
                if [[ -f "$DEPOT_TEST_STATE" ]]; then
                    poll_count="$(<"$DEPOT_TEST_STATE")"
                fi
                if [[ "$poll_count" -lt 4 ]]; then
                    ((poll_count += 1))
                    echo "$poll_count" >"$DEPOT_TEST_STATE"
                    echo "fake transient status query failure" >&2
                    exit 24
                fi
                printf '{"status":"finished"}\n'
                ;;
            status-stderr)
                echo "fake status warning" >&2
                printf '{"status":"finished"}\n'
                ;;
            finished | untracked)
                printf '{"status":"finished"}\n'
                ;;
            unknown)
                printf '{"status":"unexpected"}\n'
                ;;
            *)
                poll_count=0
                if [[ -f "$DEPOT_TEST_STATE" ]]; then
                    poll_count="$(<"$DEPOT_TEST_STATE")"
                fi
                if [[ "$poll_count" -eq 0 ]]; then
                    echo 1 >"$DEPOT_TEST_STATE"
                    printf '{"status":"running"}\n'
                else
                    printf '{"status":"finished"}\n'
                fi
                ;;
        esac
        exit 0
    fi

    if [[ "$1 $2" == "ci logs" ]]; then
        if [[ "$DEPOT_TEST_SCENARIO" == "log-stream-failure" ]]; then
            echo "fake live log failure" >&2
            exit 27
        fi
        echo "fake live logs"
        exit 0
    fi

    if [[ "$1 $2" == "ci status" ]]; then
        echo "DETAILED STATUS: failed"
        exit 0
    fi

    echo "unexpected fake depot invocation: $*" >&2
    exit 25
fi

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
script="$repo_root/scripts/depot-ci.sh"
test_dir="$(mktemp -d)"
trap 'rm -rf "$test_dir"' EXIT
mkdir -p "$test_dir/bin"
ln -s "$repo_root/scripts/test-depot-ci.sh" "$test_dir/bin/depot"
ln -s "$repo_root/scripts/test-depot-ci.sh" "$test_dir/bin/git"

export DEPOT_TEST_LOG="$test_dir/depot.log"
export DEPOT_TEST_STATE="$test_dir/status-count"
export DEPOT_CI_TEST_FAKE=1
export PATH="$test_dir/bin:$PATH"
export RCP_DEPOT_CI_POLL_INTERVAL=0

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    [[ "$haystack" == *"$needle"* ]] || fail "expected output to contain: $needle"
}

assert_status() {
    local expected="$1"
    [[ "$status" -eq "$expected" ]] || \
        fail "expected exit $expected, got $status; output: $output"
}

assert_call() {
    local expected="$1"
    grep -Fxq "$expected" "$DEPOT_TEST_LOG" || \
        fail "missing call: $expected; calls: $(<"$DEPOT_TEST_LOG")"
}

assert_call_count() {
    local expected_count="$1"
    local expected_call="$2"
    local actual_count
    actual_count="$(grep -Fxc "$expected_call" "$DEPOT_TEST_LOG" || true)"
    [[ "$actual_count" -eq "$expected_count" ]] || \
        fail "expected $expected_count calls to: $expected_call; got $actual_count"
}

assert_call_before() {
    local first="$1"
    local second="$2"
    local first_line
    local second_line
    first_line="$(grep -nFx "$first" "$DEPOT_TEST_LOG" | head -n 1 | cut -d: -f1)"
    second_line="$(grep -nFx "$second" "$DEPOT_TEST_LOG" | head -n 1 | cut -d: -f1)"
    [[ -n "$first_line" && -n "$second_line" && "$first_line" -lt "$second_line" ]] || \
        fail "expected call before '$second': $first"
}

run_case() {
    local scenario="$1"
    shift
    : >"$DEPOT_TEST_LOG"
    rm -f "$DEPOT_TEST_STATE"
    set +e
    output="$(DEPOT_TEST_SCENARIO="$scenario" "$script" "$@" 2>&1)"
    status=$?
    set -e
}

[[ -x "$script" ]] || fail "scripts/depot-ci.sh is absent or not executable"

run_case success test "doc job"
assert_status 0
assert_contains "$output" "fake launch output"
assert_contains "$output" "Run: test-run-id"
assert_call $'CALL\tci\trun\t--workflow\t.depot/workflows/ci.yml\t--job\ttest\t--job\tdoc job'
assert_call $'CALL\tci\trun\tshow\ttest-run-id\t--output\tjson'
while IFS= read -r call; do
    if [[ "$call" == $'CALL\tci\trun\t--workflow\t'* && "$call" == *$'\t--follow'* ]]; then
        fail "submit command received --follow"
    fi
done <"$DEPOT_TEST_LOG"
if grep -Fq $'CALL\tci\tlogs' "$DEPOT_TEST_LOG"; then
    fail "multi-job run streamed a single job's logs"
fi

run_case success test
assert_status 0
assert_call $'CALL\tci\tlogs\ttest-run-id\t--job\ttest\t--workflow\tci.yml\t--follow'
assert_call $'CALL\tci\trun\tshow\ttest-run-id\t--output\tjson'
assert_call_before \
    $'CALL\tci\tlogs\ttest-run-id\t--job\ttest\t--workflow\tci.yml\t--follow' \
    $'CALL\tci\trun\tshow\ttest-run-id\t--output\tjson'

run_case log-stream-failure test
assert_status 0
assert_contains "$output" "live log streaming failed"
assert_call_before \
    $'CALL\tci\tlogs\ttest-run-id\t--job\ttest\t--workflow\tci.yml\t--follow' \
    $'CALL\tci\trun\tshow\ttest-run-id\t--output\tjson'

run_case failed lint
[[ "$status" -ne 0 ]] || fail "failed terminal status returned success"
assert_contains "$output" "ended with status failed"
assert_contains "$output" "DETAILED STATUS: failed"
assert_call $'CALL\tci\tstatus\ttest-run-id'

run_case cancelled lint
[[ "$status" -ne 0 ]] || fail "cancelled terminal status returned success"
assert_contains "$output" "ended with status cancelled"
assert_call $'CALL\tci\tstatus\ttest-run-id'

run_case launch-failure doc
assert_status 23
assert_contains "$output" "fake launch failed"
if grep -Fq $'CALL\tci\trun\tshow' "$DEPOT_TEST_LOG"; then
    fail "launch failure was polled"
fi

run_case missing-run-id doc
[[ "$status" -ne 0 ]] || fail "missing run ID returned success"
assert_contains "$output" "run ID"

run_case malformed doc
[[ "$status" -ne 0 ]] || fail "malformed status JSON returned success"
assert_contains "$output" "status JSON"

run_case unknown doc
[[ "$status" -ne 0 ]] || fail "unknown status returned success"
assert_contains "$output" "unexpected"

run_case status-query-failure doc
assert_status 24
assert_contains "$output" "fake status query failed"
assert_call_count 5 $'CALL\tci\trun\tshow\ttest-run-id\t--output\tjson'

run_case status-query-transient doc
assert_status 0
assert_contains "$output" "fake transient status query failure"
assert_call_count 2 $'CALL\tci\trun\tshow\ttest-run-id\t--output\tjson'

run_case status-query-four-transient doc
assert_status 0
assert_call_count 5 $'CALL\tci\trun\tshow\ttest-run-id\t--output\tjson'

run_case status-stderr doc
assert_status 0
assert_contains "$output" "fake status warning"

run_case untracked doc
assert_status 0
assert_contains "$output" "ordinary untracked files are excluded"
assert_contains "$output" "needed.txt"
assert_contains "$output" "nested/file with spaces.txt"

run_case depot-untracked doc
assert_status 0
if [[ "$output" == *"ordinary untracked files are excluded"* ]]; then
    fail "Depot's auto-included .depot files produced an untracked-file warning"
fi

run_case git-scan-failure doc
assert_status 42
assert_contains "$output" "fake untracked-file scan failed"
if grep -Fq $'CALL\tci\trun' "$DEPOT_TEST_LOG"; then
    fail "untracked-file scan failure was detected after launch"
fi

: >"$DEPOT_TEST_LOG"
rm -f "$DEPOT_TEST_STATE"
set +e
output="$(
    PATH="$test_dir/bin" \
        DEPOT_TEST_SCENARIO=finished \
        "$script" doc 2>&1
)"
status=$?
set -e
[[ "$status" -ne 0 ]] || fail "missing jq returned success"
assert_contains "$output" "required command 'jq' was not found"
if grep -Fq $'CALL\tci\trun' "$DEPOT_TEST_LOG"; then
    fail "missing jq was detected after launch"
fi

run_case success
[[ "$status" -ne 0 ]] || fail "empty job list returned success"
assert_contains "$output" "job"

echo "depot-ci helper tests passed"
