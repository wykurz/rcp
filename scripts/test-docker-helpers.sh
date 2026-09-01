#!/bin/bash

set -euo pipefail

invoked_as="${0##*/}"
if [[ "$invoked_as" == "docker" || "$invoked_as" == "docker-compose" ]]; then
    {
        printf 'CALL\t%s' "$invoked_as"
        printf '\t%s' "$@"
        printf '\n'
    } >>"$RCP_DOCKER_TEST_LOG"

    if [[ "$invoked_as" == "docker-compose" ]]; then
        case "$*" in
            "up -d" | logs | "logs master")
                exit 0
                ;;
            *)
                echo "unexpected fake docker-compose invocation: $*" >&2
                exit 31
                ;;
        esac
    fi

    if [[ "$*" == info ]]; then
        exit 0
    fi

    prefix="exec -u testuser rcp-test-master ssh -o BatchMode=yes -o ConnectTimeout=5"
    for host in host-a host-b; do
        if [[ "$*" == "$prefix $host hostname" ]]; then
            counter="$RCP_DOCKER_TEST_STATE/$host"
            count=0
            if [[ -f "$counter" ]]; then
                count="$(<"$counter")"
            fi
            ((count += 1))
            echo "$count" >"$counter"

            if [[ "$RCP_DOCKER_TEST_SCENARIO" == slow-probe ]]; then
                sleep 3
                exit 1
            fi
            if [[ "$RCP_DOCKER_TEST_SCENARIO" == term-resistant-probe ]]; then
                trap '' TERM
                sleep 4
                exit 1
            fi
            if [[ "$RCP_DOCKER_TEST_SCENARIO" == eventual-success ]]; then
                if [[ "$host" == host-a && "$count" -ge 2 ]]; then
                    exit 0
                fi
                if [[ "$host" == host-b && "$count" -ge 3 ]]; then
                    exit 0
                fi
            fi
            exit 1
        fi
    done

    echo "unexpected fake docker invocation: $*" >&2
    exit 32
fi

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
source_helper="$repo_root/tests/docker/test-helpers.sh"
test_dir="$(mktemp -d)"
trap 'rm -rf "$test_dir"' EXIT

fixture="$test_dir/repo"
fake_bin="$test_dir/bin"
state_dir="$test_dir/state"
mkdir -p "$fixture/tests/docker" "$fixture/target/x86_64-unknown-linux-musl/debug" \
    "$fake_bin" "$state_dir"
cp "$source_helper" "$fixture/tests/docker/test-helpers.sh"
chmod +x "$fixture/tests/docker/test-helpers.sh"
for binary in rcp rcpd rrm rlink rcmp; do
    touch "$fixture/target/x86_64-unknown-linux-musl/debug/$binary"
done
ln -s "$repo_root/scripts/test-docker-helpers.sh" "$fake_bin/docker"
ln -s "$repo_root/scripts/test-docker-helpers.sh" "$fake_bin/docker-compose"

helper="$fixture/tests/docker/test-helpers.sh"
export PATH="$fake_bin:$PATH"
export RCP_DOCKER_TEST_LOG="$test_dir/docker.log"
export RCP_DOCKER_TEST_STATE="$state_dir"
export RCP_DOCKER_SSH_READY_POLL_SECONDS=0

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    [[ "$haystack" == *"$needle"* ]] || fail "expected output to contain: $needle"
}

assert_call() {
    local expected="$1"
    grep -Fxq "$expected" "$RCP_DOCKER_TEST_LOG" || \
        fail "missing call: $expected; calls: $(<"$RCP_DOCKER_TEST_LOG")"
}

run_case() {
    local scenario="$1"
    shift
    local command=("$helper" "$@")
    : >"$RCP_DOCKER_TEST_LOG"
    rm -f "$state_dir/host-a" "$state_dir/host-b"
    set +e
    output="$(RCP_DOCKER_TEST_SCENARIO="$scenario" "${command[@]}" 2>&1)"
    status=$?
    set -e
}

[[ -x "$source_helper" ]] || fail "tests/docker/test-helpers.sh is absent or not executable"

export RCP_DOCKER_SSH_READY_TIMEOUT_SECONDS=5
run_case eventual-success start
[[ "$status" -eq 0 ]] || fail "eventually ready SSH hosts failed: $output"
[[ -f "$state_dir/host-a" ]] || fail "host-a was never polled as testuser"
[[ -f "$state_dir/host-b" ]] || fail "host-b was never polled as testuser"
[[ "$(<"$state_dir/host-a")" -eq 2 ]] || fail "host-a was not polled until ready"
[[ "$(<"$state_dir/host-b")" -eq 3 ]] || fail "host-b was not polled until ready"
assert_call $'CALL\tdocker\texec\t-u\ttestuser\trcp-test-master\tssh\t-o\tBatchMode=yes\t-o\tConnectTimeout=5\thost-a\thostname'
assert_call $'CALL\tdocker\texec\t-u\ttestuser\trcp-test-master\tssh\t-o\tBatchMode=yes\t-o\tConnectTimeout=5\thost-b\thostname'

export RCP_DOCKER_SSH_READY_TIMEOUT_SECONDS=1
started_at="$SECONDS"
run_case never-ready start
elapsed=$((SECONDS - started_at))
[[ "$status" -ne 0 ]] || fail "SSH readiness timeout returned success"
assert_contains "$output" "SSH connectivity did not become ready within 1s"
[[ "$elapsed" -ge 1 && "$elapsed" -le 2 ]] || \
    fail "one-second readiness polling stopped after ${elapsed}s"
assert_call $'CALL\tdocker-compose\tlogs'

export RCP_DOCKER_SSH_READY_TIMEOUT_SECONDS=1
started_at="$SECONDS"
run_case slow-probe start
elapsed=$((SECONDS - started_at))
[[ "$status" -ne 0 ]] || fail "stalled SSH probe returned success"
assert_contains "$output" "SSH connectivity did not become ready within 1s"
[[ "$elapsed" -le 2 ]] || fail "one-second readiness budget took ${elapsed}s"

started_at="$SECONDS"
run_case term-resistant-probe start
elapsed=$((SECONDS - started_at))
[[ "$status" -ne 0 ]] || fail "TERM-resistant SSH probe returned success"
assert_contains "$output" "SSH connectivity did not become ready within 1s"
[[ "$elapsed" -le 2 ]] || fail "TERM-resistant probe blocked diagnostics for ${elapsed}s"

run_case logs logs
[[ "$status" -eq 0 ]] || fail "all-service log capture failed: $output"
assert_call $'CALL\tdocker-compose\tlogs'

run_case logs logs master
[[ "$status" -eq 0 ]] || fail "single-service log capture failed: $output"
assert_call $'CALL\tdocker-compose\tlogs\tmaster'

echo "Docker helper tests passed"
