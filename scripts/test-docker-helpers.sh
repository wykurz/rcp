#!/bin/bash

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
helper="$repo_root/tests/docker/test-helpers.sh"
monotonic_python="$(command -v python3)"
real_sleep="$(command -v sleep)"
test_dir="$(mktemp -d)"
trap 'rm -rf "$test_dir"' EXIT

fake_bin="$test_dir/bin"
state_dir="$test_dir/state"
mkdir -p "$fake_bin" "$state_dir"

cat >"$fake_bin/docker" <<'FAKE'
#!/bin/bash
set -euo pipefail

{
    printf 'CALL'
    printf '\t%s' "$@"
    printf '\n'
} >>"$RCP_DOCKER_TEST_DOCKER_LOG"

if [[ "$#" -eq 3 && "$1" == info && "$2" == --format &&
    "$3" == '{{.Architecture}}' ]]; then
    printf 'resolve\n' >>"$RCP_DOCKER_TEST_EVENT_LOG"
    printf 'info\n' >>"$RCP_DOCKER_TEST_INFO_LOG"
    printf '%s\n' "${FAKE_DOCKER_ARCH:-arm64}"
    exit 0
fi

prefix="exec -u testuser rcp-test-master ssh -o BatchMode=yes -o ConnectTimeout=5"
for host in host-a host-b; do
    if [[ "$*" == "$prefix $host hostname" ]]; then
        printf 'probe:%s\n' "$host" >>"$RCP_DOCKER_TEST_EVENT_LOG"
        counter="$RCP_DOCKER_TEST_STATE/$host"
        count=0
        if [[ -f "$counter" ]]; then
            count="$(<"$counter")"
        fi
        ((count += 1))
        printf '%s\n' "$count" >"$counter"

        case "$RCP_DOCKER_TEST_SCENARIO" in
            slow-probe)
                sleep 6
                exit 1
                ;;
            term-resistant-probe)
                trap '' TERM
                probe_group="$(ps -o pgid= -p "$$" | tr -d '[:space:]')"
                printf '%s\n' "$probe_group" >"$RCP_DOCKER_TEST_PROBE_PID_FILE"
                sleep 6 &
                descendant_pid=$!
                printf '%s\n' "$descendant_pid" \
                    >"$RCP_DOCKER_TEST_PROBE_DESCENDANT_PID_FILE"
                if kill -0 -- "-$probe_group" 2>/dev/null; then
                    printf 'yes\n' >"$RCP_DOCKER_TEST_PROBE_GROUP_FILE"
                else
                    printf 'no\n' >"$RCP_DOCKER_TEST_PROBE_GROUP_FILE"
                fi
                descendant_group="$(ps -o pgid= -p "$descendant_pid" | tr -d '[:space:]')"
                printf '%s\n' "$descendant_group" \
                    >"$RCP_DOCKER_TEST_PROBE_DESCENDANT_GROUP_FILE"
                wait "$descendant_pid"
                exit 1
                ;;
            leader-exits-first | leader-succeeds-first)
                if [[ "$host" != host-a ]]; then
                    exit 0
                fi
                probe_group="$(ps -o pgid= -p "$$" | tr -d '[:space:]')"
                printf '%s\n' "$probe_group" >"$RCP_DOCKER_TEST_PROBE_PID_FILE"
                (
                    trap '' TERM
                    printf 'ready\n' >"$RCP_DOCKER_TEST_PROBE_DESCENDANT_READY_FILE"
                    sleep 6 &
                    wait
                ) &
                descendant_pid=$!
                printf '%s\n' "$descendant_pid" \
                    >"$RCP_DOCKER_TEST_PROBE_DESCENDANT_PID_FILE"
                if kill -0 -- "-$probe_group" 2>/dev/null; then
                    printf 'yes\n' >"$RCP_DOCKER_TEST_PROBE_GROUP_FILE"
                else
                    printf 'no\n' >"$RCP_DOCKER_TEST_PROBE_GROUP_FILE"
                fi
                descendant_group="$(ps -o pgid= -p "$descendant_pid" | tr -d '[:space:]')"
                printf '%s\n' "$descendant_group" \
                    >"$RCP_DOCKER_TEST_PROBE_DESCENDANT_GROUP_FILE"
                for _ in {1..100}; do
                    [[ -s "$RCP_DOCKER_TEST_PROBE_DESCENDANT_READY_FILE" ]] && break
                    sleep 0.01
                done
                sleep 0.2
                if [[ "$RCP_DOCKER_TEST_SCENARIO" == leader-succeeds-first ]]; then
                    exit 0
                fi
                exit 1
                ;;
            immediate-success)
                exit 0
                ;;
            eventual-success)
                if [[ "$host" == host-a && "$count" -ge 2 ]]; then
                    exit 0
                fi
                if [[ "$host" == host-b && "$count" -ge 3 ]]; then
                    exit 0
                fi
                ;;
        esac
        exit 1
    fi
done

echo "unexpected fake docker invocation: $*" >&2
exit 32
FAKE

cat >"$fake_bin/cargo" <<'FAKE'
#!/bin/bash
set -euo pipefail

printf '%s|%s\n' "${CARGO_BUILD_TARGET-UNSET}" "$*" >>"$RCP_DOCKER_TEST_CARGO_LOG"
printf 'payload:%s|%s\n' "${CARGO_BUILD_TARGET-UNSET}" "$*" \
    >>"$RCP_DOCKER_TEST_EVENT_LOG"
FAKE

cat >"$fake_bin/compose" <<'FAKE'
#!/bin/bash
set -euo pipefail

printf '%s|%s\n' "${RCP_DOCKER_TARGET-UNSET}" "$*" >>"$RCP_DOCKER_TEST_COMPOSE_LOG"
printf 'compose:%s|%s\n' "${RCP_DOCKER_TARGET-UNSET}" "$*" \
    >>"$RCP_DOCKER_TEST_EVENT_LOG"
operation="${1:-none}"
if [[ "$operation" == logs ]]; then
    printf 'finite diagnostic logs\n'
    if [[ " $* " == *' -f '* ]]; then
        while :; do
            sleep 1
        done
    fi
fi
operation_upper="$(printf '%s' "$operation" | tr '[:lower:]' '[:upper:]')"
status_name="FAKE_COMPOSE_${operation_upper}_STATUS"
exit "${!status_name:-0}"
FAKE

chmod +x "$fake_bin/docker" "$fake_bin/cargo" "$fake_bin/compose"

export RCP_DOCKER_TEST_DOCKER_LOG="$test_dir/docker.log"
export RCP_DOCKER_TEST_INFO_LOG="$test_dir/docker-info.log"
export RCP_DOCKER_TEST_CARGO_LOG="$test_dir/cargo.log"
export RCP_DOCKER_TEST_COMPOSE_LOG="$test_dir/compose.log"
export RCP_DOCKER_TEST_EVENT_LOG="$test_dir/events.log"
export RCP_DOCKER_TEST_STATE="$state_dir"
export RCP_DOCKER_TEST_PROBE_PID_FILE="$state_dir/probe-pid"
export RCP_DOCKER_TEST_PROBE_DESCENDANT_PID_FILE="$state_dir/probe-descendant-pid"
export RCP_DOCKER_TEST_PROBE_GROUP_FILE="$state_dir/probe-group"
export RCP_DOCKER_TEST_PROBE_DESCENDANT_GROUP_FILE="$state_dir/probe-descendant-group"
export RCP_DOCKER_TEST_PROBE_DESCENDANT_READY_FILE="$state_dir/probe-descendant-ready"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_contains() { # $1 = file, $2 = expected text
    local file="$1"
    local expected="$2"
    grep -Fq -- "$expected" "$file" ||
        fail "expected $file to contain '$expected'; got: $(<"$file")"
}

assert_not_contains() { # $1 = file, $2 = unexpected text
    local file="$1"
    local unexpected="$2"
    if grep -Fq -- "$unexpected" "$file"; then
        fail "expected $file not to contain '$unexpected'; got: $(<"$file")"
    fi
}

assert_count() { # $1 = expected count, $2 = text, $3 = file
    local expected="$1"
    local text="$2"
    local file="$3"
    local actual
    actual="$(grep -Fc -- "$text" "$file" || true)"
    [[ "$actual" -eq "$expected" ]] ||
        fail "expected $expected occurrences of '$text' in $file, got $actual: $(<"$file")"
}

assert_file_equals() { # $1 = expected file, $2 = actual file
    local expected="$1"
    local actual="$2"
    cmp -s "$expected" "$actual" ||
        fail "expected '$(<"$expected")', got '$(<"$actual")'"
}

monotonic_millis() {
    "$monotonic_python" -c \
        'import time; print(time.monotonic_ns() // 1_000_000)'
}

process_is_live() { # $1 = PID
    local pid="$1"
    local state
    kill -0 "$pid" 2>/dev/null || return 1
    state="$(ps -o stat= -p "$pid" 2>/dev/null || true)"
    [[ "$state" != Z* ]]
}

process_group_has_live_members() { # $1 = process group ID
    local group="$1"
    ps -eo pgid=,stat= 2>/dev/null | awk -v group="$group" '
        $1 == group && $2 !~ /^Z/ { found = 1 }
        END { exit !found }
    '
}

assert_no_group_action_after_reap() { # $1 = event log, $2 = description
    local event_log="$1"
    local description="$2"
    if ! awk -F: '
        $1 == "group" { signaled[$2] = 1 }
        $1 == "reap" && signaled[$2] { observed = 1 }
        END { exit !observed }
    ' "$event_log"; then
        fail "$description did not record a group operation before its matching reap: $(<"$event_log")"
    fi
    if ! awk -F: '
        $1 == "reap" { reaped[$2] = 1 }
        $1 == "group" && reaped[$2] {
            print "group operation after reap for PID " $2
            exit 1
        }
    ' "$event_log"; then
        fail "$description reused a PID as a process group after reap: $(<"$event_log")"
    fi
}

reset_case() {
    : >"$RCP_DOCKER_TEST_DOCKER_LOG"
    : >"$RCP_DOCKER_TEST_INFO_LOG"
    : >"$RCP_DOCKER_TEST_CARGO_LOG"
    : >"$RCP_DOCKER_TEST_COMPOSE_LOG"
    : >"$RCP_DOCKER_TEST_EVENT_LOG"
    rm -f "$state_dir/host-a" "$state_dir/host-b" \
        "$RCP_DOCKER_TEST_PROBE_PID_FILE" \
        "$RCP_DOCKER_TEST_PROBE_DESCENDANT_PID_FILE" \
        "$RCP_DOCKER_TEST_PROBE_GROUP_FILE" \
        "$RCP_DOCKER_TEST_PROBE_DESCENDANT_GROUP_FILE" \
        "$RCP_DOCKER_TEST_PROBE_DESCENDANT_READY_FILE"
}

assert_probe_group_stopped() { # $1 = scenario description
    local description="$1"
    local probe_pid
    local descendant_pid
    local cleanup_deadline
    [[ -s "$RCP_DOCKER_TEST_PROBE_PID_FILE" ]] ||
        fail "$description did not record its probe leader"
    [[ -s "$RCP_DOCKER_TEST_PROBE_DESCENDANT_PID_FILE" ]] ||
        fail "$description did not record its probe descendant"
    probe_pid="$(<"$RCP_DOCKER_TEST_PROBE_PID_FILE")"
    descendant_pid="$(<"$RCP_DOCKER_TEST_PROBE_DESCENDANT_PID_FILE")"
    [[ "$(<"$RCP_DOCKER_TEST_PROBE_GROUP_FILE")" == yes ]] ||
        fail "$description leader $probe_pid did not own group $probe_pid"
    [[ "$(<"$RCP_DOCKER_TEST_PROBE_DESCENDANT_GROUP_FILE")" == "$probe_pid" ]] ||
        fail "$description descendant $descendant_pid did not join group $probe_pid"

    cleanup_deadline=$(($(monotonic_millis) + 1500))
    while process_is_live "$probe_pid" ||
        process_is_live "$descendant_pid" ||
        process_group_has_live_members "$probe_pid"; do
        if [[ "$(monotonic_millis)" -ge "$cleanup_deadline" ]]; then
            kill -KILL "$descendant_pid" 2>/dev/null || true
            kill -KILL -- "-$probe_pid" 2>/dev/null || true
            fail "$description leaked leader $probe_pid, descendant $descendant_pid, or group $probe_pid"
        fi
        sleep 0.05
    done
}

check_lifecycle_cancellation_during_probe() {
    local lifecycle_pid
    local lifecycle_status
    local elapsed
    local ready=0
    local started_at

    reset_case
    : >"$test_dir/cancel-probe-stdout"
    : >"$test_dir/cancel-probe-stderr"
    env -u DOCKER_DEFAULT_PLATFORM \
        "PATH=$fake_bin:$PATH" \
        "DOCKER=$fake_bin/docker" \
        "CARGO=$fake_bin/cargo" \
        "RCP_DOCKER_COMPOSE=$fake_bin/compose" \
        RCP_DOCKER_TARGET=x86_64-unknown-linux-musl \
        RCP_DOCKER_TEST_SCENARIO=term-resistant-probe \
        RCP_DOCKER_SSH_READY_TIMEOUT_SECONDS=10 \
        RCP_DOCKER_SSH_READY_POLL_SECONDS=0 \
        "$helper" lifecycle /bin/true \
        >"$test_dir/cancel-probe-stdout" 2>"$test_dir/cancel-probe-stderr" &
    lifecycle_pid=$!

    for _ in {1..200}; do
        if [[ -s "$RCP_DOCKER_TEST_PROBE_PID_FILE" &&
            -s "$RCP_DOCKER_TEST_PROBE_DESCENDANT_PID_FILE" &&
            -s "$RCP_DOCKER_TEST_PROBE_DESCENDANT_GROUP_FILE" ]]; then
            ready=1
            break
        fi
        kill -0 "$lifecycle_pid" 2>/dev/null || break
        sleep 0.01
    done
    if [[ "$ready" -ne 1 ]]; then
        kill -KILL "$lifecycle_pid" 2>/dev/null || true
        wait "$lifecycle_pid" 2>/dev/null || true
        fail "lifecycle cancellation probe did not start: $(<"$test_dir/cancel-probe-stderr")"
    fi

    started_at="$(monotonic_millis)"
    kill -TERM "$lifecycle_pid"
    set +e
    wait "$lifecycle_pid"
    lifecycle_status=$?
    set -e
    elapsed=$(($(monotonic_millis) - started_at))
    [[ "$lifecycle_status" -eq 143 ]] ||
        fail "probe cancellation lifecycle returned $lifecycle_status"
    [[ "$elapsed" -lt 3000 ]] ||
        fail "probe cancellation lifecycle took ${elapsed}ms"
    assert_count 1 '|down' "$RCP_DOCKER_TEST_COMPOSE_LOG"
    assert_probe_group_stopped "lifecycle cancellation during readiness"
}

run_case() { # $1 = scenario, remaining arguments = command
    local scenario="$1"
    shift
    reset_case
    set +e
    output="$(
        env -u DOCKER_DEFAULT_PLATFORM \
            "PATH=$fake_bin:$PATH" \
            "DOCKER=$fake_bin/docker" \
            "CARGO=$fake_bin/cargo" \
            "RCP_DOCKER_COMPOSE=$fake_bin/compose" \
            RCP_DOCKER_TARGET=x86_64-unknown-linux-musl \
            RCP_DOCKER_TEST_SCENARIO="$scenario" \
            "$@" 2>&1
    )"
    status=$?
    set -e
}

check_probe_state_restored() { # $1 = initial monitor mode (on/off)
    local initial_mode="$1"

    cat >"$test_dir/check-probe-state.bash" <<'BASH_ENV_FIXTURE'
if [[ -n "${PROBE_STATE_ROOT_PID:-}" ]]; then
    return
fi

export PROBE_STATE_ROOT_PID="$$"
if [[ "$PROBE_STATE_INITIAL_MONITOR" == on ]]; then
    set -m
else
    set +m
fi
probe_state_original_int() { :; }
probe_state_original_term() { :; }
trap probe_state_original_int INT
trap probe_state_original_term TERM
set -T
probe_state_before_command() {
    if [[ "$$" != "$PROBE_STATE_ROOT_PID" ||
        "$BASH_COMMAND" != 'info "SSH connectivity to $host verified"' ]]; then
        return
    fi
    if [[ "$-" == *m* ]]; then
        printf 'on\n' >"$PROBE_STATE_MONITOR_FILE"
    else
        printf 'off\n' >"$PROBE_STATE_MONITOR_FILE"
    fi
    trap -p INT >"$PROBE_STATE_INT_TRAP_FILE"
    trap -p TERM >"$PROBE_STATE_TERM_TRAP_FILE"
    trap - DEBUG
}
trap probe_state_before_command DEBUG
BASH_ENV_FIXTURE

    rm -f "$state_dir/probe-monitor" "$state_dir/probe-int-trap" \
        "$state_dir/probe-term-trap"
    run_case eventual-success env \
        BASH_ENV="$test_dir/check-probe-state.bash" \
        PROBE_STATE_INITIAL_MONITOR="$initial_mode" \
        PROBE_STATE_MONITOR_FILE="$state_dir/probe-monitor" \
        PROBE_STATE_INT_TRAP_FILE="$state_dir/probe-int-trap" \
        PROBE_STATE_TERM_TRAP_FILE="$state_dir/probe-term-trap" \
        "$helper" setup

    [[ "$status" -eq 0 ]] ||
        fail "readiness with monitor mode initially $initial_mode failed with $status: $output"
    [[ -s "$state_dir/probe-monitor" && -s "$state_dir/probe-int-trap" &&
        -s "$state_dir/probe-term-trap" ]] ||
        fail "readiness state hook did not observe the post-probe command"
    [[ "$(<"$state_dir/probe-monitor")" == "$initial_mode" ]] ||
        fail "readiness changed monitor mode from $initial_mode to $(<"$state_dir/probe-monitor")"
    assert_contains "$state_dir/probe-int-trap" probe_state_original_int
    assert_contains "$state_dir/probe-term-trap" probe_state_original_term
}

check_probe_group_actions_precede_reap() {
    cat >"$test_dir/record-probe-group-order.bash" <<'BASH_ENV_FIXTURE'
if [[ -n "${PROBE_ORDER_ROOT_PID:-}" ]]; then
    return
fi
export PROBE_ORDER_ROOT_PID="$$"
kill() {
    local after_separator=no
    local argument
    for argument in "$@"; do
        if [[ "$after_separator" == yes && "$argument" == -[0-9]* ]]; then
            printf 'group:%s\n' "${argument#-}" >>"$PROBE_ORDER_LOG"
        fi
        if [[ "$argument" == -- ]]; then
            after_separator=yes
        fi
    done
    builtin kill "$@"
}
wait() {
    local wait_status
    builtin wait "$@"
    wait_status=$?
    if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
        printf 'reap:%s\n' "$1" >>"$PROBE_ORDER_LOG"
    fi
    return "$wait_status"
}
BASH_ENV_FIXTURE

    : >"$state_dir/probe-order"
    run_case leader-succeeds-first env \
        BASH_ENV="$test_dir/record-probe-group-order.bash" \
        PROBE_ORDER_LOG="$state_dir/probe-order" \
        "$helper" setup
    [[ "$status" -eq 0 ]] ||
        fail "probe ordering fixture failed with $status: $output"
    assert_no_group_action_after_reap "$state_dir/probe-order" \
        "readiness cleanup"
}

check_immediate_probe_skips_escalation_delay() {
    cat >"$test_dir/record-probe-sleeps.bash" <<'BASH_ENV_FIXTURE'
if [[ -n "${PROBE_SLEEP_ROOT_PID:-}" ]]; then
    return
fi
export PROBE_SLEEP_ROOT_PID="$$"
kill() {
    if [[ "${1:-}" == -0 && "${2:-}" == -- && "${3:-}" == -[0-9]* ]]; then
        return 0
    fi
    builtin kill "$@"
}
sleep() {
    printf '%s\n' "$*" >>"$PROBE_SLEEP_LOG"
    "$PROBE_REAL_SLEEP" "$@"
}
BASH_ENV_FIXTURE

    : >"$state_dir/probe-sleeps"
    run_case immediate-success env \
        BASH_ENV="$test_dir/record-probe-sleeps.bash" \
        PROBE_REAL_SLEEP="$real_sleep" \
        PROBE_SLEEP_LOG="$state_dir/probe-sleeps" \
        "$helper" setup
    [[ "$status" -eq 0 ]] ||
        fail "immediate-success readiness failed with $status: $output"
    if grep -Fxq 0.1 "$state_dir/probe-sleeps"; then
        fail "immediate-success readiness used the 0.1s escalation delay: $(<"$state_dir/probe-sleeps")"
    fi
}

[[ -x "$helper" ]] || fail "tests/docker/test-helpers.sh is absent or not executable"

export RCP_DOCKER_SSH_READY_POLL_SECONDS=0
export RCP_DOCKER_SSH_READY_TIMEOUT_SECONDS=5
run_case eventual-success "$helper" setup
[[ "$status" -eq 0 ]] || fail "eventually ready SSH hosts failed with $status: $output"
[[ "$(<"$state_dir/host-a")" -eq 2 ]] || fail "host-a was not polled until ready"
[[ "$(<"$state_dir/host-b")" -eq 3 ]] || fail "host-b was not polled until ready"
assert_count 1 info "$RCP_DOCKER_TEST_INFO_LOG"
assert_contains "$RCP_DOCKER_TEST_CARGO_LOG" \
    'aarch64-unknown-linux-musl|build --workspace'
assert_contains "$RCP_DOCKER_TEST_COMPOSE_LOG" \
    'aarch64-unknown-linux-musl|config --quiet'
assert_contains "$RCP_DOCKER_TEST_COMPOSE_LOG" \
    'aarch64-unknown-linux-musl|up -d'
assert_not_contains "$RCP_DOCKER_TEST_COMPOSE_LOG" 'x86_64-unknown-linux-musl|'
assert_contains "$RCP_DOCKER_TEST_DOCKER_LOG" \
    $'CALL\texec\t-u\ttestuser\trcp-test-master\tssh\t-o\tBatchMode=yes\t-o\tConnectTimeout=5\thost-a\thostname'
assert_contains "$RCP_DOCKER_TEST_DOCKER_LOG" \
    $'CALL\texec\t-u\ttestuser\trcp-test-master\tssh\t-o\tBatchMode=yes\t-o\tConnectTimeout=5\thost-b\thostname'
check_probe_state_restored off
check_probe_state_restored on
check_immediate_probe_skips_escalation_delay
check_probe_group_actions_precede_reap

run_case eventual-success env DOCKER="$fake_bin/no-such-docker" "$helper" rebuild
[[ "$status" -eq 1 ]] || fail "rebuild without Docker returned $status: $output"
[[ "$output" == *'Docker is not installed or not in PATH'* ]] ||
    fail "rebuild preflight did not report the missing Docker command: $output"
assert_count 0 info "$RCP_DOCKER_TEST_INFO_LOG"
assert_count 0 'build --workspace' "$RCP_DOCKER_TEST_CARGO_LOG"
[[ ! -s "$RCP_DOCKER_TEST_COMPOSE_LOG" ]] ||
    fail "rebuild preflight invoked Compose: $(<"$RCP_DOCKER_TEST_COMPOSE_LOG")"

printf '%s\n' \
    resolve \
    'compose:aarch64-unknown-linux-musl|down' \
    'compose:aarch64-unknown-linux-musl|build --no-cache' \
    'payload:aarch64-unknown-linux-musl|build --workspace' \
    'compose:aarch64-unknown-linux-musl|config --quiet' \
    'compose:aarch64-unknown-linux-musl|up -d' \
    'probe:host-a' \
    'probe:host-b' \
    'probe:host-a' \
    'probe:host-b' \
    'probe:host-b' \
    >"$test_dir/expected-rebuild-events"
run_case eventual-success "$helper" rebuild
[[ "$status" -eq 0 ]] || fail "rebuild did not reach SSH readiness: $output"
assert_count 1 info "$RCP_DOCKER_TEST_INFO_LOG"
assert_file_equals "$test_dir/expected-rebuild-events" "$RCP_DOCKER_TEST_EVENT_LOG"
[[ "$(<"$state_dir/host-a")" -eq 2 ]] || fail "rebuild did not wait for host-a"
[[ "$(<"$state_dir/host-b")" -eq 3 ]] || fail "rebuild did not wait for host-b"

export RCP_DOCKER_SSH_READY_TIMEOUT_SECONDS=1
started_at="$(monotonic_millis)"
run_case never-ready env FAKE_COMPOSE_LOGS_STATUS=55 "$helper" setup
elapsed=$(($(monotonic_millis) - started_at))
[[ "$status" -eq 1 ]] || fail "readiness failure status was $status instead of 1: $output"
[[ "$elapsed" -lt 3000 ]] ||
    fail "one-second readiness budget stopped after ${elapsed}ms"
[[ "$output" == *'SSH connectivity did not become ready within 1s: host-a host-b'* ]] ||
    fail "readiness timeout did not name both hosts: $output"
assert_count 1 info "$RCP_DOCKER_TEST_INFO_LOG"
assert_contains "$RCP_DOCKER_TEST_COMPOSE_LOG" 'aarch64-unknown-linux-musl|logs'
assert_not_contains "$RCP_DOCKER_TEST_COMPOSE_LOG" 'logs -f'
assert_not_contains "$RCP_DOCKER_TEST_COMPOSE_LOG" '|down'
assert_not_contains "$RCP_DOCKER_TEST_COMPOSE_LOG" 'x86_64-unknown-linux-musl|'

started_at="$(monotonic_millis)"
run_case slow-probe timeout 5 "$helper" setup
elapsed=$(($(monotonic_millis) - started_at))
[[ "$status" -eq 1 ]] || fail "stalled SSH probe returned $status: $output"
[[ "$elapsed" -lt 3500 ]] ||
    fail "one-second stalled-probe budget took ${elapsed}ms"
assert_count 1 info "$RCP_DOCKER_TEST_INFO_LOG"
assert_contains "$RCP_DOCKER_TEST_COMPOSE_LOG" 'aarch64-unknown-linux-musl|logs'

started_at="$(monotonic_millis)"
run_case term-resistant-probe timeout 5 "$helper" setup
elapsed=$(($(monotonic_millis) - started_at))
[[ "$status" -eq 1 ]] || fail "TERM-resistant SSH probe returned $status: $output"
[[ "$elapsed" -lt 3500 ]] ||
    fail "TERM-resistant probe blocked diagnostics for ${elapsed}ms"
assert_count 1 info "$RCP_DOCKER_TEST_INFO_LOG"
assert_contains "$RCP_DOCKER_TEST_COMPOSE_LOG" 'aarch64-unknown-linux-musl|logs'
assert_probe_group_stopped "TERM-resistant SSH probe timeout"

check_lifecycle_cancellation_during_probe

run_case leader-exits-first env \
    RCP_DOCKER_SSH_READY_TIMEOUT_SECONDS=2 \
    RCP_DOCKER_SSH_READY_POLL_SECONDS=10 \
    "$helper" setup
[[ "$status" -eq 1 ]] || fail "early-exit SSH probe returned $status: $output"
assert_count 1 'probe:host-a' "$RCP_DOCKER_TEST_EVENT_LOG"
assert_count 1 'probe:host-b' "$RCP_DOCKER_TEST_EVENT_LOG"
assert_contains "$RCP_DOCKER_TEST_COMPOSE_LOG" 'aarch64-unknown-linux-musl|logs'
assert_probe_group_stopped "early-exit readiness probe"

run_case leader-succeeds-first env \
    RCP_DOCKER_SSH_READY_TIMEOUT_SECONDS=2 \
    RCP_DOCKER_SSH_READY_POLL_SECONDS=10 \
    "$helper" setup
[[ "$status" -eq 0 ]] || fail "early-success SSH probe returned $status: $output"
assert_count 1 'probe:host-a' "$RCP_DOCKER_TEST_EVENT_LOG"
assert_count 1 'probe:host-b' "$RCP_DOCKER_TEST_EVENT_LOG"
assert_probe_group_stopped "early-success readiness probe"

run_case never-ready "$helper" rebuild
[[ "$status" -eq 1 ]] || fail "rebuild readiness failure returned $status: $output"
assert_count 1 info "$RCP_DOCKER_TEST_INFO_LOG"
assert_count 1 'aarch64-unknown-linux-musl|build --workspace' \
    "$RCP_DOCKER_TEST_CARGO_LOG"
assert_count 1 'aarch64-unknown-linux-musl|down' "$RCP_DOCKER_TEST_COMPOSE_LOG"
assert_count 1 'aarch64-unknown-linux-musl|build --no-cache' \
    "$RCP_DOCKER_TEST_COMPOSE_LOG"
assert_count 1 'aarch64-unknown-linux-musl|config --quiet' \
    "$RCP_DOCKER_TEST_COMPOSE_LOG"
assert_count 1 'aarch64-unknown-linux-musl|up -d' "$RCP_DOCKER_TEST_COMPOSE_LOG"
assert_count 1 'aarch64-unknown-linux-musl|logs' "$RCP_DOCKER_TEST_COMPOSE_LOG"
assert_not_contains "$RCP_DOCKER_TEST_COMPOSE_LOG" 'logs -f'
assert_not_contains "$RCP_DOCKER_TEST_COMPOSE_LOG" 'x86_64-unknown-linux-musl|'

run_case never-ready env FAKE_COMPOSE_LOGS_STATUS=55 timeout 4 \
    "$helper" lifecycle /bin/true
[[ "$status" -eq 1 ]] || fail "readiness lifecycle returned $status: $output"
assert_count 1 info "$RCP_DOCKER_TEST_INFO_LOG"
assert_count 1 'aarch64-unknown-linux-musl|logs' "$RCP_DOCKER_TEST_COMPOSE_LOG"
assert_count 1 '|down' "$RCP_DOCKER_TEST_COMPOSE_LOG"
assert_not_contains "$RCP_DOCKER_TEST_COMPOSE_LOG" 'logs -f'

echo "Docker helper tests passed"
