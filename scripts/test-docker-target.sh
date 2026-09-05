#!/bin/bash
# tests Docker target resolution and test-environment lifecycle ownership.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MONOTONIC_PYTHON="$(command -v python3)"
REAL_SLEEP="$(command -v sleep)"
export REAL_SLEEP
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT

cat > "$TEMP_DIR/docker" <<'MOCK'
#!/bin/bash
set -euo pipefail

if [[ "$#" -eq 3 && "$1" == info && "$2" == --format &&
    "$3" == '{{.Architecture}}' ]]; then
    if [[ -n "${DOCKER_INFO_CALLS:-}" ]]; then
        printf 'info\n' >> "$DOCKER_INFO_CALLS"
    fi
    if [[ "${FAKE_DOCKER_FAIL:-}" == 1 ]]; then
        printf 'docker-inspection-stdout'
        printf 'docker-inspection-stderr\n' >&2
        exit "${FAKE_DOCKER_STATUS:-37}"
    fi
    if [[ "${FAKE_DOCKER_CRLF:-}" == 1 ]]; then
        printf '%s\r\n' "${FAKE_DOCKER_ARCH:-amd64}"
    else
        printf '%s\n' "${FAKE_DOCKER_ARCH:-amd64}"
    fi
    exit 0
fi

if [[ "${1:-}" == exec ]]; then
    exit 0
fi

if [[ "${1:-}" == compose && "${2:-}" == version ]]; then
    exit "${FAKE_DOCKER_COMPOSE_VERSION_STATUS:-2}"
fi

if [[ "${1:-}" == compose && "${FAKE_DOCKER_COMPOSE_VERSION_STATUS:-2}" == 0 ]]; then
    shift
    printf 'v2:%s|%s\n' "${RCP_DOCKER_TARGET-UNSET}" "$*" >> "$COMPOSE_CALLS"
    exit 0
fi

printf 'unexpected fake docker arguments: %s\n' "$*" >&2
exit 2
MOCK

cat > "$TEMP_DIR/cargo" <<'MOCK'
#!/bin/bash
set -euo pipefail

printf '%s|%s\n' "${CARGO_BUILD_TARGET-UNSET}" "$*" >> "$CARGO_CALLS"
case " $* " in
    *' build '*)
        if [[ "${FAKE_CARGO_BUILD_STATUS:-0}" -ne 0 ]]; then
            printf 'cargo-build-stdout\n'
            printf 'cargo-build-stderr\n' >&2
            exit "$FAKE_CARGO_BUILD_STATUS"
        fi
        ;;
    *' nextest '*)
        if [[ "${FAKE_CARGO_TEST_STATUS:-0}" -ne 0 ]]; then
            printf 'cargo-test-stdout\n'
            printf 'cargo-test-stderr\n' >&2
            exit "$FAKE_CARGO_TEST_STATUS"
        fi
        ;;
esac
MOCK

cat > "$TEMP_DIR/compose" <<'MOCK'
#!/bin/bash
set -euo pipefail

operation="${1:-none}"
printf '%s|%s\n' "${RCP_DOCKER_TARGET-UNSET}" "$*" >> "$COMPOSE_CALLS"
printf 'compose-%s-stdout\n' "$operation"
printf 'compose-%s-stderr\n' "$operation" >&2
if [[ "$operation" == logs && "${FAKE_COMPOSE_HANG_ON_FOLLOW:-}" == 1 &&
    " $* " == *' -f '* ]]; then
    while :; do
        sleep 1
    done
fi
operation_upper="$(printf '%s' "$operation" | tr '[:lower:]' '[:upper:]')"
status_name="FAKE_COMPOSE_${operation_upper}_STATUS"
exit "${!status_name:-0}"
MOCK

cat > "$TEMP_DIR/docker-compose" <<'MOCK'
#!/bin/bash
set -euo pipefail

printf 'legacy:%s|%s\n' "${RCP_DOCKER_TARGET-UNSET}" "$*" >> "$COMPOSE_CALLS"
MOCK

cat > "$TEMP_DIR/wait-for-signal" <<'MOCK'
#!/bin/bash
set -euo pipefail

if [[ "${SIGNAL_CHILD_IGNORE_TERM:-0}" == 1 ]]; then
    trap '' TERM
elif [[ -n "${SIGNAL_CHILD_COOPERATIVE_DELAY:-}" ]]; then
    trap 'sleep "$SIGNAL_CHILD_COOPERATIVE_DELAY"; exit 0' TERM
fi
signal_child_group="$(ps -o pgid= -p "$$" | tr -d '[:space:]')"
if ! kill -0 -- "-$signal_child_group" 2>/dev/null; then
    printf 'lifecycle child %s cannot reach process group %s\n' \
        "$$" "$signal_child_group" >&2
    exit 88
fi
if [[ -n "${SIGNAL_CHILD_PID_FILE:-}" ]]; then
    printf '%s\n' "$$" > "$SIGNAL_CHILD_PID_FILE"
fi
if [[ -n "${SIGNAL_CHILD_GROUP_FILE:-}" ]]; then
    printf '%s\n' "$signal_child_group" > "$SIGNAL_CHILD_GROUP_FILE"
fi
printf 'ready\n' > "$SIGNAL_READY_FILE"
while :; do
    sleep 1
done
MOCK

cat > "$TEMP_DIR/leader-exits-with-descendant" <<'MOCK'
#!/bin/bash
set -euo pipefail

(
    if [[ "$DESCENDANT_SIGNAL_MODE" == resistant ]]; then
        trap '' TERM
    else
        trap 'exit 0' TERM
    fi
    printf 'ready\n' > "$DESCENDANT_READY_FILE"
    while :; do
        sleep 1
    done
) &
descendant_pid=$!
printf '%s\n' "$$" > "$DESCENDANT_LEADER_PID_FILE"
printf '%s\n' "$descendant_pid" > "$DESCENDANT_PID_FILE"
for _ in {1..100}; do
    [[ -s "$DESCENDANT_READY_FILE" ]] && break
    sleep 0.01
done
ps -o pgid= -p "$descendant_pid" | tr -d '[:space:]' \
    > "$DESCENDANT_GROUP_FILE"
printf '\n' >> "$DESCENDANT_GROUP_FILE"
exit 0
MOCK

cat > "$TEMP_DIR/setsid" <<'MOCK'
#!/bin/bash
set -euo pipefail

printf 'setsid\n' >> "$LAUNCHER_FALLBACK_CALLS"
printf 'external setsid must not be required\n' >&2
exit 127
MOCK

cat > "$TEMP_DIR/python3" <<'MOCK'
#!/bin/bash
set -euo pipefail

printf 'python3\n' >> "$LAUNCHER_FALLBACK_CALLS"
printf 'external python3 must not be required\n' >&2
exit 127
MOCK

cat > "$TEMP_DIR/sleep" <<'MOCK'
#!/bin/bash
set -euo pipefail

if [[ -n "${SLEEP_CALLS:-}" ]]; then
    printf '%s\n' "${1:-}" >> "$SLEEP_CALLS"
fi
if [[ -n "${FAIL_SLEEP_DURATION:-}" &&
    "${1:-}" == "$FAIL_SLEEP_DURATION" ]]; then
    exit 97
fi
if [[ "${1:-}" == 5 && -n "${WATCHDOG_SLEEP_PID_FILE:-}" ]]; then
    printf '%s\n' "$PPID" > "$WATCHDOG_TIMER_PID_FILE"
    printf '%s\n' "$$" > "$WATCHDOG_SLEEP_PID_FILE"
    ps -o pgid= -p "$$" | tr -d '[:space:]' > "$WATCHDOG_GROUP_FILE"
    printf '\n' >> "$WATCHDOG_GROUP_FILE"
fi
exec "$REAL_SLEEP" "$@"
MOCK

cat > "$TEMP_DIR/non-executable-command" <<'MOCK'
#!/bin/bash
exit 0
MOCK
chmod 0644 "$TEMP_DIR/non-executable-command"

chmod +x "$TEMP_DIR/docker" "$TEMP_DIR/cargo" "$TEMP_DIR/compose" \
    "$TEMP_DIR/docker-compose" "$TEMP_DIR/wait-for-signal" "$TEMP_DIR/setsid" \
    "$TEMP_DIR/python3" "$TEMP_DIR/sleep" \
    "$TEMP_DIR/leader-exits-with-descendant"

FAILED=0
fail() {
    echo -e "${RED}FAIL: $*${NC}"
    FAILED=1
}

assert_equals() { # $1 = description, $2 = expected, $3 = actual
    if [[ "$2" != "$3" ]]; then
        fail "$1: expected '$2', got '$3'"
    fi
}

assert_contains() { # $1 = description, $2 = needle, $3 = file
    if ! grep -Fq -- "$2" "$3"; then
        fail "$1: '$2' not found in $(cat "$3")"
    fi
}

assert_not_contains() { # $1 = description, $2 = needle, $3 = file
    if grep -Fq -- "$2" "$3"; then
        fail "$1: unexpectedly found '$2' in $(cat "$3")"
    fi
}

assert_file_equals() { # $1 = description, $2 = expected file, $3 = actual file
    if ! cmp -s "$2" "$3"; then
        fail "$1: expected '$(cat "$2")', got '$(cat "$3")'"
    fi
}

monotonic_millis() {
    "$MONOTONIC_PYTHON" -c \
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

wait_for_process_group_to_stop() { # $1 = process group ID, $2 = timeout in milliseconds
    local group="$1"
    local timeout_millis="$2"
    local deadline
    deadline=$(($(monotonic_millis) + timeout_millis))
    while process_group_has_live_members "$group"; do
        if [[ "$(monotonic_millis)" -ge "$deadline" ]]; then
            return 1
        fi
        "$REAL_SLEEP" 0.05
    done
}

assert_no_group_action_after_reap() { # $1 = event log, $2 = description
    local event_log="$1"
    local description="$2"
    if ! awk -F: '
        $1 == "group" { signaled[$2] = 1 }
        $1 == "reap" && signaled[$2] { observed = 1 }
        END { exit !observed }
    ' "$event_log"; then
        fail "$description did not record a group operation before its matching reap: $(cat "$event_log")"
    fi
    if grep -Fq 'unanchored:' "$event_log"; then
        fail "$description used an unanchored process-group number: $(cat "$event_log")"
    fi
    if ! awk -F: '
        $1 == "reap" { reaped[$2] = 1 }
        $1 == "group" && reaped[$2] {
            print "group operation after reap for PID " $2
            exit 1
        }
    ' "$event_log"; then
        fail "$description reused a PID as a process group after reap: $(cat "$event_log")"
    fi
}

run_resolver() {
    set +e
    "$@" > "$TEMP_DIR/stdout" 2> "$TEMP_DIR/stderr"
    RESOLVER_STATUS=$?
    set -e
}

check_target() { # $1 = Docker architecture, $2 = expected Rust target
    : > "$TEMP_DIR/docker-info-calls"
    run_resolver env -u RCP_DOCKER_TARGET -u DOCKER_DEFAULT_PLATFORM \
        "DOCKER=$TEMP_DIR/docker" \
        "DOCKER_INFO_CALLS=$TEMP_DIR/docker-info-calls" \
        "FAKE_DOCKER_ARCH=$1" \
        "$REPO_ROOT/scripts/docker-target.sh"
    assert_equals "Docker architecture $1 status" 0 "$RESOLVER_STATUS"
    assert_equals "Docker architecture $1 target" "$2" "$(cat "$TEMP_DIR/stdout")"
    assert_equals "Docker architecture $1 query count" 1 \
        "$(wc -l < "$TEMP_DIR/docker-info-calls")"
}

check_default_platform_precedes_internal_target() {
    : > "$TEMP_DIR/docker-info-calls"
    run_resolver env \
        RCP_DOCKER_TARGET=aarch64-unknown-linux-musl \
        DOCKER_DEFAULT_PLATFORM=linux/amd64 \
        "DOCKER=$TEMP_DIR/docker" \
        FAKE_DOCKER_FAIL=1 \
        "DOCKER_INFO_CALLS=$TEMP_DIR/docker-info-calls" \
        "$REPO_ROOT/scripts/docker-target.sh"
    assert_equals "DOCKER_DEFAULT_PLATFORM precedence status" 0 "$RESOLVER_STATUS"
    assert_equals "DOCKER_DEFAULT_PLATFORM precedence target" \
        x86_64-unknown-linux-musl "$(cat "$TEMP_DIR/stdout")"
    assert_equals "DOCKER_DEFAULT_PLATFORM avoids inspection" 0 \
        "$(wc -l < "$TEMP_DIR/docker-info-calls")"
}

check_default_platform_variant() { # $1 = Docker platform
    local platform="$1"

    : > "$TEMP_DIR/docker-info-calls"
    run_resolver env -u RCP_DOCKER_TARGET \
        "DOCKER_DEFAULT_PLATFORM=$platform" \
        "DOCKER=$TEMP_DIR/docker" \
        FAKE_DOCKER_FAIL=1 \
        "DOCKER_INFO_CALLS=$TEMP_DIR/docker-info-calls" \
        "$REPO_ROOT/scripts/docker-target.sh"
    assert_equals "Docker platform $platform status" 0 "$RESOLVER_STATUS"
    assert_equals "Docker platform $platform target" \
        x86_64-unknown-linux-musl "$(cat "$TEMP_DIR/stdout")"
    assert_equals "Docker platform $platform avoids inspection" 0 \
        "$(wc -l < "$TEMP_DIR/docker-info-calls")"
}

check_unsupported_default_platform() { # $1 = Docker platform
    local platform="$1"

    run_resolver env -u RCP_DOCKER_TARGET \
        "DOCKER_DEFAULT_PLATFORM=$platform" \
        "DOCKER=$TEMP_DIR/docker" \
        "$REPO_ROOT/scripts/docker-target.sh"
    assert_equals "unsupported Docker platform $platform status" 1 "$RESOLVER_STATUS"
    assert_contains "unsupported Docker platform $platform diagnostic" \
        "unsupported Docker platform: $platform" "$TEMP_DIR/stderr"
}

check_crlf_architecture_is_normalized() {
    run_resolver env -u DOCKER_DEFAULT_PLATFORM \
        RCP_DOCKER_TARGET=x86_64-unknown-linux-musl \
        "DOCKER=$TEMP_DIR/docker" \
        FAKE_DOCKER_ARCH=arm64 \
        FAKE_DOCKER_CRLF=1 \
        "$REPO_ROOT/scripts/docker-target.sh"
    assert_equals "CRLF Docker architecture status" 0 "$RESOLVER_STATUS"
    assert_equals "CRLF Docker architecture target" \
        aarch64-unknown-linux-musl "$(cat "$TEMP_DIR/stdout")"
}

check_inspection_failure_preserves_process_result() {
    run_resolver env -u RCP_DOCKER_TARGET -u DOCKER_DEFAULT_PLATFORM \
        "DOCKER=$TEMP_DIR/docker" \
        FAKE_DOCKER_FAIL=1 \
        FAKE_DOCKER_STATUS=37 \
        "$REPO_ROOT/scripts/docker-target.sh"
    assert_equals "failed Docker inspection status" 37 "$RESOLVER_STATUS"
    assert_equals "failed Docker inspection stdout" docker-inspection-stdout \
        "$(cat "$TEMP_DIR/stdout")"
    assert_contains "failed Docker inspection stderr" docker-inspection-stderr \
        "$TEMP_DIR/stderr"
}

base_test_env() {
    env -u DOCKER_DEFAULT_PLATFORM \
        -u RCP_DOCKER_LIFECYCLE_SIGNAL_GRACE_SECONDS \
        "PATH=$TEMP_DIR:$PATH" \
        "DOCKER=$TEMP_DIR/docker" \
        "CARGO=$TEMP_DIR/cargo" \
        "RCP_DOCKER_COMPOSE=$TEMP_DIR/compose" \
        "DOCKER_INFO_CALLS=$TEMP_DIR/docker-info-calls" \
        "CARGO_CALLS=$TEMP_DIR/cargo-calls" \
        "COMPOSE_CALLS=$TEMP_DIR/compose-calls" \
        "LAUNCHER_FALLBACK_CALLS=$TEMP_DIR/launcher-fallback-calls" \
        RCP_DOCKER_START_DELAY=0 \
        "$@"
}

reset_process_calls() {
    : > "$TEMP_DIR/docker-info-calls"
    : > "$TEMP_DIR/cargo-calls"
    : > "$TEMP_DIR/compose-calls"
    : > "$TEMP_DIR/launcher-fallback-calls"
}

run_process() {
    reset_process_calls
    set +e
    base_test_env "$@" > "$TEMP_DIR/stdout" 2> "$TEMP_DIR/stderr"
    PROCESS_STATUS=$?
    set -e
}

check_setup_resolves_once_and_threads_target() {
    run_process env \
        RCP_DOCKER_TARGET=x86_64-unknown-linux-musl \
        FAKE_DOCKER_ARCH=arm64 \
        just --justfile "$REPO_ROOT/justfile" --working-directory "$REPO_ROOT" docker-up
    assert_equals "Docker setup status" 0 "$PROCESS_STATUS"
    assert_equals "Docker setup architecture query count" 1 \
        "$(wc -l < "$TEMP_DIR/docker-info-calls")"
    assert_contains "Docker payload build target" \
        'aarch64-unknown-linux-musl|build --workspace' "$TEMP_DIR/cargo-calls"
    assert_contains "Docker Compose validation target" \
        'aarch64-unknown-linux-musl|config --quiet' "$TEMP_DIR/compose-calls"
    assert_contains "Docker Compose startup target" \
        'aarch64-unknown-linux-musl|up -d' "$TEMP_DIR/compose-calls"
    assert_not_contains "stale target is not threaded into Compose" \
        'x86_64-unknown-linux-musl|' "$TEMP_DIR/compose-calls"
}

check_restart_uses_single_setup_owner() {
    run_process env \
        RCP_DOCKER_TARGET=stale-internal-value \
        FAKE_DOCKER_ARCH=arm64 \
        "$REPO_ROOT/tests/docker/test-helpers.sh" restart
    assert_equals "Docker restart status" 0 "$PROCESS_STATUS"
    assert_equals "Docker restart architecture query count" 1 \
        "$(wc -l < "$TEMP_DIR/docker-info-calls")"
    assert_contains "Docker restart tears down without discovery" \
        'x86_64-unknown-linux-musl|down' "$TEMP_DIR/compose-calls"
    assert_contains "Docker restart threads setup target" \
        'aarch64-unknown-linux-musl|up -d' "$TEMP_DIR/compose-calls"
}

check_setup_preserves_discovery_failure() {
    printf 'docker-inspection-stdout' > "$TEMP_DIR/expected-stdout"
    run_process env \
        RCP_DOCKER_TARGET=stale-internal-value \
        FAKE_DOCKER_FAIL=1 \
        FAKE_DOCKER_STATUS=37 \
        "$REPO_ROOT/tests/docker/test-helpers.sh" setup
    assert_equals "Docker setup preserves discovery status" 37 "$PROCESS_STATUS"
    assert_file_equals "Docker setup preserves discovery stdout bytes" \
        "$TEMP_DIR/expected-stdout" "$TEMP_DIR/stdout"
    assert_contains "Docker setup preserves discovery stderr" \
        docker-inspection-stderr "$TEMP_DIR/stderr"
}

check_existing_project_operation() { # $1 = helper op, $2 = Compose op, $3 = status
    local helper_operation="$1"
    local compose_operation="$2"
    local expected_status="$3"
    local compose_operation_upper
    compose_operation_upper="$(
        printf '%s' "$compose_operation" | tr '[:lower:]' '[:upper:]'
    )"
    run_process env \
        RCP_DOCKER_TARGET=stale-internal-value \
        FAKE_DOCKER_FAIL=1 \
        "FAKE_COMPOSE_${compose_operation_upper}_STATUS=$expected_status" \
        "$REPO_ROOT/tests/docker/test-helpers.sh" "$helper_operation"
    assert_equals "$helper_operation preserves Compose status" \
        "$expected_status" "$PROCESS_STATUS"
    assert_contains "$helper_operation preserves Compose stdout" \
        "compose-$compose_operation-stdout" "$TEMP_DIR/stdout"
    assert_contains "$helper_operation preserves Compose stderr" \
        "compose-$compose_operation-stderr" "$TEMP_DIR/stderr"
    assert_equals "$helper_operation does not inspect Docker architecture" 0 \
        "$(wc -l < "$TEMP_DIR/docker-info-calls")"
    assert_contains "$helper_operation supplies harmless Compose interpolation" \
        "x86_64-unknown-linux-musl|$compose_operation" "$TEMP_DIR/compose-calls"
    assert_not_contains "$helper_operation ignores stale internal target" \
        'stale-internal-value|' "$TEMP_DIR/compose-calls"
}

check_compose_command_selection() { # $1 = v2 probe status, $2 = expected command marker
    run_process env -u RCP_DOCKER_COMPOSE \
        RCP_DOCKER_TARGET=stale-internal-value \
        "FAKE_DOCKER_COMPOSE_VERSION_STATUS=$1" \
        "$REPO_ROOT/tests/docker/test-helpers.sh" status
    assert_equals "Compose command selection status" 0 "$PROCESS_STATUS"
    assert_contains "Compose command selection" \
        "$2:x86_64-unknown-linux-musl|ps" "$TEMP_DIR/compose-calls"
}

check_interactive_logs_follow() {
    run_process "$REPO_ROOT/tests/docker/test-helpers.sh" logs
    assert_equals "interactive Docker logs status" 0 "$PROCESS_STATUS"
    assert_contains "interactive Docker logs follow" \
        'x86_64-unknown-linux-musl|logs -f' "$TEMP_DIR/compose-calls"
}

check_diagnostic_logs_return_without_following() {
    run_process env \
        FAKE_COMPOSE_HANG_ON_FOLLOW=1 \
        timeout 2 \
        just --justfile "$REPO_ROOT/justfile" --working-directory "$REPO_ROOT" \
        docker-logs-once
    assert_equals "workflow diagnostic logs return" 0 "$PROCESS_STATUS"
    assert_contains "workflow diagnostic logs invoke Compose" \
        'x86_64-unknown-linux-musl|logs' "$TEMP_DIR/compose-calls"
    assert_not_contains "workflow diagnostic logs do not follow" \
        'logs -f' "$TEMP_DIR/compose-calls"
}

check_lifecycle() { # $1 = recipe, $2 = Cargo test status, $3 = expect down
    local recipe="$1"
    local test_status="$2"
    local expect_down="$3"
    run_process env \
        RCP_DOCKER_TARGET=stale-internal-value \
        FAKE_DOCKER_ARCH=arm64 \
        "FAKE_CARGO_TEST_STATUS=$test_status" \
        just --justfile "$REPO_ROOT/justfile" --working-directory "$REPO_ROOT" "$recipe"
    assert_equals "$recipe preserves test failure" "$test_status" "$PROCESS_STATUS"
    if [[ "$expect_down" == yes ]]; then
        assert_equals "$recipe cleanup count after test failure" 1 \
            "$(grep -Fc '|down' "$TEMP_DIR/compose-calls" || true)"
    else
        assert_not_contains "$recipe intentionally keeps containers" \
            '|down' "$TEMP_DIR/compose-calls"
    fi
}

check_lifecycle_requires_command() { # $1 = lifecycle command
    local lifecycle_command="$1"

    run_process "$REPO_ROOT/tests/docker/test-helpers.sh" "$lifecycle_command"
    assert_equals "$lifecycle_command without a command status" 2 "$PROCESS_STATUS"
    assert_contains "$lifecycle_command without a command diagnostic" \
        'lifecycle requires a command' "$TEMP_DIR/stderr"
    assert_equals "$lifecycle_command without a command skips Docker discovery" 0 \
        "$(wc -l < "$TEMP_DIR/docker-info-calls")"
    assert_equals "$lifecycle_command without a command skips Cargo" "" \
        "$(cat "$TEMP_DIR/cargo-calls")"
    assert_equals "$lifecycle_command without a command skips Compose" "" \
        "$(cat "$TEMP_DIR/compose-calls")"
}

check_lifecycle_poll_interval() {
    local sleep_calls="$TEMP_DIR/lifecycle-sleep-calls"
    local reduced_poll_count
    : > "$sleep_calls"
    run_process env \
        "SLEEP_CALLS=$sleep_calls" \
        "$REPO_ROOT/tests/docker/test-helpers.sh" lifecycle "$REAL_SLEEP" 0.35
    assert_equals "reduced-poll lifecycle status" 0 "$PROCESS_STATUS"
    reduced_poll_count=$(grep -Fxc 0.1 "$sleep_calls" || true)
    if [[ "$reduced_poll_count" -lt 2 ]]; then
        fail "lifecycle did not use 100ms polling while its command ran: $(tr '\n' ';' < "$sleep_calls")"
    fi
}

check_setup_failure_lifecycle() { # $1 = recipe, $2 = expect down
    local recipe="$1"
    local expect_down="$2"
    run_process env \
        RCP_DOCKER_TARGET=stale-internal-value \
        FAKE_DOCKER_ARCH=arm64 \
        FAKE_CARGO_BUILD_STATUS=41 \
        just --justfile "$REPO_ROOT/justfile" --working-directory "$REPO_ROOT" "$recipe"
    assert_equals "$recipe preserves setup failure" 41 "$PROCESS_STATUS"
    if [[ "$expect_down" == yes ]]; then
        assert_equals "$recipe cleanup count after setup failure" 1 \
            "$(grep -Fc '|down' "$TEMP_DIR/compose-calls" || true)"
    else
        assert_not_contains "$recipe does not tear down in keep mode" \
            '|down' "$TEMP_DIR/compose-calls"
    fi
}

check_discovery_failure_lifecycle() { # $1 = recipe, $2 = expect down
    local recipe="$1"
    local expect_down="$2"
    run_process env \
        RCP_DOCKER_TARGET=stale-internal-value \
        FAKE_DOCKER_FAIL=1 \
        FAKE_DOCKER_STATUS=37 \
        just --justfile "$REPO_ROOT/justfile" --working-directory "$REPO_ROOT" "$recipe"
    assert_equals "$recipe preserves discovery failure" 37 "$PROCESS_STATUS"
    assert_contains "$recipe preserves discovery stdout" \
        docker-inspection-stdout "$TEMP_DIR/stdout"
    assert_contains "$recipe preserves discovery stderr" \
        docker-inspection-stderr "$TEMP_DIR/stderr"
    if [[ "$expect_down" == yes ]]; then
        assert_equals "$recipe cleanup count after discovery failure" 1 \
            "$(grep -Fc '|down' "$TEMP_DIR/compose-calls" || true)"
    else
        assert_not_contains "$recipe does not tear down discovery failure in keep mode" \
            '|down' "$TEMP_DIR/compose-calls"
    fi
}

check_sigterm_lifecycle_cleanup() {
    local lifecycle_pid
    local monitor_was_enabled=no
    local signal_status
    local down_count
    local watchdog_timer_pid
    local watchdog_sleep_pid
    local watchdog_group
    local watchdog_cleanup_deadline
    local started_at
    local elapsed
    local ready=0

    reset_process_calls
    : > "$TEMP_DIR/signal-stdout"
    : > "$TEMP_DIR/signal-stderr"
    rm -f "$TEMP_DIR/signal-ready" "$TEMP_DIR/signal-child-group" \
        "$TEMP_DIR/watchdog-timer-pid" "$TEMP_DIR/watchdog-sleep-pid" \
        "$TEMP_DIR/watchdog-group"
    if [[ "$-" == *m* ]]; then
        monitor_was_enabled=yes
    else
        set -m
    fi
    env -u DOCKER_DEFAULT_PLATFORM \
        -u RCP_DOCKER_LIFECYCLE_SIGNAL_GRACE_SECONDS \
        "PATH=$TEMP_DIR:$PATH" \
        "DOCKER=$TEMP_DIR/docker" \
        "CARGO=$TEMP_DIR/cargo" \
        "RCP_DOCKER_COMPOSE=$TEMP_DIR/compose" \
        "DOCKER_INFO_CALLS=$TEMP_DIR/docker-info-calls" \
        "CARGO_CALLS=$TEMP_DIR/cargo-calls" \
        "COMPOSE_CALLS=$TEMP_DIR/compose-calls" \
        "LAUNCHER_FALLBACK_CALLS=$TEMP_DIR/launcher-fallback-calls" \
        RCP_DOCKER_START_DELAY=0 \
        SIGNAL_CHILD_COOPERATIVE_DELAY=0.5 \
        "SIGNAL_READY_FILE=$TEMP_DIR/signal-ready" \
        "SIGNAL_CHILD_GROUP_FILE=$TEMP_DIR/signal-child-group" \
        "WATCHDOG_TIMER_PID_FILE=$TEMP_DIR/watchdog-timer-pid" \
        "WATCHDOG_SLEEP_PID_FILE=$TEMP_DIR/watchdog-sleep-pid" \
        "WATCHDOG_GROUP_FILE=$TEMP_DIR/watchdog-group" \
        "$REPO_ROOT/tests/docker/test-helpers.sh" lifecycle \
        "$TEMP_DIR/wait-for-signal" \
        > "$TEMP_DIR/signal-stdout" 2> "$TEMP_DIR/signal-stderr" &
    lifecycle_pid=$!
    if [[ "$monitor_was_enabled" != yes ]]; then
        set +m
    fi

    for _ in {1..100}; do
        if [[ -s "$TEMP_DIR/signal-ready" ]]; then
            ready=1
            break
        fi
        if ! kill -0 "$lifecycle_pid" 2> /dev/null; then
            break
        fi
        sleep 0.05
    done
    if [[ "$ready" -ne 1 ]]; then
        kill -TERM -- "-$lifecycle_pid" 2> /dev/null || true
        wait "$lifecycle_pid" 2> /dev/null || true
        fail "SIGTERM lifecycle did not reach its interruptible command: $(cat "$TEMP_DIR/signal-stderr")"
        return
    fi

    started_at="$(monotonic_millis)"
    kill -TERM -- "-$lifecycle_pid"
    set +e
    wait "$lifecycle_pid"
    signal_status=$?
    set -e
    elapsed=$(($(monotonic_millis) - started_at))
    down_count="$(grep -Fc '|down' "$TEMP_DIR/compose-calls" || true)"
    assert_equals "SIGTERM lifecycle status" 143 "$signal_status"
    assert_equals "SIGTERM lifecycle cleanup count" 1 "$down_count"
    if [[ "$elapsed" -ge 2500 ]]; then
        fail "cooperative SIGTERM lifecycle took ${elapsed}ms instead of cancelling its five-second grace timer"
    fi
    if [[ ! -s "$TEMP_DIR/watchdog-timer-pid" ||
        ! -s "$TEMP_DIR/watchdog-sleep-pid" ||
        ! -s "$TEMP_DIR/watchdog-group" ]]; then
        fail "cooperative SIGTERM did not record its escalation watchdog group"
        return
    fi
    watchdog_timer_pid="$(<"$TEMP_DIR/watchdog-timer-pid")"
    watchdog_sleep_pid="$(<"$TEMP_DIR/watchdog-sleep-pid")"
    watchdog_group="$(<"$TEMP_DIR/watchdog-group")"
    assert_equals "watchdog timer owns its process group" \
        "$watchdog_timer_pid" "$watchdog_group"
    if [[ -s "$TEMP_DIR/signal-child-group" ]]; then
        if [[ "$watchdog_group" == "$(<"$TEMP_DIR/signal-child-group")" ]]; then
            fail "watchdog shared the lifecycle child process group $watchdog_group"
        fi
    else
        fail "cooperative lifecycle child did not record its process group"
    fi
    watchdog_cleanup_deadline=$(($(monotonic_millis) + 1500))
    while process_is_live "$watchdog_timer_pid" ||
        process_is_live "$watchdog_sleep_pid" ||
        process_group_has_live_members "$watchdog_group"; do
        if [[ "$(monotonic_millis)" -ge "$watchdog_cleanup_deadline" ]]; then
            kill -KILL -- "-$watchdog_group" 2>/dev/null || true
            fail "cooperative SIGTERM leaked watchdog leader $watchdog_timer_pid, sleep $watchdog_sleep_pid, or group $watchdog_group"
            break
        fi
        sleep 0.05
    done
}

check_parent_only_sigterm_lifecycle_cleanup() {
    local lifecycle_pid
    local child_group
    local signal_status
    local down_count
    local started_at
    local elapsed
    local ready=0
    local exited=0

    reset_process_calls
    : > "$TEMP_DIR/parent-signal-stdout"
    : > "$TEMP_DIR/parent-signal-stderr"
    rm -f "$TEMP_DIR/parent-signal-ready" "$TEMP_DIR/parent-signal-child-pid" \
        "$TEMP_DIR/parent-signal-child-group"
    env -u DOCKER_DEFAULT_PLATFORM \
        "PATH=$TEMP_DIR:$PATH" \
        "DOCKER=$TEMP_DIR/docker" \
        "CARGO=$TEMP_DIR/cargo" \
        "RCP_DOCKER_COMPOSE=$TEMP_DIR/compose" \
        "DOCKER_INFO_CALLS=$TEMP_DIR/docker-info-calls" \
        "CARGO_CALLS=$TEMP_DIR/cargo-calls" \
        "COMPOSE_CALLS=$TEMP_DIR/compose-calls" \
        "LAUNCHER_FALLBACK_CALLS=$TEMP_DIR/launcher-fallback-calls" \
        RCP_DOCKER_START_DELAY=0 \
        RCP_DOCKER_LIFECYCLE_SIGNAL_GRACE_SECONDS=1 \
        SIGNAL_CHILD_IGNORE_TERM=1 \
        "SIGNAL_READY_FILE=$TEMP_DIR/parent-signal-ready" \
        "SIGNAL_CHILD_PID_FILE=$TEMP_DIR/parent-signal-child-pid" \
        "SIGNAL_CHILD_GROUP_FILE=$TEMP_DIR/parent-signal-child-group" \
        "$REPO_ROOT/tests/docker/test-helpers.sh" lifecycle \
        "$TEMP_DIR/wait-for-signal" \
        > "$TEMP_DIR/parent-signal-stdout" 2> "$TEMP_DIR/parent-signal-stderr" &
    lifecycle_pid=$!

    for _ in {1..100}; do
        if [[ -s "$TEMP_DIR/parent-signal-ready" &&
            -s "$TEMP_DIR/parent-signal-child-pid" &&
            -s "$TEMP_DIR/parent-signal-child-group" ]]; then
            ready=1
            break
        fi
        if ! kill -0 "$lifecycle_pid" 2> /dev/null; then
            break
        fi
        sleep 0.05
    done
    if [[ "$ready" -ne 1 ]]; then
        kill -KILL "$lifecycle_pid" 2> /dev/null || true
        wait "$lifecycle_pid" 2> /dev/null || true
        fail "parent-only SIGTERM lifecycle did not reach its child: $(cat "$TEMP_DIR/parent-signal-stderr")"
        return
    fi

    child_group="$(<"$TEMP_DIR/parent-signal-child-group")"
    if [[ ! "$child_group" =~ ^[0-9]+$ ]]; then
        fail "lifecycle child did not record a numeric process group"
    fi
    started_at="$(monotonic_millis)"
    kill -TERM "$lifecycle_pid"
    for _ in {1..80}; do
        if ! kill -0 "$lifecycle_pid" 2> /dev/null; then
            exited=1
            break
        fi
        sleep 0.05
    done
    if [[ "$exited" -ne 1 ]]; then
        kill -KILL -- "-$child_group" 2> /dev/null || true
        kill -KILL "$lifecycle_pid" 2> /dev/null || true
        wait "$lifecycle_pid" 2> /dev/null || true
        fail "parent-only SIGTERM did not terminate the active lifecycle child"
        return
    fi

    set +e
    wait "$lifecycle_pid"
    signal_status=$?
    set -e
    elapsed=$(($(monotonic_millis) - started_at))
    down_count="$(grep -Fc '|down' "$TEMP_DIR/compose-calls" || true)"
    assert_equals "parent-only SIGTERM lifecycle status" 143 "$signal_status"
    assert_equals "parent-only SIGTERM lifecycle cleanup count" 1 "$down_count"
    if ! wait_for_process_group_to_stop "$child_group" 1500; then
        kill -KILL -- "-$child_group" 2> /dev/null || true
        fail "parent-only SIGTERM left the active lifecycle process group running"
    fi
    if [[ "$elapsed" -lt 700 || "$elapsed" -ge 3000 ]]; then
        fail "TERM-resistant lifecycle used ${elapsed}ms instead of its one-second escalation grace"
    fi
}

check_sigterm_during_lifecycle_child_registration() {
    local child_group

cat > "$TEMP_DIR/signal-before-registration.bash" <<'MOCK'
set -T
signal_registration_count=0
signal_before_lifecycle_pid_registration() {
    if [[ "$BASH_COMMAND" != 'LIFECYCLE_ACTIVE_PID=$!' ]]; then
        return
    fi
    signal_registration_count=$((signal_registration_count + 1))
    if [[ "$signal_registration_count" -ne 2 ]]; then
        return
    fi
    printf '%s\n' "$!" > "$SIGNAL_PENDING_PGID_FILE"
    trap - DEBUG
    kill -TERM "$$"
}
trap signal_before_lifecycle_pid_registration DEBUG
MOCK

    run_process env \
        BASH_ENV="$TEMP_DIR/signal-before-registration.bash" \
        RCP_DOCKER_LIFECYCLE_SIGNAL_GRACE_SECONDS=0.1 \
        SIGNAL_PENDING_PGID_FILE="$TEMP_DIR/signal-pending-pgid" \
        SIGNAL_READY_FILE="$TEMP_DIR/signal-pending-ready" \
        "$REPO_ROOT/tests/docker/test-helpers.sh" lifecycle \
        "$TEMP_DIR/wait-for-signal"

    assert_equals "SIGTERM during lifecycle child registration status" \
        143 "$PROCESS_STATUS"
    assert_equals "SIGTERM during lifecycle child registration cleanup count" 1 \
        "$(grep -Fc '|down' "$TEMP_DIR/compose-calls" || true)"
    if [[ ! -s "$TEMP_DIR/signal-pending-pgid" ]]; then
        fail "registration-race signal hook did not observe the unregistered child"
        return
    fi
    child_group="$(<"$TEMP_DIR/signal-pending-pgid")"
    if ! wait_for_process_group_to_stop "$child_group" 1500; then
        kill -KILL -- "-$child_group" 2> /dev/null || true
        fail "SIGTERM during registration left the lifecycle process group running"
    fi
}

check_lifecycle_exec_failure_is_reaped() { # $1 = path, $2 = status, $3 = diagnostic
    local command_path="$1"
    local expected_status="$2"
    local diagnostic="$3"
    local failed_child_pid

    cat > "$TEMP_DIR/record-lifecycle-child.bash" <<'MOCK'
set -T
recorded_lifecycle_children=0
record_lifecycle_child() {
    if [[ "$BASH_COMMAND" != 'LIFECYCLE_ACTIVE_PID=$!' ]]; then
        return
    fi
    recorded_lifecycle_children=$((recorded_lifecycle_children + 1))
    if [[ "$recorded_lifecycle_children" -eq 2 ]]; then
        printf '%s\n' "$!" > "$FAILED_LIFECYCLE_CHILD_PID_FILE"
        trap - DEBUG
    fi
}
trap record_lifecycle_child DEBUG
MOCK

    rm -f "$TEMP_DIR/failed-lifecycle-child-pid"
    run_process env \
        BASH_ENV="$TEMP_DIR/record-lifecycle-child.bash" \
        FAILED_LIFECYCLE_CHILD_PID_FILE="$TEMP_DIR/failed-lifecycle-child-pid" \
        "$REPO_ROOT/tests/docker/test-helpers.sh" lifecycle "$command_path"

    assert_equals "lifecycle exec failure status for $command_path" \
        "$expected_status" "$PROCESS_STATUS"
    assert_contains "lifecycle exec failure path for $command_path" \
        "$command_path" "$TEMP_DIR/stderr"
    assert_contains "lifecycle exec failure diagnostic for $command_path" \
        "$diagnostic" "$TEMP_DIR/stderr"
    assert_equals "lifecycle exec failure cleanup count for $command_path" 1 \
        "$(grep -Fc '|down' "$TEMP_DIR/compose-calls" || true)"
    assert_equals "lifecycle exec failure uses no external launcher" "" \
        "$(cat "$TEMP_DIR/launcher-fallback-calls")"
    if [[ ! -s "$TEMP_DIR/failed-lifecycle-child-pid" ]]; then
        fail "exec-failure hook did not record the lifecycle child"
        return
    fi
    failed_child_pid="$(<"$TEMP_DIR/failed-lifecycle-child-pid")"
    if ! wait_for_process_group_to_stop "$failed_child_pid" 1500; then
        kill -KILL -- "-$failed_child_pid" 2>/dev/null || true
        kill -KILL "$failed_child_pid" 2>/dev/null || true
        fail "exec failure left lifecycle PID or process group $failed_child_pid running"
    fi
}

check_lifecycle_uses_no_external_launcher() {
    local leaked_temp_file

    mkdir -p "$TEMP_DIR/lifecycle-tmp"
    run_process env TMPDIR="$TEMP_DIR/lifecycle-tmp" \
        "$REPO_ROOT/tests/docker/test-helpers.sh" lifecycle /bin/true

    assert_equals "launcher-free lifecycle status" 0 "$PROCESS_STATUS"
    assert_equals "launcher-free lifecycle cleanup count" 1 \
        "$(grep -Fc '|down' "$TEMP_DIR/compose-calls" || true)"
    assert_equals "lifecycle does not fall back to setsid or Python" "" \
        "$(cat "$TEMP_DIR/launcher-fallback-calls")"
    leaked_temp_file="$(find "$TEMP_DIR/lifecycle-tmp" -mindepth 1 -maxdepth 1 -print -quit)"
    assert_equals "lifecycle leaves no temporary launcher state" "" "$leaked_temp_file"
}

check_monitor_mode_restored() { # $1 = initial monitor mode (on/off)
    local initial_mode="$1"

    cat > "$TEMP_DIR/check-monitor-mode.bash" <<'MOCK'
set -T
if [[ -z "${MONITOR_TEST_ROOT_PID:-}" ]]; then
    export MONITOR_TEST_ROOT_PID="$$"
    if [[ "$MONITOR_TEST_INITIAL" == on ]]; then
        set -m
    else
        set +m
    fi
fi
if [[ "$$" != "$MONITOR_TEST_ROOT_PID" ]]; then
    return
fi
check_monitor_mode_before_command_phase() {
    if [[ "$BASH_COMMAND" == *run_lifecycle_child* ]]; then
        printf '%s|%s\n' "$-" "$BASH_COMMAND" >> "$MONITOR_TEST_COMMANDS_FILE"
    fi
    if [[ "$BASH_COMMAND" != 'run_lifecycle_child "$REPO_ROOT" "$@"' ]]; then
        return
    fi
    if [[ "$-" == *m* ]]; then
        printf 'on\n' > "$MONITOR_TEST_OBSERVED_FILE"
        [[ "$MONITOR_TEST_INITIAL" == on ]] || exit 89
    else
        printf 'off\n' > "$MONITOR_TEST_OBSERVED_FILE"
        [[ "$MONITOR_TEST_INITIAL" == off ]] || exit 89
    fi
}
trap check_monitor_mode_before_command_phase DEBUG
MOCK

    rm -f "$TEMP_DIR/monitor-observed" "$TEMP_DIR/monitor-commands"
    run_process env \
        BASH_ENV="$TEMP_DIR/check-monitor-mode.bash" \
        MONITOR_TEST_INITIAL="$initial_mode" \
        MONITOR_TEST_OBSERVED_FILE="$TEMP_DIR/monitor-observed" \
        MONITOR_TEST_COMMANDS_FILE="$TEMP_DIR/monitor-commands" \
        "$REPO_ROOT/tests/docker/test-helpers.sh" lifecycle /bin/true

    assert_equals "lifecycle with monitor mode initially $initial_mode" 0 "$PROCESS_STATUS"
    if [[ ! -s "$TEMP_DIR/monitor-observed" ]]; then
        fail "monitor-mode hook did not observe the command phase: $(cat "$TEMP_DIR/monitor-commands" 2>/dev/null || true)"
    else
        assert_equals "monitor mode restored to $initial_mode" "$initial_mode" \
            "$(cat "$TEMP_DIR/monitor-observed")"
    fi
    assert_equals "monitor restoration cleanup count" 1 \
        "$(grep -Fc '|down' "$TEMP_DIR/compose-calls" || true)"
    assert_equals "monitor restoration uses no external launcher" "" \
        "$(cat "$TEMP_DIR/launcher-fallback-calls")"
}

clean_up_stuck_lifecycle_order_fixture() { # $1 = lifecycle PID
    local lifecycle_pid="$1"
    local group

    while IFS= read -r group; do
        if [[ "$group" =~ ^[0-9]+$ ]]; then
            kill -KILL -- "-$group" 2>/dev/null || true
        fi
    done < <(awk -F: '$1 == "group" && !seen[$2]++ { print $2 }' \
        "$TEMP_DIR/lifecycle-order")
    kill -KILL "$lifecycle_pid" 2>/dev/null || true
    wait "$lifecycle_pid" 2>/dev/null || true
}

run_lifecycle_group_order_case() { # $1 = description, $2 = failed sleep duration, $3/$4 = elapsed bounds
    local description="$1"
    local failed_sleep_duration="$2"
    local minimum_millis="$3"
    local maximum_millis="$4"
    local lifecycle_pid
    local lifecycle_status
    local started_at
    local elapsed
    local exit_deadline
    local ready=0

    reset_process_calls
    : > "$TEMP_DIR/lifecycle-order"
    rm -f "$TEMP_DIR/order-ready"
    env -u DOCKER_DEFAULT_PLATFORM \
        "PATH=$TEMP_DIR:$PATH" \
        "DOCKER=$TEMP_DIR/docker" \
        "CARGO=$TEMP_DIR/cargo" \
        "RCP_DOCKER_COMPOSE=$TEMP_DIR/compose" \
        "DOCKER_INFO_CALLS=$TEMP_DIR/docker-info-calls" \
        "CARGO_CALLS=$TEMP_DIR/cargo-calls" \
        "COMPOSE_CALLS=$TEMP_DIR/compose-calls" \
        "LAUNCHER_FALLBACK_CALLS=$TEMP_DIR/launcher-fallback-calls" \
        BASH_ENV="$TEMP_DIR/record-lifecycle-group-order.bash" \
        LIFECYCLE_ORDER_LOG="$TEMP_DIR/lifecycle-order" \
        FAIL_SLEEP_DURATION="$failed_sleep_duration" \
        RCP_DOCKER_START_DELAY=0 \
        RCP_DOCKER_LIFECYCLE_SIGNAL_GRACE_SECONDS=0.3 \
        SIGNAL_CHILD_IGNORE_TERM=1 \
        SIGNAL_READY_FILE="$TEMP_DIR/order-ready" \
        "$REPO_ROOT/tests/docker/test-helpers.sh" lifecycle \
        "$TEMP_DIR/wait-for-signal" \
        > "$TEMP_DIR/order-stdout" 2> "$TEMP_DIR/order-stderr" &
    lifecycle_pid=$!

    for _ in {1..100}; do
        if [[ -s "$TEMP_DIR/order-ready" ]]; then
            ready=1
            break
        fi
        process_is_live "$lifecycle_pid" || break
        "$REAL_SLEEP" 0.05
    done
    if [[ "$ready" -ne 1 ]]; then
        clean_up_stuck_lifecycle_order_fixture "$lifecycle_pid"
        fail "$description fixture did not become ready: $(cat "$TEMP_DIR/order-stderr")"
        return
    fi

    started_at="$(monotonic_millis)"
    kill -TERM "$lifecycle_pid"
    exit_deadline=$((started_at + maximum_millis + 500))
    while process_is_live "$lifecycle_pid"; do
        if [[ "$(monotonic_millis)" -ge "$exit_deadline" ]]; then
            clean_up_stuck_lifecycle_order_fixture "$lifecycle_pid"
            fail "$description fixture did not exit within its bounded grace"
            return
        fi
        "$REAL_SLEEP" 0.02
    done
    set +e
    wait "$lifecycle_pid"
    lifecycle_status=$?
    set -e
    elapsed=$(($(monotonic_millis) - started_at))
    assert_equals "$description fixture status" 143 "$lifecycle_status"
    if [[ "$elapsed" -lt "$minimum_millis" ||
        "$elapsed" -ge "$maximum_millis" ]]; then
        fail "$description lifecycle grace took ${elapsed}ms, expected ${minimum_millis}-${maximum_millis}ms"
    fi
    assert_no_group_action_after_reap "$TEMP_DIR/lifecycle-order" \
        "$description cleanup"
}

check_lifecycle_group_actions_precede_reap() {
    cat > "$TEMP_DIR/record-lifecycle-group-order.bash" <<'MOCK'
if [[ -n "${LIFECYCLE_ORDER_ROOT_PID:-}" ]]; then
    return
fi
export LIFECYCLE_ORDER_ROOT_PID="$$"
kill() {
    local after_separator=no
    local argument
    local group_pid
    for argument in "$@"; do
        if [[ "$after_separator" == yes && "$argument" == -[0-9]* ]]; then
            group_pid="${argument#-}"
            printf 'group:%s\n' "$group_pid" >> "$LIFECYCLE_ORDER_LOG"
            if ! builtin kill -0 "$group_pid" 2>/dev/null; then
                printf 'unanchored:%s\n' "$group_pid" >> "$LIFECYCLE_ORDER_LOG"
            fi
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
        printf 'reap:%s\n' "$1" >> "$LIFECYCLE_ORDER_LOG"
    fi
    return "$wait_status"
}
sleep() {
    if [[ "${1:-}" == 0.3 ]]; then
        return 97
    fi
    command sleep "$@"
}
MOCK

    run_lifecycle_group_order_case "sleep-function shadow" '' 200 1500
    run_lifecycle_group_order_case "failed sleep command" 0.3 0 1500
}

check_leader_exit_descendant_grace() { # $1 = mode, $2/$3 = elapsed bounds
    local mode="$1"
    local minimum_millis="$2"
    local maximum_millis="$3"
    local started_at
    local elapsed
    local descendant_pid
    local descendant_group
    local cleanup_deadline

    rm -f "$TEMP_DIR/descendant-ready" "$TEMP_DIR/descendant-pid" \
        "$TEMP_DIR/descendant-group" "$TEMP_DIR/descendant-leader-pid"
    started_at="$(monotonic_millis)"
    run_process env \
        RCP_DOCKER_LIFECYCLE_SIGNAL_GRACE_SECONDS=2 \
        DESCENDANT_SIGNAL_MODE="$mode" \
        DESCENDANT_READY_FILE="$TEMP_DIR/descendant-ready" \
        DESCENDANT_PID_FILE="$TEMP_DIR/descendant-pid" \
        DESCENDANT_GROUP_FILE="$TEMP_DIR/descendant-group" \
        DESCENDANT_LEADER_PID_FILE="$TEMP_DIR/descendant-leader-pid" \
        "$REPO_ROOT/tests/docker/test-helpers.sh" lifecycle \
        "$TEMP_DIR/leader-exits-with-descendant"
    elapsed=$(($(monotonic_millis) - started_at))

    assert_equals "$mode descendant lifecycle status" 0 "$PROCESS_STATUS"
    assert_equals "$mode descendant lifecycle cleanup count" 1 \
        "$(grep -Fc '|down' "$TEMP_DIR/compose-calls" || true)"
    if [[ ! -s "$TEMP_DIR/descendant-pid" ||
        ! -s "$TEMP_DIR/descendant-group" ]]; then
        fail "$mode leader-exit fixture did not record its descendant"
        return
    fi
    descendant_pid="$(cat "$TEMP_DIR/descendant-pid")"
    descendant_group="$(cat "$TEMP_DIR/descendant-group")"
    if [[ "$elapsed" -lt "$minimum_millis" || "$elapsed" -ge "$maximum_millis" ]]; then
        fail "$mode leader-exit cleanup took ${elapsed}ms, expected ${minimum_millis}-${maximum_millis}ms"
    fi

    cleanup_deadline=$(($(monotonic_millis) + 1500))
    while process_is_live "$descendant_pid" ||
        process_group_has_live_members "$descendant_group"; do
        if [[ "$(monotonic_millis)" -ge "$cleanup_deadline" ]]; then
            kill -KILL -- "-$descendant_group" 2>/dev/null || true
            fail "$mode leader-exit cleanup leaked descendant $descendant_pid or group $descendant_group"
            break
        fi
        sleep 0.05
    done
}

echo "🔍 Testing Docker target and lifecycle behavior..."

check_target amd64 x86_64-unknown-linux-musl
check_target x86_64 x86_64-unknown-linux-musl
check_target arm64 aarch64-unknown-linux-musl
check_target aarch64 aarch64-unknown-linux-musl
check_default_platform_precedes_internal_target
check_default_platform_variant linux/amd64/v1
check_default_platform_variant linux/amd64/v2
check_default_platform_variant linux/amd64/v3
check_default_platform_variant linux/amd64/v4
check_unsupported_default_platform linux/amd64/v0
check_unsupported_default_platform linux/amd64/vnext
check_crlf_architecture_is_normalized
check_inspection_failure_preserves_process_result
check_setup_resolves_once_and_threads_target
check_restart_uses_single_setup_owner
check_setup_preserves_discovery_failure
check_existing_project_operation status ps 19
check_existing_project_operation logs logs 20
check_existing_project_operation stop down 21
check_compose_command_selection 0 v2
check_compose_command_selection 2 legacy
check_interactive_logs_follow
check_diagnostic_logs_return_without_following
check_lifecycle docker-test 42 yes
check_lifecycle docker-chaos-test 42 yes
check_lifecycle docker-test-keep 42 no
check_lifecycle docker-chaos-test-keep 42 no
check_lifecycle_requires_command lifecycle
check_lifecycle_requires_command lifecycle-keep
check_lifecycle_poll_interval
check_setup_failure_lifecycle docker-test yes
check_setup_failure_lifecycle docker-test-keep no
check_discovery_failure_lifecycle docker-test yes
check_discovery_failure_lifecycle docker-test-keep no
check_sigterm_lifecycle_cleanup
check_parent_only_sigterm_lifecycle_cleanup
check_sigterm_during_lifecycle_child_registration
check_lifecycle_uses_no_external_launcher
check_monitor_mode_restored off
check_monitor_mode_restored on
check_lifecycle_group_actions_precede_reap
check_leader_exit_descendant_grace cooperative 0 1500
check_leader_exit_descendant_grace resistant 1500 4000
check_lifecycle_exec_failure_is_reaped \
    "$TEMP_DIR/no-such-lifecycle-command" 127 "No such file or directory"
check_lifecycle_exec_failure_is_reaped \
    "$TEMP_DIR/non-executable-command" 126 "Permission denied"

if [[ "$FAILED" -ne 0 ]]; then
    exit 1
fi
echo -e "${GREEN}✅ Docker target and lifecycle tests passed!${NC}"
