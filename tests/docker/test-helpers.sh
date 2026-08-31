#!/usr/bin/env bash
# Helper script for RCP Docker test environment

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
cd "$SCRIPT_DIR"
DOCKER_BIN="${DOCKER:-docker}"
SLEEP_BIN="$(type -P sleep)"
COMPOSE_INTERPOLATION_TARGET=x86_64-unknown-linux-musl
LIFECYCLE_CLEAN_UP=no
LIFECYCLE_CLEANUP_DONE=0
LIFECYCLE_ACTIVE_PID=
LIFECYCLE_ACTIVE_STATUS_FILE=
LIFECYCLE_REGISTERING_CHILD=no
LIFECYCLE_TERMINATING_CHILD=no
LIFECYCLE_PENDING_SIGNAL=
LIFECYCLE_PENDING_SIGNAL_STATUS=

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # no color

info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

resolve_docker_target() { # $1 = result variable
    local result_variable="$1"
    local output
    local output_file
    local status
    output_file="$(mktemp)"
    set +e
    "$REPO_ROOT/scripts/docker-target.sh" > "$output_file"
    status=$?
    set -e
    if [[ "$status" -ne 0 ]]; then
        cat "$output_file"
        rm -f "$output_file"
        return "$status"
    fi
    output="$(cat "$output_file")"
    rm -f "$output_file"
    printf -v "$result_variable" '%s' "$output"
}

compose() {
    if [[ -n "${RCP_DOCKER_COMPOSE:-}" ]]; then
        "$RCP_DOCKER_COMPOSE" "$@"
    elif "$DOCKER_BIN" compose version &> /dev/null; then
        "$DOCKER_BIN" compose "$@"
    else
        docker-compose "$@"
    fi
}

compose_for_target() { # $1 = target, remaining arguments = Compose command
    local target="$1"
    shift
    RCP_DOCKER_TARGET="$target" compose "$@"
}

compose_existing() {
    RCP_DOCKER_TARGET="$COMPOSE_INTERPOLATION_TARGET" compose "$@"
}

# Check if Docker is available
check_docker() {
    if ! command -v "$DOCKER_BIN" &> /dev/null; then
        error "Docker is not installed or not in PATH"
        error "See README.md for installation instructions"
        exit 1
    fi
}

# build payload binaries for a resolved Docker platform.
build_payload() { # $1 = resolved target
    CARGO_BUILD_TARGET="$1" "$REPO_ROOT/scripts/cargo-host.sh" build --workspace
}

# resolve and build without starting a Compose project.
build() {
    local target
    check_docker
    resolve_docker_target target
    build_payload "$target"
}

# build and start with one target resolution owned by setup.
setup_project() { # $1 = setup mode (reuse/rebuild)
    local mode="$1"
    local target
    check_docker
    resolve_docker_target target

    if [[ "$mode" == rebuild ]]; then
        compose_for_target "$target" down
        compose_for_target "$target" build --no-cache
    fi

    build_payload "$target"
    start "$target"
}

setup() {
    setup_project reuse
}

probe_ssh_until() {
    local host="$1"
    local deadline="$2"
    local monitor_was_enabled=no
    local probe_pid=
    local probe_natural_completion=no
    local probe_reaped=no
    local probe_signal=
    local probe_signal_status=
    local probe_status=
    local probe_status_file=
    local saved_int_trap
    local saved_term_trap
    if [[ "$SECONDS" -ge "$deadline" ]]; then
        return 124
    fi

    saved_int_trap="$(trap -p INT)"
    saved_term_trap="$(trap -p TERM)"
    trap 'if [[ -z "$probe_signal_status" ]]; then probe_signal=INT; probe_signal_status=130; fi' INT
    trap 'if [[ -z "$probe_signal_status" ]]; then probe_signal=TERM; probe_signal_status=143; fi' TERM

    if ! probe_status_file="$(mktemp)"; then
        probe_status=1
    fi

    if [[ -z "$probe_status" && "$-" == *m* ]]; then
        monitor_was_enabled=yes
    fi
    if [[ -z "$probe_status" ]]; then
        set -m
        (
            set +m
            trap ':' INT TERM
            set +e
            "$DOCKER_BIN" exec -u testuser rcp-test-master \
                ssh -o BatchMode=yes -o ConnectTimeout=5 "$host" hostname \
                </dev/null &>/dev/null
            command_status=$?
            set -e
            printf '%s\n' "$command_status" >"$probe_status_file"
            # keep the group leader alive until its owner has made every group
            # signal decision. Ignored signals survive exec.
            trap '' INT TERM
            exec "$SLEEP_BIN" 2147483647
        ) &
        probe_pid=$!
        if [[ "$monitor_was_enabled" == yes ]]; then
            set -m
        else
            set +m
        fi
    fi

    while [[ -z "$probe_status" ]]; do
        if [[ -n "$probe_signal_status" ]]; then
            probe_status="$probe_signal_status"
            break
        fi
        if [[ "$SECONDS" -ge "$deadline" ]]; then
            probe_status=124
            break
        fi
        if [[ -s "$probe_status_file" ]]; then
            probe_status="$(<"$probe_status_file")"
            probe_natural_completion=yes
            break
        fi
        if ! kill -0 "$probe_pid" 2>/dev/null; then
            if wait "$probe_pid"; then
                probe_status=0
            else
                probe_status=$?
            fi
            probe_reaped=yes
            break
        fi
        sleep 0.01
    done

    if [[ -n "$probe_signal_status" ]]; then
        probe_status="$probe_signal_status"
        probe_natural_completion=no
    fi

    # signal traps only record intent, so another signal during cleanup cannot
    # re-enter it. The live supervisor is the ownership anchor for every group
    # operation; wait is deliberately last.
    if [[ -n "$probe_pid" && "$probe_reaped" != yes ]]; then
        kill -TERM -- "-$probe_pid" 2>/dev/null || true
        if [[ "$probe_natural_completion" != yes ]]; then
            sleep 0.1
        fi
        kill -KILL -- "-$probe_pid" 2>/dev/null || true
        wait "$probe_pid" 2>/dev/null || true
    fi
    rm -f "$probe_status_file"

    # a signal can arrive after the first status check while teardown waits for
    # the process group to disappear.
    if [[ -n "$probe_signal_status" ]]; then
        probe_status="$probe_signal_status"
    fi
    if [[ -n "$saved_int_trap" ]]; then
        eval "$saved_int_trap"
    else
        trap - INT
    fi
    if [[ -n "$saved_term_trap" ]]; then
        eval "$saved_term_trap"
    else
        trap - TERM
    fi

    if [[ -n "$probe_signal" ]]; then
        probe_status="$probe_signal_status"
        kill -s "$probe_signal" "$$" 2>/dev/null || true
        exit "$probe_status"
    fi
    return "$probe_status"
}

# Start containers
start() { # $1 = resolved target
    local target="$1"
    info "Starting Docker containers..."
    compose_for_target "$target" config --quiet
    compose_for_target "$target" up -d

    # wait for SSH to be ready
    info "Waiting for SSH servers to start..."
    local timeout_seconds="${RCP_DOCKER_SSH_READY_TIMEOUT_SECONDS:-60}"
    local poll_seconds="${RCP_DOCKER_SSH_READY_POLL_SECONDS:-2}"
    local deadline=$((SECONDS + timeout_seconds))
    local pending_hosts=(host-a host-b)
    while [[ "${#pending_hosts[@]}" -gt 0 ]]; do
        local still_pending=()
        local host
        for host in "${pending_hosts[@]}"; do
            if probe_ssh_until "$host" "$deadline"; then
                info "SSH connectivity to $host verified"
            else
                still_pending+=("$host")
            fi
        done
        pending_hosts=("${still_pending[@]}")

        if [[ "${#pending_hosts[@]}" -eq 0 ]]; then
            break
        fi
        if [[ "$SECONDS" -ge "$deadline" ]]; then
            error "SSH connectivity did not become ready within ${timeout_seconds}s: ${pending_hosts[*]}"
            compose_for_target "$target" logs || true
            return 1
        fi
        local remaining_seconds=$((deadline - SECONDS))
        if [[ "$remaining_seconds" -le 0 ]]; then
            continue
        fi
        if [[ "$poll_seconds" -gt "$remaining_seconds" ]]; then
            sleep "$remaining_seconds"
        else
            sleep "$poll_seconds"
        fi
    done

    info "Containers are ready!"
    echo ""
    info "To exec into master: docker exec -it rcp-test-master /bin/bash"
    info "To run a test copy: ./test-helpers.sh test-copy"
    info "To view logs: just docker-logs (from the repository root)"
    info "To stop: just docker-down (from the repository root)"
}

# Stop containers
stop() {
    info "Stopping Docker containers..."
    compose_existing down
    info "Containers stopped"
}

# Restart containers
restart() {
    stop
    setup
}

# Show status
status() {
    compose_existing ps
}

# Run a simple test copy
test_copy() {
    info "Running test copy: host-a:/tmp/test.txt → host-b:/tmp/test-out.txt"

    # Clean up from any previous test runs
    "$DOCKER_BIN" exec rcp-test-host-a rm -f /tmp/test.txt 2>/dev/null || true
    "$DOCKER_BIN" exec rcp-test-host-b rm -f /tmp/test-out.txt 2>/dev/null || true

    # Create test file on host-a
    "$DOCKER_BIN" exec rcp-test-host-a sh -c 'echo "Hello from RCP Docker test" > /tmp/test.txt'
    info "Created test file on host-a"

    # Copy using rcp from master (using full path since docker exec doesn't inherit ENV)
    # Run as testuser to use the correct SSH keys
    "$DOCKER_BIN" exec -u testuser rcp-test-master /home/testuser/.local/bin/rcp -vv host-a:/tmp/test.txt host-b:/tmp/test-out.txt

    # Verify on host-b
    result=$("$DOCKER_BIN" exec rcp-test-host-b cat /tmp/test-out.txt)

    if [[ "$result" == "Hello from RCP Docker test" ]]; then
        info "✅ Test PASSED - File copied successfully!"
        echo "Content: $result"
    else
        error "❌ Test FAILED - Unexpected content: $result"
        exit 1
    fi
}

# Test SSH connectivity
test_ssh() {
    info "Testing SSH connectivity..."

    echo -n "master → host-a: "
    if "$DOCKER_BIN" exec rcp-test-master ssh -o ConnectTimeout=5 host-a hostname; then
        echo "✅"
    else
        echo "❌"
    fi

    echo -n "master → host-b: "
    if "$DOCKER_BIN" exec rcp-test-master ssh -o ConnectTimeout=5 host-b hostname; then
        echo "✅"
    else
        echo "❌"
    fi

    echo -n "host-a → host-b: "
    if "$DOCKER_BIN" exec rcp-test-host-a ssh -o ConnectTimeout=5 host-b hostname; then
        echo "✅"
    else
        echo "❌"
    fi
}

# Show logs
logs() {
    if [[ -n "${1:-}" ]]; then
        compose_existing logs -f "$1"
    else
        compose_existing logs -f
    fi
}

# show logs once for unattended diagnostics.
logs_once() {
    if [[ -n "${1:-}" ]]; then
        compose_existing logs "$1"
    else
        compose_existing logs
    fi
}

# Clean test files from containers
cleanup() {
    info "Cleaning test files from all containers..."
    for container in master host-a host-b; do
        "$DOCKER_BIN" exec "rcp-test-$container" sh -c 'rm -rf /tmp/test* /tmp/role-* /tmp/rapid-* /tmp/bidir-* /tmp/rcpd-delayed* 2>/dev/null || true'
    done
    info "Cleanup complete"
}

# Rebuild containers
rebuild() {
    info "Rebuilding containers..."
    setup_project rebuild
    info "Rebuild complete"
}

# perform lifecycle cleanup at most once.
lifecycle_cleanup_once() {
    if [[ "$LIFECYCLE_CLEAN_UP" != yes || "$LIFECYCLE_CLEANUP_DONE" -eq 1 ]]; then
        return 0
    fi
    LIFECYCLE_CLEANUP_DONE=1
    "$SCRIPT_PATH" stop
}

# preserve the triggering status while the EXIT trap owns teardown.
lifecycle_exit() {
    local status=$?
    local cleanup_status
    trap - EXIT INT TERM
    set +e
    lifecycle_cleanup_once
    cleanup_status=$?
    if [[ "$status" -eq 0 && "$cleanup_status" -ne 0 ]]; then
        status="$cleanup_status"
    fi
    exit "$status"
}

lifecycle_clear_active_child() {
    if [[ -n "$LIFECYCLE_ACTIVE_STATUS_FILE" ]]; then
        rm -f "$LIFECYCLE_ACTIVE_STATUS_FILE"
    fi
    LIFECYCLE_ACTIVE_PID=
    LIFECYCLE_ACTIVE_STATUS_FILE=
}

lifecycle_group_has_other_members() { # $1 = anchored process-group ID
    ps -eo pid=,pgid=,stat= | awk -v group="$1" -v anchor="$1" '
        $2 == group && $1 != anchor && $3 !~ /^Z/ { found = 1 }
        END { exit !found }
    '
}

lifecycle_terminate_active_child() { # $1 = signal to forward
    local signal="$1"
    local active_pid="$LIFECYCLE_ACTIVE_PID"
    local grace_seconds="${RCP_DOCKER_LIFECYCLE_SIGNAL_GRACE_SECONDS:-5}"
    local decision_claim=
    local decision_directory=
    local decision_owner=timer
    local kill_timer_pid=
    local monitor_was_enabled=no

    if [[ -z "$active_pid" ]]; then
        return 0
    fi
    LIFECYCLE_TERMINATING_CHILD=yes
    if [[ ! "$grace_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
        grace_seconds=5
    fi

    # the supervisor remains the live group leader until every negative-PGID
    # operation is complete. Only then may wait reap that ownership anchor.
    kill -s "$signal" -- "-$active_pid" 2>/dev/null || true
    if ! lifecycle_group_has_other_members "$active_pid"; then
        kill -KILL -- "-$active_pid" 2>/dev/null || true
        wait "$active_pid" 2>/dev/null || true
        lifecycle_clear_active_child
        LIFECYCLE_TERMINATING_CHILD=no
        return 0
    fi

    if ! decision_directory="$(mktemp -d)"; then
        kill -KILL -- "-$active_pid" 2>/dev/null || true
        wait "$active_pid" 2>/dev/null || true
        lifecycle_clear_active_child
        LIFECYCLE_TERMINATING_CHILD=no
        return 0
    fi
    decision_claim="$decision_directory/claimed"

    if [[ "$-" == *m* ]]; then
        monitor_was_enabled=yes
    fi
    set -m
    (
        set +m
        trap ':' INT TERM
        # always make the timeout decision if the absolute delay fails or is interrupted.
        "$SLEEP_BIN" "$grace_seconds" || true
        if mkdir "$decision_claim" 2>/dev/null; then
            kill -KILL -- "-$active_pid" 2>/dev/null || true
        fi
        trap '' INT TERM
        exec "$SLEEP_BIN" 2147483647
    ) &
    kill_timer_pid=$!
    if [[ "$monitor_was_enabled" == yes ]]; then
        set -m
    else
        set +m
    fi

    while [[ ! -d "$decision_claim" ]]; do
        if ! lifecycle_group_has_other_members "$active_pid"; then
            if mkdir "$decision_claim" 2>/dev/null; then
                decision_owner=parent
            fi
            break
        fi
        "$SLEEP_BIN" 0.05
    done

    if [[ "$decision_owner" == parent ]]; then
        # winning the atomic claim guarantees that the timer cannot race this
        # group operation by killing the anchor first.
        kill -KILL -- "-$active_pid" 2>/dev/null || true
        kill -KILL -- "-$kill_timer_pid" 2>/dev/null || true
        wait "$active_pid" 2>/dev/null || true
        wait "$kill_timer_pid" 2>/dev/null || true
    else
        # the timer owns the active-group decision and remains anchored after
        # firing. Never address the active PGID again after its reap.
        wait "$active_pid" 2>/dev/null || true
        kill -KILL -- "-$kill_timer_pid" 2>/dev/null || true
        wait "$kill_timer_pid" 2>/dev/null || true
    fi
    rmdir "$decision_claim" "$decision_directory" 2>/dev/null || true
    lifecycle_clear_active_child
    LIFECYCLE_TERMINATING_CHILD=no
}

lifecycle_signal() { # $1 = signal, $2 = conventional signal-derived status
    if [[ "$LIFECYCLE_REGISTERING_CHILD" == yes ||
        "$LIFECYCLE_TERMINATING_CHILD" == yes ]]; then
        if [[ -z "$LIFECYCLE_PENDING_SIGNAL" ]]; then
            LIFECYCLE_PENDING_SIGNAL="$1"
            LIFECYCLE_PENDING_SIGNAL_STATUS="$2"
        fi
        return
    fi
    trap '' INT TERM
    lifecycle_terminate_active_child "$1"
    exit "$2"
}

# run one lifecycle phase in an owned process group so signal traps can
# interrupt wait, terminate the phase, and clean up before the parent exits.
run_lifecycle_child() { # $1 = working directory, remaining arguments = command
    local working_directory="$1"
    local monitor_was_enabled=no
    local pending_signal
    local pending_status
    local status
    shift

    if [[ "$-" == *m* ]]; then
        monitor_was_enabled=yes
    fi

    LIFECYCLE_REGISTERING_CHILD=yes
    if ! LIFECYCLE_ACTIVE_STATUS_FILE="$(mktemp)"; then
        LIFECYCLE_REGISTERING_CHILD=no
        return 1
    fi
    set -m
    (
        set +m
        trap ':' INT TERM
        cd "$working_directory"
        set +e
        "$@" </dev/null
        command_status=$?
        set -e
        printf '%s\n' "$command_status" >"$LIFECYCLE_ACTIVE_STATUS_FILE"
        trap '' INT TERM
        exec "$SLEEP_BIN" 2147483647
    ) &
    LIFECYCLE_ACTIVE_PID=$!
    if [[ "$monitor_was_enabled" == yes ]]; then
        set -m
    else
        set +m
    fi
    LIFECYCLE_REGISTERING_CHILD=no

    if [[ -n "$LIFECYCLE_PENDING_SIGNAL" ]]; then
        pending_signal="$LIFECYCLE_PENDING_SIGNAL"
        pending_status="$LIFECYCLE_PENDING_SIGNAL_STATUS"
        LIFECYCLE_PENDING_SIGNAL=
        LIFECYCLE_PENDING_SIGNAL_STATUS=
        lifecycle_signal "$pending_signal" "$pending_status"
    fi

    while [[ ! -s "$LIFECYCLE_ACTIVE_STATUS_FILE" ]]; do
        if ! kill -0 "$LIFECYCLE_ACTIVE_PID" 2>/dev/null; then
            if wait "$LIFECYCLE_ACTIVE_PID"; then
                status=0
            else
                status=$?
            fi
            lifecycle_clear_active_child
            return "$status"
        fi
        "$SLEEP_BIN" 0.01
    done
    status="$(<"$LIFECYCLE_ACTIVE_STATUS_FILE")"
    lifecycle_terminate_active_child TERM
    if [[ -n "$LIFECYCLE_PENDING_SIGNAL" ]]; then
        pending_signal="$LIFECYCLE_PENDING_SIGNAL"
        pending_status="$LIFECYCLE_PENDING_SIGNAL_STATUS"
        LIFECYCLE_PENDING_SIGNAL=
        LIFECYCLE_PENDING_SIGNAL_STATUS=
        lifecycle_signal "$pending_signal" "$pending_status"
    fi
    return "$status"
}

# run setup and a command with optional trap-backed teardown.
run_lifecycle() { # $1 = clean up (yes/no), remaining arguments = command
    LIFECYCLE_CLEAN_UP="$1"
    shift

    if [[ "$LIFECYCLE_CLEAN_UP" == yes ]]; then
        trap lifecycle_exit EXIT
    fi
    trap 'lifecycle_signal INT 130' INT
    trap 'lifecycle_signal TERM 143' TERM

    run_lifecycle_child "$SCRIPT_DIR" "$SCRIPT_PATH" setup
    run_lifecycle_child "$REPO_ROOT" "$@"
}

# Show help
usage() {
    cat << EOF
RCP Docker Test Environment - Helper Script

Usage: $0 <command> [args]

Commands:
    build       Build payload binaries for Docker's selected target
    setup       Build payloads, start containers, and wait for both SSH hosts
    start       Alias for setup
    stop        Stop all containers
    restart     Restart all containers
    status      Show container status
    test-copy   Run a simple multi-host copy test
    test-ssh    Test SSH connectivity between containers
    logs [svc]  Follow logs (optionally for a specific service)
    logs-once   Capture current logs without following
    cleanup     Remove test files from containers
    rebuild     Rebuild images and payloads, then wait for both SSH hosts
    lifecycle   Run setup and a command, then stop exactly once
    lifecycle-keep
                Run setup and a command without stopping containers
    shell       Open shell in master container
    help        Show this help message

Examples:
    $0 start                  # Start the environment
    $0 test-copy              # Run a quick test
    $0 logs master            # View master container logs
    $0 shell                  # Get a shell in master container

See README.md for more details.
EOF
}

# Open shell in master
shell() {
    info "Opening shell in master container..."
    "$DOCKER_BIN" exec -it rcp-test-master /bin/bash
}

# Main command dispatcher
main() {
    case "${1:-}" in
        build)
            build
            ;;
        setup)
            setup
            ;;
        start)
            setup
            ;;
        stop)
            stop
            ;;
        restart)
            restart
            ;;
        status)
            status
            ;;
        test-copy)
            test_copy
            ;;
        test-ssh)
            test_ssh
            ;;
        logs)
            logs "${2:-}"
            ;;
        logs-once)
            logs_once "${2:-}"
            ;;
        cleanup)
            cleanup
            ;;
        rebuild)
            rebuild
            ;;
        lifecycle)
            shift
            run_lifecycle yes "$@"
            ;;
        lifecycle-keep)
            shift
            run_lifecycle no "$@"
            ;;
        shell)
            shell
            ;;
        help|--help|-h|"")
            usage
            ;;
        *)
            error "Unknown command: $1"
            usage
            exit 1
            ;;
    esac
}

main "$@"
