#!/usr/bin/env bash
# Helper script for RCP Docker test environment

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

# Check if Docker is available
check_docker() {
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed or not in PATH"
        error "See README.md for installation instructions"
        exit 1
    fi

    if ! docker info &> /dev/null; then
        error "Docker daemon is not running"
        error "Start Docker Desktop or run: sudo systemctl start docker"
        exit 1
    fi

    info "Docker is available"
}

# Check if binaries are built
check_binaries() {
    # docker-compose.yml mounts from musl target, so we require it
    local target_dir="../../target/x86_64-unknown-linux-musl/debug"

    if [[ ! -d "$target_dir" ]]; then
        error "Musl target directory not found: $target_dir"
        error "This project uses musl target by default."
        error "Run: cargo build"
        error ""
        error "Note: If you built with the standard target (target/debug),"
        error "you need to rebuild with the musl target or update"
        error "docker-compose.yml to mount from target/debug instead."
        exit 1
    fi

    local missing=()
    for bin in rcp rcpd rrm rlink rcmp; do
        if [[ ! -f "$target_dir/$bin" ]]; then
            missing+=("$bin")
        fi
    done

    if [[ ${#missing[@]} -gt 0 ]]; then
        error "Missing binaries in musl target: ${missing[*]}"
        error "Run: cargo build"
        exit 1
    fi

    info "All binaries found in: $target_dir"
}

# Start containers
start() {
    info "Starting Docker containers..."
    docker-compose up -d

    # Wait for SSH to be ready
    info "Waiting for SSH servers to start..."
    sleep 3

    # Test SSH connectivity
    if docker exec rcp-test-master ssh -o ConnectTimeout=5 host-a hostname &> /dev/null; then
        info "SSH connectivity verified"
    else
        warn "SSH connectivity check failed, containers may still be initializing"
    fi

    info "Containers are ready!"
    echo ""
    info "To exec into master: docker exec -it rcp-test-master /bin/bash"
    info "To run a test copy: ./test-helpers.sh test-copy"
    info "To view logs: docker-compose logs -f"
    info "To stop: docker-compose down"
}

# Stop containers
stop() {
    info "Stopping Docker containers..."
    docker-compose down
    info "Containers stopped"
}

# Restart containers
restart() {
    stop
    start
}

# Show status
status() {
    docker-compose ps
}

# Run a simple test copy
test_copy() {
    info "Running test copy: host-a:/tmp/test.txt → host-b:/tmp/test-out.txt"

    # Clean up from any previous test runs
    docker exec rcp-test-host-a rm -f /tmp/test.txt 2>/dev/null || true
    docker exec rcp-test-host-b rm -f /tmp/test-out.txt 2>/dev/null || true

    # Create test file on host-a
    docker exec rcp-test-host-a sh -c 'echo "Hello from RCP Docker test" > /tmp/test.txt'
    info "Created test file on host-a"

    # Copy using rcp from master (using full path since docker exec doesn't inherit ENV)
    # Run as testuser to use the correct SSH keys
    docker exec -u testuser rcp-test-master /home/testuser/.local/bin/rcp -vv host-a:/tmp/test.txt host-b:/tmp/test-out.txt

    # Verify on host-b
    result=$(docker exec rcp-test-host-b cat /tmp/test-out.txt)

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
    if docker exec rcp-test-master ssh -o ConnectTimeout=5 host-a hostname; then
        echo "✅"
    else
        echo "❌"
    fi

    echo -n "master → host-b: "
    if docker exec rcp-test-master ssh -o ConnectTimeout=5 host-b hostname; then
        echo "✅"
    else
        echo "❌"
    fi

    echo -n "host-a → host-b: "
    if docker exec rcp-test-host-a ssh -o ConnectTimeout=5 host-b hostname; then
        echo "✅"
    else
        echo "❌"
    fi
}

# Show logs
logs() {
    docker-compose logs -f "${1:-}"
}

# Clean test files from containers
cleanup() {
    info "Cleaning test files from all containers..."
    for container in master host-a host-b; do
        docker exec "rcp-test-$container" sh -c 'rm -rf /tmp/test* /tmp/role-* /tmp/rapid-* /tmp/bidir-* /tmp/rcpd-delayed* 2>/dev/null || true'
    done
    info "Cleanup complete"
}

# Rebuild containers
rebuild() {
    info "Rebuilding containers..."
    docker-compose down
    docker-compose build --no-cache
    docker-compose up -d
    info "Rebuild complete"
}

# Show help
usage() {
    cat << EOF
RCP Docker Test Environment - Helper Script

Usage: $0 <command>

Commands:
    start       Start all containers
    stop        Stop all containers
    restart     Restart all containers
    status      Show container status
    test-copy   Run a simple multi-host copy test
    test-ssh    Test SSH connectivity between containers
    logs [svc]  Show logs (optionally for specific service)
    cleanup     Remove test files from containers
    rebuild     Rebuild containers from scratch
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
    docker exec -it rcp-test-master /bin/bash
}

# Main command dispatcher
main() {
    case "${1:-}" in
        start)
            check_docker
            check_binaries
            start
            ;;
        stop)
            stop
            ;;
        restart)
            check_docker
            check_binaries
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
        cleanup)
            cleanup
            ;;
        rebuild)
            check_docker
            check_binaries
            rebuild
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
