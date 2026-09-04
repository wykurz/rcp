#!/usr/bin/env bash
# prints the Linux musl target matching the Docker payload platform.

set -euo pipefail

DOCKER_BIN="${DOCKER:-docker}"
TARGET=""

if [ -n "${DOCKER_DEFAULT_PLATFORM:-}" ]; then
    case "$DOCKER_DEFAULT_PLATFORM" in
        linux/amd64)
            TARGET=x86_64-unknown-linux-musl
            ;;
        linux/amd64/v*)
            AMD64_VARIANT="${DOCKER_DEFAULT_PLATFORM#linux/amd64/v}"
            case "$AMD64_VARIANT" in
                ''|0*|*[!0-9]*)
                    echo "unsupported Docker platform: $DOCKER_DEFAULT_PLATFORM" >&2
                    exit 1
                    ;;
                *)
                    TARGET=x86_64-unknown-linux-musl
                    ;;
            esac
            ;;
        linux/arm64|linux/arm64/v8)
            TARGET=aarch64-unknown-linux-musl
            ;;
        *)
            echo "unsupported Docker platform: $DOCKER_DEFAULT_PLATFORM" >&2
            exit 1
            ;;
    esac
fi

if [ -z "$TARGET" ]; then
    ARCHITECTURE_OUTPUT="$(mktemp)"
    trap 'rm -f "$ARCHITECTURE_OUTPUT"' EXIT
    set +e
    "$DOCKER_BIN" info --format '{{.Architecture}}' > "$ARCHITECTURE_OUTPUT"
    DOCKER_STATUS=$?
    set -e
    if [ "$DOCKER_STATUS" -ne 0 ]; then
        cat "$ARCHITECTURE_OUTPUT"
        echo "failed to detect Docker architecture; ensure the Docker daemon is available" >&2
        exit "$DOCKER_STATUS"
    fi
    ARCHITECTURE="$(tr -d '\r' < "$ARCHITECTURE_OUTPUT")"
    case "$ARCHITECTURE" in
        amd64|x86_64)
            TARGET=x86_64-unknown-linux-musl
            ;;
        arm64|aarch64)
            TARGET=aarch64-unknown-linux-musl
            ;;
        *)
            echo "unsupported Docker architecture: $ARCHITECTURE" >&2
            exit 1
            ;;
    esac
fi

printf '%s\n' "$TARGET"
