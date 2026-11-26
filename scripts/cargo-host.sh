#!/usr/bin/env bash
# runs Cargo with the supported, runnable target for this host.

set -euo pipefail

CARGO_BIN="${CARGO:-cargo}"

if [ -z "${CARGO_BUILD_TARGET:-}" ]; then
    HOST_SYSTEM="$(uname -s)"
    if [ "$HOST_SYSTEM" != Linux ]; then
        echo "unsupported operating system: $HOST_SYSTEM" >&2
        exit 1
    fi

    HOST_ARCHITECTURE="$(uname -m)"
    case "$HOST_ARCHITECTURE" in
        x86_64)
            CARGO_BUILD_TARGET=x86_64-unknown-linux-musl
            ;;
        aarch64)
            CARGO_BUILD_TARGET=aarch64-unknown-linux-musl
            ;;
        *)
            echo "unsupported Linux architecture: $HOST_ARCHITECTURE" >&2
            exit 1
            ;;
    esac
fi

export CARGO_BUILD_TARGET

if [ "${1:-}" = --print-target ]; then
    if [ "$#" -ne 1 ]; then
        echo 'usage: cargo-host.sh --print-target' >&2
        exit 2
    fi
    printf '%s\n' "$CARGO_BUILD_TARGET"
    exit 0
fi

# cargo-nextest's interactive progress bar (`indicatif`) suppresses status and
# failure lines on `TERM=dumb` PTYs (common in agent runners, Emacs, and CI).
# Default to counter progress there unless the caller already chose a mode.
if [ "${1:-}" = nextest ] && [ "${TERM:-}" = dumb ] && [ -z "${NEXTEST_SHOW_PROGRESS:-}" ]; then
    export NEXTEST_SHOW_PROGRESS=counter
fi

exec "$CARGO_BIN" "$@"
