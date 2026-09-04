#!/usr/bin/env bash
# runs Cargo with the supported, runnable target for this host.

set -euo pipefail

CARGO_BIN="${CARGO:-cargo}"
RUSTC_BIN="${RUSTC:-rustc}"

if [ -z "${CARGO_BUILD_TARGET:-}" ]; then
    HOST_SYSTEM="$(uname -s)"
    if [ "$HOST_SYSTEM" = Linux ]; then
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
    else
        RUSTC_VERSION="$("$RUSTC_BIN" -vV)"
        RUSTC_HOST="$(sed -n 's/^host: //p' <<< "$RUSTC_VERSION")"
        if [ -z "$RUSTC_HOST" ] || [[ "$RUSTC_HOST" == *$'\n'* ]]; then
            echo "could not determine the host target from '$RUSTC_BIN -vV'" >&2
            exit 1
        fi
        CARGO_BUILD_TARGET="$RUSTC_HOST"
    fi
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

exec "$CARGO_BIN" "$@"
