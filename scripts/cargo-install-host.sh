#!/usr/bin/env bash
# installs a third-party Cargo tool for rustc's native, runnable host target.

set -euo pipefail

if [ "$#" -eq 0 ]; then
    echo 'usage: cargo-install-host.sh <crate> [cargo-install arguments...]' >&2
    exit 2
fi

for argument in "$@"; do
    case "$argument" in
        --target | --target=*)
            echo 'cargo-install-host.sh does not accept a caller-provided --target' >&2
            exit 2
            ;;
    esac
done

RUSTC_BIN="${RUSTC:-rustc}"
RUSTC_VERSION="$("$RUSTC_BIN" -vV)"
RUSTC_HOST="$(sed -n 's/^host: //p' <<< "$RUSTC_VERSION")"
if [ -z "$RUSTC_HOST" ] || [[ "$RUSTC_HOST" == *$'\n'* ]]; then
    echo "could not determine the host target from '$RUSTC_BIN -vV'" >&2
    exit 1
fi

unset CARGO_BUILD_TARGET
exec "${CARGO:-cargo}" install --target "$RUSTC_HOST" "$@"
