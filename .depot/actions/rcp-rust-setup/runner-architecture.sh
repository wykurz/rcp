#!/bin/bash
# maps GitHub's runner architecture to RCP's native CI toolchain inputs.

set -euo pipefail

case "${1:-}" in
    X64)
        rust_target=x86_64-unknown-linux-musl
        linker_alias=x86_64-unknown-linux-musl-gcc
        dprint_asset=dprint-x86_64-unknown-linux-gnu.zip
        dprint_checksum=8cb5925a0d6d0d8aa74c82a00f76734577592dfa1eda9517c261a84fe06accd7
        ;;
    ARM64)
        rust_target=aarch64-unknown-linux-musl
        linker_alias=aarch64-unknown-linux-musl-gcc
        dprint_asset=dprint-aarch64-unknown-linux-gnu.zip
        dprint_checksum=6b86329e17678ff3358f88d69a3774d371b601c665cc8cebbf2a4e1234a6d289
        ;;
    *)
        printf 'unsupported runner architecture: %s\n' "${1:-<empty>}" >&2
        exit 64
        ;;
esac

printf 'Runner architecture %s selects Rust target %s\n' "$1" "$rust_target" >&2
printf 'rust_target=%s\n' "$rust_target"
printf 'linker_alias=%s\n' "$linker_alias"
printf 'dprint_asset=%s\n' "$dprint_asset"
printf 'dprint_checksum=%s\n' "$dprint_checksum"
