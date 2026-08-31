#!/bin/bash

set -euo pipefail

root_dir=${1:-/tmp}
dirwidth=${2:-0}
num_files=${3:-1}
file_size=${4:-10M}
starting_dir="$PWD"

cargo_target="$(./scripts/cargo-host.sh --print-target)"
CARGO_BUILD_TARGET="$cargo_target" ./scripts/cargo-host.sh build --release

target_dir="${CARGO_TARGET_DIR:-target}"
case "$target_dir" in
    /*) ;;
    *) target_dir="$starting_dir/$target_dir" ;;
esac
binary_dir="$target_dir/$cargo_target/release"

"$binary_dir/rrm" --quiet "$root_dir/filegen" "$root_dir/filegen-test"
"$binary_dir/filegen" -- "$root_dir" "$dirwidth" "$num_files" "$file_size"

strace -fttt "$binary_dir/rcp" --progress --summary --overwrite \
    "$root_dir/filegen" "$root_dir/filegen-test" 2> "$root_dir/strace.log"

scripts/parse-strace.sh "$root_dir/strace.log"
