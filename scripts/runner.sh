#!/bin/bash

root_dir=${1:-/tmp}
dirwidth=${2:-0}
num_files=${3:-1}
file_size=${4:-10M}

cargo build --release

export PATH=$(pwd)/target/release:$PATH

rrm --quiet $root_dir/filegen $root_dir/filegen-test
filegen -- $root_dir $dirwidth $num_files $file_size

echo $cwd
echo $pwd
strace -fttt rcp --progress --summary --overwrite $root_dir/filegen $root_dir/filegen-test 2> $root_dir/strace.log

scripts/parse-strace.sh $root_dir/strace.log