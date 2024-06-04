#!/bin/bash

# Capture strace output or provide filename as argument
strace_output=${1}

# Check if strace output provided
if [[ -z "$strace_output" ]]; then
  echo "Usage: $0 <strace_output_file>"
  exit 1
fi

read_bytes=$(rg 'read.* = (\d+)' $strace_output -or '$1'|awk '{ SUM += $0 } END { print SUM }')
write_bytes=$(rg 'write.* = (\d+)' $strace_output -or '$1'|awk '{ SUM += $0 } END { print SUM }')

# Print results
echo " read bytes: $read_bytes"
echo "write bytes: $write_bytes"
