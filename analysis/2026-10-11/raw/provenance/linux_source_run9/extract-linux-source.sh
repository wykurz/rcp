#!/usr/bin/env bash
# Extract and atomically publish the Linux source benchmark dataset.

set -euo pipefail

readonly ROOT="/d/phscratch/public/user/mateusz/filesets"
readonly ARCHIVE="$ROOT/linux-7.2.9.tar.gz"
readonly DESTINATION="$ROOT/linux-source"
readonly TEMPORARY="$ROOT/.linux-source.extracting-20261011"
readonly EXPECTED_SHA256="ba4996423cf38afc564f795be41d171495344ad6103c35bf2ce62918a1213ec6"

[[ "$(sha256sum "$ARCHIVE" | awk '{print $1}')" == "$EXPECTED_SHA256" ]] || {
  printf 'archive checksum mismatch: %s\n' "$ARCHIVE" >&2
  exit 1
}
[[ ! -e "$DESTINATION" ]] || {
  printf 'destination already exists: %s\n' "$DESTINATION" >&2
  exit 1
}
[[ ! -e "$TEMPORARY" ]] || {
  printf 'temporary path already exists: %s\n' "$TEMPORARY" >&2
  exit 1
}

mkdir "$TEMPORARY"
tar -xzf "$ARCHIVE" --strip-components=1 -C "$TEMPORARY"
[[ -f "$TEMPORARY/Makefile" && -d "$TEMPORARY/Documentation" &&
  -d "$TEMPORARY/arch" && -d "$TEMPORARY/drivers" ]] || {
  printf 'extracted tree does not have the expected Linux source layout\n' >&2
  exit 1
}
mv "$TEMPORARY" "$DESTINATION"

printf 'published=%s\n' "$DESTINATION"
printf 'regular_files=%s\n' "$(find "$DESTINATION" -type f | wc -l)"
printf 'symlinks=%s\n' "$(find "$DESTINATION" -type l | wc -l)"
printf 'apparent_bytes=%s\n' "$(du -sb --apparent-size "$DESTINATION" | awk '{print $1}')"
printf 'inventory_sha256=%s\n' "$(
  find "$DESTINATION" -printf '%P\t%y\t%s\t%m\t%l\n' |
    LC_ALL=C sort |
    sha256sum |
    awk '{print $1}'
)"
