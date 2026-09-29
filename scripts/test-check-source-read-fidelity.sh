#!/bin/bash
# tests for source payload lint coverage across test-only fields and split modules.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
linter="$script_dir/check-source-read-fidelity.sh"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/common/src" "$fixture/rcp/src/source"
files=(common/src/copy.rs common/src/link.rs common/src/safedir.rs rcp/src/source.rs rcp/src/source/discovery.rs)
for file in "${files[@]}"; do
    : > "$fixture/$file"
done

expect_result() {
    local expected_status="$1" expected_text="$2" output status=0
    output=$(cd "$fixture" && bash "$linter" 2>&1) || status=$?
    if [[ "$status" != "$expected_status" ]] || ! grep -Fq "$expected_text" <<< "$output"; then
        echo "FAIL: expected status $expected_status containing $expected_text; got $status" >&2
        echo "$output" >&2
        exit 1
    fi
}

cat > "$fixture/common/src/safedir.rs" <<'RS'
struct Cursor {
    #[cfg(test)]
    hook: bool,
}
fn source_payload() { tokio::fs::File::open("source"); }
#[cfg(test)]
mod tests {
    fn fixture() { std::fs::File::open("fixture"); }
}
RS
expect_result 1 'Line 5:'

cat > "$fixture/common/src/safedir.rs" <<'RS'
#[cfg(test)]
static HOOK: bool = false;
fn source_payload() { tokio::fs::File::open("source"); }
RS
expect_result 1 'Line 3:'

cat > "$fixture/common/src/safedir.rs" <<'RS'
fn explicit_dereference() { tokio::fs::File::open("source"); } // rcp-toctou-allow: explicit -L path
#[cfg(test)]
mod renamed_tests {
    fn fixture() { std::fs::File::open("fixture"); }
}
RS
expect_result 0 'Source-read fidelity check passed'

printf '%s\n' 'fn source_payload() { tokio::fs::read_link("source"); }' > "$fixture/rcp/src/source/discovery.rs"
expect_result 1 'rcp/src/source/discovery.rs:'
rm "$fixture/rcp/src/source/discovery.rs"
expect_result 1 'expected file not found: rcp/src/source/discovery.rs'

echo 'Source-read fidelity checker tests passed'
