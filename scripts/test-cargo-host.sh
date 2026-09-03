#!/bin/bash
# tests the host Cargo wrapper through fake operating-system and toolchain commands.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT
BIN_DIR="$TEMP_DIR/bin"
CALLS="$TEMP_DIR/cargo-calls"
mkdir -p "$BIN_DIR"

cat > "$BIN_DIR/uname" <<'MOCK'
#!/bin/bash
set -euo pipefail

if [ "${FAKE_UNAME_FAIL:-}" = 1 ]; then
    echo "fake uname failed" >&2
    exit 23
fi

case "$1" in
    -s) printf '%s\n' "$FAKE_UNAME_SYSTEM" ;;
    -m) printf '%s\n' "$FAKE_UNAME_MACHINE" ;;
    *) echo "unexpected fake uname arguments: $*" >&2; exit 2 ;;
esac
MOCK

cat > "$BIN_DIR/rustc" <<'MOCK'
#!/bin/bash
set -euo pipefail

if [ "${FAKE_RUSTC_FAIL:-}" = 1 ]; then
    echo "fake rustc failed" >&2
    exit 24
fi

if [ "$#" -eq 1 ] && [ "$1" = -vV ]; then
    printf 'rustc 1.95.0 (test)\nhost: %s\n' "$FAKE_RUSTC_HOST"
    exit 0
fi

echo "unexpected fake rustc arguments: $*" >&2
exit 2
MOCK

cat > "$BIN_DIR/cargo" <<'MOCK'
#!/bin/bash
set -euo pipefail

printf '%s|%s\n' "${CARGO_BUILD_TARGET-UNSET}" "$*" >> "$CARGO_CALLS"
if [ "${FAKE_CARGO_APPEND_EXTRA_CALL:-}" = 1 ]; then
    printf '%s|%s\n' "${CARGO_BUILD_TARGET-UNSET}" 'unexpected dynamic build' >> "$CARGO_CALLS"
fi

if [ "${FAKE_CARGO_STATUS:-0}" -ne 0 ]; then
    exit "$FAKE_CARGO_STATUS"
fi

if [ "${FAKE_CARGO_CREATE_RUNNER_BINARIES:-}" = 1 ] && [ "$*" = 'build --release' ]; then
    output_dir="${CARGO_TARGET_DIR:-$PWD/target}/${CARGO_BUILD_TARGET}/release"
    mkdir -p "$output_dir"
    for binary in rrm filegen rcp; do
        if [ "${FAKE_CARGO_OMIT_RUNNER_BINARY:-}" = "$binary" ]; then
            continue
        fi
        cat > "$output_dir/$binary" <<'RUNNER_BINARY'
#!/bin/bash
set -euo pipefail

binary_name="$(basename "$0")"
binary_path="$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$0")"
if [ "$binary_name" = rrm ] && [ "${RUNNER_RRM_REQUIRE_EXISTING:-}" = 1 ]; then
    for cleanup_path in "$@"; do
        [ "$cleanup_path" = --quiet ] && continue
        if [ ! -e "$cleanup_path" ] && [ ! -L "$cleanup_path" ]; then
            exit 1
        fi
    done
fi
printf '%s|%s\n' "$binary_name" "$binary_path" >> "$RUNNER_CALLS"
if [ "${RUNNER_FAIL_COMMAND:-}" = "$binary_name" ]; then
    exit 47
fi
if [ "$binary_name" = filegen ]; then
    if [ "${RUNNER_FILEGEN_REQUIRE_DEFAULTS:-}" = 1 ] && [ "${3:-}" != 1 ]; then
        exit 2
    fi
    mkdir -p "$2"
fi
RUNNER_BINARY
        chmod +x "$output_dir/$binary"
    done
fi
MOCK

cat > "$BIN_DIR/dprint" <<'MOCK'
#!/bin/bash
set -euo pipefail
MOCK

for binary in rrm filegen rcp; do
    cat > "$BIN_DIR/$binary" <<'MOCK'
#!/bin/bash
set -euo pipefail

printf '%s\n' "$(basename "$0")" >> "$RUNNER_AMBIENT_CALLS"
exit 93
MOCK
done

cat > "$BIN_DIR/strace" <<'MOCK'
#!/bin/bash
set -euo pipefail

if [ "${1:-}" != -fttt ]; then
    echo "unexpected fake strace arguments: $*" >&2
    exit 2
fi
shift
set +e
"$@"
status=$?
set -e
printf 'read(3, "x", 1) = 1\nwrite(4, "x", 1) = 1\n' >&2
exit "$status"
MOCK

chmod +x "$BIN_DIR/uname" "$BIN_DIR/rustc" "$BIN_DIR/cargo" "$BIN_DIR/dprint" \
    "$BIN_DIR/rrm" "$BIN_DIR/filegen" "$BIN_DIR/rcp" "$BIN_DIR/strace"

FAILED=0
fail() {
    echo -e "${RED}FAIL: $*${NC}"
    FAILED=1
}

assert_call() { # $1 = expected target, $2 = expected Cargo arguments
    local expected="$1|$2"
    if ! grep -Fxq -- "$expected" "$CALLS"; then
        fail "Cargo did not receive '$expected'; calls were: $(tr '\n' ';' < "$CALLS")"
    fi
}

assert_no_call() {
    if [ -s "$CALLS" ]; then
        fail "Cargo ran after resolver failure: $(tr '\n' ';' < "$CALLS")"
    fi
}

run_print_target() { # $1 = system, $2 = machine, $3 = rustc host, $4 = expected target
    local system="$1"
    local machine="$2"
    local rustc_host="$3"
    local expected_target="$4"
    local output status
    : > "$CALLS"
    set +e
    output=$(env -u CARGO_BUILD_TARGET \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        "FAKE_UNAME_SYSTEM=$system" \
        "FAKE_UNAME_MACHINE=$machine" \
        "FAKE_RUSTC_HOST=$rustc_host" \
        "$REPO_ROOT/scripts/cargo-host.sh" --print-target 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 0 ] || [ "$output" != "$expected_target" ]; then
        fail "target query for $system/$machine returned status $status and '$output' (expected '$expected_target')"
    fi
    assert_no_call
}

run_print_explicit_target() {
    local output status
    : > "$CALLS"
    set +e
    output=$(env \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu \
        FAKE_UNAME_FAIL=1 \
        FAKE_UNAME_SYSTEM=Linux \
        FAKE_UNAME_MACHINE=riscv64 \
        FAKE_RUSTC_HOST=riscv64gc-unknown-linux-gnu \
        "$REPO_ROOT/scripts/cargo-host.sh" --print-target 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 0 ] || [ "$output" != x86_64-unknown-linux-gnu ]; then
        fail "explicit target query returned status $status and '$output'"
    fi
    assert_no_call
}

run_print_target_with_spaced_rustc_path() {
    local rustc_dir="$TEMP_DIR/rustc tools"
    local rustc_path="$rustc_dir/rustc test double"
    local output status
    mkdir -p "$rustc_dir"
    cp "$BIN_DIR/rustc" "$rustc_path"
    : > "$CALLS"
    set +e
    output=$(env -u CARGO_BUILD_TARGET \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        "RUSTC=$rustc_path" \
        FAKE_UNAME_SYSTEM=Darwin \
        FAKE_UNAME_MACHINE=arm64 \
        FAKE_RUSTC_HOST=aarch64-apple-darwin \
        "$REPO_ROOT/scripts/cargo-host.sh" --print-target 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 0 ] || [ "$output" != aarch64-apple-darwin ]; then
        fail "spaced RUSTC path lost host target/status: status=$status output=$output"
    fi
    assert_no_call
}

run_print_unsupported_target() {
    local output status
    : > "$CALLS"
    set +e
    output=$(env -u CARGO_BUILD_TARGET \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        FAKE_UNAME_SYSTEM=Linux \
        FAKE_UNAME_MACHINE=riscv64 \
        FAKE_RUSTC_HOST=riscv64gc-unknown-linux-gnu \
        "$REPO_ROOT/scripts/cargo-host.sh" --print-target 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 1 ] || [[ "$output" != *'unsupported Linux architecture: riscv64'* ]]; then
        fail "unsupported target query lost status/diagnostic: status=$status output=$output"
    fi
    assert_no_call
}

run_host_installer() {
    local installer="$REPO_ROOT/scripts/cargo-install-host.sh"
    local output status
    : > "$CALLS"
    if [ ! -x "$installer" ]; then
        fail "host-tool installer is missing or not executable: $installer"
        return
    fi
    set +e
    output=$(env \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        CARGO_BUILD_TARGET=aarch64-unknown-linux-musl \
        FAKE_RUSTC_HOST=x86_64-unknown-linux-gnu \
        "$installer" cargo-nextest --version 0.9.85 --locked 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 0 ]; then
        fail "host-tool installer returned status $status: $output"
        return
    fi
    assert_call UNSET \
        'install --target x86_64-unknown-linux-gnu cargo-nextest --version 0.9.85 --locked'
}

run_host_installer_rustc_failure() {
    local installer="$REPO_ROOT/scripts/cargo-install-host.sh"
    local output status
    : > "$CALLS"
    [ -x "$installer" ] || return 0
    set +e
    output=$(env \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        FAKE_RUSTC_FAIL=1 \
        FAKE_RUSTC_HOST=x86_64-unknown-linux-gnu \
        "$installer" cargo-nextest --locked 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 24 ] || [[ "$output" != *'fake rustc failed'* ]]; then
        fail "host-tool installer lost rustc failure status/diagnostic: status=$status output=$output"
    fi
    assert_no_call
}

run_host_installer_rejects_target_override() {
    local installer="$REPO_ROOT/scripts/cargo-install-host.sh"
    local output status
    : > "$CALLS"
    [ -x "$installer" ] || return 0
    set +e
    output=$(env \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        FAKE_RUSTC_HOST=x86_64-unknown-linux-gnu \
        "$installer" cargo-nextest --target aarch64-unknown-linux-musl 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 2 ] || [[ "$output" != *'does not accept a caller-provided --target'* ]]; then
        fail "host-tool installer accepted a target override: status=$status output=$output"
    fi
    assert_no_call
}

run_wrapper() { # $1 = system, $2 = machine, $3 = rustc host, $4 = expected target, $5 = explicit target
    local system="$1"
    local machine="$2"
    local rustc_host="$3"
    local expected_target="$4"
    local explicit_target="$5"
    local output
    local -a environment=(
        env
        -u CARGO_BUILD_TARGET
        "PATH=$BIN_DIR:$PATH"
        "CARGO=$BIN_DIR/cargo"
        "CARGO_CALLS=$CALLS"
        "FAKE_UNAME_SYSTEM=$system"
        "FAKE_UNAME_MACHINE=$machine"
        "FAKE_RUSTC_HOST=$rustc_host"
    )
    if [ -n "$explicit_target" ]; then
        environment+=("CARGO_BUILD_TARGET=$explicit_target")
    fi
    : > "$CALLS"
    if ! output=$("${environment[@]}" "$REPO_ROOT/scripts/cargo-host.sh" nextest run --profile host-test 2>&1); then
        fail "wrapper failed for $system/$machine: $output"
        return
    fi
    assert_call "$expected_target" 'nextest run --profile host-test'
}

run_just_recipe() { # $1 = recipe, $2... = expected Cargo arguments
    local recipe="$1"
    shift
    local output expected_arguments
    local expected_calls="$TEMP_DIR/expected-cargo-calls"
    : > "$CALLS"
    if ! output=$(cd "$REPO_ROOT" && env -u CARGO_BUILD_TARGET \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        FAKE_UNAME_SYSTEM=Linux \
        FAKE_UNAME_MACHINE=x86_64 \
        FAKE_RUSTC_HOST=x86_64-unknown-linux-gnu \
        CARGO_HOST_TEST_INTEGRATION=1 \
        just "$recipe" 2>&1); then
        echo "just $recipe failed: $output" >&2
        return 1
    fi
    : > "$expected_calls"
    for expected_arguments in "$@"; do
        printf '%s|%s\n' x86_64-unknown-linux-musl "$expected_arguments" \
            >> "$expected_calls"
    done
    if ! cmp -s "$expected_calls" "$CALLS"; then
        echo "just $recipe Cargo calls differed:" >&2
        echo "expected: $(tr '\n' ';' < "$expected_calls")" >&2
        echo "actual:   $(tr '\n' ';' < "$CALLS")" >&2
        return 1
    fi
}

check_just_recipe() {
    local output
    if ! output=$(run_just_recipe "$@" 2>&1); then
        fail "$output"
    fi
}

assert_nix_realization_recipe_boundaries() {
    local invocation_count output
    if ! output=$(cd "$REPO_ROOT" && just --dry-run lint 2>&1); then
        fail "could not inspect lint recipe: $output"
    elif [[ "$output" != *'./scripts/test-nix-targets.sh --evaluate-only'* ]]; then
        fail 'lint omits Nix target evaluation checks'
    else
        invocation_count=$(grep -Fo './scripts/test-nix-targets.sh' <<< "$output" | wc -l)
        if [ "$invocation_count" -ne 1 ]; then
            fail 'lint includes full Nix target realization checks'
        fi
    fi
    if ! output=$(cd "$REPO_ROOT" && just --dry-run ci 2>&1); then
        fail "could not inspect ci recipe: $output"
    else
        invocation_count=$(grep -Fo './scripts/test-nix-targets.sh' <<< "$output" | wc -l)
        if [ "$invocation_count" -ne 2 ] ||
            [[ "$output" != *'./scripts/test-nix-targets.sh;'* ]]; then
            fail 'ci omits full Nix target realization checks'
        fi
    fi
}

assert_runner_call() { # $1 = binary name, $2 = expected executable path
    local expected="$1|$2"
    if ! grep -Fxq -- "$expected" "$RUNNER_CALLS"; then
        fail "runner did not invoke '$expected'; calls were: $(tr '\n' ';' < "$RUNNER_CALLS")"
    fi
}

run_runner_first_run() {
    local runner_target_dir="$TEMP_DIR/runner-target-first-run"
    local runner_root="$TEMP_DIR/runner-files-first-run"
    local executable_dir="$runner_target_dir/x86_64-unknown-linux-musl/release"
    local output status
    : > "$CALLS"
    : > "$RUNNER_CALLS"
    set +e
    output=$(cd "$REPO_ROOT" && env -u CARGO_BUILD_TARGET \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        "CARGO_TARGET_DIR=$runner_target_dir" \
        "RUNNER_CALLS=$RUNNER_CALLS" \
        FAKE_CARGO_CREATE_RUNNER_BINARIES=1 \
        RUNNER_RRM_REQUIRE_EXISTING=1 \
        RUNNER_FILEGEN_REQUIRE_DEFAULTS=1 \
        FAKE_UNAME_SYSTEM=Linux \
        FAKE_UNAME_MACHINE=x86_64 \
        FAKE_RUSTC_HOST=x86_64-unknown-linux-gnu \
        ./scripts/runner.sh "$runner_root" 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 0 ]; then
        fail "runner first run returned status $status: $output"
    fi
    if grep -q '^rrm|' "$RUNNER_CALLS"; then
        fail "runner tried to remove absent first-run paths"
    fi
    assert_runner_call filegen "$executable_dir/filegen"
    assert_runner_call rcp "$executable_dir/rcp"
}

run_runner_success() { # $1 = machine, $2 = target, $3 = target dir, $4 = test label
    local machine="$1"
    local target="$2"
    local runner_target_dir="$3"
    local label="$4"
    local runner_root="$TEMP_DIR/runner-files-$label"
    local output status
    : > "$CALLS"
    : > "$RUNNER_CALLS"
    mkdir -p "$runner_root/filegen" "$runner_root/filegen-test"
    set +e
    output=$(cd "$REPO_ROOT" && env -u CARGO_BUILD_TARGET \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        "CARGO_TARGET_DIR=$runner_target_dir" \
        "RUNNER_CALLS=$RUNNER_CALLS" \
        FAKE_CARGO_CREATE_RUNNER_BINARIES=1 \
        FAKE_UNAME_SYSTEM=Linux \
        "FAKE_UNAME_MACHINE=$machine" \
        "FAKE_RUSTC_HOST=$machine-unknown-linux-gnu" \
        ./scripts/runner.sh "$runner_root" 1 1 1 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 0 ]; then
        fail "runner returned status $status: $output"
    fi
    assert_call "$target" 'build --release'
    local executable_dir target_root
    case "$runner_target_dir" in
        /*) target_root="$runner_target_dir" ;;
        *) target_root="$REPO_ROOT/$runner_target_dir" ;;
    esac
    executable_dir="$(python3 -c \
        'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve(strict=False))' \
        "$target_root/$target/release")"
    assert_runner_call rrm "$executable_dir/rrm"
    assert_runner_call filegen "$executable_dir/filegen"
    assert_runner_call rcp "$executable_dir/rcp"
}

run_runner_command_failure() {
    local runner_target_dir="$TEMP_DIR/runner-target-command-failure"
    local runner_root="$TEMP_DIR/runner-files-command-failure"
    local output status
    : > "$CALLS"
    : > "$RUNNER_CALLS"
    mkdir -p "$runner_root/filegen" "$runner_root/filegen-test"
    set +e
    output=$(cd "$REPO_ROOT" && env -u CARGO_BUILD_TARGET \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        "CARGO_TARGET_DIR=$runner_target_dir" \
        "RUNNER_CALLS=$RUNNER_CALLS" \
        FAKE_CARGO_CREATE_RUNNER_BINARIES=1 \
        RUNNER_FAIL_COMMAND=rrm \
        FAKE_UNAME_SYSTEM=Linux \
        FAKE_UNAME_MACHINE=x86_64 \
        FAKE_RUSTC_HOST=x86_64-unknown-linux-gnu \
        ./scripts/runner.sh "$runner_root" 1 1 1 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 47 ]; then
        fail "runner returned status $status after rrm failed with 47: $output"
    fi
    local expected="rrm|$runner_target_dir/x86_64-unknown-linux-musl/release/rrm"
    if [ "$(cat "$RUNNER_CALLS")" != "$expected" ]; then
        fail "runner continued after rrm failed; calls were: $(tr '\n' ';' < "$RUNNER_CALLS")"
    fi
}

run_runner_build_failure() {
    local output status
    : > "$CALLS"
    : > "$RUNNER_CALLS"
    set +e
    output=$(cd "$REPO_ROOT" && env -u CARGO_BUILD_TARGET \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        "CARGO_TARGET_DIR=$TEMP_DIR/runner-target-build-failure" \
        "RUNNER_CALLS=$RUNNER_CALLS" \
        FAKE_CARGO_STATUS=46 \
        FAKE_UNAME_SYSTEM=Linux \
        FAKE_UNAME_MACHINE=x86_64 \
        FAKE_RUSTC_HOST=x86_64-unknown-linux-gnu \
        ./scripts/runner.sh "$TEMP_DIR/runner-files-build-failure" 1 1 1 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 46 ]; then
        fail "runner returned status $status after Cargo failed with 46: $output"
    fi
    if [ -s "$RUNNER_CALLS" ]; then
        fail "runner invoked a binary after Cargo failed: $(tr '\n' ';' < "$RUNNER_CALLS")"
    fi
}

run_runner_missing_target_binary() {
    local runner_target_dir="$TEMP_DIR/runner-target-missing-binary"
    local runner_root="$TEMP_DIR/runner-files-missing-binary"
    local ambient_calls="$TEMP_DIR/runner-ambient-calls"
    local output status
    : > "$CALLS"
    : > "$RUNNER_CALLS"
    : > "$ambient_calls"
    mkdir -p "$runner_root/filegen" "$runner_root/filegen-test"
    set +e
    output=$(cd "$REPO_ROOT" && env -u CARGO_BUILD_TARGET \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        "CARGO_TARGET_DIR=$runner_target_dir" \
        "RUNNER_CALLS=$RUNNER_CALLS" \
        "RUNNER_AMBIENT_CALLS=$ambient_calls" \
        FAKE_CARGO_CREATE_RUNNER_BINARIES=1 \
        FAKE_CARGO_OMIT_RUNNER_BINARY=rrm \
        FAKE_UNAME_SYSTEM=Linux \
        FAKE_UNAME_MACHINE=x86_64 \
        FAKE_RUSTC_HOST=x86_64-unknown-linux-gnu \
        ./scripts/runner.sh "$runner_root" 1 1 1 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 127 ]; then
        fail "runner did not preserve missing target binary status 127: status=$status output=$output"
    fi
    if [ -s "$ambient_calls" ]; then
        fail "runner fell back to an ambient binary: $(tr '\n' ';' < "$ambient_calls")"
    fi
    if [ -s "$RUNNER_CALLS" ]; then
        fail "runner continued after its target binary was missing: $(tr '\n' ';' < "$RUNNER_CALLS")"
    fi
}

run_runner_hostile_bash_env() {
    local runner_target_dir="$TEMP_DIR/runner-target-hostile-bash-env"
    local runner_root="$TEMP_DIR/runner-files-hostile-bash-env"
    local bash_env="$TEMP_DIR/hostile-bash-env"
    local shadow_calls="$TEMP_DIR/runner-shadow-calls"
    local output status
    cat > "$bash_env" <<'BASH_ENV'
rrm() {
    printf '%s\n' rrm >> "$RUNNER_SHADOW_CALLS"
    return 94
}
filegen() {
    printf '%s\n' filegen >> "$RUNNER_SHADOW_CALLS"
    return 94
}
rcp() {
    printf '%s\n' rcp >> "$RUNNER_SHADOW_CALLS"
    return 94
}
BASH_ENV
    : > "$CALLS"
    : > "$RUNNER_CALLS"
    : > "$shadow_calls"
    mkdir -p "$runner_root/filegen" "$runner_root/filegen-test"
    set +e
    output=$(cd "$REPO_ROOT" && env -u CARGO_BUILD_TARGET \
        "PATH=$BIN_DIR:$PATH" \
        "CARGO=$BIN_DIR/cargo" \
        "CARGO_CALLS=$CALLS" \
        "CARGO_TARGET_DIR=$runner_target_dir" \
        "RUNNER_CALLS=$RUNNER_CALLS" \
        "RUNNER_SHADOW_CALLS=$shadow_calls" \
        "BASH_ENV=$bash_env" \
        FAKE_CARGO_CREATE_RUNNER_BINARIES=1 \
        FAKE_UNAME_SYSTEM=Linux \
        FAKE_UNAME_MACHINE=x86_64 \
        FAKE_RUSTC_HOST=x86_64-unknown-linux-gnu \
        ./scripts/runner.sh "$runner_root" 1 1 1 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 0 ]; then
        fail "runner allowed BASH_ENV to shadow a target binary: status=$status output=$output"
    fi
    if [ -s "$shadow_calls" ]; then
        fail "runner invoked a BASH_ENV function: $(tr '\n' ';' < "$shadow_calls")"
    fi
    local executable_dir="$runner_target_dir/x86_64-unknown-linux-musl/release"
    assert_runner_call rrm "$executable_dir/rrm"
    assert_runner_call filegen "$executable_dir/filegen"
    assert_runner_call rcp "$executable_dir/rcp"
}

assert_no_retired_target_examples() {
    local matches
    if matches=$(rg -n 'runnable-target\.sh' "$REPO_ROOT/docs" "$REPO_ROOT/tests" 2>/dev/null); then
        fail "documentation still invokes the retired target resolver: $matches"
    fi
}

assert_release_deb_archive_paths() {
    local workflow="$REPO_ROOT/.github/workflows/release.yml"
    local expected_path_count
    local target_qualified_paths
    expected_path_count=$(grep -Fc 'target/debian/*.deb' "$workflow" || true)
    if [ "$expected_path_count" -ne 2 ]; then
        fail "release workflow must archive both cargo-deb outputs from target/debian"
    fi
    if target_qualified_paths=$(
        rg -n 'target/[^ /]+/debian/\*\.deb' "$workflow" 2>/dev/null
    ); then
        fail "release workflow uses nonexistent target-qualified cargo-deb output paths: $target_qualified_paths"
    fi
}

assert_no_raw_host_build_guidance() {
    local matches
    if matches=$(rg -n \
        'cargo install (just|cargo-nextest|dprint|inferno)|cargo build --release --bin rcpd' \
        "$REPO_ROOT/AGENTS.md" "$REPO_ROOT/README.md" \
        "$REPO_ROOT/docs" "$REPO_ROOT/remote/src/deploy.rs" 2>/dev/null); then
        fail "host-tool/build guidance bypasses the host-target wrappers: $matches"
    fi
}

echo "🔍 Testing musl-first host Cargo selection..."

# wrong Linux target selection would compile the host against a different libc or architecture.
run_wrapper Linux x86_64 x86_64-unknown-linux-gnu x86_64-unknown-linux-musl ''
run_wrapper Linux aarch64 aarch64-unknown-linux-gnu aarch64-unknown-linux-musl ''
run_print_target Linux x86_64 x86_64-unknown-linux-gnu x86_64-unknown-linux-musl
run_print_target Linux aarch64 aarch64-unknown-linux-gnu aarch64-unknown-linux-musl
run_print_explicit_target
run_print_target_with_spaced_rustc_path
run_print_unsupported_target

# an explicit caller target is the documented escape hatch and must skip host discovery entirely.
run_wrapper Linux riscv64 riscv64-unknown-linux-gnu x86_64-unknown-linux-gnu x86_64-unknown-linux-gnu

: > "$CALLS"
set +e
unsupported_output=$(env -u CARGO_BUILD_TARGET \
    "PATH=$BIN_DIR:$PATH" \
    "CARGO=$BIN_DIR/cargo" \
    "CARGO_CALLS=$CALLS" \
    FAKE_UNAME_SYSTEM=Linux \
    FAKE_UNAME_MACHINE=riscv64 \
    FAKE_RUSTC_HOST=riscv64gc-unknown-linux-gnu \
    "$REPO_ROOT/scripts/cargo-host.sh" check 2>&1)
unsupported_status=$?
set -e
if [ "$unsupported_status" -ne 1 ] ||
    [[ "$unsupported_output" != *'unsupported Linux architecture: riscv64'* ]]; then
    fail "unsupported Linux architecture lost status/diagnostic: status=$unsupported_status output=$unsupported_output"
fi
assert_no_call

run_wrapper Darwin arm64 aarch64-apple-darwin aarch64-apple-darwin ''

: > "$CALLS"
set +e
resolver_output=$(env -u CARGO_BUILD_TARGET \
    "PATH=$BIN_DIR:$PATH" \
    "CARGO=$BIN_DIR/cargo" \
    "CARGO_CALLS=$CALLS" \
    FAKE_UNAME_SYSTEM=Linux \
    FAKE_UNAME_MACHINE=x86_64 \
    FAKE_RUSTC_HOST=x86_64-unknown-linux-gnu \
    FAKE_UNAME_FAIL=1 \
    "$REPO_ROOT/scripts/cargo-host.sh" check 2>&1)
resolver_status=$?
set -e
if [ "$resolver_status" -ne 23 ] || [[ "$resolver_output" != *'fake uname failed'* ]]; then
    fail "resolver failure status/diagnostic was lost: status=$resolver_status output=$resolver_output"
fi
assert_no_call

# every target-sensitive non-Docker recipe must enter Cargo through the host-target wrapper.
if FAKE_CARGO_APPEND_EXTRA_CALL=1 \
    run_just_recipe check 'check --workspace' >/dev/null 2>&1; then
    fail 'Just recipe matrix accepted an additional Cargo invocation'
fi
check_just_recipe fmt 'fmt'
check_just_recipe clean 'clean'
check_just_recipe check 'check --workspace'
if [ "${CARGO_HOST_TEST_SKIP_LINT_RECIPE:-}" != 1 ]; then
    check_just_recipe _lint-build 'fmt --check' 'clippy --workspace --all-targets -- -D warnings'
fi
check_just_recipe build 'build --workspace'
check_just_recipe build-release 'build --workspace --release'
check_just_recipe doc 'doc --no-deps --workspace'
check_just_recipe test 'nextest run'
check_just_recipe test-release 'nextest run --release'
check_just_recipe doctest 'test --doc'
check_just_recipe doctest-release 'test --doc --release'
assert_nix_realization_recipe_boundaries
RUNNER_CALLS="$TEMP_DIR/runner-calls"
run_runner_first_run
run_runner_success x86_64 x86_64-unknown-linux-musl \
    "$TEMP_DIR/runner-target-absolute" absolute
relative_runner_target=$(python3 -c \
    'import os, sys; print(os.path.relpath(sys.argv[1], sys.argv[2]))' \
    "$TEMP_DIR/runner-target-relative" "$REPO_ROOT")
run_runner_success aarch64 aarch64-unknown-linux-musl \
    "$relative_runner_target" relative
run_runner_command_failure
run_runner_build_failure
run_runner_missing_target_binary
run_runner_hostile_bash_env
run_host_installer
run_host_installer_rustc_failure
run_host_installer_rejects_target_override

# Docs must keep commands runnable without a second target resolver.
assert_no_retired_target_examples
assert_release_deb_archive_paths
assert_no_raw_host_build_guidance

: > "$CALLS"
set +e
just_failure_output=$(cd "$REPO_ROOT" && env -u CARGO_BUILD_TARGET \
    "PATH=$BIN_DIR:$PATH" \
    "CARGO=$BIN_DIR/cargo" \
    "CARGO_CALLS=$CALLS" \
    FAKE_UNAME_SYSTEM=Linux \
    FAKE_UNAME_MACHINE=x86_64 \
    FAKE_RUSTC_HOST=x86_64-unknown-linux-gnu \
    FAKE_UNAME_FAIL=1 \
    CARGO_HOST_TEST_INTEGRATION=1 \
    just check 2>&1)
just_failure_status=$?
set -e
if [ "$just_failure_status" -ne 23 ] || [[ "$just_failure_output" != *'fake uname failed'* ]]; then
    fail "just check masked wrapper failure: status=$just_failure_status output=$just_failure_output"
fi
assert_no_call

if [ "$FAILED" -ne 0 ]; then
    exit 1
fi
echo -e "${GREEN}✅ host Cargo selection tests passed!${NC}"
