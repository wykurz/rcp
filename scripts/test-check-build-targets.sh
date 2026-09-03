#!/bin/bash
# tests the build-target consistency checker against executable policy fixtures.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECKER="$SCRIPT_DIR/check-build-targets.sh"
NIX_TARGET_TEST="$SCRIPT_DIR/test-nix-targets.sh"
CHECKER_ARGS=()
if [ "${RCP_BUILD_TARGET_TEST_SKIP_NIX_EVAL:-}" = 1 ]; then
    CHECKER_ARGS+=(--skip-nix-eval)
fi
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT

fail() {
    echo -e "${RED}FAIL: $*${NC}"
    exit 1
}

create_fixture() { # $1 = fixture root
    local root="$1"
    mkdir -p "$root/.cargo" "$root/.depot/actions/rcp-rust-setup" \
        "$root/.depot/workflows" "$root/.github/workflows" "$root/nix" "$root/scripts"
    cat > "$root/.cargo/config.toml" <<'FIXTURE'
[build]
target = "x86_64-unknown-linux-musl"
rustflags = ["--cfg", "tokio_unstable"]

[target.x86_64-unknown-linux-gnu]
rustflags = ["--cfg", "tokio_unstable"]

[target.x86_64-unknown-linux-musl]
linker = "x86_64-unknown-linux-musl-gcc"
rustflags = ["--cfg", "tokio_unstable", "-C", "target-feature=+crt-static"]

[target.aarch64-unknown-linux-musl]
linker = "aarch64-unknown-linux-musl-gcc"
rustflags = ["--cfg", "tokio_unstable", "-C", "target-feature=+crt-static"]
FIXTURE
    cat > "$root/rust-toolchain.toml" <<'FIXTURE'
[toolchain]
channel = "1.95.0"
targets = ["aarch64-unknown-linux-musl", "x86_64-unknown-linux-musl"]
FIXTURE
    cat > "$root/nix/target-platform.nix" <<'FIXTURE'
{ system }:

let
  platforms = {
    x86_64-linux = {
      cargoTarget = "x86_64-unknown-linux-musl";
      isLinux = true;
    };
    aarch64-linux = {
      cargoTarget = "aarch64-unknown-linux-musl";
      isLinux = true;
    };
  };
  disconnectedPlatforms = {
    x86_64-linux = {
      cargoTarget = "x86_64-unknown-linux-gnu";
      isLinux = true;
    };
    aarch64-linux = {
      cargoTarget = "aarch64-unknown-linux-gnu";
      isLinux = true;
    };
  };
  platform = builtins.getAttr system platforms;
  environment = {
    CARGO_BUILD_TARGET = platform.cargoTarget;
  };
in
{
  inherit (platform) cargoTarget isLinux;
  inherit environment;
  rustTargets = [ platform.cargoTarget ];
}
FIXTURE
    cat > "$root/scripts/cargo-host.sh" <<'FIXTURE'
#!/bin/bash
set -euo pipefail

case "$(uname -m)" in
    x86_64) CARGO_BUILD_TARGET=x86_64-unknown-linux-musl ;;
    aarch64) CARGO_BUILD_TARGET=aarch64-unknown-linux-musl ;;
    *) exit 1 ;;
esac
export CARGO_BUILD_TARGET
exec "${CARGO:-cargo}" "$@"
FIXTURE
    cat > "$root/scripts/docker-target.sh" <<'FIXTURE'
#!/bin/bash
set -euo pipefail

if [ -n "${DOCKER_DEFAULT_PLATFORM:-}" ]; then
    architecture="$DOCKER_DEFAULT_PLATFORM"
else
    architecture="$(${DOCKER:-docker} info --format '{{.Architecture}}')"
fi
case "$architecture" in
    linux/amd64|amd64|x86_64) printf '%s\n' x86_64-unknown-linux-musl ;;
    linux/arm64|linux/arm64/v8|arm64|aarch64) printf '%s\n' aarch64-unknown-linux-musl ;;
    *) exit 1 ;;
esac
FIXTURE
    cat > "$root/.depot/actions/rcp-rust-setup/runner-architecture.sh" <<'FIXTURE'
#!/bin/bash
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

printf 'rust_target=%s\n' "$rust_target"
printf 'linker_alias=%s\n' "$linker_alias"
printf 'dprint_asset=%s\n' "$dprint_asset"
printf 'dprint_checksum=%s\n' "$dprint_checksum"
FIXTURE
    cat > "$root/.depot/actions/rcp-rust-setup/action.yml" <<'FIXTURE'
name: Set up RCP Rust CI
runs:
  using: composite
  steps:
  - name: Select runner architecture
    id: architecture
    shell: bash
    run: |
      "$GITHUB_ACTION_PATH/runner-architecture.sh" "$RUNNER_ARCH" >>"$GITHUB_OUTPUT"
  - name: Setup Rust
    uses: actions-rust-lang/setup-rust-toolchain@v1
    with:
      target: ${{ steps.architecture.outputs.rust_target }}
  - name: Install system dependencies
    shell: bash
    run: |
      sudo ln -sf /usr/bin/musl-gcc "/usr/bin/${{ steps.architecture.outputs.linker_alias }}"
  - name: Install dprint
    if: inputs.install-dprint == 'true'
    shell: bash
    run: |
      curl -fsSL -o /tmp/dprint.zip \
        "https://github.com/dprint/dprint/releases/download/0.54.0/${{ steps.architecture.outputs.dprint_asset }}"
      echo "${{ steps.architecture.outputs.dprint_checksum }}  /tmp/dprint.zip" | sha256sum -c -
FIXTURE
    cat > "$root/.depot/workflows/ci.yml" <<'FIXTURE'
name: RCP CI
on:
  workflow_dispatch:
jobs:
  lint:
    runs-on: depot-ubuntu-24.04-4
  doc:
    runs-on: depot-ubuntu-24.04-4
  test:
    strategy:
      matrix:
        runner:
        - depot-ubuntu-24.04-16
        - depot-ubuntu-24.04-arm-16
    runs-on: ${{ matrix.runner }}
    steps:
    - name: Install Nix for native Arm package ABI smoke
      if: runner.arch == 'ARM64'
      uses: DeterminateSystems/nix-installer-action@ef8a148080ab6020fd15196c2084a2eea5ff2d25
    - name: Verify native Arm Nix package ABI smoke
      if: runner.arch == 'ARM64'
      run: nix build --no-update-lock-file .#checks.aarch64-linux.package-abi-smoke
  doctest:
    runs-on: depot-ubuntu-24.04-4
  test-release:
    runs-on: depot-ubuntu-24.04-16
  doctest-release:
    runs-on: depot-ubuntu-24.04-4
  docker-test:
    strategy:
      matrix:
        runner:
        - depot-ubuntu-24.04-16
        - depot-ubuntu-24.04-arm-16
    runs-on: ${{ matrix.runner }}
  docker-chaos-test:
    runs-on: depot-ubuntu-24.04-16
FIXTURE
    cat > "$root/.github/workflows/validate.yml" <<'FIXTURE'
name: Validate
on:
  workflow_call:
jobs:
  nix:
    runs-on: ubuntu-latest
    steps:
    - name: Flake check (evaluate all systems)
      run: nix flake check --all-systems --no-build --no-update-lock-file -L
FIXTURE
    chmod +x "$root/scripts/cargo-host.sh" "$root/scripts/docker-target.sh" \
        "$root/.depot/actions/rcp-rust-setup/runner-architecture.sh"
}

expect_success() { # $1 = fixture root
    local output
    if ! output=$("$CHECKER" "${CHECKER_ARGS[@]}" "$1" 2>&1); then
        fail "coherent fixture was rejected: $output"
    fi
    if [[ "$output" != *'build target declarations are consistent'* ]]; then
        fail "coherent fixture did not report success: $output"
    fi
}

expect_success_without_nix() { # $1 = fixture root
    local output
    if ! output=$(NIX="$TEMP_DIR/missing-nix" \
        "$CHECKER" --skip-nix-eval "$1" 2>&1); then
        fail "coherent non-Nix fixture was rejected: $output"
    fi
    if [[ "$output" != *'build target declarations are consistent'* ]]; then
        fail "coherent non-Nix fixture did not report success: $output"
    fi
}

expect_failure_without_nix() { # $1 = expected diagnostic, $2 = fixture root
    local expected="$1"
    local root="$2"
    local output
    local status
    set +e
    output=$(NIX="$TEMP_DIR/missing-nix" \
        "$CHECKER" --skip-nix-eval "$root" 2>&1)
    status=$?
    set -e
    if [ "$status" -eq 0 ] || [[ "$output" != *"$expected"* ]]; then
        fail "expected non-Nix failure containing '$expected'; got status $status: $output"
    fi
}

expect_failure() { # $1 = expected diagnostic, $2 = fixture root
    local expected="$1"
    local root="$2"
    local output
    local status
    set +e
    output=$("$CHECKER" "${CHECKER_ARGS[@]}" "$root" 2>&1)
    status=$?
    set -e
    if [ "$status" -eq 0 ] || [[ "$output" != *"$expected"* ]]; then
        fail "expected failure containing '$expected'; got status $status: $output"
    fi
}

expect_failure_without_ambient_docker() { # $1 = expected diagnostic, $2 = fixture root
    local expected="$1"
    local root="$2"
    local ambient_docker="$TEMP_DIR/ambient-docker"
    local ambient_log="$TEMP_DIR/ambient-docker.log"
    local output
    local status
    cat > "$ambient_docker" <<'FAKE'
#!/bin/bash
set -euo pipefail
printf 'called\n' >> "$RCP_AMBIENT_DOCKER_LOG"
printf 'amd64\n'
FAKE
    chmod +x "$ambient_docker"
    set +e
    output=$(DOCKER="$ambient_docker" RCP_AMBIENT_DOCKER_LOG="$ambient_log" \
        "$CHECKER" "${CHECKER_ARGS[@]}" "$root" 2>&1)
    status=$?
    set -e
    if [ -e "$ambient_log" ]; then
        fail "checker contacted the ambient Docker command during platform resolution: $output"
    fi
    if [ "$status" -eq 0 ] || [[ "$output" != *"$expected"* ]]; then
        fail "expected failure containing '$expected'; got status $status: $output"
    fi
}

mutated_fixture() { # $1 = fixture name
    local root="$TEMP_DIR/$1"
    cp -R "$TEMP_DIR/coherent" "$root"
    printf '%s\n' "$root"
}

check_nix_evaluate_only_mode() {
    local fake_nix_dir="$TEMP_DIR/fake-nix-bin"
    local fake_nix="$fake_nix_dir/nix"
    local nix_calls="$TEMP_DIR/fake-nix-calls"
    local output status
    mkdir -p "$fake_nix_dir"
    cat > "$fake_nix" <<'FAKE'
#!/bin/bash
set -euo pipefail

printf '%s\n' "$1" >> "$FAKE_NIX_CALLS"
case "$1" in
    eval)
        last_argument=''
        for last_argument in "$@"; do :; done
        if [ "$last_argument" = builtins.currentSystem ]; then
            printf 'x86_64-linux'
        else
            printf 'Nix target behavior tests passed\n'
        fi
        ;;
    build | develop)
        exit 91
        ;;
    *)
        exit 92
        ;;
esac
FAKE
    chmod +x "$fake_nix"

    : > "$nix_calls"
    set +e
    output=$(PATH="$fake_nix_dir:$PATH" NIX="$fake_nix" FAKE_NIX_CALLS="$nix_calls" \
        "$NIX_TARGET_TEST" --evaluate-only 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 0 ]; then
        fail "Nix evaluate-only mode returned status $status: $output"
    fi
    if grep -Eq '^(build|develop)$' "$nix_calls"; then
        fail "Nix evaluate-only mode realized a derivation: $(tr '\n' ';' < "$nix_calls")"
    fi

    : > "$nix_calls"
    set +e
    PATH="$fake_nix_dir:$PATH" NIX="$fake_nix" FAKE_NIX_CALLS="$nix_calls" \
        "$NIX_TARGET_TEST" >/dev/null 2>&1
    status=$?
    set -e
    if [ "$status" -ne 91 ] || ! grep -Fxq build "$nix_calls"; then
        fail "default Nix target test stopped being a full realization check: status=$status calls=$(tr '\n' ';' < "$nix_calls")"
    fi
}

echo "🔍 Testing build target consistency..."

if grep -Eq '(^|[^[:alnum:]_])(mapfile|readarray)([^[:alnum:]_]|$)' "$CHECKER"; then
    fail 'build target checker uses a Bash 4-only line-reading builtin'
fi

check_nix_evaluate_only_mode

create_fixture "$TEMP_DIR/coherent"
expect_success "$TEMP_DIR/coherent"
expect_success_without_nix "$TEMP_DIR/coherent"

fixture=$(mutated_fixture github-nix-flake-check-builds-all-systems)
sed -i 's/ --no-build//' "$fixture/.github/workflows/validate.yml"
expect_failure_without_nix 'GitHub Nix all-systems flake check command is not exact' "$fixture"

fixture=$(mutated_fixture non-nix-wrong-default)
sed -i 's/target = "x86_64-unknown-linux-musl"/target = "aarch64-unknown-linux-musl"/' \
    "$fixture/.cargo/config.toml"
expect_failure_without_nix 'Cargo default target' "$fixture"

fixture=$(mutated_fixture depot-workflow-duplicates-test-job)
cat >>"$fixture/.depot/workflows/ci.yml" <<'FIXTURE'
  test:
    runs-on: depot-ubuntu-24.04-16
FIXTURE
expect_failure "duplicate YAML mapping key 'test'" "$fixture"

fixture=$(mutated_fixture depot-action-duplicates-top-level-runs)
sed -i '/^runs:/i\
runs:\
  using: composite\
  steps: []' "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure "duplicate YAML mapping key 'runs'" "$fixture"

fixture=$(mutated_fixture wrong-default)
sed -i 's/target = "x86_64-unknown-linux-musl"/target = "aarch64-unknown-linux-musl"/' \
    "$fixture/.cargo/config.toml"
expect_failure 'Cargo default target' "$fixture"

fixture=$(mutated_fixture missing-target)
sed -i 's/\[target.aarch64-unknown-linux-musl\]/[target.aarch64-unknown-linux-gnu]/' \
    "$fixture/.cargo/config.toml"
expect_failure 'supported musl target sections' "$fixture"

fixture=$(mutated_fixture wrong-linker)
sed -i 's/aarch64-unknown-linux-musl-gcc/aarch64-linux-musl-gcc/' \
    "$fixture/.cargo/config.toml"
expect_failure 'aarch64-unknown-linux-musl linker' "$fixture"

fixture=$(mutated_fixture x86-static-crt-substring)
sed -i '0,/target-feature=+crt-static/s//target-feature=+crt-static-disabled/' \
    "$fixture/.cargo/config.toml"
expect_failure 'x86_64-unknown-linux-musl rustflags do not enable crt-static' "$fixture"

fixture=$(mutated_fixture arm-static-crt-substring)
sed -i '/\[target.aarch64-unknown-linux-musl\]/,/^$/s/target-feature=+crt-static/target-feature=+crt-static-disabled/' \
    "$fixture/.cargo/config.toml"
expect_failure 'aarch64-unknown-linux-musl rustflags do not enable crt-static' "$fixture"

fixture=$(mutated_fixture x86-static-crt-comment)
sed -i '/\[target.x86_64-unknown-linux-musl\]/,/^$/s/rustflags = .*/rustflags = ["--cfg", "tokio_unstable"] # target-feature=+crt-static/' \
    "$fixture/.cargo/config.toml"
expect_failure 'x86_64-unknown-linux-musl rustflags do not enable crt-static' "$fixture"

fixture=$(mutated_fixture arm-static-crt-comment)
sed -i '/\[target.aarch64-unknown-linux-musl\]/,/^$/s/rustflags = .*/rustflags = ["--cfg", "tokio_unstable"] # target-feature=+crt-static/' \
    "$fixture/.cargo/config.toml"
expect_failure 'aarch64-unknown-linux-musl rustflags do not enable crt-static' "$fixture"

fixture=$(mutated_fixture missing-rust-target)
sed -i 's/"aarch64-unknown-linux-musl", //' "$fixture/rust-toolchain.toml"
expect_failure 'Rust std targets' "$fixture"

if [ "${RCP_BUILD_TARGET_TEST_SKIP_NIX_EVAL:-}" != 1 ]; then
    fixture=$(mutated_fixture wrong-nix-mapping)
    sed -i 's/aarch64-unknown-linux-musl/aarch64-unknown-linux-gnu/' \
        "$fixture/nix/target-platform.nix"
    expect_failure 'Nix mapping for aarch64-linux' "$fixture"

    fixture=$(mutated_fixture disconnected-nix-mapping)
    sed -i 's/builtins.getAttr system platforms/builtins.getAttr system disconnectedPlatforms/' \
        "$fixture/nix/target-platform.nix"
    expect_failure 'Nix mapping for x86_64-linux' "$fixture"
fi

fixture=$(mutated_fixture wrong-host-mapping)
sed -i 's/aarch64-unknown-linux-musl/aarch64-unknown-linux-gnu/' \
    "$fixture/scripts/cargo-host.sh"
expect_failure 'host Cargo mapping for aarch64' "$fixture"

fixture=$(mutated_fixture unsupported-resolver-output)
sed -i '0,/x86_64-unknown-linux-musl/s//riscv64gc-unknown-linux-musl/' \
    "$fixture/scripts/docker-target.sh"
expect_failure 'Docker resolver for linux/amd64' "$fixture"

fixture=$(mutated_fixture platform-resolver-queries-docker)
# keep the resolver's environment expressions literal in the mutated fixture.
# shellcheck disable=SC2016
sed -i '/if \[ -n "${DOCKER_DEFAULT_PLATFORM:-}" \]; then/a\
    "${DOCKER:-docker}" info --format '\''{{.Architecture}}'\'' > /dev/null' \
    "$fixture/scripts/docker-target.sh"
expect_failure_without_ambient_docker \
    'Docker command must not be called while DOCKER_DEFAULT_PLATFORM is set' "$fixture"

fixture=$(mutated_fixture wrong-depot-x64-target)
sed -i '0,/rust_target=x86_64-unknown-linux-musl/s//rust_target=x86_64-unknown-linux-gnu/' \
    "$fixture/.depot/actions/rcp-rust-setup/runner-architecture.sh"
expect_failure 'Depot runner mapping for X64' "$fixture"

fixture=$(mutated_fixture wrong-depot-arm-linker)
sed -i 's/linker_alias=aarch64-unknown-linux-musl-gcc/linker_alias=aarch64-linux-musl-gcc/' \
    "$fixture/.depot/actions/rcp-rust-setup/runner-architecture.sh"
expect_failure 'Depot runner mapping for ARM64' "$fixture"

fixture=$(mutated_fixture wrong-depot-arm-dprint)
sed -i 's/dprint-aarch64-unknown-linux-gnu.zip/dprint-x86_64-unknown-linux-gnu.zip/' \
    "$fixture/.depot/actions/rcp-rust-setup/runner-architecture.sh"
expect_failure 'Depot runner mapping for ARM64' "$fixture"

fixture=$(mutated_fixture wrong-depot-arm-dprint-checksum)
sed -i 's/6b86329e17678ff3358f88d69a3774d371b601c665cc8cebbf2a4e1234a6d289/8cb5925a0d6d0d8aa74c82a00f76734577592dfa1eda9517c261a84fe06accd7/' \
    "$fixture/.depot/actions/rcp-rust-setup/runner-architecture.sh"
expect_failure 'Depot runner mapping for ARM64' "$fixture"

fixture=$(mutated_fixture depot-accepts-unsupported-architecture)
sed -i 's/exit 64/exit 0/' \
    "$fixture/.depot/actions/rcp-rust-setup/runner-architecture.sh"
expect_failure 'Depot runner mapping accepts unsupported architecture' "$fixture"

fixture=$(mutated_fixture depot-accepts-x86-runner-architecture)
sed -i '/    \*)/i\
    X86)\
        rust_target=x86_64-unknown-linux-musl\
        linker_alias=x86_64-unknown-linux-musl-gcc\
        dprint_asset=dprint-x86_64-unknown-linux-gnu.zip\
        dprint_checksum=8cb5925a0d6d0d8aa74c82a00f76734577592dfa1eda9517c261a84fe06accd7\
        ;;' \
    "$fixture/.depot/actions/rcp-rust-setup/runner-architecture.sh"
expect_failure 'Depot runner mapping accepts unsupported architecture X86' "$fixture"

fixture=$(mutated_fixture depot-accepts-arm-runner-architecture)
sed -i '/    \*)/i\
    ARM)\
        rust_target=aarch64-unknown-linux-musl\
        linker_alias=aarch64-unknown-linux-musl-gcc\
        dprint_asset=dprint-aarch64-unknown-linux-gnu.zip\
        dprint_checksum=6b86329e17678ff3358f88d69a3774d371b601c665cc8cebbf2a4e1234a6d289\
        ;;' \
    "$fixture/.depot/actions/rcp-rust-setup/runner-architecture.sh"
expect_failure 'Depot runner mapping accepts unsupported architecture ARM' "$fixture"

fixture=$(mutated_fixture depot-action-hardcodes-rust-target)
sed -i 's/${{ steps.architecture.outputs.rust_target }}/x86_64-unknown-linux-musl/' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action does not thread rust_target' "$fixture"

fixture=$(mutated_fixture depot-action-hardcodes-linker)
sed -i 's/${{ steps.architecture.outputs.linker_alias }}/x86_64-unknown-linux-musl-gcc/' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action does not thread linker_alias' "$fixture"

fixture=$(mutated_fixture depot-action-hardcodes-dprint-asset)
sed -i 's/${{ steps.architecture.outputs.dprint_asset }}/dprint-x86_64-unknown-linux-gnu.zip/' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action does not thread dprint_asset' "$fixture"

fixture=$(mutated_fixture depot-action-hardcodes-dprint-checksum)
sed -i 's/${{ steps.architecture.outputs.dprint_checksum }}/8cb5925a0d6d0d8aa74c82a00f76734577592dfa1eda9517c261a84fe06accd7/' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action does not thread dprint_checksum' "$fixture"

fixture=$(mutated_fixture depot-action-comment-masks-hardcoded-mapper)
sed -i '/runner-architecture.sh.*RUNNER_ARCH/c\
      # "$GITHUB_ACTION_PATH/runner-architecture.sh" "$RUNNER_ARCH" >>"$GITHUB_OUTPUT"\
      printf '\''rust_target=x86_64-unknown-linux-musl\\n'\'' >>"$GITHUB_OUTPUT"' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action does not execute the runner architecture mapping' "$fixture"

fixture=$(mutated_fixture depot-action-comment-masks-hardcoded-target)
sed -i '/target: ${{ steps.architecture.outputs.rust_target }}/c\
      target: x86_64-unknown-linux-musl\
      # target: ${{ steps.architecture.outputs.rust_target }}' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action does not thread rust_target' "$fixture"

fixture=$(mutated_fixture depot-action-comment-masks-hardcoded-linker)
sed -i '/steps.architecture.outputs.linker_alias/c\
      sudo ln -sf /usr/bin/musl-gcc /usr/bin/x86_64-unknown-linux-musl-gcc\
      # sudo ln -sf /usr/bin/musl-gcc "/usr/bin/${{ steps.architecture.outputs.linker_alias }}"' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action does not thread linker_alias' "$fixture"

fixture=$(mutated_fixture depot-action-comment-masks-hardcoded-dprint-asset)
sed -i '/steps.architecture.outputs.dprint_asset/c\
        "https://github.com/dprint/dprint/releases/download/0.54.0/dprint-x86_64-unknown-linux-gnu.zip"\
      # ${{ steps.architecture.outputs.dprint_asset }}' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action does not thread dprint_asset' "$fixture"

fixture=$(mutated_fixture depot-action-comment-masks-hardcoded-dprint-checksum)
sed -i '/steps.architecture.outputs.dprint_checksum/c\
      echo "8cb5925a0d6d0d8aa74c82a00f76734577592dfa1eda9517c261a84fe06accd7  /tmp/dprint.zip" | sha256sum -c -\
      # ${{ steps.architecture.outputs.dprint_checksum }}' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action does not thread dprint_checksum' "$fixture"

fixture=$(mutated_fixture depot-action-dead-step-masks-hardcoded-target)
sed -i '/target: ${{ steps.architecture.outputs.rust_target }}/c\
      target: x86_64-unknown-linux-musl' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
cat >>"$fixture/.depot/actions/rcp-rust-setup/action.yml" <<'FIXTURE'
  - name: Dead intended Rust setup
    if: ${{ false }}
    uses: actions-rust-lang/setup-rust-toolchain@v1
    with:
      target: ${{ steps.architecture.outputs.rust_target }}
FIXTURE
expect_failure 'Depot setup action must have exactly one active Rust setup step' "$fixture"

fixture=$(mutated_fixture depot-action-conditionally-skips-rust-setup)
sed -i '/  - name: Setup Rust/a\
    if: ${{ false }}' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action conditionally skips Rust setup' "$fixture"

fixture=$(mutated_fixture depot-action-duplicates-rust-target)
sed -i '/      target: ${{ steps.architecture.outputs.rust_target }}/a\
      target: x86_64-unknown-linux-musl' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure "duplicate YAML mapping key 'target'" "$fixture"

fixture=$(mutated_fixture depot-action-rust-target-outside-with)
sed -i '/    with:/,/      target: ${{ steps.architecture.outputs.rust_target }}/{
    /    with:/a\
      components: rustfmt
    /      target: ${{ steps.architecture.outputs.rust_target }}/c\
    env:\
      target: ${{ steps.architecture.outputs.rust_target }}
}' "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action must declare exactly one Rust target under with' "$fixture"

fixture=$(mutated_fixture depot-action-conditionally-skips-system-dependencies)
sed -i '/  - name: Install system dependencies/a\
    if: ${{ false }}' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action conditionally skips system dependencies' "$fixture"

fixture=$(mutated_fixture depot-action-unpinned-dprint-condition)
sed -i "/    if: inputs.install-dprint == 'true'/c\\
    if: \${{ true }}" \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action dprint condition is not pinned to its input' "$fixture"

fixture=$(mutated_fixture depot-action-duplicates-dprint-condition)
sed -i "/    if: inputs.install-dprint == 'true'/a\\
    if: \${{ false }}" \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure "duplicate YAML mapping key 'if'" "$fixture"

fixture=$(mutated_fixture depot-action-noop-masks-hardcoded-linker)
sed -i '/steps.architecture.outputs.linker_alias/c\
      sudo ln -sf /usr/bin/musl-gcc /usr/bin/x86_64-unknown-linux-musl-gcc\
      : "${{ steps.architecture.outputs.linker_alias }}"' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action does not thread linker_alias' "$fixture"

fixture=$(mutated_fixture depot-action-noop-masks-hardcoded-dprint-asset)
sed -i '/steps.architecture.outputs.dprint_asset/c\
        "https://github.com/dprint/dprint/releases/download/0.54.0/dprint-x86_64-unknown-linux-gnu.zip"\
      : "${{ steps.architecture.outputs.dprint_asset }}"' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action does not thread dprint_asset' "$fixture"

fixture=$(mutated_fixture depot-action-noop-masks-hardcoded-dprint-checksum)
sed -i '/steps.architecture.outputs.dprint_checksum/c\
      echo "8cb5925a0d6d0d8aa74c82a00f76734577592dfa1eda9517c261a84fe06accd7  /tmp/dprint.zip" | sha256sum -c -\
      : "${{ steps.architecture.outputs.dprint_checksum }}"' \
    "$fixture/.depot/actions/rcp-rust-setup/action.yml"
expect_failure 'Depot setup action does not thread dprint_checksum' "$fixture"

fixture=$(mutated_fixture depot-test-misses-arm-nix-installer)
sed -i '/  test:/,/  doctest:/{
    /uses: DeterminateSystems\/nix-installer-action@/d
}' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test Arm Nix smoke must have exactly one Nix installer step' "$fixture"

fixture=$(mutated_fixture depot-test-job-conditionally-skipped)
sed -i '/  test:/a\
    if: ${{ false }}' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test job must not be conditional' "$fixture"

fixture=$(mutated_fixture depot-test-job-continues-on-error)
sed -i '/  test:/a\
    continue-on-error: true' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test job must gate Arm Nix smoke failures' "$fixture"

fixture=$(mutated_fixture depot-test-arm-nix-installer-alternate-sha)
sed -i 's/ef8a148080ab6020fd15196c2084a2eea5ff2d25/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/' \
    "$fixture/.depot/workflows/ci.yml"
expect_success_without_nix "$fixture"

fixture=$(mutated_fixture depot-test-unpinned-arm-nix-installer)
sed -i '/  test:/,/  doctest:/s#DeterminateSystems/nix-installer-action@[^[:space:]]*#DeterminateSystems/nix-installer-action@v22#' \
    "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test Arm Nix smoke installer is not pinned' "$fixture"

fixture=$(mutated_fixture depot-test-arm-nix-installer-runs-on-x86)
sed -i "/name: Install Nix for native Arm package ABI smoke/{n;/if:/d;}" \
    "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test Arm Nix smoke installer condition must be runner.arch ARM64' "$fixture"

fixture=$(mutated_fixture depot-test-misses-arm-nix-smoke-build)
sed -i '/  test:/,/  doctest:/{
    /run: nix build --no-update-lock-file .*package-abi-smoke/d
}' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test must have exactly one Arm Nix package ABI smoke build step' "$fixture"

fixture=$(mutated_fixture depot-test-builds-x86-nix-smoke-on-arm)
sed -i '/  test:/,/  doctest:/s/checks\.aarch64-linux\.package-abi-smoke/checks.x86_64-linux.package-abi-smoke/' \
    "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test Arm Nix package ABI smoke command is not exact' "$fixture"

fixture=$(mutated_fixture depot-test-arm-nix-smoke-updates-lock)
sed -i '/  test:/,/  doctest:/s/nix build --no-update-lock-file/nix build/' \
    "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test Arm Nix package ABI smoke command is not exact' "$fixture"

fixture=$(mutated_fixture depot-test-arm-nix-smoke-wrong-condition)
sed -i "/name: Verify native Arm Nix package ABI smoke/{n;s/ARM64/X64/;}" \
    "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test Arm Nix smoke build condition must be runner.arch ARM64' "$fixture"

fixture=$(mutated_fixture depot-test-arm-nix-smoke-continues-on-error)
sed -i '/run: nix build --no-update-lock-file .*package-abi-smoke/a\
      continue-on-error: true' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test Arm Nix smoke build must gate the job' "$fixture"

fixture=$(mutated_fixture depot-test-arm-nix-smoke-uses-inert-shell)
sed -i '/run: nix build --no-update-lock-file .*package-abi-smoke/a\
      shell: true {0}' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test Arm Nix smoke build must not override its shell' "$fixture"

fixture=$(mutated_fixture depot-test-arm-nix-smoke-uses-unsafe-shell)
sed -i '/run: nix build --no-update-lock-file .*package-abi-smoke/a\
      shell: bash -c true {0}' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test Arm Nix smoke build must not override its shell' "$fixture"

fixture=$(mutated_fixture depot-test-arm-nix-smoke-inherits-job-shell)
sed -i '/  test:/a\
    defaults:\
      run:\
        shell: true {0}' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test Arm Nix smoke build must not inherit a custom shell' "$fixture"

fixture=$(mutated_fixture depot-test-arm-nix-smoke-inherits-workflow-shell)
sed -i '/^jobs:/i\
defaults:\
  run:\
    shell: true {0}' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test Arm Nix smoke build must not inherit a custom shell' "$fixture"

fixture=$(mutated_fixture depot-test-arm-nix-smoke-duplicates-shell)
sed -i '/run: nix build --no-update-lock-file .*package-abi-smoke/a\
      shell: bash\
      shell: true {0}' "$fixture/.depot/workflows/ci.yml"
expect_failure "duplicate YAML mapping key 'shell'" "$fixture"

fixture=$(mutated_fixture depot-test-duplicates-arm-nix-smoke)
sed -i '/  doctest:/i\
    - name: Duplicate native Arm Nix package ABI smoke\
      if: runner.arch == '\''ARM64'\''\
      run: nix build --no-update-lock-file .#checks.aarch64-linux.package-abi-smoke' \
    "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test must have exactly one Arm Nix package ABI smoke build step' "$fixture"

fixture=$(mutated_fixture depot-test-dead-arm-nix-smoke-masks-removal)
sed -i '/name: Verify native Arm Nix package ABI smoke/{n;n;s#.*#      run: true#;}' \
    "$fixture/.depot/workflows/ci.yml"
sed -i '/  doctest:/i\
    - name: Dead native Arm Nix package ABI smoke\
      if: ${{ false }}\
      run: nix build --no-update-lock-file .#checks.aarch64-linux.package-abi-smoke' \
    "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test Arm Nix smoke build condition must be runner.arch ARM64' "$fixture"

fixture=$(mutated_fixture depot-test-runs-full-nix-target-suite-on-arm)
sed -i '/  doctest:/i\
    - name: Overbroad native Arm Nix verification\
      if: runner.arch == '\''ARM64'\''\
      run: ./scripts/test-nix-targets.sh' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test must not run the full Nix target suite' "$fixture"

fixture=$(mutated_fixture depot-test-excludes-arm-runner)
sed -i '0,/        - depot-ubuntu-24.04-arm-16/{
    /        - depot-ubuntu-24.04-arm-16/a\
        exclude:\
        - runner: depot-ubuntu-24.04-arm-16
}' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test runner matrix must not use exclude' "$fixture"

fixture=$(mutated_fixture depot-test-excludes-escaped-arm-runner)
sed -i '0,/        - depot-ubuntu-24.04-arm-16/{
    /        - depot-ubuntu-24.04-arm-16/a\
        exclude:\
        - runner: "depot-ubuntu-24.04-arm-\\x31\\x36"
}' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test runner matrix must not use exclude' "$fixture"

fixture=$(mutated_fixture depot-test-has-expression-exclude)
sed -i '0,/        - depot-ubuntu-24.04-arm-16/{
    /        - depot-ubuntu-24.04-arm-16/a\
        exclude: ${{ fromJSON('\''[]'\'') }}
}' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test runner matrix must not use exclude' "$fixture"

fixture=$(mutated_fixture depot-test-misses-arm-runner)
sed -i '0,/        - depot-ubuntu-24.04-arm-16/{/        - depot-ubuntu-24.04-arm-16/d;}' \
    "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test runner matrix' "$fixture"

fixture=$(mutated_fixture depot-test-arm-outside-runner-matrix)
sed -i '0,/        - depot-ubuntu-24.04-arm-16/{/        - depot-ubuntu-24.04-arm-16/d;}' \
    "$fixture/.depot/workflows/ci.yml"
sed -i '/  test:/,/  doctest:/{/    runs-on:/i\
        documentation_only:\
        - depot-ubuntu-24.04-arm-16
}' \
    "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot test runner matrix' "$fixture"

fixture=$(mutated_fixture depot-test-duplicates-strategy)
sed -i '/  test:/,/  doctest:/{/    runs-on:/i\
    strategy:\
      matrix:\
        runner: []
}' \
    "$fixture/.depot/workflows/ci.yml"
expect_failure "duplicate YAML mapping key 'strategy'" "$fixture"

fixture=$(mutated_fixture depot-test-duplicates-matrix)
sed -i '/  test:/,/  doctest:/{/        runner:/i\
      matrix:\
        documentation_only: []
}' \
    "$fixture/.depot/workflows/ci.yml"
expect_failure "duplicate YAML mapping key 'matrix'" "$fixture"

fixture=$(mutated_fixture depot-test-duplicates-runner)
sed -i '/  test:/,/  doctest:/{/        - depot-ubuntu-24.04-arm-16/a\
        runner: []
}' \
    "$fixture/.depot/workflows/ci.yml"
expect_failure "duplicate YAML mapping key 'runner'" "$fixture"

fixture=$(mutated_fixture depot-test-duplicates-runs-on)
sed -i '/  test:/,/  doctest:/{/    runs-on: ${{ matrix.runner }}/a\
    runs-on: depot-ubuntu-24.04-16
}' "$fixture/.depot/workflows/ci.yml"
expect_failure "duplicate YAML mapping key 'runs-on'" "$fixture"

fixture=$(mutated_fixture depot-lint-duplicates-runs-on)
sed -i '/  lint:/,/  doc:/{/    runs-on: depot-ubuntu-24.04-4/a\
    runs-on: depot-ubuntu-24.04-16
}' "$fixture/.depot/workflows/ci.yml"
expect_failure "duplicate YAML mapping key 'runs-on'" "$fixture"

fixture=$(mutated_fixture depot-docker-test-misses-arm-runner)
sed -i '/  docker-test:/,/  docker-chaos-test:/{/        - depot-ubuntu-24.04-arm-16/d;}' \
    "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot docker-test runner matrix' "$fixture"

fixture=$(mutated_fixture depot-docker-test-excludes-x86-runner)
sed -i '/  docker-test:/,/  docker-chaos-test:/ {
    /        - depot-ubuntu-24.04-arm-16/a\
        exclude:\
        - runner: depot-ubuntu-24.04-16
}' "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot docker-test runner matrix must not use exclude' "$fixture"

fixture=$(mutated_fixture depot-matrix-inline-comments)
sed -i '/  test:/,/  doctest:/ {
    s|- depot-ubuntu-24.04-16$|- depot-ubuntu-24.04-16 # native x86 runner|
    s|runs-on: ${{ matrix.runner }}$|runs-on: ${{ matrix.runner }} # native fanout|
}' "$fixture/.depot/workflows/ci.yml"
expect_success_without_nix "$fixture"

fixture=$(mutated_fixture depot-matrix-orthogonal-axis)
sed -i '0,/        - depot-ubuntu-24.04-arm-16/ {
    /        - depot-ubuntu-24.04-arm-16/a\
        feature:\
        - default\
        - minimal
}' "$fixture/.depot/workflows/ci.yml"
expect_success_without_nix "$fixture"

fixture=$(mutated_fixture depot-arm-runner-full-line-comment)
sed -i '/  lint:/a\
    # depot-ubuntu-24.04-arm-16 is intentionally reserved for matrix jobs' \
    "$fixture/.depot/workflows/ci.yml"
expect_success_without_nix "$fixture"

fixture=$(mutated_fixture depot-arm-runner-inline-comment)
sed -i '/  lint:/,/  doc:/s|runs-on: depot-ubuntu-24.04-4|runs-on: depot-ubuntu-24.04-4 # depot-ubuntu-24.04-arm-16 is reserved|' \
    "$fixture/.depot/workflows/ci.yml"
expect_success_without_nix "$fixture"

fixture=$(mutated_fixture depot-arm-runner-alternate-matrix-axis)
cat >> "$fixture/.depot/workflows/ci.yml" <<'FIXTURE'
  alternate-arm:
    strategy:
      matrix:
        worker:
        - depot-ubuntu-24.04-arm-16
    runs-on: ${{ matrix.worker }}
FIXTURE
expect_failure_without_nix 'Depot Arm64 runner appears outside test and docker-test' "$fixture"

fixture=$(mutated_fixture depot-arm-release-duplication)
sed -i '/  test-release:/,/  doctest-release:/s/runs-on: depot-ubuntu-24.04-16/runs-on: depot-ubuntu-24.04-arm-16/' \
    "$fixture/.depot/workflows/ci.yml"
expect_failure 'Depot Arm64 runner appears outside test and docker-test' "$fixture"

echo -e "${GREEN}✅ Build target consistency tests passed!${NC}"
