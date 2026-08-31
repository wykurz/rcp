#!/bin/bash
# checks that build-target declarations agree with the Cargo distribution policy.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$DEFAULT_ROOT"
SKIP_NIX_EVAL=0
ROOT_SET=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        --skip-nix-eval)
            SKIP_NIX_EVAL=1
            ;;
        --*)
            echo "unknown argument: $1" >&2
            exit 2
            ;;
        *)
            if [ "$ROOT_SET" -eq 1 ]; then
                echo "unexpected repository root: $1" >&2
                exit 2
            fi
            REPO_ROOT="$1"
            ROOT_SET=1
            ;;
    esac
    shift
done
EXPECTED_DEFAULT=x86_64-unknown-linux-musl
EXPECTED_TARGETS=(aarch64-unknown-linux-musl x86_64-unknown-linux-musl)
SUPPORTED_TARGETS=()
NIX_BIN="${NIX:-nix}"
FAILED=0

fail() {
    printf 'build target inconsistency: %s\n' "$1" >&2
    FAILED=1
}

require_file() { # $1 = relative path
    if [ ! -f "$REPO_ROOT/$1" ]; then
        fail "missing $1"
        return 1
    fi
}

check_yaml_duplicates() {
    local validator="$SCRIPT_DIR/check-yaml-duplicates.py"
    local output
    local status
    set +e
    output=$("$validator" \
        "$REPO_ROOT/.depot/actions/rcp-rust-setup/action.yml" \
        "$REPO_ROOT/.depot/workflows/ci.yml" \
        "$REPO_ROOT/.github/workflows/validate.yml" 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 0 ]; then
        fail "YAML validation failed: $output"
    fi
}

toml_without_comments() { # $1 = file
    awk '
        {
            output = ""
            in_string = 0
            escaped = 0
            for (position = 1; position <= length($0); position++) {
                character = substr($0, position, 1)
                if (escaped) {
                    output = output character
                    escaped = 0
                } else if (in_string && character == "\\") {
                    output = output character
                    escaped = 1
                } else if (character == "\"") {
                    output = output character
                    in_string = !in_string
                } else if (!in_string && character == "#") {
                    break
                } else {
                    output = output character
                }
            }
            print output
        }
    ' "$1"
}

toml_value() { # $1 = file, $2 = section, $3 = key
    toml_without_comments "$1" | awk -v wanted_section="$2" -v wanted_key="$3" '
        function trim(value) {
            sub(/^[[:space:]]+/, "", value)
            sub(/[[:space:]]+$/, "", value)
            return value
        }
        /^[[:space:]]*\[/ {
            section = $0
            gsub(/[[:space:]]/, "", section)
            in_section = section == "[" wanted_section "]"
            next
        }
        in_section {
            equals = index($0, "=")
            if (equals == 0) next
            key = trim(substr($0, 1, equals - 1))
            if (key != wanted_key) next
            value = trim(substr($0, equals + 1))
            if (value ~ /^".*"$/) {
                value = substr(value, 2, length(value) - 2)
            }
            print value
            exit
        }
    '
}

toml_array_strings() { # $1 = file, $2 = section, $3 = key
    toml_without_comments "$1" | awk -v wanted_section="$2" -v wanted_key="$3" '
        function trim(value) {
            sub(/^[[:space:]]+/, "", value)
            sub(/[[:space:]]+$/, "", value)
            return value
        }
        /^[[:space:]]*\[/ && ! collecting {
            section = $0
            gsub(/[[:space:]]/, "", section)
            in_section = section == "[" wanted_section "]"
            next
        }
        in_section && ! collecting {
            equals = index($0, "=")
            if (equals == 0) next
            key = trim(substr($0, 1, equals - 1))
            if (key != wanted_key) next
            collecting = 1
            print substr($0, equals + 1)
            if (index(substr($0, equals + 1), "]") != 0) exit
            next
        }
        collecting {
            print
            if (index($0, "]") != 0) exit
        }
    ' | grep -oE '"[^"]+"' | tr -d '"'
}

rustflags_enable_crt_static() { # arguments are Rust flag tokens
    local -a flags=("$@")
    local state=disabled
    local index flag features feature
    for index in "${!flags[@]}"; do
        flag="${flags[$index]}"
        features=""
        case "$flag" in
            -Ctarget-feature=*) features="${flag#-Ctarget-feature=}" ;;
            --codegen=target-feature=*) features="${flag#--codegen=target-feature=}" ;;
            -C|--codegen)
                if [ "$((index + 1))" -lt "${#flags[@]}" ] &&
                    [[ "${flags[$((index + 1))]}" == target-feature=* ]]; then
                    features="${flags[$((index + 1))]#target-feature=}"
                fi
                ;;
        esac
        [ -n "$features" ] || continue
        local -a feature_list=()
        IFS=',' read -r -a feature_list <<< "$features"
        for feature in "${feature_list[@]}"; do
            case "$feature" in
                +crt-static) state=enabled ;;
                -crt-static) state=disabled ;;
            esac
        done
    done
    [ "$state" = enabled ]
}

matches_policy_targets() { # arguments are actual values
    local -a actual=("$@")
    local -a expected=("${EXPECTED_TARGETS[@]}")
    [ "${#actual[@]}" -eq "${#expected[@]}" ] || return 1
    local index
    for index in "${!expected[@]}"; do
        [ "${actual[$index]}" = "${expected[$index]}" ] || return 1
    done
}

describe_array() {
    local IFS=', '
    printf '[%s]' "$*"
}

check_cargo_config() {
    local config="$REPO_ROOT/.cargo/config.toml"
    local default_target target linker rustflag
    local -a rustflags=()
    default_target=$(toml_value "$config" build target)
    if [ "$default_target" != "$EXPECTED_DEFAULT" ]; then
        fail "Cargo default target is '$default_target' (expected '$EXPECTED_DEFAULT')"
    fi
    SUPPORTED_TARGETS=()
    while IFS= read -r target; do
        SUPPORTED_TARGETS[${#SUPPORTED_TARGETS[@]}]="$target"
    done < <(
        sed -n -E \
            's/^[[:space:]]*\[target\.([^]]*-unknown-linux-musl)\][[:space:]]*$/\1/p' \
            "$config" | sort -u
    )
    if ! matches_policy_targets "${SUPPORTED_TARGETS[@]}"; then
        fail "supported musl target sections are $(describe_array "${SUPPORTED_TARGETS[@]}") (expected $(describe_array "${EXPECTED_TARGETS[@]}"))"
    fi
    for target in "${SUPPORTED_TARGETS[@]}"; do
        linker=$(toml_value "$config" "target.$target" linker)
        if [ "$linker" != "$target-gcc" ]; then
            fail "$target linker is '$linker' (expected '$target-gcc')"
        fi
        rustflags=()
        while IFS= read -r rustflag; do
            rustflags[${#rustflags[@]}]="$rustflag"
        done < <(toml_array_strings "$config" "target.$target" rustflags)
        if ! rustflags_enable_crt_static "${rustflags[@]}"; then
            fail "$target rustflags do not enable crt-static"
        fi
    done
}

check_rust_targets() {
    local rust_target
    local -a rust_targets=()
    while IFS= read -r rust_target; do
        rust_targets[${#rust_targets[@]}]="$rust_target"
    done < <(
        toml_array_strings "$REPO_ROOT/rust-toolchain.toml" toolchain targets | sort -u
    )
    if [ "$(describe_array "${rust_targets[@]}")" != "$(describe_array "${SUPPORTED_TARGETS[@]}")" ]; then
        fail "Rust std targets are $(describe_array "${rust_targets[@]}") (expected $(describe_array "${SUPPORTED_TARGETS[@]}") from .cargo/config.toml)"
    fi
}

declared_target_for_machine() { # $1 = machine prefix
    local target
    for target in "${SUPPORTED_TARGETS[@]}"; do
        if [[ "$target" == "$1-unknown-linux-musl" ]]; then
            printf '%s\n' "$target"
            return 0
        fi
    done
    return 1
}

check_nix_mapping() { # $1 = Nix system, $2 = expected target
    local system="$1"
    local expected_target="$2"
    local output
    local status
    set +e
    output=$(RCP_BUILD_TARGET_CHECK_ROOT="$REPO_ROOT" \
        RCP_BUILD_TARGET_CHECK_SYSTEM="$system" \
        "$NIX_BIN" eval --impure --raw --expr '
            let
              root = builtins.toPath (builtins.getEnv "RCP_BUILD_TARGET_CHECK_ROOT");
              system = builtins.getEnv "RCP_BUILD_TARGET_CHECK_SYSTEM";
              mapped = import (root + "/nix/target-platform.nix") { inherit system; };
              exportedTargets = [
                mapped.cargoTarget
                mapped.environment.CARGO_BUILD_TARGET
              ] ++ mapped.rustTargets;
            in
              builtins.concatStringsSep "\n" exportedTargets
        ' 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 0 ]; then
        fail "could not evaluate Nix mapping for $system: $output"
        return
    fi
    local exported_target
    local -a exported_targets=()
    while IFS= read -r exported_target; do
        exported_targets[${#exported_targets[@]}]="$exported_target"
    done <<< "$output"
    local -a expected_targets=("$expected_target" "$expected_target" "$expected_target")
    if [ "$(describe_array "${exported_targets[@]}")" != \
        "$(describe_array "${expected_targets[@]}")" ]; then
        fail "Nix mapping for $system exports $(describe_array "${exported_targets[@]}") (expected cargoTarget, environment.CARGO_BUILD_TARGET, and rustTargets to all be '$expected_target')"
    fi
}

create_fake_machine_tools() { # $1 = bin directory
    local bin_dir="$1"
    mkdir -p "$bin_dir"
    cat > "$bin_dir/uname" <<'FAKE'
#!/bin/bash
set -euo pipefail
case "$1" in
    -s) printf '%s\n' Linux ;;
    -m) printf '%s\n' "$RCP_BUILD_TARGET_MACHINE" ;;
    *) exit 2 ;;
esac
FAKE
    cat > "$bin_dir/cargo" <<'FAKE'
#!/bin/bash
set -euo pipefail
printf '%s\n' "${CARGO_BUILD_TARGET-}"
FAKE
    cat > "$bin_dir/docker" <<'FAKE'
#!/bin/bash
set -euo pipefail
if [ -n "${DOCKER_DEFAULT_PLATFORM:-}" ]; then
    echo "Docker command must not be called while DOCKER_DEFAULT_PLATFORM is set" >&2
    exit 97
fi
if [ "$#" -eq 3 ] && [ "$1" = info ] && [ "$2" = --format ]; then
    printf '%s\n' "$RCP_BUILD_TARGET_DOCKER_ARCH"
else
    exit 2
fi
FAKE
    chmod +x "$bin_dir/uname" "$bin_dir/cargo" "$bin_dir/docker"
}

check_host_mapping() { # $1 = machine, $2 = expected target, $3 = fake bin
    local machine="$1"
    local expected_target="$2"
    local bin_dir="$3"
    local output
    local status
    set +e
    output=$(env -u CARGO_BUILD_TARGET \
        "PATH=$bin_dir:$PATH" \
        "CARGO=$bin_dir/cargo" \
        "RCP_BUILD_TARGET_MACHINE=$machine" \
        "$REPO_ROOT/scripts/cargo-host.sh" check 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 0 ]; then
        fail "could not execute host Cargo mapping for $machine: $output"
    elif [ "$output" != "$expected_target" ]; then
        fail "host Cargo mapping for $machine returned '$output' (expected '$expected_target')"
    fi
}

check_docker_platform() { # $1 = platform, $2 = expected target, $3 = fake bin
    local platform="$1"
    local expected_target="$2"
    local bin_dir="$3"
    local output
    local status
    set +e
    output=$(env -u RCP_DOCKER_TARGET \
        "DOCKER_DEFAULT_PLATFORM=$platform" \
        "DOCKER=$bin_dir/docker" \
        "$REPO_ROOT/scripts/docker-target.sh" 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 0 ]; then
        fail "could not execute Docker resolver for $platform: $output"
    elif [ "$output" != "$expected_target" ]; then
        fail "Docker resolver for $platform returned '$output' (expected '$expected_target')"
    fi
}

check_docker_architecture() { # $1 = architecture, $2 = expected target, $3 = fake bin
    local architecture="$1"
    local expected_target="$2"
    local bin_dir="$3"
    local output
    local status
    set +e
    output=$(env -u DOCKER_DEFAULT_PLATFORM -u RCP_DOCKER_TARGET \
        "DOCKER=$bin_dir/docker" \
        "RCP_BUILD_TARGET_DOCKER_ARCH=$architecture" \
        "$REPO_ROOT/scripts/docker-target.sh" 2>&1)
    status=$?
    set -e
    if [ "$status" -ne 0 ]; then
        fail "could not execute Docker resolver for daemon architecture $architecture: $output"
    elif [ "$output" != "$expected_target" ]; then
        fail "Docker resolver for daemon architecture $architecture returned '$output' (expected '$expected_target')"
    fi
}

check_depot_runner_mapping() { # $1 = runner arch, remaining args = expected output values
    local architecture="$1"
    local expected_target="$2"
    local expected_linker="$3"
    local expected_asset="$4"
    local expected_checksum="$5"
    local helper="$REPO_ROOT/.depot/actions/rcp-rust-setup/runner-architecture.sh"
    local output
    local status
    local expected_output
    set +e
    output=$("$helper" "$architecture" 2> /dev/null)
    status=$?
    set -e
    expected_output=$(printf \
        'rust_target=%s\nlinker_alias=%s\ndprint_asset=%s\ndprint_checksum=%s' \
        "$expected_target" "$expected_linker" "$expected_asset" "$expected_checksum")
    if [ "$status" -ne 0 ] || [ "$output" != "$expected_output" ]; then
        fail "Depot runner mapping for $architecture returned status $status and output $(printf '%q' "$output") (expected $(printf '%q' "$expected_output"))"
    fi
}

check_depot_unsupported_architecture() {
    local helper="$REPO_ROOT/.depot/actions/rcp-rust-setup/runner-architecture.sh"
    local architecture
    local output
    local status
    for architecture in X86 ARM RISCV64; do
        set +e
        output=$("$helper" "$architecture" 2>&1)
        status=$?
        set -e
        if [ "$status" -eq 0 ]; then
            fail "Depot runner mapping accepts unsupported architecture $architecture"
        elif [[ "$output" != *"unsupported runner architecture: $architecture"* ]]; then
            fail "Depot unsupported runner diagnostic for $architecture was '$output'"
        fi
    done
}

depot_action_step_by_prefix() { # $1 = active YAML line prefix
    local prefix="$1"
    awk -v prefix="$prefix" '
        function flush() {
            if (matched) printf "%s", block
        }
        /^  - / {
            flush()
            block = ""
            matched = 0
        }
        {
            block = block $0 ORS
            if (index($0, prefix) == 1) matched = 1
        }
        END { flush() }
    ' "$REPO_ROOT/.depot/actions/rcp-rust-setup/action.yml"
}

depot_action_run_commands() {
    awk '
        $0 == "    run: |" {
            in_run = 1
            next
        }
        in_run && $0 ~ /^[[:space:]]*$/ { next }
        in_run && $0 ~ /^      / {
            line = substr($0, 7)
            sub(/^[[:space:]]+/, "", line)
            sub(/[[:space:]]+$/, "", line)
            if (line == "" || line ~ /^#/) next
            continued = line ~ /\\$/
            sub(/[[:space:]]*\\$/, "", line)
            if (command == "") command = line
            else command = command " " line
            if (!continued) {
                print command
                command = ""
            }
            next
        }
        in_run { exit }
        END {
            if (command != "") print command
        }
    '
}

require_one_action_step() { # $1 = prefix, $2 = diagnostic
    local prefix="$1"
    local diagnostic="$2"
    local count
    count=$(grep -Fxc -- "$prefix" \
        "$REPO_ROOT/.depot/actions/rcp-rust-setup/action.yml" || true)
    if [ "$count" -ne 1 ]; then
        fail "$diagnostic"
        return 1
    fi
}

require_action_key_count() { # $1 = step block, $2 = indent, $3 = key, $4 = count, $5 = diagnostic
    local block="$1"
    local prefix="$2$3:"
    local expected_count="$4"
    local diagnostic="$5"
    local actual_count
    actual_count=$(awk -v prefix="$prefix" '
        index($0, prefix) == 1 { count++ }
        END { print count + 0 }
    ' <<< "$block")
    if [ "$actual_count" -ne "$expected_count" ]; then
        fail "$diagnostic"
    fi
}

depot_action_mapping_block() { # $1 = step block, $2 = direct mapping key
    local block="$1"
    local mapping="$2"
    awk -v header="    $mapping:" '
        $0 == header {
            in_mapping = 1
            next
        }
        !in_mapping { next }
        /^[[:space:]]*($|#)/ { next }
        /^      / {
            print
            next
        }
        { exit }
    ' <<< "$block"
}

check_depot_action() {
    local block
    local commands
    local count
    local mapping
    local operation_count

    require_one_action_step '    id: architecture' \
        'Depot setup action must have exactly one architecture mapping step' || true
    block=$(depot_action_step_by_prefix '    id: architecture')
    require_action_key_count "$block" '    ' id 1 \
        'Depot setup action must have exactly one architecture mapping step'
    require_action_key_count "$block" '    ' shell 1 \
        'Depot setup action architecture mapping must use Bash'
    require_action_key_count "$block" '    ' run 1 \
        'Depot setup action architecture mapping must declare exactly one run block'
    require_action_key_count "$block" '    ' if 0 \
        'Depot setup action conditionally skips the runner architecture mapping'
    if ! grep -Fqx -- '    id: architecture' <<< "$block"; then
        fail 'Depot setup action must have exactly one architecture mapping step'
    fi
    if ! grep -Fqx -- '    shell: bash' <<< "$block"; then
        fail 'Depot setup action architecture mapping must use Bash'
    fi
    if ! grep -Fqx -- '    run: |' <<< "$block"; then
        fail 'Depot setup action architecture mapping must declare exactly one run block'
    fi
    commands=$(depot_action_run_commands <<< "$block")
    if [ "$commands" != \
        '"$GITHUB_ACTION_PATH/runner-architecture.sh" "$RUNNER_ARCH" >>"$GITHUB_OUTPUT"' ]; then
        fail 'Depot setup action does not execute the runner architecture mapping'
    fi

    count=$(grep -Ec \
        '^    uses: actions-rust-lang/setup-rust-toolchain@' \
        "$REPO_ROOT/.depot/actions/rcp-rust-setup/action.yml" || true)
    if [ "$count" -ne 1 ]; then
        fail 'Depot setup action must have exactly one active Rust setup step'
    fi
    block=$(depot_action_step_by_prefix \
        '    uses: actions-rust-lang/setup-rust-toolchain@')
    require_action_key_count "$block" '    ' uses 1 \
        'Depot setup action must have exactly one active Rust setup step'
    require_action_key_count "$block" '    ' with 1 \
        'Depot setup action Rust setup must declare exactly one with mapping'
    require_action_key_count "$block" '    ' if 0 \
        'Depot setup action conditionally skips Rust setup'
    count=$(grep -Ec \
        '^    uses: actions-rust-lang/setup-rust-toolchain@' \
        <<< "$block" || true)
    if [ "$count" -ne 1 ]; then
        fail 'Depot setup action must have exactly one active Rust setup step'
    fi
    if ! grep -Fqx -- '    with:' <<< "$block"; then
        fail 'Depot setup action Rust setup must declare exactly one with mapping'
    fi
    mapping=$(depot_action_mapping_block "$block" with)
    require_action_key_count "$mapping" '      ' target 1 \
        'Depot setup action must declare exactly one Rust target under with'
    count=$(grep -Fxc -- \
        '      target: ${{ steps.architecture.outputs.rust_target }}' \
        <<< "$mapping" || true)
    if [ "$count" -ne 1 ]; then
        fail 'Depot setup action does not thread rust_target'
    fi

    require_one_action_step '  - name: Install system dependencies' \
        'Depot setup action must have exactly one system dependency step' || true
    block=$(depot_action_step_by_prefix '  - name: Install system dependencies')
    require_action_key_count "$block" '    ' if 0 \
        'Depot setup action conditionally skips system dependencies'
    require_action_key_count "$block" '    ' shell 1 \
        'Depot setup action system dependencies must use Bash'
    require_action_key_count "$block" '    ' run 1 \
        'Depot setup action system dependencies must declare exactly one run block'
    if ! grep -Fqx -- '    shell: bash' <<< "$block"; then
        fail 'Depot setup action system dependencies must use Bash'
    fi
    if ! grep -Fqx -- '    run: |' <<< "$block"; then
        fail 'Depot setup action system dependencies must declare exactly one run block'
    fi
    commands=$(depot_action_run_commands <<< "$block")
    count=$(grep -Fxc -- \
        'sudo ln -sf /usr/bin/musl-gcc "/usr/bin/${{ steps.architecture.outputs.linker_alias }}"' \
        <<< "$commands" || true)
    operation_count=$(grep -Fc -- 'sudo ln -sf /usr/bin/musl-gcc' \
        <<< "$commands" || true)
    if [ "$count" -ne 1 ] || [ "$operation_count" -ne 1 ]; then
        fail 'Depot setup action does not thread linker_alias'
    fi

    require_one_action_step '  - name: Install dprint' \
        'Depot setup action must have exactly one dprint step' || true
    block=$(depot_action_step_by_prefix '  - name: Install dprint')
    require_action_key_count "$block" '    ' if 1 \
        'Depot setup action must declare exactly one dprint condition'
    require_action_key_count "$block" '    ' shell 1 \
        'Depot setup action dprint installation must use Bash'
    require_action_key_count "$block" '    ' run 1 \
        'Depot setup action dprint installation must declare exactly one run block'
    count=$(grep -Fxc -- "    if: inputs.install-dprint == 'true'" \
        <<< "$block" || true)
    if [ "$count" -ne 1 ]; then
        fail 'Depot setup action dprint condition is not pinned to its input'
    fi
    if ! grep -Fqx -- '    shell: bash' <<< "$block"; then
        fail 'Depot setup action dprint installation must use Bash'
    fi
    if ! grep -Fqx -- '    run: |' <<< "$block"; then
        fail 'Depot setup action dprint installation must declare exactly one run block'
    fi
    commands=$(depot_action_run_commands <<< "$block")
    count=$(grep -Fxc -- \
        'curl -fsSL -o /tmp/dprint.zip "https://github.com/dprint/dprint/releases/download/0.54.0/${{ steps.architecture.outputs.dprint_asset }}"' \
        <<< "$commands" || true)
    operation_count=$(grep -Fc -- 'curl -fsSL -o /tmp/dprint.zip' \
        <<< "$commands" || true)
    if [ "$count" -ne 1 ] || [ "$operation_count" -ne 1 ]; then
        fail 'Depot setup action does not thread dprint_asset'
    fi
    count=$(grep -Fxc -- \
        'echo "${{ steps.architecture.outputs.dprint_checksum }}  /tmp/dprint.zip" | sha256sum -c -' \
        <<< "$commands" || true)
    operation_count=$(grep -Fc -- 'sha256sum -c -' <<< "$commands" || true)
    if [ "$count" -ne 1 ] || [ "$operation_count" -ne 1 ]; then
        fail 'Depot setup action does not thread dprint_checksum'
    fi
}

depot_job_block() { # $1 = job id
    local job="$1"
    awk -v wanted="$job" '
        $0 == "  " wanted ":" {
            inside = 1
        }
        inside {
            if (seen && $0 ~ /^  [[:alnum:]_-]+:[[:space:]]*$/) exit
            print
            seen = 1
        }
    ' "$REPO_ROOT/.depot/workflows/ci.yml"
}

depot_runner_matrix_values() {
    awk '
        function active_line() {
            return $0 !~ /^[[:space:]]*($|#)/
        }
        $0 == "    strategy:" {
            in_strategy = 1
            in_matrix = 0
            in_runner = 0
            next
        }
        in_strategy && $0 == "      matrix:" {
            in_matrix = 1
            in_runner = 0
            next
        }
        in_matrix && $0 == "        runner:" {
            in_runner = 1
            next
        }
        in_runner && active_line() {
            value = $0
            sub(/^[[:space:]]*-[[:space:]]*/, "", value)
            if (value != $0) {
                print value
                next
            }
            in_runner = 0
        }
        active_line() {
            indent = match($0, /[^ ]/) - 1
            if (in_matrix && indent <= 6) {
                in_matrix = 0
                in_runner = 0
            }
            if (in_strategy && indent <= 4) {
                in_strategy = 0
                in_matrix = 0
                in_runner = 0
            }
        }
    '
}

check_depot_matrix_job() { # $1 = job id
    local job="$1"
    local block
    block=$(depot_job_block "$job")
    local strategy_count
    local matrix_count
    local runner_count
    strategy_count=$(grep -Ec -- '^    strategy:[[:space:]]*' \
        <<< "$block" || true)
    matrix_count=$(grep -Ec -- '^      matrix:[[:space:]]*' \
        <<< "$block" || true)
    runner_count=$(grep -Ec -- '^        runner:[[:space:]]*' \
        <<< "$block" || true)
    if [ "$strategy_count" -ne 1 ] || [ "$matrix_count" -ne 1 ] ||
        [ "$runner_count" -ne 1 ]; then
        fail "Depot $job must declare exactly one strategy.matrix.runner"
    fi
    local runner
    local -a runners=()
    while IFS= read -r runner; do
        runners[${#runners[@]}]="$runner"
    done < <(depot_runner_matrix_values <<< "$block")
    local -a expected=(depot-ubuntu-24.04-16 depot-ubuntu-24.04-arm-16)
    if [ "$(describe_array "${runners[@]}")" != "$(describe_array "${expected[@]}")" ]; then
        fail "Depot $job runner matrix is $(describe_array "${runners[@]}") (expected $(describe_array "${expected[@]}"))"
    fi
    local runs_on_count
    runs_on_count=$(grep -Ec -- '^    runs-on:[[:space:]]*' \
        <<< "$block" || true)
    if [ "$runs_on_count" -ne 1 ]; then
        fail "Depot $job must declare exactly one runs-on"
    elif ! grep -Fqx -- '    runs-on: ${{ matrix.runner }}' <<< "$block"; then
        fail "Depot $job does not run on its runner matrix"
    fi
}

check_depot_x86_job() { # $1 = job id, $2 = expected runner
    local job="$1"
    local expected_runner="$2"
    local block
    block=$(depot_job_block "$job")
    if grep -Eq '^[[:space:]]*[^#].*depot-ubuntu-24.04-arm-16' <<< "$block"; then
        fail "Depot Arm64 runner appears outside test and docker-test in job $job"
    fi
    local runs_on_count
    runs_on_count=$(grep -Ec -- '^    runs-on:[[:space:]]*' \
        <<< "$block" || true)
    if [ "$runs_on_count" -ne 1 ]; then
        fail "Depot $job must declare exactly one runs-on"
    elif ! grep -Fqx -- "    runs-on: $expected_runner" <<< "$block"; then
        fail "Depot $job runner is not $expected_runner"
    fi
}

check_depot_workflow_semantics() {
    local output
    local status
    set +e
    output=$(python3 - "$REPO_ROOT/.depot/workflows/ci.yml" 2>&1 <<'PYTHON'
import sys

import yaml


def reject(message):
    print(message)
    raise SystemExit(1)


def has_run_shell_default(owner):
    defaults = owner.get("defaults")
    if not isinstance(defaults, dict):
        return False
    run_defaults = defaults.get("run")
    return isinstance(run_defaults, dict) and "shell" in run_defaults


with open(sys.argv[1], encoding="utf-8") as workflow_file:
    workflow = yaml.safe_load(workflow_file)

jobs = workflow.get("jobs") or {}
for matrix_job_name in ("test", "docker-test"):
    matrix_job = jobs.get(matrix_job_name)
    if not isinstance(matrix_job, dict):
        continue
    strategy = matrix_job.get("strategy")
    if not isinstance(strategy, dict):
        continue
    matrix = strategy.get("matrix")
    if isinstance(matrix, dict) and "exclude" in matrix:
        reject(f"Depot {matrix_job_name} runner matrix must not use exclude")

test_job = jobs.get("test")
if not isinstance(test_job, dict):
    reject("Depot test job is missing")
if has_run_shell_default(workflow) or has_run_shell_default(test_job):
    reject("Depot test Arm Nix smoke build must not inherit a custom shell")
if "if" in test_job:
    reject("Depot test job must not be conditional")
if test_job.get("continue-on-error") not in (None, False):
    reject("Depot test job must gate Arm Nix smoke failures")

steps = test_job.get("steps")
if not isinstance(steps, list):
    reject("Depot test Arm Nix smoke must have exactly one Nix installer step")
steps = [step for step in steps if isinstance(step, dict)]

installer_prefix = "DeterminateSystems/nix-installer-action@"
installer_pin = (
    "DeterminateSystems/nix-installer-action@"
    "ef8a148080ab6020fd15196c2084a2eea5ff2d25"
)
installers = [
    (index, step)
    for index, step in enumerate(steps)
    if str(step.get("uses", "")).startswith(installer_prefix)
]
if len(installers) != 1:
    reject("Depot test Arm Nix smoke must have exactly one Nix installer step")
installer_index, installer = installers[0]
if installer.get("uses") != installer_pin:
    reject("Depot test Arm Nix smoke installer is not pinned")
arm_condition = "runner.arch == 'ARM64'"
if installer.get("if") != arm_condition:
    reject("Depot test Arm Nix smoke installer condition must be runner.arch ARM64")
if installer.get("continue-on-error") not in (None, False):
    reject("Depot test Arm Nix smoke installer must gate the job")

expected_command = (
    "nix build --no-update-lock-file "
    ".#checks.aarch64-linux.package-abi-smoke"
)
smoke_steps = [
    (index, step)
    for index, step in enumerate(steps)
    if "package-abi-smoke" in str(step.get("run", ""))
]
if len(smoke_steps) != 1:
    reject("Depot test must have exactly one Arm Nix package ABI smoke build step")
smoke_index, smoke = smoke_steps[0]
if str(smoke.get("run", "")).strip() != expected_command:
    reject("Depot test Arm Nix package ABI smoke command is not exact")
if "shell" in smoke:
    reject("Depot test Arm Nix smoke build must not override its shell")
if smoke.get("if") != arm_condition:
    reject("Depot test Arm Nix smoke build condition must be runner.arch ARM64")
if smoke.get("continue-on-error") not in (None, False):
    reject("Depot test Arm Nix smoke build must gate the job")
if installer_index >= smoke_index:
    reject("Depot test Arm Nix smoke installer must run before the build")

for step in steps:
    run = step.get("run")
    if not isinstance(run, str):
        continue
    active_lines = [
        line.strip()
        for line in run.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if any("test-nix-targets.sh" in line for line in active_lines):
        reject("Depot test must not run the full Nix target suite")
PYTHON
    )
    status=$?
    set -e
    if [ "$status" -ne 0 ]; then
        fail "$output"
    fi
}

check_depot_workflow() {
    check_depot_matrix_job test
    check_depot_matrix_job docker-test
    check_depot_workflow_semantics
    check_depot_x86_job lint depot-ubuntu-24.04-4
    check_depot_x86_job doc depot-ubuntu-24.04-4
    check_depot_x86_job doctest depot-ubuntu-24.04-4
    check_depot_x86_job test-release depot-ubuntu-24.04-16
    check_depot_x86_job doctest-release depot-ubuntu-24.04-4
    check_depot_x86_job docker-chaos-test depot-ubuntu-24.04-16

    local arm_runner_count
    arm_runner_count=$(awk '
        $0 !~ /^[[:space:]]*#/ && $0 ~ /depot-ubuntu-24.04-arm-16/ { count++ }
        END { print count + 0 }
    ' "$REPO_ROOT/.depot/workflows/ci.yml")
    if [ "$arm_runner_count" -ne 2 ]; then
        fail "Depot Arm64 runner appears $arm_runner_count times (expected only test and docker-test)"
    fi
}

check_github_nix_workflow() {
    local output
    local status
    set +e
    output=$(python3 - "$REPO_ROOT/.github/workflows/validate.yml" 2>&1 <<'PYTHON'
import sys

import yaml


def reject(message):
    print(message)
    raise SystemExit(1)


def has_run_shell_default(owner):
    defaults = owner.get("defaults")
    if not isinstance(defaults, dict):
        return False
    run_defaults = defaults.get("run")
    return isinstance(run_defaults, dict) and "shell" in run_defaults


with open(sys.argv[1], encoding="utf-8") as workflow_file:
    workflow = yaml.safe_load(workflow_file)

jobs = workflow.get("jobs") or {}
nix_job = jobs.get("nix")
if not isinstance(nix_job, dict):
    reject("GitHub Nix job is missing")
if has_run_shell_default(workflow) or has_run_shell_default(nix_job):
    reject("GitHub Nix all-systems flake check must not inherit a custom shell")
if "if" in nix_job:
    reject("GitHub Nix job must not be conditional")
if nix_job.get("continue-on-error") not in (None, False):
    reject("GitHub Nix job must gate all-systems evaluation failures")

steps = nix_job.get("steps")
if not isinstance(steps, list):
    reject("GitHub Nix workflow must have exactly one all-systems flake check step")
steps = [step for step in steps if isinstance(step, dict)]
flake_steps = [
    step
    for step in steps
    if "nix flake check" in str(step.get("run", ""))
]
if len(flake_steps) != 1:
    reject("GitHub Nix workflow must have exactly one all-systems flake check step")

flake_step = flake_steps[0]
expected_command = "nix flake check --all-systems --no-build --no-update-lock-file -L"
if str(flake_step.get("run", "")).strip() != expected_command:
    reject("GitHub Nix all-systems flake check command is not exact")
if "if" in flake_step:
    reject("GitHub Nix all-systems flake check must not be conditional")
if "shell" in flake_step:
    reject("GitHub Nix all-systems flake check must not override its shell")
if flake_step.get("continue-on-error") not in (None, False):
    reject("GitHub Nix all-systems flake check must gate the job")
PYTHON
    )
    status=$?
    set -e
    if [ "$status" -ne 0 ]; then
        fail "$output"
    fi
}

for required in .cargo/config.toml rust-toolchain.toml nix/target-platform.nix \
    scripts/cargo-host.sh scripts/docker-target.sh \
    .depot/actions/rcp-rust-setup/action.yml \
    .depot/actions/rcp-rust-setup/runner-architecture.sh \
    .depot/workflows/ci.yml .github/workflows/validate.yml; do
    require_file "$required" || true
done
if [ "$FAILED" -ne 0 ]; then
    exit 1
fi

check_yaml_duplicates
if [ "$FAILED" -ne 0 ]; then
    exit 1
fi

check_cargo_config
check_rust_targets
X86_TARGET="$(declared_target_for_machine x86_64)"
ARM_TARGET="$(declared_target_for_machine aarch64)"
if [ "$SKIP_NIX_EVAL" -eq 0 ]; then
    check_nix_mapping x86_64-linux "$X86_TARGET"
    check_nix_mapping aarch64-linux "$ARM_TARGET"
fi

TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT
create_fake_machine_tools "$TEMP_DIR/bin"
check_host_mapping x86_64 "$X86_TARGET" "$TEMP_DIR/bin"
check_host_mapping aarch64 "$ARM_TARGET" "$TEMP_DIR/bin"
check_docker_platform linux/amd64 "$X86_TARGET" "$TEMP_DIR/bin"
check_docker_platform linux/arm64 "$ARM_TARGET" "$TEMP_DIR/bin"
check_docker_platform linux/arm64/v8 "$ARM_TARGET" "$TEMP_DIR/bin"
check_docker_architecture amd64 "$X86_TARGET" "$TEMP_DIR/bin"
check_docker_architecture x86_64 "$X86_TARGET" "$TEMP_DIR/bin"
check_docker_architecture arm64 "$ARM_TARGET" "$TEMP_DIR/bin"
check_docker_architecture aarch64 "$ARM_TARGET" "$TEMP_DIR/bin"
check_depot_runner_mapping X64 "$X86_TARGET" "$X86_TARGET-gcc" \
    dprint-x86_64-unknown-linux-gnu.zip \
    8cb5925a0d6d0d8aa74c82a00f76734577592dfa1eda9517c261a84fe06accd7
check_depot_runner_mapping ARM64 "$ARM_TARGET" "$ARM_TARGET-gcc" \
    dprint-aarch64-unknown-linux-gnu.zip \
    6b86329e17678ff3358f88d69a3774d371b601c665cc8cebbf2a4e1234a6d289
check_depot_unsupported_architecture
check_depot_action
check_depot_workflow
check_github_nix_workflow

if [ "$FAILED" -ne 0 ]; then
    exit 1
fi
printf 'build target declarations are consistent: %s, %s (raw Cargo default: %s)\n' \
    "${SUPPORTED_TARGETS[0]}" "${SUPPORTED_TARGETS[1]}" "$EXPECTED_DEFAULT"
