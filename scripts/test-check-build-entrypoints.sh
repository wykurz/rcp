#!/bin/bash
# tests the lexical build-entrypoint tripwire against isolated Git repositories.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECKER="$SCRIPT_DIR/check-build-entrypoints.sh"
CHECKER_SHELL="${RCP_BUILD_ENTRYPOINT_TEST_CHECKER_SHELL:-}"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT

FAILED=0
CASE_ROOT=''
ALLOWLIST=''

fail() {
    echo "FAIL: $*" >&2
    FAILED=1
}

new_case() { # $1 = label
    CASE_ROOT="$TEMP_DIR/$1"
    ALLOWLIST="$CASE_ROOT/approved.allow"
    mkdir -p "$CASE_ROOT"
    git -C "$CASE_ROOT" init -q
    : > "$ALLOWLIST"
}

write_case_file() { # $1 = relative path; body on stdin
    local relative_path="$1"
    mkdir -p "$(dirname "$CASE_ROOT/$relative_path")"
    cat > "$CASE_ROOT/$relative_path"
}

invoke_checker() {
    if [ -n "$CHECKER_SHELL" ]; then
        "$CHECKER_SHELL" "$CHECKER" "$@"
    else
        "$CHECKER" "$@"
    fi
}

run_checker() { # $1 = output path
    local output_path="$1"
    set +e
    invoke_checker --root "$CASE_ROOT" --allowlist "$ALLOWLIST" \
        > "$output_path" 2>&1
    local status=$?
    set -e
    return "$status"
}

expect_result() { # $1 = status, $2 = label, $3 = diagnostic
    local expected_status="$1"
    local label="$2"
    local diagnostic="$3"
    local output_path="$TEMP_DIR/output-$label"
    local status
    if run_checker "$output_path"; then
        status=0
    else
        status=$?
    fi
    if [ "$status" -ne "$expected_status" ]; then
        fail "$label: expected status $expected_status, got $status: $(cat "$output_path")"
    elif [ -n "$diagnostic" ] && ! grep -Fq -- "$diagnostic" "$output_path"; then
        fail "$label: missing diagnostic '$diagnostic': $(cat "$output_path")"
    fi
}

expect_clean() { expect_result 0 "$1" ''; }
expect_violation() { expect_result 1 "$1" "$2"; }

expect_clean_and_quiet() { # $1 = label
    local label="$1"
    local output_path="$TEMP_DIR/output-$label"
    local status
    if run_checker "$output_path"; then
        status=0
    else
        status=$?
    fi
    if [ "$status" -ne 0 ] || [ -s "$output_path" ]; then
        fail "$label: expected status 0 with no diagnostic, got status $status: $(cat "$output_path")"
    fi
}

expect_rejected_shell_line() { # $1 = label, $2 = active source line
    local label="$1"
    local line="$2"
    new_case "$label"
    printf '#!/bin/bash\n%s\n' "$line" | write_case_file ci/build.sh
    git -C "$CASE_ROOT" add ci/build.sh
    expect_violation "$label" 'unexpected build entrypoint'
}

echo "Testing lexical build-entrypoint checks..."

missing_root="$TEMP_DIR/missing-root"
set +e
missing_root_output=$(invoke_checker --root "$missing_root" 2>&1)
missing_root_status=$?
set -e
if [ "$missing_root_status" -ne 2 ] ||
    [[ "$missing_root_output" != *"repository root does not exist: $missing_root"* ]]; then
    fail "invalid root lost status/path: status=$missing_root_status output=$missing_root_output"
fi

new_case unexpected-raw-command
write_case_file ci/build.sh <<'FIXTURE'
#!/bin/bash
cargo b
FIXTURE
git -C "$CASE_ROOT" add ci/build.sh
expect_violation unexpected-raw-command 'unexpected build entrypoint'

new_case comments-are-inert
write_case_file ci/build.sh <<'FIXTURE'
#!/bin/bash
# cargo build
true # cross build
FIXTURE
write_case_file workflow.yml <<'FIXTURE'
# run: cargo test
jobs: {}
FIXTURE
write_case_file justfile <<'FIXTURE'
# cargo check
default:
    true
FIXTURE
git -C "$CASE_ROOT" add ci/build.sh workflow.yml justfile
expect_clean comments-are-inert

new_case exact-approval
write_case_file update-version.sh <<'FIXTURE'
#!/bin/bash
cargo update --workspace
FIXTURE
printf '%s\t%s\n' update-version.sh 'cargo update --workspace' > "$ALLOWLIST"
git -C "$CASE_ROOT" add update-version.sh
expect_clean exact-approval

new_case stale-approval
printf '%s\t%s\n' update-version.sh 'cargo update --workspace' > "$ALLOWLIST"
expect_violation stale-approval 'stale approved build entrypoint'

new_case pending-deletion
write_case_file removed.sh <<'FIXTURE'
#!/bin/bash
cargo b
FIXTURE
git -C "$CASE_ROOT" add removed.sh
rm "$CASE_ROOT/removed.sh"
expect_clean pending-deletion

new_case unreadable-extensionless-source
write_case_file ci/build <<'FIXTURE'
#!/bin/bash
cargo b
FIXTURE
git -C "$CASE_ROOT" add ci/build
chmod 000 "$CASE_ROOT/ci/build"
if [ -r "$CASE_ROOT/ci/build" ]; then
    # root and capability-bearing test users can still read mode-000 files.
    chmod 600 "$CASE_ROOT/ci/build"
else
    expect_violation unreadable-extensionless-source \
        'automation source is not readable: ci/build'
    chmod 600 "$CASE_ROOT/ci/build"
fi

new_case binary-extensionless-source
printf '\0binary data without a newline' | write_case_file assets/demo.bin
git -C "$CASE_ROOT" add assets/demo.bin
expect_clean_and_quiet binary-extensionless-source

expect_rejected_shell_line raw-alias 'cargo b'
expect_rejected_shell_line env-wrapper 'env cargo b'
expect_rejected_shell_line command-wrapper 'command cargo b'
expect_rejected_shell_line assigned-tool 'TOOL=cargo; "$TOOL" b'
expect_rejected_shell_line alias-definition "alias compile='cargo b'"
expect_rejected_shell_line eval-string "eval 'cargo b'"
expect_rejected_shell_line raw-cross 'cross b'
expect_rejected_shell_line absolute-cargo-path '/usr/bin/cargo b'
expect_rejected_shell_line relative-cargo-path './cargo b'
expect_rejected_shell_line assigned-cargo-path 'CARGO=/usr/bin/cargo'

new_case untracked-shell
write_case_file ci/build.sh <<'FIXTURE'
#!/bin/bash
cargo b
FIXTURE
expect_violation untracked-shell 'unexpected build entrypoint'

new_case envrc-shell-source
write_case_file .envrc <<'FIXTURE'
cargo b
FIXTURE
git -C "$CASE_ROOT" add .envrc
expect_violation envrc-shell-source 'unexpected build entrypoint'

new_case shell-shebang
write_case_file ci/build <<'FIXTURE'
#!/bin/bash
cargo b
FIXTURE
git -C "$CASE_ROOT" add ci/build
expect_violation shell-shebang 'unexpected build entrypoint'

new_case alternate-shell-shebang
write_case_file ci/build <<'FIXTURE'
#!/bin/dash
cargo b
FIXTURE
git -C "$CASE_ROOT" add ci/build
expect_violation alternate-shell-shebang 'unexpected build entrypoint'

new_case executable-automation
write_case_file ci/build-tool <<'FIXTURE'
cargo b
FIXTURE
chmod +x "$CASE_ROOT/ci/build-tool"
git -C "$CASE_ROOT" add ci/build-tool
expect_violation executable-automation 'unexpected build entrypoint'

new_case action-anywhere
write_case_file components/build/action.yml <<'FIXTURE'
name: Build
runs:
  using: composite
  steps:
    - shell: bash
      run: cargo b
FIXTURE
git -C "$CASE_ROOT" add components/build/action.yml
expect_violation action-anywhere 'unexpected build entrypoint'

new_case workflow-anywhere
write_case_file ci/pipeline.YAML <<'FIXTURE'
jobs:
  build:
    steps:
      - run: cargo b
FIXTURE
git -C "$CASE_ROOT" add ci/pipeline.YAML
expect_violation workflow-anywhere 'unexpected build entrypoint'

new_case additional-justfile
write_case_file justfile <<'FIXTURE'
default:
    true
FIXTURE
write_case_file nested/.justfile <<'FIXTURE'
build:
    true
FIXTURE
git -C "$CASE_ROOT" add justfile nested/.justfile
expect_violation additional-justfile 'additional Justfile is not allowed'

new_case just-module
write_case_file justfile <<'FIXTURE'
mod build
default:
    true
FIXTURE
write_case_file build.just <<'FIXTURE'
build:
    true
FIXTURE
git -C "$CASE_ROOT" add justfile build.just
expect_violation just-module 'Just modules are not allowed'

new_case just-interpolation
write_case_file justfile <<'FIXTURE'
tail := "; cargo b"
build:
    true {{tail}}
FIXTURE
git -C "$CASE_ROOT" add justfile
expect_violation just-interpolation 'Just interpolation is not allowed'

new_case just-backtick
write_case_file justfile <<'FIXTURE'
artifact := `cargo b`
build:
    true
FIXTURE
git -C "$CASE_ROOT" add justfile
expect_violation just-backtick 'evaluated Just assignment is not allowed'

new_case just-custom-shell
write_case_file justfile <<'FIXTURE'
set shell := ["pwsh", "-c"]
build:
    true
FIXTURE
git -C "$CASE_ROOT" add justfile
expect_violation just-custom-shell 'custom Just shell is not allowed'

for modifier in - @- -@; do
    label="just-modifier-${modifier//@/at}"
    label="${label//-/dash}"
    new_case "$label"
    printf 'build:\n    %scargo b\n' "$modifier" | write_case_file justfile
    git -C "$CASE_ROOT" add justfile
    expect_violation "$label" 'unexpected build entrypoint'
done

new_case nonfixture-test-is-scanned
write_case_file scripts/test-sneaky.sh <<'FIXTURE'
#!/bin/bash
cargo b
FIXTURE
git -C "$CASE_ROOT" add scripts/test-sneaky.sh
expect_violation nonfixture-test-is-scanned 'unexpected build entrypoint'

new_case arm-cross-builds
write_case_file .github/workflows/release.yml <<'FIXTURE'
jobs:
  deb:
    steps:
      - run: cross build --release --target=aarch64-unknown-linux-musl
  rpm:
    steps:
      - run: cross build --release --target=aarch64-unknown-linux-musl
FIXTURE
printf '%s\t%s\n%s\t%s\n' \
    .github/workflows/release.yml \
    '- run: cross build --release --target=aarch64-unknown-linux-musl' \
    .github/workflows/release.yml \
    '- run: cross build --release --target=aarch64-unknown-linux-musl' > "$ALLOWLIST"
git -C "$CASE_ROOT" add .github/workflows/release.yml
expect_clean arm-cross-builds

expect_bad_cross() { # $1 = label, $2 = run line
    local label="$1"
    local line="$2"
    new_case "$label"
    printf 'jobs:\n  build:\n    steps:\n      - %s\n' "$line" \
        | write_case_file .github/workflows/release.yml
    printf '%s\t- %s\n' .github/workflows/release.yml "$line" > "$ALLOWLIST"
    git -C "$CASE_ROOT" add .github/workflows/release.yml
    expect_violation "$label" 'ARM cross build requires a pre--- nonempty aarch64 musl target'
}

expect_bad_cross arm-cross-missing-target 'run: cross build --release'
expect_bad_cross arm-cross-empty-target 'run: cross build --release --target='
expect_bad_cross arm-cross-post-separator \
    'run: cross build --release -- --target=aarch64-unknown-linux-musl'
expect_bad_cross arm-cross-wrong-libc \
    'run: cross build --release --target=aarch64-unknown-linux-gnu'
expect_bad_cross arm-cross-absolute-path-missing-target \
    'run: /usr/bin/cross build --release'
expect_bad_cross arm-cross-relative-path-missing-target \
    'run: ./cross build --release'
expect_bad_cross arm-cross-quoted-absolute-path-missing-target \
    'run: "/usr/bin/cross" build --release'
expect_bad_cross arm-cross-quoted-relative-path-missing-target \
    "run: './cross' build --release"

new_case generate-rpm-explicit-target
write_case_file .github/workflows/release.yml <<'FIXTURE'
jobs:
  rpm:
    steps:
      - run: CARGO_BUILD_TARGET=x86_64-unknown-linux-musl ./scripts/cargo-host.sh generate-rpm --target=x86_64-unknown-linux-musl -p rcp
FIXTURE
git -C "$CASE_ROOT" add .github/workflows/release.yml
expect_clean generate-rpm-explicit-target

expect_bad_generate_rpm() { # $1 = label, $2 = run line
    local label="$1"
    local line="$2"
    new_case "$label"
    printf 'jobs:\n  rpm:\n    steps:\n      - %s\n' "$line" \
        | write_case_file .github/workflows/release.yml
    git -C "$CASE_ROOT" add .github/workflows/release.yml
    expect_violation "$label" 'cargo generate-rpm requires an explicit target matching CARGO_BUILD_TARGET'
}

expect_bad_generate_rpm generate-rpm-missing-target \
    'run: CARGO_BUILD_TARGET=x86_64-unknown-linux-musl ./scripts/cargo-host.sh generate-rpm -p rcp'
expect_bad_generate_rpm generate-rpm-empty-target \
    'run: CARGO_BUILD_TARGET=x86_64-unknown-linux-musl ./scripts/cargo-host.sh generate-rpm --target= -p rcp'
expect_bad_generate_rpm generate-rpm-wrong-target \
    'run: CARGO_BUILD_TARGET=x86_64-unknown-linux-musl ./scripts/cargo-host.sh generate-rpm --target=aarch64-unknown-linux-musl -p rcp'
expect_bad_generate_rpm generate-rpm-post-separator-target \
    'run: CARGO_BUILD_TARGET=x86_64-unknown-linux-musl ./scripts/cargo-host.sh generate-rpm -- -p rcp --target=x86_64-unknown-linux-musl'
expect_bad_generate_rpm generate-rpm-non-prefix-target-declaration \
    'run: ./scripts/cargo-host.sh generate-rpm CARGO_BUILD_TARGET=x86_64-unknown-linux-musl --target=x86_64-unknown-linux-musl -p rcp'
expect_bad_generate_rpm generate-rpm-duplicate-target \
    'run: CARGO_BUILD_TARGET=x86_64-unknown-linux-musl ./scripts/cargo-host.sh generate-rpm --target=x86_64-unknown-linux-musl --target=x86_64-unknown-linux-musl -p rcp'
expect_bad_generate_rpm generate-rpm-conflicting-target \
    'run: CARGO_BUILD_TARGET=x86_64-unknown-linux-musl ./scripts/cargo-host.sh generate-rpm --target=x86_64-unknown-linux-musl --target=aarch64-unknown-linux-musl -p rcp'
expect_bad_generate_rpm generate-rpm-empty-target-declaration \
    'run: CARGO_BUILD_TARGET= ./scripts/cargo-host.sh generate-rpm --target=x86_64-unknown-linux-musl -p rcp'
expect_bad_generate_rpm generate-rpm-quoted-empty-targets \
    'run: CARGO_BUILD_TARGET="" ./scripts/cargo-host.sh generate-rpm --target="" -p rcp'
expect_bad_generate_rpm generate-rpm-unsupported-matching-target \
    'run: CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu ./scripts/cargo-host.sh generate-rpm --target=x86_64-unknown-linux-gnu -p rcp'
expect_bad_generate_rpm generate-rpm-assignment-after-command \
    'run: command CARGO_BUILD_TARGET=x86_64-unknown-linux-musl ./scripts/cargo-host.sh generate-rpm --target=x86_64-unknown-linux-musl -p rcp'
expect_bad_generate_rpm generate-rpm-assignment-before-command-missing-target \
    'run: CARGO_BUILD_TARGET=x86_64-unknown-linux-musl command ./scripts/cargo-host.sh generate-rpm -p rcp'
expect_bad_generate_rpm generate-rpm-assignment-before-env-missing-target \
    'run: CARGO_BUILD_TARGET=x86_64-unknown-linux-musl env ./scripts/cargo-host.sh generate-rpm -p rcp'

new_case cross-build-description-is-inert
write_case_file .github/workflows/release.yml <<'FIXTURE'
jobs:
  build:
    name: ARM cross build
FIXTURE
printf '%s\t%s\n' .github/workflows/release.yml 'name: ARM cross build' > "$ALLOWLIST"
git -C "$CASE_ROOT" add .github/workflows/release.yml
expect_clean cross-build-description-is-inert

new_case publish-with-no-verify
write_case_file .github/workflows/publish.yml <<'FIXTURE'
jobs:
  publish:
    steps:
      - run: |
          cargo workspaces publish \
            --yes \
            --no-verify
FIXTURE
printf '%s\t%s\n' .github/workflows/publish.yml 'cargo workspaces publish \' > "$ALLOWLIST"
git -C "$CASE_ROOT" add .github/workflows/publish.yml
expect_clean publish-with-no-verify

new_case publish-missing-no-verify
write_case_file .github/workflows/publish.yml <<'FIXTURE'
jobs:
  publish:
    steps:
      - run: |
          cargo workspaces publish \
            --yes
FIXTURE
printf '%s\t%s\n' .github/workflows/publish.yml 'cargo workspaces publish \' > "$ALLOWLIST"
git -C "$CASE_ROOT" add .github/workflows/publish.yml
expect_violation publish-missing-no-verify 'workspace publication requires pre--- --no-verify'

new_case publish-repeated-whitespace-missing-no-verify
write_case_file .github/workflows/publish.yml <<'FIXTURE'
jobs:
  publish:
    steps:
      - run: cargo  workspaces   publish --yes
FIXTURE
printf '%s\t%s\n' .github/workflows/publish.yml \
    '- run: cargo  workspaces   publish --yes' > "$ALLOWLIST"
git -C "$CASE_ROOT" add .github/workflows/publish.yml
expect_violation publish-repeated-whitespace-missing-no-verify \
    'workspace publication requires pre--- --no-verify'

new_case publish-tabs-missing-no-verify
publish_line=$'- run: cargo\tworkspaces\tpublish --yes'
printf 'jobs:\n  publish:\n    steps:\n      %s\n' "$publish_line" \
    | write_case_file .github/workflows/publish.yml
printf '%s\t%s\n' .github/workflows/publish.yml "$publish_line" > "$ALLOWLIST"
git -C "$CASE_ROOT" add .github/workflows/publish.yml
expect_violation publish-tabs-missing-no-verify \
    'workspace publication requires pre--- --no-verify'

new_case publish-description-is-inert
write_case_file .github/workflows/publish.yml <<'FIXTURE'
jobs:
  publish:
    name: Explain cargo workspaces publish behavior
FIXTURE
printf '%s\t%s\n' .github/workflows/publish.yml \
    'name: Explain cargo workspaces publish behavior' > "$ALLOWLIST"
git -C "$CASE_ROOT" add .github/workflows/publish.yml
expect_clean publish-description-is-inert

new_case echoed-publish-command-is-inert
write_case_file .github/workflows/publish.yml <<'FIXTURE'
jobs:
  publish:
    steps:
      - run: echo "cargo workspaces publish"
FIXTURE
printf '%s\t%s\n' .github/workflows/publish.yml \
    '- run: echo "cargo workspaces publish"' > "$ALLOWLIST"
git -C "$CASE_ROOT" add .github/workflows/publish.yml
expect_clean echoed-publish-command-is-inert

new_case unquoted-echoed-publish-command-is-inert
write_case_file .github/workflows/publish.yml <<'FIXTURE'
jobs:
  publish:
    steps:
      - run: echo cargo workspaces publish now
FIXTURE
printf '%s\t%s\n' .github/workflows/publish.yml \
    '- run: echo cargo workspaces publish now' > "$ALLOWLIST"
git -C "$CASE_ROOT" add .github/workflows/publish.yml
expect_clean unquoted-echoed-publish-command-is-inert

new_case publish-post-separator-no-verify
write_case_file .github/workflows/publish.yml <<'FIXTURE'
jobs:
  publish:
    steps:
      - run: |
          cargo workspaces publish \
            -- \
            --no-verify
FIXTURE
printf '%s\t%s\n' .github/workflows/publish.yml 'cargo workspaces publish \' > "$ALLOWLIST"
git -C "$CASE_ROOT" add .github/workflows/publish.yml
expect_violation publish-post-separator-no-verify \
    'workspace publication requires pre--- --no-verify'

new_case wrapped-publish-missing-no-verify
write_case_file .github/workflows/release.yml <<'FIXTURE'
jobs:
  publish:
    steps:
      - run: |
          ./scripts/cargo-host.sh workspaces publish \
            --yes
FIXTURE
git -C "$CASE_ROOT" add .github/workflows/release.yml
expect_violation wrapped-publish-missing-no-verify \
    'workspace publication requires pre--- --no-verify'

new_case workspace-list
write_case_file .github/workflows/publish.yml <<'FIXTURE'
jobs:
  list:
    steps:
      - run: cargo workspaces list --all
FIXTURE
printf '%s\t%s\n' .github/workflows/publish.yml '- run: cargo workspaces list --all' > "$ALLOWLIST"
git -C "$CASE_ROOT" add .github/workflows/publish.yml
expect_clean workspace-list

if [ "$FAILED" -ne 0 ]; then
    exit 1
fi

echo "Lexical build-entrypoint tests passed"
