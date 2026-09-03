#!/bin/bash
# checks an exact lexical inventory of build-tool entrypoints in repository automation.
#
# This is deliberately a regression tripwire, not a shell-language proof. It collects active
# source lines containing standalone literal build-tool names and compares them with the reviewed
# inventory. Shell sources without shebangs use a small explicit suffix list. Target and
# publication semantics have separate focused checks below.

set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ALLOWLIST="$SCRIPT_DIR/build-entrypoints.allow"

while [ "$#" -gt 0 ]; do
    case "$1" in
        --root)
            [ "$#" -ge 2 ] || { echo 'missing value for --root' >&2; exit 2; }
            REPO_ROOT="$2"
            shift 2
            ;;
        --allowlist)
            [ "$#" -ge 2 ] || { echo 'missing value for --allowlist' >&2; exit 2; }
            ALLOWLIST="$2"
            shift 2
            ;;
        *)
            echo "unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

REQUESTED_ROOT="$REPO_ROOT"
if ! RESOLVED_ROOT="$(cd "$REQUESTED_ROOT" 2>/dev/null && pwd -P)"; then
    echo "repository root does not exist: $REQUESTED_ROOT" >&2
    exit 2
fi
REPO_ROOT="$RESOLVED_ROOT"
case "$ALLOWLIST" in
    /*) ;;
    *) ALLOWLIST="$REPO_ROOT/$ALLOWLIST" ;;
esac
if [ ! -r "$ALLOWLIST" ]; then
    echo "build-entrypoint allowlist is not readable: $ALLOWLIST" >&2
    exit 2
fi

TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT
PATHS="$TEMP_DIR/paths"
CANDIDATES="$TEMP_DIR/candidates"
APPROVED="$TEMP_DIR/approved"
UNEXPECTED="$TEMP_DIR/unexpected"
STALE="$TEMP_DIR/stale"
POLICY_ERRORS="$TEMP_DIR/policy-errors"
LOGICAL_LINES="$TEMP_DIR/logical-lines"
: > "$CANDIDATES"
: > "$POLICY_ERRORS"
: > "$LOGICAL_LINES"

if ! git -C "$REPO_ROOT" ls-files -z --cached --others --exclude-standard > "$PATHS"; then
    echo "could not enumerate repository files under $REPO_ROOT" >&2
    exit 2
fi

is_automation() { # $1 = relative path
    local relative_path="$1"
    local basename lower
    basename="${relative_path##*/}"
    lower="$(printf '%s' "$basename" | tr '[:upper:]' '[:lower:]')"
    case "$lower" in
        justfile | .justfile) return 0 ;;
    esac
    case "$lower" in
        .envrc | .profile | *.sh | *.bash | *.bashrc | *.zsh | *.zshrc | *.fish | \
            *.yaml | *.yml) return 0 ;;
    esac
    if [ -x "$REPO_ROOT/$relative_path" ]; then
        return 0
    fi
    IFS= read -r first_line < "$REPO_ROOT/$relative_path" || true
    case "$first_line" in
        '#!'*sh*) return 0 ;;
    esac
    return 1
}

check_just_source() { # $1 = relative path
    local relative_path="$1"
    local line
    if [ "$relative_path" != justfile ]; then
        echo "additional Justfile is not allowed: $relative_path" >> "$POLICY_ERRORS"
    fi
    while IFS= read -r line; do
        case "$line" in
            *'{{'* | *'}}'*)
                echo "Just interpolation is not allowed: $relative_path: $line" \
                    >> "$POLICY_ERRORS"
                ;;
        esac
        case "$line" in
            *'`'*)
                echo "evaluated Just assignment is not allowed: $relative_path: $line" \
                    >> "$POLICY_ERRORS"
                ;;
        esac
        if [[ "$line" =~ ^set[[:space:]]+(shell|windows-shell)[[:space:]]*:?= ]]; then
            echo "custom Just shell is not allowed: $relative_path: $line" \
                >> "$POLICY_ERRORS"
        fi
        if [[ "$line" =~ ^(mod|import)[[:space:]] ]]; then
            echo "Just modules are not allowed: $relative_path: $line" >> "$POLICY_ERRORS"
        fi
    done < <(
        LC_ALL=C awk '
            {
                line = $0
                sub(/\r$/, "", line)
                sub(/^[[:space:]]*/, "", line)
                if (line == "" || line ~ /^#/) next
                sub(/[[:space:]]+#.*$/, "", line)
                sub(/[[:space:]]+$/, "", line)
                if (line != "") print line
            }
        ' "$REPO_ROOT/$relative_path"
    )
}

is_fixture_test() { # $1 = relative path
    case "$1" in
        scripts/test-cargo-host.sh) return 0 ;;
        scripts/test-check-build-entrypoints.sh) return 0 ;;
        scripts/test-check-build-targets.sh) return 0 ;;
        scripts/test-docker-helpers.sh) return 0 ;;
        scripts/test-docker-target.sh) return 0 ;;
        scripts/test-nix-targets.sh) return 0 ;;
        scripts/test-update-deps.sh) return 0 ;;
        *) return 1 ;;
    esac
}

while IFS= read -r -d '' relative_path; do
    # These files contain inert fixture source or mock-tool diagnostics. Keep exclusions exact so
    # a newly added test or helper is still inventoried by default.
    is_fixture_test "$relative_path" && continue
    # `git ls-files --cached` includes paths pending deletion. The inventory describes the current
    # worktree, so a path that no longer exists has no active source to review.
    [ -e "$REPO_ROOT/$relative_path" ] || continue
    [ -f "$REPO_ROOT/$relative_path" ] || continue
    case "$relative_path" in
        *.just)
            echo "Just modules are not allowed: $relative_path" >> "$POLICY_ERRORS"
            continue
            ;;
    esac
    is_automation "$relative_path" || continue
    basename="${relative_path##*/}"
    lower_basename="$(printf '%s' "$basename" | tr '[:upper:]' '[:lower:]')"
    case "$lower_basename" in
        justfile | .justfile) check_just_source "$relative_path" ;;
    esac
    LC_ALL=C awk -v path="$relative_path" '
        BEGIN { tool = "(ca" "rgo|cr" "oss)" }
        {
            line = $0
            sub(/\r$/, "", line)
            sub(/^[[:space:]]*/, "", line)
            if (line == "" || line ~ /^#/) next
            sub(/[[:space:]]+#.*$/, "", line)
            sub(/[[:space:]]+$/, "", line)
            if (line ~ ("(^|[^[:alnum:]_.])" tool "([^[:alnum:]_./-]|$)"))
                print path "\t" line
        }
    ' "$REPO_ROOT/$relative_path" >> "$CANDIDATES"
    LC_ALL=C awk -v path="$relative_path" '
        function emit(line) {
            if (continued != "") line = continued " " line
            if (line ~ /\\[[:space:]]*$/) {
                sub(/[[:space:]]*\\[[:space:]]*$/, "", line)
                continued = line
            } else {
                print path "\t" line
                continued = ""
            }
        }
        {
            line = $0
            sub(/\r$/, "", line)
            sub(/^[[:space:]]*/, "", line)
            if (line == "" || line ~ /^#/) next
            sub(/[[:space:]]+#.*$/, "", line)
            sub(/[[:space:]]+$/, "", line)
            gsub(/[[:space:]]+/, " ", line)
            if (line != "") emit(line)
        }
        END { if (continued != "") print path "\t" continued }
    ' "$REPO_ROOT/$relative_path" >> "$LOGICAL_LINES"
done < "$PATHS"

TOOL_CROSS=cr"oss"
TOOL_CARGO=ca"rgo"
ENV_ASSIGNMENT='[[:alpha:]_][[:alnum:]_]*=[^[:space:]]*[[:space:]]+'
YAML_COMMAND_PREFIX='^(-[[:space:]]+)?(run:[[:space:]]+)?'
LEADING_ASSIGNMENTS="(${ENV_ASSIGNMENT})*"
COMMAND_WRAPPERS="(command[[:space:]]+)?(env[[:space:]]+(${ENV_ASSIGNMENT})*)?"
COMMAND_PREFIX="${YAML_COMMAND_PREFIX}${LEADING_ASSIGNMENTS}${COMMAND_WRAPPERS}"
MALFORMED_COMMAND_PREFIX="${YAML_COMMAND_PREFIX}${LEADING_ASSIGNMENTS}command[[:space:]]+(${ENV_ASSIGNMENT})+"
CROSS_EXECUTABLE="[\"']?([^[:space:]\"']*/)?${TOOL_CROSS}[\"']?"
CARGO_EXECUTABLE="[\"']?([^[:space:]\"']*/)?${TOOL_CARGO}(-host\\.sh)?[\"']?"
while IFS=$'\t' read -r path line; do
    before_separator="$line"
    case "$before_separator" in
        *' -- '*) before_separator="${before_separator%% -- *}" ;;
    esac
    if [ "$path" = .github/workflows/release.yml ] &&
        [[ "$line" =~ ${COMMAND_PREFIX}${CROSS_EXECUTABLE}[[:space:]]+build([^[:alnum:]_./-]|$) ]]; then
        if [[ " $before_separator " != *' --target=aarch64-unknown-linux-musl '* ]] &&
            [[ " $before_separator " != *' --target aarch64-unknown-linux-musl '* ]]; then
            echo "ARM $TOOL_CROSS build requires a pre--- nonempty aarch64 musl target: $path: $line" \
                >> "$POLICY_ERRORS"
        fi
    fi
    if [ "$path" = .github/workflows/release.yml ] &&
        { [[ "$line" =~ ${COMMAND_PREFIX}${CARGO_EXECUTABLE}[[:space:]]+generate-rpm([[:space:]]|$) ]] ||
            [[ "$line" =~ ${MALFORMED_COMMAND_PREFIX}${CARGO_EXECUTABLE}[[:space:]]+generate-rpm([[:space:]]|$) ]]; }; then
        valid_rpm_command=no
        invocation_prefix=''
        if [[ "$line" =~ ${COMMAND_PREFIX}${CARGO_EXECUTABLE}[[:space:]]+generate-rpm([[:space:]]|$) ]]; then
            valid_rpm_command=yes
            invocation_prefix="${BASH_REMATCH[0]}"
        fi
        declared_target=''
        declared_target_count=0
        if [ "$valid_rpm_command" = yes ]; then
            read -r -a invocation_words <<< "$invocation_prefix"
            for word in "${invocation_words[@]}"; do
                case "$word" in
                    CARGO_BUILD_TARGET=*)
                        declared_target="${word#CARGO_BUILD_TARGET=}"
                        declared_target_count=$((declared_target_count + 1))
                        ;;
                esac
            done
        fi
        explicit_target=''
        explicit_target_count=0
        read -r -a command_words <<< "$before_separator"
        for ((word_index = 0; word_index < ${#command_words[@]}; word_index++)); do
            word="${command_words[$word_index]}"
            case "$word" in
                --target=*)
                    explicit_target="${word#--target=}"
                    explicit_target_count=$((explicit_target_count + 1))
                    ;;
                --target)
                    explicit_target_count=$((explicit_target_count + 1))
                    word_index=$((word_index + 1))
                    if [ "$word_index" -lt "${#command_words[@]}" ]; then
                        explicit_target="${command_words[$word_index]}"
                    fi
                    ;;
            esac
        done
        supported_target=no
        case "$declared_target" in
            x86_64-unknown-linux-musl | aarch64-unknown-linux-musl)
                supported_target=yes
                ;;
        esac
        if [ "$valid_rpm_command" != yes ] || [ "$declared_target_count" -ne 1 ] ||
            [ "$supported_target" != yes ] ||
            [ "$explicit_target_count" -ne 1 ] ||
            [ "$explicit_target" != "$declared_target" ]; then
            echo "$TOOL_CARGO generate-rpm requires an explicit target matching CARGO_BUILD_TARGET: $path: $line" \
                >> "$POLICY_ERRORS"
        fi
    fi
    if [[ "$line" =~ ${COMMAND_PREFIX}${CARGO_EXECUTABLE}[[:space:]]+workspaces[[:space:]]+publish([[:space:]]|$) ]] &&
        [[ " $before_separator " != *' --no-verify '* ]]; then
        echo "workspace publication requires pre--- --no-verify: $path: $line" \
            >> "$POLICY_ERRORS"
    fi
done < "$LOGICAL_LINES"

LC_ALL=C sort "$CANDIDATES" -o "$CANDIDATES"
LC_ALL=C awk 'NF && $0 !~ /^#/' "$ALLOWLIST" | sort > "$APPROVED"
comm -13 "$APPROVED" "$CANDIDATES" > "$UNEXPECTED"
comm -23 "$APPROVED" "$CANDIDATES" > "$STALE"

status=0
if [ -s "$POLICY_ERRORS" ]; then
    cat "$POLICY_ERRORS" >&2
    status=1
fi
while IFS=$'\t' read -r path line; do
    [ -n "$path" ] || continue
    echo "unexpected build entrypoint: $path: $line" >&2
    status=1
done < "$UNEXPECTED"
while IFS=$'\t' read -r path line; do
    [ -n "$path" ] || continue
    echo "stale approved build entrypoint: $path: $line" >&2
    status=1
done < "$STALE"

exit "$status"
