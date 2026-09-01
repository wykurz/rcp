#!/bin/bash

set -euo pipefail

if [[ "$#" -eq 0 ]]; then
    echo "depot-ci: at least one workflow job is required" >&2
    exit 2
fi

for required_command in depot git jq; do
    if ! command -v "$required_command" >/dev/null 2>&1; then
        echo "depot-ci: required command '$required_command' was not found; enter nix develop or install it" >&2
        exit 127
    fi
done

jobs=("$@")
run_args=(ci run --workflow .depot/workflows/ci.yml)
for job in "${jobs[@]}"; do
    run_args+=(--job "$job")
done

untracked_files=()
untracked_list="$(mktemp)"
trap 'rm -f "$untracked_list"' EXIT
set +e
git ls-files --others --exclude-standard -z >"$untracked_list"
untracked_status=$?
set -e
if [[ "$untracked_status" -ne 0 ]]; then
    echo "depot-ci: failed to inspect ordinary untracked files" >&2
    exit "$untracked_status"
fi
while IFS= read -r -d '' path; do
    if [[ "$path" != .depot/* ]]; then
        untracked_files+=("$path")
    fi
done <"$untracked_list"
rm -f "$untracked_list"
trap - EXIT
if [[ "${#untracked_files[@]}" -gt 0 ]]; then
    echo "depot-ci: warning: ordinary untracked files are excluded from Depot's uploaded patch:" >&2
    printf '  %s\n' "${untracked_files[@]}" >&2
    echo "depot-ci: stage any of these files that the remote check needs" >&2
fi

set +e
launch_output="$(depot "${run_args[@]}" 2>&1)"
launch_status=$?
set -e
printf '%s\n' "$launch_output"
if [[ "$launch_status" -ne 0 ]]; then
    exit "$launch_status"
fi

run_id=""
while IFS= read -r line; do
    if [[ "$line" =~ ^Run:[[:space:]]+([^[:space:]]+) ]]; then
        run_id="${BASH_REMATCH[1]}"
        break
    fi
done <<<"$launch_output"
if [[ -z "$run_id" ]]; then
    echo "depot-ci: launch output did not contain a run ID" >&2
    exit 1
fi

if [[ "${#jobs[@]}" -eq 1 ]]; then
    if ! depot ci logs "$run_id" --job "${jobs[0]}" --workflow ci.yml --follow; then
        echo "depot-ci: live log streaming failed; checking authoritative run status" >&2
    fi
fi

poll_interval="${RCP_DEPOT_CI_POLL_INTERVAL:-5}"
status_query_failures=0
while true; do
    set +e
    status_json="$(depot ci run show "$run_id" --output json)"
    status_query_status=$?
    set -e
    if [[ "$status_query_status" -ne 0 ]]; then
        if [[ -n "$status_json" ]]; then
            printf '%s\n' "$status_json" >&2
        fi
        ((status_query_failures += 1))
        if [[ "$status_query_failures" -ge 5 ]]; then
            echo "depot-ci: status query failed 5 consecutive times" >&2
            exit "$status_query_status"
        fi
        echo "depot-ci: retrying status query after failure $status_query_failures/5" >&2
        sleep "$poll_interval"
        continue
    fi
    status_query_failures=0

    if ! status="$(jq -er '.status | select(type == "string")' <<<"$status_json" 2>/dev/null)"; then
        echo "depot-ci: invalid status JSON for run $run_id:" >&2
        printf '%s\n' "$status_json" >&2
        exit 1
    fi

    case "$status" in
        queued | running)
            sleep "$poll_interval"
            ;;
        finished)
            exit 0
            ;;
        failed | cancelled)
            echo "depot-ci: run $run_id ended with status $status" >&2
            depot ci status "$run_id" || true
            exit 1
            ;;
        *)
            echo "depot-ci: unexpected run status '$status' for $run_id" >&2
            exit 1
            ;;
    esac
done
