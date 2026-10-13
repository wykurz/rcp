#!/usr/bin/env bash
# Keep the fixed remote destination node allocated until the remote-copy comparison completes.

#SBATCH --partition=perf_copy_m8idn_16xl-on-demand
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=200G
#SBATCH --time=4-00:00:00
#SBATCH --requeue
#SBATCH --signal=B:USR1@600

set -euo pipefail

LABEL=""
RESULTS_ROOT=""
PEER_NODE=""
EXPECTED_SUCCESSES=48

usage() {
  cat <<'EOF'
Usage: campaign-slurm-peer.sh --label NAME --results-root PATH --peer-node HOST [options]

Options:
  --expected-successes N  successful remote rcp run IDs required (default: 48)
EOF
}

while (($#)); do
  case "$1" in
    --label) LABEL="$2"; shift 2 ;;
    --results-root) RESULTS_ROOT="$2"; shift 2 ;;
    --peer-node) PEER_NODE="$2"; shift 2 ;;
    --expected-successes) EXPECTED_SUCCESSES="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$LABEL" && -n "$RESULTS_ROOT" && -n "$PEER_NODE" ]] || {
  usage >&2
  exit 2
}
[[ -n "${SLURM_JOB_ID:-}" ]] || {
  printf 'campaign-slurm-peer.sh must run as a Slurm batch job\n' >&2
  exit 2
}
[[ "$EXPECTED_SUCCESSES" =~ ^[1-9][0-9]*$ ]] || {
  printf -- '--expected-successes must be a positive integer\n' >&2
  exit 2
}

readonly RESULTS_DIR="$RESULTS_ROOT/$LABEL"
readonly RESULTS_JSONL="$RESULTS_DIR/results.jsonl"
readonly DRIVER_LOG="$RESULTS_DIR/control/peer-driver.log"
readonly COMPLETE_FILE="$RESULTS_DIR/.remote-complete"
readonly STOP_FILE="$RESULTS_DIR/.stop-after-row"
readonly LAST_RENEWAL_FILE="$RESULTS_DIR/.peer-last-renewal"

log() {
  printf '%s  peer-job[%s]: %s\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$SLURM_JOB_ID" "$*" | tee -a "$DRIVER_LOG"
}

successful_rows() {
  [[ -f "$RESULTS_JSONL" ]] || {
    printf '0\n'
    return
  }
  python3 - "$RESULTS_JSONL" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    print(len({
        row["run_id"]
        for line in handle
        if (row := json.loads(line)).get("status") == "ok"
    }))
PY
}

allocation_has_node() {
  local wanted="$1"
  scontrol show hostnames "$SLURM_JOB_NODELIST" | grep -Fxq "$wanted"
}

mkdir -p "$RESULTS_DIR/control"
allocation_has_node "$PEER_NODE" || {
  log "peer node is not in allocation $SLURM_JOB_NODELIST: $PEER_NODE"
  exit 2
}

requeue_requested=0
request_requeue() {
  local reason="${1:-pre-timeout signal}"
  ((requeue_requested)) && return
  requeue_requested=1
  log "requeueing allocation: $reason"
  scontrol requeue "$SLURM_JOB_ID"
  exit 0
}

handle_usr1() {
  request_requeue "ten-minute pre-timeout fallback"
}
trap handle_usr1 USR1

completed="$(successful_rows)"
if ((completed < EXPECTED_SUCCESSES)); then
  rm -f "$COMPLETE_FILE"
fi
if [[ -s "$STOP_FILE" ]]; then
  # A fresh allocation already satisfies a marker that predates this process start.
  renewal_marker="$(<"$STOP_FILE")"
  printf '%s\n' "$renewal_marker" >"$LAST_RENEWAL_FILE"
fi

log "holding peer=$PEER_NODE; completed=$completed/$EXPECTED_SUCCESSES"
while :; do
  if [[ -s "$COMPLETE_FILE" ]]; then
    log "campaign completion marker present; releasing peer"
    exit 0
  fi
  completed="$(successful_rows)"
  if ((completed >= EXPECTED_SUCCESSES)); then
    printf '%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$COMPLETE_FILE"
    log "campaign complete with $completed successful rows; releasing peer"
    exit 0
  fi
  if [[ -s "$STOP_FILE" ]]; then
    renewal_marker="$(<"$STOP_FILE")"
    previous_marker=""
    [[ -f "$LAST_RENEWAL_FILE" ]] && previous_marker="$(<"$LAST_RENEWAL_FILE")"
    if [[ "$renewal_marker" != "$previous_marker" ]]; then
      printf '%s\n' "$renewal_marker" >"$LAST_RENEWAL_FILE"
      request_requeue "source renewal marker $renewal_marker"
    fi
  fi
  sleep 60
done
