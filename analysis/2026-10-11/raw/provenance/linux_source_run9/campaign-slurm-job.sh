#!/usr/bin/env bash
# Run the Linux source local and remote XFS matrix on a fixed source node.

#SBATCH --partition=perf_copy_m8idn_16xl-on-demand
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=200G
#SBATCH --time=4-00:00:00
#SBATCH --requeue
#SBATCH --signal=B:USR1@600

set -euo pipefail

readonly LABEL="run9-20261011-linux-source-local-4tool"
readonly RESULTS_ROOT="/d/phscratch/public/user/mateusz/rcp-perf-results-20260902"
readonly RESULTS_DIR="$RESULTS_ROOT/$LABEL"
readonly RESULTS_JSONL="$RESULTS_DIR/results.jsonl"
readonly DRIVER_LOG="$RESULTS_DIR/control/source-driver.log"
readonly RUNNER="/w/home/mateusz/projects/rcp-perf-results-20260902/results/$LABEL/tools/rcp-copy-bench.sh"
readonly EXPECTED_SUCCESSES=18

if (($# != 2)); then
  printf 'usage: %s SOURCE_NODE PEER_NODE\n' "$0" >&2
  exit 2
fi
readonly SOURCE_NODE="$1"
readonly PEER_NODE="$2"

mkdir -p "$RESULTS_DIR/control"

log() {
  printf '%s  source-job[%s]: %s\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$SLURM_JOB_ID" "$*" |
    tee -a "$DRIVER_LOG"
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

scontrol show hostnames "$SLURM_JOB_NODELIST" | grep -Fxq "$SOURCE_NODE" || {
  log "source node is not in allocation: $SOURCE_NODE"
  exit 2
}
peer_ip="$(
  scontrol show node "$PEER_NODE" --oneliner |
    sed -n 's/.* NodeAddr=\([^ ]*\).*/\1/p'
)"
[[ -n "$peer_ip" ]] || {
  log "could not resolve NodeAddr for $PEER_NODE"
  exit 2
}
readonly peer_ip

requeue_requested=0
request_requeue() {
  ((requeue_requested)) && return
  requeue_requested=1
  log "requeueing before allocation timeout"
  scontrol requeue "$SLURM_JOB_ID"
  exit 0
}
trap request_requeue USR1

log "started source=$SOURCE_NODE peer=$PEER_NODE peer_ip=$peer_ip"
while :; do
  completed="$(successful_rows)"
  if ((completed >= EXPECTED_SUCCESSES)); then
    date -u '+%Y-%m-%dT%H:%M:%SZ' >"$RESULTS_DIR/.campaign-complete"
    log "campaign complete with $completed successful rows"
    exit 0
  fi

  set +e
  "$RUNNER" \
    --label "$LABEL" \
    --resume \
    --peer "$peer_ip" \
    --scopes xfs,remote \
    --cache-eviction sudo-sysctl \
    --results-root "$RESULTS_ROOT" \
    >>"$DRIVER_LOG" 2>&1
  runner_rc=$?
  set -e

  completed="$(successful_rows)"
  if ((runner_rc == 0 && completed == EXPECTED_SUCCESSES)); then
    date -u '+%Y-%m-%dT%H:%M:%SZ' >"$RESULTS_DIR/.campaign-complete"
    log "campaign matrix complete with $completed successful rows"
    exit 0
  fi
  log "runner exited status=$runner_rc; completed=$completed/$EXPECTED_SUCCESSES"
  sleep 60
done
