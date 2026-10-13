#!/usr/bin/env bash
# Benchmark a real Linux source tree across local and remote XFS.
#
# This runner deliberately owns only paths below its configured work/destination roots. It stages
# one fileset at a time and appends durable result records after each measured copy.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
readonly DEFAULT_FILESETS="linux-source"
readonly DEFAULT_SCOPES="xfs,remote"
readonly DEFAULT_RCP="$SCRIPT_DIR/../artifacts/bin/rcp"
readonly DEFAULT_RCPD="$SCRIPT_DIR/../artifacts/bin/rcpd"
readonly DEFAULT_RRM="$SCRIPT_DIR/../artifacts/bin/rrm"
readonly DEFAULT_DCP="/opt/hpcc-infra/bin/dcp"
readonly DEFAULT_MPIRUN="/opt/hpcc-infra/bin/mpirun"
readonly DEFAULT_SRC_ROOT="/d/phscratch/public/user/mateusz/filesets"
readonly DEFAULT_NFS_DEST_ROOT="/d/phscratch/public/user/mateusz/rcp-perf-work-20260902"
readonly SERIAL_NFS_FILESET="10x10x10x10x1024x1K"
readonly SERIAL_NFS_TIMEOUT="12h"
readonly SERIAL_NFS_TIMEOUT_SECONDS=43200

FILESETS="$DEFAULT_FILESETS"
SCOPES="$DEFAULT_SCOPES"
ITERATIONS=3
PEER=""
SRC_ROOT="$DEFAULT_SRC_ROOT"
NFS_DEST_ROOT="$DEFAULT_NFS_DEST_ROOT"
ZFS_ROOT=""
WORK_DIR="/instance-store/rcp-copy-bench-run9-20261011"
RESULTS_ROOT="$SCRIPT_DIR/results"
LABEL=""
RCP_BIN="$DEFAULT_RCP"
RCPD_BIN="$DEFAULT_RCPD"
RRM_BIN="$DEFAULT_RRM"
DCP_BIN="$DEFAULT_DCP"
MPIRUN_BIN="$DEFAULT_MPIRUN"
RUN_TIMEOUT="48h"
SETTLE_SECS=5
CACHE_EVICTION="auto"
CACHE_EVICTION_METHOD=""
DRY_RUN=0
RESUME=0
KEEP_STAGE=0
RCP_BENCH_STOP_FILE="${RCP_BENCH_STOP_FILE:-}"

RESULTS_DIR=""
RESULTS_JSONL=""
MANIFEST=""
TMP_STATE=""
declare -A SOURCE_STATS_CACHE=()

log() {
  printf '%s  %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
}

stop_if_requested() {
  if [[ -n "$RCP_BENCH_STOP_FILE" && -e "$RCP_BENCH_STOP_FILE" ]]; then
    log "stop requested between measured rows: $RCP_BENCH_STOP_FILE"
    exit 75
  fi
}

die() {
  log "FATAL: $*"
  exit 1
}

usage() {
  cat <<'EOF'
Usage: rcp-copy-bench.sh --label NAME [OPTIONS]

Runs the 18-row Linux source comparison matrix. This run-specific snapshot supports local and
remote XFS; remote XFS needs --peer.

Core options:
  --label NAME             durable campaign/run name under results/
  --peer IP                remote XFS destination compute; required for scope remote
  --filesets LIST          comma-separated fileset names
  --scopes LIST            xfs,remote (default: xfs,remote)
  --iterations N           measured iterations per configuration (default: 3)
  --resume                 skip result rows already recorded for this label
  --dry-run                print all planned rows and commands; do not mutate storage

Filesystem options:
  --src-root PATH          NFS fileset root
  --work-dir PATH          XFS work root on this compute
  --results-root PATH      writable result root on this host
  --nfs-dest-root PATH     owned NFS destination root
  --zfs-root PATH          owned ZFS destination/state root for the deferred ZFS slice
  --keep-stage             retain staged local source after this invocation

Tool options:
  --rcp-bin PATH           current rcp binary
  --rcpd-bin PATH          matching current rcpd binary
  --rrm-bin PATH           tree remover used only under owned roots
  --dcp-bin PATH           dcp binary
  --mpirun-bin PATH        mpirun binary
  --run-timeout DURATION   GNU timeout value per copy (default: 48h)
  --settle-secs N          sleep after cache eviction (default: 5)
  --cache-eviction METHOD  auto, sudo-sysctl, or fadvise (default: auto)
EOF
}

need_arg() {
  [[ $# -ge 2 ]] || die "$1 needs an argument"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --label) need_arg "$@"; LABEL="$2"; shift 2 ;;
    --peer) need_arg "$@"; PEER="$2"; shift 2 ;;
    --filesets) need_arg "$@"; FILESETS="$2"; shift 2 ;;
    --scopes) need_arg "$@"; SCOPES="$2"; shift 2 ;;
    --iterations) need_arg "$@"; ITERATIONS="$2"; shift 2 ;;
    --src-root) need_arg "$@"; SRC_ROOT="$2"; shift 2 ;;
    --work-dir) need_arg "$@"; WORK_DIR="$2"; shift 2 ;;
    --results-root) need_arg "$@"; RESULTS_ROOT="$2"; shift 2 ;;
    --nfs-dest-root) need_arg "$@"; NFS_DEST_ROOT="$2"; shift 2 ;;
    --zfs-root) need_arg "$@"; ZFS_ROOT="$2"; shift 2 ;;
    --keep-stage) KEEP_STAGE=1; shift ;;
    --rcp-bin) need_arg "$@"; RCP_BIN="$2"; shift 2 ;;
    --rcpd-bin) need_arg "$@"; RCPD_BIN="$2"; shift 2 ;;
    --rrm-bin) need_arg "$@"; RRM_BIN="$2"; shift 2 ;;
    --dcp-bin) need_arg "$@"; DCP_BIN="$2"; shift 2 ;;
    --mpirun-bin) need_arg "$@"; MPIRUN_BIN="$2"; shift 2 ;;
    --run-timeout) need_arg "$@"; RUN_TIMEOUT="$2"; shift 2 ;;
    --settle-secs) need_arg "$@"; SETTLE_SECS="$2"; shift 2 ;;
    --cache-eviction) need_arg "$@"; CACHE_EVICTION="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --resume) RESUME=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

[[ -n "$LABEL" ]] || die "--label is required"
[[ "$LABEL" =~ ^[A-Za-z0-9._-]+$ ]] || die "--label must contain only letters, digits, ., _, or -"
[[ "$ITERATIONS" =~ ^[1-9][0-9]*$ ]] || die "--iterations must be a positive integer"
case "$CACHE_EVICTION" in
  auto|sudo-sysctl|fadvise) ;;
  *) die "--cache-eviction must be auto, sudo-sysctl, or fadvise" ;;
esac

split_csv() {
  local value="$1"
  local item
  local IFS=','
  read -r -a SPLIT_ITEMS <<<"$value"
  for item in "${SPLIT_ITEMS[@]}"; do
    [[ -n "$item" ]] || die "empty item in list '$value'"
    printf '%s\n' "$item"
  done
}

has_scope() {
  local wanted="$1"
  local scope
  while read -r scope; do
    [[ "$scope" == "$wanted" ]] && return 0
  done < <(split_csv "$SCOPES")
  return 1
}

path_is_safe_root() {
  local path="$1"
  [[ "$path" == /* && "$path" != "/" && "$path" != "/instance-store" && "$path" != "/d" ]]
}

require_tool() {
  [[ -x "$1" ]] || die "required executable is missing: $1"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
}

validate_fileset_layout() {
  local fileset="$1"
  local src="$SRC_ROOT/$fileset"
  [[ -d "$src" ]] || die "fileset does not exist: $src"
  [[ -n "$(find "$src" -mindepth 1 -maxdepth 1 -print -quit)" ]] ||
    die "fileset is empty: $src"
}

validate_config() {
  require_command bash
  require_command rsync
  require_command ssh
  require_command sync
  require_command timeout
  require_command python3
  [[ -x /usr/bin/time ]] || die "required executable is missing: /usr/bin/time"
  require_tool "$RCP_BIN"
  require_tool "$RCPD_BIN"
  require_tool "$RRM_BIN"
  require_tool "$DCP_BIN"
  require_tool "$MPIRUN_BIN"
  path_is_safe_root "$WORK_DIR" || die "--work-dir must be an owned absolute path below /instance-store"
  path_is_safe_root "$NFS_DEST_ROOT" || die "--nfs-dest-root must be an owned absolute path"
  path_is_safe_root "$RESULTS_ROOT" || die "--results-root must be an owned absolute path"

  if has_scope remote; then
    [[ -n "$PEER" ]] || die "--peer is required for the remote scope"
    [[ "$PEER" != *:* ]] || die "--peer must be an IPv4 address or hostname without ':'"
  fi
  if has_scope zfs; then
    [[ -n "$ZFS_ROOT" ]] || die "--zfs-root is required for the zfs scope"
    path_is_safe_root "$ZFS_ROOT" || die "--zfs-root must be an owned absolute path"
  fi

  local scope
  while read -r scope; do
    case "$scope" in
      xfs|remote) ;;
      *) die "this run-specific runner supports only xfs and remote, not: $scope" ;;
    esac
  done < <(split_csv "$SCOPES")

  local fileset
  while read -r fileset; do
    validate_fileset_layout "$fileset"
  done < <(split_csv "$FILESETS")
}

monotonic() {
  python3 -c 'import time; print(f"{time.monotonic():.9f}")'
}

elapsed() {
  python3 - "$1" "$2" <<'PY'
import sys
print(f"{float(sys.argv[2]) - float(sys.argv[1]):.9f}")
PY
}

shell_join() {
  local output=""
  local part
  for part in "$@"; do
    printf -v part '%q' "$part"
    output+="${output:+ }$part"
  done
  printf '%s' "$output"
}

json_record() {
  local run_id="$1"
  local scope="$2"
  local fileset="$3"
  local variant="$4"
  local iteration="$5"
  local source="$6"
  local destination="$7"
  local command_line="$8"
  local status="$9"
  local exit_code="${10}"
  local copy_seconds="${11}"
  local sync_seconds="${12}"
  local source_files="${13}"
  local source_bytes="${14}"
  local destination_files="${15}"
  local destination_bytes="${16}"
  local log_file="${17}"
  local user_cpu_seconds="${18}"
  local system_cpu_seconds="${19}"
  local max_rss_kib="${20}"
  local attempt="${21}"
  local time_file="${22}"
  local failure_phase="${23}"
  local failure_reason="${24}"
  python3 - "$RESULTS_JSONL" "$run_id" "$scope" "$fileset" "$variant" "$iteration" \
    "$source" "$destination" "$command_line" "$status" "$exit_code" "$copy_seconds" \
    "$sync_seconds" "$source_files" "$source_bytes" "$destination_files" \
    "$destination_bytes" "$log_file" "$user_cpu_seconds" "$system_cpu_seconds" \
    "$max_rss_kib" "$attempt" "$time_file" "$failure_phase" "$failure_reason" <<'PY'
import datetime
import json
import sys

keys = (
    "run_id", "scope", "fileset", "variant", "iteration", "source", "destination",
    "command", "status", "exit_code", "copy_seconds", "sync_seconds", "source_files",
    "source_bytes", "destination_files", "destination_bytes", "log_file",
    "user_cpu_seconds", "system_cpu_seconds", "max_rss_kib", "attempt", "time_file",
    "failure_phase", "failure_reason",
)
row = dict(zip(keys, sys.argv[2:]))
for key in ("iteration", "exit_code", "source_files", "source_bytes", "destination_files",
            "destination_bytes", "attempt"):
    row[key] = int(row[key])
for key in ("copy_seconds", "sync_seconds"):
    row[key] = float(row[key])
for key in ("user_cpu_seconds", "system_cpu_seconds"):
    row[key] = float(row[key]) if row[key] else None
row["max_rss_kib"] = int(row["max_rss_kib"]) if row["max_rss_kib"] else None
row["recorded_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
with open(sys.argv[1], "a", encoding="utf-8") as handle:
    handle.write(json.dumps(row, sort_keys=True) + "\n")
PY
}

next_attempt_number() {
  local run_id="$1"
  [[ -f "$RESULTS_JSONL" ]] || {
    printf '1\n'
    return
  }
  python3 - "$RESULTS_JSONL" "$run_id" <<'PY'
import json
import sys

path, run_id = sys.argv[1:]
attempts = 0
with open(path, encoding="utf-8") as handle:
    for line in handle:
        row = json.loads(line)
        if row["run_id"] == run_id:
            attempts += 1
print(attempts + 1)
PY
}

time_metrics() {
  local path="$1"
  local user system rss
  user="$(awk -F: '/^[[:space:]]*User time \(seconds\):/ {gsub(/^[[:space:]]+/, "", $2); print $2}' "$path")"
  system="$(awk -F: '/^[[:space:]]*System time \(seconds\):/ {gsub(/^[[:space:]]+/, "", $2); print $2}' "$path")"
  rss="$(awk -F: '/^[[:space:]]*Maximum resident set size \(kbytes\):/ {gsub(/^[[:space:]]+/, "", $2); print $2}' "$path")"
  printf '%s\t%s\t%s\n' "$user" "$system" "$rss"
}

tree_stats() {
  local path="$1"
  local files bytes
  files="$(find "$path" -type f | wc -l)"
  bytes="$(du -sb --apparent-size "$path" | awk '{print $1}')"
  printf '%s\t%s\n' "$files" "$bytes"
}

cache_source_stats() {
  local source="$1"
  [[ -n "${SOURCE_STATS_CACHE[$source]+cached}" ]] && return 0
  if [[ -f "$RESULTS_JSONL" ]]; then
    local durable_stats
    durable_stats="$(
      python3 - "$RESULTS_JSONL" "$source" <<'PY'
import json
import sys

path, source = sys.argv[1:]
values = set()
with open(path, encoding="utf-8") as handle:
    for line in handle:
        row = json.loads(line)
        if row.get("status") == "ok" and row.get("source") == source:
            values.add((row["source_files"], row["source_bytes"]))
if len(values) > 1:
    raise SystemExit(f"inconsistent durable source stats for {source}: {sorted(values)}")
if values:
    files, size = values.pop()
    print(f"{files}\t{size}")
PY
    )" || die "could not load durable source stats for $source"
    if [[ -n "$durable_stats" ]]; then
      SOURCE_STATS_CACHE["$source"]="$durable_stats"
      return 0
    fi
  fi
  SOURCE_STATS_CACHE["$source"]="$(tree_stats "$source")"
}

remote_tree_stats() {
  local path="$1"
  ssh "$PEER" bash -s -- "$path" <<'REMOTE'
set -euo pipefail
path="$1"
printf '%s\t%s\n' "$(find "$path" -type f | wc -l)" "$(du -sb --apparent-size "$path" | awk '{print $1}')"
REMOTE
}

run_remote() {
  # shellcheck disable=SC2029 # arguments intentionally form the remote command.
  # Never let SSH consume the caller's matrix iterator. Commands that need stdin use their own
  # explicit redirection, such as remote_tree_stats above.
  ssh "$PEER" "$@" </dev/null
}

remove_local() {
  local path="$1"
  [[ -e "$path" ]] || return 0
  "$RRM_BIN" --quiet "$path"
}

remove_remote() {
  local path="$1"
  if run_remote test -e "$path"; then
    if run_remote "$RRM_BIN" --quiet "$path"; then
      return 0
    fi
    # rrm can report nonzero after removing its root over an SSH session. The postcondition is what
    # matters: a missing destination cannot contaminate the next timed copy.
    if ! run_remote test -e "$path"; then
      return 0
    fi
    return 1
  fi
}

probe_sudo_cache_eviction() {
  sync
  sudo -n /bin/sh -c 'echo 3 > /proc/sys/vm/drop_caches' >/dev/null 2>&1
}

resolve_cache_eviction() {
  local previous=""
  if ((RESUME)) && [[ -f "$MANIFEST" ]]; then
    previous="$(awk -F= '$1 == "drop_caches_method" {print $2; exit}' "$MANIFEST")"
  fi

  if [[ -n "$previous" ]]; then
    if [[ "$CACHE_EVICTION" != auto && "$CACHE_EVICTION" != "$previous" ]]; then
      die "resume cache method mismatch: manifest=$previous requested=$CACHE_EVICTION"
    fi
    CACHE_EVICTION_METHOD="$previous"
  elif [[ "$CACHE_EVICTION" == auto ]]; then
    if probe_sudo_cache_eviction; then
      CACHE_EVICTION_METHOD="sudo-sysctl"
    else
      CACHE_EVICTION_METHOD="fadvise"
    fi
  else
    CACHE_EVICTION_METHOD="$CACHE_EVICTION"
  fi

  case "$CACHE_EVICTION_METHOD" in
    sudo-sysctl)
      probe_sudo_cache_eviction ||
        die "sudo cache eviction is required by this campaign but is unavailable"
      ;;
    fadvise)
      python3 -c 'import os; assert hasattr(os, "posix_fadvise")' ||
        die "Python does not provide os.posix_fadvise"
      ;;
    *) die "manifest has unknown cache eviction method: $CACHE_EVICTION_METHOD" ;;
  esac
}

evict_tree_fadvise() {
  local source="$1"
  python3 - "$source" <<'PY'
import json
import os
import sys

source = sys.argv[1]
os.sync()
files = 0
evicted_bytes = 0
errors = 0
for directory, _directories, names in os.walk(source):
    for name in names:
        path = os.path.join(directory, name)
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except OSError:
            errors += 1
            continue
        try:
            size = os.fstat(descriptor).st_size
            os.posix_fadvise(descriptor, 0, 0, os.POSIX_FADV_DONTNEED)
            files += 1
            evicted_bytes += size
        except OSError:
            errors += 1
        finally:
            os.close(descriptor)

print(json.dumps({
    "method": "posix_fadvise_dontneed",
    "path": source,
    "files": files,
    "bytes": evicted_bytes,
    "errors": errors,
    "clears_page_cache": True,
    "clears_dentry_inode_cache": False,
}, sort_keys=True))
raise SystemExit(1 if errors else 0)
PY
}

drop_source_caches() {
  local source="$1"
  case "$CACHE_EVICTION_METHOD" in
    sudo-sysctl)
      sync
      sudo -n /bin/sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      ;;
    fadvise) evict_tree_fadvise "$source" ;;
    *) die "internal: unresolved cache eviction method" ;;
  esac
}

write_manifest() {
  local scope
  if ((RESUME)) && [[ -f "$MANIFEST" ]]; then
    return
  fi
  {
    printf 'label=%s\n' "$LABEL"
    printf 'started_utc=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'filesets=%s\n' "$FILESETS"
    printf 'scopes=%s\n' "$SCOPES"
    printf 'iterations=%s\n' "$ITERATIONS"
    printf 'src_root=%s\n' "$SRC_ROOT"
    printf 'work_dir=%s\n' "$WORK_DIR"
    printf 'nfs_dest_root=%s\n' "$NFS_DEST_ROOT"
    printf 'zfs_root=%s\n' "${ZFS_ROOT:-none}"
    printf 'peer=%s\n' "${PEER:-none}"
    printf 'run_timeout=%s\n' "$RUN_TIMEOUT"
    printf 'campaign_kind=linux-source-xfs-local-and-remote\n'
    printf 'source_hostname=%s\n' "$(hostname -f)"
    printf 'slurm_job_id=%s\n' "${SLURM_JOB_ID:-none}"
    printf 'source_archive=%s/linux-7.2.9.tar.gz\n' "$SRC_ROOT"
    printf 'source_archive_sha256=ba4996423cf38afc564f795be41d171495344ad6103c35bf2ce62918a1213ec6\n'
    printf 'runner_sha256=%s\n' "$(sha256sum "$0" | awk '{print $1}')"
    printf 'rcp=%s\n' "$("$RCP_BIN" --version)"
    printf 'rcpd_protocol=%s\n' "$("$RCPD_BIN" --protocol-version)"
    printf 'rcp_sha256=%s\n' "$(sha256sum "$RCP_BIN" | awk '{print $1}')"
    printf 'rcpd_sha256=%s\n' "$(sha256sum "$RCPD_BIN" | awk '{print $1}')"
    printf 'rrm_sha256=%s\n' "$(sha256sum "$RRM_BIN" | awk '{print $1}')"
    printf 'dcp=%s\n' "$("$DCP_BIN" --version 2>&1 | head -1)"
    printf 'mpirun=%s\n' "$("$MPIRUN_BIN" --version 2>&1 | head -1)"
    printf 'drop_caches_method=%s\n' "$CACHE_EVICTION_METHOD"
    if [[ "$CACHE_EVICTION_METHOD" == fadvise ]]; then
      printf 'drop_caches_clears_page_cache=true\n'
      printf 'drop_caches_clears_dentry_inode_cache=false\n'
    else
      printf 'drop_caches_clears_page_cache=true\n'
      printf 'drop_caches_clears_dentry_inode_cache=true\n'
    fi
    printf 'mpirun_options=--map-by :OVERSUBSCRIBE\n'
    printf 'rcp_remote_daemon=%s\n' "$RCPD_BIN"
    printf 'rcp_parallelism=default\n'
    printf 'dcp_parallelism=default(32)\n'
    printf 'cp_rsync_parallelism=1\n'
    printf 'remote_rsync_parallelism=1\n'
    printf 'semantics=rcp-default; dcp--preserve; cp--archive--reflink=auto; rsync--archive\n'
    printf 'note.variants=local default rcp/dcp and serial cp/rsync; remote default rcp and serial rsync\n'
    printf 'note=cache eviction is client-side only; storage-server caches are not controlled\n'
    for scope in $(split_csv "$SCOPES"); do
      printf 'scope.%s=enabled\n' "$scope"
    done
  } >"$MANIFEST"
}

source_for_scope() {
  local scope="$1"
  local fileset="$2"
  case "$scope" in
    nfs|zfs) printf '%s/%s' "$SRC_ROOT" "$fileset" ;;
    xfs|remote) printf '%s/stage/%s' "$WORK_DIR" "$fileset" ;;
    *) die "internal: unknown scope $scope" ;;
  esac
}

destination_for() {
  local scope="$1"
  local fileset="$2"
  local variant="$3"
  local iteration="$4"
  case "$scope" in
    xfs) printf '%s/dest/xfs/%s/%s/%s' "$WORK_DIR" "$fileset" "$variant" "$iteration" ;;
    nfs) printf '%s/%s/%s/%s' "$NFS_DEST_ROOT" "$fileset" "$variant" "$iteration" ;;
    remote) printf '%s/dest/remote/%s/%s/%s' "$WORK_DIR" "$fileset" "$variant" "$iteration" ;;
    zfs) printf '%s/dest/zfs/%s/%s/%s' "$ZFS_ROOT" "$fileset" "$variant" "$iteration" ;;
    *) die "internal: unknown scope $scope" ;;
  esac
}

stage_fileset() {
  local scope="$1"
  local fileset="$2"
  local stage
  case "$scope" in
    xfs|remote) stage="$WORK_DIR/stage/$fileset" ;;
    nfs|zfs) return 0 ;;
  esac

  if [[ -d "$stage" ]]; then
    local expected actual
    expected="$(tree_stats "$SRC_ROOT/$fileset")"
    actual="$(tree_stats "$stage")"
    if [[ "$expected" == "$actual" ]]; then
      SOURCE_STATS_CACHE["$SRC_ROOT/$fileset"]="$expected"
      SOURCE_STATS_CACHE["$stage"]="$actual"
      return 0
    fi
    log "removing incomplete or wrong stage before restaging: $stage"
    remove_local "$stage"
  fi

  mkdir -p "$(dirname -- "$stage")"
  log "staging $fileset to $stage for $scope"
  rsync -a "$SRC_ROOT/$fileset/" "$stage/"
  local expected actual
  expected="$(tree_stats "$SRC_ROOT/$fileset")"
  actual="$(tree_stats "$stage")"
  [[ "$expected" == "$actual" ]] || die "staged fileset failed count/byte validation: $fileset"
  SOURCE_STATS_CACHE["$SRC_ROOT/$fileset"]="$expected"
  SOURCE_STATS_CACHE["$stage"]="$actual"
}

variant_list() {
  local scope="$1"
  case "$scope" in
    xfs)
      printf '%s\n' rcp-default dcp-default cp-1x rsync-1x
      ;;
    remote)
      printf '%s\n' rcp-default rsync-1x
      ;;
    *) die "internal: unsupported scope $scope" ;;
  esac
}

rcp_parallelism() {
  case "$1" in
    rcp-default) printf '' ;;
    rcp-*) printf '%s' "${1#rcp-}" ;;
    *) die "internal: invalid rcp variant $1" ;;
  esac
}

dcp_parallelism() {
  case "$1" in
    dcp-default) printf '32' ;;
    dcp-*) printf '%s' "${1#dcp-}" ;;
    *) die "internal: invalid dcp variant $1" ;;
  esac
}

parallel_dirs_local() {
  local tool="$1"
  local source="$2"
  local destination="$3"
  local dir
  local pids=()
  mkdir -p "$destination"
  for dir in "$source"/dir[0-9]; do
    case "$tool" in
      cp) cp --reflink=auto -a "$dir" "$destination/" ;;
      rsync) rsync -a "$dir/" "$destination/$(basename -- "$dir")/" ;;
      *) die "internal: invalid local parallel tool $tool" ;;
    esac &
    pids+=("$!")
  done
  local pid
  for pid in "${pids[@]}"; do
    wait "$pid"
  done
}

parallel_dirs_remote_rsync() {
  local source="$1"
  local destination="$2"
  local dir
  local pids=()
  run_remote mkdir -p "$destination"
  for dir in "$source"/dir[0-9]; do
    rsync -a "$dir/" "$PEER:$destination/$(basename -- "$dir")/" &
    pids+=("$!")
  done
  local pid
  for pid in "${pids[@]}"; do
    wait "$pid"
  done
}

run_variant() {
  local scope="$1"
  local variant="$2"
  local source="$3"
  local destination="$4"
  case "$scope:$variant" in
    xfs:rcp-*|nfs:rcp-*|zfs:rcp-*)
      local rcp_limit
      rcp_limit="$(rcp_parallelism "$variant")"
      if [[ -n "$rcp_limit" ]]; then
        "$RCP_BIN" "--max-files-in-flight=$rcp_limit" "$source" "$destination"
      else
        "$RCP_BIN" "$source" "$destination"
      fi
      ;;
    remote:rcp-*)
      local remote_limit
      remote_limit="$(rcp_parallelism "$variant")"
      if [[ -n "$remote_limit" ]]; then
        "$RCP_BIN" "--max-files-in-flight=$remote_limit" "--max-connections=$remote_limit" \
          --rcpd-path "$RCPD_BIN" --remote-copy-conn-timeout-sec 60 \
          "$source" "$PEER:$destination"
      else
        "$RCP_BIN" --rcpd-path "$RCPD_BIN" --remote-copy-conn-timeout-sec 60 \
          "$source" "$PEER:$destination"
      fi
      ;;
    xfs:dcp-*|nfs:dcp-*|zfs:dcp-*)
      "$MPIRUN_BIN" --map-by :OVERSUBSCRIBE -np "$(dcp_parallelism "$variant")" \
        "$DCP_BIN" -p "$source" "$destination"
      ;;
    xfs:cp-1x|nfs:cp-1x|zfs:cp-1x)
      mkdir -p "$destination"
      cp --reflink=auto -a "$source/." "$destination/"
      ;;
    xfs:cp-10x|nfs:cp-10x|zfs:cp-10x)
      parallel_dirs_local cp "$source" "$destination"
      ;;
    xfs:rsync-1x|nfs:rsync-1x|zfs:rsync-1x)
      mkdir -p "$destination"
      rsync -a "$source/" "$destination/"
      ;;
    xfs:rsync-10x|nfs:rsync-10x|zfs:rsync-10x)
      parallel_dirs_local rsync "$source" "$destination"
      ;;
    remote:rsync-1x)
      run_remote mkdir -p "$destination"
      rsync -a "$source/" "$PEER:$destination/"
      ;;
    remote:rsync-10x)
      parallel_dirs_remote_rsync "$source" "$destination"
      ;;
    *) die "internal: unsupported variant $scope/$variant" ;;
  esac
}

describe_variant() {
  local scope="$1"
  local variant="$2"
  local source="$3"
  local destination="$4"
  case "$scope:$variant" in
    remote:rcp-default)
      shell_join "$RCP_BIN" --rcpd-path "$RCPD_BIN" --remote-copy-conn-timeout-sec 60 \
        "$source" "$PEER:$destination"
      ;;
    remote:rcp-*)
      local p="${variant#rcp-}"
      shell_join "$RCP_BIN" "--max-files-in-flight=$p" "--max-connections=$p" \
        --rcpd-path "$RCPD_BIN" --remote-copy-conn-timeout-sec 60 "$source" "$PEER:$destination"
      ;;
    xfs:rcp-default|nfs:rcp-default|zfs:rcp-default)
      shell_join "$RCP_BIN" "$source" "$destination"
      ;;
    xfs:rcp-*|nfs:rcp-*|zfs:rcp-*)
      shell_join "$RCP_BIN" "--max-files-in-flight=${variant#rcp-}" "$source" "$destination"
      ;;
    *:dcp-*) shell_join "$MPIRUN_BIN" --map-by :OVERSUBSCRIBE -np "$(dcp_parallelism "$variant")" "$DCP_BIN" -p "$source" "$destination" ;;
    *:cp-1x) shell_join cp --reflink=auto -a "$source/." "$destination/" ;;
    *:cp-10x) printf '10 concurrent: cp --reflink=auto -a SOURCE/dirN DEST/' ;;
    remote:rsync-1x) shell_join rsync -a "$source/" "$PEER:$destination/" ;;
    *:rsync-1x) shell_join rsync -a "$source/" "$destination/" ;;
    *:rsync-10x) printf '10 concurrent: rsync -a SOURCE/dirN/ DEST/dirN/' ;;
  esac
}

row_completed_successfully() {
  local run_id="$1"
  [[ -f "$RESULTS_JSONL" ]] || return 1
  python3 - "$RESULTS_JSONL" "$run_id" <<'PY'
import json
import sys

path, run_id = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    for line in handle:
        row = json.loads(line)
        if row["run_id"] == run_id and row["status"] == "ok":
            raise SystemExit(0)
raise SystemExit(1)
PY
}

is_capped_serial_nfs_row() {
  local scope="$1"
  local fileset="$2"
  local variant="$3"
  [[ "$scope" == nfs && "$fileset" == "$SERIAL_NFS_FILESET" &&
    ( "$variant" == cp-1x || "$variant" == rsync-1x ) ]]
}

serial_nfs_sample_was_censored() {
  local run_id="$1"
  [[ -f "$RESULTS_JSONL" ]] || return 1
  python3 - "$RESULTS_JSONL" "$run_id" "$SERIAL_NFS_TIMEOUT_SECONDS" <<'PY'
import json
import sys

path, run_id, cap = sys.argv[1], sys.argv[2], float(sys.argv[3])
with open(path, encoding="utf-8") as handle:
    for line in handle:
        row = json.loads(line)
        if (
            row["run_id"] == run_id
            and row.get("status") != "ok"
            and (
                row.get("failure_reason") == "timeout"
                or float(row.get("copy_seconds", 0)) >= cap - 60
            )
        ):
            raise SystemExit(0)
raise SystemExit(1)
PY
}

row_skipped_by_policy() {
  local scope="$1"
  local fileset="$2"
  local variant="$3"
  local iteration="$4"
  is_capped_serial_nfs_row "$scope" "$fileset" "$variant" || return 1
  ((iteration > 1)) && return 0
  serial_nfs_sample_was_censored "$scope/$fileset/$variant/$iteration"
}

scope_completed_successfully() {
  local scope="$1"
  local fileset="$2"
  local variant iteration
  while read -r variant; do
    for ((iteration = 1; iteration <= ITERATIONS; iteration++)); do
      row_skipped_by_policy "$scope" "$fileset" "$variant" "$iteration" && continue
      row_completed_successfully "$scope/$fileset/$variant/$iteration" || return 1
    done
  done < <(variant_list "$scope")
}

measure_one() {
  local scope="$1"
  local fileset="$2"
  local variant="$3"
  local iteration="$4"
  local source="$5"
  local destination="$6"
  stop_if_requested
  local run_id="$scope/$fileset/$variant/$iteration"
  local attempt
  attempt="$(next_attempt_number "$run_id")"
  local log_file="$RESULTS_DIR/logs/${scope}-${fileset}-${variant}-${iteration}.attempt-${attempt}.log"
  local time_file="$RESULTS_DIR/logs/${scope}-${fileset}-${variant}-${iteration}.attempt-${attempt}.time.txt"
  local source_stats source_files source_bytes
  local destination_stats destination_files destination_bytes
  local command_line t0 t1 t2 rc status failure_phase failure_reason run_timeout
  local time_stats user_cpu_seconds system_cpu_seconds max_rss_kib

  if row_skipped_by_policy "$scope" "$fileset" "$variant" "$iteration"; then
    log "policy: skipping $run_id after one 12-hour-capped serial sample"
    if ((iteration == 1)); then
      remove_local "$destination"
    fi
    return 0
  fi

  if ((RESUME)) && row_completed_successfully "$run_id"; then
    log "resume: skipping $run_id"
    return 0
  fi

  if ((DRY_RUN)); then
    printf '%s\t%s\n' "$run_id" "$(describe_variant "$scope" "$variant" "$source" "$destination")"
    return 0
  fi

  case "$scope" in
    remote)
      remove_remote "$destination"
      run_remote mkdir -p "$(dirname -- "$destination")"
      ;;
    *)
      remove_local "$destination"
      mkdir -p "$(dirname -- "$destination")"
      ;;
  esac

  cache_source_stats "$source"
  source_stats="${SOURCE_STATS_CACHE[$source]}"
  IFS=$'\t' read -r source_files source_bytes <<<"$source_stats"
  drop_source_caches "$source"
  sleep "$SETTLE_SECS"

  command_line="$(describe_variant "$scope" "$variant" "$source" "$destination")"
  log "run $run_id attempt=$attempt"
  run_timeout="$RUN_TIMEOUT"
  if is_capped_serial_nfs_row "$scope" "$fileset" "$variant"; then
    run_timeout="$SERIAL_NFS_TIMEOUT"
  fi
  t0="$(monotonic)"
  rc=0
  failure_phase=""
  failure_reason=""
  export PEER RCP_BIN RCPD_BIN DCP_BIN MPIRUN_BIN
  export -f die run_remote rcp_parallelism dcp_parallelism parallel_dirs_local
  export -f parallel_dirs_remote_rsync run_variant
  if /usr/bin/time -v -o "$time_file" timeout --kill-after=60s "$run_timeout" bash -c \
    'run_variant "$@"' -- "$scope" "$variant" "$source" "$destination" \
    >"$log_file" 2>&1 </dev/null; then
    :
  else
    rc=$?
    failure_phase=copy
    if ((rc == 124)); then
      failure_reason=timeout
    else
      failure_reason="command exited with status $rc"
    fi
  fi
  t1="$(monotonic)"
  time_stats="$(time_metrics "$time_file")"
  IFS=$'\t' read -r user_cpu_seconds system_cpu_seconds max_rss_kib <<<"$time_stats"

  if ((rc == 0)); then
    case "$scope" in
      remote)
        if run_remote sync -f "$destination" >>"$log_file" 2>&1; then
          :
        else
          rc=$?
          failure_phase=sync
          failure_reason="destination sync exited with status $rc"
        fi
        destination_stats="$(remote_tree_stats "$destination")"
        ;;
      *)
        if sync -f "$destination" >>"$log_file" 2>&1; then
          :
        else
          rc=$?
          failure_phase=sync
          failure_reason="destination sync exited with status $rc"
        fi
        destination_stats="$(tree_stats "$destination")"
        ;;
    esac
  else
    case "$scope" in
      remote)
        if run_remote test -e "$destination"; then
          destination_stats="$(remote_tree_stats "$destination")" || destination_stats=$'0\t0'
        else
          destination_stats=$'0\t0'
        fi
        ;;
      *)
        if [[ -e "$destination" ]]; then
          destination_stats="$(tree_stats "$destination")" || destination_stats=$'0\t0'
        else
          destination_stats=$'0\t0'
        fi
        ;;
    esac
  fi
  t2="$(monotonic)"

  IFS=$'\t' read -r destination_files destination_bytes <<<"$destination_stats"
  status=ok
  if ((rc != 0)); then
    status=failed
  elif [[ "$source_files" != "$destination_files" || "$source_bytes" != "$destination_bytes" ]]; then
    status=validation_failed
    rc=1
    failure_phase=validation
    failure_reason="expected ${source_files} files/${source_bytes} bytes, got ${destination_files} files/${destination_bytes} bytes"
  fi

  json_record "$run_id" "$scope" "$fileset" "$variant" "$iteration" "$source" "$destination" \
    "$command_line" "$status" "$rc" "$(elapsed "$t0" "$t1")" "$(elapsed "$t0" "$t2")" \
    "$source_files" "$source_bytes" "$destination_files" "$destination_bytes" "$log_file" \
    "$user_cpu_seconds" "$system_cpu_seconds" "$max_rss_kib" "$attempt" "$time_file" \
    "$failure_phase" "$failure_reason"
  [[ "$status" == ok ]] || die "run failed: $run_id; see $log_file"

  # Each destination can be close to 1 TiB. Keep only the current copy, not a matrix's worth.
  case "$scope" in
    remote) remove_remote "$destination" ;;
    *) remove_local "$destination" ;;
  esac
}

run_scope_for_fileset() {
  local scope="$1"
  local fileset="$2"
  local source variant iteration destination
  stop_if_requested
  if ((RESUME)) && scope_completed_successfully "$scope" "$fileset"; then
    log "resume: skipping complete scope $scope/$fileset"
    return 0
  fi
  source="$(source_for_scope "$scope" "$fileset")"
  case "$scope" in
    xfs|remote) stage_fileset "$scope" "$fileset" ;;
  esac
  [[ -d "$source" ]] || die "source is missing for $scope: $source"

  for ((iteration = 1; iteration <= ITERATIONS; iteration++)); do
    while read -r variant; do
      destination="$(destination_for "$scope" "$fileset" "$variant" "$iteration")"
      measure_one "$scope" "$fileset" "$variant" "$iteration" "$source" "$destination"
    done < <(variant_list "$scope")
  done

  if (( ! KEEP_STAGE )) && [[ "$scope" == xfs || "$scope" == remote ]]; then
    # xfs and remote share the same stage, so it is removed only after the caller's final scope.
    :
  fi
}

cleanup_stage_after_fileset() {
  local fileset="$1"
  ((KEEP_STAGE)) && return 0
  if has_scope xfs || has_scope remote; then
    remove_local "$WORK_DIR/stage/$fileset"
  fi
}

main() {
  validate_config
  RESULTS_DIR="$RESULTS_ROOT/$LABEL"
  RESULTS_JSONL="$RESULTS_DIR/results.jsonl"
  MANIFEST="$RESULTS_DIR/manifest.txt"
  if has_scope zfs && ! has_scope xfs && ! has_scope nfs && ! has_scope remote; then
    TMP_STATE="$ZFS_ROOT/state"
  else
    TMP_STATE="$WORK_DIR/state"
  fi

  if ((DRY_RUN)); then
    local fileset scope variant iteration
    while read -r fileset; do
      while read -r scope; do
        local source
        source="$(source_for_scope "$scope" "$fileset")"
        while read -r variant; do
          for ((iteration = 1; iteration <= ITERATIONS; iteration++)); do
            printf '%s\t%s\n' "$scope/$fileset/$variant/$iteration" \
              "$(describe_variant "$scope" "$variant" "$source" \
                "$(destination_for "$scope" "$fileset" "$variant" "$iteration")")"
          done
        done < <(variant_list "$scope")
      done < <(split_csv "$SCOPES")
    done < <(split_csv "$FILESETS")
    exit 0
  fi

  mkdir -p "$RESULTS_DIR/logs" "$TMP_STATE" ||
    die "cannot create results or scratch state under $RESULTS_ROOT / $TMP_STATE"
  export TMPDIR="$TMP_STATE"
  export XDG_STATE_HOME="$TMP_STATE/state"
  mkdir -p "$XDG_STATE_HOME"
  resolve_cache_eviction
  write_manifest

  local fileset scope
  while read -r fileset; do
    for scope in xfs nfs remote zfs; do
      has_scope "$scope" || continue
      run_scope_for_fileset "$scope" "$fileset"
    done
    cleanup_stage_after_fileset "$fileset"
  done < <(split_csv "$FILESETS")
}

main
