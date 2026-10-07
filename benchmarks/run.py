"""Run repeatable copy workloads and retain validated measurements."""

import argparse
from datetime import datetime, timezone
import hashlib
import html
import json
import math
import os
from pathlib import Path
import platform
import re
import resource
import shlex
import shutil
import signal
import stat
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from benchmarks.strict_json import parse_json
from benchmarks import timings, operations, transport, pairs, measurements, clocks
from benchmarks.operations import expected_counts


TIMING_POLICY = "monotonic launch-to-last-child-exit; excludes verification and cache preparation"
VERIFICATION_POLICY = "exact relative directory and regular-file paths, sizes, and SHA256 contents; source tree must match initial scan after all case trials"
FIXTURE_CONTRACT_REVISION = 2
ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
MOUNTINFO_ESCAPE = re.compile(r"\\(040|011|012|134)")


def _fields(value, required, optional, name):
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    missing = set(required) - value.keys()
    unknown = value.keys() - set(required) - set(optional)
    if missing or unknown:
        raise ValueError(f"{name}: missing {sorted(missing)}; unknown {sorted(unknown)}")


def _positive(value, name):
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def load_manifest(path):
    data = parse_json(Path(path).read_text())
    _fields(data, {"schema_version", "cases", "variants"}, set(), "manifest")
    if type(data["schema_version"]) is not int or data["schema_version"] != 1:
        raise ValueError("unsupported manifest schema_version")
    if not isinstance(data["cases"], list) or not data["cases"] or not isinstance(data["variants"], list) or not data["variants"]:
        raise ValueError("manifest requires nonempty cases and variants")
    for kind in ("cases", "variants"):
        seen = set()
        for entry in data[kind]:
            required = {"id", "directory_widths", "file_size_bytes"} if kind == "cases" else {"id", "tool", "args", "processes"}
            optional = {"description", "mode", "files_per_leaf", "files_per_directory"} if kind == "cases" else {"description"}
            _fields(entry, required, optional, kind[:-1])
            identifier = entry["id"]
            if not isinstance(identifier, str) or not ID_PATTERN.fullmatch(identifier):
                raise ValueError(f"invalid {kind} id: {identifier!r}")
            if kind == "variants" and identifier == "rcp-baseline":
                raise ValueError("reserved variant id: rcp-baseline")
            if identifier in seen:
                raise ValueError(f"duplicate {kind} id: {identifier}")
            seen.add(identifier)
            if "description" in entry and not isinstance(entry["description"], str):
                raise ValueError("description must be a string")
            if kind == "cases":
                operations.validate_case(entry)
            else:
                if entry["tool"] not in ("rcp", "rsync", "cp"):
                    raise ValueError("variant tool must be rcp, rsync, or cp")
                if not isinstance(entry["args"], list) or not all(isinstance(arg, str) and arg for arg in entry["args"]):
                    raise ValueError("variant args must be nonempty strings")
                _positive(entry["processes"], "processes")
    return data


def scan_tree(root):
    root = Path(root)
    try:
        root_metadata = root.stat(follow_symlinks=False)
    except OSError as exc:
        raise ValueError(f"missing regular directory: {root}") from exc
    if not stat.S_ISDIR(root_metadata.st_mode):
        raise ValueError(f"missing regular directory: {root}")
    entries = {}
    counts = {"directories": 0, "files": 0, "bytes": 0}
    for current, dirs, files in os.walk(root, followlinks=False):
        for name in sorted(dirs + files):
            path = Path(current) / name
            relative = path.relative_to(root).as_posix()
            metadata = path.stat(follow_symlinks=False)
            if stat.S_ISLNK(metadata.st_mode):
                raise ValueError(f"unexpected symlink: {relative}")
            if stat.S_ISDIR(metadata.st_mode):
                entries[relative] = {"type": "directory"}
                counts["directories"] += 1
            elif stat.S_ISREG(metadata.st_mode):
                digest = hashlib.sha256()
                with path.open("rb") as handle:
                    for block in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(block)
                size = metadata.st_size
                entries[relative] = {"type": "file", "size": size, "sha256": digest.hexdigest()}
                counts["files"] += 1
                counts["bytes"] += size
            else:
                raise ValueError(f"unexpected special path: {relative}")
    digest = hashlib.sha256(json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"entries": entries, "counts": counts, "digest": digest}


def validate_tree(source, destination, expected=None):
    expected = expected or scan_tree(source)
    try:
        actual = scan_tree(destination)
    except (OSError, ValueError) as exc:
        return {"ok": False, "error": str(exc)}
    missing = sorted(expected["entries"].keys() - actual["entries"].keys())
    extra = sorted(actual["entries"].keys() - expected["entries"].keys())
    changed = sorted(key for key in expected["entries"].keys() & actual["entries"].keys() if expected["entries"][key] != actual["entries"][key])
    result = {"ok": not (missing or extra or changed), "counts": actual["counts"], "digest": actual["digest"]}
    if not result["ok"]:
        result["error"] = f"missing={missing[:5]}, extra={extra[:5]}, changed={changed[:5]}"
    return result


def _validate_variant_mode(variant, mode):
    tool = variant["tool"]
    if tool == "cp" and mode != "local":
        raise ValueError("cp variants require local mode")
    if tool == "rsync" and mode == "loopback" and any(arg == "--rsync-path" or arg.startswith("--rsync-path=") for arg in variant["args"]):
        raise ValueError("custom --rsync-path is reserved in loopback mode")


def plan_commands(variant, source, destination, tools, mode, operation="fresh", *, source_endpoint=None):
    _validate_variant_mode(variant, mode)
    operations.validate_variant(variant, operation)
    if source_endpoint is not None:
        if not isinstance(source_endpoint, transport.SourceEndpoint):
            raise ValueError("source_endpoint must be a typed owned transport endpoint")
        transport.validate_request(mode, 0, [variant], {})
    host = source_endpoint.host if source_endpoint is not None else "localhost"
    source = Path(source)
    destination = Path(destination)
    tool = variant["tool"]
    executable = str(tools[tool])
    args = list(variant["args"])
    if tool == "rcp" and operation != "fresh":
        args.append("--overwrite")
    if tool == "rcp" and mode == "loopback":
        args += ["--force-remote", f"--rcpd-path={tools['rcpd']}"]
    if tool == "rsync" and source_endpoint is not None:
        args.append(f"--rsh={shlex.quote(str(source_endpoint.ssh_launcher))}")
    if tool == "rsync" and mode == "loopback":
        args.append(f"--rsync-path={shlex.quote(executable)}")
    processes = variant["processes"]
    if processes == 1:
        operand = f"{host}:{source}" if tool == "rcp" and mode == "loopback" else str(source)
        if tool == "rsync":
            operand = f"{host}:{source}" if mode == "loopback" else str(source)
            return [[executable, *args, operand + "/", str(destination) + "/"]]
        return [[executable, *args, operand, str(destination)]]
    children = sorted(source.iterdir())
    if not children or any(not child.is_dir() for child in children):
        raise ValueError("parallel variant needs top-level directories only")
    if len(children) > processes:
        raise ValueError(f"top-level directories ({len(children)}) exceed configured processes ({processes})")
    commands = []
    for child in children:
        operand = f"{host}:{child}" if mode == "loopback" else str(child)
        target = destination / child.name if tool == "rcp" else destination
        commands.append([executable, *args, operand, str(target)])
    return commands


class _CancelledSignal(Exception):
    pass


class _SignalCancellation:
    def __init__(self, condition):
        self.condition = condition
        self.previous = {}
        self.pending = None

    def __enter__(self):
        if threading.current_thread() is not threading.main_thread():
            return self
        self.previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        installed = []
        try:
            for number in self.previous:
                signal.signal(number, self._record)
                installed.append(number)
        except BaseException:
            for number in reversed(installed):
                signal.signal(number, self.previous[number])
            raise
        return self

    def _record(self, number, _frame):
        if self.previous[number] == signal.SIG_IGN:
            return
        if self.pending is None:
            self.pending = number
        with self.condition:
            self.condition.notify_all()

    def checkpoint(self):
        if self.pending is not None:
            raise _CancelledSignal

    def __exit__(self, _type, _value, _traceback):
        for number, handler in self.previous.items():
            signal.signal(number, handler)
        if self.pending is not None:
            number = self.pending
            handler = self.previous[number]
            if callable(handler):
                handler(number, None)
            if number == signal.SIGINT:
                raise KeyboardInterrupt
            raise InterruptedError("SIGTERM")
        return False


def _execution_failure(outcome, *, wrapped=False):
    """Classify primary execution failure once; resource diagnostics are secondary."""
    if outcome["launch_error"] is not None:
        kind, stage, category = "launch", "outer_launch", "execution"
        message = f"resource supervisor launch failed: {outcome['launch_error']}"
    elif outcome["timed_out"]:
        kind, stage, category = "timeout", "command", "timeout"
        message = "command timed out; payload termination is unconfirmed"
    elif not outcome["ok"]:
        kind, stage, category = "command", "command", "execution"
        message = f"resource supervisor failed with exit codes {outcome['exit_codes']}; payload failure cause is unconfirmed"
    elif wrapped and outcome["resources"]["status"] != "complete":
        return dict(kind="resources", stage="postflight", category="validation",
                    message=f"resource collection failed: {outcome['resources']['error']}")
    else:
        return None
    return dict(kind=kind, stage=stage, category=category,
                message=message if wrapped else "command failed or timed out")


def execute_commands(commands, log_dir, timeout, *, stable_summary_locale=False, resource_time=None, command_clocks=False):
    if resource_time is not None and len(commands) != 1:
        raise ValueError("local resources require exactly one command")
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=False)
    resource_path = log_dir / "resources.json"
    launched_commands = [measurements.wrap(command, resource_time, resource_path) for command in commands] if resource_time else commands
    children = []
    files = []
    waiters = []
    finished = {}
    condition = threading.Condition()
    child_environment = {**os.environ, "LC_ALL": operations.SUMMARY_LOCALE, "LANG": operations.SUMMARY_LOCALE} if stable_summary_locale or resource_time is not None else None
    started = time.monotonic()
    clock_start = clocks.sample() if command_clocks else None
    clock_finishes = {}
    launch_error = None
    timed_out = False
    cleanup_groups = False
    resources = None
    def terminate_groups():
        groups = {process.pid for process in children}
        def signal_groups(number):
            for group in tuple(groups):
                try:
                    os.killpg(group, number)
                except ProcessLookupError:
                    groups.remove(group)
        signal_groups(signal.SIGTERM)
        grace = time.monotonic() + 1
        with condition:
            while groups:
                # a reaped supervisor can leave its measured command alive in the group
                signal_groups(0)
                remaining = grace - time.monotonic()
                if not groups or remaining <= 0:
                    break
                condition.wait(min(remaining, .05))
        signal_groups(signal.SIGKILL)
    with _SignalCancellation(condition) as cancellation:
        try:
            for index, command in enumerate(launched_commands):
                cancellation.checkpoint()
                stdout_path = log_dir / f"{index}.stdout.log"
                stderr_path = log_dir / f"{index}.stderr.log"
                stdout = stdout_path.open("wb")
                files.append(stdout)
                stderr = stderr_path.open("wb")
                files.append(stderr)
                try:
                    process = subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True, env=child_environment)
                except OSError as exc:
                    launch_error = str(exc)
                else:
                    children.append(process)
                    def wait_child(i=index, p=process):
                        code = p.wait()
                        stamp = time.monotonic()
                        with condition:
                            finished[i] = (code, stamp)
                            condition.notify_all()
                        if command_clocks:
                            clock_finish = clocks.sample()
                            with condition:
                                clock_finishes[i] = clock_finish
                    waiter = threading.Thread(target=wait_child, daemon=True)
                    waiter.start()
                    waiters.append(waiter)
                cancellation.checkpoint()
                if launch_error is not None:
                    break
            deadline = started + timeout
            with condition:
                while len(finished) < len(children) and launch_error is None:
                    cancellation.checkpoint()
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    condition.wait(min(remaining, .1))
                cancellation.checkpoint()
                timed_out = launch_error is None and len(finished) < len(children)
            cleanup_groups = bool(launch_error is not None or timed_out or any(code != 0 for code, _ in finished.values()))
            if resource_time is not None and not cleanup_groups:
                resources = measurements.collect(resource_path, successful=True)
                cleanup_groups = resources["status"] != "complete"
            cancellation.checkpoint()
        except BaseException:
            cleanup_groups = True
            raise
        finally:
            if cleanup_groups:
                terminate_groups()
            for waiter in waiters:
                waiter.join()
            for process in children:
                if process.returncode is None:
                    process.wait()
            for handle in files:
                handle.close()
    codes = [process.returncode for process in children]
    completion = max((stamp for _, stamp in finished.values()), default=time.monotonic())
    status = measurements.payload_status(codes, timed_out, launch_error, expected_commands=len(commands))
    outcome = {"ok": status == "succeeded", "timed_out": timed_out, "launch_error": launch_error, "elapsed_seconds": max(0, completion - started), "exit_codes": codes, "logs": [{"stdout": str(log_dir / f"{index}.stdout.log"), "stderr": str(log_dir / f"{index}.stderr.log")} for index in range(len(children))]}
    if command_clocks:
        index = max(finished, key=lambda i: finished[i][1]) if finished else None
        outcome["command_clocks"] = clocks.observation(clock_start, clock_finishes.get(index), index)
    if resource_time is not None:
        outcome["measurement_commands"] = launched_commands
        outcome["exit_status_scope"] = measurements.EXIT_STATUS_SCOPE
        outcome["payload_status"] = status
        outcome["resources"] = resources or measurements.collect(resource_path, status == "succeeded")
    failure = _execution_failure(outcome, wrapped=resource_time is not None)
    if failure is not None:
        outcome["failure"] = failure
        outcome["ok"] = False
    return outcome


def _read(path):
    try:
        return Path(path).read_text()
    except OSError:
        return ""


def _mount(path):
    def decoded(field):
        return MOUNTINFO_ESCAPE.sub(lambda match: chr(int(match.group(1), 8)), field)
    resolved = str(Path(path).resolve())
    best = {}
    for line in _read("/proc/self/mountinfo").splitlines():
        before, separator, after = line.partition(" - ")
        if not separator:
            continue
        left = before.split()
        right = after.split()
        if len(left) < 6 or len(right) < 3:
            continue
        mountpoint = decoded(left[4])
        if (resolved == mountpoint or resolved.startswith(mountpoint.rstrip("/") + "/")) and len(mountpoint) > len(best.get("mountpoint", "")):
            best = {"mountpoint": mountpoint, "filesystem_type": right[0], "mount_source": decoded(right[1]), "mount_options": sorted(set(left[5].split(",") + right[2].split(",")))}
    return best


def environment(source_root, destination_root):
    cpu = ""
    for line in _read("/proc/cpuinfo").splitlines():
        if line.startswith("model name"):
            cpu = line.partition(":")[2].strip()
            break
    quota = _read("/sys/fs/cgroup/cpu.max").strip()
    memory = _read("/sys/fs/cgroup/memory.max").strip()
    return {"kernel": platform.release(), "architecture": platform.machine(), "cpu_model": cpu, "effective_parallelism": len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count(), "cpu_quota": quota, "memory_limit": memory, "fd_limit": resource.getrlimit(resource.RLIMIT_NOFILE)[0], "filesystem": {"source": _mount(source_root), "destination": _mount(destination_root)}}


def series_id(case, variant, cache_policy, topology, runner_label, endpoint_environment, tools, storage_ids=None, ssh_transport_profile=None, timing_collection="legacy", timing_capability=None, timing_request="legacy", operation_revision=None, owned_transport=None, experiment=None):
    filesystem = endpoint_environment.get("filesystem", {})
    storage_ids = storage_ids or {}
    comparable_environment = {key: value for key, value in endpoint_environment.items() if key != "filesystem"}
    def semantic_mount(side, info):
        options = info.get("mount_options", [])
        if info.get("filesystem_type") == "overlay":
            options = [option for option in options if option.partition("=")[0] not in ("lowerdir", "upperdir", "workdir")]
        identity = {"kind": "explicit", "value": storage_ids[side]} if storage_ids.get(side) is not None else {"kind": "observed", "mount_source": info.get("mount_source"), "mountpoint": info.get("mountpoint")}
        return {"filesystem_type": info.get("filesystem_type"), "mount_options": sorted(set(options)), "storage_identity": identity}
    comparable_environment["filesystem"] = {side: semantic_mount(side, info) for side, info in filesystem.items()}
    stable_references = {}
    for tool in ("rsync", "cp", "rcp-baseline", "rcpd-baseline") + (("ssh",) if topology == "loopback" else ()):
        if tool in tools:
            stable_references[tool] = {"version": tools[tool]["version"], "sha256": tools[tool]["sha256"]}
    value = {"case": {key: item for key, item in case.items() if key != "description"}, "variant": {key: item for key, item in variant.items() if key != "description"}, "cache_policy": cache_policy, "topology": topology, "runner_label": runner_label, "environment": comparable_environment, "reference_versions": stable_references, "fixture_contract_revision": FIXTURE_CONTRACT_REVISION, "timing_policy": TIMING_POLICY, "timing_request": timing_request, "timing_collection": timing_collection, "timing_capability": timing_capability, "verification_policy": VERIFICATION_POLICY}
    if experiment is not None:
        value["experiment"] = experiment
    if operation_revision is not None:
        value["case"]["mode"] = case.get("mode", "fresh")
        value["operation_contract_revision"] = operation_revision
        value["child_locale"] = measurements.child_locale(variant, bool((experiment or {}).get("local_resources")))
        if cache_policy == "source-verified":
            value["cache_contract_revision"] = operations.CACHE_REVISION
    if owned_transport is not None:
        value["owned_transport"] = owned_transport
    if topology == "loopback":
        value["ssh_transport_profile"] = ssh_transport_profile
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _tool(path, version_flag="--version"):
    # preserve the invoked basename for multicall binaries such as Nix coreutils
    path = Path(path).absolute()
    if not os.access(path, os.X_OK):
        raise ValueError(f"not executable: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    result = subprocess.run([str(path), version_flag], capture_output=True, text=True, timeout=10)
    if result.returncode:
        raise ValueError(f"version command failed: {path}")
    version_output = result.stdout.strip() or result.stderr.strip()
    if not version_output:
        raise ValueError(f"missing version output: {path}")
    return {"path": str(path), "version": version_output.splitlines()[0], "sha256": digest.hexdigest()}


def _supports_timings(path):
    result = subprocess.run([str(path), "--help"], capture_output=True, text=True, timeout=10)
    return result.returncode == 0 and re.search(r"--timings(?:[=\s]|$)", result.stdout + "\n" + result.stderr) is not None


def _git(*args):
    try:
        result = subprocess.run(["git", *args], capture_output=True, text=True)
    except OSError:
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def _revision():
    commit = _git("rev-parse", "HEAD")
    branch = _git("branch", "--show-current")
    status = _git("status", "--porcelain")
    return {"commit": commit or None, "branch": branch or None, "dirty": None if status is None else bool(status)}


def _persist(output, record):
    temporary = output / "results.json.tmp"
    temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    temporary.replace(output / "results.json")
    lines = [f"# Benchmark {record['run_id']}", "", f"Status: **{record['status']}**", ""]
    if record.get("context", {}).get("purpose") == "smoke":
        lines += ["Smoke check; excluded from performance trends.", ""]
    if record.get("error"):
        lines += [f"Error: {record['error']}", ""]
    if record["summaries"]:
        lines += ["| Case | Variant | Median (s) | Min–max (s) | Repeats |",
                  "| --- | --- | ---: | ---: | ---: |"]
    for summary in record["summaries"]:
        lines.append(f"| {summary['case_id']} | {summary['variant_id']} | {summary['median']:.3f} | {summary['minimum']:.3f}–{summary['maximum']:.3f} | {len(summary['samples'])} |")
    if "pairing" in record["context"]:
        lines += ["", "## Adjacent pairs", "", "Candidate / reference wall time; incomplete or failed pairs and pairs from unfinished or failed cases are not compared.", "", "| Case | Pair | Block | Order | Ratio |", "| --- | ---: | ---: | --- | ---: |"]
        for pair in pairs.comparisons(record):
            lines.append(f"| {pair['case_id']} | {pair['pair']} | {pair['block']} | {' → '.join(pair['order'])} | {pair['candidate_over_reference']:.4f} |")
    if "command_clocks" in record["context"]:
        lines += ["", "## Command clock observations", "", clocks.QUALIFICATION, "",
                  "Bounds account for sequential-read skew only. Bracketed endpoints do not identify when a clock changed.", "",
                  "| Case | Variant | Repeat | Brackets | RAW minus MONOTONIC (s) | REALTIME minus MONOTONIC (s) |",
                  "| --- | --- | ---: | --- | --- | --- |"]
        for trial in record["trials"]:
            if "command_clocks" not in trial:
                continue
            observed = clocks.project(trial["command_clocks"])
            def bounds(key):
                values = observed[key + "_minus_monotonic_bounds_seconds"]
                return "unavailable" if values is None else f"[{values[0]:.9f}, {values[1]:.9f}]"
            lines.append(f"| {trial['case_id']} | {trial['variant_id']} | {trial['iteration']} | {observed['status']} | {bounds('raw')} | {bounds('realtime')} |")
    if "local_resources" in record["context"]:
        lines += ["", "## Local process resources", "", measurements.SCOPE + ". RSS is a per-command peak, never a sum of simultaneous resident memory.", "", "| Case | Variant | Repeat | Trial status | User CPU s | System CPU s | Peak RSS KiB |", "| --- | --- | ---: | --- | ---: | ---: | ---: |"]
        for trial in record["trials"]:
            metrics = trial.get("resources", {}).get("metrics")
            if metrics is not None:
                lines.append(f"| {trial['case_id']} | {trial['variant_id']} | {trial['iteration']} | {trial['status']} | {metrics['user_seconds']:.2f} | {metrics['system_seconds']:.2f} | {metrics['max_rss_kib']} |")
    timing_trials = [trial for trial in record["trials"] if "timings" in trial]
    if timing_trials:
        lines += ["", "## Scoped timings", "", "Scope durations are cumulative elapsed seconds across invocations. Scopes can overlap each other and command wall time; their totals are not additive wall time.", "", "| Case | Variant | Repeat | Role | Scope | Count | Finished | Interrupted | Cumulative elapsed (s) | Mean (s) | P50 (s) | P95 (s) | Max (s) |", "| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
        for trial in timing_trials:
            timing = trial["timings"]
            if not timing["reports"]:
                lines.append(f"| {trial['case_id']} | {trial['variant_id']} | {trial['iteration']} | {timing['status']} | — | — | — | — | — | — | — | — | — |")
            for item in timing["reports"]:
                for scope in item["scopes"]:
                    name = html.escape(scope["name"]).replace("|", "\\|")
                    lines.append(f"| {trial['case_id']} | {trial['variant_id']} | {trial['iteration']} | {item['identifier']} | {name} | {scope['count']} | {scope['finished']} | {scope['interrupted']} | {scope['total_seconds']:.6f} | {scope['mean_seconds']:.6f} | {scope['p50_seconds']:.6f} | {scope['p95_seconds']:.6f} | {scope['max_seconds']:.6f} |")
    if record.get("context", {}).get("purpose") != "smoke":
        short = [f"{item['case_id']}/{item['variant_id']}" for item in record["summaries"] if item["minimum"] < 10]
        if short:
            lines += ["", "Short samples (<10 s): " + ", ".join(short) + ". Treat small timing changes cautiously."]
    (output / "summary.md").write_text("\n".join(lines) + "\n")


def _prepare_cache(policy, source, timeout, expected=None, metadata=None):
    if policy == "source-verified":
        if expected is None or metadata is None:
            raise ValueError("source-verified requires the initial source snapshots")
        return operations.validate_source(source, expected, metadata)
    if policy == "uncontrolled":
        return
    subprocess.run(["sync"], check=True, timeout=timeout)
    if policy == "source-warm":
        for current, _, files in os.walk(source):
            for name in files:
                with (Path(current) / name).open("rb") as handle:
                    for _ in iter(lambda: handle.read(1024 * 1024), b""):
                        pass
        return
    if sys.platform != "linux":
        raise ValueError("linux-drop-caches requires Linux")
    subprocess.run(["sudo", "-n", "sh", "-c", "printf 3 > /proc/sys/vm/drop_caches"], check=True, capture_output=True, text=True, timeout=timeout)


def _select(items, selected, kind):
    if len(selected) != len(set(selected)):
        raise ValueError(f"duplicate {kind} selection")
    by_id = {entry["id"]: entry for entry in items}
    missing = set(selected) - by_id.keys()
    if missing:
        raise ValueError(f"unknown {kind}: {sorted(missing)}")
    return [by_id[key] for key in selected]


def _nonblank(value):
    if not value.strip():
        raise argparse.ArgumentTypeError("value must not be blank")
    return value.strip()


def _arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("cases.json"))
    parser.add_argument("--case", action="append", dest="cases")
    parser.add_argument("--variant", action="append", dest="variants")
    parser.add_argument("--bin-dir", type=Path)
    parser.add_argument("--baseline-bin-dir", type=Path)
    parser.add_argument("--mode", choices=("local", "loopback"), default="local")
    parser.add_argument("--rtt-ms", type=int, choices=(0, 2, 10), help="opt into a private per-trial loopback namespace")
    parser.add_argument("--purpose", choices=("smoke", "performance"), default="performance")
    parser.add_argument("--source-root", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--destination-root", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--source-storage-id", type=_nonblank)
    parser.add_argument("--destination-storage-id", type=_nonblank)
    parser.add_argument("--cache", choices=("source-warm", "linux-drop-caches", "uncontrolled", "source-verified"), default="source-warm")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runner-label", type=_nonblank, default="local")
    parser.add_argument("--files-in-flight")
    parser.add_argument("--ssh-transport-profile", type=_nonblank)
    parser.add_argument("--no-timings", action="store_true", help="disable scoped timing collection for overhead diagnostics")
    parser.add_argument("--paired-seed", type=int, help="opt into adjacent balanced local rcp/base pairs; repetitions counts pairs and must be even")
    parser.add_argument("--local-resources", action="store_true", help="collect local single-process GNU-time CPU/RSS/context-switch metrics")
    parser.add_argument("--resource-time", type=Path, help="GNU time executable for --local-resources")
    parser.add_argument("--build-provenance", type=Path, help="local-only schema-one caller-declared build metadata bound to executable hashes")
    parser.add_argument("--command-clocks", action="store_true", help="record bracketed local command clock observations without changing elapsed timing")
    args = parser.parse_args(argv)
    if args.command_clocks and args.mode != "local":
        parser.error("--command-clocks requires local mode")
    if args.build_provenance is not None:
        try:
            measurements.validate_provenance_mode(args.mode)
        except ValueError as error:
            parser.error(str(error))
    if args.resource_time is not None and not args.local_resources:
        parser.error("--resource-time requires --local-resources")
    if args.rtt_ms is not None and args.mode != "loopback":
        parser.error("--rtt-ms requires loopback mode")
    if args.mode == "local" and args.ssh_transport_profile is not None:
        parser.error("--ssh-transport-profile requires loopback mode")
    return args


def _default_bin_dir():
    repository = Path(__file__).resolve().parent.parent
    target_root = Path(os.environ.get("CARGO_TARGET_DIR", repository / "target"))
    target = subprocess.run([str(repository / "scripts" / "cargo-host.sh"), "--print-target"], capture_output=True, text=True, check=True).stdout.strip()
    return target_root / target / "release"


def main(argv=None):
    args = _arguments(argv)
    run_started = time.monotonic()
    collect_costs = args.paired_seed is not None or args.local_resources
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / "logs").mkdir()
    record = {"schema_version": 1, "run_id": uuid.uuid4().hex, "timestamp": datetime.now(timezone.utc).isoformat(), "status": "running", "revision": _revision(), "context": {"runner_label": args.runner_label, "topology": args.mode, "purpose": args.purpose, "cache_policy": args.cache, "timing_policy": TIMING_POLICY, "verification_policy": VERIFICATION_POLICY, "source": str(args.source_root.resolve()), "destination": str(args.destination_root.resolve()), "repository": os.environ.get("GITHUB_REPOSITORY", ""), "run_url": os.environ.get("BENCHMARK_RUN_URL", ""), "fixture_policy": "filegen --leaf-files --bufsize=min(file_size_bytes,1048576); random bytes without fixed seed; verified counts and digest", "fixture_contract_revision": FIXTURE_CONTRACT_REVISION, "directory_count_policy": "directories below fixture root; excludes fixture root"}, "tools": {}, "cases": [], "variants": [], "trials": [], "summaries": []}
    if args.command_clocks:
        record["context"]["command_clocks"] = clocks.POLICY
    if collect_costs or args.build_provenance is not None or args.command_clocks:
        record["context"]["measurement_environment"] = {key: os.environ[key] for key in measurements.ENVIRONMENT_KEYS if key in os.environ}
    record["context"]["operation_contract_revision"] = operations.CONTRACT_REVISION
    record["context"]["summary_locale"] = operations.SUMMARY_LOCALE
    if args.cache == "source-verified":
        record["context"]["cache_contract_revision"] = operations.CACHE_REVISION
    if args.mode == "loopback":
        record["context"]["ssh_transport_profile"] = transport.PROFILE if args.rtt_ms is not None else args.ssh_transport_profile
    if args.rtt_ms is not None:
        record["context"]["owned_transport"] = transport.semantics(args.rtt_ms)
    if args.baseline_bin_dir:
        record["context"]["baseline_commit"] = os.environ.get("RCP_BENCH_BASELINE_COMMIT", "")
    storage_ids = {"source": args.source_storage_id, "destination": args.destination_storage_id}
    record["context"]["storage_ids"] = storage_ids
    record["context"]["timing_request"] = "disabled" if args.no_timings else "automatic"
    _persist(output, record)
    source_scratch = None
    destination_scratch = None
    try:
        if args.repetitions <= 0 or not math.isfinite(args.timeout) or args.timeout <= 0:
            raise ValueError("repetitions and timeout must be positive; timeout must be finite")
        if args.paired_seed is not None:
            if not args.no_timings:
                raise ValueError("paired v1 requires --no-timings for identical role instrumentation")
            record["context"]["pairing"] = pairs.configuration(args.paired_seed, args.repetitions)
        manifest = load_manifest(args.manifest)
        cases = _select(manifest["cases"], args.cases or ["tiny-10k"], "case")
        all_directory_files = any("files_per_directory" in case for case in cases)
        default_variants = ["rcp-default", "rsync-a"] + ([] if all_directory_files else ["rsync-a-10"]) + (["cp-a"] if args.mode == "local" else [])
        variants = _select(manifest["variants"], args.variants or default_variants, "variant")
        for variant in variants:
            _validate_variant_mode(variant, args.mode)
        if all_directory_files:
            if any(variant["processes"] > 1 for variant in variants):
                raise ValueError("partitioned variants cannot copy files_per_directory cases with root files")
            record["context"]["fixture_policy"] = "filegen --leaf-files for files_per_leaf; files_per_directory includes fixture root and intermediate directories; --bufsize=min(file_size_bytes,1048576); random bytes without fixed seed; verified counts and digest"
        if args.files_in_flight is not None:
            if not any(variant["tool"] == "rcp" for variant in variants):
                raise ValueError("files-in-flight requires a selected rcp variant")
            limits = args.files_in_flight.split(",")
            if any(not item.isdecimal() or int(item) <= 0 for item in limits):
                raise ValueError("files-in-flight must be positive comma-separated integers")
            variants += [{**variant, "id": f"{variant['id']}-f{limit}", "args": [*variant["args"], f"--max-files-in-flight={limit}"]} for variant in variants if variant["tool"] == "rcp" for limit in limits]
        if args.baseline_bin_dir:
            default = next((variant for variant in variants if variant["id"] == "rcp-default"), None)
            if default is None or default["tool"] != "rcp":
                raise ValueError("baseline requires rcp-default selection using tool rcp")
            variants.append({**default, "id": "rcp-baseline"})
        if len({variant["id"] for variant in variants}) != len(variants):
            raise ValueError("variant expansion produced duplicate ids")
        if args.rtt_ms is not None:
            transport.validate_request(args.mode, args.rtt_ms, variants, os.environ)
            if args.ssh_transport_profile is not None:
                raise ValueError("owned RTT supplies its own SSH profile; omit --ssh-transport-profile")
        for case in cases:
            for variant in variants:
                operations.validate_variant(variant, case.get("mode", "fresh"))
        if args.paired_seed is not None:
            pairs.validate_selection(args.mode, variants)
        if args.local_resources:
            measurements.validate_selection(args.mode, variants)
        record["cases"] = cases
        record["variants"] = variants
        _persist(output, record)
        for root in (args.source_root, args.destination_root):
            if not root.is_dir():
                raise ValueError(f"scratch parent does not exist: {root}")
        source_scratch = Path(tempfile.mkdtemp(prefix="rcp-bench-source-", dir=args.source_root)).resolve()
        destination_scratch = Path(tempfile.mkdtemp(prefix="rcp-bench-destination-", dir=args.destination_root)).resolve()
        bin_dir = (args.bin_dir or _default_bin_dir()).resolve()
        needs_rcp = any(variant["tool"] == "rcp" for variant in variants)
        record["tools"]["filegen"] = _tool(bin_dir / "filegen")
        if needs_rcp:
            record["tools"]["rcp"] = _tool(bin_dir / "rcp")
            record["tools"]["rcpd"] = _tool(bin_dir / "rcpd")
        for tool in ("rsync", "cp"):
            if any(variant["tool"] == tool for variant in variants):
                executable = shutil.which(tool)
                if not executable:
                    raise ValueError(f"{tool} executable not found")
                record["tools"][tool] = _tool(executable)
        if args.baseline_bin_dir:
            record["tools"]["rcp-baseline"] = _tool(args.baseline_bin_dir / "rcp")
            record["tools"]["rcpd-baseline"] = _tool(args.baseline_bin_dir / "rcpd")
        if args.mode == "loopback":
            ssh = shutil.which("ssh")
            if not ssh:
                raise ValueError("ssh executable not found")
            record["tools"]["ssh"] = _tool(ssh, "-V")
        resource_time = None
        if args.local_resources:
            selected_time = args.resource_time or shutil.which("time")
            if not selected_time:
                raise ValueError("GNU time executable not found")
            time_tool = _tool(selected_time)
            record["context"]["local_resources"] = measurements.policy(time_tool)
            resource_time = time_tool["path"]
        if args.build_provenance is not None:
            record["context"]["build_provenance"] = measurements.load_builds(args.build_provenance, record["tools"])
        experiment = measurements.identity(record["context"])
        endpoints = environment(args.source_root, args.destination_root)
        record["context"]["environment"] = endpoints
        _persist(output, record)
        tools = {key: value["path"] for key, value in record["tools"].items()}
        timing_capability = {}
        timing_collection = {}
        for variant in variants:
            if variant["tool"] != "rcp":
                timing_collection[variant["id"]] = "not_applicable"
                continue
            executable = tools["rcp-baseline"] if variant["id"] == "rcp-baseline" else tools["rcp"]
            capable = _supports_timings(executable)
            timing_capability[variant["id"]] = capable
            timing_collection[variant["id"]] = "disabled" if args.no_timings else "coarse" if capable else "unsupported"
        record["context"]["timing_capability"] = timing_capability
        record["context"]["timing_collection"] = timing_collection
        _persist(output, record)
        for case in cases:
            fixture_root = source_scratch / case["id"]
            fixture_root.mkdir()
            leaf_files = "files_per_leaf" in case
            files_per_directory = case["files_per_leaf" if leaf_files else "files_per_directory"]
            filegen_command = [tools["filegen"], str(fixture_root), ",".join(map(str, case["directory_widths"])), str(files_per_directory), str(case["file_size_bytes"])]
            if leaf_files:
                filegen_command.append("--leaf-files")
            filegen_command.append(f"--bufsize={min(case['file_size_bytes'], 1048576)}")
            generation_started = time.monotonic()
            generated = subprocess.run(filegen_command, capture_output=True, text=True, timeout=args.timeout)
            if collect_costs:
                measurements.add_cost(record, "generation", generation_started)
            (output / "logs" / f"{case['id']}.filegen.stdout.log").write_text(generated.stdout)
            (output / "logs" / f"{case['id']}.filegen.stderr.log").write_text(generated.stderr)
            if generated.returncode:
                raise RuntimeError(f"filegen failed for {case['id']} ({generated.returncode})")
            source = fixture_root / "filegen"
            initial_started = time.monotonic()
            source_scan = scan_tree(source)
            if source_scan["counts"] != expected_counts(case):
                raise ValueError(f"filegen count mismatch for {case['id']}: {source_scan['counts']} != {expected_counts(case)}")
            operation = case.get("mode", "fresh")
            source_metadata = operations.metadata_tree(source) if args.cache == "source-verified" or operation != "fresh" or any(operations.summary_supported(variant) for variant in variants) else None
            if collect_costs:
                measurements.add_cost(record, "initial_verification", initial_started)
            stale_names = operations.select_stale(source_scan["entries"]) if operation == "partial" else []
            expected_transfer = operations.transfer_counts(operation, source_scan["counts"])
            case["fixture_digest"] = source_scan["digest"]
            case["realized_counts"] = source_scan["counts"]
            _persist(output, record)
            for iteration in range(1, args.repetitions + 1):
                order = variants[(iteration - 1) % len(variants):] + variants[:(iteration - 1) % len(variants)]
                if args.paired_seed is not None:
                    by_id = {variant["id"]: variant for variant in variants}
                    order = [by_id[key] for key in pairs.order(record["context"]["pairing"], case["id"], iteration)]
                for position, variant in enumerate(order):
                    trial_path = Path(case["id"]) / variant["id"] / str(iteration)
                    destination = destination_scratch / trial_path
                    timing_policy = timing_collection[variant["id"]]
                    child_locale = measurements.child_locale(variant, args.local_resources)
                    stable_summary_locale = child_locale == operations.SUMMARY_LOCALE
                    trial = {"case_id": case["id"], "variant_id": variant["id"], "iteration": iteration,
                             "operation": operation, "expected_transfer": expected_transfer,
                             "child_locale": child_locale,
                             "commands": [], "status": "running", "validation": {"ok": False},
                             "exit_codes": [], "logs": [], "timings": {"status": timing_policy, "reports": []}}
                    if args.paired_seed is not None:
                        trial["pairing"] = pairs.trial_metadata(record["context"]["pairing"], case["id"], iteration, position)
                    if args.local_resources:
                        trial["resources"] = {"status": "unavailable", "metrics": None}
                    record["trials"].append(trial)
                    _persist(output, record)
                    preparation_started = time.monotonic()
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    if operation != "fresh":
                        stale = operations.seed_destination(source, destination, source_scan, source_metadata, stale_names)
                        trial["seed_validation"] = operations.validate_seed(source, destination, source_scan, source_metadata, stale)
                    elif variant["processes"] > 1 or variant["tool"] == "rsync":
                        destination.mkdir()
                    selected_tools = dict(tools)
                    if variant["id"] == "rcp-baseline":
                        selected_tools["rcp"] = tools["rcp-baseline"]
                        if args.mode == "loopback":
                            selected_tools["rcpd"] = tools["rcpd-baseline"]
                    commands = [] if args.rtt_ms is not None else plan_commands(variant, source, destination, selected_tools, args.mode, operation)
                    timing_prefix = output / "timings" / trial_path / "trace"
                    if timing_policy == "coarse":
                        timing_prefix.parent.mkdir(parents=True, exist_ok=False)
                        commands = [[command[0], f"--timings={timing_prefix}", *command[1:]] for command in commands]
                    trial["commands"] = commands
                    if args.rtt_ms is not None:
                        trial["transport_artifacts"] = str(output / "transport" / trial_path)
                    try:
                        proof = _prepare_cache(args.cache, source, args.timeout, source_scan, source_metadata)
                        if args.cache == "source-verified":
                            trial["cache_validation"] = proof
                    except Exception as exc:
                        trial["status"] = "failed"
                        trial["validation"] = {"ok": False, "error": f"cache preparation failed: {exc}"}
                        _persist(output, record)
                        raise
                    if collect_costs:
                        measurements.add_cost(trial, "preparation", preparation_started)
                    _persist(output, record)
                    if args.rtt_ms is not None:
                        identities = {key: record["tools"][key + "-baseline" if variant["id"] == "rcp-baseline" else key]["sha256"] for key in (("rcp", "rcpd") if variant["tool"] == "rcp" else ("rsync",))}
                        outcome = transport.execute_trial(variant, source, destination, selected_tools,
                            output / "logs" / trial_path, Path(trial["transport_artifacts"]), args.timeout,
                            args.rtt_ms, operation, source_scan["counts"], timing_policy, timing_prefix,
                            len(record["trials"]) - 1, expected_pins=identities, ssh_identity=record["tools"]["ssh"])
                        commands = outcome["commands"]
                    else:
                        options = {"resource_time": resource_time} if resource_time is not None else {}
                        if args.command_clocks:
                            options["command_clocks"] = True
                        outcome = execute_commands(commands, output / "logs" / trial_path, args.timeout, stable_summary_locale=stable_summary_locale, **options)
                    verification_started = time.monotonic()
                    trial.update(outcome)
                    timing_error = None
                    if timing_policy == "coarse":
                        expected_roles = ["rcp-master"] * len(commands)
                        if args.mode == "loopback":
                            expected_roles += ["rcpd-source"] * len(commands)
                            expected_roles += ["rcpd-destination"] * len(commands)
                        try:
                            trial["timings"]["reports"] = timings.collect(timing_prefix, expected_roles, successful=outcome["ok"])
                        except timings.CollectionError as error:
                            trial["timings"]["reports"] = error.reports
                            timing_error = str(error)
                    trial["validation"] = validate_tree(source, destination, source_scan) if outcome["ok"] else {"ok": False, "error": (outcome.get("failure") or {}).get("message") or "command failed or timed out"}
                    if outcome["ok"] and trial["validation"]["ok"]:
                        try:
                            if operations.summary_supported(variant):
                                text = "\n".join(Path(path).read_text() for item in outcome["logs"] for path in item.values())
                                trial["copy_summary"] = operations.validate_summary(variant, text, expected_transfer)
                                trial["metadata_validation"] = operations.validate_metadata(destination, source_metadata)
                            if source_metadata is not None:
                                trial["source_validation"] = operations.validate_source(source, source_scan, source_metadata)
                        except (OSError, ValueError) as error:
                            trial["validation"].update(ok=False, error=str(error))
                    if timing_error:
                        trial["validation"]["timing_error"] = timing_error
                        if trial["validation"]["ok"]:
                            trial["validation"].update(ok=False, error=f"timing collection failed: {timing_error}")
                    if collect_costs:
                        measurements.add_cost(trial, "verification", verification_started)
                    trial["status"] = "ok" if outcome["ok"] and trial["validation"]["ok"] else "failed"
                    _persist(output, record)
                    if trial["status"] != "ok":
                        raise RuntimeError(f"trial {trial_path} failed: {trial['validation'].get('error')}; exit_codes={trial['exit_codes']}; timed_out={trial['timed_out']}")
                    cleanup_started = time.monotonic()
                    shutil.rmtree(destination)
                    if collect_costs:
                        measurements.add_cost(trial, "cleanup", cleanup_started)
            source_validation = record["trials"][-1]["source_validation"] if source_metadata is not None else validate_tree(source, source, source_scan)
            if not source_validation["ok"]:
                raise RuntimeError(f"source changed during case {case['id']}: {source_validation['error']}")
            case_summaries = []
            for variant in variants:
                samples = [trial["elapsed_seconds"] for trial in record["trials"] if trial["case_id"] == case["id"] and trial["variant_id"] == variant["id"] and trial["status"] == "ok"]
                median = statistics.median(samples)
                owned_semantics = None
                if args.rtt_ms is not None:
                    accepted = [trial for trial in record["trials"] if trial["case_id"] == case["id"] and trial["variant_id"] == variant["id"] and trial["status"] == "ok"]
                    observations = [transport.series_observations(trial["transport"]) for trial in accepted]
                    if any(observation != observations[0] for observation in observations):
                        raise ValueError("owned role capacity/resource observations changed between repetitions; refusing to pool samples")
                    owned_semantics = {**transport.semantics(args.rtt_ms), "observations": observations[0]}
                case_summaries.append({"series_id": series_id({key: value for key, value in case.items() if key not in ("fixture_digest", "realized_counts")}, variant, args.cache, args.mode, args.runner_label, endpoints, record["tools"], storage_ids, args.ssh_transport_profile, timing_collection[variant["id"]], timing_capability.get(variant["id"]), record["context"]["timing_request"], operation_revision=operations.CONTRACT_REVISION, owned_transport=owned_semantics, experiment=experiment), "case_id": case["id"], "variant_id": variant["id"], "unit": "seconds", "median": median, "minimum": min(samples), "maximum": max(samples), "stdev": statistics.stdev(samples) if len(samples) > 1 else 0.0, "samples": samples, "files_per_second": source_scan["counts"]["files"] / median if median else 0.0})
            record["summaries"].extend(case_summaries)
            _persist(output, record)
            shutil.rmtree(fixture_root)
        record["status"] = "complete"
        shutil.rmtree(source_scratch)
        shutil.rmtree(destination_scratch)
    except BaseException as exc:
        record["status"] = "failed"
        if collect_costs:
            measurements.add_cost(record, "total", run_started)
        record["error"] = str(exc) or type(exc).__name__
        for trial in record["trials"]:
            if trial["status"] == "running":
                trial["status"] = "failed"
                trial["validation"] = {"ok": False, "error": record["error"]}
        if source_scratch or destination_scratch:
            record["context"]["failure_artifacts"] = {"source_scratch": str(source_scratch) if source_scratch else "", "destination_scratch": str(destination_scratch) if destination_scratch else ""}
        _persist(output, record)
        raise
    if collect_costs:
        measurements.add_cost(record, "total", run_started)
    _persist(output, record)
    return record


if __name__ == "__main__":
    def _sigterm(_signum, _frame):
        raise InterruptedError("SIGTERM")
    signal.signal(signal.SIGTERM, _sigterm)
    try:
        main()
    except Exception as exc:
        print(f"benchmark failed: {exc}", file=sys.stderr)
        sys.exit(1)
