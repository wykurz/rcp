"""Run repeatable copy workloads and retain validated measurements."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
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


TIMING_POLICY = "monotonic launch-to-last-child-exit; excludes verification and cache preparation"
VERIFICATION_POLICY = "exact relative directory and regular-file paths, sizes, and SHA256 contents"
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
    data = json.loads(Path(path).read_text())
    _fields(data, {"schema_version", "cases", "variants"}, set(), "manifest")
    if type(data["schema_version"]) is not int or data["schema_version"] != 1:
        raise ValueError("unsupported manifest schema_version")
    if not isinstance(data["cases"], list) or not data["cases"] or not isinstance(data["variants"], list) or not data["variants"]:
        raise ValueError("manifest requires nonempty cases and variants")
    for kind in ("cases", "variants"):
        seen = set()
        for entry in data[kind]:
            required = {"id", "directory_widths", "files_per_leaf", "file_size_bytes"} if kind == "cases" else {"id", "tool", "args", "processes"}
            _fields(entry, required, {"description"}, kind[:-1])
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
                widths = entry["directory_widths"]
                if not isinstance(widths, list) or not widths:
                    raise ValueError("directory_widths must be a nonempty list")
                for width in widths:
                    _positive(width, "directory width")
                _positive(entry["files_per_leaf"], "files_per_leaf")
                _positive(entry["file_size_bytes"], "file_size_bytes")
            else:
                if entry["tool"] not in ("rcp", "rsync", "cp"):
                    raise ValueError("variant tool must be rcp, rsync, or cp")
                if not isinstance(entry["args"], list) or not all(isinstance(arg, str) and arg for arg in entry["args"]):
                    raise ValueError("variant args must be nonempty strings")
                _positive(entry["processes"], "processes")
    return data


def expected_counts(case):
    directories = 0
    breadth = 1
    for width in case["directory_widths"]:
        breadth *= width
        directories += breadth
    files = breadth * case["files_per_leaf"]
    return {"directories": directories, "files": files, "bytes": files * case["file_size_bytes"]}


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


def plan_commands(variant, source, destination, tools, mode):
    source = Path(source)
    destination = Path(destination)
    tool = variant["tool"]
    if tool == "cp" and mode != "local":
        raise ValueError("cp variants require local mode")
    executable = str(tools[tool])
    args = list(variant["args"])
    if tool == "rcp" and mode == "loopback":
        args += ["--force-remote", f"--rcpd-path={tools['rcpd']}"]
    processes = variant["processes"]
    if processes == 1:
        operand = f"localhost:{source}" if tool == "rcp" and mode == "loopback" else str(source)
        if tool == "rsync":
            operand = f"localhost:{source}" if mode == "loopback" else str(source)
            return [[executable, *args, operand + "/", str(destination) + "/"]]
        return [[executable, *args, operand, str(destination)]]
    children = sorted(source.iterdir())
    if not children or any(not child.is_dir() for child in children):
        raise ValueError("parallel variant needs top-level directories only")
    if len(children) > processes:
        raise ValueError(f"top-level directories ({len(children)}) exceed configured processes ({processes})")
    commands = []
    for child in children:
        operand = f"localhost:{child}" if mode == "loopback" else str(child)
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


def execute_commands(commands, log_dir, timeout):
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=False)
    children = []
    files = []
    waiters = []
    finished = {}
    condition = threading.Condition()
    started = time.monotonic()
    launch_error = None
    timed_out = False
    cleanup_groups = False
    def terminate_groups():
        for signal_to_send in (signal.SIGTERM, signal.SIGKILL):
            for process in children:
                try:
                    os.killpg(process.pid, signal_to_send)
                except ProcessLookupError:
                    pass
            if signal_to_send == signal.SIGTERM:
                grace = time.monotonic() + 1
                with condition:
                    while len(finished) < len(children) and time.monotonic() < grace:
                        condition.wait(grace - time.monotonic())
    with _SignalCancellation(condition) as cancellation:
        try:
            for index, command in enumerate(commands):
                cancellation.checkpoint()
                stdout_path = log_dir / f"{index}.stdout.log"
                stderr_path = log_dir / f"{index}.stderr.log"
                stdout = stdout_path.open("wb")
                files.append(stdout)
                stderr = stderr_path.open("wb")
                files.append(stderr)
                try:
                    process = subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True)
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
    return {"ok": not (launch_error or timed_out) and len(children) == len(commands) and all(code == 0 for code in codes), "timed_out": timed_out, "launch_error": launch_error, "elapsed_seconds": max(0, completion - started), "exit_codes": codes, "logs": [{"stdout": str(log_dir / f"{index}.stdout.log"), "stderr": str(log_dir / f"{index}.stderr.log")} for index in range(len(children))]}


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


def series_id(case, variant, cache_policy, topology, runner_label, endpoint_environment, tools, storage_ids=None):
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
    for tool in ("rsync", "cp"):
        if tool in tools:
            stable_references[tool] = {"version": tools[tool]["version"], "sha256": tools[tool]["sha256"]}
    value = {"case": {key: item for key, item in case.items() if key != "description"}, "variant": {key: item for key, item in variant.items() if key != "description"}, "cache_policy": cache_policy, "topology": topology, "runner_label": runner_label, "environment": comparable_environment, "reference_versions": stable_references, "fixture_contract_revision": FIXTURE_CONTRACT_REVISION, "timing_policy": TIMING_POLICY, "verification_policy": VERIFICATION_POLICY}
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _tool(path):
    # preserve the invoked basename for multicall binaries such as Nix coreutils
    path = Path(path).absolute()
    if not os.access(path, os.X_OK):
        raise ValueError(f"not executable: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    result = subprocess.run([str(path), "--version"], capture_output=True, text=True, timeout=10)
    if result.returncode:
        raise ValueError(f"version command failed: {path}")
    version_output = result.stdout.strip() or result.stderr.strip()
    if not version_output:
        raise ValueError(f"missing version output: {path}")
    return {"path": str(path), "version": version_output.splitlines()[0], "sha256": digest.hexdigest()}


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
    if record.get("context", {}).get("purpose") != "smoke":
        short = [f"{item['case_id']}/{item['variant_id']}" for item in record["summaries"] if item["minimum"] < 10]
        if short:
            lines += ["", "Short samples (<10 s): " + ", ".join(short) + ". Treat small timing changes cautiously."]
    (output / "summary.md").write_text("\n".join(lines) + "\n")


def _prepare_cache(policy, source, timeout):
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
    parser.add_argument("--purpose", choices=("smoke", "performance"), default="performance")
    parser.add_argument("--source-root", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--destination-root", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--source-storage-id", type=_nonblank)
    parser.add_argument("--destination-storage-id", type=_nonblank)
    parser.add_argument("--cache", choices=("source-warm", "linux-drop-caches", "uncontrolled"), default="source-warm")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runner-label", type=_nonblank, default="local")
    parser.add_argument("--files-in-flight")
    return parser.parse_args(argv)


def _default_bin_dir():
    repository = Path(__file__).resolve().parent.parent
    target_root = Path(os.environ.get("CARGO_TARGET_DIR", repository / "target"))
    target = subprocess.run([str(repository / "scripts" / "cargo-host.sh"), "--print-target"], capture_output=True, text=True, check=True).stdout.strip()
    return target_root / target / "release"


def main(argv=None):
    args = _arguments(argv)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / "logs").mkdir()
    record = {"schema_version": 1, "run_id": uuid.uuid4().hex, "timestamp": datetime.now(timezone.utc).isoformat(), "status": "running", "revision": _revision(), "context": {"runner_label": args.runner_label, "topology": args.mode, "purpose": args.purpose, "cache_policy": args.cache, "timing_policy": TIMING_POLICY, "verification_policy": VERIFICATION_POLICY, "source": str(args.source_root.resolve()), "destination": str(args.destination_root.resolve()), "repository": os.environ.get("GITHUB_REPOSITORY", ""), "run_url": os.environ.get("BENCHMARK_RUN_URL", ""), "fixture_policy": "filegen --leaf-files --bufsize=min(file_size_bytes,1048576); random bytes without fixed seed; verified counts and digest", "fixture_contract_revision": FIXTURE_CONTRACT_REVISION, "directory_count_policy": "directories below fixture root; excludes fixture root"}, "tools": {}, "cases": [], "variants": [], "trials": [], "summaries": []}
    if args.baseline_bin_dir:
        record["context"]["baseline_commit"] = os.environ.get("RCP_BENCH_BASELINE_COMMIT", "")
    storage_ids = {"source": args.source_storage_id, "destination": args.destination_storage_id}
    record["context"]["storage_ids"] = storage_ids
    _persist(output, record)
    source_scratch = None
    destination_scratch = None
    try:
        if args.repetitions <= 0 or args.timeout <= 0:
            raise ValueError("repetitions and timeout must be positive")
        manifest = load_manifest(args.manifest)
        cases = _select(manifest["cases"], args.cases or ["tiny-10k"], "case")
        default_variants = ["rcp-default", "rsync-a", "rsync-a-10"] + (["cp-a"] if args.mode == "local" else [])
        variants = _select(manifest["variants"], args.variants or default_variants, "variant")
        if args.mode != "local" and any(variant["tool"] == "cp" for variant in variants):
            raise ValueError("cp variants require local mode")
        if args.files_in_flight:
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
        record["cases"] = cases
        record["variants"] = variants
        record["context"]["metadata_policies"] = {variant["id"]: ("rcp preserve-settings=all includes atime" if variant["tool"] == "rcp" and "--preserve-settings=all" in variant["args"] else "rcp defaults" if variant["tool"] == "rcp" else "cp -a archive; preserves all supported attributes" if variant["tool"] == "cp" else "rsync -a archive; differs from rcp preserve-all for atime") for variant in variants}
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
        endpoints = environment(args.source_root, args.destination_root)
        record["context"]["environment"] = endpoints
        _persist(output, record)
        tools = {key: value["path"] for key, value in record["tools"].items()}
        for case in cases:
            fixture_root = source_scratch / case["id"]
            fixture_root.mkdir()
            filegen_command = [tools["filegen"], str(fixture_root), ",".join(map(str, case["directory_widths"])), str(case["files_per_leaf"]), str(case["file_size_bytes"]), "--leaf-files", f"--bufsize={min(case['file_size_bytes'], 1048576)}"]
            generated = subprocess.run(filegen_command, capture_output=True, text=True, timeout=args.timeout)
            (output / "logs" / f"{case['id']}.filegen.stdout.log").write_text(generated.stdout)
            (output / "logs" / f"{case['id']}.filegen.stderr.log").write_text(generated.stderr)
            if generated.returncode:
                raise RuntimeError(f"filegen failed for {case['id']} ({generated.returncode})")
            source = fixture_root / "filegen"
            source_scan = scan_tree(source)
            if source_scan["counts"] != expected_counts(case):
                raise ValueError(f"filegen count mismatch for {case['id']}: {source_scan['counts']} != {expected_counts(case)}")
            case["fixture_digest"] = source_scan["digest"]
            case["realized_counts"] = source_scan["counts"]
            _persist(output, record)
            for iteration in range(1, args.repetitions + 1):
                order = variants[(iteration - 1) % len(variants):] + variants[:(iteration - 1) % len(variants)]
                for variant in order:
                    trial_path = Path(case["id"]) / variant["id"] / str(iteration)
                    destination = destination_scratch / trial_path
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    if variant["processes"] > 1 or variant["tool"] == "rsync":
                        destination.mkdir()
                    selected_tools = dict(tools)
                    if variant["id"] == "rcp-baseline":
                        selected_tools["rcp"] = tools["rcp-baseline"]
                        if args.mode == "loopback":
                            selected_tools["rcpd"] = tools["rcpd-baseline"]
                    commands = plan_commands(variant, source, destination, selected_tools, args.mode)
                    trial = {"case_id": case["id"], "variant_id": variant["id"], "iteration": iteration, "commands": commands, "status": "running", "validation": {"ok": False}, "exit_codes": [], "logs": []}
                    record["trials"].append(trial)
                    _persist(output, record)
                    try:
                        _prepare_cache(args.cache, source, args.timeout)
                    except Exception as exc:
                        trial["status"] = "failed"
                        trial["validation"] = {"ok": False, "error": f"cache preparation failed: {exc}"}
                        _persist(output, record)
                        raise
                    outcome = execute_commands(commands, output / "logs" / trial_path, args.timeout)
                    trial.update(outcome)
                    trial["validation"] = validate_tree(source, destination, source_scan) if outcome["ok"] else {"ok": False, "error": "command failed or timed out"}
                    trial["status"] = "ok" if outcome["ok"] and trial["validation"]["ok"] else "failed"
                    _persist(output, record)
                    if trial["status"] != "ok":
                        raise RuntimeError(f"trial {trial_path} failed: {trial['validation'].get('error')}; exit_codes={trial['exit_codes']}; timed_out={trial['timed_out']}")
                    shutil.rmtree(destination)
            case_summaries = []
            for variant in variants:
                samples = [trial["elapsed_seconds"] for trial in record["trials"] if trial["case_id"] == case["id"] and trial["variant_id"] == variant["id"] and trial["status"] == "ok"]
                median = statistics.median(samples)
                case_summaries.append({"series_id": series_id({key: value for key, value in case.items() if key not in ("fixture_digest", "realized_counts")}, variant, args.cache, args.mode, args.runner_label, endpoints, record["tools"], storage_ids), "case_id": case["id"], "variant_id": variant["id"], "unit": "seconds", "median": median, "minimum": min(samples), "maximum": max(samples), "stdev": statistics.stdev(samples) if len(samples) > 1 else 0.0, "samples": samples, "files_per_second": source_scan["counts"]["files"] / median if median else 0.0})
            record["summaries"].extend(case_summaries)
            _persist(output, record)
            shutil.rmtree(fixture_root)
        record["status"] = "complete"
        shutil.rmtree(source_scratch)
        shutil.rmtree(destination_scratch)
    except BaseException as exc:
        record["status"] = "failed"
        record["error"] = str(exc) or type(exc).__name__
        for trial in record["trials"]:
            if trial["status"] == "running":
                trial["status"] = "failed"
                trial["validation"] = {"ok": False, "error": record["error"]}
        if source_scratch or destination_scratch:
            record["context"]["failure_artifacts"] = {"source_scratch": str(source_scratch) if source_scratch else "", "destination_scratch": str(destination_scratch) if destination_scratch else ""}
        _persist(output, record)
        raise
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
