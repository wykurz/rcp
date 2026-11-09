"""Bounded Linux host/cgroup diagnostics; never process inventories or payload evidence."""

import copy
import json
import math
import os
from pathlib import Path
import platform
import re
import threading
import time

PERIOD_SECONDS = 10
MAX_SAMPLES = 4096
MAX_BYTES = 8 * 1024 * 1024
MAX_SECONDS = 8 * 3600
MAX_PHASES = 192
READ_BYTES = 64 * 1024
MAX_NUMBER = (1 << 63) - 1
ARTIFACT = "system-telemetry.jsonl"
PHASES = frozenset({"setup", "generation", "initial_verification", "capacity", "preparation", "cache",
    "command", "timings", "destination", "summary", "metadata", "source", "cleanup", "unattributed"})
COUNTERS = {"cpu": ("usage_usec", "user_usec", "system_usec", "nr_periods", "nr_throttled", "throttled_usec"),
    "events": ("low", "high", "max", "oom", "oom_kill", "oom_group_kill")}
MEMORY = ("MemTotal", "MemAvailable", "MemFree", "Buffers", "Cached", "SwapTotal", "SwapFree", "Dirty", "Writeback")
CPU = ("user_ticks", "nice_ticks", "system_ticks", "idle_ticks", "iowait_ticks", "irq_ticks", "softirq_ticks", "steal_ticks")
IO = ("rbytes", "wbytes", "rios", "wios")
SUMMARY_METRICS = (
    ("cgroup_cpu", "usage_usec", "cpu_usage_usec", True),
    ("cgroup_io", "rbytes", "read_bytes", True), ("cgroup_io", "wbytes", "write_bytes", True),
    ("cgroup_memory", "current_bytes", "memory_current_bytes", False),
    ("cgroup_memory", "peak_bytes", "cgroup_run_memory_peak_bytes", False),
    ("host_cpu", "total_ticks", "host_cpu_total_ticks", True),
    ("host_cpu", "idle_ticks", "host_cpu_idle_ticks", True),
    ("host_cpu", "iowait_ticks", "host_cpu_iowait_ticks", True),
    ("host_memory", "MemAvailable_bytes", "host_memory_available_bytes", False),
    *((side + "_io", field, side + "_" + field, True) for side in ("source", "destination")
        for field in ("read_sectors", "write_sectors", "io_milliseconds")),
    *((scope + "_" + kind + "_pressure", category + "_total_usec",
        scope + "_" + kind + "_pressure_" + category + "_usec", True)
        for scope in ("host", "cgroup") for kind in ("cpu", "memory", "io") for category in ("some", "full")))
# diskstats sectors are 512-byte units; PSI totals are cumulative stall microseconds
# cgroup_run_memory_peak_bytes remains the cgroup's run-level high water, never a phase-local peak
GROUPS = ("host_cpu", "host_memory", "host_cpu_pressure", "host_memory_pressure", "host_io_pressure",
    "cgroup_cpu", "cgroup_memory", "cgroup_events", "cgroup_io", "cgroup_cpu_pressure",
    "cgroup_memory_pressure", "cgroup_io_pressure", "source_io", "destination_io")


def descriptor():
    return dict(policy="linux-bounded-system-sampling-v1", period_seconds=PERIOD_SECONDS,
        max_samples=MAX_SAMPLES, max_bytes=MAX_BYTES, max_seconds=MAX_SECONDS, max_phases=MAX_PHASES,
        scope="host aggregate and current harness cgroup with descendants; logical mount-device IO; not per-process attribution")


def integer(text):
    if not re.fullmatch(r"[0-9]{1,19}", text) or int(text) > MAX_NUMBER:
        raise ValueError("invalid counter")
    return int(text)


def available(values):
    if not values:
        raise ValueError("empty counters")
    return dict(status="available", values=values)


def key_values(text, allowed):
    result = {}
    for line in text.splitlines():
        fields = line.split()
        if fields and fields[0] in allowed:
            if len(fields) != 2 or fields[0] in result:
                raise ValueError("invalid counters")
            result[fields[0]] = integer(fields[1])
    return result


def pressure(text):
    result = {}
    for line in text.splitlines():
        fields = line.split()
        if not fields or fields[0] not in ("some", "full"):
            continue
        category = fields[0]
        if category + "_total_usec" in result or len(fields) != 5:
            raise ValueError("invalid pressure")
        values = dict(field.split("=", 1) for field in fields[1:])
        if set(values) != {"avg10", "avg60", "avg300", "total"}:
            raise ValueError("invalid pressure")
        for key in ("avg10", "avg60", "avg300"):
            if not re.fullmatch(r"[0-9]{1,3}(?:\.[0-9]{1,6})?", values[key]):
                raise ValueError("invalid pressure")
            value = float(values[key])
            if not math.isfinite(value) or not 0 <= value <= 100:
                raise ValueError("invalid pressure")
            result[category + "_" + key] = value
        result[category + "_total_usec"] = integer(values["total"])
    return result


class Reader:
    """Read only bounded kernel files; raw names and errors never enter diagnostics."""
    def __init__(self, source, destination, *, root=Path("/")):
        self.root = Path(root)
        self.devices = {}
        for side, path in (("source", source), ("destination", destination)):
            try:
                info = os.stat(path)
                self.devices[side] = (os.major(info.st_dev), os.minor(info.st_dev))
            except (OSError, ValueError):
                pass
        self.cgroup = self._cgroup()

    def read(self, path, *, prefix=False):
        path = self.root / str(path).lstrip("/")
        with path.open("rb") as source:
            data = source.read(READ_BYTES + 1)
        if len(data) > READ_BYTES:
            if not prefix:
                raise ValueError("kernel input exceeds bound")
            data = data[:READ_BYTES].rsplit(b"\n", 1)[0]
        return data.decode("ascii")

    def _cgroup(self):
        try:
            memberships = [line[3:] for line in self.read("/proc/self/cgroup").splitlines() if line.startswith("0::")]
            if len(memberships) != 1:
                return None
            member = Path(memberships[0])
            if not member.is_absolute() or ".." in member.parts:
                return None
            matches = []
            for line in self.read("/proc/self/mountinfo").splitlines():
                fields = line.split()
                separator = fields.index("-")
                if fields[separator + 1] != "cgroup2":
                    continue
                unescape = lambda value: re.sub(r"\\(040|011|012|134)", lambda item: chr(int(item[1], 8)), value)
                base, mount = Path(unescape(fields[3])), Path(unescape(fields[4]))
                if not base.is_absolute() or not mount.is_absolute() or ".." in (*base.parts, *mount.parts):
                    continue
                if member.is_relative_to(base):
                    matches.append((len(base.parts), mount / member.relative_to(base)))
            return max(matches, key=lambda item: item[0])[1] if matches else None
        except (OSError, UnicodeError, ValueError, IndexError):
            return None

    def _group(self, path, parser):
        if path is None:
            return dict(status="unavailable", reason="unsupported")
        try:
            return available(parser(self.read(path)))
        except OSError:
            return dict(status="unavailable", reason="unreadable")
        except (UnicodeError, ValueError, IndexError, OverflowError):
            return dict(status="unavailable", reason="invalid")

    @staticmethod
    def _memory(text):
        result = {}
        for line in text.splitlines():
            fields = line.split()
            if fields and fields[0].rstrip(":") in MEMORY:
                key = fields[0].rstrip(":")
                if len(fields) != 3 or fields[2] != "kB" or key + "_bytes" in result:
                    raise ValueError("invalid memory")
                value = integer(fields[1]) * 1024
                if value > MAX_NUMBER:
                    raise ValueError("invalid memory")
                result[key + "_bytes"] = value
        return result

    @staticmethod
    def _cpu(text):
        fields = text.splitlines()[0].split()
        if fields[0] != "cpu" or len(fields) < 9:
            raise ValueError("invalid cpu")
        result = dict(zip(CPU, (integer(value) for value in fields[1:9])))
        result["total_ticks"] = integer(str(sum(result.values())))
        return result

    @staticmethod
    def _io(text):
        result = dict.fromkeys(IO, 0)
        seen = set()
        for line in text.splitlines():
            fields = line.split()
            if not fields or not re.fullmatch(r"[0-9]+:[0-9]+", fields[0]) or fields[0] in seen:
                raise ValueError("invalid io")
            seen.add(fields[0])
            values = dict(field.split("=", 1) for field in fields[1:])
            if len(values) != len(fields) - 1 or not set(IO) <= set(values):
                raise ValueError("invalid io")
            for key in IO:
                result[key] += integer(values[key])
                if result[key] > MAX_NUMBER:
                    raise ValueError("invalid io")
        return result

    @staticmethod
    def _disk(text, device):
        matches = []
        for line in text.splitlines():
            fields = line.split()
            if len(fields) >= 14 and (integer(fields[0]), integer(fields[1])) == device:
                matches.append(dict(read_ios=integer(fields[3]), read_sectors=integer(fields[5]),
                    read_milliseconds=integer(fields[6]), write_ios=integer(fields[7]),
                    write_sectors=integer(fields[9]), write_milliseconds=integer(fields[10]),
                    inflight_ios=integer(fields[11]), io_milliseconds=integer(fields[12])))
        if len(matches) != 1:
            raise ValueError("missing logical device")
        return matches[0]

    def snapshot(self):
        groups = {"host_cpu": self._group("/proc/stat", self._cpu),
            "host_memory": self._group("/proc/meminfo", self._memory)}
        for kind in ("cpu", "memory", "io"):
            groups["host_" + kind + "_pressure"] = self._group("/proc/pressure/" + kind, pressure)
            path = self.cgroup / (kind + ".pressure") if self.cgroup is not None else None
            groups["cgroup_" + kind + "_pressure"] = self._group(path, pressure)
        for name, file, parser in (("cpu", "cpu.stat", lambda text: key_values(text, COUNTERS["cpu"])),
                ("events", "memory.events", lambda text: key_values(text, COUNTERS["events"])),
                ("io", "io.stat", self._io)):
            groups["cgroup_" + name] = self._group(self.cgroup / file if self.cgroup is not None else None, parser)
        memory = {}
        for file in ("current", "peak"):
            memory[file + "_bytes"] = self._group(self.cgroup / ("memory." + file) if self.cgroup is not None else None,
                lambda text: {"bytes": integer(text.strip())})
        values = {key: item["values"]["bytes"] for key, item in memory.items() if item["status"] == "available"}
        groups["cgroup_memory"] = dict(status="available" if len(values) == 2 else "partial" if values else "unavailable", values=values)
        for side in ("source", "destination"):
            groups[side + "_io"] = self._group("/proc/diskstats", lambda text: self._disk(text, self.devices[side])) \
                if side in self.devices else dict(status="unavailable", reason="unsupported")
        return groups

    def setup(self):
        def text_value(value):
            return value if isinstance(value, str) and 0 < len(value) <= 160 and re.fullmatch(r"[A-Za-z0-9 ._()+/@:-]+", value) else None
        cpu = None
        try:
            models = {line.split(":", 1)[1].strip() for line in self.read("/proc/cpuinfo", prefix=True).splitlines()
                if line.split(":", 1)[0].strip() in ("model name", "Hardware")}
            if len(models) == 1:
                cpu = text_value(models.pop())
        except (OSError, UnicodeError, ValueError):
            pass
        memory = self._group("/proc/meminfo", self._memory)
        values = dict(cpu_model=cpu, logical_cpu_count=os.cpu_count(),
            host_memory_total_bytes=memory.get("values", {}).get("MemTotal_bytes"),
            os=text_value(platform.system()), kernel=text_value(platform.release()), architecture=text_value(platform.machine()))
        return {key: dict(status="available", value=value) if value is not None else dict(status="unavailable")
            for key, value in values.items()}


class Sampler:
    """One daemon owns diagnostics IO; the benchmark thread only queues/copies bounded state."""
    def __init__(self, output, source, destination, *, reader=None, clock=time.monotonic):
        self.reader = reader
        self.output, self.source, self.destination = Path(output), source, destination
        self.clock, self.started = clock, clock()
        self.samples = self.bytes = 0
        self.phases, self.current = [], None
        self.events = []
        self.reason, self.closed, self.partial = None, False, False
        self.lock, self.wake, self.stop_event = threading.Lock(), threading.Event(), threading.Event()
        self.thread = None
        self.system_setup = {key: dict(status="unavailable") for key in
            ("cpu_model", "logical_cpu_count", "host_memory_total_bytes", "os", "kernel", "architecture")}
        self.mark("setup")

    def _limit(self):
        elapsed = max(0, self.clock() - self.started)
        if elapsed > MAX_SECONDS or self.samples >= MAX_SAMPLES:
            self.reason = self.reason or ("time_limit" if elapsed > MAX_SECONDS else "sample_limit")
        return elapsed

    def _event(self, phase):
        self.events.append(dict(kind="phase", phase=phase["phase"], case_index=phase["case_index"],
            trial_index=phase["trial_index"], elapsed_seconds=phase["started"]))

    def _write(self, file, value):
        encoded = (json.dumps(value, allow_nan=False, separators=(",", ":")) + "\n").encode("ascii")
        with self.lock:
            if self.closed:
                return False
            if self.bytes + len(encoded) > MAX_BYTES:
                self.reason = self.reason or "byte_limit"
                return False
        try:
            file.write(encoded)
            file.flush()
        except OSError:
            with self.lock:
                self.reason = self.reason or "artifact_unavailable"
            return False
        with self.lock:
            if self.closed:
                return False
            self.bytes += len(encoded)
        return True

    def _record(self, groups, elapsed, finished, current):
        with self.lock:
            if self.closed or self.reason is not None:
                return None
            if current is not self.current:
                # a kernel read crossed a phase boundary: no fresh per-phase attribution exists
                self.partial = True
                return None
            sample = dict(kind="sample", elapsed_seconds=round(elapsed, 6),
                read_finished_seconds=round(finished, 6), phase=current["phase"],
                case_index=current["case_index"], trial_index=current["trial_index"], metrics=groups)
            self.samples += 1
            current["samples"] += 1
            self.partial |= any(value["status"] != "available" for value in groups.values())
            for group, field, label, counter in SUMMARY_METRICS:
                value = groups.get(group, {}).get("values", {}).get(field)
                if type(value) is not int:
                    continue
                item = current["metrics"].get(label)
                if item is None:
                    item = current["metrics"][label] = dict(first=value, last=value, minimum=value,
                        maximum=value, counter=counter, reset=False)
                item["reset"] |= counter and value < item["last"]
                item.update(last=value, minimum=min(item["minimum"], value), maximum=max(item["maximum"], value))
            return sample

    def _collect(self):
        with self.lock:
            elapsed = self._limit()
            if self.closed or self.reason is not None or self.current is None or self.stop_event.is_set():
                return None
            current = self.current
        try:
            groups = self.reader.snapshot()
        except Exception:
            groups = {key: dict(status="unavailable", reason="read_failed") for key in GROUPS}
        finished = max(elapsed, self.clock() - self.started)
        with self.lock:
            self._limit()
        return self._record(groups, elapsed, finished, current)

    def sample(self):
        """Request a sample without waiting for a reader or accessing kernel files."""
        self.wake.set()

    def _run(self):
        try:
            with (self.output / ARTIFACT).open("xb") as file:
                if not self._write(file, dict(kind="policy", policy=descriptor())):
                    return
                # reader construction includes mount/cgroup discovery and belongs only to this daemon
                self.reader = self.reader or Reader(self.source, self.destination)
                setup = self.reader.setup()
                with self.lock:
                    if not self.closed:
                        self.system_setup = setup
                while True:
                    self.wake.clear()
                    with self.lock:
                        if self.closed:
                            return
                        events, self.events = self.events, []
                    for event in events:
                        if not self._write(file, event):
                            return
                    if self.stop_event.is_set():
                        return
                    sample = self._collect()
                    if sample is not None and not self._write(file, sample):
                        return
                    with self.lock:
                        if self.reason is not None:
                            return
                    self.wake.wait(PERIOD_SECONDS)
        except Exception:
            with self.lock:
                if not self.closed:
                    self.reason = self.reason or "artifact_unavailable"

    def start(self):
        with self.lock:
            if self.closed or self.thread is not None:
                return self
            thread = threading.Thread(target=self._run, name="benchmark-telemetry", daemon=True)
            self.thread = thread
        try:
            thread.start()
        except (OSError, RuntimeError):
            with self.lock:
                self.thread = None
                self.reason = self.reason or "artifact_unavailable"
        return self

    def mark(self, phase, case_index=None, trial_index=None):
        if phase not in PHASES or any(value is not None and (type(value) is not int or not 0 <= value < 1000000)
                for value in (case_index, trial_index)):
            raise ValueError("invalid telemetry phase")
        with self.lock:
            if self.closed:
                return
            now = self._limit()
            if self.current is not None:
                self.current.update(elapsed_seconds=max(0, now - self.current["started"]), status="observed")
            if len(self.phases) >= MAX_PHASES:
                self.reason = self.reason or "phase_limit"
                self.current = None
                return
            self.current = dict(phase=phase, case_index=case_index, trial_index=trial_index,
                started=now, elapsed_seconds=0, status="running", samples=0, metrics={})
            self.phases.append(self.current)
            self._event(self.current)
        self.wake.set()

    def finish(self, successful):
        self.stop_event.set()
        self.wake.set()
        if self.thread is not None:
            self.thread.join(timeout=1)
        with self.lock:
            if self.closed:
                return
            if self.thread is not None and self.thread.is_alive():
                self.reason = self.reason or "reader_stalled"
            if self.current is not None:
                self.current.update(elapsed_seconds=max(0, self.clock() - self.started - self.current["started"]),
                    status="observed" if successful else "interrupted")
            self.closed = True

    def snapshot(self, trial_index=None, *, non_trial_only=False):
        """Count selected phases; artifact bytes/samples always describe the shared journal."""
        if type(non_trial_only) is not bool or non_trial_only and trial_index is not None:
            raise ValueError("invalid telemetry summary selection")
        with self.lock:
            phases = copy.deepcopy([item for item in self.phases if
                (item["trial_index"] is None if non_trial_only else
                 trial_index is None or item["trial_index"] == trial_index)])
            for item in phases:
                item.pop("started")
                for metric in item["metrics"].values():
                    counter, reset = metric.pop("counter"), metric.pop("reset")
                    metric["status"] = "counter_reset" if reset else "available"
                    if counter:
                        metric["delta"] = None if reset else metric["last"] - metric["first"]
                for _, _, label, _ in SUMMARY_METRICS:
                    item["metrics"].setdefault(label, dict(status="unavailable"))
            count = self.samples if trial_index is None and not non_trial_only else sum(item["samples"] for item in phases)
            status = "unavailable" if count == 0 else "truncated" if self.reason else \
                "partial" if self.partial or not self.closed or any(item["status"] == "interrupted" for item in phases) else "complete"
            return dict(status=status, artifact=ARTIFACT, samples=count, artifact_samples=self.samples,
                bytes=self.bytes, reason=self.reason, phases=phases)

    def attach(self, record):
        with self.lock:
            record["system_setup"] = copy.deepcopy(self.system_setup)
        # serialize each phase once: fixture/setup phases here, copy phases on their owning trial
        record["system_telemetry"] = self.snapshot(non_trial_only=True)
        for index, trial in enumerate(record["trials"]):
            trial["system_telemetry"] = self.snapshot(index)
