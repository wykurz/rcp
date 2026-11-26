"""Bounded work counters reported by a command on the Linux monotonic clock."""

import os
import stat

from benchmarks import strict_json


FORMAT = "--progress-type=json-lines"
OPTIONS = ("--progress", FORMAT, "--progress-delay=10s")
SCHEMA = "rcp-progress-v1"
METRICS = {"rcp": "files_copied", "rlink": "hard_links_created"}
MAX_BYTES = 1024 * 1024
MAX_SAMPLES = 12 * 3600 // 10 + 2
PUBLIC_SAMPLES = 16
MAX_RUN_MS = 12 * 3600 * 1000
STATUSES = ("unsupported", "missing", "unavailable", "invalid", "limit")


def _count(value):
    return type(value) is int and 0 <= value < 2**63


def read(path, metric, started, elapsed, origin, files):
    """Relate each complete observation to the command and the system sampler's start."""
    try:
        with open(path, "rb", opener=lambda name, flags: os.open(name, flags | os.O_NONBLOCK | os.O_NOFOLLOW)) as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode):
                return {"status": "unavailable"}
            if info.st_size > MAX_BYTES:
                return {"status": "limit"}
            data = source.read(MAX_BYTES + 1)
    except InterruptedError:
        raise
    except OSError:
        return {"status": "unavailable"}
    if len(data) > MAX_BYTES:
        return {"status": "limit"}
    start_ns, end_ns = round(started * 1e9), round((started + elapsed) * 1e9)
    samples = []
    previous = None
    for line in data.splitlines(keepends=True):
        if not line.startswith(b"{") or b'"rcp-progress-v1"' not in line:
            continue
        if len(line) > 1024 or len(samples) >= min(MAX_SAMPLES, 2 + int(elapsed // 5)):
            return {"status": "limit"}
        if not line.endswith(b"\n"):
            return {"status": "invalid"}
        try:
            point = strict_json.parse_json(line)
        except (ValueError, RecursionError):
            return {"status": "invalid"}
        if (not isinstance(point, dict) or set(point) != {"schema", "monotonic_ns", *METRICS.values()}
                or point["schema"] != SCHEMA or not all(_count(point[k]) for k in ("monotonic_ns", *METRICS.values()))):
            return {"status": "invalid"}
        stamp, value = point["monotonic_ns"], point[metric]
        if (not start_ns <= stamp <= end_ns or value > files or
                (previous is not None and (stamp <= previous or value < samples[-1][1]))):
            return {"status": "invalid"}
        samples.append([(stamp - start_ns) // 1_000_000, value])
        previous = stamp
    if not samples:
        return {"status": "missing"}
    return dict(status="observed", metric=metric, command_start_ms=round((started - origin) * 1000), samples=samples)


def _valid_samples(value, metric, files, duration_ms, limit):
    samples = value.get("samples")
    start = value.get("command_start_ms")
    if (value.get("metric") != metric or not _count(start) or start > MAX_RUN_MS
            or not isinstance(samples, list) or not 1 <= len(samples) <= limit):
        return False
    previous_time, previous_value = -1, 0
    for sample in samples:
        if not isinstance(sample, list) or len(sample) != 2 or not all(_count(n) for n in sample):
            return False
        stamp, count = sample
        if not (previous_time <= stamp <= duration_ms and previous_value <= count <= files and start + stamp <= MAX_RUN_MS):
            return False
        previous_time, previous_value = stamp, count
    return True


def public(value, metric, files, duration_ms):
    """Keep up to sixteen evenly spaced observations, including the first and last."""
    if not isinstance(value, dict):
        return {"status": "invalid"}
    if value.get("status") in STATUSES:
        return {"status": value["status"]}
    if value.get("status") != "observed" or not _valid_samples(value, metric, files, duration_ms, MAX_SAMPLES):
        return {"status": "invalid"}
    samples = value["samples"]
    if len(samples) > PUBLIC_SAMPLES:
        shown = [samples[i * (len(samples) - 1) // (PUBLIC_SAMPLES - 1)] for i in range(PUBLIC_SAMPLES)]
    else:
        shown = samples
    return dict(status="observed", metric=metric, command_start_ms=value["command_start_ms"],
                observations=len(samples), samples=shown)


def valid_public(value, metric, files, duration_ms):
    if not isinstance(value, dict):
        return False
    if value.get("status") in STATUSES:
        return set(value) == {"status"}
    observations = value.get("observations")
    return (set(value) == {"status", "metric", "command_start_ms", "observations", "samples"}
            and value["status"] == "observed" and _count(observations) and 1 <= observations <= MAX_SAMPLES
            and _valid_samples(value, metric, files, duration_ms, PUBLIC_SAMPLES)
            and len(value["samples"]) == min(observations, PUBLIC_SAMPLES))
