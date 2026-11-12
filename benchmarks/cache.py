"""Bounded cache preparation and the worker's fixed-operation socket protocol."""

import json
import math
import os
import socket
import subprocess
import tempfile
import time

from benchmarks.strict_json import parse_json

DEADLINE_ENV = "RCP_BENCH_CACHE_DEADLINE_NS"
WORKER_DEADLINE_ENV = "RCP_BENCH_WORKER_DEADLINE_NS"
MAX_BROKER_SECONDS = 8 * 3600
DEFAULT_SECONDS = 1200
MAX_REQUEST = 256
MAX_REPLY = 4096
DIAGNOSTIC_BYTES = 2048
VERSION = 1


def remaining(deadline):
    if type(deadline) is not int or not 0 < deadline < 2**63:
        raise ValueError("invalid cache deadline")
    seconds = (deadline - time.monotonic_ns()) / 1_000_000_000
    if seconds <= 0:
        raise TimeoutError("cache operation deadline exhausted")
    return seconds


def deadline_after(seconds):
    now = time.monotonic_ns()
    # validate representability before multiplying or converting a large finite float
    if (isinstance(seconds, bool) or not isinstance(seconds, (int, float))
            or not 0 < seconds < (2**63 - 1 - now) / 1_000_000_000
            or not math.isfinite(seconds)):
        raise ValueError("cache timeout must be finite, positive and fit the monotonic deadline")
    deadline = now + int(seconds * 1_000_000_000)
    remaining(deadline)
    return deadline


def default_seconds(command_timeout):
    # legacy preparation gave sync and the following command separate timeout budgets
    deadline_after(command_timeout)
    value = max(DEFAULT_SECONDS, 2 * command_timeout)
    deadline_after(value)
    return value


def receive(connection, deadline, limit):
    data = bytearray()
    while True:
        connection.settimeout(remaining(deadline))
        part = connection.recv(min(1024, limit + 1 - len(data)))
        if not part:
            raise ValueError("incomplete cache response or request")
        data.extend(part)
        if len(data) > limit:
            raise ValueError("cache frame exceeds limit")
        if b"\n" in part:
            if not data.endswith(b"\n") or data.count(b"\n") != 1:
                raise ValueError("invalid cache frame")
            remaining(deadline)
            return parse_json(data.decode("ascii"))


def send(connection, value, deadline, limit):
    data = (json.dumps(value, separators=(",", ":"), allow_nan=False) + "\n").encode("ascii")
    if len(data) > limit:
        raise ValueError("cache frame exceeds limit")
    connection.settimeout(remaining(deadline))
    connection.sendall(data)
    remaining(deadline)


def request_deadline(value):
    if not isinstance(value, dict) or set(value) != {"version", "operation", "deadline_ns"} or type(value["version"]) is not int or value["version"] != VERSION or value["operation"] != "drop":
        raise ValueError("invalid cache request")
    remaining(value["deadline_ns"])
    return value["deadline_ns"]


def response(value):
    required = {"version", "ok", "stage", "phases_seconds"}
    if not isinstance(value, dict) or not required <= set(value) or set(value) - required - {"error_type", "errno"}:
        raise ValueError("invalid cache response")
    if type(value["version"]) is not int or value["version"] != VERSION or type(value["ok"]) is not bool or not isinstance(value["stage"], str) or value["stage"] not in {"request", "sync", "drop", "reply"}:
        raise ValueError("invalid cache response")
    phases = value["phases_seconds"]
    if not isinstance(phases, dict) or set(phases) - {"sync", "drop"} or any(isinstance(v, bool) or not isinstance(v, (int, float)) or not 0 <= v <= 86400 or not math.isfinite(v) for v in phases.values()):
        raise ValueError("invalid cache phase timings")
    if "error_type" in value and (not isinstance(value["error_type"], str) or not value["error_type"].isidentifier() or len(value["error_type"]) > 80):
        raise ValueError("invalid cache error type")
    if "errno" in value and (type(value["errno"]) is not int or not 0 <= value["errno"] <= 4095):
        raise ValueError("invalid cache errno")
    if value["ok"] and (value["stage"] != "reply" or set(phases) != {"sync", "drop"} or "error_type" in value or "errno" in value):
        raise ValueError("invalid successful cache response")
    return value


def _captured(stream):
    stream.seek(0, os.SEEK_END)
    size = stream.tell()
    stream.seek(0)
    data = stream.read(DIAGNOSTIC_BYTES)
    if size > DIAGNOSTIC_BYTES:
        stream.seek(-DIAGNOSTIC_BYTES // 2, os.SEEK_END)
        data = data[:DIAGNOSTIC_BYTES // 2] + stream.read(DIAGNOSTIC_BYTES // 2)
    text = data.decode("utf-8", errors="replace")
    text = "".join(c if c in "\n\t" or c.isprintable() else "?" for c in text)
    return {"text": text, "bytes": size, "truncated": size > DIAGNOSTIC_BYTES}


class PreparationError(ValueError):
    def __init__(self, observation):
        self.observation = observation

    def __str__(self):
        detail = self.observation.get("stderr", {}).get("text", "").strip()
        return (f"cache preparation failed at {self.observation['stage']}: "
                f"{self.observation['error_type']}" + (f"; stderr: {detail}" if detail else ""))


def _checked(argv, deadline, stage, report, *, env=None):
    started = time.monotonic_ns()
    observation = {"stage": stage, "status": "running"}
    report["commands"].append(observation)
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        try:
            subprocess.run(argv, check=True, stdout=stdout, stderr=stderr,
                           timeout=remaining(deadline), env=env)
            remaining(deadline)
            observation["status"] = "complete"
        except BaseException as error:
            observation.update(status="failed", error_type=type(error).__name__)
            if isinstance(error, subprocess.CalledProcessError):
                observation["returncode"] = error.returncode
            # a timed-out facade does not cancel the privileged kernel operation
            observation["completion"] = "unknown" if stage == "drop" else "not-confirmed"
            if isinstance(error, (KeyboardInterrupt, InterruptedError)):
                raise
            raise PreparationError(observation) from error
        finally:
            observation["elapsed_seconds"] = (time.monotonic_ns() - started) / 1_000_000_000
            observation["stdout"] = _captured(stdout)
            observation["stderr"] = _captured(stderr)
        return observation


def prepare(seconds, report, *, inherit_worker=False):
    """Fail the trial on any uncertain operation, without replay or warm-cache fallback."""
    report.update(timeout_seconds=seconds, policy="explicit-or-local", status="running", commands=[])
    try:
        deadline = deadline_after(seconds)
        worker_deadline = os.environ.get(WORKER_DEADLINE_ENV)
        if worker_deadline is not None:
            if not worker_deadline.isascii() or not worker_deadline.isdecimal() or len(worker_deadline) > 19:
                raise ValueError("invalid worker cache deadline")
            worker_deadline = int(worker_deadline)
            remaining(worker_deadline)
            deadline = worker_deadline if inherit_worker else min(deadline, worker_deadline)
            if inherit_worker:
                report.update(timeout_seconds=remaining(deadline), policy="remaining-worker-budget")
        report["effective_timeout_seconds"] = remaining(deadline)
        _checked(["sync"], deadline, "sync", report)
        result = _checked(["sudo", "-n", "sh", "-c", "printf 3 > /proc/sys/vm/drop_caches"],
            deadline, "drop", report, env={**os.environ, DEADLINE_ENV: str(deadline)})
        output = result["stdout"]
        if output["text"] and not output["truncated"]:
            try:
                report["broker"] = response(parse_json(output["text"]))
            except (ValueError, TypeError):
                # real sudo need not speak the AMI-only broker protocol
                pass
        report["status"] = "complete"
    except BaseException:
        report["status"] = "failed"
        raise
