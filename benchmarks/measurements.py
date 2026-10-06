"""Scoped local GNU-time measurements and explicitly declared build provenance."""

import hashlib
import math
from pathlib import Path
import re
import sys
import time

from benchmarks import operations
from benchmarks.strict_json import parse_json


POLICY = "linux-gnu-time-fresh-exec-v1"
SCOPE = "one local command, its threads and waited descendants; no remote attribution or simultaneous-tree RSS"
EXIT_STATUS_SCOPE = "resource-supervisor"
PAYLOAD_STATUSES = frozenset({"succeeded", "unconfirmed"})
RESOURCE_STATUSES = frozenset({"complete", "invalid", "unavailable"})
UNAVAILABLE_REASONS = frozenset({"execution_failed"})
FLOATS = ("user_seconds", "system_seconds")
COUNTS = ("max_rss_kib", "voluntary_switches", "involuntary_switches", "minor_faults", "major_faults")
FORMAT = '{"user_seconds":%U,"system_seconds":%S,"max_rss_kib":%M,"voluntary_switches":%w,"involuntary_switches":%c,"minor_faults":%R,"major_faults":%F,"exit_code":%x}'
TRIAL_PHASES = frozenset({"preparation", "verification", "cleanup"})
RUN_PHASES = frozenset({"generation", "initial_verification", "total"})
PHASES = TRIAL_PHASES | RUN_PHASES
PHASE_TOTAL_ABS_TOLERANCE = 1e-6
PHASE_TOTAL_REL_TOLERANCE = 1e-9
ENVIRONMENT_KEYS = ("MIMALLOC_ALLOW_THP", "MIMALLOC_SHOW_STATS", "MIMALLOC_VERBOSE", "MALLOC_CONF",
                    "GLIBC_TUNABLES", "LD_PRELOAD", "LD_LIBRARY_PATH", "RUST_LOG", "TOKIO_WORKER_THREADS")
HASH = re.compile(r"[0-9a-f]{64}\Z")
COMMIT = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")


def nonnegative(value, integer=False):
    if type(value) not in ((int,) if integer else (int, float)) or value < 0:
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def child_locale(variant, local_resources=False):
    return operations.SUMMARY_LOCALE if local_resources or operations.summary_supported(variant) else "inherited"


def payload_status(exit_codes, timed_out=False, launch_error=None, *, expected_commands=1):
    """Qualify execution only from exactly typed zero exits and no interruption."""
    succeeded = (type(expected_commands) is int and expected_commands >= 0
                 and isinstance(exit_codes, list) and len(exit_codes) == expected_commands
                 and all(type(code) is int and code == 0 for code in exit_codes)
                 and timed_out is False and launch_error is None)
    return "succeeded" if succeeded else "unconfirmed"


def validate_provenance_mode(mode):
    if mode != "local":
        raise ValueError("build provenance requires local mode; runner environment does not describe remote payloads")


def add_cost(record, phase, started):
    costs = record.setdefault("phase_seconds", {})
    costs[phase] = costs.get(phase, 0) + max(0, time.monotonic() - started)


def identity(context):
    result = {}
    if "pairing" in context:
        result["pairing_policy"] = context["pairing"]["policy"]
    for key in ("local_resources", "measurement_environment"):
        if key in context:
            result[key] = context[key]
    if "build_provenance" in context:
        result["declared_build_configuration"] = {key: {field: sorted(build[field]) if field == "features" else build[field] for field in ("target", "profile", "features", "rustflags", "rustc")}
                                                   for key, build in context["build_provenance"]["builds"].items()}
    return result or None


def policy(tool):
    if sys.platform != "linux" or not isinstance(tool, dict) or not isinstance(tool.get("version"), str) or "GNU" not in tool["version"] or not isinstance(tool.get("sha256"), str) or not HASH.fullmatch(tool["sha256"]):
        raise ValueError("local resources require Linux and GNU time")
    return dict(policy=POLICY, scope=SCOPE, time_sha256=tool["sha256"], time_version=tool["version"])


def validate_selection(mode, variants):
    if mode != "local" or not isinstance(variants, list) or any(not isinstance(item, dict) or type(item.get("processes")) is not int or item["processes"] != 1 for item in variants):
        raise ValueError("local resources require local single-process variants")


def wrap(command, supervisor, path):
    return [supervisor, "--quiet", "--format", FORMAT, "--output", str(path), "--", *command]


def validate_metrics(value):
    if not isinstance(value, dict) or set(value) != set(FLOATS + COUNTS + ("exit_code",)):
        raise ValueError("resource metrics have invalid fields")
    if any(not nonnegative(value[key]) for key in FLOATS) or any(not nonnegative(value[key], True) for key in COUNTS):
        raise ValueError("resource metrics must be finite nonnegative values in the declared units")
    if type(value["exit_code"]) is not int or not 0 <= value["exit_code"] <= 255:
        raise ValueError("resource exit code must be a byte")
    return value


def collect(path, successful):
    # the GNU time supervisor cannot distinguish failed exec from a payload exit of 126/127,
    # and its %x field reports zero for signal deaths: accept only successful commands
    if not successful:
        return dict(status="unavailable", metrics=None, reason="execution_failed",
                    error="resource metrics withheld because successful command execution was not confirmed")
    try:
        raw = Path(path).read_bytes()
    except OSError as error:
        return dict(status="unavailable", metrics=None, error=str(error))
    fingerprint = hashlib.sha256(raw).hexdigest()
    try:
        metrics = validate_metrics(parse_json(raw.decode()))
        if metrics["exit_code"] != 0:
            raise ValueError("successful command has nonzero resource exit code")
    except (UnicodeError, ValueError) as error:
        return dict(status="invalid", metrics=None, raw_sha256=fingerprint, error=str(error))
    return dict(status="complete", metrics=metrics, raw_sha256=fingerprint)


def validate_builds(value, tools):
    if not isinstance(value, dict) or not value or set(value) - {"rcp", "rcp-baseline"}:
        raise ValueError("build provenance must map rcp and/or rcp-baseline")
    required = {"binary_sha256", "source_revision", "source_dirty", "patch_sha256", "cargo_lock_sha256",
                "flake_lock_sha256", "target", "profile", "features", "rustflags", "rustc"}
    for key, build in value.items():
        if not isinstance(build, dict) or set(build) != required:
            raise ValueError("build provenance has invalid fields")
        for field in ("binary_sha256", "cargo_lock_sha256", "flake_lock_sha256"):
            if not isinstance(build[field], str) or not HASH.fullmatch(build[field]):
                raise ValueError(f"build provenance {field} must be SHA256")
        if not isinstance(tools, dict) or not isinstance(tools.get(key), dict) or build["binary_sha256"] != tools[key].get("sha256"):
            raise ValueError("declared build does not match the observed executable hash")
        if not isinstance(build["source_revision"], str) or not COMMIT.fullmatch(build["source_revision"]):
            raise ValueError("build source_revision must be a full commit ID")
        if type(build["source_dirty"]) is not bool:
            raise ValueError("build source_dirty must be boolean")
        if build["patch_sha256"] is not None and (not isinstance(build["patch_sha256"], str) or not HASH.fullmatch(build["patch_sha256"])):
            raise ValueError("build patch_sha256 must be SHA256 or null")
        if build["source_dirty"] and build["patch_sha256"] is None:
            raise ValueError("dirty build provenance requires a patch hash")
        if any(not isinstance(build[key], str) or not build[key].strip() for key in ("target", "profile", "rustc")):
            raise ValueError("build target, profile and rustc must be nonblank strings")
        flags = build["rustflags"]
        if not isinstance(flags, list) or any(not isinstance(flag, str) or not flag for flag in flags):
            raise ValueError("build rustflags must be nonempty argument strings")
        features = build["features"]
        if not isinstance(features, list) or any(not isinstance(feature, str) or not feature for feature in features) or len(set(features)) != len(features):
            raise ValueError("build features must be distinct nonempty strings")
    return value


def load_builds(path, tools):
    raw = Path(path).read_bytes()
    value = parse_json(raw.decode())
    if not isinstance(value, dict) or set(value) != {"schema_version", "builds"} or type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("build provenance requires schema_version 1 and builds")
    return dict(qualification="caller-declared; executable hashes verified, source claims not attested",
                input_sha256=hashlib.sha256(raw).hexdigest(), builds=validate_builds(value["builds"], tools))


def validate(run):
    try:
        _validate(run)
    except (KeyError, TypeError, OverflowError) as error:
        raise ValueError("malformed resource measurement or provenance record") from error


def _validate_resource_trial(row):
    result = row.get("resources")
    if not isinstance(result, dict) or result.get("status") not in RESOURCE_STATUSES:
        raise ValueError("missing resource collection status")
    timed_out = row.get("timed_out", False)
    if type(timed_out) is not bool:
        raise ValueError("resource trial timed_out must be boolean")
    codes = row.get("exit_codes")
    status = payload_status(codes, timed_out, row.get("launch_error"))
    command_succeeded = status == "succeeded"
    execution_recorded = (bool(codes) or timed_out or row.get("launch_error") is not None
                          or result["status"] in ("complete", "invalid")
                          or "exit_status_scope" in row or "payload_status" in row
                          or result.get("reason") == "execution_failed")
    if execution_recorded:
        commands = row.get("commands")
        if not isinstance(commands, list) or len(commands) != 1 or not isinstance(commands[0], list) or not commands[0] or any(not isinstance(arg, str) for arg in commands[0]):
            raise ValueError("local resource execution requires exactly one recorded command")
        if row.get("exit_status_scope") != EXIT_STATUS_SCOPE or row.get("payload_status") != status:
            raise ValueError("resource supervisor outcome has invalid qualification")
    if "reason" in result and (result["reason"] not in UNAVAILABLE_REASONS or result["status"] != "unavailable"):
        raise ValueError("invalid unavailable resource reason")
    if result["status"] == "complete":
        validate_metrics(result.get("metrics"))
        if not command_succeeded or result["metrics"]["exit_code"] != 0:
            raise ValueError("accepted resource metrics require successful command execution")
    elif "metrics" not in result or result["metrics"] is not None:
        raise ValueError("incomplete resource record requires null metrics")
    if result["status"] in ("complete", "invalid"):
        if not command_succeeded:
            raise ValueError("resource artifact requires successful command execution")
        if not isinstance(result.get("raw_sha256"), str) or not HASH.fullmatch(result["raw_sha256"]):
            raise ValueError(f"{result['status']} resource record requires raw fingerprint")
    elif execution_recorded and not command_succeeded:
        if result.get("reason") != "execution_failed":
            raise ValueError("failed execution requires unavailable resources with execution_failed reason")
    elif "reason" in result:
        raise ValueError("execution_failed reason requires unsuccessful recorded execution")
    if row["status"] == "ok" and result["status"] != "complete":
        raise ValueError("successful trial requires complete successful resource measurements")


def _validate(run):
    context = run["context"]
    if not isinstance(context, dict) or not isinstance(run["trials"], list) or any(not isinstance(row, dict) for row in run["trials"]):
        raise ValueError("measurement context must be an object and trials an array of objects")
    resource = context.get("local_resources")
    if resource is None and any("resources" in row for row in run["trials"]):
        raise ValueError("resource trials require context.local_resources")
    if "local_resources" in context:
        if not isinstance(resource, dict) or set(resource) != {"policy", "scope", "time_sha256", "time_version"} or resource["policy"] != POLICY or resource["scope"] != SCOPE:
            raise ValueError("unsupported local resource policy")
        if not isinstance(resource["time_sha256"], str) or not HASH.fullmatch(resource["time_sha256"]) or not isinstance(resource["time_version"], str) or "GNU" not in resource["time_version"]:
            raise ValueError("resource supervisor identity is invalid")
        validate_selection(context["topology"], run["variants"])
        for row in run["trials"]:
            _validate_resource_trial(row)
    declared = context.get("build_provenance")
    if "build_provenance" in context:
        if not isinstance(declared, dict) or set(declared) != {"qualification", "input_sha256", "builds"} or declared["qualification"] != "caller-declared; executable hashes verified, source claims not attested":
            raise ValueError("invalid build provenance qualification")
        validate_provenance_mode(context.get("topology"))
        if not isinstance(declared["input_sha256"], str) or not HASH.fullmatch(declared["input_sha256"]):
            raise ValueError("build provenance requires input fingerprint")
        validate_builds(declared["builds"], run["tools"])
    env = context.get("measurement_environment")
    if any(key in context for key in ("pairing", "local_resources", "build_provenance")) and "measurement_environment" not in context:
        raise ValueError("experiment context requires measurement_environment evidence")
    if "measurement_environment" in context and (not isinstance(env, dict) or set(env) - set(ENVIRONMENT_KEYS) or any(not isinstance(value, str) for value in env.values())):
        raise ValueError("invalid measurement environment allowlist")
    complete_measurement = run.get("status") == "complete" and (resource is not None or "pairing" in context)
    phase_values = []
    for row in run["trials"]:
        costs = row.get("phase_seconds", {})
        if not isinstance(costs, dict) or set(costs) - TRIAL_PHASES or any(not nonnegative(value) for value in costs.values()):
            raise ValueError("invalid trial phase costs")
        if complete_measurement and not TRIAL_PHASES <= set(costs):
            raise ValueError("completed measured trial requires preparation, verification and cleanup phase costs")
        phase_values.extend(costs.values())
    costs = run.get("phase_seconds", {})
    if not isinstance(costs, dict) or set(costs) - RUN_PHASES or any(not nonnegative(value) for value in costs.values()):
        raise ValueError("invalid run phase costs")
    if complete_measurement:
        if not RUN_PHASES <= set(costs):
            raise ValueError("completed measured run requires generation, initial_verification and total phase costs")
        # these wall intervals are disjoint and enclosed by total; command time is not added
        phase_values.extend(costs[phase] for phase in RUN_PHASES if phase != "total")
        try:
            phase_total = math.fsum(phase_values)
        except OverflowError as error:
            raise ValueError("recorded phase cost sum must be finite") from error
        if not math.isfinite(phase_total):
            raise ValueError("recorded phase cost sum must be finite")
        if costs["total"] < phase_total and not math.isclose(costs["total"], phase_total,
                rel_tol=PHASE_TOTAL_REL_TOLERANCE, abs_tol=PHASE_TOTAL_ABS_TOLERANCE):
            raise ValueError("completed measured run total must cover non-overlapping recorded phase costs")
