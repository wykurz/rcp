"""Frozen public schemas and collector-time projection, independent of later validators."""

import re

from . import results
from .common import digest, durable_plan_contract, require, timestamp
from .json_bytes import encoded

SCHEMA = "rcp-production-public-report-v1"
MAX_PUBLIC_RECORD = 1024 * 1024
MAX_PUBLIC_HTML = 2 * 1024 * 1024
# public schemas are frozen history contracts. change these labels or the series-ID
# algorithm only by adding a new schema; old records keep their original namespace.
PUBLIC_CONTRACTS = {
    "rcp-production-public-report-v1": {
        "validation": "Metadata/size only; does not prove content integrity or detect same-size corruption.",
        "series_contract": "aws-local-ext4-v1",
    },
    "rcp-production-public-report-v3": {
        "validation": "Metadata/size only; does not prove content integrity or detect same-size corruption.",
        "series_contract": "aws-local-ext4-v1",
    },
    "rcp-production-public-report-v2": {
        "validation": "Metadata/size only; does not prove content integrity or detect same-size corruption.",
        "series_contract": "aws-local-ext4-v1",
    },
}
VALIDATION = PUBLIC_CONTRACTS[SCHEMA]["validation"]



MEASUREMENT_CAVEAT = ("Measurements are self-reported by the benchmark worker and are not independently attested. "
    "Candidate code can alter measurement files; consistency checks and verified resource cleanup do not prove honest timings.")


def comparison_label(identity, controller):
    """Derive frozen comparison wording from the recorded plan's binary contract."""
    if identity["baseline"] != identity["candidate"]:
        return "A/B (different source revisions)"
    request = controller.get("request")
    if not isinstance(request, dict):
        return "Same source revision; binary identity unavailable"
    window = controller.get("window")
    require(isinstance(window, dict), "recorded comparison window is unavailable")
    names = durable_plan_contract(window.get("plan"), exact=False)["binary_names"]
    hashes = request.get("sha256")
    if not isinstance(hashes, dict) or not all(isinstance(hashes.get(role + "/" + name), str) and
            re.fullmatch(r"[0-9a-f]{64}", hashes[role + "/" + name])
            for role in ("baseline", "candidate") for name in names):
        return "Same source revision; binary identity unavailable"
    if all(hashes["baseline/" + name] == hashes["candidate/" + name] for name in names):
        return "A/A (identical source revision and binary digests)"
    return "Same source revision; different binary digests (not A/A)"


def qualification_outcome(value):
    return value.get("qualification_outcome", "passed" if value["benchmark_valid"] else "failed")


def outcome_label(value):
    if value.get("qualification_outcome") == "evidence-unavailable":
        return "unavailable; workload outcome unknown"
    return "consistency checks passed" if value["benchmark_valid"] else "failed; excluded from performance history"


def collected_time(value):
    return value.get("collected_at", "unavailable (legacy record)")


def freeze(run_key, identity, pins, valid, evidence, raw, *, outcome=None, execution_status=None, collected_at=None):
    """Create a public projection at qualification time, before validators can drift."""
    schema = "rcp-production-public-report-v3" if collected_at is not None else (
        "rcp-production-public-report-v2" if outcome == "evidence-unavailable" else SCHEMA)
    contract = PUBLIC_CONTRACTS[schema]
    value = dict(schema=schema, run_key=run_key, retirement_sha256="0" * 64,
        benchmark_valid=valid, cleanup_verified=True, validation=contract["validation"],
        identity={key: identity[key] for key in ("run", "run_url", "suite", "baseline", "candidate", "controller")},
        comparison=comparison_label(identity, evidence["controller"]), measurements=[],
        series_contract=contract["series_contract"],
        note="Four copies form two local pairs; this small run does not establish broad variance.")
    if collected_at is not None:
        value.update(collected_at=timestamp(results.utc(collected_at)),
                     qualification_outcome=outcome or ("passed" if valid else "failed"))
    if valid:
        require(raw is not None and raw["status"] == "complete", "successful projection requires complete measurements")
        value["measurements"] = [{
            **{key: item[key] for key in ("case_id", "variant_id", "median", "samples")},
            "series_id": digest(encoded([
                contract["series_contract"], item["series_id"], pins["ami"], pins["source"]])),
        } for item in raw["summaries"]]
        value["verification"] = evidence["expected_verification"]
        value["timestamp"] = raw["timestamp"]
    elif outcome == "evidence-unavailable":
        value["qualification_outcome"] = outcome
        value["note"] = ("Safely retired with unavailable evidence; workload success or failure is unknown. "
                         "Excluded from performance history.")
    else:
        value["note"] = "Safely retired failed qualification; excluded from performance history."
    if execution_status is not None and execution_status != "SUCCEEDED":
        require(execution_status in {"FAILED", "TIMED_OUT", "ABORTED", "NEVER_SUBMITTED", "NEVER_STARTED"},
                "invalid terminal execution status")
        value["note"] += " Coordinator execution: " + execution_status + "."
    validate_public(value)
    del value["retirement_sha256"]
    return value


def bind(value, retirement_sha256):
    """Attach the later retirement fence without reinterpreting historical measurements."""
    require(isinstance(value, dict) and "retirement_sha256" not in value, "invalid retained public projection")
    return validate_public(dict(value, retirement_sha256=retirement_sha256))


def validate_public(value):
    require(isinstance(value, dict) and isinstance(value.get("schema"), str) and
            value["schema"] in PUBLIC_CONTRACTS, "unsupported public report schema")
    contract = PUBLIC_CONTRACTS[value["schema"]]
    require(isinstance(value.get("run_key"), str) and re.fullmatch(r"repo-[1-9][0-9]{0,19}-run-[1-9][0-9]{0,19}", value["run_key"]) and
            isinstance(value.get("retirement_sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", value["retirement_sha256"]) and
            type(value.get("benchmark_valid")) is bool and value.get("cleanup_verified") is True and
            value.get("validation") == contract["validation"], "invalid public report")
    fields = {"schema", "run_key", "retirement_sha256", "benchmark_valid", "cleanup_verified", "validation",
              "identity", "comparison", "measurements", "series_contract", "note"}
    if value["schema"] in {"rcp-production-public-report-v2", "rcp-production-public-report-v3"}:
        fields.add("qualification_outcome")
        require(value.get("qualification_outcome") in {"passed", "failed", "evidence-unavailable"} and
                (value["qualification_outcome"] == "passed") == value["benchmark_valid"],
                "invalid public qualification outcome")
    if value["schema"] == "rcp-production-public-report-v3":
        fields.add("collected_at")
        require(isinstance(value.get("collected_at"), str) and
                value["collected_at"] == timestamp(results.utc(value["collected_at"])), "invalid trusted collection time")
    if value["benchmark_valid"]:
        fields.update({"verification", "timestamp"})
    require(set(value) == fields and value.get("series_contract") == contract["series_contract"] and
            isinstance(value.get("comparison"), str) and 0 < len(value["comparison"]) <= 256 and
            isinstance(value.get("note"), str) and 0 < len(value["note"]) <= 512,
            "unexpected or unbounded public report fields")
    if value["benchmark_valid"]:
        require(isinstance(value["verification"], dict) and value["verification"] and
                len(encoded(value["verification"])) <= 4096 and
                isinstance(value["timestamp"], str) and 0 < len(value["timestamp"]) <= 64,
                "invalid public verification or timestamp")
        results.utc(value["timestamp"])
    require(isinstance(value.get("identity"), dict) and set(value["identity"]) == {
        "run", "run_url", "suite", "baseline", "candidate", "controller"}, "invalid public identity")
    identity = value["identity"]
    require(isinstance(identity["suite"], str) and 0 < len(identity["suite"]) <= 128, "invalid public suite")
    require(isinstance(identity["run"], str) and re.fullmatch(r"[1-9][0-9]{0,19}-[1-9][0-9]{0,9}", identity["run"]),
            "invalid public run")
    run, attempt = identity["run"].split("-")
    require(value["run_key"].endswith("-run-" + run) and
            identity["run_url"] == "https://github.com/wykurz/rcp/actions/runs/" + run + "/attempts/" + attempt and
            all(isinstance(identity[key], str) and re.fullmatch(r"[0-9a-f]{40}", identity[key])
                for key in ("baseline", "candidate", "controller")), "invalid public revision or link")
    require(isinstance(value.get("measurements"), list) and len(value["measurements"]) <= 100 and
            (value["benchmark_valid"] or not value["measurements"]), "failed run has performance measurements")
    for item in value["measurements"]:
        require(isinstance(item, dict) and set(item) == {"case_id", "variant_id", "series_id", "median", "samples"} and
                all(isinstance(item[key], str) and 0 < len(item[key]) <= 256 for key in ("case_id", "variant_id", "series_id")) and
                results.measurements.nonnegative(item["median"]) and
                isinstance(item["samples"], list) and 0 < len(item["samples"]) <= 100 and
                all(results.measurements.nonnegative(sample) for sample in item["samples"]),
                "invalid public measurements")
    require(len(encoded(value)) <= MAX_PUBLIC_RECORD, "public report exceeds bound")
    return value
