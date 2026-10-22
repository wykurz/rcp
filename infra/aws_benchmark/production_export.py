"""Offline public projection and immutable local history staging.

No AWS or GitHub client is accepted. Retained HTML is evidence, never copied into
public output. Replaying an already staged manifest preserves its historical
measurement bytes even when today's renderer or parser has changed. Existing history
storage is trusted prior exporter output: a source digest alone does not attest
arbitrary saved measurement bytes. Publication must use the protected history path.
"""

from html import escape
import os
from pathlib import Path
import re
import tempfile

from . import production_collection as collection, results
from .common import digest, require

SCHEMA = "rcp-production-public-report-v1"
MAX_PUBLIC_RECORD = 1024 * 1024
MAX_PUBLIC_HTML = 2 * 1024 * 1024
VALIDATION = "Metadata/size only; does not prove content integrity or detect same-size corruption."


def source_fields(bundle, manifest):
    identity = manifest["identity"]
    controller = results.json_value(bundle.payloads["controller.json"])
    fields = dict(run_key=bundle.run_key, retirement_sha256=bundle.record.retirement_sha256,
        benchmark_valid=manifest["benchmark_valid"],
        identity={key: identity[key] for key in ("run", "run_url", "suite", "baseline", "candidate", "controller")},
        comparison=results.comparison_label(identity, controller))
    if manifest["benchmark_valid"]:
        report = results.json_value(bundle.payloads["report.json"], collection.MAX_REPORT)
        recorded = report.get("evidence", {}).get("expected_verification")
        require(isinstance(recorded, dict) and recorded, "retained verification identity is unavailable")
        fields["verification"] = recorded
    return fields


def verify_replay(saved, bundle):
    validate_public(saved)
    fields = source_fields(bundle, bundle.validate())
    require(all(saved.get(key) == value for key, value in fields.items()), "conflicting production history replay")
    return saved


def project(bundle):
    manifest = bundle.validate()
    identity = manifest["identity"]
    value = dict(schema=SCHEMA, run_key=bundle.run_key, retirement_sha256=bundle.record.retirement_sha256,
        benchmark_valid=manifest["benchmark_valid"], cleanup_verified=True, validation=VALIDATION,
        identity={key: identity[key] for key in ("run", "run_url", "suite", "baseline", "candidate", "controller")},
        comparison=results.comparison_label(identity, results.json_value(bundle.payloads["controller.json"])),
        measurements=[], series_contract=results.SERIES_CONTRACT,
        note="Four copies form two local pairs; this small run does not establish broad variance.")
    if manifest["benchmark_valid"]:
        # recheck new exports through the established measurement contract. The retired
        # manifest supplies authenticated policy; callers cannot request eligibility.
        with tempfile.TemporaryDirectory(prefix="production-export-") as directory:
            root = Path(directory)
            for name in ("controller.json", "status.json", "results.zip"):
                if name in bundle.payloads:
                    (root / name).write_bytes(bundle.payloads[name])
            evidence, raw, _ = results.inspect(root, identity, manifest["producer_conclusion"])
        pins = bundle.record.state["request"]["pins"]
        policy = dict(schema_version=1, identity=identity,
            archive_sha256=digest(bundle.payloads["results.zip"]), ami_id=pins["ami"], harness_revision=pins["source"])
        results.approve(evidence, raw, policy)
        accepted = results.derived_record(evidence, raw)
        value["comparison"] = results.comparison_label(identity, evidence["controller"])
        value["measurements"] = [{key: item[key] for key in (
            "case_id", "variant_id", "series_id", "median", "samples")} for item in accepted["summaries"]]
        value["verification"] = evidence["expected_verification"]
        value["timestamp"] = accepted["timestamp"]
    else:
        value["note"] = "Safely retired failed qualification; excluded from performance history."
    require(all(value.get(key) == expected for key, expected in source_fields(bundle, manifest).items()),
            "public projection differs from retained source identity")
    raw = collection.encoded(value)
    require(len(raw) <= MAX_PUBLIC_RECORD, "public report exceeds bound")
    return value


def validate_public(value):
    require(isinstance(value, dict) and value.get("schema") == SCHEMA and
            isinstance(value.get("run_key"), str) and re.fullmatch(r"repo-[1-9][0-9]{0,19}-run-[1-9][0-9]{0,19}", value["run_key"]) and
            isinstance(value.get("retirement_sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", value["retirement_sha256"]) and
            type(value.get("benchmark_valid")) is bool and value.get("cleanup_verified") is True and
            value.get("validation") == VALIDATION, "invalid public report")
    fields = {"schema", "run_key", "retirement_sha256", "benchmark_valid", "cleanup_verified", "validation",
              "identity", "comparison", "measurements", "series_contract", "note"}
    if value["benchmark_valid"]:
        fields.update({"verification", "timestamp"})
    require(set(value) == fields and value.get("series_contract") == results.SERIES_CONTRACT and
            isinstance(value.get("comparison"), str) and 0 < len(value["comparison"]) <= 256 and
            isinstance(value.get("note"), str) and 0 < len(value["note"]) <= 512,
            "unexpected or unbounded public report fields")
    if value["benchmark_valid"]:
        require(isinstance(value["verification"], dict) and value["verification"] and
                len(collection.encoded(value["verification"])) <= 4096 and
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
                collection.results.measurements.nonnegative(item["median"]) and
                isinstance(item["samples"], list) and 0 < len(item["samples"]) <= 100 and
                all(collection.results.measurements.nonnegative(sample) for sample in item["samples"]),
                "invalid public measurements")
    require(len(collection.encoded(value)) <= MAX_PUBLIC_RECORD, "public report exceeds bound")
    return value


def stage(bundle, destination):
    """Create one local immutable record; exact source replay returns saved bytes."""
    bundle.validate()
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    require(not destination.is_symlink(), "history directory must not be a symlink")
    path = destination / (bundle.run_key + ".json")
    if path.exists() or path.is_symlink():
        saved = results.json_value(results.read_file(path, MAX_PUBLIC_RECORD, single_link=False))
        return verify_replay(saved, bundle)
    value = validate_public(project(bundle))
    payload = collection.encoded(value)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(prefix=".production-stage-", dir=destination, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            saved = validate_public(results.json_value(results.read_file(path, MAX_PUBLIC_RECORD, single_link=False)))
            return verify_replay(saved, bundle)
        return value
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def render(value):
    """Escaped script-free public HTML from allowlisted fields, never retained HTML."""
    validate_public(value)
    identity = value["identity"]
    fields = [("Run", identity["run"]), ("Suite", identity["suite"]), ("Comparison", value["comparison"]),
        ("Baseline", identity["baseline"]), ("Candidate", identity["candidate"]),
        ("Qualification", "passed" if value["benchmark_valid"] else "failed"), ("Cleanup", "verified"),
        ("Validation", value["validation"])]
    rows = "".join("<tr><th>" + escape(name) + "</th><td>" + escape(str(item)) + "</td></tr>" for name, item in fields)
    metrics = "".join("<tr>" + "".join("<td>" + escape(str(item[key])) + "</td>" for key in
        ("case_id", "variant_id", "median", "samples")) + "</tr>" for item in value["measurements"])
    html = ("<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width'>"
        "<title>AWS benchmark " + escape(identity["run"]) + "</title><h1>AWS benchmark</h1>"
        "<p><a href='" + escape(identity["run_url"], quote=True) + "'>Workflow attempt</a></p><table>" + rows + "</table>"
        "<p>" + escape(value["note"]) + "</p><table><tr><th>Case</th><th>Variant</th><th>Median seconds</th>"
        "<th>Samples in seconds</th></tr>" + metrics + "</table>")
    require(len(html.encode()) <= MAX_PUBLIC_HTML, "rendered public HTML exceeds bound")
    return html
