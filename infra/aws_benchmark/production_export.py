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
import tempfile

from . import production_collection as collection, results
from .common import require

from . import production_projection as projection
from .production_projection import (SCHEMA, MAX_PUBLIC_RECORD, MAX_PUBLIC_HTML,
                                    PUBLIC_CONTRACTS, VALIDATION, validate_public)


def source_fields(bundle, manifest):
    identity = manifest["identity"]
    controller = results.json_value(bundle.payloads["controller.json"])
    fields = dict(run_key=bundle.run_key, retirement_sha256=bundle.record.retirement_sha256,
        benchmark_valid=manifest["benchmark_valid"],
        identity={key: identity[key] for key in ("run", "run_url", "suite", "baseline", "candidate", "controller")},
        comparison=projection.comparison_label(identity, controller))
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
    fields = source_fields(bundle, manifest)
    report = results.json_value(bundle.payloads["report.json"], collection.MAX_REPORT)
    # retirement and the cross-job inventory authenticate collector-produced bytes
    value = projection.bind(report["public_projection"], bundle.record.retirement_sha256)
    require(all(value.get(key) == expected for key, expected in fields.items()),
            "public projection differs from retained source identity")
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
        ("Qualification", projection.outcome_label(value)), ("Cleanup", "resource termination verified"),
        ("Collected at", projection.collected_time(value)),
        ("Validation", value["validation"])]
    rows = "".join("<tr><th>" + escape(name) + "</th><td>" + escape(str(item)) + "</td></tr>" for name, item in fields)
    metrics = "".join("<tr>" + "".join("<td>" + escape(str(item[key])) + "</td>" for key in
        ("case_id", "variant_id", "median", "samples")) + "</tr>" for item in value["measurements"])
    html = ("<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width'>"
        "<title>AWS benchmark " + escape(identity["run"]) + "</title><h1>AWS benchmark</h1>"
        "<p><a href='" + escape(identity["run_url"], quote=True) + "'>Workflow attempt</a></p><table>" + rows + "</table>"
        "<p>" + escape(projection.MEASUREMENT_CAVEAT) + "</p><p>" + escape(value["note"]) + "</p><table><tr><th>Case</th><th>Variant</th><th>Median seconds</th>"
        "<th>Samples in seconds</th></tr>" + metrics + "</table>")
    require(len(html.encode()) <= MAX_PUBLIC_HTML, "rendered public HTML exceeds bound")
    return html
