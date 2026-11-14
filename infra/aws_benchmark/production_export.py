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
from . import production_diagnostics as resource_diagnostics
from . import production_history as history
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


def diagnostic_companion(bundle):
    """Derive optional resource presentation without altering frozen history bytes."""
    bundle.validate()
    record = project(bundle)
    archive = bundle.payloads.get("results.zip")
    if archive is None:
        return resource_diagnostics.unavailable(record, "raw_unavailable")
    try:
        return resource_diagnostics.from_archive(record, archive)
    except Exception:
        return resource_diagnostics.unavailable(record, "invalid_diagnostics")


def badge(value):
    """Show the frozen outcome wording; the class only selects its colour."""
    kind = {"passed": "passed", "failed": "failed"}.get(projection.qualification_outcome(value), "unavailable")
    return "<span class='badge badge-" + kind + "'>" + escape(projection.outcome_label(value)) + "</span>"


def commit_html(identity, revision):
    """Link a validated revision to its commit in the repository that ran the workflow."""
    repository = identity["run_url"].split("/actions/")[0]
    return ("<a href='" + escape(repository + "/commit/" + revision) + "'><code>" +
            escape(revision[:7]) + "</code></a>")


def collected_html(value):
    """Show the trusted collection time readably while keeping its exact form in the markup."""
    if "collected_at" not in value:
        return escape(projection.collected_time(value))
    return history.time_html(value["collected_at"])


def render(value, *, diagnostics=None):
    """Escaped script-free page body from allowlisted fields, never retained HTML.

    The caller supplies the document shell, so nothing here depends on where the page is served.
    """
    validate_public(value)
    if diagnostics is not None:
        resource_diagnostics.validate(diagnostics, value)
    identity = value["identity"]
    commit = lambda revision: commit_html(identity, revision)
    facts = ([("Commit", commit(identity["candidate"]))] if identity["baseline"] == identity["candidate"] else
             [("Candidate", commit(identity["candidate"])), ("Baseline", commit(identity["baseline"]))])
    facts += [("Controller", commit(identity["controller"])),
        ("Suite", "<code>" + escape(identity["suite"]) + "</code>"), ("Comparison", escape(value["comparison"])),
        ("Collected at", collected_html(value)), ("Cleanup", "resource termination verified"),
        ("Validation", escape(value["validation"])),
        ("Workflow", "<a href='" + escape(identity["run_url"]) + "'>Workflow attempt</a>")]
    html = ("<h1>Run " + escape(identity["run"]) + " " + badge(value) + "</h1><dl class='facts'>" +
        "".join("<div><dt>" + name + "</dt><dd>" + item + "</dd></div>" for name, item in facts) +
        "</dl><aside class='note'><p>" + escape(projection.MEASUREMENT_CAVEAT) + "</p><p>" +
        escape(value["note"]) + "</p></aside><section aria-labelledby='results'><h2 id='results'>Results</h2>")
    if not value["measurements"]:
        html += "<p>This run has no performance measurements.</p>"
    for case, item in history.point(value, diagnostics).items():
        html += ("<h3>" + history.CASES[case] + "</h3><p>This run as one history point: the median of the "
            "copies made by the build under test" + (", counting every copy because baseline and candidate "
            "binaries are identical" if item["pooled"] else "") + ".</p><div class='metrics'>")
        for metric, title, unit in history.METRICS:
            data = item[metric]
            html += "<div class='metric'><h4>" + title + "</h4>" + (
                "<p class='value'>unavailable</p><p class='delta'>No recorded value for this run.</p>"
                if data is None else
                "<p class='value'>" + history.rounded(data["value"]) + " <span class='unit'>" + unit +
                "</span></p><p class='delta'>" + history.rounded(data["low"]) + "–" + history.rounded(data["high"]) +
                " " + unit + " over " + history.plural(data["samples"], "copy", "copies") + "</p>") + "</div>"
        html += "</div>"
    if value["measurements"]:
        html += ("<h3>Frozen measurements</h3><div class='table-scroll'><table class='data'><thead><tr><th>Case</th>"
            "<th>Variant</th><th>Copies</th><th>Median s</th><th>Samples s</th><th>Frozen series</th></tr></thead>"
            "<tbody>" + "".join(
                "<tr><td><code>" + escape(item["case_id"]) + "</code></td><td><code>" + escape(item["variant_id"]) +
                "</code></td><td>" + str(len(item["samples"])) + "</td><td>" + history.exact(item["median"]) +
                "</td><td>" + ", ".join(history.exact(sample) for sample in item["samples"]) +
                "</td><td><code title='" + escape(item["series_id"]) + "'>" + escape(item["series_id"][:12]) +
                "</code></td></tr>" for item in value["measurements"]) + "</tbody></table></div>")
    html += "</section>"
    if diagnostics is not None:
        html += resource_diagnostics.render(diagnostics, value)
    require(len(html.encode()) <= MAX_PUBLIC_HTML, "rendered public HTML exceeds bound")
    return html
