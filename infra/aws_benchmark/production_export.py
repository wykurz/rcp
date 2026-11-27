"""Offline public projection and immutable local history staging.

No AWS or GitHub client is accepted. Retained HTML is evidence, never copied into
public output. Replaying an already staged manifest preserves its historical
measurement bytes even when today's renderer or parser has changed. Existing history
storage is trusted prior exporter output: a source digest alone does not attest
arbitrary saved measurement bytes. Publication must use the protected history path.
"""

from datetime import timedelta
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


# retention of everything under production/runs/ in the benchmark bucket (production-stack.yaml)
EVIDENCE_RETENTION_DAYS = 90
RESULTS = {"complete": "measured", "failed": "failed", "timed_out": "timed out", "interrupted": "cut short",
           "not_run": "not run"}
STAGES = {"capacity": "During the free-space check", "generation": "During generation",
          "cache": "During cache preparation", "copy": "During copy", "summary": "In the copy summary",
          "harness": "In the benchmark harness", "transport": "Setting up the remote copy",
          "storage": "Preparing the storage"}


def badge(value):
    """Show the frozen outcome wording; the class only selects its colour."""
    kind = {"passed": "passed", "failed": "failed"}.get(projection.qualification_outcome(value), "unavailable")
    if kind == "passed" and any(cell["outcome"] != "complete" for cell in value.get("cells", ())):
        # a run that measured only some of its cells counts, and is not shown as a clean pass
        kind = "partial"
    return "<span class='badge badge-" + kind + "'>" + escape(projection.outcome_label(value)) + "</span>"


def evidence_html(value):
    """Say where the retained worker evidence is and until when; None when no trusted time bounds it."""
    if "collected_at" not in value:
        return None
    expiry = results.utc(value["collected_at"]) + timedelta(days=EVIDENCE_RETENTION_DAYS)
    return ("<code>production/runs/" + escape(value["run_key"]) + "/collection/</code> in the benchmark bucket "
            "until about " + escape(history.moment(expiry).rsplit(" ", 2)[0]) + "; fetch it with <code>just "
            "aws-benchmark-evidence " + escape(value["run_key"]) + " DIRECTORY</code>")


def _name(value, names):
    return escape(names[value]) if value in names else "<code>" + escape(value) + "</code>"


def cells_html(value):
    """List every cell of a matrix run: what was measured and why the rest was not."""
    measured = {(item["case_id"], item["scenario"]): item for item in value["measurements"]}
    rows = []
    for cell in value["cells"]:
        item = measured.get((cell["case_id"], cell["scenario"]))
        numbers = ("<td>—</td>" * 3 if item is None else
            "<td>" + history.exact(item["median"]) + "</td><td>" + history.exact(min(item["samples"])) + "–" +
            history.exact(max(item["samples"])) + "</td><td>" + str(len(item["samples"])) + "</td>")
        reason = ": ".join(escape(part) for part in (STAGES.get(cell["stage"], cell["stage"]), cell["message"])
                           if part)
        rows.append("<tr><td>" + _name(cell["case_id"], history.NAMES) + "</td><td>" +
                    _name(cell["scenario"], history.SCENARIOS) + "</td><td>" + RESULTS[cell["outcome"]] + "</td>" +
                    numbers + "<td class='reason'>" + reason + "</td></tr>")
    return ("<div class='table-scroll'><table class='data cells'><thead><tr><th>File set</th><th>Scenario</th>"
            "<th>Result</th><th>Median s</th><th>Range s</th><th>Samples</th><th>What happened</th></tr></thead>"
            "<tbody>" + "".join(rows) + "</tbody></table></div>")


def reference_html(value):
    """Set what each other tool took beside rcp, for every copy of a run that such a tool made too."""
    found = history.references(value)
    if not found:
        return ""
    tools = [tool for tool in history.REFERENCE_TOOLS if any(tool in entry["tools"] for entry in found.values())]
    multiple = next(iter(found.values()))["multiple"]
    rows = []
    for (case, copies), entry in found.items():
        cells = []
        for tool in tools:
            figures = history.reference_figures(entry, tool) if tool in entry["tools"] else ("—",)
            cells.append("not measured" if figures is None else "<br>".join(part for part in figures if part))
        rows.append("<tr><td>" + _name(case, history.NAMES) + "</td><td>" + _name(copies, history.SCENARIOS) +
                    "</td><td>" + history.span(entry["rcp"]) + "</td>" +
                    "".join("<td>" + cell + "</td>" for cell in cells) + "</tr>")
    return ("<h3 id='reference'>cp and rsync beside rcp</h3><p>What each tool took for the copies of this run, as the "
            "median of its copies, and how that stands against rcp." +
            ("" if multiple is None else f" A tool has {multiple:g} times what rcp took and is cut off then.") +
            "</p><div class='table-scroll'><table class='data'><thead><tr><th>File set</th><th>Copy</th><th>rcp</th>" +
            "".join("<th><code>" + history.REFERENCE_TOOLS[tool] + "</code></th>" for tool in tools) +
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>")


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
    # a matrix run measures one build: it has cells, and nothing to compare
    matrix = "cells" in value
    commit = lambda revision: commit_html(identity, revision)
    facts = ([("Commit", commit(identity["candidate"]))] if identity["baseline"] == identity["candidate"] else
             [("Candidate", commit(identity["candidate"])), ("Baseline", commit(identity["baseline"]))])
    facts += [("Controller", commit(identity["controller"])),
        ("Suite", "<code>" + escape(identity["suite"]) + "</code>")]
    if not matrix:
        facts.append(("Comparison", escape(value["comparison"])))
    facts += [("Collected at", collected_html(value)), ("Cleanup", "resource termination verified"),
        ("Validation", escape(value["validation"])),
        ("Workflow", "<a href='" + escape(identity["run_url"]) + "'>Workflow attempt</a>")]
    html = ("<h1>Run " + escape(identity["run"]) + " " + badge(value) + "</h1><dl class='facts'>" +
        "".join("<div><dt>" + name + "</dt><dd>" + item + "</dd></div>" for name, item in facts) +
        "</dl><aside class='note'><p>" + escape(value["note"]) + "</p></aside>")
    if matrix and value["diagnostics"]:
        html += ("<section aria-labelledby='run-notes'><h2 id='run-notes'>Run notes</h2><ul>" +
                 "".join("<li>" + escape(item) + "</li>" for item in value["diagnostics"]) + "</ul></section>")
    html += "<section aria-labelledby='results'><h2 id='results'>Results</h2>"
    if not value["measurements"]:
        html += "<p>This run has no performance measurements.</p>"
    if matrix and value["cells"]:
        html += cells_html(value) + reference_html(value)
    # a run that measured more than local copies names the scenario of each summary
    several = matrix and any(cell["scenario"] != history.LOCAL for cell in value["cells"])
    points = {scenario: history.point(value, diagnostics, scenario) for scenario in history.TRENDS}
    # what a summary is of: a kind of copy by its cell's name, a tool by its own
    names = dict(history.SCENARIOS, **history.TOOL_TITLES)
    # in the order the record measured its file sets, a file set's trends next to each other
    measured = list(dict.fromkeys(item["case_id"] for item in value["measurements"]))
    for scenario, case, item in ((scenario, case, points[scenario][case]) for case in measured
                                 for scenario in history.TRENDS if case in points[scenario]):
        # a tool's point holds its runs after both kinds of copy when the run timed it after both
        counted = ("runs of the tool by the build under test" +
                   (", after its local and its remote copies" if item["pooled"] else "")
                   if scenario in history.TOOLS else
                   "copies made by the build under test" +
                   (", counting every copy because baseline and candidate binaries are identical"
                    if item["pooled"] else ""))
        html += ("<h3>" + history.NAMES[case] + (" · " + names[scenario] if several else "") +
            "</h3><p>This run as one history point: the median of the " + counted + ".</p><div class='" +
            history.layout(scenario) + "'>")
        for metric, title, unit in history.panels(scenario):
            data = item[metric]
            html += "<div class='metric'><h4>" + title + "</h4>" + (
                "<p class='value'>unavailable</p><p class='delta'>No recorded value for this run.</p>"
                if data is None else
                "<p class='value'>" + history.rounded(data["value"]) + " <span class='unit'>" + unit +
                "</span></p><p class='delta'>" + history.rounded(data["low"]) + "–" + history.rounded(data["high"]) +
                " " + unit + " over " + history.plural(data["samples"], *item["noun"]) + "</p>") + "</div>"
        html += "</div>"
    if value["measurements"]:
        # a run that timed the other tools has samples that are not copies
        counted = "Copies or tool runs" if matrix and any(
            cell["scenario"] in history.TOOL_CELLS for cell in value["cells"]) else "Copies"
        html += ("<h3>Frozen measurements</h3><div class='table-scroll'><table class='data'><thead><tr><th>Case</th>"
            "<th>" + ("Scenario" if matrix else "Variant") +
            "</th><th>" + counted + "</th><th>Median s</th><th>Samples s</th><th>Frozen series</th></tr></thead>"
            "<tbody>" + "".join(
                "<tr><td><code>" + escape(item["case_id"]) + "</code></td><td><code>" +
                escape(item["scenario"] if matrix else item["variant_id"]) +
                "</code></td><td>" + str(len(item["samples"])) + "</td><td>" + history.exact(item["median"]) +
                "</td><td>" + ", ".join(history.exact(sample) for sample in item["samples"]) +
                "</td><td><code title='" + escape(item["series_id"]) + "'>" + escape(item["series_id"][:12]) +
                "</code></td></tr>" for item in value["measurements"]) + "</tbody></table></div>")
    html += "</section>"
    if diagnostics is not None:
        html += (history.time_use_html(resource_diagnostics.time_use(diagnostics, value)) +
                 resource_diagnostics.render(diagnostics, value))
    require(len(html.encode()) <= MAX_PUBLIC_HTML, "rendered public HTML exceeds bound")
    return html
