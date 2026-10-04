"""Derive advisory changes from validated, immutable benchmark records."""

import datetime as dt
import html
import math
import re
from urllib.parse import urlsplit


POLICY = "previous-compatible-terminal-clean-run-v1"
COMMIT = re.compile(r"[0-9a-f]{7,64}\Z")
INTRO = ("Advisory comparison with the previous strictly compatible historical run. "
         "Current median / reference median: above 1x is slower. These are unpaired observations "
         "on different runners/times, not regression verdicts or timing gates. Repeat ranges are "
         "not confidence intervals. Same-checkout comparisons do not establish identical binaries.")
TRIAGE = ("For a concerning change, inspect all tool rows, binary hashes and repeat ranges in the "
          "same workload. Confirm with paired candidate/base samples before attributing it to code. "
          "Use the recorded case, series, revisions and trial indices to choose one bounded diagnostic "
          "capture; inspect elapsed scopes by role rather than treating their overlapping totals as CPU time.")


def timestamp(run):
    return dt.datetime.fromisoformat(run["timestamp"].replace("Z", "+00:00"))


def exclusion(run):
    if run["context"].get("purpose", "performance") != "performance":
        return "not-performance"
    if run["status"] == "running":
        return "producer-running"
    repository = run["context"].get("repository")
    if not isinstance(repository, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        return "unqualified-repository"
    commit = run["revision"].get("commit")
    if run["revision"]["dirty"] is not False or not isinstance(commit, str) or not COMMIT.fullmatch(commit):
        return "unqualified-revision"
    return None


def identity(run, summary):
    # series IDs already bind workload, tools, environment and measurement contracts
    return (run["context"]["repository"], summary["series_id"], summary["case_id"], summary["variant_id"])


def fingerprints(sources):
    # input order and filenames are not durable identities; exact content hashes are
    return [{"artifact_id": "sha256:" + digest, "sha256": digest} for digest in sorted({source["sha256"] for source in sources})]


def point(run, summary, sources):
    indices = [index for index, trial in enumerate(run["trials"])
               if trial["case_id"] == summary["case_id"] and trial["variant_id"] == summary["variant_id"]]
    return {
        "run_id": run["run_id"], "timestamp": run["timestamp"], "run_status": run["status"],
        "revision": run["revision"], "sources": fingerprints(sources[run["run_id"]]),
        "median_seconds": summary["median"], "minimum_seconds": summary["minimum"],
        "maximum_seconds": summary["maximum"], "samples_seconds": summary["samples"],
        "trial_indices": indices, "case_order": [case["id"] for case in run["cases"]],
        "timing_statuses": sorted({run["trials"][index].get("timings", {}).get("status", "missing-legacy") for index in indices}),
    }


def build(runs, sources):
    """Compare only strictly earlier compatible observations; never evaluate acceptance."""
    ordered = sorted(runs, key=lambda run: (timestamp(run), run["run_id"]))
    previous = {}
    rows = []
    for run in ordered:
        reason = exclusion(run)
        for summary in run["summaries"]:
            key = identity(run, summary) if reason is None else None
            current = point(run, summary, sources)
            row = {
                "case_id": summary["case_id"], "variant_id": summary["variant_id"],
                "series_id": summary["series_id"], "repository": run["context"].get("repository"), "current": current,
                "reference": None, "ratio": None, "change_percent": None,
                "status": reason or "no-compatible-reference",
                "current_short_sample": reason is None and summary["minimum"] < 10,
                "reference_short_sample": False,
            }
            # tied timestamps cannot establish an order, and an ambiguous latest reference
            # must not silently turn into a comparison against an older convenient sample
            candidates = [item for item in previous.get(key, []) if timestamp(item[0]) < timestamp(run)]
            if reason is None and candidates:
                latest = max(timestamp(item[0]) for item in candidates)
                matches = [item for item in candidates if timestamp(item[0]) == latest]
                if len(matches) != 1:
                    row["status"] = "ambiguous-reference"
                else:
                    before, baseline = matches[0]
                    row["reference"] = point(before, baseline, sources)
                    ratio = summary["median"] / baseline["median"] if baseline["median"] > 0 and summary["median"] > 0 else math.nan
                    change = (ratio - 1) * 100
                    if ratio > 0 and math.isfinite(ratio) and math.isfinite(change):
                        row.update(status="compared", ratio=ratio, change_percent=change)
                    else:
                        row["status"] = "unrepresentable-ratio"
                    row["reference_short_sample"] = baseline["minimum"] < 10
            rows.append(row)
            if reason is None:
                previous.setdefault(key, []).append((run, summary))
    return {"schema_version": 1, "comparison_policy": POLICY, "acceptance_evaluated": False,
            "ratio_direction": "current_median / reference_median; above 1 is slower",
            "runs": [{"run_id": run["run_id"], "timestamp": run["timestamp"], "status": run["status"],
                      "revision": run["revision"], "repository": run["context"].get("repository"),
                      "topology": run["context"]["topology"], "runner_label": run["context"]["runner_label"],
                      "cache_policy": run["context"]["cache_policy"],
                      "case_order": [case["id"] for case in run["cases"]],
                      "exclusion": exclusion(run), "completed_summaries": len(run["summaries"]),
                      "sources": fingerprints(sources[run["run_id"]])} for run in ordered], "changes": rows}


def literal(value):
    # HTML block tables avoid GFM pipe/backslash interactions; code also suppresses linkification
    return "<code>" + html.escape(str(value), quote=True).replace("\n", "&#10;").replace("\r", "&#13;") + "</code>"


def link_prefix(base):
    if base is None or base == "":
        return base
    parsed = urlsplit(base)
    if (parsed.scheme not in ("https", "http") or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or parsed.query or parsed.fragment
            or any(character.isspace() or ord(character) < 32 for character in base)):
        raise ValueError("report link base must be an absolute HTTP(S) URL without credentials, query or fragment")
    return base.rstrip("/") + "/"


def run_link(item, prefix):
    label = literal(item["run_id"])
    if prefix is None:
        return label
    target = html.escape(prefix + "index.html#run-" + item["run_id"], quote=True)
    return f'<a href="{target}">{label}</a>'


def revision(item):
    commit = item["revision"].get("commit")
    label = commit[:10] if isinstance(commit, str) and COMMIT.fullmatch(commit) else commit or "unknown"
    return literal(label)


def measurement(item):
    return f"{item['median_seconds']:.3f} [{item['minimum_seconds']:.3f}–{item['maximum_seconds']:.3f}] ({len(item['samples_seconds'])} repeats)"


def notes(row):
    current, reference = row["current"], row["reference"]
    result = [row["status"], "current timings: " + ", ".join(current["timing_statuses"])]
    if reference:
        result.append("reference timings: " + ", ".join(reference["timing_statuses"]))
        if current["revision"]["commit"] == reference["revision"]["commit"]:
            result.append("same recorded checkout revision")
        if reference["run_status"] != "complete":
            result.append("reference case from failed run")
        if current["case_order"] != reference["case_order"]:
            result.append("case composition/order differs")
        if len(current["samples_seconds"]) != len(reference["samples_seconds"]):
            result.append("repeat count differs")
    for side in ("current", "reference"):
        if row[side + "_short_sample"]:
            result.append(side + " repeat under 10 s")
    return "; ".join(result)


def markdown(evidence, limit=10, link_base=""):
    """Render one HTML fragment for both GFM job summaries and the standalone HTML page."""
    if limit is not None and limit < 0:
        raise ValueError("summary limit must be nonnegative")
    prefix = link_prefix(link_base)
    selected = evidence["runs"] if limit is None else evidence["runs"][-limit:] if limit else []
    lines = ["<h1>Benchmark changes</h1>", f"<p>{INTRO}</p>",
             "<p>changes.json retains full provenance, raw repeats, reference run IDs and original input SHA256 hashes. "
             "history.json and the dashboard's View context tables retain commands, binary hashes and scoped timings.</p>",
             "<p>References are recomputed from the available history by producer start time. Late-arriving records "
             "can change previous comparisons. Case composition/order and repeat counts are shown but are not part "
             "of compatibility identity.</p>"]
    if prefix is None:
        lines.append("<p>Pages URL unavailable; run IDs are shown without links. Inspect the generated report artifact.</p>")
    if len(selected) < len(evidence["runs"]):
        lines.append(f"<p>Showing the latest {limit} runs; changes.json retains all {len(evidence['runs'])} runs.</p>")
    for item in reversed(selected):
        lines += [f"<h2>{literal(item['timestamp'])} — {literal(item['topology'])}</h2>",
                  f"<p>Run {run_link(item, prefix)} · {literal(item['status'])} · checkout revision {revision(item)} · "
                  f"{literal(item['repository'])} · {literal(item['runner_label'])} · {literal(item['cache_policy'])}</p>",
                  "<p>Recorded case order: " + ", ".join(literal(case) for case in item["case_order"]) + "</p>"]
        if item["exclusion"]:
            lines.append(f"<p>Excluded from historical comparisons: {literal(item['exclusion'])}.</p>")
        if item["status"] != "complete":
            lines.append("<p>Triage the producer failure/interruption first. Only fully validated, completed cases can have comparisons.</p>")
        rows = [row for row in evidence["changes"] if row["current"]["run_id"] == item["run_id"]]
        if not rows:
            lines.append("<p>No completed case measurements. Inspect producer logs and the raw record.</p>")
            continue
        lines.append("<table><thead><tr><th>Case / variant</th><th>Current median [range] s</th>"
                     "<th>Reference median [range] s</th><th>Change</th><th>Reference run / checkout / date</th><th>Evidence</th></tr></thead><tbody>")
        for row in rows:
            current, reference = row["current"], row["reference"]
            change = "—"
            if row["ratio"] is not None:
                percent = row["change_percent"] if round(row["change_percent"], 1) else 0.0
                ratio = format(row["ratio"], ".3f" if row["ratio"] >= 0.0005 else ".3g")
                change = f"{ratio}x ({percent:+.1f}%)"
            reference_label = (f"{run_link(reference, prefix)}<br>checkout {revision(reference)}<br>"
                               f"{literal(reference['timestamp'])}") if reference else "—"
            cells = [literal(row["case_id"]) + " / " + literal(row["variant_id"]), measurement(current),
                     measurement(reference) if reference else "—", change, reference_label, literal(notes(row))]
            lines.append("<tr>" + "".join(f"<td>{value}</td>" for value in cells) + "</tr>")
        lines.append("</tbody></table>")
    lines.append(f"<p>{TRIAGE}</p>")
    return "\n".join(lines) + "\n"


def html_report(evidence):
    return ('<!doctype html>\n<html lang="en"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            '<title>Benchmark changes</title><style>'
            'body{font:16px/1.5 system-ui,sans-serif;margin:2rem;color:#182d36;background:#f5f7f5}'
            'a{color:#086b70}table{border-collapse:collapse;display:block;overflow-x:auto;background:white}'
            'th,td{padding:.6rem;border:1px solid #d8e3df;text-align:left;vertical-align:top}'
            'code{white-space:pre-wrap;overflow-wrap:anywhere}p{max-width:100ch}'
            '</style></head><body><nav><a href="index.html">History dashboard</a> · '
            '<a href="changes.json">Comparison JSON</a> · <a href="changes.md" download>Job summary</a></nav>\n'
            + markdown(evidence, limit=None) + '</body></html>\n')
