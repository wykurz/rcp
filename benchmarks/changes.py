"""Derive advisory changes from validated, immutable benchmark records."""

import datetime as dt
import html
import math
import re


POLICY = "previous-compatible-terminal-clean-run-v1"


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
    if run["revision"]["dirty"] is not False or not run["revision"].get("commit"):
        return "unqualified-revision"
    return None


def identity(run, summary):
    # series IDs already bind workload, tools, environment and measurement contracts
    return (run["context"]["repository"], summary["series_id"], summary["case_id"], summary["variant_id"])


def point(run, summary, sources):
    return {
        "run_id": run["run_id"], "timestamp": run["timestamp"], "run_status": run["status"],
        "revision": run["revision"], "sources": sources[run["run_id"]],
        "median_seconds": summary["median"], "minimum_seconds": summary["minimum"],
        "maximum_seconds": summary["maximum"], "samples_seconds": summary["samples"],
        "trial_indices": [index for index, trial in enumerate(run["trials"])
                          if trial["case_id"] == summary["case_id"] and trial["variant_id"] == summary["variant_id"]],
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
                "series_id": summary["series_id"], "current": current,
                "reference": None, "ratio": None, "change_percent": None,
                "status": reason or "no-compatible-reference",
                "short_sample": summary["minimum"] < 10,
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
                    if math.isfinite(ratio) and math.isfinite(change):
                        row.update(status="compared", ratio=ratio, change_percent=change)
                    else:
                        row["status"] = "unrepresentable-ratio"
                    row["short_sample"] |= baseline["minimum"] < 10
            rows.append(row)
            if reason is None:
                previous.setdefault(key, []).append((run, summary))
    return {"schema_version": 1, "comparison_policy": POLICY, "acceptance_evaluated": False,
            "ratio_direction": "current_median / reference_median; above 1 is slower",
            "runs": [{"run_id": run["run_id"], "timestamp": run["timestamp"], "status": run["status"],
                      "exclusion": exclusion(run), "completed_summaries": len(run["summaries"]),
                      "sources": sources[run["run_id"]]} for run in ordered], "changes": rows}


def cell(value):
    # result metadata is data, including when rendered as a CI Markdown summary
    return html.escape(str(value), quote=True).replace("|", "&#124;").replace("\n", " ").replace("\r", " ").replace("`", "&#96;").replace("[", "&#91;").replace("]", "&#93;")


def markdown(evidence, runs, limit=10):
    """Keep CI summaries bounded; the JSON retains every observation and input hash."""
    lines = ["# Benchmark changes", "",
             "Advisory comparison with the previous strictly compatible historical run. "
             "Above 1x is slower. These are unpaired observations on different runners/times, "
             "not regression verdicts or timing gates. Repeat ranges are not confidence intervals.", "",
             "Full provenance, raw repeats, reference run IDs and original input SHA256 hashes are "
             "in changes.json; commands, binary hashes and scoped timings are in history.json "
             "and the dashboard's View context tables.", ""]
    selected = evidence["runs"][-limit:]
    if len(evidence["runs"]) > limit:
        lines += [f"Showing the latest {limit} runs; changes.json retains all {len(evidence['runs'])} runs.", ""]
    by_id = {run["run_id"]: run for run in runs}
    for item in reversed(selected):
        run = by_id[item["run_id"]]
        context = run["context"]
        lines += [f"## {cell(run['timestamp'])} — {cell(context['topology'])}", "",
                  f"Run [{run['run_id']}](index.html#run-{run['run_id']}) · {cell(run['status'])} · revision `{cell(run['revision'].get('commit') or 'unknown')}` · "
                  f"{cell(context['runner_label'])} · {cell(context['cache_policy'])}", ""]
        if item["exclusion"]:
            lines += [f"Excluded from historical comparisons: {item['exclusion']}.", ""]
        if run["status"] != "complete":
            lines += ["Triage the producer failure/interruption first. Only fully validated, completed cases can have comparisons.", ""]
        rows = [row for row in evidence["changes"] if row["current"]["run_id"] == run["run_id"]]
        if not rows:
            lines += ["No completed case measurements. Inspect producer logs and the raw record.", ""]
            continue
        lines += ["| Case / variant | Current median [range] s | Reference median [range] s | Change | Reference run | Evidence |",
                  "| --- | --- | --- | --- | --- | --- |"]
        for row in rows:
            current, reference = row["current"], row["reference"]
            def measurement(item):
                return f"{item['median_seconds']:.3f} [{item['minimum_seconds']:.3f}–{item['maximum_seconds']:.3f}] ({len(item['samples_seconds'])} repeats)"
            trials = [run["trials"][index] for index in current["trial_indices"]]
            has_timings = any(trial.get("timings", {}).get("reports") for trial in trials)
            notes = [row["status"], "scoped timings available" if has_timings else "no scoped timings"]
            if row["short_sample"]:
                notes.append("repeat under 10 s")
            if reference and reference["run_status"] != "complete":
                notes.append("reference case from failed run")
            change = f"{row['ratio']:.3f}x ({row['change_percent']:+.1f}%)" if row["ratio"] is not None else "—"
            reference_link = f"[{reference['run_id']}](index.html#run-{reference['run_id']})" if reference else "—"
            lines.append(f"| {cell(row['case_id'])} / {cell(row['variant_id'])} | {measurement(current)} | "
                         f"{measurement(reference) if reference else '—'} | {change} | "
                         f"{reference_link} | {'; '.join(notes)} |")
        lines += ["", "For a concerning change, inspect all tool rows and repeat ranges in the same workload. "
                  "Confirm with paired candidate/base samples before attributing it to code. Use the recorded "
                  "case, series, revisions and trial indices to choose one bounded diagnostic capture; "
                  "inspect elapsed scopes by role rather than treating their overlapping totals as CPU time.", ""]
    return "\n".join(lines) + "\n"
