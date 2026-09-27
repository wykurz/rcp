"""Validate benchmark results and render a standalone history dashboard."""

import argparse
import datetime as dt
import json
import math
import re
import statistics
from pathlib import Path


RUN_ID = re.compile(r"[0-9a-f]{32}\Z")
SERIES_ID = re.compile(r"[0-9a-f]{64}\Z")


def _object(value, name):
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def _text(value, name):
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a nonempty string")
    return value


def _number(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be a finite nonnegative number")
    return value


def _list(value, name):
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array")
    return value


def _reject_constant(value):
    raise ValueError(f"invalid JSON number: {value}")


def _finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"nonfinite JSON number: {value}")
    return number


def _unique_pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def parse_result(text):
    """Parse strict JSON and validate one schema-one result."""
    return validate_result(json.loads(text, parse_constant=_reject_constant, parse_float=_finite_float, object_pairs_hook=_unique_pairs))


def validate_result(value):
    """Return a schema-one result, or raise ValueError with its invalid field."""
    run = _object(value, "result")
    if type(run.get("schema_version")) is not int or run["schema_version"] != 1:
        raise ValueError("schema_version must be 1")
    if not RUN_ID.fullmatch(_text(run.get("run_id"), "run_id")):
        raise ValueError("run_id must be 32 lowercase hexadecimal characters")
    stamp = _text(run.get("timestamp"), "timestamp")
    try:
        parsed = dt.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("timestamp must be ISO 8601") from error
    if parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
        raise ValueError("timestamp must be UTC")
    if run.get("status") not in ("running", "complete", "failed"):
        raise ValueError("status must be running, complete, or failed")
    revision = _object(run.get("revision"), "revision")
    if revision.get("commit") is not None:
        _text(revision["commit"], "revision.commit")
    if revision.get("branch") is not None:
        _text(revision["branch"], "revision.branch")
    if "dirty" not in revision or (revision["dirty"] is not None and not isinstance(revision["dirty"], bool)):
        raise ValueError("revision.dirty must be a boolean or null")
    context = _object(run.get("context"), "context")
    for field in ("runner_label", "topology", "cache_policy", "timing_policy"):
        _text(context.get(field), f"context.{field}")
    for field in ("source", "destination"):
        if field not in context or not isinstance(context[field], (str, dict)):
            raise ValueError(f"context.{field} must be a string or object")
    _object(run.get("tools"), "tools")
    case_ids = set()
    for i, case in enumerate(_list(run.get("cases"), "cases")):
        case_id = _text(_object(case, f"cases[{i}]").get("id"), f"cases[{i}].id")
        if case_id in case_ids:
            raise ValueError(f"duplicate case id: {case_id}")
        case_ids.add(case_id)
    variant_ids = set()
    for i, variant in enumerate(_list(run.get("variants"), "variants")):
        variant_id = _text(_object(variant, f"variants[{i}]").get("id"), f"variants[{i}].id")
        if variant_id in variant_ids:
            raise ValueError(f"duplicate variant id: {variant_id}")
        variant_ids.add(variant_id)
    if run["status"] == "complete" and (not case_ids or not variant_ids):
        raise ValueError("cases and variants must be nonempty")
    for i, trial in enumerate(_list(run.get("trials"), "trials")):
        trial = _object(trial, f"trials[{i}]")
        if trial.get("case_id") not in case_ids or trial.get("variant_id") not in variant_ids:
            raise ValueError(f"trials[{i}] references an unknown case or variant")
        if type(trial.get("iteration")) is not int or trial["iteration"] < 1:
            raise ValueError(f"trials[{i}].iteration must be a positive integer")
        if trial.get("status") not in ("ok", "failed", "running"):
            raise ValueError(f"trials[{i}].status must be ok, failed, or running")
        if trial.get("elapsed_seconds") is not None:
            _number(trial["elapsed_seconds"], f"trials[{i}].elapsed_seconds")
            if trial["status"] == "ok" and trial["elapsed_seconds"] == 0:
                raise ValueError(f"trials[{i}].elapsed_seconds must be positive for an ok trial")
        elif trial["status"] == "ok":
            raise ValueError(f"trials[{i}].elapsed_seconds is required for an ok trial")
        if not isinstance(trial.get("exit_codes"), (list, dict)):
            raise ValueError(f"trials[{i}].exit_codes must be an array or object")
        if trial["status"] == "ok" and (not isinstance(trial["exit_codes"], list) or not trial["exit_codes"] or any(type(code) is not int or code != 0 for code in trial["exit_codes"])):
            raise ValueError(f"trials[{i}].exit_codes must contain only zero child exits for an ok trial")
        _list(trial.get("commands"), f"trials[{i}].commands")
        _object(trial.get("validation"), f"trials[{i}].validation")
        if not isinstance(trial["validation"].get("ok"), bool):
            raise ValueError(f"trials[{i}].validation.ok must be a boolean")
        if trial["status"] == "ok" and not trial["validation"]["ok"]:
            raise ValueError(f"trials[{i}] is ok without passing validation")
        if not isinstance(trial.get("logs"), (list, dict)):
            raise ValueError(f"trials[{i}].logs must be an array or object")
    for i, summary in enumerate(_list(run.get("summaries"), "summaries")):
        summary = _object(summary, f"summaries[{i}]")
        if not SERIES_ID.fullmatch(_text(summary.get("series_id"), f"summaries[{i}].series_id")):
            raise ValueError(f"summaries[{i}].series_id must be a SHA256 hex digest")
        if summary.get("case_id") not in case_ids or summary.get("variant_id") not in variant_ids:
            raise ValueError(f"summaries[{i}] references an unknown case or variant")
        if summary.get("unit") != "seconds":
            raise ValueError(f"summaries[{i}].unit must be seconds")
        for field in ("median", "minimum", "maximum", "stdev", "files_per_second"):
            _number(summary.get(field), f"summaries[{i}].{field}")
        samples = _list(summary.get("samples"), f"summaries[{i}].samples")
        if not samples:
            raise ValueError(f"summaries[{i}].samples must be nonempty")
        for j, sample in enumerate(samples):
            _number(sample, f"summaries[{i}].samples[{j}]")
        if not summary["minimum"] <= summary["median"] <= summary["maximum"]:
            raise ValueError(f"summaries[{i}] minimum, median, maximum are inconsistent")
        if min(samples) < summary["minimum"] - 1e-9 or max(samples) > summary["maximum"] + 1e-9:
            raise ValueError(f"summaries[{i}] samples fall outside minimum/maximum")
    summarized = set()
    for i, summary in enumerate(run["summaries"]):
        pair = (summary["case_id"], summary["variant_id"])
        if pair in summarized:
            raise ValueError(f"summaries[{i}] duplicates a case and variant")
        summarized.add(pair)
        matching = [trial for trial in run["trials"] if (trial["case_id"], trial["variant_id"]) == pair]
        if not matching or any(trial["status"] != "ok" for trial in matching):
            raise ValueError(f"summaries[{i}].samples require only successful matching trials")
        samples = [trial["elapsed_seconds"] for trial in matching]
        if samples != summary["samples"]:
            raise ValueError(f"summaries[{i}].samples do not match successful trials")
        for name, expected in (("minimum", min(samples)), ("maximum", max(samples)), ("median", statistics.median(samples)), ("stdev", statistics.stdev(samples) if len(samples) > 1 else 0.0)):
            if not math.isclose(summary[name], expected, rel_tol=1e-9, abs_tol=1e-12):
                raise ValueError(f"summaries[{i}].{name} does not match successful trials")
    for case_id in {case_id for case_id, _ in summarized}:
        if any((case_id, variant_id) not in summarized for variant_id in variant_ids):
            raise ValueError(f"summaries contain an incomplete case: {case_id}")
        repetitions = None
        for variant_id in variant_ids:
            iterations = sorted(trial["iteration"] for trial in run["trials"] if (trial["case_id"], trial["variant_id"]) == (case_id, variant_id))
            if iterations != list(range(1, len(iterations) + 1)) or (repetitions is not None and iterations != repetitions):
                raise ValueError(f"summaries for {case_id} have incomplete repetitions")
            repetitions = iterations
    if run["status"] == "complete":
        if not run["trials"] or not run["summaries"]:
            raise ValueError("complete run requires trials and summaries")
        if any(trial["status"] != "ok" for trial in run["trials"]):
            raise ValueError("complete run contains an unfinished or failed trial")
        if summarized != {(case_id, variant_id) for case_id in case_ids for variant_id in variant_ids}:
            raise ValueError("summaries must cover every selected case and variant")
    if run.get("error") is not None and not isinstance(run["error"], str):
        raise ValueError("error must be a string")
    return run


def load_results(path):
    """Load one result or an immutable history tree, deduplicated by run ID."""
    if path.is_file():
        files = [path]
    elif (path / "results.json").is_file():
        files = [path / "results.json"]
    elif (path / "runs").is_dir():
        files = sorted((path / "runs").glob("*.json"))
    else:
        raise ValueError(f"input is not a result or history directory: {path}")
    if not files:
        raise ValueError(f"history contains no JSON runs: {path}")
    by_id = {}
    for file in files:
        try:
            run = parse_result(file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, ValueError) as error:
            raise ValueError(f"{file}: {error}") from error
        earlier = by_id.get(run["run_id"])
        if earlier is not None and earlier != run:
            raise ValueError(f"duplicate run_id {run['run_id']} has conflicting data")
        by_id[run["run_id"]] = run
    return sorted(by_id.values(), key=lambda run: (dt.datetime.fromisoformat(run["timestamp"].replace("Z", "+00:00")), run["run_id"]))


def render(input_path, output):
    """Write history.json and a self-contained dashboard after validation."""
    runs = load_results(input_path)
    history = {"schema_version": 1, "runs": runs}
    embedded = json.dumps(history, ensure_ascii=False, separators=(",", ":"))
    for literal, escape in (("&", "\\u0026"), ("<", "\\u003c"), (">", "\\u003e"), ("\u2028", "\\u2028"), ("\u2029", "\\u2029")):
        embedded = embedded.replace(literal, escape)
    template = Path(__file__).with_name("dashboard.html").read_text(encoding="utf-8")
    page = template.replace("__HISTORY_JSON__", embedded)
    output.mkdir(parents=True, exist_ok=True)
    (output / "history.json").write_text(json.dumps(history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "index.html").write_text(page, encoding="utf-8")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Render benchmark history as a standalone dashboard")
    parser.add_argument("input", type=Path, help="results.json, a result directory, or history with runs/*.json")
    parser.add_argument("--output", required=True, type=Path, help="site output directory")
    args = parser.parse_args(argv)
    try:
        render(args.input, args.output)
    except (OSError, ValueError) as error:
        parser.exit(1, f"benchmark report: {error}\n")


if __name__ == "__main__":
    main()
