"""Validate benchmark results and render a standalone history dashboard."""

import argparse
import datetime as dt
import hashlib
import json
import math
import re
import statistics
import tempfile
from pathlib import Path

from benchmarks.strict_json import parse_json
from benchmarks.timings import require_roles, validate_report
from benchmarks import operations, transport, sanitized, changes, pairs, measurements, clocks


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
    if not measurements.nonnegative(value):
        raise ValueError(f"{name} must be a finite nonnegative number")
    return value


def _list(value, name):
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array")
    return value


def parse_result(text):
    """Parse strict JSON and validate one schema-one result."""
    return validate_result(parse_json(text))


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
    if "timing_request" in context and context["timing_request"] not in ("automatic", "disabled"):
        raise ValueError("context.timing_request must be automatic or disabled")
    for field in ("source", "destination"):
        if field not in context or not isinstance(context[field], (str, dict)):
            raise ValueError(f"context.{field} must be a string or object")
    if "ssh_transport_profile" in context:
        if context["topology"] != "loopback":
            raise ValueError("context.ssh_transport_profile requires loopback topology")
        profile = context["ssh_transport_profile"]
        if profile is not None and (not isinstance(profile, str) or not profile.strip()):
            raise ValueError("context.ssh_transport_profile must be a nonblank string or null")
    cases = _list(run.get("cases"), "cases")
    trials = _list(run.get("trials"), "trials")
    operation_revision = context.get("operation_contract_revision")
    if operation_revision is None and (context["cache_policy"] == "source-verified" or any("mode" in case for case in cases if isinstance(case, dict)) or any("operation" in trial for trial in trials if isinstance(trial, dict))):
        raise ValueError("context.operation_contract_revision is required for operation/cache records")
    if operation_revision is not None and (type(operation_revision) is not int or operation_revision != operations.CONTRACT_REVISION):
        raise ValueError("unsupported context.operation_contract_revision")
    if operation_revision is not None and context.get("summary_locale") != operations.SUMMARY_LOCALE:
        raise ValueError("context.summary_locale must be C for the operation contract")
    if operation_revision is not None and context["cache_policy"] == "source-verified":
        if type(context.get("cache_contract_revision")) is not int or context["cache_contract_revision"] != operations.CACHE_REVISION:
            raise ValueError("unsupported context.cache_contract_revision")
    _object(run.get("tools"), "tools")
    case_ids = set()
    cases_by_id = {}
    for i, case in enumerate(cases):
        case_id = _text(_object(case, f"cases[{i}]").get("id"), f"cases[{i}].id")
        if case_id in case_ids:
            raise ValueError(f"duplicate case id: {case_id}")
        case_ids.add(case_id)
        cases_by_id[case_id] = case
        if operation_revision is not None:
            operations.validate_case(case)
    variant_ids = set()
    variants_by_id = {}
    for i, variant in enumerate(_list(run.get("variants"), "variants")):
        variant_id = _text(_object(variant, f"variants[{i}]").get("id"), f"variants[{i}].id")
        if variant_id in variant_ids:
            raise ValueError(f"duplicate variant id: {variant_id}")
        variant_ids.add(variant_id)
        variants_by_id[variant_id] = variant
        if operation_revision is not None:
            if variant.get("tool") not in ("rcp", "rsync", "cp") or type(variant.get("processes")) is not int or variant["processes"] < 1 or not isinstance(variant.get("args"), list) or any(not isinstance(arg, str) or not arg for arg in variant["args"]):
                raise ValueError(f"variants[{i}] has invalid tool/processes/args")
    owned = context.get("owned_transport")
    if owned is None and ("owned_transport" in context or context.get("ssh_transport_profile") == transport.PROFILE or any(isinstance(trial, dict) and ("transport" in trial or "transport_artifacts" in trial) for trial in trials)):
        raise ValueError("context.owned_transport is required for owned profile/trial records")
    if owned is not None:
        try:
            expected_owned = transport.semantics(owned["requested_rtt_ms"])
            previous_owned = {key:value for key,value in expected_owned.items() if key != "account_policy"}
            failed_previous = owned == previous_owned and run["status"] == "failed" and all(trial.get("status") != "ok" for trial in trials)
            if context["topology"] != "loopback" or (owned != expected_owned and not failed_previous):
                raise ValueError("owned context must declare its exact per-trial semantics")
            if operation_revision is None or context.get("ssh_transport_profile") != transport.PROFILE:
                raise ValueError("owned context requires the operation contract and owned SSH profile")
            transport.validate_request(context["topology"], owned["requested_rtt_ms"], list(variants_by_id.values()), {})
        except (KeyError, TypeError) as error:
            raise ValueError("malformed context.owned_transport") from error
    policies = context.get("timing_collection")
    if policies is not None:
        _object(policies, "context.timing_collection")
        if policies.keys() - variant_ids:
            raise ValueError("context.timing_collection references unknown variants")
        for variant_id, policy in policies.items():
            if policy not in ("coarse", "unsupported", "disabled", "not_applicable"):
                raise ValueError(f"context.timing_collection.{variant_id} has invalid policy")
        if run["status"] == "complete" and policies.keys() != variant_ids:
            raise ValueError("context.timing_collection must cover every selected variant")
    elif context.get("timing_request") is not None and run.get("trials"):
        raise ValueError("context.timing_collection is required for new trials")
    capabilities = context.get("timing_capability")
    if capabilities is not None:
        _object(capabilities, "context.timing_capability")
        if capabilities.keys() - variant_ids or any(type(value) is not bool for value in capabilities.values()):
            raise ValueError("context.timing_capability must map known variants to booleans")
    if run["status"] == "complete" and (not case_ids or not variant_ids):
        raise ValueError("cases and variants must be nonempty")
    for i, trial in enumerate(trials):
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
        if operation_revision is not None:
            _validate_operation_trial(trial, cases_by_id[trial["case_id"]], variants_by_id[trial["variant_id"]], context)
        if owned is not None and trial["status"] == "ok":
            case = cases_by_id[trial["case_id"]]
            counts = operations.expected_counts(case)
            transport.validate_transport(trial.get("transport"), variants_by_id[trial["variant_id"]],
                run["tools"], counts, case.get("mode", "fresh"), owned)
        if not isinstance(trial.get("logs"), (list, dict)):
            raise ValueError(f"trials[{i}].logs must be an array or object")
        if policies is not None and "timings" not in trial:
            raise ValueError(f"trials[{i}].timings is required by context.timing_collection")
        if "timings" in trial:
            timing = _object(trial["timings"], f"trials[{i}].timings")
            if set(timing) != {"status", "reports"}:
                raise ValueError(f"trials[{i}].timings has invalid fields")
            if policies is None or timing["status"] != policies.get(trial["variant_id"]):
                raise ValueError(f"trials[{i}].timings.status does not match collection policy")
            reports = _list(timing["reports"], f"trials[{i}].timings.reports")
            role_count = 3 if context["topology"] == "loopback" else 1
            if len(reports) > len(trial["commands"]) * role_count:
                raise ValueError(f"trials[{i}].timings.reports exceeds command role count")
            for j, item in enumerate(reports):
                try:
                    validate_report(item)
                except ValueError as error:
                    raise ValueError(f"trials[{i}].timings.reports[{j}]: {error}") from error
            if timing["status"] != "coarse" and reports:
                raise ValueError(f"trials[{i}].timings.reports must be empty when timings are unavailable")
            if timing["status"] == "coarse" and trial["status"] == "ok" and not reports:
                raise ValueError(f"trials[{i}].timings.reports missing for successful trial")
            if timing["status"] == "coarse" and trial["status"] == "ok" and any(not item["scopes"] or not any(scope["name"] == "operation" for scope in item["scopes"]) for item in reports):
                raise ValueError(f"trials[{i}].timings.reports missing operation scope")
            if timing["status"] == "coarse":
                if variants_by_id[trial["variant_id"]].get("tool") != "rcp":
                    raise ValueError(f"trials[{i}].timings.coarse requires rcp")
                roles = ["rcp-master"] * len(trial["commands"])
                if context["topology"] == "loopback":
                    roles += ["rcpd-source"] * len(trial["commands"])
                    roles += ["rcpd-destination"] * len(trial["commands"])
                try:
                    require_roles(reports, roles, successful=trial["status"] == "ok")
                except ValueError as error:
                    raise ValueError(f"trials[{i}].timings.reports: {error}") from error
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
    pairs.validate(run)
    measurements.validate(run)
    clocks.validate(run)
    return run


def _validate_operation_trial(trial, case, variant, context):
    operation = case.get("mode", "fresh")
    operations.validate_variant(variant, operation)
    child_locale = measurements.child_locale(variant, context.get("local_resources") is not None)
    if trial.get("child_locale") != child_locale:
        raise ValueError("trial.child_locale differs from exact-summary locale policy")
    if trial.get("operation") != operation:
        raise ValueError("trial.operation differs from case mode")
    counts = operations.expected_counts(case)
    directories, files = counts["directories"], counts["files"]
    expected = operations.transfer_counts(operation, counts)
    value = trial.get("expected_transfer")
    if value != expected or not isinstance(value, dict) or any(type(item) is not int for item in value.values()):
        raise ValueError("trial.expected_transfer differs from operation counts")
    if trial["status"] != "ok":
        return

    def proof(field):
        value = _object(trial.get(field), f"trial.{field}")
        if value.get("ok") is not True:
            raise ValueError(f"trial.{field} is not a successful proof")
        return value

    def metadata(value, field, timestamps=False):
        expected_fields = ["mode", "uid", "gid", "mtime_ns"] if timestamps else ["mode"]
        if value.get("ok") is not True or type(value.get("entries")) is not int or value["entries"] != directories + files + 1 or value.get("checked_fields") != expected_fields:
            raise ValueError(f"trial.{field} lacks required metadata proof")

    def exact_counts(value):
        return isinstance(value, dict) and value == counts and all(type(item) is int for item in value.values())

    def content(value, field):
        if not exact_counts(value.get("counts")) or value.get("digest") != case.get("fixture_digest") or not isinstance(value.get("digest"), str) or not SERIES_ID.fullmatch(value["digest"]):
            raise ValueError(f"trial.{field} lacks exact source content proof")

    content(proof("validation"), "validation")
    if operation != "fresh":
        seed = proof("seed_validation")
        if not exact_counts(seed.get("counts")) or type(seed.get("stale_files")) is not int or seed["stale_files"] != expected["files_copied"] or seed.get("independent_files") is not True or seed.get("exact_seed_mtimes") is not True or not isinstance(seed.get("digest"), str) or not SERIES_ID.fullmatch(seed["digest"]):
            raise ValueError("trial.seed_validation lacks exact seed proof")
        metadata(_object(seed.get("modes"), "trial.seed_validation.modes"), "seed_validation.modes")
    if operations.summary_supported(variant):
        summary = trial.get("copy_summary")
        if summary != expected or not isinstance(summary, dict) or any(type(item) is not int for item in summary.values()):
            raise ValueError("trial.copy_summary differs from operation counts")
        metadata(proof("metadata_validation"), "metadata_validation")
    if context["cache_policy"] == "source-verified":
        cache = proof("cache_validation")
        content(cache, "cache_validation")
        metadata(_object(cache.get("metadata"), "trial.cache_validation.metadata"), "cache_validation.metadata", timestamps=True)
    if operation != "fresh" or operations.summary_supported(variant) or context["cache_policy"] == "source-verified":
        source = proof("source_validation")
        content(source, "source_validation")
        metadata(_object(source.get("metadata"), "trial.source_validation.metadata"), "source_validation.metadata", timestamps=True)


def load_result_records(path):
    """Validate/deduplicate runs and retain exact original input fingerprints."""
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
    by_id, sources = {}, {}
    for ordinal, file in enumerate(files):
        try:
            original = file.read_bytes()
            run = parse_result(original.decode("utf-8"))
        except (OSError, json.JSONDecodeError, ValueError) as error:
            raise ValueError(f"{file}: {error}") from error
        earlier = by_id.get(run["run_id"])
        if earlier is not None and earlier != run:
            raise ValueError(f"duplicate run_id {run['run_id']} has conflicting data")
        by_id[run["run_id"]] = run
        sources.setdefault(run["run_id"], []).append(dict(artifact_id=f"source-{ordinal + 1}", sha256=hashlib.sha256(original).hexdigest()))
    return sorted(by_id.values(), key=lambda run: (dt.datetime.fromisoformat(run["timestamp"].replace("Z", "+00:00")), run["run_id"])), sources


def render(input_path, output):
    """Write history.json and a self-contained dashboard after validation."""
    runs, sources = load_result_records(input_path)
    evidence = changes.build(runs, sources)
    history = {"schema_version": 1, "runs": runs}
    embedded = json.dumps(history, ensure_ascii=True, separators=(",", ":"))
    for literal, escape in (("&", "\\u0026"), ("<", "\\u003c"), (">", "\\u003e")):
        embedded = embedded.replace(literal, escape)
    template = Path(__file__).with_name("dashboard.html").read_text(encoding="utf-8")
    page = template.replace("__CLOCK_QUALIFICATION_JSON__", json.dumps(clocks.QUALIFICATION)).replace("__HISTORY_JSON__", embedded)
    # serialize every representation before publishing any output files
    files = {
        "history.json": json.dumps(history, ensure_ascii=True, indent=2) + "\n",
        "index.html": page,
        "changes.json": json.dumps(evidence, ensure_ascii=True, indent=2, allow_nan=False) + "\n",
        "changes.md": changes.markdown(evidence),
        "changes.html": changes.html_report(evidence),
    }
    # encode and stage all outputs before replacing any existing report file
    encoded = {name: content.encode("utf-8") for name, content in files.items()}
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".render-", dir=output) as temporary:
        staged = Path(temporary)
        for name, content in encoded.items():
            (staged / name).write_bytes(content)
        for name in encoded:
            (staged / name).replace(output / name)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Render benchmark history as a standalone dashboard")
    parser.add_argument("input", type=Path, help="results.json, a result directory, or history with runs/*.json")
    parser.add_argument("--output", required=True, type=Path, help="output directory (sanitized JSON requires a new directory)")
    parser.add_argument("--format", choices=("html", "sanitized-json"), default="html")
    args = parser.parse_args(argv)
    try:
        if args.format == "sanitized-json":
            sanitized.export_results(args.input, args.output)
        else:
            render(args.input, args.output)
    except (OSError, ValueError) as error:
        parser.exit(1, f"benchmark report: {error}\n")


if __name__ == "__main__":
    main()
