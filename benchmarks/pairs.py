"""Opt-in adjacent candidate/reference pairs; old rotating runs keep their contract."""

import hashlib
import math
from collections import Counter


POLICY = "adjacent-balanced-sha256-v1"
CANDIDATE = "rcp-default"
REFERENCE = "rcp-baseline"
ROLES = (CANDIDATE, REFERENCE)
MAX_SEED = 2**53 - 1


def configuration(seed, repetitions):
    if type(seed) is not int or not 0 <= seed <= MAX_SEED:
        raise ValueError("paired seed must be a JSON-safe integer from 0 through 9007199254740991")
    if type(repetitions) is not int or repetitions < 2 or repetitions % 2:
        raise ValueError("paired repetitions must be a positive even number of pairs")
    return dict(policy=POLICY, seed=seed, pairs_per_case=repetitions, block_pairs=2,
                candidate=CANDIDATE, reference=REFERENCE)


def validate_selection(mode, variants):
    if (mode != "local" or not isinstance(variants, list) or len(variants) != 2
            or any(not isinstance(variant, dict) for variant in variants)
            or [variant.get("id") for variant in variants] != list(ROLES)):
        raise ValueError("paired mode requires local rcp-default and its supplied baseline only")
    for variant in variants:
        if (variant.get("tool") != "rcp" or type(variant.get("processes")) is not int
                or variant["processes"] != 1 or not isinstance(variant.get("args"), list)
                or any(not isinstance(arg, str) or not arg for arg in variant["args"])):
            raise ValueError("paired mode requires identical single-process rcp flags")
        if any(arg == "--timings" or arg.startswith("--timings=") for arg in variant["args"]):
            raise ValueError("paired mode requires disabled timings, including manifest arguments")
    if variants[0]["args"] != variants[1]["args"]:
        raise ValueError("paired mode requires identical single-process rcp flags")


def order(config, case_id, iteration):
    block = (iteration - 1) // 2 + 1
    identity = f"{config['seed']}:{case_id}:{block}".encode()
    draw = hashlib.sha256(identity).digest()[0] & 1
    reverse = draw ^ ((iteration - 1) % 2)
    return [REFERENCE, CANDIDATE] if reverse else [CANDIDATE, REFERENCE]


def trial_metadata(config, case_id, iteration, position):
    return dict(pair=iteration, block=(iteration - 1) // 2 + 1, position=position,
                order=order(config, case_id, iteration))


def _array(value, name):
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise ValueError(f"paired {name} must be an array of objects")
    return value


def validate(run):
    if not isinstance(run, dict) or not isinstance(run.get("context"), dict):
        raise ValueError("paired result requires a context object")
    context = run["context"]
    rows = _array(run.get("trials"), "trials")
    if "pairing" not in context:
        if any("pairing" in row for row in rows):
            raise ValueError("pairing metadata requires context.pairing")
        return
    config = context["pairing"]
    if (not isinstance(config, dict)
            or config != configuration(config.get("seed"), config.get("pairs_per_case"))
            or type(config.get("block_pairs")) is not int):
        raise ValueError("unsupported pairing configuration")
    if run.get("status") not in ("running", "complete", "failed"):
        raise ValueError("invalid paired run status")
    variants = _array(run.get("variants"), "variants")
    cases = _array(run.get("cases"), "cases")
    summaries = _array(run.get("summaries"), "summaries")
    # failed preflight records may precede selection or timing capability resolution
    if variants or rows:
        validate_selection(context.get("topology"), variants)
    if rows or "timing_collection" in context:
        if (context.get("timing_request") != "disabled"
                or context.get("timing_collection") != dict.fromkeys(ROLES, "disabled")):
            raise ValueError("paired trials require disabled timing request and collection for both roles")
    case_ids = []
    for case in cases:
        case_id = case.get("id")
        if not isinstance(case_id, str) or not case_id:
            raise ValueError("paired cases require nonempty string ids")
        case_ids.append(case_id)
    if len(set(case_ids)) != len(case_ids):
        raise ValueError("paired case ids must be unique")
    rows_per_case = config["pairs_per_case"] * 2
    planned_rows = len(cases) * rows_per_case
    if len(rows) > planned_rows or run["status"] == "complete" and len(rows) != planned_rows:
        raise ValueError("paired run has missing or extra trials")
    counts = Counter()
    for index, row in enumerate(rows):
        case = case_ids[index // rows_per_case]
        iteration, position = divmod(index % rows_per_case, 2)
        iteration += 1
        metadata = trial_metadata(config, case, iteration, position)
        variant = metadata["order"][position]
        actual = row.get("pairing")
        if (not isinstance(actual, dict) or set(actual) != set(metadata)
                or any(type(actual.get(key)) is not int for key in ("pair", "block", "position"))
                or type(row.get("iteration")) is not int):
            raise ValueError("invalid pair metadata")
        if (row.get("case_id"), row.get("variant_id"), row["iteration"], actual) != (case, variant, iteration, metadata):
            raise ValueError("paired trials must follow the recorded adjacent balanced schedule")
        counts[case] += 1
    summarized = set()
    for summary in summaries:
        case, variant = summary.get("case_id"), summary.get("variant_id")
        if not isinstance(case, str) or case not in case_ids or variant not in ROLES:
            raise ValueError("paired summary references an unknown case or role")
        if (case, variant) in summarized:
            raise ValueError("paired summary duplicates a case and role")
        summarized.add((case, variant))
        if counts[case] != rows_per_case:
            raise ValueError("paired summary requires every planned pair")
    for case, _ in summarized:
        if any((case, role) not in summarized for role in ROLES):
            raise ValueError("paired case requires both committed summaries")


def comparisons(run):
    """Expose successful pairs only after the complete case has passed validation."""
    if "pairing" not in run["context"]:
        return []
    committed = {(item["case_id"], item["variant_id"]) for item in run["summaries"]}
    eligible = {case for case, _ in committed if all((case, role) in committed for role in ROLES)}
    result = []
    rows = run["trials"]
    for index in range(0, len(rows) - 1, 2):
        pair = rows[index:index + 2]
        if pair[0]["case_id"] not in eligible or any(row["status"] != "ok" for row in pair):
            continue
        by_id = {row["variant_id"]: (index + offset, row) for offset, row in enumerate(pair)}
        candidate_index, candidate = by_id[CANDIDATE]
        reference_index, reference = by_id[REFERENCE]
        if reference["elapsed_seconds"] <= 0:
            continue
        ratio = candidate["elapsed_seconds"] / reference["elapsed_seconds"]
        if not math.isfinite(ratio) or ratio <= 0:
            continue
        result.append(dict(case_id=candidate["case_id"], pair=candidate["pairing"]["pair"],
                           block=candidate["pairing"]["block"], order=pair[0]["pairing"]["order"],
                           candidate_trial=candidate_index, reference_trial=reference_index,
                           candidate_over_reference=ratio))
    return result
