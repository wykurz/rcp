"""Validate and collect per-process scoped timing summaries."""

from collections import Counter
import math
from pathlib import Path
import re

from benchmarks.strict_json import parse_json


MAX_REPORT_BYTES = 256 * 1024
MAX_SCOPES = 256
MAX_NAME_LENGTH = 128
COUNT_FIELDS = ("count", "finished", "interrupted")
SECOND_FIELDS = ("total_seconds", "mean_seconds", "p50_seconds", "p95_seconds", "max_seconds")


class CollectionError(ValueError):
    """A collection failure with independently valid reports retained."""

    def __init__(self, message, reports):
        super().__init__(message)
        self.reports = reports


def validate_report(value):
    """Return a valid schema-one report, or raise ValueError."""
    if not isinstance(value, dict) or set(value) != {"schema_version", "identifier", "pid", "scopes"}:
        raise ValueError("timing report must contain schema_version, identifier, pid, scopes")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("timing schema_version must be 1")
    if not isinstance(value["identifier"], str) or not 0 < len(value["identifier"]) <= MAX_NAME_LENGTH or not value["identifier"].isascii() or not all(char.isalnum() or char in "-_" for char in value["identifier"]):
        raise ValueError("timing identifier must be a short ASCII role")
    if type(value["pid"]) is not int or value["pid"] <= 0:
        raise ValueError("timing pid must be a positive integer")
    scopes = value["scopes"]
    if not isinstance(scopes, list) or len(scopes) > MAX_SCOPES:
        raise ValueError("timing scopes must be a bounded array")
    seen = set()
    for index, scope in enumerate(scopes):
        label = f"timing scopes[{index}]"
        if not isinstance(scope, dict) or set(scope) != {"name", *COUNT_FIELDS, *SECOND_FIELDS}:
            raise ValueError(f"{label} has invalid fields")
        name = scope["name"]
        if not isinstance(name, str) or not 0 < len(name) <= MAX_NAME_LENGTH or any(ord(char) < 32 for char in name):
            raise ValueError(f"{label}.name must be a short printable string")
        if name in seen:
            raise ValueError(f"{label}.name duplicates {name}")
        seen.add(name)
        for field in COUNT_FIELDS:
            if type(scope[field]) is not int or scope[field] < 0:
                raise ValueError(f"{label}.{field} must be a nonnegative integer")
        if scope["count"] != scope["finished"] + scope["interrupted"]:
            raise ValueError(f"{label}.count must equal finished plus interrupted")
        for field in SECOND_FIELDS:
            number = scope[field]
            if isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(number) or number < 0:
                raise ValueError(f"{label}.{field} must be finite and nonnegative")
        if scope["p50_seconds"] > scope["p95_seconds"]:
            raise ValueError(f"{label} percentile order is invalid")
    return value


def collect(prefix, expected_roles, successful):
    """Collect reports for one trial, checking role cardinality after success."""
    prefix = Path(prefix)
    reports = []
    errors = []
    omitted_errors = 0
    expected = Counter(expected_roles)
    found = Counter()
    def note_error(message):
        nonlocal omitted_errors
        if len(errors) < 16:
            errors.append(message)
        else:
            omitted_errors += 1
    for path in prefix.parent.glob(prefix.name + "-*.timings.json"):
        try:
            if not path.is_file() or path.is_symlink() or path.stat().st_size > MAX_REPORT_BYTES:
                note_error(f"invalid timing report file: {path}")
                continue
            report = validate_report(parse_json(path.read_text(encoding="utf-8")))
        except (OSError, UnicodeError, ValueError) as error:
            note_error(f"invalid timing report {path}: {error}")
            continue
        stem = prefix.name + "-" + report["identifier"] + "-"
        suffix = path.name[len(stem):] if path.name.startswith(stem) else ""
        if not re.fullmatch(rf".+-{report['pid']}-[^/]+\.timings\.json", suffix):
            note_error(f"timing identifier does not match filename: {path}")
            continue
        if found[report["identifier"]] >= expected[report["identifier"]]:
            note_error(f"unexpected or duplicate timing role: {report['identifier']}")
            continue
        found[report["identifier"]] += 1
        reports.append(report)
    try:
        require_roles(reports, expected_roles, successful)
    except ValueError as error:
        note_error(str(error))
    if errors:
        if omitted_errors:
            errors.append(f"{omitted_errors} more timing artifact errors")
        reports.sort(key=lambda item: (item["identifier"], item["pid"]))
        raise CollectionError("; ".join(errors), reports)
    reports.sort(key=lambda item: (item["identifier"], item["pid"]))
    return reports


def require_roles(reports, expected_roles, successful):
    """Check a trial's process roles without discarding partial reports."""
    expected = Counter(expected_roles)
    found = Counter(item["identifier"] for item in reports)
    excess = found - expected
    if excess:
        raise ValueError(f"unexpected or duplicate timing role: {dict(excess)}")
    missing = expected - found
    if successful and missing:
        raise ValueError(f"missing timing report role: {dict(missing)}")
    if successful and any(not item["scopes"] or not any(scope["name"] == "operation" for scope in item["scopes"]) for item in reports):
        raise ValueError("timing report missing operation scope")
