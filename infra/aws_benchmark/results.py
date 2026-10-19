"""Bounded offline AWS result ingestion; never extract, execute or publish worker data."""

import argparse
import copy
import html
import io
import json
import os
from pathlib import Path
import re
import stat
import struct
import tempfile
import zipfile

from benchmarks import operations, pairs, report
from benchmarks.strict_json import parse_json
from . import identity
from .common import (MAX_RESULT, SUITES, digest, require, utc,
                     validate_request, validate_window)


MAX_METADATA = 1024 * 1024
MAX_JSON = 8 * 1024 * 1024
MAX_ENTRIES = 1001
RESULT = "benchmark/results/results.json"
SERIES_CONTRACT = "aws-local-ext4-v1"


def encoded(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                       separators=(",", ":")) + "\n").encode()


def read_file(path, maximum, *, single_link=True):
    # callers own the directory prefixes; bind type/size checks and reads to one fd
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and (not single_link or info.st_nlink == 1) and 0 < info.st_size <= maximum,
                "input must be a bounded, singly linked regular file")
        data = stream.read(maximum + 1)
        require(0 < len(data) <= maximum, "input exceeds its byte limit")
        return data


def json_value(data, maximum=MAX_METADATA):
    require(len(data) <= maximum, "JSON exceeds its byte limit")
    # cap nesting before the parser allocates containers, including malformed JSON
    depth, quoted, escaped = 0, False, False
    for byte in data:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        elif byte in (91, 123):
            depth += 1
            require(depth <= 40, "JSON nesting exceeds limit")
        elif byte in (93, 125):
            depth -= 1
    value = parse_json(data.decode("utf-8"))
    pending, count = [value], 0
    while pending:
        item = pending.pop()
        count += 1
        require(count <= 100000, "JSON node count exceeds limit")
        if isinstance(item, str):
            require(len(item) <= 16384, "JSON string exceeds limit")
            require(not any(0xD800 <= ord(char) <= 0xDFFF for char in item), "JSON contains a lone surrogate")
        elif isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
    return value


def members(data):
    require(22 <= len(data) <= MAX_RESULT, "invalid archive size")
    # the worker emits plain, single-disk, comment-free ZIP_STORED, without ZIP64
    end = struct.unpack("<4s4H2LH", data[-22:])
    require(end[0] == b"PK\x05\x06" and end[1:3] == (0, 0) and end[3] == end[4]
            and 0 < end[4] <= MAX_ENTRIES and end[7] == 0
            and end[5] + end[6] == len(data) - 22, "invalid archive directory")
    # independently count central records before ZipFile allocates ZipInfo objects
    offset = end[6]
    for _ in range(end[4]):
        require(data[offset:offset + 4] == b"PK\x01\x02" and offset + 46 <= len(data) - 22,
                "invalid central record")
        name_size, extra_size, comment_size = struct.unpack_from("<3H", data, offset + 28)
        require(0 < name_size <= 1200 and extra_size == comment_size == 0,
                "unsupported ZIP extra fields or comments")
        require(10 <= struct.unpack_from("<H", data, offset + 6)[0] <= 20
                and struct.unpack_from("<H", data, offset + 34)[0] == 0, "unsupported ZIP version or volume")
        offset += 46 + name_size
    require(offset == len(data) - 22, "archive entry count mismatch")
    selected, hashes, seen, total, next_header = {}, {}, set(), 0, 0
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        require(len(archive.infolist()) == end[4], "archive entry count mismatch")
        for member in archive.infolist():
            name = member.filename
            parts = name.split("/")
            require(name == member.orig_filename and name.isascii()
                    and all(32 <= ord(char) < 127 and char not in "\\:" for char in name)
                    and all(part not in ("", ".", "..") and len(part) <= 200 for part in parts)
                    and len(parts) <= 7 and name.casefold() not in seen
                    and (name == "worker.json" or parts[0] == "benchmark"), "unsafe or duplicate member name")
            seen.add(name.casefold())
            mode = member.external_attr >> 16
            require(not member.is_dir() and stat.S_IFMT(mode) in (0, stat.S_IFREG)
                    and not member.external_attr & 16 and member.flag_bits in (0, 0x800)
                    and member.compress_type == zipfile.ZIP_STORED
                    and member.compress_size == member.file_size, "unsupported member type or encoding")
            total += member.file_size
            limit = MAX_METADATA if name == "worker.json" else MAX_JSON if name == RESULT else MAX_RESULT
            require(member.file_size <= limit and total <= MAX_RESULT, "archive expansion exceeds limit")
            # require contiguous local records: no prefix, overlaps, descriptors or hidden payloads
            require(member.header_offset == next_header and data[next_header:next_header + 4] == b"PK\x03\x04",
                    "non-contiguous archive records")
            fields = struct.unpack_from("<4s5H3L2H", data, next_header)
            require(fields[1] == member.extract_version and fields[2] == member.flag_bits
                    and fields[3] == 0 and fields[6] == member.CRC
                    and fields[7] == fields[8] == member.file_size and fields[10] == 0,
                    "local and central records disagree")
            next_header += 30 + fields[9] + member.file_size
            require(next_header <= end[6], "member overlaps archive directory")
            with archive.open(member) as stream:
                body = stream.read(limit + 1)
                require(len(body) == member.file_size, "member size mismatch")
            hashes[name] = digest(body)
            if name in ("worker.json", RESULT):
                selected[name] = body
    require(next_header == end[6] and "worker.json" in selected, "missing worker metadata or extra data")
    return selected, hashes


def inspect(source, expected, conclusion):
    identity.validate(expected)
    require(conclusion in {"success", "failure", "cancelled", "skipped"}, "invalid producer conclusion")
    evidence = {"schema_version": 1, "identity": expected, "producer_conclusion": conclusion,
                "history_eligible": False, "files": {}, "members": {}, "errors": [],
                "controller": None, "worker": None, "raw_revision": None}
    snapshots = {}
    # preserve each bounded file even if another file is absent or invalid
    for name, limit in (("controller.json", MAX_METADATA), ("status.json", MAX_METADATA), ("results.zip", MAX_RESULT)):
        try:
            snapshots[name] = read_file(source / name, limit)
            evidence["files"][name] = digest(snapshots[name])
        except (OSError, ValueError) as error:
            evidence["errors"].append(name + ": " + type(error).__name__)
    result = None
    try:
        require(set(snapshots) == {"controller.json", "status.json", "results.zip"}, "incomplete result bundle")
        receipt = json_value(snapshots["controller.json"])
        require(isinstance(receipt, dict) and type(receipt.get("schema_version")) is int
                and receipt["schema_version"] == 1 and receipt["identity"] == expected, "controller identity mismatch")
        instance = receipt.get("instance")
        require(instance is None or isinstance(instance, dict) and set(instance) == {
            "instance_id", "image_id", "instance_type"} and all(isinstance(value, str) for value in instance.values()),
            "invalid controller instance observation")
        require(receipt.get("outcome") in ("incomplete", "success", "failure")
                and receipt.get("cleanup") in ("not-requested", "unconfirmed", "verified"), "invalid controller outcome")
        evidence["controller"] = receipt
        window, request = receipt["window"], receipt["request"]
        validate_window(window, now=utc(window["opens"]))
        require(all(window[key] == expected[key] for key in ("run", "baseline", "candidate", "suite")),
                "window identity mismatch")
        validate_request(request, window)
        status = json_value(snapshots["status.json"])
        require(status["run"] == expected["run"] and status["state"] == "completed"
                and type(status["success"]) is bool, "invalid terminal status")
        require(status["sha256"] == evidence["files"]["results.zip"] == receipt["archive_sha256"]
                and receipt["status_sha256"] == evidence["files"]["status.json"], "bundle digest mismatch")
        contents, evidence["members"] = members(snapshots["results.zip"])
        worker = json_value(contents["worker.json"])
        require(isinstance(worker, dict), "invalid worker metadata")
        evidence["worker"] = worker
        require(worker["window"] == window, "worker window mismatch")
        require(worker.get("request", request) == request, "worker binary request mismatch")
        require(worker["status"] == {key: value for key, value in status.items() if key != "sha256"},
                "worker status mismatch")
        if RESULT in contents:
            value = json_value(contents[RESULT], MAX_JSON)
            require(isinstance(value, dict) and all(isinstance(value.get(key), list) and len(value[key]) <= limit
                    for key, limit in (("cases", 16), ("variants", 16), ("trials", 512), ("summaries", 256))),
                    "measurement collection exceeds reporting limit")
            result = report.validate_result(value)
            evidence["raw_revision"] = copy.deepcopy(result["revision"])
        else:
            evidence["errors"].append("harness result unavailable")
    except Exception as error:
        # format/parser failures always leave diagnostics; never echo arbitrary worker strings
        evidence["errors"].append("bundle validation: " + type(error).__name__)
        result = None
    return evidence, result, snapshots


def approve(evidence, result, policy):
    """Offline gate; the caller must authenticate this independent per-attempt policy."""
    expected = evidence["identity"]
    require(expected["ref"] == identity.REF, "history requires the exact approved main branch")
    require(isinstance(policy, dict) and set(policy) == {"schema_version", "identity", "archive_sha256",
            "ami_id", "harness_revision"} and type(policy["schema_version"]) is int
            and policy["schema_version"] == 1, "invalid history policy")
    require(policy["identity"] == expected and policy["archive_sha256"] == evidence["files"].get("results.zip"),
            "policy does not approve this exact attempt and archive")
    require(isinstance(policy["ami_id"], str) and re.fullmatch(r"ami-[0-9a-f]{17}", policy["ami_id"])
            and isinstance(policy["harness_revision"], str)
            and re.fullmatch(r"[0-9a-f]{40}", policy["harness_revision"]), "invalid approved AMI provenance")
    require(not evidence["errors"] and result is not None and evidence["producer_conclusion"] == "success"
            and expected["suite"] != "aa-smoke", "diagnostic run cannot enter performance history")
    receipt, worker = evidence["controller"], evidence["worker"]
    instance = receipt.get("instance") or {}
    require(receipt["outcome"] == "success" and receipt["cleanup"] == "verified"
            and instance.get("image_id") == policy["ami_id"] and instance.get("instance_type") == "c5d.24xlarge"
            and worker.get("harness_revision") == policy["harness_revision"], "unverified execution provenance")
    require(worker["status"]["success"] is True and worker.get("service_returncode") == 0
            and worker.get("admission", {}).get("accepted") is True
            and worker.get("request") == receipt["request"], "worker did not complete the approved request")
    require(result["status"] == "complete" and result["context"].get("purpose") == "performance"
            and result["context"].get("repository") == expected["repository"]
            and result["context"].get("baseline_commit") == expected["baseline"]
            and result["context"].get("topology") == "local"
            and result["context"].get("cache_policy") == "linux-drop-caches"
            and result["context"].get("operation_contract_revision") == operations.CONTRACT_REVISION
            and result["context"].get("pairing") == pairs.configuration(0, 4)
            and result["context"].get("runner_label") == "aws-c5d24-ext4-isolated-v1"
            and [case["id"] for case in result["cases"]] == list(SUITES[expected["suite"]]),
            "unexpected performance measurement contract")
    for tool, key in (("rcp", "candidate/rcp"), ("rcpd", "candidate/rcpd"), ("filegen", "candidate/filegen"),
                      ("rcp-baseline", "baseline/rcp"), ("rcpd-baseline", "baseline/rcpd")):
        require(result["tools"].get(tool, {}).get("sha256") == receipt["request"]["sha256"][key],
                "measured tool does not match approved binary digest")
    require({variant["id"] for variant in result["variants"]} == {"rcp-default", "rcp-baseline"}
            and len(result["trials"]) == 8 * len(result["cases"])
            and all(trial["status"] == "ok" and trial["validation"].get("ok") is True
                    and trial.get("source_validation", {}).get("ok") is True
                    and trial.get("resources", {}).get("status") == "complete"
                    for trial in result["trials"]), "incomplete paired comparison")
    evidence["history_eligible"] = True
    evidence["policy_sha256"] = digest(encoded(policy))


def derived_record(evidence, raw):
    result = copy.deepcopy(raw)
    expected = evidence["identity"]
    eligible = evidence["history_eligible"]
    result["run_id"] = digest(encoded(["aws-v1", expected["repository"], expected["workflow"], expected["run"]]))[:32]
    # only the separately approved history copy assigns the candidate revision
    if eligible:
        result["revision"] = {"commit": expected["candidate"], "branch": None, "dirty": False,
                              "role": "approved candidate"}
    result["context"].update(repository=expected["repository"], run_url=expected["run_url"],
                            purpose="performance" if eligible else "diagnostic")
    worker, controller = evidence["worker"], evidence["controller"]
    provenance = dict(identity=expected, history_eligible=eligible, raw_run_id=raw["run_id"],
        raw_revision=raw["revision"], files=evidence["files"], members=evidence["members"],
        instance=controller.get("instance"), supervisor_harness_revision=worker.get("harness_revision"),
        source_series_ids=[item["series_id"] for item in raw["summaries"]])
    result["context"]["aws_provenance"] = provenance
    for item in result["summaries"]:
        item["series_id"] = digest(encoded([SERIES_CONTRACT, item["series_id"],
            (controller.get("instance") or {}).get("image_id"), worker.get("harness_revision")]))
    return report.validate_result(result)


def stage_history(record, destination):
    require(record["context"]["aws_provenance"]["history_eligible"] is True, "history approval required")
    runs = destination / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    path = runs / (record["run_id"] + ".json")
    payload = encoded(record)
    # finish bytes before atomically linking an immutable name; a failed write exposes no run
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=runs, prefix=".aws-stage-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            # a crash after link but before unlink can leave the completed record with two links
            require(read_file(path, MAX_JSON * 2, single_link=False) == payload, "conflicting history replay")
    finally:
        if temporary is not None:
            temporary.unlink()
    return path


def literal(value):
    # numeric entities cannot introduce Markdown, HTML or workflow command syntax
    return "".join(char if char.isascii() and (char.isalnum() or char == " ") else f"&#{ord(char)};"
                   for char in str(value)[:200])


def summary(evidence, result):
    expected = evidence["identity"]
    worker, controller = evidence["worker"] or {}, evidence["controller"] or {}
    instance = controller.get("instance") or {}
    lines = ["# AWS benchmark result", "", f"[Workflow attempt]({expected['run_url']})", "",
             "History: " + ("approved for offline staging" if evidence["history_eligible"] else "diagnostic only; not published"), "",
             "| Provenance | Value |", "| --- | --- |"]
    fields = {"Run": expected["run"], "Producer conclusion": evidence["producer_conclusion"],
        "Suite": expected["suite"], "Candidate": expected["candidate"], "Baseline": expected["baseline"],
        "Controller": expected["controller"], "Controller observed AMI": instance.get("image_id", "unknown"),
        "Supervisor reported baked source": worker.get("harness_revision", "unknown"),
        "Raw harness checkout": (evidence["raw_revision"] or {}).get("commit") or "unknown",
        "Controller cleanup": controller.get("cleanup", "unknown"),
        "Archive SHA256": evidence["files"].get("results.zip", "unavailable")}
    lines += [f"| {key} | {literal(value)} |" for key, value in fields.items()]
    if result is not None:
        lines += ["", "| Case | Variant | Median seconds | Repeats |", "| --- | --- | ---: | ---: |"]
        lines += [f"| {literal(item['case_id'])} | {literal(item['variant_id'])} | {item['median']:.6g} | {len(item['samples'])} |"
                  for item in result["summaries"]]
    lines += ["", "Validation checks integrity and structure; it does not attest that candidate-controlled measurements are honest.",
              "Raw worker evidence is retained separately from derived metadata. Missing provenance stays unknown."]
    if evidence["errors"]:
        lines += ["", "Diagnostics: " + "; ".join(map(literal, evidence["errors"]))]
    return "\n".join(lines) + "\n"


def ingest(source, output, expected, conclusion, policy=None, history=None):
    require(source.resolve() != output.resolve(), "raw input and derived output must differ")
    # a fresh directory prevents a failed rerun from leaving an older success report
    output.mkdir(parents=True, exist_ok=False)
    evidence, raw, snapshots = inspect(source, expected, conclusion)
    raw_dir = output / "raw"
    raw_dir.mkdir()
    for name, payload in snapshots.items():
        (raw_dir / name).write_bytes(payload)
    if policy is not None:
        try:
            approve(evidence, raw, policy)
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            evidence["errors"].append("history approval: " + type(error).__name__)
    record = derived_record(evidence, raw) if raw is not None else None
    if history is not None:
        try:
            require(evidence["history_eligible"] and record is not None, "history staging refused")
            stage_history(record, history)
        except (OSError, ValueError) as error:
            evidence["errors"].append("history staging: " + type(error).__name__)
            evidence["history_eligible"] = False
            record = derived_record(evidence, raw) if raw is not None else None
    (output / "provenance.json").write_bytes(encoded(evidence))
    text = summary(evidence, raw)
    (output / "summary.md").write_text(text, encoding="utf-8")
    if record is not None:
        try:
            (output / "derived.json").write_bytes(encoded(record))
            report.render(output / "derived.json", output / "dashboard")
        except Exception as error:
            evidence["errors"].append("dashboard rendering: " + type(error).__name__)
            record = None
            (output / "provenance.json").write_bytes(encoded(evidence))
            text = summary(evidence, raw)
            (output / "summary.md").write_text(text, encoding="utf-8")
    # a standalone, script-free provenance landing page; the dashboard is standalone too
    link = '<p><a href="dashboard/index.html">Measurement dashboard</a></p>' if record is not None else ""
    (output / "index.html").write_text('<!doctype html><meta charset="utf-8"><title>AWS benchmark result</title>'
        + link + '<pre style="white-space:pre-wrap">' + html.escape(html.unescape(text)) + '</pre>', encoding="utf-8")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--identity", type=Path, help="independent trusted identity; defaults to workflow environment")
    parser.add_argument("--conclusion", choices=("success", "failure", "cancelled", "skipped"), required=True)
    parser.add_argument("--policy", type=Path, help="independent reviewed per-attempt policy, offline only")
    parser.add_argument("--history", type=Path, help="optional local staging directory; no network publication")
    args = parser.parse_args()
    expected = json_value(read_file(args.identity, MAX_METADATA)) if args.identity else identity.environment()
    policy = json_value(read_file(args.policy, MAX_METADATA)) if args.policy else None
    evidence = ingest(args.input, args.output, expected, args.conclusion, policy, args.history)
    if evidence["errors"]:
        parser.exit(1, "AWS result ingestion retained diagnostics; see provenance.json.\n")


if __name__ == "__main__":
    main()
