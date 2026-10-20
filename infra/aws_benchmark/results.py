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

from benchmarks import changes, operations, pairs, report
from benchmarks.strict_json import parse_json
from . import identity
from .common import (MAX_RESULT, SUITES, INSTANCE_TYPE, ValidationError, archive_name_supported,
                     digest, measurement_contract, require, utc)


MAX_METADATA = 1024 * 1024
MAX_JSON = 8 * 1024 * 1024
MAX_ENTRIES = 1001
RESULT = "benchmark/results/results.json"
SERIES_CONTRACT = "aws-local-ext4-v1"


CONCLUSIONS = ("success", "failure", "cancelled", "skipped")


def diagnostic(error):
    # generic parsers may echo candidate-controlled input; only our constants are safe
    return str(error) if isinstance(error, ValidationError) else type(error).__name__


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
        # worker paths fit below this structural bound, including older nonportable names
        require(0 < name_size <= 2048, "archive member name exceeds limit")
        require(extra_size == comment_size == 0,
                "unsupported ZIP extra fields or comments")
        require(10 <= struct.unpack_from("<H", data, offset + 6)[0] <= 20
                and struct.unpack_from("<H", data, offset + 34)[0] == 0, "unsupported ZIP version or volume")
        offset += 46 + name_size
    require(offset == len(data) - 22, "archive entry count mismatch")
    # ZipFile honors this locator even when a central name absorbed its bytes
    require(data[-42:-38] != b"PK\x06\x07", "unsupported ZIP64 records")
    selected, hashes, seen, folded, issues, total, next_header = {}, {}, set(), set(), [], 0, 0
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        require(len(archive.infolist()) == end[4], "archive entry count mismatch")
        for member in archive.infolist():
            name = member.filename
            parts = name.split("/")
            require(name == member.orig_filename and "\\" not in name
                    and all(part not in ("", ".", "..") for part in parts)
                    and name not in seen and (name == "worker.json" or parts[0] == "benchmark"),
                    "unsafe or duplicate archive path")
            seen.add(name)
            # candidate-owned names can make measurements unavailable, never erase worker.json
            if (not name.isascii() or not all(32 <= ord(char) < 127 and char != ":" for char in name)
                    or not archive_name_supported(name) or name.casefold() in folded):
                if "unsupported benchmark member names" not in issues:
                    issues.append("unsupported benchmark member names")
            folded.add(name.casefold())
            mode = member.external_attr >> 16
            require(not member.is_dir() and stat.S_IFMT(mode) in (0, stat.S_IFREG)
                    and not member.external_attr & 16 and member.flag_bits in (0, 0x800)
                    and member.compress_type == zipfile.ZIP_STORED
                    and member.compress_size == member.file_size, "unsupported member type or encoding")
            total += member.file_size
            limit = MAX_METADATA if name == "worker.json" else MAX_RESULT
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
            if name == RESULT and member.file_size > MAX_JSON:
                issues.append("measurement JSON exceeds reporting byte limit")
            elif name in ("worker.json", RESULT):
                selected[name] = body
    require(next_header == end[6] and "worker.json" in selected, "missing worker metadata or extra data")
    return selected, hashes, issues


def historical_inputs(receipt, expected):
    # receipts record admitted inputs; offline readers validate protocol shape and bindings,
    # not the current launch duration or binary admission policy
    window, request = receipt.get("window"), receipt.get("request")
    require(isinstance(window, dict) and set(window) ==
            {"run", "baseline", "candidate", "suite", "opens", "deadline"}, "unexpected window fields")
    require(all(isinstance(value, str) for value in window.values()), "window fields must be strings")
    require(all(window[key] == expected[key] for key in ("run", "baseline", "candidate", "suite")),
            "window identity mismatch")
    try:
        opens, deadline = utc(window["opens"]), utc(window["deadline"])
    except ValidationError:
        raise
    except ValueError as error:
        raise ValidationError("invalid window timestamp") from error
    require(opens < deadline, "window must open before its deadline")
    require(isinstance(request, dict) and set(request) ==
            {"run", "baseline", "candidate", "suite", "deadline", "sha256"}, "unexpected request fields")
    require(all(request[key] == window[key] for key in ("run", "baseline", "candidate", "suite", "deadline")),
            "mailbox request does not match the operator window")
    hashes = request["sha256"]
    require(isinstance(hashes, dict) and hashes and all(isinstance(key, str) and key for key in hashes),
            "unexpected binary digest map")
    require(all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
                for value in hashes.values()), "binary digests must be lowercase SHA-256")
    return window, request


def inspect(source, expected, conclusion):
    evidence = {"schema_version": 1, "identity": expected, "producer_conclusion": conclusion,
                "history_eligible": False, "files": {}, "members": {}, "errors": [],
                "controller": None, "status": None, "worker": None, "raw_revision": None}
    snapshots = {}
    # preserve each bounded file even if another file is absent or invalid
    for name, limit in (("controller.json", MAX_METADATA), ("status.json", MAX_METADATA), ("results.zip", MAX_RESULT)):
        try:
            snapshots[name] = read_file(source / name, limit)
            evidence["files"][name] = digest(snapshots[name])
        except (OSError, ValueError) as error:
            evidence["errors"].append(name + ": " + diagnostic(error))
    result = None
    stage = "controller receipt"
    try:
        require("controller.json" in snapshots, "controller receipt unavailable")
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
        if "status.json" in snapshots:
            stage = "terminal status"
            status = json_value(snapshots["status.json"])
            require(isinstance(status, dict) and status.get("run") == expected["run"], "invalid terminal status")
            require(receipt.get("status_sha256") == evidence["files"]["status.json"], "status digest mismatch")
            # retain the bound observation even when the archive or terminal protocol is absent
            evidence["status"] = status
            require(status.get("state") == "completed" and type(status.get("success")) is bool,
                    "invalid terminal status")
        stage = "result bundle"
        require(set(snapshots) == {"controller.json", "status.json", "results.zip"}, "incomplete result bundle")
        stage = "controller window/request"
        window, request = historical_inputs(receipt, expected)
        stage = "bundle integrity"
        require(isinstance(receipt.get("archive_sha256"), str)
                and re.fullmatch(r"[0-9a-f]{64}", receipt["archive_sha256"]), "controller archive digest unavailable")
        require(status.get("sha256") == evidence["files"]["results.zip"] == receipt["archive_sha256"],
                "bundle digest mismatch")
        stage = "worker archive"
        contents, evidence["members"], issues = members(snapshots["results.zip"])
        stage = "worker metadata"
        worker = json_value(contents["worker.json"])
        require(isinstance(worker, dict), "invalid worker metadata")
        require(worker["window"] == window, "worker window mismatch")
        require(worker.get("request", request) == request, "worker binary request mismatch")
        require(worker["status"] == {key: value for key, value in status.items() if key != "sha256"},
                "worker status mismatch")
        evidence["worker"] = worker
        if issues:
            evidence["errors"].extend(issues)
            return evidence, None, snapshots
        stage = "measurement record"
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
        evidence["errors"].append(stage + ": " + diagnostic(error))
        result = None
    return evidence, result, snapshots


def approve(evidence, result, policy):
    """Offline gate; the caller must authenticate this independent per-attempt policy."""
    expected = evidence["identity"]
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
            and instance.get("image_id") == policy["ami_id"] and instance.get("instance_type") == INSTANCE_TYPE
            and worker.get("harness_revision") == policy["harness_revision"], "unverified execution provenance")
    require(worker["status"]["success"] is True and worker.get("service_returncode") == 0
            and worker.get("admission", {}).get("accepted") is True
            and worker.get("request") == receipt["request"], "worker did not complete the approved request")
    contract = measurement_contract(expected["suite"])
    require(result["status"] == "complete" and result["context"].get("purpose") == contract["purpose"]
            and result["context"].get("repository") == expected["repository"]
            and result["context"].get("baseline_commit") == expected["baseline"]
            and result["context"].get("topology") == "local"
            and result["context"].get("cache_policy") == contract["cache_policy"]
            and result["context"].get("operation_contract_revision") == operations.CONTRACT_REVISION
            and result["context"].get("pairing") == pairs.configuration(contract["paired_seed"], contract["repetitions"])
            and result["context"].get("runner_label") == contract["runner_label"]
            and [case["id"] for case in result["cases"]] == list(SUITES[expected["suite"]]),
            "unexpected performance measurement contract")
    for tool, key in (("rcp", "candidate/rcp"), ("rcpd", "candidate/rcpd"), ("filegen", "candidate/filegen"),
                      ("rcp-baseline", "baseline/rcp"), ("rcpd-baseline", "baseline/rcpd")):
        require(result["tools"].get(tool, {}).get("sha256") == receipt["request"]["sha256"][key],
                "measured tool does not match approved binary digest")
    require(all(trial.get("source_validation", {}).get("ok") is True
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
    else:
        result["revision"] = {key: raw["revision"].get(key) for key in ("commit", "branch", "dirty")}
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
            require(read_file(path, len(payload), single_link=False) == payload, "conflicting history replay")
    finally:
        if temporary is not None:
            try:
                temporary.unlink()
            except OSError:
                # cleanup cannot reverse a committed link or mask a prior failure
                pass
    return path


def literal(value):
    return changes.literal(value, limit=200)


def table(headers, rows):
    return "<table>\n<thead><tr>" + "".join("<th>" + html.escape(label) + "</th>" for label in headers) + "</tr></thead>\n<tbody>\n" + "\n".join(
        "<tr>" + "".join("<td>" + literal(value) + "</td>" for value in row) + "</tr>" for row in rows) + "\n</tbody></table>"


def summary(evidence, result):
    expected = evidence["identity"]
    worker, controller = evidence["worker"] or {}, evidence["controller"] or {}
    instance = controller.get("instance") or {}
    lines = ['<h1>AWS benchmark result</h1>',
             '<p><a href="' + html.escape(expected['run_url'], quote=True) + '">Workflow attempt</a></p>',
             '<p>History: ' + ("approved for offline staging" if evidence["history_eligible"] else "diagnostic only; not published") + '</p>']
    fields = {"Run": expected["run"], "Producer conclusion": evidence["producer_conclusion"],
        "Suite": expected["suite"], "Candidate": expected["candidate"], "Baseline": expected["baseline"],
        "Controller": expected["controller"], "Controller observed AMI": instance.get("image_id", "unknown"),
        "Supervisor reported baked source": worker.get("harness_revision", "unknown"),
        "Raw harness checkout": (evidence["raw_revision"] or {}).get("commit") or "unknown",
        "Controller cleanup": controller.get("cleanup", "unknown"),
        "Archive SHA256": evidence["files"].get("results.zip", "unavailable")}
    status = evidence.get("status") or {}
    if status:
        fields.update({"Worker state": status.get("state", "unknown"), "Worker success": status.get("success", "unknown")})
        for key in ("error", "collection_error", "fallback_error", "upload_error", "stop_error"):
            if key in status:
                fields["Worker " + key.replace("_", " ")] = status[key]
    lines.append(table(("Provenance", "Value"), fields.items()))
    if result is not None:
        lines.append(table(("Case", "Variant", "Median seconds", "Repeats"),
            [(item['case_id'], item['variant_id'], f"{item['median']:.6g}", len(item['samples'])) for item in result["summaries"]]))
    lines += ['<p>Validation checks integrity and structure; it does not attest that candidate-controlled measurements are honest.</p>',
              '<p>Raw worker evidence is retained separately from derived metadata. Missing provenance stays unknown.</p>']
    if evidence["errors"]:
        lines.append('<p>Diagnostics: ' + "; ".join(map(literal, evidence["errors"])) + '</p>')
    return "\n\n".join(lines) + "\n"


def ingest(source, output, expected, conclusion, policy=None, history=None):
    # reject invalid trusted arguments before claiming a must-be-new output directory
    identity.validate(expected)
    require(conclusion in CONCLUSIONS, "invalid producer conclusion")
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
            evidence["errors"].append("history approval: " + diagnostic(error))
    record = derived_record(evidence, raw) if raw is not None else None
    if history is not None:
        try:
            require(evidence["history_eligible"] and record is not None, "history staging refused")
            stage_history(record, history)
        except (OSError, ValueError) as error:
            evidence["errors"].append("history staging: " + diagnostic(error))
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
        + link + text, encoding="utf-8")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--identity", type=Path, required=True, help="independent trusted producer identity")
    parser.add_argument("--conclusion", choices=CONCLUSIONS, required=True)
    parser.add_argument("--policy", type=Path, help="independent reviewed per-attempt policy, offline only")
    parser.add_argument("--history", type=Path, help="optional local staging directory; no network publication")
    args = parser.parse_args()
    expected = identity.load(args.identity)
    policy = json_value(read_file(args.policy, MAX_METADATA)) if args.policy else None
    evidence = ingest(args.input, args.output, expected, args.conclusion, policy, args.history)
    if evidence["errors"]:
        parser.exit(1, "AWS result ingestion retained diagnostics; see provenance.json.\n")


if __name__ == "__main__":
    main()
