"""Bounded offline AWS result ingestion; never extract, execute or publish worker data."""

import argparse
import copy
import html
import io
import os
from pathlib import Path
import re
import stat
import struct
import tempfile
import zipfile

from benchmarks import changes, matrix, measurements, operations, pairs, report
from benchmarks.strict_json import parse_json
from benchmarks.verification import verification_label
from . import identity
from .json_bytes import encoded
from .common import (BINARY_NAMES, MATRIX_SUITES, MAX_RESULT, SUITES, INSTANCE_TYPE, ValidationError,
                     archive_name_supported, digest, matrix_contract, measurement_contract, verification_contract,
                     verification_contracts, require, utc,
                     DURABLE_PROTOCOL, DIRECT_PROTOCOL, durable_plan_contract, durable_worker_reserve, validate_durable_hashes, validate_durable_window, validate_request)


MAX_METADATA = 1024 * 1024
MAX_JSON = 8 * 1024 * 1024
MAX_ENTRIES = 1001
RESULT = "benchmark/results/results.json"
SERIES_CONTRACT = "aws-local-ext4-v1"
# publication bounds for text that a worker wrote about one cell, and for run notes. They are
# part of public record version 4: records already written must keep validating, so these may
# grow and never shrink.
MAX_CELL_STAGE = 32
MAX_CELL_MESSAGE = 256
MAX_DIAGNOSTICS = 16


CONCLUSIONS = ("success", "failure", "cancelled", "skipped")
# submission-only production workflows may end with any terminal GitHub conclusion;
# this does not relax the legacy execution producer's required successful outcome.
SUBMISSION_CONCLUSIONS = frozenset({"success", "failure", "cancelled", "timed_out", "neutral", "skipped",
                                    "action_required", "stale", "startup_failure"})


def diagnostic(error):
    # generic parsers may echo candidate-controlled input; only our constants are safe
    return str(error) if isinstance(error, ValidationError) else type(error).__name__


def completion_label(done, total):
    return f"{done} of {total} measurement{'' if total == 1 else 's'} completed"


def display_text(value, limit):
    """Bound worker-written text for publication: printable ASCII, cut to a fixed length."""
    if value is None:
        return None
    return "".join(char if " " <= char <= "~" else "?" for char in value[:limit]) or None


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
    require(isinstance(window, dict), "unexpected window fields")
    if "protocol" in window:
        # the versioned plan is part of the historical receipt, never local clock time
        validate_durable_window(window, now=utc(window.get("launch_at", "")), minimum=0, exact=False)
    else:
        require(set(window) == {"run", "baseline", "candidate", "suite", "opens", "deadline"},
                "unexpected window fields")
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
    if "protocol" in window:
        validate_durable_hashes(hashes, window["plan"], exact=False)
    return window, request


def validate_recorded_verification(context, suite, *, exact=False):
    recorded = context.get("verification")
    allowed = (verification_contract(suite),) if exact else verification_contracts(suite)
    require(recorded in allowed, "unexpected suite verification contract")
    return copy.deepcopy(recorded)


def recorded_timing_labels(worker):
    # optional historical observations remain unknown; labels are text, never arbitrary JSON
    labels = dict(runtime_origin=worker.get("runtime_origin"), basis=worker.get("timing_basis"))
    require(all(value is None or isinstance(value, str) and 0 < len(value) <= 256
                and value.isprintable() for value in labels.values()), "invalid worker timing labels")
    return labels


def inspect(source, expected, conclusion):
    evidence = {"schema_version": 1, "identity": expected, "producer_conclusion": conclusion,
                "history_eligible": False, "files": {}, "members": {}, "errors": [],
                "controller": None, "status": None, "worker": None, "raw_revision": None,
                "expected_verification": None}
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
            "instance_id", "image_id", "instance_type"} and isinstance(instance["instance_id"], str)
            and all(instance[key] is None or isinstance(instance[key], str) for key in ("image_id", "instance_type")),
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
        if window.get("protocol") in (DURABLE_PROTOCOL, DIRECT_PROTOCOL):
            recorded_timing_labels(worker)
        if issues:
            evidence["errors"].extend(issues)
            return evidence, None, snapshots
        stage = "measurement record"
        if RESULT in contents:
            value = json_value(contents[RESULT], MAX_JSON)
            if expected["suite"] in MATRIX_SUITES:
                try:
                    result = matrix.validate(value)
                except matrix.Invalid as error:
                    # the runner's validator names fields and never quotes the document
                    raise ValidationError(str(error)) from error
            else:
                require(isinstance(value, dict) and all(isinstance(value.get(key), list) and len(value[key]) <= limit
                        for key, limit in (("cases", 16), ("variants", 16), ("trials", 512), ("summaries", 256))),
                        "measurement collection exceeds reporting limit")
                result = report.validate_result(value)
            evidence["expected_verification"] = validate_recorded_verification(result["context"], expected["suite"])
            evidence["raw_revision"] = copy.deepcopy(result["revision"])
        else:
            evidence["errors"].append("harness result unavailable")
    except Exception as error:
        # format/parser failures always leave diagnostics; never echo arbitrary worker strings
        evidence["errors"].append(stage + ": " + diagnostic(error))
        result = None
    return evidence, result, snapshots


def validate_qualification(evidence, result, policy, *, producer_role="execution"):
    """Validate an authenticated attempt contract without authorizing history publication."""
    expected = evidence["identity"]
    matrix_suite = expected["suite"] in MATRIX_SUITES
    require(isinstance(policy, dict) and set(policy) == {"schema_version", "identity", "archive_sha256",
            "ami_id", "harness_revision"} and type(policy["schema_version"]) is int
            and policy["schema_version"] == 1, "invalid history policy")
    require(policy["identity"] == expected and policy["archive_sha256"] == evidence["files"].get("results.zip"),
            "policy does not approve this exact attempt and archive")
    require(isinstance(policy["ami_id"], str) and re.fullmatch(r"ami-[0-9a-f]{17}", policy["ami_id"])
            and isinstance(policy["harness_revision"], str)
            and re.fullmatch(r"[0-9a-f]{40}", policy["harness_revision"]), "invalid approved AMI provenance")
    require(producer_role in {"execution", "submission"}, "unknown producer responsibility")
    producer_ok = evidence["producer_conclusion"] == "success" or (producer_role == "submission" and
        evidence["producer_conclusion"] in SUBMISSION_CONCLUSIONS)
    intact = not evidence["errors"] and result is not None and producer_ok
    if matrix_suite:
        require(intact, "run evidence is incomplete")
    else:
        require(intact and measurement_contract(expected["suite"])["purpose"] == "performance",
                "diagnostic run cannot enter performance history")
    receipt, worker = evidence["controller"], evidence["worker"]
    instance = receipt.get("instance") or {}
    # a paired run is all or nothing and needs a clean exit. A matrix result stands cell by
    # cell, so a command that was stopped keeps the cells its saved document shows as finished.
    require((matrix_suite or receipt["outcome"] == "success") and receipt["cleanup"] == "verified"
            and instance.get("image_id") == policy["ami_id"] and instance.get("instance_type") == INSTANCE_TYPE
            and worker.get("harness_revision") == policy["harness_revision"], "unverified execution provenance")
    require((matrix_suite or clean_exit(evidence))
            and worker.get("admission", {}).get("accepted") is True
            and worker.get("request") == receipt["request"], "worker did not complete the approved request")
    if receipt["window"].get("protocol") in (DURABLE_PROTOCOL, DIRECT_PROTOCOL):
        window = receipt["window"]
        labels = recorded_timing_labels(worker)
        require(labels == dict(runtime_origin="attempt_started_at" if window["protocol"] == DIRECT_PROTOCOL else "launch_at",
                basis=durable_plan_contract(window["plan"])["timing_basis"]),
                "unverified worker timing labels")
        validate_durable_window(window, now=utc(window["launch_at"]), minimum=0, exact=True)
        validate_request(receipt["request"], window)
        required = durable_worker_reserve(window["plan"], exact=True)
        admission = worker.get("admission", {})
        remaining = admission.get("remaining_seconds")
        require(worker.get("guest_instance_id") == window["instance_id"] == instance.get("instance_id"),
                "durable worker instance binding differs from controller")
        require(admission.get("required_seconds") == required and measurements.nonnegative(remaining)
                and required <= remaining <= window["runtime_seconds"],
                "durable admission does not prove the disclosed workload headroom")
    validate_recorded_verification(result["context"], expected["suite"], exact=True)
    if matrix_suite:
        contract, context = matrix_contract(expected["suite"]), result["context"]
        require(context["purpose"] == contract["purpose"] and context["repository"] == expected["repository"]
                and context["cache_policy"] == contract["cache_policy"]
                and context["runner_label"] == contract["runner_label"]
                and context["repetitions"] == contract["repetitions"]
                and context["timeout_seconds"] == contract["timeout"]
                and [(cell["case_id"], cell["scenario"]) for cell in result["cells"]] == list(contract["cells"]),
                "unexpected matrix measurement contract")
        # one build is measured; the request's baseline copies of the same bytes are not run. Every
        # binary of the plan was run: a suite that times the other tools names them in its plan
        for tool in durable_plan_contract(receipt["window"]["plan"])["binary_names"]:
            require(result["tools"].get(tool, {}).get("sha256") == receipt["request"]["sha256"]["candidate/" + tool],
                    "measured tool does not match approved binary digest")
        return
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
    # approval also validates the current source proof; callers cannot bypass inspection
    report.validate_result(result)


def clean_exit(evidence):
    """Whether the supervisor saw the benchmark command finish with exit status zero."""
    worker = evidence["worker"]
    return worker["status"]["success"] is True and worker.get("service_returncode") == 0


def qualify(evidence, result, policy, *, producer_role="execution"):
    """Qualify one attempt, recording every reason in the evidence; return whether it counts.

    A matrix attempt also gets evidence["cells"]: each cell's outcome whenever the
    document is the approved run's, even if no cell completed, and nothing otherwise.
    """
    matrix_suite = evidence["identity"]["suite"] in MATRIX_SUITES
    if matrix_suite:
        evidence["cells"] = []
    try:
        validate_qualification(evidence, result, policy, producer_role=producer_role)
        if not matrix_suite:
            return True
        clean = clean_exit(evidence)
        cells = [dict(case_id=cell["case_id"], scenario=cell["scenario"], outcome=cell["outcome"]["status"],
                      stage=display_text(cell["outcome"]["stage"], MAX_CELL_STAGE),
                      message=display_text(cell["outcome"]["message"], MAX_CELL_MESSAGE)) for cell in result["cells"]]
    except (ValueError, KeyError, TypeError, AttributeError) as error:
        # a record of the wrong shape is a failed run; an exception that escaped would leave it held
        evidence["errors"].append("qualification: " + diagnostic(error))
        return False
    # the document is the approved run's: its cells are reported even when none completed
    evidence["cells"] = cells
    if not clean:
        evidence["errors"].append("worker: the benchmark command did not exit cleanly")
    if not any(cell["outcome"] == "complete" for cell in cells):
        evidence["errors"].append("qualification: no measurement completed")
        return False
    return True


def approve(evidence, result, policy):
    """Offline history gate; the caller must authenticate this per-attempt policy."""
    validate_qualification(evidence, result, policy)
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
        source_series_ids=[item["series_id"] for item in raw["summaries"]],
        expected_verification=evidence["expected_verification"])
    if worker.get("window", {}).get("protocol") in (DURABLE_PROTOCOL, DIRECT_PROTOCOL):
        provenance["timing"] = {key: copy.deepcopy(worker["window"][key]) for key in
            ("protocol", "plan", "launch_at", "deadline", "runtime_seconds", "outer_deadline", "request_expires")}
        if worker["window"]["protocol"] == DIRECT_PROTOCOL:
            provenance["timing"].update({key: worker["window"][key]
                                        for key in ("attempt_id", "attempt_started_at")})
            provenance["timing"]["outer_deadline_role"] = "control reconciliation horizon; not instance expiry"
        # valid supervisor text remains verbatim; older missing observations stay unknown
        provenance["timing"].update(recorded_timing_labels(worker))
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


def comparison_label(expected, controller):
    if expected["baseline"] != expected["candidate"]:
        return "A/B (different source revisions)"
    request = controller.get("request")
    hashes = request.get("sha256") if isinstance(request, dict) else None
    if not isinstance(hashes, dict):
        hashes = {}
    # the binaries of the recorded plan; a window from before plans has the first three tools
    window = controller.get("window")
    plan = window.get("plan") if isinstance(window, dict) else None
    names = BINARY_NAMES if plan is None else durable_plan_contract(plan, exact=False)["binary_names"]
    if not all(isinstance(hashes.get(role + "/" + name), str)
               and re.fullmatch(r"[0-9a-f]{64}", hashes[role + "/" + name])
               for role in ("baseline", "candidate") for name in names):
        return "Same source revision; binary identity unavailable"
    if all(hashes["baseline/" + name] == hashes["candidate/" + name] for name in names):
        return "A/A (identical source revision and binary digests)"
    return "Same source revision; different binary digests (not A/A)"


def summary(evidence, result):
    expected = evidence["identity"]
    worker, controller = evidence["worker"] or {}, evidence["controller"] or {}
    instance = controller.get("instance") or {}
    lines = ['<h1>AWS benchmark result</h1>',
             '<p><a href="' + html.escape(expected['run_url'], quote=True) + '">Workflow attempt</a></p>',
             '<p>History: ' + ("approved for offline staging" if evidence["history_eligible"] else "diagnostic only; not published") + '</p>']
    matrix_suite = expected["suite"] in MATRIX_SUITES
    # a matrix run measures one build; the request's second copy of the same bytes is not run
    revisions = {"Measured revision": expected["candidate"]} if matrix_suite else {
        "Comparison": comparison_label(expected, controller),
        "Candidate": expected["candidate"], "Baseline": expected["baseline"]}
    fields = {"Run": expected["run"], "Producer conclusion": evidence["producer_conclusion"],
        "Suite": expected["suite"], **revisions,
        "Controller": expected["controller"], "Controller observed AMI": instance.get("image_id", "unknown"),
        "Supervisor reported baked source": worker.get("harness_revision", "unknown"),
        "Raw harness checkout": (evidence["raw_revision"] or {}).get("commit") or "unknown",
        "Controller cleanup": controller.get("cleanup", "unknown"),
        "Archive SHA256": evidence["files"].get("results.zip", "unavailable"),
        "Recorded contract file validation": verification_label(
            {"verification": evidence["expected_verification"]}) if result is not None else "unavailable",
        "Observed file validation": verification_label(result["context"]) if result is not None else "unavailable"}
    if "qualification_accepted" in evidence:
        fields["Qualification"] = "passed; history not published" if evidence["qualification_accepted"] else "failed"
    status = evidence.get("status") or {}
    if status:
        fields.update({"Worker state": status.get("state", "unknown"), "Worker success": status.get("success", "unknown")})
        for key in ("error", "collection_error", "fallback_error", "upload_error", "stop_error"):
            if key in status:
                fields["Worker " + key.replace("_", " ")] = status[key]
    if result is not None and matrix_suite:
        cells = result["cells"]
        done = sum(cell["outcome"]["status"] == "complete" for cell in cells)
        fields["Measurements"] = completion_label(done, len(cells))
    lines.append(table(("Provenance", "Value"), fields.items()))
    if result is not None and matrix_suite:
        lines.append(table(("File set", "Scenario", "Outcome", "Median seconds", "Copies", "Note"), [(
            cell["case_id"], cell["scenario"], cell["outcome"]["status"],
            f"{cell['summary']['median']:.6g}" if cell["summary"] else "-",
            sum(item["status"] == "ok" for item in cell["copies"]),
            ": ".join(part for part in (cell["outcome"]["stage"], cell["outcome"]["message"]) if part))
            for cell in cells]))
    elif result is not None:
        lines.append(table(("Case", "Variant", "Median seconds", "Repeats"),
            [(item['case_id'], item['variant_id'], f"{item['median']:.6g}", len(item['samples'])) for item in result["summaries"]]))
    lines += ['<p>Artifact validation checks bundle integrity and structure; it does not attest that candidate-controlled measurements are honest.</p>',
              '<p>Raw worker evidence is retained separately from derived metadata. Missing provenance stays unknown.</p>']
    if evidence["errors"]:
        lines.append('<p>Diagnostics: ' + "; ".join(map(literal, evidence["errors"])) + '</p>')
    return "\n\n".join(lines) + "\n"


def ingest(source, output, expected, conclusion, policy=None, history=None, *, qualification=None):
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
    if qualification is not None:
        evidence["qualification_accepted"] = False
        try:
            validate_qualification(evidence, raw, qualification)
            evidence["qualification_accepted"] = True
            evidence["qualification_contract_sha256"] = digest(encoded(qualification))
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            evidence["errors"].append("qualification: " + diagnostic(error))
    if policy is not None:
        try:
            approve(evidence, raw, policy)
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            evidence["errors"].append("history approval: " + diagnostic(error))
    # the dashboard and its history read the paired harness format; a matrix result is
    # reported through the summary only
    dashboard = raw is not None and expected["suite"] not in MATRIX_SUITES
    record = derived_record(evidence, raw) if dashboard else None
    if history is not None:
        try:
            require(evidence["history_eligible"] and record is not None, "history staging refused")
            stage_history(record, history)
        except (OSError, ValueError) as error:
            evidence["errors"].append("history staging: " + diagnostic(error))
            evidence["history_eligible"] = False
            record = derived_record(evidence, raw) if dashboard else None
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
