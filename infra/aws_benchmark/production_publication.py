"""Read-only discovery/download of retired production evidence.

This module does not collect, retire, publish, render retained HTML or execute
archives. Its injected clients need only S3 List/Get and consistent DynamoDB Get.
Manifest discovery is separate from a bounded observation of the one active owner.
"""

from dataclasses import dataclass
import re
import time

from . import production_collection as collection, production_projection as projection
from .common import MAX_RESULT, digest, require
from .production_store import PHASES, RunStore

MAX_PAGES = 20
PAGE_SIZE = 500
MAX_RUNS = 500
MAX_DISCOVERY_SECONDS = 120
MAX_BUNDLE_BYTES = MAX_RESULT + 8 * 1024 * 1024
LIMITS = {"durable.json": collection.MAX_RECEIPT, "status.json": collection.MAX_DOCUMENT,
          "results.zip": MAX_RESULT, "controller.json": collection.MAX_DOCUMENT,
          "report.json": collection.MAX_REPORT, "summary.html": collection.MAX_REPORT}


def _reference(ref, key, limit):
    require(isinstance(ref, dict) and set(ref) == {"key", "version", "sha256", "bytes"} and
            ref["key"] == key and isinstance(ref["version"], str) and
            0 < len(ref["version"]) <= 1024 and ref["version"] != "null" and
            all(32 < ord(char) < 127 for char in ref["version"]) and
            isinstance(ref["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", ref["sha256"]) and
            type(ref["bytes"]) is int and 0 < ref["bytes"] <= limit, "retained reference differs")


def _public_manifest(record, manifest):
    """Validate the retirement-anchored envelope without downloading raw evidence."""
    binding = dict(schema=collection.SCHEMA, run_key=record.run_key, epoch=record.epoch,
        state_sha256=digest(collection.encoded(record.state)),
        admission_sha256=digest(collection.encoded(record.admission)), identity=collection.identity(record))
    require(isinstance(manifest, dict) and set(manifest) == set(binding) | {
        "execution", "producer_conclusion", "sources", "retained", "benchmark_valid"} and
        all(manifest.get(key) == value for key, value in binding.items()) and
        type(manifest["benchmark_valid"]) is bool and manifest["producer_conclusion"] in collection.CONCLUSIONS and
        isinstance(manifest["execution"], dict) and
        manifest["execution"].get("status") in collection.TERMINAL | {"NEVER_SUBMITTED", "NEVER_STARTED"},
        "retained manifest belongs to different evidence")
    refs, sources = manifest["retained"], manifest["sources"]
    require(isinstance(refs, dict) and {"durable.json", "controller.json", "report.json", "summary.html"}
            <= set(refs) <= set(LIMITS), "retained file set differs")
    prefix = "production/runs/" + record.run_key + "/collection/"
    for name, ref in refs.items():
        _reference(ref, prefix + name, LIMITS[name])
    require(isinstance(sources, dict) and set(sources) == set(refs) - {
        "controller.json", "report.json", "summary.html"}, "source references differ")
    keys = {"durable.json": "production/runs/" + record.run_key + "/durable-receipt.json"}
    if record.state.get("binding") is not None:
        pins = record.state["request"]["pins"]
        paths = collection.MailboxPaths.for_instance(pins["account"], pins["region"],
                                                     record.state["binding"]["instance_id"])
        keys.update({"status.json": paths.status, "results.zip": paths.results})
    for name, ref in sources.items():
        require(name in keys, "source has no bound worker")
        _reference(ref, keys[name], LIMITS[name])
        require(all(ref[field] == refs[name][field] for field in ("sha256", "bytes")), "source binding differs")
        field = {"status.json": "status", "results.zip": "results"}.get(name)
        committed = (record.state.get("worker_output") or {}).get(field) if field else None
        require(committed is None or committed == ref, "retained source differs from committed worker version")
    return binding


def _frozen_public(record, manifest, report, controller):
    """Authenticate the collector's projection, not a fresh qualification of raw data."""
    expected, refs = manifest["identity"], manifest["retained"]
    require(isinstance(controller, dict) and controller.get("schema_version") == 1 and
            controller.get("identity") == expected and controller.get("cleanup") == "verified" and
            controller.get("outcome") in {"success", "failure"}, "retained controller provenance differs")
    bound, request = record.state.get("binding"), record.state["request"]
    if bound is None:
        require(all(controller.get(key) is None for key in ("instance", "window", "request")),
                "retained unallocated controller differs")
    else:
        require(controller.get("instance") == dict(instance_id=bound["instance_id"],
            image_id=request["pins"]["ami"], instance_type="c5d.24xlarge"), "retained instance differs")
        window, command = controller.get("window", {}), controller.get("request", {})
        require(isinstance(window, dict) and
                all(window.get(key) == request[key] for key in
                    ("protocol", "run", "baseline", "candidate", "suite", "runtime_seconds", "plan", "outer_deadline")) and
                all(window.get(key) == bound[key] for key in
                    ("instance_id", "launch_at", "deadline", "attempt_id", "attempt_started_at")) and
                window.get("opens") == bound["attempt_started_at"] and
                window.get("request_expires") == request["capacity_deadline"], "retained worker window differs")
        require(isinstance(command, dict) and
                all(command.get(key) == request[key] for key in ("run", "baseline", "candidate", "suite", "sha256")) and
                command.get("deadline") == bound["deadline"], "retained worker request differs")
    for field, name in (("status_sha256", "status.json"), ("archive_sha256", "results.zip")):
        require(controller.get(field) == (refs[name]["sha256"] if name in refs else None),
                "retained controller digest differs")
    evidence = report.get("evidence", {}) if isinstance(report, dict) else {}
    require(isinstance(report, dict) and report.get("schema") == manifest["schema"] and
            report.get("identity") == expected and type(report.get("benchmark_valid")) is bool and
            report["benchmark_valid"] == manifest["benchmark_valid"] and report.get("cleanup_verified") is True and
            report.get("execution_proof") == manifest["execution"] and isinstance(evidence, dict) and
            evidence.get("identity") == expected and evidence.get("producer_conclusion") == manifest["producer_conclusion"] and
            evidence.get("controller") == controller and evidence.get("files") == {
                name: refs[name]["sha256"] for name in ("controller.json", "status.json", "results.zip") if name in refs},
            "retained report provenance differs")
    outcome = report.get("qualification_outcome")
    require(outcome in {"passed", "failed", "evidence-unavailable"} and
            (outcome == "passed") == manifest["benchmark_valid"] and
            (outcome == "evidence-unavailable") == (report.get("evidence_availability") is not None),
            "retained qualification outcome differs")
    value = projection.bind(report["public_projection"], record.retirement_sha256)
    require(value["run_key"] == record.run_key and value["benchmark_valid"] == manifest["benchmark_valid"] and
            value["identity"] == {key: expected[key] for key in
                ("run", "run_url", "suite", "baseline", "candidate", "controller")} and
            value["comparison"] == projection.comparison_label(expected, controller) and
            projection.qualification_outcome(value) == outcome and
            (not value["benchmark_valid"] or value["verification"] == evidence.get("expected_verification")),
            "retained public projection provenance differs")
    if "collection_checkpoint" in report:
        binding = {key: manifest[key] for key in ("schema", "run_key", "epoch", "state_sha256",
            "admission_sha256", "identity", "execution", "producer_conclusion")}
        require(report["collection_checkpoint"] == dict(schema=collection.CHECKPOINT_SCHEMA, binding=binding,
            sources=manifest["sources"], derived={name: {field: refs[name][field] for field in ("sha256", "bytes")}
                for name in ("controller.json", "summary.html")}), "retained collection checkpoint differs")
        collected_at = value.get("collected_at")
        require(collected_at is not None and collection.timestamp(collection.utc(collected_at)) == collected_at and
                collection.utc(collected_at) >= collection.utc(record.state["cleanup"]["verified_at"]),
                "retained collection time differs")
    return value


@dataclass(frozen=True)
class Bundle:
    run_key: str
    record: object
    record_item: dict
    manifest_bytes: bytes
    manifest_ref: dict
    payloads: dict

    def validate(self):
        # live downloads retain both the SDK item and decoded record. offline loading
        # reconstructs the item, so equality there checks consistency, not authenticity;
        # the caller must bind that bundle to the trusted download inventory.
        decoded = RunStore(None, "offline", self.run_key, self.record.epoch)._record(self.record_item)
        require(decoded == self.record, "downloaded tombstone differs from decoded record")
        require(self.record.run_key == self.run_key and self.record.closed and self.record.terminal,
                "publication requires a retired terminal record")
        expected = "production/runs/" + self.run_key + "/collection/manifest.json"
        ref = self.manifest_ref
        require(isinstance(ref, dict) and set(ref) == {"key", "version", "sha256", "bytes"} and
                ref["key"] == expected and isinstance(ref["version"], str) and
                0 < len(ref["version"]) <= 1024 and ref["version"] != "null" and
                all(32 < ord(char) < 127 for char in ref["version"]) and
                isinstance(self.manifest_bytes, bytes) and 0 < len(self.manifest_bytes) <= collection.MAX_DOCUMENT and
                type(ref["bytes"]) is int and ref["bytes"] == len(self.manifest_bytes) and
                ref["sha256"] == self.record.retirement_sha256 == digest(self.manifest_bytes),
                "manifest is not bound to the retirement tombstone")
        manifest = collection.results.json_value(self.manifest_bytes)
        collection.validate_retained(self.record, manifest, self.payloads)
        require(sum(map(len, self.payloads.values())) + len(self.manifest_bytes) <= MAX_BUNDLE_BYTES,
                "complete publication bundle exceeds bound")
        return manifest


class Reader:
    def __init__(self, s3, dynamodb, *, bucket, table, repository_id, clock=time.monotonic):
        require(isinstance(repository_id, str) and re.fullmatch(r"[1-9][0-9]{0,19}", repository_id),
                "invalid publication repository ID")
        require(isinstance(bucket, str) and re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", bucket),
                "invalid publication bucket")
        require(isinstance(table, str) and re.fullmatch(r"[A-Za-z0-9_.-]{3,255}", table),
                "invalid publication table")
        self.s3, self.db, self.bucket, self.table, self.repository_id, self.clock = (
            s3, dynamodb, bucket, table, repository_id, clock)
        self.prefix = "production/runs/repo-" + repository_id + "-run-"
        self.pattern = re.compile(re.escape(self.prefix) + r"([1-9][0-9]{0,19})/collection/manifest[.]json")
        self.objects = collection.Objects(s3, bucket)

    def run_key(self, key):
        require(isinstance(key, str) and self.pattern.fullmatch(key), "invalid publication manifest key")
        return key.split("/")[2]

    def discover(self):
        """Complete bounded traversal, not an atomic snapshot; rescan the full prefix later."""
        started, token, tokens, keys, found = self.clock(), None, set(), set(), []
        for _ in range(MAX_PAGES):
            require(self.clock() - started <= MAX_DISCOVERY_SECONDS, "manifest discovery exceeded time bound")
            args = dict(Bucket=self.bucket, Prefix=self.prefix, MaxKeys=PAGE_SIZE)
            if token is not None:
                args["ContinuationToken"] = token
            page = self.s3.list_objects_v2(**args)
            require(not page.get("CommonPrefixes") and not page.get("Delimiter"),
                    "manifest inventory unexpectedly groups keys")
            items = page.get("Contents", [])
            require(isinstance(items, list) and len(items) <= PAGE_SIZE and
                    type(page.get("KeyCount")) is int and page["KeyCount"] == len(items) and
                    type(page.get("IsTruncated")) is bool, "manifest inventory is incomplete")
            for item in items:
                key = item.get("Key") if isinstance(item, dict) else None
                require(isinstance(key, str) and key.startswith(self.prefix) and len(key.encode()) <= 1024 and
                        key not in keys, "manifest inventory contains invalid or duplicate keys")
                keys.add(key)
                if self.pattern.fullmatch(key):
                    found.append(key)
                    require(len(found) <= MAX_RUNS, "manifest inventory exceeds run bound")
            next_token = page.get("NextContinuationToken")
            require(self.clock() - started <= MAX_DISCOVERY_SECONDS, "manifest discovery exceeded time bound")
            if not page["IsTruncated"]:
                require(next_token is None, "terminal manifest page has a continuation")
                return sorted(found)
            require(items and isinstance(next_token, str) and 0 < len(next_token) <= 4096 and
                    next_token not in tokens, "manifest continuation is missing or repeated")
            tokens.add(next_token)
            token = next_token
        raise ValueError("manifest discovery exceeds page bound")

    def record(self, run_key):
        require(re.fullmatch(r"repo-" + self.repository_id + r"-run-[1-9][0-9]{0,19}", run_key),
                "record belongs to another repository")
        response = self.db.get_item(TableName=self.table, Key={"pk": {"S": "run#" + run_key}}, ConsistentRead=True)
        item = response.get("Item")
        require(isinstance(item, dict), "publication run record is missing")
        # the service record selects its epoch; a manifest cannot select a different owner
        epoch = item.get("epoch", {}).get("S")
        store = RunStore(None, self.table, run_key, epoch)
        return store._record(item), item

    def active_observation(self):
        """Observe the one owner without scanning or asserting a cross-service snapshot."""
        args = dict(TableName=self.table, Key={"pk": {"S": "active"}}, ConsistentRead=True)
        owner = self.db.get_item(**args).get("Item")
        if owner is None:
            return dict(state="idle", retired=False, reason="no active owner observed")
        require(isinstance(owner, dict) and set(owner) == {"pk", "run_key", "epoch"} and
                owner["pk"] == {"S": "active"}, "invalid active publication owner")
        run_key = owner.get("run_key", {}).get("S")
        record, _ = self.record(run_key)
        require(RunStore(None, self.table, run_key, record.epoch)._owner(owner) and not record.closed,
                "active owner differs from run record")
        require(self.db.get_item(**args).get("Item") == owner, "active owner changed during observation")
        phase = record.state["phase"]
        require(phase in PHASES, "invalid active phase")
        return dict(state="pending", retired=False, run_key=run_key, phase=phase,
                    reason="ownership remains held until collection and retirement")

    def _retired_manifest(self, key):
        run_key = self.run_key(key)
        record, item = self.record(run_key)
        require(record.closed and record.terminal, "manifest is not retired; publication remains pending")
        raw, ref = self.objects.read(key, collection.MAX_DOCUMENT)
        require(ref["sha256"] == record.retirement_sha256, "retirement digest differs")
        # bind the discovery read to an immutable object version before accepting payload references
        require(self.objects.read(key, collection.MAX_DOCUMENT, version=ref["version"]) == (raw, ref),
                "manifest version changed")
        return record, item, raw, ref

    def download(self, key):
        run_key = self.run_key(key)
        record, item, raw, ref = self._retired_manifest(key)
        manifest = collection.results.json_value(raw)
        refs = manifest.get("retained") if isinstance(manifest, dict) else None
        require(isinstance(refs, dict) and {"durable.json", "controller.json", "report.json", "summary.html"}
                <= set(refs) <= set(LIMITS), "retained file set differs")
        prefix = "production/runs/" + run_key + "/collection/"
        payloads = {name: self.objects.referenced(ref, prefix + name, LIMITS[name]) for name, ref in refs.items()}
        bundle = Bundle(run_key, record, item, raw, ref, payloads)
        bundle.validate()
        require(self.record(run_key) == (record, item), "retirement record changed during download")
        return bundle

    def public_record(self, key):
        """Read only the retirement-bound frozen projection and its controller context.

        This authenticates collector-produced public bytes. It deliberately does not
        download or requalify raw worker output, durable receipts or retained HTML.
        """
        record, item, raw, _ = self._retired_manifest(key)
        manifest = collection.results.json_value(raw)
        _public_manifest(record, manifest)
        prefix = "production/runs/" + record.run_key + "/collection/"
        payloads = {name: self.objects.referenced(manifest["retained"][name], prefix + name, LIMITS[name])
                    for name in ("report.json", "controller.json")}
        value = _frozen_public(record, manifest,
            collection.results.json_value(payloads["report.json"], collection.MAX_REPORT),
            collection.results.json_value(payloads["controller.json"], collection.MAX_DOCUMENT))
        require(self.record(record.run_key) == (record, item), "retirement record changed during download")
        return value

    def public_diagnostics(self, key, public_record, *, maximum_bytes):
        """Read an optional retirement-bound archive without changing its frozen verdict.

        The caller supplies the already authenticated frozen record. Diagnostics
        are supplemental: unavailable/expired raw data cannot erase that record.
        """
        from . import production_diagnostics as diagnostics
        require(type(maximum_bytes) is int and maximum_bytes >= 0, "invalid diagnostic read budget")
        manifest_cost = 2 * (collection.MAX_DOCUMENT + 2)
        if maximum_bytes < manifest_cost:
            return diagnostics.unavailable(public_record, "raw_budget"), 0
        consumed = manifest_cost
        try:
            record, item, raw, _ = self._retired_manifest(key)
            manifest = collection.results.json_value(raw)
            _public_manifest(record, manifest)
            require(public_record["run_key"] == record.run_key and
                    public_record["retirement_sha256"] == record.retirement_sha256 and
                    public_record["benchmark_valid"] == manifest["benchmark_valid"] and
                    public_record["identity"] == {name: manifest["identity"][name] for name in
                        ("run", "run_url", "suite", "baseline", "candidate", "controller")},
                    "diagnostic frozen record identity differs")
            ref = manifest["retained"].get("results.zip")
            if ref is None:
                return diagnostics.unavailable(public_record, "raw_unavailable"), consumed
            raw_cost = ref["bytes"] + 2  # Objects.read checks two possible trailing bytes.
            if raw_cost + manifest_cost > maximum_bytes:
                return diagnostics.unavailable(public_record, "raw_budget"), consumed
            # Reserve before the request: a failed/checksum-invalid body still consumes bytes.
            consumed += raw_cost
            archive = self.objects.referenced(ref,
                "production/runs/" + record.run_key + "/collection/results.zip", ref["bytes"])
            value = diagnostics.from_archive(public_record, archive)
            require(self.record(record.run_key) == (record, item),
                    "retirement changed during diagnostic read")
            return value, consumed
        except Exception:
            # raw SDK bodies, archive names and local paths are never public reasons.
            return diagnostics.unavailable(public_record, "raw_unavailable"), consumed

    def observe_public(self, key):
        """Observe the frozen public path with the same pending/held semantics as raw evidence."""
        retired = False
        try:
            record, _ = self.record(self.run_key(key))
            retired = record.closed
            if not retired:
                return dict(state="pending", retired=False, reason="manifest exists but ownership is not retired")
            return dict(state="ready", retired=True, record=self.public_record(key))
        except Exception as error:
            return dict(state="held", retired=retired, reason=collection.results.diagnostic(error))

    def observe(self, key):
        """Describe an individual failure without losing known retirement or exposing SDK secrets."""
        retired = False
        try:
            record, _ = self.record(self.run_key(key))
            retired = record.closed
            if not retired:
                return dict(state="pending", retired=False, reason="manifest exists but ownership is not retired")
            bundle = self.download(key)
            return dict(state="ready", retired=True, bundle=bundle)
        except Exception as error:
            return dict(state="held", retired=retired, reason=collection.results.diagnostic(error))
