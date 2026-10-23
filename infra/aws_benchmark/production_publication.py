"""Read-only discovery/download of retired production evidence.

This module does not collect, retire, publish, render retained HTML or execute
archives. Its injected clients need only S3 List/Get and consistent DynamoDB Get.
Manifest discovery is separate from a bounded observation of the one active owner.
"""

from dataclasses import dataclass
import re
import time

from . import production_collection as collection
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

    def download(self, key):
        run_key = self.run_key(key)
        record, item = self.record(run_key)
        require(record.closed and record.terminal, "manifest is not retired; publication remains pending")
        raw, ref = self.objects.read(key, collection.MAX_DOCUMENT)
        require(ref["sha256"] == record.retirement_sha256, "retirement digest differs")
        # bind the discovery read to an immutable object version before accepting payload references
        require(self.objects.read(key, collection.MAX_DOCUMENT, version=ref["version"]) == (raw, ref),
                "manifest version changed")
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
