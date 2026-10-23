"""Run-scoped adapter for the reviewed direct allocation and expiry mechanisms.

Admission is a separate service. This adapter cannot reserve a new run or change a
cutoff. Production publication uses a unique instance mailbox; its DDB ownership
checks are snapshots, while the instance prefix and conditional writes prevent a
late publisher from replacing another run's objects. If the active state cannot
be read, the independent EC2 expiry scan still runs; immediate kill-switch
termination requires the readable active assignment and otherwise falls back
to that immutable instance expiry.
"""

import base64
import copy
from datetime import datetime, timedelta, timezone
import json
import os
import re
import time

from benchmarks.strict_json import parse_json

from . import durable
from .common import (BINARIES, MAX_BINARY, MAX_RESULT, RevisionConflict, digest, durable_plan_contract, error_code, get_bytes,
                     pending_status, require, timestamp, utc)
from .durable_aws import AllocationDeferred, Configuration, Service, clients, transient
from .production_mailbox import MailboxPaths
from .production_objects import Objects
from .production_store import MAX_STATE_BYTES, RunStore
from .production_startup import verify_startup

STATUS_FIELDS = frozenset({"run", "state", "success", "sha256", "error", "stop_error",
                           "collection_error", "fallback_error", "upload_error"})


class ProductionService(Service):
    receipt_limit = MAX_STATE_BYTES

    def __init__(self, api, config, run_key, epoch, *, store=None,
                 now=lambda: datetime.now(timezone.utc), sleep=time.sleep, monotonic=time.monotonic):
        super().__init__(api, config, now, sleep=sleep, monotonic=monotonic)
        self.store = store if store is not None else RunStore(api["dynamodb"], config.table, run_key, epoch)
        require(self.store.run_key == run_key and self.store.epoch == epoch, "runtime store identity differs")
        self.run_key, self.epoch, self.record = run_key, epoch, None

    def verify_startup(self, data):
        return verify_startup(data, account=self.config.account, bucket=self.config.bucket, region=self.config.region)

    def validate_state(self, state):
        complete = state.get("phase") == "complete"
        durable.validate(state["request"], exact=not complete)
        pins = state["request"]["pins"]
        keys = ("account", "region", "namespace", "bucket") if complete else durable.PIN_KEYS
        require(all(pins[key] == getattr(self.config, key) for key in keys), "production deployment pins changed")
        self.receipt_bytes(state)

    def load(self):
        state, revision = self.store.load()
        record = self.store.read()
        require(record is not None and not record.closed and
                record.admission["initial_payload"]["state"]["request"] == state["request"],
                "production admission record differs")
        self.validate_state(state)
        self.record = record
        return state, revision

    def save(self, state, revision):
        self.validate_state(state)
        return self.store.save(state, revision)

    def save_launch_intent(self, state, revision):
        self.validate_state(state)
        return self.store.save_launch_intent(state, revision)

    def coordinator(self, event):
        require(isinstance(event, dict) and set(event) == {
            "run_key", "epoch", "run", "execution_arn", "execution_name"}, "unexpected production execution fields")
        require(event["run_key"] == self.run_key and event["epoch"] == self.epoch,
                "production execution owner differs")
        state, _ = self.load()
        require(event["run"] == state["request"]["run"] and
                event["execution_name"] == event["run"] and
                event["execution_arn"] == state.get("submission_arn") and
                isinstance(event["execution_arn"], str) and re.fullmatch(
                    r"arn:aws:states:" + re.escape(self.config.region) + ":" + self.config.account
                    + r":execution:[A-Za-z0-9_-]{1,80}:" + re.escape(event["run"]), event["execution_arn"]),
                "production execution differs from sealed submission")
        if state["phase"] in ("launching", "uncertain"):
            self.retry_launch(state)
            return dict(state="wait", run=state["request"]["run"])
        return super().coordinator({key: event[key] for key in ("run", "execution_arn", "execution_name")})

    def launch_recovery_open(self, state):
        allocation = state.get("attempt")
        return (state["phase"] in ("launching", "uncertain") and state["launch_outcome"] == "uncertain" and
                state["allocation_consumed"] and allocation is not None and not state["binding"] and
                not state["known_instance_ids"] and not state["seen"] and not state["close_reason"] and
                utc(allocation["started_at"]) <= self.now() < utc(state["request"]["capacity_deadline"]) and
                (utc(allocation["close_at"]) - self.now()).total_seconds() >=
                durable.minimum_runtime(state["request"]["plan"]))

    def retry_launch(self, state):
        if not self.launch_recovery_open(state):
            return
        request, allocation = state["request"], state["attempt"]
        self.fresh_guard()
        if self.inventory(request):
            return
        expected = durable.attempt(request, allocation["ordinal"], utc(allocation["started_at"]))
        expected["parameters"] = self.launch_parameters(request, expected)
        require(allocation == expected and state["attempts"] == allocation["ordinal"],
                "saved allocation parameters differ from the immutable attempt")
        self.validate_inputs(state)
        current, revision = self.load()
        if current != state or not self.launch_recovery_open(current):
            return
        observed = self.fresh_guard()
        # the monotonic replay marker fences delayed negative proof from an older sender;
        # a lost checkpoint response never grants this invocation permission to send.
        current["launch_replayed"] = True
        self.store.save_launch_retry(current, revision)
        if self.launch_recovery_open(current) and self.launch_allowed(request, observed, allocation):
            self.launch(request, allocation, observed, prior_uncertain=True)

    def sealed_inputs(self, state):
        sealed = state.get("sealed_inputs")
        require(isinstance(sealed, dict) and set(sealed) == set(BINARIES), "production input seal is missing")
        require(self.record is not None, "production admission record has not been loaded")
        manifest = self.record.admission["initial_payload"]["manifest"]["binaries"]
        for binary, entry in sealed.items():
            require(isinstance(entry, dict) and set(entry) == {"key", "version", "sha256", "bytes"},
                    "unexpected sealed input fields")
            require(entry["key"] == "production/staging/" + self.run_key + "/" + binary,
                    "sealed input belongs to another run")
            require(isinstance(entry["version"], str) and entry["version"] not in ("", "null") and
                    len(entry["version"]) <= 1024 and all(32 < ord(c) < 127 for c in entry["version"]),
                    "sealed input version is missing")
            require(type(entry["bytes"]) is int and 0 < entry["bytes"] <= MAX_BINARY and
                    isinstance(entry["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) and
                    entry["sha256"] == state["request"]["sha256"][binary] and
                    {key: entry[key] for key in ("sha256", "bytes")} == manifest[binary],
                    "sealed binary differs from admitted manifest")
        return sealed

    @staticmethod
    def verify_head(head, entry, *, version=None):
        expected = base64.b64encode(bytes.fromhex(entry["sha256"])).decode("ascii")
        require(type(head.get("ContentLength")) is int and head["ContentLength"] == entry["bytes"] and
                head.get("ChecksumSHA256") == expected and head.get("ChecksumType", "FULL_OBJECT") == "FULL_OBJECT"
                and head.get("ServerSideEncryption") == "AES256", "production binary bytes or encryption differ")
        observed = head.get("VersionId")
        require(isinstance(observed, str) and observed not in ("", "null") and len(observed) <= 1024 and
                all(32 < ord(c) < 127 for c in observed) and (version is None or observed == version),
                "production binary version differs")

    def validate_inputs(self, state):
        for entry in self.sealed_inputs(state).values():
            head = self.api["s3"].head_object(Bucket=self.config.bucket, Key=entry["key"],
                                             VersionId=entry["version"], ChecksumMode="ENABLED")
            self.verify_head(head, entry, version=entry["version"])

    def paths(self, state):
        binding = state.get("binding")
        require(isinstance(binding, dict), "worker mailbox has no instance binding")
        return MailboxPaths.for_instance(self.config.account, self.config.region, binding["instance_id"])

    def publication_current(self, expected):
        current, _ = self.load()
        require(current["phase"] == "requested" and not current["close_reason"] and current["runtime_verified"] and
                all(current[key] == expected[key] for key in
                    ("request", "binding", "attempt", "publication", "sealed_inputs")),
                "production publication no longer owns this assignment")
        require(self.now() < utc(current["binding"]["deadline"]), "production assignment deadline exhausted")
        return current

    def copy_binary(self, destination, entry):
        # create-only copying avoids both overwritten evidence and ambiguous missing-object HEAD403 responses
        try:
            self.api["s3"].copy_object(Bucket=self.config.bucket, Key=destination,
                CopySource={"Bucket": self.config.bucket, "Key": entry["key"], "VersionId": entry["version"]},
                ChecksumAlgorithm="SHA256", ServerSideEncryption="AES256", MetadataDirective="REPLACE", AnnotationDirective="EXCLUDE",
                ContentType="application/octet-stream", IfNoneMatch="*")
        except Exception as error:
            if error_code(error) not in ("PreconditionFailed", "412"):
                raise
        head = self.api["s3"].head_object(Bucket=self.config.bucket, Key=destination, ChecksumMode="ENABLED")
        self.verify_head(head, entry)

    def immutable_json(self, key, value):
        data = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        require(len(data) <= 16_384, "production worker message exceeds bound")
        try:
            self.api["s3"].put_object(Bucket=self.config.bucket, Key=key, Body=data,
                ContentType="application/json", ServerSideEncryption="AES256", IfNoneMatch="*")
        except Exception as error:
            if error_code(error) not in ("PreconditionFailed", "412"):
                raise
            require(get_bytes(self.api["s3"], self.config.bucket, key, 16_384) == data,
                    "existing production assignment differs")

    def publish_worker(self, state):
        state = self.publication_current(state)
        paths = self.paths(state)
        for binary, entry in self.sealed_inputs(state).items():
            self.publication_current(state)
            self.copy_binary(paths.binary(binary), entry)
        self.publication_current(state)
        self.immutable_json(paths.request, durable.worker_command(state["request"], state["binding"]))
        self.publication_current(state)
        self.immutable_json(paths.window, durable.window(state["request"], state["binding"]))

    def read_worker_status(self, state):
        paths, objects = self.paths(state), Objects(self.api["s3"], self.config.bucket)
        committed = state.get("worker_output")
        if committed is not None:
            require(isinstance(committed, dict) and set(committed) == {"status", "results"},
                    "worker output commitment differs")
            data = objects.referenced(committed["status"], paths.status, 16_384)
            reference = committed["status"]
        else:
            observed = objects.read(paths.status, 16_384, optional=True)
            if observed is None:
                return pending_status(state["request"]["run"])
            data, reference = observed
        value = parse_json(data.decode("utf-8"))
        require(isinstance(value, dict) and set(value) <= STATUS_FIELDS and
                value.get("run") == state["request"]["run"] and value.get("state") == "completed" and
                type(value.get("success")) is bool, "invalid production terminal status")
        if value["success"] or "sha256" in value:
            require(isinstance(value.get("sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", value["sha256"]),
                    "production status has no valid archive digest")
        if committed is None:
            archive = objects.head(paths.results, MAX_RESULT, sha256=value["sha256"]) if "sha256" in value else None
            state["worker_output"] = dict(status=reference, results=archive)
        else:
            require((committed["results"] is None and "sha256" not in value) or
                    (isinstance(committed["results"], dict) and committed["results"].get("key") == paths.results and
                     committed["results"].get("sha256") == value.get("sha256")), "archive commitment differs from status")
        return value

    def observe_closed_worker(self, state):
        if state["phase"] == "closing" and state["binding"] and not state.get("worker_output"):
            # closure reason is not an evidence inventory; preserve late successful or failed output alike.
            self.read_worker_status(state)

    def watchdog_state(self, state, instances):
        response = self.api["dynamodb"].get_item(TableName=self.config.table,
            Key={"pk": {"S": "policy"}}, ConsistentRead=True)
        enabled = (response.get("Item") or {}).get("enabled", {}).get("BOOL")
        require(type(enabled) is bool, "production kill switch is unavailable")
        if not enabled:
            state.update(phase="closing", close_reason=state["close_reason"] or "production kill switch is off")
        if state["phase"] == "awaiting-inputs":
            expiry = self.record.admission["initial_payload"]["upload_deadline"]
            if self.now() >= utc(expiry):
                state.update(phase="closing", close_reason=state["close_reason"] or "production input upload expired")

    def root_cleanup(self, state):
        # exact-ID disappearance is useful only after every launch/producer horizon,
        # and only together with the already bound root's independent deletion proof.
        after = utc(state["request"]["outer_deadline"]) + timedelta(
            seconds=durable_plan_contract(state["request"]["plan"])["delivery_drain_seconds"])
        require(self.now() >= after, "physical reconciliation horizon has not passed")
        candidate = copy.deepcopy(state)
        for identity in candidate["known_instance_ids"]:
            seen = candidate["seen"].get(identity)
            if seen is None or seen["state"] == "terminated":
                continue
            require(candidate["launch_outcome"] == "accepted" and len(seen["root_volumes"]) == 1,
                    "exact instance reconciliation lacks accepted identity or root proof")
            try:
                response = self.api["ec2"].describe_instances(InstanceIds=[identity])
            except Exception as error:
                if error_code(error) != "InvalidInstanceID.NotFound":
                    raise
                seen["state"] = "terminated"
            else:
                instances = [instance for reservation in response["Reservations"] for instance in reservation["Instances"]]
                require(not response.get("NextToken") and len(instances) == 1 and instances[0].get("InstanceId") == identity and
                        instances[0].get("State", {}).get("Name") == "terminated",
                        "exact instance probe did not prove termination")
                durable.validate_allocation_instance(candidate["request"], instances[0], candidate["attempt"],
                                                     reconciliation=True)
                roots = [entry["Ebs"]["VolumeId"] for entry in instances[0].get("BlockDeviceMappings", [])
                         if "Ebs" in entry and entry.get("DeviceName") == instances[0].get("RootDeviceName")]
                require((seen["launch_at"] is None or seen["launch_at"] == timestamp(instances[0]["LaunchTime"])) and
                        (not roots or roots == seen["root_volumes"]), "exact instance proof changed its binding")
                self.remember_instance(candidate, instances[0])
        deleted = super().root_cleanup(candidate)
        state["seen"] = candidate["seen"]
        return deleted

    def receipt(self, state):
        current, _ = self.load()
        if current != state and current["phase"] != "complete" and current["request"] == state["request"]:
            raise RevisionConflict("receipt state advanced under the same active owner; retry from a fresh load")
        require(state == current, "production receipt is not the current committed state")
        data = self.receipt_bytes(state)
        self.api["s3"].put_object(Bucket=self.config.bucket,
            Key="production/runs/" + self.run_key + "/durable-receipt.json",
            Body=data, ContentType="application/json", ServerSideEncryption="AES256",
            ChecksumSHA256=base64.b64encode(bytes.fromhex(digest(data))).decode("ascii"))


def retry_budget(service, context):
    if context is not None:
        service.retry_until = service.monotonic() + max(0, context.get_remaining_time_in_millis() / 1000 - 5)


def coordinator(event, context):
    require(os.environ.get("RCP_PRODUCTION_ENABLED") == "true", "production runtime is disabled")
    require(isinstance(event, dict) and set(event) == {
        "run_key", "epoch", "run", "execution_arn", "execution_name"}, "unexpected production execution fields")
    config = Configuration.environment()
    service = ProductionService(clients(config.region), config, event["run_key"], event["epoch"])
    retry_budget(service, context)
    try:
        return service.coordinator(event)
    except Exception as error:
        if isinstance(error, AllocationDeferred) or transient(error):
            return dict(state="wait", run=event["run"])
        raise


def watchdog(event, context):
    config = Configuration.environment()
    api = clients(config.region)
    # the scan and its expiry terminations precede every active-pointer read, including idle invocations
    instances = Service(api, config).independent_scan()
    response = api["dynamodb"].get_item(TableName=config.table, Key={"pk": {"S": "active"}}, ConsistentRead=True)
    item = response.get("Item")
    if item is None:
        return dict(state="idle")
    require(isinstance(item, dict) and set(item) == {"pk", "run_key", "epoch"} and
            item["pk"] == {"S": "active"} and all(isinstance(item[key], dict) and set(item[key]) == {"S"}
                                                 for key in ("run_key", "epoch")),
            "production active pointer is malformed")
    service = ProductionService(api, config, item["run_key"]["S"], item["epoch"]["S"])
    retry_budget(service, context)
    try:
        return service.watchdog(instances=instances)
    except Exception as error:
        if transient(error):
            return dict(state="deferred")
        raise
