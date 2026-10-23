"""Short authenticated production submission; builds never receive launch credentials.

This module is not an enabled Lambda entrypoint. Deployment must supply a verified
server policy, pinned configuration, narrowly scoped clients and an outer execution
deadline. Input URLs are bearer capabilities and must never be logged or retained.
"""

import base64
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re

from . import durable
from .common import BINARIES, require, timestamp, utc, error_code, retryable_io
from .durable_client import execution_arn
from .production_policy import authorize, canonical, manifest, Policy, MAX_RUN_AGE_SECONDS
from .production_store import AdmissionDecision, RunStore

UPLOAD_SECONDS = 600


def run_key(identity):
    return "repo-" + identity.repository_id + "-run-" + identity.run_id


def epoch_for(key):
    return hashlib.sha256(key.encode()).hexdigest()


def staged_key(key, binary):
    require(re.fullmatch(r"repo-[1-9][0-9]{0,19}-run-[1-9][0-9]{0,19}", key)
            and binary in BINARIES, "invalid immutable upload key")
    return "production/staging/" + key + "/" + binary


def receipt(record):
    request = record.state["request"]
    return dict(schema_version=1, run_key=record.run_key, run=request["run"], epoch=record.epoch,
        state="retired" if record.closed else record.state["phase"],
        reserve_microusd=record.admission["reserve_microusd"],
        capacity_deadline=request["capacity_deadline"], outer_deadline=request["outer_deadline"],
        runtime_seconds=request["runtime_seconds"], reservation_created=True)


class Admission:
    def __init__(self, api, config, machine_arn, policy_reader, verifier, run_reader, *,
                 now=lambda: datetime.now(timezone.utc), store_factory=RunStore):
        self.api, self.config, self.machine = api, config, machine_arn
        self.policy_reader, self.verifier, self.run_reader = policy_reader, verifier, run_reader
        self.now, self.store_factory = now, store_factory
        require(re.fullmatch(r"arn:aws:states:" + re.escape(config.region) + ":" + config.account
                + r":stateMachine:[A-Za-z0-9_-]{1,80}", machine_arn), "state machine scope differs")

    def store(self, identity):
        key = run_key(identity)
        return self.store_factory(self.api["dynamodb"], self.config.table, key, epoch_for(key))

    def policy(self):
        value = self.policy_reader()
        require(isinstance(value, Policy), "admission requires a validated server policy")
        return value

    def upload_urls(self, identity, record):
        original = record.admission["initial_payload"]
        require(identity.run_attempt == original["producer"]["run_attempt"],
                "a workflow rerun cannot resume an earlier producer's uploads")
        deadline = min(utc(original["upload_deadline"]), utc(record.state["request"]["capacity_deadline"]))
        remaining = int((deadline - self.now()).total_seconds())
        require(0 < remaining <= UPLOAD_SECONDS and record.state["phase"] == "awaiting-inputs",
                "input upload period is closed")
        result = receipt(record)
        result["upload_deadline"] = timestamp(deadline)
        result["uploads"] = {}
        for binary, entry in original["manifest"]["binaries"].items():
            checksum = base64.b64encode(bytes.fromhex(entry["sha256"])).decode("ascii")
            parameters = dict(Bucket=self.config.bucket, Key=staged_key(record.run_key, binary),
                ContentLength=entry["bytes"], ChecksumSHA256=checksum, IfNoneMatch="*",
                ContentType="application/octet-stream", ServerSideEncryption="AES256")
            result["uploads"][binary] = dict(url=self.api["s3"].generate_presigned_url("put_object",
                Params=parameters, ExpiresIn=remaining, HttpMethod="PUT"),
                headers={"Content-Length": str(entry["bytes"]), "x-amz-checksum-sha256": checksum,
                    "If-None-Match": "*", "Content-Type": "application/octet-stream",
                    "x-amz-server-side-encryption": "AES256"})
        return result

    def begin(self, token, supplied_manifest):
        identity = self.verifier.verify(token)
        store = self.store(identity)
        checked = manifest(supplied_manifest)
        existing = store.read()
        if existing is not None:
            # run identity deduplicates across all attempts, independently of policy changes
            if existing.closed or existing.state["phase"] != "awaiting-inputs" or identity.run_attempt !=                     existing.admission["initial_payload"]["producer"]["run_attempt"]:
                return receipt(existing)
            require(checked == existing.admission["initial_payload"]["manifest"], "existing upload manifest differs")
            self.policy().reservation(self.now())
            self.run_reader.read(identity)
            store.load()  # require the same active slot before issuing scoped upload capabilities
            return self.upload_urls(identity, existing)
        require(identity.run_attempt == "1", "workflow reruns cannot create a new reservation")
        policy = self.policy()
        policy.reservation(self.now())
        observed = self.run_reader.read(identity)
        approved = authorize(policy, identity, observed, checked, self.now())
        require((self.now() - utc(observed.created_at)).total_seconds() + UPLOAD_SECONDS < MAX_RUN_AGE_SECONDS,
                "workflow run has insufficient time for input upload")
        created = self.now()
        producer = dict(run=identity.run_id + "-" + identity.run_attempt,
            baseline=checked["baseline"], candidate=checked["candidate"], suite=checked["suite"],
            controller=identity.workflow_sha)
        request = durable.request(producer, created,
            {name: value["sha256"] for name, value in checked["binaries"].items()}, self.config.pins(),
            runtime_seconds=policy.runtime_seconds, capacity_seconds=policy.capacity_seconds)
        state = durable.initial_lease(request)
        state["phase"] = "awaiting-inputs"
        state["sealed_inputs"] = None
        state["submission_arn"] = None
        payload = dict(producer=approved["producer"], manifest=checked,
            upload_deadline=timestamp(created + timedelta(seconds=UPLOAD_SECONDS)), state=state)
        decision = AdmissionDecision(**{key: approved[key] for key in (
            "run_key", "epoch", "fingerprint", "reserve_microusd", "month", "day", "schedule_slot")},
            policy_version=policy.version, total_cap_microusd=policy.total_cap_microusd,
            monthly_cap_microusd=policy.monthly_cap_microusd, daily_run_limit=policy.daily_run_limit,
            initial_payload=payload)
        record = store.admit(decision)
        # no EC2 or Step Functions operation precedes all six immutable input uploads
        return self.upload_urls(identity, record)

    def sealed_inputs(self, record):
        sealed = {}
        for binary, expected in record.admission["initial_payload"]["manifest"]["binaries"].items():
            key = staged_key(record.run_key, binary)
            head = self.api["s3"].head_object(Bucket=self.config.bucket, Key=key, ChecksumMode="ENABLED")
            checksum = base64.b64encode(bytes.fromhex(expected["sha256"])).decode("ascii")
            require(type(head.get("ContentLength")) is int and head["ContentLength"] == expected["bytes"]
                    and head.get("ChecksumSHA256") == checksum and head.get("ChecksumType", "FULL_OBJECT") == "FULL_OBJECT"
                    and head.get("ServerSideEncryption") == "AES256", "uploaded binary bytes or encryption differ")
            version = head.get("VersionId")
            require(isinstance(version, str) and version not in ("", "null") and len(version) <= 1024
                    and all(32 < ord(c) < 127 for c in version), "input bucket versioning is required")
            sealed[binary] = dict(key=key, version=version, **expected)
        return sealed

    def verify_execution(self, found, state, record):
        request = state["request"]
        expected = dict(run_key=record.run_key, epoch=record.epoch, run=request["run"])
        require(found.get("executionArn") == state["submission_arn"] and found.get("stateMachineArn") == self.machine
                and found.get("name") == request["run"] and json.loads(found.get("input", "null")) == expected,
                "execution differs from immutable submission")
        return dict(execution_arn=found["executionArn"], execution_status=found["status"])

    def seal_and_start(self, token):
        identity = self.verifier.verify(token)
        store = self.store(identity)
        record = store.read()
        require(record is not None, "no admitted run exists")
        if record.closed:
            return receipt(record)
        original = record.admission["initial_payload"]
        require(identity.run_attempt == original["producer"]["run_attempt"], "workflow reruns cannot submit another allocation")
        state, revision = store.load()
        if state["phase"] in ("awaiting-inputs", "armed"):
            policy = self.policy()
            require(policy.enabled and policy.version == record.admission["policy_version"], "admission policy changed or is disabled")
            self.run_reader.read(identity)
            # proof reads can race normal coordinator progress; branch on the fresh owner snapshot
            state, revision = store.load()
        if state["phase"] in {"launching", "uncertain", "requested"}:
            require(state["submission_arn"] == execution_arn(self.machine, state["request"]["run"]),
                    "progressed submission differs")
            found = self.api["stepfunctions"].describe_execution(executionArn=state["submission_arn"])
            return receipt(store.read()) | self.verify_execution(found, state, record)
        if state["phase"] not in ("awaiting-inputs", "armed"):
            return receipt(store.read())
        if state["phase"] == "awaiting-inputs":
            deadline = min(utc(original["upload_deadline"]), utc(state["request"]["capacity_deadline"]))
            require(self.now() < deadline, "input upload or capacity period expired")
            state["sealed_inputs"] = self.sealed_inputs(record)
            require(self.now() < deadline, "input sealing exceeded upload or capacity period")
            state["submission_arn"] = execution_arn(self.machine, state["request"]["run"])
            state["phase"] = "armed"
            store.save(state, revision)
        require(state["phase"] == "armed" and state["sealed_inputs"] is not None
                and self.now() < utc(state["request"]["capacity_deadline"]), "run is no longer eligible for submission")
        inputs = dict(run_key=record.run_key, epoch=record.epoch, run=state["request"]["run"])
        try:
            found = self.api["stepfunctions"].describe_execution(executionArn=state["submission_arn"])
        except Exception as error:
            if error_code(error) != "ExecutionDoesNotExist":
                raise
        else:
            return receipt(store.read()) | self.verify_execution(found, state, record)
        try:
            started = self.api["stepfunctions"].start_execution(stateMachineArn=self.machine,
                name=state["request"]["run"], input=canonical(inputs).decode())
        except Exception as start_error:
            # the exact name/input remains idempotent; only typed transient failures may retry
            try:
                found = self.api["stepfunctions"].describe_execution(executionArn=state["submission_arn"])
            except Exception as read_error:
                if error_code(read_error) == "ExecutionDoesNotExist" and retryable_io(start_error):
                    raise start_error
                raise
            return receipt(store.read()) | self.verify_execution(found, state, record)
        require(started.get("executionArn") == state["submission_arn"], "submission returned another execution")
        return receipt(store.read()) | dict(execution_arn=started["executionArn"], execution_status="submitted")
