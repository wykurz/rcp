"""Bounded acceptance observations over injected clients, without SDK or entrypoint.

The caller must verify immutable bindings and execution-role identity, disable SDK
retries and impose an outer deadline on blocking adapters before invoking a phase.
These observations do not grant execution authority or establish whole-system live
acceptance. Exceptions and summaries exclude secret values, tokens and headers.
Each phase retains its isolated fixtures; none retries, cleans up or deletes them.
"""
from dataclasses import dataclass
import math
import re
import time
from functools import wraps
from infra.aws_benchmark.production_store import AdmissionDecision, RunStore

PAYLOAD = b"rcp-acceptance-fixed-non-secret-v1\n"
CASE_LIMIT = 32

class Hold(RuntimeError):
    """Allowlisted diagnostics only; caller exceptions cannot supply secret text."""
    SAFE = frozenset({
        "acceptance bindings or evidence incomplete",
        "adapter exceeded deadline; outcome requires reconciliation",
        "acceptance observation failed; reconcile before retry",
        "request outcome uncertain; reconcile before retry",
        "unexpected rejection; retain evidence and reconcile",
        "negative operation succeeded; stop and reconcile",
    })
    def __init__(self, message):
        super().__init__(message if type(message) is str and message in self.SAFE else
                         "acceptance observation failed; reconcile before retry")


def require(condition):
    if not condition:
        raise Hold("acceptance bindings or evidence incomplete")


@dataclass(frozen=True)
class Bindings:
    account: str = ""
    region: str = ""
    role_arn: str = ""
    secret_arn: str = ""
    current_version: str = ""
    previous_version: str = ""
    unrelated_secret_arn: str = ""
    reader_kind: str = ""
    app_client_id: str = ""
    installation_id: str = ""
    bucket: str = ""
    table: str = ""
    nonce: str = ""

    def _scope(self):
        require(type(self.account) is str and re.fullmatch(r"[0-9]{12}", self.account) is not None)
        require(type(self.region) is str and self.region == "us-east-1")

    def reader(self):
        self._scope()
        require(_matches(r"arn:aws:iam::" + self.account + r":role/[A-Za-z0-9+=,.@_/-]{1,128}", self.role_arn))
        prefix = re.escape("arn:aws:secretsmanager:" + self.region + ":" + self.account + ":secret:")
        require(_matches(prefix + r"/rcp/production/github-read-app-[A-Za-z0-9]{6}", self.secret_arn))
        require(_matches(prefix + r"/rcp/acceptance/reader-denied-[A-Za-z0-9]{6}", self.unrelated_secret_arn))
        require(type(self.reader_kind) is str and self.reader_kind in {"admission", "collector"})
        require(all(_matches(r"[A-Za-z0-9-]{32,64}", v) for v in (self.current_version, self.previous_version)))
        require(self.current_version != self.previous_version)
        require(_matches(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}", self.app_client_id))
        require(_matches(r"[1-9][0-9]{0,19}", self.installation_id))
        return self

    def storage(self):
        self._scope()
        require(_matches(r"[0-9a-f]{16}", self.nonce))
        require(type(self.bucket) is str and self.bucket == "rcp-production-acceptance-" + self.nonce)
        require(type(self.table) is str and self.table == "rcp-production-acceptance-" + self.nonce)
        return self


def _matches(pattern, value):
    return type(value) is str and re.fullmatch(pattern, value) is not None


class BoundedCalls:
    """Finite allowlisted calls, no retries; caller must bound blocking adapters.

    Raw service exceptions are consumed by denial() or a sanitized phase boundary.
    The deadline also covers caller-side proof and response-body handling.
    """
    OPERATIONS = {
        "secrets": frozenset({"get_secret_value"}),
        "s3": frozenset({"put_object", "copy_object", "get_object"}),
        "dynamodb": frozenset({"get_item", "put_item", "transact_write_items"}),
    }

    def __init__(self, clients, *, seconds=45, clock=time.monotonic):
        require(type(seconds) is int and 0 < seconds <= 120)
        require(type(clients) is dict and callable(clock))
        self.clients, self.clock, self.calls, self._last = clients, clock, 0, None
        self.deadline = self._now() + seconds

    def _now(self):
        try:
            now = self.clock()
            require(type(now) in (int, float) and math.isfinite(now))
            require(self._last is None or now >= self._last)
            self._last = now
            return now
        except Exception:
            raise Hold("acceptance observation failed; reconcile before retry") from None

    def check_deadline(self):
        if self._now() >= self.deadline:
            raise Hold("adapter exceeded deadline; outcome requires reconciliation")

    def call(self, service, operation, arguments):
        require(self.calls < CASE_LIMIT)
        self.check_deadline()
        require(type(service) is str and service in self.OPERATIONS and service in self.clients)
        require(type(operation) is str and operation in self.OPERATIONS[service] and type(arguments) is dict)
        self.calls += 1
        try:
            response = getattr(self.clients[service], operation)(**arguments)
        except Exception:
            self.check_deadline()
            raise
        try:
            self.check_deadline()
        except Hold:
            # ownership has not transferred: discard the returned stream before holding
            try:
                if service == "s3" and operation == "get_object" and isinstance(response, dict):
                    body = response.get("Body")
                    if body is not None:
                        body.close()
            finally:
                raise Hold("adapter exceeded deadline; outcome requires reconciliation") from None
        return response


def sanitized(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Hold as error:
            raise Hold(str(error)) from None
        except Exception:
            raise Hold("acceptance observation failed; reconcile before retry") from None
    return wrapped

@sanitized
def denial(calls, service, operation, arguments, *, condition=False):
    """Only the exact service error counts; missing fixtures never prove denial."""
    require(type(condition) is bool and (not condition or service == "s3"))
    try:
        calls.call(service, operation, arguments)
    except Hold:
        raise
    except Exception as error:
        response = getattr(error, "response", None)
        if not isinstance(response, dict):
            raise Hold("request outcome uncertain; reconcile before retry") from None
        detail, metadata = response.get("Error"), response.get("ResponseMetadata")
        require(isinstance(detail, dict) and isinstance(metadata, dict))
        code, status = detail.get("Code"), metadata.get("HTTPStatusCode")
        require(type(code) is str and type(status) is int)
        if condition and code == "PreconditionFailed" and status == 412:
            return "pass"
        denied = {"secrets": ("AccessDeniedException", 400),
                  "s3": ("AccessDenied", 403), "dynamodb": ("AccessDeniedException", 400)}
        if not condition and (code, status) == denied[service]:
            return "pass"
        missing = {"secrets": {("ResourceNotFoundException", 400), ("InvalidRequestException", 400)},
                   "s3": {("NoSuchKey", 404), ("NoSuchVersion", 404)},
                   "dynamodb": {("ResourceNotFoundException", 400)}}
        if (code, status) in missing[service]:
            return "unproven"
        raise Hold("unexpected rejection; retain evidence and reconcile") from None
    raise Hold("negative operation succeeded; stop and reconcile")

@sanitized
def reader_cases(bindings, secrets, proof_reader, *, fixture_inventory, clock=time.monotonic):
    """Fixed-role probe core. Inventory is separately approved positive-control evidence.

    proof_reader is a fixed-route adapter over the merged GitHub App provider.
    The caller must validate real secret fixtures and execution-role identity first;
    the inventory booleans below are not security-policy attestation.
    """
    require(type(bindings) is Bindings and callable(proof_reader))
    b = bindings.reader()
    require(type(fixture_inventory) is dict
        and fixture_inventory.get("previous_stage_exists") is True
        and fixture_inventory.get("unrelated_secret_exists") is True)
    require(fixture_inventory == {"current_version": b.current_version, "previous_version": b.previous_version,
        "previous_stage_exists": True, "unrelated_secret_exists": True})
    calls = BoundedCalls({"secrets": secrets}, clock=clock)
    response = calls.call("secrets", "get_secret_value", {"SecretId": b.secret_arn, "VersionStage": "AWSCURRENT"})
    require(isinstance(response, dict) and response.get("ARN") == b.secret_arn
        and response.get("VersionId") == b.current_version)
    stages = response.get("VersionStages")
    require(type(stages) is list and 1 <= len(stages) <= 20
        and all(type(stage) is str and 0 < len(stage) <= 256 for stage in stages)
        and len(set(stages)) == len(stages) and "AWSCURRENT" in stages and "AWSPREVIOUS" not in stages)
    # no key serialization, hashing or diagnostic output; the provider separately consumes the key
    value = response.get("SecretString")
    require(isinstance(value, str) and 0 < len(value.encode()) <= 16 * 1024 and "SecretBinary" not in response)
    del value, response
    proof = proof_reader()
    calls.check_deadline()
    require(type(proof) is dict and proof.get("fixed_proof_read") is True)
    require(proof == {"repository_id": "655073755", "owner_id": "483465", "app_client_id": b.app_client_id,
        "installation_id": b.installation_id, "actions": "read", "metadata": "read", "fixed_proof_read": True})
    result = {"current_secret": "pass", "app_proof": "pass"}
    requests = {
        "omitted_stage": {"SecretId": b.secret_arn},
        "previous_stage": {"SecretId": b.secret_arn, "VersionStage": "AWSPREVIOUS"},
        "version_only": {"SecretId": b.secret_arn, "VersionId": b.current_version},
        "other_secret": {"SecretId": b.unrelated_secret_arn, "VersionStage": "AWSCURRENT"},
    }
    for name, request in requests.items():
        result[name] = denial(calls, "secrets", "get_secret_value", request)
    return {"probe_observations": True, "live_accepted": False, "kind": b.reader_kind, "cases": result,
        "all_cases_observed": all(v == "pass" for v in result.values()), "calls": calls.calls}


def _version(response):
    require(isinstance(response, dict) and isinstance(response.get("VersionId"), str)
        and re.fullmatch(r"[A-Za-z0-9_.~+/=-]{1,1024}", response["VersionId"]) is not None
        and response["VersionId"] != "null")
    return response["VersionId"]

@sanitized
def storage_cases(bindings, s3, *, clock=time.monotonic):
    """Small non-secret objects in an isolated canary bucket; never deletes evidence."""
    require(type(bindings) is Bindings)
    b = bindings.storage()
    calls = BoundedCalls({"s3": s3}, seconds=120, clock=clock)
    source, target = "acceptance/source", "acceptance/copy"
    put = dict(Bucket=b.bucket, Key=source, Body=PAYLOAD, IfNoneMatch="*", ExpectedBucketOwner=b.account,
               ServerSideEncryption="AES256", ChecksumAlgorithm="SHA256", ContentType="application/octet-stream")
    original_version = _version(calls.call("s3", "put_object", put))
    outcomes = {"create_only": denial(calls, "s3", "put_object", put, condition=True)}
    copy = dict(Bucket=b.bucket, Key=target, CopySource={"Bucket": b.bucket, "Key": source,
        "VersionId": original_version}, IfNoneMatch="*", ExpectedBucketOwner=b.account,
        ExpectedSourceBucketOwner=b.account, ServerSideEncryption="AES256", ChecksumAlgorithm="SHA256",
        AnnotationDirective="EXCLUDE", MetadataDirective="REPLACE", ContentType="application/octet-stream")
    copied_version = _version(calls.call("s3", "copy_object", copy))
    outcomes["copy_create_only"] = denial(calls, "s3", "copy_object", copy, condition=True)
    observed = calls.call("s3", "get_object", dict(Bucket=b.bucket, Key=target,
        VersionId=copied_version, ExpectedBucketOwner=b.account))
    body = observed.get("Body") if isinstance(observed, dict) else None
    try:
        require(isinstance(observed, dict) and observed.get("VersionId") == copied_version
            and type(observed.get("ContentLength")) is int and observed["ContentLength"] == len(PAYLOAD))
        require(body is not None)
        value = body.read(len(PAYLOAD) + 1)
        require(type(value) is bytes and value == PAYLOAD)
    finally:
        if body is not None:
            body.close()
    calls.check_deadline()
    outcomes["sealed_version_bytes"] = "pass"
    unrestricted = dict(put)
    unrestricted.pop("IfNoneMatch")
    outcomes["unconditional_put_denied"] = denial(calls, "s3", "put_object", unrestricted)
    return {"probe_observations": True, "live_accepted": False, "cases": outcomes, "calls": calls.calls,
        "source_version": original_version, "copy_version": copied_version, "cleanup": "retain until reviewed cleanup"}


def transaction_request(bindings):
    """Reuse the merged six-action admission transaction; no independent expression copy."""
    require(type(bindings) is Bindings)
    b = bindings.storage()
    decision = AdmissionDecision(run_key="repo-655073755-run-1", epoch="a" * 64, fingerprint="b" * 64,
        policy_version="acceptance-v1", reserve_microusd=1, total_cap_microusd=2,
        monthly_cap_microusd=2, daily_run_limit=1, month="2026-10", day="2026-10-27",
        initial_payload={"producer": {"run_id": "1", "run_attempt": "1"},
            "state": {"request": {"run": "1-1", "pins": {"image": "no-launch-canary"}}, "phase": "armed"}})
    return RunStore(None, b.table, decision.run_key, decision.epoch)._admission_items(decision)

@sanitized
def transaction_cases(bindings, dynamodb, *, clock=time.monotonic):
    """Isolated table only, preseeded policy and total=0. No production table accepted.

    Canonical admission writes have no launch capability by themselves. IAM clone
    scope, all postconditions and comparison to production policy require live review.
    """
    require(type(bindings) is Bindings)
    b = bindings.storage()
    calls = BoundedCalls({"dynamodb": dynamodb}, seconds=120, clock=clock)
    items = transaction_request(b)
    token = "acceptance-" + b.nonce
    keys = ["policy", "run#repo-655073755-run-1", "active", "budget#total", "budget#month#2026-10", "rate#day#2026-10-27"]
    def snapshot():
        result = {}
        for key in keys:
            response = calls.call("dynamodb", "get_item", dict(TableName=b.table,
                Key={"pk": {"S": key}}, ConsistentRead=True))
            require(isinstance(response, dict))
            item = response.get("Item")
            if "Item" in response:
                require(type(item) is dict and item.get("pk") == {"S": key})
                for name, attribute in item.items():
                    require(type(name) is str and type(attribute) is dict and len(attribute) == 1)
                    kind, value = next(iter(attribute.items()))
                    require(kind in {"S", "N", "BOOL"}
                        and type(value) is (bool if kind == "BOOL" else str))
            result[key] = item
        return result
    baseline = snapshot()
    require(baseline == {"policy": {"pk": {"S": "policy"}, "version": {"S": "acceptance-v1"}, "enabled": {"BOOL": True}},
        "budget#total": {"pk": {"S": "budget#total"}, "reserved": {"N": "0"}},
        **{key: None for key in keys if key not in {"policy", "budget#total"}}})
    response = calls.call("dynamodb", "transact_write_items", dict(TransactItems=items, ClientRequestToken=token))
    require(isinstance(response, dict))
    key = {"pk": {"S": "run#repo-655073755-run-1"}}
    before = snapshot()
    expected = items[1]["Put"]["Item"]
    require(before == {"policy": baseline["policy"], "run#repo-655073755-run-1": expected,
        "active": items[2]["Put"]["Item"],
        "budget#total": {"pk": {"S": "budget#total"}, "reserved": {"N": "1"}},
        "budget#month#2026-10": {"pk": {"S": "budget#month#2026-10"}, "reserved": {"N": "1"}},
        "rate#day#2026-10-27": {"pk": {"S": "rate#day#2026-10-27"}, "runs": {"N": "1"}}})
    outcomes = {"mixed_transaction": "pass"}
    outcomes["direct_write_denied"] = denial(calls, "dynamodb", "put_item",
        dict(TableName=b.table, Item=expected, ConditionExpression="attribute_exists(pk)"))
    outcomes["immutable_mutation_denied"] = denial(calls, "dynamodb", "transact_write_items",
        dict(TransactItems=[{"Update": {"TableName": b.table, "Key": key,
             "UpdateExpression": "SET #field = :value", "ExpressionAttributeNames": {"#field": "fingerprint"},
             "ExpressionAttributeValues": {":value": {"S": "c" * 64}}}}], ClientRequestToken="negative-" + b.nonce))
    require(snapshot() == before)
    outcomes["records_unchanged_after_denials"] = "pass"
    return {"probe_observations": True, "live_accepted": False, "cases": outcomes, "calls": calls.calls}


@sanitized
def oidc_case(token, verifier, *, actor_id="483465"):
    """Evaluate a supplied signed token through the merged verifier, never fetch/mint here.

    Offline tests inject local JWKS. The caller owns trusted verifier configuration
    and any live workflow gate; this core does not perform STS role assumption.
    """
    require(_matches(r"[1-9][0-9]{0,19}", actor_id))
    identity = verifier.verify(token)
    require(identity.actor_id == actor_id and identity.run_attempt == "1" and identity.event_name == "workflow_dispatch")
    return {"probe_observations": True, "live_accepted": False, "repository_id": identity.repository_id,
        "run_id": identity.run_id, "actor_id": identity.actor_id, "ref": identity.ref,
        "lifetime_seconds": identity.expires_at - identity.issued_at}
