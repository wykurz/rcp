"""Isolated real-Admission/RunStore cases, never production activation.

Identity and GitHub metadata are synthetic. S3 URLs are inert. Only the injected
fixture DynamoDB client can issue service calls. Operator seeding and cleanup are
separate; this module has no seed, reset, scan, delete, launch or AWS-client factory.
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from datetime import timedelta
import copy
import hashlib
import json
import re
import threading
from types import SimpleNamespace

from .common import BINARIES, ValidationError, error_code, require, timestamp
from .durable_aws import Configuration
from .production_admission import Admission, epoch_for, run_key
from .production_handler import read_policy
from .production_policy import Policy, WorkflowRun, canonical
from .production_store import ReconciliationRequired

FIXTURE_SOURCE = "92829601e1a4d806465a9c393a7411de7309b78b"
ACCOUNT = "381183037896"
REPOSITORY_ID = "655073755"
OWNER = "483465"
PHASES = ("dedup", "competing-runs", "total-exact", "monthly-exact", "daily-exact",
          "total-cap", "monthly-cap", "daily-cap",
          "missing-ledger", "disabled-policy", "stale-price", "unauthorized-actor",
          "unapproved-event", "policy-fence", "lost-write-response")
RUN_IDS = tuple(str(9000000000000000000 + i) for i in range(4))
BASE_RESERVED = 68_210_000  # synthetic fixture seed, not a live accounting write
CALL_LIMIT = 160
MAX_WORKERS = 4


def table_name(value):
    require(isinstance(value, str) and re.fullmatch(
        r"rcp-production-admission-acceptance-[0-9a-f]{12}", value),
        "fixture table is outside candidate namespace")
    return value


def fixture_policy(now):
    return Policy(version="fixture-1", owner_id=OWNER, enabled=True,
        total_cap_microusd=350_000_000, monthly_cap_microusd=80_000_000,
        daily_run_limit=MAX_WORKERS, hourly_microusd=4_608_000, ancillary_microusd=1_000_000,
        priced_at=timestamp(now), price_evidence="SYNTHETIC FIXTURE; NOT AWS PRICE EVIDENCE")


def policy_item(policy):
    return {"pk": {"S": "policy"}, "version": {"S": policy.version},
        "enabled": {"BOOL": policy.enabled}, "payload": {"S": canonical(asdict(policy)).decode()}}


def number_item(key, field, value):
    return {"pk": {"S": key}, field: {"N": str(value)}}


def fixture_items(phase, now):
    """Pure operator preparation; never writes to an AWS service."""
    require(phase in PHASES, "unknown fixture phase")
    policy = fixture_policy(now)
    amount = policy.reservation(now)
    items = {"policy": policy_item(policy),
        "budget#total": number_item("budget#total", "reserved", BASE_RESERVED)}
    month, day = now.strftime("%Y-%m"), now.strftime("%Y-%m-%d")
    if phase in {"total-cap", "total-exact"}:
        items["budget#total"] = number_item("budget#total", "reserved",
            policy.total_cap_microusd - amount + (phase == "total-cap"))
    elif phase in {"monthly-cap", "monthly-exact"}:
        key = "budget#month#" + month
        items[key] = number_item(key, "reserved", policy.monthly_cap_microusd - amount + (phase == "monthly-cap"))
    elif phase in {"daily-cap", "daily-exact"}:
        key = "rate#day#" + day
        items[key] = number_item(key, "runs", policy.daily_run_limit if phase == "daily-cap" else policy.daily_run_limit - 1)
    elif phase == "missing-ledger":
        del items["budget#total"]
    elif phase == "disabled-policy":
        items["policy"] = policy_item(replace(policy, enabled=False))
    elif phase == "stale-price":
        items["policy"] = policy_item(replace(policy, priced_at=timestamp(now - timedelta(days=32))))
    elif phase == "policy-fence":
        items["policy"] = policy_item(replace(policy, version="fixture-2"))
    return items


def fixture_keys(now):
    return frozenset({"policy", "active", "budget#total",
        "budget#month#" + now.strftime("%Y-%m"), "rate#day#" + now.strftime("%Y-%m-%d"),
        *("run#repo-" + REPOSITORY_ID + "-run-" + run_id for run_id in RUN_IDS)})


class FixtureDynamo:
    """Exact-table/key-scoped forwarding with shared bounded call accounting.

    This is an additional code guard; exact IAM scopes remain necessary in a later
    live deployment. Automatic SDK retries must be disabled by its caller.
    """
    def __init__(self, client, table, now, before_call=lambda: None):
        self._client, self.table = client, table_name(table)
        self.before_call = before_call
        self.keys = fixture_keys(now)
        self.calls = []
        self.lock = threading.Lock()
        self.inject_lost_write = False
        self.write_gate = None
        self.write_outcomes = []

    def _operation(self, value):
        require(type(value) is dict and value.get("TableName") == self.table,
            "fixture operation table differs")
        key = (value.get("Key") or value.get("Item") or {}).get("pk")
        require(type(key) is dict and set(key) == {"S"} and key["S"] in self.keys,
            "fixture operation key differs")

    def _call(self, name, request):
        self.before_call()
        with self.lock:
            require(len(self.calls) < CALL_LIMIT, "fixture call budget exhausted")
            self.calls.append(name)
        result = getattr(self._client, name)(**request)
        self.before_call()
        return result

    def get_item(self, **request):
        require(set(request) == {"TableName", "Key", "ConsistentRead"}
            and request["ConsistentRead"] is True, "fixture read shape differs")
        self._operation(request)
        return self._call("get_item", request)

    def transact_get_items(self, **request):
        require(set(request) == {"TransactItems"} and type(request["TransactItems"]) is list
            and 1 <= len(request["TransactItems"]) <= 10, "fixture read transaction differs")
        for action in request["TransactItems"]:
            require(type(action) is dict and set(action) == {"Get"}, "fixture read action differs")
            self._operation(action["Get"])
        return self._call("transact_get_items", request)

    def transact_write_items(self, **request):
        require(set(request) == {"TransactItems", "ClientRequestToken"}
            and type(request["ClientRequestToken"]) is str
            and re.fullmatch(r"[0-9a-f-]{36}", request["ClientRequestToken"])
            and type(request["TransactItems"]) is list
            and 1 <= len(request["TransactItems"]) <= 10, "fixture write transaction differs")
        for action in request["TransactItems"]:
            require(type(action) is dict and len(action) == 1
                and next(iter(action)) in {"ConditionCheck", "Put", "Update"},
                "fixture cannot delete or perform another write action")
            self._operation(next(iter(action.values())))
        if self.write_gate is not None:
            self.write_gate.wait(timeout=5)
        try:
            result = self._call("transact_write_items", request)
        except Exception as error:
            response = getattr(error, "response", {}) or {}
            reasons = response.get("CancellationReasons")
            code = response.get("Error", {}).get("Code")
            safe = dict(code=code if code in {"TransactionCanceledException", "AccessDeniedException",
                "ValidationException", "ThrottlingException"} else "other-error", reasons=(
                [reason.get("Code") if type(reason) is dict and reason.get("Code") in
                    {"None", "ConditionalCheckFailed", "TransactionConflict"} else "other-reason"
                    for reason in reasons] if type(reasons) is list and len(reasons) <= 10 else None))
            with self.lock:
                self.write_outcomes.append(safe)
            raise
        with self.lock:
            self.write_outcomes.append(dict(code="success", reasons=None))
        if self.inject_lost_write:
            self.inject_lost_write = False
            raise TimeoutError("synthetic response loss after acknowledged fixture commit")
        return result


class SyntheticVerifier:
    def __init__(self, who):
        self.who = who

    def verify(self, token):
        require(token == "SYNTHETIC-NOT-A-JWT", "only explicit synthetic input is supported")
        return self.who


class SyntheticRunReader:
    def __init__(self, now):
        self.now = now

    def read(self, who):
        # exercise the real run-record validator, not a real authenticated GitHub read
        return WorkflowRun.verify(dict(id=int(who.run_id), run_attempt=int(who.run_attempt),
            repository=dict(id=int(who.repository_id), full_name=who.repository,
                owner=dict(id=int(who.repository_owner_id))),
            head_sha=who.sha, head_branch="main", event=who.event_name,
            actor=dict(id=int(who.actor_id)), path=".github/workflows/aws-benchmark-production.yml",
            status="in_progress", conclusion=None,
            created_at=timestamp(self.now - timedelta(minutes=10))), who, self.now)


class InertUploads:
    def generate_presigned_url(self, method, **request):
        require(method == "put_object" and request["HttpMethod"] == "PUT"
            and request["Params"]["IfNoneMatch"] == "*"
            and request["Params"]["ServerSideEncryption"] == "AES256"
            and request["ExpiresIn"] == 600, "unexpected upload capability request")
        return "https://no-upload.invalid/INERT-NOT-A-CAPABILITY"


class NoLaunch:
    def __getattr__(self, name):
        raise AssertionError("state-machine or launch access forbidden in candidate")


def identity(run_id=RUN_IDS[0], **updates):
    return SimpleNamespace(**(dict(repository="wykurz/rcp", repository_id=REPOSITORY_ID,
        repository_owner_id=OWNER, actor_id=OWNER, actor="SYNTHETIC", ref="refs/heads/main",
        workflow_ref="wykurz/rcp/.github/workflows/aws-benchmark-production.yml@refs/heads/main",
        workflow_sha=FIXTURE_SOURCE, sha=FIXTURE_SOURCE, run_id=run_id, run_attempt="1",
        event_name="workflow_dispatch") | updates))


def manifest(source_commit=FIXTURE_SOURCE):
    return dict(baseline=source_commit, candidate=source_commit, suite="bulk-1g-metadata",
        binaries={name: dict(sha256="b" * 64, bytes=100) for name in BINARIES})


def service(ddb, table, now, who, phase, source_commit=FIXTURE_SOURCE):
    who = SimpleNamespace(**(vars(who) | dict(sha=source_commit, workflow_sha=source_commit)))
    # fabricated EC2 pins are inert request-format fixtures and cannot be deployed
    pins = dict(region="us-east-1", account=ACCOUNT, namespace="acceptance-fixture",
        bucket="rcp-inert-no-upload", lt_id="lt-" + "a" * 17, lt_version="1",
        ami="ami-" + "b" * 17, source=source_commit, subnet="subnet-" + "c" * 17,
        security_group="sg-" + "d" * 17,
        profile_arn="arn:aws:iam::" + ACCOUNT + ":instance-profile/INERT-NOT-CREATED")
    config = Configuration(**pins, table=table, runtime_group="INERT-NOT-CREATED",
        termination_role="arn:aws:iam::" + ACCOUNT + ":role/INERT-NOT-CREATED")
    policy_reader = lambda: read_policy(ddb, table, OWNER)
    if phase == "policy-fence":
        # deliberately stale captured policy; actual transaction must fence fixture-2
        policy_reader = lambda: fixture_policy(now)
    return Admission({"dynamodb": ddb, "s3": InertUploads(), "stepfunctions": NoLaunch()},
        config, "arn:aws:states:us-east-1:" + ACCOUNT + ":stateMachine:INERT-NOT-CREATED",
        policy_reader, SyntheticVerifier(who), SyntheticRunReader(now), now=lambda: now)


def snapshot(ddb, table, now):
    keys = sorted(fixture_keys(now))
    responses = ddb.transact_get_items(TransactItems=[
        {"Get": {"TableName": table, "Key": {"pk": {"S": key}}}} for key in keys])["Responses"]
    require(type(responses) is list and len(responses) == len(keys),
        "fixture transactional snapshot is incomplete")
    result = {}
    for key, response in zip(keys, responses):
        require(type(response) is dict, "invalid fixture transactional read")
        value = response.get("Item")
        if value:
            require(value.get("pk") == {"S": key}, "fixture response key differs")
            result[key] = value
    return result


def must_reject(call):
    try:
        call()
    except (ValueError, ReconciliationRequired) as error:
        return error
    raise AssertionError("expected admission rejection")


def run_phase(client, table, phase, now, *, source_commit=FIXTURE_SOURCE, before_call=lambda: None):
    """One isolated, operator-seeded phase; has no reset/cleanup or service factory.

    The separate admission_probe entrypoint supplies immutable bindings, role/table
    checks, an outer deadline and a durable one-shot guard before entering here.
    """
    require(phase in PHASES, "unknown fixture phase")
    require(isinstance(source_commit, str) and re.fullmatch(r"[0-9a-f]{40}", source_commit), "invalid source pin")
    table = table_name(table)
    ddb = FixtureDynamo(client, table, now, before_call)
    expected = fixture_items(phase, now)
    before = snapshot(ddb, table, now)
    require(before == expected, "fixture seed or retained owner differs; do not reset or retry")
    who = identity()
    if phase == "unauthorized-actor":
        who = identity(actor_id="999999")
    if phase == "unapproved-event":
        who = identity(event_name="pull_request")
    candidate = service(ddb, table, now, who, phase, source_commit)
    token = "SYNTHETIC-NOT-A-JWT"
    value = manifest(source_commit)
    accepted = phase in {"dedup", "competing-runs", "lost-write-response",
        "total-exact", "monthly-exact", "daily-exact"}
    if phase == "competing-runs":
        gate = threading.Barrier(MAX_WORKERS)
        ddb.write_gate = threading.Barrier(MAX_WORKERS)
        def attempt(run_id):
            gate.wait(timeout=5)
            try:
                return service(ddb, table, now, identity(run_id), phase, source_commit).begin(token, value)
            except ReconciliationRequired:
                return None
            except Exception as error:
                response = getattr(error, "response", {}) or {}
                reasons = response.get("CancellationReasons")
                codes = [reason.get("Code") for reason in reasons if type(reason) is dict] \
                    if type(reasons) is list else []
                require(error_code(error) == "TransactionCanceledException" and len(codes) == 6
                    and set(codes) <= {"None", "TransactionConflict"} and "TransactionConflict" in codes,
                    "unexpected contender failure")
                return None
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            outcomes = list(pool.map(attempt, RUN_IDS))
        require(sum(result is not None for result in outcomes) == 1,
            "exactly one competing fixture request must fully reconcile admission")
        require(len(ddb.write_outcomes) == MAX_WORKERS
            and sum(outcome["code"] == "success" for outcome in ddb.write_outcomes) == 1,
            "race requires four forwarded writes and exactly one acknowledged commit")
        owner_codes = ["None", "None", "ConditionalCheckFailed", "None", "None", "None"]
        for outcome in ddb.write_outcomes:
            if outcome["code"] == "success":
                continue
            codes = outcome["reasons"]
            require(outcome["code"] == "TransactionCanceledException" and type(codes) is list
                and len(codes) == 6 and (codes == owner_codes or
                    set(codes) <= {"None", "TransactionConflict"} and "TransactionConflict" in codes),
                "race did not prove owner-condition rejection or transaction contention")
        ddb.write_gate = None
        winner = next(result for result in outcomes if result is not None)["run_key"]
        loser_id = next(run_id for run_id in RUN_IDS if run_key(identity(run_id)) != winner)
        # conflicts alone can mask a missing owner condition; isolate it after contention
        must_reject(lambda: service(ddb, table, now, identity(loser_id), phase, source_commit).begin(token, value))
        require(len(ddb.write_outcomes) == MAX_WORKERS + 1 and ddb.write_outcomes[-1] ==
            dict(code="TransactionCanceledException", reasons=owner_codes),
            "serial losing request did not isolate the active-owner fence")
    elif accepted:
        ddb.inject_lost_write = phase == "lost-write-response"
        first = candidate.begin(token, value)
        require(candidate.begin(token, value) == first, "same-run deduplication differs")
        rerun = service(ddb, table, now, identity(run_attempt="2"), phase, source_commit).begin(token, value)
        require("uploads" not in rerun and rerun["run_key"] == first["run_key"],
            "rerun recreated upload authority or changed durable identity")
        changed = copy.deepcopy(value)
        for entry in changed["binaries"].values():
            entry["sha256"] = "c" * 64
        rejected = must_reject(lambda: candidate.begin(token, changed))
        require(type(rejected) is ValidationError and str(rejected) == "existing upload manifest differs",
            "changed manifest was not rejected at its exact local guard")
        if phase in {"dedup", "lost-write-response"}:
            must_reject(lambda: service(ddb, table, now, identity(RUN_IDS[1]), phase, source_commit).begin(token, value))
            require(ddb.write_outcomes == [dict(code="success", reasons=None),
                dict(code="TransactionCanceledException",
                    reasons=["None", "None", "ConditionalCheckFailed", "None", "None", "None"])],
                "other-run rejection did not isolate the active-owner fence")
        else:
            require(ddb.write_outcomes == [dict(code="success", reasons=None)],
                "exact-cap control did not acknowledge exactly one transaction")
    else:
        rejection = must_reject(lambda: candidate.begin(token, value))
        expected_index = {"total-cap": 3, "monthly-cap": 4, "daily-cap": 5,
            "missing-ledger": 3, "policy-fence": 0}.get(phase)
        if expected_index is not None:
            codes = ["None"] * 6
            codes[expected_index] = "ConditionalCheckFailed"
            require(ddb.write_outcomes == [
                dict(code="TransactionCanceledException", reasons=codes)],
                "expected exact service-side condition rejection was not observed")
        else:
            expected_error = {"disabled-policy": "production admission is disabled",
                "stale-price": "server price evidence is stale",
                "unauthorized-actor": "manual requester is not approved",
                "unapproved-event": "only approved manual and scheduled requests may enter admission"}[phase]
            expected_type = ValueError if phase == "unapproved-event" else ValidationError
            require(type(rejection) is expected_type and str(rejection) == expected_error
                and not ddb.write_outcomes, "expected pre-write policy rejection was not observed")
    after = snapshot(ddb, table, now)
    if accepted:
        amount = fixture_policy(now).reservation(now)
        run_items = [item for key, item in after.items() if key.startswith("run#")]
        require(len(run_items) == 1 and "active" in after, "fixture ownership count differs")
        owner = after["active"]
        stored = json.loads(run_items[0]["admission"]["S"])
        require(owner["run_key"]["S"] == stored["run_key"]
            and owner["epoch"]["S"] == epoch_for(stored["run_key"])
            and run_items[0]["closed"] == {"BOOL": False}
            and run_items[0]["terminal"] == {"BOOL": False}
            and json.loads(run_items[0]["state"]["S"])["phase"] == "awaiting-inputs",
            "fixture owner or initial phase differs")
        initial_day = int(before.get("rate#day#" + now.strftime("%Y-%m-%d"),
            {"runs": {"N": "0"}})["runs"]["N"])
        initial_total = int(before["budget#total"]["reserved"]["N"])
        initial_month = int(before.get("budget#month#" + now.strftime("%Y-%m"),
            {"reserved": {"N": "0"}})["reserved"]["N"])
        require(after["budget#total"]["reserved"] == {"N": str(initial_total + amount)}
            and after["budget#month#" + now.strftime("%Y-%m")]["reserved"] == {"N": str(initial_month + amount)}
            and after["rate#day#" + now.strftime("%Y-%m-%d")]["runs"] == {"N": str(initial_day + 1)},
            "reservation or daily counter was not charged exactly once")
        require(after["policy"] == before["policy"] and len(after) == 6,
            "fixture policy changed or unexpected fixture keys appeared")
    else:
        require(after == before, "rejection changed ownership or accounting")
    return dict(schema="rcp-admission-candidate-observation-v1", phase=phase,
        ok=True, live_accepted=False, source_commit=source_commit,
        proof_scope="real Admission/read_policy/RunStore; fixture DynamoDB client",
        synthetic_identity=True, synthetic_github_proof=True, inert_upload_urls=True,
        no_seal_or_start=True, calls=len(ddb.calls),
        injected_stale_policy=phase == "policy-fence",
        injected_response_loss=phase == "lost-write-response",
        complete_table_inventory_proved=False,
        write_outcomes=copy.deepcopy(ddb.write_outcomes),
        before_sha256=hashlib.sha256(canonical(before)).hexdigest(),
        after_sha256=hashlib.sha256(canonical(after)).hexdigest())
