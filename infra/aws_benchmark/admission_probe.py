"""One-shot isolated admission probe; this module never deploys or launches workers.

Only a fixed phase name comes from the event. Every resource, source and clock
binding is immutable package input. Synthetic identity/run evidence and inert
upload URLs are deliberate; observations never claim whole-system acceptance.
"""
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import threading
import time
import uuid

from benchmarks.strict_json import parse_json
from . import admission_acceptance as cases
from .acceptance_runtime import sdk_client as acceptance_client, verify_role
from .common import require, timestamp, utc

SCHEMA = "rcp-isolated-admission-probe-v1"
BINDING_FILE = Path(__file__).resolve().parents[2] / "admission-bindings.json"
FIELDS = {"schema", "account", "region", "nonce", "function_name", "function_version",
          "role_arn", "role_id", "log_group", "table", "table_id", "source_commit",
          "approval_capture_sha256", "not_before", "expires_at", "fixture_at"}
MAX_BINDINGS = 8192
TIMEOUT = 120
CONTROL_CALL_LIMIT = cases.CALL_LIMIT + 4


def validate_bindings(value):
    require(type(value) is dict and set(value) == FIELDS, "invalid admission probe bindings")
    require(value["schema"] == SCHEMA and value["account"] == cases.ACCOUNT
            and value["region"] == "us-east-1", "invalid admission probe scope")
    require(type(value["nonce"]) is str and re.fullmatch(r"[0-9a-f]{12}", value["nonce"]),
            "invalid fixture nonce")
    name = "rcp-admission-acceptance-" + value["nonce"]
    require(value["function_name"] == name and value["function_version"] == "1"
            and value["log_group"] == "/aws/lambda/" + name, "invalid function identity")
    require(value["role_arn"] == "arn:aws:iam::" + cases.ACCOUNT + ":role/" + name
            and type(value["role_id"]) is str and re.fullmatch(r"AROA[A-Z0-9]{17}", value["role_id"]),
            "invalid fixture role identity")
    require(value["table"] == "rcp-production-admission-acceptance-" + value["nonce"],
            "invalid fixture table")
    require(type(value["table_id"]) is str and str(uuid.UUID(value["table_id"])) == value["table_id"],
            "invalid immutable table ID")
    require(type(value["source_commit"]) is str and re.fullmatch(r"[0-9a-f]{40}", value["source_commit"])
            and type(value["approval_capture_sha256"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", value["approval_capture_sha256"]), "invalid source or approval pin")
    require(type(value["not_before"]) is int and type(value["expires_at"]) is int
            and 0 < value["not_before"] < value["expires_at"]
            and value["expires_at"] - value["not_before"] <= 7200, "invalid probe window")
    require(type(value["fixture_at"]) is str and timestamp(value["fixture_at"]) == value["fixture_at"]
            and value["not_before"] <= utc(value["fixture_at"]).timestamp() < value["expires_at"],
            "invalid fixed synthetic fixture clock")
    return json.loads(json.dumps(value, sort_keys=True, allow_nan=False))


def binding_bytes(value):
    return cases.canonical(validate_bindings(value))


def digest_bindings(value):
    return hashlib.sha256(binding_bytes(value)).hexdigest()


def parse_bindings(raw):
    require(type(raw) is bytes and 0 < len(raw) <= MAX_BINDINGS, "invalid binding file")
    return validate_bindings(parse_json(raw.decode("utf-8")))


def phase_event(event):
    require(type(event) is dict and set(event) == {"phase"}
            and type(event["phase"]) is str and event["phase"] in cases.PHASES,
            "only an exact fixed admission phase is accepted")
    return event["phase"]


def number(value):
    require(type(value) in {int, float} and math.isfinite(value) and value >= 0,
            "invalid probe clock")
    return value


class Deadline:
    def __init__(self, config, context, clock, monotonic):
        self.config, self.context = config, context
        self.clock, self.monotonic = clock, monotonic
        self.started = number(monotonic())
        name = config["function_name"]
        expected = "arn:aws:lambda:us-east-1:" + cases.ACCOUNT + ":function:" + name + ":1"
        require(getattr(context, "invoked_function_arn", None) == expected
                and getattr(context, "function_version", None) == "1"
                and getattr(context, "log_group_name", None) == config["log_group"],
                "probe must use its exact qualified function version")
        request_id = getattr(context, "aws_request_id", None)
        require(type(request_id) is str and str(uuid.UUID(request_id)) == request_id,
                "invalid invocation request identity")
        self.check(initial=True)

    def check(self, *, initial=False):
        now = number(self.clock())
        elapsed = number(self.monotonic()) - self.started
        require(self.config["not_before"] <= now
                and now + (TIMEOUT + 5 if initial else 5) < self.config["expires_at"]
                and 0 <= elapsed < TIMEOUT - 5, "probe deadline or approval window elapsed")
        remaining = self.context.get_remaining_time_in_millis()
        require(type(remaining) is int and 5000 < remaining <= TIMEOUT * 1000,
                "outer function deadline is absent or exhausted")


class DynamoGate:
    """Scope and bound every forwarded control/case request; never retry."""
    def __init__(self, client, config, deadline):
        self.client, self.config, self.deadline = client, config, deadline
        self.calls = 0
        self.lock = threading.Lock()

    def call(self, method, **request):
        require(method in {"describe_table", "put_item", "get_item",
                           "transact_get_items", "transact_write_items"}, "unsupported probe API")
        if method.startswith("transact_"):
            operations = request.get("TransactItems")
            require(type(operations) is list and 1 <= len(operations) <= 10, "invalid probe transaction")
            targets = [next(iter(action.values())) for action in operations
                       if type(action) is dict and len(action) == 1]
            require(len(targets) == len(operations), "invalid probe transaction action")
        else:
            targets = [request]
        require(all(type(target) is dict and target.get("TableName") == self.config["table"]
                    for target in targets), "probe attempted another table")
        self.deadline.check()
        with self.lock:
            require(self.calls < CONTROL_CALL_LIMIT, "probe control call budget exhausted")
            self.calls += 1
        result = getattr(self.client, method)(**request)
        self.deadline.check()
        return result

    def get_item(self, **request):
        return self.call("get_item", **request)

    def transact_get_items(self, **request):
        return self.call("transact_get_items", **request)

    def transact_write_items(self, **request):
        return self.call("transact_write_items", **request)


def table_identity(gate, config):
    value = gate.call("describe_table", TableName=config["table"]).get("Table", {})
    arn = "arn:aws:dynamodb:us-east-1:" + cases.ACCOUNT + ":table/" + config["table"]
    require(value.get("TableId") == config["table_id"] and value.get("TableArn") == arn
            and value.get("TableName") == config["table"] and value.get("TableStatus") == "ACTIVE"
            and value.get("KeySchema") == [{"AttributeName": "pk", "KeyType": "HASH"}]
            and value.get("AttributeDefinitions") == [{"AttributeName": "pk", "AttributeType": "S"}]
            and value.get("BillingModeSummary", {}).get("BillingMode") == "PAY_PER_REQUEST"
            and value.get("DeletionProtectionEnabled") is True,
            "fixture table identity or deletion protection differs")
    # TableId is a readback, not a DynamoDB request precondition. Deletion protection
    # and exclusive operator lifecycle control prevent replacement during this test.


def sdk_client(service, region):
    require(service in {"sts", "dynamodb"} and region == "us-east-1", "unsupported probe service")
    return acceptance_client(service, region)


def run_probe(config, event, context, *, factory=sdk_client, clock=time.time, monotonic=time.monotonic):
    clients = []
    stage = "bindings"
    try:
        config = validate_bindings(config)
        phase = phase_event(event)
        deadline = Deadline(config, context, clock, monotonic)
        stage = "role"
        sts = factory("sts", config["region"])
        clients.append(sts)
        deadline.check()
        verify_role(config, sts.get_caller_identity())
        deadline.check()
        stage = "table"
        client = factory("dynamodb", config["region"])
        clients.append(client)
        gate = DynamoGate(client, config, deadline)
        table_identity(gate, config)
        fixture_time = utc(config["fixture_at"])
        stage = "claim"
        claim_key = "probe#" + config["nonce"] + "#" + phase
        seed_digest = hashlib.sha256(cases.canonical(cases.fixture_items(phase, fixture_time))).hexdigest()
        # Any ambiguous response holds. Never adopt/replay/delete a spent claim.
        gate.call("put_item", TableName=config["table"], Item={
            "pk": {"S": claim_key}, "bindings_sha256": {"S": digest_bindings(config)},
            "phase": {"S": phase}, "seed_sha256": {"S": seed_digest},
            "source_commit": {"S": config["source_commit"]},
            "request_id": {"S": context.aws_request_id}},
            ConditionExpression="attribute_not_exists(pk)")
        stage = "cases"
        result = cases.run_phase(gate, config["table"], phase, fixture_time,
            source_commit=config["source_commit"], before_call=deadline.check)
        stage = "table-readback"
        table_identity(gate, config)
        deadline.check()
        return dict(schema="rcp-isolated-admission-observation-v1", ok=True, live_accepted=False,
            bindings_sha256=digest_bindings(config), phase=phase, source_commit=config["source_commit"],
            request_id=context.aws_request_id, role_arn=config["role_arn"], table_id=config["table_id"],
            function_version="1", calls=gate.calls, observation=result)
    except Exception:
        # never echo exception text, service bodies, request payloads or credentials
        return dict(schema="rcp-isolated-admission-observation-v1", ok=False, live_accepted=False,
            stage=stage, hold="probe incomplete; preserve claim and evidence and reconcile before retry")
    finally:
        for client in clients:
            try:
                client.close()
            except Exception:
                pass


def lambda_handler(event, context):
    try:
        with BINDING_FILE.open("rb") as source:
            config = parse_bindings(source.read(MAX_BINDINGS + 1))
        return run_probe(config, event, context)
    except Exception:
        return dict(ok=False, live_accepted=False, stage="bindings",
            hold="probe bindings unavailable; no acceptance established")
