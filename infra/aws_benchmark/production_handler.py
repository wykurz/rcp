"""Initially disabled synchronous Lambda entrypoint for production submission.

The invocation role must be restricted by IAM; its request also carries the
original admission-audience GitHub JWT. Never log events, tokens or upload URLs.
An admission_unavailable response can follow a committed reservation or uncertain
start: it does not authorize refunds, ownership release or another run. Only typed
transient responses permit bounded retries of the same action and producer attempt.

RCP_PRODUCTION_ENABLED and RCP_PRODUCTION_ADMISSION_ENABLED default false and
must both be literal "true". Deployment also supplies RCP_PRODUCTION_REPOSITORY,
REPOSITORY_ID, OWNER_ID, AUDIENCE, SUBMIT_WORKFLOW, MACHINE_ARN, GITHUB_APP_CLIENT_ID,
GITHUB_APP_INSTALLATION_ID and GITHUB_APP_SECRET_ARN (each with the
same RCP_PRODUCTION_ prefix). Service pins use Configuration.environment's
RCP_DURABLE_ variables. Lambda must impose a finite outer execution timeout;
SDK socket timeouts alone are not a hard wall-clock bound.
"""

from collections.abc import Mapping
from dataclasses import fields
from datetime import datetime, timezone
from functools import lru_cache
import json
import os
import re
import time

from benchmarks.strict_json import parse_json
from . import durable
from .common import require, validate_durable_pins, retryable_io
from .durable_aws import Configuration
from .production_admission import Admission
from .production_auth import GitHubIdentityError, GitHubIdentityVerifier, MAX_TOKEN_BYTES
from .production_github import GitHubRunError, GitHubRateLimitError, GitHubRunReader
from .production_github_app import GitHubAppConfig, cached_provider
from .production_policy import Policy, canonical, manifest
from .production_errors import transient


MAX_EVENT_BYTES = 32 * 1024
MAX_POLICY_BYTES = 16 * 1024
SUBMIT_WORKFLOW_PATH = ".github/workflows/aws-benchmark-production.yml"
_POLICY_FIELDS = frozenset(field.name for field in fields(Policy))


class InvalidRequest(ValueError):
    """A request does not match the bounded synchronous invocation protocol."""


def _event(value):
    try:
        require(isinstance(value, dict) and isinstance(value.get("action"), str), "invalid request")
        action = value["action"]
        require(action in {"begin", "seal"}, "invalid request")
        expected = {"action", "token", "manifest"} if action == "begin" else {"action", "token"}
        require(set(value) == expected, "invalid request")
        token = value["token"]
        require(isinstance(token, str) and 0 < len(token) <= MAX_TOKEN_BYTES and token.isascii(),
                "invalid request")
        checked = manifest(value["manifest"]) if action == "begin" else None
        require(len(canonical(value)) <= MAX_EVENT_BYTES, "invalid request")
        return action, token, checked
    except Exception:
        raise InvalidRequest("invalid request") from None


def read_policy(dynamodb, table, owner_id):
    """Read the exact versioned server envelope used by RunStore's transaction.

    Item attributes are exactly pk:S=policy, version:S, enabled:BOOL, payload:S.
    payload is canonical JSON containing every Policy field. Its version and
    enabled values must match the transaction fence attributes without coercion.
    Policy-content changes require a new immutable version. The emergency enabled
    toggle may keep its version but must update both envelope and payload values.
    """
    try:
        require(isinstance(table, str) and re.fullmatch(r"[A-Za-z0-9_.-]{3,255}", table),
                "invalid policy configuration")
        require(isinstance(owner_id, str) and re.fullmatch(r"[1-9][0-9]{0,19}", owner_id),
                "invalid policy configuration")
        response = dynamodb.get_item(TableName=table, Key={"pk": {"S": "policy"}}, ConsistentRead=True)
        item = response.get("Item")
        require(isinstance(item, dict) and set(item) == {"pk", "version", "enabled", "payload"},
                "invalid server policy")
        require(item["pk"] == {"S": "policy"}, "invalid server policy")
        for name in ("version", "payload"):
            require(isinstance(item[name], dict) and set(item[name]) == {"S"}
                    and isinstance(item[name]["S"], str), "invalid server policy")
        require(isinstance(item["enabled"], dict) and set(item["enabled"]) == {"BOOL"}
                and type(item["enabled"]["BOOL"]) is bool, "invalid server policy")
        text = item["payload"]["S"]
        require(0 < len(text.encode("utf-8")) <= MAX_POLICY_BYTES, "invalid server policy")
        value = parse_json(text)
        require(isinstance(value, dict) and set(value) == _POLICY_FIELDS
                and canonical(value).decode() == text, "invalid server policy")
        require(type(value["enabled"]) is bool and value["enabled"] == item["enabled"]["BOOL"]
                and value["version"] == item["version"]["S"] and value["owner_id"] == owner_id,
                "invalid server policy")
        require(isinstance(value["manual_actor_ids"], list) and len(value["manual_actor_ids"]) <= 20
                and isinstance(value["schedule_slots"], list) and len(value["schedule_slots"]) <= 3,
                "invalid server policy")
        require(all(isinstance(slot, list) and len(slot) == 3 for slot in value["schedule_slots"]),
                "invalid server policy")
        value["manual_actor_ids"] = tuple(value["manual_actor_ids"])
        value["schedule_slots"] = tuple(tuple(slot) for slot in value["schedule_slots"])
        return Policy(**value)
    except Exception as error:
        if retryable_io(error):
            raise
        raise ValueError("server admission policy unavailable") from None


class SubmissionClients(Mapping):
    """Construct only the requested fixed-endpoint SDK client, with no retries."""

    def __init__(self, region):
        require(isinstance(region, str) and re.fullmatch(r"[a-z]{2}-[a-z]+-[1-9][0-9]?", region),
                "invalid service region")
        self.region, self._clients = region, {}

    def __iter__(self):
        return iter(("dynamodb", "s3", "stepfunctions"))

    def __len__(self):
        return 3

    def __getitem__(self, name):
        if name not in ("dynamodb", "s3", "stepfunctions"):
            raise KeyError("service unavailable")
        if name not in self._clients:
            import boto3
            from botocore.config import Config
            service = "states" if name == "stepfunctions" else name
            config = Config(connect_timeout=3, read_timeout=5,
                retries={"total_max_attempts": 1}, proxies={}, ignore_configured_endpoint_urls=True,
                signature_version="s3v4" if name == "s3" else "v4",
                s3={"addressing_style": "virtual"} if name == "s3" else None)
            self._clients[name] = boto3.client(name, region_name=self.region, config=config,
                endpoint_url=f"https://{service}.{self.region}.amazonaws.com")
        return self._clients[name]


class _StandardMachineVerifier:
    def __init__(self, verifier, api, machine):
        self.verifier, self.api, self.machine = verifier, api, machine

    def verify(self, token):
        identity = self.verifier.verify(token)
        value = self.api["stepfunctions"].describe_state_machine(stateMachineArn=self.machine)
        require(isinstance(value, dict) and value.get("stateMachineArn") == self.machine
                and value.get("name") == self.machine.rsplit(":", 1)[1]
                and value.get("type") == "STANDARD" and value.get("status") == "ACTIVE",
                "submission state machine metadata differs")
        return identity


@lru_cache(maxsize=1)
def _cached_verifier(repository, repository_id, owner_id, audience, workflow):
    # reuse signing keys only; policy, state-machine and producer observations stay fresh.
    return GitHubIdentityVerifier(repository=repository, repository_id=repository_id,
        repository_owner_id=owner_id, audience=audience, workflow_refs={workflow})


def build_admission(*, api=None, clock=time.time):
    """Build the submit-only service from validated deployment environment values."""
    config = Configuration.environment()
    validate_durable_pins(config.pins(), durable.PLAN)
    # generated production buckets have no dots; dotted names force path-style TLS addressing
    require(re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", config.bucket),
            "production bucket must support virtual-host HTTPS")
    require(re.fullmatch(r"[A-Za-z0-9_.-]{3,255}", config.table)
            and re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", config.runtime_group)
            and re.fullmatch(r"arn:aws:iam::" + re.escape(config.account)
                            + r":role/[A-Za-z0-9+=,.@_/-]{1,512}", config.termination_role),
            "invalid deployment configuration")
    repository = os.environ["RCP_PRODUCTION_REPOSITORY"]
    repository_id = os.environ["RCP_PRODUCTION_REPOSITORY_ID"]
    owner_id = os.environ["RCP_PRODUCTION_OWNER_ID"]
    audience = os.environ["RCP_PRODUCTION_AUDIENCE"]
    workflow = os.environ["RCP_PRODUCTION_SUBMIT_WORKFLOW"]
    require(workflow == f"{repository}/{SUBMIT_WORKFLOW_PATH}@refs/heads/main",
            "submission workflow differs")
    machine = os.environ["RCP_PRODUCTION_MACHINE_ARN"]
    require(re.fullmatch(r"arn:aws:states:" + re.escape(config.region) + ":" + re.escape(config.account)
                        + r":stateMachine:[A-Za-z0-9_-]{1,80}", machine), "state machine scope differs")
    verifier = _cached_verifier(repository, repository_id, owner_id, audience, workflow) if clock is time.time else \
        GitHubIdentityVerifier(repository=repository, repository_id=repository_id,
            repository_owner_id=owner_id, audience=audience, workflow_refs={workflow}, clock=clock)
    app = GitHubAppConfig.from_environment(os.environ, region=config.region, account=config.account)
    reader = GitHubRunReader(repository=repository, repository_id=repository_id,
        repository_owner_id=owner_id, clock=clock, observation_factory=cached_provider(app).observation)
    clients = SubmissionClients(config.region) if api is None else api
    return Admission(clients, config, machine,
        lambda: read_policy(clients["dynamodb"], config.table, owner_id),
        _StandardMachineVerifier(verifier, clients, machine), reader,
        now=lambda: datetime.fromtimestamp(clock(), timezone.utc))


def lambda_handler(event, context):
    """Return a bounded success/error envelope without logging bearer input."""
    if (os.environ.get("RCP_PRODUCTION_ENABLED", "false") != "true"
            or os.environ.get("RCP_PRODUCTION_ADMISSION_ENABLED", "false") != "true"):
        return {"ok": False, "error": "admission_disabled"}
    try:
        action, token, checked = _event(event)
    except InvalidRequest:
        print(json.dumps(dict(event="admission_failure", category="invalid_request", retryable=False), sort_keys=True))
        return {"ok": False, "error": "invalid_request"}
    try:
        service = build_admission()
        result = service.begin(token, checked) if action == "begin" else service.seal_and_start(token)
        return {"ok": True, "result": result}
    except Exception as error:
        # log fixed categories only, never bearer input, capabilities or raw exception text.
        retryable = transient(error)
        category = "github_rate_limit" if isinstance(error, GitHubRateLimitError) else \
            "github_identity" if isinstance(error, GitHubIdentityError) else \
            "github_metadata" if isinstance(error, GitHubRunError) else \
            "transient_service" if retryable else "validation_or_service"
        detail = dict(event="admission_failure", action=action, category=category, retryable=retryable)
        request_id = getattr(context, "aws_request_id", None)
        if isinstance(request_id, str) and re.fullmatch(r"[A-Za-z0-9-]{1,80}", request_id):
            detail["request_id"] = request_id
        print(json.dumps(detail, sort_keys=True))
        if retryable:
            return {"ok": False, "error": "admission_retryable",
                    "retry_after_seconds": getattr(error, "retry_after_seconds", 5)}
        # failures may hide a reservation; only same-attempt reconciliation is permitted.
        return {"ok": False, "error": "admission_unavailable"}
