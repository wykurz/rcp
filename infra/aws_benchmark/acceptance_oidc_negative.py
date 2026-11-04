"""Signed different-workflow STS denial, without changing production access.

Authenticate this exact STS-audience JWT independently before interpreting denial.
Ambiguous sends or session issuance hold. No token/credential/service body is output.
"""
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum
import hashlib
import json
import math
import re
import time
import uuid

from .acceptance_cases import require
from . import acceptance_oidc as positive
from . import production_auth as auth

SCHEMA = "rcp-production-oidc-negative-v1"
OBSERVATION = "rcp-production-oidc-negative-observation-v1"
WORKFLOW = "wykurz/rcp/.github/workflows/aws-benchmark-identity-denial.yml@refs/heads/main"
NAME = "AWS benchmark identity denial"
ROLE = "arn:aws:iam::381183037896:role/rcp-production-benchmark-GitHubSubmitRole-hg2kGsuS3Qa1"
ROLE_ID = "AROAVRQCHSHEKT2CRHFFR"
SECONDS = 45
SESSION_SECONDS = 900
AWS_OIDC_SKEW_SECONDS = 300
RECOVERY_MARGIN_SECONDS = 60
FIELDS = {"schema", "account", "region", "source_commit", "role_arn", "role_id",
          "workflow_ref", "workflow_name", "run_id", "run_attempt", "actor",
          "actor_id", "event_name", "not_before", "expires_at", "recovery_cutoff",
          "approval_capture_sha256"}
UNPROVEN = ["negative-actor-ref-audience", "same-name-workflow-path-isolation",
            "whole-system-acceptance"]


class Stage(Enum):
    CONFIGURATION = "configuration"
    TOKEN_REQUEST = "token-request"
    SIGNED_CONTEXT = "signed-context"
    BEFORE_SEND = "before-sts-send"
    STS = "sts-exchange"
    AFTER_SEND = "after-sts-response"
    COMPLETE = "complete"


class ErrorCode(Enum):
    NOT_OBSERVED = "not-observed"
    ACCESS_DENIED = "AccessDenied"
    INVALID_TOKEN = "InvalidIdentityToken"
    EXPIRED_TOKEN = "ExpiredToken"
    IDP_REJECTED = "IDPRejectedClaim"
    IDP_UNAVAILABLE = "IDPCommunicationError"
    THROTTLED = "Throttling"
    OTHER = "other"


@dataclass(frozen=True)
class Recovery:
    possible_session: bool = False
    send_started_at: int | None = None
    token_expires_at: int | None = None
    session_expires_at: int | None = None
    reconcile_until: int | None = None
    session_identity: str = "not-observed"
    coverage_exceeded: bool = False


@dataclass
class Progress:
    stage: Stage = Stage.CONFIGURATION
    error_code: ErrorCode = ErrorCode.NOT_OBSERVED
    request_id: str | None = None
    recovery: Recovery = Recovery()

    def snapshot(self):
        recovery = self.recovery if type(self.recovery) is Recovery else Recovery(True)
        return dict(stage=self.stage.value if type(self.stage) is Stage else "unknown",
            sts_error=self.error_code.value if type(self.error_code) is ErrorCode else "other",
            request_id=request_id(self.request_id), recovery=dict(
                possible_session=recovery.possible_session is True,
                send_started_at=_optional_integer(recovery.send_started_at),
                token_expires_at=_optional_integer(recovery.token_expires_at),
                session_expires_at=_optional_integer(recovery.session_expires_at),
                reconcile_until=_optional_integer(recovery.reconcile_until),
                session_identity=recovery.session_identity if recovery.session_identity in
                    {"not-observed", "expected", "other"} else "other",
                coverage_exceeded=recovery.coverage_exceeded is True))


def _optional_integer(value):
    return value if type(value) is int and 0 < value < 2**53 else None


def request_id(value):
    try:
        return value if type(value) is str and str(uuid.UUID(value)) == value else None
    except (ValueError, TypeError, AttributeError):
        return None


def validate_config(value):
    require(type(value) is dict and set(value) == FIELDS)
    require(value["schema"] == SCHEMA and value["account"] == "381183037896"
            and value["region"] == "us-east-1" and value["role_arn"] == ROLE
            and value["role_id"] == ROLE_ID and value["workflow_ref"] == WORKFLOW
            and value["workflow_name"] == NAME and value["actor"] == "wykurz"
            and value["actor_id"] == "483465" and value["run_attempt"] == "1"
            and value["event_name"] == "workflow_dispatch")
    require(type(value["source_commit"]) is str
            and re.fullmatch(r"[0-9a-f]{40}", value["source_commit"]))
    require(type(value["run_id"]) is str
            and re.fullmatch(r"[1-9][0-9]{0,19}", value["run_id"]))
    require(type(value["approval_capture_sha256"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", value["approval_capture_sha256"]))
    for field in ("not_before", "expires_at", "recovery_cutoff"):
        require(_optional_integer(value[field]) is not None)
    require(0 < value["expires_at"] - value["not_before"] <= 900)
    latest_recovery = (value["expires_at"] + auth.MAX_TOKEN_LIFETIME_SECONDS
        + AWS_OIDC_SKEW_SECONDS + SESSION_SECONDS + RECOVERY_MARGIN_SECONDS)
    require(latest_recovery <= value["recovery_cutoff"] <= value["not_before"] + 7200)
    return json.loads(json.dumps(value, sort_keys=True, allow_nan=False))


class _WrongWorkflowVerifier(auth._GitHubTokenVerifier):
    """Fixed test context; production has no STS-audience opt-in."""

    def __init__(self, config, clock, jwks_fetcher):
        self.expected = config
        super().__init__(repository="wykurz/rcp", repository_id="655073755",
            repository_owner_id="483465", audience="sts.amazonaws.com",
            workflow_refs={WORKFLOW}, clock=clock, jwks_fetcher=jwks_fetcher)

    def _validate_claims(self, claims, now):
        super()._validate_claims(claims, now)
        require(claims.get("workflow") == NAME
            and claims.get("sub") == "repo:wykurz/rcp:ref:refs/heads/main"
            and claims.get("sha") == self.expected["source_commit"]
            and all(claims.get(name) == self.expected[name] for name in
                ("run_id", "run_attempt", "actor", "actor_id", "event_name")))


def _hold(progress):
    return dict(schema=OBSERVATION, ok=False, live_accepted=False,
        cases={"signed_different_workflow_denied": False}, unproven=list(UNPROVEN),
        diagnostics=progress.snapshot(),
        hold="Negative identity probe incomplete; preserve evidence and reconcile before retry")


def _metadata(response):
    metadata = response.get("ResponseMetadata") if type(response) is dict else None
    return metadata if type(metadata) is dict else {}


def _service_error(error, progress):
    response = error.response if type(getattr(error, "response", None)) is dict else {}
    metadata = _metadata(response)
    info = response.get("Error")
    code = info.get("Code") if type(info) is dict else None
    progress.error_code = next((entry for entry in ErrorCode if entry.value == code), ErrorCode.OTHER)
    progress.request_id = request_id(metadata.get("RequestId"))
    return (getattr(error, "operation_name", None) == "AssumeRoleWithWebIdentity"
        and code == "AccessDenied" and type(metadata.get("HTTPStatusCode")) is int
        and metadata["HTTPStatusCode"] == 403 and progress.request_id is not None)


def _capture_unexpected_metadata(response, config, session_name, progress):
    # capture only safe metadata before a post-request deadline can interrupt
    if type(response) is not dict:
        return
    progress.request_id = request_id(_metadata(response).get("RequestId"))
    credentials = response.get("Credentials")
    expiration = credentials.get("Expiration") if type(credentials) is dict else None
    actual = None
    if isinstance(expiration, datetime) and expiration.tzinfo is not None:
        stamp = expiration.timestamp()
        start = progress.recovery.send_started_at
        if math.isfinite(stamp) and start < stamp < 2**53:
            actual = math.ceil(stamp)
    assumed = response.get("AssumedRoleUser")
    expected_arn = ("arn:aws:sts::" + config["account"] + ":assumed-role/"
        + ROLE.rsplit("/", 1)[1] + "/" + session_name)
    matching = (type(assumed) is dict and assumed.get("Arn") == expected_arn
        and assumed.get("AssumedRoleId") == ROLE_ID + ":" + session_name)
    until = max(progress.recovery.reconcile_until,
        actual + RECOVERY_MARGIN_SECONDS if actual is not None else 0)
    progress.recovery = replace(progress.recovery, session_expires_at=actual,
        session_identity="expected" if matching else "other", reconcile_until=until,
        coverage_exceeded=until > config["recovery_cutoff"])


def run_probe(config, token_reader, *, evidence, factory=positive.sdk_sts,
              jwks_fetcher=None, clock=time.time, monotonic=time.monotonic, progress=None):
    """Authenticate, journal one send and require a specific denial; never retry."""
    from botocore.exceptions import ClientError
    progress = Progress() if progress is None else progress
    clients = []
    try:
        config = validate_config(config)
        start = positive._clock(monotonic)
        def check():
            now = positive._clock(clock)
            require(config["not_before"] <= now and now + 1 < config["expires_at"])
            require(0 <= positive._clock(monotonic) - start < SECONDS)
            return now
        require(check() + SECONDS < config["expires_at"])
        progress.stage = Stage.TOKEN_REQUEST
        token = token_reader()
        progress.stage = Stage.SIGNED_CONTEXT
        identity = _WrongWorkflowVerifier(config, clock, jwks_fetcher).verify(token)
        require(check() + SECONDS < identity.expires_at)
        until = (identity.expires_at + AWS_OIDC_SKEW_SECONDS + SESSION_SECONDS
            + RECOVERY_MARGIN_SECONDS)
        require(until <= config["recovery_cutoff"])
        unsigned = factory()
        clients.append(unsigned)
        session_name = "rcp-oidc-negative-" + config["run_id"] + "-1"
        progress.recovery = Recovery(True, math.floor(check()), identity.expires_at,
            None, until)
        progress.stage = Stage.BEFORE_SEND
        evidence(dict(event="before-sts-send", diagnostics=progress.snapshot()))
        progress.stage = Stage.STS
        try:
            response = unsigned.assume_role_with_web_identity(RoleArn=ROLE,
                RoleSessionName=session_name, WebIdentityToken=token,
                DurationSeconds=SESSION_SECONDS, Policy=positive.SESSION_POLICY)
        except ClientError as error:
            if not _service_error(error, progress):
                raise
            progress.recovery = replace(progress.recovery, possible_session=False,
                reconcile_until=None)
            progress.stage = Stage.AFTER_SEND
            evidence(dict(event="denied", diagnostics=progress.snapshot()))
            check()
            progress.stage = Stage.COMPLETE
            digest = hashlib.sha256(json.dumps(config, sort_keys=True,
                separators=(",", ":")).encode()).hexdigest()
            return dict(schema=OBSERVATION, ok=True, live_accepted=False,
                cases={"signed_different_workflow_denied": True}, unproven=list(UNPROVEN),
                diagnostics=progress.snapshot(), bindings_sha256=digest,
                source_commit=config["source_commit"], role_arn=ROLE, role_id=ROLE_ID,
                workflow_name=NAME, identity={name: getattr(identity, name)
                    for name in positive.IDENTITY_FIELDS})
        _capture_unexpected_metadata(response, config, session_name, progress)
        progress.stage = Stage.AFTER_SEND
        evidence(dict(event="unexpected-session-response", diagnostics=progress.snapshot()))
        return _hold(progress)
    except Exception:
        return _hold(progress)
    finally:
        for client in clients:
            try:
                client.close()
            except Exception:
                pass
