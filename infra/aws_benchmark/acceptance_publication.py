"""Bounded publication-role read observation; never publish or invoke a handler.

STS authenticates the single token before any claims are decoded as evidence.
The inline policy leaves the two tested read actions eligible on all resources,
so it cannot manufacture their same-action negative resource denials. It permits
only exact-table DynamoDB-mediated KMS decryption and denies other actions. It
does not prove base write/launch denial or live KMS negative cases.
All token, credential and returned fixture bytes remain in memory.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import hashlib
import json
import math
import os
import re
import time
import urllib.parse

from . import acceptance_oidc as oidc
from .acceptance_cases import require

SCHEMA = "rcp-publication-read-approval-v2"
OBSERVATION = "rcp-publication-read-observation-v2"
SECONDS = 45
ACCOUNT = "381183037896"
REGION = "us-east-1"
WORKFLOW = "wykurz/rcp/.github/workflows/aws-benchmark-publish.yml@refs/heads/main"
WORKFLOW_NAME = "Publish retired AWS benchmark history"
ROLE = "arn:aws:iam::381183037896:role/rcp-production-benchmark-GitHubPublicationRole-34PMVuYgLR5R"
ROLE_ID = "AROAVRQCHSHEHGOUK7XQR"
BUCKET = "rcp-production-benchmark-artifactbucket-augcame1lsq0"
TABLE = "rcp-production-benchmark-LeaseTable-137SHCUGC8HHU"
TABLE_KEY_ARN = "arn:aws:kms:us-east-1:381183037896:key/fda6ab01-08d2-49d9-9866-f75012f06ed6"
RUN = "repo-655073755-run-41973977673"
MANIFEST = "production/runs/" + RUN + "/collection/manifest.json"
VERSION = "oXca.tk9xmd6NXHo9uji.RHBM8ru6Bcp"
MANIFEST_BYTES = 3536
MANIFEST_SHA256 = "0e56b863e2b66186875a7155d5c9806f545ac8e49f12f0cb40f25148ee8f8895"
FORBIDDEN = "production/runs/" + RUN + "/durable-receipt.json"
SUBJECT = "repo:wykurz/rcp:ref:refs/heads/main"
CALL_PLAN = ["github:oidc:sts.amazonaws.com", "sts:AssumeRoleWithWebIdentity:900",
             "sts:GetCallerIdentity", "dynamodb:GetItem:run:pk-only",
             "s3:GetObjectVersion:collection-manifest", "dynamodb:GetItem:policy:pk-only:AccessDeniedException",
             "s3:GetObjectVersion:durable-receipt:AccessDenied"]
FIELDS = {"schema", "source_commit", "approval_nonce", "role_arn", "role_id", "bucket", "table", "table_key_arn",
          "manifest_key", "manifest_version", "manifest_bytes", "manifest_sha256",
          "negative_key", "negative_version", "fixture_capture_sha256", "not_before", "expires_at",
          "approval_capture_sha256", "call_plan"}
# These single-valued conditions bind service delegation, not individual DynamoDB items.
# Each mismatch needs its own deny: different condition keys in one statement are ANDed.
KMS_CONTEXT = {
    "kms:ViaService": "dynamodb." + REGION + ".amazonaws.com",
    "kms:CallerAccount": ACCOUNT,
    "kms:EncryptionContext:aws:dynamodb:tableName": TABLE,
    "kms:EncryptionContext:aws:dynamodb:subscriberId": ACCOUNT,
}
SESSION_POLICY = json.dumps({"Version": "2012-10-17", "Statement": [
    {"Effect": "Deny", "NotAction": ["dynamodb:GetItem", "s3:GetObjectVersion", "kms:Decrypt"], "Resource": "*"},
    {"Effect": "Allow", "Action": ["dynamodb:GetItem", "s3:GetObjectVersion"], "Resource": "*"},
    {"Effect": "Allow", "Action": "kms:Decrypt", "Resource": TABLE_KEY_ARN,
        "Condition": {"StringEquals": KMS_CONTEXT}},
    {"Effect": "Deny", "Action": "kms:Decrypt", "NotResource": TABLE_KEY_ARN},
    *[{"Effect": "Deny", "Action": "kms:Decrypt", "Resource": "*",
        "Condition": {"StringNotEqualsIfExists": {key: value}}} for key, value in KMS_CONTEXT.items()]]},
    separators=(",", ":"))
SESSION_POLICY_SHA256 = hashlib.sha256(SESSION_POLICY.encode("ascii")).hexdigest()
UNPROVEN = ["signed-wrong-workflow-and-other-native-trust-negatives", "other-publication-read-scopes",
            "base-policy-write-launch-invoke-and-secret-denials", "live-kms-direct-and-context-denials",
            "durable-publication", "whole-system-acceptance"]


class Stage(Enum):
    CONFIGURATION = "configuration"
    TOKEN_REQUEST = "token-request"
    STS_EXCHANGE = "sts-exchange"
    SIGNED_CLAIMS = "signed-claims"
    CALLER_IDENTITY = "caller-identity"
    POSITIVE_ITEM = "positive-item"
    POSITIVE_OBJECT = "positive-object"
    NEGATIVE_ITEM = "negative-item"
    NEGATIVE_OBJECT = "negative-object"
    COMPLETE = "complete"


@dataclass
class Progress:
    """Only fixed labels cross exception/cancellation boundaries."""
    stage: Stage = Stage.CONFIGURATION


def hold(progress=None):
    progress = Progress() if progress is None else progress
    return dict(schema=OBSERVATION, ok=False, live_accepted=False, unproven=list(UNPROVEN),
        hold="Publication reader observation incomplete; reconcile before any new approval",
        diagnostics={"stage": progress.stage.value if type(progress.stage) is Stage else "unknown"})


def timestamp(clock):
    value = clock()
    require(type(value) in (int, float) and math.isfinite(value) and value >= 0)
    return value


def validate_approval(value):
    """Validate exact nonsecret bindings; capture hashes do not authenticate external evidence."""
    require(type(value) is dict and set(value) == FIELDS)
    fixed = dict(schema=SCHEMA, role_arn=ROLE, role_id=ROLE_ID, bucket=BUCKET, table=TABLE, table_key_arn=TABLE_KEY_ARN,
        manifest_key=MANIFEST, manifest_version=VERSION, manifest_bytes=MANIFEST_BYTES,
        manifest_sha256=MANIFEST_SHA256, negative_key=FORBIDDEN, call_plan=CALL_PLAN)
    require(all(value.get(key) == expected for key, expected in fixed.items()))
    require(type(value["manifest_bytes"]) is int)
    for field, pattern in (("source_commit", r"[0-9a-f]{40}"), ("approval_nonce", r"[0-9a-f]{32}"),
            ("fixture_capture_sha256", r"[0-9a-f]{64}"), ("approval_capture_sha256", r"[0-9a-f]{64}"),
            ("negative_version", r"[A-Za-z0-9_.+/=-]{1,1024}")):
        require(isinstance(value[field], str) and re.fullmatch(pattern, value[field]) is not None)
    require(value["negative_version"] != "null")
    require(type(value["not_before"]) is int and type(value["expires_at"]) is int
        and 0 < value["not_before"] < value["expires_at"] < 2**53
        and value["expires_at"] - value["not_before"] <= 3600)
    return dict(value)


def validate_bindings(value):
    require(type(value) is dict and set(value) == FIELDS | {"run_id", "run_attempt"})
    validate_approval({key: value[key] for key in FIELDS})
    require(isinstance(value["run_id"], str) and re.fullmatch(r"[1-9][0-9]{0,19}", value["run_id"]) is not None
        and value["run_attempt"] == "1")
    return dict(value)


def sdk_client(service, *, credentials=None):
    """No operator credential chain, endpoint/proxy override, redirects or SDK retries."""
    require(service in {"sts", "dynamodb", "s3"})
    require(service == "sts" or credentials is not None)
    from botocore import UNSIGNED
    from botocore.config import Config
    from botocore.session import Session
    oidc._suppress_sdk_debug()
    session = Session(session_vars={"profile": (None, None, None, None),
        "data_path": (None, None, None, None)})
    for name, value in (("config_file", os.devnull), ("credentials_file", os.devnull), ("csm_enabled", False)):
        session.set_config_variable(name, value)
    keys = {} if credentials is None else dict(aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"], aws_session_token=credentials["SessionToken"])
    client = session.create_client(service, region_name=REGION, verify=True,
        endpoint_url="https://" + service + "." + REGION + ".amazonaws.com",
        **keys, config=Config(signature_version=UNSIGNED if credentials is None else "v4", connect_timeout=1, read_timeout=3,
            retries={"total_max_attempts": 1}, proxies={}, ignore_configured_endpoint_urls=True,
            s3={"addressing_style": "path", "use_accelerate_endpoint": False},
            endpoint_discovery_enabled=False))
    allowed = {"sts": {"AssumeRoleWithWebIdentity"} if credentials is None else {"GetCallerIdentity"},
               "dynamodb": {"GetItem"}, "s3": {"GetObject"}}[service]
    pending = False
    def operation(model, **kwargs):
        nonlocal pending
        require(model.name in allowed)
        pending = True
    def send(request, **kwargs):
        nonlocal pending
        require(pending)
        pending = False
        url = urllib.parse.urlsplit(request.url)
        require(url.scheme == "https" and url.netloc == service + "." + REGION + ".amazonaws.com")
    # SDK-specific S3 redirect hooks may attempt a HeadBucket or a second send even
    # with retries disabled. Reject both before transport, rather than contacting it.
    client.meta.events.register_first("before-call.*.*", operation)
    client.meta.events.register_first("before-send.*.*", send)
    return client


def get_item(client, key):
    return client.get_item(TableName=TABLE, Key={"pk": {"S": key}}, ConsistentRead=True,
        ProjectionExpression="#pk", ExpressionAttributeNames={"#pk": "pk"}, ReturnConsumedCapacity="NONE")


def denied(call, code):
    """Only service-specific authorization denial counts; absent fixtures/outages hold."""
    from botocore.exceptions import ClientError
    try:
        response = call()
    except ClientError as error:
        require(error.response.get("Error", {}).get("Code") == code)
        require(error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == (403 if code == "AccessDenied" else 400))
        return
    # an unexpected allowed S3 response must release its body without reading it
    if type(response) is dict and response.get("Body") is not None:
        response["Body"].close()
    require(False)


def run_probe(approval, token_reader, *, factory=sdk_client, clock=time.time,
              monotonic=time.monotonic, progress=None):
    """One token, one STS session, one identity lookup and four fixed read calls."""
    progress = Progress() if progress is None else progress
    clients = []
    try:
        progress.stage = Stage.CONFIGURATION
        approval = validate_bindings(approval)
        started = timestamp(monotonic)
        def check():
            now = timestamp(clock)
            require(approval["not_before"] <= now < approval["expires_at"]
                and 0 <= timestamp(monotonic) - started < SECONDS)
            return now
        require(check() + SECONDS < approval["expires_at"])
        progress.stage = Stage.TOKEN_REQUEST
        token = token_reader()
        require(isinstance(token, str) and len(token) <= oidc.auth.MAX_TOKEN_BYTES
            and re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token) is not None)
        check()
        progress.stage = Stage.STS_EXCHANGE
        unsigned = factory("sts")
        clients.append(unsigned)
        name = "rcp-publication-test-" + approval["run_id"] + "-1"
        check()
        response = unsigned.assume_role_with_web_identity(RoleArn=ROLE, RoleSessionName=name,
            WebIdentityToken=token, DurationSeconds=900, Policy=SESSION_POLICY)
        now = check()
        require(oidc.provider_form(response.get("Provider"), ACCOUNT) in
            (oidc.ProviderForm.DOCUMENTED_ISSUER, oidc.ProviderForm.EXPECTED_PROVIDER_ARN)
            and response.get("Audience") == "sts.amazonaws.com"
            and response.get("SubjectFromWebIdentityToken") == SUBJECT)
        progress.stage = Stage.SIGNED_CLAIMS
        # a successful STS exchange authenticated this exact token; decoding alone proves nothing
        claims = oidc._authenticated_claims(token)
        expected = dict(iss=oidc.auth.ISSUER, aud="sts.amazonaws.com", sub=SUBJECT,
            repository="wykurz/rcp", repository_id="655073755", repository_owner="wykurz",
            repository_owner_id="483465", ref="refs/heads/main", workflow_ref=WORKFLOW,
            workflow=WORKFLOW_NAME, workflow_sha=approval["source_commit"], sha=approval["source_commit"],
            run_id=approval["run_id"], run_attempt="1", actor="wykurz", actor_id="483465",
            event_name="workflow_dispatch", runner_environment="github-hosted")
        require(all(claims.get(key) == value for key, value in expected.items()))
        require(all(type(claims.get(key)) is int and 0 < claims[key] < 2**53 for key in ("iat", "nbf", "exp")))
        require(claims["nbf"] <= now + oidc.auth.CLOCK_SKEW_SECONDS and claims["nbf"] < claims["exp"]
            and claims["iat"] <= now + oidc.auth.CLOCK_SKEW_SECONDS
            and now - claims["iat"] < oidc.auth.MAX_TOKEN_AGE_SECONDS
            and 0 < claims["exp"] - claims["iat"] <= oidc.auth.MAX_TOKEN_LIFETIME_SECONDS and now < claims["exp"])
        arn = "arn:aws:sts::" + ACCOUNT + ":assumed-role/" + ROLE.rsplit("/", 1)[1] + "/" + name
        user_id = ROLE_ID + ":" + name
        assumed = response.get("AssumedRoleUser", {})
        require(assumed.get("Arn") == arn and assumed.get("AssumedRoleId") == user_id)
        credentials = response["Credentials"]
        require(all(isinstance(credentials.get(key), str) and credentials[key]
            for key in ("AccessKeyId", "SecretAccessKey", "SessionToken")))
        expiration = credentials.get("Expiration")
        require(isinstance(expiration, datetime) and expiration.tzinfo is not None
            and now < expiration.timestamp() <= now + 960)
        def live():
            require(check() < min(claims["exp"], expiration.timestamp()))
        progress.stage = Stage.CALLER_IDENTITY
        signed = factory("sts", credentials=credentials)
        clients.append(signed)
        live()
        caller = signed.get_caller_identity()
        require(caller.get("Account") == ACCOUNT and caller.get("Arn") == arn and caller.get("UserId") == user_id)
        live()
        db = factory("dynamodb", credentials=credentials)
        clients.append(db)
        s3 = factory("s3", credentials=credentials)
        clients.append(s3)
        progress.stage = Stage.POSITIVE_ITEM
        live()
        require(get_item(db, "run#" + RUN).get("Item") == {"pk": {"S": "run#" + RUN}})
        live()
        progress.stage = Stage.POSITIVE_OBJECT
        obj = s3.get_object(Bucket=BUCKET, Key=MANIFEST, VersionId=VERSION, ExpectedBucketOwner=ACCOUNT)
        body = obj.get("Body")
        try:
            require(body is not None and obj.get("VersionId") == VERSION
                and obj.get("ContentLength") == MANIFEST_BYTES)
            raw = body.read(MANIFEST_BYTES + 1)
            require(type(raw) is bytes and len(raw) == MANIFEST_BYTES
                and hashlib.sha256(raw).hexdigest() == MANIFEST_SHA256)
            live()
        finally:
            if body is not None:
                body.close()
        progress.stage = Stage.NEGATIVE_ITEM
        live()
        denied(lambda: get_item(db, "policy"), "AccessDeniedException")
        live()
        progress.stage = Stage.NEGATIVE_OBJECT
        denied(lambda: s3.get_object(Bucket=BUCKET, Key=FORBIDDEN,
            VersionId=approval["negative_version"], ExpectedBucketOwner=ACCOUNT), "AccessDenied")
        live()
        progress.stage = Stage.COMPLETE
        binding = hashlib.sha256(json.dumps(approval, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return dict(schema=OBSERVATION, ok=True, live_accepted=False, unproven=list(UNPROVEN),
            diagnostics={"stage": progress.stage.value}, bindings_sha256=binding,
            source_commit=approval["source_commit"], run_id=approval["run_id"], run_attempt="1",
            role_arn=ROLE, role_id=ROLE_ID, table_key_arn=TABLE_KEY_ARN,
            session_policy_sha256=SESSION_POLICY_SHA256, fixture_capture_sha256=approval["fixture_capture_sha256"],
            cases={"sts_same_run_main_publication": True, "exact_role_identity": True,
                "retained_run_pk_read": True, "versioned_collection_manifest_read": True,
                "policy_pk_denied": True, "versioned_durable_receipt_denied": True})
    except Exception:
        return hold(progress)
    finally:
        for client in clients:
            try:
                client.close()
            except Exception:
                pass
