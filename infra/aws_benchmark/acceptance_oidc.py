"""Positive main-lane OIDC/STS observation; never invoke admission or start workers.

Execute separately in an explicitly approved existing trusted GitHub workflow.
Supply one bounded stdin JSON object containing config and two tokens, never
arguments, environment variables, token files or logs. The admission token uses
production_auth's admission audience; STS requires a distinct sts.amazonaws.com
token. Only a successful STS exchange authenticates the second token before its
claims are compared. No workflow or token is created by this module.

The CLI arms a 45-second Unix timer, but an outer GitHub job timeout is required
for process stalls and cancellation. Injected adapters have elapsed-time checks,
not a hard deadline. Failures, including AccessDenied, establish no negative
acceptance. Real signed negative-ref and role fixtures remain separate live gates.
"""

from datetime import datetime
import hashlib
import json
import math
import os
import re
import signal
import stat
import sys
import time

from benchmarks.strict_json import parse_json
from .acceptance_cases import require
from . import production_auth as auth
from .production_github_app import _suppress_sdk_debug

SCHEMA = "rcp-production-oidc-acceptance-v1"
OBSERVATION = "rcp-production-oidc-observation-v1"
SECONDS = 45
MAX_INPUT = 48 * 1024
FIELDS = {"schema", "account", "region", "source_commit", "role_arn", "role_id",
          "admission_audience", "workflow_ref", "run_id", "run_attempt", "actor", "actor_id",
          "event_name", "not_before", "expires_at", "approval_capture_sha256"}
IDENTITY_FIELDS = ("repository", "repository_id", "repository_owner", "repository_owner_id",
                   "ref", "workflow_ref", "workflow_sha", "sha", "run_id", "run_attempt",
                   "actor", "actor_id", "event_name")
UNPROVEN = ["signed-negative-ref-rejection", "negative-role-policy-cases", "whole-system-acceptance"]
# an explicit deny also constrains resource-based grants to the session; GetCallerIdentity
# needs no permission even under explicit deny (see the AWS API reference)
SESSION_POLICY = '{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}'


def _matches(pattern, value):
    return isinstance(value, str) and re.fullmatch(pattern, value) is not None


def _clock(clock):
    value = clock()
    require(type(value) in (int, float) and math.isfinite(value) and value >= 0)
    return value


def validate_config(value):
    """Check exact non-secret approval bindings; this does not prove their provenance."""
    require(type(value) is dict and set(value) == FIELDS)
    require(value["schema"] == SCHEMA and value["account"] == "381183037896"
            and value["region"] == "us-east-1")
    require(_matches(r"[0-9a-f]{40}", value["source_commit"]))
    require(_matches(r"arn:aws:iam::381183037896:role/[A-Za-z0-9+=,.@_-]+(?:/[A-Za-z0-9+=,.@_-]+)*",
                     value["role_arn"]) and len(value["role_arn"]) <= 2048)
    require(_matches(r"AROA[A-Z0-9]{17}", value["role_id"]))
    require(_matches(r"wykurz/rcp/\.github/workflows/[A-Za-z0-9_-]+\.ya?ml@refs/heads/main",
                     value["workflow_ref"]))
    require(_matches(r"[1-9][0-9]{0,19}", value["run_id"]) and value["run_attempt"] == "1")
    require(_matches(r"[A-Za-z0-9][A-Za-z0-9_\[\]-]{0,99}", value["actor"]))
    require(_matches(r"[1-9][0-9]{0,19}", value["actor_id"]))
    require(value["event_name"] in {"workflow_dispatch", "schedule"})
    require(type(value["not_before"]) is int and type(value["expires_at"]) is int
            and 0 < value["not_before"] < value["expires_at"] < 2**53
            and value["expires_at"] - value["not_before"] <= 3600)
    require(_matches(r"[0-9a-f]{64}", value["approval_capture_sha256"]))
    # constructing the production verifier validates the admission audience without fetching keys
    _verifier(value, time.time, None)
    return dict(value)


def _verifier(config, clock, jwks_fetcher):
    return auth.GitHubIdentityVerifier(repository="wykurz/rcp", repository_id="655073755",
        repository_owner_id="483465", audience=config["admission_audience"],
        workflow_refs={config["workflow_ref"]}, clock=clock, jwks_fetcher=jwks_fetcher)


def sdk_sts(*, credentials=None):
    """Use only the regional TLS endpoint and never resolve operator credentials."""
    from botocore import UNSIGNED
    from botocore.config import Config
    from botocore.session import Session
    _suppress_sdk_debug()
    session = Session(session_vars={"profile": (None, None, None, None),
                                    "data_path": (None, None, None, None)})
    for name, value in (("config_file", os.devnull), ("credentials_file", os.devnull),
                        ("csm_enabled", False)):
        session.set_config_variable(name, value)
    kwargs = {} if credentials is None else dict(aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"], aws_session_token=credentials["SessionToken"])
    return session.create_client("sts", region_name="us-east-1", verify=True,
        endpoint_url="https://sts.us-east-1.amazonaws.com", **kwargs,
        config=Config(signature_version=UNSIGNED if credentials is None else "v4",
            connect_timeout=1, read_timeout=3, retries={"total_max_attempts": 1},
            proxies={}, ignore_configured_endpoint_urls=True))


def _authenticated_claims(token):
    # reuse the production strict JSON/base64 parser only AFTER independent authentication
    parts = token.split(".")
    require(len(parts) == 3)
    return auth._json(auth._segment(parts[1]))


def _hold():
    return dict(schema=OBSERVATION, ok=False, live_accepted=False, unproven=list(UNPROVEN),
                hold="OIDC probe incomplete; retain evidence and reconcile before retry")


def run_probe(config, token_reader, *, factory=sdk_sts, jwks_fetcher=None,
              clock=time.time, monotonic=time.monotonic):
    """Call only JWKS, AssumeRoleWithWebIdentity and GetCallerIdentity, without retries."""
    clients = []
    try:
        start = _clock(monotonic)
        config = validate_config(config)
        def check():
            now = _clock(clock)
            require(config["not_before"] <= now < config["expires_at"])
            require(0 <= _clock(monotonic) - start < SECONDS)
            return now
        require(check() + SECONDS < config["expires_at"])
        tokens = token_reader()
        require(type(tokens) is dict and set(tokens) == {"admission_token", "sts_token"})
        for token in tokens.values():
            require(isinstance(token, str) and 0 < len(token) <= auth.MAX_TOKEN_BYTES and token.isascii())
        require(tokens["admission_token"] != tokens["sts_token"])
        check()
        identity = _verifier(config, clock, jwks_fetcher).verify(tokens["admission_token"])
        expected = {name: getattr(identity, name) for name in IDENTITY_FIELDS}
        for name in ("workflow_ref", "run_id", "run_attempt", "actor", "actor_id", "event_name"):
            require(expected[name] == config[name])
        require(identity.sha == config["source_commit"])
        subject = "repo:wykurz/rcp:ref:refs/heads/main"
        require(_authenticated_claims(tokens["admission_token"])["sub"] == subject)
        require(check() < identity.expires_at)
        unsigned = factory()
        clients.append(unsigned)
        session_name = "rcp-acceptance-" + config["run_id"] + "-1"
        check()
        response = unsigned.assume_role_with_web_identity(RoleArn=config["role_arn"],
            RoleSessionName=session_name, WebIdentityToken=tokens["sts_token"],
            DurationSeconds=900, Policy=SESSION_POLICY)
        now = check()
        require(response.get("Provider") == auth.ISSUER
                and response.get("Audience") == "sts.amazonaws.com"
                and response.get("SubjectFromWebIdentityToken") == subject)
        # STS success authenticates this exact token. Parsing alone is never identity evidence.
        claims = _authenticated_claims(tokens["sts_token"])
        require(claims.get("iss") == auth.ISSUER and claims.get("aud") == "sts.amazonaws.com"
                and claims.get("sub") == subject)
        require(all(claims.get(name) == value for name, value in expected.items()))
        require(all(type(claims.get(name)) is int and 0 < claims[name] < 2**53
                    for name in ("iat", "nbf", "exp")))
        require(claims["nbf"] <= now + auth.CLOCK_SKEW_SECONDS and claims["nbf"] < claims["exp"]
                and claims["iat"] <= now + auth.CLOCK_SKEW_SECONDS
                and now - claims["iat"] < auth.MAX_TOKEN_AGE_SECONDS
                and 0 < claims["exp"] - claims["iat"] <= auth.MAX_TOKEN_LIFETIME_SECONDS
                and now < min(claims["exp"], identity.expires_at))
        role_name = config["role_arn"].rsplit("/", 1)[1]
        arn = "arn:aws:sts::" + config["account"] + ":assumed-role/" + role_name + "/" + session_name
        user_id = config["role_id"] + ":" + session_name
        assumed = response.get("AssumedRoleUser", {})
        require(assumed.get("Arn") == arn and assumed.get("AssumedRoleId") == user_id)
        credentials = response["Credentials"]
        require(all(isinstance(credentials.get(name), str) and credentials[name]
                    for name in ("AccessKeyId", "SecretAccessKey", "SessionToken")))
        expiration = credentials.get("Expiration")
        require(isinstance(expiration, datetime) and expiration.tzinfo is not None
                and now < expiration.timestamp() <= now + 960)
        signed = factory(credentials=credentials)
        clients.append(signed)
        require(check() < min(claims["exp"], identity.expires_at, expiration.timestamp()))
        caller = signed.get_caller_identity()
        require(caller.get("Account") == config["account"] and caller.get("Arn") == arn
                and caller.get("UserId") == user_id)
        require(check() < min(claims["exp"], identity.expires_at, expiration.timestamp()))
        digest = hashlib.sha256(json.dumps(config, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return dict(schema=OBSERVATION, ok=True, live_accepted=False, unproven=list(UNPROVEN),
            bindings_sha256=digest, source_commit=config["source_commit"],
            account=config["account"], region=config["region"], role_arn=config["role_arn"],
            role_id=config["role_id"], identity=expected,
            cases={"signed_admission_main": True, "sts_same_run_main": True, "exact_role_identity": True})
    except Exception:
        # token, credential, SDK error and response bytes never enter output or exception text
        return _hold()
    finally:
        for client in clients:
            try:
                client.close()
            except Exception:
                pass


def main(argv=None, *, stream=None, output=None, runner=run_probe):
    """Read one ephemeral runner pipe; reject arguments and interactive terminals."""
    output = sys.stdout if output is None else output
    try:
        require(not (sys.argv[1:] if argv is None else argv))
        if stream is None:
            require(stat.S_ISFIFO(os.fstat(sys.stdin.fileno()).st_mode))
            stream = sys.stdin.buffer
        require(not stream.isatty())
        raw = stream.read(MAX_INPUT + 1)
        require(isinstance(raw, bytes) and 0 < len(raw) <= MAX_INPUT)
        envelope = parse_json(raw.decode("utf-8"))
        require(type(envelope) is dict and set(envelope) == {"config", "admission_token", "sts_token"})
        result = runner(envelope["config"], lambda: {name: envelope[name]
            for name in ("admission_token", "sts_token")})
    except Exception:
        result = _hold()
    output.write(json.dumps(result, sort_keys=True, allow_nan=False) + "\n")
    return 0 if result["ok"] else 1


def _timeout(signum, frame):
    raise TimeoutError("OIDC probe deadline exceeded")


if __name__ == "__main__":
    signal.signal(signal.SIGALRM, _timeout)
    signal.setitimer(signal.ITIMER_REAL, SECONDS)
    raise SystemExit(main())
