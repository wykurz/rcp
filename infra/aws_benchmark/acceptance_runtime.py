"""Fixed empty-input Lambda probes; deployment and invocation require separate approval.

No code here deploys, launches EC2, changes access, deletes evidence, or invokes
production handlers. The existing reader roles have mutation authority, so code
review plus disabled production preflights remain mandatory. A returned observation
is not complete live acceptance. Cold-start replay is not an exactly-once guarantee.
"""

import json
import math
from pathlib import Path
import re
import time

from .acceptance_bindings import parse_bindings, validate_bindings, digest_bindings
from .acceptance_cases import Bindings, Hold, require, reader_cases, storage_cases, transaction_cases
from .production_github_app import GitHubAppConfig, GitHubAppProvider, _suppress_sdk_debug
from .production_github import TerminalProducerReader

BINDING_FILE = Path(__file__).resolve().parents[2] / "acceptance-bindings.json"
_USED = False


def sdk_client(service, region):
    """Ignore user-configured endpoints/proxies and disable automatic SDK retries."""
    import boto3
    from botocore.config import Config
    require(service in {"sts", "secretsmanager", "s3", "dynamodb"} and region == "us-east-1")
    _suppress_sdk_debug()
    return boto3.client(service, region_name=region,
        endpoint_url=f"https://{service}.{region}.amazonaws.com",
        config=Config(connect_timeout=1, read_timeout=3, retries={"total_max_attempts": 1},
                      proxies={}, ignore_configured_endpoint_urls=True,
                      s3={"addressing_style": "path"} if service == "s3" else None))


def clock_value(clock):
    value = clock()
    require(type(value) in (int, float) and math.isfinite(value) and value >= 0)
    return value


def verify_context(config, context, clock):
    now = clock_value(clock)
    seconds = 45 if config["kind"].startswith("reader-") else 120
    require(config["not_before"] <= now and now + seconds + 5 < config["expires_at"])
    expected = "arn:aws:lambda:" + config["region"] + ":" + config["account"] + ":function:" + config["function_name"] + ":1"
    require(getattr(context, "invoked_function_arn", None) == expected)
    require(getattr(context, "function_version", None) == "1")
    require(getattr(context, "log_group_name", None) == config["log_group"])
    remaining = context.get_remaining_time_in_millis()
    require(type(remaining) is int and 5000 < remaining <= seconds * 1000)
    return seconds


def verify_role(config, identity):
    """Bind account, exact role name and immutable IAM role ID, never a session name alone."""
    require(isinstance(identity, dict) and identity.get("Account") == config["account"])
    name = config["role_arn"].rsplit("/", 1)[1]
    require(isinstance(identity.get("Arn"), str) and
        re.fullmatch(r"arn:aws:sts::" + config["account"] + r":assumed-role/" +
                     re.escape(name) + r"/[A-Za-z0-9+=,.@_-]{1,64}", identity["Arn"]) is not None)
    require(isinstance(identity.get("UserId"), str) and
            re.fullmatch(re.escape(config["role_id"]) + r":[A-Za-z0-9+=,.@_-]{1,64}", identity["UserId"]) is not None)


class BoundCurrentSecret:
    """Every provider refresh must use the approved current version, including after rotation."""
    def __init__(self, cases, client):
        self.cases, self.client = cases, client

    def get_secret_value(self, **request):
        c = self.cases
        require(request == {"SecretId": c["secret_arn"], "VersionStage": "AWSCURRENT"})
        response = self.client.get_secret_value(**request)
        require(type(response) is dict and response.get("ARN") == c["secret_arn"]
                and response.get("VersionId") == c["current_version"])
        stages = response.get("VersionStages")
        require(type(stages) is list and 1 <= len(stages) <= 20
                and all(type(stage) is str and 0 < len(stage) <= 256 for stage in stages)
                and len(set(stages)) == len(stages) and "AWSCURRENT" in stages
                and "AWSPREVIOUS" not in stages)
        return response


def github_proof(config, secrets, *, provider_factory=GitHubAppProvider):
    b, proof = config["cases"], config["proof"]
    provider = provider_factory(GitHubAppConfig(repository="wykurz/rcp", repository_id="655073755",
        repository_owner_id="483465", region=config["region"], account=config["account"],
        app_client_id=b["app_client_id"], installation_id=b["installation_id"], secret_arn=b["secret_arn"]),
        secrets=BoundCurrentSecret(b, secrets))
    reader = TerminalProducerReader(repository="wykurz/rcp", repository_id="655073755",
        repository_owner_id="483465", observation_factory=provider.observation)
    observation = reader.read(dict(repository="wykurz/rcp", repository_id="655073755",
        repository_owner_id="483465", run_id=proof["run_id"], run_attempt=proof["run_attempt"]))
    for key in ("attempt", "current"):
        run = observation[key]
        require(run["id"] == int(proof["run_id"]) and run["run_attempt"] == 1
                and run.get("head_sha") == proof["head_sha"] and run.get("status") == "completed"
                and run["repository"].get("full_name") == "wykurz/rcp"
                and run["repository"]["id"] == 655073755
                and run["repository"]["owner"]["id"] == 483465)
    require(not observation["pending_deployments"])
    for key in ("attempt_jobs", "current_jobs"):
        require(all(j.get("status") == "completed" and j.get("run_id") == int(proof["run_id"])
                    and j.get("head_sha") == proof["head_sha"] for j in observation[key]["jobs"]))
    return dict(repository_id="655073755", owner_id="483465", app_client_id=b["app_client_id"],
                installation_id=b["installation_id"], actions="read", metadata="read", fixed_proof_read=True)


def run_probe(config, event, context, *, factory=sdk_client, proof_reader=github_proof, clock=time.time):
    """No service clients are created until the entire invocation binding is checked."""
    clients = []
    try:
        config = validate_bindings(config)
        require(type(event) is dict and not event)
        verify_context(config, context, clock)
        def client(service):
            result = factory(service, config["region"])
            clients.append(result)
            return result
        verify_role(config, client("sts").get_caller_identity())
        verify_context(config, context, clock)
        bindings = Bindings(**config["cases"])
        kind = config["kind"]
        if kind.startswith("reader-"):
            secrets = client("secretsmanager")
            result = reader_cases(bindings, secrets, lambda: proof_reader(config, secrets),
                                  fixture_inventory=config["fixture_inventory"])
        elif kind == "s3":
            result = storage_cases(bindings, client("s3"))
        else:
            result = transaction_cases(bindings, client("dynamodb"))
        require(clock_value(clock) < config["expires_at"])
        complete = all(case == "pass" for case in result["cases"].values())
        return dict(schema="rcp-production-acceptance-observation-v1", ok=complete,
                    bindings_sha256=digest_bindings(config), source_commit=config["source_commit"],
                    kind=kind, role_arn=config["role_arn"], function_version="1",
                    live_accepted=False, observation=result)
    except Exception:
        # never echo SDK errors, invocation data, secrets, tokens or response bodies
        return dict(schema="rcp-production-acceptance-observation-v1", ok=False,
                    live_accepted=False, hold="probe incomplete; retain evidence and reconcile before retry")
    finally:
        for client in clients:
            try:
                client.close()
            except Exception:
                pass


def lambda_handler(event, context):
    global _USED
    if _USED:
        return dict(ok=False, live_accepted=False, hold="warm invocation already attempted; reconcile before retry")
    _USED = True
    try:
        with BINDING_FILE.open("rb") as source:
            config = parse_bindings(source.read(16385))
        return run_probe(config, event, context)
    except Exception:
        return dict(ok=False, live_accepted=False, hold="probe bindings unavailable; no acceptance established")
