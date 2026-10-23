"""Initially disabled scheduled collection of the one active production lease.

There is no request-selected run or launch authority. GitHub metadata uses the
separately provisioned repository-scoped App credential; it cannot trigger workflows.
The independent watchdog is a different function/role and continues on failures.
Publishing immutable evidence precedes the revision-fenced ownership release.
"""

from datetime import datetime, timezone
import os
import json
import re

from .common import require, error_code
from .durable_aws import Configuration, clients
from .results import diagnostic
from .production_errors import transient
from .production_collection import Collector
from .production_github import TerminalProducerReader
from .production_github_app import GitHubAppConfig, cached_provider
from .production_runtime import ProductionService


def collection_clients(region):
    api = clients(region)
    import boto3
    from botocore.config import Config
    api["stepfunctions"] = boto3.client("stepfunctions", region_name=region,
        config=Config(connect_timeout=3, read_timeout=5, retries={"total_max_attempts": 1},
                      proxies={}, ignore_configured_endpoint_urls=True),
        endpoint_url=f"https://states.{region}.amazonaws.com")
    return api


def collect_active(api, config, machine, reader, *, now=lambda: datetime.now(timezone.utc),
                   service_factory=ProductionService, collector_factory=Collector):
    require(re.fullmatch(r"arn:aws:states:" + re.escape(config.region) + ":" + re.escape(config.account)
                        + r":stateMachine:[A-Za-z0-9_-]{1,80}", machine), "collection machine scope differs")
    value = api["dynamodb"].get_item(TableName=config.table, Key={"pk": {"S": "active"}}, ConsistentRead=True).get("Item")
    if value is None:
        return dict(state="idle", retired=False)
    require(isinstance(value, dict) and set(value) == {"pk", "run_key", "epoch"}
            and value["pk"] == {"S": "active"}
            and isinstance(value["run_key"], dict) and set(value["run_key"]) == {"S"}
            and isinstance(value["epoch"], dict) and set(value["epoch"]) == {"S"}, "invalid active collection pointer")
    run_key, epoch = value["run_key"]["S"], value["epoch"]["S"]
    require(isinstance(run_key, str) and re.fullmatch(r"repo-[1-9][0-9]{0,19}-run-[1-9][0-9]{0,19}", run_key)
            and isinstance(epoch, str) and re.fullmatch(r"[0-9a-f]{64}", epoch), "invalid active collection identity")
    service = service_factory(api, config, run_key, epoch, now=now)
    return collector_factory(service, machine, reader, now=now).collect(retire=True)


def lambda_handler(event, context):
    if os.environ.get("RCP_PRODUCTION_ENABLED", "false") != "true":
        return dict(state="disabled", retired=False)
    if event != {}:
        return dict(state="held", retired=False, reason="unexpected collection invocation")
    try:
        config = Configuration.environment()
        app = GitHubAppConfig.from_environment(os.environ, region=config.region, account=config.account)
        reader = TerminalProducerReader(repository=app.repository, repository_id=app.repository_id,
            repository_owner_id=app.repository_owner_id, observation_factory=cached_provider(app).observation)
        result = collect_active(collection_clients(config.region), config,
                                os.environ["RCP_PRODUCTION_MACHINE_ARN"], reader)
        if result.get("state") == "held":
            print(json.dumps(dict(event="production_collection_held", reason=result.get("reason", "unknown")[:256])))
        return result
    except Exception as error:
        detail = dict(event="production_collection_held", category=type(error).__name__,
                      reason=diagnostic(error)[:256], retryable=transient(error))
        code = error_code(error)
        if isinstance(code, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9.]{0,79}", code):
            detail["service_error"] = code
        print(json.dumps(detail, sort_keys=True))
        return dict(state="held", retired=False, reason="collection requires reconciliation")
