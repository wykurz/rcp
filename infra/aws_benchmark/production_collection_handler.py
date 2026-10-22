"""Initially disabled scheduled collection of the one active production lease.

There is no request-selected run, no launch authority and no GitHub credential.
The independent watchdog is a different function/role and continues on failures.
Publishing immutable evidence precedes the revision-fenced ownership release.
"""

from datetime import datetime, timezone
import os
import re

from .common import require
from .durable_aws import Configuration, clients
from .production_collection import Collector
from .production_github import TerminalProducerReader
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
        reader = TerminalProducerReader(repository=os.environ["RCP_PRODUCTION_REPOSITORY"],
            repository_id=os.environ["RCP_PRODUCTION_REPOSITORY_ID"],
            repository_owner_id=os.environ["RCP_PRODUCTION_OWNER_ID"])
        return collect_active(collection_clients(config.region), config,
                              os.environ["RCP_PRODUCTION_MACHINE_ARN"], reader)
    except Exception:
        return dict(state="held", retired=False, reason="collection requires reconciliation")
