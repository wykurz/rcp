"""Credential-free early workflow guard; server admission remains authoritative.

No prices, budgets or schedule slots are accepted from workflow inputs. Selected
source builds never receive OIDC or service credentials. A scheduled run always
builds the signed triggering main revision once for both A/A sides.
"""

from datetime import datetime, timezone
import os
from pathlib import Path
import re

from .common import require

OWNER = "483465"
REPOSITORY = "wykurz/rcp"
REPOSITORY_ID = "655073755"
WORKFLOW = "wykurz/rcp/.github/workflows/aws-benchmark-production.yml@refs/heads/main"


REGULAR_CRON = "17 8 * * 1,5"
COMMISSIONING_CRON = "17 5 8 11 *"
COMMISSIONING_START = datetime(2026, 11, 8, 5, 17, tzinfo=timezone.utc)
COMMISSIONING_END = datetime(2026, 11, 8, 5, 47, tzinfo=timezone.utc)


def source(environment, now=None):
    require(environment.get("GITHUB_REPOSITORY") == REPOSITORY
            and environment.get("GITHUB_REPOSITORY_ID") == REPOSITORY_ID
            and environment.get("GITHUB_REF") == "refs/heads/main"
            and environment.get("GITHUB_WORKFLOW_REF") == WORKFLOW
            and environment.get("GITHUB_ACTOR_ID") == OWNER
            and environment.get("GITHUB_RUN_ATTEMPT") == "1",
            "only the owner may request the current main benchmark workflow")
    require(environment.get("RCP_PRODUCTION_ENABLED") == "true", "production workflow is disabled")
    require(environment.get("RCP_MODE") == "benchmark", "not a benchmark request")
    current = environment.get("GITHUB_SHA", "")
    require(re.fullmatch(r"[0-9a-f]{40}", current)
            and environment.get("GITHUB_WORKFLOW_SHA") == current,
            "triggering source and controller SHA differ")
    requested = environment.get("RCP_REQUESTED_REVISION", "")
    if environment.get("GITHUB_EVENT_NAME") == "workflow_dispatch":
        revision = requested or current
    elif environment.get("GITHUB_EVENT_NAME") == "schedule":
        require(environment.get("RCP_SCHEDULE_ENABLED") == "true", "scheduled admission is disabled")
        require(requested == "", "scheduled requests cannot select another revision")
        event_schedule = environment.get("RCP_EVENT_SCHEDULE")
        require(event_schedule in (REGULAR_CRON, COMMISSIONING_CRON),
                "unknown or missing scheduled event context")
        if event_schedule == COMMISSIONING_CRON:
            current_time = datetime.now(timezone.utc) if now is None else now
            require(COMMISSIONING_START <= current_time <= COMMISSIONING_END,
                    "temporary commissioning schedule is outside its approved window")
        revision = current
    else:
        raise ValueError("only owner manual and approved scheduled requests are supported")
    require(re.fullmatch(r"[0-9a-f]{40}", revision), "a full source SHA is required")
    return revision


def main():
    revision = source(os.environ)
    destination = os.environ.get("GITHUB_OUTPUT", "")
    require(destination != "", "workflow output destination is missing")
    with Path(destination).open("a", encoding="utf-8") as stream:
        stream.write("revision=" + revision + "\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("Benchmark request rejected (" + type(error).__name__ + ").") from None
