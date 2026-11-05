"""Manual publication reader lane; no publication, mutation, invocation or secret access."""

import json
import os
import re
import signal
import sys
import time

from benchmarks.strict_json import parse_json
from . import acceptance_publication as probe
from .acceptance_cases import require
from .acceptance_oidc_workflow import Interrupted, checkout_commit, interrupt, request_token


def configuration(env, *, clock=time.time, source=checkout_commit):
    """All local approval/source/run checks precede token requests and client creation."""
    require(env.get("RCP_PUBLICATION_MODE") == "reader-test")
    require(all(env.get(key, "") in {"", "false"} for key in
        ("RCP_PRODUCTION_ENABLED", "RCP_PUBLISH_ENABLED", "RCP_TRUST_READY")))
    expected = dict(GITHUB_REPOSITORY="wykurz/rcp", GITHUB_REPOSITORY_ID="655073755",
        GITHUB_REPOSITORY_OWNER="wykurz", GITHUB_REPOSITORY_OWNER_ID="483465",
        GITHUB_REF="refs/heads/main", GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_ACTOR="wykurz",
        GITHUB_ACTOR_ID="483465", GITHUB_TRIGGERING_ACTOR="wykurz", GITHUB_RUN_ATTEMPT="1",
        GITHUB_WORKFLOW_REF=probe.WORKFLOW, GITHUB_WORKFLOW=probe.WORKFLOW_NAME,
        RUNNER_ENVIRONMENT="github-hosted")
    require(all(env.get(key) == value for key, value in expected.items()))
    raw = env.get("RCP_PUBLICATION_APPROVAL", "")
    require(isinstance(raw, str) and 0 < len(raw.encode("utf-8")) <= 8192)
    approval = probe.validate_approval(parse_json(raw))
    require(approval["source_commit"] == env.get("GITHUB_SHA") == env.get("GITHUB_WORKFLOW_SHA"))
    require(isinstance(env.get("GITHUB_RUN_ID"), str)
        and re.fullmatch(r"[1-9][0-9]{0,19}", env["GITHUB_RUN_ID"]) is not None)
    now = probe.timestamp(clock)
    require(approval["not_before"] <= now and now + probe.SECONDS < approval["expires_at"])
    require(source() == approval["source_commit"])
    return probe.validate_bindings({**approval, "run_id": env["GITHUB_RUN_ID"], "run_attempt": "1"})


def run(env, *, clock=time.time, source=checkout_commit, fetch=request_token,
        runner=probe.run_probe, progress=None):
    progress = probe.Progress() if progress is None else progress
    try:
        approval = configuration(env, clock=clock, source=source)
        used = False
        def token():
            nonlocal used
            progress.stage = probe.Stage.TOKEN_REQUEST
            require(not used)
            used = True
            require(approval["not_before"] <= probe.timestamp(clock) < approval["expires_at"])
            return fetch("sts.amazonaws.com", env)
        return runner(approval, token, clock=clock, progress=progress)
    except Exception:
        return probe.hold(progress)


def main(argv=None, *, env=None, output=None, runner=run):
    output = sys.stdout if output is None else output
    progress = probe.Progress()
    try:
        require(not (sys.argv[1:] if argv is None else argv))
        result = runner(os.environ if env is None else env, progress=progress)
    except (Exception, Interrupted):
        result = probe.hold(progress)
    output.write(json.dumps(result, sort_keys=True, allow_nan=False) + "\n")
    return 0 if result["ok"] else 1


def cli():
    for signum in (signal.SIGALRM, signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, interrupt)
    signal.setitimer(signal.ITIMER_REAL, probe.SECONDS)
    try:
        return main()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)


if __name__ == "__main__":
    raise SystemExit(cli())
