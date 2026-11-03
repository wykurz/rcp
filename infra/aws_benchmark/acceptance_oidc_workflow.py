"""Manual identity-only workflow adapter; no production clients or worker operations.

The runner's built-in request capability is used only after validating all local
bindings. OIDC JWTs and the deny-all STS session stay in memory in this process.
Approval hashes are audit references, not authorization credentials. Actual live
dispatch and its exact role/trust/window require separate operator approval.
"""

import http.client
import json
import math
import os
from pathlib import Path
import re
import signal
import ssl
import subprocess
import sys
import time
import urllib.parse

from benchmarks.strict_json import parse_json
from . import acceptance_oidc as probe
from .acceptance_cases import require

WORKFLOW = "wykurz/rcp/.github/workflows/aws-benchmark-production.yml@refs/heads/main"
ROLE = "arn:aws:iam::381183037896:role/rcp-production-benchmark-GitHubSubmitRole-hg2kGsuS3Qa1"
ROLE_ID = "AROAVRQCHSHEKT2CRHFFR"
AUDIENCE = "rcp-production-benchmark-admission"
APPROVAL_SCHEMA = "rcp-oidc-workflow-approval-v1"
APPROVAL_FIELDS = {"schema", "source_commit", "not_before", "expires_at", "approval_capture_sha256"}
MAX_RESPONSE = 65536
TOKEN_TIMEOUT = 10
ROOT = Path(__file__).resolve().parents[2]


class Interrupted(BaseException):
    """Cancellation must pass through SDK/transport cleanup that catches Exception."""


def interrupt(signum, frame):
    raise Interrupted()


def checkout_commit():
    """Verify the immutable trusted checkout, without inheriting token capabilities."""
    env = {"PATH": "/usr/bin:/bin", "LANG": "C", "GIT_CONFIG_NOSYSTEM": "1",
           "GIT_CONFIG_GLOBAL": os.devnull}
    def git(*args):
        return subprocess.run(["/usr/bin/git", "--no-optional-locks", *args], cwd=ROOT,
            env=env, check=True, capture_output=True, timeout=5).stdout.decode("ascii").strip()
    require(not git("status", "--porcelain", "--untracked-files=all"))
    return git("rev-parse", "HEAD")


def configuration(env, *, clock=time.time, source=checkout_commit):
    """Reject every mode/source/run/window mismatch before requesting either token."""
    require(env.get("RCP_ACCEPTANCE_MODE") == "identity-test")
    require(env.get("RCP_PRODUCTION_ENABLED", "") in {"", "false"})
    require(env.get("RCP_REQUESTED_REVISION", "") == "")
    expected = {"GITHUB_REPOSITORY": "wykurz/rcp", "GITHUB_REPOSITORY_ID": "655073755",
        "GITHUB_REPOSITORY_OWNER": "wykurz", "GITHUB_REPOSITORY_OWNER_ID": "483465",
        "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_ACTOR": "wykurz", "GITHUB_ACTOR_ID": "483465",
        "GITHUB_TRIGGERING_ACTOR": "wykurz", "GITHUB_RUN_ATTEMPT": "1",
        "GITHUB_WORKFLOW_REF": WORKFLOW, "GITHUB_WORKFLOW": "AWS production benchmark",
        "RUNNER_ENVIRONMENT": "github-hosted"}
    require(all(env.get(name) == value for name, value in expected.items()))
    raw = env.get("RCP_IDENTITY_APPROVAL", "")
    require(isinstance(raw, str) and 0 < len(raw.encode("utf-8")) <= 4096)
    approval = parse_json(raw)
    require(type(approval) is dict and set(approval) == APPROVAL_FIELDS)
    require(approval["schema"] == APPROVAL_SCHEMA)
    require(approval["source_commit"] == env.get("GITHUB_SHA") == env.get("GITHUB_WORKFLOW_SHA"))
    config = probe.validate_config(dict(schema=probe.SCHEMA, account="381183037896",
        region="us-east-1", source_commit=approval["source_commit"], role_arn=ROLE,
        role_id=ROLE_ID, admission_audience=AUDIENCE, workflow_ref=WORKFLOW,
        run_id=env.get("GITHUB_RUN_ID"), run_attempt="1", actor="wykurz", actor_id="483465",
        event_name="workflow_dispatch", not_before=approval["not_before"],
        expires_at=approval["expires_at"], approval_capture_sha256=approval["approval_capture_sha256"]))
    now = clock()
    require(type(now) in (int, float) and math.isfinite(now)
            and config["not_before"] <= now and now + probe.SECONDS < config["expires_at"])
    require(source() == config["source_commit"])
    return config


def request_token(audience, env, *, connection_factory=http.client.HTTPSConnection,
                  monotonic=time.monotonic):
    """One bounded HTTPS request to the runner's GitHub Actions endpoint; no redirects/proxies."""
    connection = response = None
    try:
        require(audience in {AUDIENCE, "sts.amazonaws.com"})
        url = env["ACTIONS_ID_TOKEN_REQUEST_URL"]
        bearer = env["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]
        require(isinstance(url, str) and 0 < len(url) <= 8192 and url.isascii()
                and all(32 < ord(c) < 127 for c in url))
        require(isinstance(bearer, str) and 0 < len(bearer) <= 16384 and bearer.isascii()
                and all(32 < ord(c) < 127 for c in bearer))
        parsed = urllib.parse.urlsplit(url)
        require(parsed.scheme == "https" and parsed.hostname is not None
                and re.fullmatch(r"[a-z0-9-]+(?:\.[a-z0-9-]+)*\.actions\.githubusercontent\.com",
                                 parsed.hostname) is not None
                and parsed.port in (None, 443) and parsed.username is None and parsed.password is None
                and not parsed.fragment and parsed.path.startswith("/"))
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True, max_num_fields=16)
        require(not any(key.lower() == "audience" for key, _ in query))
        target = urllib.parse.urlunsplit(("", "", parsed.path,
            urllib.parse.urlencode(query + [("audience", audience)]), ""))
        deadline = monotonic() + TOKEN_TIMEOUT
        connection = connection_factory(parsed.hostname, timeout=TOKEN_TIMEOUT,
                                         context=ssl.create_default_context())
        connection.request("GET", target, headers={"Authorization": "Bearer " + bearer,
            "Accept": "application/json", "Connection": "close"})
        transport = connection.sock
        response = connection.getresponse()
        require(response.status == 200 and response.getheader("Content-Encoding", "identity") == "identity")
        chunks, remaining = [], MAX_RESPONSE + 1
        while remaining:
            timeout = deadline - monotonic()
            require(timeout > 0)
            if transport is not None and not response.isclosed():
                transport.settimeout(timeout)
            chunk = response.read1(remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        require(remaining > 0 and response.length in (None, 0) and monotonic() < deadline)
        body = parse_json(b"".join(chunks).decode("utf-8"))
        require(type(body) is dict)
        token = body.get("value")
        require(isinstance(token, str) and len(token) <= probe.auth.MAX_TOKEN_BYTES
                and re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token) is not None)
        return token
    except Exception:
        # never expose a URL, bearer, body, returned token or raw transport exception
        raise ValueError("GitHub identity token request failed") from None
    finally:
        for item in (response, connection):
            if item is not None:
                try:
                    item.close()
                except Exception:
                    pass


def run(env, *, source=checkout_commit, clock=time.time, fetch=request_token, runner=probe.run_probe):
    """Validate first, fetch each audience once, then reuse the identity-only adapter."""
    try:
        config = configuration(env, source=source, clock=clock)
        used = False
        def tokens():
            nonlocal used
            require(not used)
            used = True
            result = {}
            for name, audience in (("admission_token", AUDIENCE), ("sts_token", "sts.amazonaws.com")):
                require(config["not_before"] <= clock() < config["expires_at"])
                result[name] = fetch(audience, env)
            return result
        return runner(config, tokens, clock=clock)
    except Exception:
        return probe._hold()


def main(argv=None, *, env=None, output=None, runner=run):
    output = sys.stdout if output is None else output
    try:
        require(not (sys.argv[1:] if argv is None else argv))
        result = runner(os.environ if env is None else env)
    except (Exception, Interrupted):
        result = probe._hold()
    output.write(json.dumps(result, sort_keys=True, allow_nan=False) + "\n")
    return 0 if result["ok"] else 1


def cli():
    # one process deadline covers both token requests, signature verification and STS
    for signum in (signal.SIGALRM, signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, interrupt)
    signal.setitimer(signal.ITIMER_REAL, probe.SECONDS)
    try:
        return main()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)


if __name__ == "__main__":
    raise SystemExit(cli())
