"""Manual main-only negative identity adapter; never change access or call admission.

The runner journal contains only sanitized metadata and is fsynced before STS.
It is not durable beyond runner loss unless artifact upload succeeds; the operator
must retain dispatch intent and the approved worst-case recovery cutoff separately.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import sys
import time

from benchmarks.strict_json import parse_json
from .acceptance_cases import require
from . import acceptance_oidc_negative as probe
from . import acceptance_oidc_workflow as positive_workflow

APPROVAL_SCHEMA = "rcp-oidc-negative-workflow-approval-v1"
APPROVAL_FIELDS = {"schema", "source_commit", "not_before", "expires_at",
                   "recovery_cutoff", "approval_capture_sha256"}
JOURNAL_NAME = "oidc-negative-journal.jsonl"
EVENTS = {"configuration", "before-sts-send", "denied", "unexpected-session-response"}


class Interrupted(BaseException):
    """Cancellation propagates past transport/SDK cleanup that catches Exception."""


def interrupt(signum, frame):
    raise Interrupted()


def configuration(env, *, clock=time.time, source=positive_workflow.checkout_commit):
    require(env.get("RCP_PRODUCTION_ENABLED", "") in {"", "false"})
    expected = {"GITHUB_REPOSITORY": "wykurz/rcp", "GITHUB_REPOSITORY_ID": "655073755",
        "GITHUB_REPOSITORY_OWNER": "wykurz", "GITHUB_REPOSITORY_OWNER_ID": "483465",
        "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_ACTOR": "wykurz", "GITHUB_ACTOR_ID": "483465",
        "GITHUB_TRIGGERING_ACTOR": "wykurz", "GITHUB_RUN_ATTEMPT": "1",
        "GITHUB_WORKFLOW_REF": probe.WORKFLOW, "GITHUB_WORKFLOW": probe.NAME,
        "RUNNER_ENVIRONMENT": "github-hosted"}
    require(all(env.get(name) == value for name, value in expected.items()))
    raw = env.get("RCP_IDENTITY_APPROVAL", "")
    require(type(raw) is str and 0 < len(raw.encode("utf-8")) <= 4096)
    approval = parse_json(raw)
    require(type(approval) is dict and set(approval) == APPROVAL_FIELDS
            and approval["schema"] == APPROVAL_SCHEMA)
    require(approval["source_commit"] == env.get("GITHUB_SHA") == env.get("GITHUB_WORKFLOW_SHA"))
    config = probe.validate_config(dict(schema=probe.SCHEMA, account="381183037896",
        region="us-east-1", source_commit=approval["source_commit"], role_arn=probe.ROLE,
        role_id=probe.ROLE_ID, workflow_ref=probe.WORKFLOW, workflow_name=probe.NAME,
        run_id=env.get("GITHUB_RUN_ID"), run_attempt="1", actor="wykurz", actor_id="483465",
        event_name="workflow_dispatch", not_before=approval["not_before"],
        expires_at=approval["expires_at"], recovery_cutoff=approval["recovery_cutoff"],
        approval_capture_sha256=approval["approval_capture_sha256"]))
    now = clock()
    require(type(now) in (int, float) and math.isfinite(now)
            and config["not_before"] <= now and now + probe.SECONDS < config["expires_at"])
    require(source() == config["source_commit"])
    return config


def checked_evidence(value):
    require(type(value) is dict and set(value) == {"event", "diagnostics"}
            and value["event"] in EVENTS)
    d = value["diagnostics"]
    require(type(d) is dict and set(d) == {"stage", "sts_error", "request_id", "recovery"})
    require(d["stage"] in {entry.value for entry in probe.Stage} | {"unknown"})
    require(d["sts_error"] in {entry.value for entry in probe.ErrorCode})
    require(d["request_id"] is None or probe.request_id(d["request_id"]) == d["request_id"])
    r = d["recovery"]
    require(type(r) is dict and set(r) == {"possible_session", "send_started_at",
        "token_expires_at", "session_expires_at", "reconcile_until", "session_identity",
        "coverage_exceeded"})
    require(type(r["possible_session"]) is bool and type(r["coverage_exceeded"]) is bool
            and r["session_identity"] in {"not-observed", "expected", "other"})
    for name in ("send_started_at", "token_expires_at", "session_expires_at", "reconcile_until"):
        require(r[name] is None or probe._optional_integer(r[name]) == r[name])
    return json.loads(json.dumps(value, sort_keys=True, allow_nan=False))


class Journal:
    """Write only checked metadata through one held descriptor; never adopt a file."""

    def __init__(self, directory, config):
        require(type(directory) is str and Path(directory).is_absolute()
                and ".." not in Path(directory).parts)
        self.fd = None
        self.sequence = 0
        self.source = config["source_commit"]
        self.digest = hashlib.sha256(json.dumps(config, sort_keys=True,
            separators=(",", ":")).encode()).hexdigest()
        parent = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            self.fd = os.open(JOURNAL_NAME, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW,
                              0o600, dir_fd=parent)
        finally:
            os.close(parent)

    def __enter__(self):
        return self

    def emit(self, value):
        value = checked_evidence(value)
        require(self.fd is not None and self.sequence < 3)
        entry = dict(schema="rcp-oidc-negative-journal-v1", source_commit=self.source,
            bindings_sha256=self.digest, sequence=self.sequence, **value)
        raw = (json.dumps(entry, sort_keys=True, allow_nan=False) + "\n").encode()
        require(len(raw) <= 8192)
        pending = memoryview(raw)
        while pending:
            written = os.write(self.fd, pending)
            require(written > 0)
            pending = pending[written:]
        os.fsync(self.fd)
        self.sequence += 1

    def __exit__(self, kind, error, traceback):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def run(env, *, source=positive_workflow.checkout_commit, clock=time.time,
        fetch=positive_workflow.request_token, runner=probe.run_probe,
        journal_factory=Journal, progress=None):
    progress = probe.Progress() if progress is None else progress
    try:
        config = configuration(env, clock=clock, source=source)
        with journal_factory(env.get("RUNNER_TEMP"), config) as journal:
            journal.emit(dict(event="configuration", diagnostics=progress.snapshot()))
            used = False
            def token():
                nonlocal used
                require(not used)
                used = True
                require(config["not_before"] <= clock() < config["expires_at"])
                return fetch("sts.amazonaws.com", env)
            return runner(config, token, evidence=journal.emit, clock=clock, progress=progress)
    except Exception:
        return probe._hold(progress)


def main(argv=None, *, env=None, output=None, runner=run):
    output = sys.stdout if output is None else output
    progress = probe.Progress()
    try:
        require(not (sys.argv[1:] if argv is None else argv))
        result = runner(os.environ if env is None else env, progress=progress)
    except (Exception, Interrupted):
        result = probe._hold(progress)
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
