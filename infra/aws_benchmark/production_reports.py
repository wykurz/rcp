"""Keyless 90-day retained reporting: read AWS, transfer public JSON, never write Git.

Only the download command imports an AWS session. The canonical source is a
consistent closed run record plus its exact versioned frozen collection report.
No phase allocates, retires, changes policy, reads a secret or mints an App token.
"""

import argparse
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import re
import time

from . import production_collection as collection, production_export as public
from . import production_publication as publication
from .common import Budget, ValidationError, digest, require
from .history_contract import OWNER_ID, PUBLICATION_WORKFLOW, REPOSITORY, REPOSITORY_ID
from .publication_artifacts import (MAX_HISTORY, MAX_INDEX, MAX_TOTAL, inventory,
    persist, read, verify_inventory, outputs)

SCHEMA = "rcp-production-retained-reports-v1"
RETENTION_DAYS = 90
MAX_SECONDS = 600
MAX_SOURCE_PER_RECORD = 3 * collection.MAX_DOCUMENT + collection.MAX_REPORT
RUN = re.compile(r"repo-" + REPOSITORY_ID + r"-run-[1-9][0-9]{0,19}")
SHA = re.compile(r"[0-9a-f]{40}")


def revision(value):
    require(isinstance(value, str) and SHA.fullmatch(value), "invalid reporting reader revision")
    return value


def check_identity(environment, *, artifact=False):
    value = revision(environment.get("GITHUB_SHA", ""))
    expected = dict(GITHUB_REPOSITORY=REPOSITORY, GITHUB_REPOSITORY_ID=REPOSITORY_ID,
        GITHUB_REF="refs/heads/main", GITHUB_ACTOR_ID=str(OWNER_ID), GITHUB_RUN_ATTEMPT="1",
        GITHUB_WORKFLOW_REF=REPOSITORY + "/.github/workflows/" + PUBLICATION_WORKFLOW + "@refs/heads/main",
        GITHUB_WORKFLOW_SHA=value)
    require(all(environment.get(key) == item for key, item in expected.items()),
            "keyless reporting workflow identity differs")
    event = environment.get("GITHUB_EVENT_NAME")
    require(event in {"workflow_dispatch", "push"} or
            (event == "schedule" and environment.get("PUBLISH_SCHEDULE_ENABLED") == "true"),
            "keyless reporting event or schedule gate differs")
    require(environment.get("PUBLICATION_ENABLED") == "true", "keyless publication readiness is closed")
    if artifact:
        require(environment.get("EXPECTED_READER") == value and
                re.fullmatch(r"[0-9a-f]{64}", environment.get("EXPECTED_INVENTORY", "")) and
                re.fullmatch(r"[1-9][0-9]*", environment.get("EXPECTED_ARTIFACT", "")),
                "keyless reporting artifact identity differs")
    return value


def site_identity(environment):
    """One Pages writer mode: legacy docs, or complete keyless AWS input, never a fallback."""
    value = revision(environment.get("GITHUB_SHA", ""))
    require(environment.get("PAGES_ENABLED") == "true" and
            environment.get("GITHUB_REPOSITORY") == REPOSITORY and
            environment.get("GITHUB_REPOSITORY_ID") == REPOSITORY_ID and
            environment.get("GITHUB_REF") == "refs/heads/main" and
            environment.get("GITHUB_RUN_ATTEMPT") == "1" and
            environment.get("GITHUB_WORKFLOW_SHA") == value, "Pages workflow identity or gate differs")
    fields = ("EXPECTED_READER", "EXPECTED_INVENTORY", "EXPECTED_ARTIFACT")
    if any(environment.get(key) for key in fields):
        check_identity(environment, artifact=True)
    else:
        require(environment.get("PUBLICATION_ENABLED", "") in {"", "false"} and
                environment.get("GITHUB_EVENT_NAME") in {"push", "workflow_dispatch"} and
                environment.get("GITHUB_WORKFLOW_REF") ==
                    REPOSITORY + "/.github/workflows/benchmark-pages.yml@refs/heads/main",
                "docs-only Pages would discard AWS reporting or has a foreign identity")
    return value


def site_gate(environment, *, observe=None):
    value = site_identity(environment)
    if observe is None:
        # no AWS/session import: this is only a public exact-main Git read
        from .publication_identity import observed_main
        observe = observed_main
    current = observe() == value
    outputs(current=str(current).lower(), commit=value)
    if not current:
        print("::warning::Pages source is stale; preserve the existing site and dispatch current main.")
    return current


def observation(value, *, active=False):
    require(isinstance(value, dict) and set(value) <= {"state", "retired", "run_key", "phase", "reason"} and
            value.get("state") in {"idle", "pending", "held", "ready", "published", "deferred"} and
            type(value.get("retired")) is bool, "invalid retained reporting observation")
    if "run_key" in value:
        require(isinstance(value["run_key"], str) and RUN.fullmatch(value["run_key"]), "invalid observed run")
    for key in ("phase", "reason"):
        if key in value:
            require(isinstance(value[key], str) and len(value[key]) <= 512, "unbounded reporting observation")
    if active:
        require(value["state"] in {"idle", "pending", "held"} and value["retired"] is False,
                "invalid active ownership observation")
    return value


def snapshot(reader, output, reader_revision, *, clock=time.monotonic, now=None):
    """Rebuild from small frozen reports, with no raw archive downloads or prior Git cache."""
    revision(reader_revision)
    started, source_budget, total, observations, records = clock(), 0, 0, [], []
    output = Path(output)
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    try:
        active = reader.active_observation()
    except Exception as error:
        active = dict(state="held", retired=False, reason=collection.results.diagnostic(error))
    observation(active, active=True)
    keys = reader.discover()
    require(isinstance(keys, list) and len(keys) <= publication.MAX_RUNS and len(set(keys)) == len(keys),
            "retained report discovery differs")
    for key in keys:
        run_key = reader.run_key(key)
        require(RUN.fullmatch(run_key), "retained report belongs to another repository")
        if clock() - started >= MAX_SECONDS or source_budget + MAX_SOURCE_PER_RECORD > MAX_TOTAL or \
                total + public.MAX_PUBLIC_RECORD + MAX_INDEX > MAX_HISTORY:
            observations.append(dict(run_key=run_key, state="deferred", retired=False,
                reason="bounded report snapshot exhausted; no partial site will be deployed"))
            continue
        source_budget += MAX_SOURCE_PER_RECORD
        try:
            value = reader.observe_public(key)
            if value["state"] == "ready":
                record = public.validate_public(value["record"])
                require(record["run_key"] == run_key, "frozen public record identity differs")
                raw = collection.encoded(record)
                require(len(raw) <= public.MAX_PUBLIC_RECORD, "public record exceeds bound")
                (output / (run_key + ".json")).write_bytes(raw)
                records.append(dict(run_key=run_key, sha256=digest(raw), bytes=len(raw)))
                total += len(raw)
            status = {name: item for name, item in value.items() if name != "record"}
            observations.append(observation(dict(status, run_key=run_key)))
        except Exception as error:
            observations.append(dict(run_key=run_key, state="held", retired=False,
                reason=collection.results.diagnostic(error)))
    value = dict(schema=SCHEMA, repository_id=REPOSITORY_ID, reader_revision=reader_revision,
        observed_at=(now or datetime.now(timezone.utc)).isoformat(), retention_days=RETENTION_DAYS,
        discovery_complete=True, processing_complete=not any(x["state"] == "deferred" for x in observations),
        active=active, observations=observations, records=sorted(records, key=lambda x: x["run_key"]))
    persist(output / "index.json", value)
    proof = inventory(output, limit=MAX_HISTORY)
    public_batch(output, reader_revision, proof)
    return proof


def public_batch(source, reader_revision, expected):
    """Validate exact current-run artifact bytes; the trusted job output supplies authenticity."""
    revision(reader_revision)
    verify_inventory(source, expected, limit=MAX_HISTORY)
    source = Path(source)
    index = collection.results.json_value(read(source / "index.json", MAX_INDEX), MAX_INDEX)
    fields = {"schema", "repository_id", "reader_revision", "observed_at", "retention_days",
              "discovery_complete", "processing_complete", "active", "observations", "records"}
    require(isinstance(index, dict) and set(index) == fields and index["schema"] == SCHEMA and
            index["repository_id"] == REPOSITORY_ID and index["reader_revision"] == reader_revision and
            type(index["retention_days"]) is int and index["retention_days"] == RETENTION_DAYS and
            index["discovery_complete"] is True and type(index["processing_complete"]) is bool,
            "retained report batch identity differs")
    collection.results.utc(index["observed_at"])
    observation(index["active"], active=True)
    require(isinstance(index["observations"], list) and len(index["observations"]) <= publication.MAX_RUNS,
            "retained report observations exceed bound")
    observed = {}
    for item in index["observations"]:
        observation(item)
        name = item.get("run_key")
        require(name is not None and name not in observed and item["state"] in {"ready", "pending", "held", "deferred"},
                "retained report observations differ")
        require(item["state"] != "ready" or item["retired"] is True, "ready report is not retired")
        observed[name] = item
    require(index["processing_complete"] == (not any(x["state"] == "deferred" for x in observed.values())),
            "retained report completeness differs")
    require(isinstance(index["records"], list) and len(index["records"]) <= publication.MAX_RUNS,
            "retained public record count exceeds bound")
    found = {}
    for ref in index["records"]:
        require(isinstance(ref, dict) and set(ref) == {"run_key", "sha256", "bytes"} and
                isinstance(ref["run_key"], str) and RUN.fullmatch(ref["run_key"]) and ref["run_key"] not in found,
                "invalid retained public record reference")
        raw = read(source / (ref["run_key"] + ".json"), public.MAX_PUBLIC_RECORD)
        require(type(ref["bytes"]) is int and len(raw) == ref["bytes"] and ref["sha256"] == digest(raw),
                "retained public record reference differs")
        record = public.validate_public(collection.results.json_value(raw, public.MAX_PUBLIC_RECORD))
        require(record["run_key"] == ref["run_key"], "retained public record identity differs")
        found[ref["run_key"]] = raw
    require(set(found) == {name for name, item in observed.items() if item["state"] == "ready"} and
            {p.name for p in source.iterdir()} == {name + ".json" for name in found} | {"index.json"},
            "retained public file set differs from observations")
    return found, index


def deployable(index):
    return index["discovery_complete"] is True and index["processing_complete"] is True and \
        index["active"]["state"] != "held" and not any(x["state"] in {"held", "deferred"} for x in index["observations"])


def summary(index):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        counts = {state: sum(x["state"] == state for x in index["observations"]) for state in ("ready", "pending", "held", "deferred")}
        text = "Retained AWS reporting: " + ", ".join(str(n) + " " + state for state, n in counts.items())
        text += ". Active ownership observation: " + index["active"]["state"] + ".\n\n"
        text += "Still-retained view under the 90-day AWS lifecycle; deletion timing may vary. Not permanent history or a new physical cleanup proof.\n\n"
        for item in index["observations"]:
            if item["state"] in {"held", "deferred"}:
                reason = collection.encoded(item.get("reason", "unspecified")).decode().strip()
                reason = reason.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("`", "&#96;")
                text += "- " + item["run_key"] + ": " + item["state"] + " — `" + reason + "`\n"
        with open(path, "a", encoding="utf-8") as stream:
            stream.write(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("identity", "site-gate", "download", "inspect"))
    parser.add_argument("--artifact", action="store_true")
    parser.add_argument("--reader-revision")
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--expected-sha256")
    parser.add_argument("--require-complete", action="store_true")
    for name in ("region", "role", "bucket", "table"):
        parser.add_argument("--" + name)
    args = parser.parse_args()
    if args.command == "identity":
        check_identity(os.environ, artifact=args.artifact)
        return
    if args.command == "site-gate":
        site_gate(os.environ)
        return
    revision(args.reader_revision)
    if args.command == "download":
        require(check_identity(os.environ) == args.reader_revision, "reporting source differs from current job")
        require(args.output is not None and re.fullmatch(r"[a-z]{2}-[a-z]+-[1-9][0-9]?", args.region or "") and
                re.fullmatch(r"arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+", args.role or ""),
                "invalid read-only reporting configuration")
        from .controller import oidc_session
        session, config = oidc_session(args.region, args.role, session_prefix="rcp-publication-", duration_seconds=900,
            budget=Budget(datetime.now(timezone.utc) + timedelta(seconds=60)), reserve=10)
        reader = publication.Reader(session.client("s3", config=config), session.client("dynamodb", config=config),
            bucket=args.bucket, table=args.table, repository_id=REPOSITORY_ID)
        proof = snapshot(reader, args.output, args.reader_revision)
        records, index = public_batch(args.output, args.reader_revision, proof)
    else:
        require(args.input is not None, "public report directory required")
        proof = args.expected_sha256
        records, index = public_batch(args.input, args.reader_revision, proof)
    summary(index)
    outputs(inventory_sha256=proof, reader_revision=args.reader_revision, record_count=str(len(records)),
        deployable=str(deployable(index)).lower())
    require(not args.require_complete or deployable(index), "retained reporting held; preserve the previous site")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        reason = str(error) if isinstance(error, ValidationError) else type(error).__name__
        print("Retained reporting failed or held: " + reason)
        raise SystemExit(1) from None
