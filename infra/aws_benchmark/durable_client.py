"""Short credentialed submission and collection; candidate executables remain opaque data."""

import argparse
import base64
from datetime import datetime, timedelta, timezone
import json
import html
import os
from pathlib import Path
import re
import urllib.request

from benchmarks.strict_json import parse_json
from . import durable, identity
from .common import (BINARIES, MAX_RESULT, RESULTS_KEY, STATUS_KEY, Budget, digest, error_code, get_bytes,
                     require, retry_io, utc)
from .controller import read_binary
from .durable_aws import RECEIPT, REQUEST


MAX_RECEIPT = 100_000
SHORT_JOB_SECONDS = 600


def short_budget():
    return Budget(datetime.now(timezone.utc) + timedelta(seconds=SHORT_JOB_SECONDS),
                  ceiling_seconds=SHORT_JOB_SECONDS)


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def read_json(api, bucket, key, limit=MAX_RECEIPT, *, budget=None):
    data = retry_io(lambda: get_bytes(api["s3"], bucket, key, limit), budget or short_budget())
    return parse_json(data.decode("utf-8"))


def execution_arn(machine, run):
    require(re.fullmatch(r"arn:aws:states:[a-z0-9-]+:[0-9]{12}:stateMachine:[A-Za-z0-9_-]{1,80}", machine),
            "state machine must be an exact unqualified ARN")
    require(re.fullmatch(r"[1-9][0-9]{0,19}-[1-9][0-9]{0,4}", run), "invalid run identity")
    return machine.replace(":stateMachine:", ":execution:") + ":" + run


def verify_machine(request, machine):
    execution_arn(machine, request["run"])
    require(machine.split(":")[3:5] == [request["pins"]["region"], request["pins"]["account"]],
            "state machine differs from pinned account or region")


def code(error):
    return error_code(error)


def describe(api, machine, run, *, budget=None):
    arn = execution_arn(machine, run)
    try:
        result = retry_io(lambda: api["stepfunctions"].describe_execution(executionArn=arn),
                          budget or short_budget())
    except Exception as error:
        if code(error) == "ExecutionDoesNotExist":
            return None
        raise
    require(result["executionArn"] == arn and result["stateMachineArn"] == machine,
            "execution resource binding differs")
    require(parse_json(result["input"]) == {"run": run}, "execution input differs from this attempt")
    return result


def operator_request(api, bucket, *, run, provenance=None, exact=True, budget=None):
    request = durable.validate(read_json(api, bucket, REQUEST, budget=budget), exact=exact)
    require(request["run"] == run and request["pins"]["bucket"] == bucket, "operator request binding differs")
    if provenance is not None:
        identity.validate(provenance)
        require(all(request[key] == provenance[key]
                    for key in ("run", "baseline", "candidate", "suite", "controller")),
                "operator request does not match this workflow producer")
    return request


def submission_receipt(request, execution, *, reused=False, reconciled=False):
    return dict(protocol=durable.PROTOCOL, run=request["run"], state="submitted",
                request_sha256=digest(encoded(request)), execution_arn=execution["executionArn"],
                execution_status=execution.get("status", "RUNNING"), reused=reused, reconciled=reconciled,
                benchmark_success=False, message="Submission is not benchmark completion or cleanup verification.")


def submit(api, bucket, machine, provenance, binaries, *, now=lambda: datetime.now(timezone.utc), budget=None):
    budget = budget or short_budget()
    identity.validate(provenance)
    request = operator_request(api, bucket, run=provenance["run"], provenance=provenance, budget=budget)
    verify_machine(request, machine)
    existing = describe(api, machine, request["run"], budget=budget)
    if existing is not None:
        return submission_receipt(request, existing, reused=True)
    require(utc(request["created_at"]) <= now() < utc(request["capacity_deadline"]), "capacity request expired")
    for key in BINARIES:
        data = read_binary(Path(binaries) / key)
        require(digest(data) == request["sha256"][key], "producer binary differs from operator manifest")
        def upload():
            require(now() < utc(request["capacity_deadline"]), "capacity request expired during upload")
            return api["s3"].put_object(Bucket=bucket, Key="input/" + key, Body=data,
                                       ContentType="application/octet-stream",
                                       ChecksumSHA256=base64.b64encode(bytes.fromhex(request["sha256"][key])).decode())
        retry_io(upload, budget)
    # a stale request cannot turn a delayed build/upload into a fresh capacity window
    require(operator_request(api, bucket, run=request["run"], provenance=provenance, budget=budget) == request,
            "operator request changed during upload")
    require(now() < utc(request["capacity_deadline"]), "capacity request expired during upload")
    arn = execution_arn(machine, request["run"])
    budget.left(reserve=30)
    try:
        response = api["stepfunctions"].start_execution(stateMachineArn=machine,
            name=request["run"], input=json.dumps({"run": request["run"]}, separators=(",", ":")))
    except Exception:
        # reconcile an uncertain response; never repeat StartExecution under another name
        found = describe(api, machine, request["run"], budget=budget)
        require(found is not None, "submission outcome is uncertain; inspect the exact execution before retrying")
        return submission_receipt(request, found, reconciled=True)
    require(response["executionArn"] == arn, "unexpected submitted execution ARN")
    return submission_receipt(request, response)


def producer(request):
    run_id, attempt = request["run"].split("-")
    return identity.validate(dict(run=request["run"], baseline=request["baseline"], candidate=request["candidate"],
        suite=request["suite"], controller=request["controller"], ref=identity.REF, repository=identity.REPOSITORY,
        workflow=identity.WORKFLOW, workflow_sha=request["controller"], event="workflow_dispatch",
        run_url=f"https://github.com/{identity.REPOSITORY}/actions/runs/{run_id}/attempts/{attempt}"))


def write(output, name, data):
    # the output directory is newly created by this trusted job; never replace earlier receipts
    with (output / name).open("xb") as stream:
        stream.write(data)


def verify_cleanup(observation, request, binding):
    """Physical cleanup is independent of coordinator, admission and result outcomes."""
    cleanup = observation.get("cleanup")
    if observation.get("phase") != "complete" or cleanup is None:
        return False
    require(set(cleanup) == {"verified_at", "deleted_roots", "live_instances", "allocation_consumed",
                             "unresolved_allocation", "lease_released"}
            and cleanup["live_instances"] == [] and cleanup["unresolved_allocation"] is False
            and cleanup["lease_released"] is False and type(cleanup["allocation_consumed"]) is bool
            and cleanup["allocation_consumed"] is observation.get("allocation_consumed"),
            "invalid physical cleanup receipt")
    earliest = utc(request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
    require(earliest <= utc(cleanup["verified_at"]) <= utc(observation["observed_at"]),
            "cleanup receipt has not drained the independent cutoff")
    seen = observation.get("seen")
    known = observation.get("known_instance_ids")
    require(isinstance(seen, dict) and len(seen) <= 20 and isinstance(known, list)
            and len(known) == len(set(known)) and set(known) == set(seen)
            and (not cleanup["allocation_consumed"] or bool(known)), "invalid physical inventory")
    roots = []
    for instance, physical in seen.items():
        require(re.fullmatch(r"i-[0-9a-f]{17}", instance) and physical.get("state") == "terminated"
                and isinstance(physical.get("root_volumes"), list) and len(physical["root_volumes"]) == 1,
                "root teardown proof is missing")
        root = physical["root_volumes"][0]
        require(isinstance(root, str) and re.fullmatch(r"vol-[0-9a-f]{17}", root), "invalid root volume identity")
        roots.append(root)
    require(len(set(roots)) == len(roots) and sorted(cleanup.get("deleted_roots", [])) == sorted(roots),
            "root teardown proof is missing")
    if binding is not None:
        require(binding["instance_id"] in seen and seen[binding["instance_id"]].get("launch_at") == binding["launch_at"],
                "cleanup differs from bound worker")
    return True


def collect(api, bucket, machine, run, output, *, now=lambda: datetime.now(timezone.utc), budget=None):
    budget = budget or short_budget()
    request = operator_request(api, bucket, run=run, exact=False, budget=budget)
    verify_machine(request, machine)
    expected = producer(request)
    execution = describe(api, machine, run, budget=budget)
    require(execution is not None, "no submission exists for this run")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    write(output, "identity.json", encoded(expected))
    receipt = dict(schema_version=1, identity=expected, outcome="incomplete", cleanup="unconfirmed",
                   instance=None, window=None, request=None, archive_sha256=None, status_sha256=None)
    observation, status, binding = {}, {}, None
    execution_bound, unlaunched = False, False
    stage = "watchdog receipt read"
    try:
        raw = retry_io(lambda: get_bytes(api["s3"], bucket, RECEIPT, MAX_RECEIPT), budget)
        write(output, "durable.json", raw)
        stage = "watchdog receipt structure and request binding"
        observation = parse_json(raw.decode("utf-8"))
        require(isinstance(observation, dict) and observation.get("protocol") == durable.PROTOCOL
                and observation.get("request") == request, "watchdog receipt does not match the operator request")
        execution_bound = observation.get("execution_arn") == execution["executionArn"]
        no_launch = dict(execution_arn=None, attempt=None, attempts=0, allocation_consumed=False,
                         launch_outcome="none", known_instance_ids=[], binding=None, seen={},
                         runtime_verified=False, worker_published=False)
        unlaunched = (execution["status"] in ("RUNNING", "FAILED", "TIMED_OUT", "ABORTED")
            and observation.get("phase") in ("armed", "closing", "complete")
            and all(key in observation and observation[key] == value for key, value in no_launch.items()))
        require(execution_bound or unlaunched, "watchdog execution binding differs")
        require(utc(request["created_at"]) <= utc(observation["observed_at"]) <= now(),
                "watchdog observation timestamp is invalid")
        binding = observation.get("binding")
        stage = "physical cleanup proof"
        if verify_cleanup(observation, request, binding):
            receipt["cleanup"] = "verified"
        stage = "worker binding"
        if binding is not None:
            receipt["window"] = durable.window(request, binding, exact=False)
            receipt["request"] = durable.worker_command(request, binding, exact=False)
            receipt["instance"] = dict(instance_id=binding["instance_id"],
                image_id=binding.get("image_id"), instance_type=binding.get("instance_type"))
        stage = "worker status"
        raw_status = retry_io(lambda: get_bytes(api["s3"], bucket, STATUS_KEY, 16384), budget)
        write(output, "status.json", raw_status)
        receipt["status_sha256"] = digest(raw_status)
        status = parse_json(raw_status.decode("utf-8"))
        require(isinstance(status, dict) and status.get("run") == run, "worker status belongs to another attempt")
        if status.get("state") == "completed":
            require(type(status.get("success")) is bool, "invalid worker terminal status")
            if "sha256" in status:
                require(isinstance(status["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", status["sha256"]),
                        "invalid result digest")
                stage = "worker archive"
                archive = retry_io(lambda: get_bytes(api["s3"], bucket, RESULTS_KEY, MAX_RESULT), budget)
                write(output, "results.zip", archive)
                receipt["archive_sha256"] = digest(archive)
                require(receipt["archive_sha256"] == status["sha256"], "archive differs from worker status")
        if receipt["cleanup"] == "verified" and execution["status"] != "RUNNING":
            admitted = (binding is not None and observation.get("runtime_verified") is True
                        and observation.get("worker_published") is True
                        and set(observation["seen"]) == {binding["instance_id"]})
            receipt["outcome"] = "success" if (execution["status"] == "SUCCEEDED" and admitted
                and status.get("state") == "completed" and status.get("success") is True
                and receipt["archive_sha256"]) else "failure"
    except Exception as error:
        message = "Durable collection could not verify " + stage + "; raw evidence is retained."
        receipt.update(outcome="failure", collection_error=type(error).__name__, collection_message=message)
        raise ValueError(message) from error
    finally:
        write(output, "controller.json", encoded(receipt))
        terminal = execution["status"] in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED")
        conclusion = "pending" if receipt["outcome"] == "incomplete" else receipt["outcome"]
        result = dict(run=run, state="complete" if receipt["cleanup"] == "verified" and terminal else "pending",
            execution_status=execution["status"], cleanup=receipt["cleanup"],
            execution_binding="exact" if execution_bound else ("not-launched" if unlaunched else "unconfirmed"),
            worker_reported_success=isinstance(status, dict) and status.get("state") == "completed" and status.get("success") is True,
            benchmark_success=False, requires_offline_validation=True, conclusion=conclusion)
        write(output, "collection.json", encoded(result))
    return result


def diagnostic_report(source, output, expected, collection, message):
    from . import results
    require(source.resolve() != output.resolve(), "raw input and derived output must differ")
    output.mkdir(parents=True, exist_ok=False)
    raw = output / "raw"
    raw.mkdir()
    for name in ("identity.json", "collection.json", "durable.json", "controller.json", "status.json"):
        if (source / name).exists():
            (raw / name).write_bytes(results.read_file(source / name, MAX_RECEIPT))
    pending = collection.get("conclusion") == "pending"
    evidence = dict(schema_version=1, identity=expected, producer_conclusion="pending" if pending else "failure",
                    history_eligible=False, qualification_accepted=False, errors=[] if pending else [message])
    verdict = dict(run=expected["run"], accepted=False, history_published=False, state="pending" if pending else "failure",
                   validation="metadata/size/tree; does not establish file-content integrity")
    (output / "provenance.json").write_bytes(encoded(evidence))
    (output / "qualification.json").write_bytes(encoded(verdict))
    (output / "summary.md").write_text("<h1>AWS benchmark " + ("pending" if pending else "diagnostic") +
        "</h1>\n<p>Run " + html.escape(expected["run"]) + ": " + html.escape(message) +
        "</p>\n<p>No qualification was accepted; history was not published.</p>\n", encoding="utf-8")
    return evidence, verdict


def validate_offline(source, output):
    """Check the complete qualification without cloud clients or history publication."""
    from . import results
    source, output = Path(source), Path(output)
    expected = identity.load(source / "identity.json")
    collection = {}
    qualification = None
    try:
        collection = results.json_value(results.read_file(source / "collection.json", 16384))
        require(isinstance(collection, dict) and collection.get("run") == expected["run"],
                "collected producer binding differs")
        if collection.get("conclusion") == "pending":
            require(collection.get("state") == "pending", "pending collection has a contradictory state")
            return diagnostic_report(source, output, expected, collection,
                "Execution or independent physical cleanup is still pending; collect this same run again later.")
        observed = results.json_value(results.read_file(source / "durable.json", MAX_RECEIPT))
        receipt = results.json_value(results.read_file(source / "controller.json", MAX_RECEIPT))
        require(isinstance(observed, dict) and isinstance(receipt, dict), "collected control-plane receipt is not an object")
        request = durable.validate(observed["request"], exact=False)
        require(observed["protocol"] == durable.PROTOCOL and producer(request) == expected,
                "collected producer binding differs")
        if collection["state"] == "complete" and collection["conclusion"] == "success":
            require(collection["execution_status"] == "SUCCEEDED" and observed["phase"] == "complete",
                    "durable control plane did not complete")
            require(receipt["identity"] == expected
                    and receipt["window"] == durable.window(request, observed["binding"], exact=False)
                    and receipt["request"] == durable.worker_command(request, observed["binding"], exact=False),
                    "qualification differs from armed request")
            qualification = dict(schema_version=1, identity=expected, archive_sha256=receipt["archive_sha256"],
                                 ami_id=request["pins"]["ami"], harness_revision=request["pins"]["source"])
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as error:
        return diagnostic_report(source, output, expected, {"conclusion": "failure"},
            "Collected control-plane evidence is invalid: " + results.diagnostic(error))
    evidence = results.ingest(source, output, expected, collection["conclusion"], qualification=qualification)
    verdict = dict(run=expected["run"], accepted=evidence.get("qualification_accepted", False)
                   and not evidence["errors"], history_published=False,
                   validation="metadata/size/tree; does not establish file-content integrity")
    (output / "qualification.json").write_bytes(encoded(verdict))
    return evidence, verdict


def oidc_clients(region, role, *, budget=None):
    import boto3
    from botocore.config import Config
    budget = budget or short_budget()
    config = Config(connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 1})
    request = urllib.request.Request(os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"] + "&audience=sts.amazonaws.com",
        headers={"Authorization": "Bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]})
    def token_request():
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.load(response)["value"]
    token = retry_io(token_request, budget, reserve=30)
    sts = boto3.client("sts", region_name=region, config=config)
    credentials = retry_io(lambda: sts.assume_role_with_web_identity(
        RoleArn=role, RoleSessionName="rcp-durable-" + os.environ["GITHUB_RUN_ID"],
        WebIdentityToken=token, DurationSeconds=900), budget, reserve=30)["Credentials"]
    session = boto3.Session(region_name=region, aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"], aws_session_token=credentials["SessionToken"])
    return {service: session.client(service, config=config) for service in ("s3", "stepfunctions")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("submit", "collect"))
    for name in ("region", "bucket", "state-machine", "role-arn", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--identity", type=Path)
    parser.add_argument("--binaries", type=Path)
    parser.add_argument("--run")
    args = parser.parse_args()
    # only the exact temporary debug workflow may request credentials
    require(os.environ.get("GITHUB_REF") == identity.REF and os.environ.get("GITHUB_REPOSITORY") == identity.REPOSITORY
            and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch", "unexpected workflow origin")
    expected_workflow = identity.WORKFLOW
    require(os.environ.get("GITHUB_WORKFLOW_REF") == expected_workflow
            and re.fullmatch(r"[0-9a-f]{40}", os.environ.get("GITHUB_SHA", ""))
            and os.environ.get("GITHUB_SHA") == os.environ.get("GITHUB_WORKFLOW_SHA"), "unexpected trusted workflow")
    budget = short_budget()
    if args.action == "submit":
        require(args.identity is not None and args.binaries is not None and args.run is None,
                "submission requires producer identity and binaries")
        provenance = identity.load(args.identity)
        require(provenance["run"] == os.environ["GITHUB_RUN_ID"] + "-" + os.environ["GITHUB_RUN_ATTEMPT"]
                and provenance["controller"] == os.environ["GITHUB_SHA"], "producer identity differs from this job")
        api = oidc_clients(args.region, args.role_arn, budget=budget)
        value = submit(api, args.bucket, args.state_machine, provenance, args.binaries, budget=budget)
        output = Path(args.output)
        output.mkdir(parents=True, exist_ok=False)
        write(output, "submission.json", encoded(value))
    else:
        require(args.run is not None and args.identity is None and args.binaries is None, "collection requires one run")
        execution_arn(args.state_machine, args.run)
        api = oidc_clients(args.region, args.role_arn, budget=budget)
        value = collect(api, args.bucket, args.state_machine, args.run, args.output, budget=budget)
    print(json.dumps(value, sort_keys=True))


if __name__ == "__main__":
    main()
