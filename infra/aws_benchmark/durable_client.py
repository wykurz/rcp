"""Short credentialed submission and collection; candidate executables remain opaque data."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import urllib.request

from benchmarks.strict_json import parse_json
from . import durable, identity
from .common import (BINARIES, MAX_RESULT, RESULTS_KEY, STATUS_KEY, digest, get_bytes,
                     require, timestamp, utc)
from .controller import read_binary
from .durable_aws import RECEIPT, REQUEST


MAX_RECEIPT = 100_000


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def read_json(api, bucket, key, limit=MAX_RECEIPT):
    return parse_json(get_bytes(api["s3"], bucket, key, limit).decode("utf-8"))


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
    return getattr(error, "response", {}).get("Error", {}).get("Code")


def describe(api, machine, run):
    arn = execution_arn(machine, run)
    try:
        result = api["stepfunctions"].describe_execution(executionArn=arn)
    except Exception as error:
        if code(error) == "ExecutionDoesNotExist":
            return None
        raise
    require(result["executionArn"] == arn and result["stateMachineArn"] == machine,
            "execution resource binding differs")
    require(parse_json(result["input"]) == {"run": run}, "execution input differs from this attempt")
    return result


def operator_request(api, bucket, *, run, provenance=None):
    request = durable.validate(read_json(api, bucket, REQUEST))
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


def submit(api, bucket, machine, provenance, binaries, *, now=lambda: datetime.now(timezone.utc)):
    identity.validate(provenance)
    request = operator_request(api, bucket, run=provenance["run"], provenance=provenance)
    verify_machine(request, machine)
    existing = describe(api, machine, request["run"])
    if existing is not None:
        return submission_receipt(request, existing, reused=True)
    require(utc(request["created_at"]) <= now() < utc(request["capacity_deadline"]), "capacity request expired")
    for key in BINARIES:
        data = read_binary(Path(binaries) / key)
        require(digest(data) == request["sha256"][key], "producer binary differs from operator manifest")
        api["s3"].put_object(Bucket=bucket, Key="input/" + key, Body=data,
                             ContentType="application/octet-stream")
    # a stale request cannot turn a delayed build/upload into a fresh capacity window
    require(operator_request(api, bucket, run=request["run"], provenance=provenance) == request,
            "operator request changed during upload")
    require(now() < utc(request["capacity_deadline"]), "capacity request expired during upload")
    arn = execution_arn(machine, request["run"])
    try:
        response = api["stepfunctions"].start_execution(stateMachineArn=machine,
            name=request["run"], input=json.dumps({"run": request["run"]}, separators=(",", ":")))
    except Exception:
        # reconcile an uncertain response; never repeat StartExecution under another name
        found = describe(api, machine, request["run"])
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


def collect(api, bucket, machine, run, output, *, now=lambda: datetime.now(timezone.utc)):
    request = operator_request(api, bucket, run=run)
    verify_machine(request, machine)
    expected = producer(request)
    execution = describe(api, machine, run)
    require(execution is not None, "no submission exists for this run")
    observation = read_json(api, bucket, RECEIPT)
    require(observation.get("protocol") == durable.PROTOCOL and observation.get("request") == request,
            "watchdog receipt does not match the operator request")
    execution_bound = observation.get("execution_arn") == execution["executionArn"]
    # a failed validation before the first attempt can still have independent
    # cleanup evidence for this exact request without an execution binding
    no_launch = dict(execution_arn=None, attempt=None, attempts=0, allocation_consumed=False,
                     launch_outcome="none", known_instance_ids=[], binding=None, seen={},
                     runtime_verified=False, worker_published=False)
    unlaunched_failure = (execution["status"] in ("FAILED", "TIMED_OUT", "ABORTED")
        and observation.get("phase") in ("armed", "closing", "complete")
        and all(key in observation and observation[key] == value for key, value in no_launch.items()))
    require(execution_bound or unlaunched_failure, "watchdog execution binding differs")
    require(utc(request["created_at"]) <= utc(observation["observed_at"]) <= now(),
            "watchdog observation timestamp is invalid")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    write(output, "identity.json", encoded(expected))
    write(output, "durable.json", encoded(observation))
    receipt = dict(schema_version=1, identity=expected, outcome="incomplete", cleanup="unconfirmed",
                   instance=None, window=None, request=None, archive_sha256=None, status_sha256=None)
    binding = observation.get("binding")
    if binding is not None:
        receipt["window"] = durable.window(request, binding)
        receipt["request"] = {key: request[key] for key in ("run", "baseline", "candidate", "suite", "sha256")}
        receipt["request"]["deadline"] = binding["deadline"]
        receipt["instance"] = dict(instance_id=binding["instance_id"], image_id=request["pins"]["ami"],
                                   instance_type="c5d.24xlarge")
    status = {}
    try:
        if verify_cleanup(observation, request, binding):
            receipt["cleanup"] = "verified"
        raw_status = get_bytes(api["s3"], bucket, STATUS_KEY, 16384)
        write(output, "status.json", raw_status)
        receipt["status_sha256"] = digest(raw_status)
        status = parse_json(raw_status.decode("utf-8"))
        require(isinstance(status, dict) and status.get("run") == run, "worker status belongs to another attempt")
        if status.get("state") == "completed":
            require(type(status.get("success")) is bool, "invalid worker terminal status")
            if "sha256" in status:
                require(isinstance(status["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", status["sha256"]),
                        "invalid result digest")
                archive = get_bytes(api["s3"], bucket, RESULTS_KEY, MAX_RESULT)
                write(output, "results.zip", archive)
                receipt["archive_sha256"] = digest(archive)
                require(receipt["archive_sha256"] == status["sha256"], "archive differs from worker status")
        if receipt["cleanup"] == "verified":
            admitted = (binding is not None and observation.get("runtime_verified") is True
                        and observation.get("worker_published") is True
                        and set(observation["seen"]) == {binding["instance_id"]})
            receipt["outcome"] = "success" if (execution["status"] == "SUCCEEDED" and admitted
                and status.get("state") == "completed" and status.get("success") is True
                and receipt["archive_sha256"]) else "failure"
            if execution["status"] == "RUNNING":
                receipt["outcome"] = "incomplete"
    except Exception as error:
        receipt.update(outcome="failure", collection_error=type(error).__name__)
        raise
    finally:
        write(output, "controller.json", encoded(receipt))
        terminal = execution["status"] in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED")
        result = dict(run=run, state="complete" if receipt["cleanup"] == "verified" and terminal else "pending",
            execution_status=execution["status"], cleanup=receipt["cleanup"],
            execution_binding="exact" if execution_bound else "not-launched",
            worker_reported_success=isinstance(status, dict) and status.get("state") == "completed" and status.get("success") is True,
            benchmark_success=False, requires_offline_validation=True,
            conclusion="success" if receipt["outcome"] == "success" else "failure")
        write(output, "collection.json", encoded(result))
    return result



def validate_offline(source, output):
    """Check the complete qualification without cloud clients or history publication."""
    from . import results
    source, output = Path(source), Path(output)
    expected = identity.load(source / "identity.json")
    collection = results.json_value(results.read_file(source / "collection.json", 16384))
    observed = results.json_value(results.read_file(source / "durable.json", MAX_RECEIPT))
    receipt = results.json_value(results.read_file(source / "controller.json", MAX_RECEIPT))
    request = durable.validate(observed["request"])
    require(observed["protocol"] == durable.PROTOCOL and producer(request) == expected
            and collection["run"] == expected["run"], "collected producer binding differs")
    qualification = None
    if collection["state"] == "complete" and collection["conclusion"] == "success":
        require(collection["execution_status"] == "SUCCEEDED" and observed["phase"] == "complete",
                "durable control plane did not complete")
        require(receipt["identity"] == expected and receipt["window"] == durable.window(request, observed["binding"])
                and receipt["request"]["sha256"] == request["sha256"], "qualification differs from armed request")
        qualification = dict(schema_version=1, identity=expected, archive_sha256=receipt["archive_sha256"],
                             ami_id=request["pins"]["ami"], harness_revision=request["pins"]["source"])
    evidence = results.ingest(source, output, expected, collection["conclusion"], qualification=qualification)
    verdict = dict(run=expected["run"], accepted=evidence.get("qualification_accepted", False)
                   and not evidence["errors"], history_published=False,
                   validation="metadata/size/tree; does not establish file-content integrity")
    (output / "qualification.json").write_bytes(encoded(verdict))
    return evidence, verdict


def oidc_clients(region, role):
    import boto3
    from botocore.config import Config
    config = Config(connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 1})
    request = urllib.request.Request(os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"] + "&audience=sts.amazonaws.com",
        headers={"Authorization": "Bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]})
    with urllib.request.urlopen(request, timeout=15) as response:
        token = json.load(response)["value"]
    credentials = boto3.client("sts", region_name=region, config=config).assume_role_with_web_identity(
        RoleArn=role, RoleSessionName="rcp-durable-" + os.environ["GITHUB_RUN_ID"],
        WebIdentityToken=token, DurationSeconds=900)["Credentials"]
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
    # only trusted main workflow code may request credentials
    require(os.environ.get("GITHUB_REF") == identity.REF and os.environ.get("GITHUB_REPOSITORY") == identity.REPOSITORY
            and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch", "unexpected workflow origin")
    expected_workflow = identity.WORKFLOW if args.action == "submit" else (
        identity.REPOSITORY + "/.github/workflows/aws-benchmark-durable-collect.yml@" + identity.REF)
    require(os.environ.get("GITHUB_WORKFLOW_REF") == expected_workflow
            and re.fullmatch(r"[0-9a-f]{40}", os.environ.get("GITHUB_SHA", ""))
            and os.environ.get("GITHUB_SHA") == os.environ.get("GITHUB_WORKFLOW_SHA"), "unexpected trusted workflow")
    if args.action == "submit":
        require(args.identity is not None and args.binaries is not None and args.run is None,
                "submission requires producer identity and binaries")
        provenance = identity.load(args.identity)
        require(provenance["run"] == os.environ["GITHUB_RUN_ID"] + "-" + os.environ["GITHUB_RUN_ATTEMPT"]
                and provenance["controller"] == os.environ["GITHUB_SHA"], "producer identity differs from this job")
        api = oidc_clients(args.region, args.role_arn)
        value = submit(api, args.bucket, args.state_machine, provenance, args.binaries)
        output = Path(args.output)
        output.mkdir(parents=True, exist_ok=False)
        write(output, "submission.json", encoded(value))
    else:
        require(args.run is not None and args.identity is None and args.binaries is None, "collection requires one run")
        execution_arn(args.state_machine, args.run)
        api = oidc_clients(args.region, args.role_arn)
        value = collect(api, args.bucket, args.state_machine, args.run, args.output)
    print(json.dumps(value, sort_keys=True))


if __name__ == "__main__":
    main()
