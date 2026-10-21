"""Fresh trusted CI job: copy data, request capacity, collect data, and verify teardown."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import stat
import time
import urllib.request

from . import identity as workflow_identity

from .common import (BINARY_NAMES, COMPLETED, MAX_BINARY, MAX_RESULT, PENDING, REQUEST_KEY, RESULTS_KEY,
                     STATUS_KEY, WINDOW_KEY, Budget, admission_contract, checked_clients, digest, get_bytes, get_json,
                     group, live_instances, pending_status, put_json, require, retry_io, scheduled, utc,
                     validate_admission, validate_request, validate_window, verify_close, verify_group)


def oidc_clients(region, role):
    import boto3
    from botocore.config import Config
    url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"] + "&audience=sts.amazonaws.com"
    request = urllib.request.Request(url, headers={
        "Authorization": "Bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]})
    budget = Budget(datetime.now(timezone.utc) + timedelta(seconds=60))
    def token_request():
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.load(response)["value"]
    token = retry_io(token_request, budget, reserve=5)
    config = Config(connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 1})
    sts = boto3.client("sts", region_name=region, config=config)
    credentials = retry_io(lambda: sts.assume_role_with_web_identity(
        RoleArn=role, RoleSessionName="rcp-benchmark-" + os.environ["GITHUB_RUN_ID"],
        WebIdentityToken=token, DurationSeconds=7200), budget, reserve=5)["Credentials"]
    session = boto3.Session(region_name=region, aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"], aws_session_token=credentials["SessionToken"])
    return checked_clients(session, config)


def read_binary(path):
    # downloaded build artifacts are data, including symlinks and special files
    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    finally:
        os.close(parent)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= MAX_BINARY, "invalid binary file")
        data = stream.read(MAX_BINARY + 1)
        require(0 < len(data) <= MAX_BINARY, "oversized binary")
        return data


def cleanup(api, name, timeout=600):
    # may be called after an uncertain launch response; request zero before observing
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            api["autoscaling"].set_desired_capacity(AutoScalingGroupName=name, DesiredCapacity=0, HonorCooldown=False)
            g = group(api["autoscaling"], name)
            if g["DesiredCapacity"] == 0 and not g["Instances"] and not live_instances(api["ec2"], name):
                print("Teardown verified: desired=0, ASG empty, no live tagged EC2 instances.")
                return
        except Exception as error:
            print("Teardown retry:", type(error).__name__)
        time.sleep(10)
    raise TimeoutError("teardown unconfirmed; leave shutdown armed and inspect EC2")


class Receipt:
    """Controller observations kept separately from untrusted worker evidence."""

    def __init__(self, output, provenance):
        self.output = output
        self.value = dict(schema_version=1, identity=provenance, outcome="incomplete",
                          cleanup="not-requested", instance=None, window=None, request=None,
                          archive_sha256=None, status_sha256=None)
        self.output.mkdir(parents=True, exist_ok=True)
        self.save(required=True)

    def save(self, required=False):
        try:
            temporary = self.output / "controller.tmp"
            temporary.write_text(json.dumps(self.value, sort_keys=True, separators=(",", ":")), encoding="utf-8")
            temporary.replace(self.output / "controller.json")
        except OSError:
            if required:
                raise
            # evidence failures must never bypass cleanup or hide its exception
            print("Controller receipt could not be updated; provenance remains incomplete.")


def run(api, name, bucket, provenance, binary_root, output):
    workflow_identity.validate(provenance)
    identity = {field: provenance[field] for field in ("run", "baseline", "candidate", "suite")}
    receipt = Receipt(output, provenance)
    try:
        _run(api, name, bucket, identity, binary_root, output, receipt)
        receipt.value["outcome"] = "success"
    except BaseException:
        receipt.value["outcome"] = "failure"
        raise
    finally:
        receipt.save()


def _run(api, name, bucket, identity, binary_root, output, receipt):
    validate_admission(identity)
    bootstrap = Budget(datetime.now(timezone.utc) + timedelta(seconds=60))
    window = retry_io(lambda: get_json(api["s3"], bucket, WINDOW_KEY), bootstrap, reserve=5)
    require(all(window[k] == identity[k] for k in identity), "operator window does not match this run")
    validate_admission(window, window=True)
    launch_reserve = admission_contract(window["suite"])["launch"]
    budget = Budget(validate_window(window, minimum=launch_reserve))
    receipt.value["window"] = window
    receipt.save()
    def admit_launch():
        if budget.left() < launch_reserve:
            raise TimeoutError("suite launch admission deadline exhausted")
    def prelaunch(operation):
        def admitted():
            admit_launch()
            return operation()
        return retry_io(admitted, budget)
    initial = prelaunch(lambda: get_json(api["s3"], bucket, STATUS_KEY))
    require(initial == pending_status(identity["run"]),
            "operator must initialize the pending status before launch")
    # do not touch a different/active run when the preflight fails
    while True:
        admit_launch()
        verify_close(prelaunch(lambda: scheduled(api["autoscaling"], name)), window)
        g = prelaunch(lambda: group(api["autoscaling"], name))
        verify_group(g, g["MaxSize"])
        require(g["MaxSize"] in (0, 1), "unexpected maximum")
        require(not prelaunch(lambda: live_instances(api["ec2"], name)), "previous worker has not terminated")
        if g["MaxSize"] == 1 and datetime.now(timezone.utc) >= utc(window["opens"]):
            break
        time.sleep(10)
    request = {**identity, "deadline": window["deadline"], "sha256": {}}
    same_source = identity["baseline"] == identity["candidate"]
    for binary_name in BINARY_NAMES:
        for role in ("baseline", "candidate"):
            admit_launch()
            key = role + "/" + binary_name
            if role == "baseline" or not same_source:
                data = read_binary(binary_root / key)
                binary_digest = digest(data)
            # same-source A/A intentionally reuses the baseline bytes and their digest
            request["sha256"][key] = binary_digest
            prelaunch(lambda: api["s3"].put_object(Bucket=bucket, Key="input/" + key, Body=data))
    validate_request(request, window)
    prelaunch(lambda: put_json(api["s3"], bucket, REQUEST_KEY, request))
    admit_launch()
    verify_close(prelaunch(lambda: scheduled(api["autoscaling"], name)), window)
    verify_group(prelaunch(lambda: group(api["autoscaling"], name)), 1)
    admit_launch()
    receipt.value["request"] = request
    receipt.value["cleanup"] = "unconfirmed"
    try:
        receipt.save(required=True)
    except BaseException:
        # the API call was never attempted; a later save must not imply otherwise
        receipt.value["cleanup"] = "not-requested"
        raise
    observation_errors = 0
    def observe_instance(reserve=30):
        nonlocal observation_errors
        if receipt.value["instance"] is not None or observation_errors >= 3:
            return
        try:
            budget.left(reserve)
        except TimeoutError:
            # optional provenance must not replace the terminal worker outcome
            return
        try:
            observed = live_instances(api["ec2"], name)
            if len(observed) == 1:
                instance = observed[0]
                receipt.value["instance"] = dict(instance_id=instance["InstanceId"],
                    image_id=instance["ImageId"], instance_type=instance["InstanceType"])
                receipt.save()
        except Exception:
            # empty answers can precede a slow launch; only errors consume retries
            observation_errors += 1
            print("Worker image observation unavailable; leave provenance unknown.")
    # a slow receipt write must not consume the launch floor unnoticed
    try:
        admit_launch()
    except BaseException:
        receipt.value["cleanup"] = "not-requested"
        raise
    # enter the cleanup boundary before the API call, including uncertain outcomes
    try:
        api["autoscaling"].set_desired_capacity(AutoScalingGroupName=name, DesiredCapacity=1, HonorCooldown=False)
        while True:
            status = retry_io(lambda: get_json(api["s3"], bucket, STATUS_KEY), budget, 30)
            require(isinstance(status, dict) and status.get("run") == identity["run"],
                    "worker status does not match this run")
            if status.get("state") != PENDING:
                output.mkdir(parents=True, exist_ok=True)
                status_bytes = json.dumps(status, separators=(",", ":")).encode()
                (output / "status.json").write_bytes(status_bytes)
                receipt.value["status_sha256"] = digest(status_bytes)
                receipt.save()
                require(status.get("state") == COMPLETED and isinstance(status.get("success"), bool),
                        "worker status protocol mismatch; rebuild WorkerAmiId from this revision")
                if status.get("sha256"):
                    data = retry_io(lambda: get_bytes(api["s3"], bucket, RESULTS_KEY, MAX_RESULT), budget, 30)
                    require(digest(data) == status["sha256"], "result archive digest mismatch")
                    # never extract or execute worker output on the credentialed job
                    (output / "results.zip").write_bytes(data)
                    receipt.value["archive_sha256"] = digest(data)
                    receipt.save()
                # retrieve mandatory evidence before spending time on optional provenance
                observe_instance()
                require(status.get("success") is True, "worker failed; see bounded partial results")
                return
            # retain 30s for mandatory reads, the configured 20s request bound and the 10s poll sleep
            observe_instance(reserve=60)
            time.sleep(10)
    finally:
        cleanup(api, name)
        receipt.value["cleanup"] = "verified"
        receipt.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ("region", "asg", "bucket"):
        parser.add_argument("--" + field, required=True)
    parser.add_argument("--identity", type=Path, required=True)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--role-arn", required=True)
    args = parser.parse_args()
    provenance = workflow_identity.load(args.identity)
    run(oidc_clients(args.region, args.role_arn), args.asg, args.bucket, provenance, args.binaries, args.output)


if __name__ == "__main__":
    main()
