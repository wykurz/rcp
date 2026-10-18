"""Fresh trusted CI job: copy data, request capacity, collect data, and verify teardown."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import stat
import time
import urllib.request

from .common import (BINARIES, MAX_BINARY, MAX_RESULT, PREPARE_RESERVE, REQUEST_KEY, RESULTS_KEY,
                     STATUS_KEY, WINDOW_KEY, Budget, checked_clients, digest, get_bytes, get_json,
                     group, live_instances, put_json, require, retry_io, scheduled, utc,
                     validate_identity, validate_window, verify_close, verify_group)


def oidc_clients(region, role):
    import boto3
    from botocore.config import Config
    url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"] + "&audience=sts.amazonaws.com"
    request = urllib.request.Request(url, headers={
        "Authorization": "Bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]})
    with urllib.request.urlopen(request, timeout=15) as response:
        token = json.load(response)["value"]
    config = Config(connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 1})
    credentials = boto3.client("sts", region_name=region, config=config).assume_role_with_web_identity(
        RoleArn=role, RoleSessionName="rcp-benchmark-" + os.environ["GITHUB_RUN_ID"],
        WebIdentityToken=token, DurationSeconds=7200)["Credentials"]
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


def run(api, name, bucket, identity, binary_root, output):
    validate_identity(identity)
    window = get_json(api["s3"], bucket, WINDOW_KEY)
    require(all(window[k] == identity[k] for k in identity), "operator window does not match this run")
    budget = Budget(validate_window(window, minimum=PREPARE_RESERVE))
    initial = retry_io(lambda: get_json(api["s3"], bucket, STATUS_KEY), budget, PREPARE_RESERVE)
    require(initial == {"run": identity["run"], "state": "pending"},
            "operator must initialize the pending status before launch")
    # do not touch a different/active run when the preflight fails
    while True:
        budget.left(PREPARE_RESERVE)
        verify_close(scheduled(api["autoscaling"], name), window)
        g = group(api["autoscaling"], name)
        verify_group(g, g["MaxSize"])
        require(g["MaxSize"] in (0, 1), "unexpected maximum")
        require(not live_instances(api["ec2"], name), "previous worker has not terminated")
        if g["MaxSize"] == 1 and datetime.now(timezone.utc) >= utc(window["opens"]):
            break
        time.sleep(10)
    request = {**identity, "deadline": window["deadline"], "sha256": {}}
    for key in BINARIES:
        budget.left(PREPARE_RESERVE)
        source = "baseline/" + key.split("/")[1] if identity["suite"] == "aa-smoke" else key
        data = read_binary(binary_root / source)
        request["sha256"][key] = digest(data)
        api["s3"].put_object(Bucket=bucket, Key="input/" + key, Body=data)
    if identity["suite"] == "aa-smoke":
        require(all(request["sha256"]["baseline/" + name] == request["sha256"]["candidate/" + name]
                    for name in ("rcp", "rcpd", "filegen")), "A/A binary digests differ")
    put_json(api["s3"], bucket, REQUEST_KEY, request)
    budget.left(PREPARE_RESERVE)
    verify_close(scheduled(api["autoscaling"], name), window)
    verify_group(group(api["autoscaling"], name), 1)
    # enter the cleanup boundary before the API call, including uncertain outcomes
    try:
        api["autoscaling"].set_desired_capacity(AutoScalingGroupName=name, DesiredCapacity=1, HonorCooldown=False)
        while True:
            budget.left(30)
            status = retry_io(lambda: get_json(api["s3"], bucket, STATUS_KEY), budget, 30)
            require(status.get("run") == identity["run"], "worker status does not match this run")
            require(status.get("state") in {"pending", "completed"}, "invalid worker status state")
            if status["state"] == "completed":
                output.mkdir(parents=True, exist_ok=True)
                (output / "status.json").write_text(json.dumps(status, indent=2))
                if status.get("sha256"):
                    data = retry_io(lambda: get_bytes(api["s3"], bucket, RESULTS_KEY, MAX_RESULT), budget, 30)
                    require(digest(data) == status["sha256"], "result archive digest mismatch")
                    # never extract or execute worker output on the credentialed job
                    (output / "results.zip").write_bytes(data)
                require(status.get("success") is True, "worker failed; see bounded partial results")
                return
            time.sleep(10)
    finally:
        cleanup(api, name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ("region", "asg", "bucket", "run", "baseline", "candidate", "suite"):
        parser.add_argument("--" + field, required=True)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--role-arn", required=True)
    args = parser.parse_args()
    identity = {field: getattr(args, field) for field in ("run", "baseline", "candidate", "suite")}
    run(oidc_clients(args.region, args.role_arn), args.asg, args.bucket, identity, args.binaries, args.output)


if __name__ == "__main__":
    main()
