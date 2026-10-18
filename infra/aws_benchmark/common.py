"""Small, shared protocol with fixed object names and bounded inputs."""

from datetime import datetime, timezone
import hashlib
import json
import re
import time
from urllib.error import HTTPError, URLError

RESERVE = 180
WORKER_START_RESERVE = RESERVE + 300
LAUNCH_RESERVE = WORKER_START_RESERVE + 120
# allow the launch floor, scheduled-action delay, and setup/polling headroom
MIN_WINDOW = LAUNCH_RESERVE + 120 + 180
MAX_WINDOW = 3600
MAX_OPEN_LEAD = 300
MAX_BINARY = 128 * 1024 * 1024
MAX_RESULT = 64 * 1024 * 1024
WINDOW_KEY = "control/window.json"
REQUEST_KEY = "input/request.json"
RESULTS_KEY = "output/results.zip"
STATUS_KEY = "output/status.json"
PENDING = "pending"
COMPLETED = "completed"
BINARIES = tuple(f"{role}/{name}" for role in ("baseline", "candidate")
                 for name in ("rcp", "rcpd", "filegen"))
SUSPEND = {"AZRebalance", "InstanceRefresh", "HealthCheck", "ReplaceUnhealthy", "AlarmNotification"}
SUITES = {"aa-smoke": ("tiny-10k",), "tiny": ("tiny-1m",),
          "medium": ("medium-128k",), "large": ("large-120",), "deep": ("deep-20x80",)}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(value.tzinfo is not None, "timestamp requires an explicit timezone")
    return value.astimezone(timezone.utc)


def timestamp(value):
    return utc(value).isoformat(timespec="seconds")


def validate_identity(value):
    require(re.fullmatch(r"[1-9][0-9]{0,19}-[1-9][0-9]{0,4}", value["run"]), "invalid run ID-attempt")
    for role in ("baseline", "candidate"):
        require(re.fullmatch(r"[0-9a-f]{40}", value[role]), "revisions must be full lowercase commit SHAs")
    require(value["suite"] in SUITES, "suite is not allowlisted")
    if value["suite"] == "aa-smoke":
        require(value["baseline"] == value["candidate"], "A/A pilot requires identical revisions")


def validate_window(value, now=None, minimum=RESERVE):
    require(isinstance(value, dict) and set(value) ==
            {"run", "baseline", "candidate", "suite", "opens", "deadline"}, "unexpected window fields")
    validate_identity(value)
    start, deadline = utc(value["opens"]), utc(value["deadline"])
    now = now or datetime.now(timezone.utc)
    require(MIN_WINDOW <= (deadline - start).total_seconds() <= MAX_WINDOW,
            f"window must be {MIN_WINDOW // 60}–{MAX_WINDOW // 60} minutes")
    require(minimum <= (deadline - now).total_seconds() <= MAX_WINDOW + MAX_OPEN_LEAD,
            "expired, too short, or excessively distant deadline")
    return deadline


def validate_request(value, window):
    require(isinstance(value, dict) and set(value) ==
            {"run", "baseline", "candidate", "suite", "deadline", "sha256"}, "unexpected request fields")
    require(all(value[key] == window[key] for key in ("run", "baseline", "candidate", "suite", "deadline")),
            "mailbox request does not match the operator window")
    hashes = value["sha256"]
    require(isinstance(hashes, dict) and set(hashes) == set(BINARIES), "unexpected binary list")
    require(all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
                for value in hashes.values()), "binary digests must be lowercase SHA-256")
    return value


def pending_status(run):
    return {"run": run, "state": PENDING}


def completed_status(run, success=False):
    require(isinstance(success, bool), "status success must be boolean")
    return {"run": run, "state": COMPLETED, "success": success}


class Budget:
    """A wall-clock deadline plus a monotonic ceiling; clock changes cannot extend it."""

    def __init__(self, deadline):
        self.deadline = utc(deadline).timestamp()
        self.end = time.monotonic() + min(MAX_WINDOW + MAX_OPEN_LEAD, self.deadline - time.time())

    def left(self, reserve=0):
        remaining = min(self.deadline - time.time(), self.end - time.monotonic()) - reserve
        if remaining <= 0:
            raise TimeoutError("benchmark deadline exhausted")
        return remaining


def error_code(error):
    return (getattr(error, "response", None) or {}).get("Error", {}).get("Code")


def retryable_io(error):
    # the SDK is optional for stdlib-only offline tests, but always installed on AWS paths
    transport_errors = (ConnectionError, TimeoutError)
    try:
        from botocore.exceptions import ConnectionError as SdkConnectionError
        from botocore.exceptions import HTTPClientError, IncompleteReadError
        from urllib3.exceptions import SSLError as BodySSLError
    except ImportError:
        pass
    else:
        transport_errors += (SdkConnectionError, HTTPClientError, IncompleteReadError, BodySSLError)
    response = getattr(error, "response", None) or {}
    return (isinstance(error, transport_errors)
            or isinstance(error, URLError) and not isinstance(error, HTTPError)
            or isinstance(error, HTTPError) and error.code in {429, 500, 502, 503, 504}
            or response.get("ResponseMetadata", {}).get("HTTPStatusCode") in {500, 502, 503, 504}
            or error_code(error) in {"InternalError", "InternalFailure", "ServiceUnavailable", "SlowDown",
                                    "RequestTimeout", "RequestTimeoutException", "Throttling",
                                    "ThrottlingException", "RequestLimitExceeded"})


def retry_io(operation, budget, reserve=0, attempts=3):
    """Retry an explicitly safe operation, including its body, under the caller's admission budget."""
    require(1 <= attempts <= 3, "operation retries must use one to three attempts")
    for attempt in range(attempts):
        budget.left(reserve)
        try:
            result = operation()
        except Exception as error:
            if attempt + 1 == attempts or not retryable_io(error):
                raise
            delay = 2 ** attempt
            if budget.left(reserve) <= delay:
                raise TimeoutError("benchmark deadline leaves no time for an operation retry") from error
            time.sleep(delay)
        else:
            # the reserve admits new work; it must not discard an already-completed operation
            return result


def clients(region):
    import boto3
    from botocore.config import Config
    config = Config(connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 1})
    session = boto3.Session(region_name=region)
    return checked_clients(session, config)


def checked_clients(session, config):
    result = {name: session.client(name, config=config) for name in ("autoscaling", "ec2", "s3")}
    require("InstanceLifecyclePolicy" in result["autoscaling"].meta.service_model.shape_for("AutoScalingGroup").members,
            "AWS SDK is too old to inspect instance retention; use the pinned requirements")
    return result


def get_bytes(s3, bucket, key, limit):
    response = s3.get_object(Bucket=bucket, Key=key)
    try:
        require(0 <= response["ContentLength"] <= limit, f"oversized object: {key}")
        data = response["Body"].read(limit + 1)
        require(len(data) <= limit, f"oversized body: {key}")
        if len(data) != response["ContentLength"]:
            raise ConnectionError(f"truncated body: {key}")
        return data
    finally:
        response["Body"].close()


def get_json(s3, bucket, key):
    return json.loads(get_bytes(s3, bucket, key, 16384))


def put_json(s3, bucket, key, value):
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(value).encode(), ContentType="application/json")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def group(asg, name):
    groups = asg.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"]
    require(len(groups) == 1, "ASG not found")
    return groups[0]


def scheduled(asg, name):
    response = asg.describe_scheduled_actions(AutoScalingGroupName=name)
    require(not response.get("NextToken"), "unexpected schedule pagination")
    return response["ScheduledUpdateGroupActions"]


def verify_close(actions, window):
    close = [a for a in actions if a["ScheduledActionName"] == "close-" + window["run"]]
    require(len(close) == 1, "independent shutdown is missing")
    action = close[0]
    require(not action.get("Recurrence") and not action.get("EndTime"), "shutdown must be one-time")
    require(utc(action["StartTime"]) == utc(window["deadline"]), "shutdown deadline mismatch")
    require(all(action.get(k) == 0 for k in ("MinSize", "DesiredCapacity", "MaxSize")),
            "shutdown must close all three capacities")
    require(all(a["ScheduledActionName"] in {"close-" + window["run"], "open-" + window["run"]}
                for a in actions), "unexpected competing schedule")
    for opening in (a for a in actions if a["ScheduledActionName"].startswith("open-")):
        require(utc(opening["StartTime"]) == utc(window["opens"])
                and not opening.get("Recurrence") and not opening.get("EndTime")
                and all(opening.get(k) == v for k, v in (("MinSize", 0), ("DesiredCapacity", 0), ("MaxSize", 1))),
                "opening action differs from operator window")


def verify_group(g, maximum):
    require((g["MinSize"], g["DesiredCapacity"], g["MaxSize"]) == (0, 0, maximum),
            "ASG must be empty with the expected capacity limits")
    require(not g["Instances"], "ASG still has instances")
    require({p["ProcessName"] for p in g.get("SuspendedProcesses", [])} == SUSPEND,
            "unexpected process state; Launch, Terminate and ScheduledActions must remain enabled")
    require(not g.get("MixedInstancesPolicy") and not g.get("CapacityRebalance")
            and not g.get("MaxInstanceLifetime") and not g.get("NewInstancesProtectedFromScaleIn")
            and not g.get("InstanceMaintenancePolicy") and not g.get("WarmPoolConfiguration")
            and not g.get("InstanceLifecyclePolicy")
            and g.get("TerminationPolicies", ["Default"]) == ["Default"],
            "unexpected replacement or retention configuration")
    require(re.fullmatch(r"[1-9][0-9]*", g["LaunchTemplate"]["Version"]), "launch template must use a numeric version")


def live_instances(ec2, name):
    response = ec2.describe_instances(Filters=[
        {"Name": "tag:aws:autoscaling:groupName", "Values": [name]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped", "shutting-down"]}])
    require(not response.get("NextToken"), "unexpected EC2 pagination")
    return [i for r in response["Reservations"] for i in r["Instances"]]
