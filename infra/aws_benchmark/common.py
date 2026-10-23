"""Small, shared protocol with fixed object names and bounded inputs."""

from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import time
from urllib.error import HTTPError, URLError
from types import MappingProxyType

from benchmarks.verification import FULL_CONTENT_POLICY, METADATA_POLICY

RESERVE = 180
WORKER_START_RESERVE = RESERVE + 300
LAUNCH_RESERVE = WORKER_START_RESERVE + 120
# allow the launch floor, scheduled-action delay, and setup/polling headroom
MIN_WINDOW = LAUNCH_RESERVE + 120 + 180
MAX_WINDOW = 3600
MAX_OPEN_LEAD = 300
DURABLE_PROTOCOL = "rcp-durable-v1"
DIRECT_PROTOCOL = "rcp-direct-v1"
MAX_BINARY = 128 * 1024 * 1024
MAX_RESULT = 64 * 1024 * 1024
INSTANCE_TYPE = "c5d.24xlarge"
WINDOW_KEY = "control/window.json"
REQUEST_KEY = "input/request.json"
RESULTS_KEY = "output/results.zip"
STATUS_KEY = "output/status.json"
PENDING = "pending"
COMPLETED = "completed"
BINARY_NAMES = ("rcp", "rcpd", "filegen")
BINARIES = tuple(f"{role}/{name}" for role in ("baseline", "candidate") for name in BINARY_NAMES)
# append versions; admission and historical reads use the same frozen policy entry
_RECORDED_DURABLE_CONTRACTS = {
    "bulk-metadata-plan-v1": MappingProxyType(dict(
        plan=MappingProxyType(dict(version="bulk-metadata-plan-v1", work_seconds=3104, setup_seconds=100,
                  report_seconds=180, safety_factor=3, boot_reserve_seconds=300,
                  shutdown_reserve_seconds=300)),
        max_runtime=10800, max_provisioning=43200, max_attempts=720, delivery_drain_seconds=900,
        binary_names=("rcp", "rcpd", "filegen"),
        pin_patterns=MappingProxyType({
            "lt_id": r"lt-[0-9a-f]{17}", "lt_version": r"[1-9][0-9]*",
            "ami": r"ami-[0-9a-f]{17}", "source": r"[0-9a-f]{40}",
            "subnet": r"subnet-[0-9a-f]{8,17}", "security_group": r"sg-[0-9a-f]{8,17}",
            "region": r"[a-z]{2}-[a-z]+-[0-9]", "account": r"[0-9]{12}",
            "namespace": r"[a-zA-Z][a-zA-Z0-9-]{0,47}",
            "bucket": r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]",
            "profile_arn": r"arn:aws:iam::{account}:instance-profile/[A-Za-z0-9+=,.@_/-]+"}))),
}
# projected work, not a measured complete run; advance the version when any bound changes
_CURRENT_DURABLE_CONTRACT = _RECORDED_DURABLE_CONTRACTS["bulk-metadata-plan-v1"]
DURABLE_PLAN = dict(_CURRENT_DURABLE_CONTRACT["plan"])
DURABLE_MAX_RUNTIME = _CURRENT_DURABLE_CONTRACT["max_runtime"]
DURABLE_MAX_PROVISIONING = _CURRENT_DURABLE_CONTRACT["max_provisioning"]
DIRECT_MAX_ATTEMPTS = _CURRENT_DURABLE_CONTRACT["max_attempts"]
SUSPEND = {"AZRebalance", "InstanceRefresh", "HealthCheck", "ReplaceUnhealthy", "AlarmNotification"}

# append versions when cloud tuning changes; never infer old settings from local defaults
_CLOUD_VERIFICATION_VERSIONS = (
    dict(workers=8),
    dict(workers=8, inline_bytes=1_048_576),
    dict(workers=8, inline_bytes=1_048_576, read_bytes=1_048_576, batch_depth=2),
)
# 3104s projected generation/four copies + 180s upload + 116s remaining-work headroom
_METADATA_ADMISSION = dict(window=3600, launch=3520, worker=3400)
# the launch floor adds 120s for startup (92s observed); neither floor guarantees completion

# historical contracts remain readable independently of current cloud admission
PERFORMANCE_CONTRACT = dict(paired_seed=0, repetitions=4, purpose="performance",
    cache_policy="linux-drop-caches", runner_label="aws-c5d24-ext4-isolated-v1")
_STREAM_PROFILE = dict(repetitions=2, purpose="smoke", runner_label="aws-c5d24-ext4-raid0-stream-v1",
                       policy=FULL_CONTENT_POLICY, verification_versions=_CLOUD_VERIFICATION_VERSIONS, active=False)
SUITE_PROFILES = {
    "aa-smoke": dict(cases=("tiny-10k",), repetitions=2, purpose="smoke", cache_policy="source-warm"),
    "tiny": dict(cases=("tiny-1m",)),
    "medium": dict(cases=("medium-128k",)),
    "large": dict(cases=("large-120",)),
    "deep": dict(cases=("deep-20x80",)),
    "bulk-1g": dict(_STREAM_PROFILE, cases=("bulk-1g-1tb",)),
    "bulk-100m": dict(_STREAM_PROFILE, cases=("bulk-100m-1024gb",)),
    "bulk-1m": dict(_STREAM_PROFILE, cases=("bulk-1m-1024gb",)),
    "tiny-1k": dict(_STREAM_PROFILE, cases=("tiny-1k-10240k",)),
    "bulk-1g-metadata": dict(cases=("bulk-1g-1tb",), repetitions=2,
        runner_label="aws-c5d24-ext4-raid0-metadata-v1", policy=METADATA_POLICY,
        verification_versions=_CLOUD_VERIFICATION_VERSIONS, admission=_METADATA_ADMISSION),
}
SUITES = {name: profile["cases"] for name, profile in SUITE_PROFILES.items()}
ACTIVE_SUITES = {name: cases for name, cases in SUITES.items() if SUITE_PROFILES[name].get("active", True)}
LARGE_SUITES = {name: cases for name, cases in SUITES.items() if "policy" in SUITE_PROFILES[name]}
METADATA_SUITES = {name: cases for name, cases in SUITES.items()
                   if SUITE_PROFILES[name].get("policy") == METADATA_POLICY}


def measurement_contract(suite):
    require(suite in SUITES, "suite is not allowlisted")
    profile = SUITE_PROFILES[suite]
    return {key: profile.get(key, value) for key, value in PERFORMANCE_CONTRACT.items()}


def verification_contracts(suite):
    """Frozen recorded identities, oldest first; missing historical fields stay unknown."""
    require(suite in SUITES, "suite is not allowlisted")
    profile = SUITE_PROFILES[suite]
    return tuple(dict(policy=profile["policy"], **version) for version in profile["verification_versions"]) \
        if "policy" in profile else (None,)


def verification_contract(suite):
    """Exact current cloud proof identity, independent of local harness defaults."""
    return verification_contracts(suite)[-1]


def admission_contract(suite):
    require(suite in SUITES, "suite is not allowlisted")
    return {**dict(window=MIN_WINDOW, launch=LAUNCH_RESERVE, worker=WORKER_START_RESERVE),
            **SUITE_PROFILES[suite].get("admission", {})}


class ValidationError(ValueError):
    """A validation failure whose message is supplied by trusted code."""


def require(condition, message):
    if not condition:
        raise ValidationError(message)


def archive_name_supported(name):
    # collection and reporting share structural limits. Portability and case-fold
    # collisions are separate reporting policy; existing diagnostic names survive.
    parts = name.split("/")
    try:
        size = len(name.encode("utf-8"))
    except UnicodeEncodeError:
        return False
    return (size <= 1200 and len(parts) <= 7 and "\\" not in name
            and all(part not in ("", ".", "..") and len(part) <= 200 for part in parts))


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


def durable_plan_contract(plan, *, exact=True):
    """Resolve a recorded version; current admission remains an explicit stricter check."""
    require(isinstance(plan, dict) and isinstance(plan.get("version"), str), "unknown durable workload plan")
    recorded = _RECORDED_DURABLE_CONTRACTS.get(plan["version"])
    require(recorded is not None and plan == recorded["plan"], "unknown durable workload plan")
    require(all(type(value) is int for key, value in plan.items() if key != "version"),
            "durable plan durations must be integers")
    if exact:
        require(plan == DURABLE_PLAN, "durable workload plan is not current")
    return {**recorded, "plan": dict(recorded["plan"]), "pin_patterns": dict(recorded["pin_patterns"]),
            "binaries": tuple(f"{role}/{name}" for role in ("baseline", "candidate")
                              for name in recorded["binary_names"])}


def durable_worker_reserve(plan, *, exact=True):
    """Return the recognized plan's projected work plus its recorded planning headroom."""
    plan = durable_plan_contract(plan, exact=exact)["plan"]
    return (plan["work_seconds"] + plan["report_seconds"]) * plan["safety_factor"]


def durable_minimum_runtime(plan=None, *, exact=True):
    """One admission floor for the coordinator and its bound worker window."""
    plan = durable_plan_contract(DURABLE_PLAN if plan is None else plan, exact=exact)["plan"]
    return durable_worker_reserve(plan, exact=exact) + plan["boot_reserve_seconds"] + plan["shutdown_reserve_seconds"]


def validate_durable_hashes(hashes, plan, *, exact=True):
    """Require the recorded paired manifest and identical A/A executable bytes."""
    contract = durable_plan_contract(plan, exact=exact)
    require(isinstance(hashes, dict) and set(hashes) == set(contract["binaries"]),
            "unexpected recorded durable binary manifest")
    require(all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
                for value in hashes.values()), "invalid binary digest")
    require(all(hashes["baseline/" + name] == hashes["candidate/" + name]
                for name in contract["binary_names"]), "A/A binary digests differ")


def validate_durable_pins(pins, plan, *, exact=True):
    """Validate only the named version's pin schema, including the profile account binding."""
    patterns = durable_plan_contract(plan, exact=exact)["pin_patterns"]
    require(isinstance(pins, dict) and set(pins) == set(patterns), "unexpected deployment pins")
    require(all(isinstance(value, str) for value in pins.values()), "deployment pins must be strings")
    for key, pattern in patterns.items():
        require(re.fullmatch(pattern.replace("{account}", pins["account"]), pins[key]), "invalid pin: " + key)


def validate_attempt_id(run, attempt_id, *, maximum=None):
    """Return the canonical bounded attempt ordinal shared by tags and windows."""
    maximum = DIRECT_MAX_ATTEMPTS if maximum is None else maximum
    require(type(maximum) is int and maximum > 0, "invalid direct attempt bound")
    require(isinstance(run, str) and isinstance(attempt_id, str), "invalid direct attempt identity")
    identity = re.fullmatch(re.escape(run) + r"-([1-9][0-9]{0," + str(len(str(maximum)) - 1) + r"})", attempt_id)
    require(identity is not None and int(identity[1]) <= maximum, "invalid direct attempt identity")
    return int(identity[1])


def validate_durable_window(value, now=None, minimum=RESERVE, *, exact=True):
    """Validate explicit clock origins without reinterpreting historical windows."""
    fields = {"run", "baseline", "candidate", "suite", "opens", "deadline", "protocol",
              "instance_id", "launch_at", "runtime_seconds", "outer_deadline", "request_expires", "plan"}
    direct = isinstance(value, dict) and value.get("protocol") == DIRECT_PROTOCOL
    if direct:
        fields |= {"attempt_id", "attempt_started_at"}
    require(isinstance(value, dict) and set(value) == fields, "unexpected durable window fields")
    require(all(isinstance(value[key], str) for key in fields - {"runtime_seconds", "plan"}),
            "durable identity and timestamps must be strings")
    require(value["protocol"] in (DURABLE_PROTOCOL, DIRECT_PROTOCOL), "unsupported durable protocol")
    validate_identity(value)
    require(value["suite"] == "bulk-1g-metadata" and value["baseline"] == value["candidate"],
            "durable qualification requires metadata-only A/A")
    require(re.fullmatch(r"i-[0-9a-f]{17}", value["instance_id"]), "invalid durable instance identity")
    contract = durable_plan_contract(value["plan"], exact=exact)
    runtime = value["runtime_seconds"]
    plan = contract["plan"]
    require(type(runtime) is int and durable_minimum_runtime(value["plan"], exact=exact)
            <= runtime <= contract["max_runtime"], "durable runtime does not fit the disclosed workload plan")
    try:
        start, launch = utc(value["opens"]), utc(value["launch_at"])
        deadline, outer, expires = (utc(value[key]) for key in ("deadline", "outer_deadline", "request_expires"))
    except (TypeError, ValueError, AttributeError) as error:
        raise ValidationError("invalid durable window timestamp") from error
    if direct:
        attempt = utc(value["attempt_started_at"])
        validate_attempt_id(value["run"], value["attempt_id"], maximum=contract["max_attempts"])
        require(start == attempt, "direct window must begin at the immutable attempt start")
        require(attempt <= launch <= deadline and launch < expires,
                "direct launch lies outside the attempt admission interval")
        require(0 < (expires - attempt).total_seconds() <= contract["max_provisioning"],
                "direct attempt lies outside the provisioning deadline")
        require(deadline == attempt + timedelta(seconds=runtime - plan["shutdown_reserve_seconds"]),
                "direct worker deadline differs from fixed attempt runtime")
    else:
        require(start == launch, "durable window must begin at the bound launch time")
        require(0 < (expires - launch).total_seconds() <= contract["max_provisioning"],
                "instance arrived outside the provisioning deadline")
        require(deadline == launch + timedelta(seconds=runtime - plan["shutdown_reserve_seconds"]),
                "durable worker deadline differs from launch-bound runtime")
    require(outer == expires + timedelta(seconds=runtime), "durable reconciliation horizon mismatch")
    now = now or datetime.now(timezone.utc)
    require(launch <= now, "durable launch is in the future")
    require(minimum <= (deadline - now).total_seconds() <= contract["max_runtime"],
            "expired or too short durable worker deadline")
    return deadline


def validate_admission(value, *, window=False):
    """Apply current cloud policy without changing historical identity validation."""
    validate_identity(value)
    require(value["suite"] in ACTIVE_SUITES,
            "suite is disabled for new AWS runs until full-content verification fits the bounded window")
    if window and "protocol" in value:
        validate_durable_window(value, now=utc(value["launch_at"]), minimum=0)
        return
    if window:
        minimum = admission_contract(value["suite"])["window"]
        require(minimum <= (utc(value["deadline"]) - utc(value["opens"])).total_seconds() <= MAX_WINDOW,
                f"suite requires a {minimum // 60}–{MAX_WINDOW // 60} minute window")


def validate_window(value, now=None, minimum=RESERVE):
    if isinstance(value, dict) and "protocol" in value:
        return validate_durable_window(value, now, minimum)
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
    if window.get("protocol") in (DURABLE_PROTOCOL, DIRECT_PROTOCOL):
        validate_durable_hashes(hashes, window["plan"])
        return value
    require(isinstance(hashes, dict) and set(hashes) == set(BINARIES), "unexpected binary list")
    require(all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
                for value in hashes.values()), "binary digests must be lowercase SHA-256")
    if value["baseline"] == value["candidate"]:
        require(all(hashes["baseline/" + name] == hashes["candidate/" + name] for name in BINARY_NAMES),
                "A/A binary digests differ")
    return value


def pending_status(run):
    return {"run": run, "state": PENDING}


def completed_status(run, success=False):
    require(isinstance(success, bool), "status success must be boolean")
    return {"run": run, "state": COMPLETED, "success": success}


class Budget:
    """A wall-clock deadline plus a monotonic ceiling; clock changes cannot extend it."""

    def __init__(self, deadline, *, ceiling_seconds=MAX_WINDOW + MAX_OPEN_LEAD):
        require(type(ceiling_seconds) is int and 0 < ceiling_seconds <= DURABLE_MAX_RUNTIME,
                "invalid monotonic budget ceiling")
        self.deadline = utc(deadline).timestamp()
        self.end = time.monotonic() + min(ceiling_seconds, self.deadline - time.time())

    def left(self, reserve=0):
        remaining = min(self.deadline - time.time(), self.end - time.monotonic()) - reserve
        if remaining <= 0:
            raise TimeoutError("benchmark deadline exhausted")
        return remaining


class RevisionConflict(RuntimeError):
    """The same open owner advanced; reload before recomputing any state transition."""


def error_code(error):
    return (getattr(error, "response", None) or {}).get("Error", {}).get("Code")


def retryable_io(error):
    """Classify a transient failure, not whether the operation committed or is safe to replay."""
    if error_code(error) == "TransactionCanceledException":
        # a canceled transaction can mix retryable contention with a failed fence or
        # validation error. Missing/unknown reasons are not evidence of a transient.
        reasons = (getattr(error, "response", None) or {}).get("CancellationReasons")
        if not isinstance(reasons, list) or not reasons:
            return False
        codes = [reason.get("Code") if isinstance(reason, dict) else None for reason in reasons]
        transient = {"TransactionConflict", "ThrottlingError", "ProvisionedThroughputExceeded"}
        return (all(isinstance(code, str) and code in transient | {"None"} for code in codes)
                and any(code in transient for code in codes))
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
                                    "RequestTimeout", "RequestTimeoutException", "Throttling", "IDPCommunicationError",
                                    "ThrottlingException", "TooManyRequestsException", "RequestLimitExceeded",
                                    "ProvisionedThroughputExceededException"})


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
            and g.get("InstanceLifecyclePolicy") in (
                None, {}, {"RetentionTriggers": {"TerminateHookAbandon": "terminate"}})
            and g.get("TerminationPolicies", ["Default"]) == ["Default"],
            "unexpected replacement or retention configuration")
    require(re.fullmatch(r"[1-9][0-9]*", g["LaunchTemplate"]["Version"]), "launch template must use a numeric version")


def live_instances(ec2, name):
    response = ec2.describe_instances(Filters=[
        {"Name": "tag:aws:autoscaling:groupName", "Values": [name]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped", "shutting-down"]}])
    require(not response.get("NextToken"), "unexpected EC2 pagination")
    return [i for r in response["Reservations"] for i in r["Instances"]]
