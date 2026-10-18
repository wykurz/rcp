"""One-shot, operator-only AMI preparation. No AWS SDK dependency; uses AWS CLI v2."""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
import uuid

HERE = Path(__file__).resolve().parent
TAG = "rcp-ami-build"
TIMEOUT = 3600
POLL = 15
# observed EC2 console snapshots have lagged stopped builders by about seven minutes
CONSOLE_GRACE = 600
READ_OPERATIONS = {
    ("sts", "get-caller-identity"),
    ("cloudformation", "describe-stacks"), ("cloudformation", "describe-stack-resources"),
    ("ec2", "describe-images"), ("ec2", "describe-instance-types"), ("ec2", "describe-subnets"),
    ("ec2", "describe-instances"), ("ec2", "get-console-output"), ("ec2", "describe-snapshots"),
    ("ec2", "describe-image-attribute"), ("ec2", "describe-volumes"),
}


class Progress:
    """Best-effort status between existing bounded requests; never part of cleanup."""

    def __init__(self, stream=None, clock=time.monotonic):
        self.stream = sys.stderr if stream is None else stream
        self.clock = clock
        self.started = clock()
        self.last = None
        self.stage = None
        self.enabled = True

    def __call__(self, stage, detail):
        current = self.clock()
        if not self.enabled or (stage == self.stage and self.last is not None and current - self.last < 30):
            return
        self.stage, self.last = stage, current
        stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        try:
            print(f"{stamp} +{int(current - self.started)}s [{stage}] {detail}",
                  file=self.stream, flush=True)
        except (OSError, ValueError):
            # a disconnected output stream must not prevent normal resource cleanup
            self.enabled = False


def require(condition, message):
    if not condition:
        raise ValueError(message)


def tags(resource):
    return {t["Key"]: t["Value"] for t in resource.get("Tags", [])}


def one(items, message):
    require(len(items) == 1, message)
    return items[0]


class AwsError(ValueError):
    def __init__(self, service, operation, reason):
        super().__init__(service + " " + operation + " failed: " + reason)
        match = re.search(r"An error occurred \(([A-Za-z0-9_.-]+)\) when calling ", reason)
        self.code = match.group(1) if match else None


class Aws:
    def __init__(self, region):
        self.region = region

    def __call__(self, service, operation, **request):
        # reads tolerate transient faults within one bounded CLI process; mutations stay single-attempt
        # CreateImage has no token, so retrying its unknown outcome could create duplicate paid artifacts
        attempts = "3" if (service, operation) in READ_OPERATIONS else "1"
        env = {**os.environ, "AWS_RETRY_MODE": "standard", "AWS_MAX_ATTEMPTS": attempts, "AWS_PAGER": ""}
        result = subprocess.run(
            ["aws", service, operation, "--region", self.region, "--output", "json",
             "--no-cli-pager", "--no-cli-auto-prompt", "--cli-connect-timeout", "5", "--cli-read-timeout", "20",
             "--cli-input-json", json.dumps(request)],
            check=False, text=True, capture_output=True, timeout=90, env=env,
        )
        if result.returncode:
            raise AwsError(service, operation, result.stderr.strip())
        return json.loads(result.stdout or "{}")


def save(directory, manifest):
    temporary = directory / "manifest.tmp"
    with temporary.open("w") as stream:
        json.dump(manifest, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(directory / "manifest.json")
    descriptor = os.open(directory, os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def prepare(args):
    require(re.fullmatch(r"[0-9]{12}", args.account), "expected a 12-digit account ID")
    require(re.fullmatch(r"[a-z]{2}-[a-z]+-[0-9]", args.region), "commercial AWS region required")
    require(re.fullmatch(r"[0-9a-f]{40}", args.revision), "pin a full lowercase commit SHA")
    for value, prefix in [(args.base_ami, "ami"), (args.vpc, "vpc"), (args.subnet, "subnet")]:
        require(re.fullmatch(prefix + r"-[0-9a-f]{8,17}", value), "invalid " + prefix + " ID")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=HERE, text=True).strip()
    require(revision == args.revision, "check out the reviewed pinned revision before preparing")
    build_id = uuid.uuid4().hex
    def committed(name):
        return subprocess.check_output(
            ["git", "show", args.revision + ":infra/aws_benchmark/" + name], cwd=HERE, text=True)
    template = json.loads(committed("ami-builder.json"))
    guest = committed("ami-guest.sh").replace("__BUILD_ID__", build_id)
    guest = guest.replace("__REVISION__", args.revision)
    guest = guest.replace("__MAINTENANCE_SOURCE__", committed("maintenance.py"))
    require(len(guest.encode()) <= 16384, "generated user data exceeds EC2 limit")
    template["Resources"]["Builder"]["Properties"]["UserData"] = {
        "Fn::Base64": guest,
    }
    body = json.dumps(template, indent=2) + "\n"
    manifest = dict(schema=1, build_id=build_id, account=args.account, region=args.region,
                    revision=args.revision, base_ami=args.base_ami, vpc=args.vpc,
                    subnet=args.subnet, stack_name="rcp-ami-" + build_id,
                    image_name="rcp-worker-" + build_id, stage="prepared",
                    template_sha256=hashlib.sha256(body.encode()).hexdigest(),
                    snapshot_ids=[])
    args.directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    (args.directory / "template.json").write_text(body)
    save(args.directory, manifest)
    print("Prepared offline. Review template.json and manifest.json before running.")


def identity(aws, m):
    caller = aws("sts", "get-caller-identity")
    require(caller["Account"] == m["account"], "AWS account differs from manifest")
    require(caller["Arn"].startswith("arn:aws:"), "only the commercial AWS partition is supported")


def image_shape(image):
    require(image.get("Architecture") == "x86_64" and image.get("VirtualizationType") == "hvm"
            and image.get("EnaSupport") is True, "AMI must support x86_64 HVM and ENA")
    require(image.get("RootDeviceType") == "ebs" and image.get("RootDeviceName") == "/dev/sda1",
            "AMI must have an EBS /dev/sda1 root")
    root = None
    devices, virtuals = set(), set()
    for mapping in image["BlockDeviceMappings"]:
        device = mapping.get("DeviceName")
        require(isinstance(device, str) and device not in devices, "missing or duplicate AMI device")
        devices.add(device)
        if "Ebs" in mapping:
            require(root is None and device == "/dev/sda1" and set(mapping) == {"DeviceName", "Ebs"},
                    "AMI must contain exactly one EBS volume, its /dev/sda1 root")
            root = mapping["Ebs"]
        else:
            # Canonical AMIs can retain legacy instance-store references; these are not EBS volumes
            virtual = mapping.get("VirtualName")
            require(set(mapping) == {"DeviceName", "VirtualName"}
                    and re.fullmatch(r"/dev/sd[b-z]", device)
                    and isinstance(virtual, str) and re.fullmatch(r"ephemeral(?:[0-9]|1[0-9]|2[0-3])", virtual)
                    and virtual not in virtuals, "unexpected AMI instance-store mapping")
            virtuals.add(virtual)
    require(root is not None, "AMI root EBS mapping is missing")
    require(0 < root["VolumeSize"] <= 16, "AMI root exceeds 16 GiB")
    return root


def preflight(aws, m):
    identity(aws, m)
    image = one(aws("ec2", "describe-images", ImageIds=[m["base_ami"]])["Images"], "AMI missing")
    require(image["OwnerId"] == "099720109477" and image["State"] == "available"
            and not image.get("ProductCodes") and re.fullmatch(
                r"ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24\.04-amd64-server-[0-9.]+",
                image.get("Name", "")), "pin a Canonical Ubuntu 24.04 amd64 server release AMI")
    image_shape(image)
    types = aws("ec2", "describe-instance-types", InstanceTypes=["t3a.small", "c5d.24xlarge"])
    effective = []
    for instance_type in types["InstanceTypes"]:
        modes = instance_type["SupportedBootModes"]
        mode = image.get("BootMode", "legacy-bios")
        if mode == "uefi-preferred":
            mode = "uefi" if "uefi" in modes else "legacy-bios"
        require(mode in modes, "AMI boot mode unsupported by instance type")
        effective.append(mode)
    require(len(effective) == 2 and effective[0] == effective[1], "builder/worker boot mode mismatch")
    m["boot_mode"] = effective[0]
    m["source_boot_mode"] = image.get("BootMode", "legacy-bios")
    subnet = one(aws("ec2", "describe-subnets", SubnetIds=[m["subnet"]])["Subnets"], "subnet missing")
    require(subnet["OwnerId"] == m["account"] and subnet["VpcId"] == m["vpc"]
            and subnet["State"] == "available", "subnet account/VPC/state mismatch")


def stack(aws, m):
    result = one(aws("cloudformation", "describe-stacks", StackName=m.get("stack_id", m["stack_name"]))
                 ["Stacks"], "stack missing; reconcile using its recorded name")
    expected = "arn:aws:cloudformation:" + m["region"] + ":" + m["account"] + ":stack/" + m["stack_name"] + "/"
    require(result["StackId"].startswith(expected) and tags(result).get(TAG) == m["build_id"],
            "stack identity/ownership mismatch")
    require(result["TimeoutInMinutes"] == 60 and result.get("DisableRollback") is False,
            "stack timeout/rollback safety configuration changed")
    m["stack_id"] = result["StackId"]
    m["stack_status"] = result["StackStatus"]
    return result


def active(aws, m, now):
    require(now() < m["submitted_at"] + TIMEOUT, "capture cutoff reached; reconcile retained resources")
    require(stack(aws, m)["StackStatus"] == "CREATE_IN_PROGRESS", "stack is no longer creating; reconcile")


def builder(aws, m, discovering=False):
    resources = aws("cloudformation", "describe-stack-resources", StackName=m["stack_id"])["StackResources"]
    ids = [r["PhysicalResourceId"] for r in resources
           if r["LogicalResourceId"] == "Builder" and r.get("PhysicalResourceId")]
    if not ids:
        return None
    m["instance_id"] = one(ids, "multiple builders")
    try:
        reservations = aws("ec2", "describe-instances", InstanceIds=[m["instance_id"]])["Reservations"]
    except AwsError as error:
        if discovering and error.code == "InvalidInstanceID.NotFound":
            return None  # the outer guest loop bounds eventual discovery by the original capture cutoff
        raise
    instances = [i for r in reservations for i in r["Instances"]]
    if discovering and not instances:
        return None
    instance = one(instances, "builder missing")
    require(tags(instance).get(TAG) == m["build_id"] and instance["ImageId"] == m["base_ami"]
            and instance["InstanceType"] == "t3a.small", "builder identity mismatch")
    if instance["State"]["Name"] in {"running", "stopping", "stopped"}:
        require(instance.get("CurrentInstanceBootMode") == m["boot_mode"], "observed builder boot mode mismatch")
    for mapping in instance.get("BlockDeviceMappings", []):
        if mapping["DeviceName"] == "/dev/sda1":
            require(mapping["Ebs"]["DeleteOnTermination"], "root is retained on termination")
            m["root_volume_id"] = mapping["Ebs"]["VolumeId"]
    return instance


def ready(aws, m, instance):
    if not instance or instance["State"]["Name"] != "stopped":
        return False
    # AWS CLI decodes this field; unlike the EC2 API, its output is already plain text
    console = aws("ec2", "get-console-output", InstanceId=m["instance_id"], Latest=True).get("Output") or ""
    stages = re.findall(r"\[guest:([a-z][a-z0-9-]{0,63})\]", console)
    if stages:
        m["guest_stage"] = stages[-1]
    failure = "RCP_AMI_FAILED:" + m["build_id"]
    detail = re.search(re.escape(failure) + r" stage=([a-z][a-z0-9-]{0,63}) line=([0-9]{1,6}) exit=([0-9]{1,3})", console)
    if detail:
        m["guest_stage"] = detail[1]
        m["guest_failure"] = dict(stage=detail[1], line=int(detail[2]), exit=int(detail[3]))
    require(failure not in console, "guest preparation failed; last guest stage=" + m.get("guest_stage", "unknown"))
    marker = "RCP_AMI_READY:" + m["build_id"] + ":" + m["revision"]
    return marker in console


def inventory(aws, m):
    # discover by ownership tags, including a lost CreateImage response or orphan snapshots
    filters = [{"Name": "tag:" + TAG, "Values": [m["build_id"]]}]
    images = aws("ec2", "describe-images", Owners=[m["account"]], Filters=filters)["Images"]
    require(len(images) <= 1, "multiple tagged images; manual reconciliation required")
    image = images[0] if images else None
    if image:
        require(image["Name"] == m["image_name"] and tags(image).get(TAG) == m["build_id"]
                and image["OwnerId"] == m["account"] and image.get("Public") is False,
                "image identity/privacy mismatch")
        require(not m.get("image_id") or m["image_id"] == image["ImageId"], "image ID changed")
        m["image_id"] = image["ImageId"]
        m["image_state"] = image["State"]
    snapshots = aws("ec2", "describe-snapshots", OwnerIds=[m["account"]], Filters=filters)["Snapshots"]
    for snapshot in snapshots:
        require(tags(snapshot).get(TAG) == m["build_id"] and snapshot["OwnerId"] == m["account"],
                "snapshot ownership mismatch")
    m["snapshot_ids"] = sorted(set(m["snapshot_ids"]) | {s["SnapshotId"] for s in snapshots})
    m["snapshot_states"] = {s["SnapshotId"]: s["State"] for s in snapshots}
    return image, snapshots


def captured(aws, m):
    image, snapshots = inventory(aws, m)
    require(not image or image["State"] in {"pending", "available"}, "AMI capture failed; reconcile")
    if not image or image["State"] != "available":
        return False
    require(image.get("BootMode", "legacy-bios") == m["source_boot_mode"], "captured AMI boot mode changed")
    root = image_shape(image)
    if not snapshots:
        return False
    snapshot = one(snapshots, "expected one owned root snapshot; reconcile before cleanup")
    require(root["SnapshotId"] == snapshot["SnapshotId"] and snapshot["Encrypted"] is True
            and snapshot["VolumeId"] == m["root_volume_id"] and snapshot["VolumeSize"] <= 16
            and m["snapshot_ids"] == [snapshot["SnapshotId"]],
            "captured root snapshot mismatch")
    require(snapshot["State"] != "error", "snapshot failed; reconcile")
    if snapshot["State"] != "completed":
        return False
    permissions = aws("ec2", "describe-image-attribute", ImageId=image["ImageId"], Attribute="launchPermission")
    require(permissions.get("LaunchPermissions") == [], "captured AMI is shared; reconcile")
    return True


def run(aws, directory, m, now=time.time, sleep=time.sleep, progress=None, clock=time.monotonic):
    progress = Progress() if progress is None else progress
    require(m["stage"] == "prepared", "run is one-shot; use inspect, never retry a launch")
    body = (directory / "template.json").read_text()
    require(hashlib.sha256(body.encode()).hexdigest() == m["template_sha256"], "prepared template changed")
    progress("preflight", "Checking account, source AMI, instance compatibility and subnet")
    preflight(aws, m)
    m.update(stage="create_requested", submitted_at=now())
    save(directory, m)  # persist intent before any paid request, even if its response is lost
    parameters = [{"ParameterKey": k, "ParameterValue": v} for k, v in
                  [("BaseAmiId", m["base_ami"]), ("VpcId", m["vpc"]), ("SubnetId", m["subnet"])]]
    progress("launch", "Submitting " + m["stack_name"] + " with 60-minute rollback timeout")
    result = aws("cloudformation", "create-stack", StackName=m["stack_name"], TemplateBody=body,
                 Parameters=parameters, Tags=[{"Key": TAG, "Value": m["build_id"]}],
                 ClientRequestToken=m["build_id"], TimeoutInMinutes=60, OnFailure="ROLLBACK")
    m["stack_id"] = result["StackId"]
    save(directory, m)
    log_hint_shown = False
    stopped_at = None
    while True:
        active(aws, m, now)
        instance = builder(aws, m, discovering=True)
        save(directory, m)
        if instance and not log_hint_shown:
            command = shlex.join(["aws", "ec2", "get-console-output", "--region", m["region"],
                                 "--instance-id", m["instance_id"], "--latest", "--query", "Output",
                                 "--output", "text", "--no-cli-pager", "--no-cli-auto-prompt",
                                 "--cli-connect-timeout", "5", "--cli-read-timeout", "20"])
            progress("console", "Boot-log snapshot (may lag or be empty): AWS_MAX_ATTEMPTS=1 " + command)
            log_hint_shown = True
        state = instance["State"]["Name"] if instance else "not yet discovered"
        progress("guest", f"stack={m['stack_status']} builder={state}; waiting for stopped + readiness marker")
        if state == "stopped" and stopped_at is None:
            stopped_at = clock()
        if ready(aws, m, instance):
            break
        detail = "; last guest stage=" + m.get("guest_stage", "unknown") + "; reconcile"
        require(state not in {"shutting-down", "terminated"}, "builder exited before readiness" + detail)
        require(stopped_at is None or clock() - stopped_at < CONSOLE_GRACE,
                "stopped builder has no matching readiness marker after console-lag grace" + detail)
        require(now() < m["submitted_at"] + 2400, "guest not ready with 20 minutes left" + detail)
        sleep(POLL)
    require(m.get("root_volume_id"), "root volume ID missing")
    active(aws, m, now)
    require(now() < m["submitted_at"] + 2400, "insufficient capture time; reconcile")
    m["stage"] = "capture_requested"
    save(directory, m)
    progress("capture", "Guest ready; requesting private AMI capture from " + m["instance_id"])
    ownership = [{"Key": TAG, "Value": m["build_id"]}, {"Key": "rcp-revision", "Value": m["revision"]}]
    result = aws("ec2", "create-image", InstanceId=m["instance_id"], Name=m["image_name"], NoReboot=True,
                 TagSpecifications=[{"ResourceType": kind, "Tags": ownership} for kind in ["image", "snapshot"]])
    m["image_id"] = result["ImageId"]
    save(directory, m)
    while True:
        active(aws, m, now)
        complete = captured(aws, m)
        save(directory, m)
        states = list(m.get("snapshot_states", {}).values())
        progress("snapshot", f"AMI={m.get('image_state', 'not yet discovered')} "
                 f"snapshots completed={states.count('completed')}/{len(states)}")
        if complete:
            break
        sleep(POLL)
    active(aws, m, now)
    m["stage"] = "delete_requested"
    save(directory, m)
    progress("cleanup", "Capture verified; deleting temporary stack")
    aws("cloudformation", "delete-stack", StackName=m["stack_id"], ClientRequestToken="delete-" + m["build_id"])
    cleanup_end = now() + 600
    while True:
        status = stack(aws, m)["StackStatus"]
        save(directory, m)
        progress("cleanup", "stack=" + status)
        if status == "DELETE_COMPLETE":
            break
        require(status == "DELETE_IN_PROGRESS" and now() < cleanup_end, "cleanup incomplete; reconcile")
        sleep(POLL)
    progress("verify", "Checking instance termination and root-volume removal")
    instance = builder(aws, m)
    require(instance and instance["State"]["Name"] == "terminated", "builder is not terminated")
    volumes = aws("ec2", "describe-volumes", Filters=[
        {"Name": "volume-id", "Values": [m["root_volume_id"]]}])["Volumes"]
    require(not volumes, "builder root volume remains; reconcile")
    m["stage"] = "complete"
    save(directory, m)
    progress("complete", "Private AMI verified; temporary instance and root volume removed")
    print("Private AMI ready:", m["image_id"], "Snapshots:", ", ".join(m["snapshot_ids"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("prepare", help="offline: generate reviewable template and manifest")
    for name in ["account", "region", "base-ami", "vpc", "subnet", "revision"]:
        plan.add_argument("--" + name, required=True)
    for command in [plan, commands.add_parser("run", help="launch one paid builder in your account"),
                    commands.add_parser("inspect", help="read-only reconciliation, never relaunch")]:
        command.add_argument("--directory", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args)
        return
    with (args.directory / "lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        m = json.loads((args.directory / "manifest.json").read_text())
        require(m["schema"] == 1, "unknown manifest version")
        aws = Aws(m["region"])
        if args.command == "inspect":
            identity(aws, m)
            errors = []
            for action in [lambda: stack(aws, m), lambda: builder(aws, m), lambda: inventory(aws, m)]:
                try:
                    action()
                except Exception as error:
                    errors.append(str(error))
            m["inspection_errors"] = errors
            save(args.directory, m)
            print(json.dumps(m, indent=2))
            require(not errors, "inspection incomplete; preserve manifest and reconcile manually")
        else:
            try:
                run(aws, args.directory, m)
            except BaseException as error:
                m["last_error"] = type(error).__name__ + ": " + str(error)
                save(args.directory, m)
                if m["stage"] == "prepared":
                    print("Pre-launch checks failed. This run submitted no stack request; "
                          "no rollback was armed. The manifest remains prepared. Correct the "
                          "reported error before retrying. Use a fresh prepare directory if "
                          "changing inputs or the reviewed source revision.", file=sys.stderr)
                else:
                    print("Build did not complete. Keep this directory and inspect before cleanup; "
                          "do not rerun run. A stack request may have been submitted; any submitted "
                          "stack is configured for independent rollback. AMI/snapshots may remain "
                          "and incur charges.", file=sys.stderr)
                raise


if __name__ == "__main__":
    main()
