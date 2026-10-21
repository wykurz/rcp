"""Operator-only preparation and one-time arming for the durable protocol.

Preparing files is offline. Arming creates an exclusive lease only after a fresh independent janitor scan.
It never requests a worker or opens an ASG; the direct launcher owns one allocation. A retained lease requires manual retirement.
"""

import argparse
import base64
from datetime import datetime, timezone
from decimal import Decimal
import json
import re
from pathlib import Path

from . import durable
from .common import BINARIES, STATUS_KEY, digest, pending_status, put_json, require, utc
from .controller import read_binary
from .durable_aws import Configuration, REQUEST, Service, clients, verify_launch_data
from .identity import load as load_identity


def reservation(request, quote, now):
    require(isinstance(quote, dict) and set(quote) == {
        "run", "priced_at", "hourly_usd", "ancillary_usd", "shutdown_seconds", "cap_usd",
        "previous_reserved_usd", "allocation_usd", "cost_evidence"}, "unexpected reservation fields")
    require(quote["run"] == request["run"], "reservation is for another attempt")
    require(0 <= (now - utc(quote["priced_at"])).total_seconds() <= 86400, "price evidence is stale")
    require(isinstance(quote["cost_evidence"], str) and bool(quote["cost_evidence"].strip()),
            "builder, image retention, storage, control and transfer estimates need evidence")
    cost = durable.cost_projection(hourly=quote["hourly_usd"], ancillary=quote["ancillary_usd"],
        runtime_seconds=request["runtime_seconds"],
        capacity_seconds=int((utc(request["capacity_deadline"]) - utc(request["created_at"])).total_seconds()),
        shutdown_seconds=quote["shutdown_seconds"])
    cap, prior, amount = (Decimal(str(quote[k])) for k in ("cap_usd", "previous_reserved_usd", "allocation_usd"))
    require(all(v.is_finite() and v >= 0 for v in (cap, prior, amount)), "invalid reservation values")
    require(amount >= Decimal(cost["guarded_failure_usd"]),
            "reservation does not cover direct runtime and independent shutdown allowance")
    require(prior + amount <= cap, "total approved budget cannot cover this attempt")
    return dict(quote=quote, projection=cost, reserved_total_usd=str(prior + amount))


def verify_image(service):
    config = service.config
    response = service.api["ec2"].describe_launch_template_versions(LaunchTemplateId=config.lt_id,
                                                                  Versions=[config.lt_version])
    versions = response["LaunchTemplateVersions"]
    require(len(versions) == 1 and str(versions[0]["VersionNumber"]) == config.lt_version,
            "numeric launch version not found")
    data = verify_launch_data(config, versions[0]["LaunchTemplateData"])
    userdata = base64.b64decode(data["UserData"], validate=True).decode()
    import yaml
    template = yaml.safe_load(Path(__file__).with_name("stack.yaml").read_text())
    expected = template["Resources"]["LaunchTemplate"]["Properties"]["LaunchTemplateData"]["UserData"]["Fn::Base64"]["Fn::Sub"]
    expected = expected.replace("${WorkerProtocol}", "direct").replace("${Artifacts}", config.bucket).replace(
        "${AWS::Region}", config.region)
    require(userdata == expected, "launch template differs from exact reviewed direct guest startup")
    images = service.api["ec2"].describe_images(ImageIds=[config.ami], Owners=[config.account])["Images"]
    require(len(images) == 1 and images[0]["State"] == "available" and not images[0].get("Public") and
            images[0].get("RootDeviceName") == "/dev/sda1" and images[0].get("RootDeviceType") == "ebs" and
            images[0].get("Architecture") == "x86_64", "worker image must be compatible, private and account-owned")
    mappings = images[0]["BlockDeviceMappings"]
    require(len(mappings) == 1 and mappings[0].get("DeviceName") == "/dev/sda1", "unexpected AMI volumes")
    root = mappings[0].get("Ebs", {})
    require(root.get("Encrypted") is True and 0 < root.get("VolumeSize", 0) <= 16, "invalid encrypted image root")
    snapshots = service.api["ec2"].describe_snapshots(SnapshotIds=[root["SnapshotId"]], OwnerIds=[config.account])["Snapshots"]
    require(len(snapshots) == 1 and snapshots[0].get("State") == "completed" and snapshots[0].get("Encrypted") is True
            and snapshots[0].get("SnapshotId") == root["SnapshotId"] and snapshots[0].get("VolumeSize") == root["VolumeSize"],
            "root snapshot differs from reviewed image")
    key_arn = snapshots[0].get("KmsKeyId", "")
    require(re.fullmatch(r"arn:aws:kms:" + re.escape(config.region) + ":" + config.account + r":key/[0-9a-f-]{36}", key_arn),
            "snapshot KMS identity differs")
    key = service.api["kms"].describe_key(KeyId=key_arn)["KeyMetadata"]
    require(key.get("Arn") == key_arn and key.get("AWSAccountId") == config.account and key.get("KeyManager") == "AWS"
            and key.get("KeyState") == "Enabled" and key.get("KeyUsage") == "ENCRYPT_DECRYPT"
            and key.get("KeySpec") == "SYMMETRIC_DEFAULT", "custom or unavailable root encryption key is unsupported")
    alias = service.api["kms"].describe_key(KeyId="alias/aws/ebs")["KeyMetadata"]
    require(alias.get("Arn") == key_arn, "root snapshot does not use the exact AWS-managed EBS key")
    permissions = service.api["ec2"].describe_image_attribute(ImageId=config.ami, Attribute="launchPermission")
    require(permissions.get("LaunchPermissions") == [], "worker image is shared")
    subnet = service.api["ec2"].describe_subnets(SubnetIds=[config.subnet])["Subnets"]
    groups = service.api["ec2"].describe_security_groups(GroupIds=[config.security_group])["SecurityGroups"]
    require(len(subnet) == len(groups) == 1 and subnet[0]["OwnerId"] == config.account and
            subnet[0]["State"] == "available" and subnet[0]["VpcId"] == groups[0]["VpcId"] and
            groups[0]["OwnerId"] == config.account and groups[0].get("IpPermissions") == [],
            "worker network ownership or isolation differs")
    # the exact AMI/source/package build receipt is still reviewed by the operator;
    # source tags alone do not establish provenance


def verify_management(service):
    config = service.config
    path = "/ssm/managed-instance/default-ec2-instance-management-role"
    arn = "arn:aws:ssm:" + config.region + ":" + config.account + ":servicesetting" + path
    setting = service.api["ssm"].get_service_setting(SettingId=arn)["ServiceSetting"]
    require(setting.get("SettingId") in {path, arn} and setting.get("Status") == "Default" and
            setting.get("SettingValue") == "$None", "Default Host Management Configuration must be disabled before arming")


def arm(service, request, quote):
    durable.validate(request)
    require(request["pins"] == service.config.pins(), "request deployment pins differ")
    allocation = reservation(request, quote, service.now())
    require(0 <= (service.now() - utc(request["created_at"])).total_seconds() <= 120,
            "prepare a fresh immutable request after the builds finish")
    response = service.api["dynamodb"].get_item(TableName=service.config.table,
        Key={"pk": {"S": "lease"}}, ConsistentRead=True)
    require(not response.get("Item"), "existing lease must be physically reconciled and retired manually")
    service.fresh_guard()
    require(not any(i["State"]["Name"] != "terminated" for i in service.inventory()), "physical managed worker exists")
    verify_management(service)
    verify_image(service)
    service.fresh_guard()
    state = durable.initial_lease(request)
    state["reservation"] = allocation
    service.api["dynamodb"].put_item(TableName=service.config.table,
        Item={"pk": {"S": "lease"}, "revision": {"N": "0"}, "payload": {"S": json.dumps(state, sort_keys=True)}},
        ConditionExpression="attribute_not_exists(pk)")
    # failures retain ownership; no automatic retry can replace the armed lease
    put_json(service.api["s3"], service.config.bucket, REQUEST, request)
    put_json(service.api["s3"], service.config.bucket, STATUS_KEY, pending_status(request["run"]))
    return dict(protocol=durable.PROTOCOL, state="armed-not-launched", request=request, reservation=allocation)


def read_json(path, limit=16384):
    with Path(path).open(encoding="utf-8") as source:
        data = source.read(limit + 1)
    require(len(data) <= limit, "operator input exceeds bound")
    return json.loads(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare", help="write a fresh request offline after build completion")
    prepare.add_argument("--identity", required=True, type=Path)
    prepare.add_argument("--binaries", required=True, type=Path)
    prepare.add_argument("--configuration", required=True, type=Path)
    prepare.add_argument("--runtime-minutes", default=180, type=int)
    prepare.add_argument("--capacity-hours", default=12, type=int)
    execute = commands.add_parser("arm", help="operator mutation; requires deployed guard and funded exposure")
    execute.add_argument("--request", required=True, type=Path)
    execute.add_argument("--configuration", required=True, type=Path)
    execute.add_argument("--reservation", required=True, type=Path)
    for command in (prepare, execute):
        command.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    config = Configuration(**read_json(args.configuration))
    require(not args.output.exists(), "output exists; do not overwrite an operation receipt")
    if args.command == "prepare":
        identity = load_identity(args.identity)
        hashes = {key: digest(read_binary(args.binaries / key)) for key in BINARIES}
        result = durable.request(identity, datetime.now(timezone.utc), hashes, config.pins(),
                                 runtime_seconds=args.runtime_minutes * 60,
                                 capacity_seconds=args.capacity_hours * 3600)
    else:
        request, quote = read_json(args.request), read_json(args.reservation)
        # validate funding before loading AWS service clients
        durable.validate(request)
        reservation(request, quote, datetime.now(timezone.utc))
        with args.output.open("x", encoding="utf-8") as output:
            json.dump(dict(state="intent-outcome-uncertain-until-readback", run=request["run"]), output)
        api = clients(config.region)
        import boto3
        from botocore.config import Config
        read_config = Config(connect_timeout=3, read_timeout=5, retries={"total_max_attempts": 1})
        for name in ("ssm", "kms"):
            api[name] = boto3.client(name, region_name=config.region, config=read_config)
        result = arm(Service(api, config), request, quote)
        args.output.with_suffix(args.output.suffix + ".complete.json").write_text(json.dumps(result, indent=2) + "\n")
        return
    with args.output.open("x", encoding="utf-8") as output:
        json.dump(result, output, indent=2)
        output.write("\n")


if __name__ == "__main__":
    main()
