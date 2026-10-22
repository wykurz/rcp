"""Shared image, numeric launch-template and reviewed startup validation."""

import base64
from pathlib import Path
import re

from .common import require


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


def launch_template_data(ec2, template_id, version, read=lambda operation: operation()):
    require(isinstance(version, str) and re.fullmatch(r"[1-9][0-9]*", version),
            "launch template version must be numeric")
    response = read(lambda: ec2.describe_launch_template_versions(LaunchTemplateId=template_id,
                                                                  Versions=[version]))
    require(isinstance(response, dict), "invalid launch template response")
    versions = response.get("LaunchTemplateVersions")
    require(isinstance(versions, list) and len(versions) == 1 and isinstance(versions[0], dict)
            and type(versions[0].get("VersionNumber")) is int
            and str(versions[0]["VersionNumber"]) == version and not response.get("NextToken"),
            "numeric launch version not found")
    require(versions[0].get("LaunchTemplateId", template_id) == template_id,
            "launch template identity differs")
    data = versions[0].get("LaunchTemplateData")
    require(isinstance(data, dict), "launch template data is missing or invalid")
    return data


def reviewed_startup(protocol, bucket, region):
    # only trusted operators parse YAML; worker and Lambda dependencies remain SDK-only
    import yaml
    require(isinstance(protocol, str) and protocol in {"operator-window", "direct"},
            "unknown reviewed worker protocol")
    values = {"WorkerProtocol": protocol, "Artifacts": bucket, "AWS::Region": region}
    require(all(isinstance(value, str) and value and "${" not in value for value in values.values()),
            "invalid reviewed startup substitution")
    template = yaml.safe_load(Path(__file__).with_name("stack.yaml").read_text())
    try:
        script = template["Resources"]["LaunchTemplate"]["Properties"]["LaunchTemplateData"]["UserData"]["Fn::Base64"]["Fn::Sub"]
    except (KeyError, TypeError):
        raise ValueError("reviewed startup template shape differs") from None
    require(isinstance(script, str), "reviewed startup must be a Fn::Sub string")
    def substitute(match):
        name = match[1]
        require(name in values, "unknown reviewed startup placeholder")
        return values[name]
    rendered = re.sub(r"\$\{([^}]+)\}", substitute, script)
    require("${" not in rendered, "unresolved reviewed startup placeholder")
    return rendered


def verify_startup(data, protocol, bucket, region):
    require(isinstance(data, dict) and isinstance(data.get("UserData"), str) and data["UserData"],
            "launch template UserData is missing or invalid")
    try:
        script = base64.b64decode(data["UserData"], validate=True).decode("utf-8")
    except (ValueError, UnicodeError):
        raise ValueError("launch template UserData must be base64-encoded UTF-8") from None
    require(script == reviewed_startup(protocol, bucket, region),
            "launch template is not the exact reviewed " + protocol + " guest startup")
    return script
