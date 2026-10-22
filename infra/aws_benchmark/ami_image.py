"""Shared AMI platform and single-root mapping validation for build and admission."""

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
