"""Exact production guest startup, independent of operator YAML dependencies.

The immutable numeric launch-template version supplies these bytes at launch. This
only binds the startup command; the separately reviewed pinned image must contain
its compatible, root-owned worker and dependencies. The guest timer is a backup
to the independent controller deadline and termination paths, not a billing cap.
"""

import base64
import re

MAX_USER_DATA_BYTES = 16 * 1024
MAX_ENCODED_USER_DATA = 4 * ((MAX_USER_DATA_BYTES + 2) // 3)


def reviewed_startup(*, account, bucket, region):
    """Render one fixed direct worker command from shell-safe deployment pins."""
    if not (isinstance(account, str) and re.fullmatch(r"[0-9]{12}", account)):
        raise ValueError("invalid production startup account")
    if not (isinstance(region, str) and not region.startswith("cn-") and
            re.fullmatch(r"[a-z]{2}-[a-z]+-[1-9][0-9]?", region)):
        raise ValueError("invalid standard-partition production startup region")
    # generated production buckets have no dots, matching admission's virtual-host contract
    if not (isinstance(bucket, str) and re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", bucket)):
        raise ValueError("invalid production startup bucket")
    return ("#!/bin/bash\n"
            "set -euo pipefail\n"
            "# trusted, root-owned production worker is baked into the pinned AMI\n"
            "systemd-run --unit=rcp-supervisor --working-directory=/opt/rcp-benchmark "
            "--property=RuntimeMaxSec=10800 --property=TimeoutStopSec=10 "
            "--property=KillMode=control-group --property=SendSIGKILL=yes "
            "--property=FailureAction=poweroff --property=SuccessAction=poweroff "
            "/opt/rcp-benchmark/venv/bin/python -m infra.aws_benchmark.worker "
            f"--direct --production-account '{account}' --bucket '{bucket}' --region '{region}' "
            "|| systemctl poweroff\n")


def verify_startup(data, *, account, bucket, region):
    """Require bounded base64 UTF-8 UserData to equal the reviewed script exactly."""
    expected = reviewed_startup(account=account, bucket=bucket, region=region)
    encoded = data.get("UserData") if isinstance(data, dict) else None
    if not (isinstance(encoded, str) and 0 < len(encoded) <= MAX_ENCODED_USER_DATA):
        raise ValueError("production launch template UserData is missing or exceeds its bound")
    try:
        raw = base64.b64decode(encoded, validate=True)
        script = raw.decode("utf-8")
    except (ValueError, UnicodeError):
        raise ValueError("production launch template UserData must be base64-encoded UTF-8") from None
    if len(raw) > MAX_USER_DATA_BYTES or script != expected:
        raise ValueError("launch template is not the exact reviewed production guest startup")
    return script
