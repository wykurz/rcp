"""Operator-only atomic ownership of the fixed benchmark mailbox.

Claims have no expiry or automatic takeover. Partial failures retain ownership;
retirement requires separate physical, producer and result reconciliation.
"""

import json
import uuid

from .common import DIRECT_PROTOCOL, digest, error_code, get_bytes, require

OWNER_KEY = "control/mailbox-owner.json"


def verify_retention(s3, bucket):
    rules = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
    require(isinstance(rules, list) and bool(rules), "mailbox lifecycle configuration is missing")
    for rule in rules:
        require(isinstance(rule, dict) and rule.get("Status") in {"Enabled", "Disabled"},
                "unexpected mailbox lifecycle rule")
        if rule["Status"] != "Enabled" or "Expiration" not in rule:
            continue
        selected = rule.get("Filter", {"Prefix": rule.get("Prefix", "")})
        require(isinstance(selected, dict) and set(selected) == {"Prefix"}
                and isinstance(selected["Prefix"], str) and not OWNER_KEY.startswith(selected["Prefix"]),
                "mailbox lifecycle can expire ownership; migrate the closed bucket first")


def claim(s3, bucket, protocol, request):
    require(protocol in {"operator-window", DIRECT_PROTOCOL}, "unsupported mailbox owner protocol")
    verify_retention(s3, bucket)
    encoded = json.dumps(request, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    owner = dict(schema_version=1, token=uuid.uuid4().hex, bucket=bucket, protocol=protocol,
                 run=request["run"], request_sha256=digest(encoded))
    body = (json.dumps(owner, sort_keys=True, separators=(",", ":")) + "\n").encode()
    try:
        s3.put_object(Bucket=bucket, Key=OWNER_KEY, Body=body, ContentType="application/json",
                      IfNoneMatch="*")
    except Exception as error:
        # a definite conflict never joins another arm, even for the same request
        if error_code(error) in {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}:
            raise
        try:
            observed = get_bytes(s3, bucket, OWNER_KEY, 4096)
        except Exception:
            raise error
        # a unique invocation token binds read-back to this possibly accepted put
        require(observed == body, "mailbox ownership is uncertain or belongs to another operator")
    return owner
