"""Validate immutable, non-secret no-worker acceptance bindings before any client exists."""

import hashlib
import json
import re

from .acceptance_cases import Bindings, Hold, require
from benchmarks.strict_json import parse_json

SCHEMA = "rcp-production-acceptance-v1"
MAX_BINDINGS = 16384
KINDS = {"reader-admission", "reader-collector", "s3", "dynamodb"}
FIELDS = {"schema", "kind", "account", "region", "source_commit", "role_arn", "role_id",
          "function_name", "function_version", "log_group", "not_before", "expires_at",
          "approval_capture_sha256", "cases", "fixture_inventory", "proof"}


def matches(pattern, value):
    return isinstance(value, str) and re.fullmatch(pattern, value) is not None


def validate_bindings(value):
    """Validate a captured approval's exact targets, not the truth of its readbacks."""
    require(isinstance(value, dict) and set(value) == FIELDS)
    require(value["schema"] == SCHEMA and value["kind"] in KINDS)
    require(value["account"] == "381183037896" and value["region"] == "us-east-1")
    require(matches(r"[0-9a-f]{40}", value["source_commit"]))
    require(matches(r"arn:aws:iam::381183037896:role/[A-Za-z0-9+=,.@_-]+(?:/[A-Za-z0-9+=,.@_-]+)*", value["role_arn"]))
    require(matches(r"AROA[A-Z0-9]{17}", value["role_id"]))
    require(value["function_version"] == "1")
    require(matches(r"[0-9a-f]{64}", value["approval_capture_sha256"]))
    require(type(value["not_before"]) is int and type(value["expires_at"]) is int
            and 0 < value["not_before"] < value["expires_at"]
            and value["expires_at"] - value["not_before"] <= 86400)
    cases = value["cases"]
    require(isinstance(cases, dict))
    common = {"account", "region", "role_arn"}
    require(all(cases.get(key) == value[key] for key in common))
    if value["kind"].startswith("reader-"):
        role = value["kind"].removeprefix("reader-")
        require(set(cases) == common | {"secret_arn", "current_version", "previous_version",
                "unrelated_secret_arn", "reader_kind", "app_client_id", "installation_id"})
        require(cases["reader_kind"] == role)
        require(value["function_name"] == "rcp-production-benchmark-f13-" + role + "-probe")
        require(matches(r"/aws/lambda/[A-Za-z0-9_-]{1,64}", value["log_group"]))
        # the exact existing role-permitted log group is approved with the role readback
        Bindings(**cases).reader()
        require(type(value["fixture_inventory"]) is dict
                and value["fixture_inventory"].get("previous_stage_exists") is True
                and value["fixture_inventory"].get("unrelated_secret_exists") is True)
        require(value["fixture_inventory"] == {
            "current_version": cases["current_version"], "previous_version": cases["previous_version"],
            "previous_stage_exists": True, "unrelated_secret_exists": True})
        proof = value["proof"]
        require(isinstance(proof, dict) and set(proof) == {"run_id", "run_attempt", "head_sha"})
        require(matches(r"[1-9][0-9]{0,19}", proof["run_id"]) and proof["run_attempt"] == "1")
        require(matches(r"[0-9a-f]{40}", proof["head_sha"]))
    else:
        require(set(cases) == common | {"bucket", "table", "nonce"})
        Bindings(**cases).storage()
        require(value["function_name"] == "rcp-production-acceptance-" + value["kind"] + "-" + cases["nonce"])
        require(value["log_group"] == "/aws/lambda/" + value["function_name"])
        require(value["fixture_inventory"] is None and value["proof"] is None)
        require(value["role_arn"] == "arn:aws:iam::" + value["account"] + ":role/" + value["function_name"])
    # clone validated ordinary JSON to detach caller-owned mutable dictionaries
    return json.loads(json.dumps(value, sort_keys=True, allow_nan=False))


def binding_bytes(value):
    return json.dumps(validate_bindings(value), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest_bindings(value):
    return hashlib.sha256(binding_bytes(value)).hexdigest()


def parse_bindings(raw):
    try:
        require(isinstance(raw, bytes) and 0 < len(raw) <= MAX_BINDINGS)
        return validate_bindings(parse_json(raw.decode("utf-8")))
    except Exception:
        raise Hold("acceptance bindings or evidence incomplete") from None
