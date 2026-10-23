"""Short production submission client; downloaded executables are treated as data.

Only the server can reserve, arm or launch. An uncertain invoke/upload fails closed;
a retry uses the original signed GitHub run and can never mint a second allocation.
This client never prints JWTs, presigned URLs, credentials or raw service errors.
"""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request

from benchmarks.strict_json import parse_json
from .common import BINARIES, Budget, require, retryable_io, utc
from .controller import oidc_session, read_binary

MAX_RESPONSE = 65536
LAMBDA_READ_TIMEOUT = 60  # exceeds the admission function's 45-second execution limit
TOKEN_READ_TIMEOUT = 10
INVOKE_RESERVE_SECONDS = TOKEN_READ_TIMEOUT + 3 + LAMBDA_READ_TIMEOUT
SUBMISSION_SECONDS = 600
MAX_INVOKE_ATTEMPTS = 4
ACTIVE_SUBMISSION_STATES = frozenset({"armed", "launching", "uncertain", "requested"})


class AdmissionRetryable(ValueError):
    """A sanitized transient response; retry only this exact action and producer."""

    def __init__(self, delay=5):
        super().__init__("admission temporarily unavailable; retain existing reservation")
        self.delay = delay


def lambda_config():
    from botocore.config import Config
    return Config(connect_timeout=3, read_timeout=LAMBDA_READ_TIMEOUT,
                  retries={"total_max_attempts": 1}, proxies={}, ignore_configured_endpoint_urls=True)


def reconcile(operation, *, deadline, reserve, clock, sleep):
    """Bound same-attempt retries; an exhausted/ambiguous result never releases ownership."""
    for attempt in range(MAX_INVOKE_ATTEMPTS):
        require(clock() + reserve < deadline, "submission reconciliation window exhausted")
        try:
            return operation()
        except Exception as error:
            if not isinstance(error, AdmissionRetryable) and not retryable_io(error):
                raise
            delay = max((5, 15, 45, 45)[attempt], getattr(error, "delay", 0))
            require(attempt + 1 < MAX_INVOKE_ATTEMPTS and clock() + delay + reserve < deadline,
                    "submission reconciliation remains pending; retain receipt")
            sleep(delay)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("production request redirect rejected")


def opener():
    return urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())


def token(audience):
    require(isinstance(audience, str) and 0 < len(audience) <= 512 and audience != "sts.amazonaws.com"
            and audience.isascii() and not any(c.isspace() for c in audience), "invalid admission audience")
    parsed = urllib.parse.urlsplit(os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"])
    require(parsed.scheme == "https" and parsed.hostname and not parsed.username and not parsed.password
            and parsed.port in (None, 443) and not parsed.fragment, "invalid GitHub token endpoint")
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    require(not any(key == "audience" for key, _ in query), "token endpoint already selects an audience")
    url = urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(query + [("audience", audience)])))
    request = urllib.request.Request(url, headers={
        "Authorization": "Bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]})
    with opener().open(request, timeout=TOKEN_READ_TIMEOUT) as response:
        require(response.status == 200, "GitHub token request failed")
        data = response.read(MAX_RESPONSE + 1)
    require(len(data) <= MAX_RESPONSE, "GitHub token response exceeds bound")
    value = parse_json(data.decode("utf-8")).get("value")
    require(isinstance(value, str) and 0 < len(value) <= 16384 and value.isascii(), "invalid GitHub token response")
    return value


def prepare(binary_root, revision):
    require(isinstance(revision, str) and re.fullmatch(r"[0-9a-f]{40}", revision), "source must be a full SHA")
    root = Path(binary_root)
    binaries = {}
    for name in ("rcp", "rcpd", "filegen"):
        data = read_binary(root / name)
        for side in ("baseline", "candidate"):
            binaries[side + "/" + name] = data
    manifest = dict(baseline=revision, candidate=revision, suite="bulk-1g-metadata",
        binaries={name: dict(sha256=hashlib.sha256(data).hexdigest(), bytes=len(data))
                  for name, data in binaries.items()})
    return manifest, binaries


def invoke(api, function, value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    require(len(raw) <= MAX_RESPONSE, "admission invocation exceeds bound")
    result = api.invoke(FunctionName=function, InvocationType="RequestResponse", LogType="None", Payload=raw)
    require(result.get("StatusCode") == 200 and not result.get("FunctionError"), "admission invocation failed")
    stream = result["Payload"]
    try:
        body = stream.read(MAX_RESPONSE + 1)
    finally:
        stream.close()
    require(len(body) <= MAX_RESPONSE, "admission response exceeds bound")
    envelope = parse_json(body.decode("utf-8"))
    if isinstance(envelope, dict) and envelope.get("ok") is False and envelope.get("error") == "admission_retryable":
        delay = envelope.get("retry_after_seconds")
        require(set(envelope) == {"ok", "error", "retry_after_seconds"}
                and type(delay) is int and 1 <= delay <= 86400, "invalid admission retry interval")
        raise AdmissionRetryable(delay)
    require(isinstance(envelope, dict) and set(envelope) == {"ok", "result"}
            and envelope["ok"] is True and isinstance(envelope["result"], dict),
            "admission rejected or requires reconciliation")
    return envelope["result"]


def public_receipt(value):
    # an explicit allowlist keeps capabilities out of artifacts even if the service adds fields
    fields = {"schema_version", "run_key", "run", "epoch", "state", "reserve_microusd", "capacity_deadline",
              "outer_deadline", "runtime_seconds", "reservation_created", "execution_arn", "execution_status",
              "upload_deadline"}
    result = {key: item for key, item in value.items() if key in fields}
    require(result.get("schema_version") == 1 and isinstance(result.get("run_key"), str)
            and re.fullmatch(r"repo-[1-9][0-9]{0,19}-run-[1-9][0-9]{0,19}", result["run_key"]),
            "invalid admission receipt")
    return result


def persist(path, value):
    path = Path(path)
    data = json.dumps(public_receipt(value), sort_keys=True, indent=2, allow_nan=False) + "\n"
    # output lives in a fresh trusted runner directory, separate from candidate artifacts
    path.write_text(data, encoding="utf-8")


def upload(capability, data, *, bucket, region):
    require(isinstance(capability, dict) and set(capability) == {"url", "headers"}, "invalid upload capability")
    url, headers = capability["url"], capability["headers"]
    require(isinstance(url, str) and len(url) <= 16384, "invalid upload URL")
    target = urllib.parse.urlsplit(url)
    hosts = {bucket + ".s3." + region + ".amazonaws.com", bucket + ".s3.amazonaws.com"}
    require(target.scheme == "https" and target.hostname in hosts and target.port in (None, 443)
            and not target.username and not target.password and not target.fragment,
            "upload destination differs from configured private bucket")
    require(isinstance(headers, dict) and set(headers) == {
        "Content-Length", "x-amz-checksum-sha256", "If-None-Match", "Content-Type", "x-amz-server-side-encryption"}
        and headers["Content-Length"] == str(len(data)) and headers["If-None-Match"] == "*"
        and headers["Content-Type"] == "application/octet-stream"
        and headers["x-amz-server-side-encryption"] == "AES256", "upload contract differs")
    import base64
    require(headers["x-amz-checksum-sha256"] == base64.b64encode(hashlib.sha256(data).digest()).decode(),
            "upload checksum differs")
    request = urllib.request.Request(url, method="PUT", data=data, headers=headers)
    try:
        with opener().open(request, timeout=30) as response:
            require(response.status == 200, "binary upload failed")
            require(len(response.read(1025)) <= 1024, "upload response exceeds bound")
    except urllib.error.HTTPError as error:
        error.close()
        if error.code in (429, 500, 502, 503, 504):
            raise AdmissionRetryable() from None
        if error.code != 412:
            raise ValueError("binary upload failed; retain admission receipt") from None
        # create-only replay does not prove equality; the server verifies the pinned checksum/version


def submitted(value, *, function, expected_run):
    require(value.get("state") in ACTIVE_SUBMISSION_STATES and value.get("run") == expected_run
            and value.get("execution_status") in {"submitted", "RUNNING"},
            "submission is not active/running; retain receipt for reconciliation")
    parts = function.split(":")
    require(len(parts) == 7 and parts[:3] == ["arn", "aws", "lambda"], "invalid admission function")
    pattern = r"arn:aws:states:" + re.escape(parts[3] + ":" + parts[4]) + \
        r":execution:[A-Za-z0-9_-]{1,80}:" + re.escape(expected_run)
    require(isinstance(value.get("execution_arn"), str)
            and re.fullmatch(pattern, value["execution_arn"]), "submission execution differs")
    return public_receipt(value)


def submit(api, function, manifest, binaries, receipt_path, *, audience, bucket, region,
           token_reader=token, uploader=upload, clock=time.monotonic, wall_clock=time.time, sleep=time.sleep,
           expected_run=None):
    if expected_run is None:
        expected_run = os.environ["GITHUB_RUN_ID"] + "-" + os.environ["GITHUB_RUN_ATTEMPT"]
    require(re.fullmatch(r"[1-9][0-9]{0,19}-[1-9][0-9]{0,4}", expected_run), "invalid producer run")
    deadline = clock() + SUBMISSION_SECONDS
    def call(action):
        value = dict(action=action, token=token_reader(audience))
        if action == "begin":
            value["manifest"] = manifest
        return invoke(api, function, value)
    admitted = reconcile(lambda: call("begin"), deadline=deadline, reserve=INVOKE_RESERVE_SECONDS,
                         clock=clock, sleep=sleep)
    persist(receipt_path, admitted)
    require(admitted.get("run") == expected_run, "workflow rerun cannot submit an earlier producer")
    capabilities = admitted.get("uploads")
    if capabilities is None:
        # a lost begin response may have been followed by a successful same-attempt seal.
        # otherwise only seal can prove the pinned execution; pending receipts never succeed.
        require(admitted.get("state") in ACTIVE_SUBMISSION_STATES, "admission is held or closed; retain receipt")
    else:
        require(admitted.get("state") == "awaiting-inputs" and isinstance(capabilities, dict)
                and set(capabilities) == set(BINARIES), "upload set differs")
        deadline = min(deadline, clock() + utc(admitted["upload_deadline"]).timestamp() - wall_clock())
        for name in BINARIES:
            reconcile(lambda: uploader(capabilities[name], binaries[name], bucket=bucket, region=region),
                      deadline=deadline, reserve=30, clock=clock, sleep=sleep)
    sealed = reconcile(lambda: call("seal"), deadline=deadline, reserve=INVOKE_RESERVE_SECONDS,
                       clock=clock, sleep=sleep)
    require(sealed.get("run_key") == admitted["run_key"] and sealed.get("epoch") == admitted.get("epoch"),
            "sealed receipt differs from admitted owner")
    persist(receipt_path, sealed)
    return submitted(sealed, function=function, expected_run=expected_run)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--role", required=True)
    parser.add_argument("--function", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--audience", required=True)
    args = parser.parse_args()
    require(re.fullmatch(r"[a-z]{2}-[a-z]+-[1-9][0-9]?", args.region), "invalid region")
    require(re.fullmatch(r"arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+", args.role), "invalid submission role")
    account = args.role.split(":")[4]
    require(re.fullmatch(r"arn:aws:lambda:" + re.escape(args.region) + ":" + account
            + r":function:[A-Za-z0-9_-]{1,64}", args.function), "invalid admission function")
    require(re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", args.bucket), "invalid production bucket")
    manifest, binaries = prepare(args.binaries, args.revision)
    budget = Budget(datetime.now(timezone.utc) + timedelta(seconds=60))
    session, _ = oidc_session(args.region, args.role, session_prefix="rcp-production-",
        duration_seconds=900, budget=budget, reserve=10)
    result = submit(session.client("lambda", config=lambda_config(),
        endpoint_url=f"https://lambda.{args.region}.amazonaws.com"), args.function, manifest, binaries, args.receipt,
                    audience=args.audience, bucket=args.bucket, region=args.region)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("Production submission failed or is held for reconciliation (" + type(error).__name__ + ").")
        raise SystemExit(1) from None
