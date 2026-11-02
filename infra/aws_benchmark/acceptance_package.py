"""Build an offline acceptance ZIP and disabled, separately approved deployment proposal.

This module never uploads, deploys, invokes, or deletes AWS resources. The build
accepts only the fixed repository source closure, a pinned offline wheel set, and
validated immutable bindings. Render independently rebuilds from those inputs and
requires byte identity with the opaque supplied ZIP. It never extracts or executes
that archive. Reproduction requires the same recorded Python/zlib toolchain; an
editable receipt is evidence to compare, never an attestation to trust.
"""

import argparse
import base64
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import zipfile

from . import production_package as production
from .acceptance_bindings import binding_bytes, digest_bindings, parse_bindings, validate_bindings
from benchmarks.strict_json import parse_json

ROOT = production.ROOT
SOURCES = ("infra/aws_benchmark/acceptance_bindings.py", "infra/aws_benchmark/acceptance_cases.py",
           "infra/aws_benchmark/acceptance_runtime.py")
BUILDERS = ("infra/aws_benchmark/acceptance_package.py", "infra/aws_benchmark/production_package.py",
            "infra/aws_benchmark/durable_package.py")
BINDING_FILE = "acceptance-bindings.json"
RECEIPT_SCHEMA = "rcp-production-acceptance-package-v1"
CODE_LIMIT = 32 * 1024 * 1024


def hashes(values):
    return {name: hashlib.sha256(data).hexdigest() for name, data in sorted(values.items())}


def encoded(value):
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def ordinary_input(path, *, limit):
    absolute = Path(path).absolute()
    return production.ordinary_bytes(Path(absolute.anchor), str(absolute.relative_to(absolute.anchor)), limit=limit)


@contextmanager
def output_parent(path):
    """Hold all output ancestry without following links; never remove failed outputs."""
    path = Path(path).absolute()
    if ".." in path.parts or not path.name:
        raise ValueError("output path must be lexically normal")
    held = []
    try:
        directory = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        held.append(directory)
        for part in path.parts[1:-1]:
            directory = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            held.append(directory)
        yield directory, path.name
    finally:
        for descriptor in reversed(held):
            os.close(descriptor)


def new_outputs(*paths):
    if len({str(Path(path).absolute()) for path in paths}) != len(paths):
        raise ValueError("output paths must be distinct")
    for path in paths:
        with output_parent(path) as (parent, name):
            try:
                os.stat(name, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                continue
            raise ValueError("output must be new; existing evidence is never overwritten")


def write_new(path, data):
    with output_parent(path) as (parent, name):
        descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())


def wheel_inputs(path):
    # pin ancestry for enumeration, then snapshot each input without following any path component
    with output_parent(Path(path) / ".wheel-directory") as (directory, _):
        names = sorted(name for name in os.listdir(directory) if name.endswith(".whl"))
    if not names:
        raise ValueError("the pinned offline wheel set is absent")
    return {name: ordinary_input(Path(path) / name, limit=64 * 1024 * 1024) for name in names}


def reviewed_sources(bindings):
    """Require byte identity with tracked HEAD, without claiming remote review proof."""
    sources = production.source_snapshot(ROOT)
    sources.update({name: production.ordinary_bytes(ROOT, name) for name in SOURCES})
    production.verify_source_closure(sources)
    requirements = {name: production.ordinary_bytes(ROOT, name) for name in production.REQUIREMENTS}
    builders = {name: production.ordinary_bytes(ROOT, name) for name in BUILDERS}
    git_environment = {"PATH": os.defpath, "LANG": "C", "LC_ALL": "C", "GIT_CONFIG_NOSYSTEM": "1",
                       "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_NO_REPLACE_OBJECTS": "1"}
    def git(*arguments):
        try:
            return subprocess.run(["/usr/bin/git", "--no-pager", "-C", str(ROOT), *arguments],
                env=git_environment, capture_output=True, check=True, timeout=30).stdout
        except subprocess.CalledProcessError as error:
            raise ValueError("package source must be tracked in the declared commit") from error
    commit = git("rev-parse", "--verify", "HEAD").decode("ascii").strip()
    if commit != bindings["source_commit"]:
        raise ValueError("source_commit must equal repository HEAD")
    for name, data in (sources | requirements | builders).items():
        if git("show", commit + ":" + name) != data:
            raise ValueError("package source differs from declared commit: " + name)
    return sources, requirements, builders


ACCEPTANCE_PROBE = r"""
import importlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1]).resolve()
attempts = []
def deny_network(event, args):
    if event.startswith('socket.'):
        attempts.append(event)
        raise RuntimeError('network is blocked during acceptance verification')
sys.addaudithook(deny_network)
sys.path.insert(0, str(root))
for name in ('acceptance_bindings', 'acceptance_cases', 'acceptance_runtime'):
    module = importlib.import_module('infra.aws_benchmark.' + name)
    if not pathlib.Path(module.__file__).resolve().is_relative_to(root):
        raise RuntimeError('acceptance module imported outside package')
from infra.aws_benchmark.acceptance_bindings import parse_bindings, digest_bindings, validate_bindings
from infra.aws_benchmark.acceptance_cases import Hold
from infra.aws_benchmark.acceptance_runtime import lambda_handler
raw = (root / 'acceptance-bindings.json').read_bytes()
config = parse_bindings(raw)
if digest_bindings(config) != sys.argv[2]:
    raise RuntimeError('embedded binding digest changed')
try:
    validate_bindings({})
except Hold:
    pass
else:
    raise RuntimeError('missing bindings were accepted')
result = lambda_handler({'unexpected': True}, None)
if result.get('ok') is not False or result.get('live_accepted') is not False or attempts:
    raise RuntimeError('invalid invocation did not fail before network')
print(json.dumps({'acceptance_imports_verified': True, 'invalid_invocation_blocked': True,
                  'bindings_sha256': digest_bindings(config), 'network_attempts': 0}, sort_keys=True))
"""


def verify_acceptance(output, python312, work, bindings):
    # the unchanged production verifier also checks native wheels on Linux x86_64 Python 3.12
    result = production.verify_package(output, python312, work)
    probe = subprocess.run([str(Path(python312).absolute()), "-I", "-S", "-B", "-c", ACCEPTANCE_PROBE,
                            str(Path(work) / "verify"), digest_bindings(bindings)],
                           cwd=work, capture_output=True, text=True, check=True, timeout=60)
    return {"production": result, "acceptance": json.loads(probe.stdout)}


def build_package(output, receipt_path, bindings, wheels, python312):
    bindings = validate_bindings(bindings)
    new_outputs(output, receipt_path)
    sources, requirements, builders = reviewed_sources(bindings)
    wheels = wheel_inputs(wheels)
    python312 = Path(python312).absolute()
    with tempfile.TemporaryDirectory(prefix="rcp-acceptance-package-") as directory:
        work = Path(directory)
        base = work / "production.zip"
        wheel_stage = work / "wheels"
        wheel_stage.mkdir()
        for name, data in wheels.items():
            (wheel_stage / name).write_bytes(data)
        base_receipt = production.package(base, wheel_stage, python312)
        # this archive was just built and isolated-verified here, never supplied by a caller
        with zipfile.ZipFile(base) as archive:
            members = {member.filename: archive.read(member) for member in archive.infolist()}
        if any(members.get(name) != sources[name] for name in production.SOURCES):
            raise ValueError("source changed while building the production package")
        if base_receipt["requirements_sha256"] != hashes({Path(k).name: v for k, v in requirements.items()}):
            raise ValueError("requirements changed while building the production package")
        if base_receipt["wheel_sha256"] != hashes(wheels):
            raise ValueError("wheel identity changed while building the production package")
        if any(name in members for name in (*SOURCES, BINDING_FILE)):
            raise ValueError("acceptance source collides with a dependency")
        members.update({name: sources[name] for name in SOURCES})
        members[BINDING_FILE] = binding_bytes(bindings)
        production.verify_source_closure({name: members[name] for name in sources})
        prepared = work / "acceptance.zip"
        production.archive_members(prepared, members)
        if prepared.stat().st_size > CODE_LIMIT:
            raise ValueError("acceptance package exceeds the existing 32 MiB allowance")
        verification_work = work / "acceptance-verification"
        verification_work.mkdir()
        verification = verify_acceptance(prepared, python312, verification_work, bindings)
        content = prepared.read_bytes()
    receipt = dict(schema=RECEIPT_SCHEMA, provisional=True, live_accepted=False,
        source_commit=bindings["source_commit"], bindings_sha256=digest_bindings(bindings),
        archive_bytes=len(content), archive_sha256=hashlib.sha256(content).hexdigest(),
        code_limit_bytes=CODE_LIMIT, within_existing_code_limit=True,
        source_sha256=hashes(sources), requirements_sha256=hashes(requirements),
        builder_sha256=hashes(builders), wheel_sha256=base_receipt["wheel_sha256"],
        compression=production.compression_toolchain(), verification=verification,
        provenance="sources and requirements byte-equal to local HEAD; remote publication and review require separate evidence")
    # publication is exclusive, not a multi-file transaction; a later failure retains prior evidence
    write_new(output, content)
    write_new(receipt_path, encoded(receipt))
    return receipt


def checked_artifact(archive_path, receipt_path, bindings, wheels, python312):
    """Rebuild the complete trusted closure; never execute the supplied artifact."""
    bindings = validate_bindings(bindings)
    receipt = parse_json(ordinary_input(receipt_path, limit=4 * 1024 * 1024).decode("utf-8"))
    content = ordinary_input(archive_path, limit=CODE_LIMIT)
    with tempfile.TemporaryDirectory(prefix="rcp-acceptance-rebuild-") as directory:
        work = Path(directory)
        rebuilt = build_package(work / "expected.zip", work / "expected-receipt.json", bindings, wheels, python312)
        # identity covers every member and header, including missing/extra/duplicate/traversal members
        # only the fresh repository/wheel build is extracted and executed by isolated verification
        if content != ordinary_input(work / "expected.zip", limit=CODE_LIMIT):
            raise ValueError("artifact must match an independent reviewed-source and pinned-wheel rebuild")
    if encoded(receipt) != encoded(rebuilt):
        raise ValueError("receipt must match independent rebuild evidence")
    return rebuilt


def code_pin(bucket, key, version):
    if not isinstance(bucket, str) or re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket) is None \
            or ".." in bucket or re.fullmatch(r"[0-9.]+", bucket):
        raise ValueError("code bucket must be an exact bucket name")
    if not isinstance(key, str) or not 1 <= len(key.encode()) <= 1024 \
            or any(part in {"", ".", ".."} for part in key.split("/")) \
            or any(ord(char) < 33 or ord(char) > 126 for char in key):
        raise ValueError("code key must be exact and bounded")
    if not isinstance(version, str) or version in {"", "null"} or len(version) > 1024 \
            or any(ord(char) < 33 or ord(char) > 126 for char in version):
        raise ValueError("a real immutable code object version is required")
    return {"S3Bucket": bucket, "S3Key": key, "S3ObjectVersion": version}


def deployment_proposal(bindings, receipt, bucket, key, version):
    bindings = validate_bindings(bindings)
    code = code_pin(bucket, key, version)
    digest = receipt.get("archive_sha256")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None \
            or receipt.get("bindings_sha256") != digest_bindings(bindings):
        raise ValueError("package receipt does not bind the code and configuration")
    name = bindings["function_name"]
    seconds = 45 if bindings["kind"].startswith("reader-") else 120
    sha256 = base64.b64encode(bytes.fromhex(digest)).decode("ascii")
    template = {"AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Disabled no-worker acceptance probe; creation, invocation and deletion need separate approval",
        "Metadata": {"Status": "proposal-only", "BindingsSha256": digest_bindings(bindings),
                     "SourceCommit": bindings["source_commit"], "ArchiveSha256": digest},
        "Resources": {
            "AcceptanceFunction": {"Type": "AWS::Lambda::Function", "DeletionPolicy": "Retain",
                "UpdateReplacePolicy": "Retain", "Properties": {"FunctionName": name,
                    "Role": bindings["role_arn"], "Runtime": "python3.12", "Architectures": ["x86_64"],
                    "Handler": "infra.aws_benchmark.acceptance_runtime.lambda_handler", "MemorySize": 256,
                    "Timeout": seconds, "ReservedConcurrentExecutions": 0, "Code": code,
                    "LoggingConfig": {"LogFormat": "Text", "LogGroup": bindings["log_group"]},
                    "Tags": [{"Key": "rcp-acceptance-bindings-sha256", "Value": digest_bindings(bindings)}]}},
            "AcceptanceVersion": {"Type": "AWS::Lambda::Version", "DeletionPolicy": "Retain",
                "UpdateReplacePolicy": "Retain", "Properties": {"FunctionName": {"Ref": "AcceptanceFunction"},
                    "CodeSha256": sha256, "Description": "new function only; must read back as version 1"}}}}
    identity = {key: bindings[key] for key in ("account", "region", "function_name", "function_version",
                "role_arn", "role_id", "log_group", "source_commit", "approval_capture_sha256",
                "not_before", "expires_at")}
    identity.update(bindings_sha256=digest_bindings(bindings), code_sha256=sha256, code_object=code)
    plan = dict(schema="rcp-production-acceptance-proposal-v1", status="proposal-only", live_accepted=False,
        identity=identity, approvals={
            "create_disabled": {
                "operation": "CREATE_ONLY", "template_sha256": hashlib.sha256(encoded(template)).hexdigest(),
                "preconditions": [
                    "Separate explicit approval binds this template, exact uploaded code object version and captured identity.",
                    "Read back the code object's bytes/version/checksum and match the package archive digest.",
                    "Confirm target account/region, role ARN plus immutable role ID, and existing approved log group.",
                    "GetFunction must establish ResourceNotFoundException for this exact name; any existing function, uncertain result or other error holds. Never update an existing function.",
                    "Capture production functions disabled, schedules/triggers disabled, and no active owner or unfinished probe.",
                    "CloudFormation change set must create only the named disabled function and version; no IAM, secret, log-group or permission changes.",
                    "After creation read back version 1, exact code SHA, role, runtime, handler, timeout, memory, logging and reserved concurrency 0. Any mismatch holds."]},
            "invoke_once": {
                "preconditions": [
                    "Requires a second explicit approval after disabled creation readbacks; creation approval never authorizes invocation.",
                    "Recheck exact version 1/code/bindings/role ID, approval validity window, disabled production and no active owner.",
                    "Verify the complete case fixture inventory and readback evidence immediately before invocation; missing prerequisites hold.",
                    "Use one serialized operator with durable intent/evidence recording. Concurrency 1 is not exactly-once authorization.",
                    "No asynchronous invocation, event source, function URL, SDK retries, log tail or additional payload fields."],
                "sdk": {"retries": {"total_max_attempts": 1}, "connect_timeout": 5, "read_timeout": seconds + 10},
                "requests_in_order": [
                    {"operation": "PutFunctionConcurrency", "parameters": {"FunctionName": name, "ReservedConcurrentExecutions": 1}},
                    {"operation": "Invoke", "parameters": {"FunctionName": name, "Qualifier": "1", "InvocationType": "RequestResponse", "LogType": "None", "Payload": "{}"}},
                    {"operation": "PutFunctionConcurrency", "parameters": {"FunctionName": name, "ReservedConcurrentExecutions": 0}}],
                "failure_policy": "Restore concurrency 0 and read it back on every exit. A timeout, lost response, FunctionError, hold or missing response is uncertain: preserve evidence and reconcile before any retry or deletion. Never automatically replay.",
                "success_policy": "A returned observation is not live acceptance; independently reconcile required case results and production-disabled/no-worker readbacks."},
            "delete_probe": {
                "preconditions": [
                    "Separate deletion approval binds exact function identity/version/code/bindings, resources and retained evidence.",
                    "Read back concurrency 0; reconcile any in-flight or uncertain invocation before deleting.",
                    "Retain the package, receipts, result and cloud readbacks; verify no active owner and production still disabled.",
                    "The template retains both resources on stack deletion. Delete only this exact probe after reconciliation; fixture cleanup needs its own exact object/version/key approval. Never delete shared role, secret, log group or production resources."],
                "request": {"operation": "DeleteFunction", "parameters": {"FunctionName": name}}}})
    return template, plan


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("package", help="build and verify locally; no AWS calls")
    for name in ("bindings", "output", "receipt", "wheels", "python312"):
        build.add_argument("--" + name, type=Path, required=True)
    render = commands.add_parser("render", help="render a disabled template and approval plan; no AWS calls")
    for name in ("bindings", "archive", "receipt", "template", "plan", "wheels", "python312"):
        render.add_argument("--" + name, type=Path, required=True)
    for name in ("code-bucket", "code-key", "code-version"):
        render.add_argument("--" + name, required=True)
    args = parser.parse_args(argv)
    bindings = parse_bindings(ordinary_input(args.bindings, limit=16384))
    if args.command == "package":
        receipt = build_package(args.output, args.receipt, bindings, args.wheels, args.python312)
        print(json.dumps({key: receipt[key] for key in ("archive_bytes", "archive_sha256", "bindings_sha256")}, sort_keys=True))
    else:
        new_outputs(args.template, args.plan)
        receipt = checked_artifact(args.archive, args.receipt, bindings, args.wheels, args.python312)
        template, plan = deployment_proposal(bindings, receipt, args.code_bucket, args.code_key, args.code_version)
        write_new(args.template, encoded(template))
        write_new(args.plan, encoded(plan))
        print(json.dumps({"status": "proposal-only", "live_accepted": False}, sort_keys=True))


if __name__ == "__main__":
    main()
