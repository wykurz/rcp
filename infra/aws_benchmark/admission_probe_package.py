"""Offline admission-probe packaging and disabled function proposal; no AWS operations.

The existing production/acceptance builders supply pinned wheels, held file
snapshots, exclusive output publication and independent opaque-artifact rebuilds.
This separate contract cannot relabel an older storage probe as admission proof.
"""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import zipfile

from benchmarks.strict_json import parse_json
from . import acceptance_package as acceptance
from . import production_package as production
from . import admission_probe as runtime
from .admission_acceptance import PHASES

ROOT = production.ROOT
SOURCES = acceptance.SOURCES + ("infra/aws_benchmark/admission_acceptance.py",
                               "infra/aws_benchmark/admission_probe.py")
BUILDERS = acceptance.BUILDERS + ("infra/aws_benchmark/admission_probe_package.py",)
BINDING_FILE = "admission-bindings.json"
RECEIPT_SCHEMA = "rcp-isolated-admission-package-v1"


def reviewed_sources(bindings):
    sources, requirements, builders = acceptance.reviewed_sources(bindings)
    env = {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C", "GIT_CONFIG_NOSYSTEM": "1",
           "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_NO_REPLACE_OBJECTS": "1"}
    for name in (*SOURCES[len(acceptance.SOURCES):], BUILDERS[-1]):
        raw = production.ordinary_bytes(ROOT, name)
        try:
            tracked = subprocess.run(["/usr/bin/git", "--no-pager", "-C", str(ROOT), "show",
                bindings["source_commit"] + ":" + name], env=env,
                capture_output=True, check=True, timeout=30).stdout
        except subprocess.CalledProcessError as error:
            raise ValueError("admission package input must be tracked at declared HEAD") from error
        if raw != tracked:
            raise ValueError("admission package input differs from declared HEAD")
        (builders if name == BUILDERS[-1] else sources)[name] = raw
    production.verify_source_closure(sources)
    return sources, requirements, builders


PROBE = r"""
import importlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1]).resolve()
attempts = []
def deny_network(event, args):
    if event.startswith('socket.'):
        attempts.append(event)
        raise RuntimeError('network blocked during admission package verification')
sys.addaudithook(deny_network)
sys.path.insert(0, str(root))
for name in ('admission_acceptance', 'admission_probe'):
    module = importlib.import_module('infra.aws_benchmark.' + name)
    if not pathlib.Path(module.__file__).resolve().is_relative_to(root):
        raise RuntimeError('admission module imported outside package')
from infra.aws_benchmark.admission_probe import parse_bindings, digest_bindings, lambda_handler
config = parse_bindings((root / 'admission-bindings.json').read_bytes())
if digest_bindings(config) != sys.argv[2]:
    raise RuntimeError('embedded admission bindings changed')
result = lambda_handler({'phase': 'NOT-AN-APPROVED-PHASE'}, None)
if result.get('ok') is not False or result.get('live_accepted') is not False or attempts:
    raise RuntimeError('invalid phase did not hold before network')
print(json.dumps({'admission_imports_verified': True, 'invalid_phase_blocked': True,
    'bindings_sha256': digest_bindings(config), 'network_attempts': 0}, sort_keys=True))
"""


def verify_package(archive, python312, directory, bindings):
    verified = production.verify_package(archive, python312, directory)
    result = subprocess.run([str(Path(python312).absolute()), "-I", "-S", "-B", "-c", PROBE,
        str(Path(directory) / "verify"), runtime.digest_bindings(bindings)], cwd=directory,
        text=True, capture_output=True, timeout=60, check=True)
    return dict(production=verified, admission=json.loads(result.stdout))


def build_package(output, receipt_path, bindings, wheels, python312):
    bindings = runtime.validate_bindings(bindings)
    acceptance.new_outputs(output, receipt_path)
    sources, requirements, builders = reviewed_sources(bindings)
    wheels = acceptance.wheel_inputs(wheels)
    python312 = Path(python312).absolute()
    with tempfile.TemporaryDirectory(prefix="rcp-admission-package-") as directory:
        work = Path(directory)
        staged = work / "wheels"
        staged.mkdir()
        for name, raw in wheels.items():
            (staged / name).write_bytes(raw)
        base = work / "production.zip"
        base_receipt = production.package(base, staged, python312)
        with zipfile.ZipFile(base) as archive:
            members = {info.filename: archive.read(info) for info in archive.infolist()}
        if any(members.get(name) != sources[name] for name in production.SOURCES):
            raise ValueError("production source changed during admission build")
        if base_receipt["requirements_sha256"] != acceptance.hashes({Path(k).name: v for k, v in requirements.items()}):
            raise ValueError("requirements changed during admission build")
        if base_receipt["wheel_sha256"] != acceptance.hashes(wheels):
            raise ValueError("wheel identity changed during admission build")
        if any(name in members for name in (*SOURCES, BINDING_FILE)):
            raise ValueError("admission source collides with a dependency")
        members.update({name: sources[name] for name in SOURCES})
        members[BINDING_FILE] = runtime.binding_bytes(bindings)
        prepared = work / "admission.zip"
        production.archive_members(prepared, members)
        if prepared.stat().st_size > acceptance.CODE_LIMIT:
            raise ValueError("admission package exceeds the existing 32 MiB allowance")
        verification_dir = work / "verification"
        verification_dir.mkdir()
        verification = verify_package(prepared, python312, verification_dir, bindings)
        content = prepared.read_bytes()
    receipt = dict(schema=RECEIPT_SCHEMA, provisional=True, live_accepted=False,
        source_commit=bindings["source_commit"], bindings_sha256=runtime.digest_bindings(bindings),
        archive_bytes=len(content), archive_sha256=hashlib.sha256(content).hexdigest(),
        source_sha256=acceptance.hashes(sources), requirements_sha256=acceptance.hashes(requirements),
        builder_sha256=acceptance.hashes(builders), wheel_sha256=base_receipt["wheel_sha256"],
        code_limit_bytes=acceptance.CODE_LIMIT, within_existing_code_limit=True,
        compression=production.compression_toolchain(), verification=verification,
        provenance="local HEAD byte equality; remote review and live acceptance require separate evidence")
    acceptance.write_new(output, content)
    acceptance.write_new(receipt_path, acceptance.encoded(receipt))
    return receipt


def checked_artifact(archive_path, receipt_path, bindings, wheels, python312):
    bindings = runtime.validate_bindings(bindings)
    receipt = parse_json(acceptance.ordinary_input(receipt_path, limit=4 * 1024 * 1024).decode("utf-8"))
    content = acceptance.ordinary_input(archive_path, limit=acceptance.CODE_LIMIT)
    with tempfile.TemporaryDirectory(prefix="rcp-admission-rebuild-") as directory:
        work = Path(directory)
        rebuilt = build_package(work / "expected.zip", work / "expected-receipt.json", bindings, wheels, python312)
        if content != acceptance.ordinary_input(work / "expected.zip", limit=acceptance.CODE_LIMIT):
            raise ValueError("admission artifact differs from independent source/wheel rebuild")
    if acceptance.encoded(receipt) != acceptance.encoded(rebuilt):
        raise ValueError("admission receipt differs from independent rebuild")
    return rebuilt


def deployment_proposal(bindings, receipt, bucket, key, version):
    bindings = runtime.validate_bindings(bindings)
    if receipt.get("schema") != RECEIPT_SCHEMA or receipt.get("bindings_sha256") != runtime.digest_bindings(bindings):
        raise ValueError("admission receipt does not bind the configuration")
    digest = receipt.get("archive_sha256")
    if type(digest) is not str or len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError("invalid admission archive hash")
    code = acceptance.code_pin(bucket, key, version)
    template = dict(AWSTemplateFormatVersion="2010-09-09",
        Description="Disabled isolated admission probe; no fixture, role, trigger or production change",
        Metadata=dict(Status="proposal-only", SourceCommit=bindings["source_commit"],
            BindingsSha256=runtime.digest_bindings(bindings), ArchiveSha256=digest),
        Resources={
            "AdmissionProbe": dict(Type="AWS::Lambda::Function", DeletionPolicy="Retain",
                UpdateReplacePolicy="Retain", Properties=dict(FunctionName=bindings["function_name"],
                    Role=bindings["role_arn"], Runtime="python3.12", Architectures=["x86_64"],
                    Handler="infra.aws_benchmark.admission_probe.lambda_handler", Timeout=runtime.TIMEOUT,
                    MemorySize=256, ReservedConcurrentExecutions=0, Code=code,
                    LoggingConfig=dict(LogFormat="Text", LogGroup=bindings["log_group"]))),
            "AdmissionProbeVersion": dict(Type="AWS::Lambda::Version", DeletionPolicy="Retain",
                UpdateReplacePolicy="Retain", Properties=dict(FunctionName={"Ref": "AdmissionProbe"},
                    CodeSha256=base64.b64encode(bytes.fromhex(digest)).decode(),
                    Description="fresh function only; read back exact version 1 before invocation"))})
    plan = dict(schema="rcp-isolated-admission-plan-v1", status="proposal-only", live_accepted=False,
        bindings_sha256=runtime.digest_bindings(bindings), source_commit=bindings["source_commit"],
        template_sha256=hashlib.sha256(acceptance.encoded(template)).hexdigest(),
        code_sha256=base64.b64encode(bytes.fromhex(digest)).decode(), code_object=code,
        phases=list(PHASES), maximum_invocations=len(PHASES), timeout_seconds=runtime.TIMEOUT,
        payloads=[dict(phase=phase) for phase in PHASES],
        gates=[
            "Explicit bundled approval must cover exact fixture/role/code creation, bounded calls, cleanup and cost.",
            "Fresh target only; reconcile absence, account, region, role immutable ID, TableId and deletion protection.",
            "Read back qualified version 1 code SHA, handler, timeout, memory, exact role and concurrency zero.",
            "Production stays disabled and idle; this probe never invokes a production function.",
            "Complete paginated fixture inventory, preserve all one-shot claims, and seed only exact known keys.",
            "Serialize phases. Prior invocation must be terminal and reconciled before any operator reset.",
            "Record durable operator intent before each invocation; phase payload cannot supply resource or policy data.",
            "Enable probe concurrency one only for one qualified synchronous invocation, no SDK retries or LogTail.",
            "Restore concurrency zero and verify it in finally on every exit; retain uncertain intent and evidence.",
            "Never retry an uncertain or spent phase, clear a claim, refund a reservation or alter production ownership.",
            "Cleanup requires exact approved fixture identity, concurrency zero, terminal invocations and retained evidence.",
            "Deletion protection must be disabled only during approved final cleanup, never between phases.",
            "Delete only the new probe, matching stack records, fixture table/data, role and dedicated logs; retain code/evidence 30 days."])
    return template, plan


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("package", help="offline build; never uploads or deploys")
    for name in ("bindings", "output", "receipt", "wheels", "python312"):
        build.add_argument("--" + name, type=Path, required=True)
    render = sub.add_parser("render", help="offline rebuild and disabled proposal only")
    for name in ("bindings", "archive", "receipt", "wheels", "python312", "template", "plan"):
        render.add_argument("--" + name, type=Path, required=True)
    for name in ("code-bucket", "code-key", "code-version"):
        render.add_argument("--" + name, required=True)
    args = parser.parse_args(argv)
    config = runtime.parse_bindings(acceptance.ordinary_input(args.bindings, limit=runtime.MAX_BINDINGS))
    if args.command == "package":
        result = build_package(args.output, args.receipt, config, args.wheels, args.python312)
        print(json.dumps({key: result[key] for key in ("archive_sha256", "archive_bytes", "bindings_sha256")}, sort_keys=True))
    else:
        acceptance.new_outputs(args.template, args.plan)
        receipt = checked_artifact(args.archive, args.receipt, config, args.wheels, args.python312)
        template, plan = deployment_proposal(config, receipt, args.code_bucket, args.code_key, args.code_version)
        acceptance.write_new(args.template, acceptance.encoded(template))
        acceptance.write_new(args.plan, acceptance.encoded(plan))
        print(json.dumps(dict(status="proposal-only", live_accepted=False), sort_keys=True))


if __name__ == "__main__":
    main()
