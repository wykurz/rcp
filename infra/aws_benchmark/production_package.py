"""Build and verify a deterministic, offline CPython 3.12 production Lambda ZIP.

No upload, deployment or package-size allowance change occurs. Explicit reviewed
sources and pinned wheels are copied into private staging before installation.
The legacy durable package and its dependencies are unchanged. DEFLATE reproducibility
requires the same Python/zlib toolchain, recorded in the receipt; the exact ZIP digest
remains the immutable deployment pin.
"""

import argparse
import ast
import hashlib
import importlib.util
import json
import os
import platform
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import zipfile
import zlib

from .durable_package import package_files

ROOT = Path(__file__).resolve().parents[2]
CODE_LIMIT = 32 * 1024 * 1024
REQUIREMENTS = ("infra/aws_benchmark/requirements.txt", "infra/aws_benchmark/production-requirements.txt")
SOURCES = (
    "benchmarks/__init__.py", "benchmarks/capacity.py", "benchmarks/changes.py", "benchmarks/clocks.py",
    "benchmarks/measurements.py", "benchmarks/operations.py", "benchmarks/pairs.py", "benchmarks/report.py",
    "benchmarks/run.py", "benchmarks/sanitized.py", "benchmarks/signals.py", "benchmarks/site.py",
    "benchmarks/streaming.py", "benchmarks/strict_json.py", "benchmarks/timings.py", "benchmarks/transport.py",
    "benchmarks/verification.py", "infra/aws_benchmark/__init__.py", "infra/aws_benchmark/ami_image.py",
    "infra/aws_benchmark/common.py", "infra/aws_benchmark/controller.py", "infra/aws_benchmark/durable.py",
    "infra/aws_benchmark/durable_aws.py", "infra/aws_benchmark/durable_client.py", "infra/aws_benchmark/identity.py",
    "infra/aws_benchmark/json_bytes.py", "infra/aws_benchmark/production_admission.py",
    "infra/aws_benchmark/production_auth.py", "infra/aws_benchmark/production_collection.py",
    "infra/aws_benchmark/production_collection_handler.py", "infra/aws_benchmark/production_errors.py",
    "infra/aws_benchmark/production_github.py", "infra/aws_benchmark/production_github_app.py",
    "infra/aws_benchmark/production_handler.py",
    "infra/aws_benchmark/production_mailbox.py", "infra/aws_benchmark/production_objects.py",
    "infra/aws_benchmark/production_policy.py",
    "infra/aws_benchmark/production_projection.py",
    "infra/aws_benchmark/production_runtime.py", "infra/aws_benchmark/production_startup.py",
    "infra/aws_benchmark/production_store.py",
    "infra/aws_benchmark/results.py", "assets/site-theme.html",
)
ENTRYPOINTS = {"infra.aws_benchmark.production_handler": ("lambda_handler",),
              "infra.aws_benchmark.production_runtime": ("coordinator", "watchdog"),
              "infra.aws_benchmark.production_collection": ("Collector",),
              "infra.aws_benchmark.production_collection_handler": ("lambda_handler",)}
THIRD_PARTY = frozenset({"boto3", "botocore", "urllib3", "jwt", "cryptography", "cffi", "pycparser",
                       "dateutil", "s3transfer", "jmespath", "six"})


def ordinary_bytes(root, relative, *, limit=4 * 1024 * 1024):
    """Snapshot a regular input through held directory descriptors without links."""
    parts = Path(relative).parts
    if not parts or any(part in ("", ".", "..") for part in parts) or Path(relative).is_absolute():
        raise ValueError("invalid relative package input")
    held = []
    try:
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        held.append(directory)
        for part in parts[:-1]:
            directory = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            held.append(directory)
        descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(descriptor, "rb") as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
                raise ValueError("package input is not a bounded ordinary file")
            content = source.read(limit + 1)
            if len(content) > limit:
                raise ValueError("package input exceeds its bound")
            return content
    except OSError as error:
        raise ValueError("package input must be an ordinary file without linked parents") from error
    finally:
        for descriptor in reversed(held):
            os.close(descriptor)


def source_snapshot(root):
    return {name: ordinary_bytes(root, name) for name in SOURCES}


def _module(name):
    return name[:-3].replace("/", ".").removesuffix(".__init__")


def verify_source_closure(sources):
    """Reject a new repository dependency until it joins the reviewed allowlist."""
    modules = {_module(name): (name, data) for name, data in sources.items() if name.endswith(".py")}
    modules["infra"] = ("infra/__init__.py", b"")
    for root in ENTRYPOINTS:
        if root not in modules:
            raise ValueError("production entrypoint source is missing: " + root)
    for module, (name, raw) in modules.items():
        tree = ast.parse(raw.decode("utf-8"), filename=name)
        package = module if name.endswith("/__init__.py") else module.rpartition(".")[0]
        optional_operator_imports = {id(node) for function in tree.body
            if isinstance(function, ast.FunctionDef) and name.endswith("/ami_image.py") and
            function.name == "reviewed_startup" for node in ast.walk(function)}
        for node in ast.walk(tree):
            dependencies = []
            if isinstance(node, ast.Import):
                dependencies = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                base = importlib.util.resolve_name("." * node.level + (node.module or ""), package) \
                    if node.level else node.module
                if base:
                    dependencies = [base]
                    base_file = modules.get(base, ("", b""))[0]
                    if base_file.endswith("/__init__.py"):
                        dependencies += [base + "." + alias.name for alias in node.names]
                    else:
                        dependencies += [base + "." + alias.name for alias in node.names
                                         if base + "." + alias.name in modules]
            for dependency in dependencies:
                top = dependency.split(".")[0]
                if top in {"infra", "benchmarks"}:
                    if dependency not in modules:
                        raise ValueError("unreviewed or missing source dependency: " + dependency)
                elif top not in sys.stdlib_module_names and top not in THIRD_PARTY:
                    if top == "yaml" and id(node) in optional_operator_imports:
                        continue
                    raise ValueError("unlisted runtime dependency: " + dependency)


def wheel_snapshot(wheels):
    wheels = Path(wheels)
    if wheels.is_symlink() or not wheels.is_dir():
        raise ValueError("wheels must be an ordinary directory")
    names = sorted(path.name for path in wheels.iterdir() if path.name.endswith(".whl"))
    if not names:
        raise ValueError("the pinned offline wheel set is absent")
    return {name: ordinary_bytes(wheels, name, limit=64 * 1024 * 1024) for name in names}


def install_dependencies(stage, wheels, requirements, python312):
    # pip runs on the selected target interpreter and cannot access package indexes
    subprocess.run([sys.executable, "-m", "pip", "--python", str(python312), "--isolated", "install",
                    "--no-compile", "--require-hashes", "--no-index", "--find-links", str(wheels),
                    "--target", str(stage), "-r", str(requirements / "production-requirements.txt")],
                   check=True, capture_output=True, text=True, timeout=180)


S3_COPY_PROBE = r"""
import botocore
from pathlib import Path
from botocore.loaders import Loader
from botocore.session import get_session
from botocore.serialize import create_serializer
model_session = get_session()
model_session.register_component('data_loader', Loader(
    extra_search_paths=[str(Path(botocore.__file__).resolve().parent / 'data')], include_default_search_paths=False))
copy_operation = model_session.get_service_model('s3').operation_model('CopyObject')
annotation = copy_operation.input_shape.members.get('AnnotationDirective')
if annotation is None or 'EXCLUDE' not in annotation.enum:
    raise RuntimeError('packaged SDK lacks required CopyObject AnnotationDirective=EXCLUDE support')
copy_parameters = dict(Bucket='offline-package-probe', Key='destination', CopySource='/source/key?versionId=version-1',
                       AnnotationDirective='EXCLUDE', ChecksumAlgorithm='SHA256', ServerSideEncryption='AES256',
                       MetadataDirective='REPLACE', ContentType='application/octet-stream', IfNoneMatch='*')
copy_wire = create_serializer('rest-xml').serialize_to_request(copy_parameters, copy_operation)
if copy_wire['headers'].get('x-amz-object-annotation-directive') != 'EXCLUDE':
    raise RuntimeError('packaged SDK failed required CopyObject annotation exclusion wire contract')
"""


ISOLATED_PROBE = r"""
import importlib, json, pathlib, platform, socket, sys
root = pathlib.Path(sys.argv[1]).resolve()
if sys.version_info[:2] != (3, 12) or platform.system() != 'Linux' or platform.machine() != 'x86_64':
    raise RuntimeError('Lambda verification requires CPython 3.12 on Linux x86_64')
def deny_network(event, args):
    if event.startswith('socket.'):
        raise RuntimeError('network is blocked during package verification')
sys.addaudithook(deny_network)
try:
    socket.socket()
except RuntimeError:
    pass
else:
    raise RuntimeError('network audit guard did not reject socket creation')
sys.path.insert(0, str(root))
entrypoints = json.loads(sys.argv[2])
for name, attributes in entrypoints.items():
    module = importlib.import_module(name)
    if not pathlib.Path(module.__file__).resolve().is_relative_to(root):
        raise RuntimeError('handler imported outside the package')
    for attribute in attributes:
        if not callable(getattr(module, attribute, None)):
            raise RuntimeError('package entrypoint is absent')
for name in ('boto3', 'botocore', 'jwt', 'cryptography', 'cffi', '_cffi_backend', 'pycparser'):
    module = importlib.import_module(name)
    if not pathlib.Path(module.__file__).resolve().is_relative_to(root):
        raise RuntimeError('dependency imported outside the package')
""" + S3_COPY_PROBE + r"""
from cryptography.hazmat.primitives.asymmetric import rsa
key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
import jwt
encoded = jwt.encode({'package_test': True}, key, algorithm='RS256')
if jwt.decode(encoded, key.public_key(), algorithms=['RS256']) != {'package_test': True}:
    raise RuntimeError('native cryptography verification failed')
from benchmarks.site import read_theme
if not read_theme().strip():
    raise RuntimeError('report theme is absent')
for module in tuple(sys.modules.values()):
    file = getattr(module, '__file__', None)
    if file and ('site-packages' in pathlib.Path(file).parts or 'dist-packages' in pathlib.Path(file).parts):
        if not pathlib.Path(file).resolve().is_relative_to(root):
            raise RuntimeError('external installed package leaked into verification')
print(json.dumps({'python': platform.python_version(), 'platform': platform.system(),
                  'machine': platform.machine(), 'network_blocked': True,
                  'external_site_packages': False, 'entrypoints': entrypoints,
                  'native_crypto_verified': True, 'report_theme_verified': True,
                   'botocore_version': botocore.__version__, 's3_copy_annotation_exclude_verified': True}, sort_keys=True))
"""


def verify_package(output, python312, work):
    extraction = Path(work) / "verify"
    extraction.mkdir()
    with zipfile.ZipFile(output) as archive:
        for member in archive.infolist():
            path = Path(member.filename)
            if path.is_absolute() or ".." in path.parts or member.is_dir() or stat.S_ISLNK(member.external_attr >> 16):
                raise ValueError("unexpected archive member")
        archive.extractall(extraction)
    result = subprocess.run([str(python312), "-I", "-S", "-B", "-c", ISOLATED_PROBE, str(extraction),
                             json.dumps(ENTRYPOINTS, sort_keys=True)], cwd=work, text=True, capture_output=True,
                            timeout=60, check=True)
    return json.loads(result.stdout)


def compression_toolchain():
    return {"method": "DEFLATED", "level": 9, "python": platform.python_version(),
            "zlib_compile": zlib.ZLIB_VERSION, "zlib_runtime": zlib.ZLIB_RUNTIME_VERSION,
            "reproducibility": "byte reproducibility requires this same compression toolchain"}


def archive_members(output, members):
    with zipfile.ZipFile(output, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name, data in sorted(members.items()):
            info = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, data, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)


def package(output, wheels, python312):
    output = Path(output)
    # keep the caller's interpreter path when the probe changes cwd, without resolving venv links
    python312 = Path(python312).absolute()
    if output.exists() or output.is_symlink() or not output.parent.is_dir():
        raise ValueError("output must be new and its parent must already exist")
    sources = source_snapshot(ROOT)
    verify_source_closure(sources)
    requirements = {Path(name).name: ordinary_bytes(ROOT, name) for name in REQUIREMENTS}
    wheel_files = wheel_snapshot(wheels)
    with tempfile.TemporaryDirectory(prefix="rcp-production-package-", dir=output.parent) as directory:
        work = Path(directory)
        stage, wheel_stage, requirements_stage = (work / name for name in ("package", "wheels", "requirements"))
        for path in (stage, wheel_stage, requirements_stage):
            path.mkdir()
        for name, data in wheel_files.items():
            (wheel_stage / name).write_bytes(data)
        for name, data in requirements.items():
            (requirements_stage / name).write_bytes(data)
        install_dependencies(stage, wheel_stage, requirements_stage, python312)
        for name, data in sources.items():
            path = stage / name
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as destination:
                destination.write(data)
        (stage / "infra/__init__.py").write_bytes(b"")
        members = package_files(stage)
        prepared = work / "verified.zip"
        archive_members(prepared, members)
        if prepared.stat().st_size > CODE_LIMIT:
            raise ValueError("production package exceeds the existing 32 MiB allowance")
        verification = verify_package(prepared, python312, work)
        content = prepared.read_bytes()
        with output.open("xb") as destination:
            destination.write(content)
    hashes = lambda values: {name: hashlib.sha256(data).hexdigest() for name, data in sorted(values.items())}
    return {"schema_version": 1, "provisional": True, "compression": compression_toolchain(), "archive_bytes": len(content), "archive_sha256": hashlib.sha256(content).hexdigest(),
            "code_limit_bytes": CODE_LIMIT, "within_existing_code_limit": len(content) <= CODE_LIMIT,
            "source_sha256": hashes(sources), "requirements_sha256": hashes(requirements),
            "wheel_sha256": hashes(wheel_files), "verification": verification,
            "provenance": "local content hashes; published source provenance must be established separately"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wheels", type=Path, required=True)
    parser.add_argument("--python312", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    if args.receipt.exists() or args.receipt.is_symlink() or not args.receipt.parent.is_dir():
        raise ValueError("receipt must be new and its parent must already exist")
    result = package(args.output, args.wheels, args.python312)
    with args.receipt.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")
    print(json.dumps({key: result[key] for key in ("archive_bytes", "archive_sha256", "within_existing_code_limit")},
                     sort_keys=True))


if __name__ == "__main__":
    main()
