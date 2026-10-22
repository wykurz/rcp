"""Build a deterministic Lambda ZIP from reviewed source and a predownloaded wheel set.

No upload or deployment occurs. The package bundles the pinned SDK instead of relying
on the mutable SDK shipped in the Lambda runtime. No repository-local configuration,
credentials, worktree metadata or candidate binaries are included.
"""

import argparse
import base64
import csv
import io
import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile
import zipfile


ROOT = Path(__file__).resolve().parents[2]


# package only the handler import closure, never arbitrary local Python configuration
SOURCES = ("infra/aws_benchmark/__init__.py",
           "infra/aws_benchmark/common.py", "infra/aws_benchmark/durable.py",
           "infra/aws_benchmark/durable_aws.py", "infra/aws_benchmark/ami_image.py",
           "benchmarks/__init__.py",
           "benchmarks/verification.py", "benchmarks/signals.py")


def source_files(root):
    for name in SOURCES:
        path = root / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("package source must be an ordinary reviewed file")
        yield path


def install_dependencies(stage, wheels):
    subprocess.run([sys.executable, "-m", "pip", "--isolated", "install", "--no-compile",
                    "--require-hashes", "--no-index", "--find-links", str(wheels), "--target", str(stage),
                    "-r", str(ROOT / "infra/aws_benchmark/requirements.txt")], check=True)


def package_files(stage):
    files = {}
    for path in sorted(stage.rglob("*")):
        if path.is_symlink():
            raise ValueError("SDK package contains a symlink")
        if not path.is_file():
            continue
        relative = path.relative_to(stage)
        if "__pycache__" in relative.parts or relative.parts[0] == "bin":
            continue
        files[relative.as_posix()] = path.read_bytes()
    # pip records interpreter-specific hashes for rewritten scripts which we do not
    # ship; regenerate RECORD from the retained members, never from excluded scripts
    for name in sorted(files):
        if not name.endswith(".dist-info/RECORD"):
            continue
        rows = []
        for row in csv.reader(io.StringIO(files[name].decode("utf-8"))):
            if len(row) != 3:
                raise ValueError("malformed installed wheel RECORD")
            member = row[0]
            if member not in files or member == name:
                continue
            content = files[member]
            digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(b"=").decode()
            rows.append((member, "sha256=" + digest, str(len(content))))
        rows.append((name, "", ""))
        normalized = io.StringIO(newline="")
        csv.writer(normalized, lineterminator="\n").writerows(sorted(rows))
        files[name] = normalized.getvalue().encode()
    return files


def package(output, wheels):
    output, wheels = Path(output), Path(wheels).resolve(strict=True)
    if output.exists() or not output.parent.is_dir():
        raise ValueError("output must be new and its parent must already exist")
    with tempfile.TemporaryDirectory(prefix="rcp-lambda-", dir=output.parent) as directory:
        stage = Path(directory)
        install_dependencies(stage, wheels)
        for path in source_files(ROOT):
            destination = stage / path.relative_to(ROOT)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(path.read_bytes())
        # make the repository namespace importable directly from the Lambda ZIP
        (stage / "infra/__init__.py").write_text("", encoding="utf-8")
        members = package_files(stage)
        # stored members avoid compressor-version-dependent bytes within the 32 MiB code cap
        with zipfile.ZipFile(output, "x", compression=zipfile.ZIP_STORED) as archive:
            for name, data in members.items():
                info = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_STORED
                info.external_attr = 0o100644 << 16
                archive.writestr(info, data)
    print("sha256=" + hashlib.sha256(output.read_bytes()).hexdigest())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wheels", type=Path, required=True)
    args = parser.parse_args()
    package(args.output, args.wheels)


if __name__ == "__main__":
    main()
