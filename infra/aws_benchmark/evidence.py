"""Operator only: download one run's retained evidence and check it against its retention manifest.

Read-only. Every run keeps its worker archive, status, collection report and summary under
production/runs/<run key>/collection/ for 90 days. Files are saved by their fixed names into a
new directory; the archive is never unpacked and nothing in it is executed. What a worker wrote
is untrusted data.
"""

import argparse
import os
from pathlib import Path
import re
import shutil

from . import results
from .common import MAX_RESULT, digest, error_code, require
from .production_collection import MAX_DOCUMENT, MAX_REPORT, SCHEMA
from .production_store import MAX_STATE_BYTES

RUN_KEY = re.compile(r"repo-[1-9][0-9]{0,19}-run-[1-9][0-9]{0,19}")
# the complete set of names a collected run may retain, with the collector's own size bounds
LIMITS = {"durable.json": MAX_STATE_BYTES, "status.json": MAX_DOCUMENT, "results.zip": MAX_RESULT,
          "controller.json": MAX_DOCUMENT, "report.json": MAX_REPORT, "summary.html": MAX_REPORT}
REQUIRED = frozenset({"durable.json", "controller.json", "report.json", "summary.html"})


def read(s3, bucket, key, limit, *, version=None):
    request = dict(Bucket=bucket, Key=key)
    if version is not None:
        request["VersionId"] = version
    body = s3.get_object(**request)["Body"]
    try:
        data = body.read(limit + 1)
    finally:
        body.close()
    require(0 < len(data) <= limit, "retained object is empty or exceeds its byte limit")
    return data


def save(directory, name, data):
    # evidence is not public: only its owner may read what is saved
    descriptor = os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(data)


def fetch(s3, bucket, run_key, directory):
    """Save the retained files of one run into a new directory; return (manifest, {name: sha256})."""
    require(isinstance(run_key, str) and RUN_KEY.fullmatch(run_key), "invalid run key")
    directory = Path(directory)
    # a new directory of its own, inside one that exists: downloaded bytes never land next to
    # anything else, and a failure leaves nothing behind
    directory.mkdir(mode=0o700)
    try:
        prefix = "production/runs/" + run_key + "/collection/"
        try:
            raw = read(s3, bucket, prefix + "manifest.json", MAX_DOCUMENT)
        except Exception as error:
            if error_code(error) not in ("NoSuchKey", "404"):
                raise
            raise ValueError("no retained evidence for this run: it was not collected, or its 90 days have passed") from None
        manifest = results.json_value(raw)
        retained = manifest.get("retained") if isinstance(manifest, dict) else None
        require(isinstance(retained, dict) and manifest.get("schema") == SCHEMA and manifest.get("run_key") == run_key
                and REQUIRED <= set(retained) <= set(LIMITS), "retention manifest differs from a collected run")
        save(directory, "manifest.json", raw)
        saved = {"manifest.json": digest(raw)}
        for name in sorted(retained):
            ref = retained[name]
            require(isinstance(ref, dict) and set(ref) == {"key", "version", "sha256", "bytes"}
                    and ref["key"] == prefix + name and isinstance(ref["version"], str)
                    and ref["version"] not in ("", "null")
                    and isinstance(ref["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", ref["sha256"])
                    and type(ref["bytes"]) is int and 0 < ref["bytes"] <= LIMITS[name], "retained reference differs")
            # the exact version the collector retained, never whatever is newest under the name
            data = read(s3, bucket, ref["key"], LIMITS[name], version=ref["version"])
            require(len(data) == ref["bytes"] and digest(data) == ref["sha256"],
                    "downloaded evidence differs from its retention manifest")
            save(directory, name, data)
            saved[name] = ref["sha256"]
        return manifest, saved
    except BaseException:
        # nothing unverified is left behind, and the same directory name can be used again
        shutil.rmtree(directory, ignore_errors=True)
        raise


def summary(manifest, saved, directory):
    """Say what was saved and where a reader starts; values come from the verified files."""
    lines = ["Saved " + str(len(saved)) + " files to " + str(directory) + ":"]
    lines += ["  " + name + "  sha256 " + sha256 for name, sha256 in sorted(saved.items())]
    try:
        report = results.json_value((Path(directory) / "report.json").read_bytes(), MAX_REPORT)
        outcome = report.get("qualification_outcome") if isinstance(report, dict) else None
    except (OSError, ValueError):
        outcome = None
    lines.append("qualification: " + (outcome if outcome in ("passed", "failed", "evidence-unavailable") else "unknown"))
    lines += ["Every file matches the size and SHA-256 that manifest.json records for it. The manifest's own",
              "digest is the retirement_sha256 in the run's public record, when the run has been retired.",
              "Start with summary.html (the collector's own summary) and report.json (every check and its reason).",
              "results.zip holds the worker's results and logs under benchmark/results/." if "results.zip" in saved
              else "This run retained no worker archive.",
              "Worker-written files are untrusted data: list or read them, never run anything from them."]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_key", help="for example repo-655073755-run-43075583972, as shown on the run page")
    parser.add_argument("directory", type=Path, help="a directory that does not exist yet")
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()
    require(re.fullmatch(r"[a-z]{2}-[a-z]+-[1-9][0-9]?", args.region), "invalid region")
    require(re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", args.bucket), "invalid bucket")
    import boto3
    from botocore.config import Config
    s3 = boto3.Session(region_name=args.region).client("s3", config=Config(
        connect_timeout=5, read_timeout=60, retries={"total_max_attempts": 3}))
    manifest, saved = fetch(s3, args.bucket, args.run_key, args.directory)
    print(summary(manifest, saved, args.directory))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # the operator's own terminal: say what went wrong without a traceback
        raise SystemExit("Evidence download failed (" + type(error).__name__ + "): " + str(error)) from None
