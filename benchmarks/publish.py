"""Append a terminal result, optionally recovering an interrupted producer's artifact."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

from benchmarks.report import parse_result, validate_result


def git(directory, *arguments, environment=None, check=True, input_text=None):
    try:
        completed = subprocess.run(
            ["git", "-C", str(directory), *arguments], env=environment, input=input_text,
            text=True, capture_output=True, timeout=60,
        )
    except subprocess.TimeoutExpired:
        raise ValueError(f"git {arguments[0]} timed out") from None
    if check and completed.returncode:
        # git diagnostics can contain credential-bearing URLs or helper output
        raise ValueError(f"git {arguments[0]} failed (exit {completed.returncode})")
    return completed


def publication_environment(directory):
    environment = os.environ.copy()
    auth = git(
        directory, "config", "--null", "--get-regexp",
        r"^(http|credential|url)\.|^core\.sshcommand$", check=False,
    )
    if auth.returncode not in (0, 1):
        raise ValueError("cannot read Git authentication configuration")
    entries = []
    for entry in filter(None, auth.stdout.split("\0")):
        key, separator, value = entry.partition("\n")
        entries.append([key, value if separator else "true"])
    for variable in git(directory, "rev-parse", "--local-env-vars").stdout.splitlines():
        environment.pop(variable, None)
    environment = {
        key: value for key, value in environment.items()
        if not key.startswith(("GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_"))
    }
    environment["GIT_CONFIG_GLOBAL"] = os.devnull
    environment["GIT_CONFIG_NOSYSTEM"] = "1"
    entries.extend([
        ["user.name", "RCP benchmark history"],
        ["user.email", "benchmark-history@users.noreply.github.com"],
        ["commit.gpgsign", "false"],
        ["core.hooksPath", os.devnull],
    ])
    count = 0
    for key, value in entries:
        environment[f"GIT_CONFIG_KEY_{count}"] = key
        environment[f"GIT_CONFIG_VALUE_{count}"] = value
        count += 1
    environment["GIT_CONFIG_COUNT"] = str(count)
    environment["GIT_TERMINAL_PROMPT"] = "0"
    return environment


def publish(result_path, repository, remote="origin", branch="benchmark-history", attempts=5, *, finalize_interrupted=False):
    """Append history; recovery requires the caller to assert the producer has ended."""
    record = parse_result(Path(result_path).read_text(encoding="utf-8"))
    if record["status"] == "running" and finalize_interrupted:
        interrupted = "Benchmark interrupted: producer ended before recording a terminal result."
        record["status"] = "failed"
        record["error"] = "; ".join(filter(None, [record.get("error"), interrupted]))
        record["context"]["publication_recovery"] = {
            "publisher": "benchmarks.publish", "original_status": "running", "producer_ended_asserted": True,
        }
        for trial in record["trials"]:
            if trial["status"] == "running":
                trial["status"] = "failed"
                trial["validation"].update({"ok": False, "error": interrupted})
        validate_result(record)
    if record["status"] not in {"complete", "failed"}:
        raise ValueError("status must be complete or failed before publishing a benchmark result")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("repository must be OWNER/REPO")
    if attempts < 1 or attempts > 20:
        raise ValueError("attempts must be between 1 and 20")
    if branch in {"main", "master"} or branch.startswith("-"):
        raise ValueError("publication requires a dedicated history branch")
    current = Path.cwd()
    reference = f"refs/heads/{branch}"
    git(current, "check-ref-format", reference)
    environment = publication_environment(current)
    resolved = git(current, "remote", "get-url", "--push", "--", remote, check=False)
    destination = resolved.stdout.strip() if resolved.returncode == 0 else remote
    if not destination or destination.startswith("-"):
        raise ValueError("remote must name a Git remote or URL")
    if Path(destination).exists():
        destination = str(Path(destination).resolve())
    serialized = json.dumps(record, sort_keys=True, indent=2, allow_nan=False) + "\n"
    relative = f"runs/{record['run_id']}.json"
    with tempfile.TemporaryDirectory(prefix="rcp-benchmark-history-") as temporary:
        directory = Path(temporary)
        git(directory, "init", "--quiet", environment=environment)
        for _ in range(attempts):
            listed = git(directory, "ls-remote", "--heads", destination, reference, environment=environment)
            if listed.stdout.strip():
                git(directory, "fetch", "--quiet", "--no-tags", "--depth", "1", destination, reference, environment=environment)
                parent = git(directory, "rev-parse", "FETCH_HEAD", environment=environment).stdout.strip()
                git(directory, "read-tree", parent, environment=environment)
                existing = git(directory, "show", f"{parent}:{relative}", environment=environment, check=False)
                if existing.returncode == 0:
                    if parse_result(existing.stdout) != record:
                        raise ValueError(f"conflicting result already exists for run_id {record['run_id']}")
                    return record["run_id"], parent, False
            else:
                parent = None
                git(directory, "read-tree", "--empty", environment=environment)
            # update the index without checking out history-controlled paths or running hooks
            blob = git(directory, "hash-object", "-w", "--stdin", environment=environment, input_text=serialized).stdout.strip()
            git(directory, "update-index", "--add", "--cacheinfo", "100644", blob, relative, environment=environment)
            tree = git(directory, "write-tree", environment=environment).stdout.strip()
            parents = ["-p", parent] if parent else []
            commit = git(
                directory, "commit-tree", tree, *parents, "-m", f"Record benchmark {record['run_id']}",
                environment=environment,
            ).stdout.strip()
            pushed = git(directory, "push", "--quiet", destination, f"{commit}:{reference}", environment=environment, check=False)
            if pushed.returncode == 0:
                return record["run_id"], commit, True
        raise ValueError(f"history push failed after {attempts} attempts; check access or concurrent publishers")


def main(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--branch", default="benchmark-history")
    parser.add_argument("--attempts", default=5, type=int)
    parser.add_argument(
        "--finalize-interrupted", action="store_true",
        help="assert the producer has ended and publish a running artifact as an interrupted failure",
    )
    arguments = parser.parse_args(arguments)
    try:
        run_id, commit, created = publish(
            arguments.result, arguments.repository, arguments.remote, arguments.branch, arguments.attempts,
            finalize_interrupted=arguments.finalize_interrupted,
        )
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"benchmark publication failed: {error}", file=sys.stderr)
        return 1
    state = "published" if created else "already published"
    print(f"{state} {run_id} to {arguments.repository}:{arguments.branch} at {commit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
