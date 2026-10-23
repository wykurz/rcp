"""Bounded read-only download, offline export and append-only history preparation.

Each job checks a complete file-inventory digest supplied by its trusted predecessor,
not an embedded claim or an artifact name. Only download imports an AWS client. The
writer consumes public JSON, never raw archives or retained HTML; it prepares a commit
without a checkout. The workflow pushes that exact commit without force and reconciles
uncertain pushes by fetching again. No phase launches or retires a worker.
"""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import re
import selectors
import subprocess
import stat
import tempfile
import time

from benchmarks.publish import git
from . import production_bundle as codec, production_collection as collection
from . import production_export as public, production_publication as publication
from .common import Budget, digest, require

REPOSITORY_ID = "655073755"
HISTORY_REF = "refs/heads/aws-benchmark-history"
DOWNLOAD_SCHEMA = "rcp-production-download-index-v1"
PUBLIC_SCHEMA = "rcp-production-public-batch-v1"
MAX_TOTAL = 512 * 1024 * 1024
MAX_HISTORY = 64 * 1024 * 1024
MAX_HISTORY_RUNS = 2000
MAX_INDEX = 1024 * 1024
MAX_SECONDS = 600
RUN = re.compile(r"repo-" + REPOSITORY_ID + r"-run-[1-9][0-9]{0,19}")


def encoded(value):
    return collection.encoded(value)


def read(path, limit):
    return collection.results.read_file(Path(path), limit, single_link=False)


def git_env():
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1", GIT_TERMINAL_PROMPT="0",
        GIT_AUTHOR_NAME="RCP AWS benchmark history", GIT_AUTHOR_EMAIL="benchmark-history@users.noreply.github.com",
        GIT_COMMITTER_NAME="RCP AWS benchmark history", GIT_COMMITTER_EMAIL="benchmark-history@users.noreply.github.com",
        GIT_CONFIG_COUNT="2", GIT_CONFIG_KEY_0="core.hooksPath", GIT_CONFIG_VALUE_0=os.devnull,
        GIT_CONFIG_KEY_1="commit.gpgsign", GIT_CONFIG_VALUE_1="false")
    return env


def git_bytes(repository, *args, limit):
    """Read fixed local Git objects with a byte/time cap and no diagnostic disclosure."""
    process = subprocess.Popen(["git", "-C", str(repository), *args], env=git_env(),
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    deadline, data = time.monotonic() + 30, bytearray()
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                remaining = deadline - time.monotonic()
                require(remaining > 0, "history read exceeded time bound")
                require(selector.select(remaining), "history read exceeded time bound")
                block = os.read(process.stdout.fileno(), min(65536, limit + 1 - len(data)))
                if not block:
                    break
                data.extend(block)
                require(len(data) <= limit, "history read exceeded byte bound")
        require(process.wait(timeout=max(0.01, deadline - time.monotonic())) == 0, "history object read failed")
        return bytes(data)
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        process.stdout.close()


def history(repository):
    """Read trusted prior exporter records from fixed Git objects, never checked-out paths."""
    env = git_env()
    require(git(repository, "rev-parse", "--is-bare-repository", environment=env).stdout.strip() == "true",
            "history must be a bare repository")
    exists = git(repository, "show-ref", "--verify", "--quiet", HISTORY_REF, environment=env, check=False)
    require(exists.returncode in (0, 1), "history reference cannot be read")
    if exists.returncode == 1:
        return {}, None
    parent = git(repository, "rev-parse", "--verify", HISTORY_REF + "^{commit}", environment=env).stdout.strip()
    require(re.fullmatch(r"[0-9a-f]{40}", parent), "invalid history parent")
    listing = git_bytes(repository, "ls-tree", "-r", "-z", "--full-tree", parent, limit=MAX_INDEX).decode("utf-8")
    entries = listing.split("\0")
    require(entries[-1] == "" and len(entries) <= MAX_HISTORY_RUNS + 1, "history entry count exceeds bound")
    records, total = {}, 0
    for entry in entries[:-1]:
        header, separator, path = entry.partition("\t")
        bits = header.split(" ")
        require(separator and len(bits) == 3 and bits[:2] == ["100644", "blob"] and
                re.fullmatch(r"[0-9a-f]{40}", bits[2]) and path.startswith("records/") and path.endswith(".json"),
                "history contains unexpected paths or modes")
        run_key = path[len("records/"):-len(".json")]
        require(RUN.fullmatch(run_key) and run_key not in records, "invalid history run path")
        size = git(repository, "cat-file", "-s", bits[2], environment=env).stdout.strip()
        require(re.fullmatch(r"[1-9][0-9]*", size) and int(size) <= public.MAX_PUBLIC_RECORD,
                "history record exceeds bound")
        total += int(size)
        require(total <= MAX_HISTORY, "history byte count exceeds bound")
        raw = git_bytes(repository, "cat-file", "blob", bits[2], limit=int(size))
        require(len(raw) == int(size), "history blob changed")
        value = public.validate_public(collection.results.json_value(raw, public.MAX_PUBLIC_RECORD))
        require(value["run_key"] == run_key, "history identity differs from path")
        records[run_key] = raw
    return records, parent


def inventory(root, *, limit=MAX_TOTAL):
    """Hash a bounded artifact tree in a job-owned directory without concurrent writers.

    Reject observed symlinks/special files and bind each file read to its descriptor.
    Directory traversal uses pathnames: a hostile concurrent directory replacement is
    outside this contract, so this is not a general TOCTOU-safe filesystem snapshot.
    The separately trusted predecessor digest supplies the cross-job byte binding.
    """
    root = Path(root)
    require(stat.S_ISDIR(root.lstat().st_mode), "artifact root must be a real directory")
    rows, total, directories, pending = [], 0, 0, [root]
    while pending:
        directory = pending.pop()
        directories += 1
        require(directories <= publication.MAX_RUNS + 2, "artifact has too many directories")
        with os.scandir(directory) as entries:
            for entry in entries:
                path = Path(entry.path)
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    require(directories + len(pending) < publication.MAX_RUNS + 2,
                            "artifact has too many directories")
                    pending.append(path)
                    continue
                require(stat.S_ISREG(info.st_mode), "artifact has a symlink or special file")
                relative = path.relative_to(root).as_posix()
                require(len(relative) <= 256 and re.fullmatch(r"[A-Za-z0-9_./-]+", relative), "invalid artifact path")
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(descriptor, "rb") as stream:
                    info = os.fstat(stream.fileno())
                    require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= publication.MAX_BUNDLE_BYTES,
                            "invalid artifact file")
                    total += info.st_size
                    require(total <= limit and len(rows) < publication.MAX_RUNS * 9 + 2, "artifact exceeds total bound")
                    hashed, count = hashlib.sha256(), 0
                    while block := stream.read(min(1024 * 1024, info.st_size + 1 - count)):
                        count += len(block)
                        require(count <= info.st_size, "artifact changed while reading")
                        hashed.update(block)
                    after = os.fstat(stream.fileno())
                    require(count == info.st_size and (after.st_size, after.st_mtime_ns, after.st_ctime_ns) ==
                            (info.st_size, info.st_mtime_ns, info.st_ctime_ns), "artifact changed while reading")
                rows.append(dict(path=relative, bytes=count, sha256=hashed.hexdigest()))
    require(rows, "empty artifact inventory")
    return digest(encoded(sorted(rows, key=lambda row: row["path"])))


def verify_inventory(root, expected, *, limit=MAX_TOTAL):
    require(isinstance(expected, str) and re.fullmatch(r"[0-9a-f]{64}", expected) and
            inventory(root, limit=limit) == expected, "artifact inventory differs from trusted predecessor")


def persist(path, value):
    raw = encoded(value)
    require(len(raw) <= MAX_INDEX, "publication index exceeds bound")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw)


def observation(value):
    require(isinstance(value, dict) and value.get("state") in {"idle", "pending", "held", "ready", "published", "deferred"}
            and type(value.get("retired")) is bool and
            set(value) <= {"state", "retired", "run_key", "phase", "reason"}, "invalid publication observation")
    if "run_key" in value:
        require(isinstance(value["run_key"], str) and RUN.fullmatch(value["run_key"]), "invalid observed run")
    for key in ("phase", "reason"):
        if key in value:
            require(isinstance(value[key], str) and len(value[key]) <= 512, "unbounded publication observation")
    return value


def download(reader, output, prior, reader_revision, *, clock=time.monotonic):
    codec.revision(reader_revision)
    started, total, observations, bundles = clock(), 0, [], []
    output = Path(output)
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    try:
        active = reader.active_observation()
    except Exception as error:
        active = dict(state="held", retired=False, reason=collection.results.diagnostic(error))
    keys = reader.discover()
    for key in keys:
        run_key = reader.run_key(key)
        if clock() - started >= MAX_SECONDS or total + publication.MAX_BUNDLE_BYTES + codec.MAX_ENVELOPE + MAX_INDEX > MAX_TOTAL:
            observations.append(dict(run_key=run_key, state="deferred", retired=False,
                reason="bounded batch exhausted; rescan after publishing completed records"))
            continue
        retired = False
        try:
            record, _ = reader.record(run_key)
            retired = record.closed
            if record.closed and run_key in prior:
                saved = public.validate_public(collection.results.json_value(prior[run_key]))
                require(saved["retirement_sha256"] == record.retirement_sha256,
                        "published source conflicts with retirement record")
                observations.append(dict(run_key=run_key, state="published", retired=True))
                continue
            value = reader.observe(key)
        except Exception as error:
            observations.append(dict(run_key=run_key, state="held", retired=retired,
                reason=collection.results.diagnostic(error)))
            continue
        if value["state"] == "ready":
            bundle = value.pop("bundle")
            total += sum(map(len, bundle.payloads.values())) + len(bundle.manifest_bytes) + codec.MAX_ENVELOPE
            require(total + MAX_INDEX <= MAX_TOTAL, "download exceeds aggregate byte bound")
            codec.save(bundle, output / run_key, reader_revision)
            bundles.append(run_key)
        observations.append(observation(dict(value, run_key=run_key)))
    index = dict(schema=DOWNLOAD_SCHEMA, repository_id=REPOSITORY_ID, reader_revision=reader_revision,
        observed_at=datetime.now(timezone.utc).isoformat(), discovery_complete=True,
        processing_complete=not any(item["state"] == "deferred" for item in observations), bundles=sorted(bundles),
        active=observation(active), observations=observations)
    persist(output / "index.json", index)
    return inventory(output)


def index_at(root, schema, revision):
    value = collection.results.json_value(read(Path(root) / "index.json", MAX_INDEX), MAX_INDEX)
    require(isinstance(value, dict) and value.get("schema") == schema and
            value.get("repository_id") == REPOSITORY_ID and value.get("reader_revision") == codec.revision(revision),
            "publication batch identity differs")
    common = {"schema", "repository_id", "reader_revision", "observed_at", "active", "observations"}
    require(set(value) == common | ({"discovery_complete", "processing_complete", "bundles"} if schema == DOWNLOAD_SCHEMA else {"records"}),
            "unexpected publication batch fields")
    collection.results.utc(value["observed_at"])
    observation(value["active"])
    require(isinstance(value["observations"], list) and len(value["observations"]) <= publication.MAX_RUNS,
            "publication observations exceed bound")
    for item in value["observations"]:
        observation(item)
    return value


def export(source, prior, output, revision, expected, *, clock=time.monotonic):
    started = clock()
    verify_inventory(source, expected)
    source, output = Path(source), Path(output)
    index = index_at(source, DOWNLOAD_SCHEMA, revision)
    names = index["bundles"]
    require(index["discovery_complete"] is True and type(index["processing_complete"]) is bool and
            index["processing_complete"] == (not any(item["state"] == "deferred" for item in index["observations"])) and
            isinstance(names, list) and names == sorted(set(names)) and
            all(isinstance(name, str) and RUN.fullmatch(name) for name in names), "invalid download inventory")
    observed = [item.get("run_key") for item in index["observations"]]
    require(len(observed) == len(set(observed)) and None not in observed and
            set(names) == {item["run_key"] for item in index["observations"] if item["state"] == "ready"} and
            all(item["retired"] is True for item in index["observations"] if item["state"] == "ready"),
            "download bundle observations differ")
    require({p.name for p in source.iterdir()} == set(names) | {"index.json"}, "download root file set differs")
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    records, held = [], {}
    observations = {item["run_key"]: item for item in index["observations"]}
    for name in names:
        if clock() - started >= MAX_SECONDS:
            held[name] = observation(dict(run_key=name, state="deferred", retired=observations[name]["retired"],
                phase="export", reason="bounded export exhausted; rescan after publishing completed records"))
            continue
        try:
            bundle = codec.load(source / name, repository_id=REPOSITORY_ID, reader_revision=revision)
            if name in prior:
                raw = prior[name]
                public.verify_replay(collection.results.json_value(raw), bundle)
            else:
                raw = encoded(public.validate_public(public.project(bundle)))
            require(len(raw) <= public.MAX_PUBLIC_RECORD, "public record exceeds bound")
        except Exception as error:
            # preserve the named hold without discarding other completed projections.
            # the trusted download observation, not a failed local parse, says retired.
            held[name] = observation(dict(run_key=name, state="held", retired=observations[name]["retired"],
                phase="export", reason=collection.results.diagnostic(error)))
            continue
        (output / (name + ".json")).write_bytes(raw)
        records.append(dict(run_key=name, sha256=digest(raw), bytes=len(raw)))
    value = {k: v for k, v in index.items() if k not in {"bundles", "discovery_complete", "processing_complete"}}
    value.update(schema=PUBLIC_SCHEMA, records=records,
        observations=[held.get(item.get("run_key"), item) for item in index["observations"]])
    persist(output / "index.json", value)
    return inventory(output, limit=MAX_HISTORY)


def public_batch(source, revision, expected):
    verify_inventory(source, expected, limit=MAX_HISTORY)
    source = Path(source)
    index = index_at(source, PUBLIC_SCHEMA, revision)
    records = index["records"]
    require(isinstance(records, list) and len(records) <= publication.MAX_RUNS, "public batch exceeds run bound")
    found = {}
    for ref in records:
        require(isinstance(ref, dict) and set(ref) == {"run_key", "sha256", "bytes"} and
                isinstance(ref["run_key"], str) and RUN.fullmatch(ref["run_key"]) and
                ref["run_key"] not in found, "invalid public record reference")
        raw = read(source / (ref["run_key"] + ".json"), public.MAX_PUBLIC_RECORD)
        require(type(ref["bytes"]) is int and len(raw) == ref["bytes"] and digest(raw) == ref["sha256"],
                "public record reference differs")
        value = public.validate_public(collection.results.json_value(raw))
        require(value["run_key"] == ref["run_key"], "public record identity differs")
        found[ref["run_key"]] = raw
    require({p.name for p in source.iterdir()} == {name + ".json" for name in found} | {"index.json"},
            "public batch contains unexpected files")
    return found, index


def publish(source, repository, revision, expected):
    records, _ = public_batch(source, revision, expected)
    prior, parent = history(repository)
    additions = {}
    for name, raw in records.items():
        if name in prior:
            require(prior[name] == raw, "conflicting history record; overwrite forbidden")
        else:
            additions[name] = raw
    require(len(prior) + len(additions) <= MAX_HISTORY_RUNS and
            sum(map(len, prior.values())) + sum(map(len, additions.values())) <= MAX_HISTORY,
            "resulting history exceeds bound")
    if not additions:
        return ""
    env = git_env()
    with tempfile.TemporaryDirectory(prefix="aws-history-index-") as directory:
        env["GIT_INDEX_FILE"] = str(Path(directory) / "index")
        git(repository, "read-tree", *([parent] if parent else ["--empty"]), environment=env)
        for name, raw in sorted(additions.items()):
            blob = git(repository, "hash-object", "-w", "--stdin", input_text=raw.decode(), environment=env).stdout.strip()
            require(re.fullmatch(r"[0-9a-f]{40}", blob), "invalid history blob ID")
            git(repository, "update-index", "--add", "--cacheinfo", "100644", blob,
                "records/" + name + ".json", environment=env)
        tree = git(repository, "write-tree", environment=env).stdout.strip()
        commit = git(repository, "commit-tree", tree, *(["-p", parent] if parent else []),
            "-m", "Record retired AWS benchmark results", environment=env).stdout.strip()
        require(re.fullmatch(r"[0-9a-f]{40}", commit), "invalid history commit ID")
        return commit


def outputs(**values):
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as stream:
            for key, value in values.items():
                require(re.fullmatch(r"[a-z_]+", key) and isinstance(value, str) and "\n" not in value,
                        "invalid workflow output")
                stream.write(key + "=" + value + "\n")


def summary(index):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        counts = {state: sum(item["state"] == state for item in index["observations"])
                  for state in ("ready", "published", "pending", "held", "deferred")}
        text = "AWS publication: " + ", ".join(str(count) + " " + state for state, count in counts.items())
        text += ". Active owner: " + index["active"]["state"] + ".\n\n"
        for item in index["observations"]:
            if item["state"] in {"held", "deferred"}:
                # keep bounded diagnostics on one line and inside a code span.
                # escape embedded delimiters and HTML without echoing SDK messages.
                reason = encoded(item.get("reason", "unspecified")).decode().strip()
                reason = reason.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("`", "&#96;")
                text += "- " + item.get("run_key", "unnamed") + ": " + item["state"] + " — `" + reason + "`\n"
        text += "\nThese are observations, not a new teardown proof. Metadata validation does not prove content integrity.\n"
        with open(path, "a", encoding="utf-8") as stream:
            stream.write(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("download", "export", "publish"))
    parser.add_argument("--history", required=True, type=Path)
    parser.add_argument("--reader-revision", required=True)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--expected-sha256")
    for name in ("region", "role", "bucket", "table"):
        parser.add_argument("--" + name)
    args = parser.parse_args()
    codec.revision(args.reader_revision)
    if args.phase == "download":
        from .controller import oidc_session
        require(args.output is not None and re.fullmatch(r"[a-z]{2}-[a-z]+-[1-9][0-9]?", args.region or "") and
                re.fullmatch(r"arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+", args.role or ""),
                "invalid read-only AWS publication configuration")
        prior, _ = history(args.history)
        session, config = oidc_session(args.region, args.role, session_prefix="rcp-publication-", duration_seconds=900,
            budget=Budget(datetime.now(timezone.utc) + timedelta(seconds=60)), reserve=10)
        reader = publication.Reader(session.client("s3", config=config), session.client("dynamodb", config=config),
            bucket=args.bucket, table=args.table, repository_id=REPOSITORY_ID)
        value = download(reader, args.output, prior, args.reader_revision)
        index = index_at(args.output, DOWNLOAD_SCHEMA, args.reader_revision)
    elif args.phase == "export":
        require(args.input is not None and args.output is not None, "export directories required")
        prior, _ = history(args.history)
        value = export(args.input, prior, args.output, args.reader_revision, args.expected_sha256)
        index = index_at(args.output, PUBLIC_SCHEMA, args.reader_revision)
    else:
        require(args.input is not None, "public batch required")
        commit = publish(args.input, args.history, args.reader_revision, args.expected_sha256)
        outputs(commit=commit)
        return
    summary(index)
    outputs(inventory_sha256=value, reader_revision=args.reader_revision)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("Publication failed or held (" + type(error).__name__ + ").")
        raise SystemExit(1) from None
