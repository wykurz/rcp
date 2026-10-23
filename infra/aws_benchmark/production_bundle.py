"""Bounded files exchanged between trusted download and credential-free export jobs."""

from dataclasses import asdict
import os
from pathlib import Path
import re
import stat

from . import production_collection as collection
from .common import require
from .production_publication import Bundle, LIMITS
from .production_store import RunStore

SCHEMA = "rcp-production-download-v1"
MAX_ENVELOPE = 512 * 1024


def record_item(value):
    """Reconstruct parser input, not an independent signature or tamper proof.

    load() checks shape and internal byte bindings. Authenticity comes from the
    download job's independently supplied inventory digest before load() is called.
    """
    require(isinstance(value, dict) and set(value) == {"run_key", "epoch", "fingerprint", "admission", "state",
        "revision", "closed", "terminal", "retirement_sha256"}, "invalid saved run record")
    require(type(value["revision"]) is int and type(value["closed"]) is bool and type(value["terminal"]) is bool,
            "invalid saved record scalar")
    return dict(pk={"S": "run#" + value["run_key"]}, epoch={"S": value["epoch"]},
        fingerprint={"S": value["fingerprint"]}, admission={"S": collection.encoded(value["admission"]).decode()},
        state={"S": collection.encoded(value["state"]).decode()}, revision={"N": str(value["revision"])},
        closed={"BOOL": value["closed"]}, terminal={"BOOL": value["terminal"]},
        **({"retirement_sha256": {"S": value["retirement_sha256"]}} if value["retirement_sha256"] is not None else {}))


def revision(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value), "invalid downloader source revision")
    return value


def save(bundle, destination, reader_revision):
    bundle.validate()
    envelope = collection.encoded(dict(schema=SCHEMA, run_key=bundle.run_key, reader_revision=revision(reader_revision),
        record=asdict(bundle.record), manifest_ref=bundle.manifest_ref, payloads=sorted(bundle.payloads)))
    require(len(envelope) <= MAX_ENVELOPE, "download envelope exceeds bound")
    destination = Path(destination)
    destination.mkdir(mode=0o700, parents=False, exist_ok=False)
    # write the envelope last; partial downloads never constitute a complete bundle
    files = dict(bundle.payloads, **{"manifest.json": bundle.manifest_bytes, "bundle.json": envelope})
    for name, raw in files.items():
        descriptor = os.open(destination / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    return destination


def load(source, *, repository_id, reader_revision):
    require(isinstance(repository_id, str) and re.fullmatch(r"[1-9][0-9]{0,19}", repository_id),
            "invalid expected repository ID")
    source = Path(source)
    require(stat.S_ISDIR(source.lstat().st_mode), "bundle root must be a real directory")
    envelope = collection.results.json_value(collection.results.read_file(source / "bundle.json", MAX_ENVELOPE), MAX_ENVELOPE)
    require(isinstance(envelope, dict) and set(envelope) == {
        "schema", "run_key", "reader_revision", "record", "manifest_ref", "payloads"} and
        envelope["schema"] == SCHEMA and envelope["reader_revision"] == revision(reader_revision) and
        isinstance(envelope["run_key"], str) and re.fullmatch(
            r"repo-" + repository_id + r"-run-[1-9][0-9]{0,19}", envelope["run_key"]), "download envelope identity differs")
    names = envelope["payloads"]
    require(isinstance(names, list) and all(isinstance(name, str) for name in names) and
            names == sorted(set(names)) and {"durable.json", "controller.json", "report.json", "summary.html"}
            <= set(names) <= set(LIMITS), "download payload set differs")
    expected = set(names) | {"manifest.json", "bundle.json"}
    seen = set()
    with os.scandir(source) as entries:
        for entry in entries:
            require(entry.name in expected and entry.name not in seen and len(seen) < len(expected),
                    "download contains unexpected files")
            seen.add(entry.name)
    require(seen == expected, "download contains missing files")
    item = record_item(envelope["record"])
    store = RunStore(None, "offline", envelope["run_key"], envelope["record"]["epoch"])
    record = store._record(item)
    raw = collection.results.read_file(source / "manifest.json", collection.MAX_DOCUMENT)
    payloads = {name: collection.results.read_file(source / name, LIMITS[name]) for name in names}
    bundle = Bundle(envelope["run_key"], record, item, raw, envelope["manifest_ref"], payloads)
    bundle.validate()
    return bundle
