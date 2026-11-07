"""Bounded cross-job artifact byte inventories; no service or Git operations.

Callers own directory prefixes and exclude concurrent writers. This is not a
hostile-filesystem snapshot; each file read uses its held no-follow descriptor.
"""

import hashlib
import os
from pathlib import Path
import re
import stat

from . import production_collection as collection, production_publication as publication
from .common import digest, require

MAX_TOTAL = 512 * 1024 * 1024
MAX_HISTORY = 64 * 1024 * 1024
MAX_INDEX = 1024 * 1024


def encoded(value):
    return collection.encoded(value)


def read(path, limit):
    return collection.results.read_file(Path(path), limit, single_link=False)



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



def outputs(**values):
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as stream:
            for key, value in values.items():
                require(re.fullmatch(r"[a-z][a-z0-9_]*", key) and isinstance(value, str) and "\n" not in value and "\r" not in value,
                        "invalid workflow output")
                stream.write(key + "=" + value + "\n")
