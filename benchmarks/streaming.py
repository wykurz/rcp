"""Bounded fresh-tree SHA256 and metadata proofs; no per-tree file dictionaries."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import itertools
import json
import os
import stat

from benchmarks import capacity

POLICY = "sha256-tree-stream-v1"
MAX_DIRECTORY_ENTRIES = 4096
MAX_DEPTH = 128
DEFAULT_WORKERS = 8
MAX_WORKERS = 32
READ_SIZE = 1024 * 1024


def _metadata(fd):
    info = os.fstat(fd)
    if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
        raise ValueError("stream verification requires regular files and directories")
    if stat.S_IMODE(info.st_mode) & 0o7000 or info.st_uid != os.geteuid() or info.st_gid != os.getegid():
        raise ValueError("special mode or foreign owner/group in fixture")
    if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
        raise ValueError("hardlinked fixture entry")
    if any(name.startswith("system.posix_acl_") for name in os.listxattr(fd)):
        raise ValueError("ACL in fixture")
    return info


def _stable(before, after):
    return all(getattr(before, key) == getattr(after, key) for key in
               ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_size", "st_mtime_ns", "st_ctime_ns", "st_nlink"))


def _file(parent, name):
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    try:
        before = _metadata(fd)
        if not stat.S_ISREG(before.st_mode) or not 0 <= before.st_size <= capacity.MAX_FILE_BYTES:
            raise ValueError("invalid bounded regular file")
        digest, size = hashlib.sha256(), 0
        with os.fdopen(os.dup(fd), "rb") as stream:
            for block in iter(lambda: stream.read(READ_SIZE), b""):
                size += len(block)
                if size > before.st_size:
                    raise ValueError("file grew during verification")
                digest.update(block)
        if size != before.st_size or not _stable(before, os.fstat(fd)):
            raise ValueError("file changed during verification")
        return before, digest.hexdigest()
    finally:
        os.close(fd)


def _feed(digest, row):
    data = json.dumps(row, ensure_ascii=True, separators=(",", ":")).encode("ascii")
    digest.update(len(data).to_bytes(8, "big"))
    digest.update(data)


def scan_tree(root, workers=DEFAULT_WORKERS, expected_counts=None):
    if type(workers) is not int or not 1 <= workers <= MAX_WORKERS:
        raise ValueError("verification workers must be 1..32")
    maximum = expected_counts or dict(files=capacity.MAX_FILES, directories=capacity.MAX_DIRECTORIES,
                                     bytes=capacity.MAX_FILES * capacity.MAX_FILE_BYTES)
    content, modes, metadata = (hashlib.sha256(POLICY.encode()) for _ in range(3))
    counts = dict(files=0, directories=0, bytes=0)
    entries = 0
    def record(path, info, digest=None):
        nonlocal entries
        directory = stat.S_ISDIR(info.st_mode)
        if path != ".":
            counts["directories" if directory else "files"] += 1
        if not directory:
            counts["bytes"] += info.st_size
        if any(counts[key] > maximum[key] for key in counts):
            raise ValueError("fixture exceeded bounded entry/byte counts")
        entries += 1
        # versioned depth-first, sorted-name records; root is included
        _feed(content, [path, "directory" if directory else "file",
                        None if directory else info.st_size, digest])
        _feed(modes, [path, stat.S_IMODE(info.st_mode)])
        _feed(metadata, [path, stat.S_IMODE(info.st_mode), info.st_uid, info.st_gid, info.st_mtime_ns])
    def walk(fd, prefix, depth, pool):
        if depth > MAX_DEPTH:
            raise ValueError("fixture depth limit exceeded")
        before = _metadata(fd)
        if not stat.S_ISDIR(before.st_mode):
            raise ValueError("expected directory")
        record(prefix or ".", before)
        with os.scandir(fd) as children:
            names = sorted(itertools.islice((entry.name for entry in children), MAX_DIRECTORY_ENTRIES + 1))
        if len(names) > MAX_DIRECTORY_ENTRIES:
            raise ValueError("fixture directory entry limit exceeded")
        # only one bounded batch of futures exists; subdirectories are visited after it drains
        for offset in range(0, len(names), workers * 2):
            batch = []
            try:
                for name in names[offset:offset + workers * 2]:
                    kind = os.stat(name, dir_fd=fd, follow_symlinks=False).st_mode
                    if stat.S_ISDIR(kind):
                        batch.append((name, None))
                    elif stat.S_ISREG(kind):
                        batch.append((name, pool.submit(_file, fd, name)))
                    else:
                        raise ValueError("unexpected symlink or special fixture entry")
                # finish all reads while this directory fd remains held
                ready = [(name, future.result() if future is not None else None) for name, future in batch]
            finally:
                for _, future in batch:
                    if future is not None:
                        # drain on error too, before closing/reusing the parent fd
                        try:
                            future.result()
                        except Exception:
                            pass
            for name, value in ready:
                relative = prefix + "/" + name if prefix else name
                if value is None:
                    child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                    try:
                        walk(child, relative, depth + 1, pool)
                    finally:
                        os.close(child)
                else:
                    record(relative, *value)
        if not _stable(before, os.fstat(fd)):
            raise ValueError("directory changed during verification")
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            walk(fd, "", 0, pool)
    finally:
        os.close(fd)
    if expected_counts is not None and counts != expected_counts:
        raise ValueError("fixture counts differ from manifest")
    return dict(counts=counts, digest=content.hexdigest(), modes=modes.hexdigest(),
                metadata=metadata.hexdigest(), entries=entries, policy=POLICY)


def validate_tree(destination, expected, workers=DEFAULT_WORKERS, *, source=False):
    try:
        actual = scan_tree(destination, workers, expected["counts"])
        if actual["digest"] != expected["digest"] or actual["modes"] != expected["modes"]:
            raise ValueError("fixture paths, contents or modes differ")
        if source and actual["metadata"] != expected["metadata"]:
            raise ValueError("source metadata changed")
        fields = ["mode", "uid", "gid", "mtime_ns"] if source else ["mode"]
        return dict(ok=True, counts=actual["counts"], digest=actual["digest"],
                    metadata=dict(ok=True, entries=actual["entries"], checked_fields=fields))
    except (OSError, ValueError) as error:
        return dict(ok=False, error=str(error))
