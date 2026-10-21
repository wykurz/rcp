"""Bounded fresh-tree content or metadata proofs; no per-tree file dictionaries."""

from concurrent.futures import ThreadPoolExecutor, wait
import hashlib
import itertools
import json
import os
import stat

from benchmarks import capacity
from benchmarks.verification import (FULL_CONTENT_POLICY as POLICY, METADATA_POLICY,
                                     DEFAULT_WORKERS, MAX_WORKERS, DEFAULT_INLINE_BYTES,
                                     MAX_INLINE_BYTES, SOURCE_METADATA_FIELDS,
                                     DEFAULT_READ_SIZE, MIN_READ_BYTES, MAX_READ_BYTES,
                                     DEFAULT_BATCH_MULTIPLIER, MAX_BATCH_DEPTH,
                                     MAX_DIRECTORY_ENTRIES, MAX_DEPTH,
                                     MismatchError, is_verification_error, failure_proof, observed_error as _observed_error)
from benchmarks.signals import SignalCancellation as _Cancellation, CancelledSignal as _ScanCancelled

READ_SIZE = DEFAULT_READ_SIZE
_ENCODE_RECORD = json.JSONEncoder(ensure_ascii=True, separators=(",", ":")).encode


class _CountBoundError(MismatchError):
    pass


class _EntryError(ValueError):
    def __init__(self, path, error):
        super().__init__(f"{path}: {error}")
        self.path = path
        self.verification_error = is_verification_error(error)


class _ScanError(ValueError):
    def __init__(self, error, actual, *, verification_error=False):
        super().__init__(str(error))
        self.actual = actual
        self.path = getattr(error, "path", None)
        self.verification_error = verification_error


def _metadata(fd):
    info = os.fstat(fd)
    if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
        raise MismatchError("stream verification requires regular files and directories")
    if stat.S_IMODE(info.st_mode) & 0o7000 or info.st_uid != os.geteuid() or info.st_gid != os.getegid():
        raise MismatchError("special mode or foreign owner/group in fixture")
    if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
        raise MismatchError("hardlinked fixture entry")
    if any(name.startswith("system.posix_acl_") for name in os.listxattr(fd)):
        raise MismatchError("ACL in fixture")
    return info


def _stable(before, after):
    return all(getattr(before, key) == getattr(after, key) for key in
               ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid", "st_size", "st_mtime_ns", "st_ctime_ns", "st_nlink"))


def _file(parent, name, policy=POLICY, cancellation=None, read_bytes=None):
    if cancellation is not None:
        cancellation.checkpoint()
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    except OSError as error:
        raise _observed_error(error, name, parent) from error
    try:
        before = _metadata(fd)
        if not stat.S_ISREG(before.st_mode) or not 0 <= before.st_size <= capacity.MAX_FILE_BYTES:
            raise MismatchError("invalid bounded regular file")
        digest = None
        if policy == POLICY:
            digest, size = hashlib.sha256(), 0
            while True:
                if cancellation is not None:
                    cancellation.checkpoint()
                block = os.read(fd, READ_SIZE if read_bytes is None else read_bytes)
                if not block:
                    break
                size += len(block)
                if size > before.st_size:
                    raise MismatchError("file grew during verification")
                digest.update(block)
            if size != before.st_size:
                raise MismatchError("file changed during verification")
        if not _stable(before, os.fstat(fd)):
            raise MismatchError("file changed during verification")
        return before, digest.hexdigest() if digest is not None else None
    finally:
        os.close(fd)


def _feed(digest, row):
    data = _ENCODE_RECORD(row).encode("ascii")
    digest.update(len(data).to_bytes(8, "big"))
    digest.update(data)


def _drain(pool, cancellation):
    cancellation.stopped = True
    # submit may enqueue work before raising, so returned futures cannot define worker lifetime
    pool.shutdown(wait=True, cancel_futures=True)


def scan_tree(root, workers=DEFAULT_WORKERS, expected_counts=None, *, policy=POLICY,
              inline_bytes=DEFAULT_INLINE_BYTES, read_bytes=DEFAULT_READ_SIZE,
              batch_depth=DEFAULT_BATCH_MULTIPLIER):
    if policy not in (POLICY, METADATA_POLICY):
        raise ValueError("unsupported streaming verification policy")
    if type(workers) is not int or not 1 <= workers <= MAX_WORKERS:
        raise ValueError(f"verification workers must be 1..{MAX_WORKERS}")
    if type(inline_bytes) is not int or not 0 <= inline_bytes <= MAX_INLINE_BYTES:
        raise ValueError(f"verification inline bytes must be 0..{MAX_INLINE_BYTES}")
    if type(read_bytes) is not int or not MIN_READ_BYTES <= read_bytes <= MAX_READ_BYTES:
        raise ValueError(f"verification read bytes must be {MIN_READ_BYTES}..{MAX_READ_BYTES}")
    if type(batch_depth) is not int or not 1 <= batch_depth <= MAX_BATCH_DEPTH:
        raise ValueError(f"verification batch depth must be 1..{MAX_BATCH_DEPTH}")
    maximum = expected_counts or dict(files=capacity.MAX_FILES, directories=capacity.MAX_DIRECTORIES,
                                     bytes=capacity.MAX_FILES * capacity.MAX_FILE_BYTES)
    content, modes, metadata = (hashlib.sha256(policy.encode()) for _ in range(3))
    counts = dict(files=0, directories=0, bytes=0)
    entries = 0
    cancellation = _Cancellation()
    def snapshot():
        return dict(counts=dict(counts), digest=content.hexdigest(), modes=modes.hexdigest(),
                    metadata=metadata.hexdigest(), entries=entries, policy=policy)
    def record(path, info, digest=None):
        nonlocal entries
        cancellation.checkpoint()
        directory = stat.S_ISDIR(info.st_mode)
        if path != ".":
            counts["directories" if directory else "files"] += 1
        if not directory:
            counts["bytes"] += info.st_size
        entries += 1
        # versioned depth-first records; metadata mode fingerprints structure, never payload bytes
        _feed(content, [path, "directory" if directory else "file",
                        None if directory else info.st_size, digest])
        _feed(modes, [path, stat.S_IMODE(info.st_mode)])
        _feed(metadata, [path, stat.S_IMODE(info.st_mode), info.st_uid, info.st_gid,
                         info.st_mtime_ns, info.st_ctime_ns, info.st_dev, info.st_ino])
        if any(counts[key] > maximum[key] for key in counts):
            raise _CountBoundError(
                f"fixture exceeded bounded entry/byte counts: observed {counts}; maximum {maximum}")
    def checked_file(fd, name, relative):
        try:
            return _file(fd, name, policy, cancellation, read_bytes)
        except InterruptedError:
            raise
        except (OSError, ValueError) as error:
            raise _EntryError(relative, error) from error
    def walk(fd, prefix, depth, pool):
        try:
            walk_directory(fd, prefix, depth, pool)
        except (InterruptedError, _EntryError, _CountBoundError):
            raise
        except (OSError, ValueError) as error:
            raise _EntryError(prefix or ".", error) from error
    def walk_directory(fd, prefix, depth, pool):
        cancellation.checkpoint()
        if depth > MAX_DEPTH:
            raise MismatchError("fixture depth limit exceeded")
        before = _metadata(fd)
        if not stat.S_ISDIR(before.st_mode):
            raise MismatchError("expected directory")
        record(prefix or ".", before)
        with os.scandir(fd) as children:
            names = sorted(itertools.islice((entry.name for entry in children), MAX_DIRECTORY_ENTRIES + 1))
        if len(names) > MAX_DIRECTORY_ENTRIES:
            raise MismatchError("fixture directory entry limit exceeded")
        # only one bounded batch exists; subdirectories are visited after its workers drain
        for offset in range(0, len(names), workers * batch_depth):
            batch, futures = [], []
            try:
                for name in names[offset:offset + workers * batch_depth]:
                    cancellation.checkpoint()
                    relative = prefix + "/" + name if prefix else name
                    try:
                        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                        if stat.S_ISDIR(info.st_mode):
                            batch.append((name, relative, None, None))
                        elif stat.S_ISREG(info.st_mode):
                            # size is only a scheduling hint; _file validates and reads its own held fd
                            if (policy == METADATA_POLICY or workers == 1 or
                                    (inline_bytes and info.st_size <= inline_bytes)):
                                batch.append((name, relative, checked_file(fd, name, relative), None))
                            else:
                                future = pool.submit(checked_file, fd, name, relative)
                                futures.append(future)
                                batch.append((name, relative, None, future))
                        else:
                            raise MismatchError("unexpected symlink or special fixture entry")
                    except (InterruptedError, _EntryError, _CountBoundError):
                        raise
                    except (OSError, ValueError) as error:
                        raise _EntryError(relative, error) from error
                pending = set(futures)
                while pending:
                    cancellation.checkpoint()
                    done, pending = wait(pending, timeout=0.05)
                    for future in done:
                        future.result()
                cancellation.checkpoint()
            except BaseException:
                # a worker can still use fd until this drain completes, including on traversal errors
                _drain(pool, cancellation)
                raise
            for name, relative, value, future in batch:
                cancellation.checkpoint()
                if future is not None:
                    value = future.result()
                if value is None:
                    try:
                        child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                    except InterruptedError:
                        raise
                    except OSError as error:
                        raise _EntryError(relative, _observed_error(error, name, fd)) from error
                    try:
                        walk(child, relative, depth + 1, pool)
                    finally:
                        os.close(child)
                else:
                    record(relative, *value)
        if not _stable(before, os.fstat(fd)):
            raise MismatchError("directory changed during verification")
    try:
        with cancellation:
            try:
                fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            except OSError as error:
                raise _EntryError(".", _observed_error(error, root)) from error
            pool = None
            try:
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    walk(fd, "", 0, pool)
            finally:
                # release the local pool reference while cancellation is still deferred
                pool = None
                os.close(fd)
    except (InterruptedError, KeyboardInterrupt) as error:
        error.actual = snapshot()
        raise
    except (OSError, ValueError, RuntimeError) as error:
        raise _ScanError(error, dict(snapshot(), complete=False),
                         verification_error=is_verification_error(error)) from error
    actual = snapshot()
    if expected_counts is not None and counts != expected_counts:
        raise _ScanError(f"fixture counts differ from manifest: observed {counts}; expected {expected_counts}",
                         dict(actual, complete=True))
    return actual


def _evidence(actual, *, complete=False):
    return dict(counts=actual["counts"], digest=actual["digest"], modes_digest=actual["modes"],
                metadata_digest=actual["metadata"], entries=actual["entries"],
                complete=actual.get("complete", complete))


def validate_tree(destination, expected, workers=DEFAULT_WORKERS, *, source=False,
                  inline_bytes=DEFAULT_INLINE_BYTES, read_bytes=DEFAULT_READ_SIZE,
                  batch_depth=DEFAULT_BATCH_MULTIPLIER):
    policy = expected.get("policy")
    try:
        if policy not in (POLICY, METADATA_POLICY):
            raise ValueError("unsupported streaming verification policy")
        actual = scan_tree(destination, workers, expected["counts"], policy=policy, inline_bytes=inline_bytes,
                           read_bytes=read_bytes, batch_depth=batch_depth)
        matches = dict(digest_match=actual["digest"] == expected["digest"],
                       modes_match=actual["modes"] == expected["modes"])
        if source:
            matches["source_metadata_match"] = actual["metadata"] == expected["metadata"]
        failure = None
        if not matches["digest_match"]:
            compared = "contents" if policy == POLICY else "sizes"
            failure = f"fixture paths or {compared} differ"
        elif not matches["modes_match"]:
            failure = "fixture modes differ"
        elif source and not matches["source_metadata_match"]:
            failure = "source metadata changed"
        if failure is not None:
            proof = dict(ok=False, error=failure, **_evidence(actual, complete=True), **matches)
        else:
            fields = list(SOURCE_METADATA_FIELDS) if source else ["mode"]
            proof = dict(ok=True, counts=actual["counts"], digest=actual["digest"],
                         metadata=dict(ok=True, entries=actual["entries"], checked_fields=fields))
    except KeyboardInterrupt as error:
        proof = failure_proof(error)
        if hasattr(error, "actual"):
            proof.update(_evidence(error.actual))
        if policy == METADATA_POLICY:
            proof["policy"] = policy
        error.proof = proof
        raise
    except InterruptedError as error:
        proof = failure_proof(error)
        if hasattr(error, "actual"):
            proof.update(_evidence(error.actual))
    except (OSError, ValueError, RuntimeError) as error:
        proof = failure_proof(error)
        if isinstance(error, _ScanError):
            proof.update(_evidence(error.actual))
            if error.path is not None:
                proof["path"] = error.path
    if policy == METADATA_POLICY:
        proof["policy"] = policy
    return proof
