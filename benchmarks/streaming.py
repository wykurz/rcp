"""Bounded fresh-tree content or metadata proofs; no per-tree file dictionaries."""

from concurrent.futures import ThreadPoolExecutor, wait
import hashlib
import itertools
import json
import os
import signal
import stat
import threading

from benchmarks import capacity
from benchmarks.verification import (FULL_CONTENT_POLICY as POLICY, METADATA_POLICY,
                                     DEFAULT_WORKERS, MAX_WORKERS, DEFAULT_INLINE_BYTES,
                                     MAX_INLINE_BYTES, SOURCE_METADATA_FIELDS)

MAX_DIRECTORY_ENTRIES = 4096
MAX_DEPTH = 128
READ_SIZE = 1024 * 1024


class _ScanCancelled(Exception):
    pass


class _Cancellation:
    def __init__(self):
        self.stopped = False
        self.pending = None
        self.previous = {}

    def __enter__(self):
        if threading.current_thread() is threading.main_thread():
            try:
                for number in (signal.SIGINT, signal.SIGTERM):
                    previous = signal.getsignal(number)
                    if previous != signal.SIG_IGN:
                        signal.signal(number, self._record)
                        self.previous[number] = previous
            except BaseException:
                self.__exit__(None, None, None)
                raise
        return self

    def _record(self, number, _frame):
        # signal handlers must neither raise inside executor locks nor acquire locks themselves
        if self.pending is None:
            self.pending = number
        self.stopped = True

    def checkpoint(self):
        if self.stopped:
            raise _ScanCancelled

    def __exit__(self, _type, _value, _traceback):
        for number, handler in self.previous.items():
            signal.signal(number, handler)
        if self.pending is not None:
            raise InterruptedError(signal.Signals(self.pending).name)
        return False


class _EntryError(ValueError):
    def __init__(self, path, error):
        super().__init__(f"{path}: {error}")
        self.path = path
        self.verification_error = isinstance(error, OSError)


class _ScanError(ValueError):
    def __init__(self, error, actual, *, verification_error=False):
        super().__init__(str(error))
        self.actual = actual
        self.path = getattr(error, "path", None)
        self.verification_error = verification_error


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


def _file(parent, name, policy=POLICY, cancellation=None):
    if cancellation is not None:
        cancellation.checkpoint()
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    try:
        before = _metadata(fd)
        if not stat.S_ISREG(before.st_mode) or not 0 <= before.st_size <= capacity.MAX_FILE_BYTES:
            raise ValueError("invalid bounded regular file")
        digest = None
        if policy == POLICY:
            digest, size = hashlib.sha256(), 0
            with os.fdopen(os.dup(fd), "rb") as stream:
                while True:
                    if cancellation is not None:
                        cancellation.checkpoint()
                    block = stream.read(READ_SIZE)
                    if not block:
                        break
                    size += len(block)
                    if size > before.st_size:
                        raise ValueError("file grew during verification")
                    digest.update(block)
            if size != before.st_size:
                raise ValueError("file changed during verification")
        if not _stable(before, os.fstat(fd)):
            raise ValueError("file changed during verification")
        return before, digest.hexdigest() if digest is not None else None
    finally:
        os.close(fd)


def _feed(digest, row):
    data = json.dumps(row, ensure_ascii=True, separators=(",", ":")).encode("ascii")
    digest.update(len(data).to_bytes(8, "big"))
    digest.update(data)


def _drain(pool, cancellation):
    cancellation.stopped = True
    # submit may enqueue work before raising, so returned futures cannot define worker lifetime
    pool.shutdown(wait=True, cancel_futures=True)


def scan_tree(root, workers=DEFAULT_WORKERS, expected_counts=None, *, policy=POLICY,
              inline_bytes=DEFAULT_INLINE_BYTES):
    if policy not in (POLICY, METADATA_POLICY):
        raise ValueError("unsupported streaming verification policy")
    if type(workers) is not int or not 1 <= workers <= MAX_WORKERS:
        raise ValueError(f"verification workers must be 1..{MAX_WORKERS}")
    if type(inline_bytes) is not int or not 0 <= inline_bytes <= MAX_INLINE_BYTES:
        raise ValueError(f"verification inline bytes must be 0..{MAX_INLINE_BYTES}")
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
            raise _EntryError(path, ValueError(
                f"fixture exceeded bounded entry/byte counts: observed {counts}; maximum {maximum}"))
    def checked_file(fd, name, relative):
        try:
            return _file(fd, name, policy, cancellation)
        except InterruptedError:
            raise
        except (OSError, ValueError) as error:
            raise _EntryError(relative, error) from error
    def walk(fd, prefix, depth, pool):
        try:
            walk_directory(fd, prefix, depth, pool)
        except (InterruptedError, _EntryError):
            raise
        except (OSError, ValueError) as error:
            raise _EntryError(prefix or ".", error) from error
    def walk_directory(fd, prefix, depth, pool):
        cancellation.checkpoint()
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
        # only one bounded batch exists; subdirectories are visited after its workers drain
        for offset in range(0, len(names), workers * 2):
            batch, futures = [], []
            try:
                for name in names[offset:offset + workers * 2]:
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
                            raise ValueError("unexpected symlink or special fixture entry")
                    except (InterruptedError, _EntryError):
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
                        raise _EntryError(relative, error) from error
                    try:
                        walk(child, relative, depth + 1, pool)
                    finally:
                        os.close(child)
                else:
                    record(relative, *value)
        if not _stable(before, os.fstat(fd)):
            raise ValueError("directory changed during verification")
    try:
        with cancellation:
            fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    walk(fd, "", 0, pool)
            finally:
                os.close(fd)
    except InterruptedError as error:
        error.actual = snapshot()
        raise
    except (OSError, ValueError) as error:
        raise _ScanError(error, dict(snapshot(), complete=False),
                         verification_error=isinstance(error, OSError) or
                         getattr(error, "verification_error", False)) from error
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
                  inline_bytes=DEFAULT_INLINE_BYTES):
    policy = expected.get("policy")
    try:
        if policy not in (POLICY, METADATA_POLICY):
            raise ValueError("unsupported streaming verification policy")
        actual = scan_tree(destination, workers, expected["counts"], policy=policy, inline_bytes=inline_bytes)
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
    except InterruptedError as error:
        proof = dict(ok=False, error=str(error), interrupted=True)
        if hasattr(error, "actual"):
            proof.update(_evidence(error.actual))
    except (OSError, ValueError) as error:
        proof = dict(ok=False, error=str(error))
        if isinstance(error, _ScanError):
            proof.update(_evidence(error.actual))
            if error.path is not None:
                proof["path"] = error.path
        if getattr(error, "verification_error", isinstance(error, OSError)):
            proof["verification_error"] = True
    if policy == METADATA_POLICY:
        proof["policy"] = policy
    return proof
