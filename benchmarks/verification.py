"""Shared verification contracts, observed-failure types and presentation labels."""

import errno
import os
import stat


LEGACY_VERIFICATION_POLICY = "exact relative directory and regular-file paths, sizes, and SHA256 contents"
LEGACY_SOURCE_VERIFICATION_POLICY = LEGACY_VERIFICATION_POLICY + "; source tree must match initial scan after all case trials"
FULL_CONTENT_POLICY = "sha256-tree-stream-v1"
METADATA_POLICY = "metadata-size-tree-v1"
DEFAULT_WORKERS = 8
MAX_WORKERS = 32
DEFAULT_INLINE_BYTES = 128 * 1024
MAX_INLINE_BYTES = 1_000_000_000
DEFAULT_READ_SIZE = 1024 * 1024
MIN_READ_BYTES = 4096
MAX_READ_BYTES = 16 * 1024 * 1024
DEFAULT_BATCH_MULTIPLIER = 2
MAX_BATCH_DEPTH = 8
MAX_DIRECTORY_ENTRIES = 4096
MAX_DEPTH = 128
LEGACY_SOURCE_METADATA_FIELDS = ("mode", "uid", "gid", "mtime_ns")
SOURCE_METADATA_FIELDS = (*LEGACY_SOURCE_METADATA_FIELDS, "ctime_ns", "dev", "ino")


class MismatchError(ValueError):
    """An observed tree property violates the expected fixture contract."""


def observed_error(error, path, parent=None):
    if error.errno in (errno.ENOENT, errno.ENOTDIR, errno.ELOOP):
        return MismatchError(str(error))
    if error.errno == errno.EACCES:
        # this is diagnostic only; every successful action still uses its own no-follow fd
        try:
            info = os.stat(path, dir_fd=parent, follow_symlinks=False)
        except InterruptedError:
            raise
        except OSError:
            return error
        required = stat.S_IRUSR | (stat.S_IXUSR if stat.S_ISDIR(info.st_mode) else 0)
        if info.st_uid == os.geteuid() and info.st_mode & required != required:
            return MismatchError("fixture owner mode prevents verification")
    return error


def is_verification_error(error):
    from benchmarks.signals import CancelledSignal
    if isinstance(error, (MismatchError, InterruptedError, KeyboardInterrupt, CancelledSignal)):
        return False
    if isinstance(error, OSError) and error.errno in (errno.ENOENT, errno.ENOTDIR, errno.ELOOP):
        return False
    return getattr(error, "verification_error", True)


def failure_proof(error):
    from benchmarks.signals import CancelledSignal
    proof = dict(ok=False, error=str(error) or type(error).__name__)
    if isinstance(error, (InterruptedError, KeyboardInterrupt, CancelledSignal)):
        proof["interrupted"] = True
    elif is_verification_error(error):
        proof["verification_error"] = True
    return proof


def verification_label(context):
    """Describe the recorded verification contract without upgrading legacy evidence."""
    policy = (context.get("verification") or {}).get("policy")
    if policy == METADATA_POLICY:
        return "Metadata/size only; file content integrity not checked"
    if policy == FULL_CONTENT_POLICY:
        return "Full-content SHA-256"
    if policy is None and context.get("verification_policy") in (
            LEGACY_VERIFICATION_POLICY, LEGACY_SOURCE_VERIFICATION_POLICY):
        return "Full-content SHA-256"
    return "Legacy validation; see recorded proof contract"
