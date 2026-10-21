"""Shared verification contract identifiers and presentation labels."""

FULL_CONTENT_POLICY = "sha256-tree-stream-v1"
METADATA_POLICY = "metadata-size-tree-v1"
DEFAULT_WORKERS = 8
MAX_WORKERS = 32
DEFAULT_INLINE_BYTES = 1024 * 1024
MAX_INLINE_BYTES = 1_000_000_000
LEGACY_SOURCE_METADATA_FIELDS = ("mode", "uid", "gid", "mtime_ns")
SOURCE_METADATA_FIELDS = (*LEGACY_SOURCE_METADATA_FIELDS, "ctime_ns", "dev", "ino")


def verification_label(context):
    """Describe the recorded verification contract without upgrading legacy evidence."""
    policy = (context.get("verification") or {}).get("policy")
    if policy == METADATA_POLICY:
        return "Metadata/size only; file content integrity not checked"
    if policy == FULL_CONTENT_POLICY:
        return "Full-content SHA-256"
    return "Legacy validation; see recorded proof contract"
