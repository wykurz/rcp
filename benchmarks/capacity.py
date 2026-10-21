"""Conservative free-space/inode admission for disposable fresh-copy fixtures."""

import os

from benchmarks.operations import expected_counts, validate_case
from benchmarks.verification import MAX_DIRECTORY_ENTRIES, MAX_DEPTH

MAX_FILE_BYTES = 1_000_000_000
MAX_FILES = 10_240_000
MAX_DIRECTORIES = 11_110
BYTE_RESERVE = 1024 ** 3
INODE_RESERVE = 1024
DEFAULT_POLICY = dict(byte_percent=20, inode_percent=20,
                      byte_reserve=BYTE_RESERVE, inode_reserve=INODE_RESERVE)


def policy(**overrides):
    if set(overrides) - set(DEFAULT_POLICY):
        raise ValueError("unknown capacity margin")
    result = {key: overrides.get(key) if overrides.get(key) is not None else value
              for key, value in DEFAULT_POLICY.items()}
    if any(type(value) is not int or not 0 <= value < 2 ** 63 for value in result.values()):
        raise ValueError("capacity margins must be nonnegative integers below 2**63")
    return result


def validate_contract(case):
    validate_case(case)
    widths = case["directory_widths"]
    maximum_entries = (max(widths) + case["files_per_directory"]
                       if "files_per_directory" in case else max(*widths, case["files_per_leaf"]))
    if len(widths) > MAX_DEPTH or maximum_entries > MAX_DIRECTORY_ENTRIES:
        raise ValueError("fixture exceeds bounded verification depth or directory entry limit")
    counts = expected_counts(case)
    if (case.get("mode", "fresh") != "fresh"
            or not 0 < case["file_size_bytes"] <= MAX_FILE_BYTES
            or not 0 < counts["files"] <= MAX_FILES
            or not 0 < counts["directories"] <= MAX_DIRECTORIES):
        raise ValueError("unsupported bounded capacity contract")
    return counts


def requirements(case, block_size, **margins):
    counts = validate_contract(case)
    if (type(block_size) is not int or not 512 <= block_size <= 65536
            or block_size & (block_size - 1)):
        raise ValueError("unsupported capacity block size")
    margins = policy(**margins)
    inodes = counts["files"] + counts["directories"] + 1
    # round every file separately; logical bytes badly undercount tiny-file blocks
    data = counts["files"] * ((case["file_size_bytes"] + block_size - 1) // block_size) * block_size
    # directory growth, xattrs and allocator overhead are estimates, not a fit guarantee
    allocated = data + 2 * block_size * (counts["directories"] + 1)
    return dict(logical_bytes=counts["bytes"], data_block_bytes=data, fixture_inodes=inodes,
        required_bytes=allocated + (allocated * margins["byte_percent"] + 99) // 100 + margins["byte_reserve"],
        required_inodes=inodes + (inodes * margins["inode_percent"] + 99) // 100 + margins["inode_reserve"])


def check(path, case, **margins):
    # query the same held directory; never follow a final-component mount-root symlink
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        identity, space = os.fstat(fd), os.fstatvfs(fd)
        fields = ("f_blocks", "f_bavail", "f_files", "f_favail")
        if (any(type(getattr(space, key)) is not int or not 0 <= getattr(space, key) < 2 ** 63
                for key in fields)
                or space.f_bavail > space.f_blocks or space.f_favail > space.f_files):
            raise ValueError("invalid or unknown free-space/inode inventory")
        needed = requirements(case, space.f_frsize, **margins)
        available = space.f_bavail * space.f_frsize
        if available < needed["required_bytes"] or space.f_favail < needed["required_inodes"]:
            raise ValueError(f"insufficient free bytes or inodes at {path}: "
                f"{available}/{space.f_favail} available; "
                f"{needed['required_bytes']}/{needed['required_inodes']} required")
        return {**needed, "available_bytes": available, "available_inodes": space.f_favail,
                "block_size": space.f_frsize, "device": identity.st_dev}
    finally:
        os.close(fd)


def check_pair(source, destination, case, **margins):
    result = {side: check(path, case, **margins) for side, path in
              (("source", source), ("destination", destination))}
    if result["source"]["device"] == result["destination"]["device"]:
        raise ValueError("capacity admission requires distinct source and destination filesystems")
    return result
