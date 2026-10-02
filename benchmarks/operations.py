"""Bounded generated-tree updates; preparation and proofs run outside copy timing."""
import hashlib
import os
from pathlib import Path
import re
import shutil
import stat


CONTRACT_REVISION = 1
CACHE_REVISION = 1
SUMMARY_LOCALE = "C"


def partial_parameters(directories, files_per_directory):
    """Floor one percent of all files, spread over sorted file-containing directories."""
    total = directories * files_per_directory
    selected = total // 100
    if not 0 < selected < total:
        raise ValueError("partial requires a nonempty proper one-percent subset (at least 100 files)")
    return divmod(selected, directories)


def expected_counts(case):
    directories = 0
    breadth = 1
    for width in case["directory_widths"]:
        breadth *= width
        directories += breadth
    files = (directories + 1) * case["files_per_directory"] if "files_per_directory" in case else breadth * case["files_per_leaf"]
    return {"directories": directories, "files": files, "bytes": files * case["file_size_bytes"]}


def validate_case(case):
    widths = case.get("directory_widths")
    if not isinstance(widths, list) or not widths or any(type(width) is not int or width <= 0 for width in widths):
        raise ValueError("directory_widths must be a nonempty list of positive integers")
    policies = case.keys() & {"files_per_leaf", "files_per_directory"}
    if len(policies) != 1:
        raise ValueError("case requires exactly one of files_per_leaf and files_per_directory")
    for field in (*policies, "file_size_bytes"):
        if type(case.get(field)) is not int or case[field] <= 0:
            raise ValueError(f"{field} must be a positive integer")
    operation = case.get("mode", "fresh")
    if operation not in ("fresh", "unchanged", "partial"):
        raise ValueError("case mode must be fresh, unchanged, or partial")
    if operation == "partial":
        if case["file_size_bytes"] > 1024:
            raise ValueError("partial only supports tiny files of at most 1024 bytes")
        if expected_counts(case)["files"] < 100:
            raise ValueError("partial requires a nonempty proper one-percent subset (at least 100 files)")


def summary_supported(variant):
    return variant["processes"] == 1 and (
        (variant["tool"] == "rcp" and variant["args"] == ["--summary"])
        or (variant["tool"] == "rsync" and variant["args"] == ["-rp", "--stats"]))


def validate_variant(variant, operation):
    if operation not in ("fresh", "unchanged", "partial"):
        raise ValueError("unknown operation")
    if operation != "fresh" and not summary_supported(variant):
        raise ValueError("update operation requires single-process rcp --summary or rsync -rp --stats")


def transfer_counts(operation, counts):
    copied = {"fresh": counts["files"], "unchanged": 0, "partial": counts["files"] // 100}[operation]
    return dict(files_copied=copied, files_unchanged=counts["files"] - copied,
                bytes_copied=copied * (counts["bytes"] // counts["files"]))


def select_stale(entries):
    directories = {}
    for name, entry in entries.items():
        if entry["type"] == "file":
            directories.setdefault(str(Path(name).parent), []).append(name)
    if not directories or len({len(names) for names in directories.values()}) != 1:
        raise ValueError("partial requires uniform file counts per populated directory")
    per_directory, extra_directories = partial_parameters(len(directories), len(next(iter(directories.values()))))
    return [name for index, directory in enumerate(sorted(directories))
            for name in sorted(directories[directory])[:per_directory + (index < extra_directories)]]


def metadata_tree(path):
    """Snapshot ordinary modes and mtimes; reject ACLs, foreign ownership and links."""
    path = Path(path)
    result = {}
    pending = [path]
    while pending:
        current = pending.pop()
        st = current.stat(follow_symlinks=False)
        if not (stat.S_ISREG(st.st_mode) or stat.S_ISDIR(st.st_mode)):
            raise ValueError(f"non-regular fixture entry: {current}")
        if stat.S_IMODE(st.st_mode) & 0o7000:
            raise ValueError(f"special fixture mode bits: {current}")
        if st.st_uid != os.geteuid() or st.st_gid != os.getegid():
            raise ValueError(f"foreign fixture owner/group: {current}")
        if stat.S_ISREG(st.st_mode) and st.st_nlink != 1:
            raise ValueError(f"fixture has hardlinks: {current}")
        if any(name.startswith("system.posix_acl_") for name in os.listxattr(current, follow_symlinks=False)):
            raise ValueError(f"fixture/destination has ACL inheritance: {current}")
        result[str(current.relative_to(path))] = dict(mode=stat.S_IMODE(st.st_mode),
            uid=st.st_uid, gid=st.st_gid, mtime_ns=st.st_mtime_ns)
        if stat.S_ISDIR(st.st_mode):
            pending.extend(current.iterdir())
    return result


def validate_metadata(path, expected, timestamps=False):
    actual = metadata_tree(path)
    fields = ("mode", "uid", "gid", "mtime_ns") if timestamps else ("mode",)
    if set(actual) != set(expected):
        raise ValueError("metadata paths differ")
    changed = [name for name in actual if any(actual[name][key] != expected[name][key] for key in fields)]
    if changed:
        raise ValueError(f"metadata differs: {changed[:5]}")
    return dict(ok=True, entries=len(actual), checked_fields=list(fields))


def seed_destination(source, destination, expected, metadata, stale_names):
    """Write independent files, optionally invert tiny stale bytes and age by two seconds."""
    source, destination = Path(source), Path(destination)
    if destination.exists() or destination.is_symlink():
        raise ValueError("seed destination must not exist")
    stale_names = set(stale_names)
    if any(expected["entries"].get(name, {}).get("type") != "file" for name in stale_names):
        raise ValueError("stale selection must contain known regular files")
    destination.mkdir(mode=0o700)
    directories = [name for name, entry in expected["entries"].items() if entry["type"] == "directory"]
    for name in sorted(directories, key=lambda value: (value.count("/"), value)):
        (destination / name).mkdir(mode=0o700)
    stale = {}
    for name, entry in expected["entries"].items():
        if entry["type"] != "file":
            continue
        src, dst = source / name, destination / name
        with src.open("rb") as reader, dst.open("xb") as writer:
            if name in stale_names:
                if entry["size"] > 1024:
                    raise ValueError("partial seed only supports tiny files")
                payload = reader.read()
                if len(payload) != entry["size"] or hashlib.sha256(payload).hexdigest() != entry["sha256"]:
                    raise ValueError("source changed while constructing stale seed")
                old = bytes(value ^ 0xff for value in payload)
                writer.write(old)
                stale[name] = dict(source_sha256=entry["sha256"], stale_sha256=hashlib.sha256(old).hexdigest(),
                                   size=entry["size"], mtime_ns=metadata[name]["mtime_ns"] - 2000000000)
            else:
                shutil.copyfileobj(reader, writer, length=1024 * 1024)
        dst.chmod(metadata[name]["mode"])
        stamp = stale[name]["mtime_ns"] if name in stale else metadata[name]["mtime_ns"]
        os.utime(dst, ns=(stamp, stamp))
    for name in sorted([".", *directories], key=lambda value: value.count("/") + (value != "."), reverse=True):
        path = destination / name
        path.chmod(metadata[name]["mode"])
        os.utime(path, ns=(metadata[name]["mtime_ns"], metadata[name]["mtime_ns"]))
    return stale


def validate_seed(source, destination, expected, metadata, stale):
    from benchmarks.run import scan_tree
    actual = scan_tree(destination)
    if actual["counts"] != expected["counts"] or set(actual["entries"]) != set(expected["entries"]):
        raise ValueError("seed paths/counts differ")
    if any(expected["entries"].get(name, {}).get("type") != "file" for name in stale):
        raise ValueError("invalid stale paths")
    for name, entry in expected["entries"].items():
        wanted = {**entry, "sha256": stale[name]["stale_sha256"]} if name in stale else entry
        if actual["entries"][name] != wanted:
            raise ValueError(f"incorrect seed bytes or type: {name}")
        if entry["type"] == "file":
            left, right = (source / name).stat(), (destination / name).stat()
            if (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino):
                raise ValueError(f"seed is hardlinked to source: {name}")
    modes = validate_metadata(destination, metadata)
    for name, entry in metadata.items():
        wanted = entry["mtime_ns"] - 2000000000 if name in stale else entry["mtime_ns"]
        if (destination / name).stat().st_mtime_ns != wanted:
            raise ValueError(f"wrong seed mtime: {name}")
    return dict(ok=True, counts=actual["counts"], digest=actual["digest"], modes=modes,
                stale_files=len(stale), independent_files=True, exact_seed_mtimes=True)


def validate_source(source, expected, metadata):
    from benchmarks.run import validate_tree
    proof = validate_tree(source, source, expected)
    if not proof["ok"]:
        raise ValueError(f"source changed: {proof.get('error')}")
    try:
        proof["metadata"] = validate_metadata(source, metadata, timestamps=True)
    except ValueError as error:
        raise ValueError(f"source changed: {error}") from error
    return proof


def validate_summary(variant, text, expected):
    if variant["tool"] == "rcp":
        labels = (("files_copied", "files copied"), ("files_unchanged", "files unchanged"))
    else:
        labels = (("files_copied", "Number of regular files transferred"), ("bytes_copied", "Total transferred file size"))
    for name, label in labels:
        found = re.findall(rf"^{label}:\s*([0-9,]+)(?: bytes)?\s*$", text, re.MULTILINE)
        if len(found) != 1 or int(found[0].replace(",", "")) != expected[name]:
            raise ValueError(f"incorrect summary {label}: {found}; expected {expected[name]}")
    return dict(expected)
