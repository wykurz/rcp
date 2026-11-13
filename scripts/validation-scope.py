#!/usr/bin/env python3
"""Classify complete PR diffs for documentation or version-only validation."""

from pathlib import PurePosixPath
import re
import subprocess
import sys
import tomllib


def git(*args: str) -> bytes:
    return subprocess.check_output(["git", *args])


def is_documentation(path: str) -> bool:
    path = PurePosixPath(path)
    if path.suffix.lower() in {".md", ".markdown", ".rst", ".adoc"}:
        return True
    return path.parts[0] in {"docs", "assets"} and path.suffix.lower() in {
        ".svg", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico", ".pdf", ".html",
    }


def valid_version(version: str) -> bool:
    match = re.fullmatch(
        r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
        r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
        r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?", version,
    )
    return match is not None and all(
        not (part.isdigit() and len(part) > 1 and part.startswith("0"))
        for part in (match[4] or "").split(".")
    )


def read_toml(commit: str, path: str) -> dict:
    return tomllib.loads(git("show", f"{commit}:{path}").decode("utf-8"))


def same_metadata(left, right) -> bool:
    # Python equates true with 1 and 1.0, but Cargo can interpret these TOML values differently
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(same_metadata(left[k], right[k]) for k in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(same_metadata(a, b) for a, b in zip(left, right))
    return left == right


def workspace_version_only(base: str, head: str) -> bool:
    """Accept exactly the version changes made by update-version.sh.

    Compare parsed snapshots, discovering package names from workspace members. Unsupported
    metadata layouts fall back to full validation; missing Git evidence remains an error.
    """
    try:
        before = read_toml(base, "Cargo.toml")
        after = read_toml(head, "Cargo.toml")
        old_version = before["workspace"]["package"]["version"]
        new_version = after["workspace"]["package"]["version"]
        if old_version == new_version or not all(map(valid_version, (old_version, new_version))):
            return False

        members = before["workspace"]["members"]
        if not isinstance(members, list) or not members or "package" in before:
            return False
        packages = {}
        for member in members:
            path = PurePosixPath(member)
            # explicit relative member paths are sufficient for this workspace; globs need
            # Cargo expansion, so do not guess at them when deciding to omit Cargo checks
            if path.is_absolute() or ".." in path.parts or re.search(r"[*?\[]", member):
                return False
            manifest = read_toml(base, f"{path}/Cargo.toml")["package"]
            name = manifest["name"]
            if manifest["version"] != {"workspace": True} or name in packages.values():
                return False
            packages[str(path)] = name

        before["workspace"]["package"]["version"] = new_version
        for alias, dependency in before["workspace"].get("dependencies", {}).items():
            if not isinstance(dependency, dict) or "path" not in dependency:
                continue
            name = packages.get(str(PurePosixPath(dependency["path"])))
            if name is None:
                continue
            if dependency.get("package", alias) != name or dependency.get("version") != old_version:
                return False
            dependency["version"] = new_version
        if not same_metadata(before, after):
            return False

        lock = read_toml(base, "Cargo.lock")
        updated = set()
        for package in lock["package"]:
            name = package["name"]
            if "source" in package or name not in packages.values():
                continue
            if name in updated or package["version"] != old_version:
                return False
            package["version"] = new_version
            updated.add(name)
        # full equality also protects dependency edges, checksums, sources and lockfile schema
        return updated == set(packages.values()) and same_metadata(lock, read_toml(head, "Cargo.lock"))
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def validation_scope(base: str, head: str) -> str:
    if not all(re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (base, head)):
        raise ValueError("validation scope requires full base and head commit SHAs")
    bases = git("merge-base", "--all", base, head).decode("ascii").splitlines()
    if len(bases) != 1:
        raise ValueError("validation scope requires one merge base")
    base = bases[0]
    # disable rename detection so moving code into a documentation path cannot hide its origin
    diff = git("diff", "--no-ext-diff", "--raw", "-z", "--no-renames", base, head, "--")
    if not diff:
        return "full"
    records = diff.split(b"\0")
    if records[-1] or len(records) % 2 != 1:
        raise ValueError("invalid raw Git diff")
    metadata = set()
    for header, raw_path in zip(records[0:-1:2], records[1:-1:2]):
        fields = header.split()
        if len(fields) != 5 or tuple(fields[:2] + fields[4:]) not in {
            (b":100644", b"100644", b"M"),
            (b":000000", b"100644", b"A"),
            (b":100644", b"000000", b"D"),
        }:
            return "full"
        path = raw_path.decode("utf-8", errors="surrogateescape")
        if is_documentation(path):
            continue
        if path not in {"Cargo.toml", "Cargo.lock"} or fields[4] != b"M":
            return "full"
        metadata.add(path)
    if not metadata:
        return "docs"
    if metadata == {"Cargo.toml", "Cargo.lock"} and workspace_version_only(base, head):
        return "version"
    return "full"


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: validation-scope.py BASE_SHA HEAD_SHA", file=sys.stderr)
        return 2
    try:
        mode = validation_scope(*sys.argv[1:])
    except (ValueError, subprocess.CalledProcessError) as error:
        print(f"cannot determine validation scope: {error}", file=sys.stderr)
        return 1
    print(mode)
    return 0


if __name__ == "__main__":
    sys.exit(main())
