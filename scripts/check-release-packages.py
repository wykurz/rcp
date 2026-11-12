#!/usr/bin/env python3
"""Check package archive filenames before bundling, without installing or executing them."""
import argparse
from collections import Counter
from pathlib import Path
import re
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[1]
ARCHITECTURES = {"amd64": {"deb": "amd64", "rpm": "x86_64"},
                 "arm64": {"deb": "arm64", "rpm": "aarch64"}}


def packages(root, kind):
    # packaging metadata, not a second hand-maintained tool list, defines the inventory
    with (root / "Cargo.toml").open("rb") as stream:
        members = tomllib.load(stream)["workspace"]["members"]
    result = {}
    key = "deb" if kind == "deb" else "generate-rpm"
    for member in members:
        with (root / member / "Cargo.toml").open("rb") as stream:
            package = tomllib.load(stream)["package"]
        metadata = package.get("metadata", {})
        if key in metadata:
            name = metadata[key].get("name", package["name"])
            if name in result.values():
                raise ValueError(f"duplicate {kind} package name: {name}")
            result[member] = name
    if not result:
        raise ValueError(f"no {kind} packages declared in workspace")
    return result


def validate(directory, kind, arch, expected):
    actual = Counter()
    architecture = ARCHITECTURES[arch][kind]
    # versions are deliberately opaque here; release tag/version validation is separate
    suffix = (rf"_[^_/]+_{architecture}\.deb" if kind == "deb"
              else rf"-[^/-]+-[^/-]+\.{architecture}\.rpm")
    for path in sorted(directory.glob("*." + kind)):
        if not path.is_file() or path.is_symlink() or path.stat().st_size == 0:
            raise ValueError(f"package archive is not a nonempty regular file: {path.name}")
        matches = [name for name in expected if re.fullmatch(re.escape(name) + suffix, path.name)]
        if len(matches) != 1:
            raise ValueError(f"unexpected {kind}/{arch} package archive: {path.name}")
        actual[matches[0]] += 1
    missing = sorted(set(expected) - actual.keys())
    duplicates = sorted(name for name, count in actual.items() if count != 1)
    if missing or duplicates:
        raise ValueError(f"{kind}/{arch} inventory mismatch: missing={missing}, duplicates={duplicates}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("deb", "rpm"))
    parser.add_argument("arch", choices=ARCHITECTURES)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--source-root", type=Path, default=ROOT,
                        help="release source containing Cargo manifests (defaults to this checkout)")
    args = parser.parse_args()
    try:
        expected = packages(args.source_root, args.kind).values()
        validate(args.directory, args.kind, args.arch, expected)
    except (OSError, KeyError, TypeError, ValueError) as error:
        print(f"Release package inventory: {error}", file=sys.stderr)
        return 1
    print(f"Validated {args.kind}/{args.arch} inventory: " + ", ".join(sorted(expected)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
