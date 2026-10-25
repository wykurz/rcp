#!/usr/bin/env python3
"""Map a matching Cargo release version to RPM's version alphabet."""

import re
import sys
import tomllib
from pathlib import Path


VERSION = re.compile(
    r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
)


def rpm_version(version: str) -> str:
    match = VERSION.fullmatch(version)
    if match is None:
        raise ValueError("expected a Cargo semantic version")
    major, minor, patch, prerelease, metadata = match.groups()
    if prerelease and any(part.isdigit() and len(part) > 1 and part[0] == "0"
                          for part in prerelease.split(".")):
        raise ValueError("numeric prerelease identifiers must not have leading zeros")
    result = f"{major}.{minor}.{patch}"
    # RPM's ~ precedes the corresponding stable version; _ is outside Cargo's identifier alphabet
    if prerelease:
        result += "~" + prerelease.replace("-", "_")
    if metadata:
        result += "+" + metadata.replace("-", "_")
    return result


def main(arguments: list[str]) -> int:
    if len(arguments) != 1:
        print("usage: release-rpm-version.py RELEASE_TAG", file=sys.stderr)
        return 2
    try:
        with Path("Cargo.toml").open("rb") as stream:
            version = tomllib.load(stream)["workspace"]["package"]["version"]
        if not isinstance(version, str) or arguments[0] != "v" + version:
            raise ValueError("release tag must match workspace.package.version")
        print(rpm_version(version))
    except (OSError, KeyError, TypeError, ValueError) as error:
        print(f"RPM version: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
