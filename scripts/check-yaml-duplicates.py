#!/usr/bin/env python3
"""Reject duplicate mapping keys in repository YAML files."""

from __future__ import annotations

import sys
from pathlib import Path

import yaml
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode


class DuplicateKeyError(ValueError):
    """Report a duplicate key with its YAML path and source locations."""


def child_path(path: str, key: str) -> str:
    if key.replace("-", "_").replace("_", "a").isalnum():
        return f"{path}.{key}"
    return f"{path}[{key!r}]"


def reject_duplicate_keys(node: Node, path: str = "$", visited: set[int] | None = None) -> None:
    if visited is None:
        visited = set()
    if id(node) in visited:
        return
    visited.add(id(node))

    if isinstance(node, MappingNode):
        seen: dict[str, ScalarNode] = {}
        for key_node, value_node in node.value:
            if not isinstance(key_node, ScalarNode):
                raise ValueError(
                    f"unsupported non-scalar YAML mapping key at {path} "
                    f"(line {key_node.start_mark.line + 1})"
                )
            key = key_node.value
            if key in seen:
                first = seen[key]
                raise DuplicateKeyError(
                    f"duplicate YAML mapping key {key!r} at {path} "
                    f"(line {key_node.start_mark.line + 1}, first declared on "
                    f"line {first.start_mark.line + 1})"
                )
            seen[key] = key_node
            reject_duplicate_keys(value_node, child_path(path, key), visited)
    elif isinstance(node, SequenceNode):
        for index, child in enumerate(node.value):
            reject_duplicate_keys(child, f"{path}[{index}]", visited)


def validate(path: Path) -> None:
    with path.open(encoding="utf-8") as stream:
        for document in yaml.compose_all(stream, Loader=yaml.BaseLoader):
            if document is not None:
                reject_duplicate_keys(document)


def main(arguments: list[str]) -> int:
    if not arguments:
        print("usage: check-yaml-duplicates.py FILE...", file=sys.stderr)
        return 2

    failed = False
    for argument in arguments:
        path = Path(argument)
        try:
            validate(path)
        except (OSError, ValueError, yaml.YAMLError) as error:
            print(f"{path}: {error}", file=sys.stderr)
            failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
