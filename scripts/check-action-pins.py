#!/usr/bin/env python3
"""Require immutable, consistent action pins in GitHub and Depot automation."""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode


FULL_SHA = re.compile(r"[0-9a-fA-F]{40}")


@dataclass(frozen=True)
class ActionUse:
    """An external action reference and its source location."""

    locator: str
    revision: str
    path: Path
    line: int


def action_uses(node: Node, path: Path, visited: set[int] | None = None) -> list[ActionUse]:
    """Collect scalar `uses` values without interpreting comments as YAML data."""

    if visited is None:
        visited = set()
    if id(node) in visited:
        return []
    visited.add(id(node))

    found: list[ActionUse] = []
    if isinstance(node, MappingNode):
        for key_node, value_node in node.value:
            if isinstance(key_node, ScalarNode) and key_node.value == "uses":
                if not isinstance(value_node, ScalarNode):
                    raise ValueError(
                        f"{path}:{value_node.start_mark.line + 1}: uses must be a scalar"
                    )
                value = value_node.value
                if not value.startswith(("./", "docker://")):
                    locator, separator, revision = value.rpartition("@")
                    if not separator or not locator:
                        revision = ""
                        locator = value
                    found.append(
                        ActionUse(
                            locator=locator,
                            revision=revision,
                            path=path,
                            line=value_node.start_mark.line + 1,
                        )
                    )
            found.extend(action_uses(value_node, path, visited))
    elif isinstance(node, SequenceNode):
        for child in node.value:
            found.extend(action_uses(child, path, visited))
    return found


def workflow_files(root: Path) -> list[Path]:
    """Return action-bearing YAML files from both automation trees."""

    files: list[Path] = []
    for relative_root in (Path(".github"), Path(".depot")):
        tree = root / relative_root
        if not tree.is_dir():
            continue
        files.extend(path for path in tree.rglob("*.yml") if path.is_file())
        files.extend(path for path in tree.rglob("*.yaml") if path.is_file())
    return sorted(files)


def check(root: Path) -> list[str]:
    """Return diagnostics for mutable or divergent external action references."""

    diagnostics: list[str] = []
    uses: list[ActionUse] = []
    for path in workflow_files(root):
        try:
            with path.open(encoding="utf-8") as stream:
                for document in yaml.compose_all(stream, Loader=yaml.BaseLoader):
                    if document is not None:
                        uses.extend(action_uses(document, path))
        except (OSError, ValueError, yaml.YAMLError) as error:
            diagnostics.append(str(error))

    pinned: dict[str, dict[str, list[ActionUse]]] = {}
    for use in uses:
        if not FULL_SHA.fullmatch(use.revision):
            diagnostics.append(
                f"{use.path}:{use.line}: {use.locator}@{use.revision} must use a full "
                "40-character commit SHA"
            )
            continue
        pinned.setdefault(use.locator, {}).setdefault(use.revision.lower(), []).append(use)

    for locator, revisions in sorted(pinned.items()):
        if len(revisions) <= 1:
            continue
        locations = ", ".join(
            f"{use.path}:{use.line} ({use.revision})"
            for revision_uses in revisions.values()
            for use in revision_uses
        )
        diagnostics.append(f"action {locator} uses inconsistent pins: {locations}")
    return diagnostics


def main(arguments: list[str]) -> int:
    root = Path(__file__).resolve().parent.parent
    if arguments[:1] == ["--root"]:
        if len(arguments) != 2:
            print("usage: check-action-pins.py [--root REPOSITORY]", file=sys.stderr)
            return 2
        root = Path(arguments[1]).resolve()
    elif arguments:
        print("usage: check-action-pins.py [--root REPOSITORY]", file=sys.stderr)
        return 2
    if not root.is_dir():
        print(f"repository root does not exist: {root}", file=sys.stderr)
        return 2

    diagnostics = check(root)
    for diagnostic in diagnostics:
        print(diagnostic, file=sys.stderr)
    if diagnostics:
        return 1
    print("Action pins are immutable and consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
