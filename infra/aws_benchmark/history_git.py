"""Small Git environment and exact-ref helpers for trusted history operations."""

import os
import re

from .common import require


def git_env():
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_") and k not in ("GH_TOKEN", "GITHUB_TOKEN", "WRITER_TOKEN")}
    env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1", GIT_TERMINAL_PROMPT="0",
        GIT_AUTHOR_NAME="RCP AWS benchmark history", GIT_AUTHOR_EMAIL="benchmark-history@users.noreply.github.com",
        GIT_COMMITTER_NAME="RCP AWS benchmark history", GIT_COMMITTER_EMAIL="benchmark-history@users.noreply.github.com")
    git_config(env, "core.hooksPath", os.devnull)
    git_config(env, "commit.gpgsign", "false")
    return env


def git_config(environment, key, value):
    index = int(environment.get("GIT_CONFIG_COUNT", "0"))
    environment["GIT_CONFIG_KEY_" + str(index)] = key
    environment["GIT_CONFIG_VALUE_" + str(index)] = value
    environment["GIT_CONFIG_COUNT"] = str(index + 1)


def exact_ref(output, ref):
    require(isinstance(output, str) and len(output.encode()) <= 65536, "ref observation exceeds bound")
    selected = []
    for line in output.splitlines():
        fields = line.split("\t")
        require(len(fields) == 2 and re.fullmatch(r"[0-9a-f]{40}", fields[0]) and fields[1].startswith("refs/"),
                "malformed ref observation")
        if fields[1] == ref:
            selected.append(fields[0])
    require(len(selected) <= 1, "duplicate exact ref observation")
    return selected[0] if selected else ""
