"""Offline inventory of the reviewed history credential consumers."""

import re

import yaml

from .common import require
from .history_contract import CANARY_WORKFLOW, ENVIRONMENT, PUBLICATION_WORKFLOW, WRITER_SECRET_NAME


def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from strings(key)
            yield from strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from strings(item)


def check_secret_expressions(value):
    # this is a future-edit guard, not a proof that arbitrary trusted-main code is safe
    # lex expressions so quoted text is inert and function/grouping spelling cannot hide a context
    token_pattern = re.compile(r"\s+|'(?:[^']|'')*'|[A-Za-z_][A-Za-z0-9_-]*|.", re.DOTALL)
    cursor = 0
    while (start := value.find("${{", cursor)) >= 0:
        cursor = start + 3
        tokens = []
        while cursor < len(value) and not value.startswith("}}", cursor):
            token = token_pattern.match(value, cursor).group()
            cursor += len(token)
            if token.isspace():
                continue
            require(token != "'", "unterminated workflow expression string")
            tokens.append(None if token.startswith("'") else token)
        require(value.startswith("}}", cursor), "unterminated workflow expression")
        cursor += 2
        for index, token in enumerate(tokens):
            if token is None or token.casefold() != "secrets" or index > 0 and tokens[index - 1] == ".":
                continue
            require(index + 2 < len(tokens) and tokens[index + 1] == "." and
                    re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", tokens[index + 2] or ""),
                    "only explicit named secret references are allowed")


def check_workflows(directory):
    expected = {(PUBLICATION_WORKFLOW, "publish"), (CANARY_WORKFLOW, "verify")}
    environments, secrets = set(), set()
    for path in sorted(directory.iterdir()):
        if path.suffix not in (".yaml", ".yml"):
            continue
        value = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
        values = list(strings(value))
        for text in values:
            check_secret_expressions(text)
        text = "\n".join(values)
        key_count = text.casefold().count(WRITER_SECRET_NAME.casefold())
        require(key_count == (1 if path.name in (PUBLICATION_WORKFLOW, CANARY_WORKFLOW) else 0),
                "history key references differ from reviewed mint steps")
        for name, job in value.get("jobs", {}).items():
            location = (path.name, name)
            environment = job.get("environment")
            label = environment.get("name") if isinstance(environment, dict) else environment
            require(label is None or isinstance(label, str) and "${{" not in label,
                    "expression-valued environments require separate credential review")
            if label and label.casefold() == ENVIRONMENT.casefold():
                environments.add(location)
            text = "\n".join(strings(job))
            if WRITER_SECRET_NAME.casefold() in text.casefold():
                secrets.add(location)
    require(environments == expected and secrets == expected, "history environment or key consumers differ from reviewed lanes")
    return expected
