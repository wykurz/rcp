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


def check_workflows(directory):
    expected = {(PUBLICATION_WORKFLOW, "publish"), (CANARY_WORKFLOW, "verify")}
    environments, secrets = set(), set()
    for path in sorted(directory.iterdir()):
        if path.suffix not in (".yaml", ".yml"):
            continue
        value = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
        text = "\n".join(strings(value))
        require(not re.search(r"tojson\s*\(\s*secrets\s*\)", text, re.IGNORECASE),
                "whole-secret serialization is forbidden")
        require(not re.search(r"\bsecrets\s*\[", text, re.IGNORECASE),
                "indexed secret expressions require separate credential review")
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
