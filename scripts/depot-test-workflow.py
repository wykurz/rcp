#!/usr/bin/env python3
"""Render the focused Depot workflow with literal nextest arguments."""

import base64
import json
from pathlib import Path
import sys

import yaml


def main():
    template = Path(__file__).resolve().parents[1] / ".depot/workflows/test.yml"
    workflow = yaml.safe_load(template.read_text())
    # the YAML 1.1 loader treats an unquoted on key as a boolean
    if True in workflow:
        workflow["on"] = workflow.pop(True)
    arguments = json.dumps(sys.argv[1:]).encode()
    # keep user arguments out of both shell source and Actions expression interpolation
    workflow["env"]["RCP_DEPOT_TEST_ARGS_B64"] = base64.b64encode(arguments).decode()
    json.dump(workflow, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
