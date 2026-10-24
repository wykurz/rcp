"""Publication identity checks; fresh main reads need neither cloud nor write access."""

import argparse
import base64
import os
import re
import subprocess

from .common import ValidationError, require
from .history_contract import CANARY_WORKFLOW, OWNER_ID, PUBLICATION_WORKFLOW, REPOSITORY, identifier
from .history_git import exact_ref, git_config, git_env

MAIN = "refs/heads/main"
REMOTE = "https://github.com/" + REPOSITORY + ".git"


def check_identity(environment, *, workflow=PUBLICATION_WORKFLOW, artifact=False, flags=False):
    revision = environment.get("GITHUB_SHA", "")
    expected = dict(GITHUB_REPOSITORY=REPOSITORY, GITHUB_REF=MAIN,
        GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_ACTOR_ID=str(OWNER_ID), GITHUB_RUN_ATTEMPT="1",
        GITHUB_WORKFLOW_REF=REPOSITORY + "/.github/workflows/" + workflow + "@" + MAIN,
        GITHUB_WORKFLOW_SHA=revision)
    require(re.fullmatch(r"[0-9a-f]{40}", revision) and
            all(environment.get(key) == value for key, value in expected.items()), "publication workflow identity differs")
    if artifact:
        require(environment.get("EXPECTED_READER") == revision and
                re.fullmatch(r"[0-9a-f]{64}", environment.get("EXPECTED_INVENTORY", "")) and
                re.fullmatch(r"[1-9][0-9]*", environment.get("EXPECTED_ARTIFACT", "")), "publication artifact identity differs")
    if flags:
        # these job-context values are defense in depth, not a live Variables API read
        require(environment.get("PUBLICATION_ENABLED") == "true" and environment.get("TRUST_READY") == "true",
                "publication or history readiness is closed")
    return revision


def current_main(revision, *, token=None):
    require(isinstance(revision, str) and re.fullmatch(r"[0-9a-f]{40}", revision), "invalid publication revision")
    clean = git_env()
    for key in ("GH_TOKEN", "GITHUB_TOKEN"):
        clean.pop(key, None)
    if token is not None:
        require(isinstance(token, str) and token, "ordinary canary read credential missing")
        git_config(clean, "http.https://github.com/.extraheader", "AUTHORIZATION: basic " +
            base64.b64encode(("x-access-token:" + token).encode()).decode())
    result = subprocess.run(["git", "-c", "credential.helper=", "ls-remote", "--exit-code", "--heads", REMOTE, MAIN],
        env=clean, capture_output=True, text=True, timeout=30, check=False)
    require(result.returncode == 0, "current main observation failed")
    require(exact_ref(result.stdout, MAIN) == revision,
            "publication revision is no longer the exact current main")


def canary_identity(environment):
    revision = check_identity(environment, workflow=CANARY_WORKFLOW)
    require(environment.get("CANARY_ENABLED") == "true" and environment.get("TRUST_READY") == "false" and
            environment.get("PUBLISH_ENABLED") == "false", "canary flags differ; close pending approvals when disabling")
    require(environment.get("CANARY_MODE", "bootstrap") in ("bootstrap", "resume"), "canary mode differs")
    if environment.get("CANARY_MODE", "bootstrap") == "bootstrap":
        require(not any(environment.get(key) for key in ("SOURCE_RUN_ID", "SOURCE_ARTIFACT_ID", "SOURCE_SHA256")),
                "bootstrap cannot carry recovery inputs")
    else:
        identifier(environment.get("SOURCE_RUN_ID", ""))
        identifier(environment.get("SOURCE_ARTIFACT_ID", ""))
        require(re.fullmatch(r"[0-9a-f]{64}", environment.get("SOURCE_SHA256", "")), "approved source receipt hash required")
    from .production_trust import writer_config
    writer_config(environment.get("CLIENT_ID", ""), environment.get("INSTALLATION_ID", ""))
    return revision


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("identity", "current-main", "canary", "canary-current-main", "installation"))
    parser.add_argument("--artifact", action="store_true")
    parser.add_argument("--flags", action="store_true")
    args = parser.parse_args()
    if args.command == "identity":
        check_identity(os.environ, artifact=args.artifact, flags=args.flags)
    elif args.command == "current-main":
        current_main(check_identity(os.environ))
    elif args.command in ("canary", "canary-current-main"):
        revision = canary_identity(os.environ)
        if args.command == "canary-current-main":
            current_main(revision, token=os.environ.get("GH_TOKEN", ""))
    else:
        require(identifier(os.environ.get("EXPECTED_INSTALLATION", "")) ==
                identifier(os.environ.get("ACTUAL_INSTALLATION", "")), "writer installation differs")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        reason = str(error) if isinstance(error, ValidationError) else "identity observation unavailable"
        raise SystemExit("Publication identity check failed: " + reason) from None
