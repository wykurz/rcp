"""Publication identity checks; fresh main reads need neither cloud nor write access."""

import argparse
import os
import re
import subprocess

from .common import require
from .history_contract import OWNER_ID, PUBLICATION_WORKFLOW, REPOSITORY
from .production_publish import git_env
from .production_trust import identifier

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


def current_main(revision):
    require(isinstance(revision, str) and re.fullmatch(r"[0-9a-f]{40}", revision), "invalid publication revision")
    clean = git_env()
    for key in ("GH_TOKEN", "GITHUB_TOKEN"):
        clean.pop(key, None)
    result = subprocess.run(["git", "-c", "credential.helper=", "ls-remote", "--exit-code", "--heads", REMOTE, MAIN],
        env=clean, capture_output=True, text=True, timeout=30, check=False)
    require(result.returncode == 0 and result.stdout == revision + "\t" + MAIN + "\n",
            "publication revision is no longer the exact current main")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("identity", "current-main", "installation"))
    parser.add_argument("--artifact", action="store_true")
    parser.add_argument("--flags", action="store_true")
    args = parser.parse_args()
    if args.command == "identity":
        check_identity(os.environ, artifact=args.artifact, flags=args.flags)
    elif args.command == "current-main":
        current_main(check_identity(os.environ))
    else:
        require(identifier(os.environ.get("EXPECTED_INSTALLATION", "")) ==
                identifier(os.environ.get("ACTUAL_INSTALLATION", "")), "writer installation differs")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.SubprocessError):
        raise SystemExit("Publication identity check failed or is unavailable.") from None
