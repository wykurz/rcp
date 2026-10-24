"""Push one prevalidated commit, retaining its exact baseline across retries."""

import base64
import os
from pathlib import Path
import re
import subprocess
import time

from .common import ValidationError, require
from .history_contract import HISTORY_REF
from .history_git import exact_ref, git_config, git_env
from .publication_identity import REMOTE, check_identity, current_main

SHA = re.compile(r"[0-9a-f]{40}")


def append(repository, commit, baseline, token, revision, *, remote=REMOTE, main_check=current_main):
    require(isinstance(commit, str) and SHA.fullmatch(commit), "invalid prepared commit")
    require(isinstance(baseline, str) and SHA.fullmatch(baseline), "protected history baseline missing; reconcile before publishing")
    require(isinstance(token, str) and token, "writer token missing")
    deadline = time.monotonic() + 180
    def command(*args, write=False):
        remaining = deadline - time.monotonic()
        require(remaining > 0, "publication reconciliation deadline exhausted")
        env = git_env()
        if write:
            git_config(env, "http.https://github.com/.extraheader", "AUTHORIZATION: basic " +
                base64.b64encode(("x-access-token:" + token).encode()).decode())
        return subprocess.run(["git", "-c", "credential.helper=", "--git-dir=" + str(repository), *args],
            env=env, text=True, capture_output=True, timeout=min(30, remaining), check=False)
    parents = command("rev-list", "--parents", "-n", "1", commit)
    require(parents.returncode == 0 and parents.stdout.strip().split() == [commit, baseline],
            "prepared commit is not the exact direct child of its approved baseline")
    def observe():
        result = command("ls-remote", "--exit-code", "--heads", remote, HISTORY_REF)
        require(result.returncode in (0, 2), "history readback unavailable; publication held")
        tip = exact_ref(result.stdout, HISTORY_REF)
        require(result.returncode == 0 or not tip, "history absence response conflicts")
        require(tip in (baseline, commit), "history moved to a foreign tip; publication held")
        return tip
    for attempt in range(3):
        if observe() == commit:
            return commit
        main_check(revision)
        try:
            command("push", "--porcelain", "--force-with-lease=" + HISTORY_REF + ":" + baseline,
                    remote, commit + ":" + HISTORY_REF, write=True)
        except subprocess.TimeoutExpired:
            # a timed-out push is uncertain until the exact ref is observed
            pass
        if observe() == commit:
            return commit
    raise ValidationError("history publication requires reconciliation after three bounded attempts")


def main():
    revision = check_identity(os.environ)
    result = append(Path(os.environ["RUNNER_TEMP"]) / "publish-history-1.git", os.environ["PREPARED_COMMIT"],
        os.environ["PREPARED_BASELINE"], os.environ["GH_TOKEN"], revision)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
        stream.write("history_commit=" + result + "\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        reason = str(error) if isinstance(error, ValidationError) else "publication observation unavailable"
        raise SystemExit("Publication held: " + reason) from None
