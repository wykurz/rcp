"""Explicitly approved, bounded history-rules canary; never called by publication.

All sentinels have empty trees. No response ambiguity is a successful negative
control, and an unexpected allowed operation is never automatically repaired.
"""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.request

from .common import ValidationError, require
from .history_contract import HISTORY_REF, REPOSITORY
from .production_publish import git_env, history
from .results import json_value, read_file

REMOTE = "https://github.com/" + REPOSITORY + ".git"
API = "https://api.github.com/repos/" + REPOSITORY
EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
LIMIT = 65536
SHA = re.compile(r"[0-9a-f]{40}")


def identity(probe, fingerprint, revision):
    require(isinstance(probe, str) and re.fullmatch(r"[0-9a-f]{16}", probe), "invalid approved canary probe ID")
    require(isinstance(fingerprint, str) and re.fullmatch(r"[0-9a-f]{64}", fingerprint), "trust readback fingerprint required")
    require(isinstance(revision, str) and SHA.fullmatch(revision), "reviewed main revision required")
    return dict(probe=probe, trust_sha256=fingerprint, revision=revision)


def control_refs(probe):
    require(re.fullmatch(r"[0-9a-f]{16}", probe), "invalid probe ID")
    return tuple("refs/heads/rcp-history-canary-" + probe + "-" + suffix for suffix in ("source", "renamed"))


def rule_rejection(code, stdout, stderr, ref):
    # require an exact server rejection; auth, stale-lease and client failures do not qualify.
    rows = stdout.splitlines()
    rejected = [row.split("\t") for row in rows if row.startswith("!\t")]
    return (code != 0 and len(rejected) == 1 and len(rejected[0]) == 3 and
        rejected[0][1].rsplit(":", 1)[-1] == ref and
        rejected[0][2].startswith("[remote rejected]") and
        re.search(r"\bGH013: Repository rule violations found for " + re.escape(ref) + r"\.", stderr) is not None)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


class Remote:
    def __init__(self, directory, ordinary, writer=None, *, resume=False, clock=time.monotonic):
        self.directory, self.tokens, self.clock = Path(directory), dict(ordinary=ordinary, writer=writer), clock
        self.deadline = clock() + 420
        require(isinstance(ordinary, str) and ordinary, "ordinary canary credential missing")
        if resume:
            require(self.directory.is_dir() and not self.directory.is_symlink(), "ordinary sentinel objects missing")
            result = self.git("--git-dir=" + str(self.directory), "rev-parse", "--is-bare-repository")
            require(result.returncode == 0 and result.stdout.strip() == "true", "canary repository is not bare")
        else:
            self.directory.mkdir(mode=0o700, exist_ok=False)
            require(self.git("init", "--bare", str(self.directory)).returncode == 0, "canary repository creation failed")

    def remaining(self):
        remaining = self.deadline - self.clock()
        require(remaining > 0, "canary deadline exhausted")
        return min(20, remaining)

    def git(self, *args, token=None, input_data=None):
        env = git_env()
        if token:
            require(self.tokens.get(token), "canary credential missing")
            env.update(GIT_CONFIG_COUNT="3", GIT_CONFIG_KEY_2="http.https://github.com/.extraheader",
                GIT_CONFIG_VALUE_2="AUTHORIZATION: basic " + base64.b64encode(
                    ("x-access-token:" + self.tokens[token]).encode()).decode())
        result = subprocess.run(["git", "-C", str(self.directory.parent), *args], input=input_data,
            capture_output=True, text=True, env=env, timeout=self.remaining())
        require(len(result.stdout.encode()) + len(result.stderr.encode()) <= LIMIT, "canary response exceeds bound")
        return result

    def refs(self):
        result = self.git("ls-remote", "--heads", REMOTE)
        require(result.returncode == 0, "anonymous ref observation failed")
        found = {}
        for line in result.stdout.splitlines():
            parts = line.split("\t")
            require(len(parts) == 2 and SHA.fullmatch(parts[0]) and parts[1].startswith("refs/heads/") and
                    parts[1] not in found, "ambiguous ref observation")
            found[parts[1]] = parts[0]
        return found

    def commit(self, parent=None, label="bootstrap"):
        result = self.git("--git-dir=" + str(self.directory), "hash-object", "-t", "tree", "-w", "--stdin", input_data="")
        require(result.returncode == 0 and result.stdout.strip() == EMPTY_TREE, "empty tree creation failed")
        args = ["--git-dir=" + str(self.directory), "commit-tree", EMPTY_TREE]
        if parent:
            require(SHA.fullmatch(parent), "invalid sentinel parent")
            args += ["-p", parent]
        result = self.git(*args, "-m", "Verify protected AWS history (empty tree): " + label)
        require(result.returncode == 0 and SHA.fullmatch(result.stdout.strip()), "sentinel creation failed")
        return result.stdout.strip()

    def push(self, actor, ref, commit, expected=None):
        args = ["--git-dir=" + str(self.directory), "push", "--porcelain"]
        if expected is not None:
            args += ["--force-with-lease=" + ref + ":" + expected]
        result = self.git(*args, REMOTE, (commit or "") + ":" + ref, token=actor)
        return dict(accepted=result.returncode == 0, rule_denial=rule_rejection(
            result.returncode, result.stdout, result.stderr, ref), response_sha256=hashlib.sha256(
                (result.stdout + result.stderr).encode()).hexdigest())

    def rename(self, source, destination):
        require(source.startswith("refs/heads/") and destination.startswith("refs/heads/"), "invalid rename target")
        url = API + "/branches/" + source.removeprefix("refs/heads/") + "/rename"
        request = urllib.request.Request(url, method="POST",
            data=json.dumps(dict(new_name=destination.removeprefix("refs/heads/"))).encode(),
            headers={"Authorization": "Bearer " + self.tokens["ordinary"], "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2022-11-28", "Content-Type": "application/json"})
        try:
            response = urllib.request.build_opener(NoRedirect()).open(request, timeout=self.remaining())
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read(LIMIT + 1)
            require(len(raw) <= LIMIT, "rename response exceeds bound")
            code = response.code
        # generic 403/422 is inconclusive; live acceptance must retain a policy-specific denial.
        message = json_value(raw, LIMIT).get("message", "")
        explicit = isinstance(message, str) and ("GH013" in message or "Repository rule violations found" in message)
        return dict(accepted=code == 201, rule_denial=code in (403, 422) and explicit,
                    response_sha256=hashlib.sha256(raw).hexdigest(), status=code)

    def settled_rename(self, source, destination, expected):
        for _ in range(5):
            refs = self.refs()
            if source not in refs and refs.get(destination) == expected:
                return refs
            time.sleep(min(1, self.remaining()))
        raise ValidationError("rename positive control did not settle")

    def verify_history(self, expected):
        result = self.git("--git-dir=" + str(self.directory), "fetch", "--no-tags", "--depth=1", REMOTE,
                          HISTORY_REF + ":" + HISTORY_REF)
        require(result.returncode == 0, "final history fetch failed")
        records, tip = history(self.directory)
        require(not records and tip == expected, "empty canary history is not publisher-compatible")


class Canary:
    def __init__(self, remote, evidence, binding):
        self.remote, self.path, self.binding = remote, Path(evidence), binding

    def record(self, stage, **values):
        # append a bounded public receipt before evaluating the outcome; never include tokens/raw diagnostics.
        value = dict(self.binding, stage=stage, **values)
        raw = json.dumps(value, sort_keys=True).encode() + b"\n"
        require(len(raw) < LIMIT, "canary receipt exceeds bound")
        descriptor = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "ab") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())

    def mutate(self, stage, operation, **intent):
        self.record(stage + "_intent", **intent)
        try:
            outcome = operation()
        except Exception as error:
            self.record(stage + "_uncertain", error_type=type(error).__name__)
            raise
        self.record(stage + "_outcome", outcome=outcome)
        return outcome

    def positive(self, stage, actor, ref, commit, expected=None):
        before = self.remote.refs()
        require(before.get(ref) == expected, "positive-control baseline changed; preserve state")
        outcome = self.mutate(stage, lambda: self.remote.push(actor, ref, commit, expected=expected or ""),
            actor=actor, ref=ref, commit=commit, expected=expected)
        refs = self.remote.refs()
        self.record(stage, outcome=outcome, ref=ref, observed=refs.get(ref))
        require(outcome["accepted"] and refs.get(ref) == commit, "canary positive control failed; activation held")

    def negative(self, stage, actor, ref, commit, expected=None):
        before = self.remote.refs()
        require(before.get(ref) == expected, "negative-control baseline differs")
        outcome = self.mutate(stage, lambda: self.remote.push(actor, ref, commit, expected=expected or ""),
            actor=actor, ref=ref, commit=commit, expected=expected)
        after = self.remote.refs()
        self.record(stage, outcome=outcome, ref=ref, before=expected, observed=after.get(ref))
        require(not outcome["accepted"] and outcome["rule_denial"] and after.get(ref) == expected,
                "negative control not proven; preserve state and hold activation")

    def rename(self, stage, source, destination, expected):
        observed = self.remote.refs()
        require(observed.get(source) == expected and destination not in observed,
                "rename baseline changed; preserve state")
        # GitHub's rename API has no CAS parameter; this is a fresh observation only.
        return self.mutate(stage, lambda: self.remote.rename(source, destination),
            actor="ordinary", source=source, destination=destination, expected=expected)

    def ordinary(self):
        require(not self.path.exists(), "canary receipt already exists; do not rerun")
        source, renamed = control_refs(self.binding["probe"])
        refs = self.remote.refs()
        require(not any(ref == HISTORY_REF or ref.startswith(HISTORY_REF + "/") for ref in refs) and
                source not in refs and renamed not in refs, "existing history namespace or probe is untrusted bootstrap state")
        self.record("initial_absence", controls=[source, renamed], history_absent=True)
        root = self.remote.commit()
        self.positive("ordinary_create_control", "ordinary", source, root)
        result = self.rename("ordinary_rename_control", source, renamed, root)
        require(result["accepted"], "ordinary rename positive control failed")
        refs = self.remote.settled_rename(source, renamed, root)
        self.record("ordinary_rename_control", source_absent=source not in refs, destination=refs.get(renamed))
        self.negative("ordinary_create_history", "ordinary", HISTORY_REF, root)
        result = self.rename("ordinary_rename_history", renamed, HISTORY_REF, root)
        refs = self.remote.refs()
        self.record("ordinary_rename_history", outcome=result, source=refs.get(renamed), destination=refs.get(HISTORY_REF))
        require(not result["accepted"] and result["rule_denial"] and refs.get(renamed) == root and HISTORY_REF not in refs,
                "history rename denial is inconclusive; preserve evidence and hold activation")
        self.record("ordinary_complete", root=root)
        return root

    def writer(self, root):
        _, renamed = control_refs(self.binding["probe"])
        refs = self.remote.refs()
        require(refs.get(renamed) == root and not any(ref == HISTORY_REF or ref.startswith(HISTORY_REF + "/") for ref in refs),
                "pre-mint bootstrap state changed")
        self.positive("writer_create_history", "writer", HISTORY_REF, root)
        child = self.remote.commit(root)
        self.positive("writer_fast_forward", "writer", HISTORY_REF, child, root)
        grandchild = self.remote.commit(child)
        self.negative("ordinary_fast_forward", "ordinary", HISTORY_REF, grandchild, child)
        # a distinct root object requests an actual rewrite, not a client-side non-fast-forward failure.
        unrelated = self.remote.commit(label="non-fast-forward probe")
        require(unrelated not in (root, child), "non-fast-forward sentinel must differ")
        for actor in ("writer", "ordinary"):
            self.negative(actor + "_non_fast_forward", actor, HISTORY_REF, unrelated, child)
            self.negative(actor + "_delete", actor, HISTORY_REF, None, child)
        self.remote.verify_history(child)
        self.record("publisher_compatible", history_tip=child, records=0, tree=EMPTY_TREE)
        # only the explicitly named positive-control ref is removed, after all history checks passed.
        outcome = self.mutate("control_cleanup", lambda: self.remote.push("ordinary", renamed, None, expected=root),
            actor="ordinary", ref=renamed, commit=None, expected=root)
        refs = self.remote.refs()
        self.record("control_cleanup", outcome=outcome, ref=renamed, absent=renamed not in refs)
        require(outcome["accepted"] and renamed not in refs and refs.get(HISTORY_REF) == child,
                "positive-control cleanup requires reconciliation")
        self.record("complete", history_tip=child, live_policy_checks=True, readiness_changed=False)


def prior_root(path, binding):
    raw = read_file(Path(path), LIMIT)
    rows = [json_value(row, LIMIT) for row in raw.splitlines()]
    require(rows and all(all(row.get(key) == value for key, value in binding.items()) for row in rows), "canary binding differs")
    last = rows[-1]
    require(last.get("stage") == "ordinary_complete" and isinstance(last.get("root"), str) and SHA.fullmatch(last["root"]),
            "ordinary canary did not complete; do not mint or resume")
    return last["root"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("ordinary", "writer"))
    for field in ("probe", "trust-sha256", "revision", "directory", "evidence"):
        parser.add_argument("--" + field, required=True)
    args = parser.parse_args()
    binding = identity(args.probe, args.trust_sha256, args.revision)
    directory = Path(args.directory)
    if args.phase == "ordinary":
        remote = Remote(directory, os.environ.get("GH_TOKEN"))
        Canary(remote, args.evidence, binding).ordinary()
    else:
        root = prior_root(args.evidence, binding)
        remote = Remote(directory, os.environ.get("GH_TOKEN"), os.environ.get("WRITER_TOKEN"), resume=True)
        require(remote.tokens["writer"], "writer canary credential missing")
        Canary(remote, args.evidence, binding).writer(root)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        import sys
        print(str(error) if isinstance(error, ValidationError) else "Canary held: " + type(error).__name__, file=sys.stderr)
        raise SystemExit(1) from None
