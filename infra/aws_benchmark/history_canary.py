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
import urllib.parse
import urllib.request

from .common import ValidationError, require
from .history_contract import HISTORY_REF, REPOSITORY
from .history_git import git_env, git_config
from .production_publish import history
from .results import json_value, read_file

REMOTE = "https://github.com/" + REPOSITORY + ".git"
API = "https://api.github.com/repos/" + REPOSITORY
EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
LIMIT = 65536
RECEIPT_LIMIT = 1024 * 1024
SHA = re.compile(r"[0-9a-f]{40}")
WRITER_STAGES = ("writer_create_history", "writer_fast_forward", "ordinary_fast_forward",
    "writer_non_fast_forward", "writer_delete", "ordinary_non_fast_forward", "ordinary_delete", "control_cleanup")


def identity(probe, fingerprint, revision):
    require(isinstance(probe, str) and re.fullmatch(r"[0-9a-f]{16}", probe), "invalid approved canary probe ID")
    require(isinstance(fingerprint, str) and re.fullmatch(r"[0-9a-f]{64}", fingerprint), "trust readback fingerprint required")
    require(isinstance(revision, str) and SHA.fullmatch(revision), "reviewed main revision required")
    return dict(probe=probe, trust_sha256=fingerprint, revision=revision)


def control_refs(probe):
    require(re.fullmatch(r"[0-9a-f]{16}", probe), "invalid probe ID")
    return tuple("refs/heads/rcp-history-canary-" + probe + "-" + suffix for suffix in ("source", "renamed"))


def rule_rejection(code, stdout, stderr, ref):
    # require an exact server rejection; auth, stale-lease and client failures do not qualify
    rejected = [row.split("\t") for row in stdout.splitlines() if row.startswith("!\t")]
    return (code != 0 and len(rejected) == 1 and len(rejected[0]) == 3 and
        rejected[0][1].rsplit(":", 1)[-1] == ref and
        rejected[0][2].startswith("[remote rejected]") and
        re.search(r"\bGH013: Repository rule violations found for " + re.escape(ref) + r"\.", stderr) is not None)


def rename_denial(code, value, source, destination):
    require(isinstance(value, dict), "rename response must be an object")
    message = value.get("message", "")
    errors = value.get("errors", [])
    require(isinstance(message, str) and isinstance(errors, list), "rename diagnostic shape differs")
    texts = [message] + [error if isinstance(error, str) else error.get("message", "")
                        for error in errors if isinstance(error, (str, dict))]
    exact = "repository rules do not permit renaming branch '" + source.removeprefix("refs/heads/") + "' to '" + destination.removeprefix("refs/heads/") + "'"
    explicit = any(isinstance(text, str) and text == exact for text in texts)
    marker = isinstance(message, str) and ("GH013" in message or "Repository rule violations found" in message)
    return code in (403, 422) and (explicit or marker), texts


def read_reason(error):
    allowed = {"authenticated read failed or access denied; no retry", "authenticated read failed after bounded transient retries",
        "canary response exceeds bound", "canary deadline exhausted", "ambiguous ref observation",
        "empty canary history is not publisher-compatible"}
    return str(error) if isinstance(error, ValidationError) and str(error) in allowed else "read failed"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


class Remote:
    def __init__(self, directory, ordinary, writer=None, *, resume=False, clock=time.monotonic, sleep=time.sleep):
        self.directory, self.tokens, self.clock, self.sleep = Path(directory), dict(ordinary=ordinary, writer=writer), clock, sleep
        self.deadline, self.probe = clock() + 420, None
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

    def public_text(self, text):
        for token in self.tokens.values():
            if token:
                for secret in (token, base64.b64encode(("x-access-token:" + token).encode()).decode()):
                    text = text.replace(secret, "[redacted]")
        return "".join(char if char in "\n\t" or ord(char) >= 32 else "?" for char in text)[:8192]

    def git(self, *args, token=None, input_data=None):
        env = git_env()
        if token:
            require(self.tokens.get(token), "canary credential missing")
            git_config(env, "http.https://github.com/.extraheader", "AUTHORIZATION: basic " + base64.b64encode(
                ("x-access-token:" + self.tokens[token]).encode()).decode())
        result = subprocess.run(["git", "-C", str(self.directory.parent), *args], input=input_data,
            capture_output=True, text=True, env=env, timeout=self.remaining())
        require(len(result.stdout.encode()) + len(result.stderr.encode()) <= LIMIT, "canary response exceeds bound")
        return result

    def read_git(self, *args):
        for attempt in range(3):
            try:
                result = self.git(*args, token="ordinary")
                if result.returncode == 0:
                    return result
                diagnostic = result.stderr.casefold()
                transient = (re.search(r"requested url returned error: 5[0-9]{2}\b", diagnostic) is not None or
                    any(marker in diagnostic for marker in ("connection reset", "recv failure", "failed to connect",
                        "could not resolve host", "operation timed out", "connection timed out")))
                denied = (re.search(r"requested url returned error: (401|403|404)\b", diagnostic) is not None or
                    any(marker in diagnostic for marker in ("authentication", "could not read username", "terminal prompts disabled",
                        "repository not found", "access denied", "permission denied", "certificate")))
                require(transient and not denied, "authenticated read failed or access denied; no retry")
            except subprocess.TimeoutExpired:
                pass
            if attempt < 2:
                self.sleep(min(1 + attempt, self.remaining()))
        raise ValidationError("authenticated read failed after bounded transient retries")

    def refs(self):
        controls = control_refs(self.probe) if self.probe else ()
        patterns = (HISTORY_REF, HISTORY_REF + "/*", *controls)
        result = self.read_git("ls-remote", "--heads", REMOTE, *patterns)
        found = {}
        for line in result.stdout.splitlines():
            parts = line.split("\t")
            require(len(parts) == 2 and SHA.fullmatch(parts[0]), "ambiguous ref observation")
            ref = parts[1]
            if ref != HISTORY_REF and not ref.startswith(HISTORY_REF + "/") and ref not in controls:
                continue
            require(ref not in found and len(ref) <= 1024, "ambiguous ref observation")
            found[ref] = parts[0]
        return found

    def commit(self, parent=None, label="bootstrap"):
        content = sentinel_content(parent, label)
        result = self.git("--git-dir=" + str(self.directory), "hash-object", "-t", "tree", "-w", "--stdin", input_data="")
        require(result.returncode == 0 and result.stdout.strip() == EMPTY_TREE, "empty tree creation failed")
        result = self.git("--git-dir=" + str(self.directory), "hash-object", "-t", "commit", "-w", "--stdin", input_data=content)
        require(result.returncode == 0 and result.stdout.strip() == sentinel_sha(parent, label), "sentinel creation failed")
        return result.stdout.strip()

    def push(self, actor, ref, commit, expected=None):
        args = ["--git-dir=" + str(self.directory), "push", "--porcelain"]
        if expected is not None:
            args += ["--force-with-lease=" + ref + ":" + expected]
        result = self.git(*args, REMOTE, (commit or "") + ":" + ref, token=actor)
        diagnostic = "\n".join(line for line in (result.stdout + result.stderr).splitlines()
            if "\t" in line or line.startswith("remote:"))
        return dict(accepted=result.returncode == 0, rule_denial=rule_rejection(
            result.returncode, result.stdout, result.stderr, ref), response_sha256=hashlib.sha256(
                (result.stdout + result.stderr).encode()).hexdigest(), diagnostic=self.public_text(diagnostic))

    def rename(self, source, destination):
        require(source.startswith("refs/heads/") and destination.startswith("refs/heads/"), "invalid rename target")
        url = API + "/branches/" + urllib.parse.quote(source.removeprefix("refs/heads/"), safe="") + "/rename"
        request = urllib.request.Request(url, method="POST",
            data=json.dumps(dict(new_name=destination.removeprefix("refs/heads/"))).encode(),
            headers={"Authorization": "Bearer " + self.tokens["ordinary"], "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2026-03-10", "Content-Type": "application/json", "Connection": "close"})
        try:
            response = urllib.request.build_opener(NoRedirect()).open(request, timeout=self.remaining())
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read(LIMIT + 1)
            require(len(raw) <= LIMIT, "rename response exceeds bound")
            length = response.headers.get("Content-Length")
            require(length is None or length.isdecimal() and int(length) == len(raw), "rename response length differs")
            code = response.code
        denied, texts = rename_denial(code, json_value(raw, LIMIT), source, destination)
        return dict(accepted=code == 201, rule_denial=denied, response_sha256=hashlib.sha256(raw).hexdigest(),
                    status=code, diagnostic=self.public_text("\n".join(text for text in texts if isinstance(text, str))))

    def settled_rename(self, source, destination, expected, observe=None):
        end = min(self.deadline, self.clock() + 45)
        for attempt in range(12):
            refs = observe() if observe else self.refs()
            if source not in refs and refs.get(destination) == expected:
                return refs
            require(refs.get(source) in (None, expected) and refs.get(destination) in (None, expected), "rename observed foreign tip")
            if self.clock() >= end:
                break
            self.sleep(min(1 + attempt, end - self.clock(), self.remaining()))
        raise ValidationError("rename positive control did not settle")

    def verify_history(self, expected):
        self.read_git("--git-dir=" + str(self.directory), "fetch", "--no-tags", "--depth=1", REMOTE,
                      HISTORY_REF + ":" + HISTORY_REF)
        records, tip = history(self.directory)
        require(not records and tip == expected, "empty canary history is not publisher-compatible")


def read_receipt(path, binding, digest=None):
    raw = read_file(Path(path), RECEIPT_LIMIT)
    if digest is not None:
        require(isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) and hashlib.sha256(raw).hexdigest() == digest,
                "approved source receipt digest differs")
    require(raw.endswith(b"\n"), "canary receipt is incomplete")
    rows = [json_value(row, LIMIT) for row in raw.splitlines()]
    require(rows and all(isinstance(row, dict) and all(row.get(key) == value for key, value in binding.items())
            for row in rows), "canary binding differs")
    return raw, rows


def sentinel_content(parent, label):
    require(isinstance(label, str) and "\n" not in label and len(label) <= 100, "invalid sentinel label")
    content = "tree " + EMPTY_TREE + "\n"
    if parent:
        require(SHA.fullmatch(parent), "invalid sentinel parent")
        content += "parent " + parent + "\n"
    person = "RCP AWS benchmark history <benchmark-history@users.noreply.github.com> 946684800 +0000\n"
    return content + "author " + person + "committer " + person + "\nVerify protected AWS history (empty tree): " + label + "\n"


def sentinel_sha(parent, label):
    content = sentinel_content(parent, label).encode()
    # git object format uses SHA1; the approved evidence fingerprint remains SHA256
    return hashlib.sha1(b"commit " + str(len(content)).encode() + b"\0" + content, usedforsecurity=False).hexdigest()


def sentinel_plan(remote, binding):
    commit = remote.commit if remote else lambda parent=None, label="bootstrap": sentinel_sha(parent, label)
    root = commit(label="bootstrap " + binding["probe"])
    child = commit(root, label="fast-forward " + binding["probe"])
    return dict(root=root, child=child, grandchild=commit(child, label="ordinary-update " + binding["probe"]),
                unrelated=commit(label="non-fast-forward " + binding["probe"]))


class Canary:
    def __init__(self, remote, evidence, binding):
        self.remote, self.path, self.binding = remote, Path(evidence), binding
        remote.probe = binding["probe"]

    def record(self, stage, **values):
        # append bounded public evidence before evaluating observations or outcomes
        value = dict(self.binding, stage=stage, **values)
        raw = json.dumps(value, sort_keys=True).encode() + b"\n"
        require(len(raw) < LIMIT, "canary receipt exceeds bound")
        descriptor = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "ab") as stream:
            require(os.fstat(stream.fileno()).st_size + len(raw) <= RECEIPT_LIMIT, "canary receipt total exceeds bound")
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())

    def observe(self, stage):
        try:
            refs = self.remote.refs()
        except Exception as error:
            self.record(stage + "_read_failed", error_type=type(error).__name__,
                reason=read_reason(error))
            raise
        self.record(stage, refs=refs)
        return refs

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
        before = self.observe(stage + "_baseline")
        require(before.get(ref) == expected, "positive-control baseline changed; preserve state")
        outcome = self.mutate(stage, lambda: self.remote.push(actor, ref, commit, expected=expected or ""),
            actor=actor, ref=ref, commit=commit, expected=expected)
        refs = self.observe(stage + "_observed")
        self.record(stage, outcome=outcome, ref=ref, observed=refs.get(ref))
        require(outcome["accepted"] and refs.get(ref) == commit, "canary positive control failed; activation held")

    def negative(self, stage, actor, ref, commit, expected=None):
        before = self.observe(stage + "_baseline")
        require(before.get(ref) == expected, "negative-control baseline differs")
        outcome = self.mutate(stage, lambda: self.remote.push(actor, ref, commit, expected=expected or ""),
            actor=actor, ref=ref, commit=commit, expected=expected)
        after = self.observe(stage + "_observed")
        self.record(stage, outcome=outcome, ref=ref, before=expected, observed=after.get(ref))
        require(not outcome["accepted"] and outcome["rule_denial"] and after.get(ref) == expected,
                "negative control not proven; preserve state and hold activation")

    def rename(self, stage, source, destination, expected):
        observed = self.observe(stage + "_baseline")
        require(observed.get(source) == expected and destination not in observed, "rename baseline changed; preserve state")
        # GitHub's rename API has no CAS parameter; this is a fresh observation only
        return self.mutate(stage, lambda: self.remote.rename(source, destination),
            actor="ordinary", source=source, destination=destination, expected=expected)

    def ordinary(self):
        require(not self.path.exists(), "canary receipt already exists; do not rerun")
        source, renamed = control_refs(self.binding["probe"])
        refs = self.observe("initial_observation")
        require(not any(ref == HISTORY_REF or ref.startswith(HISTORY_REF + "/") for ref in refs) and
                source not in refs and renamed not in refs, "existing history namespace or probe is untrusted bootstrap state")
        self.record("initial_absence", controls=[source, renamed], history_absent=True)
        plan = sentinel_plan(self.remote, self.binding)
        self.record("sentinels", **plan)
        root = plan["root"]
        self.positive("ordinary_create_control", "ordinary", source, root)
        result = self.rename("ordinary_rename_control", source, renamed, root)
        require(result["accepted"], "ordinary rename positive control failed")
        refs = self.remote.settled_rename(source, renamed, root, observe=lambda: self.observe("ordinary_rename_control_poll"))
        self.record("ordinary_rename_control", source_absent=source not in refs, destination=refs.get(renamed))
        self.negative("ordinary_create_history", "ordinary", HISTORY_REF, root)
        result = self.rename("ordinary_rename_history", renamed, HISTORY_REF, root)
        refs = self.observe("ordinary_rename_history_observed")
        self.record("ordinary_rename_history", outcome=result, source=refs.get(renamed), destination=refs.get(HISTORY_REF))
        require(not result["accepted"] and result["rule_denial"] and refs.get(renamed) == root and HISTORY_REF not in refs,
                "history rename denial is inconclusive; preserve evidence and hold activation")
        self.record("ordinary_complete", root=root)
        return root

    def writer(self, root, completed=()):
        _, rows = read_receipt(self.path, self.binding)
        plans = [row for row in rows if row["stage"] == "sentinels"]
        require(len(plans) == 1 and plans[0]["root"] == root, "sentinel plan missing or ambiguous")
        plan = plans[0]
        source, renamed = control_refs(self.binding["probe"])
        child, grandchild, unrelated = (plan[name] for name in ("child", "grandchild", "unrelated"))
        refs = self.observe("writer_precondition")
        expected = child if "writer_fast_forward" in completed else root if "writer_create_history" in completed else None
        require(source not in refs and refs.get(renamed) == (None if "control_cleanup" in completed else root) and refs.get(HISTORY_REF) == expected and
                not any(ref.startswith(HISTORY_REF + "/") for ref in refs), "pre-mint bootstrap state changed")
        if "writer_create_history" not in completed:
            self.positive("writer_create_history", "writer", HISTORY_REF, root)
        if "writer_fast_forward" not in completed:
            self.positive("writer_fast_forward", "writer", HISTORY_REF, child, root)
        if "ordinary_fast_forward" not in completed:
            self.negative("ordinary_fast_forward", "ordinary", HISTORY_REF, grandchild, child)
        for actor in ("writer", "ordinary"):
            for suffix, commit in (("non_fast_forward", unrelated), ("delete", None)):
                stage = actor + "_" + suffix
                if stage not in completed:
                    self.negative(stage, actor, HISTORY_REF, commit, child)
        try:
            self.remote.verify_history(child)
        except Exception as error:
            self.record("publisher_verification_read_failed", error_type=type(error).__name__,
                reason=read_reason(error))
            raise
        self.record("publisher_compatible", history_tip=child, records=0, tree=EMPTY_TREE)
        if "control_cleanup" not in completed:
            outcome = self.mutate("control_cleanup", lambda: self.remote.push("ordinary", renamed, None, expected=root),
                actor="ordinary", ref=renamed, commit=None, expected=root)
            refs = self.observe("control_cleanup_observed")
            self.record("control_cleanup", outcome=outcome, ref=renamed, absent=renamed not in refs)
            require(outcome["accepted"] and renamed not in refs and refs.get(HISTORY_REF) == child,
                    "positive-control cleanup requires reconciliation")
        self.record("complete", history_tip=child, live_policy_checks=True, readiness_changed=False)


def prior_root(path, binding):
    _, rows = read_receipt(path, binding)
    last = rows[-1]
    require(last.get("stage") == "ordinary_complete" and isinstance(last.get("root"), str) and SHA.fullmatch(last["root"]),
            "ordinary canary did not complete; do not mint or resume")
    return last["root"]


def resume_state(path, binding, digest):
    raw, rows = read_receipt(path, binding, digest)
    indexed = {}
    for index, row in enumerate(rows):
        stage = row.get("stage")
        require(isinstance(stage, str) and re.fullmatch(r"[a-z_]+", stage), "invalid receipt stage")
        indexed.setdefault(stage, []).append((index, row))
    def one(stage):
        items = indexed.get(stage, [])
        require(len(items) == 1, "required acceptance evidence missing or duplicated")
        return items[0]
    plan_index, recorded_plan = one("sentinels")
    ordinary_index, ordinary = one("ordinary_complete")
    plan = sentinel_plan(None, binding)
    require(all(recorded_plan.get(key) == value for key, value in plan.items()) and ordinary.get("root") == plan["root"],
            "retained sentinels differ from the deterministic approved plan")
    source, renamed = control_refs(binding["probe"])
    initial_index, initial = one("initial_observation")
    absence_index, absence = one("initial_absence")
    require(initial_index < absence_index < plan_index and initial.get("refs") == {} and
            absence.get("controls") == [source, renamed] and absence.get("history_absent") is True,
            "initial namespace absence was not established")
    ordinary_specs = (
        ("ordinary_create_control", dict(actor="ordinary", ref=source, commit=plan["root"], expected=None), True),
        ("ordinary_rename_control", dict(actor="ordinary", source=source, destination=renamed, expected=plan["root"]), True),
        ("ordinary_create_history", dict(actor="ordinary", ref=HISTORY_REF, commit=plan["root"], expected=None), False),
        ("ordinary_rename_history", dict(actor="ordinary", source=renamed, destination=HISTORY_REF, expected=plan["root"]), False))
    writer_specs = (
        ("writer_create_history", "writer", HISTORY_REF, plan["root"], None),
        ("writer_fast_forward", "writer", HISTORY_REF, plan["child"], plan["root"]),
        ("ordinary_fast_forward", "ordinary", HISTORY_REF, plan["grandchild"], plan["child"]),
        ("writer_non_fast_forward", "writer", HISTORY_REF, plan["unrelated"], plan["child"]),
        ("writer_delete", "writer", HISTORY_REF, None, plan["child"]),
        ("ordinary_non_fast_forward", "ordinary", HISTORY_REF, plan["unrelated"], plan["child"]),
        ("ordinary_delete", "ordinary", HISTORY_REF, None, plan["child"]),
        ("control_cleanup", "ordinary", renamed, None, plan["root"]))
    known = {stage for stage, *_ in ordinary_specs + writer_specs}
    allowed = {"initial_observation", "initial_observation_read_failed", "initial_absence", "sentinels", "ordinary_complete",
        "ordinary_rename_control_poll", "ordinary_rename_control_poll_read_failed", "writer_precondition", "writer_precondition_read_failed",
        "publisher_compatible", "publisher_verification_read_failed", "complete", "approved_resume"}
    allowed.update(stage + suffix for stage in known for suffix in ("", "_baseline", "_baseline_read_failed", "_intent", "_outcome",
        "_uncertain", "_observed", "_observed_read_failed"))
    require(set(indexed) <= allowed, "unknown receipt observation or mutation stage")
    for stage in indexed:
        if stage.endswith(("_intent", "_outcome", "_uncertain")):
            require(stage.rsplit("_", 1)[0] in known, "unknown mutation evidence")
    def mutation(stage, expected, accepted, previous):
        intent_index, intent = one(stage + "_intent")
        outcome_index, result = one(stage + "_outcome")
        require(previous < intent_index < outcome_index and not indexed.get(stage + "_uncertain"),
                "mutation was uncertain or evidence is out of order; separate reconciliation required")
        require(all(intent.get(key) == value for key, value in expected.items()), "mutation intent differs from approved plan")
        outcome = result.get("outcome", {})
        require(outcome.get("accepted") is accepted and (accepted or outcome.get("rule_denial") is True),
                "recorded mutation was not the required policy outcome")
        return outcome_index
    previous = plan_index
    for stage, intent, accepted in ordinary_specs:
        previous = mutation(stage, intent, accepted, previous)
        proof_index, proof = one(stage)
        require(previous < proof_index, "ordinary proof preceded its mutation")
        if stage == "ordinary_create_control":
            require(proof.get("observed") == plan["root"], "ordinary create readback differs")
        elif stage == "ordinary_rename_control":
            require(proof.get("source_absent") is True and proof.get("destination") == plan["root"], "ordinary rename control readback differs")
        elif stage == "ordinary_create_history":
            require(proof.get("before") is None and proof.get("observed") is None, "ordinary create denial readback differs")
        previous = proof_index
    require(previous < ordinary_index, "ordinary acceptance preceded its evidence")
    renamed_index, renamed_row = one("ordinary_rename_history")
    require(previous == renamed_index < ordinary_index and renamed_row.get("source") == plan["root"] and
            renamed_row.get("destination") is None, "ordinary rename readback differs")
    completed, previous, missing = [], ordinary_index, False
    for stage, actor, ref, commit, expected in writer_specs:
        present = any(indexed.get(stage + suffix) for suffix in ("_intent", "_outcome", "_uncertain"))
        if not present:
            missing = True
            continue
        require(not missing, "writer evidence is not a contiguous prefix")
        accepted = stage in ("writer_create_history", "writer_fast_forward", "control_cleanup")
        previous = mutation(stage, dict(actor=actor, ref=ref, commit=commit, expected=expected), accepted, previous)
        for index, observed in indexed.get(stage + "_observed", []):
            require(index > previous and isinstance(observed.get("refs"), dict), "mutation readback ordering differs")
            value = commit if accepted else expected
            require(observed["refs"].get(ref) == value, "retained readback contains unexpected remote state")
            if stage == "control_cleanup":
                require(observed["refs"].get(HISTORY_REF) == plan["child"], "cleanup changed retained history")
        completed.append(stage)
    require(completed, "resume requires a definitive writer mutation outcome")
    state = {renamed: plan["root"]}
    changes = {stage: (ref, commit) for stage, _actor, ref, commit, _expected in writer_specs
               if stage in ("writer_create_history", "writer_fast_forward", "control_cleanup")}
    for row in rows[ordinary_index + 1:]:
        stage = row["stage"]
        if stage.endswith("_outcome") and stage.removesuffix("_outcome") in changes:
            ref, commit = changes[stage.removesuffix("_outcome")]
            if commit is None:
                state.pop(ref, None)
            else:
                state[ref] = commit
        if stage == "writer_precondition" or any(stage == name + suffix for name in WRITER_STAGES
                for suffix in ("_baseline", "_observed")):
            require(row.get("refs") == state, "retained baseline or readback conflicts; separate reconciliation required")
    return raw, plan, tuple(completed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("ordinary", "writer", "resume-check", "resume"))
    for field in ("probe", "trust-sha256", "revision", "directory", "evidence"):
        parser.add_argument("--" + field, required=True)
    parser.add_argument("--source-evidence")
    parser.add_argument("--source-sha256")
    args = parser.parse_args()
    binding = identity(args.probe, args.trust_sha256, args.revision)
    directory = Path(args.directory)
    if args.phase.startswith("resume"):
        require(args.source_evidence and args.source_sha256, "approved source evidence and digest required")
        raw, plan, completed = resume_state(args.source_evidence, binding, args.source_sha256)
        if args.phase == "resume-check":
            return
        require(not Path(args.evidence).exists(), "resume output already exists")
        remote = Remote(directory, os.environ.get("GH_TOKEN"), os.environ.get("WRITER_TOKEN"))
        require(remote.tokens["writer"], "writer canary credential missing")
        require(sentinel_plan(remote, binding) == plan, "retained sentinels cannot be reconstructed")
        descriptor = os.open(args.evidence, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
        run = Canary(remote, args.evidence, binding)
        run.record("approved_resume", source_sha256=args.source_sha256, completed=list(completed))
        run.writer(plan["root"], completed)
    elif args.phase == "ordinary":
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
