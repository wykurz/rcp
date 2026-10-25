#!/usr/bin/env python3
import importlib.util
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
RESOLVER = "resolve-release"
COMMIT = "${{ needs.resolve-release.outputs.commit }}"
TAG = "${{ needs.resolve-release.outputs.tag }}"
CHECKOUT = "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
BUILDS = {"build-deb-amd64", "build-rpm-amd64", "build-deb-arm64", "build-rpm-arm64"}
spec = importlib.util.spec_from_file_location("check_yaml_duplicates", ROOT / "scripts/check-yaml-duplicates.py")
DUPLICATES = importlib.util.module_from_spec(spec)
spec.loader.exec_module(DUPLICATES)


def workflow(name):
    path = ROOT / ".github/workflows" / name
    DUPLICATES.validate(path)
    return yaml.load(path.read_text(), Loader=yaml.BaseLoader)


class Condition:
    # a bounded expression adapter, not a workflow engine: unsupported syntax fails loudly
    # status inputs are transitive job ancestors, or preceding steps for a step gate
    pattern = re.compile(r"\s*(==|!=|&&|\|\||[!()]|'[^']*'|[A-Za-z_][A-Za-z0-9_.-]*)")

    def __init__(self, text, values, needs, cancelled=False):
        text = text.strip()
        if text.startswith("${{") and text.endswith("}}"):
            text = text[3:-2].strip()
        self.tokens = []
        position = 0
        while position < len(text):
            match = self.pattern.match(text, position)
            if match is None:
                raise AssertionError("unsupported workflow condition syntax: " + text[position:])
            self.tokens.append(match[1])
            position = match.end()
        self.position, self.values = 0, values
        self.needs, self.cancelled, self.explicit_status = needs, cancelled, False

    def accept(self, token):
        if self.position < len(self.tokens) and self.tokens[self.position] == token:
            self.position += 1
            return True
        return False

    def atom(self):
        if self.accept("!"):
            return not self.atom()
        if self.accept("("):
            value = self.either()
            assert self.accept(")"), "unbalanced condition"
            return value
        assert self.position < len(self.tokens), "missing condition operand"
        token = self.tokens[self.position]
        self.position += 1
        if token.startswith("'"):
            return token[1:-1]
        if token in ("true", "false"):
            return token == "true"
        if self.accept("("):
            assert self.accept(")"), "only zero-argument status functions are supported"
            assert token in ("cancelled", "success", "failure", "always"), token
            self.explicit_status = True
            return {"cancelled": self.cancelled,
                    "success": not self.cancelled and all(x == "success" for x in self.needs),
                    "failure": any(x == "failure" for x in self.needs), "always": True}[token]
        assert token in self.values, "unknown condition context: " + token
        return self.values[token]

    @staticmethod
    def equal(left, right):
        if isinstance(left, str) and isinstance(right, str):
            return left.casefold() == right.casefold()
        assert type(left) is type(right), "mixed-type expression comparison is unsupported"
        return left == right

    def comparison(self):
        value = self.atom()
        if self.accept("=="):
            return self.equal(value, self.atom())
        if self.accept("!="):
            return not self.equal(value, self.atom())
        return value

    def both(self):
        value = self.comparison()
        while self.accept("&&"):
            other = self.comparison()
            value = value and other
        return value

    def either(self):
        value = self.both()
        while self.accept("||"):
            other = self.both()
            value = value or other
        return value

    def value(self):
        value = self.either()
        assert self.position == len(self.tokens), "unparsed condition tokens"
        return value

    def result(self):
        value = self.value()
        # Actions adds success() unless a status-check function is explicitly present
        if not self.explicit_status:
            value = value and not self.cancelled and all(x == "success" for x in self.needs)
        return bool(value)


def expression(text, values):
    if text.startswith("${{") and text.endswith("}}"):
        return Condition(text, values, []).value()
    return text


def context(event="workflow_dispatch", tag="v1.2.3", ref="refs/tags/v9.9.9", sha="", dry=False, skip=False):
    return {"github.event_name": event, "github.ref": ref, "github.ref_name": ref.rsplit("/", 1)[-1],
            "github.sha": sha, "inputs.release_tag": tag, "inputs.dry_run": dry,
            "inputs.skip_validation": skip, "inputs.dry-run": str(dry).lower(),
            "github.event.inputs.dry-run": str(dry).lower()}


def dependencies(job):
    value = job.get("needs", [])
    return [value] if isinstance(value, str) else value


def ancestors(jobs, name, visiting=()):
    assert name not in visiting, "cyclic job dependency"
    result = set(dependencies(jobs[name]))
    for dependency in dependencies(jobs[name]):
        result.update(ancestors(jobs, dependency, (*visiting, name)))
    return result


def runs(jobs, name, states, event="workflow_dispatch", dry=False, skip=False, cancelled=False):
    values = context(event=event, dry=dry, skip=skip)
    values.update({"needs." + n + ".result": states[n] for n in dependencies(jobs[name])})
    return Condition(jobs[name].get("if", "true"), values,
                     [states[n] for n in ancestors(jobs, name)], cancelled).result()


def consumer_checkouts(flows, values):
    for flow_name, flow in flows.items():
        for job_name, job in flow["jobs"].items():
            if job.get("uses") == "./.github/workflows/validate.yml":
                nested = dict(values, **{"inputs.ref": expression(job["with"]["ref"], values)})
                for name, validation in workflow("validate.yml")["jobs"].items():
                    for step in validation.get("steps", []):
                        if step.get("uses", "").startswith("actions/checkout@"):
                            yield flow_name + "/" + job_name + "/" + name, step, nested
            for step in job.get("steps", []):
                if step.get("uses", "").startswith("actions/checkout@"):
                    yield flow_name + "/" + job_name, step, values


class ExpressionTests(unittest.TestCase):
    def test_status_functions_include_skipped_and_failed_ancestors(self):
        jobs = {"validate": {}, "build": {"needs": "validate"}, "publish": {"needs": "build"}}
        states = {"validate": "skipped", "build": "success"}
        self.assertFalse(runs(jobs, "publish", states))
        jobs["publish"]["if"] = "success()"
        self.assertFalse(runs(jobs, "publish", states))
        jobs["publish"]["if"] = "failure()"
        states["validate"] = "failure"
        self.assertTrue(runs(jobs, "publish", states))
        jobs["publish"]["if"] = "!cancelled() && needs.build.result == 'success'"
        self.assertTrue(runs(jobs, "publish", states))
        self.assertFalse(runs(jobs, "publish", states, cancelled=True))

    def test_strings_compare_without_case_and_boolean_operators_return_values(self):
        values = {"needs.validate.result": "failure"}
        self.assertTrue(Condition("needs.validate.result == 'Failure'", values, []).result())
        self.assertFalse(Condition("needs.validate.result != 'FAILURE'", values, []).result())
        self.assertEqual(expression("${{ false && 'wrong' || 'wanted' }}", {}), "wanted")

    def test_duplicate_yaml_keys_are_rejected_before_loading(self):
        with tempfile.TemporaryDirectory(prefix="rcp-release-yaml-") as directory:
            path = Path(directory) / "duplicate.yml"
            path.write_text("jobs:\n  build:\n    with:\n      ref: old\n      ref: new\n")
            with self.assertRaises(DUPLICATES.DuplicateKeyError):
                workflow(str(path))


class ReleaseTagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.resolver = workflow("resolve-release-tag.yml")
        cls.steps = {s["id"]: s for s in cls.resolver["jobs"]["resolve"]["steps"] if "id" in s}

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="rcp-release-ref-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.home = self.root / "home"
        self.home.mkdir()
        self.env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        self.env.pop("BASH_ENV", None)
        self.env.update(HOME=str(self.home), GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                        GIT_ALLOW_PROTOCOL="file")
        self.source = self.root / "source"
        self.source.mkdir()
        self.git(self.source, "init", "-b", "main")
        self.git(self.source, "config", "user.name", "Release fixture")
        self.git(self.source, "config", "user.email", "fixture@example.invalid")
        self.intended = self.commit("intended tag contents")
        self.other = self.commit("same-named branch contents")
        self.output_number = 0

    def git(self, repo, *args, check=True):
        return subprocess.run(["git", "-C", str(repo), *args], env=self.env,
                              text=True, capture_output=True, check=check)

    def commit(self, contents):
        (self.source / "payload").write_text(contents)
        self.git(self.source, "add", "payload")
        self.git(self.source, "commit", "-qm", contents)
        return self.git(self.source, "rev-parse", "HEAD").stdout.strip()

    def make_tag(self, name="v1.2.3", annotated=False):
        args = ["tag"] + (["-a", "-m", "annotation"] if annotated else [])
        self.git(self.source, *args, name, self.intended)
        self.git(self.source, "branch", name, self.other)
        return self.git(self.source, "rev-parse", "refs/tags/" + name).stdout.strip()

    def run_step(self, flow, job, step, repo, values):
        self.output_number += 1
        output = self.root / ("output-" + str(self.output_number))
        environment = self.root / ("environment-" + str(self.output_number))
        env = dict(self.env, GITHUB_OUTPUT=str(output), GITHUB_ENV=str(environment),
                   RUNNER_TEMP=str(self.root))
        for source in (flow, job, step):
            env.update({key: str(expression(value, values)) for key, value in source.get("env", {}).items()})
        self.assertIn(step.get("shell", "bash"), ("bash",), "unsupported workflow shell")
        script = re.sub(r"\$\{\{.*?\}\}", lambda match: str(expression(match[0], values)), step["run"])
        result = subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script],
                                cwd=repo, env=env, text=True, capture_output=True)
        parsed = {}
        for line in output.read_text().splitlines() if output.exists() else []:
            key, separator, value = line.partition("=")
            self.assertTrue(separator, line)
            self.assertNotIn(key, parsed, "duplicate workflow output")
            parsed[key] = value
        return result, parsed

    def step(self, name, repo, tag="v1.2.3", event="workflow_dispatch", event_ref="refs/heads/main",
             event_sha=None, release_ref=None):
        values = context(event=event, tag=tag, ref=event_ref,
                         sha=self.other if event_sha is None else event_sha)
        values["steps.tag.outputs.ref"] = "refs/tags/" + tag if release_ref is None else release_ref
        return self.run_step(self.resolver, self.resolver["jobs"]["resolve"], self.steps[name], repo, values)

    def checkout(self, step, values, name="checkout", check=True):
        # subset of the pinned checkout's input-helper/getRefSpec/getCheckoutInfo, with local git
        # https://github.com/actions/checkout/blob/3d3c42e5aac5ba805825da76410c181273ba90b1/src/ref-helper.ts
        # credentials, networking, retries, and other action behavior are intentionally not emulated
        self.assertEqual(step["uses"], CHECKOUT, "review the checkout adapter when upgrading the action")
        settings = step.get("with", {})
        self.assertLessEqual(set(settings), {"ref", "persist-credentials", "fetch-depth"})
        self.assertEqual(settings.get("fetch-depth", "1"), "1")
        ref = str(expression(settings.get("ref", ""), values)).strip()
        commit = ""
        if not ref:
            ref, commit = values["github.ref"], values["github.sha"]
        elif re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", ref):
            ref, commit = "", ref
        upper = ref.upper()
        target = ref
        if commit and not ref:
            specs, target = [commit], commit
        elif upper.startswith("REFS/HEADS/"):
            target = "refs/remotes/origin/" + ref[len("refs/heads/"):]
            specs = ["+" + (commit or ref) + ":" + target]
        elif upper.startswith("REFS/PULL/"):
            target = "refs/remotes/pull/" + ref[len("refs/pull/"):]
            specs = ["+" + (commit or ref) + ":" + target]
        elif upper.startswith("REFS/"):
            specs = ["+" + ref + ":" + ref]
        else:
            specs = ["+refs/heads/" + ref + "*:refs/remotes/origin/" + ref + "*",
                     "+refs/tags/" + ref + "*:refs/tags/" + ref + "*"]
        repo = self.root / name
        repo.mkdir()
        self.git(repo, "init", "-b", "main")
        self.git(repo, "remote", "add", "origin", self.source.as_uri())
        fetched = self.git(repo, "fetch", "--no-tags", "--depth=1", "origin", *specs, check=check)
        if fetched.returncode == 0:
            if ref and not upper.startswith("REFS/"):
                branch = "refs/remotes/origin/" + ref
                target = branch if self.git(repo, "show-ref", "--verify", branch, check=False).returncode == 0 else "refs/tags/" + ref
            checked = self.git(repo, "checkout", "--force", "--detach", target, check=check)
            if checked.returncode:
                return repo, checked
        return repo, fetched

    def resolver_checkout(self, release_ref="refs/tags/v1.2.3", name="checkout", check=True):
        step = next(s for s in self.resolver["jobs"]["resolve"]["steps"] if "uses" in s)
        values = context(sha=self.other)
        values["steps.tag.outputs.ref"] = release_ref
        values["steps.tag.outputs.tag"] = release_ref.removeprefix("refs/tags/")
        return self.checkout(step, values, name=name, check=check)

    def resolve(self, annotated=False, event="workflow_dispatch", event_object=False):
        tag_object = self.make_tag(annotated=annotated)
        sha = tag_object if event_object else self.intended
        tag_result, outputs = self.step("tag", self.source, event=event,
                                        event_ref="refs/tags/v1.2.3", event_sha=sha)
        self.assertEqual(tag_result.returncode, 0, tag_result.stderr)
        self.assertEqual(outputs, {"tag": "v1.2.3", "ref": "refs/tags/v1.2.3"})
        repo, _ = self.resolver_checkout(outputs["ref"])
        result, outputs = self.step("resolve", repo, event=event, event_ref="refs/tags/v1.2.3",
                                    event_sha=sha, release_ref=outputs["ref"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(outputs, {"commit": self.intended})
        self.assertEqual((repo / "payload").read_text(), "intended tag contents")
        return repo, outputs["commit"]

    def test_lightweight_tag_wins_over_same_named_branch(self):
        self.resolve()

    def test_annotated_tag_wins_over_same_named_branch(self):
        self.resolve(annotated=True)

    def test_push_is_bound_to_its_commit(self):
        self.resolve(event="push")

    def test_annotated_push_accepts_original_tag_object(self):
        self.resolve(annotated=True, event="push", event_object=True)

    def test_annotated_push_accepts_original_peeled_commit(self):
        self.resolve(annotated=True, event="push")

    def test_manual_old_tag_does_not_use_controller_commit(self):
        self.make_tag()
        repo, _ = self.resolver_checkout()
        result, outputs = self.step("resolve", repo, event_sha=self.other)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(outputs, {"commit": self.intended})

    def test_missing_tag_does_not_fall_back_to_same_named_branch(self):
        self.git(self.source, "branch", "v1.2.3", self.other)
        result, outputs = self.step("tag", self.source)
        self.assertEqual(result.returncode, 0, result.stderr)
        repo, fetched = self.resolver_checkout(outputs["ref"], check=False)
        self.assertNotEqual(fetched.returncode, 0)
        self.assertNotEqual(self.git(repo, "rev-parse", "--verify", "HEAD", check=False).returncode, 0)

    def test_non_commit_tag_cannot_emit_commit(self):
        blob = self.git(self.source, "rev-parse", self.intended + ":payload").stdout.strip()
        self.git(self.source, "update-ref", "refs/tags/v1.2.3", blob)
        result, outputs = self.step("resolve", self.source)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(outputs, {})

    def test_mismatched_checkout_cannot_emit_commit(self):
        self.make_tag()
        result, outputs = self.step("resolve", self.source)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(outputs, {})

    def test_push_mismatch_or_missing_object_cannot_emit_commit(self):
        self.make_tag()
        repo, _ = self.resolver_checkout()
        # a separate differing-object branch complements the fresh-checkout moved-tag case below
        self.git(repo, "fetch", "--no-tags", "origin", self.other)
        for sha in (self.other, "f" * 40):
            with self.subTest(sha=sha):
                result, outputs = self.step("resolve", repo, event="push", event_sha=sha)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(outputs, {})
                self.assertIn("::error::Release tag moved since the push event", result.stdout)

    def assert_consumers_keep_pin(self, flows):
        values = context(sha=self.other)
        values.update({"needs.resolve-release.outputs.commit": self.intended,
                       "needs.resolve-release.outputs.tag": "v1.2.3"})
        count = 0
        for label, step, checkout_values in consumer_checkouts(flows, values):
            with self.subTest(consumer=label):
                consumer, result = self.checkout(step, checkout_values, name="consumer-" + str(count))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.git(consumer, "rev-parse", "HEAD").stdout.strip(), self.intended)
                self.assertEqual((consumer / "payload").read_text(), "intended tag contents")
            count += 1
        self.assertGreater(count, 9)

    def test_every_consumer_keeps_resolved_sha_after_remote_tag_moves(self):
        self.resolve()
        self.git(self.source, "tag", "-f", "v1.2.3", self.other)
        self.assert_consumers_keep_pin({name: workflow(name) for name in ("release.yml", "publish.yml")})

    def test_moved_push_tag_fails_against_original_event(self):
        self.make_tag()
        self.git(self.source, "tag", "-f", "v1.2.3", self.other)
        repo, _ = self.resolver_checkout()
        result, outputs = self.step("resolve", repo, event="push", event_sha=self.intended)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(outputs, {})
        self.assertIn("::error::Release tag moved since the push event", result.stdout)

    def test_prerelease_and_build_metadata_names_are_preserved(self):
        for tag in ("v1.2.3", "v1.2.3-rc.1", "v1.2.3+build.7", "v1.2.3-rc.1+build.7"):
            with self.subTest(tag=tag):
                result, outputs = self.step("tag", self.source, tag=tag)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(outputs, {"tag": tag, "ref": "refs/tags/" + tag})

    def test_malformed_names_fail_before_emitting_outputs(self):
        for tag in ("", "v", "1.2.3", "--upload-pack=evil", "refs/tags/v1.2.3", "refs/heads/main",
                    "v1.2.3^{}", "v1.2.3@{1}", "v1..2.3", "v1.2.3.lock", "v1.2.3 ",
                    "v1.2.3\ncommit=" + self.other, "v1.2.3%0Acommit=bad", "v1.2.3/child",
                    "v1.2.3;touch injected", "v$(touch injected)", "v1.2.3\u202e"):
            with self.subTest(tag=tag):
                result, outputs = self.step("tag", self.source, tag=tag)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(outputs, {})
                self.assertFalse((self.source / "injected").exists())

    def test_unsupported_or_conflicting_events_fail_before_outputs(self):
        for event, ref, sha in (("pull_request", "refs/tags/v1.2.3", self.intended),
                ("push", "refs/heads/v1.2.3", self.intended),
                ("push", "refs/tags/v9.9.9", self.intended),
                ("push", "refs/tags/v1.2.3", "not-a-sha"),
                ("push", "refs/tags/v1.2.3", ""),
                ("push", "refs/tags/v1.2.3", self.intended + "\ncommit=bad")):
            with self.subTest(event=event, ref=ref, sha=sha):
                result, outputs = self.step("tag", self.source, event=event, event_ref=ref, event_sha=sha)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(outputs, {})

    def test_dispatch_uses_requested_old_tag_from_a_different_controller_tag(self):
        self.make_tag()
        self.git(self.source, "tag", "v9.9.9", self.other)
        for index, caller in enumerate(("release.yml", "publish.yml")):
            values = context(ref="refs/tags/v9.9.9", sha=self.other)
            values["inputs.release_tag"] = expression(workflow(caller)["jobs"][RESOLVER]["with"]["release_tag"], values)
            job = self.resolver["jobs"]["resolve"]
            checked, tag_outputs = self.run_step(self.resolver, job, self.steps["tag"], self.source, values)
            self.assertEqual(checked.returncode, 0, checked.stderr)
            values.update({"steps.tag.outputs." + k: v for k, v in tag_outputs.items()})
            step = next(s for s in job["steps"] if "uses" in s)
            repo, fetched = self.checkout(step, values, name="dispatch-" + str(index))
            self.assertEqual(fetched.returncode, 0, fetched.stderr)
            result, outputs = self.run_step(self.resolver, job, self.steps["resolve"], repo, values)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(outputs, {"commit": self.intended})

    def test_missing_exact_tag_rejects_git_dwim_shadow_namespaces(self):
        shadows = ("refs/remotes/refs/tags/v1.2.3", "refs/refs/tags/v1.2.3",
                   "refs/heads/refs/tags/v1.2.3", "refs/tags/refs/tags/v1.2.3",
                   "refs/remotes/refs/tags/v1.2.3/HEAD")
        for index, shadow in enumerate(shadows):
            with self.subTest(shadow=shadow):
                self.git(self.source, "update-ref", shadow, self.other)
                repo, fetched = self.resolver_checkout(name="shadow-" + str(index), check=False)
                self.assertEqual(fetched.returncode, 0, fetched.stderr)
                result, outputs = self.step("resolve", repo)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(outputs, {})
                self.assertIn("::error::Remote has no exact release tag matching the checkout", result.stdout)
                self.git(self.source, "update-ref", "-d", shadow)

    def test_exact_lightweight_and_annotated_tags_win_with_shadow_refs(self):
        for annotated in (False, True):
            with self.subTest(annotated=annotated):
                self.make_tag(annotated=annotated)
                self.git(self.source, "update-ref", "refs/remotes/refs/tags/v1.2.3", self.other)
                self.git(self.source, "update-ref", "refs/refs/tags/v1.2.3", self.other)
                repo, _ = self.resolver_checkout(name="real-" + str(annotated))
                result, outputs = self.step("resolve", repo)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(outputs, {"commit": self.intended})
                self.git(self.source, "tag", "-d", "v1.2.3")
                self.git(self.source, "branch", "-D", "v1.2.3")

    def test_deleted_moved_and_reannotated_remote_tag_fail_after_fetch(self):
        for index, change in enumerate(("delete", "move", "reannotate")):
            with self.subTest(change=change):
                self.make_tag(annotated=True)
                repo, _ = self.resolver_checkout(name="changed-" + str(index))
                if change == "delete":
                    self.git(self.source, "tag", "-d", "v1.2.3")
                elif change == "move":
                    self.git(self.source, "tag", "-f", "v1.2.3", self.other)
                else:
                    self.git(self.source, "tag", "-f", "-a", "-m", "different annotation", "v1.2.3", self.intended)
                result, outputs = self.step("resolve", repo)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(outputs, {})
                self.assertIn("::error::Remote has no exact release tag matching the checkout", result.stdout)
                self.git(self.source, "tag", "-d", "v1.2.3", check=False)
                self.git(self.source, "branch", "-D", "v1.2.3")

    def test_resolver_ignores_executable_scripts_in_tag_tree(self):
        # probes known repository scripts and common guarded-call mutations; this is not a shell proof
        self.git(self.source, "checkout", "--detach", self.intended)
        paths = {Path("scripts/x"), Path("scripts/anything.sh")}
        paths.update(p.relative_to(ROOT) for p in (ROOT / "scripts").rglob("*") if p.is_file())
        marker = self.root / "tag-script-executed"
        for relative in paths:
            target = self.source / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("#!/bin/bash\nprintf executed > '" + str(marker) + "'\nexit 94\n")
            target.chmod(0o755)
        self.git(self.source, "add", "scripts")
        self.git(self.source, "commit", "-qm", "hostile scripts must remain data")
        self.intended = self.git(self.source, "rev-parse", "HEAD").stdout.strip()
        self.make_tag()
        repo, _ = self.resolver_checkout()
        result, outputs = self.step("resolve", repo)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(outputs, {"commit": self.intended})
        self.assertFalse(marker.exists())

    def test_every_build_restores_release_describe_from_the_pinned_commit(self):
        self.resolve()
        self.git(self.source, "tag", "-f", "v1.2.3", self.other)
        flow = workflow("release.yml")
        values = context(sha=self.other)
        values.update({"needs.resolve-release.outputs.commit": self.intended,
                       "needs.resolve-release.outputs.tag": "v1.2.3"})
        for name in sorted(BUILDS):
            with self.subTest(job=name):
                job = flow["jobs"][name]
                checkout = next(i for i, s in enumerate(job["steps"]) if s.get("uses", "").startswith("actions/checkout@"))
                stamp = job["steps"][checkout + 1]
                self.assertEqual(stamp.get("name"), "Restore release tag for build metadata")
                self.assertEqual(stamp.get("shell"), "bash")
                self.assertEqual(stamp.get("env"), {"RELEASE_COMMIT": COMMIT})
                self.assertNotIn("if", stamp)
                consumer, _ = self.checkout(job["steps"][checkout], values, name=name)
                self.assertEqual(self.git(consumer, "tag", "--list").stdout, "")
                result, _ = self.run_step(flow, job, stamp, consumer, values)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertRegex(self.git(consumer, "describe", "--tags", "--long", "--always").stdout.strip(), r"^v1\.2\.3-0-g[0-9a-f]+$")
                self.assertEqual(self.git(consumer, "rev-parse", "HEAD").stdout.strip(), self.intended)
                self.git(consumer, "tag", "-d", "v1.2.3")
                for bad in ("", self.other):
                    wrong = dict(values, **{"needs.resolve-release.outputs.commit": bad})
                    result, _ = self.run_step(flow, job, stamp, consumer, wrong)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(self.git(consumer, "tag", "--list").stdout, "")

    def test_standalone_dry_run_flag_comes_from_real_choice_input(self):
        flow = workflow("publish.yml")
        job = flow["jobs"]["publish"]
        step = next(s for s in job["steps"] if s.get("name") == "Set publish flags")
        for dry in (False, True):
            with self.subTest(dry=dry):
                result, _ = self.run_step(flow, job, step, self.source, context(dry=dry))
                self.assertEqual(result.returncode, 0, result.stderr)
                envfile = self.root / ("environment-" + str(self.output_number))
                self.assertEqual(envfile.read_text(), "DRY_RUN=" + ("--dry-run" if dry else "") + "\n")

    def test_release_notes_match_literal_versions_and_classify_prereleases(self):
        flow = workflow("release.yml")
        job = flow["jobs"]["create-release"]
        step = next(s for s in job["steps"] if s.get("id") == "changelog")
        versions = {"v1.2.3": "false", "v1.2.3-rc.1": "true", "v1.2.3+build.7": "false",
                    "v1.2.3+build-with-hyphen.7": "false", "v1.2.3-rc.1+build.7": "true"}
        for tag, prerelease in versions.items():
            with self.subTest(tag=tag):
                version = tag[1:]
                (self.source / "CHANGELOG.md").write_text(
                    "# Changes\n## [" + version + "0]\nWrong newer section\n## [" + version +
                    "] - 2026-10-24\nWanted release notes\n## [0.0.1]\nWrong older section\n")
                values = {"needs.resolve-release.outputs.tag": tag}
                result, outputs = self.run_step(flow, job, step, self.source, values)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(outputs["prerelease"], prerelease)
                notes = Path(outputs["notes_file"])
                self.assertEqual(notes.parent, self.root)
                self.assertEqual(notes.read_text().strip(), "Wanted release notes")
        (self.source / "CHANGELOG.md").write_text("## [9.9.9]\nUnrelated version\n")
        result, outputs = self.run_step(flow, job, step, self.source, {"needs.resolve-release.outputs.tag": "v1.2.3"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(outputs, {})
        self.assertIn("::error::No release notes found", result.stdout)


class ReleaseWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.flows = {name: workflow(name) for name in ("release.yml", "publish.yml")}

    def test_resolver_has_only_read_permission_and_trusted_inline_steps(self):
        flow = workflow("resolve-release-tag.yml")
        self.assertEqual(set(flow["on"]), {"workflow_call"})
        self.assertEqual(flow["permissions"], {"contents": "read"})
        self.assertEqual(flow["on"]["workflow_call"]["inputs"]["release_tag"]["required"], "true")
        for name in ("tag", "commit"):
            self.assertEqual(flow["on"]["workflow_call"]["outputs"][name]["value"],
                             "${{ jobs.resolve.outputs." + name + " }}")
        steps = flow["jobs"]["resolve"]["steps"]
        self.assertEqual(len(steps), 3)
        self.assertEqual([s.get("id") for s in steps], ["tag", None, "resolve"])
        self.assertRegex(steps[1]["uses"], r"^actions/checkout@[0-9a-f]{40}$")
        self.assertEqual(steps[1]["with"], {"ref": "${{ steps.tag.outputs.ref }}",
                                          "fetch-depth": "1", "persist-credentials": "false"})
        self.assertEqual(flow["jobs"]["resolve"]["outputs"],
                         {"tag": "${{ steps.tag.outputs.tag }}", "commit": "${{ steps.resolve.outputs.commit }}"})
        job = flow["jobs"]["resolve"]
        self.assertEqual(flow.get("env", {}), {})
        self.assertEqual(job["env"], {"RELEASE_TAG": "${{ inputs.release_tag }}",
                         "EVENT_NAME": "${{ github.event_name }}", "EVENT_REF": "${{ github.ref }}",
                         "EVENT_SHA": "${{ github.sha }}"})
        self.assertEqual(steps[0].get("env", {}), {})
        self.assertEqual(steps[1].get("env", {}), {})
        self.assertEqual(steps[2]["env"], {"RELEASE_REF": "${{ steps.tag.outputs.ref }}",
                         "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
                         "GIT_TERMINAL_PROMPT": "0"})
        for source in (flow, job, *steps):
            self.assertNotIn("defaults", source)
            self.assertNotIn("working-directory", source)
        for step in (steps[0], steps[2]):
            self.assertEqual(step.get("shell"), "bash")
            self.assertNotIn("if", step)
            self.assertNotIn("continue-on-error", step)

    def test_every_source_and_release_target_uses_one_resolver_identity(self):
        for name, flow in self.flows.items():
            with self.subTest(workflow=name):
                self.assertNotIn("RELEASE_TAG", flow.get("env", {}))
                jobs = flow["jobs"]
                self.assertEqual(jobs[RESOLVER]["uses"], "./.github/workflows/resolve-release-tag.yml")
                checkouts, releases, validations = 0, 0, 0
                for job_name, job in jobs.items():
                    if job_name == RESOLVER:
                        continue
                    self.assertIn(RESOLVER, dependencies(job), job_name)
                    self.assertIn("needs.resolve-release.result == 'success'", job["if"], job_name)
                    if job.get("uses") == "./.github/workflows/validate.yml":
                        validations += 1
                        self.assertEqual(job["with"]["ref"], COMMIT)
                    for step in job.get("steps", []):
                        if "RELEASE_TAG" in step.get("run", ""):
                            env = dict(job.get("env", {}), **step.get("env", {}))
                            self.assertEqual(env.get("RELEASE_TAG"), TAG, job_name)
                        if step.get("uses", "").startswith("actions/checkout@"):
                            checkouts += 1
                            self.assertEqual(step["with"]["ref"], COMMIT, job_name)
                            self.assertEqual(step["with"]["persist-credentials"], "false", job_name)
                        if step.get("uses", "").startswith("softprops/action-gh-release@"):
                            releases += 1
                            self.assertNotIn("target_commitish", step["with"], job_name)
                            self.assertEqual(step["with"]["draft"], "false" if job_name == "publish-release" else "true", job_name)
                            self.assertEqual(step["with"]["tag_name"], TAG, job_name)
                            if job_name == "create-release":
                                self.assertEqual(step["with"]["prerelease"], "${{ steps.changelog.outputs.prerelease }}")
                            else:
                                self.assertNotIn("prerelease", step["with"], job_name)
                            self.assertNotIn("make_latest", step["with"], job_name)
                        self.assertNotIn("inputs.release_tag", str(step))
                self.assertEqual(validations, 1)
                self.assertEqual(checkouts, 6 if name == "release.yml" else 1)
                self.assertEqual(releases, 6 if name == "release.yml" else 0)

    def test_dispatch_input_schema_matches_the_typed_test_context(self):
        expected = {
            "release.yml": {
                "release_tag": {"type": "string", "required": "true"},
                "skip_validation": {"type": "boolean", "required": "false", "default": "false"},
                "dry_run": {"type": "boolean", "required": "false", "default": "false"}},
            "publish.yml": {
                "release_tag": {"type": "string", "required": "true"},
                "dry-run": {"type": "choice", "required": "false", "default": "true",
                            "options": ["true", "false"]}}}
        for name, wanted in expected.items():
            actual = {}
            for key, declaration in self.flows[name]["on"]["workflow_dispatch"]["inputs"].items():
                actual[key] = {k: v for k, v in declaration.items() if k != "description"}
                actual[key].setdefault("type", "string")
                actual[key].setdefault("required", "false")
            self.assertEqual(actual, wanted, name)
        inputs = workflow("resolve-release-tag.yml")["on"]["workflow_call"]["inputs"]
        self.assertEqual({key: {k: v for k, v in declaration.items() if k != "description"}
                          for key, declaration in inputs.items()},
                         {"release_tag": {"type": "string", "required": "true"}})

    def test_release_source_selection_handles_push_and_dispatch_controller_tags(self):
        cases = (("release.yml", "push", "", "refs/tags/v1.2.3"),
                 ("release.yml", "workflow_dispatch", "v1.2.3", "refs/tags/v9.9.9"),
                 ("publish.yml", "workflow_dispatch", "v1.2.3", "refs/tags/v9.9.9"),
                 ("release.yml", "push", "", "refs/tags/v2.3.4"),
                 ("release.yml", "workflow_dispatch", "v2.3.4", "refs/heads/main"),
                 ("publish.yml", "workflow_dispatch", "v2.3.4", "refs/heads/main"))
        for name, event, requested, ref in cases:
            with self.subTest(workflow=name, event=event, requested=requested, ref=ref):
                actual = self.flows[name]["jobs"][RESOLVER]["with"]["release_tag"]
                wanted = ref.removeprefix("refs/tags/") if event == "push" else requested
                self.assertEqual(expression(actual, context(event=event, tag=requested, ref=ref)), wanted)

    def test_publication_dependency_graph_cannot_drop_required_edges(self):
        jobs = self.flows["release.yml"]["jobs"]
        expected = {RESOLVER: set(), "validate": {RESOLVER},
                    "create-release": {RESOLVER, "validate"},
                    "publish-crates": {RESOLVER, *BUILDS},
                    "publish-release": {RESOLVER, "publish-crates"}}
        expected.update({name: {RESOLVER, "validate", "create-release"} for name in BUILDS})
        self.assertEqual(set(jobs), set(expected))
        for name, needs in expected.items():
            self.assertEqual(set(dependencies(jobs[name])), needs, name)
        for name in ("publish-crates", "publish-release"):
            self.assertLessEqual({"validate", *BUILDS}, ancestors(jobs, name))
        jobs = self.flows["publish.yml"]["jobs"]
        self.assertEqual({name: set(dependencies(job)) for name, job in jobs.items()},
                         {RESOLVER: set(), "validate": {RESOLVER}, "publish": {RESOLVER, "validate"}})

    def test_release_regressions_run_in_required_lint_job(self):
        steps = workflow("validate.yml")["jobs"]["lint-and-docs"]["steps"]
        matches = [s for s in steps if "release-ref-test" in s.get("run", "")]
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["run"], "just release-ref-test")
        self.assertNotIn("if", matches[0])
        self.assertNotIn("continue-on-error", matches[0])

    def test_reusable_validation_forwards_nonempty_pin_to_every_checkout(self):
        flow = workflow("validate.yml")
        self.assertEqual(flow["on"]["workflow_call"]["inputs"]["ref"]["type"], "string")
        checkouts = []
        for job_name, job in flow["jobs"].items():
            for step in job.get("steps", []):
                if step.get("uses", "").startswith("actions/checkout@"):
                    checkouts.append(job_name)
                    self.assertEqual(step["with"]["ref"], "${{ inputs.ref || github.sha }}", job_name)
        self.assertEqual(set(checkouts), set(flow["jobs"]))

    def test_dry_run_holds_every_release_action(self):
        jobs = self.flows["release.yml"]["jobs"]
        states = {name: "success" for name in jobs}
        uploads = 0
        for job_name, job in jobs.items():
            for step in job.get("steps", []):
                if not step.get("uses", "").startswith("softprops/action-gh-release@"):
                    continue
                uploads += 1
                # a release call is reachable only when both its job and step gates pass
                for dry in (False, True):
                    values = {"github.event_name": "workflow_dispatch", "inputs.dry_run": dry,
                              "needs.resolve-release.result": "success"}
                    step_runs = Condition(step.get("if", "true"), values, []).result()
                    reached = runs(jobs, job_name, states, dry=dry) and step_runs
                    self.assertEqual(reached, not dry, (job_name, dry))
        self.assertEqual(uploads, 6)

    def test_no_consumer_runs_after_failed_skipped_or_cancelled_resolution(self):
        for flow_name, flow in self.flows.items():
            jobs = flow["jobs"]
            for outcome in ("failure", "cancelled", "skipped"):
                for skip in (False, True):
                    for dry in (False, True):
                        states = {name: "success" for name in jobs}
                        states.update({RESOLVER: outcome, "validate": "skipped" if skip else "success"})
                        for name, job in jobs.items():
                            if name != RESOLVER:
                                with self.subTest(workflow=flow_name, job=name, outcome=outcome, skip=skip, dry=dry):
                                    self.assertFalse(runs(jobs, name, states, skip=skip, dry=dry))

    def test_release_preserves_tag_push_dry_run_and_skip_validation_gates(self):
        jobs = self.flows["release.yml"]["jobs"]
        builds = {name for name in jobs if name.startswith("build-")}
        self.assertEqual(len(builds), 4)
        for event, dry, skip in (("push", False, False), ("workflow_dispatch", False, False),
                ("workflow_dispatch", True, False), ("workflow_dispatch", False, True),
                ("workflow_dispatch", True, True)):
            states = {name: "success" for name in jobs}
            states["validate"] = "skipped" if skip else "success"
            states["create-release"] = "skipped" if dry else "success"
            expected = {"validate": not skip, "create-release": not dry,
                        "publish-crates": not dry, "publish-release": not dry}
            expected.update({name: True for name in builds})
            for name, wanted in expected.items():
                with self.subTest(event=event, dry=dry, skip=skip, job=name):
                    self.assertEqual(runs(jobs, name, states, event=event, dry=dry, skip=skip), wanted)

    def test_validation_build_and_cancellation_failures_still_hold_publication(self):
        for flow in self.flows.values():
            jobs = flow["jobs"]
            for name, job in jobs.items():
                if name == RESOLVER:
                    continue
                states = {n: "success" for n in jobs}
                self.assertFalse(runs(jobs, name, states, cancelled=True), name)
                for dependency in dependencies(job):
                    for state in ("failure", "cancelled"):
                        bad = dict(states, **{dependency: state})
                        with self.subTest(job=name, dependency=dependency, state=state):
                            self.assertFalse(runs(jobs, name, bad), name)

    def test_upstream_failures_propagate_to_publication_through_the_graph(self):
        for flow_name, flow in self.flows.items():
            jobs = flow["jobs"]
            for failed in ({RESOLVER, "validate", "create-release", *BUILDS} & set(jobs)):
                states = {}
                pending = set(jobs)
                while pending:
                    ready = [n for n in pending if set(dependencies(jobs[n])) <= set(states)]
                    self.assertTrue(ready, "cyclic dependency graph")
                    for name in ready:
                        active = runs(jobs, name, states)
                        states[name] = ("failure" if name == failed else "success") if active else "skipped"
                        pending.remove(name)
                with self.subTest(workflow=flow_name, failed=failed):
                    for publication in ({"publish", "publish-crates", "publish-release"} & set(jobs)):
                        self.assertEqual(states[publication], "skipped")

    def test_standalone_publish_uses_validation_in_both_modes(self):
        jobs = self.flows["publish.yml"]["jobs"]
        states = {name: "success" for name in jobs}
        self.assertIn("validate", dependencies(jobs["publish"]))
        for dry in (False, True):
            self.assertTrue(runs(jobs, "validate", states, dry=dry))
            self.assertTrue(runs(jobs, "publish", states, dry=dry))
        self.assertEqual(jobs["publish"]["permissions"], {"contents": "read"})


if __name__ == "__main__":
    unittest.main()
