#!/usr/bin/env python3
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


def workflow(name):
    return yaml.load((ROOT / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader)


class Condition:
    # evaluate only the documented expression subset used by these workflow gates, never Python eval
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

    def comparison(self):
        value = self.atom()
        if self.accept("=="):
            return value == self.atom()
        if self.accept("!="):
            return value != self.atom()
        return value

    def both(self):
        value = self.comparison()
        while self.accept("&&"):
            other = self.comparison()
            value = bool(value and other)
        return value

    def either(self):
        value = self.both()
        while self.accept("||"):
            other = self.both()
            value = bool(value or other)
        return value

    def result(self):
        value = self.either()
        assert self.position == len(self.tokens), "unparsed condition tokens"
        # Actions adds success() unless a status-check function is explicitly present
        if not self.explicit_status:
            value = value and not self.cancelled and all(x == "success" for x in self.needs)
        return bool(value)


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

    def step(self, name, repo, tag="v1.2.3", event="workflow_dispatch", event_ref="refs/heads/main",
             event_sha=None, release_ref=None):
        self.output_number += 1
        output = self.root / ("output-" + str(self.output_number))
        env = dict(self.env, RELEASE_TAG=tag, EVENT_NAME=event, EVENT_REF=event_ref,
                   EVENT_SHA=self.other if event_sha is None else event_sha,
                   RELEASE_REF="refs/tags/" + tag if release_ref is None else release_ref,
                   GITHUB_OUTPUT=str(output))
        result = subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c",
                                 self.steps[name]["run"]], cwd=repo, env=env,
                                text=True, capture_output=True)
        lines = output.read_text().splitlines() if output.exists() else []
        parsed = {}
        for line in lines:
            key, separator, value = line.partition("=")
            self.assertTrue(separator, line)
            self.assertNotIn(key, parsed, "duplicate workflow output")
            parsed[key] = value
        return result, parsed

    def checkout(self, ref, name="checkout", check=True):
        repo = self.root / name
        repo.mkdir()
        self.git(repo, "init", "-b", "main")
        self.git(repo, "remote", "add", "origin", self.source.as_uri())
        # match pinned checkout's exact refs/tags refspec, not ambiguous `git checkout <name>`
        fetched = self.git(repo, "fetch", "--no-tags", "--depth=1", "origin", "+" + ref + ":" + ref,
                           check=check)
        if fetched.returncode == 0:
            self.git(repo, "checkout", "--force", "--detach", ref, check=check)
        return repo, fetched

    def resolve(self, annotated=False, event="workflow_dispatch", event_object=False):
        tag_object = self.make_tag(annotated=annotated)
        sha = tag_object if event_object else self.intended
        tag_result, outputs = self.step("tag", self.source, event=event,
                                        event_ref="refs/tags/v1.2.3", event_sha=sha)
        self.assertEqual(tag_result.returncode, 0, tag_result.stderr)
        self.assertEqual(outputs, {"tag": "v1.2.3", "ref": "refs/tags/v1.2.3"})
        repo, _ = self.checkout(outputs["ref"])
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
        repo, _ = self.checkout("refs/tags/v1.2.3")
        result, outputs = self.step("resolve", repo, event_sha=self.other)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(outputs, {"commit": self.intended})

    def test_missing_tag_does_not_fall_back_to_same_named_branch(self):
        self.git(self.source, "branch", "v1.2.3", self.other)
        result, outputs = self.step("tag", self.source)
        self.assertEqual(result.returncode, 0, result.stderr)
        repo, fetched = self.checkout(outputs["ref"], check=False)
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
        repo, _ = self.checkout("refs/tags/v1.2.3")
        self.git(repo, "fetch", "--no-tags", "origin", self.other)
        for sha in (self.other, "f" * 40):
            with self.subTest(sha=sha):
                result, outputs = self.step("resolve", repo, event="push", event_sha=sha)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(outputs, {})

    def test_consumer_keeps_resolved_sha_after_remote_tag_moves(self):
        _, commit = self.resolve()
        self.git(self.source, "tag", "-f", "v1.2.3", self.other)
        consumer = self.root / "consumer"
        consumer.mkdir()
        self.git(consumer, "init", "-b", "main")
        self.git(consumer, "remote", "add", "origin", self.source.as_uri())
        self.git(consumer, "fetch", "--no-tags", "--depth=1", "origin", commit)
        self.git(consumer, "checkout", "--detach", commit)
        self.assertEqual(self.git(consumer, "rev-parse", "HEAD").stdout.strip(), self.intended)
        self.assertEqual((consumer / "payload").read_text(), "intended tag contents")

    def test_moved_push_tag_fails_against_original_event(self):
        self.make_tag()
        self.git(self.source, "tag", "-f", "v1.2.3", self.other)
        repo, _ = self.checkout("refs/tags/v1.2.3")
        result, outputs = self.step("resolve", repo, event="push", event_sha=self.intended)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(outputs, {})

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


class ReleaseWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.flows = {name: workflow(name) for name in ("release.yml", "publish.yml")}

    def needs(self, job):
        value = job.get("needs", [])
        return [value] if isinstance(value, str) else value

    def runs(self, job, states, event="workflow_dispatch", dry=False, skip=False, cancelled=False):
        needs = self.needs(job)
        values = {"github.event_name": event, "inputs.dry_run": dry, "inputs.skip_validation": skip}
        values.update({"needs." + name + ".result": states[name] for name in needs})
        return Condition(job.get("if", "true"), values, [states[n] for n in needs], cancelled).result()

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
                    self.assertIn(RESOLVER, self.needs(job), job_name)
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
                            self.assertEqual(step["with"]["target_commitish"], COMMIT, job_name)
                            self.assertEqual(step["with"]["tag_name"], TAG, job_name)
                        self.assertNotIn("inputs.release_tag", str(step))
                self.assertEqual(validations, 1)
                self.assertEqual(checkouts, 6 if name == "release.yml" else 1)
                self.assertEqual(releases, 6 if name == "release.yml" else 0)

    def test_controller_regressions_do_not_require_a_recipe_in_historical_tags(self):
        flow = workflow("rust.yml")
        self.assertEqual(set(flow["on"]), {"push", "pull_request"})
        self.assertEqual(flow["on"]["push"], {"branches": ["main"]})
        self.assertEqual(flow["on"]["pull_request"], {"branches": ["main"]})
        self.assertEqual(flow["permissions"], {"contents": "read"})
        job = flow["jobs"]["release-identity"]
        self.assertEqual(job.get("permissions", flow["permissions"]), {"contents": "read"})
        self.assertEqual(self.needs(job), [])
        checkout, dependency, regression = job["steps"]
        self.assertRegex(checkout["uses"], r"^actions/checkout@[0-9a-f]{40}$")
        self.assertEqual(checkout["with"], {"ref": "${{ github.sha }}", "persist-credentials": "false"})
        self.assertIn("python3-yaml", dependency["run"])
        self.assertEqual(regression["run"], "/usr/bin/python3 scripts/test-release-refs.py")
        historical = workflow("validate.yml")
        self.assertNotIn("release-ref-test", str(historical))
        self.assertNotIn("test-release-refs.py", str(historical))

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
                    reached = self.runs(job, states, dry=dry) and step_runs
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
                                    self.assertFalse(self.runs(job, states, skip=skip, dry=dry))

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
                    self.assertEqual(self.runs(jobs[name], states, event=event, dry=dry, skip=skip), wanted)

    def test_validation_build_and_cancellation_failures_still_hold_publication(self):
        for flow in self.flows.values():
            jobs = flow["jobs"]
            for name, job in jobs.items():
                if name == RESOLVER:
                    continue
                states = {n: "success" for n in jobs}
                self.assertFalse(self.runs(job, states, cancelled=True), name)
                for dependency in self.needs(job):
                    for state in ("failure", "cancelled"):
                        bad = dict(states, **{dependency: state})
                        with self.subTest(job=name, dependency=dependency, state=state):
                            self.assertFalse(self.runs(job, bad), name)

    def test_standalone_publish_uses_validation_in_both_modes(self):
        jobs = self.flows["publish.yml"]["jobs"]
        states = {name: "success" for name in jobs}
        self.assertIn("validate", self.needs(jobs["publish"]))
        for dry in (False, True):
            self.assertTrue(self.runs(jobs["validate"], states, dry=dry))
            self.assertTrue(self.runs(jobs["publish"], states, dry=dry))
        self.assertEqual(jobs["publish"]["permissions"], {"contents": "read"})


if __name__ == "__main__":
    unittest.main()
