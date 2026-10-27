#!/usr/bin/env python3
import copy
import importlib.util
import json
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
RELEASE_ACTION = "softprops/action-gh-release@efb35369e0ad2afab669f228072c1b0d510eae64"
BUILDS = {"build-deb-amd64", "build-rpm-amd64", "build-deb-arm64", "build-rpm-arm64"}
spec = importlib.util.spec_from_file_location("check_yaml_duplicates", ROOT / "scripts/check-yaml-duplicates.py")
DUPLICATES = importlib.util.module_from_spec(spec)
spec.loader.exec_module(DUPLICATES)


def workflow(name):
    path = ROOT / ".github/workflows" / name
    DUPLICATES.validate(path, case_insensitive=True)
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
        try:
            return self.values[token]
        except KeyError:
            raise AssertionError("unknown condition context: " + token) from None

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


class WorkflowContext(dict):
    # missing properties in modeled input/output contexts evaluate to an empty string on Actions
    def __missing__(self, key):
        if key.startswith(("inputs.", "github.event.inputs.")) or re.fullmatch(
                r"(?:steps|jobs|needs)\.[^.]+\.outputs\.[^.]+", key):
            return ""
        raise KeyError(key)


def rendered(value):
    return str(value).lower() if isinstance(value, bool) else str(value)


def declared_inputs(flow, invocation, supplied):
    declarations = flow.get("on", {}).get(invocation, {}).get("inputs", {})
    assert set(supplied) <= set(declarations), "undeclared workflow inputs"
    values = {}
    for name, declaration in declarations.items():
        kind = declaration.get("type", "string")
        value = supplied.get(name, declaration.get("default", "false" if kind == "boolean" else ""))
        if kind == "boolean":
            assert value in (True, False, "true", "false"), "invalid boolean input"
            value = value is True or value == "true"
        else:
            assert kind in ("string", "choice"), "unsupported workflow input type: " + kind
            value = rendered(value)
            if kind == "choice":
                assert value in declaration["options"], "invalid workflow choice"
        values["inputs." + name] = value
    return values


def context(flow, event="workflow_dispatch", tag="v1.2.3", ref="refs/tags/v9.9.9", sha="",
            dry=None, skip=None, invocation=None, inputs=None):
    values = WorkflowContext({"github.event_name": event, "github.ref": ref,
                              "github.ref_name": ref.rsplit("/", 1)[-1], "github.sha": sha})
    invocation = invocation or event
    if invocation not in ("workflow_dispatch", "workflow_call"):
        return values
    declarations = flow.get("on", {}).get(invocation, {}).get("inputs", {})
    supplied = dict(inputs or {})
    controls = {"release_tag": tag, "dry_run": dry, "dry-run": dry, "skip_validation": skip}
    supplied.update({k: v for k, v in controls.items() if k in declarations and v is not None and k not in supplied})
    values.update(declared_inputs(flow, invocation, supplied))
    if invocation == "workflow_dispatch":
        values.update({"github.event." + k: rendered(v) for k, v in list(values.items()) if k.startswith("inputs.")})
    return values


def called_context(flow, caller, inputs):
    values = WorkflowContext({k: v for k, v in caller.items() if not k.startswith("inputs.")})
    values.update(declared_inputs(flow, "workflow_call", inputs))
    return values


def shell_command(flow, job, step):
    # model Linux runner defaults, rejecting working directories and shells outside this adapter
    assert "container" not in job, "container execution is not modeled"
    settings = {}
    for source in (flow, job):
        defaults = source.get("defaults", {}).get("run", {})
        assert set(defaults) <= {"shell", "working-directory"}, "unsupported run defaults"
        settings.update(defaults)
        assert "working-directory" not in defaults, "working-directory is not modeled"
    assert "working-directory" not in step, "working-directory is not modeled"
    shell = step.get("shell", settings.get("shell"))
    assert shell in (None, "bash"), "unsupported workflow shell: " + str(shell)
    return ["bash", "-e"] if shell is None else ["bash", "--noprofile", "--norc", "-eo", "pipefail"]


def mapped_resolver_outputs(flow, step_outputs):
    values = WorkflowContext({"steps." + step + ".outputs." + key: value
                              for step, outputs in step_outputs.items() for key, value in outputs.items()})
    job_outputs = {key: expression(value, values) for key, value in flow["jobs"]["resolve"]["outputs"].items()}
    values = WorkflowContext({"jobs.resolve.outputs." + key: value for key, value in job_outputs.items()})
    return {key: expression(output["value"], values)
            for key, output in flow["on"]["workflow_call"]["outputs"].items()}


def dependencies(job):
    value = job.get("needs", [])
    return [value] if isinstance(value, str) else value


def ancestors(jobs, name, visiting=()):
    assert name not in visiting, "cyclic job dependency"
    result = set(dependencies(jobs[name]))
    for dependency in dependencies(jobs[name]):
        result.update(ancestors(jobs, dependency, (*visiting, name)))
    return result


def runs(flow, name, states, event="workflow_dispatch", dry=False, skip=False, cancelled=False):
    jobs = flow["jobs"]
    values = context(flow, event=event, dry=dry, skip=skip)
    values.update({"needs." + n + ".result": states[n] for n in dependencies(jobs[name])})
    return Condition(jobs[name].get("if", "true"), values,
                     [states[n] for n in ancestors(jobs, name)], cancelled).result()


def consumer_checkouts(flows, outputs, sha):
    for flow_name, flow in flows.items():
        values = context(flow, sha=sha)
        values.update({"needs.resolve-release.outputs." + key: value for key, value in outputs.items()})
        for job_name, job in flow["jobs"].items():
            if job.get("uses") == "./.github/workflows/validate.yml":
                nested = called_context(workflow("validate.yml"), values, {"ref": expression(job["with"]["ref"], values)})
                for name, validation in workflow("validate.yml")["jobs"].items():
                    for step in validation.get("steps", []):
                        if step.get("uses", "").startswith("actions/checkout@"):
                            yield flow_name + "/" + job_name + "/" + name, step, nested
            for step in job.get("steps", []):
                if step.get("uses", "").startswith("actions/checkout@") and job_name != "upload-release":
                    yield flow_name + "/" + job_name, step, values


class ExpressionTests(unittest.TestCase):
    def test_status_functions_include_skipped_and_failed_ancestors(self):
        jobs = {"validate": {}, "build": {"needs": "validate"}, "publish": {"needs": "build"}}
        flow = {"jobs": jobs}
        states = {"validate": "skipped", "build": "success"}
        self.assertFalse(runs(flow, "publish", states))
        jobs["publish"]["if"] = "success()"
        self.assertFalse(runs(flow, "publish", states))
        jobs["publish"]["if"] = "failure()"
        states["validate"] = "failure"
        self.assertTrue(runs(flow, "publish", states))
        jobs["publish"]["if"] = "!cancelled() && needs.build.result == 'success'"
        self.assertTrue(runs(flow, "publish", states))
        self.assertFalse(runs(flow, "publish", states, cancelled=True))

    def test_strings_compare_without_case_and_boolean_operators_return_values(self):
        values = {"needs.validate.result": "failure"}
        self.assertTrue(Condition("needs.validate.result == 'Failure'", values, []).result())
        self.assertFalse(Condition("needs.validate.result != 'FAILURE'", values, []).result())
        self.assertEqual(expression("${{ false && 'wrong' || 'wanted' }}", {}), "wanted")

    def test_dispatch_context_uses_only_declared_inputs_and_event_types(self):
        release, publish = workflow("release.yml"), workflow("publish.yml")
        release_values = context(release, dry=True, skip=True)
        self.assertIs(release_values["inputs.dry_run"], True)
        self.assertIs(release_values["inputs.skip_validation"], True)
        self.assertEqual(release_values["github.event.inputs.dry_run"], "true")
        self.assertEqual(release_values["inputs.dry-run"], "")
        self.assertEqual(release_values["github.event.inputs.dry-run"], "")
        publish_values = context(publish)
        self.assertEqual(publish_values["inputs.dry-run"], "true")
        self.assertEqual(publish_values["github.event.inputs.dry-run"], "true")
        self.assertEqual(publish_values["inputs.dry_run"], "")
        self.assertEqual(publish_values["inputs.skip_validation"], "")
        self.assertIs(context(release)["inputs.dry_run"], False)
        push = context(release, event="push", dry=True, skip=True)
        self.assertFalse(any(k.startswith(("inputs.", "github.event.inputs.")) for k in push))
        self.assertEqual(expression("${{ inputs.dry_run }}", push), "")

    def test_undeclared_input_does_not_suppress_a_release_dry_run(self):
        flow = workflow("release.yml")
        states = {name: "success" for name in flow["jobs"]}
        for name in ("create-release", "publish-crates", "publish-release"):
            with self.subTest(job=name):
                self.assertFalse(runs(flow, name, states, dry=True))
                changed = copy.deepcopy(flow)
                gate = changed["jobs"][name]["if"]
                self.assertIn("inputs.dry_run", gate)
                changed["jobs"][name]["if"] = gate.replace("inputs.dry_run", "inputs.dry-run == 'true'")
                self.assertTrue(runs(changed, name, states, dry=True),
                                "the misspelled input is empty on Actions, leaving this gate open")

    def test_shell_defaults_use_step_then_job_then_workflow_precedence(self):
        explicit = ["bash", "--noprofile", "--norc", "-eo", "pipefail"]
        self.assertEqual(shell_command({}, {}, {}), ["bash", "-e"])
        self.assertEqual(shell_command({"defaults": {"run": {"shell": "bash"}}}, {}, {}), explicit)
        self.assertEqual(shell_command({"defaults": {"run": {"shell": "sh"}}},
                                       {"defaults": {"run": {"shell": "bash"}}}, {}), explicit)
        self.assertEqual(shell_command({}, {"defaults": {"run": {"shell": "sh"}}},
                                       {"shell": "bash"}), explicit)
        for flow, job, step in (
                ({"defaults": {"run": {"shell": "sh"}}}, {}, {}),
                ({}, {"defaults": {"run": {"shell": "sh"}}}, {}),
                ({}, {}, {"shell": "sh"}),
                ({"defaults": {"run": {"working-directory": "docs"}}}, {}, {}),
                ({}, {"defaults": {"run": {"working-directory": "docs"}}}, {}),
                ({}, {}, {"working-directory": "docs"}),
                ({}, {"container": "example"}, {})):
            with self.subTest(flow=flow, job=job, step=step), self.assertRaises(AssertionError):
                shell_command(flow, job, step)

    def test_missing_step_outputs_stay_empty_through_reusable_output_mappings(self):
        flow = workflow("resolve-release-tag.yml")
        steps = {"tag": {"tag": "v1.2.3"}, "resolve": {"sha": "a" * 40}}
        self.assertEqual(mapped_resolver_outputs(flow, steps), {"tag": "v1.2.3", "commit": ""})
        steps["resolve"] = {"commit": "b" * 40}
        self.assertEqual(mapped_resolver_outputs(flow, steps), {"tag": "v1.2.3", "commit": "b" * 40})
        changed = copy.deepcopy(flow)
        changed["on"]["workflow_call"]["outputs"]["commit"]["value"] = "${{ jobs.resolve.outputs.sha }}"
        self.assertEqual(mapped_resolver_outputs(changed, steps)["commit"], "")

    def test_duplicate_yaml_keys_are_rejected_before_loading(self):
        with tempfile.TemporaryDirectory(prefix="rcp-release-yaml-") as directory:
            path = Path(directory) / "duplicate.yml"
            for first in ("ref", "Ref"):
                with self.subTest(first=first):
                    path.write_text("jobs:\n  build:\n    with:\n      " + first + ": old\n      ref: new\n")
                    with self.assertRaises(DUPLICATES.DuplicateKeyError):
                        workflow(str(path))
            path.write_text("Ref: old\nref: new\n")
            DUPLICATES.validate(path)


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

    def run_step(self, flow, job, step, repo, values, resolver=False):
        self.output_number += 1
        output = self.root / ("output-" + str(self.output_number))
        environment = self.root / ("environment-" + str(self.output_number))
        pathfile = self.root / ("path-" + str(self.output_number))
        summary = self.root / ("summary-" + str(self.output_number))
        env = dict(self.env, GITHUB_OUTPUT=str(output), GITHUB_ENV=str(environment),
                   GITHUB_PATH=str(pathfile), GITHUB_STEP_SUMMARY=str(summary), RUNNER_TEMP=str(self.root))
        for source in (flow, job, step):
            env.update({key: rendered(expression(value, values)) for key, value in source.get("env", {}).items()})
        script = re.sub(r"\$\{\{.*?\}\}", lambda match: rendered(expression(match[0], values)), step["run"])
        scriptfile = self.root / ("step-" + str(self.output_number) + ".sh")
        scriptfile.write_text(script)
        result = subprocess.run([*shell_command(flow, job, step), str(scriptfile)],
                                cwd=repo, env=env, text=True, capture_output=True)
        if resolver:
            self.assertFalse(environment.exists(), "resolver must not write GITHUB_ENV")
            self.assertFalse(pathfile.exists(), "resolver must not write GITHUB_PATH")
        parsed = {}
        for line in output.read_text().splitlines() if output.exists() else []:
            key, separator, value = line.partition("=")
            self.assertTrue(separator, line)
            self.assertNotIn(key, parsed, "duplicate workflow output")
            parsed[key] = value
        return result, parsed

    def step(self, name, repo, tag="v1.2.3", event="workflow_dispatch", event_ref="refs/heads/main",
             event_sha=None, release_ref=None):
        values = context(self.resolver, event=event, ref=event_ref, invocation="workflow_call",
                         inputs={"release_tag": tag}, sha=self.other if event_sha is None else event_sha)
        values["steps.tag.outputs.ref"] = "refs/tags/" + tag if release_ref is None else release_ref
        return self.run_step(self.resolver, self.resolver["jobs"]["resolve"], self.steps[name], repo, values, resolver=True)

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
        values = context(self.resolver, invocation="workflow_call", sha=self.other)
        values["steps.tag.outputs.ref"] = release_ref
        values["steps.tag.outputs.tag"] = release_ref.removeprefix("refs/tags/")
        return self.checkout(step, values, name=name, check=check)

    def resolve(self, annotated=False, event="workflow_dispatch", event_object=False):
        tag_object = self.make_tag(annotated=annotated)
        sha = tag_object if event_object else self.intended
        tag_result, tag_outputs = self.step("tag", self.source, event=event,
                                        event_ref="refs/tags/v1.2.3", event_sha=sha)
        self.assertEqual(tag_result.returncode, 0, tag_result.stderr)
        self.assertEqual(tag_outputs, {"tag": "v1.2.3", "ref": "refs/tags/v1.2.3"})
        repo, _ = self.resolver_checkout(tag_outputs["ref"])
        result, outputs = self.step("resolve", repo, event=event, event_ref="refs/tags/v1.2.3",
                                    event_sha=sha, release_ref=tag_outputs["ref"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(outputs, {"commit": self.intended})
        self.assertEqual((repo / "payload").read_text(), "intended tag contents")
        return repo, mapped_resolver_outputs(self.resolver, {"tag": tag_outputs, "resolve": outputs})

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
        self.git(self.source, "remote", "add", "origin", self.source.as_uri())
        result, outputs = self.step("resolve", self.source)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(outputs, {})

    def test_mismatched_checkout_cannot_emit_commit(self):
        self.make_tag()
        self.git(self.source, "remote", "add", "origin", self.source.as_uri())
        result, outputs = self.step("resolve", self.source)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(outputs, {})
        self.assertIn("::error::Checkout does not match the exact release tag", result.stdout)

    def test_push_mismatch_or_missing_object_cannot_emit_commit(self):
        self.make_tag()
        repo, _ = self.resolver_checkout()
        # a separate differing-object branch complements the fresh-checkout moved-tag case below
        self.git(repo, "fetch", "--no-tags", "origin", self.other)
        for sha in (self.other, "f" * 40):
            with self.subTest(sha=sha):
                result, outputs = self.step("resolve", repo, event="push", event_ref="refs/tags/v1.2.3", event_sha=sha)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(outputs, {})
                self.assertIn("::error::Release tag moved since the push event", result.stdout)

    def assert_consumers_keep_pin(self, flows, outputs):
        count = 0
        for label, step, checkout_values in consumer_checkouts(flows, outputs, self.other):
            with self.subTest(consumer=label):
                consumer, result = self.checkout(step, checkout_values, name="consumer-" + str(count))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.git(consumer, "rev-parse", "HEAD").stdout.strip(), self.intended)
                self.assertEqual((consumer / "payload").read_text(), "intended tag contents")
            count += 1
        self.assertGreater(count, 9)

    def test_every_consumer_keeps_resolved_sha_after_remote_tag_moves(self):
        _, outputs = self.resolve()
        self.git(self.source, "tag", "v9.9.9", self.other)
        self.git(self.source, "tag", "-f", "v1.2.3", self.other)
        self.assert_consumers_keep_pin({name: workflow(name) for name in ("release.yml", "publish.yml")}, outputs)

    def test_moved_push_tag_fails_against_original_event(self):
        self.make_tag()
        self.git(self.source, "tag", "-f", "v1.2.3", self.other)
        repo, _ = self.resolver_checkout()
        result, outputs = self.step("resolve", repo, event="push", event_ref="refs/tags/v1.2.3", event_sha=self.intended)
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
            flow = workflow(caller)
            caller_values = context(flow, ref="refs/tags/v9.9.9", sha=self.other)
            values = called_context(self.resolver, caller_values, {
                "release_tag": expression(flow["jobs"][RESOLVER]["with"]["release_tag"], caller_values)})
            job = self.resolver["jobs"]["resolve"]
            checked, tag_outputs = self.run_step(self.resolver, job, self.steps["tag"], self.source, values, resolver=True)
            self.assertEqual(checked.returncode, 0, checked.stderr)
            values.update({"steps.tag.outputs." + k: v for k, v in tag_outputs.items()})
            step = next(s for s in job["steps"] if "uses" in s)
            repo, fetched = self.checkout(step, values, name="dispatch-" + str(index))
            self.assertEqual(fetched.returncode, 0, fetched.stderr)
            result, outputs = self.run_step(self.resolver, job, self.steps["resolve"], repo, values, resolver=True)
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

    def test_unavailable_origin_is_retryable_and_never_emits_true_or_shadow_commit(self):
        for index, remote_ref in enumerate(("refs/tags/v1.2.3", "refs/remotes/refs/tags/v1.2.3")):
            with self.subTest(remote_ref=remote_ref):
                self.git(self.source, "update-ref", remote_ref, self.intended)
                repo, fetched = self.resolver_checkout(name="outage-" + str(index))
                self.assertEqual(fetched.returncode, 0, fetched.stderr)
                self.git(repo, "remote", "set-url", "origin", (self.root / "unavailable-origin").as_uri())
                result, outputs = self.step("resolve", repo)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(outputs, {})
                self.assertIn("::error::Could not read the remote release tag; "
                              "rerun resolution after GitHub reads recover", result.stdout)
                self.assertNotIn("::error::Remote has no exact release tag matching the checkout", result.stdout)
                self.git(self.source, "update-ref", "-d", remote_ref)

    def test_command_files_are_isolated_and_resolver_writes_are_rejected(self):
        outer_env, outer_path = self.root / "outer-environment", self.root / "outer-path"
        self.env.update(GITHUB_ENV=str(outer_env), GITHUB_PATH=str(outer_path))
        for command in ("GITHUB_ENV", "GITHUB_PATH"):
            with self.subTest(command=command):
                step = {"run": 'echo harmless >> "$' + command + '"'}
                result, _ = self.run_step({}, {}, step, self.source, {})
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertFalse(outer_env.exists())
                self.assertFalse(outer_path.exists())
                with self.assertRaisesRegex(AssertionError, "resolver must not write " + command):
                    self.run_step({}, {}, step, self.source, {}, resolver=True)
                self.assertFalse(outer_env.exists())
                self.assertFalse(outer_path.exists())

    def test_shell_runner_preserves_pipeline_and_boolean_rendering_semantics(self):
        step = {"run": "false | true\nprintf 'value=%s\\n' '${{ true }}' >> \"$GITHUB_OUTPUT\""}
        result, outputs = self.run_step({}, {}, step, self.source, {})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(outputs, {"value": "true"})
        result, outputs = self.run_step({}, {}, dict(step, shell="bash"), self.source, {})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(outputs, {})

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
        for event in ("workflow_dispatch", "push"):
            with self.subTest(event=event):
                result, outputs = self.step("resolve", repo, event=event,
                                            event_ref="refs/tags/v1.2.3", event_sha=self.intended)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(outputs, {"commit": self.intended})
                self.assertFalse(marker.exists())

    def test_every_build_restores_release_describe_from_the_pinned_commit(self):
        _, outputs = self.resolve()
        self.git(self.source, "tag", "-f", "v1.2.3", self.other)
        flow = workflow("release.yml")
        values = context(flow, sha=self.other)
        values.update({"needs.resolve-release.outputs." + key: value for key, value in outputs.items()})
        arrays = re.findall(r'\.args\(\[(.*?)\]\)', (ROOT / "common/build.rs").read_text(), re.DOTALL)
        describe = [json.loads("[" + args + "]") for args in arrays if args.lstrip().startswith('"describe"')]
        self.assertEqual(len(describe), 1, "expected one Git describe argv in common/build.rs")
        self.assertIn("--tags", describe[0], "lightweight build tags require git describe --tags")
        self.assertIn("--dirty", describe[0], "build metadata must expose dirty source trees")
        for name in sorted(BUILDS):
            with self.subTest(job=name):
                job = flow["jobs"][name]
                checkout = next(i for i, s in enumerate(job["steps"]) if s.get("uses", "").startswith("actions/checkout@"))
                stamp = job["steps"][checkout + 1]
                self.assertEqual(stamp.get("name"), "Restore release tag for build metadata")
                self.assertEqual(stamp.get("shell"), "bash")
                self.assertEqual(stamp.get("env"), {"RELEASE_COMMIT": COMMIT})
                self.assertNotIn("if", stamp)
                self.assertNotIn("continue-on-error", stamp)
                consumer, _ = self.checkout(job["steps"][checkout], values, name=name)
                self.assertEqual(self.git(consumer, "tag", "--list").stdout, "")
                result, _ = self.run_step(flow, job, stamp, consumer, values)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertRegex(self.git(consumer, *describe[0]).stdout.strip(), r"^v1\.2\.3-0-g[0-9a-f]+$")
                self.assertEqual(self.git(consumer, "rev-parse", "HEAD").stdout.strip(), self.intended)
                self.git(consumer, "tag", "-d", "v1.2.3")
                self.git(consumer, "fetch", "--no-tags", "--depth=1", "origin", self.other)
                for bad in ("", self.other):
                    wrong = dict(values, **{"needs.resolve-release.outputs.commit": bad})
                    result, _ = self.run_step(flow, job, stamp, consumer, wrong)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(self.git(consumer, "tag", "--list").stdout, "")

    def test_real_publishing_steps_require_a_credential_and_dry_run_never_gets_one(self):
        bindir = self.root / "bin"
        bindir.mkdir()
        capture = self.root / "cargo-call.json"
        cargo = bindir / "cargo"
        cargo.write_text("#!/usr/bin/env python3\nimport json, os, sys\n"
                         "from pathlib import Path\n"
                         "Path(os.environ['CAPTURE']).write_text(json.dumps({"
                         "'args': sys.argv[1:], 'token': bool(os.environ.get('CARGO_REGISTRY_TOKEN'))}))\n"
                         "sys.exit(int(os.environ.get('CARGO_EXIT', '0')))\n")
        cargo.chmod(0o755)
        scripts = self.source / "scripts"
        scripts.mkdir()
        host = scripts / "cargo-host.sh"
        host.write_text('#!/bin/sh\nexec cargo "$@"\n')
        host.chmod(0o755)
        self.env.pop("CARGO_REGISTRY_TOKEN", None)
        self.env.update(PATH=str(bindir) + os.pathsep + self.env["PATH"], CAPTURE=str(capture))
        for filename, jobname in (("release.yml", "publish-crates"), ("publish.yml", "publish")):
            flow = workflow(filename)
            job = flow["jobs"][jobname]
            preflight = next(s for s in job["steps"] if s.get("name") == "Require the environment publishing credential")
            publish = next(s for s in job["steps"] if s.get("name") == "Publish to crates.io")
            for token in ("", "fixture-publish-credential"):
                values = context(flow)
                values.update({"needs.resolve-release.outputs.tag": "v1.2.3",
                               "secrets.CRATES_IO_PUBLISH_TOKEN": token})
                capture.unlink(missing_ok=True)
                checked, _ = self.run_step(flow, job, preflight, self.source, values)
                self.assertNotIn("fixture-publish-credential", checked.stdout + checked.stderr)
                if not token:
                    self.assertNotEqual(checked.returncode, 0)
                    self.assertFalse(capture.exists())
                    continue
                self.assertEqual(checked.returncode, 0, checked.stderr)
                result, _ = self.run_step(flow, job, publish, self.source, values)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(capture.read_text()),
                                 {"args": ["workspaces", "publish", "--publish-as-is", "--yes", "--no-verify"],
                                  "token": True})
        flow = workflow("publish.yml")
        job = flow["jobs"]["dry-run"]
        step = next(s for s in job["steps"] if s.get("name") == "Check publishing without a credential")
        for status in (0, 73):
            self.env["CARGO_EXIT"] = str(status)
            result, _ = self.run_step(flow, job, step, self.source, context(flow, dry=True))
            self.assertEqual(result.returncode, status, result.stderr)
            self.assertEqual(json.loads(capture.read_text()),
                             {"args": ["workspaces", "publish", "--publish-as-is", "--yes", "--no-verify", "--dry-run"],
                              "token": False})

    def test_publication_summary_reports_actual_stage_without_adding_a_failure(self):
        flow = workflow("publish.yml")
        job = flow["jobs"]["publish"]
        gate = next(step for step in job["steps"] if step.get("id") == "credential")
        summary = next(step for step in job["steps"] if step.get("name") == "Summary")
        self.assertEqual(summary["if"], "always()")
        missing_credential = dict(context(flow), **{"secrets.CRATES_IO_PUBLISH_TOKEN": ""})
        failed, _ = self.run_step(flow, job, gate, self.source, missing_credential)
        self.assertNotEqual(failed.returncode, 0)
        for credential, publish, message in (("failure", "skipped", "credential gate"),
                                             ("skipped", "skipped", "preceding checkout"),
                                             ("success", "skipped", "version check and setup"),
                                             ("success", "failure", "Publication failed"),
                                             ("success", "cancelled", "Publication was cancelled"),
                                             ("success", "success", "Workspace packages published"),
                                             ("success", "", "outcome is unavailable")):
            with self.subTest(credential=credential, publish=publish):
                values = context(flow)
                values.update({"steps.credential.outcome": credential, "steps.publish.outcome": publish})
                result, _ = self.run_step(flow, job, summary, self.source, values)
                self.assertEqual(result.returncode, 0, result.stderr)
                report = (self.root / ("summary-" + str(self.output_number))).read_text()
                self.assertIn(message, report)
                if publish != "success":
                    self.assertNotIn("packages published", report)
                self.assertNotIn("::error::", result.stdout + result.stderr)

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

    def test_release_source_jobs_and_reusable_callees_cannot_request_cache_access(self):
        # cache-mode scopes the runtime token; GITHUB_TOKEN permissions or disabling a cache action do not
        for filename, flow in self.flows.items():
            with self.subTest(workflow=filename):
                self.assertEqual(flow.get("cache-mode"), "none")
                pending = [flow]
                visited = set()
                while pending:
                    caller = pending.pop()
                    self.assertIn(caller.get("cache-mode", "none"), ("none",))
                    for name, job in caller["jobs"].items():
                        self.assertEqual(job.get("cache-mode", "none"), "none", name)
                        reusable = job.get("uses", "")
                        if reusable:
                            self.assertTrue(reusable.startswith("./.github/workflows/"), reusable)
                            if reusable not in visited:
                                visited.add(reusable)
                                pending.append(workflow(Path(reusable).name))

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
        self.assertNotIn("container", job)
        self.assertNotIn("continue-on-error", job)
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
                            self.assertEqual(step["with"]["ref"], "${{ github.sha }}" if job_name == "upload-release" else COMMIT, job_name)
                            self.assertEqual(step["with"]["persist-credentials"], "false", job_name)
                        if step.get("uses", "").startswith("softprops/action-gh-release@"):
                            releases += 1
                            self.assertEqual(step["uses"], RELEASE_ACTION,
                                             "review draft/target/prerelease handling in src/github.ts "
                                             "release()/finalizeRelease() when upgrading the release action")
                            if job_name == "upload-release":
                                self.assertEqual(step["with"].get("fail_on_unmatched_files"), "true", job_name)
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
                self.assertEqual(checkouts, 7 if name == "release.yml" else 2)
                self.assertEqual(releases, 3 if name == "release.yml" else 0)

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
                self.assertEqual(expression(actual, context(self.flows[name], event=event, tag=requested, ref=ref)), wanted)

    def test_publication_dependency_graph_cannot_drop_required_edges(self):
        flow = self.flows["release.yml"]
        jobs = flow["jobs"]
        expected = {RESOLVER: set(), "validate": {RESOLVER},
                    "create-release": {RESOLVER, "validate"},
                    "upload-release": {RESOLVER, "create-release", *BUILDS},
                    "publish-crates": {RESOLVER, "upload-release"},
                    "publish-release": {RESOLVER, "publish-crates"}}
        expected.update({name: {RESOLVER, "validate", "create-release"} for name in BUILDS})
        self.assertEqual(set(jobs), set(expected))
        for name, needs in expected.items():
            self.assertEqual(set(dependencies(jobs[name])), needs, name)
            self.assertNotIn("continue-on-error", jobs[name], name)
        for name in ("publish-crates", "publish-release"):
            self.assertLessEqual({"validate", *BUILDS}, ancestors(jobs, name))
        flow = self.flows["publish.yml"]
        jobs = flow["jobs"]
        for name, job in jobs.items():
            self.assertNotIn("continue-on-error", job, name)
        self.assertEqual({name: set(dependencies(job)) for name, job in jobs.items()},
                         {RESOLVER: set(), "validate": {RESOLVER}, "publish": {RESOLVER, "validate"},
                           "dry-run": {RESOLVER, "validate"}})

    def test_release_regressions_run_in_required_lint_job(self):
        flow = workflow("validate.yml")
        job = flow["jobs"]["lint-and-docs"]
        self.assertNotIn("if", job)
        self.assertNotIn("continue-on-error", job)
        self.assertEqual(dependencies(job), [])
        matches = [s for s in job["steps"] if "release-ref-test" in s.get("run", "")]
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["run"], "just release-ref-test")
        self.assertNotIn("if", matches[0])
        self.assertNotIn("continue-on-error", matches[0])
        self.assertIn(shell_command(flow, job, matches[0]),
                      (["bash", "-e"], ["bash", "--noprofile", "--norc", "-eo", "pipefail"]))
        recipe = re.search(r"^release-ref-test:\n((?:[ \t].*\n|\n)+)", (ROOT / "justfile").read_text(), re.MULTILINE)
        self.assertIsNotNone(recipe, "release-ref-test recipe must exist")
        self.assertEqual([line.strip() for line in recipe[1].splitlines() if line.strip()],
                         ["python3 scripts/test-release-refs.py", "python3 scripts/test-release-rpm-version.py",
                           "python3 scripts/test-stage-release-assets.py"])

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
        flow = self.flows["release.yml"]
        jobs = flow["jobs"]
        states = {name: "success" for name in jobs}
        uploads = 0
        for job_name, job in jobs.items():
            for step in job.get("steps", []):
                if not step.get("uses", "").startswith("softprops/action-gh-release@"):
                    continue
                uploads += 1
                # a release call is reachable only when both its job and step gates pass
                for dry in (False, True):
                    values = {"github.event_name": "workflow_dispatch", "inputs.dry_run": dry}
                    step_runs = Condition(step.get("if", "true"), values, []).result()
                    reached = runs(flow, job_name, states, dry=dry) and step_runs
                    self.assertEqual(reached, not dry, (job_name, dry))
        self.assertEqual(uploads, 3)

    def test_no_consumer_runs_after_failed_skipped_or_cancelled_resolution(self):
        for flow_name, flow in self.flows.items():
            jobs = flow["jobs"]
            for event in flow["on"]:
                for outcome in ("failure", "cancelled", "skipped"):
                    for skip in (False, True):
                        for dry in (False, True):
                            states = {name: "success" for name in jobs}
                            states.update({RESOLVER: outcome, "validate": "skipped" if skip else "success"})
                            for name, job in jobs.items():
                                if name != RESOLVER:
                                    with self.subTest(workflow=flow_name, event=event, job=name, outcome=outcome, skip=skip, dry=dry):
                                        self.assertFalse(runs(flow, name, states, event=event, skip=skip, dry=dry))

    def test_release_preserves_tag_push_dry_run_and_skip_validation_gates(self):
        flow = self.flows["release.yml"]
        jobs = flow["jobs"]
        for event, dry, skip in (("push", False, False), ("workflow_dispatch", False, False),
                ("workflow_dispatch", True, False), ("workflow_dispatch", False, True),
                ("workflow_dispatch", True, True)):
            states = {name: "success" for name in jobs}
            states["validate"] = "skipped" if skip else "success"
            states["create-release"] = "skipped" if dry else "success"
            expected = {"validate": not skip, "create-release": not dry,
                        "upload-release": not dry, "publish-crates": not dry, "publish-release": not dry}
            expected.update({name: True for name in BUILDS})
            for name, wanted in expected.items():
                with self.subTest(event=event, dry=dry, skip=skip, job=name):
                    self.assertEqual(runs(flow, name, states, event=event, dry=dry, skip=skip), wanted)

    def test_validation_build_and_cancellation_failures_still_hold_publication(self):
        for flow in self.flows.values():
            jobs = flow["jobs"]
            for event in flow["on"]:
                for name, job in jobs.items():
                    if name == RESOLVER:
                        continue
                    states = {n: "success" for n in jobs}
                    self.assertFalse(runs(flow, name, states, event=event, cancelled=True), name)
                    for dependency in dependencies(job):
                        for state in ("failure", "cancelled"):
                            bad = dict(states, **{dependency: state})
                            with self.subTest(event=event, job=name, dependency=dependency, state=state):
                                self.assertFalse(runs(flow, name, bad, event=event), name)

    def test_upstream_failures_propagate_to_publication_through_the_graph(self):
        for flow_name, flow in self.flows.items():
            jobs = flow["jobs"]
            for event in flow["on"]:
                for failed in ({RESOLVER, "validate", "create-release", "upload-release", *BUILDS} & set(jobs)):
                    states = {}
                    pending = set(jobs)
                    while pending:
                        ready = [n for n in pending if set(dependencies(jobs[n])) <= set(states)]
                        self.assertTrue(ready, "cyclic dependency graph")
                        for name in ready:
                            active = runs(flow, name, states, event=event)
                            states[name] = ("failure" if name == failed else "success") if active else "skipped"
                            pending.remove(name)
                    with self.subTest(workflow=flow_name, event=event, failed=failed):
                        for publication in ({"publish", "publish-crates", "publish-release"} & set(jobs)):
                            self.assertEqual(states[publication], "skipped")

    def test_source_builds_have_no_release_write_authority_even_in_dry_runs(self):
        flow = self.flows["release.yml"]
        jobs = flow["jobs"]
        for name in BUILDS:
            job = jobs[name]
            self.assertEqual(job["permissions"], {"contents": "read"})
            self.assertNotIn("environment", job)
            self.assertEqual(job["outputs"], {"artifact-id": "${{ steps.package.outputs.artifact-id }}"})
            self.assertFalse(any("softprops/" in step.get("uses", "") for step in job["steps"]))
            artifact = next(step for step in job["steps"] if step.get("id") == "package")
            self.assertEqual(artifact["uses"], "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a")
            kind = name.removeprefix("build-")
            self.assertEqual(artifact["with"], {
                "name": "release-" + kind + "-" + COMMIT + "-${{ github.run_attempt }}",
                "path": "./rcp-" + kind + "-${{ env.RELEASE_TAG }}.zip",
                "if-no-files-found": "error", "retention-days": "7", "compression-level": "0"})
        states = {name: "success" for name in jobs}
        for name, job in jobs.items():
            if job.get("permissions", {}).get("contents") == "write":
                self.assertFalse(runs(flow, name, states, dry=True), name)

    def test_release_writer_executes_only_held_controller_code_and_fixed_artifact_paths(self):
        job = self.flows["release.yml"]["jobs"]["upload-release"]
        self.assertEqual(job["permissions"], {"contents": "write", "actions": "read"})
        self.assertEqual(job["timeout-minutes"], "15")
        self.assertEqual(len(job["steps"]), 3)
        checkout, stage, upload = job["steps"]
        self.assertEqual(checkout["uses"], CHECKOUT)
        self.assertEqual(checkout["with"], {"ref": "${{ github.sha }}", "path": "controller", "persist-credentials": "false"})
        self.assertEqual(stage["run"], 'python3 -I "$GITHUB_WORKSPACE/controller/scripts/stage-release-assets.py"')
        self.assertNotIn("if", stage)
        self.assertNotIn("continue-on-error", stage)
        self.assertEqual(stage["env"]["GH_TOKEN"], "${{ github.token }}")
        self.assertEqual(stage["env"]["RELEASE_COMMIT"], COMMIT)
        self.assertEqual(stage["env"]["RELEASE_TAG"], TAG)
        for kind in ("deb-amd64", "rpm-amd64", "deb-arm64", "rpm-arm64"):
            self.assertEqual(stage["env"][kind.replace("-", "_").upper() + "_ID"],
                             "${{ needs.build-" + kind + ".outputs.artifact-id }}")
        self.assertEqual(upload["uses"], RELEASE_ACTION)
        self.assertEqual(upload["with"]["files"].splitlines(), [
            "${{ steps.assets.outputs.directory }}/rcp-" + kind + "-" + TAG + ".zip"
            for kind in ("deb-amd64", "rpm-amd64", "deb-arm64", "rpm-arm64")])
        self.assertNotIn("if", upload)
        self.assertNotIn("continue-on-error", upload)

    def test_standalone_publish_uses_validation_in_both_modes(self):
        flow = self.flows["publish.yml"]
        jobs = flow["jobs"]
        states = {name: "success" for name in jobs}
        self.assertIn("validate", dependencies(jobs["publish"]))
        for dry in (False, True):
            self.assertTrue(runs(flow, "validate", states, dry=dry))
            self.assertEqual(runs(flow, "publish", states, dry=dry), not dry)
            self.assertEqual(runs(flow, "dry-run", states, dry=dry), dry)
        self.assertEqual(jobs["publish"]["permissions"], {"contents": "read"})
        self.assertEqual(jobs["dry-run"]["permissions"], {"contents": "read"})


    def test_only_actual_publishing_jobs_request_environment_credentials(self):
        expected = {("release.yml", "publish-crates"), ("publish.yml", "publish")}
        found = set()
        for filename, flow in self.flows.items():
            self.assertNotIn("secrets.", json.dumps(flow.get("env", {})))
            for jobname, job in flow["jobs"].items():
                identity = filename, jobname
                self.assertNotIn("CRATES_IO_TOKEN", json.dumps(job))
                self.assertNotIn("id-token", job.get("permissions", {}))
                if identity not in expected:
                    self.assertNotIn("environment", job)
                    self.assertNotIn("secrets", json.dumps(job))
                    continue
                found.add(identity)
                self.assertEqual(job["environment"], {"name": "crates-io",
                    "url": "https://github.com/wykurz/rcp/commit/" + COMMIT})
                self.assertEqual(job["permissions"], {"contents": "read"})
                self.assertNotIn("secrets", json.dumps(job.get("env", {})))
                references = [step for step in job["steps"] if "secrets" in json.dumps(step)]
                self.assertEqual([step["name"] for step in references],
                                 ["Require the environment publishing credential", "Publish to crates.io"])
                for step in references:
                    self.assertEqual(step["env"]["CARGO_REGISTRY_TOKEN"], "${{ secrets.CRATES_IO_PUBLISH_TOKEN }}")
                gate = references[0]
                self.assertNotIn("if", gate)
                self.assertNotIn("continue-on-error", gate)
                # missing credentials stop before any selected-source shell step can run
                self.assertIs(next(step for step in job["steps"] if "run" in step), gate)
        self.assertEqual(found, expected)
        for flow in self.flows.values():
            states = {name: "success" for name in flow["jobs"]}
            for jobname in ("publish", "publish-crates"):
                if jobname in states:
                    self.assertFalse(runs(flow, jobname, states, dry=True))


if __name__ == "__main__":
    unittest.main()
