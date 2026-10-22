"""Temporary exact-debug dispatch and credential boundaries; no cloud requests."""

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml

from infra.aws_benchmark import durable_client, identity
from infra.aws_benchmark.test_durable_client import fixture


class DebugIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[2] / ".github/workflows/aws-benchmark.yml"
        cls.text = path.read_text()
        cls.workflow = yaml.load(cls.text, Loader=yaml.BaseLoader)
        cls.jobs = cls.workflow["jobs"]

    def environment(self, **changes):
        result = dict(GITHUB_REF=identity.REF, GITHUB_REPOSITORY=identity.REPOSITORY,
            GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_WORKFLOW_REF=identity.WORKFLOW,
            GITHUB_SHA="c" * 40, GITHUB_WORKFLOW_SHA="c" * 40, GITHUB_RUN_ID="123",
            GITHUB_RUN_ATTEMPT="1", EXECUTION="durable-collect", PRODUCER_RUN="456-2")
        result.update(changes)
        return result

    def validate_script(self, env):
        script = next(step["run"] for step in self.jobs["validate"]["steps"] if step.get("id") == "protocol")
        code = compile("\n".join(script.splitlines()[1:-1]), "debug workflow validation", "exec")
        with patch.dict(os.environ, env, clear=True), redirect_stdout(io.StringIO()):
            exec(code, {})

    def test_literal_debug_origin_in_all_credential_and_validation_jobs(self):
        self.assertEqual(identity.REF, "refs/heads/aws-benchmark-debug-20261019")
        self.assertEqual(identity.WORKFLOW, "wykurz/rcp/.github/workflows/aws-benchmark.yml@" + identity.REF)
        for name in ("validate", "durable-submit", "durable-collect"):
            steps = self.jobs[name]["steps"]
            self.assertEqual(set(steps[0]), {"name", "run"})
            self.assertTrue(steps[1]["uses"].startswith("actions/checkout@"))
            self.assertEqual(steps[1]["with"]["ref"], "${{ github.sha }}")
            for ref in (identity.REF, "refs/heads/main", identity.REF.upper(), identity.REF + "0",
                        identity.REF + "/child", "refs/tags/aws-benchmark-debug-20261019", "refs/pull/361/merge"):
                with self.subTest(job=name, ref=ref):
                    result = subprocess.run(["bash", "-e", "-c", steps[0]["run"]],
                        env=dict(os.environ, **self.environment(GITHUB_REF=ref)), capture_output=True, timeout=5)
                    self.assertEqual(result.returncode == 0, ref == identity.REF)

    def test_collection_needs_no_revisions_builds_or_enabled_admission(self):
        self.validate_script(self.environment(RCP_AWS_BENCHMARK_ENABLED="false", RCP_AWS_DURABLE_ENABLED="false"))
        self.assertEqual(self.jobs["durable-collect"]["needs"], "validate")
        self.assertEqual(self.jobs["validate"]["if"], "github.ref == '" + identity.REF + "'")
        self.assertEqual(self.jobs["build"]["if"], "inputs.execution == 'durable-submit'")
        self.assertEqual(self.jobs["durable-submit"]["needs"], "build")
        condition = self.jobs["durable-collect"]["if"]
        self.assertIn("inputs.execution == 'durable-collect'", condition)
        self.assertNotIn("ENABLED", condition)
        for name in ("benchmark", "report"):
            self.assertEqual(self.jobs[name]["if"], "${{ false }}")
        inputs = self.workflow["on"]["workflow_dispatch"]["inputs"]
        self.assertEqual(inputs["execution"]["options"], ["durable-submit", "durable-collect"])
        self.assertEqual(inputs["suite"]["options"], ["bulk-1g-metadata"])
        for name in ("baseline", "candidate", "run"):
            self.assertEqual(inputs[name]["required"], "false")

    def test_native_workflow_scalar_types_are_preserved(self):
        native = yaml.safe_load(self.text)
        inputs = native[True]["workflow_dispatch"]["inputs"]
        self.assertIs(inputs["execution"]["required"], True)
        self.assertIs(inputs["baseline"]["required"], False)
        self.assertIs(native["jobs"]["build"]["strategy"]["fail-fast"], True)
        self.assertIs(native["jobs"]["durable-submit"]["concurrency"]["cancel-in-progress"], False)
        for job in native["jobs"].values():
            self.assertIs(type(job["timeout-minutes"]), int)

    def test_actual_collection_validation_rejects_wrong_identity_and_unknown_modes(self):
        changes = [dict(GITHUB_REF="refs/heads/main"), dict(GITHUB_REPOSITORY="attacker/rcp"),
                   dict(GITHUB_EVENT_NAME="push"), dict(GITHUB_WORKFLOW_SHA="d" * 40),
                   dict(GITHUB_SHA="invalid", GITHUB_WORKFLOW_SHA="invalid"),
                   dict(GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/aws-benchmark-durable-collect.yml@" + identity.REF),
                   dict(EXECUTION="operator-window"), dict(PRODUCER_RUN="456"), dict(PRODUCER_RUN="456-0")]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.validate_script(self.environment(**change))

    def test_submit_requires_metadata_aa_and_cannot_select_another_producer(self):
        with tempfile.TemporaryDirectory() as directory:
            env = self.environment(EXECUTION="durable-submit", PRODUCER_RUN="", BASELINE="a" * 40,
                CANDIDATE="a" * 40, SUITE="bulk-1g-metadata", GITHUB_OUTPUT=str(Path(directory) / "output"))
            self.validate_script(env)
            output = (Path(directory) / "output").read_text()
            self.assertIn('build-roles=["baseline"]', output)
            self.assertEqual(self.jobs["build"]["strategy"]["matrix"]["role"],
                             "${{ fromJSON(needs.validate.outputs.build-roles) }}")
            candidate = next(step for step in self.jobs["durable-submit"]["steps"]
                             if step.get("with", {}).get("path", "").endswith("/candidate"))
            self.assertEqual(candidate["with"]["name"],
                             "${{ inputs.baseline == inputs.candidate && 'aws-baseline' || 'aws-candidate' }}")
            for change in (dict(PRODUCER_RUN="456-2"), dict(CANDIDATE="b" * 40), dict(SUITE="aa-smoke")):
                with self.subTest(change=change), self.assertRaises(ValueError):
                    self.validate_script(dict(env, **change))

    def test_cli_origin_gate_is_nonvacuous_for_both_actions(self):
        expected, request = fixture()
        for action in ("collect", "submit"):
            for change in ({}, dict(GITHUB_REF="refs/heads/main"), dict(GITHUB_WORKFLOW_SHA="d" * 40),
                           dict(GITHUB_WORKFLOW_REF=identity.WORKFLOW.replace("aws-benchmark.yml", "other.yml"))):
                with self.subTest(action=action, change=change), tempfile.TemporaryDirectory() as directory:
                    args = ["client", action, "--region", "us-east-1", "--bucket", "bucket", "--state-machine",
                            "arn:aws:states:us-east-1:123456789012:stateMachine:benchmark", "--role-arn", "new-role",
                            "--output", str(Path(directory) / "receipt")]
                    args += ["--run", "456-2"] if action == "collect" else ["--identity", "identity.json", "--binaries", "binaries"]
                    with patch.dict(os.environ, self.environment(**change), clear=True), patch("sys.argv", args), \
                         patch.object(identity, "load", return_value=expected), \
                         patch.object(durable_client, "oidc_clients", return_value={}) as oidc, \
                         patch.object(durable_client, "submit", return_value={}) as submit, \
                         patch.object(durable_client, "collect", return_value={}) as collect, redirect_stdout(io.StringIO()):
                        if change:
                            with self.assertRaises(ValueError):
                                durable_client.main()
                            oidc.assert_not_called()
                            submit.assert_not_called()
                            collect.assert_not_called()
                        else:
                            durable_client.main()
                            oidc.assert_called_once()
                            self.assertEqual(oidc.call_args.args, ("us-east-1", "new-role"))
                            budget = oidc.call_args.kwargs["budget"]
                            self.assertIsInstance(budget, durable_client.Budget)
                            selected = submit if action == "submit" else collect
                            selected.assert_called_once()
                            self.assertIs(selected.call_args.kwargs["budget"], budget)

    def test_collection_keeps_original_producer_and_uses_only_new_role(self):
        expected, request = fixture()
        self.assertEqual(durable_client.producer(request), expected)
        jobs = self.jobs
        for name in ("durable-submit", "durable-collect"):
            job = jobs[name]
            self.assertEqual(job["environment"], "aws-benchmark")
            self.assertEqual(job["permissions"], {"contents": "read", "id-token": "write"})
            source = json.dumps(job)
            self.assertIn("vars.RCP_AWS_DURABLE_ROLE_ARN", source)
            self.assertNotIn("vars.RCP_AWS_ROLE_ARN", source)
            self.assertNotIn("infra.aws_benchmark.controller", source)
            self.assertLessEqual(int(job["timeout-minutes"]), 15)
        report = jobs["durable-report"]
        self.assertNotIn("environment", report)
        self.assertEqual(report["permissions"], {"contents": "read"})
        self.assertEqual(report["needs"], "durable-collect")
        download = next(step for step in report["steps"] if step.get("uses", "").startswith("actions/download-artifact@"))
        self.assertEqual(download["with"]["artifact-ids"], "${{ needs.durable-collect.outputs.artifact-id }}")
        self.assertNotIn("name", download["with"])
        self.assertNotIn("validate_offline", json.dumps(jobs["durable-collect"]))
        self.assertIn("validate_offline", json.dumps(report))
        self.assertNotIn("--history", json.dumps(report))


if __name__ == "__main__":
    unittest.main()
