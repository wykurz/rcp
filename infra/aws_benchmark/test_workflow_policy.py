"""Offline checks of the privileged workflow's branch and artifact boundaries."""

import ast
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
from unittest.mock import patch

import yaml

from infra.aws_benchmark import common, identity


class WorkflowPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[2] / ".github/workflows/aws-benchmark.yml"
        # retain YAML's on key and scalar strings without YAML 1.1 boolean coercion
        cls.workflow = yaml.load(path.read_text(), Loader=yaml.BaseLoader)

    def test_actions_guard_limits_admission_with_case_insensitive_equality(self):
        expression = self.workflow["jobs"]["validate"]["if"]
        expression = expression.replace("github.ref", "ref").replace(
            "vars.RCP_AWS_BENCHMARK_ENABLED", "enabled").replace("&&", "and").replace("||", "or")
        tree = ast.parse("(" + expression + ")", mode="eval")
        allowed = (ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.Compare,
                   ast.Eq, ast.Name, ast.Load, ast.Constant)
        self.assertTrue(all(isinstance(node, allowed) for node in ast.walk(tree)))
        self.assertTrue(all(node.id in {"ref", "enabled"} for node in ast.walk(tree)
                            if isinstance(node, ast.Name)))
        code = compile(tree, "workflow branch guard", "eval")
        trusted = {"refs/heads/main"}
        refs = trusted | {"refs/heads/other", "refs/heads/aws-benchmark-debug-20261019", "refs/heads/aws-benchmark-debug-202610190",
                          "refs/heads/aws-benchmark-debug-20261019/other", "refs/tags/main",
                          "refs/tags/aws-benchmark-debug-20261019", "refs/pull/357/merge", "refs/heads/MAIN",
                          "refs/heads/AWS-BENCHMARK-DEBUG-20261019", ""}
        for ref in refs:
            for enabled in ("true", "false", "", "TRUE"):
                with self.subTest(ref=ref, enabled=enabled):
                    # actions equality ignores case; the next step enforces literal branch names
                    actual = eval(code, {"__builtins__": {}}, {"ref": ref.lower(), "enabled": enabled.lower()})
                    self.assertEqual(actual, ref.lower() in trusted and enabled.lower() == "true")

    def test_literal_ref_guard_runs_before_checkout_or_repository_imports(self):
        steps = self.workflow["jobs"]["validate"]["steps"]
        self.assertEqual(set(steps[0]), {"name", "run"})
        self.assertTrue(steps[1]["uses"].startswith("actions/checkout@"))
        for ref in ("refs/heads/main", "refs/heads/MAIN", "refs/tags/main", "refs/heads/other",
                    "refs/heads/aws-benchmark-debug-20261019"):
            for repository, event in (("wykurz/rcp", "workflow_dispatch"), ("attacker/rcp", "workflow_dispatch"),
                                      ("wykurz/rcp", "pull_request")):
                env = dict(os.environ, GITHUB_REF=ref, GITHUB_REPOSITORY=repository, GITHUB_EVENT_NAME=event)
                result = subprocess.run(["bash", "-e", "-c", steps[0]["run"]], env=env,
                                        capture_output=True, timeout=5)
                expected = ref == "refs/heads/main" and repository == "wykurz/rcp" and event == "workflow_dispatch"
                self.assertEqual(result.returncode == 0, expected, (ref, repository, event))

    def test_actual_validation_script_rejects_other_refs_and_records_run_binding(self):
        steps = self.workflow["jobs"]["validate"]["steps"]
        script = next(s["run"] for s in steps if s.get("id") == "protocol").splitlines()
        self.assertEqual(script[0], "python3 - <<'PY'")
        self.assertEqual(script[-1], "PY")
        program = compile("\n".join(script[1:-1]), "actual workflow validation", "exec")
        trusted = {"refs/heads/main"}
        refs = trusted | {"refs/heads/MAIN", "refs/heads/AWS-BENCHMARK-DEBUG-20261019",
                          "refs/heads/other", "refs/heads/aws-benchmark-debug-20261019", "refs/tags/main", "refs/tags/aws-benchmark-debug-20261019",
                          "refs/pull/357/merge", "refs/heads/aws-benchmark-debug-202610190", ""}
        for ref in refs:
            with self.subTest(ref=ref), tempfile.TemporaryDirectory() as root:
                env = dict(GITHUB_REF=ref, GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT="1",
                           GITHUB_SHA="c" * 40, GITHUB_REPOSITORY="wykurz/rcp",
                           GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/aws-benchmark.yml@" + ref,
                           GITHUB_WORKFLOW_SHA="c" * 40, BASELINE="a" * 40, CANDIDATE="a" * 40,
                           SUITE="aa-smoke", GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_OUTPUT=str(Path(root) / "output"))
                output = io.StringIO()
                with patch.dict(os.environ, env), redirect_stdout(output), \
                     patch.object(identity, "validate_identity", wraps=identity.validate_identity) as validate:
                    if ref not in trusted:
                        with self.assertRaisesRegex(ValueError, "not the exact approved main branch"):
                            exec(program, {})
                        self.assertFalse(Path(env["GITHUB_OUTPUT"]).exists())
                        continue
                    exec(program, {})
                    validate.assert_called_once()
                binding = json.loads(output.getvalue().removeprefix("RCP_AWS_RUN_IDENTITY="))
                self.assertEqual(binding, dict(run="123-1", baseline="a" * 40, candidate="a" * 40,
                    suite="aa-smoke", controller="c" * 40, ref=ref, repository="wykurz/rcp",
                    workflow=env["GITHUB_WORKFLOW_REF"], workflow_sha="c" * 40, event="workflow_dispatch",
                    run_url="https://github.com/wykurz/rcp/actions/runs/123/attempts/1"))

    def test_dispatch_choices_and_real_validation_exclude_disabled_suites(self):
        choices = self.workflow["on"]["workflow_dispatch"]["inputs"]["suite"]["options"]
        self.assertEqual(set(choices), set(common.ACTIVE_SUITES))
        script = next(step["run"] for step in self.workflow["jobs"]["validate"]["steps"]
                      if step.get("id") == "protocol").splitlines()
        program = compile("\n".join(script[1:-1]), "actual workflow validation", "exec")
        for suite in common.SUITES.keys() - common.ACTIVE_SUITES.keys():
            with self.subTest(suite=suite), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "output"
                env = dict(GITHUB_REF=identity.REF, GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT="1",
                    GITHUB_SHA="c" * 40, GITHUB_REPOSITORY=identity.REPOSITORY,
                    GITHUB_WORKFLOW_REF=identity.WORKFLOW, GITHUB_WORKFLOW_SHA="c" * 40,
                    BASELINE="a" * 40, CANDIDATE="a" * 40, SUITE=suite,
                    GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_OUTPUT=str(output))
                with patch.dict(os.environ, env), self.assertRaisesRegex(ValueError, "disabled for new AWS runs"):
                    exec(program, {})
                self.assertFalse(output.exists())

    def test_manual_trigger_and_single_mailbox_gate_every_privileged_run(self):
        self.assertEqual(set(self.workflow["on"]), {"workflow_dispatch"})
        self.assertNotIn("concurrency", self.workflow)
        jobs = self.workflow["jobs"]
        self.assertEqual(jobs["benchmark"]["concurrency"],
                         {"group": "aws-benchmark-single-mailbox", "cancel-in-progress": "false"})
        for name in ("validate", "build", "report"):
            self.assertNotIn("concurrency", jobs[name])
        self.assertEqual(set(jobs), {"validate", "build", "benchmark", "report"})
        self.assertEqual(jobs["build"]["needs"], "validate")
        self.assertEqual(jobs["benchmark"]["needs"], "build")
        self.assertEqual(jobs["benchmark"]["environment"], "aws-benchmark")

    def test_candidate_builds_have_no_privileged_environment_or_oidc(self):
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        jobs = self.workflow["jobs"]
        self.assertEqual(jobs["build"]["permissions"], {"contents": "read"})
        for name in ("validate", "build", "report"):
            self.assertNotIn("environment", jobs[name])
            self.assertNotIn("id-token", jobs[name].get("permissions", {}))
            self.assertEqual(jobs[name]["runs-on"], "ubuntu-24.04")
        self.assertEqual(jobs["report"]["permissions"], {"contents": "read"})
        self.assertEqual(jobs["benchmark"]["permissions"], {"contents": "read", "id-token": "write"})

    def test_privileged_checkout_and_artifacts_are_bound_to_this_run(self):
        steps = self.workflow["jobs"]["benchmark"]["steps"]
        checkouts = [s for s in steps if s.get("uses", "").startswith("actions/checkout@")]
        self.assertEqual(len(checkouts), 1)
        self.assertEqual(checkouts[0]["with"],
                         {"ref": "${{ github.sha }}", "persist-credentials": "false"})
        downloads = [s for s in steps if s.get("uses", "").startswith("actions/download-artifact@")]
        self.assertEqual(len(downloads), 2)
        self.assertEqual({s["with"]["name"] for s in downloads}, {"aws-baseline", "aws-candidate"})
        for step in downloads:
            self.assertEqual(set(step["with"]), {"name", "path"})


    def test_reporting_download_is_bound_to_trusted_producer_id_without_publication(self):
        jobs = self.workflow["jobs"]
        self.assertEqual(jobs["benchmark"]["outputs"],
                         {"run-identity": "${{ steps.identity.outputs.run-identity }}",
                          "result-artifact-id": "${{ steps.raw-results.outputs.artifact-id }}"})
        job = jobs["report"]
        self.assertEqual(job["needs"], ["validate", "benchmark"])
        self.assertEqual(job["if"], "always() && needs.validate.result == 'success' && needs.benchmark.outputs.result-artifact-id != '' && needs.benchmark.outputs.run-identity != ''")
        downloads = [step for step in job["steps"] if step.get("uses", "").startswith("actions/download-artifact@")]
        self.assertEqual([step["with"] for step in downloads], [{
            "artifact-ids": "${{ needs.benchmark.outputs.result-artifact-id }}", "path": "${{ runner.temp }}/aws-raw"}])
        checkout = job["steps"][0]
        self.assertEqual(checkout["with"], {"ref": "${{ github.sha }}", "persist-credentials": "false"})
        source = json.dumps(job)
        for forbidden in ("workflow_run", "repository_dispatch", "benchmarks.publish", "--policy", "--history",
                          "secrets.", "aws-sdk", "configure-aws-credentials"):
            self.assertNotIn(forbidden, source)
        self.assertEqual([step["with"]["retention-days"] for step in job["steps"]
                          if step.get("uses", "").startswith("actions/upload-artifact@")], ["90"])


    def producer(self, root, attempt="1"):
        steps = self.workflow["jobs"]["benchmark"]["steps"]
        step = next(step for step in steps if step.get("id") == "identity")
        self.assertLess(steps.index(step), next(index for index, item in enumerate(steps)
                        if item.get("uses", "").startswith("actions/download-artifact@")))
        lines = step["run"].splitlines()
        self.assertEqual((lines[0], lines[-1]), ("python3 - <<'PY'", "PY"))
        env = dict(GITHUB_REF=identity.REF, GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT=attempt,
                   GITHUB_SHA="c" * 40, GITHUB_REPOSITORY=identity.REPOSITORY,
                   GITHUB_WORKFLOW_REF=identity.WORKFLOW, GITHUB_WORKFLOW_SHA="c" * 40,
                   BASELINE="a" * 40, CANDIDATE="a" * 40, SUITE="aa-smoke",
                   GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_OUTPUT=str(root / "output"),
                   RUNNER_TEMP=str(root), GITHUB_STEP_SUMMARY=str(root / "summary.md"))
        printed = io.StringIO()
        with patch.dict(os.environ, env), redirect_stdout(printed), \
             patch.object(identity, "environment", wraps=identity.environment) as read:
            exec(compile("\n".join(lines[1:-1]), "producer identity step", "exec"), {})
            read.assert_called_once_with()
        name, encoded = (root / "output").read_text().strip().split("=", 1)
        self.assertEqual(name, "run-identity")
        self.assertEqual((root / "aws-identity.json").read_text(), encoded)
        self.assertEqual(printed.getvalue(), "RCP_AWS_PRODUCER_IDENTITY=" + encoded + "\n")
        self.assertEqual((root / "summary.md").read_text(),
                         "Benchmark producer identity\n\n```json\n" + encoded + "\n```\n\n")
        return env, encoded

    def test_benchmark_rerun_prints_actual_producer_attempt_for_offline_ingestion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, encoded = self.producer(root, attempt="2")
            binding = identity.load(root / "aws-identity.json")
            self.assertEqual(binding, json.loads(encoded))
            self.assertEqual(binding["run"], "123-2")
            self.assertEqual(binding["run_url"], "https://github.com/wykurz/rcp/actions/runs/123/attempts/2")

    def test_controller_receives_the_producer_identity_file_without_duplicate_flags(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, encoded = self.producer(root)
            binding = identity.load(root / "aws-identity.json")
            self.assertEqual(binding, json.loads(encoded))
            self.assertEqual(binding["run"], "123-1")
        step = next(step for step in self.workflow["jobs"]["benchmark"]["steps"]
                    if step.get("name") == "Run within the operator window and verify teardown")
        self.assertIn('--identity "$RUNNER_TEMP/aws-identity.json"', step["run"])
        for field in ("run", "baseline", "candidate", "suite"):
            self.assertNotIn("--" + field + " ", step["run"])
            self.assertNotIn(field.upper(), step["env"])

    def test_report_only_retry_keeps_original_producer_attempt_and_raw_evidence(self):
        from infra.aws_benchmark.test_results import bundle
        step = next(step for step in self.workflow["jobs"]["report"]["steps"]
                    if step.get("name") == "Validate bounded evidence and render diagnostic report")
        self.assertEqual(step["env"], {
            "PRODUCER_IDENTITY": "${{ needs.benchmark.outputs.run-identity }}",
            "PRODUCER_CONCLUSION": "${{ needs.benchmark.result }}"})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env, encoded = self.producer(root)
            expected = json.loads(encoded)
            bundle(root / "aws-raw", expected)
            before = {path.name: path.read_bytes() for path in (root / "aws-raw").iterdir()}
            retry = dict(os.environ, **env)
            retry.update(GITHUB_RUN_ATTEMPT="2", BASELINE="d" * 40, CANDIDATE="e" * 40,
                         PRODUCER_IDENTITY=encoded, PRODUCER_CONCLUSION="success",
                         PATH=str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"])
            completed = subprocess.run(["bash", "-e", "-c", step["run"]], env=retry,
                                       capture_output=True, text=True, timeout=20)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            report = root / "aws-report"
            provenance = json.loads((report / "provenance.json").read_text())
            self.assertEqual(provenance["identity"], expected)
            self.assertEqual(provenance["errors"], [])
            self.assertTrue((report / "dashboard/index.html").is_file())
            for name, data in before.items():
                self.assertEqual((root / "aws-raw" / name).read_bytes(), data)
                self.assertEqual((report / "raw" / name).read_bytes(), data)
        upload = next(step for step in self.workflow["jobs"]["report"]["steps"]
                      if step.get("id") == "report-results")
        name = upload["with"]["name"]
        self.assertIn("${{ github.run_attempt }}", name)
        self.assertNotEqual(name.replace("${{ github.run_attempt }}", "1"),
                            name.replace("${{ github.run_attempt }}", "2"))
        self.assertNotIn("overwrite", upload["with"])

    def test_identity_file_rejects_oversize_duplicate_keys_and_other_attempt_url(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, encoded = self.producer(root)
            path = root / "aws-identity.json"
            for payload, message in (
                    (" " * 4097, "exceeds its size limit"),
                    (encoded[:-1] + ',"run":"123-2"}', "duplicate JSON key"),
                    (encoded.replace("/attempts/1", "/attempts/2"), "run URL does not match"),
                    (encoded.replace("refs/heads/main", "refs/heads/other"), "not the exact approved main branch")):
                with self.subTest(message=message):
                    path.write_text(payload)
                    with self.assertRaisesRegex(ValueError, message):
                        identity.load(path)

    def test_trusted_upload_summaries_bind_ids_and_digests_and_warn_on_missing_upload(self):
        for job_name, producer, label in (("benchmark", "raw-results", "raw"),
                                         ("report", "report-results", "report")):
            steps = self.workflow["jobs"][job_name]["steps"]
            upload = next(step for step in steps if step.get("id") == producer)
            summary = next(step for step in steps if step.get("name") == "Identify the trusted " + label + " artifact")
            self.assertGreater(steps.index(summary), steps.index(upload))
            self.assertEqual(summary["if"], "always()")
            self.assertEqual(summary["env"], {
                "ARTIFACT_ID": "${{ steps." + producer + ".outputs.artifact-id }}",
                "ARTIFACT_DIGEST": "${{ steps." + producer + ".outputs.artifact-digest }}"})
            for artifact_id, digest, valid in (("456", "a" * 64, True), ("", "", False),
                    ("456", "", False), ("", "a" * 64, False), ("456", "forged", False),
                    ("456\n::notice::forged", "a" * 64, False)):
                with self.subTest(job=job_name, artifact_id=artifact_id), tempfile.TemporaryDirectory() as directory:
                    output = Path(directory) / "summary.md"
                    env = dict(os.environ, GITHUB_RUN_ID="123", GITHUB_STEP_SUMMARY=str(output),
                               ARTIFACT_ID=artifact_id, ARTIFACT_DIGEST=digest)
                    completed = subprocess.run(["bash", "-e", "-c", summary["run"]], env=env,
                                               capture_output=True, text=True, timeout=5)
                    self.assertEqual(completed.returncode, 0, completed.stderr)
                    text = output.read_text()
                    self.assertTrue(text.startswith("\n"))
                    if valid:
                        self.assertIn("[ID 456](https://github.com/wykurz/rcp/actions/runs/123/artifacts/456)", text)
                        self.assertIn("SHA-256: `" + digest + "`", text)
                    else:
                        self.assertEqual(text, "\nNo trusted " + label + " artifact was uploaded. "
                                              "Do not select an artifact by name.\n")


if __name__ == "__main__":
    unittest.main()
