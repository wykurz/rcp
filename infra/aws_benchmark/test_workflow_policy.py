"""Offline checks of the privileged workflow's branch and artifact boundaries."""

import ast
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

from infra.aws_benchmark import common


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
        trusted = {"refs/heads/main", "refs/heads/aws-benchmark-debug-20261019"}
        refs = trusted | {"refs/heads/other", "refs/heads/aws-benchmark-debug-202610190",
                          "refs/heads/aws-benchmark-debug-20261019/other", "refs/tags/main",
                          "refs/tags/aws-benchmark-debug-20261019", "refs/pull/357/merge", "refs/heads/MAIN",
                          "refs/heads/AWS-BENCHMARK-DEBUG-20261019", ""}
        for ref in refs:
            for enabled in ("true", "false", "", "TRUE"):
                with self.subTest(ref=ref, enabled=enabled):
                    # actions equality ignores case; the next step enforces literal branch names
                    actual = eval(code, {"__builtins__": {}}, {"ref": ref.lower(), "enabled": enabled.lower()})
                    self.assertEqual(actual, ref.lower() in trusted and enabled.lower() == "true")

    def test_actual_validation_script_rejects_other_refs_and_records_run_binding(self):
        steps = self.workflow["jobs"]["validate"]["steps"]
        script = next(s["run"] for s in steps if s.get("id") == "protocol").splitlines()
        self.assertEqual(script[0], "python3 - <<'PY'")
        self.assertEqual(script[-1], "PY")
        program = compile("\n".join(script[1:-1]), "actual workflow validation", "exec")
        trusted = {"refs/heads/main", "refs/heads/aws-benchmark-debug-20261019"}
        refs = trusted | {"refs/heads/MAIN", "refs/heads/AWS-BENCHMARK-DEBUG-20261019",
                          "refs/heads/other", "refs/tags/main", "refs/tags/aws-benchmark-debug-20261019",
                          "refs/pull/357/merge", "refs/heads/aws-benchmark-debug-202610190", ""}
        for ref in refs:
            with self.subTest(ref=ref), tempfile.TemporaryDirectory() as root:
                env = dict(GITHUB_REF=ref, GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT="1",
                           GITHUB_SHA="c" * 40, GITHUB_REPOSITORY="wykurz/rcp",
                           GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/aws-benchmark.yml@" + ref,
                           GITHUB_WORKFLOW_SHA="c" * 40, BASELINE="a" * 40, CANDIDATE="a" * 40,
                           SUITE="aa-smoke", GITHUB_OUTPUT=str(Path(root) / "output"))
                output = io.StringIO()
                with patch.dict(os.environ, env), redirect_stdout(output), \
                     patch.object(common, "validate_identity", wraps=common.validate_identity) as validate:
                    if ref not in trusted:
                        with self.assertRaisesRegex(ValueError, "not an approved branch"):
                            exec(program, {})
                        validate.assert_not_called()
                        self.assertFalse(Path(env["GITHUB_OUTPUT"]).exists())
                        continue
                    exec(program, {})
                    validate.assert_called_once()
                binding = json.loads(output.getvalue().removeprefix("RCP_AWS_RUN_IDENTITY="))
                self.assertEqual(binding, dict(run="123-1", baseline="a" * 40, candidate="a" * 40,
                    suite="aa-smoke", controller="c" * 40, ref=ref, repository="wykurz/rcp",
                    workflow=env["GITHUB_WORKFLOW_REF"], workflow_sha="c" * 40))

    def test_manual_trigger_and_single_mailbox_gate_every_privileged_run(self):
        self.assertEqual(set(self.workflow["on"]), {"workflow_dispatch"})
        self.assertEqual(self.workflow["concurrency"],
                         {"group": "aws-benchmark-single-mailbox", "cancel-in-progress": "false"})
        jobs = self.workflow["jobs"]
        self.assertEqual(set(jobs), {"validate", "build", "benchmark"})
        self.assertEqual(jobs["build"]["needs"], "validate")
        self.assertEqual(jobs["benchmark"]["needs"], "build")
        self.assertEqual(jobs["benchmark"]["environment"], "aws-benchmark")

    def test_candidate_builds_have_no_privileged_environment_or_oidc(self):
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        jobs = self.workflow["jobs"]
        self.assertEqual(jobs["build"]["permissions"], {"contents": "read"})
        for name in ("validate", "build"):
            self.assertNotIn("environment", jobs[name])
            self.assertNotIn("id-token", jobs[name].get("permissions", {}))
            self.assertEqual(jobs[name]["runs-on"], "ubuntu-24.04")
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


if __name__ == "__main__":
    unittest.main()
