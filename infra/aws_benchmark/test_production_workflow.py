import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[2]


class ProductionWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.load((ROOT / ".github/workflows/aws-benchmark-production.yml").read_text(), Loader=yaml.BaseLoader)

    def test_initial_workflow_has_only_manual_trigger_and_disabled_variable_gate(self):
        self.assertEqual(set(self.workflow["on"]), {"workflow_dispatch"})
        guard = self.workflow["jobs"]["validate"]["if"]
        for check in ("github.repository == 'wykurz/rcp'", "github.ref == 'refs/heads/main'",
                      "github.event_name == 'workflow_dispatch'", "vars.RCP_AWS_PRODUCTION_ENABLED == 'true'"):
            self.assertIn(check, guard)
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})

    def test_real_early_guard_rejects_other_requesters_and_unpinned_sources(self):
        source = self.workflow["jobs"]["validate"]["steps"][0]["run"]
        for actor, revision, valid in (("483465", "a" * 40, True), ("1", "a" * 40, False),
                                       ("483465", "main", False), ("483465", "a" * 39, False)):
            with self.subTest(actor=actor, revision=revision), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "output"
                env = dict(os.environ, REQUESTER_ID=actor, REVISION=revision, GITHUB_OUTPUT=str(output))
                result = subprocess.run(["bash", "-e", "-c", source], env=env, capture_output=True, timeout=5)
                self.assertEqual(result.returncode == 0, valid)
                if valid:
                    self.assertEqual(output.read_text(), "revision=" + revision + "\n")
                else:
                    self.assertFalse(output.exists())

    def test_candidate_builds_without_cloud_credentials_or_shared_cache(self):
        job = self.workflow["jobs"]["build"]
        self.assertEqual(job["permissions"], {"contents": "read"})
        self.assertNotIn("environment", job)
        self.assertEqual(job["runs-on"], "ubuntu-24.04")
        steps = job["steps"]
        self.assertEqual(steps[0]["with"], {"ref": "${{ needs.validate.outputs.revision }}", "persist-credentials": "false"})
        self.assertEqual(steps[1]["with"]["cache"], "false")
        self.assertNotIn("matrix", job)
        text = str(job)
        for forbidden in ("id-token", "secrets.", "configure-aws-credentials", "self-hosted"):
            self.assertNotIn(forbidden, text)

    def test_privileged_submit_checks_out_controller_and_never_executes_candidate(self):
        job = self.workflow["jobs"]["submit"]
        self.assertEqual(job["needs"], ["validate", "build"])
        self.assertEqual(job["permissions"], {"contents": "read", "id-token": "write"})
        self.assertEqual(job["timeout-minutes"], "15")
        self.assertNotIn("environment", job)
        self.assertEqual(job["steps"][0]["with"], {"ref": "${{ github.sha }}", "persist-credentials": "false"})
        download = job["steps"][1]["with"]
        self.assertEqual(download, {"name": "production-aa-binaries", "path": "${{ runner.temp }}/candidate-data"})
        run = next(step["run"] for step in job["steps"] if step.get("name") == "Reserve, upload data and submit the existing run")
        self.assertIn("infra.aws_benchmark.production_client", run)
        self.assertNotIn("chmod", run)
        self.assertNotIn("eval", run)
        self.assertNotIn("source ", run)
        self.assertEqual(job["concurrency"]["cancel-in-progress"], "false")

    def test_capabilities_never_become_workflow_artifacts(self):
        job = self.workflow["jobs"]["submit"]
        uploads = [step for step in job["steps"] if step.get("uses", "").startswith("actions/upload-artifact@")]
        self.assertEqual(len(uploads), 1)
        self.assertEqual(uploads[0]["with"]["path"], "${{ runner.temp }}/production-admission.json")
        self.assertEqual(uploads[0]["if"], "always()")
        self.assertEqual(uploads[0]["with"]["retention-days"], "90")
        self.assertNotIn("overwrite", uploads[0]["with"])


if __name__ == "__main__":
    unittest.main()
