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

    def test_only_owner_manual_or_explicitly_enabled_schedule_can_build(self):
        self.assertEqual(set(self.workflow["on"]), {"workflow_dispatch", "schedule"})
        self.assertEqual(self.workflow["on"]["schedule"],
                         [{"cron": "17 8 * * 1,5"}])
        guard = self.workflow["jobs"]["validate"]["if"]
        for check in ("github.repository == 'wykurz/rcp'", "github.ref == 'refs/heads/main'",
                      "github.event_name == 'workflow_dispatch'", "vars.RCP_AWS_PRODUCTION_ENABLED == 'true'",
                      "vars.RCP_AWS_PRODUCTION_SCHEDULE_ENABLED == 'true'", "github.actor_id == '483465'",
                      "github.run_attempt == '1'"):
            self.assertIn(check, guard)
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})

    def test_real_early_guard_rejects_other_requesters_and_unpinned_sources(self):
        from .test_production_workflow_request import environment
        step = next(step for step in self.workflow["jobs"]["validate"]["steps"] if step.get("id") == "source")
        for actor, revision, valid in (("483465", "a" * 40, True), ("1", "a" * 40, False),
                                       ("483465", "main", False), ("483465", "a" * 39, False)):
            with self.subTest(actor=actor, revision=revision), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "output"
                env = dict(os.environ, **environment(GITHUB_ACTOR_ID=actor, RCP_REQUESTED_REVISION=revision),
                           GITHUB_OUTPUT=str(output))
                result = subprocess.run(["bash", "-e", "-c", step["run"]], env=env,
                                        cwd=ROOT, capture_output=True, timeout=5)
                self.assertEqual(result.returncode == 0, valid, result.stderr)
                if valid:
                    self.assertEqual(output.read_text(), "revision=" + revision + "\n")
                else:
                    self.assertFalse(output.exists())

    def test_retired_commissioning_schedule_cannot_emit_a_build_revision(self):
        from .test_production_workflow_request import environment
        step = next(step for step in self.workflow["jobs"]["validate"]["steps"] if step.get("id") == "source")
        self.assertEqual(step["env"]["RCP_EVENT_SCHEDULE"], "${{ github.event.schedule }}")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            env = dict(os.environ, **environment(GITHUB_EVENT_NAME="schedule", RCP_SCHEDULE_ENABLED="true",
                RCP_REQUESTED_REVISION="", RCP_EVENT_SCHEDULE="17 7 8 11 *"), GITHUB_OUTPUT=str(output))
            result = subprocess.run(["bash", "-e", "-c", step["run"]], env=env,
                                    cwd=ROOT, capture_output=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"Benchmark request rejected", result.stderr)
            self.assertFalse(output.exists())

    def test_schedule_has_no_privileged_dispatch_or_unreviewed_credentials(self):
        validate = self.workflow["jobs"]["validate"]
        self.assertEqual(validate["steps"][0]["with"]["ref"], "${{ github.sha }}")
        self.assertNotIn("actions: write", str(self.workflow))
        for job in ("build", "submit"):
            self.assertIn("github.event_name == 'schedule'", self.workflow["jobs"][job]["if"])
        self.assertEqual(self.workflow["jobs"]["build"]["needs"], "validate")
        self.assertNotIn("environment", validate)

    def test_candidate_builds_without_cloud_credentials_or_shared_cache(self):
        job = self.workflow["jobs"]["build"]
        self.assertEqual(job["permissions"], {"contents": "read"})
        self.assertEqual(job["cache-mode"], "none")
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
