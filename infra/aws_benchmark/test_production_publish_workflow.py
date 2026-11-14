"""Offline capability, provenance and Git reconciliation checks for publication."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import yaml

from . import production_reports as reports
from . import history_contract as contract

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/aws-benchmark-publish.yml"
SHA = "a" * 40
INVENTORY = "b" * 64
REF = "refs/heads/aws-benchmark-history"
REMOTE = "https://github.com/wykurz/rcp.git"


def trusted_environment(directory):
    return dict(os.environ, RUNNER_TEMP=directory, GITHUB_REPOSITORY="wykurz/rcp",
        GITHUB_REPOSITORY_ID="655073755", GITHUB_REF="refs/heads/main", GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_ACTOR_ID="483465", GITHUB_SHA=SHA,
        GITHUB_WORKFLOW_SHA=SHA, GITHUB_RUN_ATTEMPT="1",
        GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/aws-benchmark-publish.yml@refs/heads/main",
        EXPECTED_READER=SHA, EXPECTED_INVENTORY=INVENTORY, EXPECTED_ARTIFACT="123",
        GITHUB_STEP_SUMMARY=str(Path(directory) / "summary"), GITHUB_OUTPUT=str(Path(directory) / "job-output"),
        PUBLICATION_ENABLED="true", TRUST_READY="true", CLIENT_ID="Iv23.test-client", INSTALLATION_ID="5678")


class PublicationWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
        self.jobs = self.workflow["jobs"]

    def test_public_reporting_has_no_writer_or_launch_lane_and_preserves_bounded_reader_test(self):
        self.assertEqual(set(self.workflow["on"]), {"workflow_dispatch", "push", "schedule"})
        self.assertEqual(self.workflow["on"]["push"]["branches"], ["main"])
        self.assertEqual(self.workflow["on"]["schedule"], [{"cron": "43 9 * * *"}, {"cron": "43 3 * * 2,6"}])
        self.assertEqual(set(self.jobs), {"download", "pages", "reader-test"})
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        reader = self.jobs["download"]
        self.assertEqual(reader["permissions"], {"contents": "read", "id-token": "write"})
        self.assertEqual(reader["timeout-minutes"], "15")
        self.assertEqual(reader["cache-mode"], "none")
        self.assertNotIn("environment", reader)
        for lane in (reader, self.jobs["pages"]):
            for guard in ("github.repository_id == '655073755'", "github.ref == 'refs/heads/main'",
                    "github.actor_id == '483465'", "github.run_attempt == '1'",
                    "vars.RCP_AWS_PRODUCTION_PUBLISH_ENABLED == 'true'",
                    "vars.RCP_AWS_PRODUCTION_PUBLISH_SCHEDULE_ENABLED == 'true'"):
                self.assertIn(guard, lane["if"])
            self.assertNotIn("TRUST_READY", lane["if"])
        text = WORKFLOW.read_text()
        for obsolete in ("private-key:", "create-github-app-token", "aws-benchmark-history.git", "secrets.",
                         "production_publish ", "production_submit", "production_arm"):
            self.assertNotIn(obsolete, text)
        bounded = self.jobs["reader-test"]
        self.assertNotIn("environment", bounded)
        self.assertEqual(bounded["timeout-minutes"], "5")
        self.assertIn("inputs.mode == 'reader-test'", bounded["if"])
        self.assertIn("github.event_name == 'workflow_dispatch'", bounded["if"])
        self.assertIn("acceptance_publication_workflow", str(bounded))

    def test_post_drain_reporting_follows_both_regular_benchmark_windows(self):
        from datetime import datetime, timedelta, timezone
        from .common import DURABLE_PLAN, durable_plan_contract
        from .test_production_policy import policy
        from .production_policy import MAX_RUN_AGE_SECONDS
        configured = policy(schedule_delay_seconds=3600)
        with self.assertRaises(ValueError):
            policy(schedule_delay_seconds=3601)
        # maximum schedule lateness and admitted workflow age precede the capacity/runtime windows,
        # then the immutable 15m drain and a generous one-hour collection margin.
        for weekday, report_weekday in ((0, 1), (4, 5)):
            slot = datetime(2026, 11, 9 + weekday, 8, 17, tzinfo=timezone.utc)
            latest = slot + timedelta(seconds=configured.schedule_delay_seconds + MAX_RUN_AGE_SECONDS +
                configured.capacity_seconds + configured.runtime_seconds + durable_plan_contract(DURABLE_PLAN)["delivery_drain_seconds"])
            reporting = (slot + timedelta(days=1)).replace(hour=3, minute=43)
            self.assertEqual(reporting.weekday(), report_weekday)
            self.assertGreaterEqual(reporting - latest, timedelta(hours=1))
        scheduled = self.workflow["on"]["schedule"]
        self.assertEqual(len(scheduled), 2)
        self.assertEqual(scheduled[1]["cron"], "43 3 * * 2,6")

    def test_real_download_identity_script_rejects_wrong_source_actor_or_flags_under_optimized_python(self):
        script = next(x["run"] for x in self.jobs["download"]["steps"] if x.get("name", "").startswith("Require the exact"))
        cases = [({}, True), ({"GITHUB_REPOSITORY_ID": "1"}, False), ({"GITHUB_ACTOR_ID": "1"}, False),
            ({"GITHUB_REF": "refs/heads/debug"}, False), ({"GITHUB_WORKFLOW_SHA": "c" * 40}, False),
            ({"GITHUB_EVENT_NAME": "pull_request"}, False), ({"GITHUB_RUN_ATTEMPT": "2"}, False),
            ({"PUBLICATION_ENABLED": "false"}, False), ({"GITHUB_EVENT_NAME": "push"}, True),
            ({"GITHUB_EVENT_NAME": "schedule", "PUBLISH_SCHEDULE_ENABLED": "true"}, True),
            ({"GITHUB_EVENT_NAME": "schedule", "PUBLISH_SCHEDULE_ENABLED": "false"}, False)]
        for change, valid in cases:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                env = trusted_environment(directory) | change | {"PYTHONOPTIMIZE": "2"}
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", script], cwd=ROOT,
                                        env=env, capture_output=True, timeout=10)
                self.assertEqual(result.returncode == 0, valid, result.stdout + result.stderr)

    def test_exact_current_run_artifact_and_trusted_digest_go_directly_to_read_only_renderer(self):
        reader = self.jobs["download"]
        upload = next(x for x in reader["steps"] if x.get("id") == "public")
        self.assertEqual(upload["with"]["name"], "aws-public-records-${{ github.run_id }}-${{ github.run_attempt }}")
        self.assertEqual(upload["with"]["retention-days"], "1")
        self.assertEqual(self.jobs["pages"]["needs"], "download")
        self.assertEqual(self.jobs["pages"]["uses"], "./.github/workflows/benchmark-pages.yml")
        self.assertEqual(self.jobs["pages"]["with"], {
            "aws_records_artifact": "${{ needs.download.outputs.artifact_id }}",
            "aws_inventory_sha256": "${{ needs.download.outputs.inventory_sha256 }}",
            "aws_reader_revision": "${{ needs.download.outputs.reader_revision }}"})
        self.assertNotIn("secrets", self.jobs["pages"])
        self.assertEqual(reader["outputs"]["artifact_id"], "${{ steps.public.outputs.artifact-id }}")
        self.assertEqual(reader["outputs"]["inventory_sha256"], "${{ steps.download.outputs.inventory_sha256 }}")
        steps = reader["steps"]
        identity = next(i for i, x in enumerate(steps) if "production_reports identity" in x.get("run", ""))
        install = next(i for i, x in enumerate(steps) if "pip" in x.get("run", ""))
        download = next(i for i, x in enumerate(steps) if x.get("id") == "download")
        self.assertLess(identity, install)
        self.assertLess(install, download)
        self.assertIn("--require-hashes", steps[install]["run"])
        self.assertIn("production_reports download", steps[download]["run"])
        self.assertIn("--require-complete", steps[download]["run"])
        self.assertNotIn("--history", steps[download]["run"])

    def test_actions_and_controller_checkouts_are_immutable_and_shell_programs_parse(self):
        for path in (WORKFLOW, WORKFLOW.parent / "benchmark-pages.yml", WORKFLOW.parent / contract.CANARY_WORKFLOW):
            value = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
            for job in value["jobs"].values():
                for step in job.get("steps", []):
                    action = step.get("uses", "")
                    if action and not action.startswith("./"):
                        self.assertRegex(action, r"^[^@]+@[0-9a-f]{40}$")
                    if action.startswith("actions/checkout@"):
                        self.assertEqual(step["with"].get("persist-credentials"), "false")
                    if "run" in step:
                        result = subprocess.run(["bash", "-n"], input=step["run"], text=True, capture_output=True)
                        self.assertEqual(result.returncode, 0, result.stderr)

    def test_only_dormant_canary_keeps_the_writer_credential_contract(self):
        from .workflow_inventory import check_workflows
        self.assertEqual(check_workflows(WORKFLOW.parent), {(contract.CANARY_WORKFLOW, "verify")})
        canary = yaml.load((WORKFLOW.parent / contract.CANARY_WORKFLOW).read_text(), Loader=yaml.BaseLoader)["jobs"]["verify"]
        self.assertEqual(canary["environment"], {"name": contract.ENVIRONMENT, "deployment": "false"})
        self.assertIn("vars.RCP_AWS_HISTORY_CANARY_ENABLED == 'true'", canary["if"])
        mint = next(x for x in canary["steps"] if x.get("id") == "writer")
        self.assertEqual(mint["with"]["private-key"], "${{ secrets." + contract.WRITER_SECRET_NAME + " }}")
        for step in canary["steps"]:
            if "publication_identity canary-current-main" in step.get("run", ""):
                self.assertEqual(step["env"]["GH_TOKEN"], "${{ github.token }}")

    def test_canary_preflight_and_resume_evidence_precede_every_mutation_and_mint(self):
        job = yaml.load((WORKFLOW.parent / contract.CANARY_WORKFLOW).read_text(), Loader=yaml.BaseLoader)["jobs"]["verify"]
        steps = job["steps"]
        self.assertEqual(job["permissions"], {"contents": "write", "actions": "read"})
        self.assertEqual(steps[1]["run"].strip(), "python3 -m infra.aws_benchmark.publication_identity canary-current-main")
        metadata = next(i for i, x in enumerate(steps) if "canary_recovery" in x.get("run", ""))
        download = next(i for i, x in enumerate(steps) if x.get("uses", "").startswith("actions/download-artifact@"))
        check = next(i for i, x in enumerate(steps) if "history_canary resume-check" in x.get("run", ""))
        ordinary = next(i for i, x in enumerate(steps) if "history_canary ordinary" in x.get("run", ""))
        mint = next(i for i, x in enumerate(steps) if x.get("id") == "writer")
        self.assertLess(metadata, download)
        self.assertLess(download, check)
        self.assertLess(check, mint)
        self.assertLess(ordinary, mint)
        for index in (metadata, download, check):
            self.assertEqual(steps[index]["if"], "inputs.mode == 'resume'")
        self.assertEqual(steps[ordinary]["if"], "inputs.mode == 'bootstrap'")
        self.assertEqual(steps[download]["with"], {
            "artifact-ids": "${{ inputs.source_artifact_id }}", "run-id": "${{ inputs.source_run_id }}",
            "github-token": "${{ github.token }}", "path": "${{ runner.temp }}/canary-source",
            "merge-multiple": "true", "digest-mismatch": "error"})
        self.assertIn('--source-evidence "$RUNNER_TEMP/canary-source/canary.jsonl"', steps[check]["run"])
        self.assertIn('--source-sha256 "$SOURCE_SHA256"', steps[check]["run"])
        for index in (1, mint - 1):
            self.assertIn("publication_identity canary-current-main", steps[index]["run"])
        self.assertEqual(steps[-1]["with"]["retention-days"], "90")
        self.assertIn("canary-source/canary.jsonl", steps[-1]["with"]["path"])
        for key in ("CLIENT_ID", "INSTALLATION_ID", "CANARY_ENABLED", "TRUST_READY", "PUBLISH_ENABLED", "CANARY_MODE"):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:
                env = dict(trusted_environment(directory), GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/" +
                    contract.CANARY_WORKFLOW + "@refs/heads/main", CANARY_ENABLED="true", TRUST_READY="false",
                    PUBLISH_ENABLED="false", CANARY_MODE="bootstrap", PYTHONOPTIMIZE="2")
                env[key] = "invalid value"
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", steps[1]["run"]], cwd=ROOT,
                    env=env, capture_output=True, text=True, timeout=10)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Publication identity check failed:", result.stderr)



if __name__ == "__main__":
    unittest.main()
