"""Offline capability, provenance and Git reconciliation checks for publication."""

import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/aws-benchmark-publish.yml"
SHA = "a" * 40
INVENTORY = "b" * 64
REF = "refs/heads/aws-benchmark-history"
REMOTE = "https://github.com/wykurz/rcp.git"


def trusted_environment(directory):
    return dict(os.environ, RUNNER_TEMP=directory, GITHUB_REPOSITORY="wykurz/rcp",
        GITHUB_REF="refs/heads/main", GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_ACTOR_ID="483465", GITHUB_SHA=SHA,
        GITHUB_WORKFLOW_SHA=SHA, GITHUB_RUN_ATTEMPT="1",
        GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/aws-benchmark-publish.yml@refs/heads/main",
        EXPECTED_READER=SHA, EXPECTED_INVENTORY=INVENTORY, EXPECTED_ARTIFACT="123",
        GITHUB_STEP_SUMMARY=str(Path(directory) / "summary"), GITHUB_OUTPUT=str(Path(directory) / "job-output"))


class PublicationWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
        self.jobs = self.workflow["jobs"]
        self.phases = {name: self.jobs[name] for name in ("download", "export", "publish")}

    def test_manual_disabled_main_only_pipeline_has_exact_separated_capabilities(self):
        self.assertEqual(set(self.workflow["on"]), {"workflow_dispatch"})
        self.assertIn(self.workflow["on"]["workflow_dispatch"], (None, ""))
        self.assertEqual(set(self.jobs), {"download", "export", "publish", "pages"})
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        expected = {"download": {"contents": "read", "id-token": "write"},
                    "export": {"contents": "read"}, "publish": {"contents": "write"}}
        for name, job in self.phases.items():
            self.assertEqual(job["permissions"], expected[name])
            self.assertEqual(job["runs-on"], "ubuntu-24.04")
            self.assertLessEqual(int(job["timeout-minutes"]), 15)
            self.assertNotIn("environment", job)
            for guard in ("github.repository == 'wykurz/rcp'", "github.ref == 'refs/heads/main'",
                          "github.event_name == 'workflow_dispatch'", "github.actor_id == '483465'",
                          "vars.RCP_AWS_PRODUCTION_PUBLISH_ENABLED == 'true'", "github.run_attempt == '1'",
                          "vars.RCP_AWS_HISTORY_TRUST_READY == 'true'"):
                self.assertIn(guard, job["if"])
        self.assertEqual(self.jobs["export"]["needs"], "download")
        self.assertEqual(self.jobs["publish"]["needs"], "export")
        self.assertEqual(self.jobs["publish"]["concurrency"],
                         {"group": "aws-production-history-publish", "cancel-in-progress": "false"})

    def test_actual_revision_guards_reject_foreign_or_malformed_runtime_inputs(self):
        for name, job in self.phases.items():
            source = job["steps"][0]["run"]
            mutations = ({}, {"GITHUB_REPOSITORY": "other/rcp"}, {"GITHUB_REF": "refs/heads/debug"},
                {"GITHUB_EVENT_NAME": "pull_request"}, {"GITHUB_ACTOR_ID": "1"}, {"GITHUB_SHA": "main"},
                {"GITHUB_WORKFLOW_SHA": "c" * 40}, {"GITHUB_WORKFLOW_REF": "other"}, {"GITHUB_RUN_ATTEMPT": "2"})
            if name != "download":
                mutations += ({"EXPECTED_READER": "c" * 40}, {"EXPECTED_INVENTORY": "bad"},
                              {"EXPECTED_ARTIFACT": "123,456"}, {"EXPECTED_ARTIFACT": ""})
            for change in mutations:
                with self.subTest(job=name, change=change), tempfile.TemporaryDirectory() as directory:
                    env = {**trusted_environment(directory), **change}
                    result = subprocess.run(["bash", "-e", "-c", source], env=env, capture_output=True, timeout=5)
                    self.assertEqual(result.returncode == 0, not change)

    def test_actions_are_immutable_and_controller_checkout_is_always_current_sha(self):
        for job in self.phases.values():
            for item in job["steps"]:
                if "uses" in item:
                    self.assertRegex(item["uses"], r"^[a-zA-Z0-9_/-]+@[0-9a-f]{40}$")
                    if item["uses"].startswith("actions/checkout@"):
                        self.assertEqual(item["with"], {"ref": "${{ github.sha }}", "persist-credentials": "false"})

    def test_exact_current_run_artifact_ids_and_trusted_inventory_hash_cross_each_boundary(self):
        for current, previous in (("export", "download"), ("publish", "export")):
            job = self.jobs[current]
            downloads = [item for item in job["steps"] if item.get("uses", "").startswith("actions/download-artifact@")]
            self.assertEqual(len(downloads), 1)
            values = downloads[0]["with"]
            self.assertEqual(values["artifact-ids"], "${{ needs." + previous + ".outputs.artifact_id }}")
            self.assertEqual(values["merge-multiple"], "true")
            self.assertEqual(values["digest-mismatch"], "error")
            self.assertEqual(set(values), {"artifact-ids", "path", "merge-multiple", "digest-mismatch"})
            guard = job["steps"][0]["env"]
            for field, output in (("EXPECTED_READER", "reader_revision"), ("EXPECTED_INVENTORY", "inventory_sha256"),
                                  ("EXPECTED_ARTIFACT", "artifact_id")):
                self.assertEqual(guard[field], "${{ needs." + previous + ".outputs." + output + " }}")
            invoke = next(item for item in job["steps"] if "production_publish " + current in item.get("run", ""))
            self.assertIn('--reader-revision "$GITHUB_SHA"', invoke["run"])
            self.assertIn('--expected-sha256 "$EXPECTED_INVENTORY"', invoke["run"])
            self.assertEqual(invoke["env"]["EXPECTED_INVENTORY"], "${{ needs." + previous + ".outputs.inventory_sha256 }}")
        self.assertEqual(self.jobs["download"]["outputs"]["artifact_id"], "${{ steps.raw.outputs.artifact-id }}")
        self.assertEqual(self.jobs["export"]["outputs"]["artifact_id"], "${{ steps.public.outputs.artifact-id }}")

    def test_raw_evidence_is_short_lived_and_never_delivered_to_writer(self):
        for name, directory in (("download", "retired-evidence"), ("export", "public-records")):
            uploads = [item for item in self.jobs[name]["steps"] if item.get("uses", "").startswith("actions/upload-artifact@")]
            self.assertEqual(len(uploads), 1)
            self.assertEqual(uploads[0]["with"]["path"], "${{ runner.temp }}/" + directory + "/")
            self.assertEqual(uploads[0]["with"]["retention-days"], "1")
            self.assertEqual(uploads[0]["with"]["if-no-files-found"], "error")
            self.assertNotIn("if", uploads[0])
            self.assertNotIn("overwrite", uploads[0]["with"])
        self.assertNotIn("retired-evidence", str(self.jobs["publish"]))
        self.assertNotIn("production_publish download", str(self.jobs["publish"]))

    def test_export_has_no_cloud_or_writer_credential_and_no_data_branch_checkout(self):
        export = str(self.jobs["export"])
        for forbidden in ("id-token", "github.token", "secrets.", "ROLE:", "aws-actions", "configure-aws", "pip install"):
            self.assertNotIn(forbidden, export)
        for job in self.phases.values():
            source = "\n".join(item.get("run", "") for item in job["steps"])
            self.assertIn("git init --bare", source)
            self.assertIn(REF + ":" + REF, source)
            for forbidden in ("git checkout", "git reset", "git clean", "--force", "--mirror", "git pull"):
                self.assertNotIn(forbidden, source)
            self.assertEqual(job["env"]["GIT_CONFIG_GLOBAL"], "/dev/null")
            self.assertEqual(job["env"]["GIT_TERMINAL_PROMPT"], "0")

    def test_pages_reuses_reviewed_workflow_only_with_verified_history_commit(self):
        job = self.jobs["pages"]
        self.assertEqual(job["needs"], "publish")
        self.assertEqual(job["uses"], "./.github/workflows/benchmark-pages.yml")
        self.assertEqual(job["with"], {"aws_history_commit": "${{ needs.publish.outputs.history_commit }}"})
        self.assertEqual(job["permissions"], {"contents": "read", "pages": "write", "id-token": "write"})
        for guard in ("vars.RCP_BENCHMARK_PAGES == 'true'", "vars.RCP_AWS_PRODUCTION_PUBLISH_ENABLED == 'true'",
                      "needs.publish.outputs.history_commit != ''", "github.ref == 'refs/heads/main'",
                      "github.actor_id == '483465'"):
            self.assertIn(guard, job["if"])
        self.assertNotIn("secrets", job)
        self.assertNotIn("concurrency", job)
        self.assertEqual(self.jobs["publish"]["outputs"], {"history_commit": "${{ steps.append.outputs.history_commit }}"})

    def test_every_embedded_shell_program_has_valid_syntax(self):
        for name, job in self.phases.items():
            for item in job["steps"]:
                if "run" in item:
                    with self.subTest(job=name, step=item.get("name")):
                        result = subprocess.run(["bash", "-n"], input=item["run"], text=True, capture_output=True, timeout=5)
                        self.assertEqual(result.returncode, 0, result.stderr)

    def writer(self, directory, *, push_results="0", ls_status="0", no_change="0", malformed=False, shadow_only=False):
        root = Path(directory)
        bin_dir = root / "bin"
        bin_dir.mkdir()
        log = root / "commands.jsonl"
        git = bin_dir / "git"
        git.write_text("#!" + sys.executable + "\n" + r"""
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ['COMMAND_LOG'], 'a') as stream:
    stream.write(json.dumps(dict(args=args, header=bool(os.environ.get('GIT_CONFIG_VALUE_0')))) + '\n')
if 'init' in args:
    pathlib.Path(args[-1]).mkdir()
    raise SystemExit(0)
if 'ls-remote' in args:
    if os.environ['LS_STATUS'] == '0':
        print('c' * 40 + '\t' + ('refs/heads/shadow/refs/heads/aws-benchmark-history' if os.environ['SHADOW_ONLY'] == '1' else 'refs/heads/aws-benchmark-history'))
    raise SystemExit(int(os.environ['LS_STATUS']))
if 'fetch' in args:
    raise SystemExit(0)
if 'rev-parse' in args:
    print('c' * 40)
    raise SystemExit(0)
if 'push' in args:
    count = pathlib.Path(os.environ['RUNNER_TEMP']) / 'push-count'
    index = int(count.read_text()) if count.exists() else 0
    count.write_text(str(index + 1))
    statuses = os.environ['PUSH_RESULTS'].split(',')
    raise SystemExit(int(statuses[min(index, len(statuses)-1)]))
raise SystemExit('unexpected git command')
""")
        git.chmod(0o700)
        python = bin_dir / "python3"
        python.write_text("#!" + sys.executable + "\n" + r"""
import os, pathlib, sys
if sys.argv[1:4] == ['-m', 'infra.aws_benchmark.production_publish', 'publish']:
    count = pathlib.Path(os.environ['RUNNER_TEMP']) / 'publish-count'
    attempt = (int(count.read_text()) if count.exists() else 0) + 1
    count.write_text(str(attempt))
    empty = int(os.environ['NO_CHANGE']) == attempt
    receipt = 'unexpected=data\n' if os.environ['MALFORMED'] == '1' else 'commit=' + ('' if empty else 'a' * 40) + '\n'
    pathlib.Path(os.environ['GITHUB_OUTPUT']).write_text(receipt)
    raise SystemExit(0)
os.execv(sys.executable, [sys.executable, *sys.argv[1:]])
""")
        python.chmod(0o700)
        env = dict(trusted_environment(directory), PATH=str(bin_dir) + os.pathsep + os.environ["PATH"],
                   COMMAND_LOG=str(log), PUSH_RESULTS=push_results, LS_STATUS=ls_status, NO_CHANGE=no_change,
                   MALFORMED="1" if malformed else "0", GH_TOKEN="synthetic-test-token", SHADOW_ONLY="1" if shadow_only else "0")
        source = self.jobs["publish"]["steps"][-1]["run"]
        result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", source], env=env,
                                text=True, capture_output=True, timeout=10)
        commands = [json.loads(line) for line in log.read_text().splitlines()]
        return result, commands

    def test_writer_reconciles_push_conflict_against_fresh_bare_history(self):
        with tempfile.TemporaryDirectory() as directory:
            result, commands = self.writer(directory, push_results="1,0")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(sum("fetch" in item["args"] for item in commands), 2)
            pushes = [item for item in commands if "push" in item["args"]]
            self.assertEqual(len(pushes), 2)
            for item in commands:
                self.assertEqual(item["header"], "push" in item["args"])
                self.assertFalse(any("synthetic-test-token" in arg for arg in item["args"]))
                self.assertNotIn("--force", item["args"])
            for item in pushes:
                self.assertEqual(item["args"][-2:], [REMOTE, SHA + ":" + REF])
            self.assertNotIn("synthetic-test-token", result.stdout + result.stderr)
            self.assertNotIn(base64.b64encode(b"x-access-token:synthetic-test-token").decode(), result.stdout + result.stderr)

    def test_writer_reconciles_lost_success_as_no_change_without_another_push(self):
        with tempfile.TemporaryDirectory() as directory:
            result, commands = self.writer(directory, push_results="1", no_change="2")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(sum("fetch" in item["args"] for item in commands), 2)
            self.assertEqual(sum("push" in item["args"] for item in commands), 1)
            self.assertEqual((Path(directory) / "job-output").read_text(), "history_commit=" + "c" * 40 + "\n")

    def test_suffix_ref_does_not_trigger_fetch_of_an_absent_exact_history_branch(self):
        with tempfile.TemporaryDirectory() as directory:
            result, commands = self.writer(directory, shadow_only=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(any("fetch" in item["args"] for item in commands))
            self.assertEqual(sum("push" in item["args"] for item in commands), 1)
        for name in ("download", "export"):
            step = next(item for item in self.jobs[name]["steps"] if "ls-remote" in item.get("run", ""))
            self.assertIn('$2 == "refs/heads/aws-benchmark-history"', step["run"])

    def test_writer_caps_push_failures_at_three_and_never_forces(self):
        with tempfile.TemporaryDirectory() as directory:
            result, commands = self.writer(directory, push_results="1")
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(sum("push" in item["args"] for item in commands), 3)
            self.assertEqual(sum("fetch" in item["args"] for item in commands), 3)
            self.assertIn("reconciliation", result.stderr)

    def test_no_change_with_no_remote_history_does_not_offer_a_pages_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            result, commands = self.writer(directory, ls_status="2", no_change="1")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(any("push" in item["args"] or "rev-parse" in item["args"] for item in commands))
            self.assertEqual((Path(directory) / "job-output").read_text(), "history_commit=\n")

    def test_only_confirmed_absent_branch_bootstraps_and_malformed_receipt_cannot_push(self):
        for status, valid in (("2", True), ("128", False)):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                result, commands = self.writer(directory, ls_status=status)
                self.assertEqual(result.returncode == 0, valid)
                self.assertEqual(sum("fetch" in item["args"] for item in commands), 0)
                self.assertEqual(sum("push" in item["args"] for item in commands), int(valid))
        with tempfile.TemporaryDirectory() as directory:
            result, commands = self.writer(directory, malformed=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(any("push" in item["args"] for item in commands))


if __name__ == "__main__":
    unittest.main()
