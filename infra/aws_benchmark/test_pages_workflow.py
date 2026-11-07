"""Real local-Git main observations tied to the Pages workflow scripts; no network."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[2]


class PagesWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.load((ROOT / ".github/workflows/benchmark-pages.yml").read_text(), Loader=yaml.BaseLoader)
        self.render = self.workflow["jobs"]["render"]
        self.deploy = self.workflow["jobs"]["deploy"]
        self.gates = {
            "source": next(step for step in self.render["steps"] if step.get("id") == "source"),
            "package": next(step for step in self.render["steps"] if step.get("id") == "current"),
            "deploy": next(step for step in self.deploy["steps"] if step.get("id") == "current"),
        }
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.remote = self.root / "remote.git"
        self.commands = self.root / "commands.jsonl"
        self.output = self.root / "outputs"
        self.real_git = shutil.which("git")
        self.env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1",
            GIT_TERMINAL_PROMPT="0", GIT_AUTHOR_NAME="Offline test", GIT_AUTHOR_EMAIL="test@example.invalid",
            GIT_COMMITTER_NAME="Offline test", GIT_COMMITTER_EMAIL="test@example.invalid")
        self.git("init", "--bare")
        binary = self.root / "bin"
        binary.mkdir()
        shim = binary / "git"
        shim.write_text("#!" + shutil.which("python3") + "\n" +
            "import json, os, subprocess, sys\n" +
            "args = sys.argv[1:]\n" +
            "with open(os.environ['RECORDED_COMMANDS'], 'a') as output: output.write(json.dumps(args) + '\\n')\n" +
            "expected = ['-c', 'credential.helper=', 'ls-remote', '--exit-code', '--heads', " +
            "'https://github.com/wykurz/rcp.git', 'refs/heads/main']\n" +
            "if args != expected: sys.exit('unexpected Git operation; no network permitted')\n" +
            "if os.environ.get('REMOTE_FAILURE'): sys.exit(128)\n" +
            "args[-2] = os.environ['LOCAL_REMOTE']\n" +
            "sys.exit(subprocess.run([os.environ['REAL_GIT'], *args]).returncode)\n")
        shim.chmod(0o755)
        self.env.update(PATH=str(binary) + os.pathsep + self.env["PATH"], REAL_GIT=self.real_git,
            LOCAL_REMOTE=str(self.remote), RECORDED_COMMANDS=str(self.commands),
            RUNNER_TEMP=str(self.root), GITHUB_OUTPUT=str(self.output),
            GITHUB_STEP_SUMMARY=str(self.root / "summary"), PYTHONPATH=str(ROOT))

    def git(self, *args, source=None):
        result = subprocess.run([self.real_git, "--git-dir=" + str(self.remote), *args],
            input=source, text=True, capture_output=True, check=True, env=self.env, timeout=10)
        return result.stdout.strip()

    def seed(self, parent=None):
        tree = self.git("mktree", source="")
        args = ("-p", parent) if parent else ()
        commit = self.git("commit-tree", tree, *args, "-m", "Newer main" if parent else "Initial main")
        self.git("update-ref", "refs/heads/main", commit)
        return commit

    def gate(self, stage, revision, *, aws=True, **extra):
        environment = dict(self.env, GITHUB_REPOSITORY="wykurz/rcp", GITHUB_REPOSITORY_ID="655073755",
            GITHUB_REF="refs/heads/main", GITHUB_SHA=revision, GITHUB_WORKFLOW_SHA=revision,
            GITHUB_ACTOR_ID="483465", GITHUB_RUN_ATTEMPT="1", GITHUB_EVENT_NAME="push", PAGES_ENABLED="true",
            PUBLICATION_ENABLED="true" if aws else "false", EXPECTED_ARTIFACT="123" if aws else "",
            EXPECTED_READER=revision if aws else "", EXPECTED_INVENTORY="b" * 64 if aws else "",
            GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/" +
                ("aws-benchmark-publish.yml" if aws else "benchmark-pages.yml") + "@refs/heads/main")
        environment.update(extra)
        return subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", self.gates[stage]["run"]],
            cwd=self.root, text=True, capture_output=True, env=environment, timeout=10)

    def assert_gate(self, stage, revision, *, current=True, **extra):
        self.output.unlink(missing_ok=True)
        result = self.gate(stage, revision, **extra)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.output.read_text(),
                         "current=" + str(current).lower() + "\ncommit=" + revision + "\n")
        return result

    def calls(self):
        return [json.loads(line) for line in self.commands.read_text().splitlines()] if self.commands.exists() else []

    def test_source_checkout_and_all_three_guards_use_the_same_current_run_source(self):
        for job in (self.render, self.deploy):
            first = job["steps"][0]
            self.assertTrue(first["uses"].startswith("actions/checkout@"))
            self.assertEqual(first["with"], {"ref": "${{ github.sha }}", "persist-credentials": "false"})
        for gate in self.gates.values():
            self.assertEqual(gate["run"], "python3 -B -m infra.aws_benchmark.production_reports site-gate")
            self.assertNotIn("env", gate)
        self.assertEqual(self.render["steps"][1], self.gates["source"])
        self.assertEqual(self.deploy["steps"][1], self.gates["deploy"])
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        self.assertEqual(self.render["permissions"], {"contents": "read"})
        self.assertEqual(self.deploy["permissions"], {"contents": "read", "pages": "write", "id-token": "write"})
        self.assertNotIn("environment", self.render)
        self.assertEqual(self.deploy["environment"]["name"], "github-pages")

    def test_artifact_inputs_are_exact_and_stale_guards_control_both_upload_and_deployment(self):
        expected = {"EXPECTED_ARTIFACT": "aws_records_artifact", "EXPECTED_INVENTORY": "aws_inventory_sha256",
                    "EXPECTED_READER": "aws_reader_revision"}
        self.assertEqual(set(self.workflow["on"]["workflow_call"]["inputs"]), set(expected.values()))
        for name, field in expected.items():
            self.assertEqual(self.workflow["env"][name], "${{ inputs." + field + " }}")
            self.assertEqual(self.workflow["on"]["workflow_call"]["inputs"][field]["required"], "true")
        download = next(step for step in self.render["steps"] if step.get("uses", "").startswith("actions/download-artifact@"))
        self.assertEqual(download["with"], {"artifact-ids": "${{ inputs.aws_records_artifact }}",
            "path": "${{ runner.temp }}/public-records", "merge-multiple": "true", "digest-mismatch": "error"})
        package = next(step for step in self.render["steps"] if step.get("id") == "package")
        upload = next(step for step in self.render["steps"] if step.get("uses", "").startswith("actions/upload-artifact@"))
        self.assertLess(self.render["steps"].index(self.gates["package"]), self.render["steps"].index(package))
        self.assertLess(self.render["steps"].index(package), self.render["steps"].index(upload))
        for step in (package, upload, self.deploy["steps"][-1]):
            self.assertEqual(step["if"], "steps.current.outputs.current == 'true'")
        self.assertEqual(self.render["outputs"]["current"], "${{ steps.current.outputs.current }}")
        self.assertEqual(self.deploy["needs"], "render")
        self.assertEqual(self.deploy["if"], "needs.render.outputs.current == 'true'")
        self.assertEqual(upload["with"]["name"], "github-pages-${{ github.run_id }}-${{ github.run_attempt }}")
        self.assertEqual(self.render["outputs"]["artifact_name"], upload["with"]["name"])
        self.assertEqual(self.deploy["steps"][-1]["with"], {"artifact_name": "${{ needs.render.outputs.artifact_name }}"})
        self.assertEqual(self.workflow["concurrency"], {"group": "benchmark-pages-publish", "cancel-in-progress": "false"})

    def test_actual_site_scripts_read_only_the_exact_main_ref_for_both_site_modes(self):
        commit = self.seed()
        for aws in (False, True):
            for stage in self.gates:
                with self.subTest(aws=aws, stage=stage):
                    count = len(self.calls())
                    self.assert_gate(stage, commit, aws=aws)
                    self.assertEqual(len(self.calls()), count + 1)
        self.assertTrue(all(call == ["-c", "credential.helper=", "ls-remote", "--exit-code", "--heads",
            "https://github.com/wykurz/rcp.git", "refs/heads/main"] for call in self.calls()))

    def test_delayed_source_is_skipped_instead_of_retargeting_the_artifact_to_new_main(self):
        older = self.seed()
        newer = self.seed(older)
        result = self.assert_gate("source", older, current=False)
        self.assertIn("::warning::", result.stdout)
        self.assertIn("preserve the existing site", result.stdout)
        self.assertNotIn(newer, self.output.read_text())
        self.assert_gate("source", newer)

    def test_main_advance_during_render_or_deployment_gate_prevents_publication(self):
        older = self.seed()
        self.assert_gate("source", older)
        newer = self.seed(older)
        self.assert_gate("package", older, current=False)
        self.assert_gate("source", newer)
        self.assert_gate("package", newer)
        self.seed(newer)
        self.assert_gate("deploy", newer, current=False)

    def test_main_ref_suffixes_cannot_impersonate_or_mask_the_exact_branch(self):
        older = self.seed()
        newer = self.seed(older)
        self.git("update-ref", "refs/heads/shadow/refs/heads/main", older)
        self.git("update-ref", "-d", "refs/heads/main")
        for stage in self.gates:
            with self.subTest(stage=stage, exact=False):
                # ls-remote matches the suffix, but the exact-ref parser returns no main.
                # That observation is non-current, never permission to publish the suffix.
                self.assert_gate(stage, older, current=False)
        self.git("update-ref", "refs/heads/main", newer)
        for stage in self.gates:
            with self.subTest(stage=stage, exact=True):
                self.assert_gate(stage, newer)
                self.assert_gate(stage, older, current=False)

    def test_absent_or_unavailable_main_fails_without_emitting_deployment_permission(self):
        for extra in ({}, {"REMOTE_FAILURE": "yes"}):
            for stage in self.gates:
                with self.subTest(extra=extra, stage=stage):
                    result = self.gate(stage, "a" * 40, **extra)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse(self.output.exists())
        commit = self.seed()
        for stage in self.gates:
            result = self.gate(stage, commit, REMOTE_FAILURE="yes")
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(self.output.exists())

    def test_invalid_render_pins_and_reruns_fail_before_git_observation(self):
        commit = self.seed()
        changes = [
            dict(GITHUB_SHA="main"), dict(GITHUB_SHA="-a"),
            dict(GITHUB_SHA=commit + "; echo unsafe"), dict(GITHUB_WORKFLOW_SHA="c" * 40),
            dict(GITHUB_RUN_ATTEMPT="2"), dict(GITHUB_REF="refs/heads/foreign"),
        ]
        for stage in self.gates:
            for extra in changes:
                with self.subTest(stage=stage, extra=extra):
                    result = self.gate(stage, commit, **extra)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse(self.output.exists())
        self.assertEqual(self.calls(), [])

    def test_publication_proofs_cannot_fall_back_to_docs_on_missing_or_changed_bindings(self):
        commit = self.seed()
        changes = [
            dict(EXPECTED_ARTIFACT=""), dict(EXPECTED_ARTIFACT="123,456"), dict(EXPECTED_INVENTORY=""),
            dict(EXPECTED_READER=""), dict(EXPECTED_READER="c" * 40),
            dict(EXPECTED_ARTIFACT="", EXPECTED_INVENTORY="", EXPECTED_READER=""),
            dict(PUBLICATION_ENABLED="false"),
        ]
        for stage in self.gates:
            for extra in changes:
                with self.subTest(stage=stage, extra=extra):
                    result = self.gate(stage, commit, **extra)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse(self.output.exists())
        self.assertEqual(self.calls(), [])
        for stage in self.gates:
            result = self.gate(stage, commit, aws=False, PUBLICATION_ENABLED="true")
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(self.output.exists())
        self.assertEqual(self.calls(), [])
        self.assert_gate("source", commit, aws=False)


if __name__ == "__main__":
    unittest.main()
