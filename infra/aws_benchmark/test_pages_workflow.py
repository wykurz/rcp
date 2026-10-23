"""Offline Pages policy and bare-history pinning; no network or benchmark jobs."""

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
        self.job = self.workflow["jobs"]["publish"]
        self.step = next(step for step in self.job["steps"] if step.get("id") == "aws_history")
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
            "from pathlib import Path\n" +
            "args = sys.argv[1:]\n" +
            "with open(os.environ['RECORDED_COMMANDS'], 'a') as output: output.write(json.dumps(args) + '\\n')\n" +
            "if 'ls-remote' in args and os.environ.get('REMOTE_FAILURE'): sys.exit(128)\n" +
            "args = [os.environ['LOCAL_REMOTE'] if a == 'https://github.com/wykurz/rcp.git' else a for a in args]\n" +
            "sys.exit(subprocess.run([os.environ['REAL_GIT'], *args]).returncode)\n")
        shim.chmod(0o755)
        self.env.update(PATH=str(binary) + os.pathsep + self.env["PATH"], REAL_GIT=self.real_git,
            LOCAL_REMOTE=str(self.remote), RECORDED_COMMANDS=str(self.commands),
            RUNNER_TEMP=str(self.root), GITHUB_OUTPUT=str(self.output),
            GITHUB_STEP_SUMMARY=str(self.root / "summary"))

    def git(self, *args, source=None):
        result = subprocess.run([self.real_git, "--git-dir=" + str(self.remote), *args],
            input=source, text=True, capture_output=True, check=True, env=self.env)
        return result.stdout.strip()

    def seed(self):
        tree = self.git("mktree", source="")
        commit = self.git("commit-tree", tree, "-m", "Public history")
        self.git("update-ref", "refs/heads/aws-benchmark-history", commit)
        return commit

    def fetch(self, commit="", **extra):
        return subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", self.step["run"]],
            text=True, capture_output=True, env=dict(self.env, AWS_HISTORY_COMMIT=commit, **extra))

    def test_pages_has_only_manual_main_push_and_reusable_triggers(self):
        self.assertEqual(set(self.workflow["on"]), {"workflow_call", "workflow_dispatch", "push"})
        self.assertEqual(self.workflow["on"]["push"]["branches"], ["main"])
        field = self.workflow["on"]["workflow_call"]["inputs"]["aws_history_commit"]
        self.assertEqual((field["type"], field["required"]), ("string", "true"))
        for required in ("github.repository == 'wykurz/rcp'", "github.ref == 'refs/heads/main'",
                         "vars.RCP_BENCHMARK_PAGES == 'true'"):
            self.assertIn(required, self.job["if"])
        self.assertEqual(self.step["if"], "vars.RCP_AWS_PRODUCTION_PUBLISH_ENABLED == 'true'")
        checkouts = [step["with"] for step in self.job["steps"] if step.get("uses", "").startswith("actions/checkout@")]
        self.assertEqual(checkouts[0]["ref"], "${{ steps.source.outputs.commit }}")
        self.assertEqual(self.job["steps"][0]["id"], "source")
        self.assertTrue(all(step["persist-credentials"] == "false" for step in checkouts))
        self.assertEqual([step["ref"] for step in checkouts[1:]], ["benchmark-history"])
        self.assertEqual(self.job["permissions"], {"contents": "read", "pages": "write", "id-token": "write"})
        self.assertEqual(self.workflow["concurrency"]["cancel-in-progress"], "false")

    def resolve_main(self, **extra):
        step = next(step for step in self.job["steps"] if step.get("id") == "source")
        return subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", step["run"]],
            text=True, capture_output=True, env=dict(self.env, **extra))

    def main_guard(self, revision, **extra):
        step = next(step for step in self.job["steps"] if step.get("id") == "current_main")
        return subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", step["run"]],
            text=True, capture_output=True, env=dict(self.env, PAGES_SOURCE_COMMIT=revision, **extra))

    def test_delayed_publication_and_rerun_render_new_main_instead_of_dispatch_sha(self):
        older = self.seed()
        tree = self.git("mktree", source="")
        newer = self.git("commit-tree", tree, "-p", older, "-m", "Newer main")
        self.git("update-ref", "refs/heads/main", newer)
        result = self.resolve_main(GITHUB_SHA=older)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.output.read_text(), "commit=" + newer + "\n")
        self.output.unlink()
        result = self.main_guard(newer, GITHUB_SHA=older)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.output.read_text(), "current=true\n")
        guard = next(step for step in self.job["steps"] if step.get("id") == "current_main")
        self.assertEqual(guard["env"]["PAGES_SOURCE_COMMIT"], "${{ steps.source.outputs.commit }}")

    def test_main_advance_during_render_fails_visibly_without_offering_deployment(self):
        older = self.seed()
        self.git("update-ref", "refs/heads/main", older)
        self.assertEqual(self.resolve_main().returncode, 0)
        self.output.unlink()
        tree = self.git("mktree", source="")
        newer = self.git("commit-tree", tree, "-p", older, "-m", "Concurrent merge")
        self.git("update-ref", "refs/heads/main", newer)
        stale = self.main_guard(older)
        self.assertNotEqual(stale.returncode, 0)
        self.assertEqual(self.output.read_text(), "current=false\n")
        summary = (self.root / "summary").read_text()
        self.assertIn("Pages deployment blocked", summary)
        self.assertIn("Re-run", summary)
        self.assertIn(older, summary)
        self.assertIn(newer, summary)
        self.assertIn("::error::", stale.stdout)
        steps = self.job["steps"]
        guard_index = next(i for i, step in enumerate(steps) if step.get("id") == "current_main")
        deployment = steps[guard_index + 1]
        self.assertTrue(deployment["uses"].startswith("actions/deploy-pages@"))
        self.assertEqual(deployment["if"], "steps.current_main.outputs.current == 'true'")
        self.assertEqual(self.workflow["concurrency"],
                         {"group": "benchmark-pages-publish", "cancel-in-progress": "false"})

    def test_main_ref_suffixes_do_not_mask_or_impersonate_the_exact_branch(self):
        commit = self.seed()
        self.git("update-ref", "refs/heads/shadow/refs/heads/main", commit)
        for read in (self.resolve_main, lambda: self.main_guard(commit)):
            result = read()
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(self.output.exists())
        self.git("update-ref", "refs/heads/main", commit)
        result = self.resolve_main()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.output.read_text(), "commit=" + commit + "\n")
        self.output.unlink()
        result = self.main_guard(commit)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.output.read_text(), "current=true\n")

    def test_unavailable_main_cannot_offer_deployment(self):
        for change in ({}, {"REMOTE_FAILURE": "yes"}):
            for read in (self.resolve_main, lambda **extra: self.main_guard("a" * 40, **extra)):
                with self.subTest(change=change, read=read):
                    result = read(**change)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse(self.output.exists())

    def test_main_guard_rejects_an_invalid_render_pin(self):
        self.git("update-ref", "refs/heads/main", self.seed())
        result = self.main_guard("main")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())

    def test_exact_history_commit_is_fetched_without_a_data_checkout(self):
        commit = self.seed()
        result = self.fetch(commit)
        self.assertEqual(result.returncode, 0, result.stderr)
        history = self.root / "aws-pages-history.git"
        ref = subprocess.check_output([self.real_git, "--git-dir=" + str(history), "rev-parse",
            "refs/heads/aws-benchmark-history"], text=True).strip()
        self.assertEqual(ref, commit)
        self.assertEqual(self.output.read_text(), "path=" + str(history) + "\n")
        calls = [json.loads(line) for line in self.commands.read_text().splitlines()]
        self.assertFalse(any("checkout" in call or "ls-remote" in call for call in calls))
        self.assertEqual(next(call[-1] for call in calls if "fetch" in call), commit)

    def test_manual_or_push_resolves_fixed_branch_once_then_pins_fetch(self):
        commit = self.seed()
        result = self.fetch()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(line) for line in self.commands.read_text().splitlines()]
        self.assertEqual(sum("ls-remote" in call for call in calls), 1)
        self.assertEqual(next(call[-1] for call in calls if "ls-remote" in call), "refs/heads/aws-benchmark-history")
        self.assertEqual(next(call[-1] for call in calls if "fetch" in call), commit)

    def test_history_ref_suffixes_do_not_mask_or_impersonate_the_exact_branch(self):
        commit = self.seed()
        self.git("update-ref", "refs/heads/shadow/refs/heads/aws-benchmark-history", commit)
        result = self.fetch()
        self.assertEqual(result.returncode, 0, result.stderr)
        history = self.root / "aws-pages-history.git"
        ref = subprocess.check_output([self.real_git, "--git-dir=" + str(history), "rev-parse",
            "refs/heads/aws-benchmark-history"], text=True).strip()
        self.assertEqual(ref, commit)
        self.output.unlink()
        self.git("update-ref", "-d", "refs/heads/aws-benchmark-history")
        before = len(self.commands.read_text().splitlines())
        result = self.fetch()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(line) for line in self.commands.read_text().splitlines()[before:]]
        self.assertFalse(any("fetch" in call for call in calls))

    def test_absent_history_is_empty_but_transport_failure_is_not_ignored(self):
        result = self.fetch()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(self.output.exists())
        self.output.unlink()
        result = self.fetch(REMOTE_FAILURE="yes")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())

    def test_invalid_or_unavailable_explicit_commit_fails_without_fallback(self):
        self.seed()
        for commit in ("main", "-a", "a" * 40 + "; echo unsafe", "f" * 40):
            with self.subTest(commit=commit):
                result = self.fetch(commit)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.output.exists())
        calls = [json.loads(line) for line in self.commands.read_text().splitlines()]
        self.assertFalse(any("ls-remote" in call for call in calls))


if __name__ == "__main__":
    unittest.main()
