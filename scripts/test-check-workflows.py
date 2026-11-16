#!/usr/bin/env python3
"""Exercise the workflow lint wrapper with the real actionlint binary."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


CHECK = Path(__file__).resolve().with_name("check-workflows.sh")
WORKFLOW = """\
on: push
cache-mode: none
jobs:
  check:
    runs-on: ubuntu-latest
    cache-mode: none
    steps:
      - run: echo hello
"""


class WorkflowLintTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        subprocess.run(["git", "init", "--quiet", str(self.root)], check=True)
        self.workflows = self.root / ".github" / "workflows"
        self.workflows.mkdir(parents=True)
        self.workflow = self.workflows / "check.yml"
        self.workflow.write_text(WORKFLOW)

    def lint(self, *files, env=None):
        return subprocess.run([str(CHECK), *map(str, files)], cwd=self.root,
                              env=env, text=True, capture_output=True)

    def assert_passes(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def assert_rejects(self, result, diagnostic):
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(diagnostic, result.stdout + result.stderr)

    def test_workflow_and_job_cache_controls_are_accepted(self):
        self.assert_passes(self.lint(self.workflow))

    def test_runner_context_is_still_rejected_in_job_env(self):
        self.workflow.write_text(WORKFLOW.replace(
            "    steps:", "    env:\n      CACHE: ${{ runner.temp }}/cache\n    steps:"))
        self.assert_rejects(self.lint(self.workflow), 'context "runner" is not allowed here')

    def test_other_unknown_keys_are_still_rejected(self):
        self.workflow.write_text(WORKFLOW.replace(
            "    steps:", "    timout-minutes: 5\n    steps:"))
        self.assert_rejects(self.lint(self.workflow), 'unexpected key "timout-minutes"')

    def test_step_cache_controls_are_still_rejected(self):
        self.workflow.write_text(WORKFLOW + "        cache-mode: none\n")
        self.assert_rejects(self.lint(self.workflow),
                            'unexpected key "cache-mode" for step to run shell command')

    def test_default_invocation_checks_both_yaml_extensions(self):
        for extension in ("yml", "yaml"):
            with self.subTest(extension=extension):
                other = self.workflows / f"manual.{extension}"
                other.write_text(WORKFLOW.replace("on: push", "on: workflow_dispatch")
                                 .replace("    steps:", "    timout-minutes: 5\n    steps:"))
                self.assert_rejects(self.lint(), other.name)
                other.unlink()
        self.assert_passes(self.lint())

    def test_optional_analyzers_are_disabled_even_when_installed(self):
        binaries = self.root / "bin"
        binaries.mkdir()
        marker = self.root / "analyzer-was-run"
        for name in ("shellcheck", "pyflakes"):
            tool = binaries / name
            tool.write_text('#!/bin/sh\ntouch "$ANALYZER_MARKER"\nexit 1\n')
            tool.chmod(0o755)
        # keep both a shell step and a Python step to exercise both optional integrations
        self.workflow.write_text(WORKFLOW + "      - shell: python\n        run: import unused_module\n")
        env = dict(os.environ, PATH=f"{binaries}{os.pathsep}{os.environ['PATH']}",
                   ANALYZER_MARKER=str(marker))
        self.assert_passes(self.lint(self.workflow, env=env))
        self.assertFalse(marker.exists(), "optional analyzer was invoked")


if __name__ == "__main__":
    unittest.main()
