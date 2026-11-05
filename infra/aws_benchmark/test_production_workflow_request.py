"""Actual credential-free request selection for manual and disabled recurring runs."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from .production_workflow import source, WORKFLOW

ROOT = Path(__file__).resolve().parents[2]
SHA = "a" * 40


def environment(**updates):
    return dict(GITHUB_REPOSITORY="wykurz/rcp", GITHUB_REPOSITORY_ID="655073755",
        GITHUB_REF="refs/heads/main", GITHUB_WORKFLOW_REF=WORKFLOW, GITHUB_ACTOR_ID="483465",
        GITHUB_RUN_ATTEMPT="1", GITHUB_SHA=SHA, GITHUB_WORKFLOW_SHA=SHA, GITHUB_EVENT_NAME="workflow_dispatch",
        RCP_PRODUCTION_ENABLED="true", RCP_MODE="benchmark", RCP_REQUESTED_REVISION="",
        RCP_SCHEDULE_ENABLED="false") | updates


class RequestTests(unittest.TestCase):
    def test_manual_default_and_pinned_source(self):
        self.assertEqual(source(environment()), SHA)
        self.assertEqual(source(environment(RCP_REQUESTED_REVISION="b" * 40)), "b" * 40)

    def test_schedule_requires_literal_separate_gate_and_no_revision_input(self):
        for flag in ("", "false", "True", "1"):
            with self.subTest(flag=flag), self.assertRaises(ValueError):
                source(environment(GITHUB_EVENT_NAME="schedule", RCP_SCHEDULE_ENABLED=flag))
        self.assertEqual(source(environment(GITHUB_EVENT_NAME="schedule", RCP_SCHEDULE_ENABLED="true")), SHA)
        with self.assertRaises(ValueError):
            source(environment(GITHUB_EVENT_NAME="schedule", RCP_SCHEDULE_ENABLED="true",
                               RCP_REQUESTED_REVISION="b" * 40))

    def test_other_events_reruns_actors_refs_and_missing_flags_are_rejected(self):
        cases = [dict(GITHUB_EVENT_NAME=event) for event in ("push", "pull_request", "pull_request_target",
                  "issue_comment", "workflow_run", "repository_dispatch")]
        cases += [dict(GITHUB_RUN_ATTEMPT="2"), dict(GITHUB_ACTOR_ID="1"),
            dict(GITHUB_REF="refs/heads/debug"), dict(GITHUB_REF="refs/tags/v1"),
            dict(GITHUB_REPOSITORY_ID="1"), dict(GITHUB_REPOSITORY="other/rcp"),
            dict(GITHUB_WORKFLOW_REF=WORKFLOW.replace("main", "debug")),
            dict(RCP_PRODUCTION_ENABLED=""), dict(RCP_PRODUCTION_ENABLED="True"),
            dict(RCP_MODE="identity-test"), dict(GITHUB_WORKFLOW_SHA="b" * 40), dict(GITHUB_SHA="main"), dict(RCP_REQUESTED_REVISION="main")]
        for changes in cases:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                source(environment(**changes))

    def test_actual_cli_emits_exact_pinned_sha_and_never_outputs_invalid_input(self):
        for event, flag, valid in (("workflow_dispatch", "false", True), ("schedule", "true", True),
                                   ("schedule", "false", False), ("pull_request", "true", False)):
            with self.subTest(event=event, flag=flag), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "output"
                env = dict(os.environ, **environment(GITHUB_EVENT_NAME=event, RCP_SCHEDULE_ENABLED=flag),
                           GITHUB_OUTPUT=str(output))
                result = subprocess.run([sys.executable, "-B", "-m", "infra.aws_benchmark.production_workflow"],
                    cwd=ROOT, env=env, capture_output=True, timeout=5)
                self.assertEqual(result.returncode == 0, valid, result.stderr)
                self.assertEqual(result.stdout, b"")
                self.assertEqual(output.read_text() if output.exists() else "", "revision=" + SHA + "\n" if valid else "")


if __name__ == "__main__":
    unittest.main()
