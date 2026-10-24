"""Runtime identity, approval-time flags and exact current-main checks without network."""

import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

from . import publication_identity as identity
from .history_contract import CANARY_WORKFLOW, PUBLICATION_WORKFLOW, REPOSITORY, OWNER_ID

ROOT = Path(__file__).resolve().parents[2]
SHA = "a" * 40


def environment(workflow=PUBLICATION_WORKFLOW):
    return dict(GITHUB_REPOSITORY=REPOSITORY, GITHUB_REF="refs/heads/main", GITHUB_EVENT_NAME="workflow_dispatch",
        GITHUB_ACTOR_ID=str(OWNER_ID), GITHUB_RUN_ATTEMPT="1", GITHUB_SHA=SHA, GITHUB_WORKFLOW_SHA=SHA,
        GITHUB_WORKFLOW_REF=REPOSITORY + "/.github/workflows/" + workflow + "@refs/heads/main",
        EXPECTED_READER=SHA, EXPECTED_INVENTORY="b" * 64, EXPECTED_ARTIFACT="123",
        PUBLICATION_ENABLED="true", TRUST_READY="true")


class PublicationIdentityTests(unittest.TestCase):
    def test_publication_and_canary_are_distinct_explicit_workflow_identities(self):
        self.assertEqual(identity.check_identity(environment(), artifact=True, flags=True), SHA)
        self.assertEqual(identity.check_identity(environment(CANARY_WORKFLOW), workflow=CANARY_WORKFLOW), SHA)
        with self.assertRaises(ValueError):
            identity.check_identity(environment(CANARY_WORKFLOW))

    def test_all_runtime_identity_fields_and_postapproval_flags_fail_closed(self):
        for key in environment():
            with self.subTest(key=key), self.assertRaises(ValueError):
                identity.check_identity({**environment(), key: ""}, artifact=True, flags=True)
        for key in ("PUBLICATION_ENABLED", "TRUST_READY"):
            for value in ("false", "True", "true\n"):
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    identity.check_identity({**environment(), key: value}, flags=True)

    def test_main_requires_one_exact_successful_ref_and_no_token_for_read(self):
        good = SHA + "\trefs/heads/main\n"
        for status, raw in ((0, good), (0, "b" * 40 + "\trefs/heads/main\n"), (0, good + good),
                            (0, good.replace("heads/main", "heads/shadow/main")), (0, ""), (2, ""), (128, good)):
            with self.subTest(status=status, raw=raw), patch.dict(os.environ, GH_TOKEN="never-pass", GITHUB_TOKEN="never-pass"), \
                    patch.object(identity.subprocess, "run", return_value=Mock(returncode=status, stdout=raw)) as run:
                if (status, raw) == (0, good):
                    identity.current_main(SHA)
                else:
                    with self.assertRaises(ValueError):
                        identity.current_main(SHA)
                args, kwargs = run.call_args
                self.assertEqual(args[0], ["git", "-c", "credential.helper=", "ls-remote", "--exit-code", "--heads",
                                          identity.REMOTE, "refs/heads/main"])
                self.assertNotIn("GH_TOKEN", kwargs["env"])
                self.assertNotIn("GITHUB_TOKEN", kwargs["env"])
                self.assertLessEqual(kwargs["timeout"], 30)
        with patch.object(identity.subprocess, "run", side_effect=AssertionError("invalid SHA must not access git")):
            with self.assertRaises(ValueError):
                identity.current_main("main")

    def test_real_cli_identity_and_installation_checks_survive_optimized_python(self):
        for optimize in ("", "1", "2"):
            for command, changes, valid in (("identity", {}, True), ("identity", {"GITHUB_RUN_ATTEMPT": "2"}, False),
                    ("installation", {"EXPECTED_INSTALLATION": "5678", "ACTUAL_INSTALLATION": "5678"}, True),
                    ("installation", {"EXPECTED_INSTALLATION": "5678", "ACTUAL_INSTALLATION": "9999"}, False),
                    ("installation", {"EXPECTED_INSTALLATION": "5678", "ACTUAL_INSTALLATION": ""}, False)):
                with self.subTest(optimize=optimize, command=command, changes=changes):
                    env = {**os.environ, **environment(), **changes, "PYTHONOPTIMIZE": optimize}
                    result = subprocess.run([sys.executable, "-m", "infra.aws_benchmark.publication_identity", command],
                        cwd=ROOT, env=env, capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode == 0, valid, result.stderr)


if __name__ == "__main__":
    unittest.main()
