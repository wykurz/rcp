"""Independent API-shaped source binding for explicitly approved canary recovery."""

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from . import canary_recovery as recovery
from .history_contract import CANARY_WORKFLOW, OWNER_ID, REPOSITORY, REPOSITORY_ID
from .test_publication_identity import SHA, environment


def captured_metadata():
    repository = dict(id=int(REPOSITORY_ID), full_name=REPOSITORY, private=False)
    run = dict(id=1201, path=".github/workflows/aws-benchmark-history-canary.yml", event="workflow_dispatch",
        head_branch="main", head_sha=SHA, run_attempt=1, actor=dict(id=OWNER_ID, login="wykurz"), status="completed",
        conclusion="failure", repository=repository, head_repository=repository)
    artifact = dict(id=4501, name="aws-history-canary-1201-1", expired=False, size_in_bytes=1000,
        digest="sha256:" + "b" * 64, workflow_run=dict(id=1201, repository_id=int(REPOSITORY_ID),
        head_repository_id=int(REPOSITORY_ID), head_branch="main", head_sha=SHA))
    return run, artifact


class CanaryRecoveryTests(unittest.TestCase):
    def test_failed_source_shapes_bind_exact_run_artifact_revision_and_workflow(self):
        for conclusion in ("failure", "cancelled", "timed_out"):
            for suffix in ("", "@main"):
                run, artifact = captured_metadata()
                run.update(conclusion=conclusion, path=run["path"] + suffix)
                receipt = recovery.validate(run, artifact, 1201, 4501, SHA)
                self.assertEqual(receipt["run_id"], 1201)
                self.assertEqual(receipt["artifact_id"], 4501)
        mutations = (("run", "id", 1202), ("run", "path", ".github/workflows/aws-benchmark.yml"),
            ("run", "path", ".github/workflows/aws-benchmark-history-canary.yml@debug"),
            ("run", "event", "pull_request"), ("run", "head_branch", "debug"), ("run", "head_sha", "c" * 40),
            ("run", "run_attempt", 2), ("run", "actor", {"id": 1}), ("run", "status", "in_progress"),
            ("run", "conclusion", "success"), ("run", "conclusion", "neutral"),
            ("run", "repository", {"id": 1, "full_name": REPOSITORY}),
            ("run", "head_repository", {"id": int(REPOSITORY_ID), "full_name": "other/rcp"}),
            ("artifact", "id", 4502), ("artifact", "expired", True), ("artifact", "name", "other"),
            ("artifact", "size_in_bytes", 0), ("artifact", "size_in_bytes", recovery.LIMIT + 1))
        for kind, key, value in mutations:
            with self.subTest(kind=kind, key=key, value=value):
                run, artifact = captured_metadata()
                (run if kind == "run" else artifact)[key] = value
                with self.assertRaises(ValueError):
                    recovery.validate(run, artifact, 1201, 4501, SHA)
        for key in ("id", "repository_id", "head_repository_id", "head_branch", "head_sha"):
            run, artifact = captured_metadata()
            artifact["workflow_run"][key] = "wrong"
            with self.subTest(binding=key), self.assertRaises(ValueError):
                recovery.validate(run, artifact, 1201, 4501, SHA)

    def test_main_only_reads_fixed_metadata_paths_and_persists_no_token(self):
        for invalid in (False, True):
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as directory:
                run, artifact = captured_metadata()
                if invalid:
                    artifact["workflow_run"]["head_sha"] = "c" * 40
                env = dict(environment(CANARY_WORKFLOW), RUNNER_TEMP=directory, CANARY_ENABLED="true",
                    TRUST_READY="false", PUBLISH_ENABLED="false", CLIENT_ID="Iv23.test-client", INSTALLATION_ID="5678",
                    CANARY_MODE="resume", SOURCE_RUN_ID="1201", SOURCE_ARTIFACT_ID="4501", SOURCE_SHA256="d" * 64,
                    GITHUB_RUN_ID="1202", GH_TOKEN="never-log-this")
                with patch.dict(os.environ, env), patch.object(recovery, "fetch", side_effect=(run, artifact)) as fetch:
                    if invalid:
                        with self.assertRaises(ValueError):
                            recovery.main()
                    else:
                        recovery.main()
                    self.assertEqual([call.args[0] for call in fetch.call_args_list],
                        ["/actions/runs/1201", "/actions/artifacts/4501"])
                output = Path(directory) / "canary-source-metadata.json"
                self.assertEqual(output.exists(), not invalid)
                if output.exists():
                    self.assertNotIn("never-log-this", output.read_text())
                    self.assertEqual(json.loads(output.read_text())["receipt_sha256"], "d" * 64)


if __name__ == "__main__":
    unittest.main()
