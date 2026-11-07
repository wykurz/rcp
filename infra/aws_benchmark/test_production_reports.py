"""Offline keyless snapshot, artifact, CLI and Pages identity regressions."""

import copy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from . import production_reports as reports, production_publication as publication
from . import test_production_publication as fixtures

ROOT = Path(__file__).resolve().parents[2]
REVISION = "a" * 40
RUN_KEY = "repo-655073755-run-123"
NOW = datetime(2026, 11, 7, 12, tzinfo=timezone.utc)


def identity(**updates):
    value = dict(GITHUB_SHA=REVISION, GITHUB_WORKFLOW_SHA=REVISION,
        GITHUB_REPOSITORY="wykurz/rcp", GITHUB_REPOSITORY_ID="655073755",
        GITHUB_REF="refs/heads/main", GITHUB_ACTOR_ID="483465", GITHUB_RUN_ATTEMPT="1",
        GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/aws-benchmark-publish.yml@refs/heads/main",
        GITHUB_EVENT_NAME="workflow_dispatch", PUBLICATION_ENABLED="true", PAGES_ENABLED="true")
    return dict(value, **updates)


def artifact_identity(**updates):
    return identity(EXPECTED_READER=REVISION, EXPECTED_INVENTORY="b" * 64,
        EXPECTED_ARTIFACT="1234", **updates)


def docs_identity(**updates):
    return identity(PUBLICATION_ENABLED="false",
        GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/benchmark-pages.yml@refs/heads/main", **updates)


class ReportsTests(unittest.TestCase):
    def setUp(self):
        fixtures.PublicationTests.setUp(self)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.public = self.reader.public_record(self.key)
        self.public["run_key"] = RUN_KEY
        self.sequence = 0

    def fake_reader(self, outcomes=None, active=None):
        values = outcomes if outcomes is not None else {
            RUN_KEY: dict(state="ready", retired=True, record=self.public)}
        reader = Mock(spec=["active_observation", "discover", "run_key", "observe_public"])
        reader.active_observation.return_value = active or dict(state="idle", retired=False, reason="no active owner observed")
        keys = {"production/runs/" + key + "/collection/manifest.json": key for key in values}
        reader.discover.return_value = list(keys)
        reader.run_key.side_effect = keys.__getitem__
        def observe(key):
            result = values[keys[key]]
            if isinstance(result, Exception):
                raise result
            return copy.deepcopy(result)
        reader.observe_public.side_effect = observe
        return reader

    def batch(self, reader=None, **kwargs):
        self.sequence += 1
        output = self.root / ("snapshot-" + str(self.sequence))
        proof = reports.snapshot(reader or self.fake_reader(), output, REVISION, now=NOW, **kwargs)
        return output, proof

    def cli(self, *args, environment=None, optimized=False):
        options = ["-O"] if optimized else []
        clean = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "PYTHONDONTWRITEBYTECODE": "1"}
        clean.update(environment or {})
        return subprocess.run([sys.executable, "-B", *options, "-m", "infra.aws_benchmark.production_reports", *args],
            cwd=ROOT, env=clean, capture_output=True, text=True, timeout=15)

    def change_index(self, output, mutate):
        path = output / "index.json"
        value = json.loads(path.read_bytes())
        mutate(value)
        path.write_bytes(reports.collection.encoded(value))
        return reports.inventory(output, limit=reports.MAX_HISTORY)

    def test_snapshot_inventory_batch_and_actual_inspect_cli_outputs(self):
        output, proof = self.batch()
        found, index = reports.public_batch(output, REVISION, proof)
        self.assertEqual(proof, reports.inventory(output))
        self.assertEqual(json.loads(found[RUN_KEY]), self.public)
        self.assertEqual(index["retention_days"], 90)
        self.assertEqual(index["observed_at"], NOW.isoformat())
        self.assertTrue(reports.deployable(index))
        github_output, summary = self.root / "outputs", self.root / "summary"
        result = self.cli("inspect", "--input", str(output), "--reader-revision", REVISION,
            "--expected-sha256", proof, "--require-complete", environment={
                "GITHUB_OUTPUT": str(github_output), "GITHUB_STEP_SUMMARY": str(summary)})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(github_output.read_text(), "inventory_sha256=" + proof + "\nreader_revision=" +
            REVISION + "\nrecord_count=1\ndeployable=true\n")
        self.assertIn("1 ready", summary.read_text())
        self.assertIn("Not permanent history", summary.read_text())

    def test_snapshot_uses_actual_authenticated_reader(self):
        self.db.get_item.side_effect = lambda **kw: {} if kw["Key"]["pk"]["S"] == "active" else {"Item": copy.deepcopy(self.item)}
        with patch.object(reports, "REPOSITORY_ID", "123"), \
                patch.object(reports, "RUN", re.compile(r"repo-123-run-[1-9][0-9]{0,19}")):
            output, proof = self.batch(self.reader)
            found, index = reports.public_batch(output, REVISION, proof)
        self.assertEqual(list(found), ["repo-123-run-123"])
        self.assertTrue(reports.deployable(index))
        self.assertEqual(json.loads(found["repo-123-run-123"]), self.reader.public_record(self.key))

    def test_download_main_uses_one_bounded_read_only_session_and_emits_inventory(self):
        output, github_output = self.root / "download-cli", self.root / "download-output"
        session, config = Mock(), Mock()
        arguments = ["reports", "download", "--reader-revision", REVISION, "--output", str(output),
            "--region", "us-east-1", "--role", "arn:aws:iam::381183037896:role/reader",
            "--bucket", "retained-evidence", "--table", "run-ledger"]
        with patch.dict(os.environ, dict(identity(), GITHUB_OUTPUT=str(github_output)), clear=True), \
                patch("sys.argv", arguments), \
                patch("infra.aws_benchmark.controller.oidc_session", return_value=(session, config)) as token, \
                patch.object(reports.publication, "Reader", return_value=self.fake_reader()) as reader:
            reports.main()
        token.assert_called_once()
        self.assertEqual(token.call_args.args, ("us-east-1", "arn:aws:iam::381183037896:role/reader"))
        self.assertEqual(token.call_args.kwargs["duration_seconds"], 900)
        self.assertEqual(token.call_args.kwargs["session_prefix"], "rcp-publication-")
        self.assertEqual([call.args for call in session.client.call_args_list], [("s3",), ("dynamodb",)])
        self.assertEqual(reader.call_args.kwargs, dict(bucket="retained-evidence", table="run-ledger", repository_id="655073755"))
        proof = reports.inventory(output)
        self.assertEqual(github_output.read_text(), "inventory_sha256=" + proof + "\nreader_revision=" +
            REVISION + "\nrecord_count=1\ndeployable=true\n")

    def test_optimized_download_rejects_missing_identity_before_any_session(self):
        result = self.cli("download", "--reader-revision", REVISION, "--output", str(self.root / "never-created"),
            "--region", "us-east-1", "--role", "arn:aws:iam::381183037896:role/reader",
            "--bucket", "retained-evidence", "--table", "run-ledger", optimized=True, environment={"GITHUB_SHA": REVISION})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("workflow identity differs", result.stdout)
        self.assertFalse((self.root / "never-created").exists())

    def test_empty_complete_batch_is_deployable_and_emits_zero(self):
        output, proof = self.batch(self.fake_reader({}))
        found, index = reports.public_batch(output, REVISION, proof)
        self.assertEqual(found, {})
        self.assertTrue(reports.deployable(index))
        target = self.root / "empty-output"
        result = self.cli("inspect", "--input", str(output), "--reader-revision", REVISION,
            "--expected-sha256", proof, environment={"GITHUB_OUTPUT": str(target)})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("record_count=0\ndeployable=true\n", target.read_text())

    def test_pending_active_and_run_are_explicit_without_blocking_completed_view(self):
        pending = dict(state="pending", retired=False, phase="running", reason="ownership retained")
        output, proof = self.batch(self.fake_reader({RUN_KEY: pending}, active=dict(pending, run_key=RUN_KEY)))
        found, index = reports.public_batch(output, REVISION, proof)
        self.assertEqual(found, {})
        self.assertEqual(index["observations"], [dict(pending, run_key=RUN_KEY)])
        self.assertTrue(reports.deployable(index))

    def test_held_report_preserves_retirement_and_blocks_actual_complete_cli(self):
        held = dict(state="held", retired=True, reason="missing retained evidence")
        output, proof = self.batch(self.fake_reader({RUN_KEY: held}))
        found, index = reports.public_batch(output, REVISION, proof)
        self.assertEqual(found, {})
        self.assertEqual(index["observations"], [dict(held, run_key=RUN_KEY)])
        self.assertFalse(reports.deployable(index))
        target = self.root / "held-output"
        result = self.cli("inspect", "--input", str(output), "--reader-revision", REVISION,
            "--expected-sha256", proof, "--require-complete", optimized=True,
            environment={"GITHUB_OUTPUT": str(target)})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("preserve the previous site", result.stdout)
        self.assertIn("deployable=false\n", target.read_text())

    def test_active_and_individual_errors_are_safe_holds(self):
        reader = self.fake_reader({RUN_KEY: RuntimeError("opaque-provider-sensitive-body")})
        reader.active_observation.side_effect = RuntimeError("another-sensitive-body")
        output, proof = self.batch(reader)
        _, index = reports.public_batch(output, REVISION, proof)
        self.assertFalse(reports.deployable(index))
        self.assertEqual(index["active"]["reason"], "RuntimeError")
        self.assertEqual(index["observations"][0]["reason"], "RuntimeError")
        self.assertNotIn("sensitive-body", (output / "index.json").read_text())

    def test_discovery_failure_does_not_publish_an_empty_complete_index(self):
        reader = self.fake_reader()
        reader.discover.side_effect = RuntimeError("incomplete traversal")
        output = self.root / "failed-discovery"
        with self.assertRaises(RuntimeError):
            reports.snapshot(reader, output, REVISION)
        self.assertFalse((output / "index.json").exists())
        reader.observe_public.assert_not_called()

    def test_duplicate_and_overbound_discovery_fail_before_payload_reads(self):
        for keys in (["same", "same"], [str(i) for i in range(publication.MAX_RUNS + 1)]):
            with self.subTest(keys=len(keys)):
                reader = self.fake_reader()
                reader.discover.return_value = keys
                with self.assertRaises(ValueError):
                    self.batch(reader)
                reader.observe_public.assert_not_called()

    def test_time_source_and_public_bounds_defer_without_partial_deployment(self):
        second = copy.deepcopy(self.public)
        second["run_key"] = "repo-655073755-run-456"
        second["identity"].update(run="456-1", run_url="https://github.com/wykurz/rcp/actions/runs/456/attempts/1")
        for mode in ("time", "source", "public"):
            with self.subTest(mode=mode):
                reader = self.fake_reader({RUN_KEY: dict(state="ready", retired=True, record=self.public),
                    second["run_key"]: dict(state="ready", retired=True, record=second)})
                bound = reports.MAX_SOURCE_PER_RECORD if mode == "source" else reports.MAX_TOTAL
                public_limit = reports.public.MAX_PUBLIC_RECORD + reports.MAX_INDEX if mode == "public" else reports.MAX_HISTORY
                clock = Mock(side_effect=[0, 0, reports.MAX_SECONDS] if mode == "time" else [0, 0, 0])
                with patch.object(reports, "MAX_TOTAL", bound), patch.object(reports, "MAX_HISTORY", public_limit):
                    output, proof = self.batch(reader, clock=clock)
                    found, index = reports.public_batch(output, REVISION, proof)
                self.assertEqual(list(found), [RUN_KEY])
                self.assertEqual(index["observations"][1]["state"], "deferred")
                self.assertFalse(index["processing_complete"])
                self.assertFalse(reports.deployable(index))
                self.assertEqual(reader.observe_public.call_count, 1)

    def test_snapshot_rejects_invalid_record_identity_without_writing_foreign_path(self):
        value = copy.deepcopy(self.public)
        value["run_key"] = "repo-655073755-run-456"
        reader = self.fake_reader({RUN_KEY: dict(state="ready", retired=True, record=value)})
        output, proof = self.batch(reader)
        found, index = reports.public_batch(output, REVISION, proof)
        self.assertEqual(found, {})
        self.assertEqual(index["observations"][0]["state"], "held")
        self.assertEqual({p.name for p in output.iterdir()}, {"index.json"})

    def test_inventory_tampering_is_rejected_before_semantic_loading(self):
        output, proof = self.batch()
        (output / (RUN_KEY + ".json")).write_bytes(b"not JSON")
        with patch.object(reports.public, "validate_public", side_effect=AssertionError("must check inventory first")):
            with self.assertRaisesRegex(ValueError, "inventory"):
                reports.public_batch(output, REVISION, proof)

    def test_recomputed_inventory_cannot_hide_index_and_reference_tampering(self):
        mutations = {
            "schema": lambda value: value.update(schema="unknown"),
            "repository": lambda value: value.update(repository_id="123"),
            "revision": lambda value: value.update(reader_revision="b" * 40),
            "retention": lambda value: value.update(retention_days=True),
            "discovery": lambda value: value.update(discovery_complete=False),
            "partial": lambda value: value.update(processing_complete=False),
            "bytes": lambda value: value["records"][0].update(bytes=True),
            "digest": lambda value: value["records"][0].update(sha256="0" * 64),
            "traversal": lambda value: value["records"][0].update(run_key="../outside"),
            "duplicate": lambda value: value["records"].append(copy.deepcopy(value["records"][0])),
            "retired": lambda value: value["observations"][0].update(retired=False),
            "missing_ready": lambda value: value.update(observations=[]),
            "active": lambda value: value["active"].update(retired=True),
            "unknown": lambda value: value.update(extra="unapproved"),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                output, _ = self.batch()
                proof = self.change_index(output, mutate)
                with self.assertRaises(ValueError):
                    reports.public_batch(output, REVISION, proof)

    def test_recomputed_ref_and_inventory_cannot_hide_public_schema_or_identity(self):
        for field in ("schema", "identity", "extra"):
            with self.subTest(field=field):
                output, _ = self.batch()
                path = output / (RUN_KEY + ".json")
                value = json.loads(path.read_bytes())
                if field == "schema":
                    value["schema"] = "unknown-schema"
                elif field == "identity":
                    value["identity"]["run_url"] = "https://example.invalid/foreign"
                else:
                    value["raw_secret"] = "not-public"
                raw = reports.collection.encoded(value)
                path.write_bytes(raw)
                proof = self.change_index(output, lambda index: index["records"][0].update(
                    sha256=reports.digest(raw), bytes=len(raw)))
                with self.assertRaises(ValueError):
                    reports.public_batch(output, REVISION, proof)

    def test_extra_paths_and_symlinks_cannot_reach_site(self):
        for mode in ("extra", "symlink", "root_symlink"):
            with self.subTest(mode=mode):
                output, _ = self.batch()
                if mode == "extra":
                    (output / "unexpected.json").write_text("{}")
                    proof = reports.inventory(output)
                    with self.assertRaises(ValueError):
                        reports.public_batch(output, REVISION, proof)
                elif mode == "symlink":
                    path = output / (RUN_KEY + ".json")
                    target = self.root / "public-source.json"
                    target.write_bytes(path.read_bytes())
                    path.unlink()
                    path.symlink_to(target)
                    with self.assertRaises(ValueError):
                        reports.inventory(output)
                else:
                    alias = self.root / "root-alias"
                    alias.symlink_to(output, target_is_directory=True)
                    with self.assertRaises(ValueError):
                        reports.inventory(alias)

    def test_actual_optimized_inspect_rejects_tampered_schema(self):
        output, _ = self.batch()
        proof = self.change_index(output, lambda value: value.update(schema="untrusted"))
        result = self.cli("inspect", "--input", str(output), "--reader-revision", REVISION,
            "--expected-sha256", proof, optimized=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("batch identity differs", result.stdout)

    def test_identity_allows_only_exact_owner_main_events_and_source(self):
        for event in ("push", "workflow_dispatch", "schedule"):
            self.assertEqual(reports.check_identity(identity(GITHUB_EVENT_NAME=event,
                PUBLISH_SCHEDULE_ENABLED="true")), REVISION)
        changes = dict(GITHUB_REPOSITORY="foreign/rcp", GITHUB_REPOSITORY_ID="123", GITHUB_REF="refs/tags/v1",
            GITHUB_ACTOR_ID="1", GITHUB_RUN_ATTEMPT="2", GITHUB_SHA="a" * 39,
            GITHUB_WORKFLOW_SHA="b" * 40, GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/other.yml@refs/heads/main",
            GITHUB_EVENT_NAME="pull_request", PUBLICATION_ENABLED="false")
        for key, value in changes.items():
            with self.subTest(key=key), self.assertRaises(ValueError):
                reports.check_identity(identity(**{key: value}))
        with self.assertRaises(ValueError):
            reports.check_identity(identity(GITHUB_EVENT_NAME="schedule", PUBLISH_SCHEDULE_ENABLED="false"))

    def test_artifact_identity_requires_all_exact_handoff_fields(self):
        self.assertEqual(reports.check_identity(artifact_identity(), artifact=True), REVISION)
        for key, value in (("EXPECTED_READER", "b" * 40), ("EXPECTED_INVENTORY", "not-a-digest"),
                           ("EXPECTED_ARTIFACT", "0"), ("EXPECTED_ARTIFACT", "123\n456")):
            with self.subTest(key=key), self.assertRaises(ValueError):
                reports.check_identity(dict(artifact_identity(), **{key: value}), artifact=True)

    def test_actual_optimized_identity_cli_rejects_foreign_owner(self):
        result = self.cli("identity", "--artifact", environment=dict(artifact_identity(), GITHUB_ACTOR_ID="1"), optimized=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("workflow identity differs", result.stdout)
        result = self.cli("identity", "--artifact", environment=artifact_identity(), optimized=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_site_identity_modes_never_downgrade_to_docs_only(self):
        self.assertEqual(reports.site_identity(docs_identity()), REVISION)
        self.assertEqual(reports.site_identity(artifact_identity()), REVISION)
        cases = [dict(docs_identity(), PUBLICATION_ENABLED="true"),
            dict(docs_identity(), EXPECTED_READER=REVISION),
            dict(docs_identity(), EXPECTED_INVENTORY="b" * 64),
            dict(docs_identity(), EXPECTED_ARTIFACT="123"),
            dict(artifact_identity(), EXPECTED_ARTIFACT=""),
            dict(artifact_identity(), PUBLICATION_ENABLED="false"),
            dict(docs_identity(), GITHUB_EVENT_NAME="schedule"),
            dict(artifact_identity(), GITHUB_WORKFLOW_REF=docs_identity()["GITHUB_WORKFLOW_REF"])]
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValueError):
                reports.site_identity(case)

    def fake_git(self, raw, status=0):
        directory = self.root / ("git-" + str(self.sequence))
        self.sequence += 1
        directory.mkdir()
        capture = directory / "argv.json"
        code = "#!" + sys.executable + "\nimport json, pathlib, sys\n"
        code += "pathlib.Path(" + repr(str(capture)) + ").write_text(json.dumps(sys.argv[1:]))\n"
        code += "sys.stdout.write(" + repr(raw) + ")\nsys.exit(" + str(status) + ")\n"
        executable = directory / "git"
        executable.write_text(code)
        executable.chmod(0o700)
        return directory, capture

    def test_actual_site_gate_fake_git_current_and_stale_preserve_existing_site(self):
        for sha, expected in ((REVISION, "true"), ("b" * 40, "false")):
            with self.subTest(current=expected):
                directory, capture = self.fake_git(sha + "\trefs/heads/main\n")
                target = directory / "github-output"
                result = self.cli("site-gate", environment=dict(docs_identity(), PATH=str(directory), GITHUB_OUTPUT=str(target)))
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(target.read_text(), "current=" + expected + "\ncommit=" + REVISION + "\n")
                self.assertEqual(json.loads(capture.read_text()), ["-c", "credential.helper=", "ls-remote", "--exit-code",
                    "--heads", "https://github.com/wykurz/rcp.git", "refs/heads/main"])
                if expected == "false":
                    self.assertIn("preserve the existing site", result.stdout)

    def test_actual_site_gate_rejects_malformed_duplicate_or_failed_ref_reads(self):
        for raw, status in (("not-a-ref\n", 0), (REVISION + "\trefs/heads/main\n" + REVISION + "\trefs/heads/main\n", 0),
                            (REVISION + " refs/heads/main\n", 0), ("", 128)):
            with self.subTest(raw=raw, status=status):
                directory, _ = self.fake_git(raw, status)
                target = directory / "github-output"
                result = self.cli("site-gate", environment=dict(docs_identity(), PATH=str(directory), GITHUB_OUTPUT=str(target)), optimized=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(target.exists())

    def test_absent_exact_main_is_stale_not_current(self):
        directory, _ = self.fake_git(REVISION + "\trefs/heads/another\n")
        target = directory / "github-output"
        result = self.cli("site-gate", environment=dict(docs_identity(), PATH=str(directory), GITHUB_OUTPUT=str(target)))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(target.read_text(), "current=false\ncommit=" + REVISION + "\n")


if __name__ == "__main__":
    unittest.main()
