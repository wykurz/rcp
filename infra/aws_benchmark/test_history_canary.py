"""Offline state-machine, real Git and workflow checks; no live protection claim."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml

from . import history_canary as canary
from .common import ValidationError
from .history_contract import CANARY_WORKFLOW, ENVIRONMENT, HISTORY_REF, PUBLICATION_WORKFLOW, WRITER_SECRET_NAME
from .production_publish import history

ROOT = Path(__file__).resolve().parents[2]
BINDING = canary.identity("0123456789abcdef", "a" * 64, "b" * 40)


class Fake:
    def __init__(self):
        self.values, self.parents, self.operations = {}, {}, []
        self.bad_stage = None
        self.mode = None
        self.verified = False

    def refs(self):
        return dict(self.values)

    def commit(self, parent=None, label="bootstrap"):
        value = f"{len(self.parents) + 1:040x}"
        self.parents[value] = parent
        return value

    def push(self, actor, ref, commit, expected=None):
        self.operations.append((actor, ref, commit, expected))
        ordinary_control = actor == "ordinary" and ref != HISTORY_REF
        fast_forward = commit is not None and self.parents[commit] == self.values.get(ref)
        allowed = ordinary_control or (actor == "writer" and fast_forward)
        if self.bad_stage == len(self.operations):
            if self.mode == "allow":
                allowed = True
            elif self.mode == "deny":
                allowed = False
        if allowed:
            if commit:
                self.values[ref] = commit
            else:
                self.values.pop(ref, None)
        if self.bad_stage == len(self.operations) and self.mode == "changed":
            self.values[ref] = "f" * 40
        return dict(accepted=allowed, rule_denial=not allowed and self.mode != "ambiguous",
                    response_sha256="0" * 64)

    def rename(self, source, destination):
        allowed = destination != HISTORY_REF
        if allowed:
            self.values[destination] = self.values.pop(source)
        return dict(accepted=allowed, rule_denial=not allowed and self.mode != "rename_ambiguous", status=201 if allowed else 422,
                    response_sha256="0" * 64)

    def settled_rename(self, source, destination, expected, observe=None):
        if source in self.values or self.values.get(destination) != expected:
            raise ValidationError("rename unsettled")
        return self.refs()

    def verify_history(self, expected):
        if self.values.get(HISTORY_REF) != expected:
            raise ValidationError("history changed")
        self.verified = True


class CanaryTests(unittest.TestCase):
    def test_complete_empty_canary_preserves_history_and_removes_only_control(self):
        with tempfile.TemporaryDirectory() as directory:
            remote = Fake()
            path = Path(directory) / "receipt.jsonl"
            run = canary.Canary(remote, path, BINDING)
            root = run.ordinary()
            self.assertEqual(canary.prior_root(path, BINDING), root)
            run.writer(root)
            self.assertTrue(remote.verified)
            self.assertEqual(set(remote.values), {HISTORY_REF})
            rows = [json.loads(row) for row in path.read_text().splitlines()]
            self.assertEqual(rows[-1]["stage"], "complete")
            self.assertFalse(rows[-1]["readiness_changed"])
            self.assertEqual([row["stage"] for row in rows if not row["stage"].endswith(("_intent", "_outcome", "_baseline", "_observed")) and row["stage"] not in ("initial_observation", "sentinels", "writer_precondition", "ordinary_rename_control_poll")], ["initial_absence", "ordinary_create_control",
                "ordinary_rename_control", "ordinary_create_history", "ordinary_rename_history", "ordinary_complete",
                "writer_create_history", "writer_fast_forward", "ordinary_fast_forward", "writer_non_fast_forward",
                "writer_delete", "ordinary_non_fast_forward", "ordinary_delete", "publisher_compatible", "control_cleanup", "complete"])
            ordinary_update = remote.operations[4]
            self.assertEqual(remote.parents[ordinary_update[2]], ordinary_update[3])
            for _, ref, commit, expected in remote.operations[5:9]:
                self.assertEqual(ref, HISTORY_REF)
                self.assertIsNotNone(expected)
                self.assertNotEqual(remote.parents.get(commit), expected)

    def test_lost_mutation_response_retains_exact_intent_and_uncertainty(self):
        with tempfile.TemporaryDirectory() as directory:
            remote = Fake()
            path = Path(directory) / "receipt"
            run = canary.Canary(remote, path, BINDING)
            commit = remote.commit()
            with patch.object(remote, "push", side_effect=TimeoutError("must not echo diagnostic")):
                with self.assertRaises(TimeoutError):
                    run.positive("writer_create_history", "writer", HISTORY_REF, commit)
            rows = [json.loads(row) for row in path.read_text().splitlines()]
            self.assertEqual(rows[1]["stage"], "writer_create_history_intent")
            self.assertEqual((rows[1]["ref"], rows[1]["commit"], rows[1]["expected"]), (HISTORY_REF, commit, None))
            self.assertEqual(rows[-1]["stage"], "writer_create_history_uncertain")
            self.assertNotIn("must not echo", path.read_text())

    def test_lost_post_write_readback_retains_the_received_outcome(self):
        with tempfile.TemporaryDirectory() as directory:
            remote = Fake()
            path = Path(directory) / "receipt"
            run = canary.Canary(remote, path, BINDING)
            commit = remote.commit()
            with patch.object(remote, "refs", side_effect=[{}, TimeoutError()]):
                with self.assertRaises(TimeoutError):
                    run.positive("writer_create_history", "writer", HISTORY_REF, commit)
            rows = [json.loads(row) for row in path.read_text().splitlines()]
            self.assertEqual(rows[-1]["stage"], "writer_create_history_observed_read_failed")
            self.assertTrue(rows[-2]["outcome"]["accepted"])
            self.assertEqual(remote.values[HISTORY_REF], commit)

    def test_existing_exact_or_descendant_history_is_never_repaired(self):
        for existing in (HISTORY_REF, HISTORY_REF + "/one", HISTORY_REF + "/one/two"):
            with self.subTest(existing=existing), tempfile.TemporaryDirectory() as directory:
                remote = Fake()
                remote.values[existing] = "d" * 40
                with self.assertRaisesRegex(ValidationError, "untrusted bootstrap"):
                    canary.Canary(remote, Path(directory) / "receipt", BINDING).ordinary()
                self.assertEqual(remote.operations, [])
                self.assertEqual(remote.values, {existing: "d" * 40})

    def test_positive_write_never_adopts_an_unexpected_tip(self):
        for expected in (None, "a" * 40):
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as directory:
                remote = Fake()
                remote.values[HISTORY_REF] = "f" * 40
                commit = remote.commit(expected)
                run = canary.Canary(remote, Path(directory) / "receipt", BINDING)
                with self.assertRaisesRegex(ValidationError, "baseline changed"):
                    run.positive("probe", "writer", HISTORY_REF, commit, expected)
                self.assertEqual(remote.operations, [])
                self.assertEqual(remote.values[HISTORY_REF], "f" * 40)

    def test_rename_stops_on_observed_source_or_destination_drift(self):
        for values in ({"source": "f" * 40}, {"source": "a" * 40, "dest": "f" * 40}):
            with self.subTest(values=values), tempfile.TemporaryDirectory() as directory:
                remote = Fake()
                remote.values = values
                run = canary.Canary(remote, Path(directory) / "receipt", BINDING)
                with patch.object(remote, "rename") as rename:
                    with self.assertRaisesRegex(ValidationError, "rename baseline changed"):
                        run.rename("rename", "source", "dest", "a" * 40)
                    rename.assert_not_called()

    def test_missing_write_authority_does_not_prove_protection(self):
        with tempfile.TemporaryDirectory() as directory:
            remote = Fake()
            remote.bad_stage, remote.mode = 1, "deny"
            path = Path(directory) / "receipt"
            with self.assertRaisesRegex(ValidationError, "positive control failed"):
                canary.Canary(remote, path, BINDING).ordinary()
            self.assertNotIn("ordinary_complete", path.read_text())
            self.assertEqual(len(remote.operations), 1)

    def test_ambiguous_rename_preserves_evidence_and_prevents_mint_phase(self):
        with tempfile.TemporaryDirectory() as directory:
            remote = Fake()
            remote.mode = "rename_ambiguous"
            path = Path(directory) / "receipt"
            with self.assertRaisesRegex(ValidationError, "rename denial is inconclusive"):
                canary.Canary(remote, path, BINDING).ordinary()
            with self.assertRaisesRegex(ValidationError, "did not complete"):
                canary.prior_root(path, BINDING)
            self.assertEqual(len(remote.operations), 2)
            self.assertNotIn(HISTORY_REF, remote.values)

    def test_unexpected_allow_changed_state_and_transport_denial_never_pass(self):
        for mode in ("allow", "changed", "ambiguous"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                remote = Fake()
                remote.bad_stage, remote.mode = 2, mode
                path = Path(directory) / "receipt"
                with self.assertRaisesRegex(ValidationError, "negative control not proven"):
                    canary.Canary(remote, path, BINDING).ordinary()
                self.assertEqual(len(remote.operations), 2)
                self.assertNotIn("ordinary_complete", path.read_text())

    def test_each_writer_failure_stops_without_automatic_repair(self):
        for operation in range(3, 10):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as directory:
                remote = Fake()
                path = Path(directory) / "receipt"
                run = canary.Canary(remote, path, BINDING)
                root = run.ordinary()
                remote.bad_stage = operation
                remote.mode = "deny" if operation in (3, 4) else "allow"
                with self.assertRaises(ValidationError):
                    run.writer(root)
                self.assertEqual(len(remote.operations), operation)
                self.assertNotIn('"stage": "complete"', path.read_text())

    def test_receipt_binding_and_reuse_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt"
            run = canary.Canary(Fake(), path, BINDING)
            run.ordinary()
            with self.assertRaisesRegex(ValidationError, "do not rerun"):
                run.ordinary()
            with self.assertRaisesRegex(ValidationError, "binding differs"):
                canary.prior_root(path, dict(BINDING, revision="e" * 40))

    def test_git_server_rule_classifier_rejects_vacuous_failures(self):
        row = "!\tabc:" + HISTORY_REF + "\t[remote rejected] (push declined due to repository rule violations)\n"
        error = "remote: error: GH013: Repository rule violations found for " + HISTORY_REF + ".\n"
        self.assertTrue(canary.rule_rejection(1, row, error, HISTORY_REF))
        for code, out, err in [(0, row, error), (1, row, "authentication failed"), (1, row, "non-fast-forward"),
                              (1, row.replace("[remote rejected]", "[rejected]"), error),
                              (1, row.replace(HISTORY_REF, "refs/heads/other"), error), (1, row + row, error),
                              (1, row, error.replace(HISTORY_REF, "refs/heads/other"))]:
            with self.subTest(code=code, out=out, err=err):
                self.assertFalse(canary.rule_rejection(code, out, err, HISTORY_REF))

    def test_real_git_empty_tree_commits_pass_real_depth_one_history_reader(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            remote = canary.Remote(base / "remote.git", "synthetic-not-used")
            root = remote.commit()
            child = remote.commit(root)
            other = remote.commit(label="non-fast-forward probe")
            self.assertNotIn(other, (root, child))
            for commit in (root, child, other):
                result = remote.git("--git-dir=" + str(remote.directory), "rev-parse", commit + "^{tree}")
                self.assertEqual(result.stdout.strip(), canary.EMPTY_TREE)
            self.assertEqual(remote.git("--git-dir=" + str(remote.directory), "update-ref", HISTORY_REF, child).returncode, 0)
            reader = canary.Remote(base / "reader.git", "synthetic-not-used")
            with patch.object(canary, "REMOTE", "file://" + str(remote.directory)):
                reader.verify_history(child)
            self.assertEqual(history(reader.directory), ({}, child))
            self.assertTrue((reader.directory / "shallow").exists())

    def test_workflow_separates_disabled_canary_from_publication_readiness(self):
        value = yaml.safe_load((ROOT / ".github/workflows" / CANARY_WORKFLOW).read_text())
        self.assertEqual(set(value[True]), {"workflow_dispatch"})
        job = value["jobs"]["verify"]
        self.assertEqual(job["permissions"], {"contents": "write", "actions": "read"})
        self.assertEqual(job["environment"], {"name": ENVIRONMENT, "deployment": False})
        self.assertIn("RCP_AWS_HISTORY_CANARY_ENABLED == 'true'", job["if"])
        self.assertIn("RCP_AWS_HISTORY_TRUST_READY == 'false'", job["if"])
        self.assertIn("RCP_AWS_PRODUCTION_PUBLISH_ENABLED == 'false'", job["if"])
        self.assertIn("github.run_attempt == '1'", job["if"])
        steps = job["steps"]
        ordinary = next(i for i, s in enumerate(steps) if "history_canary ordinary" in s.get("run", ""))
        mint = next(i for i, s in enumerate(steps) if s.get("id") == "writer")
        writer = next(i for i, s in enumerate(steps) if "history_canary writer" in s.get("run", ""))
        self.assertLess(ordinary, mint)
        self.assertLess(mint, writer)
        self.assertFalse(steps[mint]["with"]["skip-token-revoke"])
        publish = yaml.safe_load((ROOT / ".github/workflows" / PUBLICATION_WORKFLOW).read_text())["jobs"]["publish"]
        self.assertEqual(job["concurrency"], publish["concurrency"])
        self.assertEqual(steps[-1]["if"], "always()")
        self.assertNotIn("id-token", str(value))

    def test_only_reviewed_lanes_reference_writer_environment_or_key(self):
        from .workflow_inventory import check_workflows
        check_workflows(ROOT / ".github/workflows")


if __name__ == "__main__":
    unittest.main()
