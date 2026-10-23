"""Offline end-to-end handoff, bounded data and immutable Git history tests."""

import copy
import json
import os
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import Mock, patch

from benchmarks.publish import git
from . import production_publish as publish, production_export as public
from . import test_production_publication as fixtures

REVISION = "a" * 40


class PublishTests(unittest.TestCase):
    def setUp(self):
        fixtures.PublicationTests.setUp(self)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        for name, value in (("REPOSITORY_ID", "123"), ("RUN", re.compile(r"repo-123-run-[1-9][0-9]{0,19}"))):
            mocked = patch.object(publish, name, value)
            mocked.start()
            self.addCleanup(mocked.stop)
        self.db.get_item.side_effect = lambda **kw: {} if kw["Key"]["pk"]["S"] == "active" else {"Item": copy.deepcopy(self.item)}
        self.repository = self.root / "history.git"
        git(self.root, "init", "--bare", str(self.repository))

    def batch(self):
        downloaded, exported = self.root / "download", self.root / "export"
        bound = publish.download(self.reader, downloaded, {}, REVISION)
        digest = publish.export(downloaded, {}, exported, REVISION, bound)
        return downloaded, bound, exported, digest

    def test_end_to_end_offline_append_and_exact_replay(self):
        downloaded, bound, exported, digest = self.batch()
        commit = publish.publish(exported, self.repository, REVISION, digest)
        self.assertRegex(commit, r"^[0-9a-f]{40}$")
        self.assertFalse((self.repository / "records").exists())
        git(self.repository, "update-ref", publish.HISTORY_REF, commit)
        records, parent = publish.history(self.repository)
        self.assertEqual(parent, commit)
        self.assertEqual(list(records), [self.fx.store.record.run_key])
        with patch.object(public, "project", side_effect=AssertionError("historical measurements must survive")):
            replayed = self.root / "replay"
            replay_digest = publish.export(downloaded, records, replayed, REVISION, bound)
        self.assertEqual((replayed / (self.fx.store.record.run_key + ".json")).read_bytes(),
                         records[self.fx.store.record.run_key])
        self.assertEqual(publish.publish(replayed, self.repository, REVISION, replay_digest), "")

    def test_published_checkpoint_avoids_raw_reads(self):
        _, _, exported, bound = self.batch()
        commit = publish.publish(exported, self.repository, REVISION, bound)
        git(self.repository, "update-ref", publish.HISTORY_REF, commit)
        records, _ = publish.history(self.repository)
        output = self.root / "checkpoint"
        with patch.object(self.reader, "observe", side_effect=AssertionError("must skip old raw")):
            publish.download(self.reader, output, records, REVISION)
        index = json.loads((output / "index.json").read_bytes())
        self.assertEqual(index["bundles"], [])
        self.assertEqual(index["observations"][0]["state"], "published")

    def test_checkpoint_conflict_is_named_held_without_overwriting_history(self):
        bundle = self.reader.download(self.key)
        value = public.project(bundle)
        value["retirement_sha256"] = "0" * 64
        output = self.root / "bad-checkpoint"
        prior = {bundle.run_key: publish.encoded(value)}
        publish.download(self.reader, output, prior, REVISION)
        index = json.loads((output / "index.json").read_bytes())
        self.assertEqual(index["bundles"], [])
        self.assertEqual(index["observations"], [dict(run_key=bundle.run_key, state="held", retired=True,
            reason="published source conflicts with retirement record")])
        self.assertEqual(prior, {bundle.run_key: publish.encoded(value)})

    def test_cross_job_digest_corruption_is_rejected_before_parsing(self):
        source, _, _, _ = self.batch()
        with patch.object(publish, "index_at", side_effect=AssertionError("must verify inventory first")):
            with self.assertRaisesRegex(ValueError, "inventory differs"):
                publish.export(source, {}, self.root / "bad", REVISION, "0" * 64)

    def test_changed_reader_revision_is_rejected(self):
        source, bound, _, _ = self.batch()
        with self.assertRaisesRegex(ValueError, "identity differs"):
            publish.export(source, {}, self.root / "wrong", "b" * 40, bound)

    def test_raw_data_cannot_reach_history_writer(self):
        source, bound, _, _ = self.batch()
        with self.assertRaisesRegex(ValueError, "identity differs"):
            publish.publish(source, self.repository, REVISION, bound)

    def test_symlink_or_extra_file_is_rejected(self):
        source, bound, exported, _ = self.batch()
        (exported / "bad").symlink_to(source)
        with self.assertRaises(ValueError):
            publish.inventory(exported)
        (exported / "bad").unlink()
        (exported / "raw.zip").write_bytes(b"private")
        with self.assertRaisesRegex(ValueError, "unexpected"):
            publish.public_batch(exported, REVISION, publish.inventory(exported))

    def test_aggregate_limits_publish_explicit_deferred_status(self):
        with patch.object(publish, "MAX_TOTAL", 1):
            output = self.root / "bytes"
            publish.download(self.reader, output, {}, REVISION)
        value = json.loads((output / "index.json").read_bytes())
        self.assertEqual(value["observations"][0]["state"], "deferred")
        self.assertTrue(value["discovery_complete"])
        self.assertFalse(value["processing_complete"])
        output = self.root / "time"
        publish.download(self.reader, output, {}, REVISION, clock=Mock(side_effect=[0, 601]))
        value = json.loads((output / "index.json").read_bytes())
        self.assertEqual(value["observations"][0]["state"], "deferred")
        self.assertFalse(value["processing_complete"])

    def test_bounded_batch_publishes_completed_subset_then_checkpoint_skips_it(self):
        other = self.key.replace("run-123/", "run-456/")
        self.reader.discover = Mock(return_value=[self.key, other])
        output, exported = self.root / "bounded", self.root / "bounded-export"
        cap = publish.publication.MAX_BUNDLE_BYTES + publish.codec.MAX_ENVELOPE + publish.MAX_INDEX
        with patch.object(publish, "MAX_TOTAL", cap):
            bound = publish.download(self.reader, output, {}, REVISION)
        index = json.loads((output / "index.json").read_bytes())
        self.assertEqual([x["state"] for x in index["observations"]], ["ready", "deferred"])
        digest = publish.export(output, {}, exported, REVISION, bound)
        commit = publish.publish(exported, self.repository, REVISION, digest)
        git(self.repository, "update-ref", publish.HISTORY_REF, commit)
        records, _ = publish.history(self.repository)
        self.reader.discover = Mock(return_value=[self.key])
        with patch.object(self.reader, "observe", side_effect=AssertionError("already published")):
            publish.download(self.reader, self.root / "rescan", records, REVISION)

    def test_frozen_v1_git_history_survives_current_contract_changes(self):
        raw = (Path(__file__).with_name("fixtures") / "production-public-report-v1.json").read_bytes()
        value = json.loads(raw)
        blob = git(self.repository, "hash-object", "-w", "--stdin", input_text=raw.decode()).stdout.strip()
        tree = git(self.repository, "mktree", input_text="100644 blob " + blob + "\t" + value["run_key"] + ".json\n").stdout.strip()
        root = git(self.repository, "mktree", input_text="040000 tree " + tree + "\trecords\n").stdout.strip()
        commit = git(self.repository, "commit-tree", root, "-m", "Frozen v1 history", environment=publish.git_env()).stdout.strip()
        git(self.repository, "update-ref", publish.HISTORY_REF, commit)
        with patch.object(public, "SCHEMA", "future-public-report-v2"), \
                patch.object(public, "VALIDATION", "future wording"), \
                patch.object(public.results, "SERIES_CONTRACT", "future-series-v2"):
            records, parent = publish.history(self.repository)
        self.assertEqual((records, parent), ({value["run_key"]: raw}, commit))

    def test_export_failure_is_named_held_and_completed_subset_is_publishable(self):
        downloaded, bound, _, _ = self.batch()
        first = self.fx.store.record.run_key
        second = "repo-123-run-456"
        (downloaded / second).mkdir()
        (downloaded / second / "unreadable.json").write_text("{}")
        index_path = downloaded / "index.json"
        index = json.loads(index_path.read_bytes())
        index["bundles"].append(second)
        index["observations"].append(dict(run_key=second, state="ready", retired=True))
        index_path.write_bytes(publish.encoded(index))
        output = self.root / "mixed-export"
        bound = publish.export(downloaded, {}, output, REVISION, publish.inventory(downloaded))
        records, index = publish.public_batch(output, REVISION, bound)
        self.assertEqual(list(records), [first])
        held = next(item for item in index["observations"] if item["run_key"] == second)
        self.assertEqual((held["state"], held["retired"], held["phase"]), ("held", True, "export"))
        self.assertFalse((output / (second + ".json")).exists())
        commit = publish.publish(output, self.repository, REVISION, bound)
        git(self.repository, "update-ref", publish.HISTORY_REF, commit)
        self.assertEqual(list(publish.history(self.repository)[0]), [first])
        summary = self.root / "job-summary"
        with patch.dict(os.environ, GITHUB_STEP_SUMMARY=str(summary)):
            publish.summary(index)
        self.assertIn(second + ": held", summary.read_text())
        self.assertIn("1 ready, 0 published, 0 pending, 1 held", summary.read_text())

    def test_export_project_failure_preserves_named_retirement_and_does_not_claim_ready(self):
        downloaded, bound, _, _ = self.batch()
        output = self.root / "unsupported-export"
        with patch.object(public, "project", side_effect=ValueError("future format")):
            result = publish.export(downloaded, {}, output, REVISION, bound)
        records, index = publish.public_batch(output, REVISION, result)
        self.assertEqual(records, {})
        self.assertEqual(index["observations"], [dict(run_key=self.fx.store.record.run_key,
            state="held", retired=True, phase="export", reason="ValueError")])
        self.assertEqual(publish.publish(output, self.repository, REVISION, result), "")

    def test_export_time_bound_defers_remaining_named_runs_without_loading(self):
        downloaded, bound, _, _ = self.batch()
        output = self.root / "deferred-export"
        with patch.object(publish.codec, "load", side_effect=AssertionError("must defer before loading")):
            result = publish.export(downloaded, {}, output, REVISION, bound, clock=Mock(side_effect=[0, 601]))
        records, index = publish.public_batch(output, REVISION, result)
        self.assertEqual(records, {})
        observed = index["observations"][0]
        self.assertEqual((observed["run_key"], observed["state"], observed["retired"], observed["phase"]),
                         (self.fx.store.record.run_key, "deferred", True, "export"))

    def test_bundle_without_unique_ready_observation_is_rejected_before_loading(self):
        downloaded, _, _, _ = self.batch()
        path = downloaded / "index.json"
        index = json.loads(path.read_bytes())
        index["observations"] = []
        path.write_bytes(publish.encoded(index))
        with patch.object(publish.codec, "load", side_effect=AssertionError("must reject first")):
            with self.assertRaisesRegex(ValueError, "observations differ"):
                publish.export(downloaded, {}, self.root / "orphan-export", REVISION, publish.inventory(downloaded))

    def test_git_object_read_enforces_byte_bound(self):
        blob = git(self.repository, "hash-object", "-w", "--stdin", input_text="bounded").stdout.strip()
        self.assertEqual(publish.git_bytes(self.repository, "cat-file", "blob", blob, limit=7), b"bounded")
        with self.assertRaisesRegex(ValueError, "byte bound"):
            publish.git_bytes(self.repository, "cat-file", "blob", blob, limit=6)

    def test_held_result_is_visible_without_raw_payload(self):
        del self.fx.s3.objects[self.key.replace("manifest.json", "results.zip")]
        output = self.root / "held"
        publish.download(self.reader, output, {}, REVISION)
        value = json.loads((output / "index.json").read_bytes())
        self.assertEqual(value["observations"][0]["state"], "held")
        self.assertTrue(value["observations"][0]["retired"])
        self.assertEqual(value["bundles"], [])
        self.assertEqual(value["active"]["state"], "idle")

    def test_empty_public_batch_is_noop(self):
        self.reader.discover = Mock(return_value=[])
        _, _, exported, bound = self.batch()
        self.assertEqual(publish.publish(exported, self.repository, REVISION, bound), "")
        self.assertEqual(publish.history(self.repository), ({}, None))

    def test_conflicting_history_preserves_existing_ref(self):
        _, _, exported, bound = self.batch()
        commit = publish.publish(exported, self.repository, REVISION, bound)
        git(self.repository, "update-ref", publish.HISTORY_REF, commit)
        path = exported / (self.fx.store.record.run_key + ".json")
        value = json.loads(path.read_bytes())
        value["note"] = "changed history"
        raw = publish.encoded(value)
        path.write_bytes(raw)
        index_path = exported / "index.json"
        index = json.loads(index_path.read_bytes())
        index["records"][0].update(sha256=publish.digest(raw), bytes=len(raw))
        index_path.write_bytes(publish.encoded(index))
        with self.assertRaisesRegex(ValueError, "overwrite forbidden"):
            publish.publish(exported, self.repository, REVISION, publish.inventory(exported))
        self.assertEqual(publish.history(self.repository)[1], commit)

    def test_unexpected_history_path_and_mode_fail_closed(self):
        blob = git(self.repository, "hash-object", "-w", "--stdin", input_text="x").stdout.strip()
        env = publish.git_env()
        tree = git(self.repository, "mktree", input_text="120000 blob " + blob + "\tunknown\n", environment=env).stdout.strip()
        commit = git(self.repository, "commit-tree", tree, "-m", "bad", environment=env).stdout.strip()
        git(self.repository, "update-ref", publish.HISTORY_REF, commit)
        with self.assertRaisesRegex(ValueError, "unexpected"):
            publish.history(self.repository)

    def test_missing_record_is_held_without_aborting_batch(self):
        with patch.object(self.reader, "record", side_effect=ValueError("missing")):
            output = self.root / "missing-record"
            publish.download(self.reader, output, {}, REVISION)
        index = json.loads((output / "index.json").read_bytes())
        self.assertEqual(index["observations"], [{"run_key": self.fx.store.record.run_key,
            "state": "held", "retired": False, "reason": "ValueError"}])
        self.assertEqual(index["bundles"], [])

    def test_active_before_manifest_and_owner_change_are_reported(self):
        item = copy.deepcopy(self.item)
        item["closed"] = {"BOOL": False}
        del item["retirement_sha256"]
        owner = dict(pk={"S": "active"}, run_key=item["pk"].copy(), epoch=item["epoch"].copy())
        owner["run_key"]["S"] = self.fx.store.record.run_key
        self.db.get_item.side_effect = lambda **kw: {"Item": copy.deepcopy(owner if kw["Key"]["pk"]["S"] == "active" else item)}
        result = self.reader.active_observation()
        self.assertEqual((result["state"], result["phase"], result["retired"]), ("pending", "complete", False))
        self.db.get_item.side_effect = [{"Item": owner}, {"Item": item}, {}]
        with self.assertRaisesRegex(ValueError, "changed during"):
            self.reader.active_observation()


if __name__ == "__main__":
    unittest.main()
