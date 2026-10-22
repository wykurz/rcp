"""Read-only retired evidence, incomplete discovery and historical byte bindings."""

import copy
from dataclasses import replace
import json
import tempfile
import unittest
from unittest.mock import Mock, patch

from . import production_collection as collection, production_publication as publication
from .production_store import AdmissionDecision
from .test_production_collection import Fixture


def stored(record):
    return dict(pk={"S": "run#" + record.run_key}, epoch={"S": record.epoch},
        fingerprint={"S": record.fingerprint}, admission={"S": collection.encoded(record.admission).decode()},
        state={"S": collection.encoded(record.state).decode()}, revision={"N": str(record.revision)},
        closed={"BOOL": record.closed}, terminal={"BOOL": record.terminal},
        **({"retirement_sha256": {"S": record.retirement_sha256}} if record.closed else {}))


class ReadOnlyS3:
    def __init__(self, source):
        self.source = source
    def get_object(self, **args):
        return self.source.get_object(**args)
    def list_objects_v2(self, **args):
        assert args["MaxKeys"] == publication.PAGE_SIZE
        assert "ContinuationToken" not in args
        keys = sorted(key for key in self.source.objects if key.startswith(args["Prefix"]))
        return dict(Contents=[dict(Key=key) for key in keys], KeyCount=len(keys), IsTruncated=False)


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.network = patch("socket.create_connection", side_effect=AssertionError("offline only"))
        self.network.start()
        self.addCleanup(self.network.stop)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.fx = Fixture(temp.name)
        old = self.fx.store.record
        decision = AdmissionDecision(old.run_key, old.epoch, old.fingerprint, "reviewed-v1", 20_000_000,
            350_000_000, 50_000_000, 1, "2026-10", "2026-10-22", old.admission["initial_payload"])
        self.fx.store.record = replace(old, admission=json.loads(decision.immutable_json()))
        result = self.fx.collector.collect(retire=True)
        self.assertEqual(result["state"], "success", result)
        self.key = result["retention"]["key"]
        self.item = stored(self.fx.store.record)
        self.db = Mock(spec=["get_item"])
        self.db.get_item.side_effect = lambda **kw: {"Item": copy.deepcopy(self.item)}
        self.s3 = ReadOnlyS3(self.fx.s3)
        self.reader = publication.Reader(self.s3, self.db, bucket="bucket", table="runs", repository_id="123")

    def test_complete_discovery_and_read_only_download(self):
        writes = len(self.fx.s3.writes)
        self.assertEqual(self.reader.discover(), [self.key])
        bundle = self.reader.download(self.key)
        self.assertTrue(bundle.validate()["benchmark_valid"])
        self.assertEqual(bundle.payloads["summary.html"], self.fx.s3.objects[self.key.replace("manifest.json", "summary.html")]["1"])
        self.assertEqual(len(self.fx.s3.writes), writes)
        self.assertTrue(all(call.kwargs["ConsistentRead"] is True for call in self.db.get_item.call_args_list))
        self.assertTrue(all(call.kwargs["Key"] == {"pk": {"S": "run#repo-123-run-123"}}
                            for call in self.db.get_item.call_args_list))

    def test_closed_replay_does_not_render_or_parse_measurements(self):
        with patch.object(collection.Collector, "analyze", side_effect=AssertionError("must not requalify")), \
                patch.object(collection.results, "inspect", side_effect=AssertionError("must not parse")):
            self.assertTrue(self.reader.download(self.key).validate()["benchmark_valid"])

    def test_unretired_manifest_is_pending_and_does_not_read_payloads(self):
        self.item["closed"] = {"BOOL": False}
        del self.item["retirement_sha256"]
        with patch.object(self.s3, "get_object", side_effect=AssertionError("unretired")):
            result = self.reader.observe(self.key)
        self.assertEqual(result["state"], "pending")
        self.assertFalse(result["retired"])

    def test_mismatched_retirement_digest_remains_held_and_retired(self):
        self.item["retirement_sha256"] = {"S": "0" * 64}
        result = self.reader.observe(self.key)
        self.assertEqual((result["state"], result["retired"]), ("held", True))
        self.assertIn("digest", result["reason"])

    def test_expired_object_preserves_retirement_without_echoing_sdk_message(self):
        del self.fx.s3.objects[self.key.replace("manifest.json", "results.zip")]
        result = self.reader.observe(self.key)
        self.assertEqual((result["state"], result["retired"]), ("held", True))
        self.assertEqual(result["reason"], "Error")

    def test_record_epoch_not_selected_by_manifest(self):
        self.item["epoch"] = {"S": "a" * 64}
        with self.assertRaises(ValueError):
            self.reader.download(self.key)

    def test_record_change_during_download_is_rejected(self):
        other = copy.deepcopy(self.item)
        other["revision"] = {"N": str(int(other["revision"]["N"]) + 1)}
        self.db.get_item.side_effect = [{"Item": self.item}, {"Item": other}]
        with self.assertRaisesRegex(ValueError, "changed during"):
            self.reader.download(self.key)

    def test_cross_run_key_rejected_before_read(self):
        for key in (self.key.replace("repo-123", "repo-124"), self.key + "/x", self.key.replace("123/", "../")):
            with self.assertRaises(ValueError):
                self.reader.download(key)
        self.db.get_item.assert_not_called()

    def test_bundle_payload_corruption_is_rejected(self):
        bundle = self.reader.download(self.key)
        changed = dict(bundle.payloads, **{"summary.html": b"different"})
        with self.assertRaisesRegex(ValueError, "reference differs"):
            replace(bundle, payloads=changed).validate()

    def test_bundle_saved_record_must_match_decoded_record(self):
        bundle = self.reader.download(self.key)
        item = copy.deepcopy(bundle.record_item)
        item["revision"] = {"N": "999"}
        with self.assertRaisesRegex(ValueError, "decoded record"):
            replace(bundle, record_item=item).validate()

    def page(self, keys, truncated=False, token=None):
        result = dict(Contents=[dict(Key=key) for key in keys], KeyCount=len(keys), IsTruncated=truncated)
        if token is not None:
            result["NextContinuationToken"] = token
        return result

    def test_discovery_paginates_without_moving_checkpoint(self):
        other = self.key.replace("run-123/", "run-456/")
        with patch.object(self.s3, "list_objects_v2", side_effect=[self.page([self.key], True, "next"), self.page([other])]) as api:
            self.assertEqual(self.reader.discover(), sorted([self.key, other]))
        self.assertEqual(api.call_args_list[1].kwargs["ContinuationToken"], "next")

    def test_discovery_rejects_duplicate_repeated_missing_and_partial_pages(self):
        cases = [
            [self.page([self.key], True, "next"), self.page([self.key])],
            [self.page([], True, "next")],
            [self.page([self.key], True)],
            [self.page([self.key], False, "next")],
            [dict(self.page([self.key]), KeyCount=2)],
            [dict(self.page([self.key]), IsTruncated=1)],
            [self.page(["unrelated"])],
            [dict(self.page([]), CommonPrefixes=[{"Prefix": self.reader.prefix}])],
            [dict(self.page([]), Delimiter="/")],
            [self.page([self.key], True, "next"), self.page([self.key + "/x"], True, "next")],
        ]
        for pages in cases:
            with self.subTest(pages=pages), patch.object(self.s3, "list_objects_v2", side_effect=pages):
                with self.assertRaises(ValueError):
                    self.reader.discover()

    def test_discovery_page_and_time_bounds_do_not_return_partial_results(self):
        with patch.object(publication, "MAX_PAGES", 1), \
                patch.object(self.s3, "list_objects_v2", return_value=self.page([self.key], True, "next")):
            with self.assertRaisesRegex(ValueError, "page bound"):
                self.reader.discover()
        self.reader.clock = Mock(side_effect=[0, 0, 121])
        with self.assertRaisesRegex(ValueError, "time bound"):
            self.reader.discover()

    def test_failed_retired_run_allows_absent_worker_payloads(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        fx = Fixture(temp.name)
        old = fx.store.record
        decision = AdmissionDecision(old.run_key, old.epoch, old.fingerprint, "v1", 1, 3, 2, 1,
            "2026-10", "2026-10-22", old.admission["initial_payload"])
        fx.store.record = replace(old, admission=json.loads(decision.immutable_json()))
        del fx.s3.objects[fx.paths.results]
        del fx.s3.objects[fx.paths.status]
        value = fx.collector.collect(retire=True)
        self.assertEqual((value["state"], value["retired"]), ("failed", True))
        db = Mock(spec=["get_item"])
        db.get_item.return_value = {"Item": stored(fx.store.record)}
        reader = publication.Reader(ReadOnlyS3(fx.s3), db, bucket="bucket", table="runs", repository_id="123")
        bundle = reader.download(value["retention"]["key"])
        self.assertFalse(bundle.validate()["benchmark_valid"])
        self.assertNotIn("status.json", bundle.payloads)
        self.assertNotIn("results.zip", bundle.payloads)


if __name__ == "__main__":
    unittest.main()
