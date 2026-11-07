"""Read-only retired evidence, incomplete discovery and historical byte bindings."""

import copy
from dataclasses import replace
from datetime import timedelta
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

    def rewrite_public(self, change):
        """Model a collector-owned envelope, distinct from unanchored object tampering."""
        manifest = json.loads(self.fx.s3.objects[self.key]["1"])
        prefix = self.key.removesuffix("manifest.json")
        report = json.loads(self.fx.s3.objects[prefix + "report.json"]["1"])
        controller = json.loads(self.fx.s3.objects[prefix + "controller.json"]["1"])
        change(manifest, report, controller)
        for name, value in (("controller.json", controller), ("report.json", report)):
            raw = collection.encoded(value)
            self.fx.s3.objects[prefix + name]["1"] = raw
            manifest["retained"][name].update(sha256=collection.digest(raw), bytes=len(raw))
        raw = collection.encoded(manifest)
        self.fx.s3.objects[self.key]["1"] = raw
        self.item["retirement_sha256"] = {"S": collection.digest(raw)}

    def test_public_record_matches_full_export_without_raw_or_html_reads(self):
        from .production_export import project
        expected = project(self.reader.download(self.key))
        prefix = self.key.removesuffix("manifest.json")
        for name in ("durable.json", "status.json", "results.zip", "summary.html"):
            del self.fx.s3.objects[prefix + name]
        original = self.s3.get_object
        def only_public(**args):
            self.assertIn(args["Key"], (self.key, prefix + "report.json", prefix + "controller.json"))
            if args["Key"] != self.key:
                self.assertEqual(args["VersionId"], "1")
            return original(**args)
        with patch.object(self.s3, "get_object", side_effect=only_public) as reads, \
                patch.object(collection.Collector, "analyze", side_effect=AssertionError("must not requalify")), \
                patch.object(collection.results, "inspect", side_effect=AssertionError("must not parse")), \
                patch.object(publication.Bundle, "validate", side_effect=AssertionError("no raw bundle")), \
                patch.object(collection.projection, "freeze", side_effect=AssertionError("no new projection")):
            self.assertEqual(self.reader.public_record(self.key), expected)
        self.assertEqual(reads.call_count, 4)
        self.assertTrue(all(call.kwargs["ConsistentRead"] is True for call in self.db.get_item.call_args_list))

    def test_observe_public_ready_and_unretired_pending(self):
        value = self.reader.observe_public(self.key)
        self.assertEqual((value["state"], value["retired"]), ("ready", True))
        self.assertEqual(value["record"]["run_key"], "repo-123-run-123")
        self.item["closed"] = {"BOOL": False}
        del self.item["retirement_sha256"]
        with patch.object(self.s3, "get_object", side_effect=AssertionError("must not read")):
            value = self.reader.observe_public(self.key)
        self.assertEqual((value["state"], value["retired"]), ("pending", False))

    def test_public_record_rejects_cross_run_before_any_read(self):
        for key in (self.key.replace("repo-123", "repo-124"), self.key + "/x"):
            with self.assertRaises(ValueError):
                self.reader.public_record(key)
        self.db.get_item.assert_not_called()

    def test_public_record_rejects_changed_retirement_record(self):
        other = copy.deepcopy(self.item)
        other["revision"] = {"N": str(int(other["revision"]["N"]) + 1)}
        self.db.get_item.side_effect = [{"Item": self.item}, {"Item": other}]
        with self.assertRaisesRegex(ValueError, "changed during"):
            self.reader.public_record(self.key)

    def test_public_record_rejects_unanchored_manifest_and_payload_tampering(self):
        prefix = self.key.removesuffix("manifest.json")
        for key in (self.key, prefix + "report.json", prefix + "controller.json"):
            with self.subTest(key=key):
                saved = self.fx.s3.objects[key]["1"]
                self.fx.s3.objects[key]["1"] = saved + b" "
                value = self.reader.observe_public(self.key)
                self.assertEqual((value["state"], value["retired"]), ("held", True))
                self.fx.s3.objects[key]["1"] = saved

    def test_public_record_rejects_changed_immutable_manifest_version(self):
        original = self.s3.get_object
        def changed(**args):
            result = original(**args)
            if args["Key"] == self.key and "VersionId" in args:
                result["VersionId"] = "another-version"
            return result
        with patch.object(self.s3, "get_object", side_effect=changed):
            self.assertEqual(self.reader.observe_public(self.key)["state"], "held")

    def test_public_record_preserves_safe_expired_payload_diagnostic(self):
        del self.fx.s3.objects[self.key.replace("manifest.json", "report.json")]
        value = self.reader.observe_public(self.key)
        self.assertEqual((value["state"], value["retired"], value["reason"]), ("held", True, "Error"))

    def test_public_record_rejects_foreign_anchored_identity(self):
        self.rewrite_public(lambda manifest, report, controller: manifest.update(run_key="repo-123-run-456"))
        with self.assertRaisesRegex(ValueError, "different evidence"):
            self.reader.public_record(self.key)

    def test_public_record_rejects_mismatched_report_outcome(self):
        self.rewrite_public(lambda manifest, report, controller: report.update(qualification_outcome="failed"))
        with self.assertRaisesRegex(ValueError, "qualification outcome"):
            self.reader.public_record(self.key)

    def test_public_record_rejects_mismatched_report_and_controller_identity(self):
        for target in ("report", "controller"):
            with self.subTest(target=target):
                def change(manifest, report, controller):
                    selected = report if target == "report" else controller
                    selected["identity"] = dict(selected["identity"], run="456-1")
                # restore each fixture after the independently anchored negative.
                saved = copy.deepcopy(self.fx.s3.objects), copy.deepcopy(self.item)
                self.rewrite_public(change)
                with self.assertRaises(ValueError):
                    self.reader.public_record(self.key)
                self.fx.s3.objects, self.item = saved

    def test_public_record_rejects_bad_checkpoint_and_verification_binding(self):
        for field in ("collection_checkpoint", "verification", "projection_identity", "schema"):
            with self.subTest(field=field):
                saved = copy.deepcopy(self.fx.s3.objects), copy.deepcopy(self.item)
                def change(manifest, report, controller):
                    if field == "collection_checkpoint":
                        report[field]["binding"]["epoch"] = "0" * 64
                    elif field == "verification":
                        report["public_projection"][field] = {"untrusted": "different"}
                    elif field == "projection_identity":
                        report["public_projection"]["identity"]["controller"] = "0" * 40
                    else:
                        report["public_projection"]["schema"] = "future-unknown-schema"
                self.rewrite_public(change)
                with self.assertRaises(ValueError):
                    self.reader.public_record(self.key)
                self.fx.s3.objects, self.item = saved

    def test_public_record_rejects_foreign_reference_and_controller_window(self):
        for field in ("report_key", "report_version", "source_digest", "controller_window", "collected_at"):
            with self.subTest(field=field):
                saved = copy.deepcopy(self.fx.s3.objects), copy.deepcopy(self.item)
                def change(manifest, report, controller):
                    if field == "report_key":
                        manifest["retained"]["report.json"]["key"] = self.key.replace("manifest.json", "../report.json")
                    elif field == "report_version":
                        manifest["retained"]["report.json"]["version"] = "missing-version"
                    elif field == "source_digest":
                        manifest["sources"]["results.zip"]["sha256"] = "0" * 64
                    elif field == "controller_window":
                        controller["window"]["instance_id"] = "i-" + "9" * 17
                        report["evidence"]["controller"] = copy.deepcopy(controller)
                    else:
                        report["public_projection"]["collected_at"] = "2000-01-01T00:00:00+00:00"
                self.rewrite_public(change)
                self.assertEqual(self.reader.observe_public(self.key)["state"], "held")
                self.fx.s3.objects, self.item = saved

    def test_public_record_preserves_collector_evidence_unavailable_outcome(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        fx = Fixture(temp.name)
        old = fx.store.record
        decision = AdmissionDecision(old.run_key, old.epoch, old.fingerprint, "v1", 1, 3, 2, 1,
            "2026-10", "2026-10-22", old.admission["initial_payload"])
        fx.store.record = replace(old, admission=json.loads(decision.immutable_json()))
        del fx.s3.objects[fx.paths.results]
        fx.finish = collection.utc(fx.store.record.state["binding"]["deadline"]) + timedelta(
            seconds=collection.EVIDENCE_RECOVERY_SECONDS)
        result = fx.collector.collect(retire=True)
        self.assertEqual((result["state"], result["retired"]), ("unavailable", True), result)
        db = Mock(spec=["get_item"])
        db.get_item.return_value = {"Item": stored(fx.store.record)}
        reader = publication.Reader(ReadOnlyS3(fx.s3), db, bucket="bucket", table="runs", repository_id="123")
        value = reader.public_record(result["retention"]["key"])
        self.assertFalse(value["benchmark_valid"])
        self.assertEqual(value["qualification_outcome"], "evidence-unavailable")
        self.assertEqual(value["measurements"], [])
        self.assertIn("unknown", value["note"])

    def test_public_record_preserves_all_frozen_public_schema_measurements(self):
        for schema in ("rcp-production-public-report-v1", "rcp-production-public-report-v2", "rcp-production-public-report-v3"):
            with self.subTest(schema=schema):
                saved = copy.deepcopy(self.fx.s3.objects), copy.deepcopy(self.item)
                def change(manifest, report, controller):
                    public = report["public_projection"]
                    public["schema"] = schema
                    if schema != "rcp-production-public-report-v3":
                        del public["collected_at"]
                        del report["collection_checkpoint"]
                    if schema == "rcp-production-public-report-v1":
                        del public["qualification_outcome"]
                self.rewrite_public(change)
                report = json.loads(self.fx.s3.objects[self.key.replace("manifest.json", "report.json")]["1"])
                expected = dict(report["public_projection"], retirement_sha256=self.item["retirement_sha256"]["S"])
                with patch.object(collection.results, "inspect", side_effect=AssertionError("no new parser")):
                    self.assertEqual(self.reader.public_record(self.key), expected)
                self.fx.s3.objects, self.item = saved

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
        public = reader.public_record(value["retention"]["key"])
        self.assertFalse(public["benchmark_valid"])
        self.assertEqual(public["qualification_outcome"], "failed")
        self.assertEqual(public["measurements"], [])


if __name__ == "__main__":
    unittest.main()
