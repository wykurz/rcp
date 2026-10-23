"""Offline interruption tests for the immutable collection transaction."""

import copy
from datetime import timedelta
import json
import tempfile
import unittest
from unittest.mock import patch

from . import common, production_collection as collection, results
from .test_production_collection import Fixture


class CollectionRetryTests(unittest.TestCase):
    def setUp(self):
        self.network = patch("socket.create_connection", side_effect=AssertionError("offline tests only"))
        self.network.start()
        self.addCleanup(self.network.stop)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def interrupt(self, fx, name, *, committed=False):
        key = fx.collector.prefix + name
        put, get = fx.s3.put_object, fx.s3.get_object
        unreadable = False
        def write(**args):
            nonlocal unreadable
            if args["Key"] != key:
                return put(**args)
            if committed:
                put(**args)
                unreadable = True
            raise TimeoutError("injected interruption")
        def read(**args):
            if unreadable and args["Key"] == key:
                raise TimeoutError("injected readback loss")
            return get(**args)
        with patch.object(fx.s3, "put_object", side_effect=write), patch.object(fx.s3, "get_object", side_effect=read):
            value = fx.collector.collect(retire=True)
        self.assertEqual((value["state"], value["retired"]), ("held", False), value)
        self.assertEqual(fx.store.retire_calls, [])
        return value

    def test_every_write_boundary_resumes_after_clock_advance_and_retires_once(self):
        names = ("report.json", "durable.json", "status.json", "results.zip", "controller.json", "summary.html", "manifest.json")
        for name in names:
            for committed in (False, True):
                with self.subTest(name=name, committed=committed):
                    fx = Fixture(self.directory.name)
                    self.interrupt(fx, name, committed=committed)
                    retained = copy.deepcopy(fx.s3.objects)
                    fx.finish += timedelta(minutes=3)
                    value = fx.collector.collect(retire=True)
                    self.assertEqual((value["state"], value["retired"]), ("success", True), value)
                    self.assertEqual(len(fx.store.retire_calls), 1)
                    self.assertEqual(fx.s3.writes[0]["Key"], fx.collector.prefix + "report.json")
                    for key, versions in retained.items():
                        self.assertEqual(fx.s3.objects[key], versions)
                    self.assertTrue(all(len(versions) == 1 for versions in fx.s3.objects.values()))
                    writes = len(fx.s3.writes)
                    self.assertEqual(fx.collector.collect(retire=True), value)
                    self.assertEqual(len(fx.s3.writes), writes)
                    self.assertEqual(len(fx.store.retire_calls), 1)

    def test_competing_collectors_converge_on_one_checkpoint_and_retirement(self):
        for rival_first in (False, True):
            with self.subTest(rival_first=rival_first):
                fx = Fixture(self.directory.name)
                rival = collection.Collector(fx.service, fx.collector.machine, fx.reader,
                    temp_root=self.directory.name, now=lambda: fx.finish + timedelta(minutes=1))
                original = fx.s3.put_object
                raced, observations = False, []
                def race(**args):
                    nonlocal raced
                    if not raced and args["Key"] == fx.collector.prefix + "report.json":
                        raced = True
                        if not rival_first:
                            original(**args)
                        observations.append(rival.collect(retire=True))
                        if rival_first:
                            return original(**args)
                        return {"VersionId": "1"}
                    return original(**args)
                with patch.object(fx.s3, "put_object", side_effect=race):
                    first = fx.collector.collect(retire=True)
                self.assertEqual(observations[0]["state"], "success", observations)
                self.assertEqual(first["state"], "held", first)
                self.assertEqual(fx.collector.collect(retire=True)["state"], "success")
                self.assertEqual(len(fx.store.retire_calls), 1)
                self.assertTrue(all(len(versions) == 1 for versions in fx.s3.objects.values()))

    def test_manifest_failure_preserves_first_collection_time(self):
        fx = Fixture(self.directory.name)
        first = common.timestamp(fx.finish)
        self.interrupt(fx, "manifest.json")
        fx.finish += timedelta(hours=2)
        self.assertEqual(fx.collector.collect(retire=True)["state"], "success")
        report = json.loads(fx.s3.objects[fx.collector.prefix + "report.json"]["1"])
        self.assertEqual(report["public_projection"]["collected_at"], first)

    def test_retained_raw_snapshot_survives_mailbox_loss_and_new_source_version(self):
        fx = Fixture(self.directory.name)
        self.interrupt(fx, "manifest.json")
        del fx.s3.objects[fx.paths.results]
        del fx.s3.objects[fx.paths.status]
        fx.s3.seed("production/runs/" + fx.store.run_key + "/durable-receipt.json", b"replacement")
        fx.finish += timedelta(days=30)
        self.assertEqual(fx.collector.collect(retire=True)["state"], "success")
        self.assertEqual(len(fx.store.retire_calls), 1)

    def test_unretained_frozen_source_loss_or_version_change_holds(self):
        for change in (lambda fx: fx.s3.objects.pop(fx.paths.results),
                       lambda fx: fx.s3.seed(fx.paths.results, fx.s3.objects[fx.paths.results]["1"])):
            with self.subTest(change=change):
                fx = Fixture(self.directory.name)
                self.interrupt(fx, "durable.json")
                change(fx)
                fx.finish += timedelta(days=30)
                value = fx.collector.collect(retire=True)
                self.assertEqual((value["state"], value["retired"]), ("held", False), value)
                self.assertEqual(fx.store.retire_calls, [])

    def test_changed_checkpoint_or_partial_object_is_never_overwritten(self):
        mutations = [lambda r: r["collection_checkpoint"]["binding"].update(epoch="0" * 64),
            lambda r: r["collection_checkpoint"]["sources"]["results.zip"].update(key="credentials"),
            lambda r: r["collection_checkpoint"]["sources"]["results.zip"].update(sha256="0" * 64),
            lambda r: r["collection_checkpoint"]["derived"]["summary.html"].update(bytes=1),
            lambda r: r["public_projection"].update(collected_at="2099-01-01T00:00:00+00:00"),
            lambda r: r.update(benchmark_valid=False),
            lambda r: r["public_projection"]["measurements"][0].update(median=1000.0),
            lambda r: r["public_projection"]["measurements"][0].update(samples=[1000.0]),
            lambda r: r.pop("collection_checkpoint")]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                fx = Fixture(self.directory.name)
                self.interrupt(fx, "manifest.json")
                key = fx.collector.prefix + "report.json"
                report = json.loads(fx.s3.objects[key]["1"])
                mutate(report)
                fx.s3.objects[key]["1"] = collection.encoded(report)
                writes = len(fx.s3.writes)
                self.assertEqual(fx.collector.collect(retire=True)["state"], "held")
                self.assertEqual(fx.store.retire_calls, [])
                self.assertEqual(len(fx.s3.writes), writes)
        for name in ("durable.json", "status.json", "results.zip", "controller.json", "summary.html"):
            with self.subTest(name=name):
                fx = Fixture(self.directory.name)
                self.interrupt(fx, "manifest.json")
                key = fx.collector.prefix + name
                fx.s3.objects[key]["1"] = b"conflicting evidence"
                self.assertEqual(fx.collector.collect(retire=True)["state"], "held")
                self.assertEqual(fx.s3.objects[key]["1"], b"conflicting evidence")
                self.assertEqual(fx.store.retire_calls, [])

    def test_renderer_change_preserves_existing_bytes_but_holds_missing_summary(self):
        for boundary in ("summary.html", "manifest.json"):
            with self.subTest(boundary=boundary):
                fx = Fixture(self.directory.name)
                self.interrupt(fx, boundary)
                saved = copy.deepcopy(fx.s3.objects)
                with patch.object(results, "summary", return_value="changed renderer"):
                    value = fx.collector.collect(retire=True)
                self.assertEqual(value["state"], "held" if boundary == "summary.html" else "success", value)
                for key, versions in saved.items():
                    self.assertEqual(fx.s3.objects[key], versions)
                self.assertEqual(fx.collector.collect(retire=True)["state"], "success")
                self.assertEqual(len(fx.store.retire_calls), 1)

    def test_changed_verdict_holds_incomplete_checkpoint(self):
        fx = Fixture(self.directory.name)
        self.interrupt(fx, "manifest.json")
        with patch.object(results, "validate_qualification", side_effect=ValueError("changed validator")):
            self.assertEqual(fx.collector.collect(retire=True)["state"], "held")
        self.assertEqual(fx.store.retire_calls, [])
        self.assertEqual(fx.collector.collect(retire=True)["state"], "success")

    def test_legacy_partial_without_report_cannot_omit_retained_expired_archive(self):
        fx = Fixture(self.directory.name)
        fx.s3.seed(fx.collector.prefix + "results.zip", fx.s3.objects[fx.paths.results]["1"])
        del fx.s3.objects[fx.paths.results]
        del fx.s3.objects[fx.paths.status]
        fx.finish += timedelta(days=30)
        value = fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "held")
        self.assertIn("no checkpoint", value["reason"])
        self.assertEqual(fx.s3.writes, [])
        self.assertEqual(fx.store.retire_calls, [])

    def test_unexpected_retained_source_cannot_join_frozen_absence_set(self):
        fx = Fixture(self.directory.name)
        del fx.s3.objects[fx.paths.results]
        del fx.s3.objects[fx.paths.status]
        self.interrupt(fx, "manifest.json")
        fx.s3.seed(fx.collector.prefix + "results.zip", b"unexpected archive")
        value = fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "held")
        self.assertIn("unexpected retained source", value["reason"])
        self.assertEqual(fx.store.retire_calls, [])

    def test_recovery_still_requires_fresh_physical_and_revision_fences(self):
        for change in (lambda fx: setattr(fx, "live", True), lambda fx: setattr(fx, "root_present", True),
                       lambda fx: fx.execution.update(status="RUNNING")):
            fx = Fixture(self.directory.name)
            self.interrupt(fx, "manifest.json")
            change(fx)
            self.assertEqual(fx.collector.collect(retire=True)["state"], "held")
            self.assertEqual(fx.store.retire_calls, [])

    def test_completed_legacy_manifest_remains_readable_without_checkpoint(self):
        fx = Fixture(self.directory.name)
        value = fx.collector.collect()
        manifest = json.loads(fx.s3.objects[value["retention"]["key"]]["1"])
        payloads = {name: fx.s3.objects[ref["key"]][ref["version"]] for name, ref in manifest["retained"].items()}
        report = json.loads(payloads["report.json"])
        del report["collection_checkpoint"]
        payloads["report.json"] = collection.encoded(report)
        manifest["retained"]["report.json"].update(collection.Collector.fingerprint(payloads["report.json"]))
        collection.validate_retained(fx.store.record, manifest, payloads)

    def test_final_manifest_validates_checkpoint_binding_and_output_digests(self):
        fx = Fixture(self.directory.name)
        value = fx.collector.collect()
        manifest = json.loads(fx.s3.objects[value["retention"]["key"]]["1"])
        payloads = {name: fx.s3.objects[ref["key"]][ref["version"]] for name, ref in manifest["retained"].items()}
        report = json.loads(payloads["report.json"])
        report["collection_checkpoint"]["derived"]["summary.html"]["sha256"] = "0" * 64
        payloads["report.json"] = collection.encoded(report)
        manifest["retained"]["report.json"].update(collection.Collector.fingerprint(payloads["report.json"]))
        with self.assertRaisesRegex(ValueError, "retained collection checkpoint differs"):
            collection.validate_retained(fx.store.record, manifest, payloads)
