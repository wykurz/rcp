"""Credential-free public projection, safe HTML and immutable historical replay."""

from dataclasses import replace
from datetime import timedelta
import json
import io
import zipfile
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from benchmarks import operations

from . import common, production_collection as collection, production_export as export
from .common import digest, completed_status
from .production_store import AdmissionDecision
from .test_production_collection import Fixture
from .test_results import archive
from . import test_production_publication as fixtures

stored = fixtures.stored


class ExportTests(unittest.TestCase):
    setUp = fixtures.PublicationTests.setUp

    def test_successful_compact_report_preserves_verification_through_replay(self):
        with tempfile.TemporaryDirectory() as temporary:
            fx = Fixture(temporary)
            old = fx.store.record
            decision = AdmissionDecision(old.run_key, old.epoch, old.fingerprint, "v1", 1, 3, 2, 1,
                "2026-10", "2026-10-22", old.admission["initial_payload"])
            fx.store.record = replace(old, admission=json.loads(decision.immutable_json()))
            with zipfile.ZipFile(io.BytesIO(fx.s3.objects[fx.paths.results]["1"])) as original:
                worker = json.loads(original.read("worker.json"))
                measurement = original.read(export.results.RESULT)
            worker.update({"detail" + str(i): "x" * 9000 for i in range(100)})
            prefix = "benchmark/" + "/".join(["n" * 190] * 5) + "/"
            raw = archive([("worker.json", collection.encoded(worker)), (export.results.RESULT, measurement)] +
                [(prefix + str(i).zfill(4) + "x" * 180, b"") for i in range(999)])
            self.assertLess(len(raw), 4_000_000)
            fx.s3.seed(fx.paths.results, raw)
            fx.s3.seed(fx.paths.status, collection.encoded(dict(completed_status("123-1", True), sha256=digest(raw))))
            result = fx.collector.collect(retire=True)
            self.assertEqual((result["state"], result["retired"]), ("success", True), result)
            report = json.loads(fx.s3.objects[fx.collector.prefix + "report.json"]["1"])
            self.assertEqual(report["presentation"]["report"], "bounded-diagnostic")
            self.item = stored(fx.store.record)
            self.reader.s3 = fixtures.ReadOnlyS3(fx.s3)
            self.reader.objects = collection.Objects(self.reader.s3, "bucket")
            bundle = self.reader.download(result["retention"]["key"])
            output = Path(temporary) / "public"
            projected = export.stage(bundle, output)
            self.assertTrue(projected["benchmark_valid"])
            self.assertEqual(projected["verification"], report["evidence"]["expected_verification"])
            with patch.object(export, "project", side_effect=AssertionError("historical replay")):
                self.assertEqual(export.stage(bundle, output), projected)

    def test_frozen_v1_record_survives_current_exporter_and_series_changes(self):
        for name in ("production-public-report-v1.json", "production-public-report-v1-failed.json"):
            with self.subTest(fixture=name):
                path = Path(__file__).with_name("fixtures") / name
                raw = path.read_bytes()
                value = json.loads(raw)
                self.assertEqual(value["schema"], "rcp-production-public-report-v1")
                with patch.object(export, "SCHEMA", "future-public-report-v2"), \
                        patch.object(export, "VALIDATION", "future wording"), \
                        patch.object(export.results, "SERIES_CONTRACT", "future-series-v2"):
                    self.assertEqual(export.validate_public(value), value)
                    self.assertIn("Metadata/size only", export.render(value))
                self.assertEqual(path.read_bytes(), raw)

    def test_retired_projection_uses_recorded_verdict_not_current_admission(self):
        bundle = self.reader.download(self.key)
        original = export.project(bundle)
        future_plan = dict(common.DURABLE_PLAN, work_seconds=common.DURABLE_PLAN["work_seconds"] + 10)
        with patch.object(export.results, "approve", side_effect=AssertionError("new-run-only gate")), \
                patch.object(common, "DURABLE_PLAN", future_plan), \
                patch.object(export.results, "verification_contract", return_value={"future": "contract"}), \
                patch.object(export.results, "SERIES_CONTRACT", "future-series-v2"), \
                patch.object(operations, "CONTRACT_REVISION", "future-operation-contract"), \
                patch.object(operations, "SUMMARY_LOCALE", "future-locale"), \
                patch.object(export.results, "inspect", side_effect=AssertionError("retired projection must not reparse")):
            self.assertEqual(export.project(bundle), original)

    def test_project_reproduces_frozen_v1_success_and_failure_records(self):
        # the projection boundary receives an authenticated bundle; its retirement
        # digest is supplied independently here to keep the original golden records
        # unchanged when collector diagnostics or retained envelope bytes evolve.
        for failed in (False, True):
            with self.subTest(failed=failed):
                name = "production-public-report-v1" + ("-failed" if failed else "") + ".json"
                golden = json.loads((Path(__file__).with_name("fixtures") / name).read_bytes())
                bundle = self.reader.download(self.key)
                manifest = json.loads(bundle.manifest_bytes)
                report = json.loads(bundle.payloads["report.json"])
                if failed:
                    manifest["benchmark_valid"] = False
                    report["benchmark_valid"] = False
                    report["public_projection"] = export.projection.freeze(bundle.run_key, manifest["identity"],
                        bundle.record.state["request"]["pins"], False, report["evidence"], None)
                value = replace(bundle, record=replace(bundle.record, retirement_sha256=golden["retirement_sha256"]),
                    payloads=dict(bundle.payloads, **{"report.json": collection.encoded(report)}))
                with patch.object(type(value), "validate", return_value=manifest):
                    self.assertEqual(export.project(value), golden)

    def test_projection_has_explicit_validation_and_only_public_fields(self):
        bundle = self.reader.download(self.key)
        value = export.validate_public(export.project(bundle))
        self.assertTrue(value["benchmark_valid"])
        self.assertEqual(value["validation"], export.VALIDATION)
        self.assertEqual(len(value["measurements"]), 2)
        self.assertNotIn("admission", value)
        self.assertNotIn("state", value)
        self.assertNotIn("arn:aws", json.dumps(value))
        self.assertNotIn("bucket", json.dumps(value))
        self.assertIn("Metadata/size", export.render(value))

    def test_unavailable_public_record_uses_explicit_v2_unknown_outcome(self):
        bundle = self.reader.download(self.key)
        manifest = bundle.validate()
        report = json.loads(bundle.payloads["report.json"])
        frozen = export.projection.freeze(bundle.run_key, manifest["identity"], bundle.record.state["request"]["pins"],
            False, report["evidence"], None, outcome="evidence-unavailable")
        value = export.projection.bind(frozen, bundle.record.retirement_sha256)
        self.assertEqual(value["schema"], "rcp-production-public-report-v2")
        self.assertEqual(value["qualification_outcome"], "evidence-unavailable")
        html = export.render(value)
        self.assertIn("unavailable; workload outcome unknown", html)
        self.assertNotIn("<td>failed</td>", html)
        changed = dict(value, benchmark_valid=True)
        with self.assertRaisesRegex(ValueError, "qualification outcome"):
            export.validate_public(changed)
        changed = dict(value, schema="rcp-production-public-report-v1")
        with self.assertRaisesRegex(ValueError, "unexpected"):
            export.validate_public(changed)

    def test_unavailable_retention_exports_without_reinterpreting_historical_projection(self):
        with tempfile.TemporaryDirectory() as temporary:
            fx = Fixture(temporary)
            old = fx.store.record
            decision = AdmissionDecision(old.run_key, old.epoch, old.fingerprint, "v1", 1, 3, 2, 1,
                "2026-10", "2026-10-22", old.admission["initial_payload"])
            fx.store.record = replace(old, admission=json.loads(decision.immutable_json()))
            del fx.s3.objects[fx.paths.status]
            del fx.s3.objects[fx.paths.results]
            fx.finish = common.utc(old.state["binding"]["deadline"]) + timedelta(seconds=collection.EVIDENCE_RECOVERY_SECONDS)
            result = fx.collector.collect(retire=True)
            self.assertEqual((result["state"], result["retired"]), ("unavailable", True), result)
            self.item = stored(fx.store.record)
            self.reader.s3 = fixtures.ReadOnlyS3(fx.s3)
            self.reader.objects = collection.Objects(self.reader.s3, "bucket")
            bundle = self.reader.download(result["retention"]["key"])
            with patch.object(export.results, "inspect", side_effect=AssertionError("historical parser")), \
                    patch.object(export.projection, "freeze", side_effect=AssertionError("historical projection")):
                value = export.stage(bundle, Path(temporary) / "public")
            self.assertEqual(value["qualification_outcome"], "evidence-unavailable")
            self.assertEqual(value["measurements"], [])
            self.assertIn("workload outcome unknown", export.render(value))
            self.assertIn(b"Workload success or failure is unknown", bundle.payloads["summary.html"])

    def test_retained_html_is_never_the_public_html(self):
        bundle = self.reader.download(self.key)
        payloads = dict(bundle.payloads, **{"summary.html": b"<script>UNTRUSTED_RETAINED_HTML</script>"})
        manifest = json.loads(bundle.manifest_bytes)
        manifest["retained"]["summary.html"].update(sha256=digest(payloads["summary.html"]), bytes=len(payloads["summary.html"]))
        raw = collection.encoded(manifest)
        record = replace(bundle.record, retirement_sha256=digest(raw))
        changed = replace(bundle, payloads=payloads, record=record, record_item=stored(record), manifest_bytes=raw,
            manifest_ref=dict(bundle.manifest_ref, sha256=digest(raw), bytes=len(raw)))
        html = export.render(export.project(changed))
        self.assertNotIn("UNTRUSTED_RETAINED_HTML", html)
        self.assertNotIn("<script", html)

    def test_renderer_escapes_fields_and_rejects_untrusted_link(self):
        value = export.project(self.reader.download(self.key))
        value["identity"]["suite"] = "<script>name</script>"
        value["note"] = "<img src=x onerror=alert(1)>"
        html = export.render(value)
        self.assertIn("&lt;script&gt;name&lt;/script&gt;", html)
        self.assertIn("&lt;img", html)
        self.assertNotIn("<script", html)
        value["identity"]["run_url"] = "javascript:alert(1)"
        with self.assertRaises(ValueError):
            export.render(value)

    def test_replay_keeps_saved_bytes_despite_current_renderer_changes(self):
        bundle = self.reader.download(self.key)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            original = export.stage(bundle, root)
            path = root / (bundle.run_key + ".json")
            saved = path.read_bytes()
            with patch.object(export, "project", side_effect=AssertionError("historical replay must not reproject")):
                self.assertEqual(export.stage(bundle, root), original)
            self.assertEqual(path.read_bytes(), saved)
            self.assertEqual(list(root.iterdir()), [path])

    def test_conflicting_history_digest_is_rejected_without_overwrite(self):
        bundle = self.reader.download(self.key)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            export.stage(bundle, root)
            path = root / (bundle.run_key + ".json")
            saved = json.loads(path.read_bytes())
            saved["retirement_sha256"] = "0" * 64
            raw = collection.encoded(saved)
            path.write_bytes(raw)
            with self.assertRaisesRegex(ValueError, "conflicting"):
                export.stage(bundle, root)
            self.assertEqual(path.read_bytes(), raw)

    def test_replay_rejects_source_relabeling_without_reparsing(self):
        bundle = self.reader.download(self.key)
        for field in ("candidate", "comparison", "verification"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                saved = export.stage(bundle, root)
                if field == "candidate":
                    saved["identity"]["candidate"] = "b" * 40
                elif field == "comparison":
                    saved["comparison"] = "different comparison"
                else:
                    saved["verification"] = {"policy": "different"}
                path = root / (bundle.run_key + ".json")
                path.write_bytes(collection.encoded(saved))
                with patch.object(export, "project", side_effect=AssertionError("must not reparse")):
                    with self.assertRaisesRegex(ValueError, "conflicting"):
                        export.stage(bundle, root)

    def test_public_schema_rejects_extra_missing_and_unbounded_fields(self):
        original = export.project(self.reader.download(self.key))
        for change in (dict(original, admission={"private": "data"}), dict(original, note="&" * 900000),
                       {key: value for key, value in original.items() if key != "comparison"}):
            with self.assertRaises(ValueError):
                export.validate_public(change)
        with patch.object(export, "MAX_PUBLIC_HTML", 10):
            with self.assertRaisesRegex(ValueError, "HTML exceeds"):
                export.render(original)

    def test_symlink_history_record_is_rejected(self):
        bundle = self.reader.download(self.key)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "other.json"
            target.write_bytes(collection.encoded(export.project(bundle)))
            (root / (bundle.run_key + ".json")).symlink_to(target)
            with self.assertRaises(OSError):
                export.stage(bundle, root)

    def test_invalid_measurements_cannot_render_as_success(self):
        value = export.project(self.reader.download(self.key))
        value["measurements"][0]["median"] = float("inf")
        with self.assertRaises(ValueError):
            export.render(value)

    def test_failed_projection_has_no_performance_data(self):
        # the retained collector verdict is authenticated; failed records never call approve
        bundle = self.reader.download(self.key)
        manifest = json.loads(bundle.manifest_bytes)
        manifest["benchmark_valid"] = False
        report = json.loads(bundle.payloads["report.json"])
        report["benchmark_valid"] = False
        report["qualification_outcome"] = "failed"
        report["public_projection"] = export.projection.freeze(bundle.run_key, manifest["identity"],
            bundle.record.state["request"]["pins"], False, report["evidence"], None)
        payloads = dict(bundle.payloads, **{"report.json": collection.encoded(report)})
        manifest["retained"]["report.json"].update(sha256=digest(payloads["report.json"]), bytes=len(payloads["report.json"]))
        raw = collection.encoded(manifest)
        record = replace(bundle.record, retirement_sha256=digest(raw))
        failed = replace(bundle, record=record, record_item=stored(record), manifest_bytes=raw, payloads=payloads,
            manifest_ref=dict(bundle.manifest_ref, sha256=digest(raw), bytes=len(raw)))
        with patch.object(export.results, "approve", side_effect=AssertionError("failed result must not enter history")):
            value = export.project(failed)
        self.assertFalse(value["benchmark_valid"])
        self.assertEqual(value["measurements"], [])
        self.assertIn("excluded", value["note"])


if __name__ == "__main__":
    unittest.main()
