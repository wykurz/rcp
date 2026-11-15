"""A retired matrix run reaches the publisher: its frozen record, its cells and its copy diagnostics."""

import copy
from dataclasses import replace
import json
import tempfile
import unittest
from unittest.mock import Mock, patch

from . import production_collection as collection, production_publication as publication
from .production_store import AdmissionDecision
from .test_matrix_results import matrix_record
from .test_production_collection import Fixture
from .test_production_publication import ReadOnlyS3, stored

SUITE = "matrix-commission-local"
FAILED = dict(status="failed", stage="copy",
              message="copy 1 failed with exit code 3; see 0.stdout.log and 0.stderr.log in logs/commission-10k/local/1")


class MatrixPublicationTests(unittest.TestCase):
    def setUp(self):
        self.network = patch("socket.create_connection", side_effect=AssertionError("offline only"))
        self.network.start()
        self.addCleanup(self.network.stop)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.fx = Fixture(temporary.name, suite=SUITE, record=matrix_record(SUITE, {1: FAILED}))
        old = self.fx.store.record
        decision = AdmissionDecision(old.run_key, old.epoch, old.fingerprint, "reviewed-v1", 20_000_000,
            350_000_000, 50_000_000, 1, "2026-10", "2026-10-22", old.admission["initial_payload"])
        self.fx.store.record = replace(old, admission=json.loads(decision.immutable_json()))
        result = self.fx.collector.collect(retire=True)
        self.assertEqual((result["state"], result["retired"]), ("success", True), result)
        self.key = result["retention"]["key"]
        self.item = stored(self.fx.store.record)
        database = Mock(spec=["get_item"])
        database.get_item.side_effect = lambda **request: {"Item": copy.deepcopy(self.item)}
        self.reader = publication.Reader(ReadOnlyS3(self.fx.s3), database, bucket="bucket", table="runs",
                                         repository_id="123")

    def rewrite(self, change):
        """A collector-owned envelope whose report was changed, with every digest made consistent."""
        prefix = self.key.removesuffix("manifest.json")
        manifest = json.loads(self.fx.s3.objects[self.key]["1"])
        report = json.loads(self.fx.s3.objects[prefix + "report.json"]["1"])
        change(report)
        raw = collection.encoded(report)
        self.fx.s3.objects[prefix + "report.json"]["1"] = raw
        manifest["retained"]["report.json"].update(sha256=collection.digest(raw), bytes=len(raw))
        raw = collection.encoded(manifest)
        self.fx.s3.objects[self.key]["1"] = raw
        self.item["retirement_sha256"] = {"S": collection.digest(raw)}

    def test_the_published_record_lists_every_cell_and_measures_the_completed_one(self):
        value = self.reader.public_record(self.key)
        self.assertEqual(value["schema"], "rcp-production-public-report-v4")
        self.assertEqual([(cell["case_id"], cell["outcome"]) for cell in value["cells"]],
                         [("commission-64m", "complete"), ("commission-10k", "failed")])
        self.assertEqual(value["cells"][1]["message"], FAILED["message"])
        self.assertEqual([item["case_id"] for item in value["measurements"]], ["commission-64m"])
        # the full export and the public-only read agree
        from .production_export import project
        self.assertEqual(project(self.reader.download(self.key)), value)

    def test_cells_that_differ_from_the_collected_evidence_are_not_published(self):
        def change(report):
            report["public_projection"]["cells"][1]["message"] = "something the collector never recorded"
        self.rewrite(change)
        with self.assertRaisesRegex(ValueError, "retained public projection provenance differs"):
            self.reader.public_record(self.key)
        with self.assertRaises(ValueError):
            self.reader.download(self.key).validate()

    def test_copy_diagnostics_come_from_the_retained_archive(self):
        value = self.reader.public_record(self.key)
        extra, consumed = self.reader.public_diagnostics(self.key, value, maximum_bytes=64 * 1024 * 1024)
        self.assertEqual((extra["schema"], extra["status"]), ("rcp-public-resource-diagnostics-v2", "partial"))
        self.assertEqual([(item["case_id"], item["scenario"], item["status"]) for item in extra["trials"]],
            [("commission-64m", "local", "complete")] * 3 + [("commission-10k", "local", "unavailable")])
        self.assertGreater(consumed, 0)
        # without the budget to read the archive, the same record still gets a companion of its version
        fallback, _ = self.reader.public_diagnostics(self.key, value, maximum_bytes=0)
        self.assertEqual((fallback["schema"], fallback["reason"]), ("rcp-public-resource-diagnostics-v2", "raw_budget"))


if __name__ == "__main__":
    unittest.main()
