"""Regression boundaries for retained receipts, archives and immutable offline staging."""

import copy
import io
import json
import os
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from infra.aws_benchmark import common, results
from infra.aws_benchmark.test_results import archive, binding, bundle, measurement


class ResultBoundaryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "raw"
        self.expected, self.policy = bundle(self.source)

    def ingest(self, name="report", **kwargs):
        return results.ingest(self.source, self.root / name, self.expected, "success", **kwargs)

    def receipt(self):
        return json.loads((self.source / "controller.json").read_bytes())

    def entries(self):
        with zipfile.ZipFile(self.source / "results.zip") as source:
            return {member.filename: source.read(member) for member in source.infolist()}

    def rewrite(self, *, receipt=None, status=None, entries=None):
        receipt = self.receipt() if receipt is None else receipt
        status = json.loads((self.source / "status.json").read_bytes()) if status is None else status
        data = (self.source / "results.zip").read_bytes() if entries is None else archive(entries.items())
        status["sha256"] = receipt["archive_sha256"] = common.digest(data)
        status_data = results.encoded(status)
        receipt["status_sha256"] = common.digest(status_data)
        for name, payload in (("controller.json", results.encoded(receipt)), ("status.json", status_data), ("results.zip", data)):
            (self.source / name).write_bytes(payload)
        return dict(self.policy, archive_sha256=common.digest(data))

    def test_large_ascii_expanded_record_replays_after_link_cleanup_interruption(self):
        raw = measurement(self.expected)
        raw["context"]["source"] = {str(index): "é" * 16384 for index in range(180)}
        payload = json.dumps(raw, ensure_ascii=False, separators=(",", ":")).encode()
        self.assertLess(len(payload), results.MAX_JSON)
        entries = self.entries()
        entries[results.RESULT] = payload
        policy = self.rewrite(entries=entries)
        history = self.root / "history"
        evidence = self.ingest(policy=policy, history=history)
        self.assertEqual(evidence["errors"], [])
        self.assertTrue(evidence["history_eligible"])
        committed = next((history / "runs").glob("*.json"))
        before = committed.read_bytes()
        self.assertGreater(len(before), results.MAX_JSON * 2)
        os.link(committed, history / "runs/.aws-stage-interrupted")
        replay = self.ingest("replay", policy=policy, history=history)
        self.assertEqual(replay["errors"], [])
        self.assertTrue(replay["history_eligible"])
        self.assertEqual(committed.read_bytes(), before)
        self.assertEqual((self.root / "replay/derived.json").read_bytes(), before)
        committed.write_bytes(before + b" ")
        refused = self.ingest("larger-existing", policy=policy, history=history)
        self.assertFalse(refused["history_eligible"])
        self.assertEqual(refused["errors"], ["history staging: input must be a bounded, singly linked regular file"])
        self.assertEqual(committed.read_bytes(), before + b" ")

    def test_historical_receipts_do_not_reapply_current_duration_or_binary_admission(self):
        with patch.object(common, "MIN_WINDOW", 2100), patch.object(common, "MAX_WINDOW", 600), patch.object(common, "BINARIES", ("candidate/new-tool",)):
            evidence = self.ingest()
        self.assertEqual(evidence["errors"], [])
        self.assertIsNotNone(evidence["worker"])
        self.assertTrue((self.root / "report/dashboard/index.html").is_file())
        receipt = self.receipt()
        receipt["request"]["sha256"]["legacy/auxiliary"] = "9" * 64
        entries = self.entries()
        metadata = json.loads(entries["worker.json"])
        metadata["request"] = copy.deepcopy(receipt["request"])
        entries["worker.json"] = results.encoded(metadata)
        self.rewrite(receipt=receipt, entries=entries)
        replay = self.ingest("historical-binary-map")
        self.assertEqual(replay["errors"], [])
        self.assertEqual(replay["worker"]["request"], receipt["request"])

    def test_window_and_request_refusals_have_fixed_specific_diagnostics(self):
        original = self.receipt()
        mutations = (
            (lambda value: value["window"].update(extra=True), "unexpected window fields"),
            (lambda value: value["window"].update(run="bad"), "window identity mismatch"),
            (lambda value: value["window"].update(candidate="bad"), "window identity mismatch"),
            (lambda value: value["window"].update(suite="bad"), "window identity mismatch"),
            (lambda value: value["window"].update(opens=123), "window fields must be strings"),
            (lambda value: value["window"].update(opens="2026-10-19T10:00:00"), "timestamp requires an explicit timezone"),
            (lambda value: value["window"].update(opens="FORGED timestamp"), "invalid window timestamp"),
            (lambda value: value["window"].update(opens=value["window"]["deadline"]), "window must open before its deadline"),
            (lambda value: value["request"].update(extra=True), "unexpected request fields"),
            (lambda value: value["request"].update(run="124-1"), "mailbox request does not match the operator window"),
            (lambda value: value["request"].update(sha256=[]), "unexpected binary digest map"),
            (lambda value: value["request"]["sha256"].update({"candidate/rcp": "FORGED digest"}), "binary digests must be lowercase SHA-256"),
        )
        for index, (mutate, reason) in enumerate(mutations):
            with self.subTest(reason=reason):
                receipt = copy.deepcopy(original)
                mutate(receipt)
                (self.source / "controller.json").write_bytes(results.encoded(receipt))
                evidence = self.ingest(str(index))
                self.assertEqual(evidence["errors"], ["controller window/request: " + reason])
                self.assertIsNotNone(evidence["controller"])
                self.assertNotIn("FORGED", (self.root / str(index) / "summary.md").read_text())

    def test_bound_terminal_status_survives_missing_archive_and_protocol_failure(self):
        for index, mutation in enumerate((dict(success=False, upload_error={"type": "ConnectionError"}),
                dict(state="legacy-completed"), dict(success=True))):
            self.expected, self.policy = bundle(self.source)
            status = json.loads((self.source / "status.json").read_bytes())
            status.update(mutation)
            self.rewrite(status=status)
            (self.source / "results.zip").unlink()
            evidence = self.ingest(str(index))
            self.assertEqual(evidence["status"], status)
            self.assertEqual(evidence["controller"], self.receipt())
            for name in ("summary.md", "index.html"):
                page = (self.root / str(index) / name).read_text()
                self.assertIn("Worker state", page)
                self.assertIn(results.literal(status["state"]), page)
                self.assertIn(results.literal(status["success"]), page)
                if index == 0:
                    self.assertIn("Worker upload error", page)
                    self.assertIn("ConnectionError", page)
        self.expected, self.policy = bundle(self.source)
        status = json.loads((self.source / "status.json").read_bytes())
        status.update(error="FORGED unbound status")
        (self.source / "status.json").write_bytes(results.encoded(status))
        evidence = self.ingest("unbound")
        self.assertIsNone(evidence["status"])
        self.assertEqual(evidence["errors"], ["terminal status: status digest mismatch"])
        self.assertNotIn("FORGED", (self.root / "unbound/summary.md").read_text())

    def test_archive_digest_binding_refuses_valid_but_swapped_archive(self):
        receipt = self.receipt()
        entries = self.entries()
        entries["benchmark/additional.log"] = b"structurally valid replacement"
        self.rewrite(entries=entries)
        changed_receipt = self.receipt()
        changed_receipt["archive_sha256"] = receipt["archive_sha256"]
        (self.source / "controller.json").write_bytes(results.encoded(changed_receipt))
        evidence = self.ingest()
        self.assertEqual(evidence["errors"], ["bundle integrity: bundle digest mismatch"])
        self.assertIsNone(evidence["worker"])
        changed_receipt.pop("archive_sha256")
        (self.source / "controller.json").write_bytes(results.encoded(changed_receipt))
        missing = self.ingest("missing-digest")
        self.assertEqual(missing["errors"], ["bundle integrity: controller archive digest unavailable"])

    def test_instance_type_and_smoke_suite_independently_refuse_history(self):
        receipt = self.receipt()
        receipt["instance"]["instance_type"] = "c5d.12xlarge"
        (self.source / "controller.json").write_bytes(results.encoded(receipt))
        evidence = self.ingest(policy=self.policy)
        self.assertEqual(evidence["errors"], ["history approval: unverified execution provenance"])
        self.assertFalse(evidence["history_eligible"])
        self.expected, self.policy = bundle(self.source, binding("aa-smoke"))
        evidence = self.ingest("smoke", policy=self.policy)
        self.assertEqual(evidence["errors"], ["history approval: diagnostic run cannot enter performance history"])
        self.assertFalse(evidence["history_eligible"])

    def test_failed_bundle_history_request_keeps_diagnostic_outputs(self):
        (self.source / "results.zip").unlink()
        evidence = self.ingest(policy=self.policy, history=self.root / "history")
        self.assertIn("history staging: history staging refused", evidence["errors"])
        self.assertFalse(evidence["history_eligible"])
        self.assertEqual(json.loads((self.root / "report/provenance.json").read_bytes()), evidence)
        for name in ("summary.md", "index.html"):
            page = (self.root / "report" / name).read_text()
            self.assertIn("Diagnostics:", page)
            self.assertIn("incomplete result bundle", page)

    def test_staging_failure_downgrades_every_derived_trust_claim(self):
        raw = measurement(self.expected)
        raw["revision"] = dict(commit="7" * 40, branch=None, dirty=False)
        self.expected, self.policy = bundle(self.source, self.expected, raw)
        with patch.object(results.os, "fsync", side_effect=OSError("injected disk failure")):
            evidence = self.ingest(policy=self.policy, history=self.root / "history")
        self.assertEqual(evidence["errors"], ["history staging: OSError"])
        self.assertFalse(evidence["history_eligible"])
        derived = json.loads((self.root / "report/derived.json").read_bytes())
        self.assertEqual(derived["context"]["purpose"], "diagnostic")
        self.assertFalse(derived["context"]["aws_provenance"]["history_eligible"])
        self.assertNotIn("role", derived["revision"])
        page = (self.root / "report/dashboard/changes.html").read_text()
        self.assertIn("checkout revision <code>7777777777", page)
        self.assertNotIn("candidate revision", page)
        self.assertIn("History: diagnostic only", (self.root / "report/summary.md").read_text())

    def test_render_failure_is_visible_in_both_diagnostic_entry_points(self):
        with patch.object(results.report, "render", side_effect=RuntimeError("FORGED renderer input")):
            evidence = self.ingest()
        self.assertEqual(evidence["errors"], ["dashboard rendering: RuntimeError"])
        self.assertEqual(json.loads((self.root / "report/provenance.json").read_bytes()), evidence)
        for name in ("summary.md", "index.html"):
            page = (self.root / "report" / name).read_text()
            self.assertIn("Diagnostics:", page)
            self.assertIn("dashboard rendering: RuntimeError", page)
            self.assertNotIn("FORGED", page)
            self.assertNotIn('href="dashboard/index.html"', page)

    def test_image_and_baked_source_each_separate_approved_series(self):
        observed = []
        for index, changed in enumerate((None, "image", "source")):
            self.expected, self.policy = bundle(self.source)
            receipt, entries = self.receipt(), self.entries()
            metadata = json.loads(entries["worker.json"])
            if changed == "image":
                receipt["instance"]["image_id"] = "ami-" + "2" * 17
            if changed == "source":
                metadata["harness_revision"] = "8" * 40
            entries["worker.json"] = results.encoded(metadata)
            policy = self.rewrite(receipt=receipt, entries=entries)
            policy.update(ami_id=receipt["instance"]["image_id"], harness_revision=metadata["harness_revision"])
            evidence = self.ingest(str(index), policy=policy)
            self.assertEqual(evidence["errors"], [])
            self.assertTrue(evidence["history_eligible"])
            derived = json.loads((self.root / str(index) / "derived.json").read_bytes())
            self.assertEqual(derived["context"]["aws_provenance"]["source_series_ids"], ["1" * 64, "1" * 64])
            observed.append(derived["summaries"][0]["series_id"])
        self.assertEqual(len(set(observed)), 3)

    def test_cli_requires_explicit_bounded_producer_identity_before_output_creation(self):
        output = self.root / "cli"
        argv = ["results", str(self.source), "--output", str(output), "--conclusion", "success"]
        with patch("sys.argv", argv), patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit) as error:
            results.main()
        self.assertEqual(error.exception.code, 2)
        self.assertFalse(output.exists())
        path = self.root / "identity.json"
        path.write_text(json.dumps(self.expected) + " " * 4097)
        with patch("sys.argv", argv + ["--identity", str(path)]), self.assertRaisesRegex(common.ValidationError, "workflow identity exceeds its size limit"):
            results.main()
        self.assertFalse(output.exists())


class ArchiveAllocationTests(unittest.TestCase):
    def test_understated_central_directory_count_is_refused_before_zipfile_allocation(self):
        payload = bytearray(archive([("worker.json", b"{}"), ("benchmark/log", b"log")]))
        struct.pack_into("<HH", payload, len(payload) - 14, 1, 1)
        with patch.object(results.zipfile, "ZipFile") as parser:
            with self.assertRaisesRegex(results.ValidationError, "^archive entry count mismatch$"):
                results.members(bytes(payload))
            parser.assert_not_called()

    def test_member_name_limit_is_enforced_before_zipfile_allocation(self):
        payload = archive([("worker.json", b"{}"), ("benchmark/" + "x" * 2048, b"log")])
        with patch.object(results.zipfile, "ZipFile") as parser:
            with self.assertRaisesRegex(results.ValidationError, "^archive member name exceeds limit$"):
                results.members(payload)
            parser.assert_not_called()
        for name in ("benchmark/" + "/".join(["x" * 200] * 6), "benchmark/" + "/".join(["x" * 255] * 6)):
            selected, hashes, issues = results.members(archive([(name, b"log"), ("worker.json", b"{}")]))
            self.assertEqual(selected["worker.json"], b"{}")
            self.assertEqual(hashes[name], common.digest(b"log"))
            self.assertEqual(issues, ["unsupported benchmark member names"])

    def test_central_entry_cannot_select_a_local_header_hidden_inside_another_name(self):
        original = archive([("worker.json", b"{}")])
        directory = original.index(b"PK\x01\x02")
        outer = bytearray(original[:30])
        struct.pack_into("<H", outer, 26, 30 + len("worker.json"))
        payload = outer + bytearray(original)
        struct.pack_into("<L", payload, directory + 30 + 42, 30)
        struct.pack_into("<L", payload, len(payload) - 6, directory + 30)
        with zipfile.ZipFile(io.BytesIO(payload)) as permissive:
            self.assertEqual(permissive.read("worker.json"), b"{}")
        with self.assertRaisesRegex(results.ValidationError, "^non-contiguous archive records$"):
            results.members(bytes(payload))


if __name__ == "__main__":
    unittest.main()
