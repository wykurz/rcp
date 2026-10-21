"""Metadata-only records keep their proof contract and visible qualification."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

from benchmarks import changes, operations, report, run, sanitized, streaming, test_report, verification
from benchmarks.test_report import sample_run


def verified_record(policy, run_id="a" * 32, timestamp="2026-09-26T12:00:00Z", inline_bytes=None):
    record = sample_run(run_id=run_id, timestamp=timestamp)
    record["revision"]["commit"] = "c" * 40
    record["context"].update(topology="local", repository="owner/repository",
                             summary_locale="C", operation_contract_revision=operations.CONTRACT_REVISION,
                             verification=dict(policy=policy, workers=8))
    if inline_bytes is not None:
        record["context"]["verification"]["inline_bytes"] = inline_bytes
    case, variant = record["cases"][0], record["variants"][0]
    case["fixture_digest"] = "a" * 64
    counts = operations.expected_counts(case)
    entries = counts["files"] + counts["directories"] + 1
    for trial in record["trials"]:
        destination = dict(ok=True, counts=dict(counts), digest=case["fixture_digest"],
                           metadata=dict(ok=True, entries=entries, checked_fields=["mode"]))
        source = dict(ok=True, counts=dict(counts), digest=case["fixture_digest"],
                      metadata=dict(ok=True, entries=entries, checked_fields=["mode", "uid", "gid", "mtime_ns"]))
        if policy == streaming.METADATA_POLICY:
            destination["policy"] = source["policy"] = policy
        trial.update(validation=destination, source_validation=source,
                     metadata_validation=copy.deepcopy(destination["metadata"]),
                     operation="fresh", child_locale="C",
                     expected_transfer=operations.transfer_counts("fresh", counts),
                     copy_summary=operations.transfer_counts("fresh", counts))
    record["summaries"][0]["series_id"] = run.series_id(
        {key: value for key, value in case.items() if key != "fixture_digest"}, variant,
        "source-warm", "local", "runner", {}, record["tools"],
        operation_revision=operations.CONTRACT_REVISION, verification=record["context"]["verification"])
    return record


class MetadataReportingTests(unittest.TestCase):
    def test_metadata_and_full_streaming_proofs_keep_distinct_policies(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            with self.subTest(policy=policy):
                record = verified_record(policy)
                before = copy.deepcopy(record)
                self.assertEqual(report.validate_result(record), before)
        self.assertEqual(report.verification_label({"verification": dict(policy=streaming.METADATA_POLICY)}),
                         "Metadata/size only; file content integrity not checked")
        self.assertEqual(report.verification_label({"verification": dict(policy=streaming.POLICY)}),
                         "Full-content SHA-256")
        self.assertIn("Legacy", report.verification_label({}))

    def test_metadata_proofs_require_policy_counts_fingerprint_and_metadata(self):
        for field in ("validation", "source_validation"):
            for mutation in (
                    lambda proof: proof.pop("policy"),
                    lambda proof: proof.update(policy=streaming.POLICY),
                    lambda proof: proof.update(digest="b" * 64),
                    lambda proof: proof["counts"].update(files=1),
                    lambda proof: proof["counts"].update(bytes=True),
                    lambda proof: proof["metadata"].update(entries=1),
                    lambda proof: proof["metadata"].update(checked_fields=[]),
                    lambda proof: proof["metadata"].update(ok=False),
                    lambda proof: proof.pop("metadata")):
                with self.subTest(field=field, mutation=mutation):
                    record = verified_record(streaming.METADATA_POLICY)
                    mutation(record["trials"][0][field])
                    with self.assertRaises(ValueError):
                        report.validate_result(record)

    def test_streaming_destination_modes_required_without_copy_summary(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            with self.subTest(policy=policy):
                record = verified_record(policy)
                record["variants"][0]["args"] = []
                for trial in record["trials"]:
                    trial["child_locale"] = "inherited"
                    trial.pop("copy_summary")
                    trial.pop("metadata_validation")
                report.validate_result(record)
                record["trials"][0]["validation"].pop("metadata")
                with self.assertRaisesRegex(ValueError, "validation.metadata"):
                    report.validate_result(record)

    def test_streaming_contract_rejects_unsupported_cache_policies(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            for cache in ("source-warm", "linux-drop-caches", "uncontrolled", "source-verified"):
                with self.subTest(policy=policy, cache=cache):
                    record = verified_record(policy)
                    record["context"].update(cache_policy=cache, cache_contract_revision=operations.CACHE_REVISION)
                    if cache in ("source-warm", "linux-drop-caches"):
                        report.validate_result(record)
                    else:
                        with self.assertRaisesRegex(ValueError, "streaming verification contract"):
                            report.validate_result(record)

    def test_legacy_records_keep_original_cache_and_metadata_contracts(self):
        original = sample_run()
        self.assertEqual(report.validate_result(original), original)
        for cache in ("source-warm", "linux-drop-caches", "uncontrolled", "source-verified"):
            with self.subTest(cache=cache):
                record = verified_record(streaming.POLICY)
                record["context"].pop("verification")
                record["context"].update(cache_policy=cache, cache_contract_revision=operations.CACHE_REVISION)
                for trial in record["trials"]:
                    trial["validation"].pop("metadata")
                    if cache == "source-verified":
                        trial["cache_validation"] = copy.deepcopy(trial["source_validation"])
                before = copy.deepcopy(record)
                self.assertEqual(report.validate_result(record), before)
                self.assertEqual(sanitized.project_run(record, [])["trials"][0]["proofs"]["content"]["qualification"], "passed")

    def test_streaming_source_fields_preserve_new_and_historical_evidence(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            for fields in (verification.LEGACY_SOURCE_METADATA_FIELDS, verification.SOURCE_METADATA_FIELDS):
                with self.subTest(policy=policy, fields=fields):
                    record = verified_record(policy)
                    record["trials"][0]["source_validation"]["metadata"]["checked_fields"] = list(fields)
                    report.validate_result(record)
                    exported = sanitized.project_run(record, [])
                    self.assertEqual(exported["trials"][0]["proofs"]["source"]["metadata"]["checked_fields"], list(fields))

    def test_successful_streaming_trial_cannot_claim_an_incomplete_proof(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            for field in ("validation", "source_validation"):
                for marker in (dict(interrupted=True), dict(verification_error=True), dict(complete=False)):
                    with self.subTest(policy=policy, field=field, marker=marker):
                        record = verified_record(policy)
                        record["trials"][0][field].update(marker)
                        with self.assertRaisesRegex(ValueError, "not a successful proof"):
                            report.validate_result(record)

    def test_streaming_inline_threshold_is_optional_bounded_and_exported(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            for threshold in (0, verification.DEFAULT_INLINE_BYTES, verification.MAX_INLINE_BYTES):
                with self.subTest(policy=policy, threshold=threshold):
                    record = verified_record(policy, inline_bytes=threshold)
                    report.validate_result(record)
                    self.assertEqual(sanitized.project_run(record, [])["conditions"]["verification"]["inline_bytes"], threshold)
            for threshold in (None, True, -1, 1.0, "1", verification.MAX_INLINE_BYTES + 1):
                with self.subTest(policy=policy, invalid_threshold=threshold):
                    record = verified_record(policy)
                    record["context"]["verification"]["inline_bytes"] = threshold
                    with self.assertRaisesRegex(ValueError, "streaming verification contract"):
                        report.validate_result(record)
            historical = verified_record(policy)
            self.assertNotIn("inline_bytes", sanitized.project_run(historical, [])["conditions"]["verification"])

    def test_scheduling_threshold_separates_imported_history_even_with_reused_series_id(self):
        records = [verified_record(streaming.POLICY, character * 32, f"2026-09-{day}T12:00:00Z", threshold)
                   for character, day, threshold in (("a", 26, None), ("b", 27, 0), ("c", 28, verification.DEFAULT_INLINE_BYTES))]
        self.assertEqual(len({record["summaries"][0]["series_id"] for record in records}), 3)
        for record in records:
            record["summaries"][0]["series_id"] = "a" * 64
        evidence = changes.build(records, {record["run_id"]: [] for record in records})
        self.assertTrue(all(row["reference"] is None for row in evidence["changes"]))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            history = root / "input" / "runs"
            history.mkdir(parents=True)
            for record in records:
                (history / (record["run_id"] + ".json")).write_text(json.dumps(record))
            output = root / "output"
            report.render(history.parent, output)
            state = test_report.ReportTests().dashboard_state(output)
            self.assertEqual(state["points"], 3)
            self.assertEqual(state["lines"], 0)

    def test_metadata_cannot_be_relabelled_as_full_or_legacy(self):
        for policy in (streaming.POLICY, None):
            record = verified_record(streaming.METADATA_POLICY)
            if policy is None:
                record["context"].pop("verification")
            else:
                record["context"]["verification"]["policy"] = policy
            with self.subTest(policy=policy), self.assertRaisesRegex(ValueError, "policy differs"):
                report.validate_result(record)

    def test_failed_metadata_trial_remains_failed_without_summaries(self):
        record = verified_record(streaming.METADATA_POLICY)
        record.update(status="failed", summaries=[])
        record["trials"] = record["trials"][:1]
        record["trials"][0].update(status="failed", source_validation=dict(
            ok=False, interrupted=True, error="benchmark interrupted by signal 15"))
        before = copy.deepcopy(record)
        report.validate_result(record)
        self.assertEqual(record, before)
        self.assertFalse(record["summaries"])
        record["status"] = "complete"
        with self.assertRaises(ValueError):
            report.validate_result(record)

    def test_mismatched_failed_proof_policy_is_rejected(self):
        record = verified_record(streaming.METADATA_POLICY)
        record.update(status="failed", summaries=[])
        record["trials"][0].update(status="failed", validation=dict(ok=False, policy=streaming.POLICY))
        with self.assertRaisesRegex(ValueError, "policy differs"):
            report.validate_result(record)

    def test_sanitized_metadata_export_never_admits_content_proof(self):
        record = verified_record(streaming.METADATA_POLICY)
        record["context"]["verification_policy"] = "private misleading content claim"
        exported = sanitized.project_run(record, [])
        self.assertNotIn("private misleading", json.dumps(exported))
        self.assertEqual(exported["conditions"]["verification"]["policy"], streaming.METADATA_POLICY)
        self.assertIn("file content integrity not checked", exported["conditions"]["verification"]["label"])
        self.assertIn("metadata fingerprint", exported["cases"][0]["fixture_digest_qualification"])
        for trial in exported["trials"]:
            self.assertEqual(trial["proofs"]["content"]["qualification"], "not-checked")
            self.assertIsNone(trial["proofs"]["content"]["digest"])
            for field in ("structure", "source"):
                self.assertEqual(trial["proofs"][field]["qualification"], "passed")
                self.assertEqual(trial["proofs"][field]["policy"], streaming.METADATA_POLICY)
                self.assertIn("metadata fingerprint", trial["proofs"][field]["digest_qualification"])
            self.assertEqual(trial["proofs"]["metadata"]["qualification"], "passed")

    def test_sanitized_full_export_retains_content_proof(self):
        record = verified_record(streaming.POLICY)
        exported = sanitized.project_run(record, [])
        self.assertEqual(exported["conditions"]["verification"]["label"], "Full-content SHA-256")
        for trial in exported["trials"]:
            self.assertEqual(trial["proofs"]["content"]["qualification"], "passed")
            self.assertNotIn("structure", trial["proofs"])
        record["context"].pop("verification")
        self.assertNotIn("verification", sanitized.project_run(record, [])["conditions"])

    def test_sanitized_source_interruption_keeps_completed_destination_evidence(self):
        record = verified_record(streaming.METADATA_POLICY)
        record.update(status="failed", summaries=[])
        record["trials"] = record["trials"][:1]
        record["trials"][0].update(status="failed", source_validation=dict(ok=False, interrupted=True))
        exported = sanitized.project_run(record, [])
        self.assertEqual(exported["status"], "failed")
        self.assertFalse(exported["summaries"])
        proofs = exported["trials"][0]["proofs"]
        self.assertEqual(proofs["content"]["qualification"], "not-checked")
        self.assertEqual(proofs["structure"]["qualification"], "recorded-unqualified")
        self.assertTrue(proofs["structure"]["recorded_ok"])
        self.assertEqual(proofs["source"]["qualification"], "interrupted")
        self.assertTrue(proofs["source"]["interrupted"])

    def test_export_distinguishes_interrupted_failed_scan_and_completed_mismatch(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            for field in ("validation", "source_validation"):
                for qualification, marker in (
                        ("interrupted", dict(interrupted=True, complete=False)),
                        ("verification-error", dict(verification_error=True, complete=False)),
                        ("failed", dict(digest_match=False, modes_match=True, complete=True))):
                    with self.subTest(policy=policy, field=field, qualification=qualification):
                        record = verified_record(policy)
                        record.update(status="failed", summaries=[])
                        record["trials"] = record["trials"][:1]
                        trial = record["trials"][0]
                        trial["status"] = "failed"
                        trial[field] = dict(ok=False, policy=policy, error="private fixture path",
                            counts=dict(files=1, directories=2, bytes=3), digest="b" * 64,
                            modes_digest="c" * 64, metadata_digest="d" * 64, entries=4, **marker)
                        if field == "validation":
                            trial.pop("source_validation")
                            trial.pop("metadata_validation")
                        before = copy.deepcopy(record)
                        exported = sanitized.project_run(record, [])
                        proofs = exported["trials"][0]["proofs"]
                        destination = "structure" if policy == streaming.METADATA_POLICY else "content"
                        failed = proofs[destination if field == "validation" else "source"]
                        self.assertEqual(failed["qualification"], qualification)
                        self.assertFalse(failed["recorded_ok"])
                        for key, value in marker.items():
                            self.assertEqual(failed[key], value)
                        for key in ("counts", "digest", "modes_digest", "metadata_digest", "entries"):
                            self.assertEqual(failed[key], trial[field][key])
                        if field == "source_validation":
                            self.assertEqual(proofs[destination]["qualification"], "recorded-unqualified")
                            self.assertTrue(proofs[destination]["recorded_ok"])
                        if policy == streaming.METADATA_POLICY:
                            self.assertEqual(proofs["content"]["qualification"], "not-checked")
                        self.assertEqual(record, before)
                        self.assertNotIn("private fixture path", json.dumps(exported))

    def test_legacy_interrupted_proofs_retain_their_qualification(self):
        for field in ("validation", "source_validation"):
            with self.subTest(field=field):
                record = verified_record(streaming.POLICY)
                record["context"].pop("verification")
                record.update(status="failed", summaries=[])
                record["trials"] = record["trials"][:1]
                record["trials"][0].update(status="failed")
                record["trials"][0][field] = dict(ok=False, interrupted=True)
                proofs = sanitized.project_run(record, [])["trials"][0]["proofs"]
                self.assertEqual(proofs["content" if field == "validation" else "source"]["qualification"], "interrupted")

    def test_history_never_pools_full_and_metadata_policies_and_discloses_both(self):
        full = verified_record(streaming.POLICY)
        metadata = verified_record(streaming.METADATA_POLICY, "b" * 32, "2026-09-27T12:00:00Z")
        self.assertNotEqual(full["summaries"][0]["series_id"], metadata["summaries"][0]["series_id"])
        # an imported, stale or wrongly reused series ID cannot override the explicit policy
        metadata["summaries"][0]["series_id"] = full["summaries"][0]["series_id"]
        evidence = changes.build([full, metadata], {item["run_id"]: [] for item in (full, metadata)})
        self.assertTrue(all(row["reference"] is None for row in evidence["changes"]))
        for renderer in (changes.markdown, changes.html_report):
            rendered = renderer(evidence)
            self.assertIn("Metadata/size only; file content integrity not checked", rendered)
            self.assertIn("Full-content SHA-256", rendered)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            history = root / "input" / "runs"
            history.mkdir(parents=True)
            for record in (full, metadata):
                (history / (record["run_id"] + ".json")).write_text(json.dumps(record))
            output = root / "output"
            report.render(history.parent, output)
            self.assertEqual(json.loads((output / "history.json").read_text())["runs"], [full, metadata])
            state = test_report.ReportTests().dashboard_state(output)
            self.assertEqual(state["points"], 2)
            self.assertEqual(state["lines"], 0)
            for field in ("table", "labels", "tooltips"):
                rendered = " ".join(state[field]) if isinstance(state[field], list) else state[field]
                self.assertIn("Metadata/size only; file content integrity not checked", rendered)
                self.assertIn("Full-content SHA-256", rendered)


if __name__ == "__main__":
    unittest.main()
