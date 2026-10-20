"""Metadata-only records keep their proof contract and visible qualification."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

from benchmarks import changes, operations, report, run, sanitized, streaming, test_report
from benchmarks.test_report import sample_run


def verified_record(policy, run_id="a" * 32, timestamp="2026-09-26T12:00:00Z"):
    record = sample_run(run_id=run_id, timestamp=timestamp)
    record["revision"]["commit"] = "c" * 40
    record["context"].update(topology="local", repository="owner/repository",
                             summary_locale="C", operation_contract_revision=operations.CONTRACT_REVISION,
                             verification=dict(policy=policy, workers=8))
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
    def test_metadata_and_legacy_full_proofs_keep_distinct_policies(self):
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

    def test_destination_modes_required_without_copy_summary(self):
        record = verified_record(streaming.METADATA_POLICY)
        record["variants"][0]["args"] = []
        for trial in record["trials"]:
            trial["child_locale"] = "inherited"
            trial.pop("copy_summary")
            trial.pop("metadata_validation")
        report.validate_result(record)
        record["trials"][0]["validation"].pop("metadata")
        with self.assertRaisesRegex(ValueError, "validation.metadata"):
            report.validate_result(record)

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

    def test_sanitized_failed_metadata_run_does_not_admit_partial_structure(self):
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
        self.assertEqual(proofs["source"]["qualification"], "failed")

    def test_history_never_pools_full_and_metadata_policies_and_discloses_both(self):
        full = verified_record(streaming.POLICY)
        metadata = verified_record(streaming.METADATA_POLICY, "b" * 32, "2026-09-27T12:00:00Z")
        self.assertNotEqual(full["summaries"][0]["series_id"], metadata["summaries"][0]["series_id"])
        # an imported, stale or wrongly reused series ID cannot override the explicit policy.
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
