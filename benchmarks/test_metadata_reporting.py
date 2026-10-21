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
                      metadata=dict(ok=True, entries=entries, checked_fields=list(
                          verification.SOURCE_METADATA_FIELDS if inline_bytes is not None
                          else verification.LEGACY_SOURCE_METADATA_FIELDS)))
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


def imported_tuning_history(policy):
    contracts = [
        {},
        dict(inline_bytes=0),
        dict(inline_bytes=0, read_bytes=verification.DEFAULT_READ_SIZE),
        dict(inline_bytes=0, batch_depth=verification.DEFAULT_BATCH_MULTIPLIER),
        dict(inline_bytes=0, read_bytes=verification.DEFAULT_READ_SIZE,
             batch_depth=verification.DEFAULT_BATCH_MULTIPLIER),
        dict(inline_bytes=0, read_bytes=verification.DEFAULT_READ_SIZE * 2,
             batch_depth=verification.DEFAULT_BATCH_MULTIPLIER),
        dict(inline_bytes=0, read_bytes=verification.DEFAULT_READ_SIZE,
             batch_depth=verification.DEFAULT_BATCH_MULTIPLIER + 1),
    ]
    records = []
    for index, contract in enumerate(contracts + contracts):
        record = verified_record(policy, f"{index + 1:032x}",
                                 f"2026-09-{index + 1:02d}T12:00:00Z", contract.get("inline_bytes"))
        record["context"]["verification"].update(contract)
        # imported producer IDs may be stale; the explicit contract must also agree
        record["summaries"][0]["series_id"] = "a" * 64
        report.validate_result(record)
        records.append(record)
    return records


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
                for trial in sanitized.project_run(record, [])["trials"]:
                    self.assertEqual(trial["proofs"]["metadata"]["qualification"], "passed")
                    self.assertEqual(trial["proofs"]["metadata"]["checked_fields"], ["mode"])
                record["trials"][0]["validation"].pop("metadata")
                with self.assertRaisesRegex(ValueError, "validation.metadata"):
                    report.validate_result(record)

    def test_nonsummary_streaming_metadata_export_retains_partial_evidence(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            for marker in (dict(ok=False), dict(ok=False, interrupted=True),
                           dict(ok=False, verification_error=True)):
                with self.subTest(policy=policy, marker=marker):
                    record = verified_record(policy)
                    record["variants"][0]["args"] = []
                    record.update(status="failed", summaries=[])
                    record["trials"] = record["trials"][:1]
                    trial = record["trials"][0]
                    trial.update(status="failed", child_locale="inherited", source_validation=marker)
                    trial.pop("copy_summary")
                    trial.pop("metadata_validation")
                    projected = sanitized.project_run(record, [])["trials"][0]
                    self.assertEqual(projected["proofs"]["metadata"]["qualification"], "recorded-unqualified")
                    self.assertTrue(projected["proofs"]["metadata"]["recorded_ok"])
                    self.assertEqual(projected["proofs"]["source"]["qualification"],
                        "interrupted" if marker.get("interrupted") else "verification-error" if marker.get("verification_error") else "failed")

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

    def test_modern_streaming_source_proofs_require_all_recorded_identity_fields(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            for threshold in (0, verification.DEFAULT_INLINE_BYTES):
                with self.subTest(policy=policy, threshold=threshold):
                    record = verified_record(policy, inline_bytes=threshold)
                    report.validate_result(record)
                    for fields in (verification.LEGACY_SOURCE_METADATA_FIELDS,
                                   verification.SOURCE_METADATA_FIELDS[:-1]):
                        record["trials"][0]["source_validation"]["metadata"]["checked_fields"] = list(fields)
                        with self.assertRaisesRegex(ValueError, "source_validation.metadata"):
                            report.validate_result(record)

    def test_known_legacy_labels_are_exact_and_do_not_override_metadata_mode(self):
        policies = (
            "exact relative directory and regular-file paths, sizes, and SHA256 contents",
            "exact relative directory and regular-file paths, sizes, and SHA256 contents; "
            "source tree must match initial scan after all case trials",
        )
        for policy in policies:
            with self.subTest(policy=policy):
                record = verified_record(streaming.POLICY)
                record["context"].pop("verification")
                record["context"]["verification_policy"] = policy
                self.assertEqual(report.verification_label(record["context"]), "Full-content SHA-256")
                self.assertEqual(report._dashboard_run(record)["verification_label"], "Full-content SHA-256")
                evidence = changes.build([record], {record["run_id"]: []})
                self.assertIn("Full-content SHA-256", changes.markdown(evidence))
                record["context"]["verification"] = dict(policy=streaming.METADATA_POLICY)
                self.assertIn("file content integrity not checked", report.verification_label(record["context"]))
            for unknown in (policy + "; unrecognized", policy.upper(), " " + policy):
                self.assertIn("Legacy", report.verification_label(dict(verification_policy=unknown)))
        self.assertIn("Legacy", report.verification_label({}))

    def test_completed_proof_remains_visible_when_later_summary_or_metadata_fails(self):
        for policy in (None, streaming.POLICY, streaming.METADATA_POLICY):
            for field in (("summary_validation", "metadata_validation") if policy is None else ("summary_validation",)):
                for qualification, marker in (
                        ("failed", {}), ("interrupted", dict(interrupted=True)),
                        ("verification-error", dict(verification_error=True))):
                    with self.subTest(policy=policy, field=field, qualification=qualification):
                        record = verified_record(policy or streaming.POLICY)
                        if policy is None:
                            record["context"].pop("verification")
                        record.update(status="failed", summaries=[])
                        record["trials"] = record["trials"][:1]
                        trial = record["trials"][0]
                        trial.update(status="failed", failure=dict(kind="interrupted" if marker.get("interrupted") else "exception",
                            stage="summary" if field == "summary_validation" else "metadata",
                            category="cancellation" if marker.get("interrupted") else "validation", message="private output"))
                        trial[field] = dict(ok=False, error="private output", **marker)
                        before = copy.deepcopy(record)
                        exported = sanitized.project_run(record, [])
                        self.assertEqual(record, before)
                        proof = exported["trials"][0]["proofs"]
                        destination = "structure" if policy == streaming.METADATA_POLICY else "content"
                        self.assertEqual(proof[destination]["qualification"], "recorded-unqualified")
                        self.assertEqual(proof["summary" if field == "summary_validation" else "metadata"]["qualification"], qualification)
                        self.assertEqual(exported["trials"][0]["failure"]["stage"], trial["failure"]["stage"])
                        self.assertEqual(exported["trials"][0]["failure"]["category"], trial["failure"]["category"])
                        self.assertNotIn("private output", json.dumps(exported))

    def test_summary_proof_is_typed_and_cannot_contradict_success(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            for marker in (dict(ok=False), dict(ok=True, interrupted=True),
                           dict(ok=True, verification_error=True), dict(ok=True, complete=False)):
                record = verified_record(policy)
                record["trials"][0]["summary_validation"] = marker
                with self.subTest(policy=policy, marker=marker), self.assertRaisesRegex(ValueError, "summary_validation"):
                    report.validate_result(record)
            record = verified_record(policy)
            for trial in record["trials"]:
                trial["summary_validation"] = dict(ok=True)
            self.assertTrue(all(trial["proofs"]["summary"]["qualification"] == "passed"
                                for trial in sanitized.project_run(record, [])["trials"]))
        for field in ("validation", "metadata_validation", "source_validation", "summary_validation"):
            for flag in ("ok", "interrupted", "verification_error", "complete"):
                record = verified_record(streaming.METADATA_POLICY)
                record.update(status="failed", summaries=[])
                record["trials"] = record["trials"][:1]
                record["trials"][0].update(status="failed")
                record["trials"][0][field] = dict(ok=False)
                record["trials"][0][field][flag] = "false"
                with self.subTest(field=field, flag=flag), self.assertRaisesRegex(ValueError, "must be a boolean"):
                    report.validate_result(record)

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

    def test_streaming_read_and_batch_tuning_are_bounded_and_exported_without_defaults(self):
        bounds = dict(read_bytes=(verification.MIN_READ_BYTES, verification.MAX_READ_BYTES),
                      batch_depth=(1, verification.MAX_BATCH_DEPTH))
        for field, (minimum, maximum) in bounds.items():
            for value in (minimum, maximum):
                with self.subTest(field=field, value=value):
                    record = verified_record(streaming.POLICY, inline_bytes=0)
                    record["context"]["verification"][field] = value
                    self.assertEqual(sanitized.project_run(record, [])["conditions"]["verification"][field], value)
            for value in (None, True, minimum - 1, maximum + 1, float(minimum), str(minimum)):
                with self.subTest(field=field, invalid=value):
                    record = verified_record(streaming.POLICY, inline_bytes=0)
                    record["context"]["verification"][field] = value
                    with self.assertRaisesRegex(ValueError, "streaming verification contract"):
                        report.validate_result(record)
            modern = verified_record(streaming.POLICY)
            modern["context"]["verification"][field] = minimum
            with self.assertRaisesRegex(ValueError, "streaming verification contract"):
                report.validate_result(modern)
        historical = sanitized.project_run(verified_record(streaming.POLICY), [])
        self.assertTrue(bounds.keys().isdisjoint(historical["conditions"]["verification"]))

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

    def test_changes_partition_imported_read_and_batch_contracts_without_defaulting_history(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            with self.subTest(policy=policy):
                records = imported_tuning_history(policy)
                evidence = changes.build(records, {record["run_id"]: [] for record in records})
                groups = len(records) // 2
                for index, row in enumerate(evidence["changes"]):
                    if index < groups:
                        self.assertIsNone(row["reference"], records[index]["context"]["verification"])
                    else:
                        self.assertEqual(row["reference"]["run_id"], records[index - groups]["run_id"])
                        self.assertEqual(row["status"], "compared")

    def test_dashboard_partitions_imported_read_and_batch_contracts_without_defaulting_history(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            with self.subTest(policy=policy), tempfile.TemporaryDirectory() as temporary:
                records = imported_tuning_history(policy)
                root = Path(temporary)
                history = root / "input" / "runs"
                history.mkdir(parents=True)
                for record in records:
                    (history / (record["run_id"] + ".json")).write_text(json.dumps(record))
                output = root / "output"
                report.render(history.parent, output)
                state = test_report.ReportTests().dashboard_state(output)
                self.assertEqual(state["points"], len(records))
                self.assertEqual(state["lines"], len(records) // 2)

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
