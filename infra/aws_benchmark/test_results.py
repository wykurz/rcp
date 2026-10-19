"""Hostile archive, provenance and future offline history boundaries without AWS."""

import copy
import io
import json
import os
from pathlib import Path
import stat
import struct
import tempfile
import unittest
from unittest.mock import patch
import warnings
import zipfile

from benchmarks import changes, operations, pairs, report
from benchmarks.test_pair_boundaries import paired_record
from benchmarks.test_resource_boundaries import resource_record
from infra.aws_benchmark import common, identity, results


def binding(suite="tiny"):
    return dict(run="123-1", baseline="a" * 40, candidate="b" * 40 if suite != "aa-smoke" else "a" * 40,
        suite=suite, controller="c" * 40, workflow_sha="c" * 40, repository=identity.REPOSITORY,
        ref=identity.REF, workflow=identity.WORKFLOW, event="workflow_dispatch",
        run_url="https://github.com/wykurz/rcp/actions/runs/123/attempts/1")


def measurement(expected):
    run = paired_record(common.SUITES[expected["suite"]], repetitions=4)
    run["revision"] = dict(commit=None, branch=None, dirty=None)
    run["context"].update(resource_record()["context"])
    run["context"].update(repository=identity.REPOSITORY, baseline_commit=expected["baseline"],
        purpose="performance", cache_policy="linux-drop-caches", runner_label="aws-c5d24-ext4-isolated-v1",
        operation_contract_revision=operations.CONTRACT_REVISION, summary_locale="C", pairing=pairs.configuration(0, 4))
    run["tools"] = {key: dict(sha256="d" * 64) for key in ("rcp", "rcpd", "filegen", "rcp-baseline", "rcpd-baseline")}
    for variant in run["variants"]:
        variant["args"] = ["--summary"]
    for case in run["cases"]:
        case["fixture_digest"] = "e" * 64
    for index, trial in enumerate(run["trials"]):
        case = run["cases"][index // 8]
        counts = operations.expected_counts(case)
        iteration, position = divmod(index % 8, 2)
        pairing = pairs.trial_metadata(run["context"]["pairing"], case["id"], iteration + 1, position)
        role = pairing["order"][position]
        proof = dict(ok=True, counts=counts, digest=case["fixture_digest"])
        metadata = dict(ok=True, entries=counts["directories"] + counts["files"] + 1)
        trial.update(resource_record(status="ok")["trials"][0])
        trial.update(variant_id=role, pairing=pairing, elapsed_seconds=1.0 if role == pairs.CANDIDATE else 2.0,
            child_locale="C", operation="fresh", expected_transfer=operations.transfer_counts("fresh", counts),
            copy_summary=operations.transfer_counts("fresh", counts), validation=proof,
            metadata_validation=dict(metadata, checked_fields=["mode"]),
            source_validation=dict(proof, metadata=dict(metadata, checked_fields=["mode", "uid", "gid", "mtime_ns"])))
    return report.validate_result(run)


def archive(entries, compression=zipfile.ZIP_STORED):
    buffer = io.BytesIO()
    with warnings.catch_warnings(), zipfile.ZipFile(buffer, "w", compression=compression) as output:
        warnings.simplefilter("ignore", UserWarning)
        for name, data in entries:
            output.writestr(name, data)
    return buffer.getvalue()


def bundle(root, expected=None, raw=None, success=True):
    expected = expected or binding()
    raw = measurement(expected) if raw is None else raw
    window = {key: expected[key] for key in ("run", "baseline", "candidate", "suite")}
    window.update(opens="2026-10-19T10:00:00Z", deadline="2026-10-19T10:30:00Z")
    request = {key: window[key] for key in ("run", "baseline", "candidate", "suite", "deadline")}
    request["sha256"] = dict.fromkeys(common.BINARIES, "d" * 64)
    status = common.completed_status(expected["run"], success)
    worker = dict(window=window, request=request, status=copy.deepcopy(status), harness_revision="f" * 40,
                  admission=dict(accepted=True), service_returncode=0 if success else 1)
    data = archive([("worker.json", results.encoded(worker)), (results.RESULT, results.encoded(raw)),
                    ("benchmark/results/summary.md", b"<script>untrusted()</script>")])
    status["sha256"] = common.digest(data)
    receipt = dict(schema_version=1, identity=expected, window=window, request=request,
        outcome="success" if success else "failure", cleanup="verified",
        instance=dict(instance_id="i-" + "1" * 17, image_id="ami-" + "1" * 17, instance_type="c5d.24xlarge"),
        archive_sha256=common.digest(data), status_sha256=common.digest(results.encoded(status)))
    root.mkdir(exist_ok=True)
    for name, payload in (("results.zip", data), ("controller.json", results.encoded(receipt)),
                          ("status.json", results.encoded(status))):
        (root / name).write_bytes(payload)
    policy = dict(schema_version=1, identity=expected, archive_sha256=common.digest(data),
                  ami_id=receipt["instance"]["image_id"], harness_revision=worker["harness_revision"])
    return expected, policy


class ResultTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source, self.output = self.root / "input", self.root / "report"
        self.expected, self.policy = bundle(self.source)

    def ingest(self, conclusion="success", policy=None, history=None, output=None):
        return results.ingest(self.source, output or self.output, self.expected, conclusion, policy, history)

    def test_complete_report_preserves_raw_evidence_and_separate_revision_roles(self):
        evidence = self.ingest()
        self.assertEqual(evidence["errors"], [])
        self.assertFalse(evidence["history_eligible"])
        for path in self.source.iterdir():
            self.assertEqual(path.read_bytes(), (self.output / "raw" / path.name).read_bytes())
        derived = report.parse_result((self.output / "derived.json").read_text())
        self.assertEqual(derived["revision"], dict(commit=None, branch=None, dirty=None))
        self.assertEqual(changes.exclusion(derived), "not-performance")
        provenance = derived["context"]["aws_provenance"]
        self.assertEqual(provenance["identity"]["candidate"], "b" * 40)
        self.assertEqual(provenance["supervisor_harness_revision"], "f" * 40)
        self.assertNotIn("<script>untrusted()", (self.output / "index.html").read_text())
        self.assertTrue((self.output / "dashboard/index.html").exists())

    def test_failed_and_cancelled_runs_keep_diagnostics_without_history(self):
        for conclusion in ("failure", "cancelled", "skipped"):
            with self.subTest(conclusion=conclusion):
                output = self.root / conclusion
                evidence = self.ingest(conclusion, self.policy, self.root / "history", output)
                self.assertFalse(evidence["history_eligible"])
                self.assertTrue(evidence["errors"])
                self.assertTrue((output / "raw/results.zip").exists())
                self.assertTrue((output / "summary.md").exists())
                self.assertFalse((self.root / "history").exists())

    def test_failed_worker_with_partial_record_remains_readable(self):
        raw = measurement(self.expected)
        raw.update(status="failed", trials=raw["trials"][:1], summaries=[])
        bundle(self.source, self.expected, raw, success=False)
        evidence = self.ingest("failure")
        self.assertEqual(evidence["errors"], [])
        self.assertFalse(evidence["history_eligible"])
        self.assertEqual(json.loads((self.output / "derived.json").read_text())["status"], "failed")

    def test_missing_cancelled_receipt_does_not_infer_success_from_worker(self):
        (self.source / "controller.json").unlink()
        evidence = self.ingest("cancelled", self.policy)
        self.assertIsNone(evidence["controller"])
        self.assertFalse(evidence["history_eligible"])
        self.assertFalse((self.output / "derived.json").exists())
        self.assertTrue((self.output / "raw/results.zip").exists())

    def test_smoke_and_arbitrary_candidate_without_independent_policy_cannot_publish(self):
        for suite in ("tiny", "aa-smoke"):
            expected = binding(suite)
            _, policy = bundle(self.source, expected)
            output = self.root / suite
            evidence = results.ingest(self.source, output, expected, "success",
                                      policy if suite == "aa-smoke" else None, self.root / "history")
            self.assertFalse(evidence["history_eligible"])
            self.assertFalse((self.root / "history").exists())

    def test_offline_approval_is_idempotent_and_series_preserves_source_identity(self):
        history = self.root / "history"
        evidence = self.ingest(policy=self.policy, history=history)
        self.assertEqual(evidence["errors"], [])
        self.assertTrue(evidence["history_eligible"])
        staged = next((history / "runs").iterdir())
        before = staged.read_bytes()
        self.ingest(policy=self.policy, history=history, output=self.root / "rerun")
        self.assertEqual(staged.read_bytes(), before)
        record = report.parse_result(before.decode())
        self.assertEqual(record["revision"]["commit"], self.expected["candidate"])
        self.assertEqual(record["revision"]["role"], "approved candidate")
        self.assertIsNone(record["context"]["aws_provenance"]["raw_revision"]["commit"])
        self.assertNotEqual(record["summaries"][0]["series_id"], "1" * 64)
        self.assertEqual(record["context"]["aws_provenance"]["source_series_ids"], ["1" * 64, "1" * 64])
        self.assertIn("candidate revision", (self.output / "dashboard/changes.html").read_text())
        self.assertNotIn("checkout revision " + self.expected["candidate"],
                         (self.output / "dashboard/changes.html").read_text())

    def test_history_retry_accepts_complete_record_after_link_before_unlink_crash(self):
        history = self.root / "history"
        self.ingest(policy=self.policy, history=history)
        path = next((history / "runs").glob("*.json"))
        os.link(path, history / "runs/.aws-stage-interrupted")
        evidence = self.ingest(policy=self.policy, history=history, output=self.root / "retry")
        self.assertTrue(evidence["history_eligible"])
        self.assertEqual(evidence["errors"], [])

    def test_conflicting_replay_cannot_overwrite_existing_history(self):
        history = self.root / "history"
        self.ingest(policy=self.policy, history=history)
        path = next((history / "runs").iterdir())
        before = path.read_bytes()
        raw = measurement(self.expected)
        raw["run_id"] = "9" * 32
        _, policy = bundle(self.source, self.expected, raw)
        evidence = self.ingest(policy=policy, history=history, output=self.root / "conflict")
        self.assertFalse(evidence["history_eligible"])
        self.assertEqual(path.read_bytes(), before)
        self.assertTrue(any("staging" in error for error in evidence["errors"]))

    def test_policy_binds_every_revision_attempt_archive_and_image(self):
        mutations = [("archive_sha256", "0" * 64), ("ami_id", "ami-" + "2" * 17), ("harness_revision", "0" * 40)]
        for field in self.expected:
            policy = copy.deepcopy(self.policy)
            policy["identity"][field] = "unapproved"
            mutations.append(("identity", policy["identity"]))
        for index, (field, value) in enumerate(mutations):
            policy = copy.deepcopy(self.policy)
            policy[field] = value
            with self.subTest(field=field):
                evidence = self.ingest(policy=policy, output=self.root / str(index))
                self.assertFalse(evidence["history_eligible"])

    def test_unknown_image_cleanup_or_controller_outcome_denies_history(self):
        for index, mutation in enumerate((dict(instance=None), dict(cleanup="unconfirmed"), dict(outcome="incomplete"))):
            receipt = json.loads((self.source / "controller.json").read_text())
            receipt.update(mutation)
            (self.source / "controller.json").write_bytes(results.encoded(receipt))
            evidence = self.ingest(policy=self.policy, output=self.root / str(index))
            self.assertFalse(evidence["history_eligible"])
            bundle(self.source)

    def test_tampered_status_archive_identity_and_binary_hash_are_rejected(self):
        for index, name in enumerate(("status.json", "results.zip", "controller.json")):
            original = (self.source / name).read_bytes()
            if name == "results.zip":
                replacement = original[:-1] + b"!"
            else:
                value = json.loads(original)
                if name == "status.json":
                    value["sha256"] = "0" * 64
                else:
                    value["identity"]["candidate"] = "0" * 40
                replacement = results.encoded(value)
            (self.source / name).write_bytes(replacement)
            evidence = self.ingest(policy=self.policy, output=self.root / str(index))
            self.assertTrue(evidence["errors"])
            self.assertFalse(evidence["history_eligible"])
            self.assertFalse((self.root / str(index) / "derived.json").exists())
            (self.source / name).write_bytes(original)

    def test_payload_claims_cannot_bypass_full_proofs_pairing_or_tool_hash_checks(self):
        mutations = [lambda run: run["context"].pop("pairing"),
                     lambda run: run["tools"]["rcp"].update(sha256="0" * 64),
                     lambda run: run["trials"][0].pop("source_validation"),
                     lambda run: run["context"].update(purpose="smoke")]
        for index, mutate in enumerate(mutations):
            raw = measurement(self.expected)
            mutate(raw)
            _, policy = bundle(self.source, self.expected, raw)
            evidence = self.ingest(policy=policy, output=self.root / str(index))
            self.assertFalse(evidence["history_eligible"])

    def test_existing_report_directory_is_not_reused_after_failure(self):
        self.ingest()
        before = (self.output / "summary.md").read_bytes()
        with self.assertRaises(FileExistsError):
            self.ingest("failure")
        self.assertEqual((self.output / "summary.md").read_bytes(), before)

    def replace_archive(self, data):
        (self.source / "results.zip").write_bytes(data)
        status = json.loads((self.source / "status.json").read_text())
        receipt = json.loads((self.source / "controller.json").read_text())
        status["sha256"] = receipt["archive_sha256"] = common.digest(data)
        payload = results.encoded(status)
        receipt["status_sha256"] = common.digest(payload)
        (self.source / "status.json").write_bytes(payload)
        (self.source / "controller.json").write_bytes(results.encoded(receipt))

    def test_malformed_metadata_and_unsupported_archives_still_produce_diagnostics(self):
        good = (self.source / "results.zip").read_bytes()
        with zipfile.ZipFile(io.BytesIO(good)) as original:
            entries = [(item.filename, original.read(item)) for item in original.infolist()]
        malformed = [archive([(name, results.encoded(value) if name == "worker.json" else data)
                             for name, data in entries]) for value in ([1], "malformed", None)]
        version = bytearray(good)
        struct.pack_into("<H", version, good.index(b"PK\x01\x02") + 6, 99)
        malformed.append(bytes(version))
        for index, data in enumerate(malformed):
            self.replace_archive(data)
            output = self.root / str(index)
            evidence = self.ingest(output=output)
            self.assertTrue(evidence["errors"])
            self.assertTrue((output / "summary.md").exists())
            self.assertEqual((output / "raw/results.zip").read_bytes(), data)
        for index, value in enumerate(([1], dict(instance=[1])), start=10):
            bundle(self.source)
            receipt = json.loads((self.source / "controller.json").read_text())
            receipt = value if isinstance(value, list) else dict(receipt, **value)
            (self.source / "controller.json").write_bytes(results.encoded(receipt))
            evidence = self.ingest(output=self.root / str(index))
            self.assertTrue(evidence["errors"])
            self.assertTrue((self.root / str(index) / "summary.md").exists())

    def test_lone_surrogate_and_renderer_failure_preserve_readable_diagnostics(self):
        raw = measurement(self.expected)
        raw["context"]["runner_label"] = chr(0xD800)
        bundle(self.source, self.expected, raw)
        evidence = self.ingest()
        self.assertTrue(evidence["errors"])
        self.assertTrue((self.output / "summary.md").exists())
        bundle(self.source)
        with patch.object(results.report, "render", side_effect=RuntimeError("injected")):
            evidence = self.ingest(output=self.root / "render-failure")
        self.assertTrue(any("rendering" in error for error in evidence["errors"]))
        self.assertTrue((self.root / "render-failure/summary.md").exists())

    def test_interrupted_history_staging_exposes_no_partial_record_and_can_retry(self):
        history = self.root / "history"
        with patch.object(results.os, "fsync", side_effect=OSError("injected")):
            evidence = self.ingest(policy=self.policy, history=history)
        self.assertFalse(evidence["history_eligible"])
        self.assertEqual(list((history / "runs").iterdir()), [])
        evidence = self.ingest(policy=self.policy, history=history, output=self.root / "retry")
        self.assertTrue(evidence["history_eligible"])
        self.assertEqual(len(list((history / "runs").iterdir())), 1)

    def test_untrusted_labels_cannot_inject_markdown_html_or_workflow_commands(self):
        value = '<script>evil()</script>\n::warning::x [link](https://evil.test) `code` |cell'
        escaped = results.literal(value)
        self.assertNotIn("<", escaped)
        self.assertNotIn("\n", escaped)
        self.assertNotIn("::", escaped)
        self.assertNotIn("[", escaped)
        self.assertNotIn("`", escaped)
        self.assertNotIn("|", escaped)


class ArchiveTests(unittest.TestCase):
    def test_only_bounded_regular_files_are_read(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "data"
            data.write_bytes(b"1234")
            self.assertEqual(results.read_file(data, 4), b"1234")
            with self.assertRaises(ValueError):
                results.read_file(data, 3)
            (root / "link").symlink_to(data)
            os.mkfifo(root / "fifo")
            os.link(data, root / "hard")
            for path in (root / "link", root / "fifo", root, root / "hard"):
                with self.subTest(path=path), self.assertRaises((OSError, ValueError)):
                    results.read_file(path, 100)

    def test_strict_json_rejects_duplicate_nonfinite_deep_and_oversized_values(self):
        for data in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}', b'[' * 41 + b'0' + b']' * 41,
                     results.encoded({"a": "x" * 16385}), b'\xff'):
            with self.subTest(data=data[:40]), self.assertRaises((ValueError, UnicodeError)):
                results.json_value(data)
        with self.assertRaises(ValueError):
            results.json_value(b"{}", 1)

    def test_unsafe_duplicate_names_symlinks_and_compression_are_rejected(self):
        unsafe = ("/worker.json", "../worker.json", "benchmark/../worker.json", "benchmark//x", "benchmark/./x",
                  "benchmark/a\\b", "benchmark/C:x", "benchmark/é", "benchmark/x\n")
        payloads = [archive([("worker.json", b"{}"), (name, b"x")]) for name in unsafe]
        payloads += [archive([("worker.json", b"{}"), (name, b"{}")]) for name in ("worker.json", "WORKER.JSON")]
        payloads.append(archive([("worker.json", b"{}"), ("benchmark/x_hidden", b"x")]).replace(
            b"benchmark/x_hidden", b"benchmark/x\x00hidden"))
        symbolic = zipfile.ZipInfo("benchmark/link")
        symbolic.external_attr = (stat.S_IFLNK | 0o777) << 16
        payloads += [archive([("worker.json", b"{}"), (symbolic, b"/etc/passwd")]),
                     archive([("worker.json", b"{}")], zipfile.ZIP_DEFLATED)]
        for index, payload in enumerate(payloads):
            with self.subTest(index=index), self.assertRaises((ValueError, zipfile.BadZipFile)):
                results.members(payload)

    def test_oversized_entry_count_is_rejected_before_zipfile_allocation(self):
        payload = bytearray(archive([("worker.json", b"{}")]))
        struct.pack_into("<HH", payload, len(payload) - 14, results.MAX_ENTRIES + 1, results.MAX_ENTRIES + 1)
        with patch.object(results.zipfile, "ZipFile") as parser, self.assertRaises(ValueError):
            results.members(bytes(payload))
        parser.assert_not_called()

    def test_count_lie_crc_truncation_encryption_overlap_and_trailing_data_fail(self):
        good = archive([("worker.json", b"{}"), ("benchmark/log", b"diagnostic")])
        payloads = [good[:-1], good + b"trailer", good.replace(b"diagnostic", b"DIAGNOSTIC")]
        central = good.index(b"PK\x01\x02")
        for offset, fmt, value in ((6, "H", 1), (central + 8, "H", 1), (central + 42, "L", 1),
                                   (len(good) - 14, "H", 1), (18, "L", results.MAX_RESULT + 1)):
            payload = bytearray(good)
            struct.pack_into("<" + fmt, payload, offset, value)
            payloads.append(bytes(payload))
        for index, payload in enumerate(payloads):
            with self.subTest(index=index), self.assertRaises((ValueError, zipfile.BadZipFile)):
                results.members(payload)

    def test_archive_member_and_json_limits_preserve_bounded_diagnostics(self):
        with patch.object(results, "MAX_METADATA", 8), self.assertRaises(ValueError):
            results.members(archive([("worker.json", b"x" * 9)]))
        with patch.object(results, "MAX_JSON", 8), self.assertRaises(ValueError):
            results.members(archive([("worker.json", b"{}"), (results.RESULT, b"x" * 9)]))
        with patch.object(results, "MAX_RESULT", 8), self.assertRaises(ValueError):
            results.members(archive([("worker.json", b"{}")]))


if __name__ == "__main__":
    unittest.main()
