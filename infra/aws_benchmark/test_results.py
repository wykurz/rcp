"""Hostile archive, provenance and future offline history boundaries without AWS."""

import copy
import io
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import stat
import struct
import tempfile
import unittest
from unittest.mock import Mock, patch
import warnings
import zipfile

from benchmarks import changes, operations, pairs, report, run as harness
from benchmarks.test_pair_boundaries import paired_record
from benchmarks.test_resource_boundaries import resource_record
from infra.aws_benchmark import common, identity, results, worker


def binding(suite="tiny"):
    return dict(run="123-1", baseline="a" * 40, candidate="b" * 40 if suite != "aa-smoke" else "a" * 40,
        suite=suite, controller="c" * 40, workflow_sha="c" * 40, repository=identity.REPOSITORY,
        ref=identity.REF, workflow=identity.WORKFLOW, event="workflow_dispatch",
        run_url="https://github.com/wykurz/rcp/actions/runs/123/attempts/1")


def measurement(expected):
    command = worker.command(expected, 600)
    args = harness._arguments(command[command.index("benchmarks.run") + 1:])
    environment = dict(value.removeprefix("--setenv=").split("=", 1)
                       for value in command if value.startswith("--setenv="))
    run = paired_record(args.cases, repetitions=args.repetitions)
    rows_per_case = 2 * args.repetitions
    run["revision"] = dict(commit=None, branch=None, dirty=None)
    run["context"].update(resource_record()["context"])
    run["context"].update(repository=environment["GITHUB_REPOSITORY"], baseline_commit=environment["RCP_BENCH_BASELINE_COMMIT"],
        topology=args.mode, purpose=args.purpose, cache_policy=args.cache, runner_label=args.runner_label,
        timing_request="disabled" if args.no_timings else "optional", operation_contract_revision=operations.CONTRACT_REVISION,
        summary_locale="C", pairing=pairs.configuration(args.paired_seed, args.repetitions))
    run["tools"] = {key: dict(sha256="d" * 64) for key in ("rcp", "rcpd", "filegen", "rcp-baseline", "rcpd-baseline")}
    for variant in run["variants"]:
        variant["args"] = ["--summary"]
    for case in run["cases"]:
        case["fixture_digest"] = "e" * 64
    for index, trial in enumerate(run["trials"]):
        case = run["cases"][index // rows_per_case]
        counts = operations.expected_counts(case)
        iteration, position = divmod(index % rows_per_case, 2)
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
            member = name if isinstance(name, zipfile.ZipInfo) else zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            output.writestr(member, data, compress_type=compression)
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
        replay = self.ingest(policy=self.policy, history=history, output=self.root / "rerun")
        self.assertEqual(replay["errors"], [])
        self.assertTrue(replay["history_eligible"])
        self.assertEqual(list((history / "runs").iterdir()), [staged])
        self.assertEqual(staged.read_bytes(), before)
        record = report.parse_result(before.decode())
        self.assertEqual(record["revision"]["commit"], self.expected["candidate"])
        self.assertEqual(record["revision"]["role"], "approved candidate")
        self.assertIsNone(record["context"]["aws_provenance"]["raw_revision"]["commit"])
        self.assertNotEqual(record["summaries"][0]["series_id"], "1" * 64)
        self.assertEqual(record["context"]["aws_provenance"]["source_series_ids"], ["1" * 64, "1" * 64])
        self.assertIn("candidate revision", (self.output / "dashboard/changes.html").read_text())
        self.assertNotIn("checkout revision <code>" + self.expected["candidate"][:10],
                         (self.output / "dashboard/changes.html").read_text())

    def test_history_retry_accepts_complete_record_after_link_before_unlink_crash(self):
        history = self.root / "history"
        self.ingest(policy=self.policy, history=history)
        path = next((history / "runs").glob("*.json"))
        os.link(path, history / "runs/.aws-stage-interrupted")
        evidence = self.ingest(policy=self.policy, history=history, output=self.root / "retry")
        self.assertTrue(evidence["history_eligible"])
        self.assertEqual(evidence["errors"], [])

    def test_committed_history_stays_approved_when_temporary_cleanup_fails(self):
        history = self.root / "history"
        original_unlink = Path.unlink
        def cleanup(path, *args, **kwargs):
            if path.name.startswith(".aws-stage-"):
                raise PermissionError("injected temporary cleanup failure")
            return original_unlink(path, *args, **kwargs)
        with patch.object(Path, "unlink", autospec=True, side_effect=cleanup):
            evidence = self.ingest(policy=self.policy, history=history)
        self.assertEqual(evidence["errors"], [])
        self.assertTrue(evidence["history_eligible"])
        committed = next((history / "runs").glob("*.json"))
        temporary = next((history / "runs").glob(".aws-stage-*"))
        self.assertTrue(committed.samefile(temporary))
        self.assertEqual(committed.read_bytes(), (self.output / "derived.json").read_bytes())
        self.assertTrue(json.loads(committed.read_bytes())["context"]["aws_provenance"]["history_eligible"])
        replay = self.ingest(policy=self.policy, history=history, output=self.root / "retry")
        self.assertEqual(replay["errors"], [])
        self.assertTrue(replay["history_eligible"])
        self.assertEqual(list((history / "runs").glob("*.json")), [committed])

    def test_temporary_cleanup_failure_does_not_replace_failed_commit_reason(self):
        evidence, raw, _ = results.inspect(self.source, self.expected, "success")
        results.approve(evidence, raw, self.policy)
        record = results.derived_record(evidence, raw)
        history = self.root / "history"
        with patch.object(results.os, "link", side_effect=OSError("injected commit failure")):
            with patch.object(Path, "unlink", side_effect=PermissionError("injected cleanup failure")):
                with self.assertRaisesRegex(OSError, "^injected commit failure$"):
                    results.stage_history(record, history)
        self.assertEqual(list((history / "runs").glob("*.json")), [])

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
            self.expected, self.policy = bundle(self.source)
            receipt = json.loads((self.source / "controller.json").read_text())
            receipt.update(mutation)
            (self.source / "controller.json").write_bytes(results.encoded(receipt))
            evidence = self.ingest(policy=self.policy, output=self.root / str(index))
            self.assertFalse(evidence["history_eligible"])
            self.assertIn("history approval: unverified execution provenance", evidence["errors"])

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
        raw["context"]["fixture_policy"] = chr(0xD800)
        bundle(self.source, self.expected, raw)
        evidence = self.ingest()
        self.assertIn("measurement record: JSON contains a lone surrogate", evidence["errors"])
        self.assertIsNone(evidence["raw_revision"])
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

    def test_untrusted_labels_are_literal_html_cell_content(self):
        value = '<script>evil()</script>\n::warning::x [link](https://evil.test) `code` |cell'
        escaped = results.literal(value)
        self.assertTrue(escaped.startswith("<code>") and escaped.endswith("</code>"))
        self.assertNotIn("<script>", escaped)
        self.assertIn("&lt;script&gt;", escaped)
        self.assertNotIn("\n", escaped)
        self.assertIn("\\n::warning::", escaped)

    def test_incomplete_bundles_retain_controller_cleanup_and_image(self):
        for index, missing in enumerate((("results.zip",), ("status.json",), ("results.zip", "status.json"))):
            for cleanup in ("verified", "unconfirmed", "not-requested"):
                self.expected, self.policy = bundle(self.source)
                receipt = json.loads((self.source / "controller.json").read_text())
                receipt.update(cleanup=cleanup, outcome="failure")
                if cleanup == "not-requested":
                    receipt.update(window=None, request=None)
                (self.source / "controller.json").write_bytes(results.encoded(receipt))
                for name in missing:
                    (self.source / name).unlink()
                evidence = self.ingest("failure", output=self.root / (str(index) + cleanup))
                self.assertEqual(evidence["controller"], receipt)
                self.assertIn("controller receipt: incomplete result bundle", evidence["errors"])
                self.assertIsNone(evidence["worker"])
                summary = results.summary(evidence, None)
                self.assertIn('<td><code>' + cleanup + '</code></td>', summary)
                self.assertIn(receipt["instance"]["image_id"], summary)

    def test_invalid_trusted_arguments_have_no_output_side_effect(self):
        for mutation in (dict(ref="refs/heads/debug"), dict(run=123)):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                results.ingest(self.source, self.output, {**self.expected, **mutation}, "success")
            self.assertFalse(self.output.exists())
        with self.assertRaisesRegex(ValueError, "invalid producer conclusion"):
            self.ingest("unexpected")
        self.assertFalse(self.output.exists())
        self.assertEqual(self.ingest()["errors"], [])

    def test_unbacked_candidate_role_is_refused_without_erasing_supervisor_evidence(self):
        raw = measurement(self.expected)
        raw["revision"] = dict(commit="9" * 40, branch=None, dirty=False, role="approved candidate")
        bundle(self.source, self.expected, raw)
        evidence = self.ingest()
        self.assertEqual(evidence["errors"], ["measurement record: ValueError"])
        self.assertIsNotNone(evidence["controller"])
        self.assertIsNotNone(evidence["worker"])
        self.assertFalse(evidence["history_eligible"])
        self.assertFalse((self.output / "derived.json").exists())
        self.assertEqual((self.source / "results.zip").read_bytes(), (self.output / "raw/results.zip").read_bytes())

    def test_candidate_revision_role_is_not_preserved_in_diagnostics(self):
        raw = measurement(self.expected)
        raw["revision"] = dict(commit="9" * 40, branch=None, dirty=False, role="approved candidate")
        # even structurally consistent candidate claims are replaced by trusted ingestion
        raw["context"]["aws_provenance"] = dict(history_eligible=True,
            identity=dict(candidate="9" * 40, repository=self.expected["repository"]))
        bundle(self.source, self.expected, raw)
        evidence = self.ingest()
        self.assertEqual(evidence["errors"], [])
        self.assertFalse(evidence["history_eligible"])
        derived = json.loads((self.output / "derived.json").read_text())
        self.assertEqual(derived["revision"], dict(commit="9" * 40, branch=None, dirty=False))
        self.assertEqual(evidence["raw_revision"], raw["revision"])
        changes_html = (self.output / "dashboard/changes.html").read_text()
        self.assertIn("checkout revision <code>" + "9" * 10, changes_html)
        self.assertNotIn("candidate revision <code>" + "9" * 10, changes_html)

    def test_forged_trust_lines_remain_in_one_measurement_cell(self):
        raw = measurement(self.expected)
        forged = 'x |\n\nHistory: approved for offline staging\n| Controller cleanup | verified </code></td></tr><p>forged</p>'
        raw["cases"][0]["id"] = forged
        for index, trial in enumerate(raw["trials"]):
            iteration, position = divmod(index, 2)
            pairing = pairs.trial_metadata(raw["context"]["pairing"], forged, iteration + 1, position)
            role = pairing["order"][position]
            trial.update(case_id=forged, pairing=pairing, variant_id=role,
                         elapsed_seconds=1.0 if role == pairs.CANDIDATE else 2.0)
        for item in raw["summaries"]:
            item["case_id"] = forged
        bundle(self.source, self.expected, raw)
        evidence = self.ingest()
        self.assertEqual(evidence["errors"], [])
        class Paragraphs(HTMLParser):
            def __init__(self):
                super().__init__()
                self.values, self.current = [], None
            def handle_starttag(self, tag, attrs):
                if tag == "p":
                    self.current = ""
            def handle_data(self, value):
                if self.current is not None:
                    self.current += value
            def handle_endtag(self, tag):
                if tag == "p" and self.current is not None:
                    self.values.append(self.current)
                    self.current = None
        for name in ("index.html", "summary.md"):
            page = (self.output / name).read_text()
            parsed = Paragraphs()
            parsed.feed(page)
            self.assertIn("History: diagnostic only; not published", parsed.values)
            self.assertNotIn("History: approved for offline staging", parsed.values)
            self.assertNotIn("forged", parsed.values)
            self.assertIn("&lt;/code&gt;&lt;/td&gt;", page)
            self.assertIn("123-1", page)

    def test_candidate_names_and_oversized_measurements_preserve_supervisor_evidence(self):
        names = ("core:dump.log", "café.log", "line\nbreak.log", "LOG", "/".join(["a" * 200] * 6))
        for index, name in enumerate(names):
            self.expected, self.policy = bundle(self.source)
            with zipfile.ZipFile(self.source / "results.zip") as original:
                entries = [(item.filename, original.read(item)) for item in original.infolist()]
            entries += [("benchmark/" + name, b"log")]
            if name == "LOG":
                entries.append(("benchmark/log", b"other log"))
            self.replace_archive(archive(entries))
            evidence = self.ingest(output=self.root / str(index))
            self.assertEqual(evidence["errors"], ["unsupported benchmark member names"])
            self.assertEqual(evidence["worker"]["harness_revision"], "f" * 40)
            self.assertIn("worker.json", evidence["members"])
            self.assertIn("benchmark/" + name, evidence["members"])
            self.assertFalse((self.root / str(index) / "derived.json").exists())
        self.expected, self.policy = bundle(self.source)
        with patch.object(results, "MAX_JSON", 32):
            evidence = self.ingest(output=self.root / "large-measurement")
        self.assertEqual(evidence["errors"], ["measurement JSON exceeds reporting byte limit"])
        self.assertEqual(evidence["worker"]["harness_revision"], "f" * 40)

    def test_each_collection_cap_reports_the_specific_diagnostic(self):
        for field, limit in (("cases", 16), ("variants", 16), ("trials", 512), ("summaries", 256)):
            raw = measurement(self.expected)
            raw[field] = [raw[field][0]] * (limit + 1)
            bundle(self.source, self.expected, raw)
            evidence = self.ingest(output=self.root / field)
            self.assertEqual(evidence["errors"], ["measurement record: measurement collection exceeds reporting limit"])

    def test_window_status_and_supervisor_bindings_have_specific_diagnostics(self):
        for target, diagnostic in (("window", "controller window/request: window identity mismatch"),
                ("status", "terminal status: invalid terminal status"),
                ("worker-window", "worker metadata: worker window mismatch"),
                ("worker-request", "worker metadata: worker binary request mismatch"),
                ("worker-status", "worker metadata: worker status mismatch")):
            self.expected, self.policy = bundle(self.source)
            if target in ("window", "status"):
                filename = "controller.json" if target == "window" else "status.json"
                value = json.loads((self.source / filename).read_text())
                (value["window"] if target == "window" else value)["run"] = "124-1"
                (self.source / filename).write_bytes(results.encoded(value))
            else:
                with zipfile.ZipFile(self.source / "results.zip") as original:
                    entries = [(item.filename, original.read(item)) for item in original.infolist()]
                metadata = json.loads(dict(entries)["worker.json"])
                metadata[target.removeprefix("worker-")]["run"] = "124-1"
                self.replace_archive(archive([(name, results.encoded(metadata) if name == "worker.json" else data)
                                               for name, data in entries]))
            evidence = self.ingest(output=self.root / target)
            self.assertEqual(evidence["errors"], [diagnostic])

    def test_generic_parser_errors_never_echo_untrusted_text(self):
        (self.source / "controller.json").write_bytes(b'{"FORGED":1,"FORGED":2}')
        evidence = self.ingest()
        self.assertEqual(evidence["errors"], ["controller receipt: ValueError"])
        self.assertNotIn("FORGED", (self.output / "summary.md").read_text())


    def test_history_policy_schema_and_shape_have_specific_refusals(self):
        for index, mutation in enumerate((dict(schema_version=2), dict(schema_version=True), dict(extra=True))):
            with self.subTest(mutation=mutation):
                policy = dict(self.policy, **mutation)
                evidence = self.ingest(policy=policy, output=self.root / str(index))
                self.assertFalse(evidence["history_eligible"])
                self.assertEqual(evidence["errors"], ["history approval: invalid history policy"])

    def test_invalid_approved_provenance_is_rejected_even_when_observations_match(self):
        for field, invalid in (("ami_id", "ami-invalid"), ("harness_revision", "not-a-commit")):
            with self.subTest(field=field):
                evidence, raw, _ = results.inspect(self.source, self.expected, "success")
                self.assertEqual(evidence["errors"], [])
                policy = dict(self.policy, **{field: invalid})
                if field == "ami_id":
                    evidence["controller"]["instance"]["image_id"] = invalid
                else:
                    evidence["worker"]["harness_revision"] = invalid
                with self.assertRaisesRegex(results.ValidationError, "^invalid approved AMI provenance$"):
                    results.approve(evidence, raw, policy)
                self.assertFalse(evidence["history_eligible"])

    def test_every_supervisor_completion_claim_is_required_for_history(self):
        mutations = {
            "success": lambda value: value["status"].update(success=False),
            "returncode": lambda value: value.update(service_returncode=1),
            "admission": lambda value: value["admission"].update(accepted=False),
            "request": lambda value: value.pop("request"),
        }
        for name, mutate in mutations.items():
            with self.subTest(field=name):
                evidence, raw, _ = results.inspect(self.source, self.expected, "success")
                self.assertEqual(evidence["errors"], [])
                mutate(evidence["worker"])
                with self.assertRaisesRegex(results.ValidationError, "^worker did not complete the approved request$"):
                    results.approve(evidence, raw, self.policy)
                self.assertFalse(evidence["history_eligible"])

    def test_controller_outcome_schema_failure_is_not_confused_with_history_refusal(self):
        original = (self.source / "controller.json").read_bytes()
        for field in ("outcome", "cleanup"):
            with self.subTest(field=field):
                receipt = json.loads(original)
                receipt[field] = "invalid"
                (self.source / "controller.json").write_bytes(results.encoded(receipt))
                evidence = self.ingest(output=self.root / field)
                self.assertEqual(evidence["errors"], ["controller receipt: invalid controller outcome"])
                self.assertIsNone(evidence["controller"])

    def test_direct_history_staging_requires_approval_before_creating_destination(self):
        evidence, raw, _ = results.inspect(self.source, self.expected, "success")
        self.assertEqual(evidence["errors"], [])
        record = results.derived_record(evidence, raw)
        destination = self.root / "history"
        with self.assertRaisesRegex(results.ValidationError, "^history approval required$"):
            results.stage_history(record, destination)
        self.assertFalse(destination.exists())

    def test_real_worker_archive_keeps_supervisor_evidence_with_nonportable_candidate_name(self):
        with zipfile.ZipFile(self.source / "results.zip") as original:
            metadata = json.loads(original.read("worker.json"))
            raw = original.read(results.RESULT)
        output = self.root / "worker-output"
        (output / "results").mkdir(parents=True)
        (output / "results/results.json").write_bytes(raw)
        (output / "core:dump.log").write_bytes(b"candidate diagnostic")
        self.replace_archive(worker.collect(output, metadata, Mock()))
        evidence = self.ingest()
        self.assertEqual(evidence["errors"], ["unsupported benchmark member names"])
        self.assertEqual(evidence["worker"], metadata)
        self.assertEqual(evidence["controller"]["cleanup"], "verified")
        self.assertIn("benchmark/core:dump.log", evidence["members"])
        self.assertFalse(evidence["history_eligible"])


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

    def test_file_growth_after_descriptor_stat_cannot_exceed_read_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "growing"
            path.write_bytes(b"1234")
            original_fstat = os.fstat
            def stat_then_grow(descriptor):
                info = original_fstat(descriptor)
                with path.open("ab") as output:
                    output.write(b"5")
                return info
            with patch.object(results.os, "fstat", side_effect=stat_then_grow):
                with self.assertRaisesRegex(results.ValidationError, "^input exceeds its byte limit$"):
                    results.read_file(path, 4)

    def test_strict_json_rejects_duplicate_nonfinite_deep_and_oversized_values(self):
        for data in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}', b'[' * 41 + b'0' + b']' * 41,
                     results.encoded({"a": "x" * 16385}), b'\xff'):
            with self.subTest(data=data[:40]), self.assertRaises((ValueError, UnicodeError)):
                results.json_value(data)
        with self.assertRaises(ValueError):
            results.json_value(b"{}", 1)

    def test_unsafe_duplicate_names_symlinks_and_compression_are_rejected(self):
        unsafe = ("/worker.json", "../worker.json", "benchmark/../worker.json", "benchmark//x", "benchmark/./x",
                  "benchmark/a\\b")
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
        with patch.object(results, "MAX_JSON", 8):
            selected, _, issues = results.members(archive([("worker.json", b"{}"), (results.RESULT, b"x" * 9)]))
        self.assertEqual(selected, {"worker.json": b"{}"})
        self.assertEqual(issues, ["measurement JSON exceeds reporting byte limit"])
        with patch.object(results, "MAX_RESULT", 8), self.assertRaises(ValueError):
            results.members(archive([("worker.json", b"{}")]))


    def test_json_node_cap_is_distinct_from_depth_and_byte_caps(self):
        with self.assertRaisesRegex(results.ValidationError, "JSON node count exceeds limit"):
            results.json_value(results.encoded([0] * 100000))

    def test_hidden_payload_between_local_records_and_directory_is_rejected(self):
        good = archive([("worker.json", b"{}")])
        central = good.index(b"PK\x01\x02")
        data = bytearray(good[:central] + bytes(60) + good[central:])
        struct.pack_into("<L", data, len(data) - 6, central + 60)
        with self.assertRaisesRegex(results.ValidationError, "missing worker metadata or extra data"):
            results.members(bytes(data))

    def test_hidden_zip64_locator_is_rejected_before_zipfile_allocation(self):
        good = archive([("worker.json", b"{}")])
        central = good.index(b"PK\x01\x02")
        end = len(good) - 22
        zip64 = struct.pack("<4sQ2H2L4Q", b"PK\x06\x06", 44, 45, 45, 0, 0, 1, 1, end - central, central)
        locator = struct.pack("<4sLQL", b"PK\x06\x07", 0, end, 1)
        data = bytearray(good[:end] + zip64 + locator + good[end:])
        struct.pack_into("<H", data, central + 28, len("worker.json") + len(zip64) + len(locator))
        struct.pack_into("<L", data, len(data) - 10, end - central + len(zip64) + len(locator))
        with patch.object(results.zipfile, "ZipFile") as parser:
            with self.assertRaisesRegex(results.ValidationError, "unsupported ZIP64 records"):
                results.members(bytes(data))
            parser.assert_not_called()


if __name__ == "__main__":
    unittest.main()
