"""Matrix runs are qualified per cell: one failed cell never discards the others."""

import copy
import json
from pathlib import Path
import statistics
import tempfile
import unittest
from unittest import mock

from benchmarks import matrix, run
from . import common, production_projection as projection, results
from .test_production_collection import Fixture

ROOT = Path(__file__).resolve().parents[2]
ROLE_METRICS = dict(source=dict(user_seconds=300.0, system_seconds=450.0, max_rss_kib=50000),
                    destination=dict(user_seconds=200.0, system_seconds=700.0, max_rss_kib=30000))
METRICS = dict(user_seconds=2.5, system_seconds=880.0, max_rss_kib=90000, voluntary_switches=10,
               involuntary_switches=5, minor_faults=100, major_faults=0, exit_code=0)


def matrix_record(suite, outcomes=None, **context_changes):
    """Build the document the runner writes for a suite; `outcomes` replaces cell outcomes by index."""
    contract = common.matrix_contract(suite)
    manifest = run.load_manifest(ROOT / "benchmarks/matrix-cases.json")
    cases = {case["id"]: case for case in manifest["cases"]}
    variant = manifest["variants"][0]
    context = dict(runner_label=contract["runner_label"], purpose=contract["purpose"],
        cache_policy=contract["cache_policy"], repetitions=contract["repetitions"],
        timeout_seconds=float(contract["timeout"]), verification=dict(common.verification_contract(suite)),
        source="/mnt/rcp-benchmark/source", destination="/mnt/rcp-benchmark/destination",
        storage_ids=dict(source="c5d-raid0-ext4-source-v1", destination="c5d-raid0-ext4-destination-v1"),
        repository="wykurz/rcp", run_url="", environment={},
        summary_format="json", resources=dict(policy=matrix.RESOURCE_POLICY))
    context.update(context_changes)
    cells, file_sets, trial = [], [], 0
    for index, (case_id, scenario) in enumerate(contract["cells"]):
        if all(item["case"]["id"] != case_id for item in file_sets):
            file_sets.append(dict(case=cases[case_id], expected_counts=matrix.operations.expected_counts(cases[case_id]),
                                  generation=dict(status="ok", elapsed_seconds=10.0, exit_code=0, message=None)))
        outcome = dict(status="complete", stage=None, message=None) | (outcomes or {}).get(index, {})
        copies = []
        for iteration in range(1, (context["repetitions"] if outcome["status"] == "complete" else 1) + 1):
            done = outcome["status"] == "complete"
            copies.append(dict(iteration=iteration, trial_index=trial, status="ok" if done else "failed",
                elapsed_seconds=600.0 + index + iteration / 10, exit_codes=[0] if done else [3], timed_out=False,
                command=["rcp", "--summary", matrix.SUMMARY_FORMAT, "source", "destination"], logs=[]))
            if done:
                # only a copy that finished has a summary to take figures from
                copies[-1]["resources"] = dict(status="complete", metrics=dict(METRICS))
                if scenario == "remote":
                    # the summary of a remote copy also carries what each daemon reported
                    copies[-1]["role_resources"] = {role: dict(status="complete", metrics=dict(METRICS, **figures))
                                                    for role, figures in ROLE_METRICS.items()}
            trial += 1
        if outcome["status"] == "not_run":
            copies = []
        samples = [item["elapsed_seconds"] for item in copies]
        cells.append(dict(case_id=case_id, scenario=scenario, variant=variant,
            cell_id=matrix.cell_id(cases[case_id], scenario, variant, context), outcome=outcome, copies=copies,
            summary=dict(unit="seconds", median=statistics.median(samples), minimum=min(samples),
                         maximum=max(samples), samples=samples) if outcome["status"] == "complete" else None))
    complete = sum(cell["outcome"]["status"] == "complete" for cell in cells)
    record = dict(schema=matrix.SCHEMA, run_id="0" * 32, timestamp="2026-11-20T08:30:00+00:00",
        status="complete" if complete == len(cells) else "partial" if complete else "failed",
        revision=dict(commit=None, branch=None, dirty=None), context=context,
        tools={name: dict(path="/run/rcp-benchmark/candidate/" + name, version=name + " 1", sha256="d" * 64)
               for name in ("filegen", "rcp", "rcpd")},
        file_sets=file_sets, cells=cells, phase_seconds=dict(total=1.0))
    return matrix.validate(record)


class MatrixCollectionTests(unittest.TestCase):
    def collect(self, suite, record, **options):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(temporary, suite=suite, record=record, **options)
            result = fixture.collector.collect(retire=True)
            report = json.loads(fixture.s3.objects[fixture.collector.prefix + "report.json"]["1"])
        return result, report, report["public_projection"]

    def test_a_complete_run_passes_and_publishes_every_cell(self):
        record = matrix_record("matrix-1g-local")
        result, report, public = self.collect("matrix-1g-local", record)
        self.assertEqual((result["state"], result["retired"]), ("success", True), result)
        self.assertEqual((report["benchmark_valid"], report["qualification_outcome"]), (True, "passed"))
        self.assertEqual(report["validation"], "not verified; exit status and reported transfer counts only")
        self.assertEqual(public["schema"], "rcp-production-public-report-v4")
        self.assertEqual(public["cells"], [dict(case_id="bulk-1g-1tb", scenario="local", outcome="complete",
                                                stage=None, message=None)])
        samples = [item["elapsed_seconds"] for item in record["cells"][0]["copies"]]
        measurement, = public["measurements"]
        self.assertEqual({key: measurement[key] for key in ("case_id", "variant_id", "scenario", "median", "samples")},
            dict(case_id="bulk-1g-1tb", variant_id="rcp-default", scenario="local",
                 median=statistics.median(samples), samples=samples))
        self.assertRegex(measurement["series_id"], r"^[0-9a-f]{64}$")
        self.assertEqual(public["validation"],
            "Not verified: a copy counts when rcp exits cleanly and reports the expected file count.")
        self.assertEqual(public["verification"], {"policy": "exit-status-summary-v1"})
        self.assertEqual(public["timestamp"], record["timestamp"])
        self.assertEqual(public["note"], "Each measurement is the median of 3 copies.")
        self.assertEqual(public["diagnostics"], [])
        self.assertEqual(projection.outcome_label(projection.bind(public, "a" * 64)), "1 of 1 measurement completed")

    def test_a_build_that_cannot_report_its_figures_still_qualifies_on_the_same_trend(self):
        record = matrix_record("matrix-commission-local")
        # what the runner writes for a revision from before rcp could print its summary as JSON
        del record["context"]["resources"]
        record["context"]["summary_format"] = "text"
        for cell in record["cells"]:
            for item in cell["copies"]:
                item["command"].remove(matrix.SUMMARY_FORMAT)
                del item["resources"]
        result, report, public = self.collect("matrix-commission-local", record)
        self.assertEqual((result["state"], report["benchmark_valid"], report["qualification_outcome"]),
                         ("success", True, "passed"), result)
        self.assertEqual(len(public["measurements"]), 2)
        # a cell's trend is its definition: what a build can say about itself does not start a new line
        usual = self.collect("matrix-commission-local", matrix_record("matrix-commission-local"))[2]
        self.assertEqual([item["series_id"] for item in public["measurements"]],
                         [item["series_id"] for item in usual["measurements"]])

    def test_a_failed_cell_is_reported_and_the_measured_cell_still_counts(self):
        record = matrix_record("matrix-commission-local", {1: dict(status="failed", stage="copy",
            message="copy 1 failed with exit codes [3]: command failed")})
        result, report, public = self.collect("matrix-commission-local", record)
        self.assertEqual((result["state"], report["benchmark_valid"], report["qualification_outcome"]),
                         ("success", True, "passed"))
        self.assertEqual([(cell["case_id"], cell["outcome"]) for cell in public["cells"]],
                         [("commission-64m", "complete"), ("commission-10k", "failed")])
        self.assertEqual((public["cells"][1]["stage"], public["cells"][1]["message"]),
                         ("copy", "copy 1 failed with exit codes [3]: command failed"))
        self.assertEqual([item["case_id"] for item in public["measurements"]], ["commission-64m"])
        self.assertEqual(public["diagnostics"], [])
        self.assertEqual(projection.outcome_label(projection.bind(public, "a" * 64)), "1 of 2 measurements completed")

    def test_a_run_without_a_measured_cell_fails_and_still_says_why(self):
        record = matrix_record("matrix-commission-local", {
            0: dict(status="timed_out", stage="copy", message="copy 1 did not finish within 3600 s"),
            1: dict(status="not_run", stage=None, message="allocation ended before this cell")})
        result, report, public = self.collect("matrix-commission-local", record)
        self.assertEqual((report["benchmark_valid"], report["qualification_outcome"]), (False, "failed"))
        self.assertEqual(public["measurements"], [])
        self.assertNotIn("verification", public)
        self.assertEqual([cell["outcome"] for cell in public["cells"]], ["timed_out", "not_run"])
        self.assertEqual(public["cells"][0]["message"], "copy 1 did not finish within 3600 s")
        self.assertEqual(public["diagnostics"], ["qualification: no measurement completed"])
        self.assertEqual(public["note"], "Safely retired failed qualification; excluded from performance history.")
        self.assertEqual(projection.outcome_label(projection.bind(public, "a" * 64)),
                         "failed; excluded from performance history")

    def test_a_result_that_differs_from_the_suite_contract_is_not_published(self):
        drifted = [
            matrix_record("matrix-1g-local", repetitions=2),
            matrix_record("matrix-1g-local", timeout_seconds=60.0),
            matrix_record("matrix-1g-local", runner_label="other"),
            matrix_record("matrix-1g-local", cache_policy="uncontrolled"),
            matrix_record("matrix-1g-local", repository="someone/else"),
            matrix_record("matrix-1g-local", purpose="commissioning"),
            matrix_record("matrix-100m-local"),
        ]
        for record in drifted:
            with self.subTest(context=record["context"], cells=[cell["case_id"] for cell in record["cells"]]):
                _, report, public = self.collect("matrix-1g-local", record)
                self.assertEqual((report["benchmark_valid"], public["measurements"]), (False, []))
                # what an unapproved document says about its cells is not published either
                self.assertEqual(public["cells"], [])
                self.assertEqual(public["diagnostics"], ["qualification: unexpected matrix measurement contract"])

    def test_cells_in_another_order_are_not_the_suite(self):
        record = matrix_record("matrix-commission-local")
        record["cells"].reverse()
        _, report, public = self.collect("matrix-commission-local", record)
        self.assertEqual((report["benchmark_valid"], public["cells"]), (False, []))
        self.assertEqual(public["diagnostics"], ["qualification: unexpected matrix measurement contract"])

    def test_measured_tools_must_be_the_approved_binaries(self):
        record = matrix_record("matrix-1g-local")
        record["tools"]["rcp"]["sha256"] = "e" * 64
        _, report, public = self.collect("matrix-1g-local", record)
        self.assertFalse(report["benchmark_valid"])
        self.assertEqual(public["diagnostics"], ["qualification: measured tool does not match approved binary digest"])
        self.assertEqual(public["cells"], [])

    def test_a_legacy_document_cannot_stand_in_for_a_matrix_run(self):
        from .test_results import binding, measurement
        legacy = measurement(dict(binding("bulk-1g-metadata"), candidate="a" * 40))
        _, report, public = self.collect("matrix-1g-local", legacy)
        self.assertFalse(report["benchmark_valid"])
        self.assertEqual(public["cells"], [])
        self.assertEqual(public["measurements"], [])

    def test_cell_messages_are_bounded_printable_text(self):
        record = matrix_record("matrix-commission-local", {1: dict(status="failed", stage="cache" + "x" * 500,
            message="<script>alert(1)</script>\x00\x1b[31m" + "y" * 5000)})
        _, _, public = self.collect("matrix-commission-local", record)
        cell = public["cells"][1]
        self.assertLessEqual(len(cell["message"]), 256)
        self.assertLessEqual(len(cell["stage"]), 32)
        self.assertTrue(cell["message"].isprintable())
        self.assertTrue(cell["message"].startswith("<script>alert(1)</script>??[31m"))
        projection.bind(public, "a" * 64)

    def test_the_eight_hour_suite_qualifies_with_its_own_headroom(self):
        record = matrix_record("matrix-all-local", {2: dict(status="not_run", stage=None,
                                                            message="allocation ended before this cell")})
        result, report, public = self.collect("matrix-all-local", record, remaining=28300,
                                              runtime_seconds=28800, capacity_seconds=7200)
        self.assertEqual((result["state"], report["benchmark_valid"]), ("success", True), public["diagnostics"])
        self.assertEqual([cell["outcome"] for cell in public["cells"]], ["complete", "complete", "not_run", "complete"])
        self.assertEqual(len(public["measurements"]), 3)
        self.assertEqual(public["note"], "Each measurement is the median of 3 copies.")

    def test_a_run_that_was_stopped_keeps_the_cells_it_finished(self):
        record = matrix_record("matrix-commission-local", {1: dict(status="interrupted", stage="copy",
            message="run stopped during copy 2 of 3")})
        record.update(status="failed", error="SIGTERM")
        result, report, public = self.collect("matrix-commission-local", record, returncode=1)
        self.assertEqual((result["state"], report["benchmark_valid"], report["qualification_outcome"]),
                         ("success", True, "passed"), public["diagnostics"])
        self.assertEqual([cell["outcome"] for cell in public["cells"]], ["complete", "interrupted"])
        self.assertEqual([item["case_id"] for item in public["measurements"]], ["commission-64m"])
        self.assertEqual(public["diagnostics"], ["worker: the benchmark command did not exit cleanly"])

    def test_a_document_saved_mid_run_counts_for_what_it_finished(self):
        record = matrix_record("matrix-commission-local", {1: dict(status="not_run", stage=None, message="not started")})
        record["status"] = "running"
        _, report, public = self.collect("matrix-commission-local", record, returncode=143)
        self.assertTrue(report["benchmark_valid"])
        self.assertEqual([(cell["outcome"], cell["message"]) for cell in public["cells"]],
                         [("complete", None), ("not_run", "not started")])

    def test_a_document_that_fails_validation_says_which_field(self):
        record = matrix_record("matrix-1g-local")
        record["cells"][0]["summary"]["median"] = 1.0
        _, report, public = self.collect("matrix-1g-local", record)
        self.assertFalse(report["benchmark_valid"])
        self.assertEqual(public["diagnostics"][0], "measurement record: cells[0].summary does not match its copies")
        self.assertEqual((public["cells"], public["measurements"]), ([], []))

    def test_the_retained_summary_lists_every_cell_and_says_copies_are_not_verified(self):
        record = matrix_record("matrix-commission-local", {1: dict(status="failed", stage="copy",
            message="copy 1 failed with exit codes [3]: <b>bold</b>")})
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(temporary, suite="matrix-commission-local", record=record)
            fixture.collector.collect(retire=True)
            report = json.loads(fixture.s3.objects[fixture.collector.prefix + "report.json"]["1"])
            summary = fixture.s3.objects[fixture.collector.prefix + "summary.html"]["1"].decode()
        self.assertEqual(report["presentation"], dict(report="full", summary="full"))
        self.assertIn("<code>Not verified; exit status and reported transfer counts only</code>", summary)
        median = f"{record['cells'][0]['summary']['median']:.6g}"
        self.assertIn("<tr><td><code>commission-64m</code></td><td><code>local</code></td><td><code>complete</code></td>"
                      "<td><code>" + median + "</code></td><td><code>3</code></td><td><code></code></td></tr>", summary)
        self.assertIn("<td><code>failed</code></td><td><code>-</code></td><td><code>0</code></td>"
                      "<td><code>copy: copy 1 failed with exit codes [3]: &lt;b&gt;bold&lt;/b&gt;</code></td>", summary)
        self.assertNotIn("<b>", summary)
        self.assertIn("<code>1 of 2 measurements completed</code>", summary)
        self.assertNotIn("Baseline", summary)
        self.assertNotIn("Comparison", summary)

    def test_a_result_the_public_record_cannot_hold_fails_the_run_instead_of_blocking_collection(self):
        # a run that cannot be collected keeps the one active slot, and so every later run, waiting
        boolean = matrix_record("matrix-1g-local")
        for item in boolean["cells"][0]["copies"]:
            item["elapsed_seconds"] = 1
        boolean["cells"][0]["summary"].update(median=True, minimum=1, maximum=1, samples=[True, True, True])
        separator = matrix_record("matrix-1g-local")
        separator["timestamp"] = "2026-11-20Z08:30:00+00:00"
        for record, reason in ((boolean, "measurement record: cells[0].summary.median must be a finite non-negative number"),
                               (separator, "measurement record: timestamp must be an ISO 8601 UTC time")):
            with self.subTest(reason=reason):
                result, report, public = self.collect("matrix-1g-local", record)
                self.assertEqual((result["state"], result["retired"], report["benchmark_valid"]), ("failed", True, False))
                self.assertEqual(public["diagnostics"][0], reason)

    def test_a_qualified_result_that_does_not_fit_the_public_record_is_failed_not_held(self):
        # whatever the runner's validator and the public format disagree about in future must
        # cost one run, never the service: here the first validator is simply switched off
        record = matrix_record("matrix-1g-local")
        for item in record["cells"][0]["copies"]:
            item["elapsed_seconds"] = 1
        record["cells"][0]["summary"].update(median=True, minimum=1, maximum=1, samples=[True, True, True])
        with mock.patch.object(results.matrix, "validate", side_effect=lambda value: value):
            result, report, public = self.collect("matrix-1g-local", record)
        self.assertEqual((result["state"], result["retired"]), ("failed", True), result)
        self.assertEqual((report["benchmark_valid"], report["qualification_outcome"]), (False, "failed"))
        self.assertEqual((public["cells"], public["measurements"]), ([], []))
        self.assertEqual(public["diagnostics"], ["qualification: result does not fit the public record"])
        self.assertEqual(report["evidence"]["cells"], [])

    def test_a_supervisor_record_of_the_wrong_shape_fails_qualification_instead_of_raising(self):
        # an exception that escapes qualification leaves the run held; a wrong shape is a failed run
        identity = dict(suite="matrix-1g-local")
        policy = dict(schema_version=1, identity=identity, archive_sha256="0" * 64,
                      ami_id="ami-" + "2" * 17, harness_revision="f" * 40)
        evidence = dict(identity=identity, errors=[], files={"results.zip": "0" * 64}, producer_conclusion="success",
            controller=dict(outcome="success", cleanup="verified", request=None, window={},
                instance=dict(image_id=policy["ami_id"], instance_type="c5d.24xlarge")),
            worker=dict(status=dict(success=True), harness_revision=policy["harness_revision"], admission="yes"))
        self.assertFalse(results.qualify(evidence, matrix_record("matrix-1g-local"), policy, producer_role="submission"))
        self.assertEqual(evidence["errors"], ["qualification: AttributeError"])
        self.assertEqual(evidence["cells"], [])

    def test_an_interrupted_collection_resumes_even_when_a_note_has_been_reworded_since(self):
        # the record's cells and numbers are identity; the wording of a run note is not
        record = matrix_record("matrix-commission-local", {
            0: dict(status="timed_out", stage="copy", message="copy 1 did not finish within 3600 s"),
            1: dict(status="not_run", stage=None, message="allocation ended before this cell")})
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(temporary, suite="matrix-commission-local", record=record)
            key, put = fixture.collector.prefix + "manifest.json", fixture.s3.put_object
            def interrupted(**request):
                if request["Key"] == key:
                    raise TimeoutError("injected interruption")
                return put(**request)
            with mock.patch.object(fixture.s3, "put_object", side_effect=interrupted):
                self.assertEqual(fixture.collector.collect(retire=True)["state"], "held")
            saved = json.loads(fixture.s3.objects[fixture.collector.prefix + "report.json"]["1"])
            freeze = projection.freeze
            def reworded(*arguments, **options):
                value = freeze(*arguments, **options)
                return dict(value, diagnostics=["qualification - nothing was measured"])
            with mock.patch.object(projection, "freeze", side_effect=reworded):
                result = fixture.collector.collect(retire=True)
            self.assertEqual((result["state"], result["retired"]), ("failed", True), result)
            kept = json.loads(fixture.s3.objects[fixture.collector.prefix + "report.json"]["1"])
            self.assertEqual(kept["public_projection"], saved["public_projection"])
            self.assertEqual(kept["public_projection"]["diagnostics"], ["qualification: no measurement completed"])
            # anything else that differs is still a different record
            fixture = Fixture(temporary, suite="matrix-commission-local", record=record)
            with mock.patch.object(fixture.s3, "put_object", side_effect=interrupted):
                fixture.collector.collect(retire=True)
            def other_cells(*arguments, **options):
                value = freeze(*arguments, **options)
                return dict(value, cells=value["cells"][:1])
            with mock.patch.object(projection, "freeze", side_effect=other_cells):
                held = fixture.collector.collect(retire=True)
            self.assertEqual((held["state"], held["retired"]), ("held", False))

    def test_a_report_too_large_to_keep_whole_still_carries_its_cells(self):
        from . import production_collection as collection
        record = matrix_record("matrix-commission-local", {1: dict(status="failed", stage="copy", message="x" * 200)})
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(temporary, suite="matrix-commission-local", record=record)
            fixture.collector.collect(retire=True)
            whole = len(fixture.s3.objects[fixture.collector.prefix + "report.json"]["1"])
            # the same run again, with a limit just too small for the whole report
            fixture = Fixture(temporary, suite="matrix-commission-local", record=record)
            with mock.patch.object(collection, "MAX_REPORT", whole), \
                    mock.patch.object(collection, "CHECKPOINT_RESERVE", whole // 2):
                result = fixture.collector.collect(retire=True)
            report = json.loads(fixture.s3.objects[fixture.collector.prefix + "report.json"]["1"])
        self.assertEqual((result["state"], result["retired"]), ("success", True), result)
        self.assertEqual(report["presentation"]["report"], "bounded-diagnostic")
        self.assertEqual(report["evidence"]["cells"], report["public_projection"]["cells"])
        self.assertEqual(len(report["evidence"]["cells"]), 2)

    def test_the_offline_report_tool_reads_a_matrix_result(self):
        from .test_results import binding, bundle
        expected = binding("matrix-commission-local")
        record = matrix_record("matrix-commission-local", {1: dict(status="failed", stage="copy",
            message="copy 1 failed with exit code 3; see 0.stdout.log and 0.stderr.log in "
                    "logs/commission-10k/local/1")},
            repository=expected["repository"])
        with tempfile.TemporaryDirectory() as temporary:
            source, output = Path(temporary) / "input", Path(temporary) / "report"
            bundle(source, expected, raw=record)
            evidence = results.ingest(source, output, expected, "success")
            summary = (output / "summary.md").read_text()
            self.assertEqual(evidence["errors"], [])
            self.assertIn("<code>1 of 2 measurements completed</code>", summary)
            self.assertIn("<td><code>commission-10k</code></td><td><code>local</code></td><td><code>failed</code></td>", summary)
            # the dashboard reads the paired harness format only; a matrix result has none
            self.assertFalse((output / "derived.json").exists())
            self.assertFalse((output / "dashboard").exists())
            self.assertTrue((output / "index.html").exists())
            self.assertNotIn("dashboard", (output / "index.html").read_text())

    def test_other_suites_keep_their_record_version_and_rules(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(temporary)
            fixture.collector.collect(retire=True)
            passed = json.loads(fixture.s3.objects[fixture.collector.prefix + "report.json"]["1"])
        self.assertEqual(passed["public_projection"]["schema"], "rcp-production-public-report-v3")
        self.assertNotIn("cells", passed["public_projection"])
        self.assertEqual(passed["validation"], "metadata/size only; does not prove content integrity")
        # a paired run is all or nothing: a command that did not exit cleanly still fails it
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(temporary, returncode=1)
            fixture.collector.collect(retire=True)
            failed = json.loads(fixture.s3.objects[fixture.collector.prefix + "report.json"]["1"])
        self.assertEqual((failed["benchmark_valid"], failed["public_projection"]["measurements"]), (False, []))


class MatrixPublicRecordTests(unittest.TestCase):
    def public(self, outcomes=None):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(temporary, suite="matrix-commission-local",
                              record=matrix_record("matrix-commission-local", outcomes))
            fixture.collector.collect(retire=True)
            report = json.loads(fixture.s3.objects[fixture.collector.prefix + "report.json"]["1"])
        return projection.bind(report["public_projection"], "a" * 64)

    def test_measurements_and_cells_must_agree(self):
        value = self.public()
        projection.validate_public(value)
        # each rule is named, so removing one of them is noticed here
        changes = (
            (lambda record: record["measurements"].pop(), "differ from the completed cells"),
            (lambda record: record["cells"][0].update(outcome="failed"), "differ from the completed cells"),
            (lambda record: record["measurements"][0].update(scenario="remote"), "differ from the completed cells"),
            (lambda record: record["cells"].append(dict(record["cells"][0], outcome="failed")), "duplicate public cell"),
            (lambda record: record["cells"][1].update(outcome="passed"), "invalid public cells"),
            (lambda record: record["cells"][0].update(message="x" * 257), "invalid public cells"),
            (lambda record: record["cells"][1].update(case_id="Bulk 1G"), "invalid public cells"),
            (lambda record: record["cells"][1].update(stage="copy\x1b"), "invalid public cells"),
            (lambda record: record["measurements"][0].pop("scenario"), "differ from the completed cells"),
            (lambda record: record["measurements"][0].update(variant_id="rcp-baseline"), "invalid public measurements"),
            (lambda record: record.pop("cells"), "invalid public cells"),
            (lambda record: record.update(extra=1), "unexpected or unbounded public report fields"),
            (lambda record: record.update(diagnostics=["x" * 300]), "invalid public diagnostics"),
            (lambda record: record.update(diagnostics=["tab\there"]), "invalid public diagnostics"),
            (lambda record: record.update(diagnostics=["note"] * 17), "invalid public diagnostics"),
            (lambda record: record.update(cells=[], measurements=[]), "contradict the run outcome"),
            (lambda record: record.update(benchmark_valid=False, qualification_outcome="failed", measurements=[]),
             "contradict the run outcome"),
            # the record says how copies were checked in two places, which must agree
            (lambda record: record.update(verification={"policy": "sha256-tree-stream-v1"}), "public verification"),
            (lambda record: record["identity"].update(suite="bulk-1g-metadata"), "not a matrix suite"),
        )
        for change, message in changes:
            broken = copy.deepcopy(value)
            change(broken)
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                projection.validate_public(broken)

    def test_unavailable_evidence_has_no_cells_to_report(self):
        value = self.public({0: dict(status="failed", stage="copy", message="x"),
                             1: dict(status="failed", stage="copy", message="x")})
        self.assertEqual(value["qualification_outcome"], "failed")
        unknown = dict(value, qualification_outcome="evidence-unavailable")
        with self.assertRaisesRegex(ValueError, "contradict the run outcome"):
            projection.validate_public(unknown)
        projection.validate_public(dict(unknown, cells=[]))

    def test_the_frozen_limits_and_outcomes_of_version_four_are_pinned(self):
        from benchmarks import matrix
        # records already written must keep validating: these may grow, never shrink or diverge
        self.assertEqual((results.MAX_CELL_STAGE, results.MAX_CELL_MESSAGE, results.MAX_DIAGNOSTICS, projection.MAX_CELLS),
                         (32, 256, 16, 64))
        self.assertEqual(projection.CELL_OUTCOMES, matrix.CELL_OUTCOMES)
        self.assertEqual(projection.PUBLIC_CONTRACTS[projection.MATRIX_SCHEMA], dict(
            validation="Not verified: a copy counts when rcp exits cleanly and reports the expected file count.",
            series_contract="aws-local-ext4-v1", verification={"policy": matrix.POLICY}, variant=matrix.VARIANT))

    def test_a_note_that_bounds_to_nothing_is_left_out_instead_of_blocking_the_record(self):
        identity = dict(run="123-1", run_url="https://github.com/wykurz/rcp/actions/runs/123/attempts/1",
            suite="matrix-1g-local", baseline="a" * 40, candidate="a" * 40, controller="b" * 40)
        evidence = dict(controller={}, errors=["", "\x00\x01", "harness result unavailable"])
        value = projection.freeze("repo-1-run-123", identity, dict(ami="ami-" + "2" * 17, source="f" * 40), False,
                                  evidence, None, outcome="failed", collected_at="2026-11-20T10:00:00+00:00")
        # a record must always be writable: an unusable note costs the note, never the retirement
        self.assertEqual(value["diagnostics"], ["??", "harness result unavailable"])
        # evidence that never reached qualification has no cells to show
        self.assertEqual(value["cells"], [])

    def test_older_record_versions_still_validate_and_keep_their_labels(self):
        legacy = json.loads((Path(__file__).with_name("fixtures") / "production-public-report-v1.json").read_text())
        self.assertEqual(projection.validate_public(legacy), legacy)
        self.assertEqual(projection.outcome_label(legacy), "consistency checks passed")
        # the third version is what paired suites keep producing
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(temporary)
            fixture.collector.collect(retire=True)
            report = json.loads(fixture.s3.objects[fixture.collector.prefix + "report.json"]["1"])
        third = projection.bind(report["public_projection"], "a" * 64)
        self.assertEqual((third["schema"], projection.outcome_label(third)),
                         ("rcp-production-public-report-v3", "consistency checks passed"))
        self.assertEqual(set(third), {"schema", "run_key", "retirement_sha256", "benchmark_valid", "cleanup_verified",
            "validation", "identity", "comparison", "measurements", "series_contract", "note",
            "qualification_outcome", "collected_at", "verification", "timestamp"})
        # a matrix-shaped record cannot be passed off under an older version, or the reverse
        with self.assertRaisesRegex(ValueError, "unexpected or unbounded public report fields"):
            projection.validate_public(dict(third, cells=[], diagnostics=[]))


if __name__ == "__main__":
    unittest.main()
