"""Matrix diagnostics: each copy's resources by file set and scenario; never a second verdict."""

import copy
import unittest

from . import common, production_diagnostics as diagnostics
from .json_bytes import encoded
from .test_matrix_results import matrix_record
from .test_production_diagnostics import inputs, phase_summary

SUITE = "matrix-commission-local"
ENVIRONMENT = inputs()[1]["context"]["environment"]
WORKER = dict(inputs()[2], window=dict(runtime_seconds=10800), command_timeout_seconds=3600)
FAILED = dict(status="failed", stage="copy", message="copy 1 failed with exit codes [3]: command failed")


def public(raw, *, valid=True):
    """The fields of a matrix public record that its diagnostics are checked against."""
    cells = [dict(case_id=cell["case_id"], scenario=cell["scenario"], outcome=cell["outcome"]["status"],
                  stage=cell["outcome"]["stage"], message=cell["outcome"]["message"]) for cell in raw["cells"]]
    return dict(run_key="repo-655073755-run-42458459014", retirement_sha256="a" * 64, benchmark_valid=valid,
        cells=cells, measurements=[dict(case_id=cell["case_id"], scenario=cell["scenario"], variant_id="rcp-default",
            samples=list(cell["summary"]["samples"])) for cell in raw["cells"]
            if valid and cell["outcome"]["status"] == "complete"])


def derived(outcomes=None, **context):
    raw = matrix_record(SUITE, outcomes, environment=ENVIRONMENT, **context)
    record = public(raw)
    return diagnostics.derive(record, b"archive", raw, WORKER), record, raw


class MatrixDiagnosticsTests(unittest.TestCase):
    def test_each_copy_keeps_its_scenario_and_process_metrics(self):
        value, record, _ = derived()
        self.assertEqual((value["schema"], value["status"], value["reason"]),
                         ("rcp-public-resource-diagnostics-v2", "complete", None))
        self.assertEqual([(item["case_id"], item["scenario"], item["variant_id"], item["iteration"], item["status"])
                          for item in value["trials"]],
            [(case, "local", "rcp-default", iteration, "complete")
             for case in ("commission-64m", "commission-10k") for iteration in (1, 2, 3)])
        self.assertEqual(value["trials"][0]["metrics"], dict(user_seconds=2.5, system_seconds=880.0, max_rss_kib=90000,
            voluntary_switches=10, involuntary_switches=5, minor_faults=100, major_faults=0))
        self.assertEqual(value["cases"], [dict(case_id=case, definition_status="recorded", copy_trials=3,
            successful_copies=3, phase_boundary_recorded=False) for case in ("commission-64m", "commission-10k")])
        self.assertEqual(value["limits"], dict(allocation_seconds=10800, command_timeout_seconds=3600))
        self.assertEqual(diagnostics.validate(copy.deepcopy(value), record), value)

    def test_the_setup_says_copies_were_not_verified_and_not_paired(self):
        value, _, _ = derived()
        setup = value["setup"]
        self.assertEqual((setup["verification_policy"], setup["pairing_policy"], setup["cache_policy"]),
                         ("exit-status-summary-v1", None, "linux-drop-caches"))
        self.assertEqual((setup["cpu_model"], setup["source"]["layout"], setup["source"]["filesystem"]),
                         ("Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz", "two-disjoint-raid0", "ext4"))
        rows = {label: (text, kind) for _, label, text, kind in diagnostics.setup_rows(setup)}
        # a change of method is marked on a trend, never a reason to start a new line
        self.assertEqual(rows["Copy check"], ("exit-status-summary-v1", "software"))
        self.assertNotIn("Verification", rows)

    def test_a_failed_cell_keeps_the_copies_it_made_without_metrics(self):
        value, record, _ = derived({1: FAILED})
        self.assertEqual(value["status"], "partial")
        self.assertEqual([item["status"] for item in value["trials"]], ["complete"] * 3 + ["unavailable"])
        self.assertIsNone(value["trials"][3]["metrics"])
        self.assertEqual(value["cases"][1], dict(case_id="commission-10k", definition_status="recorded",
            copy_trials=1, successful_copies=0, phase_boundary_recorded=False))
        diagnostics.validate(value, record)

    def test_a_cell_that_was_never_started_still_appears_in_the_progress(self):
        value, _, _ = derived({1: dict(status="not_run", stage=None, message="allocation ended before this cell")})
        self.assertEqual(len(value["trials"]), 3)
        self.assertEqual(value["cases"][1], dict(case_id="commission-10k", definition_status="recorded",
            copy_trials=0, successful_copies=0, phase_boundary_recorded=False))

    def test_a_failed_run_whose_cells_were_reported_keeps_its_copies(self):
        raw = matrix_record(SUITE, {0: FAILED, 1: dict(status="not_run", stage=None, message="not started")},
                            environment=ENVIRONMENT)
        record = public(raw, valid=False)
        self.assertEqual((record["benchmark_valid"], record["measurements"], len(record["cells"])), (False, [], 2))
        value = diagnostics.derive(record, b"archive", raw, WORKER)
        self.assertEqual((value["status"], [item["status"] for item in value["trials"]]), ("partial", ["unavailable"]))
        diagnostics.validate(value, record)

    def test_nothing_is_published_about_cells_the_collector_did_not_report(self):
        # a run whose result failed its contract has no cells in its record: what that result
        # says about itself, including names it chose, stays out of the public diagnostics
        raw = matrix_record(SUITE, environment=ENVIRONMENT)
        for cell in raw["cells"]:
            cell["scenario"] = "words-chosen-by-the-candidate"
        unqualified = dict(public(raw, valid=False), cells=[])
        value = diagnostics.derive(unqualified, b"archive", raw, WORKER)
        self.assertEqual((value["status"], value["reason"], value["trials"], value["cases"], value["setup"]),
                         ("unavailable", "not_matched", [], [], None))
        # and a stored companion cannot name a cell its record does not have
        good, record, _ = derived()
        foreign = copy.deepcopy(good)
        for trial in foreign["trials"][:3]:
            trial["scenario"] = "remote"
        with self.assertRaisesRegex(ValueError, "invalid public resource trial"):
            diagnostics.validate(foreign, record)

    def test_published_samples_must_be_the_recorded_copies(self):
        raw = matrix_record(SUITE, environment=ENVIRONMENT)
        record = public(raw)
        record["measurements"][0]["samples"][0] += 1
        value = diagnostics.derive(record, b"archive", raw, WORKER)
        self.assertEqual((value["schema"], value["status"], value["reason"], value["trials"]),
                         ("rcp-public-resource-diagnostics-v2", "unavailable", "not_matched", []))

    def test_good_copies_of_an_unfinished_cell_are_reported_but_never_expected_as_samples(self):
        raw = matrix_record(SUITE, environment=ENVIRONMENT)
        cell = raw["cells"][1]
        cell.update(outcome=dict(status="interrupted", stage="copy", message="run stopped after copy 1 of 3"),
                    copies=cell["copies"][:1], summary=None)
        raw["status"] = "partial"
        record = public(raw)
        self.assertEqual([item["case_id"] for item in record["measurements"]], ["commission-64m"])
        value = diagnostics.derive(record, b"archive", raw, WORKER)
        self.assertEqual((value["status"], len(value["trials"])), ("complete", 4))
        self.assertEqual(value["cases"][1]["successful_copies"], 1)

    def test_a_document_that_is_not_a_valid_matrix_result_yields_no_diagnostics(self):
        raw = matrix_record(SUITE, environment=ENVIRONMENT)
        record = public(raw)
        broken = copy.deepcopy(raw)
        broken["cells"][0]["summary"]["median"] = 0
        for source in (broken, inputs()[1], dict(schema="rcp-benchmark-matrix-v1")):
            value = diagnostics.derive(record, b"archive", source, WORKER)
            self.assertEqual((value["schema"], value["status"], value["reason"]),
                             ("rcp-public-resource-diagnostics-v2", "unavailable", "invalid_diagnostics"))

    def test_sampled_phases_are_attached_to_the_run_and_to_each_copy(self):
        raw = matrix_record(SUITE, environment=ENVIRONMENT, system_telemetry=diagnostics.TELEMETRY_DESCRIPTOR)
        raw["system_telemetry"] = dict(phase_summary(), phases=[
            dict(phase_summary()["phases"][0], phase="generation", case_index=1, trial_index=None)])
        for cell in raw["cells"]:
            for item in cell["copies"]:
                item["system_telemetry"] = dict(phase_summary(), phases=[
                    dict(phase_summary()["phases"][0], trial_index=item["trial_index"])])
        record = public(raw)
        value = diagnostics.derive(record, b"archive", raw, WORKER)
        self.assertEqual(value["system"]["phases"][0]["phase"], "generation")
        self.assertEqual([item["system"]["phases"][0]["trial_index"] for item in value["trials"]], list(range(6)))
        self.assertEqual([case["phase_boundary_recorded"] for case in value["cases"]], [True, True])
        self.assertEqual(value["setup"]["instrumentation"], diagnostics.TELEMETRY_DESCRIPTOR)
        diagnostics.validate(value, record)

    def test_the_two_diagnostic_versions_do_not_stand_in_for_each_other(self):
        value, record, _ = derived()
        legacy_record, legacy_raw, worker = inputs()
        legacy = diagnostics.derive(legacy_record, b"archive", legacy_raw, worker)
        self.assertEqual(legacy["schema"], "rcp-public-resource-diagnostics-v1")
        self.assertNotIn("scenario", legacy["trials"][0])
        for document, owner in ((legacy, record), (value, legacy_record)):
            with self.assertRaisesRegex(ValueError, "invalid public diagnostic identity"):
                diagnostics.validate(copy.deepcopy(document), owner)
        self.assertEqual(diagnostics.unavailable(record)["schema"], "rcp-public-resource-diagnostics-v2")
        self.assertEqual(diagnostics.unavailable(legacy_record)["schema"], "rcp-public-resource-diagnostics-v1")
        diagnostics.validate(diagnostics.unavailable(record, "raw_budget"), record)

    def test_validation_rejects_copies_that_cannot_be_told_apart(self):
        value, record, _ = derived()
        changes = (
            lambda item: item["trials"][0].pop("scenario"),
            lambda item: item["trials"][1].update(iteration=1),
            lambda item: item["trials"][0].update(scenario="Local copy"),
            lambda item: item["trials"][0].update(variant_id="rcp-baseline"),
            lambda item: item["trials"][0].update(case_id="bulk-9g"),
            lambda item: item["cases"][0].update(case_id="bulk-9g"),
            lambda item: item["setup"].update(verification_policy="trust-me"),
        )
        for change in changes:
            broken = copy.deepcopy(value)
            change(broken)
            if broken["setup"] != value["setup"]:
                # a fingerprint that matches the changed setup, so only the setup rule can refuse it
                broken["setup_id"] = common.digest(encoded(broken["setup"]))
            with self.subTest(change=change), self.assertRaises(ValueError) as refused:
                diagnostics.validate(broken, record)
            self.assertNotIn("fingerprint", str(refused.exception))

    def test_a_paired_result_cannot_claim_the_matrix_rule_in_its_setup(self):
        # version 1 documents derive exactly what they derived before the matrix existed
        record, raw, worker = inputs()
        raw["context"]["verification"] = dict(policy="exit-status-summary-v1")
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertEqual((value["schema"], value["setup"]["verification_policy"]),
                         ("rcp-public-resource-diagnostics-v1", None))

    def test_the_run_page_lists_copies_by_file_set_and_scenario(self):
        value, record, _ = derived({1: FAILED})
        text = diagnostics.render(value, record)
        self.assertIn("<th>File set</th><th>Scenario</th><th>Copy</th>", text)
        self.assertIn("<tr><td><code>commission-64m</code></td><td><code>local</code></td><td>1</td>"
                      "<td>complete</td><td>2.500</td><td>880.000</td><td>90,000</td>", text)
        self.assertIn("<tr><td><code>commission-10k</code></td><td><code>local</code></td><td>1</td>"
                      "<td>unavailable</td>", text)
        self.assertNotIn("Variant", text)
        self.assertNotIn("rcp-baseline", text)


if __name__ == "__main__":
    unittest.main()
