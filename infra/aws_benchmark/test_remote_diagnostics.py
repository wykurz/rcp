"""A remote copy is three processes: its diagnostics report the rcp command and each daemon."""

import copy
import json
from pathlib import Path
import unittest

from . import production_diagnostics as diagnostics, results
from .json_bytes import encoded
from .test_matrix_diagnostics import ENVIRONMENT, FAILED, WORKER, public
from .test_matrix_results import matrix_record
from .test_production_diagnostics import phase_summary

SUITE = "matrix-commission-local+remote"
COMMAND = dict(user_seconds=2.5, system_seconds=880.0, max_rss_kib=90000, voluntary_switches=10,
               involuntary_switches=5, minor_faults=100, major_faults=0)
SOURCE = dict(COMMAND, user_seconds=300.0, system_seconds=450.0, max_rss_kib=50000)
DESTINATION = dict(COMMAND, user_seconds=200.0, system_seconds=700.0, max_rss_kib=30000)


def derived(outcomes=None, change=None, suite=SUITE):
    raw = matrix_record(suite, outcomes, environment=ENVIRONMENT)
    if change is not None:
        change(raw)
    record = public(raw)
    return diagnostics.derive(record, b"archive", raw, WORKER), record, raw


class RemoteDiagnosticsTests(unittest.TestCase):
    def test_a_remote_copy_reports_the_command_and_each_daemon(self):
        value, record, _ = derived()
        self.assertEqual((value["schema"], value["status"], value["reason"]),
                         ("rcp-public-resource-diagnostics-v3", "complete", None))
        self.assertEqual([(item["case_id"], item["scenario"], item["iteration"]) for item in value["trials"]],
                         [(case, scenario, iteration) for case in ("commission-64m", "commission-10k")
                          for scenario in ("local", "remote") for iteration in (1, 2, 3)])
        local, remote = value["trials"][0], value["trials"][3]
        self.assertEqual((local["metrics"], local["roles"]), (COMMAND, None))
        self.assertEqual((remote["metrics"], remote["roles"]), (COMMAND, dict(source=SOURCE, destination=DESTINATION)))
        self.assertEqual(set(remote), set(local))
        self.assertIn("source rcpd and the destination rcpd", value["process_scope"])
        self.assertEqual(diagnostics.validate(copy.deepcopy(value), record), value)

    def test_a_run_without_remote_cells_keeps_the_earlier_document(self):
        for suite in ("matrix-commission-local", "matrix-all-local"):
            raw = matrix_record(suite, environment=ENVIRONMENT)
            # the file sets of the full suite do not have the small fixtures' shapes
            value = diagnostics.derive(public(raw), b"archive", raw, WORKER)
            self.assertEqual(value["schema"], "rcp-public-resource-diagnostics-v2")
            self.assertTrue(all("roles" not in item for item in value["trials"]))
            self.assertEqual(value["process_scope"], diagnostics.PROCESS_SCOPE)

    def test_a_daemon_without_figures_is_unavailable_and_the_copy_still_counts(self):
        def lose(raw):
            copies = raw["cells"][1]["copies"]
            copies[0]["role_resources"]["source"] = dict(status="invalid", metrics=None, raw_sha256="0" * 64)
            del copies[1]["role_resources"]
            copies[2]["role_resources"] = "words"
        value, record, _ = derived(change=lose)
        first, second, third = value["trials"][3:6]
        self.assertEqual(first["roles"], dict(source=None, destination=DESTINATION))
        for trial in (second, third):
            self.assertEqual(trial["roles"], dict(source=None, destination=None))
        # the copy command's own figures are still there, and the gap is not hidden
        self.assertEqual({trial["status"] for trial in (first, second, third)}, {"complete"})
        self.assertEqual(value["status"], "partial")
        diagnostics.validate(value, record)

    def test_a_good_copy_without_its_own_figures_publishes_none_for_its_daemons(self):
        def lose(raw):
            raw["cells"][1]["copies"][0]["resources"] = dict(status="invalid", metrics=None, raw_sha256="0" * 64)
        value, record, raw = derived(change=lose)
        # the daemons' figures were recorded; they are withheld with the command's, never shown alone
        self.assertEqual({role["status"] for role in raw["cells"][1]["copies"][0]["role_resources"].values()},
                         {"complete"})
        trial = value["trials"][3]
        self.assertEqual((trial["status"], trial["metrics"], trial["roles"]),
                         ("unavailable", None, dict(source=None, destination=None)))
        self.assertEqual(value["status"], "partial")
        diagnostics.validate(value, record)

    def test_a_companion_stored_before_remote_cells_existed_stays_valid_and_is_derived_alike(self):
        frozen = json.loads((Path(__file__).with_name("fixtures") / "production-diagnostics-v2.json").read_text())
        record, companion = frozen["record"], frozen["companion"]
        self.assertEqual(companion["schema"], "rcp-public-resource-diagnostics-v2")
        self.assertEqual(diagnostics.validate(copy.deepcopy(companion), record), companion)
        # the same worker result today: the same bytes, not merely an equivalent document
        raw = matrix_record("matrix-commission-local", {1: dict(status="failed", stage="copy", message="copy 1 failed")},
                            environment=ENVIRONMENT, system_telemetry=diagnostics.TELEMETRY_DESCRIPTOR)
        raw["system_telemetry"] = dict(phase_summary(), phases=[
            dict(phase_summary()["phases"][0], phase="generation", case_index=1, trial_index=None)])
        for cell in raw["cells"]:
            for item in cell["copies"]:
                item["system_telemetry"] = dict(phase_summary(), phases=[
                    dict(phase_summary()["phases"][0], trial_index=item["trial_index"])])
        self.assertEqual(public(raw), record)
        self.assertEqual(encoded(diagnostics.derive(record, b"archive", raw, WORKER)), encoded(companion))

    def test_eight_cells_with_every_sampled_metric_stay_within_the_public_bounds(self):
        suite, largest = "matrix-all-local+remote", diagnostics.MAX_NUMBER
        raw = matrix_record(suite, environment=ENVIRONMENT, system_telemetry=diagnostics.TELEMETRY_DESCRIPTOR)
        metrics = {key: dict(first=1, last=largest, minimum=1, maximum=largest, status="available")
                   if key in diagnostics.GAUGES else
                   dict(first=0, last=largest, minimum=0, maximum=largest, delta=largest, status="available")
                   for key in diagnostics.PHASE_METRICS}
        def phases(names, **where):
            return dict(phase_summary(), phases=[
                dict(phase_summary()["phases"][0], phase=name, metrics=metrics, **where) for name in names])
        raw["system_telemetry"] = dict(phase_summary(), phases=[
            dict(phase_summary()["phases"][0], phase=name, case_index=index, trial_index=None, metrics=metrics)
            for index in range(4) for name in ("capacity", "generation", "cleanup")])
        for cell in raw["cells"]:
            for item in cell["copies"]:
                item["system_telemetry"] = phases(("cache", "command", "summary", "cleanup"),
                                                  trial_index=item["trial_index"])
        record = public(raw)
        value = diagnostics.derive(record, b"archive", raw, WORKER)
        self.assertEqual((value["schema"], value["status"], value["reason"], len(value["trials"])),
                         ("rcp-public-resource-diagnostics-v3", "complete", None, 24))
        # nothing had to be dropped to fit: every copy keeps its sampled phases
        self.assertTrue(all(len(trial["system"]["phases"]) == 4 for trial in value["trials"]))
        self.assertLess(len(encoded(value)), diagnostics.MAX_DOCUMENT)
        # and the worker's own document is one the collector reads
        self.assertEqual(results.json_value(encoded(raw), results.MAX_JSON)["schema"], "rcp-benchmark-matrix-v1")

    def test_figures_of_a_failed_remote_copy_are_never_published(self):
        value, record, _ = derived({1: FAILED})
        failed = value["trials"][3]
        self.assertEqual((failed["scenario"], failed["status"], failed["metrics"], failed["roles"]),
                         ("remote", "unavailable", None, dict(source=None, destination=None)))
        diagnostics.validate(value, record)

    def test_daemon_figures_are_checked_like_the_command_s(self):
        def figures(**changes):
            return lambda raw: raw["cells"][1]["copies"][0]["role_resources"]["source"]["metrics"].update(changes)
        for change in (figures(exit_code=1), figures(max_rss_kib=-1), figures(user_seconds="2"),
                       figures(max_rss_kib=1.5), figures(system_seconds=float("inf")),
                       lambda raw: raw["cells"][1]["copies"][0]["role_resources"]["source"]["metrics"].pop("minor_faults"),
                       lambda raw: raw["cells"][1]["copies"][0]["role_resources"]["source"].update(status="unavailable")):
            with self.subTest(change=change):
                value, record, _ = derived(change=change)
                self.assertEqual(value["trials"][3]["roles"], dict(source=None, destination=DESTINATION))
                diagnostics.validate(value, record)

    def test_the_versions_do_not_stand_in_for_each_other(self):
        value, record, raw = derived()
        earlier_raw = matrix_record("matrix-commission-local", environment=ENVIRONMENT)
        earlier_record = public(earlier_raw)
        earlier = diagnostics.derive(earlier_record, b"archive", earlier_raw, WORKER)
        for document, owner in ((earlier, record), (value, earlier_record)):
            with self.assertRaisesRegex(ValueError, "invalid public diagnostic identity"):
                diagnostics.validate(copy.deepcopy(document), owner)
        self.assertEqual(diagnostics.unavailable(record)["schema"], "rcp-public-resource-diagnostics-v3")
        diagnostics.validate(diagnostics.unavailable(record, "raw_budget"), record)
        # a record whose result failed its contract has no cells, remote or otherwise
        self.assertEqual(diagnostics.unavailable(dict(public(raw, valid=False), cells=[]))["schema"],
                         "rcp-public-resource-diagnostics-v2")

    def test_validation_rejects_daemon_figures_where_none_belong(self):
        value, record, _ = derived()
        changes = (
            lambda item: item["trials"][0].update(roles=dict(source=SOURCE, destination=DESTINATION)),
            lambda item: item["trials"][3].update(roles=None),
            lambda item: item["trials"][3].pop("roles"),
            lambda item: item["trials"][3]["roles"].pop("destination"),
            lambda item: item["trials"][3]["roles"].update(master=COMMAND),
            lambda item: item["trials"][3]["roles"].update(source=dict(SOURCE, exit_code=0)),
            lambda item: item["trials"][3]["roles"].update(source=dict(SOURCE, max_rss_kib=-1)),
            lambda item: item["trials"][3]["roles"].update(source="complete"),
            lambda item: item.update(process_scope=diagnostics.PROCESS_SCOPE),
            lambda item: item.update(status="partial"),
        )
        for change in changes:
            broken = copy.deepcopy(value)
            change(broken)
            with self.subTest(change=change), self.assertRaises(ValueError):
                diagnostics.validate(broken, record)

    def test_a_copy_without_command_figures_publishes_no_daemon_figures(self):
        value, record, _ = derived()
        broken = copy.deepcopy(value)
        broken["trials"][3].update(status="unavailable", metrics=None)
        broken["status"] = "partial"
        with self.assertRaisesRegex(ValueError, "invalid public daemon metrics"):
            diagnostics.validate(broken, record)

    def test_the_run_page_lists_each_process_of_a_remote_copy(self):
        value, record, _ = derived({3: FAILED})
        text = diagnostics.render(value, record)
        self.assertIn("<th>File set</th><th>Scenario</th><th>Copy</th><th>Process</th>", text)
        self.assertIn("<tr><td><code>commission-64m</code></td><td><code>local</code></td><td>1</td><td>rcp</td>"
                      "<td>complete</td><td>2.500</td><td>880.000</td><td>90,000</td>", text)
        remote = ("<tr><td><code>commission-64m</code></td><td><code>remote</code></td><td>1</td>")
        self.assertIn(remote + "<td>rcp</td><td>complete</td><td>2.500</td><td>880.000</td><td>90,000</td>", text)
        self.assertIn(remote + "<td>source rcpd</td><td>complete</td><td>300.000</td><td>450.000</td><td>50,000</td>",
                      text)
        self.assertIn(remote + "<td>destination rcpd</td><td>complete</td><td>200.000</td><td>700.000</td>"
                      "<td>30,000</td>", text)
        failed = "<tr><td><code>commission-10k</code></td><td><code>remote</code></td><td>1</td>"
        for process in ("rcp", "source rcpd", "destination rcpd"):
            self.assertIn(failed + "<td>" + process + "</td><td>unavailable</td><td>unavailable</td>", text)
        self.assertIn("source rcpd and the destination rcpd", text)


if __name__ == "__main__":
    unittest.main()
