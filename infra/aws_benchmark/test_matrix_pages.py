"""Public pages for matrix runs: every cell's result on the run page, one continuing trend per file set."""

import copy
import hashlib
import unittest

from . import production_diagnostics as diagnostics, production_export as export
from . import production_history as history, production_projection as projection
from .test_matrix_diagnostics import ENVIRONMENT, WORKER
from .test_matrix_results import matrix_record
from .test_production_history import observed, split

ONE_GIGABYTE = "bulk-1g-1tb"
FAILED = dict(status="failed", stage="copy", message="copy 1 failed with exit codes [3]: <b>bold</b>")
NOT_RUN = dict(status="not_run", stage=None, message="allocation ended before this cell")


def matrix_example(suite="matrix-1g-local", run="43200000001", at="2026-11-20T08:30:00+00:00", outcomes=None,
                   notes=()):
    """One frozen matrix record with its diagnostics, as the publisher would hold them."""
    raw = matrix_record(suite, outcomes, environment=ENVIRONMENT)
    raw["timestamp"] = at
    cells = [dict(case_id=cell["case_id"], scenario=cell["scenario"], outcome=cell["outcome"]["status"],
                  stage=cell["outcome"]["stage"], message=cell["outcome"]["message"]) for cell in raw["cells"]]
    measurements = [dict(case_id=cell["case_id"], variant_id="rcp-default", scenario=cell["scenario"],
        series_id=hashlib.sha256(cell["cell_id"].encode()).hexdigest(), median=cell["summary"]["median"],
        samples=cell["summary"]["samples"]) for cell in raw["cells"] if cell["outcome"]["status"] == "complete"]
    valid = bool(measurements)
    record = dict(schema="rcp-production-public-report-v4", run_key="repo-655073755-run-" + run,
        retirement_sha256="a" * 64, benchmark_valid=valid, cleanup_verified=True,
        validation=projection.PUBLIC_CONTRACTS["rcp-production-public-report-v4"]["validation"],
        identity=dict(run=run + "-1", run_url="https://github.com/wykurz/rcp/actions/runs/" + run + "/attempts/1",
            suite=suite, baseline="1" * 40, candidate="1" * 40, controller="2" * 40),
        comparison=projection.IDENTICAL_BINARIES, measurements=measurements, series_contract="aws-local-ext4-v1",
        note="Each measurement is the median of 3 copies." if valid
             else "Safely retired failed qualification; excluded from performance history.",
        qualification_outcome="passed" if valid else "failed", collected_at="2026-11-20T18:44:32+00:00",
        cells=cells, diagnostics=list(notes))
    if valid:
        record.update(timestamp=at, verification=dict(policy="exit-status-summary-v1"))
    projection.validate_public(record)
    return record, diagnostics.derive(record, b"archive", raw, WORKER)


def elsewhere(extra, scenario):
    """Move every copy of a companion to another scenario, as the companion of such a record is written."""
    extra.update(schema=diagnostics.REMOTE_SCHEMA, process_scope=diagnostics.REMOTE_PROCESS_SCOPE, status="partial")
    for trial in extra["trials"]:
        trial.update(scenario=scenario, roles=dict(source=None, destination=None))


class MatrixRunPageTests(unittest.TestCase):
    def test_every_cell_is_listed_with_its_result_and_the_reason_it_was_not_measured(self):
        record, extra = matrix_example("matrix-all-local", outcomes={1: FAILED, 3: NOT_RUN})
        html = export.render(record, diagnostics=extra)
        self.assertIn("<h1>Run 43200000001-1 <span class='badge badge-partial'>2 of 4 measurements completed</span></h1>",
                      html)
        self.assertIn("<th>File set</th><th>Scenario</th><th>Result</th><th>Median s</th><th>Range s</th>"
                      "<th>Samples</th><th>What happened</th>", html)
        self.assertIn("<tr><td>1 GB files</td><td>Local copy</td><td>measured</td><td>600.200</td>"
                      "<td>600.100–600.300</td><td>3</td><td class='reason'></td></tr>", html)
        self.assertIn("<tr><td>100 MB files</td><td>Local copy</td><td>failed</td><td>—</td><td>—</td><td>—</td>"
                      "<td class='reason'>During copy: copy 1 failed with exit codes [3]: &lt;b&gt;bold&lt;/b&gt;</td></tr>",
                      html)
        self.assertIn("<tr><td>1 KB files</td><td>Local copy</td><td>not run</td><td>—</td><td>—</td><td>—</td>"
                      "<td class='reason'>allocation ended before this cell</td></tr>", html)
        self.assertNotIn("<b>", html)
        # one build is measured, so there is nothing to compare and nothing was verified
        self.assertNotIn("Comparison", html)
        self.assertIn("<dt>Validation</dt><dd>Not verified: a copy counts when rcp exits cleanly and reports the "
                      "expected file count.</dd>", html)

    def test_measured_file_sets_keep_their_summary_tiles_and_frozen_samples(self):
        record, extra = matrix_example("matrix-all-local", outcomes={1: FAILED})
        html = export.render(record, diagnostics=extra)
        self.assertEqual([title for title in ("1 GB files", "100 MB files", "1 MB files", "1 KB files")
                          if "<h3>" + title + "</h3>" in html], ["1 GB files", "1 MB files", "1 KB files"])
        self.assertIn("<th>Case</th><th>Scenario</th><th>Copies</th><th>Median s</th>", html)
        self.assertIn("<tr><td><code>bulk-1g-1tb</code></td><td><code>local</code></td><td>3</td><td>600.200</td>"
                      "<td>600.100, 600.200, 600.300</td>", html)
        self.assertNotIn("<th>Variant</th>", html)
        self.assertNotIn("identical", html)

    def test_a_fully_measured_run_and_a_run_without_measurements_use_the_plain_badges(self):
        record, extra = matrix_example()
        self.assertIn("<span class='badge badge-passed'>1 of 1 measurement completed</span>",
                      export.render(record, diagnostics=extra))
        record, extra = matrix_example(outcomes={0: FAILED})
        html = export.render(record, diagnostics=extra)
        self.assertIn("<span class='badge badge-failed'>failed; excluded from performance history</span>", html)
        self.assertIn("<td>failed</td>", html)
        self.assertIn("<p>This run has no performance measurements.</p>", html)

    def test_run_level_notes_say_when_a_run_was_cut_short(self):
        record, extra = matrix_example("matrix-commission-local",
            outcomes={1: dict(status="interrupted", stage="copy", message="run stopped during copy 2 of 3")},
            notes=["worker: the benchmark command did not exit cleanly"])
        html = export.render(record, diagnostics=extra)
        self.assertIn("<h2 id='run-notes'>Run notes</h2><ul><li>worker: the benchmark command did not exit "
                      "cleanly</li></ul>", html)
        self.assertIn("<td><code>commission-10k</code></td><td>Local copy</td><td>cut short</td>", html)
        self.assertIn("<td class='reason'>During copy: run stopped during copy 2 of 3</td>", html)
        self.assertNotIn("run-notes", export.render(*[matrix_example()[0]], diagnostics=matrix_example()[1]))

    def test_a_record_says_where_its_raw_evidence_is_and_until_when(self):
        record, _ = matrix_example()
        self.assertEqual(export.evidence_html(record),
            "<code>production/runs/repo-655073755-run-43200000001/collection/</code> in the benchmark bucket "
            "until about 18 Feb 2027; fetch it with "
            "<code>just aws-benchmark-evidence repo-655073755-run-43200000001 DIRECTORY</code>")
        # earlier records with a trusted collection time point at their evidence the same way
        legacy, _ = observed("42458459014", "08")
        self.assertIn("run-42458459014/collection/</code> in the benchmark bucket until about 6 Feb 2027",
                      export.evidence_html(legacy))
        old = {key: value for key, value in legacy.items() if key not in ("collected_at", "qualification_outcome")}
        old["schema"] = "rcp-production-public-report-v1"
        projection.validate_public(old)
        self.assertIsNone(export.evidence_html(old))

    def test_a_long_reason_wraps_instead_of_widening_the_table(self):
        from pathlib import Path
        record, extra = matrix_example("matrix-all-local", outcomes={1: FAILED})
        self.assertIn("<td class='reason'>During copy: copy 1 failed", export.render(record, diagnostics=extra))
        style = (Path(__file__).resolve().parents[2] / "scripts/report-site/aws.css").read_text()
        rule = style[style.index(".aws table.data td.reason"):].split("}", 1)[0]
        self.assertIn("white-space: normal", rule)

    def test_an_error_inside_the_harness_is_labelled_as_such(self):
        record, extra = matrix_example("matrix-commission-local", outcomes={
            1: dict(status="failed", stage="harness", message="unexpected RuntimeError: no such summary field")})
        self.assertIn("<td class='reason'>In the benchmark harness: unexpected RuntimeError: no such summary field</td>",
                      export.render(record, diagnostics=extra))

    def test_the_stated_retention_is_the_bucket_rule_for_run_evidence(self):
        from pathlib import Path
        import re
        stack = (Path(__file__).with_name("production-stack.yaml")).read_text()
        rule = stack.split("Id: BoundProductionRuns", 1)[1].split("- Id:", 1)[0]
        self.assertIn("Prefix: production/runs/", rule)
        self.assertEqual(int(re.search(r"ExpirationInDays: (\d+)", rule).group(1)), export.EVIDENCE_RETENTION_DAYS)

    def test_paired_run_pages_keep_their_comparison_and_variant_columns(self):
        legacy, companion = observed("42458459014", "08")
        html = export.render(legacy, diagnostics=companion)
        self.assertIn("<dt>Comparison</dt><dd>A/A (identical source revision and binary digests)</dd>", html)
        self.assertIn("<th>Case</th><th>Variant</th><th>Copies</th>", html)
        self.assertIn("<span class='badge badge-passed'>consistency checks passed</span>", html)
        self.assertNotIn("What happened", html)


class MatrixHistoryTests(unittest.TestCase):
    def test_a_matrix_run_continues_the_trend_of_earlier_runs_of_its_file_set(self):
        cases, outside = history.model(*split([observed("42458459014", "08"), matrix_example()]))
        earlier, latest = cases[ONE_GIGABYTE]
        self.assertEqual((earlier["segment"], latest["segment"], latest["starts_segment"]), (0, 0, False))
        # the change of method is marked on the line; an unrecorded pairing is not a change
        # named for what it is: a matrix run checks an exit status and a count, it verifies nothing
        self.assertEqual(latest["changes"], [("Copy check", "metadata-size-tree-v1", "exit-status-summary-v1")])
        self.assertEqual(latest["duration"], dict(value=600.2, low=600.1, high=600.3, samples=3))
        self.assertEqual((latest["cpu"]["value"], latest["memory"]["value"], latest["pooled"]),
                         (882.5, 90000 / 1024, False))
        self.assertEqual(outside, 0)

    def test_every_file_set_of_a_full_run_gets_its_own_point(self):
        cases, outside = history.model(*split([matrix_example("matrix-all-local", outcomes={2: NOT_RUN})]))
        self.assertEqual({case: len(points) for case, points in cases.items()},
            {"bulk-1g-1tb": 1, "bulk-100m-1024gb": 1, "bulk-1m-1024gb": 0, "tiny-1k-10240k": 1})
        self.assertEqual(outside, 0)
        html = history.render(*split([matrix_example("matrix-all-local", outcomes={2: NOT_RUN})]))
        self.assertIn("No qualified runs yet for 1 MB files.", html)

    def test_a_file_set_measured_less_often_keeps_its_plot_next_to_one_measured_every_few_days(self):
        runs = [matrix_example("matrix-all-local", run="43000000001", at="2026-11-01T08:30:00+00:00")]
        for index in range(history.MAX_PLOT_RUNS + 1):
            runs.append(observed(str(43100000000 + index), "%02d" % (2 + index % 27)))
        html = history.render(*split(runs))
        # the full run is the oldest of all, and still the only point of three file sets
        for case in ("bulk-100m-1024gb", "bulk-1m-1024gb", "tiny-1k-10240k"):
            self.assertIn("<section id='history-" + case + "'", html)
            section = html[html.index("<section id='history-" + case + "'"):]
            self.assertIn("runs/repo-655073755-run-43000000001.html", section[:section.index("</section>")])
        self.assertNotIn("No qualified runs yet", html)
        # the one-gigabyte plot shows its own newest runs, and the page says what it left out
        one_gigabyte = html[html.index("<section id='history-bulk-1g-1tb'"):html.index("<section id='history-bulk-100m")]
        self.assertNotIn("run-43000000001.html", one_gigabyte)
        self.assertIn("The plots show each file set's newest 32 qualified runs. Older runs are not plotted", html)

    def test_only_a_file_set_that_was_never_measured_is_said_to_have_no_runs(self):
        html = history.render(*split([matrix_example("matrix-1g-local")]))
        self.assertIn("No qualified runs yet for 100 MB files, 1 MB files and 1 KB files.", html)

    def test_small_test_fixtures_and_other_scenarios_stay_outside_the_trends(self):
        cases, outside = history.model(*split([matrix_example("matrix-commission-local")]))
        self.assertEqual((sum(map(len, cases.values())), outside), (0, 2))
        record, extra = matrix_example()
        record["cells"][0]["scenario"] = record["measurements"][0]["scenario"] = "remote-loopback"
        elsewhere(extra, "remote-loopback")
        cases, outside = history.model(*split([(record, extra)]))
        self.assertEqual((cases[ONE_GIGABYTE], outside), ([], 1))

    def test_resources_of_another_scenario_never_fill_a_local_point(self):
        record, extra = matrix_example()
        # the same file set measured a second way, whose copies are the only ones with resources
        record["cells"].append(dict(record["cells"][0], scenario="remote-loopback"))
        record["measurements"].append(dict(record["measurements"][0], scenario="remote-loopback"))
        elsewhere(extra, "remote-loopback")
        (point,), outside = history.model(*split([(record, extra)]))[0][ONE_GIGABYTE], \
            history.model(*split([(record, extra)]))[1]
        self.assertEqual((point["duration"]["samples"], point["cpu"], point["memory"], outside), (3, None, None, 1))

    def test_a_point_without_complete_resources_has_no_cpu_or_memory_value(self):
        record, extra = matrix_example()
        extra["trials"][0].update(status="unavailable", metrics=None)
        extra["status"] = "partial"
        point, = history.model(*split([(record, extra)]))[0][ONE_GIGABYTE]
        self.assertEqual((point["cpu"], point["memory"]), (None, None))


class MatrixPublicationSummaryTests(unittest.TestCase):
    def test_the_job_summary_names_each_measurement_by_scenario_and_says_nothing_was_verified(self):
        import os
        import tempfile
        from unittest import mock
        from . import production_publish
        record, _ = matrix_example("matrix-all-local", outcomes={1: FAILED})
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "summary.md")
            with mock.patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": target}):
                production_publish.summary(dict(observations=[], active=dict(state="idle")), [record])
            with open(target, encoding="utf-8") as stream:
                text = stream.read()
        self.assertIn('{"case_id":"bulk-1g-1tb","median":600.2,"scenario":"local","variant_id":"rcp-default"}', text)
        self.assertIn('"qualification":"3 of 4 measurements completed"', text)
        self.assertIn("matrix copies are not verified", text)
        self.assertNotIn("attested", text)
        # one build was measured: there is no comparison or second revision to report
        self.assertIn('"revision":"' + "1" * 40 + '"', text)
        for field in ('"comparison"', '"baseline"', '"candidate"'):
            self.assertNotIn(field, text)
        self.assertNotIn("<b>", text)


class MatrixMethodTextTests(unittest.TestCase):
    def test_the_setup_page_describes_each_kind_of_run_by_its_suite_not_by_when_it_ran(self):
        # paired runs keep being scheduled next to matrix runs, so neither is "earlier" or "now"
        text = diagnostics.configuration_html([])
        self.assertIn("Runs of a <code>matrix-…</code> suite make three fresh copies of each file set with one build",
                      text)
        self.assertIn("Matrix runs do not verify copies: a copy counts when rcp exits cleanly and reports the "
                      "expected file count", text)
        self.assertIn("Runs of the other suites make four copies", text)
        self.assertIn("check paths, types, counts, sizes and metadata, not file contents", text)
        self.assertIn("<code>matrix-commission-local</code>", text)
        reading = history.reading_html()
        self.assertIn("three copies in a matrix run, four in a paired run", reading)
        for page in (text, reading):
            for word in ("Earlier runs", "Runs now", "earlier runs"):
                self.assertNotIn(word, page)
        self.assertNotIn("future remote modes require their own validated contract", reading)


if __name__ == "__main__":
    unittest.main()
