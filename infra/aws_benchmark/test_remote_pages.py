"""Remote copies have a page of their own: one trend per file set, and each daemon's resources."""

import unittest
from unittest import mock

from . import production_export as export, production_history as history
from .test_matrix_pages import ONE_GIGABYTE, matrix_example
from .test_production_history import figure, split

BOTH = "matrix-1g-local+remote"
UNREACHABLE = dict(status="failed", stage="transport",
                   message="the private ssh endpoint could not be started: sshd is not an executable file: /usr/sbin/sshd")


def later(suite=BOTH, run="43200000002", day="27", **options):
    return matrix_example(suite, run=run, at=f"2026-11-{day}T08:30:00+00:00", **options)


class RemoteHistoryTests(unittest.TestCase):
    def test_a_remote_cell_is_a_point_on_the_remote_trend_of_its_file_set(self):
        runs = [matrix_example(BOTH), later()]
        cases, outside = history.model(*split(runs), history.REMOTE)
        first, second = cases[ONE_GIGABYTE]
        self.assertEqual((first["segment"], second["segment"], second["changes"]), (0, 0, []))
        # the remote cell is the suite's second: its copies are the ones recorded for it
        self.assertEqual(first["duration"], dict(value=601.2, low=601.1, high=601.3, samples=3))
        self.assertEqual(first["copies"], [dict(variant="rcp-default", series_id=runs[0][0]["measurements"][1]["series_id"])])
        # every process of the copy: the rcp command, and each daemon on its own
        self.assertEqual((first["cpu"]["value"], first["memory"]["value"]), (882.5, 90000 / 1024))
        self.assertEqual((first["source_cpu"], first["destination_cpu"]),
                         (dict(value=750.0, low=750.0, high=750.0, samples=3),
                          dict(value=900.0, low=900.0, high=900.0, samples=3)))
        self.assertEqual((first["source_memory"]["value"], first["destination_memory"]["value"]),
                         (50000 / 1024, 30000 / 1024))
        self.assertEqual(outside, 0)

    def test_the_local_trend_of_the_same_run_is_untouched_by_its_remote_cell(self):
        cases, outside = history.model(*split([matrix_example(BOTH)]))
        point, = cases[ONE_GIGABYTE]
        self.assertEqual(point["duration"], dict(value=600.2, low=600.1, high=600.3, samples=3))
        self.assertEqual((point["cpu"]["value"], point["memory"]["value"]), (882.5, 90000 / 1024))
        self.assertNotIn("source_cpu", point)
        # a measurement plotted on the other page is not outside the trends
        self.assertEqual(outside, 0)
        self.assertEqual(history.model(*split([matrix_example("matrix-1g-remote")]))[0][ONE_GIGABYTE], [])

    def test_a_run_without_a_remote_cell_has_no_remote_point(self):
        cases, outside = history.model(*split([matrix_example("matrix-all-local")]), history.REMOTE)
        self.assertEqual((sum(map(len, cases.values())), outside), (0, 0))

    def test_a_daemon_without_figures_for_every_copy_has_no_value_and_the_other_keeps_its_own(self):
        record, extra = matrix_example(BOTH)
        extra["trials"][3]["roles"]["source"] = None
        extra["status"] = "partial"
        point, = history.model(*split([(record, extra)]), history.REMOTE)[0][ONE_GIGABYTE]
        self.assertEqual((point["source_cpu"], point["source_memory"]), (None, None))
        self.assertEqual(point["destination_cpu"]["samples"], 3)
        self.assertEqual(point["cpu"]["samples"], 3)

    def test_the_remote_page_plots_duration_and_each_daemon(self):
        html = history.render(*split([matrix_example(BOTH), later()]), history.REMOTE)
        self.assertIn("<h2 id='history-bulk-1g-1tb-title'>1 GB files</h2>", html)
        self.assertIn("1,000 files of 1 GB each (1 TB), copied by rcp through a source and a destination rcpd "
                      "over loopback, with default settings · 2 runs", html)
        self.assertEqual([part.split("'", 1)[0] for part in html.split("data-history-metric='")[1:]],
                         ["duration", "source_cpu", "destination_cpu", "source_memory", "destination_memory"])
        for metric, title in (("duration", "Copy duration"), ("source_cpu", "Source rcpd CPU time"),
                              ("destination_cpu", "Destination rcpd CPU time"),
                              ("source_memory", "Source rcpd peak memory"),
                              ("destination_memory", "Destination rcpd peak memory")):
            self.assertIn(">" + title + "</h3>", figure(html, metric))
            self.assertEqual(figure(html, metric).count("<circle class='dot'"), 2)
        self.assertIn("<p class='value'>750.0 <span class='unit'>s</span></p>", figure(html, "source_cpu"))
        # the rcp command's own figures are in the table, with every other value of the run
        self.assertIn("<th>Duration s: median (range)</th><th>Source rcpd CPU s</th><th>Destination rcpd CPU s</th>"
                      "<th>Source rcpd peak RSS MiB</th><th>Destination rcpd peak RSS MiB</th><th>rcp CPU s</th>"
                      "<th>rcp peak RSS MiB</th>", html)
        self.assertIn("<td>601.200 (601.100–601.300)</td><td>750.000 (750.000–750.000)</td>", html)
        self.assertNotIn("copied locally", html)

    def test_the_local_page_of_the_same_runs_is_what_it_was(self):
        both = history.render(*split([matrix_example(BOTH), later()]))
        alone = history.render(*split([matrix_example("matrix-1g-local"), later("matrix-1g-local")]))
        # one sentence differs: with two kinds of run, it says which kind a file set has none of
        self.assertEqual(both.replace("No qualified local runs yet", "No qualified runs yet"), alone)
        self.assertIn("copied locally by rcp with default settings", both)
        self.assertNotIn("rcpd", both)

    def test_file_sets_never_measured_remotely_are_named_on_the_remote_page(self):
        html = history.render(*split([matrix_example(BOTH)]), history.REMOTE)
        self.assertIn("<p class='empty'>No qualified remote runs yet for 100 MB files, 1 MB files and 1 KB files.</p>",
                      html)
        # a file set measured remotely only is not said to have no runs at all
        self.assertIn("<p class='empty'>No qualified local runs yet for 100 MB files, 1 MB files and 1 KB files.</p>",
                      history.render(*split([matrix_example(BOTH)])))
        self.assertIn("<p class='empty'>No qualified runs yet for 100 MB files, 1 MB files and 1 KB files.</p>",
                      history.render(*split([matrix_example("matrix-1g-local")])))

    def test_the_reading_notes_describe_remote_copies_only_once_there_is_a_remote_page(self):
        text = history.reading_html(remote=True)
        self.assertIn('Remote copies over loopback have <a href="remote.html">a page of their own</a>', text)
        self.assertIn("source and the destination <code>rcpd</code>", text)
        # no word the pages never define
        self.assertIn("A run counts for every file set and scenario it measured", text)
        self.assertNotIn("cell", text)
        earlier = history.reading_html()
        self.assertNotIn("rcpd", earlier)
        self.assertNotIn("remote", earlier.lower())
        self.assertIn("The plots show local copies on one host.", earlier)

    def test_the_setup_page_describes_remote_copies_only_once_there_is_a_remote_page(self):
        from . import production_diagnostics as diagnostics
        earlier, text = diagnostics.configuration_html([]), diagnostics.configuration_html([], remote=True)
        self.assertNotIn("rcpd", earlier)
        self.assertIn("The copy datapath is local on one host, not a network copy", earlier)
        self.assertIn("<code>matrix-commission-local</code>", earlier)
        self.assertIn("<strong>Remote copies.</strong>", text)
        # loopback is a network device too: what is absent is hardware and added latency
        self.assertIn("No network hardware and no added latency are involved", text)
        self.assertNotIn("no network device", text)
        self.assertIn("three fresh copies of each file set under each scenario", text)
        self.assertIn("The <code>matrix-commission-…</code> suites copy two small fixtures", text)

    def test_each_page_counts_only_its_own_unplotted_measurements(self):
        from .test_production_history import observed
        # a paired run with different builds: its reference copies are local measurements with no trend
        record, extra = observed("42458459014", "08")
        record["comparison"] = "A/B (different source revisions)"
        runs = [(record, extra), matrix_example("matrix-commission-local+remote", run="43200000002",
                                                at="2026-11-27T08:30:00+00:00")]
        local, remote = (history.model(*split(runs), scenario)[1] for scenario in (history.LOCAL, history.REMOTE))
        # one reference build and two small fixtures locally; the two small fixtures remotely
        self.assertEqual((local, remote), (3, 2))
        self.assertIn("2 qualified measurements are outside these trends",
                      history.render(*split(runs), history.REMOTE))

    def test_the_remote_page_points_at_the_run_list_where_it_is(self):
        runs = [later(run=str(43200000002 + index), day=str(10 + index)) for index in range(3)]
        with mock.patch.object(history, "MAX_PLOT_RUNS", 2):
            remote, local = (history.render(*split(runs), scenario) for scenario in (history.REMOTE, history.LOCAL))
        self.assertIn("The plots show each file set's newest 2 qualified runs. Older runs are not plotted; every "
                      "run stays in the <a href='index.html#runs'>list of runs</a>.", remote)
        self.assertIn("Older runs are not plotted; every run stays in the list below.", local)
        self.assertEqual(remote.count("<circle class='dot'"), 2 * 5)


class RemoteRunPageTests(unittest.TestCase):
    def test_each_cell_is_named_by_its_scenario_and_summarized_on_its_own(self):
        record, extra = matrix_example(BOTH)
        html = export.render(record, diagnostics=extra)
        self.assertIn("<tr><td>1 GB files</td><td>Local copy</td><td>measured</td><td>600.200</td>", html)
        self.assertIn("<tr><td>1 GB files</td><td>Remote copy over loopback</td><td>measured</td><td>601.200</td>",
                      html)
        local, remote = html.split("<h3>1 GB files · ")[1:3]
        self.assertTrue(local.startswith("Local copy</h3>"))
        self.assertTrue(remote.startswith("Remote copy over loopback</h3>"))
        self.assertEqual([part.split("<", 1)[0] for part in local.split("<h4>")[1:]],
                         ["Copy duration", "CPU time", "Peak memory"])
        self.assertEqual([part.split("<", 1)[0] for part in remote.split("<h4>")[1:4 + 2]],
                         ["Copy duration", "Source rcpd CPU time", "Destination rcpd CPU time",
                          "Source rcpd peak memory", "Destination rcpd peak memory"])
        self.assertIn("<p class='value'>750.0 <span class='unit'>s</span></p>", remote)
        self.assertIn("<th>File set</th><th>Scenario</th><th>Copy</th><th>Process</th>", html)

    def test_the_summaries_of_a_file_set_stand_next_to_each_other_in_the_order_of_the_cells(self):
        record, extra = matrix_example("matrix-all-local+remote")
        html = export.render(record, diagnostics=extra)
        self.assertEqual([part.split("</h3>", 1)[0] for part in html.split("<h3>")[1:9]],
                         [name + " · " + scenario for name in ("1 GB files", "100 MB files", "1 MB files", "1 KB files")
                          for scenario in ("Local copy", "Remote copy over loopback")])

    def test_the_headers_of_the_cell_table_stay_on_one_line(self):
        from pathlib import Path
        record, extra = matrix_example(BOTH)
        self.assertIn("<table class='data cells'><thead><tr><th>File set</th>", export.render(record, diagnostics=extra))
        style = (Path(__file__).resolve().parents[2] / "scripts/report-site/aws.css").read_text()
        rule = style[style.index(".aws table.cells th"):].split("}", 1)[0]
        self.assertIn("white-space: nowrap", rule)

    def test_a_run_with_local_cells_only_keeps_its_plain_headings(self):
        record, extra = matrix_example("matrix-1g-local")
        html = export.render(record, diagnostics=extra)
        self.assertIn("<h3>1 GB files</h3>", html)
        self.assertNotIn(" · Local copy", html)

    def test_a_remote_cell_without_its_endpoint_says_so(self):
        record, extra = matrix_example(BOTH, outcomes={1: UNREACHABLE})
        html = export.render(record, diagnostics=extra)
        self.assertIn("<td>Remote copy over loopback</td><td>failed</td><td>—</td><td>—</td><td>—</td>"
                      "<td class='reason'>Setting up the remote copy: the private ssh endpoint could not be started: "
                      "sshd is not an executable file: /usr/sbin/sshd</td>", html)
        self.assertIn("badge-partial'>1 of 2 measurements completed", html)


if __name__ == "__main__":
    unittest.main()
