"""Pages for the other tools: a trend page for each tool, and a row for them on the first page."""

from datetime import datetime, timedelta
import re
import unittest

from . import production_diagnostics as diagnostics, production_export as export, production_history as history
from .test_matrix_pages import matrix_example
from .test_production_history import split

# every file set, both kinds of copy, and the tools after each: a tool has six runs per file set
WEEKLY = "matrix-all-local+tools+remote+tools"
# the same without tools after the remote copies: three runs
AFTER_LOCAL = "matrix-all-local+tools+remote"
EARLIER = "matrix-all-local+remote"
TOOLS = ("rcmp", "rlink", "rrm")
CELLS = [copies + "-" + tool for copies in ("local", "remote") for tool in TOOLS]
MANY_FILES = ["1 MB files", "1 KB files"]
EVERY = ["1 GB files", "100 MB files", "1 MB files", "1 KB files"]
FAILED = dict(status="failed", stage="command", message="run 1 failed with exit code 3; see logs/x/remote-rrm/1")


def weeks(count, suite=WEEKLY, first="2026-11-23T08:30:00+00:00", **options):
    """Runs of one suite a week apart, the first at `first`."""
    start = datetime.fromisoformat(first)
    return [matrix_example(suite, run=str(43200000001 + index), at=(start + timedelta(days=7) * index).isoformat(),
                           **options) for index in range(count)]


def titles(html):
    return re.findall(r"<h2 id='history-[^']+-title'>([^<]+)</h2>", html)


def cell_index(run, case, scenario):
    return [(cell["case_id"], cell["scenario"]) for cell in run[0]["cells"]].index((case, scenario))


class ToolTrendTests(unittest.TestCase):
    def test_cells_and_trends_are_named_once(self):
        # every cell scenario has a title, for the tables that list cells
        # the cells on other storage and their trends came later (test_storage_pages.py)
        self.assertEqual(list(history.SCENARIOS)[:8], ["local", "remote", *CELLS])
        self.assertEqual(history.SCENARIOS["remote-rrm"], "Remove with rrm, after a remote copy")
        self.assertEqual(history.SCENARIOS["local-rcmp"], "Compare with rcmp, after a local copy")
        # a trend is a kind of copy or a tool; a tool's trend takes the runs of its cells after either kind
        self.assertEqual(tuple(trend for trend in history.TRENDS if trend not in ("nfs", "memory")),
                         ("local", "remote", *TOOLS))
        self.assertEqual(history.TOOLS, TOOLS)
        self.assertEqual({trend: history.cells(trend)[:2] for trend in ("local", "remote", *TOOLS)},
                         dict(local=("local",), remote=("remote",), rcmp=("local-rcmp", "remote-rcmp"),
                              rlink=("local-rlink", "remote-rlink"), rrm=("local-rrm", "remote-rrm")))
        self.assertEqual({trend: history.ROWS[trend][:2] for trend in ("local", "remote", *TOOLS)},
                         dict(local=("Local copies", "local.html"), remote=("Remote copies", "remote.html"),
                              rcmp=("Compare with rcmp", "rcmp.html"), rlink=("Hard-link with rlink", "rlink.html"),
                              rrm=("Remove with rrm", "rrm.html")))

    def test_a_tool_has_a_trend_for_each_file_set_where_it_takes_real_time(self):
        records, companions = split(weeks(2))
        for trend, plotted in (("rcmp", MANY_FILES), ("rlink", MANY_FILES), ("rrm", EVERY)):
            with self.subTest(trend=trend):
                cases, outside = history.model(records, companions, trend)
                self.assertEqual([history.CASES[case] for case in cases], plotted)
                self.assertEqual({len(points) for points in cases.values()}, {2})
                # what a tool does with a thousand files is not a trend, and is not counted as left out either
                self.assertEqual(outside, 0)

    def test_a_point_takes_the_runs_of_the_tool_after_every_copy_of_the_file_set(self):
        records, companions = split(weeks(2))
        point = history.model(records, companions, "rrm")[0]["tiny-1k-10240k"][-1]
        record = records[point["run_key"]]
        samples = sorted(sample for item in record["measurements"] if item["case_id"] == "tiny-1k-10240k"
                         and item["scenario"] in ("local-rrm", "remote-rrm") for sample in item["samples"])
        self.assertEqual(len(samples), 6)
        # the median and the range of all six runs, three after the local copies and three after the remote ones
        self.assertEqual(point["duration"], dict(value=(samples[2] + samples[3]) / 2, low=samples[0], high=samples[5],
                                                samples=6))
        self.assertEqual(point["noun"], ("tool run", "tool runs"))
        self.assertTrue(point["pooled"])
        self.assertEqual([copy["cell"] for copy in point["copies"]], ["local-rrm", "remote-rrm"])
        self.assertEqual(len({copy["series_id"] for copy in point["copies"]}), 2)
        # the tool's own figures, over the same six runs: 2.5 s user and 880 s system, 90,000 KiB
        self.assertEqual((point["cpu"]["value"], point["cpu"]["samples"], round(point["memory"]["value"], 2)),
                         (882.5, 6, 87.89))

    def test_a_run_without_tools_after_remote_copies_has_a_point_of_three_runs_on_the_same_trend(self):
        runs = [matrix_example(AFTER_LOCAL, run="43100000001", at="2026-11-16T08:30:00+00:00"), *weeks(1)]
        records, companions = split(runs)
        first, second = history.model(records, companions, "rrm")[0]["tiny-1k-10240k"]
        self.assertEqual((first["duration"]["samples"], first["pooled"], first["cpu"]["samples"]), (3, False, 3))
        self.assertEqual((second["duration"]["samples"], second["pooled"]), (6, True))
        self.assertEqual({first["segment"], second["segment"]}, {0})

    def test_a_tool_that_failed_after_one_kind_of_copy_keeps_its_runs_after_the_other(self):
        runs = weeks(2)
        failed = cell_index(runs[1], "tiny-1k-10240k", "remote-rrm")
        runs[1] = matrix_example(WEEKLY, run="43200000002", at="2026-11-30T08:30:00+00:00", outcomes={failed: FAILED})
        records, companions = split(runs)
        whole, partial = history.model(records, companions, "rrm")[0]["tiny-1k-10240k"]
        self.assertEqual((whole["duration"]["samples"], partial["duration"]["samples"]), (6, 3))
        self.assertEqual([copy["cell"] for copy in partial["copies"]], ["local-rrm"])
        # figures of the three runs that count; the failed run's are not among them
        self.assertEqual(partial["cpu"]["samples"], 3)
        self.assertEqual(len(history.model(records, companions, "rcmp")[0]["tiny-1k-10240k"]), 2)
        self.assertEqual(len(history.model(records, companions)[0]["tiny-1k-10240k"]), 2)

    def test_a_tool_that_failed_after_both_kinds_of_copy_has_no_point_for_that_run(self):
        runs = weeks(2)
        failed = {cell_index(runs[1], "tiny-1k-10240k", scenario): FAILED for scenario in ("local-rrm", "remote-rrm")}
        runs[1] = matrix_example(WEEKLY, run="43200000002", at="2026-11-30T08:30:00+00:00", outcomes=failed)
        records, companions = split(runs)
        self.assertEqual(len(history.model(records, companions, "rrm")[0]["tiny-1k-10240k"]), 1)
        self.assertEqual(len(history.model(records, companions, "rrm")[0]["bulk-1g-1tb"]), 2)

    def test_a_tool_page_shows_duration_cpu_time_and_peak_memory_of_the_tool(self):
        records, companions = split(weeks(2))
        html = history.render(records, companions, "rrm")
        self.assertEqual(titles(html), EVERY)
        self.assertIn("10,240,000 files of 1 KB each (10.24 GB), removed by rrm after each copy · 2 runs", html)
        for title in ("Duration", "CPU time", "Peak memory"):
            self.assertIn(">" + title + "</h3>", html)
        self.assertNotIn("Copy duration", html)
        self.assertEqual(html.count("<svg "), 12)
        # a point is the median of the tool's runs, and says so wherever it counts them
        self.assertIn("the median of its tool runs, with a bar from the fastest to the slowest tool run", html)
        self.assertIn("<th>Tool runs</th>", html)
        self.assertIn(" · 6 tool runs", html)
        self.assertNotIn("copies", html.split("<details>")[0])
        # the table names the cells whose runs a point holds
        self.assertIn("<code>local-rrm</code> <code>", html)
        self.assertIn(", <code>remote-rrm</code> <code>", html)
        compare = history.render(records, companions, "rcmp")
        self.assertEqual(titles(compare), MANY_FILES)
        self.assertIn("1,024,000 files of 1 MB each (1.024 TB), compared with their source by rcmp, by file size, "
                      "after each copy", compare)
        self.assertIn("hard-linked into a new tree by rlink after each copy",
                      history.render(records, companions, "rlink"))
        # the file sets that are not plotted are named, with the reason
        self.assertIn("<p class='window'>rcmp is timed on every file set, but on the 1,000 files of the 1 GB set and "
                      "the 10,240 of the 100 MB set it takes milliseconds: those runs are on the run pages and are "
                      "not plotted.</p>", compare)
        self.assertNotIn("is timed on every file set", html)

    def test_copies_keep_their_trends_through_runs_with_and_without_tool_cells(self):
        earlier = matrix_example(EARLIER, run="43100000001", at="2026-11-17T08:30:00+00:00")
        records, companions = split([earlier, *weeks(2)])
        for trend in (history.LOCAL, history.REMOTE):
            cases, outside = history.model(records, companions, trend)
            # one line through all three runs: a tool cell neither breaks a copy's trend nor counts as left out
            self.assertEqual(({len(points) for points in cases.values()}, outside), ({3}, 0))
            self.assertEqual({point["segment"] for points in cases.values() for point in points}, {0})
            self.assertEqual({point["duration"]["samples"] for points in cases.values() for point in points}, {3})
        cases, _ = history.model(records, companions, "rrm")
        self.assertEqual({len(points) for points in cases.values()}, {2})
        self.assertNotIn("outside these trends", history.render(records, companions))

    def test_a_trend_has_a_page_once_a_run_has_a_cell_of_it(self):
        self.assertEqual(history.present(split([matrix_example("matrix-all-local")])[0]), ("local",))
        self.assertEqual(history.present(split([matrix_example(EARLIER)])[0]), ("local", "remote"))
        self.assertEqual(history.present(split(weeks(1))[0]), ("local", "remote", *TOOLS))
        self.assertEqual(history.present(split(weeks(1, suite="matrix-1k-local+tools"))[0]), ("local", *TOOLS))
        # measured or not: a tool that failed everywhere still has its page, which says that nothing qualified
        failed = matrix_example("matrix-1k-local+tools", outcomes={3: FAILED})
        self.assertEqual(history.present(split([failed])[0]), ("local", *TOOLS))
        self.assertIn("No qualified rrm runs yet", history.render(*split([failed]), "rrm"))


class ToolOverviewTests(unittest.TestCase):
    def row(self, html):
        return html.split("<section id='overview-tools'")[1].split("</section>")[0]

    def test_the_first_page_has_one_row_for_the_other_tools_on_the_file_set_with_the_most_files(self):
        html = history.overview(*split(weeks(2)))
        self.assertLess(html.index("id='overview-remote'"), html.index("id='overview-tools'"))
        row = self.row(html)
        self.assertIn("<h2 id='overview-tools-title'>Other tools</h2>", row)
        self.assertIn("10,240,000 files of 1 KB, after each copy", row)
        tiles = row.split("data-overview='")[1:]
        self.assertEqual([tile.split("'")[0] for tile in tiles], list(TOOLS))
        for tool, tile in zip(TOOLS, tiles):
            self.assertIn("<a href='" + tool + ".html#history-tiny-1k-10240k'", tile)
            self.assertIn(history.ROWS[tool][0], tile)
            self.assertEqual(tile.count("<svg "), 1)
            self.assertEqual(tile.count("<circle class='dot'"), 2)
            self.assertIn(" · 6 tool runs", tile)
        # the rows of the copies are what they were
        self.assertEqual(html.count("data-overview='local'"), 4)
        self.assertEqual(html.count("data-overview='remote'"), 4)

    def test_there_is_no_such_row_before_a_run_times_the_tools(self):
        html = history.overview(*split([matrix_example(EARLIER)]))
        self.assertNotIn("overview-tools", html)
        self.assertNotIn("tool", html)

    def test_a_tool_without_a_qualified_run_keeps_its_place_in_the_row(self):
        failed = matrix_example("matrix-1k-local+tools", outcomes={3: FAILED})
        row = self.row(history.overview(*split([failed])))
        tiles = row.split("data-overview='")[1:]
        self.assertEqual([tile.split("'")[0] for tile in tiles], list(TOOLS))
        self.assertIn("No qualified run yet.", tiles[2])
        self.assertNotIn("<svg ", tiles[2])
        self.assertEqual(tiles[0].count("<svg "), 1)


class ToolRunPageTests(unittest.TestCase):
    def test_the_run_page_lists_every_tool_cell_and_summarizes_each_tool_as_a_history_point(self):
        suite = "matrix-1k-local+tools+remote+tools"
        record, extra = matrix_example(suite, outcomes={2: dict(FAILED, message="run 1 failed with exit code 3")})
        html = export.render(record, diagnostics=extra)
        self.assertIn("<span class='badge badge-partial'>7 of 8 measurements completed</span>", html)
        for name, result in (("Local copy", "measured"), ("Compare with rcmp, after a local copy", "measured"),
                             ("Hard-link with rlink, after a local copy", "failed"),
                             ("Remove with rrm, after a local copy", "measured"),
                             ("Remote copy over loopback", "measured"),
                             ("Hard-link with rlink, after a remote copy", "measured"),
                             ("Remove with rrm, after a remote copy", "measured")):
            self.assertRegex(html, "<td>1 KB files</td><td>" + name + "</td><td>[^<]*" + result)
        # one summary for each tool, over its runs after both kinds of copy
        self.assertIn("<h3>1 KB files · Remove with rrm</h3><p>This run as one history point: the median of the "
                      "runs of the tool by the build under test, after its local and its remote copies.</p>", html)
        self.assertIn(" over 6 tool runs</p>", html)
        # rlink failed after the local copies: its point is the three runs after the remote ones
        self.assertIn("<h3>1 KB files · Hard-link with rlink</h3><p>This run as one history point: the median of the "
                      "runs of the tool by the build under test.</p>", html)
        self.assertIn(" over 3 tool runs</p>", html)
        self.assertIn("<h3>1 KB files · Local copy</h3><p>This run as one history point: the median of the "
                      "copies made by the build under test.</p>", html)
        self.assertEqual(html.count("<h3>1 KB files · "), 5)
        self.assertIn("<h4>Duration</h4>", html)
        self.assertIn("<th>Copies or tool runs</th>", html)

    def test_what_the_arrays_did_is_tabulated_for_tool_runs_as_for_copies(self):
        rows = [dict(case_id="tiny-1k-10240k", scenario=scenario, written=1.0e9, destination_busy=.5, read=2.0e9,
                     source_busy=.25, cpu=.6) for scenario in ("remote-rrm", "local-rrm", "local", "local-rcmp")]
        html = history.storage_html("43200000001-1", "runs/x.html", rows)
        self.assertIn("<th>File set</th><th>Scenario</th>", html)
        # in the order of the cells of a run, whatever order the rows came in
        self.assertEqual(re.findall(r"<tr><td>1 KB files</td><td>([^<]+)</td>", html),
                         ["Local copy", "Compare with rcmp, after a local copy",
                          "Remove with rrm, after a local copy", "Remove with rrm, after a remote copy"])

    def test_the_setup_page_says_how_the_tools_are_timed_once_a_run_times_them(self):
        text = diagnostics.configuration_html([], remote=True, tools=True)
        self.assertIn("<li><strong>Other tools.</strong>", text)
        self.assertIn("<code>rcmp --metadata-compare=f:size</code>", text)
        self.assertIn("No cache is dropped for a tool", text)
        self.assertIn("clears every tree with that <code>rrm</code>", text)
        self.assertNotIn("Other tools.", diagnostics.configuration_html([], remote=True))
        reading = history.reading_html(remote=True, tools=True)
        self.assertIn("<li><strong>Other tools.</strong>", reading)
        self.assertIn('<a href="rrm.html">', reading)
        self.assertNotIn("Other tools.", history.reading_html(remote=True))
        self.assertIn('<a href="remote.html">remote copies</a>', history.reading_html(remote=True, tools=True))
        self.assertIn("<li><strong>Other tools.</strong>", history.reading_html(tools=True))
        self.assertNotIn("remote.html", history.reading_html(tools=True))


if __name__ == "__main__":
    unittest.main()
