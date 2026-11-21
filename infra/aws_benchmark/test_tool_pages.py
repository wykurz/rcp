"""Pages for the other tools: a trend page for each tool, and a row for them on the first page."""

from datetime import datetime, timedelta
import re
import unittest

from . import production_diagnostics as diagnostics, production_export as export, production_history as history
from .test_matrix_pages import matrix_example
from .test_production_history import split

WEEKLY = "matrix-all-local+tools+remote"
EARLIER = "matrix-all-local+remote"
TOOLS = ("rcmp", "rlink", "rrm")
MANY_FILES = ["1 MB files", "1 KB files"]
EVERY = ["1 GB files", "100 MB files", "1 MB files", "1 KB files"]
FAILED = dict(status="failed", stage="command", message="run 1 failed with exit code 3; see logs/tiny-1k-10240k/rrm/1")


def weeks(count, suite=WEEKLY, first="2026-11-23T08:30:00+00:00", **options):
    """Runs of one suite a week apart, the first at `first`."""
    start = datetime.fromisoformat(first)
    return [matrix_example(suite, run=str(43200000001 + index), at=(start + timedelta(days=7) * index).isoformat(),
                           **options) for index in range(count)]


def titles(html):
    return re.findall(r"<h2 id='history-[^']+-title'>([^<]+)</h2>", html)


class ToolTrendTests(unittest.TestCase):
    def test_the_scenarios_and_their_pages_are_named_once(self):
        self.assertEqual(list(history.SCENARIOS), ["local", "remote", *TOOLS])
        self.assertEqual(history.TOOLS, TOOLS)
        self.assertEqual({scenario: history.ROWS[scenario][1] for scenario in history.SCENARIOS},
                         dict(local="local.html", remote="remote.html", rcmp="rcmp.html", rlink="rlink.html",
                              rrm="rrm.html"))
        self.assertEqual(history.SCENARIOS["rrm"], "Remove with rrm")

    def test_a_tool_has_a_trend_for_each_file_set_where_it_takes_real_time(self):
        records, companions = split(weeks(2))
        for scenario, plotted in (("rcmp", MANY_FILES), ("rlink", MANY_FILES), ("rrm", EVERY)):
            with self.subTest(scenario=scenario):
                cases, outside = history.model(records, companions, scenario)
                self.assertEqual([history.CASES[case] for case in cases], plotted)
                self.assertEqual({len(points) for points in cases.values()}, {2})
                # what a tool does with a thousand files is not a trend, and is not counted as left out either
                self.assertEqual(outside, 0)
                point = cases["tiny-1k-10240k"][-1]
                self.assertEqual(point["duration"]["samples"], 3)
                # the tool's own figures: 2.5 s user and 880 s system, 90,000 KiB
                self.assertEqual((point["cpu"]["value"], round(point["memory"]["value"], 2)), (882.5, 87.89))
                self.assertEqual([copy["variant"] for copy in point["copies"]],
                                 [dict(rcmp="rcmp-size", rlink="rlink-default", rrm="rrm-default")[scenario]])

    def test_a_tool_page_shows_duration_cpu_time_and_peak_memory_of_the_tool(self):
        records, companions = split(weeks(2))
        html = history.render(records, companions, "rrm")
        self.assertEqual(titles(html), EVERY)
        self.assertIn("10,240,000 files of 1 KB each (10.24 GB), removed by rrm after each local copy · 2 runs", html)
        for title in ("Duration", "CPU time", "Peak memory"):
            self.assertIn(">" + title + "</h3>", html)
        self.assertNotIn("Copy duration", html)
        self.assertEqual(html.count("<svg "), 12)
        # a point is the median of the tool's runs, and says so wherever it counts them
        self.assertIn("the median of its runs of the tool, with a bar from the fastest to the slowest", html)
        self.assertIn("<th>Runs</th>", html)
        self.assertIn(" · 3 runs", html)
        self.assertNotIn("copies", html.split("<details>")[0].replace("after each local copy", ""))
        compare = history.render(records, companions, "rcmp")
        self.assertEqual(titles(compare), MANY_FILES)
        self.assertIn("1,024,000 files of 1 MB each (1.024 TB), compared with their source by rcmp, by file size, "
                      "after each local copy", compare)
        self.assertIn("hard-linked into a new tree by rlink after each local copy",
                      history.render(records, companions, "rlink"))
        # the file sets that are not plotted are named, with the reason
        self.assertIn("<p class='window'>rcmp is timed on every file set, but on the 1,000 files of the 1 GB set and "
                      "the 10,240 of the 100 MB set it takes milliseconds: those runs are on the run pages and are "
                      "not plotted.</p>", compare)
        self.assertNotIn("is timed on every file set", html)

    def test_a_tool_that_failed_in_a_run_has_no_point_for_it_and_the_others_keep_theirs(self):
        runs = weeks(2)
        index = [(cell["case_id"], cell["scenario"]) for cell in runs[1][0]["cells"]].index(("tiny-1k-10240k", "rrm"))
        runs[1] = matrix_example(WEEKLY, run="43200000002", at="2026-11-30T08:30:00+00:00", outcomes={index: FAILED})
        records, companions = split(runs)
        self.assertEqual(len(history.model(records, companions, "rrm")[0]["tiny-1k-10240k"]), 1)
        self.assertEqual(len(history.model(records, companions, "rcmp")[0]["tiny-1k-10240k"]), 2)
        self.assertEqual(len(history.model(records, companions)[0]["tiny-1k-10240k"]), 2)

    def test_copies_keep_their_trends_through_runs_with_and_without_tool_cells(self):
        earlier = matrix_example(EARLIER, run="43100000001", at="2026-11-17T08:30:00+00:00")
        records, companions = split([earlier, *weeks(2)])
        for scenario in (history.LOCAL, history.REMOTE):
            cases, outside = history.model(records, companions, scenario)
            # one line through all three runs: a tool cell neither breaks a copy's trend nor counts as left out
            self.assertEqual(({len(points) for points in cases.values()}, outside), ({3}, 0))
            self.assertEqual({point["segment"] for points in cases.values() for point in points}, {0})
        cases, _ = history.model(records, companions, "rrm")
        self.assertEqual({len(points) for points in cases.values()}, {2})
        self.assertNotIn("outside these trends", history.render(records, companions))

    def test_a_scenario_has_a_page_once_a_run_has_a_cell_of_it(self):
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
        self.assertIn("10,240,000 files of 1 KB, after each local copy", row)
        tiles = row.split("data-overview='")[1:]
        self.assertEqual([tile.split("'")[0] for tile in tiles], list(TOOLS))
        for scenario, tile in zip(TOOLS, tiles):
            self.assertIn("<a href='" + scenario + ".html#history-tiny-1k-10240k'", tile)
            self.assertIn(history.SCENARIOS[scenario], tile)
            self.assertEqual(tile.count("<svg "), 1)
            self.assertEqual(tile.count("<circle class='dot'"), 2)
        # the rows of the copies are what they were
        self.assertEqual(html.count("data-overview='local'"), 4)
        self.assertEqual(html.count("data-overview='remote'"), 4)

    def test_there_is_no_such_row_before_a_run_times_the_tools(self):
        html = history.overview(*split([matrix_example(EARLIER)]))
        self.assertNotIn("overview-tools", html)
        self.assertEqual(html, history.overview(*split([matrix_example(EARLIER)])))

    def test_a_tool_without_a_qualified_run_keeps_its_place_in_the_row(self):
        failed = matrix_example("matrix-1k-local+tools", outcomes={3: FAILED})
        row = self.row(history.overview(*split([failed])))
        tiles = row.split("data-overview='")[1:]
        self.assertEqual([tile.split("'")[0] for tile in tiles], list(TOOLS))
        self.assertIn("No qualified run yet.", tiles[2])
        self.assertNotIn("<svg ", tiles[2])
        self.assertEqual(tiles[0].count("<svg "), 1)


class ToolRunPageTests(unittest.TestCase):
    def test_the_run_page_lists_every_tool_cell_and_summarizes_each_as_a_history_point(self):
        record, extra = matrix_example("matrix-1k-local+tools", outcomes={2: FAILED})
        html = export.render(record, diagnostics=extra)
        self.assertIn("<span class='badge badge-partial'>3 of 4 measurements completed</span>", html)
        for name, result in (("Local copy", "measured"), ("Compare with rcmp", "measured"),
                             ("Hard-link with rlink", "failed"), ("Remove with rrm", "measured")):
            self.assertRegex(html, "<td>1 KB files</td><td>" + name + "</td><td>[^<]*" + result)
        self.assertIn("<h3>1 KB files · Remove with rrm</h3><p>This run as one history point: the median of the "
                      "runs of the tool by the build under test.</p>", html)
        self.assertIn("<h3>1 KB files · Local copy</h3><p>This run as one history point: the median of the "
                      "copies made by the build under test.</p>", html)
        self.assertNotIn("<h3>1 KB files · Hard-link with rlink</h3>", html)
        self.assertIn(" over 3 runs</p>", html)
        self.assertIn("<h4>Duration</h4>", html)
        self.assertIn("<th>Copies or runs</th>", html)

    def test_what_the_arrays_did_is_tabulated_for_tool_runs_as_for_copies(self):
        record, extra = matrix_example("matrix-1k-local+tools")
        rows = [dict(case_id="tiny-1k-10240k", scenario=scenario, written=1.0e9, destination_busy=.5, read=2.0e9,
                     source_busy=.25, cpu=.6) for scenario in ("rrm", "local", "rcmp")]
        html = history.storage_html("43200000001-1", "runs/x.html", rows)
        self.assertIn("<th>File set</th><th>Scenario</th>", html)
        # in the order of the pages, whatever order the rows came in
        self.assertEqual(re.findall(r"<tr><td>1 KB files</td><td>([^<]+)</td>", html),
                         ["Local copy", "Compare with rcmp", "Remove with rrm"])

    def test_the_setup_page_says_how_the_tools_are_timed_once_a_run_times_them(self):
        text = diagnostics.configuration_html([], remote=True, tools=True)
        self.assertIn("<li><strong>Other tools.</strong>", text)
        self.assertIn("<code>rcmp --metadata-compare=f:size</code>", text)
        self.assertIn("No cache is dropped for a tool", text)
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
