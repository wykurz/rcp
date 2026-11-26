"""Pages for the cells on other storage: a trend page for each kind of copy there, and the tools on their file sets."""

import re
import unittest

from . import common, other_storage, production_diagnostics as diagnostics, production_export as export
from . import production_history as history
from .test_matrix_pages import matrix_example
from .test_production_history import split
from .test_tool_pages import weeks

# every file set under the scenarios of its storage
WEEKLY = "matrix-all-local+tools+remote+tools+nfs+tools+memory+tools"
EARLIER = "matrix-all-local+tools+remote+tools"
FILE_SETS = ["bulk-1g-1tb", "bulk-100m-1024gb", "bulk-1m-1024gb", "tiny-1k-10240k"]
NFS_SET, MEMORY_SET = "tiny-1k-700x1024", "tiny-1k-10240k-memory"
ON_NFS, IN_MEMORY = "1 KB files on NFS", "1 KB files in memory"
EVERY = ["1 GB files", "100 MB files", "1 MB files", "1 KB files"]
TOOLS = ("rcmp", "rlink", "rrm")
COPIES = ("local", "remote", "nfs", "memory")
FAILED = dict(status="failed", stage="storage", message="the worker could not prepare this storage: OSError")


def titles(html):
    return re.findall(r"<h2 id='history-[^']+-title'>([^<]+)</h2>", html)


class StorageTrendTests(unittest.TestCase):
    def test_cells_and_trends_are_named_once(self):
        tools = lambda *copies: [kind + "-" + tool for kind in copies for tool in TOOLS]
        # the names of the cells that there were keep their place and their wording
        self.assertEqual(list(history.SCENARIOS), ["local", "remote", *tools("local", "remote"),
                                                   "nfs", "memory", *tools("nfs", "memory")])
        self.assertEqual({scenario: history.SCENARIOS[scenario] for scenario in ("nfs", "memory", "nfs-rrm",
                                                                                 "memory-rcmp", "remote-rrm")},
                         {"nfs": "Copy between NFS mounts", "memory": "Copy in memory",
                          "nfs-rrm": "Remove with rrm, after a copy between NFS mounts",
                          "memory-rcmp": "Compare with rcmp, after a copy in memory",
                          "remote-rrm": "Remove with rrm, after a remote copy"})
        # every scenario that a suite can select has a name
        self.assertEqual(set(history.SCENARIOS), {scenario for cells in common.MATRIX_SUITES.values()
                                                  for _, scenario in cells})
        self.assertEqual(history.TOOL_CELLS, frozenset(tools(*COPIES)))
        # a trend is a kind of copy or a tool; a tool's trend takes its cells after every kind of copy
        self.assertEqual(history.TRENDS, (*COPIES, *TOOLS))
        self.assertEqual({trend: history.cells(trend) for trend in ("nfs", "memory", "rrm")},
                         dict(nfs=("nfs",), memory=("memory",),
                              rrm=("local-rrm", "remote-rrm", "nfs-rrm", "memory-rrm")))
        self.assertEqual({trend: history.ROWS[trend][:2] for trend in ("nfs", "memory")},
                         dict(nfs=("Copies between NFS mounts", "nfs.html"), memory=("Copies in memory", "memory.html")))
        # a file set of other storage is named with its storage: its files are those of another set
        self.assertEqual(history.FIRST.other, {NFS_SET: ON_NFS, MEMORY_SET: IN_MEMORY})
        self.assertEqual(list(history.FIRST.cases), FILE_SETS)

    def test_each_kind_of_copy_on_other_storage_has_a_trend_on_its_own_file_set(self):
        records, companions = split(weeks(2, suite=WEEKLY))
        self.assertEqual(history.present(records), (*COPIES, *TOOLS))
        for trend, case, title in (("nfs", NFS_SET, ON_NFS), ("memory", MEMORY_SET, IN_MEMORY)):
            with self.subTest(trend=trend):
                cases, outside = history.model(records, companions, trend)
                self.assertEqual((list(cases), outside), ([case], 0))
                self.assertEqual(len(cases[case]), 2)
                point = cases[case][-1]
                self.assertEqual(point["duration"]["samples"], 3)
                # one rcp process, as a local copy: its own CPU time and memory, and no daemons
                self.assertEqual((point["cpu"]["samples"], point["memory"]["samples"]), (3, 3))
                self.assertNotIn("source_cpu", point)
                html = history.render(records, companions, trend)
                self.assertEqual(titles(html), [title])
                self.assertEqual(re.findall(r"<h3[^>]*>([^<]+)</h3>", html)[:3],
                                 ["Copy duration", "CPU time", "Peak memory"])
                self.assertNotIn("rcpd", html)
                self.assertNotIn("outside these trends", html)
                self.assertNotIn("it takes milliseconds", html)

    def test_the_trends_of_the_arrays_are_what_they_were(self):
        records, companions = split(weeks(2, suite=WEEKLY))
        earlier = split(weeks(2, suite=EARLIER))
        for trend in ("local", "remote"):
            with self.subTest(trend=trend):
                cases, outside = history.model(records, companions, trend)
                # nothing is counted beside these trends any more: each cell has a page of its own
                self.assertEqual((list(cases), outside), (FILE_SETS, 0))
                self.assertEqual(titles(history.render(records, companions, trend)), EVERY)
                before, _ = history.model(*earlier, trend)
                self.assertEqual([[point["duration"] for point in points] for points in cases.values()],
                                 [[point["duration"] for point in points] for points in before.values()])

    def test_a_page_says_what_was_copied_and_how(self):
        records, companions = split(weeks(1, suite=WEEKLY))
        self.assertIn("716,800 files of 1 KB each (716.8 MB), copied by rcp with default settings between two mounts of "
                      "a network filesystem, where a request to the server costs 5 ms",
                      history.render(records, companions, "nfs"))
        self.assertIn("10,240,000 files of 1 KB each (10.24 GB), copied by rcp with default settings between two "
                      "filesystems in memory", history.render(records, companions, "memory"))

    def test_a_tool_has_a_trend_on_the_file_sets_of_other_storage_too(self):
        records, companions = split(weeks(2, suite=WEEKLY))
        for trend in TOOLS:
            with self.subTest(trend=trend):
                cases, outside = history.model(records, companions, trend)
                self.assertEqual((list(cases)[-2:], outside), ([NFS_SET, MEMORY_SET], 0))
                for case, copies in ((NFS_SET, "nfs"), (MEMORY_SET, "memory")):
                    # there the tool follows one kind of copy: three runs
                    point = cases[case][-1]
                    self.assertEqual(point["duration"]["samples"], 3)
                    self.assertEqual([copy["cell"] for copy in point["copies"]], [copies + "-" + trend])
                # and on the arrays both kinds, as before: six
                self.assertEqual(cases["tiny-1k-10240k"][-1]["duration"]["samples"], 6)
        self.assertEqual(titles(history.render(records, companions, "rrm")), [*EVERY, ON_NFS, IN_MEMORY])
        html = history.render(records, companions, "rcmp")
        self.assertEqual(titles(html), ["1 MB files", "1 KB files", ON_NFS, IN_MEMORY])
        # the file sets a tool is too quick on are two of the arrays', and no others
        self.assertIn("rcmp is timed on every file set, but on the 1,000 files of the 1 GB set and the 10,240 of the "
                      "100 MB set it takes milliseconds", html)

    def test_a_storage_that_was_missing_leaves_that_run_out_of_its_trends_only(self):
        cells = common.MATRIX_SUITES[WEEKLY]
        missing = {index: dict(FAILED, stage="storage" if scenario == "nfs" else "copy")
                   for index, (_, scenario) in enumerate(cells) if scenario.startswith("nfs")}
        later = matrix_example(WEEKLY, run="43200000002", at="2026-11-30T08:30:00+00:00", outcomes=missing)
        records, companions = split(weeks(1, suite=WEEKLY) + [later])
        # the run without its mounts has no point between them, and every other trend has both runs
        self.assertEqual(len(history.model(records, companions, "nfs")[0][NFS_SET]), 1)
        self.assertEqual(len(history.model(records, companions, "memory")[0][MEMORY_SET]), 2)
        self.assertEqual(len(history.model(records, companions, "local")[0]["tiny-1k-10240k"]), 2)
        removed, _ = history.model(records, companions, "rrm")
        self.assertEqual([len(removed[case]) for case in (NFS_SET, MEMORY_SET, "tiny-1k-10240k")], [1, 2, 2])
        # a run that only failed there still gives the storage its page
        only = split([later])
        self.assertEqual(history.present(only[0]), (*COPIES, *TOOLS))
        self.assertIn("No qualified nfs runs yet for " + ON_NFS + ".", history.render(*only, "nfs"))

    def test_runs_before_there_was_other_storage_have_no_such_pages(self):
        records, companions = split(weeks(2, suite=EARLIER))
        self.assertEqual(history.present(records), ("local", "remote", *TOOLS))
        for trend, plotted in (("rcmp", FILE_SETS[2:]), ("rlink", FILE_SETS[2:]), ("rrm", FILE_SETS)):
            cases, _ = history.model(records, companions, trend)
            # a tool's page has no section for a file set that no run has had a cell on
            self.assertEqual(list(cases), plotted)
            self.assertNotIn("NFS", history.render(records, companions, trend))


class StorageDescriptionTests(unittest.TestCase):
    def test_the_pages_describe_the_storage_as_the_worker_sets_it_up(self):
        # the server packages do not ship the worker's module: their own table must say the same
        self.assertEqual(diagnostics.CONFIGURED_STORAGE, dict(
            nfs_version=other_storage.NFS_VERSION, nfs_delay_microseconds=other_storage.NFS_DELAY_MICROSECONDS,
            nfs_threads=other_storage.NFS_THREADS, memory_bytes=other_storage.MEMORY_BYTES))
        for module in (history, diagnostics, export):
            self.assertNotIn("other_storage", vars(module))
        # and the file sets they name are the ones the suites measure there
        self.assertEqual({kind: cases for kind, cases in common.MATRIX_OTHER_FILE_SETS["all"].items()},
                         dict(nfs=(NFS_SET,), memory=(MEMORY_SET,)))
        self.assertEqual({trend: history.plotted(trend, history.FIRST) for trend in ("nfs", "memory")},
                         dict(nfs=(NFS_SET,), memory=(MEMORY_SET,)))


class StorageOverviewTests(unittest.TestCase):
    def test_the_first_page_has_one_row_for_the_copies_on_other_storage(self):
        records, companions = split(weeks(2, suite=WEEKLY))
        html = history.overview(records, companions)
        # after the rows of the arrays and before the tools: one row, a tile for each kind of copy
        self.assertEqual(re.findall(r"<section id='overview-([a-z]+)'", html), ["local", "remote", "storage", "tools"])
        self.assertIn("<section id='overview-storage' aria-labelledby='overview-storage-title'>"
                      "<div class='overview-title'><h2 id='overview-storage-title'>Copies on other storage</h2>"
                      "<span>between two NFS mounts, and between two filesystems in memory</span></div>", html)
        row = html.split("<section id='overview-storage'", 1)[1].split("</section>", 1)[0]
        self.assertEqual(re.findall(r"data-overview='([a-z]+)'", row), ["nfs", "memory"])
        for trend, page, case, name in (("nfs", "nfs.html", NFS_SET, ON_NFS), ("memory", "memory.html", MEMORY_SET,
                                                                                IN_MEMORY)):
            # a tile is named by its file set, after its row's title, and opens the page of its copies
            self.assertIn("<figure class='metric' data-overview='" + trend + "' aria-labelledby='overview-storage-title "
                          "overview-" + trend + "-" + case + "'><figcaption><h3 id='overview-" + trend + "-" + case +
                          "'><a href='" + page + "#history-" + case + "' aria-label='" + name + ", " + trend +
                          " copies'>" + name + "</a></h3>", row)
        # a row of the arrays has its four file sets and no other
        self.assertEqual(len(re.findall(r"data-overview='local'", html)), 4)

    def test_a_kind_of_copy_without_a_qualified_run_keeps_its_place_in_the_row(self):
        cells = common.MATRIX_SUITES[WEEKLY]
        missing = {index: dict(FAILED, stage="storage" if scenario == "nfs" else "copy")
                   for index, (_, scenario) in enumerate(cells) if scenario.startswith("nfs")}
        records, companions = split([matrix_example(WEEKLY, outcomes=missing)])
        row = history.overview(records, companions).split("<section id='overview-storage'", 1)[1].split("</section>", 1)[0]
        self.assertEqual(re.findall(r"data-overview='([a-z]+)'", row), ["nfs", "memory"])
        self.assertIn("<h3 id='overview-nfs-" + NFS_SET + "'>" + ON_NFS + "</h3></figcaption>"
                      "<p class='delta'>No qualified run yet.</p>", row)

    def test_there_is_no_such_row_before_a_run_measures_there(self):
        records, companions = split(weeks(2, suite=EARLIER))
        self.assertEqual(re.findall(r"<section id='overview-([a-z]+)'", history.overview(records, companions)),
                         ["local", "remote", "tools"])


class StorageRunPageTests(unittest.TestCase):
    def test_the_run_page_names_each_cell_and_summarizes_it_as_a_history_point(self):
        record, extra = weeks(1, suite=WEEKLY)[0]
        page = export.render(record, diagnostics=extra)
        self.assertIn("<td>" + ON_NFS + "</td><td>Copy between NFS mounts</td><td>measured</td>", page)
        self.assertIn("<td>" + IN_MEMORY + "</td><td>Remove with rrm, after a copy in memory</td><td>measured</td>",
                      page)
        for heading in (ON_NFS + " · Copy between NFS mounts", ON_NFS + " · Remove with rrm",
                        IN_MEMORY + " · Copy in memory", IN_MEMORY + " · Compare with rcmp",
                        "1 KB files · Local copy"):
            self.assertIn("<h3>" + heading + "</h3>", page)
        self.assertIn("the median of the runs of the tool by the build under test.</p>", page)

    def test_the_setup_page_describes_other_storage_once_a_run_measures_there(self):
        text = diagnostics.configuration_html([], remote=True, tools=True, elsewhere=True)
        for part in ("<li><strong>Network filesystem.</strong>", "NFS 4.2", "costs 5 ms", "<li><strong>Memory.</strong>",
                     "<tr><td>1 KB on NFS</td><td>716,800</td><td>1,000</td><td>716.8 MB</td></tr>",
                     "<tr><td>1 KB in memory</td><td>10,240,000</td><td>1,000</td><td>10.24 GB</td></tr>",
                     "before every copy and, on the network filesystem, before every run of a tool"):
            self.assertIn(part, text)
        before = diagnostics.configuration_html([], remote=True, tools=True)
        for part in ("Network filesystem", "NFS", "1 KB in memory", "filesystems in memory"):
            self.assertNotIn(part, before)


if __name__ == "__main__":
    unittest.main()
