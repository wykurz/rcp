"""Pages once the weekly suite has run: its file sets are the current ones, and the ones before them are kept below."""

import re
import unittest

from . import common, production_diagnostics as diagnostics, production_export as export, production_history as history
from .test_matrix_pages import matrix_example
from .test_production_history import split
from .test_tool_pages import weeks

WEEKLY = common.MATRIX_WEEKLY
# every file set there was, under every scenario of its storage: the runs before the weekly suite
BEFORE = "matrix-all-local+tools+remote+tools+nfs+tools+memory+tools"
OLD = ["bulk-1g-1tb", "bulk-100m-1024gb", "bulk-1m-1024gb", "tiny-1k-10240k"]
NEW = ["bulk-1g-200gb", "bulk-1m-200gb", "tiny-1k-2048k"]
SMALL = NEW[-1]
NFS_SET, OLD_MEMORY, NEW_MEMORY = "tiny-1k-700x1024", "tiny-1k-10240k-memory", "tiny-1k-2048k-memory"
EVERY = ["1 GB files", "100 MB files", "1 MB files", "1 KB files"]
# the weekly suite has no set of 100 MB files: it gave what the set of 1 GB files gave
CURRENT = ["1 GB files", "1 MB files", "1 KB files"]
# the suite that was defined first and never ran
NEVER_RAN = "matrix-weekly-1"
TOOLS = ("rcmp", "rlink", "rrm")


def titles(html):
    return re.findall(r"<h2 id='history-[^']+-title'>([^<]+)</h2>", html)


def sections(html):
    return re.findall(r"<section id='history-([^']+)'", html)


def halves(html):
    """A page's current sections and the ones under "Earlier file sets"."""
    current, _, earlier = html.partition("<details class='earlier'>")
    return sections(current), sections(earlier)


def runs(before=2, weekly=2, **options):
    """Weekly runs of the suite before, then of the weekly suite, a week apart."""
    first = weeks(before, suite=BEFORE)
    later = [matrix_example(WEEKLY, run=str(43300000001 + index), at=f"2026-12-{7 + 7 * index:02d}T08:30:00+00:00",
                            **options) for index in range(weekly)]
    return split(first + later)


class GenerationTests(unittest.TestCase):
    def test_the_file_sets_of_each_generation_are_named_alike(self):
        first, sized = history.GENERATIONS
        self.assertEqual((list(first.cases), list(sized.cases)), (OLD, NEW))
        self.assertEqual((list(first.cases.values()), list(sized.cases.values())), (EVERY, CURRENT))
        self.assertEqual(first.other, {NFS_SET: "1 KB files on NFS", OLD_MEMORY: "1 KB files in memory"})
        self.assertEqual(sized.other, {NFS_SET: "1 KB files on NFS", NEW_MEMORY: "1 KB files in memory"})
        self.assertEqual((first.most_files, sized.most_files), ("tiny-1k-10240k", SMALL))
        # the pages name every file set that a suite's trends are made of
        self.assertEqual(set(history.NAMES), {*OLD, *NEW, NFS_SET, OLD_MEMORY, NEW_MEMORY})
        # the weekly suite measures exactly the sets of its generation
        self.assertEqual({case for case, _ in common.MATRIX_SUITES[WEEKLY]}, {*sized.cases, *sized.other})
        # and its tools are plotted where it times them: on the sets of small files
        timed = {scenario.split("-")[1]: set() for _, scenario in common.MATRIX_SUITES[WEEKLY] if "-" in scenario}
        for case, scenario in common.MATRIX_SUITES[WEEKLY]:
            if "-" in scenario:
                timed[scenario.split("-")[1]].add(case)
        self.assertEqual({tool: set(history.plotted(tool, sized)) for tool in TOOLS}, timed)
        # on the arrays it times rcmp and rlink after the local copies only, and the pages say so
        self.assertEqual((first.local_only, sized.local_only), ((), ("rcmp", "rlink")))
        after = {tool: {scenario.split("-")[0] for case, scenario in common.MATRIX_SUITES[WEEKLY]
                        if case == SMALL and scenario.endswith("-" + tool)} for tool in TOOLS}
        self.assertEqual(after, dict(rcmp={"local"}, rlink={"local"}, rrm={"local", "remote"}))

    def test_the_file_sets_of_the_suite_that_never_ran_have_no_place_on_the_pages(self):
        own = {case for case, _ in common.MATRIX_SUITES[NEVER_RAN]} - {NFS_SET}
        self.assertEqual(len(own), 5)
        self.assertFalse(own & set(history.NAMES))
        # should it run after all, its run is listed and has its page, and its points are in no trend
        record, extra = matrix_example(NEVER_RAN)
        records, companions = split([(record, extra)])
        self.assertIs(history.generation(records), history.GENERATIONS[0])
        cases, outside = history.model(records, companions, "local")
        self.assertEqual(([len(points) for points in cases.values()], outside), ([0, 0, 0, 0], 4))
        self.assertIn("4 qualified measurements are outside these trends", history.render(records, companions, "local"))
        self.assertIn("<td><code>bulk-100m-400gb</code></td><td>Local copy</td><td>measured</td>",
                      export.render(record, diagnostics=extra))
        self.assertIn("No qualified run yet", history.overview(records, companions))

    def test_the_pages_are_what_they_were_until_the_weekly_suite_has_measured_something(self):
        records, companions = split(weeks(2, suite=BEFORE))
        self.assertIs(history.generation(records), history.GENERATIONS[0])
        local = history.render(records, companions, "local")
        self.assertEqual((sections(local), "Earlier file sets" in local), (OLD, False))
        # a run of the weekly suite that measured nothing changes no page
        nothing = {index: dict(status="failed", stage="harness", message="unexpected OSError: no space")
                   for index in range(len(common.MATRIX_SUITES[WEEKLY]))}
        failed, extras = runs(weekly=1, outcomes=nothing)
        self.assertIs(history.generation(failed), history.GENERATIONS[0])
        self.assertEqual(history.render(failed, extras, "local"), local)
        self.assertEqual(history.overview(failed, extras), history.overview(records, companions))

    def test_the_file_sets_of_the_weekly_suite_are_the_current_ones_once_it_has_run(self):
        records, companions = runs()
        self.assertIs(history.generation(records), history.GENERATIONS[1])
        for trend in ("local", "remote"):
            with self.subTest(trend=trend):
                cases, outside = history.model(records, companions, trend)
                self.assertEqual((list(cases), {len(points) for points in cases.values()}, outside), (NEW, {2}, 0))
                html = history.render(records, companions, trend)
                self.assertEqual(halves(html), (NEW, OLD))
                # both halves name a file set by the size of its files; the line under it says how many
                self.assertEqual(titles(html), CURRENT + EVERY)
                self.assertIn("200 files of 1 GB each (200 GB)", html)
                self.assertIn("1,000 files of 1 GB each (1 TB)", html)
        html = history.render(records, companions, "local")
        self.assertIn("<details class='earlier'><summary>Earlier file sets</summary><p class='window'>These file sets "
                      "were measured until the weekly suite replaced them with smaller ones, so that a command takes "
                      "a minute or two. Their numbers are not comparable with those above.</p>", html)
        # the earlier trends keep every run they had
        earlier = html.partition("<details class='earlier'>")[2]
        self.assertEqual(earlier.count(" · 2 runs</p>"), 4)

    def test_a_data_table_of_an_earlier_file_set_stands_where_the_current_ones_do(self):
        from pathlib import Path
        records, companions = runs()
        earlier = history.render(records, companions, "local").partition("<details class='earlier'>")[2]
        # each earlier file set keeps its data table, a block that opens inside the block of the earlier sets
        self.assertEqual(earlier.count("<details><summary>Data table</summary>"), 4)
        # and the stylesheet indents a block inside another, which here would set the table off from its plots
        style = (Path(__file__).resolve().parents[2] / "scripts/report-site/aws.css").read_text()
        self.assertIn(".aws details details { margin-left: 1rem; }", style)
        rule = style[style.index(".aws details.earlier details"):].split("}", 1)[0]
        self.assertIn("margin-left: 0", rule)
        self.assertLess(style.index(".aws details details {"), style.index(".aws details.earlier details"))

    def test_the_copies_on_other_storage_follow(self):
        records, companions = runs()
        # the mounts show the same file set in both generations: one trend, four runs
        cases, _ = history.model(records, companions, "nfs")
        self.assertEqual({case: len(points) for case, points in cases.items()}, {NFS_SET: 4})
        self.assertEqual(halves(history.render(records, companions, "nfs")), ([NFS_SET], []))
        # the set in memory was made smaller with the one on the arrays
        cases, _ = history.model(records, companions, "memory")
        self.assertEqual({case: len(points) for case, points in cases.items()}, {NEW_MEMORY: 2})
        self.assertEqual(halves(history.render(records, companions, "memory")), ([NEW_MEMORY], [OLD_MEMORY]))

    def test_a_first_run_without_its_other_storage_still_makes_its_file_sets_the_current_ones(self):
        # no worker has prepared the mounts or the roots in memory yet: the first run may lose those cells
        lost = {index: dict(status="failed", stage="storage", message="the NFS server did not start")
                for index, (_, scenario) in enumerate(common.MATRIX_SUITES[WEEKLY])
                if common.matrix_storage(scenario) != "disks"}
        records, companions = runs(before=1, weekly=1, outcomes=lost)
        self.assertIs(history.generation(records), history.GENERATIONS[1])
        self.assertEqual(halves(history.render(records, companions, "local")), (NEW, OLD))
        # the mounts keep the trend they had, and the set in memory has no run yet
        nfs, memory = (history.render(records, companions, trend) for trend in ("nfs", "memory"))
        self.assertEqual((halves(nfs), nfs.count(" · 1 run</p>")), (([NFS_SET], []), 1))
        self.assertEqual(halves(memory), ([], [OLD_MEMORY]))
        self.assertIn("<p class='empty'>No qualified memory runs yet for 1 KB files in memory.</p>", memory)
        # a tool has its runs on the arrays, and the first page a place for every file set
        compare = history.render(records, companions, "rcmp")
        self.assertEqual(halves(compare), ([SMALL, NFS_SET], [*OLD[2:], OLD_MEMORY]))
        self.assertIn("<p class='empty'>No qualified rcmp runs yet for 1 KB files in memory.</p>", compare)
        first = history.overview(records, companions)
        self.assertEqual(re.findall(r"data-overview='(?:nfs|memory)' aria-labelledby='overview-storage-title "
                                    r"overview-(?:nfs|memory)-([^']+)'", first), [NFS_SET, NEW_MEMORY])
        self.assertEqual(first.count("<p class='delta'>No qualified run yet.</p>"), 1)

    def test_a_tool_is_plotted_on_the_sets_of_small_files_and_its_earlier_trends_are_kept(self):
        records, companions = runs()
        for trend in TOOLS:
            with self.subTest(trend=trend):
                cases, outside = history.model(records, companions, trend)
                self.assertEqual((list(cases), outside), ([SMALL, NFS_SET, NEW_MEMORY], 0))
                # on the arrays rrm follows the local and the remote copies and the two others the local
                # ones; elsewhere there is one kind of copy to follow
                self.assertEqual([cases[case][-1]["duration"]["samples"] for case in cases],
                                 [6 if trend == "rrm" else 3, 3, 3])
                self.assertEqual(len(cases[NFS_SET]), 4)
                html = history.render(records, companions, trend)
                current, earlier = halves(html)
                self.assertEqual(current, [SMALL, NFS_SET, NEW_MEMORY])
                self.assertEqual(earlier, [*(OLD if trend == "rrm" else OLD[2:]), OLD_MEMORY])
                # the weekly suite does not time a tool on large files, and the page says why
                self.assertIn(trend + " is timed on the sets of 1 KB files. It takes as long as its files are many, "
                              "and the sets of larger files have too few for a run of a minute.", html)
                self.assertNotIn("it takes milliseconds", html)
                # the line under a file set says after which copies the tool ran
                small, nfs = (html[html.index("<section id='history-" + case + "'"):].split("</p>", 1)[0]
                              for case in (SMALL, NFS_SET))
                self.assertIn("2,048,000 files of 1 KB each (2.048 GB), ", small)
                self.assertTrue(small.endswith((" after each copy" if trend == "rrm" else " after each local copy") +
                                               " · 2 runs"), small)
                self.assertTrue(nfs.endswith(" after each copy · 4 runs"), nfs)
                # and an earlier file set, whose tools followed every copy, keeps its line
                earlier_small = html[html.index("<section id='history-tiny-1k-10240k'"):].split("</p>", 1)[0]
                self.assertTrue(earlier_small.endswith(" after each copy · 2 runs"), earlier_small)

    def test_the_first_page_shows_the_current_file_sets(self):
        records, companions = runs()
        html = history.overview(records, companions)
        self.assertEqual(re.findall(r"<section id='overview-([a-z]+)'", html), ["local", "remote", "storage", "tools"])
        self.assertEqual(re.findall(r"<figure class='metric' data-overview='local' aria-labelledby='overview-local-title "
                                    r"overview-local-([^']+)'", html), NEW)
        self.assertEqual(re.findall(r"data-overview='(?:nfs|memory)' aria-labelledby='overview-storage-title "
                                    r"overview-(?:nfs|memory)-([^']+)'", html), [NFS_SET, NEW_MEMORY])
        self.assertEqual(re.findall(r"data-overview='(?:rcmp|rlink|rrm)' aria-labelledby='overview-tools-title "
                                    r"overview-(?:rcmp|rlink|rrm)-([^']+)'", html), [SMALL] * 3)
        self.assertIn("<span>2,048,000 files of 1 KB: rcmp and rlink after each local copy, rrm after every copy"
                      "</span>", html)
        # the runs before had every tool after every copy, and their first page said so in fewer words
        before = history.overview(*split(weeks(2, suite=BEFORE)))
        self.assertIn("<span>10,240,000 files of 1 KB, after each copy</span>", before)
        self.assertNotIn("Earlier file sets", html)
        for old in (*OLD, OLD_MEMORY):
            self.assertNotIn(old, html)

    def test_a_run_page_shows_its_run_on_the_trends_of_its_own_file_sets(self):
        records, companions = runs()
        for suite, sets in ((BEFORE, history.GENERATIONS[0]), (WEEKLY, history.GENERATIONS[1])):
            record = next(value for value in records.values() if value["identity"]["suite"] == suite)
            with self.subTest(suite=suite):
                page = export.render(record, diagnostics=companions[record["run_key"]])
                small = sets.most_files
                self.assertEqual(set(history.point(record, companions[record["run_key"]], "local")), set(sets.cases))
                self.assertEqual(set(history.point(record, companions[record["run_key"]], "rrm")),
                                 set(history.plotted("rrm", sets)))
                self.assertIn("<h3>1 KB files · Remove with rrm</h3>", page)
                self.assertIn("<td>1 KB files</td><td>Remove with rrm, after a remote copy</td><td>measured</td>", page)
                self.assertEqual(history.point(record, None, "rrm")[small]["duration"]["samples"], 6)
                # the weekly suite compares the local copies only
                self.assertEqual(history.point(record, None, "rcmp")[small]["duration"]["samples"],
                                 6 if suite == BEFORE else 3)
                self.assertEqual("Compare with rcmp, after a remote copy" in page, suite == BEFORE)
        weekly = next(value for value in records.values() if value["identity"]["suite"] == WEEKLY)
        page = export.render(weekly, diagnostics=companions[weekly["run_key"]])
        self.assertEqual(page.count("<h3>1 GB files · "), 2)
        self.assertIn("<p>This run as one history point: the median of the runs of the tool by the build under "
                      "test.</p>", page.split("<h3>1 KB files · Compare with rcmp</h3>", 1)[1].split("<div", 1)[0])
        self.assertIn("the median of the runs of the tool by the build under test, after its local and its remote "
                      "copies.</p>", page.split("<h3>1 KB files · Remove with rrm</h3>", 1)[1].split("<div", 1)[0])

    def test_what_the_arrays_did_is_tabulated_for_the_file_sets_that_the_pages_describe(self):
        first, sized = history.GENERATIONS
        for suite, sets, arrays in ((BEFORE, first, OLD), (WEEKLY, sized, NEW)):
            cells = common.MATRIX_SUITES[suite]
            # as `diagnostics.storage_use` reports them, in an order that is not the pages'
            use = [dict(case_id=case, scenario=scenario, written=1.5e9, destination_busy=.9, read=1.5e9,
                        source_busy=.5, cpu=.1) for case, scenario in reversed(cells)]
            table = history.storage_html("1-1", "runs/x.html", use, sets)
            with self.subTest(suite=suite):
                rows = re.findall(r"<tr><td>([^<]+)</td><td>([^<]+)</td>", table)
                # the file sets of the arrays in the order of the trends, and none of other storage
                on_arrays = [(case, scenario) for case, scenario in cells if case in arrays]
                self.assertEqual(rows, [(history.NAMES[case], history.SCENARIOS[scenario])
                                        for case, scenario in sorted(on_arrays, key=lambda cell: (
                                            arrays.index(cell[0]), list(history.SCENARIOS).index(cell[1])))])
                self.assertEqual(len(rows), 32 if suite == BEFORE else 10)
                # a run on the file sets that the page does not describe has no table there
                other = sized if sets is first else first
                self.assertEqual(history.storage_html("1-1", "runs/x.html", use, other), "")

    def test_a_later_run_of_an_earlier_suite_adds_to_the_earlier_trends_only(self):
        # the schedule is switched after the weekly suite has run by hand: a scheduled run of the
        # suite before may well come in between
        records, companions = runs(before=1, weekly=1)
        late = matrix_example("matrix-all-local+remote", run="43400000001", at="2026-12-21T08:17:00+00:00")
        records[late[0]["run_key"]], companions[late[0]["run_key"]] = late
        self.assertIs(history.generation(records), history.GENERATIONS[1])
        for trend in ("local", "remote"):
            with self.subTest(trend=trend):
                html = history.render(records, companions, trend)
                current, _, earlier = html.partition("<details class='earlier'>")
                self.assertEqual((sections(current), sections(earlier)), (NEW, OLD))
                self.assertEqual((current.count(" · 1 run</p>"), earlier.count(" · 2 runs</p>")), (3, 4))
        # and the first page is that of the weekly suite's file sets, with the one run that measured them
        first = history.overview(records, companions)
        self.assertEqual(re.findall(r"<figure class='metric' data-overview='local' aria-labelledby='overview-local-title "
                                    r"overview-local-([^']+)'", first), NEW)
        for old in (*OLD, OLD_MEMORY):
            self.assertNotIn(old, first)


class SetupPageTests(unittest.TestCase):
    def text(self, **flags):
        return diagnostics.configuration_html([], remote=True, tools=True, elsewhere=True, **flags)

    def test_the_setup_page_describes_the_file_sets_of_the_weekly_suite_once_it_has_run(self):
        text = self.text(sized=True)
        rows = ("<tr><td>1 GB</td><td>200</td><td>1,000,000,000</td><td>200 GB</td></tr>"
                "<tr><td>1 MB</td><td>200,000</td><td>1,000,000</td><td>200 GB</td></tr>"
                "<tr><td>1 KB</td><td>2,048,000</td><td>1,000</td><td>2.048 GB</td></tr>"
                "<tr><td>1 KB on NFS</td><td>716,800</td><td>1,000</td><td>716.8 MB</td></tr>"
                "<tr><td>1 KB in memory</td><td>2,048,000</td><td>1,000</td><td>2.048 GB</td></tr>")
        self.assertIn("<tbody>" + rows + "</tbody>", text)
        self.assertIn("The sets are sized so that a copy, and a run of a tool where one is timed, takes a minute or "
                      "two on this machine.", text)
        self.assertIn("Until the weekly suite replaced them, the sets were five times larger (a terabyte of large "
                      "files, and 10,240,000 files of 1 KB), and one more held files of 100 MB: the trends of those "
                      "sets are under “Earlier file sets” on the page of each kind of copy and of each tool.", text)
        self.assertIn("<p>The 1 GB and 1 MB file sets hold 200 GB each, about as much as the machine has memory. The "
                      "kernel lets a copy run ahead of the disks by a part of that memory only, so most of such a "
                      "copy goes at the pace at which the destination array takes writes. A local copy and a remote "
                      "copy of the same file set that both keep up with the array therefore take almost the same "
                      "time.", text)
        self.assertIn("The 1 KB file set holds about 2 GB, which fits in memory many times over", text)
        self.assertIn("times the other tools on the copies of the sets of 1 KB files", text)
        self.assertIn("<code>rm -rf</code> clears the trees from then on and the run page says so. A tool takes as "
                      "long as its files are many, and the sets of larger files have too few of them for a run of a "
                      "minute, so no tool is timed there. On the arrays <code>rcmp</code> and <code>rlink</code> "
                      "follow the local copies only: a remote copy leaves the same tree, and comparing it or "
                      "hard-linking it once more would measure the same again. <code>rrm</code> follows every copy, "
                      "since its run is what removes it.</li>", text)
        for gone in ("1.024 TB", "about a terabyte each", "on each local copy, and on each remote",
                     "<td>10,240,000</td>", "100 MB</td>", "400 GB", "4,096,000", "one to five minutes"):
            self.assertNotIn(gone, text)
        # the sizes before are named once, where the page says what became of their trends
        self.assertEqual(text.count("10,240,000"), 1)

    def test_the_numbers_of_the_setup_page_are_those_of_the_file_sets(self):
        shapes = diagnostics.MATRIX_CASE_SHAPES
        for name, files in (("bulk-1g-200gb", "200"), ("bulk-1m-200gb", "200,000"),
                            ("tiny-1k-2048k", "2,048,000"), ("tiny-1k-2048k-memory", "2,048,000")):
            shape = shapes[name]
            count = shape["files_per_leaf"]
            for width in shape["directory_widths"]:
                count *= width
            self.assertEqual(f"{count:,}", files, name)

    def test_the_notes_on_reading_the_plots_follow_the_file_sets(self):
        before, after = history.reading_html(True, True), history.reading_html(True, True, sized=True)
        self.assertEqual(before, history.reading_html(True, True, sized=False))
        self.assertIn("For the terabyte file sets it cannot be shorter than the destination array allows", before)
        self.assertIn("A tool's page leaves out the file sets on which it takes milliseconds.", before)
        self.assertIn("For the sets of 1 GB and 1 MB files it cannot be shorter than the destination array "
                      "allows: see <a href='#storage'>What the storage allows</a>.", after)
        self.assertIn("The AWS benchmarks page shows each tool on the file set with the most files. The tools are "
                      "timed on the sets of 1 KB files only: a tool takes as long as its files are many.</li>", after)
        for gone in ("terabyte", "milliseconds"):
            self.assertNotIn(gone, after)
        # nothing else differs
        strip = lambda text: text.replace(
            "For the terabyte file sets", "For the sets of 1 GB and 1 MB files").replace(
            "A tool's page leaves out the file sets on which it takes milliseconds.",
            "The tools are timed on the sets of 1 KB files only: a tool takes as long as its files are many.")
        self.assertEqual(strip(before), after)
        # and a page without the other tools says nothing of them
        self.assertNotIn("1 KB files only", history.reading_html(True, False, sized=True))

    def test_before_that_it_describes_the_file_sets_there_were(self):
        text = self.text()
        self.assertEqual(text, self.text(sized=False))
        for row in ("<tr><td>1 GB</td><td>1,000</td><td>1,000,000,000</td><td>1 TB</td></tr>",
                    "<tr><td>1 KB</td><td>10,240,000</td><td>1,000</td><td>10.24 GB</td></tr>",
                    "<tr><td>1 KB in memory</td><td>10,240,000</td><td>1,000</td><td>10.24 GB</td></tr>"):
            self.assertIn(row, text)
        self.assertNotIn("200 GB", text)
        self.assertNotIn("a minute or two", text)
        self.assertNotIn("no tool is timed there", text)


if __name__ == "__main__":
    unittest.main()
