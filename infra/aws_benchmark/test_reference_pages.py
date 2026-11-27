"""Pages once cp and rsync were measured: each kind of copy says what they took beside rcp, and how."""

import re
import statistics
import unittest
from unittest import mock

from benchmarks import matrix
from . import common, production_diagnostics as diagnostics, production_export as export, production_history as history
from . import test_matrix_diagnostics
from .test_matrix_pages import matrix_example
from .test_production_history import split
from .test_reference_cells import COMMISSION, CUT, SUITE, index, sampled_reference
from .test_storage_cells import PREPARED
from .test_tool_pages import weeks

WEEKLY = common.MATRIX_WEEKLY
SMALL, LARGE, MOUNTED = "tiny-1k-2048k", "bulk-1g-200gb", "tiny-1k-700x1024"
FAILED = dict(status="failed", stage="copy", message="copy 1 failed with exit code 23; see the logs")


def took(case, scenario, *seconds):
    """Give one cell of a runner's document these durations, and the cells beside it the limit that follows.

    A cell that did not complete keeps what it has: it has no such copies.
    """
    def change(raw):
        cell = next(cell for cell in raw["cells"] if (cell["case_id"], cell["scenario"]) == (case, scenario))
        if cell["summary"] is None:
            return
        for item, value in zip(cell["copies"], seconds, strict=True):
            item["elapsed_seconds"] = value
        cell["summary"] = dict(unit="seconds", median=statistics.median(seconds), minimum=min(seconds),
                               maximum=max(seconds), samples=list(seconds))
        for other in raw["cells"]:
            if other["case_id"] == case and "limit_seconds" in other and \
                    matrix.REFERENCES[other["scenario"]]["beside"] == scenario:
                other["limit_seconds"] = raw["context"]["reference_limit"] * cell["summary"]["median"]
        matrix.validate(raw)
    return change


def every(*changes):
    def change(raw):
        for item in changes:
            item(raw)
    return change


# rcp in 130 s, cp in 2.8 times that, rsync in 4.5 times that; a remote copy in 41 s and the pull in 9.2 times that
TIMES = every(took(SMALL, "local", 129.0, 130.0, 131.5), took(SMALL, "local-cp", 360.0, 364.0, 370.0),
              took(SMALL, "local-rsync", 580.0, 585.0, 601.0), took(SMALL, "remote", 40.0, 41.0, 43.0),
              took(SMALL, "remote-rsync", 370.0, 377.2, 380.0), took(MOUNTED, "nfs", 300.0, 330.0, 331.0))


def reference(run="43400000001", at="2026-12-03T10:00:00+00:00", outcomes=None, adjust=TIMES, suite=SUITE):
    return matrix_example(suite, run=run, at=at, outcomes=outcomes, adjust=adjust)


def runs(*more, weekly=2):
    """Weekly runs of the weekly suite, then the given ones."""
    return split(weeks(weekly, suite=WEEKLY, first="2026-12-07T08:30:00+00:00") + list(more))


def section(html, case):
    start = html.index("<section id='history-" + case + "'")
    return html[start:html.index("</section>", start)]


def other_tools(html, case):
    """The part of a file set's section that says what other tools took."""
    text = section(html, case)
    start = text.index("<div class='reference'")
    return text[start:text.index("</div><details>", start)]


def rows(html):
    return [re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row) for row in re.findall(r"<tr>(.*?)</tr>", html)]


class ReferenceSectionTests(unittest.TestCase):
    def test_the_page_of_local_copies_says_what_cp_and_rsync_took_beside_rcp(self):
        html = history.render(*runs(reference()))
        part = other_tools(html, SMALL)
        self.assertIn("<h3 id='reference-" + SMALL + "'>Other tools</h3>", part)
        self.assertIn("<p>What <code>cp</code> and <code>rsync</code> took for this copy, on the same machine and after "
                      "rcp. Measured once, for reference: run <a href='runs/repo-655073755-run-43400000001.html'>"
                      "43400000001-1</a> on <time datetime='2026-12-03T10:00:00Z'>3 Dec 2026</time>, with "
                      "<code>cp (GNU coreutils) 9.4</code> and <code>rsync  version 3.2.7  protocol version 31</code>."
                      "</p>", part)
        self.assertEqual(rows(part), [
            ["Tool", "Took", "Against rcp"],
            ["rcp", "2 min 10 s", ""],
            ["<code>cp -a</code>", "6 min 04 s", "2.8 times as long"],
            ["<code>rsync -a</code>", "9 min 45 s", "4.5 times as long"]])
        self.assertIn("<p class='reading'>Each time is the median of that run's three copies by the tool. "
                      "<a href='setups.html#reference'>How these were measured</a>.</p>", part)
        # between the plots of rcp and their data table
        text = section(html, SMALL)
        self.assertLess(text.index("How to read these plots"), text.index("<div class='reference'"))
        self.assertLess(text.index("<div class='reference'"), text.index("<details><summary>Data table</summary>"))
        # every file set of the arrays was copied by them
        self.assertEqual(html.count("<div class='reference'"), 3)

    def test_the_page_of_remote_copies_has_rsync_alone_and_says_how_it_copied(self):
        html = history.render(*runs(reference()), history.REMOTE)
        part = other_tools(html, SMALL)
        self.assertIn("<p>What <code>rsync</code> took to pull this file set over SSH on the same loopback, after rcp. "
                      "Measured once, for reference: run <a href=", part)
        self.assertIn(", with <code>rsync  version 3.2.7  protocol version 31</code>.</p>", part)
        self.assertNotIn("coreutils", part)
        self.assertEqual(rows(part), [["Tool", "Took", "Against rcp"], ["rcp", "41.0 s", ""],
                                      ["<code>rsync -a</code>", "6 min 17 s", "9.2 times as long"]])

    def test_the_page_of_copies_on_a_network_filesystem_has_both(self):
        html = history.render(*runs(reference()), history.NFS)
        part = other_tools(html, MOUNTED)
        self.assertIn("<p>What <code>cp</code> and <code>rsync</code> took for this copy, between the same two "
                      "mounts and after rcp. Measured once", part)
        self.assertEqual([row[0] for row in rows(part)], ["Tool", "rcp", "<code>cp -a</code>", "<code>rsync -a</code>"])
        self.assertEqual(rows(part)[1], ["rcp", "5 min 30 s", ""])

    def test_a_copy_that_was_cut_off_is_shown_as_not_finished_in_the_time_it_was_given(self):
        cut = {index(SUITE, SMALL, "local-rsync"): CUT, index(SUITE, MOUNTED, "nfs-cp"): CUT,
               index(SUITE, MOUNTED, "nfs-rsync"): CUT}
        records, companions = runs(reference(outcomes=cut))
        part = other_tools(history.render(records, companions), SMALL)
        # ten times the 130 s that rcp took
        self.assertEqual(rows(part)[2:], [
            ["<code>cp -a</code>", "6 min 04 s", "2.8 times as long"],
            ["<code>rsync -a</code>", "did not finish in 21 min 40 s", "more than 10 times as long"]])
        self.assertIn("<p class='reading'>Each time is the median of that run's three copies by the tool. A tool has "
                      "10 times what rcp took: a copy that takes longer is cut off and the tool is not tried again. "
                      "<a href='setups.html#reference'>How these were measured</a>.</p>", part)
        part = other_tools(history.render(records, companions, history.NFS), MOUNTED)
        self.assertEqual(rows(part)[1:], [
            ["rcp", "5 min 30 s", ""],
            ["<code>cp -a</code>", "did not finish in 55 min 00 s", "more than 10 times as long"],
            ["<code>rsync -a</code>", "did not finish in 55 min 00 s", "more than 10 times as long"]])

    def test_a_tool_that_the_run_did_not_measure_says_so_and_leaves_the_why_to_the_run_page(self):
        missing = {index(SUITE, SMALL, "local-cp"): FAILED}
        part = other_tools(history.render(*runs(reference(outcomes=missing))), SMALL)
        self.assertEqual(rows(part)[2:], [
            ["<code>cp -a</code>", "not measured: see <a href='runs/repo-655073755-run-43400000001.html#results'>"
                                   "the run</a>", ""],
            ["<code>rsync -a</code>", "9 min 45 s", "4.5 times as long"]])

    def test_a_cell_that_timed_out_before_its_copy_started_was_not_cut_off(self):
        # only a copy that ran into its limit has a time that it did not finish in
        early = {index(SUITE, SMALL, "local-cp"): dict(status="timed_out", stage="cache",
                                                       message="cache preparation before copy 1 failed: timed out")}
        part = other_tools(history.render(*runs(reference(outcomes=early))), SMALL)
        self.assertEqual(rows(part)[2][1:], ["not measured: see <a href='runs/repo-655073755-run-43400000001.html"
                                             "#results'>the run</a>", ""])
        self.assertNotIn("is cut off", part)

    def test_a_file_set_whose_copy_by_rcp_was_not_measured_has_no_such_part(self):
        # without rcp's time there is nothing to stand beside: the run page lists the cells all the same
        failed = {index(SUITE, SMALL, scenario): dict(FAILED, message="copy 1 failed") if scenario == "local" else dict(
            FAILED, message="the local cell whose copies set this cell's time limit did not complete: copy 1 failed")
            for scenario in ("local", "local-cp", "local-rsync")}
        html = history.render(*runs(reference(outcomes=failed)))
        self.assertNotIn("<div class='reference'", section(html, SMALL))
        self.assertIn("<div class='reference'", section(html, LARGE))

    def test_the_newest_run_that_copied_a_file_set_with_them_is_the_one_shown(self):
        later = reference(run="43400000009", at="2027-01-05T10:00:00+00:00",
                          adjust=every(TIMES, took(SMALL, "local-cp", 400.0, 403.0, 410.0)))
        html = history.render(*runs(later, reference()))
        part = other_tools(html, SMALL)
        self.assertIn("run-43400000009.html'>43400000009-1</a> on <time datetime='2027-01-05T10:00:00Z'>5 Jan 2027</time>",
                      part)
        self.assertEqual(rows(part)[2], ["<code>cp -a</code>", "6 min 43 s", "3.1 times as long"])
        # a later run that did not measure a file set leaves the one before it standing
        failed = {index(SUITE, SMALL, scenario): dict(FAILED, message="no") for scenario in ("local", "local-cp",
                                                                                           "local-rsync")}
        broken = reference(run="43400000011", at="2027-02-02T10:00:00+00:00", outcomes=failed)
        self.assertIn("run-43400000009.html'>", other_tools(history.render(*runs(later, reference(), broken)), SMALL))

    def test_a_ratio_is_given_to_two_figures_and_a_tool_that_was_faster_is_said_to_be(self):
        for seconds, said in ((130.0, "as long"), (131.0, "as long"), (140.0, "1.1 times as long"),
                              (1287.0, "9.9 times as long"), (1299.0, "10 times as long"), (104.0, "0.8 times as long"),
                              (65.0, "0.5 times as long")):
            with self.subTest(seconds=seconds):
                run = reference(adjust=every(TIMES, took(SMALL, "local-cp", seconds, seconds, seconds)))
                self.assertEqual(rows(other_tools(history.render(*runs(run)), SMALL))[2][2], said)

    def test_the_pages_of_the_other_tools_have_no_such_part(self):
        records, companions = runs(reference())
        for tool in history.TOOLS:
            self.assertNotIn("class='reference'", history.render(records, companions, tool), tool)

    def test_the_copies_by_rcp_of_such_a_run_are_points_of_its_trends_and_nothing_is_outside_them(self):
        records, companions = runs(reference())
        for scenario in (history.LOCAL, history.REMOTE, history.NFS):
            html = history.render(records, companions, scenario)
            self.assertNotIn("outside these trends", html, scenario)
            case = MOUNTED if scenario == history.NFS else SMALL
            self.assertIn(" · 3 runs</p>", section(html, case), scenario)
        # the run is one of the list like any other
        self.assertEqual(len(history.model(records, companions)[0][SMALL]), 3)

    def test_pages_without_such_a_run_have_no_such_part(self):
        records, companions = runs()
        for scenario in history.present(records):
            self.assertNotIn("reference", history.render(records, companions, scenario), scenario)
        self.assertNotIn("measured once", history.overview(records, companions))

    def test_a_run_that_only_tried_has_no_part_either(self):
        # a run without a measurement is no run of the history
        nothing = {number: FAILED for number in range(len(common.MATRIX_SUITES[SUITE]))}
        records, companions = runs(reference(outcomes=nothing))
        self.assertNotIn("class='reference'", history.render(records, companions))

    def test_the_first_page_says_where_they_are(self):
        html = history.overview(*runs(reference()))
        self.assertIn("<p class='window'>What <code>cp</code> and <code>rsync</code> take for the same copies was "
                      "measured once, for reference: see the page of each kind of copy.</p>", html)

    def test_the_run_of_the_commissioning_suite_is_shown_on_no_trend(self):
        # its fixtures have no trend, so nothing of it stands beside one
        records, companions = runs(reference(suite=COMMISSION, adjust=None))
        for scenario in (history.LOCAL, history.REMOTE, history.NFS):
            self.assertNotIn("class='reference'", history.render(records, companions, scenario))
        self.assertNotIn("measured once", history.overview(records, companions))


class ReferenceRunPageTests(unittest.TestCase):
    def test_the_cells_of_another_tool_are_named_after_the_copy_and_the_tool(self):
        self.assertEqual({scenario: name for scenario, name in history.SCENARIOS.items() if scenario in matrix.REFERENCES},
                         {"local-cp": "Local copy by cp", "nfs-cp": "Copy between NFS mounts by cp",
                          "memory-cp": "Copy in memory by cp", "local-rsync": "Local copy by rsync",
                          "remote-rsync": "Remote copy over loopback by rsync",
                          "nfs-rsync": "Copy between NFS mounts by rsync", "memory-rsync": "Copy in memory by rsync"})
        self.assertEqual(list(history.SCENARIOS)[-7:], list(matrix.REFERENCES))
        # a cell of another tool is no tool cell: those time the tools of the suite on a copy
        self.assertEqual(history.TOOL_CELLS, frozenset(matrix.TOOLS))
        self.assertEqual(set(history.REFERENCE_CELLS), set(matrix.REFERENCES))

    def test_the_run_page_lists_the_cells_and_sets_every_tool_beside_rcp(self):
        cut = {index(SUITE, MOUNTED, "nfs-cp"): CUT, index(SUITE, SMALL, "local-rsync"): FAILED}
        record, extra = reference(outcomes=cut)
        html = export.render(record, diagnostics=extra)
        self.assertIn("<td>1 KB files</td><td>Local copy by cp</td><td>measured</td><td>364.000</td>", html)
        self.assertIn("<td>1 KB files on NFS</td><td>Copy between NFS mounts by cp</td><td>timed out</td>", html)
        start = html.index("<h3 id='reference'>cp and rsync beside rcp</h3>")
        part = html[start:html.index("</table>", start)]
        self.assertIn("<p>What each tool took for the copies of this run, as the median of its copies, and how that "
                      "stands against rcp. A tool has 10 times what rcp took and is cut off then.</p>", part)
        table = rows(part)
        self.assertEqual(table[0], ["File set", "Copy", "rcp", "<code>cp -a</code>", "<code>rsync -a</code>"])
        self.assertEqual(table[1:4], [
            ["1 KB files", "Local copy", "2 min 10 s", "6 min 04 s<br>2.8 times as long", "not measured"],
            ["1 KB files", "Remote copy over loopback", "41.0 s", "—", "6 min 17 s<br>9.2 times as long"],
            ["1 KB files on NFS", "Copy between NFS mounts", "5 min 30 s",
             "did not finish in 55 min 00 s<br>more than 10 times as long", "10 min 07 s<br>1.8 times as long"]])
        self.assertEqual(len(table), 1 + 7)
        # in the results, before the summaries of the run as history points
        self.assertLess(html.index("<table class='data cells'>"), start)
        self.assertLess(start, html.index("This run as one history point"))
        self.assertIn("<h3>Frozen measurements</h3>", html)

    def test_a_run_without_such_cells_has_no_such_table(self):
        record, extra = weeks(1, suite=WEEKLY)[0]
        self.assertNotIn("id='reference'", export.render(record, diagnostics=extra))

    def test_a_run_that_measured_no_copy_by_rcp_has_no_such_table(self):
        nothing = {number: FAILED for number in range(len(common.MATRIX_SUITES[SUITE]))}
        record, extra = reference(outcomes=nothing)
        html = export.render(record, diagnostics=extra)
        self.assertNotIn("id='reference'", html)
        self.assertIn("<td>Local copy by rsync</td><td>failed</td>", html)

    def test_where_the_time_went_names_the_copies_of_each_tool(self):
        raw, _ = sampled_reference(COMMISSION)
        record, _ = matrix_example(COMMISSION)
        extra = diagnostics.derive(record, b"archive", raw, dict(test_matrix_diagnostics.WORKER, other_storage=PREPARED))
        html = history.time_use_html(diagnostics.time_use(extra, record))
        for name in ("Local copies", "Remote copies", "Copies between NFS mounts", "Copies by cp", "Copies by rsync"):
            self.assertIn("<tr><td>" + name + "</td>", html)
        self.assertNotIn("<code>cp</code>", html)


class ReferenceSetupPageTests(unittest.TestCase):
    def configuration(self, **options):
        return diagnostics.configuration_html([], remote=True, tools=True, elsewhere=True, sized=True, **options)

    def test_the_setup_page_says_how_cp_and_rsync_are_measured_once_a_run_has_measured_them(self):
        html = self.configuration(reference=True)
        start = html.index("<li id='reference'><strong>cp and rsync.</strong>")
        item = html[start:html.index("</li>", start)]
        for said in ("<code>cp -a</code> and <code>rsync -a</code>, as the image of the worker has them",
                     "three times, after rcp's three copies of it, between the same two places and with the cache "
                     "dropped before each copy",
                     "<code>rsync</code> pulls the file set over SSH from the private sshd on loopback",
                     "counts when it exits cleanly",
                     "no CPU time and no memory",
                     "10 times the median of rcp's copies",
                     "is cut off, shown as not finished, and the tool is not tried again on that file set",
                     "not every week"):
            self.assertIn(said, item)
        self.assertEqual(self.configuration(reference=False), self.configuration())
        self.assertNotIn("id='reference'", self.configuration())

    def test_the_multiple_on_the_pages_is_the_one_of_the_suites(self):
        self.assertEqual({common.SUITE_PROFILES[suite]["reference_limit"] for suite in common.MATRIX_REFERENCE_SUITES},
                         {common.MATRIX_REFERENCE_LIMIT})
        with mock.patch.object(diagnostics, "MATRIX_REFERENCE_LIMIT", 7):
            self.assertIn("has 7 times the median of rcp's copies", self.configuration(reference=True))

    def test_the_table_of_what_the_arrays_did_names_the_copies_of_each_tool(self):
        record, _ = matrix_example(SUITE)
        raw, _ = sampled_reference(SUITE)
        extra = diagnostics.derive(record, b"archive", raw, dict(test_matrix_diagnostics.WORKER, other_storage=PREPARED))
        figures = diagnostics.storage_use(extra, record)
        html = history.storage_html("43400000001-1", "runs/x.html", figures, history.SIZED)
        # every cell of the three file sets of the arrays; the mounts show those arrays under another name
        self.assertEqual((len(figures), html.count("<tr><td>")), (18, 15))
        self.assertIn("<td>1 KB files</td><td>Local copy by cp</td>", html)
        self.assertIn("<td>1 GB files</td><td>Remote copy over loopback by rsync</td>", html)
        # the copies of another tool are copies: the table has no tool run to speak of
        self.assertIn("<th>File set</th><th>Copy</th>", html)
        self.assertNotIn("tool run", html)


if __name__ == "__main__":
    unittest.main()
