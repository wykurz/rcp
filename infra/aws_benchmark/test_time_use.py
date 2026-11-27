"""A run page says where the run's time went: each kind of work with its share, then each file set."""

import re
import unittest

from . import production_diagnostics as diagnostics, production_export as export, production_history as history
from .test_matrix_pages import matrix_example
from .test_production_history import observed

# one file set, copied locally and remotely, with the three tools after each kind of copy
TOOLS = "matrix-1g-local+tools+remote+tools"
# four file sets, each copied locally and remotely
EVERY = "matrix-all-local+remote"


def phase(name, seconds, case=0, status="observed"):
    """One phase as the sampler publishes it: it lasts from its mark to the next one."""
    return dict(phase=name, case_index=case, trial_index=None, status=status, elapsed_seconds=seconds,
                samples=int(seconds // 10) + 1, metrics={})


def system(phases):
    return dict(status="partial", samples=sum(item["samples"] for item in phases), phases=phases)


def sample(extra, run_level, steps):
    """Give a companion the phases its sampler marked: those outside any command, and each command's own."""
    extra["setup"]["instrumentation"] = dict(diagnostics.TELEMETRY_DESCRIPTOR)
    extra["setup_id"] = diagnostics.setup_id(extra["setup"])
    extra["system"] = system(run_level)
    for index, trial in enumerate(extra["trials"]):
        phases = steps(trial)
        if phases is not None:
            trial["system"] = system([dict(item, trial_index=index) for item in phases])


def marked(suite, run_level, steps, **options):
    """A matrix run whose sampler marked phases. `steps` gives a command's phases, or None when it recorded none."""
    record, extra = matrix_example(suite, **options)
    sample(extra, run_level, steps)
    diagnostics.validate(extra, record)
    return record, extra


def with_tools(trial):
    """What a command of the suite with tools recorded: a copy drops the cache first, rlink leaves a tree to remove."""
    scenario = trial["scenario"]
    if scenario == "local":
        return [phase("cache", 10.0), phase("command", 620.0)]
    if scenario == "remote":
        return [phase("cache", 8.0), phase("command", 640.0)]
    if scenario.endswith("-rcmp"):
        return [phase("command", 60.0)]
    if scenario.endswith("-rlink"):
        return [phase("command", 50.0), phase("cleanup", 20.0)]
    return [phase("command", 30.0)]


RUN_LEVEL = [phase("setup", 0.5, None), phase("capacity", 0.0), phase("generation", 600.0), phase("cleanup", 30.0)]


def section(html):
    start = html.index("<section aria-labelledby='time-use'>")
    return html[start:html.index("</section>", start)]


def rows(html):
    """Each row of the section's first table as its cells' text, a heading row as one cell."""
    table = section(html)
    table = table[table.index("<tbody>"):table.index("</table>")]
    return [[re.sub(r"<[^>]+>", "", cell) for cell in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row)]
            for row in re.findall(r"<tr>(.*?)</tr>", table)]


class TimeUseTests(unittest.TestCase):
    def test_every_recorded_second_is_counted_once_by_kind_of_work(self):
        record, extra = marked(TOOLS, RUN_LEVEL, with_tools)
        use = diagnostics.time_use(extra, record)
        # three copies of each kind, and three runs of each tool after each kind of copy
        self.assertEqual(use["commands"], dict(local=1860.0, remote=1920.0, rcmp=360.0, rlink=300.0, rrm=180.0))
        self.assertEqual(use["around"], dict(generation=600.0, cache=54.0, links=120.0, file_sets=30.0, other=0.5))
        self.assertEqual(use["total"], 1860.0 + 1920.0 + 360.0 + 300.0 + 180.0 + 600.0 + 54.0 + 120.0 + 30.0 + 0.5)
        self.assertTrue(use["complete"])

    def test_what_is_removed_after_a_copy_is_told_from_what_is_removed_after_rlink(self):
        record, extra = marked(EVERY, RUN_LEVEL[:1], lambda trial: [phase("command", 600.0), phase("cleanup", 40.0)])
        use = diagnostics.time_use(extra, record)
        self.assertEqual(use["around"], dict(copies=24 * 40.0, other=0.5))

    def test_each_file_set_has_its_commands_its_generation_and_the_rest(self):
        run_level = [phase("setup", 0.5, None)]
        for index, seconds in enumerate((610.0, 630.0, 680.0, 660.0)):
            run_level += [phase("capacity", 0.0, index), phase("generation", seconds, index),
                          phase("cleanup", 10.0 * index, index)]
        cases = ["bulk-1g-1tb", "bulk-100m-1024gb", "bulk-1m-1024gb", "tiny-1k-10240k"]
        steps = lambda trial: [phase("cache", 5.0, cases.index(trial["case_id"])),
                               phase("command", 200.0 if trial["scenario"] == "remote" else 650.0,
                                     cases.index(trial["case_id"]))]
        record, extra = marked(EVERY, run_level, steps)
        use = diagnostics.time_use(extra, record)
        self.assertEqual([item["case_id"] for item in use["cases"]], cases)
        # three local copies and three remote ones; six cache drops and the removal of the file set
        self.assertEqual([(item["commands"], item["generation"], item["rest"]) for item in use["cases"]],
                         [(2550.0, 610.0, 30.0), (2550.0, 630.0, 40.0), (2550.0, 680.0, 50.0), (2550.0, 660.0, 60.0)])
        # what belongs to no file set is in the run's total alone
        self.assertEqual(use["total"], sum(item["commands"] + item["generation"] + item["rest"]
                                           for item in use["cases"]) + 0.5)

    def test_copies_on_other_storage_are_kinds_of_their_own_and_a_tool_counts_whichever_copies_it_follows(self):
        record, extra = marked("matrix-weekly-1", [phase("setup", 1.0, None)], lambda trial: [phase("command", 100.0)])
        use = diagnostics.time_use(extra, record)
        # three copies in each cell. The three sets of large files and the small files on the arrays are copied
        # locally and remotely, the small files also between NFS mounts and in memory; a tool follows each of the
        # four kinds of copy of small files
        self.assertEqual(use["commands"], dict(local=1200.0, remote=1200.0, nfs=300.0, memory=300.0,
                                               rcmp=1200.0, rlink=1200.0, rrm=1200.0))
        self.assertEqual([item["case_id"] for item in use["cases"]],
                         ["bulk-1g-400gb", "bulk-100m-400gb", "bulk-1m-400gb", "tiny-1k-4096k", "tiny-1k-700x1024",
                          "tiny-1k-4096k-memory"])
        names = [row[0] for row in rows(export.render(record, diagnostics=extra))]
        self.assertEqual(names[:8], ["Commands the run times", "Local copies", "Remote copies", "Compare with rcmp",
                                     "Hard-link with rlink", "Remove with rrm", "Copies between NFS mounts",
                                     "Copies in memory"])

    def test_a_run_that_marked_no_phase_has_no_breakdown(self):
        record, extra = matrix_example(TOOLS)
        self.assertIsNone(diagnostics.time_use(extra, record))

    def test_a_phase_that_was_cut_counts_for_as_long_as_it_ran(self):
        steps = lambda trial: [phase("command", 100.0, status="interrupted" if trial["iteration"] == 3 else "observed")]
        record, extra = marked("matrix-1g-local", [phase("generation", 50.0, status="observed")], steps)
        use = diagnostics.time_use(extra, record)
        self.assertEqual((use["commands"], use["around"]), (dict(local=300.0), dict(generation=50.0)))

    def test_a_run_with_a_command_that_recorded_no_phase_is_not_complete(self):
        steps = lambda trial: None if trial["iteration"] == 2 else [phase("command", 100.0)]
        record, extra = marked("matrix-1g-local", RUN_LEVEL, steps)
        use = diagnostics.time_use(extra, record)
        self.assertEqual(use["commands"], dict(local=200.0))
        self.assertFalse(use["complete"])

    def test_a_run_whose_steps_outside_its_commands_recorded_no_phase_is_not_complete(self):
        record, extra = marked("matrix-1g-local", [], lambda trial: [phase("command", 100.0)])
        use = diagnostics.time_use(extra, record)
        self.assertEqual((use["commands"], use["around"]), (dict(local=300.0), {}))
        self.assertFalse(use["complete"])

    def test_the_paired_harness_has_its_checks_as_a_kind_of_work_and_no_file_sets(self):
        record, extra = observed("42458459014", "08")
        sample(extra, [phase("setup", 0.25, None), phase("generation", 615.5), phase("initial_verification", 25.0)],
               lambda trial: [phase("preparation", 0.25), phase("cache", 20.0), phase("command", 621.0),
                              phase("destination", 6.0), phase("metadata", 0.5), phase("source", 6.5),
                              phase("cleanup", 15.0)])
        diagnostics.validate(extra, record)
        use = diagnostics.time_use(extra, record)
        # four copies: two by each build
        self.assertEqual(use["commands"], dict(local=4 * 621.0))
        self.assertEqual(use["around"], dict(generation=615.5, checks=25.0 + 4 * 13.0, cache=80.0, copies=60.0,
                                             other=0.25 + 4 * 0.25))
        self.assertEqual(use["cases"], [])


class SpanTests(unittest.TestCase):
    def test_a_time_is_read_in_the_two_units_that_matter_at_its_size(self):
        for seconds, text in ((0.0, "0.0 s"), (0.74, "0.7 s"), (59.94, "59.9 s"), (59.96, "1 min 00 s"),
                              (620.4, "10 min 20 s"), (3599.4, "59 min 59 s"), (3599.6, "1 h 00 min"),
                              (18791.0, "5 h 13 min"), (30009.0, "8 h 20 min")):
            self.assertEqual(history.span(seconds), text)


class RunPageTests(unittest.TestCase):
    def test_the_section_follows_the_results_and_gives_the_total_and_its_two_halves(self):
        record, extra = marked(TOOLS, RUN_LEVEL, with_tools)
        html = export.render(record, diagnostics=extra)
        self.assertLess(html.index("<h2 id='results'>"), html.index("<h2 id='time-use'>Where the time went</h2>"))
        self.assertLess(html.index("<h2 id='time-use'>"), html.index("<h2 id='resource-diagnostics'>"))
        # 4,620 s of commands and 804.5 s around them; half a second rounds up
        self.assertIn("<p>The worker's harness recorded <strong>1 h 30 min</strong> of work in this run: "
                      "1 h 17 min (85.2%) in the commands it times and 13 min 25 s (14.8%) around them.", section(html))

    def test_each_kind_of_work_has_a_row_largest_first_within_its_half(self):
        record, extra = marked(TOOLS, RUN_LEVEL, with_tools)
        self.assertEqual(rows(export.render(record, diagnostics=extra)), [
            ["Commands the run times"],
            ["Remote copies", "32 min 00 s", "35.4%", ""],
            ["Local copies", "31 min 00 s", "34.3%", ""],
            ["Compare with rcmp", "6 min 00 s", "6.6%", ""],
            ["Hard-link with rlink", "5 min 00 s", "5.5%", ""],
            ["Remove with rrm", "3 min 00 s", "3.3%", ""],
            ["Around them"],
            ["Generating the file sets", "10 min 00 s", "11.1%", ""],
            ["Removing the trees that rlink made", "2 min 00 s", "2.2%", ""],
            ["Emptying the page cache before a command", "54.0 s", "1.0%", ""],
            ["Removing each file set and what was left of its copies", "30.0 s", "0.6%", ""],
            ["Other steps", "0.5 s", "0.0%", ""]])
        # each half is a group of rows that its heading names
        html = section(export.render(record, diagnostics=extra))
        self.assertIn("</thead><tbody><tr><th colspan='4' scope='rowgroup'>Commands the run times</th></tr>", html)
        self.assertIn("</tr></tbody><tbody><tr><th colspan='4' scope='rowgroup'>Around them</th></tr>", html)
        self.assertEqual(html.count("<tbody>"), html.count("</tbody>"))

    def test_the_bars_are_in_proportion_and_the_longest_fills_its_column(self):
        steps = lambda trial: [phase("command", 100.0)]
        record, extra = marked("matrix-1g-local", [phase("generation", 100.0)], steps)
        html = section(export.render(record, diagnostics=extra))
        # three copies of 100 s and a generation of 100 s: the whole column of 160 units and a third of it,
        # each less the round end of four units
        self.assertEqual(re.findall(r"<svg class='share' width='(\d+)' height='(\d+)'", html), [("160", "10")] * 2)
        self.assertEqual(re.findall(r"<path class='bar' d='M0,1h([\d.]+)a4,4 0 0 1 0,8H0z'/>", html), ["156.00", "49.33"])

    def test_a_kind_of_work_too_short_for_a_rounded_end_is_a_plain_sliver(self):
        steps = lambda trial: [phase("cache", 1.0), phase("command", 1000.0)]
        record, extra = marked("matrix-1g-local", [phase("setup", 0.0, None)], steps)
        html = section(export.render(record, diagnostics=extra))
        # 3 s beside copies of 3,000 s: a thousandth of the longest bar, 0.16 of its 160 units
        self.assertIn("<rect class='bar' y='1' width='0.16' height='8'/>", html)
        # and what took no time has no bar at all
        self.assertIn("<tr><td>Other steps</td><td class='number'>0.0 s</td><td class='number'>0.0%</td><td></td></tr>",
                      html)

    def test_a_matrix_run_lists_its_file_sets_by_name_with_their_share(self):
        run_level = [phase("setup", 0.5, None), phase("generation", 600.0, 0), phase("generation", 0.5, 1)]
        index = lambda trial: ("commission-64m", "commission-10k").index(trial["case_id"])
        steps = lambda trial: [phase("cache", 2.0, index(trial)),
                               phase("command", (300.0, 1.0)[index(trial)], index(trial))]
        record, extra = marked("matrix-commission-local", run_level, steps)
        html = section(export.render(record, diagnostics=extra))
        self.assertIn("<h3>By file set</h3>", html)
        self.assertIn("<th>File set</th><th class='number'>Commands</th><th class='number'>Generating it</th>"
                      "<th class='number'>The rest</th><th class='number'>Together</th>"
                      "<th class='number'>Share of the run</th>", html)
        # a file set the pages have no name for keeps its identifier
        self.assertIn("<tr><td><code>commission-64m</code></td><td class='number'>15 min 00 s</td>"
                      "<td class='number'>10 min 00 s</td><td class='number'>6.0 s</td>"
                      "<td class='number'>25 min 06 s</td><td class='number'>99.3%</td></tr>", html)
        record, extra = marked("matrix-1g-local", RUN_LEVEL, lambda trial: [phase("command", 100.0)])
        self.assertIn("<tr><td>1 GB files</td>", section(export.render(record, diagnostics=extra)))

    def test_the_paired_harness_has_the_kinds_of_work_and_no_table_of_file_sets(self):
        record, extra = observed("42458459014", "08")
        sample(extra, [phase("generation", 600.0), phase("initial_verification", 25.0)],
               lambda trial: [phase("command", 621.0), phase("destination", 6.0)])
        diagnostics.validate(extra, record)
        html = export.render(record, diagnostics=extra)
        self.assertIn(["Checking the file sets and the copies", "49.0 s", "1.6%", ""], rows(html))
        self.assertNotIn("By file set", section(html))

    def test_a_run_that_marked_no_phase_has_no_section(self):
        record, extra = matrix_example(TOOLS)
        self.assertNotIn("time-use", export.render(record, diagnostics=extra))
        self.assertNotIn("time-use", export.render(record))

    def test_a_run_that_recorded_one_kind_of_work_has_its_one_row(self):
        steps = lambda trial: [phase("command", 100.0)] if trial["iteration"] == 1 else None
        record, extra = marked("matrix-1g-local", [], steps)
        self.assertEqual(rows(export.render(record, diagnostics=extra)),
                         [["Commands the run times"], ["Local copies", "1 min 40 s", "100.0%", ""]])

    def test_a_run_whose_phases_took_no_time_has_its_rows_without_shares_or_bars(self):
        record, extra = marked("matrix-1g-local", [phase("setup", 0.0, None)], lambda trial: [phase("command", 0.0)])
        html = export.render(record, diagnostics=extra)
        self.assertEqual(rows(html), [["Commands the run times"], ["Local copies", "0.0 s", "0.0%", ""],
                                      ["Around them"], ["Other steps", "0.0 s", "0.0%", ""]])
        self.assertNotIn("<svg class='share'", section(html))
        self.assertIn("<strong>0.0 s</strong> of work in this run: 0.0 s (0.0%) in the commands it times",
                      section(html))

    def test_a_run_with_a_command_that_recorded_no_phase_says_that_its_total_is_a_lower_bound(self):
        steps = lambda trial: None if trial["iteration"] == 2 else [phase("command", 100.0)]
        record, extra = marked("matrix-1g-local", RUN_LEVEL, steps)
        html = section(export.render(record, diagnostics=extra))
        self.assertIn("<p>The worker's harness recorded <strong>13 min 51 s</strong> of work in this run", html)
        self.assertIn("Some of its steps recorded no phase, so the run took longer than that.", html)
        # and its phases are not said to cover the harness
        self.assertIn("<p class='reading'>A phase lasts from the moment the harness marks it to its next mark. "
                      "Starting the machine", html)
        record, extra = marked("matrix-1g-local", RUN_LEVEL, lambda trial: [phase("command", 100.0)])
        self.assertNotIn("took longer", section(export.render(record, diagnostics=extra)))

    def test_the_stylesheet_draws_the_bars_in_the_colour_of_the_plots_and_sets_figures_to_the_right(self):
        from pathlib import Path
        style = (Path(__file__).resolve().parents[2] / "scripts/report-site/aws.css").read_text()
        rule = lambda selector: style[style.index(selector):].split("}", 1)[0]
        self.assertIn("fill: var(--series)", rule(".aws .share .bar {"))
        # a bar is a block of its own, so that each starts at the edge of its cell
        self.assertIn("display: block", rule(".aws .share {"))
        self.assertIn("text-align: right", rule(".aws th.number, .aws td.number {"))
        # a heading of figures stays on one line: a narrow window scrolls the table instead
        self.assertIn("white-space: nowrap", rule(".aws th.number {"))

    def test_a_run_that_times_rrm_says_that_its_runs_are_what_removes_the_copies(self):
        sentence = ("Each copy of this run is removed by a run of rrm that the run times: removing the copies is in "
                    "the row of rrm.")
        record, extra = marked(TOOLS, RUN_LEVEL, with_tools)
        self.assertIn(sentence + "</p></section>", section(export.render(record, diagnostics=extra)) + "</section>")
        # a run without rrm removes its copies around the commands, and has a row for that
        record, extra = marked(EVERY, RUN_LEVEL, lambda trial: [phase("command", 600.0), phase("cleanup", 40.0)])
        html = export.render(record, diagnostics=extra)
        self.assertNotIn("removed by a run of rrm", html)
        self.assertIn("Removing the copies", [row[0] for row in rows(html)])
        # and a run whose rrm failed at a copy has both: the sentence would then not hold for every copy
        steps = lambda trial: with_tools(trial) + ([phase("cleanup", 5.0)] if trial["scenario"] == "local" else [])
        record, extra = marked(TOOLS, RUN_LEVEL, steps)
        self.assertNotIn("removed by a run of rrm", export.render(record, diagnostics=extra))
        # nor does a run say it that does not time rrm, whether or not it recorded the removal of a copy
        record, extra = marked("matrix-1g-local", RUN_LEVEL, lambda trial: [phase("command", 100.0)])
        self.assertNotIn("removed by a run of rrm", export.render(record, diagnostics=extra))

    def test_the_section_says_what_its_times_cover(self):
        record, extra = marked("matrix-1g-local", RUN_LEVEL, lambda trial: [phase("command", 100.0)])
        self.assertIn("A phase lasts from the moment the harness marks it to its next mark, so the phases cover the "
                      "harness from its start to its end. Starting the machine and preparing its storage come before "
                      "the harness and are not in these times.", section(export.render(record, diagnostics=extra)))
