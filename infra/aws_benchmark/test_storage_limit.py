"""The setup page says what the storage allows and shows what the arrays did during the newest run's copies."""

import re
import unittest

from . import production_diagnostics as diagnostics, production_history as history
from .test_matrix_pages import FAILED, matrix_example

BOTH = "matrix-1g-local+remote"
GIGABYTE = 10 ** 9
# one copy: seconds, GB written to the destination, seconds it was busy, GB read from the source, seconds it was
# busy, share of CPU time that was neither idle nor waiting for I/O
TYPICAL = (600.0, 936.0, 588.0, 966.0, 210.0, .0125)


def command(elapsed, written, destination_busy, read, source_busy, cpu, samples=None, **changes):
    """What the sampler publishes for the command phase of one copy, read when it starts and every ten seconds."""
    counter = lambda value: dict(status="available", delta=round(value))
    metrics = dict(
        destination_write_sectors=counter(written * GIGABYTE / 512),
        destination_io_milliseconds=counter(destination_busy * 1000),
        source_read_sectors=counter(read * GIGABYTE / 512), source_io_milliseconds=counter(source_busy * 1000),
        host_cpu_total_ticks=counter(1_000_000), host_cpu_idle_ticks=counter(1_000_000 * (1 - cpu) * .6),
        host_cpu_iowait_ticks=counter(1_000_000 * (1 - cpu) * .4))
    metrics.update(changes)
    return dict(phase="command", case_index=0, trial_index=0, status="observed", elapsed_seconds=elapsed,
                samples=int(elapsed // 10) + 1 if samples is None else samples, metrics=metrics)


def sampled(suite=BOTH, copies=None, **options):
    """A matrix run whose copies carry sampled figures; `copies` replaces a copy's phases by its index."""
    record, extra = matrix_example(suite, **options)
    extra["setup"]["instrumentation"] = dict(diagnostics.TELEMETRY_DESCRIPTOR)
    extra["setup_id"] = diagnostics.setup_id(extra["setup"])
    for index, trial in enumerate(extra["trials"]):
        phases = (copies or {}).get(index, [command(*TYPICAL)])
        trial["system"] = (dict(status="unavailable", samples=0, phases=[]) if phases is None else
                           dict(status="partial", samples=sum(phase["samples"] for phase in phases), phases=phases))
    diagnostics.validate(extra, record)
    return record, extra


class StorageUseTests(unittest.TestCase):
    def test_each_measured_file_set_and_scenario_has_the_median_of_its_copies(self):
        record, extra = sampled(copies={
            0: [command(600.0, 930.0, 594.0, 960.0, 204.0, .012)],
            1: [command(620.0, 961.0, 607.6, 992.0, 223.2, .013)],
            2: [command(610.0, 951.6, 585.6, 988.2, 207.4, .011)],
            # a remote copy that used far more CPU and left the destination waiting
            5: [command(660.0, 990.0, 501.6, 1023.0, 198.0, .70)]})
        local, remote = diagnostics.storage_use(extra, record)
        self.assertEqual((local["case_id"], local["scenario"]), ("bulk-1g-1tb", "local"))
        # 930 GB in 600 s, 961 GB in 620 s and 951.6 GB in 610 s: 1.55, 1.55 and 1.56 GB/s
        self.assertAlmostEqual(local["written"], 1.55 * GIGABYTE, delta=1)
        self.assertAlmostEqual(local["destination_busy"], .98)
        self.assertAlmostEqual(local["read"], 1.60 * GIGABYTE, delta=1)
        self.assertAlmostEqual(local["source_busy"], .34)
        self.assertAlmostEqual(local["cpu"], .012)
        self.assertEqual((remote["case_id"], remote["scenario"]), ("bulk-1g-1tb", "remote"))
        self.assertAlmostEqual(remote["written"], 1.56 * GIGABYTE, delta=1)
        self.assertAlmostEqual(remote["destination_busy"], .98)
        self.assertAlmostEqual(remote["cpu"], .0125)

    def test_figures_cover_a_copy_from_its_first_sample_to_its_last(self):
        # a copy of 626.5 s is read at 0, 10, ... 620 s, so its counters lack the last 6.5 s: over the whole
        # copy the array would seem to write 1.54 GB/s and to be busy 98% of the time
        record, extra = sampled("matrix-1g-local", copies={
            index: [command(626.5, 967.2, 613.8, 998.2, 210.8, .0125)] for index in range(3)})
        row, = diagnostics.storage_use(extra, record)
        self.assertAlmostEqual(row["written"], 1.56 * GIGABYTE, delta=1)
        self.assertAlmostEqual(row["destination_busy"], .99)
        self.assertAlmostEqual(row["read"], 1.61 * GIGABYTE, delta=1)
        self.assertAlmostEqual(row["source_busy"], .34)

    def test_a_copy_with_fewer_than_two_samples_has_no_figure(self):
        # one reading spans no time, so there is no rate to give: not for that copy, and so not for its cell
        record, extra = sampled("matrix-1g-local", copies={1: [command(8.0, 12.0, 7.0, 12.5, 3.0, .01)]})
        self.assertEqual(extra["trials"][1]["system"]["phases"][0]["samples"], 1)
        self.assertEqual(diagnostics.storage_use(extra, record), [])
        record, extra = sampled("matrix-1g-local", copies={1: [command(*TYPICAL, samples=0)]})
        self.assertEqual(diagnostics.storage_use(extra, record), [])

    def test_only_the_time_of_the_copy_itself_counts(self):
        # cache preparation before a copy and cleanup after it move data of their own
        before = dict(command(40.0, 0.0, 0.0, 0.0, 0.0, .5), phase="cache")
        after = dict(command(8.0, 30.0, 8.0, 0.0, 0.0, .2), phase="cleanup")
        record, extra = sampled("matrix-1g-local", copies={
            index: [before, command(*TYPICAL), after] for index in range(3)})
        row, = diagnostics.storage_use(extra, record)
        self.assertAlmostEqual(row["written"], 1.56 * GIGABYTE, delta=1)
        self.assertAlmostEqual(row["destination_busy"], .98)

    def test_a_figure_needs_every_copy_of_its_cell(self):
        # a median over the copies that happen to have a figure would pick among the repeats
        record, extra = sampled(copies={1: None})
        row, = diagnostics.storage_use(extra, record)
        self.assertEqual(row["scenario"], "remote")

    def test_a_figure_the_sampler_could_not_read_is_unavailable_and_never_zero(self):
        lost = dict(status="unavailable")
        record, extra = sampled("matrix-1g-local", copies={index: [command(
            *TYPICAL, source_read_sectors=lost, source_io_milliseconds=lost, host_cpu_idle_ticks=lost)]
            for index in range(3)})
        row, = diagnostics.storage_use(extra, record)
        self.assertEqual((row["read"], row["source_busy"], row["cpu"]), (None, None, None))
        self.assertAlmostEqual(row["written"], 1.56 * GIGABYTE, delta=1)

    def test_a_counter_that_was_reset_in_one_copy_leaves_the_cell_without_that_figure_only(self):
        reset = dict(status="counter_reset", delta=None)
        record, extra = sampled("matrix-1g-local", copies={0: [command(*TYPICAL, destination_write_sectors=reset)]})
        row, = diagnostics.storage_use(extra, record)
        self.assertIsNone(row["written"])
        self.assertAlmostEqual(row["destination_busy"], .98)
        self.assertAlmostEqual(row["read"], 1.61 * GIGABYTE, delta=1)

    def test_a_cell_that_was_not_measured_has_no_row(self):
        record, extra = sampled(outcomes={1: FAILED})
        self.assertEqual([row["scenario"] for row in diagnostics.storage_use(extra, record)], ["local"])

    def test_a_run_that_sampled_nothing_has_no_rows(self):
        record, extra = matrix_example(BOTH)
        self.assertEqual(diagnostics.storage_use(extra, record), [])
        self.assertEqual(diagnostics.storage_use(diagnostics.unavailable(record), record), [])

    def test_a_busy_share_is_never_more_than_the_whole(self):
        # samples are a few milliseconds more than ten seconds apart: an array that always had a request in
        # progress was busy for a little longer than the 620 s that 63 samples nominally span
        record, extra = sampled("matrix-1g-local", copies={index: [command(621.0, 967.2, 620.2, 998.2, 210.8, .01)]
                                                           for index in range(3)})
        row, = diagnostics.storage_use(extra, record)
        self.assertEqual(row["destination_busy"], 1.0)

    def test_the_period_is_the_one_the_run_recorded(self):
        record, extra = sampled("matrix-1g-local")
        row, = diagnostics.storage_use(extra, record)
        self.assertAlmostEqual(row["written"], 1.56 * GIGABYTE, delta=1)
        # a later sampling contract may read at another period: 61 samples five seconds apart span half the time
        extra["setup"]["instrumentation"] = dict(diagnostics.TELEMETRY_DESCRIPTOR, period_seconds=5)
        row, = diagnostics.storage_use(extra, record)
        self.assertAlmostEqual(row["written"], 3.12 * GIGABYTE, delta=1)
        # a run without a recorded sampling contract has no samples to read, whatever its copies carry
        extra["setup"]["instrumentation"] = None
        self.assertEqual(diagnostics.storage_use(extra, record), [])


class StorageTableTests(unittest.TestCase):
    def table(self, *runs, **options):
        record, extra = runs[0] if runs else sampled(**options)
        return history.storage_html(record["identity"]["run"], "runs/" + record["run_key"] + ".html",
                                    diagnostics.storage_use(extra, record), history.FIRST)

    def test_the_table_names_each_file_set_and_copy_and_gives_rates_and_shares(self):
        html = self.table()
        self.assertIn("during the copies of run <a href='runs/repo-655073755-run-43200000001.html'>43200000001-1</a>",
                      html)
        self.assertIn("<th>File set</th><th>Copy</th><th>Written to the destination</th><th>Destination busy</th>"
                      "<th>Read from the source</th><th>Source busy</th><th>CPU busy</th>", html)
        self.assertIn("<tr><td>1 GB files</td><td>Local copy</td><td>1.56 GB/s</td><td>98%</td><td>1.61 GB/s</td>"
                      "<td>35%</td><td>1%</td></tr>", html)
        self.assertIn("<tr><td>1 GB files</td><td>Remote copy over loopback</td><td>1.56 GB/s</td>", html)
        # what the two shares mean, and what the figures are not
        self.assertIn("Busy is the share of that time during which the array had a request in progress", html)
        self.assertIn("each array as a whole, not its drives", html)
        # what a figure covers: a reader comparing it with a copy's duration finds up to ten seconds missing
        self.assertIn("read every ten seconds, so they cover each copy from its first reading to its last, up to "
                      "ten seconds less than the whole", html)

    def test_rows_come_in_the_order_of_the_trends_whatever_order_they_are_given_in(self):
        record, extra = sampled("matrix-all-local+remote")
        rows = diagnostics.storage_use(extra, record)
        self.assertEqual(len(rows), 8)
        html = history.storage_html("1-1", "runs/x.html", rows[::-1], history.FIRST)
        self.assertEqual([tuple(re.findall(r"<td>([^<]+)</td>", line)[:2]) for line in html.split("<tr>")[2:]],
                         [(name, copy) for name in ("1 GB files", "100 MB files", "1 MB files", "1 KB files")
                          for copy in ("Local copy", "Remote copy over loopback")])

    def test_file_sets_without_a_trend_are_left_out(self):
        # the small fixtures of a commissioning run are measured and sampled, and are not performance data
        record, extra = sampled("matrix-commission-local+remote")
        rows = diagnostics.storage_use(extra, record)
        self.assertEqual(len(rows), 4)
        self.assertEqual(history.storage_html("1-1", "runs/x.html", rows, history.FIRST), "")

    def test_a_missing_figure_reads_unavailable_and_a_slow_small_one_keeps_its_digits(self):
        lost = dict(status="unavailable")
        html = self.table(sampled("matrix-1g-local", copies={index: [command(
            600.0, 48.0, 126.0, 42.0, 450.0, .652, source_io_milliseconds=lost)] for index in range(3)}))
        self.assertIn("<td>0.08 GB/s</td><td>21%</td><td>0.07 GB/s</td><td>unavailable</td><td>65%</td>", html)

    def test_no_rows_no_table(self):
        self.assertEqual(history.storage_html("1-1", "runs/x.html", [], history.FIRST), "")


class SetupPageTests(unittest.TestCase):
    def test_the_setup_page_says_what_the_drives_allow_whether_or_not_a_run_recorded_it(self):
        for remote in (False, True):
            text = diagnostics.configuration_html([], remote=remote)
            section = text[text.index("<section aria-labelledby='storage'>"):]
            section = section[:section.index("</section>")]
            self.assertLess(text.index("id='machine'"), text.index("id='storage'"))
            self.assertLess(text.index("id='storage'"), text.index("id='datasets'"))
            self.assertIn("<h2 id='storage'>What the storage allows</h2>", section)
            # the provider's figures, and what they come to for an array of two
            self.assertIn("1,400,000 random reads and 680,000 writes of 4 KiB a second, at full queue depth", section)
            self.assertIn("For an array of two that is about 2.9 GB/s read and 1.4 GB/s written in requests of that "
                          "size; larger requests can move somewhat more", section)
            self.assertIn("the provider's specification, not a measurement of this machine", section)
            # the consequence a reader would otherwise be surprised by
            self.assertIn("hold about a terabyte each, several times the machine's memory, so a copy of them cannot "
                          "go faster than the destination array takes writes", section)
            self.assertIn("shows whether rcp keeps up with the storage, not how fast rcp could go: a change in rcp "
                          "shows first in CPU time and memory", section)
            self.assertIn("The 1 KB file set holds about 10 GB, which fits in memory many times over, and is not "
                          "bound this way", section)
            self.assertNotIn("<table", section)
        local, both = (diagnostics.configuration_html([], remote=remote) for remote in (False, True))
        self.assertIn("A local copy and a remote copy of the same file set that both keep up with the array "
                      "therefore take almost the same time", both)
        # until a run has a remote cell no page says anything about remote copies
        self.assertNotIn("remote", local.lower())

    def test_the_table_of_the_newest_run_follows_the_description(self):
        record, extra = sampled()
        table = history.storage_html(record["identity"]["run"], "runs/x.html", diagnostics.storage_use(extra, record),
                                     history.FIRST)
        text = diagnostics.configuration_html([], remote=True, storage=table)
        section = text[text.index("<section aria-labelledby='storage'>"):]
        section = section[:section.index("</section>")]
        self.assertTrue(section.endswith(table))
        self.assertLess(section.index("1,400,000 random reads"), section.index("<table"))


if __name__ == "__main__":
    unittest.main()
