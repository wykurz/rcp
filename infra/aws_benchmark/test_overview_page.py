"""The first AWS page shows every trend at once: copy duration of each file set, locally and remotely."""

from datetime import datetime, timedelta, timezone
import re
import unittest
from unittest import mock

from . import production_history as history, production_projection as projection
from .test_matrix_pages import FAILED, NOT_RUN, matrix_example
from .test_production_history import observed, split
from .test_remote_pages import UNREACHABLE, later

EVERYTHING = "matrix-all-local+remote"
TITLES = ["1 GB files", "100 MB files", "1 MB files", "1 KB files"]


def tiles(html, scenario):
    """The tiles of one row, in page order."""
    parts = html.split("data-overview='" + scenario + "'")[1:]
    return [part[:part.index("</figure>")] for part in parts]


def series(count, every, first="2026-11-20T08:30:00+00:00", suite=EVERYTHING):
    """Runs of one suite at a fixed interval, the first at `first`."""
    start = datetime.fromisoformat(first)
    return [matrix_example(suite, run=str(43200000001 + index), at=(start + every * index).isoformat())
            for index in range(count)]


def day_labels(chart):
    """The date labels of one plot with their centres, left to right."""
    return [(float(x), label) for x, label in re.findall(
        r"<text class='xtick' x='([-0-9.]+)' y='[-0-9.]+' text-anchor='middle'>([^<]+)</text>", chart)]


def charts(html):
    return re.findall(r"<svg class='chart'.*?</svg>", html, flags=re.S)


class OverviewTests(unittest.TestCase):
    def test_every_file_set_has_a_duration_tile_in_each_row_local_first(self):
        html = history.overview(*split([matrix_example(EVERYTHING), later(EVERYTHING)]))
        self.assertLess(html.index("id='overview-local'"), html.index("id='overview-remote'"))
        for scenario in (history.LOCAL, history.REMOTE):
            row = tiles(html, scenario)
            self.assertEqual([re.search(r"<h3 [^>]*>(?:<a [^>]*>)?([^<]+)", tile).group(1) for tile in row], TITLES)
            self.assertEqual(len(set(re.findall(r"aria-labelledby='([^']+)'", "".join(row)))), 4)
            for tile in row:
                self.assertEqual(tile.count("<svg "), 1)
                self.assertEqual(tile.count("<circle class='dot'"), 2)
                self.assertIn("over worker-reported run start, in s", tile)
        self.assertEqual(html.count("<svg "), 8)
        # duration only: the other metrics are on the page of each scenario
        self.assertNotIn("data-history-metric", html)
        for word in ("CPU time</h3>", "Peak memory</h3>", "<table"):
            self.assertNotIn(word, html)

    def test_a_tile_shows_the_latest_median_and_its_change(self):
        html = history.overview(*split([matrix_example(EVERYTHING), later(EVERYTHING)]))
        local, remote = tiles(html, history.LOCAL)[0], tiles(html, history.REMOTE)[0]
        self.assertIn("<p class='value'>600.2 <span class='unit'>s</span></p>", local)
        self.assertIn("<p class='delta'>27 Nov 2026 · +0.00% vs previous point</p>", local)
        self.assertIn("<p class='value'>601.2 <span class='unit'>s</span></p>", remote)

    def test_each_row_and_tile_leads_to_the_page_of_its_scenario(self):
        html = history.overview(*split([matrix_example(EVERYTHING)]))
        # a row is headed by one line, its title and the way to its page, so that both rows fit a screen
        self.assertIn("<div class='overview-title'><h2 id='overview-local-title'>Local copies</h2>"
                      "<a href='local.html'>CPU time, peak memory and data tables</a></div><div class='overview'>",
                      html)
        self.assertIn("<div class='overview-title'><h2 id='overview-remote-title'>Remote copies</h2>"
                      "<a href='remote.html'>CPU time and peak memory of each rcpd, and data tables</a></div>"
                      "<div class='overview'>", html)
        self.assertNotIn("<p class='dataset'>", html)
        self.assertIn("<a href='local.html#history-bulk-1g-1tb' ", tiles(html, history.LOCAL)[0])
        self.assertIn("<a href='remote.html#history-tiny-1k-10240k' ", tiles(html, history.REMOTE)[3])
        # a point still opens its run
        self.assertIn("href='runs/repo-655073755-run-43200000001.html'", tiles(html, history.REMOTE)[0])
        self.assertIn("<a href='setups.html#reading'>How to read these plots</a>", html)

    def test_the_two_tiles_of_a_file_set_are_told_apart_without_seeing_their_row(self):
        html = history.overview(*split([matrix_example(EVERYTHING)]))
        local, remote = tiles(html, history.LOCAL)[0], tiles(html, history.REMOTE)[0]
        # a tile is named by its row and its file set; its link and its plot say which copy they are
        self.assertIn(" aria-labelledby='overview-local-title overview-local-bulk-1g-1tb'>", local)
        self.assertIn(" aria-labelledby='overview-remote-title overview-remote-bulk-1g-1tb'>", remote)
        self.assertIn("<a href='local.html#history-bulk-1g-1tb' aria-label='1 GB files, local copies'>1 GB files</a>",
                      local)
        self.assertIn("<a href='remote.html#history-bulk-1g-1tb' aria-label='1 GB files, remote copies'>"
                      "1 GB files</a>", remote)
        self.assertIn("aria-label='1 GB files, local copy duration over worker-reported run start, in s'", local)
        self.assertIn("aria-label='1 GB files, remote copy duration over worker-reported run start, in s'", remote)
        empty = tiles(history.overview(*split([matrix_example("matrix-1g-local+remote")])), history.REMOTE)[1]
        self.assertIn(" aria-labelledby='overview-remote-title overview-remote-bulk-100m-1024gb'>", empty)

    def test_a_remote_tile_compares_the_run_with_its_own_local_copy(self):
        html = history.overview(*split([matrix_example(EVERYTHING)]))
        # 601.2 s through rcpd against 600.2 s locally, in the same run
        self.assertIn("<p class='delta'>+0.17% vs local copy</p>", tiles(html, history.REMOTE)[0])
        for tile in tiles(html, history.LOCAL):
            self.assertNotIn("vs local copy", tile)

    def test_the_comparison_reaches_a_local_copy_that_is_no_longer_plotted(self):
        # the newest remote copy is in a run whose local copy has since left the local plot
        runs = [matrix_example("matrix-1g-local+remote"), later("matrix-1g-local")]
        with mock.patch.object(history, "MAX_PLOT_RUNS", 1):
            html = history.overview(*split(runs))
        local, remote = tiles(html, history.LOCAL)[0], tiles(html, history.REMOTE)[0]
        self.assertIn("27 Nov 2026 · first run on this line", local)
        self.assertNotIn("runs/repo-655073755-run-43200000001.html", local)
        self.assertIn("<p class='delta'>+0.17% vs local copy</p>", remote)

    def test_a_local_copy_that_took_no_time_is_not_compared_with(self):
        record, _ = matrix_example("matrix-1g-local+remote")
        record["measurements"][0].update(samples=[0.0, 0.0, 0.0], median=0.0)
        html = history.overview(*split([(record, None)]))
        self.assertIn("<p class='value'>0 <span class='unit'>s</span></p>", tiles(html, history.LOCAL)[0])
        self.assertNotIn("vs local copy", html)

    def test_a_difference_that_rounds_to_nothing_has_no_minus_sign(self):
        record, _ = matrix_example("matrix-1g-local+remote")
        local, remote = record["measurements"]
        remote.update(samples=[x - 1.001 for x in remote["samples"]], median=remote["median"] - 1.001)
        self.assertLess(remote["median"], local["median"])
        html = history.overview(*split([(record, None)]))
        self.assertIn("<p class='delta'>+0.00% vs local copy</p>", tiles(html, history.REMOTE)[0])
        self.assertNotIn("-0.00%", html)

    def test_a_remote_tile_without_a_local_copy_in_its_run_compares_nothing(self):
        runs = [matrix_example("matrix-1g-local"), later("matrix-1g-remote")]
        html = history.overview(*split(runs))
        remote = tiles(html, history.REMOTE)[0]
        # the only cell of that run, so the fixture's first duration
        self.assertIn("<p class='value'>600.2 <span class='unit'>s</span></p>", remote)
        self.assertNotIn("vs local copy", remote)

    def test_without_a_remote_cell_there_is_one_row_and_no_word_about_remote_copies(self):
        html = history.overview(*split([observed("42458459014", "08"), matrix_example("matrix-all-local")]))
        self.assertEqual(len(tiles(html, history.LOCAL)), 4)
        self.assertEqual(html.count("<svg "), 4)
        for phrase in ("remote", "Remote", "rcpd", "vs local copy"):
            self.assertNotIn(phrase, html)

    def test_a_file_set_without_runs_keeps_its_place_and_links_nowhere(self):
        html = history.overview(*split([matrix_example("matrix-1g-local+remote")]))
        for scenario in (history.LOCAL, history.REMOTE):
            row = tiles(html, scenario)
            self.assertEqual(len(row), 4)
            self.assertEqual([tile.count("<svg ") for tile in row], [1, 0, 0, 0])
            for tile in row[1:]:
                self.assertIn("<p class='delta'>No qualified run yet.</p>", tile)
                self.assertNotIn("<a ", tile)

    def test_a_row_that_was_never_measured_says_so_instead_of_showing_empty_tiles(self):
        record, extra = matrix_example("matrix-1g-local+remote", outcomes={1: UNREACHABLE})
        html = history.overview(*split([(record, extra)]))
        self.assertEqual(len(tiles(html, history.LOCAL)), 4)
        self.assertEqual(tiles(html, history.REMOTE), [])
        self.assertIn("<div class='overview-title'><h2 id='overview-remote-title'>Remote copies</h2></div>"
                      "<p class='empty'>No qualified remote runs yet.</p>", html)
        # nothing to open: the page of remote copies would say the same
        self.assertNotIn("remote.html", html)

    def test_a_cell_that_did_not_fit_leaves_its_tile_on_the_earlier_run(self):
        runs = [matrix_example(EVERYTHING), later(EVERYTHING, outcomes={7: NOT_RUN})]
        html = history.overview(*split(runs))
        last = tiles(html, history.REMOTE)[3]
        self.assertEqual(last.count("<circle class='dot'"), 1)
        self.assertIn("20 Nov 2026 · first run on this line", last)

    def test_the_overview_keeps_the_history_anchor_and_the_plot_window(self):
        runs = [later(EVERYTHING, run=str(43200000002 + index), day=str(10 + index)) for index in range(3)]
        with mock.patch.object(history, "MAX_PLOT_RUNS", 2):
            html = history.overview(*split(runs))
        self.assertTrue(html.startswith("<section id='history' "))
        self.assertEqual(html.count("<circle class='dot'"), 2 * 8)
        self.assertIn("The plots show each file set's newest 2 qualified runs. Older runs are not plotted; every "
                      "run stays in the list below.", html)

    def test_an_oversized_overview_drops_the_oldest_plotted_runs_first(self):
        runs = [later(EVERYTHING, run=str(43200000002 + index), day=str(10 + index)) for index in range(4)]
        whole = history.overview(*split(runs))
        with mock.patch.object(projection, "MAX_PUBLIC_HTML", len(whole.encode()) - 1):
            html = history.overview(*split(runs))
        self.assertEqual(html.count("<circle class='dot'"), 3 * 8)
        self.assertNotIn("runs/repo-655073755-run-43200000002.html", html)
        self.assertIn("newest 3 qualified runs", html)

    def test_nothing_measured_yet_still_renders_the_history_anchor(self):
        html = history.overview({}, {})
        self.assertTrue(html.startswith("<section id='history' "))
        self.assertIn("<p class='empty'>No qualified runs yet.</p>", html)
        self.assertNotIn("<figure", html)


class CompactChartTests(unittest.TestCase):
    def test_overview_plots_are_drawn_small_so_their_labels_stay_readable_four_abreast(self):
        runs = split([matrix_example(EVERYTHING)])
        overview, detail = history.overview(*runs), history.render(*runs)
        self.assertEqual(set(re.findall(r"viewBox='([^']+)'", overview)), {"0 0 232 150"})
        self.assertEqual(set(re.findall(r"viewBox='([^']+)'", detail)), {"0 0 360 200"})

    def test_every_mark_of_a_small_plot_stays_inside_it(self):
        html = history.overview(*split(series(6, timedelta(days=1))))
        self.assertEqual(len(charts(html)), 8)
        for chart in charts(html):
            for name, limit in (("x", 232), ("x1", 232), ("x2", 232), ("cx", 232), ("y", 150), ("y1", 150),
                                ("y2", 150), ("cy", 150)):
                for value in re.findall(r"\s" + name + r"='([-0-9.]+)'", chart):
                    self.assertTrue(0 <= float(value) <= limit, (name, value))

    def test_a_tooltip_stays_inside_its_plot_and_never_covers_the_point_it_describes(self):
        # a flat trend keeps every point in the middle of its plot, where a small plot has least room;
        # one slow run puts a point at the top and the others at the bottom
        flat, spiked = series(6, timedelta(days=1)), series(6, timedelta(days=1))
        for item in spiked[2][0]["measurements"]:
            item.update(samples=[x * 1.5 for x in item["samples"]], median=item["median"] * 1.5)
        for runs, places in ((split(flat), 1), (split([(record, None) for record, _ in spiked]), 2)):
            for render, (width, height) in ((history.overview, (232, 150)), (history.render, (360, 200))):
                points = re.findall(r"<circle class='ring' cx='[-0-9.]+' cy='([-0-9.]+)' r='6'/>"
                                    r"<g class='tip' transform='translate\(([-0-9.]+),([-0-9.]+)\)'>", render(*runs))
                # at least the duration of every file set in every run
                self.assertGreaterEqual(len(points), 24)
                for cy, left, top in points:
                    cy, left, top = float(cy), float(left), float(top)
                    self.assertTrue(0 <= left <= width - history.TIP_WIDTH and
                                    0 <= top <= height - history.TIP_HEIGHT, (left, top))
                    # the ring around a hovered point has a radius of six
                    self.assertTrue(top + history.TIP_HEIGHT <= cy - 6 or top >= cy + 6, (cy, top))
                # the slow run's point is at the top with its tooltip below, the others the other way round
                self.assertEqual(len({top for _, _, top in points}), places)

    def test_a_small_plot_names_the_days_of_its_first_and_last_run(self):
        cases = (
            (series(1, timedelta(days=1)), ["20 Nov"]),
            # a week of runs has no whole week in it, and a small plot no room for a label a day
            (series(2, timedelta(days=6), first="2026-11-19T12:00:00+00:00"), ["19 Nov", "25 Nov"]),
            (series(5, timedelta(days=7), first="2026-11-23T08:17:00+00:00"), ["23 Nov", "21 Dec"]),
            # two runs of one day would print the same date twice, one over the other
            (series(2, timedelta(hours=10)), ["20 Nov"]),
            # either side of midnight the two dates differ and still have no room next to each other
            (series(2, timedelta(hours=2), first="2026-11-20T23:00:00+00:00"), ["21 Nov"]),
            # across a new year a date needs its year
            (series(9, timedelta(days=7), first="2026-11-23T08:17:00+00:00"), ["23 Nov 2026", "18 Jan 2027"]),
        )
        for runs, expected in cases:
            with self.subTest(expected=expected):
                plots = charts(history.overview(*split(runs)))
                self.assertEqual(len(plots), 8)
                for chart in plots:
                    self.assertEqual([label for _, label in day_labels(chart)], expected)
                    # each label has its tick under the run it names
                    marks = re.findall(r"<line class='axis' x1='([-0-9.]+)' x2='\1' y1='126' y2='130'/>", chart)
                    dots = re.findall(r"<circle class='dot' cx='([-0-9.]+)'", chart)
                    self.assertEqual(marks, [dots[0], dots[-1]] if len(expected) == 2 else [dots[-1]])

    def test_the_plots_of_a_scenario_page_keep_their_whole_day_ticks(self):
        runs = split(series(2, timedelta(days=6), first="2026-11-19T12:00:00+00:00"))
        for chart in charts(history.render(*runs)):
            self.assertEqual([label for _, label in day_labels(chart)], ["21 Nov", "23 Nov", "25 Nov"])

    def test_a_date_label_at_the_right_edge_of_a_plot_is_pulled_inside_it(self):
        # one run just after midnight has its next day's tick at the edge of a scenario page's plot; the last
        # run of a long series stands at the edge of a small one
        cases = ((history.render, 360, [matrix_example(EVERYTHING, at="2026-11-20T00:10:00+00:00")], "21 Nov"),
                 (history.overview, 232, series(12, timedelta(days=14)), "23 Apr 2027"))
        for render, width, runs, last in cases:
            with self.subTest(width=width):
                plots = charts(render(*split(runs)))
                self.assertTrue(plots)
                for chart in plots:
                    position, label = day_labels(chart)[-1]
                    self.assertEqual(label, last)
                    extent = len(label) * history.TICK_CHARACTER / 2
                    self.assertLessEqual(position + extent, width)
                    # without the pull it would cross the edge: its tick is nearer than half its width
                    tick = max(float(x) for x in re.findall(r"<line class='axis' x1='([-0-9.]+)' x2='\1' y1=", chart))
                    self.assertGreater(tick + extent, width)

    def test_a_label_pulled_inside_gives_way_to_the_one_it_would_run_into(self):
        # two month labels sixty units apart fit; the second, at the edge, is pulled twelve units towards the first
        x = {1: 288.0, 2: 348.0}.get
        self.assertEqual([(centre, label) for centre, _, _, label in history._date_labels(
            [(1, "Jul 2027"), (2, "Oct 2027")], x, history.DETAIL)], [(288.0, "Jul 2027")])
        # in the order of preference of a small plot, the last run keeps its date
        x = {1: 200.0, 2: 215.0}.get
        self.assertEqual([label for _, _, _, label in history._date_labels(
            [(2, "21 Nov"), (1, "20 Nov")], x, history.COMPACT)], ["21 Nov"])
        # with room for both, they come back left to right
        x = {1: 60.0, 2: 215.0}.get
        self.assertEqual([label for _, _, _, label in history._date_labels(
            [(2, "21 Nov"), (1, "20 Nov")], x, history.COMPACT)], ["20 Nov", "21 Nov"])

    def test_date_labels_never_run_into_each_other(self):
        # a year and more of runs puts month labels close together, and the last is pulled inside the plot
        for count, every in ((32, timedelta(days=14)), (32, timedelta(days=7)), (6, timedelta(days=1))):
            runs = split(series(count, every, first="2027-02-08T08:17:00+00:00"))
            for render in (history.overview, history.render):
                for chart in charts(render(*runs)):
                    labels = day_labels(chart)
                    self.assertTrue(labels)
                    for (left, before), (right, after) in zip(labels, labels[1:]):
                        self.assertGreaterEqual(right - left, (len(before) + len(after)) * history.TICK_CHARACTER / 2,
                                                (before, after))


if __name__ == "__main__":
    unittest.main()
