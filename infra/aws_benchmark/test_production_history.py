"""History follows rcp over time: one linked trend per file set from frozen records."""
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
from statistics import median
import unittest
from unittest.mock import patch

from . import production_history as history, production_diagnostics as diagnostics
from . import production_projection as projection
from .test_production_diagnostics import inputs

CASE = "bulk-1g-1tb"


def example(run="42458459014", at="2026-11-08T07:30:02+00:00"):
    _, raw, worker = inputs()
    measurements = []
    for variant in ("rcp-default", "rcp-baseline"):
        samples = [t["elapsed_seconds"] for t in raw["trials"] if t["variant_id"] == variant]
        measurements.append(dict(case_id=CASE, variant_id=variant,
            series_id=hashlib.sha256(variant.encode()).hexdigest(), median=median(samples), samples=samples))
    record = dict(schema="rcp-production-public-report-v3", run_key="repo-655073755-run-" + run,
        retirement_sha256="a" * 64, benchmark_valid=True, cleanup_verified=True,
        validation=projection.VALIDATION,
        identity=dict(run=run + "-1", run_url="https://github.com/wykurz/rcp/actions/runs/" + run + "/attempts/1",
            suite="bulk-1g-metadata", baseline="1" * 40, candidate="1" * 40, controller="2" * 40),
        comparison="A/A (identical source revision and binary digests)", measurements=measurements,
        series_contract="aws-local-ext4-v1", note="Four copies form two local pairs.",
        qualification_outcome="passed", collected_at="2026-11-08T22:44:32+00:00",
        timestamp=at, verification=dict(policy="metadata-size-tree-v1"))
    projection.validate_public(record)
    return record, raw, worker


def companion(record, raw, worker):
    return diagnostics.derive(record, b"archive", raw, worker)


def observed(run, day, change=None):
    """One qualified run with its companion; `change` edits the raw worker evidence first."""
    record, raw, worker = example(run, "2026-11-" + day + "T07:30:02+00:00")
    if change is not None:
        change(raw, worker)
    return record, companion(record, raw, worker)


def split(runs):
    return ({record["run_key"]: record for record, _ in runs},
            {record["run_key"]: extra for record, extra in runs if extra is not None})


def figure(html, metric):
    start = html.index("data-history-metric='" + metric + "'")
    return html[start:html.index("</figure>", start)]


class HistoryModelTests(unittest.TestCase):
    def points(self, *runs):
        return history.model(*split(runs))[0][CASE]

    def test_pooling_recognizes_the_wording_frozen_in_published_records(self):
        frozen = json.loads((Path(__file__).with_name("fixtures") / "production-public-report-v1.json").read_text())
        self.assertEqual(frozen["comparison"], projection.IDENTICAL_BINARIES)

    def test_identical_binaries_pool_every_copy_into_one_point(self):
        record, raw, worker = example()
        point, = self.points((record, companion(record, raw, worker)))
        samples = [x for item in record["measurements"] for x in item["samples"]]
        self.assertEqual(point["duration"],
                         dict(value=median(samples), low=min(samples), high=max(samples), samples=4))
        self.assertTrue(point["pooled"])
        self.assertEqual([c["variant"] for c in point["copies"]], ["rcp-default", "rcp-baseline"])
        self.assertEqual(point["cpu"]["samples"], 4)
        self.assertEqual(point["revision"], "1" * 40)

    def test_different_binaries_plot_only_the_build_under_test(self):
        record, raw, worker = example()
        record["comparison"] = "A/B (different source revisions)"
        record["identity"]["baseline"] = "3" * 40
        cases, outside = history.model(*split([(record, companion(record, raw, worker))]))
        point, = cases[CASE]
        default = record["measurements"][0]
        self.assertEqual(point["duration"], dict(value=default["median"], low=min(default["samples"]),
                                                 high=max(default["samples"]), samples=2))
        self.assertFalse(point["pooled"])
        self.assertEqual(point["cpu"]["samples"], 2)
        self.assertEqual(outside, 1)

    def test_a_single_measurement_keeps_its_frozen_median(self):
        record, _, _ = example()
        record["measurements"] = record["measurements"][:1]
        record["measurements"][0]["median"] = 600.0
        point, = self.points((record, None))
        self.assertEqual(point["duration"]["value"], 600.0)

    def test_other_variants_and_file_sets_are_counted_not_plotted(self):
        record, _, _ = example()
        record["measurements"] += [
            dict(case_id=CASE, variant_id="rcp-default-f8", series_id="f" * 64, median=1.0, samples=[1.0]),
            dict(case_id="tiny-10k", variant_id="rcp-default", series_id="e" * 64, median=2.0, samples=[2.0])]
        cases, outside = history.model({record["run_key"]: record}, {})
        self.assertEqual(outside, 2)
        self.assertEqual(cases[CASE][0]["duration"]["samples"], 4)
        self.assertEqual(set(cases), set(history.CASES))

    def test_cpu_is_user_plus_system_per_copy_before_the_median(self):
        record, raw, worker = example()
        for trial, (user, system) in zip(raw["trials"], ((0, 100), (0, 0), (100, 0), (50, 50))):
            trial["resources"]["metrics"].update(user_seconds=user, system_seconds=system)
        point, = self.points((record, companion(record, raw, worker)))
        self.assertEqual(point["cpu"], dict(value=100, low=0, high=100, samples=4))

    def test_peak_memory_converts_kib_to_mib_and_keeps_the_copy_range(self):
        record, raw, worker = example()
        for trial, rss in zip(raw["trials"], (1024, 2048, 3072, 4096)):
            trial["resources"]["metrics"]["max_rss_kib"] = rss
        point, = self.points((record, companion(record, raw, worker)))
        self.assertEqual(point["memory"], dict(value=2.5, low=1, high=4, samples=4))

    def test_partial_resource_coverage_leaves_cpu_and_memory_unavailable(self):
        record, raw, worker = example()
        next(t for t in raw["trials"] if t["variant_id"] == "rcp-baseline")["resources"] = {}
        point, = self.points((record, companion(record, raw, worker)))
        self.assertIsNone(point["cpu"])
        self.assertIsNone(point["memory"])
        self.assertEqual(point["duration"]["samples"], 4)

    def test_missing_companion_keeps_duration_without_resource_values(self):
        record, _, _ = example()
        point, = self.points((record, None))
        self.assertIsNotNone(point["duration"])
        self.assertIsNone(point["cpu"])
        self.assertIsNone(point["memory"])
        self.assertIsNone(point["setup_id"])

    def test_failed_retired_run_has_no_performance_points(self):
        record, _, _ = example()
        record.update(benchmark_valid=False, qualification_outcome="failed", measurements=[])
        del record["timestamp"]
        del record["verification"]
        self.assertFalse(self.points((record, None)))
        self.assertEqual(history.point(record, None), {})

    def test_one_record_summary_matches_its_plotted_point(self):
        record, raw, worker = example()
        extra = companion(record, raw, worker)
        summary = history.point(record, extra)[CASE]
        plotted, = self.points((record, extra))
        for metric in ("duration", "cpu", "memory"):
            self.assertEqual(summary[metric], plotted[metric])

    def test_time_uses_normalized_worker_start_not_collection_time(self):
        one, _, _ = example("123", "2026-11-08T05:00:00+05:00")
        two, _, _ = example("124", "2026-11-07T23:00:00+00:00")
        two["collected_at"] = "2026-11-09T23:00:00+00:00"
        points = self.points((one, None), (two, None))
        self.assertEqual(points[0]["run_key"], two["run_key"])
        self.assertEqual(points[-1]["timestamp"], "2026-11-08T00:00:00Z")
        self.assertEqual(points[-1]["date"], "8 Nov 2026 00:00 UTC")
        self.assertEqual(points[-1]["day"], "8 Nov 2026")
        self.assertNotEqual(points[-1]["timestamp"], one["collected_at"])

    def test_missing_or_invalid_start_is_never_replaced_with_collection_time(self):
        record, _, _ = example()
        for bad in [None, "not-a-timestamp", "<script>"]:
            broken = copy.deepcopy(record)
            if bad is None:
                del broken["timestamp"]
            else:
                broken["timestamp"] = bad
            with self.assertRaises(ValueError):
                self.points((broken, None))

    def test_runs_link_across_code_harness_series_and_unrecorded_setups(self):
        one = observed("123", "08")
        two, _, _ = example("124", "2026-11-09T07:30:02+00:00")
        two["identity"].update(candidate="4" * 40, baseline="4" * 40, controller="5" * 40)
        for item in two["measurements"]:
            item["series_id"] = hashlib.sha256(b"next " + item["variant_id"].encode()).hexdigest()
        three = observed("125", "10")
        points = self.points(one, (two, None), three)
        self.assertEqual([p["segment"] for p in points], [0, 0, 0])
        self.assertEqual([p["changes"] for p in points], [[], [], []])
        self.assertFalse(any(p["starts_segment"] for p in points))

    def test_a_recorded_hardware_change_starts_a_new_line(self):
        other = "Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz"
        points = self.points(observed("123", "08"), observed("124", "09",
            lambda raw, worker: raw["context"]["environment"].update(cpu_model=other)))
        self.assertEqual([p["segment"] for p in points], [0, 1])
        self.assertEqual([p["starts_segment"] for p in points], [False, True])
        self.assertEqual(points[1]["changes"],
                         [("CPU model", "Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz", other)])

    def test_a_recorded_storage_change_names_its_side(self):
        points = self.points(observed("123", "08"), observed("124", "09",
            lambda raw, worker: worker["storage"]["arrays"]["destination"].update(chunk_bytes=1048576)))
        self.assertEqual(points[1]["changes"],
                         [("Destination storage: RAID chunk", "524,288 bytes", "1,048,576 bytes")])
        self.assertTrue(points[1]["starts_segment"])

    def test_a_recorded_software_change_is_marked_without_breaking_the_line(self):
        points = self.points(observed("123", "08"), observed("124", "09",
            lambda raw, worker: raw["context"]["environment"].update(kernel="7.0.0-1020-aws")))
        self.assertEqual([p["segment"] for p in points], [0, 0])
        self.assertEqual(points[1]["changes"], [("Kernel", "7.0.0-1014-aws", "7.0.0-1020-aws")])
        self.assertFalse(points[1]["starts_segment"])

    def test_a_field_first_recorded_by_a_newer_run_is_not_a_change(self):
        def newer(count):
            return lambda raw, worker: raw.update(system_setup=dict(
                logical_cpu_count=dict(status="available", value=count),
                host_memory_total_bytes=dict(status="available", value=200000000000),
                os=dict(status="available", value="Linux")))
        points = self.points(observed("123", "08"), observed("124", "09", newer(96)),
                             observed("125", "10", newer(64)))
        self.assertEqual(points[1]["changes"], [])
        self.assertEqual([p["segment"] for p in points], [0, 0, 1])
        self.assertEqual(points[2]["changes"], [("Logical CPUs", "96", "64")])

    def test_a_change_is_measured_against_the_last_run_that_recorded_the_field(self):
        two, _, _ = example("124", "2026-11-09T07:30:02+00:00")
        points = self.points(observed("123", "08"), (two, None), observed("125", "10",
            lambda raw, worker: raw["context"]["environment"].update(kernel="7.0.0-1020-aws")))
        self.assertEqual([p["changes"] for p in points],
                         [[], [], [("Kernel", "7.0.0-1014-aws", "7.0.0-1020-aws")]])

    def test_host_ram_and_sampler_differences_never_mark_the_trend(self):
        def ram(amount, sampled):
            def change(raw, worker):
                raw["system_setup"] = dict(host_memory_total_bytes=dict(status="available", value=amount))
                if sampled:
                    raw["context"]["system_telemetry"] = diagnostics.TELEMETRY_DESCRIPTOR
            return change
        runs = (observed("123", "08", ram(200000000000, True)), observed("124", "09", ram(100000000000, False)),
                observed("125", "10", ram(200000000000, True)))
        self.assertEqual([extra["setup"]["instrumentation"] is None for _, extra in runs], [False, True, False])
        points = self.points(*runs)
        self.assertEqual([p["changes"] for p in points], [[], [], []])
        self.assertEqual([p["segment"] for p in points], [0, 0, 0])

    def test_a_changed_measurement_contract_starts_a_new_line(self):
        one, _, _ = example("123", "2026-11-08T07:30:02+00:00")
        two, _, _ = example("124", "2026-11-09T07:30:02+00:00")
        future = dict(projection.PUBLIC_CONTRACTS["rcp-production-public-report-v1"],
                      series_contract="aws-local-xfs-v1")
        two.update(schema="rcp-production-public-report-v9", series_contract="aws-local-xfs-v1")
        del two["qualification_outcome"], two["collected_at"]
        with patch.dict(projection.PUBLIC_CONTRACTS, {"rcp-production-public-report-v9": future}):
            points = self.points((one, None), (two, None))
        self.assertEqual([p["segment"] for p in points], [0, 1])
        self.assertEqual(points[1]["changes"],
                         [("Measurement contract", "aws-local-ext4-v1", "aws-local-xfs-v1")])
        self.assertTrue(points[1]["starts_segment"])

    def test_frozen_inputs_unchanged_and_companion_identity_rechecked(self):
        record, raw, worker = example()
        extra = companion(record, raw, worker)
        before = copy.deepcopy((record, extra))
        history.render({record["run_key"]: record}, {record["run_key"]: extra})
        self.assertEqual((record, extra), before)
        extra["retirement_sha256"] = "b" * 64
        with self.assertRaises(ValueError):
            self.points((record, extra))
        extra["retirement_sha256"] = "a" * 64
        extra["schema"] = "unreviewed-version"
        with self.assertRaises(ValueError):
            self.points((record, extra))


class HistoryRenderTests(unittest.TestCase):
    def render(self, *runs):
        return history.render(*split(runs))

    def test_each_file_set_with_runs_gets_one_panel_per_metric(self):
        html = self.render(observed("123", "08"))
        for metric in ("duration", "cpu", "memory"):
            self.assertEqual(html.count("data-history-metric='" + metric + "'"), 1)
        self.assertEqual(html.count("<svg "), 3)
        self.assertIn("id='history'", html)
        self.assertIn("id='history-" + CASE + "'", html)
        self.assertIn("No qualified runs yet for 100 MB files, 1 MB files and 1 KB files.", html)
        self.assertNotIn("<script", html)
        self.assertNotIn("<polyline", html)
        self.assertIn("first run on this line", figure(html, "duration"))
        # a native title tooltip would sit on top of the styled one; the name is an attribute instead
        self.assertNotIn("<title", html)
        self.assertIn("aria-label='Copy duration over worker-reported run start, in s'", html)
        # a revealed tooltip must not swallow the pointer on its way to a neighbouring point
        self.assertEqual(html.count("<g class='hover' visibility='hidden' pointer-events='none'>"),
                         html.count("class='pt'"))

    def test_no_qualified_runs_still_renders_the_history_anchor(self):
        html = history.render({}, {})
        self.assertIn("id='history'", html)
        self.assertIn("No qualified runs yet", html)
        self.assertNotIn("<svg", html)

    def test_missing_resources_show_an_unavailable_panel_instead_of_a_zero(self):
        record, _, _ = example()
        html = self.render((record, None))
        self.assertEqual(html.count("<svg "), 1)
        self.assertIn("unavailable", figure(html, "cpu"))
        self.assertNotIn("<svg", figure(html, "memory"))

    def test_linked_runs_draw_one_line_per_metric(self):
        html = self.render(observed("123", "08"), observed("124", "09"))
        for metric in ("duration", "cpu", "memory"):
            self.assertEqual(figure(html, metric).count("<polyline"), 1)
        self.assertNotIn("class='changes'", html)
        self.assertNotIn("class='marker'", html)

    def test_a_run_without_resources_leaves_a_gap_instead_of_an_interpolated_line(self):
        middle, _, _ = example("124", "2026-11-09T07:30:02+00:00")
        html = self.render(observed("123", "08"), (middle, None), observed("125", "10"))
        self.assertEqual(figure(html, "duration").count("<polyline"), 1)
        self.assertEqual(len(re.findall(r"<polyline class='line' points='([^']+)'",
                                        figure(html, "duration"))[0].split()), 3)
        for metric in ("cpu", "memory"):
            self.assertEqual(figure(html, metric).count("<polyline"), 0)
            self.assertEqual(figure(html, metric).count("class='dot'"), 2)
            self.assertIn("1 run has no recorded value; the gap is not filled.", figure(html, metric))

    def test_a_hardware_change_breaks_the_line_and_is_listed(self):
        other = "Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz"
        html = self.render(observed("123", "08"), observed("124", "09",
            lambda raw, worker: raw["context"]["environment"].update(cpu_model=other)))
        self.assertNotIn("<polyline", html)
        self.assertEqual(html.count("class='marker'"), 3)
        changes = html[html.index("<ul class='changes'>"):]
        self.assertIn("CPU model", changes)
        self.assertIn("Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz", changes)
        self.assertIn("starts a new line", changes)
        self.assertIn(">new line</text>", figure(html, "duration"))
        self.assertIn("first run on this line", figure(html, "duration"))

    def test_a_software_change_keeps_the_line_and_is_marked(self):
        html = self.render(observed("123", "08"), observed("124", "09",
            lambda raw, worker: raw["context"]["environment"].update(kernel="7.0.0-1020-aws")))
        self.assertEqual(html.count("<polyline"), 3)
        self.assertEqual(html.count("class='marker'"), 3)
        changes = html[html.index("<ul class='changes'>"):]
        self.assertIn("<code>7.0.0-1014-aws</code>", changes)
        self.assertIn("<code>7.0.0-1020-aws</code>", changes)
        self.assertNotIn("starts a new line", changes)
        self.assertIn(">setup change</text>", figure(html, "duration"))
        self.assertIn("vs previous point", figure(html, "duration"))

    def test_marker_labels_never_overlap(self):
        def run(number, at, kernel):
            record, raw, worker = example(number, at)
            raw["context"]["environment"]["kernel"] = kernel
            return record, companion(record, raw, worker)
        # two changes 70 plot units apart, the later one near the right edge where its label is pulled inwards
        html = self.render(run("123", "2026-11-01T07:30:02+00:00", "7.0.0-1014-aws"),
                           run("124", "2026-11-08T05:06:02+00:00", "7.0.0-1020-aws"),
                           run("125", "2026-11-10T17:06:02+00:00", "7.0.0-1021-aws"),
                           run("126", "2026-11-11T07:30:02+00:00", "7.0.0-1021-aws"))
        chart = figure(html, "duration")
        self.assertEqual(chart.count("class='marker'"), 2)
        self.assertEqual(chart.count("class='marker-label'"), 1)
        self.assertEqual(html[html.index("<ul class='changes'>"):].count("<li>"), 2)

    def test_measurements_outside_the_trend_are_counted_on_the_page(self):
        record, _, _ = example()
        record["measurements"] += [
            dict(case_id=CASE, variant_id="rcp-default-f8", series_id="f" * 64, median=1.0, samples=[1.0]),
            dict(case_id="tiny-10k", variant_id="rcp-default", series_id="e" * 64, median=2.0, samples=[2.0])]
        self.assertIn("2 qualified measurements are outside these trends", self.render((record, None)))

    def test_axis_is_fitted_to_the_data_with_round_ticks_and_a_minimum_span(self):
        one, _, _ = example("123", "2026-11-08T07:30:02+00:00")
        two, _, _ = example("124", "2026-11-09T07:30:02+00:00")
        for item in two["measurements"]:
            item.update(samples=[x + .6 for x in item["samples"]], median=item["median"] + .6)
        chart = figure(self.render((one, None), (two, None)), "duration")
        self.assertEqual(re.findall(r"<text class='ytick'[^>]*>([^<]+)</text>", chart), ["600", "620", "640"])
        first, second = (float(cy) for cy in re.findall(r"<circle class='dot' cx='[0-9.]+' cy='([0-9.]+)'", chart))
        # a 0.1% difference must not be stretched over the plot; both sit near mid-height
        self.assertLess(abs(first - second), 3)
        self.assertTrue(60 < second < first < 130, (first, second))
        self.assertIn("+0.10% vs previous point", chart)

    def test_value_axis_has_three_to_six_evenly_spaced_exactly_labelled_ticks(self):
        for low, high in ((490, 498), (2040, 2100), (0, 0), (0.0101, 0.0102), (0.007, 0.008), (5, 601),
                          (86.2, 91.4), (1e-9, 2e-9), (3e7, 3.1e7), (0, 1e-300)):
            with self.subTest(low=low, high=high):
                record, _, _ = example()
                record["measurements"] = record["measurements"][:1]
                record["measurements"][0].update(samples=[low, high], median=low / 2 + high / 2)
                chart = figure(self.render((record, None)), "duration")
                ticks = [float(label.replace(",", ""))
                         for label in re.findall(r"<text class='ytick'[^>]*>([^<]+)</text>", chart)]
                self.assertTrue(3 <= len(ticks) <= 6, ticks)
                steps = [after - before for before, after in zip(ticks, ticks[1:])]
                self.assertTrue(all(step > 0 for step in steps), ticks)
                # a rounded label would make neighbouring gaps unequal
                self.assertTrue(all(abs(step - steps[0]) <= steps[0] * 1e-6 for step in steps), ticks)

    def test_pooled_and_frozen_medians_stay_finite_and_inside_the_plot(self):
        pooled, _, _ = example("123", "2026-11-08T07:30:02+00:00")
        for item in pooled["measurements"]:
            item.update(samples=[1e308], median=1e308)
        point, = history.model({pooled["run_key"]: pooled}, {})[0][CASE]
        self.assertEqual(point["duration"]["value"], 1e308)
        stray, _, _ = example("124", "2026-11-09T07:30:02+00:00")
        stray["measurements"] = stray["measurements"][:1]
        stray["measurements"][0].update(samples=[600.0, 601.0], median=5.0)
        for record in (pooled, stray):
            chart = figure(self.render((record, None)), "duration")
            self.assertNotRegex(chart.lower(), r"\b(?:nan|inf)\b")
            heights = re.findall(r"<circle class='dot' cx='[0-9.]+' cy='([^']+)'", chart)
            self.assertEqual(len(heights), 1)
            self.assertTrue(14 <= float(heights[0]) <= 174, heights)

    def test_time_ticks_are_utc_days_inside_the_viewport(self):
        one, _, _ = example("123", "2026-11-08T07:30:02+00:00")
        two, _, _ = example("124", "2026-11-13T08:30:00+00:00")
        for runs in ([(one, None)], [(one, None), (two, None)]):
            chart = figure(self.render(*runs), "duration")
            ticks = re.findall(r"<text class='xtick' x='([0-9.]+)'[^>]*>([^<]+)</text>", chart)
            self.assertTrue(2 <= len(ticks) <= 5, ticks)
            for x, label in ticks:
                self.assertRegex(label, r"^\d{1,2} Nov$")
                self.assertTrue(46 <= float(x) <= 348, ticks)

    def test_single_run_zero_values_are_real_points_not_missing(self):
        record, raw, worker = example()
        for item in record["measurements"]:
            item.update(samples=[0, 0], median=0)
        for trial in raw["trials"]:
            trial["elapsed_seconds"] = 0
            trial["resources"]["metrics"].update(user_seconds=0, system_seconds=0, max_rss_kib=0)
        extra = companion(record, raw, worker)
        html = self.render((record, extra))
        self.assertEqual(html.count("<svg "), 3)
        self.assertNotRegex(html.lower(), r"(?:cx|cy|x|y|x1|x2|y1|y2|points)='[^']*(?:nan|inf)")
        point, = history.model(*split([(record, extra)]))[0][CASE]
        self.assertEqual((point["cpu"]["value"], point["memory"]["value"]), (0, 0))

    def test_largest_finite_duration_never_overflows_svg_coordinates(self):
        record, _, _ = example()
        maximum = float.fromhex("0x1.fffffffffffffp+1023")
        record["measurements"] = record["measurements"][:1]
        record["measurements"][0].update(samples=[maximum], median=maximum)
        html = self.render((record, None))
        self.assertNotRegex(html.lower(), r"(?:cx|cy|x|y|x1|x2|y1|y2|points)='[^']*(?:nan|inf)")
        self.assertNotIn(">nan<", html.lower())
        self.assertNotIn(">inf<", html.lower())

    def test_denormal_and_unbounded_ratios_do_not_break_the_page(self):
        tiny, _, _ = example("123", "2026-11-08T07:30:02+00:00")
        huge, _, _ = example("124", "2026-11-09T07:30:02+00:00")
        for record, samples in ((tiny, [0.0, 2e-323]), (huge, [1e308, 1e308])):
            record["measurements"] = record["measurements"][:1]
            record["measurements"][0].update(samples=samples, median=samples[-1])
        for runs in ([(tiny, None)], [(tiny, None), (huge, None)]):
            html = self.render(*runs)
            self.assertEqual(html.count("<svg "), 1)
            self.assertNotRegex(html.lower(), r"(?:cx|cy|x|y|x1|x2|y1|y2|points)='[^']*(?:nan|inf)")
            self.assertNotIn("inf%", html)
            self.assertNotIn("nan%", html)

    def test_points_link_to_their_run_and_describe_themselves(self):
        record, extra = observed("123", "08")
        chart = figure(self.render((record, extra)), "duration")
        label = re.search(r"<a class='pt' href='runs/" + record["run_key"] + r".html' aria-label='([^']+)'", chart)
        self.assertIsNotNone(label)
        for part in ("621.4 s", "8 Nov 2026 07:30 UTC", "run 123-1", "4 copies", "621.1–621.7"):
            self.assertIn(part, label.group(1))
        self.assertIn("class='tip'", chart)
        table = self.render((record, extra))
        table = table[table.index("<summary>Data table</summary>"):]
        self.assertIn("runs/" + record["run_key"] + ".html#observed-setup", table)
        for cell in ("<time datetime='2026-11-08T07:30:02Z'>8 Nov 2026 07:30 UTC</time>",
                     "<time datetime='2026-11-08T22:44:32+00:00'>8 Nov 2026 22:44 UTC</time>",
                     "<td>621.367 (621.100–621.654)</td>", "<code>rcp-baseline</code>"):
            self.assertIn(cell, table)

    def test_hostile_record_strings_are_escaped_in_the_data_table(self):
        record, _, _ = example()
        record["measurements"][0]["series_id"] = "'><img src=x onerror=alert(1)>"
        html = self.render((record, None))
        self.assertNotIn("<img", html)
        self.assertIn("&#x27;&gt;&lt;img src=x", html)
        self.assertIn("<summary>Data table</summary>", html)
        self.assertIn("<td>not recorded</td>", html)

    def test_reading_guide_explains_scope_ranges_and_linking(self):
        text = history.reading_html()
        for phrase in ("id='reading'", "not whole-host totals", "not confidence intervals",
                       "worker-reported", "starts a new line", "not independently attested"):
            self.assertIn(phrase, text)
        self.assertNotIn("<script", text)
        # pending runs are listed under collection status, not in the run list
        self.assertNotIn("pending", text)

    def test_full_retained_inventory_keeps_a_bounded_visible_plot_window(self):
        template, raw, worker = example()
        base_measurements, base_trials = template["measurements"], raw["trials"]
        template["measurements"] = [dict(item, case_id=case)
            for case in history.CASES for item in base_measurements]
        raw["trials"] = [dict(item, case_id=case) for case in history.CASES for item in base_trials]
        records, extras = {}, {}
        for index in range(150):
            record = copy.deepcopy(template)
            run = str(1000 + index)
            record.update(run_key="repo-655073755-run-" + run,
                timestamp=(datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=index)).isoformat())
            record["identity"].update(run=run + "-1",
                run_url="https://github.com/wykurz/rcp/actions/runs/" + run + "/attempts/1")
            records[record["run_key"]] = record
            extras[record["run_key"]] = companion(record, raw, worker)
        html = history.render(records, extras)
        self.assertLessEqual(len(html.encode()), projection.MAX_PUBLIC_HTML)
        self.assertIn("118 older qualified runs", html)
        self.assertIn("run-1149.html", html)
        self.assertNotIn("run-1000.html", html)
        self.assertEqual(html.count("<svg "), 12)
        self.assertEqual(html.count("<polyline"), 12)
        self.assertNotIn("No qualified runs yet", html)

    def test_an_oversized_page_drops_the_oldest_plotted_runs_first(self):
        records = {}
        for index in range(5):
            record, _, _ = example(str(1000 + index), "2026-11-0%dT07:30:02+00:00" % (index + 1))
            records[record["run_key"]] = record
        full = history.render(records, {})
        self.assertIn("run-1000.html", full)
        with patch.object(projection, "MAX_PUBLIC_HTML", len(full.encode()) - 1):
            html = history.render(records, {})
        self.assertIn("run-1004.html", html)
        self.assertNotIn("run-1000.html", html)
        self.assertIn("1 older qualified run ", html)

    def test_supported_legacy_inventory_limit_keeps_separate_plot_bounds(self):
        from . import production_publish
        self.assertEqual(projection.MAX_PUBLIC_HISTORY_RUNS, production_publish.MAX_HISTORY_RUNS)
        record, _, _ = example()
        records = {}
        for index in range(projection.MAX_PUBLIC_HISTORY_RUNS):
            value = copy.deepcopy(record)
            run = str(10000 + index)
            value.update(run_key="repo-655073755-run-" + run)
            value["identity"].update(run=run + "-1",
                run_url="https://github.com/wykurz/rcp/actions/runs/" + run + "/attempts/1")
            records[value["run_key"]] = value
        html = history.render(records, {})
        self.assertLessEqual(len(html.encode()), projection.MAX_PUBLIC_HTML)
        self.assertIn("1968 older qualified runs", html)
        self.assertNotIn("run-10000.html", html)
        self.assertIn("run-11999.html", html)
        records["extra"] = record
        with self.assertRaisesRegex(ValueError, "bounded record set"):
            history.render(records, {})

    def test_output_remains_bounded(self):
        record, _, _ = example()
        with patch.object(projection, "MAX_PUBLIC_HTML", 100):
            with self.assertRaisesRegex(ValueError, "history plots"):
                history.render({record["run_key"]: record}, {})


if __name__ == "__main__":
    unittest.main()
