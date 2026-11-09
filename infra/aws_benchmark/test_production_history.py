"""Qualified history points preserve frozen timing and complete resource scope."""
import copy
from datetime import datetime, timedelta, timezone
import hashlib
from statistics import median
import unittest
from unittest.mock import patch

from . import production_history as history, production_diagnostics as diagnostics
from . import production_projection as projection
from .test_production_diagnostics import inputs


def example(run="42458459014", at="2026-11-08T07:30:02+00:00"):
    _, raw, worker = inputs()
    measurements = []
    for variant in ("rcp-default", "rcp-baseline"):
        samples = [t["elapsed_seconds"] for t in raw["trials"] if t["variant_id"] == variant]
        measurements.append(dict(case_id="bulk-1g-1tb", variant_id=variant,
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


class HistoryTests(unittest.TestCase):
    def model(self, record, extra=None):
        return history.model({record["run_key"]: record},
            {} if extra is None else {record["run_key"]: extra})[0]["bulk-1g-1tb"]

    def test_three_figures_for_each_configured_file_set(self):
        record, raw, worker = example()
        html = history.render({record["run_key"]: record},
                              {record["run_key"]: companion(record, raw, worker)})
        for metric in ("duration", "cpu", "memory"):
            self.assertEqual(html.count("data-history-metric='" + metric + "'"), 4)
        self.assertEqual(html.count("<svg "), 3)
        self.assertIn(projection.MEASUREMENT_CAVEAT, html)
        self.assertIn(projection.VALIDATION, html)
        self.assertIn("not whole-host totals", html)
        self.assertIn("not confidence intervals", html)
        self.assertIn("No recorded qualified", html)

    def test_cpu_is_sum_per_copy_before_median(self):
        record, raw, worker = example()
        default = next(x for x in raw["trials"] if x["variant_id"] == "rcp-default")
        rows = []
        for i, (user, system) in enumerate(((0, 100), (0, 0), (100, 0)), 1):
            t = copy.deepcopy(default)
            t.update(iteration=i, elapsed_seconds=float(i))
            t["resources"]["metrics"].update(user_seconds=user, system_seconds=system)
            rows.append(t)
        raw["trials"] = rows + [t for t in raw["trials"] if t["variant_id"] == "rcp-baseline"]
        target = next(m for m in record["measurements"] if m["variant_id"] == "rcp-default")
        target.update(samples=[1.0, 2.0, 3.0], median=2.0)
        point = next(p for p in self.model(record, companion(record, raw, worker))
                     if p["variant"] == "rcp-default")
        self.assertEqual(point["cpu"], dict(value=100, low=0, high=100, samples=3))
        self.assertEqual(point["duration"]["value"], 2)

    def test_peak_rss_uses_kib_to_mib_and_repeat_range(self):
        record, raw, worker = example()
        rows = [t for t in raw["trials"] if t["variant_id"] == "rcp-default"]
        rows[0]["resources"]["metrics"]["max_rss_kib"] = 1024
        rows[1]["resources"]["metrics"]["max_rss_kib"] = 3072
        point = next(p for p in self.model(record, companion(record, raw, worker))
                     if p["variant"] == "rcp-default")
        self.assertEqual(point["memory"], dict(value=2, low=1, high=3, samples=2))

    def test_partial_resource_coverage_does_not_cherry_pick_repeats(self):
        record, raw, worker = example()
        next(t for t in raw["trials"] if t["variant_id"] == "rcp-default")["resources"] = {}
        points = self.model(record, companion(record, raw, worker))
        point = next(p for p in points if p["variant"] == "rcp-default")
        self.assertIsNone(point["cpu"])
        self.assertIsNone(point["memory"])
        self.assertIsNotNone(point["duration"])
        self.assertIsNotNone(next(p for p in points if p["variant"] == "rcp-baseline")["cpu"])

    def test_missing_companion_keeps_duration_without_resource_zero(self):
        record, _, _ = example()
        points = self.model(record)
        self.assertEqual(len(points), 2)
        self.assertTrue(all(p["duration"] is not None and p["cpu"] is None and p["memory"] is None for p in points))
        html = history.render({record["run_key"]: record}, {})
        self.assertIn("unavailable", html)
        self.assertEqual(html.count("<svg "), 1)

    def test_failed_retired_run_has_no_performance_points(self):
        record, _, _ = example()
        record.update(benchmark_valid=False, qualification_outcome="failed", measurements=[])
        del record["timestamp"]
        del record["verification"]
        self.assertFalse(self.model(record))

    def test_time_uses_normalized_worker_start_not_collection_time(self):
        one, _, _ = example("123", "2026-11-08T05:00:00+05:00")
        two, _, _ = example("124", "2026-11-07T23:00:00+00:00")
        two["collected_at"] = "2026-11-09T23:00:00+00:00"
        points = history.model({one["run_key"]: one, two["run_key"]: two}, {})[0]["bulk-1g-1tb"]
        self.assertEqual(points[0]["run_key"], two["run_key"])
        self.assertEqual(points[-1]["timestamp"], "2026-11-08T00:00:00Z")
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
                self.model(broken)

    def test_baseline_and_candidate_never_pool_even_if_series_ids_match(self):
        record, raw, worker = example()
        record["measurements"][1]["series_id"] = record["measurements"][0]["series_id"]
        points = self.model(record, companion(record, raw, worker))
        self.assertNotEqual(points[0]["group"], points[1]["group"])

    def test_known_setup_and_changed_series_split_traces(self):
        one, raw, worker = example("123")
        two = copy.deepcopy(one)
        two["run_key"] = "repo-655073755-run-124"
        two["identity"].update(run="124-1", run_url="https://github.com/wykurz/rcp/actions/runs/124/attempts/1")
        first = companion(one, raw, worker)
        raw["context"]["environment"]["cpu_model"] = "Different CPU model"
        second = companion(two, raw, worker)
        cases, _ = history.model({one["run_key"]: one, two["run_key"]: two},
            {one["run_key"]: first, two["run_key"]: second})
        grouped = [p["group"] for p in cases["bulk-1g-1tb"] if p["variant"] == "rcp-default"]
        self.assertNotEqual(grouped[0], grouped[1])
        second["schema"] = "unreviewed-version"
        with self.assertRaises(ValueError):
            history.model({two["run_key"]: two}, {two["run_key"]: second})

    def test_first_and_last_dates_align_inside_the_svg_viewport(self):
        one, _, _ = example("123")
        two, _, _ = example("124", "2026-11-09T07:30:02+00:00")
        html = history.render({one["run_key"]: one, two["run_key"]: two}, {})
        self.assertIn("x='78.000' y='236' text-anchor='start'", html)
        self.assertIn("x='780.000' y='236' text-anchor='end'", html)

    def test_unobserved_setups_are_not_connected_across_runs(self):
        one, _, _ = example("123")
        two, _, _ = example("124", "2026-11-09T07:30:02+00:00")
        html = history.render({one["run_key"]: one, two["run_key"]: two}, {})
        self.assertNotIn("<polyline", html)

    def test_missing_middle_companion_breaks_otherwise_comparable_trace(self):
        records, extras = {}, {}
        for run, date in (("123", "08"), ("124", "09"), ("125", "10")):
            record, raw, worker = example(run, "2026-11-" + date + "T07:30:02+00:00")
            records[record["run_key"]] = record
            if run != "124":
                extras[record["run_key"]] = companion(record, raw, worker)
        html = history.render(records, extras)
        self.assertNotIn("<polyline", html)
        middle, raw, worker = example("124", "2026-11-09T07:30:02+00:00")
        extras[middle["run_key"]] = companion(middle, raw, worker)
        self.assertEqual(history.render(records, extras).count("<polyline"), 6)

    def test_single_run_zero_values_are_real_points_not_missing(self):
        record, raw, worker = example()
        for item in record["measurements"]:
            item.update(samples=[0, 0], median=0)
        for trial in raw["trials"]:
            trial["elapsed_seconds"] = 0
            trial["resources"]["metrics"].update(user_seconds=0, system_seconds=0, max_rss_kib=0)
        extra = companion(record, raw, worker)
        html = history.render({record["run_key"]: record}, {record["run_key"]: extra})
        self.assertEqual(html.count("<svg "), 3)
        self.assertNotRegex(html.lower(), r"(?:cx|cy|x1|x2|y1|y2)=.?(?:nan|inf)")
        self.assertTrue(all(p["cpu"]["value"] == 0 and p["memory"]["value"] == 0
                            for p in self.model(record, extra)))

    def test_largest_finite_duration_never_overflows_svg_coordinates(self):
        record, _, _ = example()
        maximum = float.fromhex("0x1.fffffffffffffp+1023")
        record["measurements"][0].update(samples=[maximum], median=maximum)
        html = history.render({record["run_key"]: record}, {})
        self.assertNotRegex(html.lower(), r"(?:cx|cy|x1|x2|y1|y2)=.?(?:nan|inf)")
        self.assertNotIn(">nan<", html.lower())
        self.assertNotIn(">inf<", html.lower())

    def test_real_freeze_preserves_candidate_history_but_splits_harness_changes(self):
        record, _, _ = example()
        raw = dict(status="complete", timestamp=record["timestamp"], summaries=record["measurements"])
        pins = dict(ami="ami-123", source="2" * 40)
        evidence = dict(controller={}, expected_verification=record["verification"])
        def frozen(candidate, harness):
            identity = dict(record["identity"], candidate=candidate)
            value = projection.freeze(record["run_key"], identity, dict(pins, source=harness),
                True, evidence, raw, collected_at=record["collected_at"])
            return projection.bind(value, record["retirement_sha256"])
        first = self.model(frozen("3" * 40, "2" * 40))
        next_candidate = self.model(frozen("4" * 40, "2" * 40))
        next_harness = self.model(frozen("4" * 40, "5" * 40))
        self.assertEqual([p["group"] for p in first], [p["group"] for p in next_candidate])
        self.assertNotEqual([p["group"] for p in first], [p["group"] for p in next_harness])

    def test_escaped_labels_and_provenance_links_never_embed_private_raw_data(self):
        record, _, _ = example()
        record["measurements"][0]["variant_id"] = "<script>alert(1)</script>"
        record["measurements"][0]["series_id"] = "'><img src=x onerror=alert(1)>"
        html = history.render({record["run_key"]: record}, {})
        self.assertNotIn("<script>", html)
        self.assertNotIn("<img", html)
        self.assertIn("&lt;script&gt;", html)
        self.assertIn("runs/" + record["run_key"] + ".html", html)
        self.assertIn("Source values and provenance", html)

    def test_frozen_inputs_unchanged_and_companion_identity_rechecked(self):
        record, raw, worker = example()
        extra = companion(record, raw, worker)
        before = copy.deepcopy((record, extra))
        history.render({record["run_key"]: record}, {record["run_key"]: extra})
        self.assertEqual((record, extra), before)
        extra["retirement_sha256"] = "b" * 64
        with self.assertRaises(ValueError):
            self.model(record, extra)

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
        self.assertEqual(len(records), 150)

    def test_adversarial_long_labels_reduce_window_instead_of_blocking_site(self):
        template, _, _ = example()
        template["measurements"] = [dict(template["measurements"][0],
            variant_id="&" * 252 + str(index), series_id="&" * 256) for index in range(100)]
        records = {}
        for index in range(5):
            record = copy.deepcopy(template)
            run = str(1000 + index)
            record.update(run_key="repo-655073755-run-" + run,
                timestamp=(datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=index)).isoformat())
            record["identity"].update(run=run + "-1",
                run_url="https://github.com/wykurz/rcp/actions/runs/" + run + "/attempts/1")
            records[record["run_key"]] = record
        html = history.render(records, {})
        self.assertLessEqual(len(html.encode()), projection.MAX_PUBLIC_HTML)
        self.assertIn("complete retained run inventory", html)
        self.assertIn("run-1004.html", html)

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
