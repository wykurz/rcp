"""Behavior tests for automatic history comparisons and their evidence trail."""

import copy
import hashlib
import json
from pathlib import Path
import statistics
import tempfile
import unittest

from benchmarks import changes, report
from benchmarks.test_report import sample_run


def observation(identifier, stamp, samples=(20, 22, 24)):
    run = sample_run(identifier * 32, stamp)
    run["context"].update(purpose="performance", repository="test/rcp")
    for trial, seconds in zip(run["trials"], samples):
        trial["elapsed_seconds"] = seconds
    summary = run["summaries"][0]
    summary.update(samples=list(samples), median=statistics.median(samples), minimum=min(samples),
                   maximum=max(samples), stdev=statistics.stdev(samples))
    return run


class ChangeTests(unittest.TestCase):
    def setUp(self):
        self.before = observation("a", "2026-09-26T12:00:00Z")
        self.after = observation("b", "2026-09-27T12:00:00Z", (24, 26.4, 28.8))

    def evidence(self, *runs):
        for run in runs:
            report.validate_result(run)
        return changes.build(runs, {run["run_id"]: [{"artifact_id": "source-1", "sha256": "f" * 64}] for run in runs})

    def test_compares_latest_earlier_compatible_run_and_preserves_raw_samples(self):
        older = observation("c", "2026-09-25T12:00:00Z", (10, 11, 12))
        evidence = self.evidence(self.after, older, self.before)
        row = evidence["changes"][-1]
        self.assertFalse(evidence["acceptance_evaluated"])
        self.assertEqual(row["status"], "compared")
        self.assertEqual(row["reference"]["run_id"], self.before["run_id"])
        self.assertAlmostEqual(row["ratio"], 1.2)
        self.assertAlmostEqual(row["change_percent"], 20)
        self.assertEqual(row["current"]["samples_seconds"], [24, 26.4, 28.8])
        self.assertEqual(row["reference"]["samples_seconds"], [20, 22, 24])
        self.assertEqual(row["current"]["trial_indices"], [0, 1, 2])
        text = changes.markdown(evidence, [older, self.before, self.after])
        self.assertIn("1.200x (+20.0%)", text)
        self.assertIn("no scoped timings", text)
        self.assertIn("unpaired observations", text)
        self.assertIn(f"(index.html#run-{self.before['run_id']})", text)

    def test_contract_or_repository_changes_do_not_cross_match(self):
        for field in ("series_id", "repository", "case_id", "variant_id"):
            with self.subTest(field=field):
                after = copy.deepcopy(self.after)
                if field == "series_id":
                    after["summaries"][0][field] = "2" * 64
                elif field == "repository":
                    after["context"][field] = "other/repo"
                else:
                    for item in after["summaries"] + after["trials"]:
                        item[field] = "different"
                    after["cases" if field == "case_id" else "variants"][0]["id"] = "different"
                row = self.evidence(self.before, after)["changes"][-1]
                self.assertEqual(row["status"], "no-compatible-reference")
                self.assertIsNone(row["ratio"])

    def test_excluded_observations_neither_compare_nor_replace_reference(self):
        later = observation("c", "2026-09-28T12:00:00Z")
        for field, value, expected in (("purpose", "smoke", "not-performance"),
                                       ("purpose", "diagnostic", "not-performance"),
                                       ("dirty", True, "unqualified-revision"),
                                       ("dirty", None, "unqualified-revision"),
                                       ("commit", None, "unqualified-revision"),
                                       ("status", "running", "producer-running")):
            with self.subTest(field=field, value=value):
                excluded = copy.deepcopy(self.after)
                parent = excluded["context"] if field == "purpose" else excluded if field == "status" else excluded["revision"]
                parent[field] = value
                evidence = self.evidence(self.before, excluded, later)
                self.assertEqual(evidence["changes"][1]["status"], expected)
                self.assertIsNone(evidence["changes"][1]["ratio"])
                self.assertEqual(evidence["changes"][-1]["reference"]["run_id"], self.before["run_id"])

    def test_equal_timestamps_do_not_establish_order_or_choose_arbitrary_reference(self):
        self.after["timestamp"] = self.before["timestamp"]
        later = observation("c", "2026-09-28T12:00:00Z")
        evidence = self.evidence(later, self.after, self.before)
        self.assertEqual([row["status"] for row in evidence["changes"]],
                         ["no-compatible-reference", "no-compatible-reference", "ambiguous-reference"])
        self.assertIsNone(evidence["changes"][-1]["ratio"])

    def test_completed_case_in_failed_run_remains_qualified_but_failure_visible(self):
        self.before["status"] = "failed"
        self.before["error"] = "a later case failed"
        evidence = self.evidence(self.before, self.after)
        row = evidence["changes"][-1]
        self.assertEqual(row["reference"]["run_status"], "failed")
        self.assertEqual(row["status"], "compared")
        self.assertIn("reference case from failed run", changes.markdown(evidence, [self.before, self.after]))

    def test_early_failure_has_a_run_entry_without_invented_numeric_evidence(self):
        self.after.update(status="failed", cases=[], variants=[], trials=[], summaries=[])
        evidence = self.evidence(self.before, self.after)
        self.assertEqual(len(evidence["changes"]), 1)
        self.assertEqual(evidence["runs"][-1]["completed_summaries"], 0)
        text = changes.markdown(evidence, [self.before, self.after])
        self.assertIn("No completed case measurements", text)
        self.assertIn("Triage the producer failure", text)

    def test_legacy_performance_and_short_reference_are_explicit(self):
        self.before = sample_run()
        self.before["context"]["repository"] = "test/rcp"
        evidence = self.evidence(self.before, self.after)
        row = evidence["changes"][-1]
        self.assertEqual(row["status"], "compared")
        self.assertTrue(row["short_sample"])
        self.assertIn("repeat under 10 s", changes.markdown(evidence, [self.before, self.after]))

    def test_original_hashes_duplicates_and_report_files_survive_rendering(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            history = root / "runs"
            history.mkdir()
            original = json.dumps(self.before)
            duplicate = json.dumps(self.before, indent=2)
            for filename, text in (("before.json", original), ("duplicate.json", duplicate), ("after.json", json.dumps(self.after))):
                (history / filename).write_text(text)
            output = root / "site"
            report.render(root, output)
            evidence = json.loads((output / "changes.json").read_text())
            sources = evidence["changes"][-1]["reference"]["sources"]
            self.assertEqual({s["sha256"] for s in sources}, {hashlib.sha256(text.encode()).hexdigest() for text in (original, duplicate)})
            self.assertEqual(len(evidence["runs"]), 2)
            self.assertIn(self.before["run_id"], (output / "changes.md").read_text())
            self.assertIn('href="changes.md"', (output / "index.html").read_text())
            self.assertEqual(json.loads((output / "history.json").read_text())["runs"], [self.before, self.after])

    def test_missing_reference_is_visible_in_standalone_smoke_artifact(self):
        self.after["context"]["purpose"] = "smoke"
        evidence = self.evidence(self.after)
        self.assertEqual(evidence["changes"][0]["status"], "not-performance")
        self.assertIn("Excluded from historical comparisons", changes.markdown(evidence, [self.after]))

    def test_markdown_bounds_run_count_and_treats_metadata_as_text(self):
        self.after["context"]["runner_label"] = '<script>x</script>|[click](https://example.com)\n`code`'
        evidence = self.evidence(self.before, self.after)
        text = changes.markdown(evidence, [self.before, self.after], limit=1)
        self.assertIn("Showing the latest 1 runs", text)
        self.assertNotIn("## " + self.before["timestamp"], text)
        self.assertNotIn("<script>", text)
        self.assertNotIn("[click]", text)
        self.assertIn("&#124;", text)

    def test_legacy_missing_commit_and_unhashable_repository_remain_reportable(self):
        for field, expected in (("commit", "unqualified-revision"), ("repository", "unqualified-repository")):
            with self.subTest(field=field):
                run = copy.deepcopy(self.after)
                if field == "commit":
                    del run["revision"]["commit"]
                else:
                    run["context"]["repository"] = {"legacy": "unknown"}
                evidence = self.evidence(self.before, run)
                self.assertEqual(evidence["changes"][-1]["status"], expected)
                self.assertIn(expected, changes.markdown(evidence, [self.before, run]))

    def test_missing_or_unqualified_repository_never_compares_or_becomes_a_reference(self):
        for repository in (None, "", " ", "rcp", "/tmp/owner/rcp", "owner/rcp/extra"):
            with self.subTest(repository=repository):
                unknown = copy.deepcopy(self.before)
                later = copy.deepcopy(self.after)
                for run in (unknown, later):
                    if repository is None:
                        del run["context"]["repository"]
                    else:
                        run["context"]["repository"] = repository
                evidence = self.evidence(unknown, later)
                self.assertTrue(all(row["status"] == "unqualified-repository" and row["ratio"] is None for row in evidence["changes"]))
                evidence = self.evidence(unknown, self.after)
                self.assertEqual(evidence["changes"][-1]["status"], "no-compatible-reference")
                self.assertIn("unqualified-repository", changes.markdown(evidence, [unknown, self.after]))

    def test_nonpositive_rounded_summary_does_not_crash_or_invent_a_ratio(self):
        for which in ("reference", "current"):
            with self.subTest(which=which):
                before = observation("a", "2026-09-26T12:00:00Z", (1e-13,) * 3)
                after = observation("b", "2026-09-27T12:00:00Z", (1e-13,) * 3)
                # the legacy validator permits this within its absolute summary tolerance
                selected = before if which == "reference" else after
                selected["summaries"][0].update(median=0, minimum=0)
                evidence = self.evidence(before, after)
                row = evidence["changes"][-1]
                self.assertEqual(row["status"], "unrepresentable-ratio")
                self.assertIsNone(row["ratio"])
                json.dumps(evidence, allow_nan=False)

    def test_invalid_measurements_fail_reporting_without_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.before["summaries"][0]["median"] = 100
            path = root / "results.json"
            path.write_text(json.dumps(self.before))
            with self.assertRaises(ValueError):
                report.render(path, root / "output")
            self.assertFalse((root / "output").exists())


if __name__ == "__main__":
    unittest.main()
