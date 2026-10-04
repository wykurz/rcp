"""Behavior tests for automatic history comparisons and their evidence trail."""

import copy
import hashlib
import json
from html.parser import HTMLParser
from pathlib import Path
import statistics
import tempfile
import unittest
from unittest import mock

from benchmarks import changes, report
from benchmarks.test_report import sample_run
from benchmarks.test_timings import timing_report


def observation(identifier, stamp, samples=(20, 22, 24)):
    run = sample_run(identifier * 32, stamp)
    run["context"].update(purpose="performance", repository="test/rcp")
    run["trials"] = [dict(run["trials"][0], iteration=index + 1, elapsed_seconds=seconds) for index, seconds in enumerate(samples)]
    summary = run["summaries"][0]
    summary.update(samples=list(samples), median=statistics.median(samples), minimum=min(samples),
                   maximum=max(samples), stdev=statistics.stdev(samples) if len(samples) > 1 else 0.0)
    return run


class Fragment(HTMLParser):
    """Inspect the actual HTML table cells, link targets and literal metadata."""
    def __init__(self, source):
        super().__init__(convert_charrefs=True)
        self.nodes = []
        self.stack = []
        self.feed(source)
    def handle_starttag(self, tag, attrs):
        node = {"tag": tag, "attrs": dict(attrs), "children": []}
        self.nodes.append(node)
        if self.stack:
            self.stack[-1]["children"].append(node)
        if tag not in ("br", "meta", "link", "hr"):
            self.stack.append(node)
    def handle_endtag(self, tag):
        if self.stack and self.stack[-1]["tag"] == tag:
            self.stack.pop()
    def handle_data(self, data):
        if self.stack:
            self.stack[-1]["children"].append(data)
    def elements(self, tag):
        return [node for node in self.nodes if node["tag"] == tag]
    def text(self, node):
        return "".join(child if isinstance(child, str) else self.text(child) for child in node["children"])
    def rows(self):
        return [[child for child in row["children"] if isinstance(child, dict) and child["tag"] == "td"] for row in self.elements("tr") if any(isinstance(child, dict) and child["tag"] == "td" for child in row["children"])]


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
        text = changes.markdown(evidence)
        self.assertIn("1.200x (+20.0%)", text)
        self.assertIn("current timings: missing-legacy", text)
        self.assertIn("unpaired observations", text)
        reference_cell = Fragment(text).rows()[0][4]
        self.assertIn(self.before["run_id"], Fragment(text).text(reference_cell))
        self.assertEqual(reference_cell["children"][0]["attrs"]["href"], "index.html#run-" + self.before["run_id"])

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
                                       ("commit", "HEAD", "unqualified-revision"),
                                       ("commit", "unknown", "unqualified-revision"),
                                       ("commit", " ", "unqualified-revision"),
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
        self.assertIn("reference case from failed run", changes.markdown(evidence))

    def test_early_failure_has_a_run_entry_without_invented_numeric_evidence(self):
        self.after.update(status="failed", cases=[], variants=[], trials=[], summaries=[])
        evidence = self.evidence(self.before, self.after)
        self.assertEqual(len(evidence["changes"]), 1)
        self.assertEqual(evidence["runs"][-1]["completed_summaries"], 0)
        text = changes.markdown(evidence)
        self.assertIn("No completed case measurements", text)
        self.assertIn("Triage the producer failure", text)

    def test_legacy_performance_and_short_reference_are_explicit(self):
        self.before = sample_run()
        self.before["context"]["repository"] = "test/rcp"
        evidence = self.evidence(self.before, self.after)
        row = evidence["changes"][-1]
        self.assertEqual(row["status"], "compared")
        self.assertFalse(row["current_short_sample"])
        self.assertTrue(row["reference_short_sample"])
        self.assertIn("repeat under 10 s", changes.markdown(evidence))

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
            self.assertIn('href="changes.html"', (output / "index.html").read_text())
            self.assertEqual(len(Fragment((output / "changes.html").read_text()).rows()), 2)
            self.assertTrue(all(source["artifact_id"] == "sha256:" + source["sha256"] for source in sources))
            self.assertEqual(json.loads((output / "history.json").read_text())["runs"], [self.before, self.after])

    def test_smoke_exclusion_is_visible_in_standalone_artifact(self):
        self.after["context"]["purpose"] = "smoke"
        evidence = self.evidence(self.after)
        self.assertEqual(evidence["changes"][0]["status"], "not-performance")
        self.assertIn("Excluded from historical comparisons", changes.markdown(evidence))

    def test_bounded_summary_keeps_reference_revision_date_and_link_outside_limit(self):
        self.after["revision"]["commit"] = "f" * 40
        evidence = self.evidence(self.before, self.after)
        parsed = Fragment(changes.markdown(evidence, limit=1))
        self.assertEqual(len(parsed.rows()), 1)
        self.assertEqual(len(parsed.elements("h2")), 1)
        reference = parsed.rows()[0][4]
        self.assertIn(self.before["timestamp"], parsed.text(reference))
        self.assertIn(self.before["revision"]["commit"][:10], parsed.text(reference))
        self.assertEqual(reference["children"][0]["attrs"]["href"], "index.html#run-" + self.before["run_id"])
        self.assertNotIn("same recorded checkout", parsed.text(parsed.rows()[0][5]))
        self.after["revision"] = self.before["revision"]
        self.assertIn("same recorded checkout revision", changes.markdown(self.evidence(self.before, self.after), limit=1))
        self.assertEqual(Fragment(changes.markdown(evidence, limit=0)).rows(), [])
        with self.assertRaisesRegex(ValueError, "nonnegative"):
            changes.markdown(evidence, limit=-1)

    def test_metadata_remains_literal_in_html_and_gfm_block_tables(self):
        value = '<script>x</script>|a\\|b **bold** _em_ ~~strike~~ @wykurz :smile: [click](https://example.com)\n\n`code`'
        value = value.replace("\\n", "\n")
        self.after["context"]["runner_label"] = value
        evidence = self.evidence(self.before, self.after)
        for source in (changes.markdown(evidence, limit=1), changes.html_report(evidence)):
            with self.subTest(source=source[:20]):
                parsed = Fragment(source)
                self.assertIn(value, [parsed.text(node) for node in parsed.elements("code")])
                for tag in ("script", "em", "strong", "del"):
                    self.assertFalse(parsed.elements(tag))
                self.assertTrue(all("example.com" not in node["attrs"]["href"] for node in parsed.elements("a")))
                self.assertNotIn("\n\n", source)
        self.after["revision"]["commit"] = "&unknown`revision"
        parsed = Fragment(changes.markdown(self.evidence(self.after)))
        self.assertIn("&unknown`revision", [parsed.text(node) for node in parsed.elements("code")])

    def test_summary_links_are_structured_and_missing_url_leaves_plain_ids(self):
        evidence = self.evidence(self.before, self.after)
        parsed = Fragment(changes.markdown(evidence, limit=1, link_base="https://example.test/rcp/"))
        self.assertEqual([node["attrs"]["href"] for node in parsed.elements("a")], ["https://example.test/rcp/index.html#run-" + run["run_id"] for run in (self.after, self.before)])
        fallback = Fragment(changes.markdown(evidence, link_base=None))
        self.assertFalse(fallback.elements("a"))
        self.assertIn(self.before["run_id"], fallback.text(fallback.rows()[0][4]))
        for base in ("/rcp", "javascript:alert(1)", "https://", "https://user@example.test", "https://example.test/?query=1", "https://example.test/#fragment", "https://example.test/\n"):
            with self.subTest(base=base), self.assertRaises(ValueError):
                changes.markdown(evidence, link_base=base)

    def test_short_sample_names_side_and_excludes_smoke_and_diagnostic_runs(self):
        self.before = observation("a", self.before["timestamp"], (8,9,10))
        for purpose in ("performance", "smoke", "diagnostic"):
            self.before["context"]["purpose"] = purpose
            self.after["context"]["purpose"] = purpose
            evidence = self.evidence(self.before, self.after)
            text = changes.markdown(evidence, limit=1)
            self.assertEqual(evidence["changes"][0]["current_short_sample"], purpose == "performance")
            self.assertEqual(evidence["changes"][-1]["reference_short_sample"], purpose == "performance")
            self.assertEqual("reference repeat under 10 s" in text, purpose == "performance")
            self.assertNotIn("current repeat under 10 s", text)
        self.after = observation("b", self.after["timestamp"], (1,2,3))
        self.before["context"]["purpose"] = "performance"
        text = changes.markdown(self.evidence(self.before, self.after), limit=1)
        self.assertIn("current repeat under 10 s", text)
        self.assertIn("reference repeat under 10 s", text)

    def test_timing_status_is_preserved_for_each_side_in_rendered_cells(self):
        for status in ("coarse", "unsupported", "disabled", "not_applicable", "missing-legacy"):
            with self.subTest(status=status):
                after = copy.deepcopy(self.after)
                if status != "missing-legacy":
                    after["context"]["timing_collection"] = {"rcp-default": status}
                    for trial in after["trials"]:
                        trial["timings"] = {"status": status, "reports": [timing_report(role) for role in ("rcp-master", "rcpd-source", "rcpd-destination")] if status == "coarse" else []}
                evidence = self.evidence(self.before, after)
                parsed = Fragment(changes.markdown(evidence, limit=1))
                text = parsed.text(parsed.rows()[0][5])
                self.assertIn("current timings: " + status, text)
                self.assertIn("reference timings: missing-legacy", text)

    def test_underflow_and_overflow_do_not_become_comparisons(self):
        for before, after in ((1e300,1e-300), (1e-300,1e300)):
            with self.subTest(before=before):
                evidence = self.evidence(observation("a", self.before["timestamp"], (before,)*3), observation("b", self.after["timestamp"], (after,)*3))
                row = evidence["changes"][-1]
                self.assertEqual(row["status"], "unrepresentable-ratio")
                self.assertIsNone(row["ratio"])
                parsed = Fragment(changes.markdown(evidence, limit=1))
                self.assertEqual(parsed.text(parsed.rows()[0][3]), "—")
                self.assertIn("unrepresentable-ratio", parsed.text(parsed.rows()[0][5]))
                json.dumps(evidence, allow_nan=False)

    def test_negligible_change_does_not_display_negative_zero(self):
        evidence = self.evidence(observation("a", self.before["timestamp"], (20,)*3), observation("b", self.after["timestamp"], (19.999,)*3))
        parsed = Fragment(changes.markdown(evidence, limit=1))
        self.assertEqual(parsed.text(parsed.rows()[0][3]), "1.000x (+0.0%)")

    def test_no_reference_status_is_in_the_measurement_row(self):
        parsed = Fragment(changes.markdown(self.evidence(self.after)))
        self.assertEqual(parsed.text(parsed.rows()[0][4]), "—")
        self.assertIn("no-compatible-reference", parsed.text(parsed.rows()[0][5]))
        self.assertNotIn("compared", parsed.text(parsed.rows()[0][5]))

    def test_late_arrival_reselects_reference_without_changing_policy(self):
        middle = observation("c", "2026-09-27T00:00:00Z", (30,)*3)
        first = self.evidence(self.before, self.after)
        second = self.evidence(self.before, middle, self.after)
        self.assertEqual(first["changes"][-1]["reference"]["run_id"], self.before["run_id"])
        self.assertEqual(second["changes"][-1]["reference"]["run_id"], middle["run_id"])
        self.assertEqual(second["comparison_policy"], "previous-compatible-terminal-clean-run-v1")
        self.assertIn("Late-arriving records can change previous comparisons", changes.markdown(second))

    def test_suite_and_repeat_differences_are_visible_without_splitting_identity(self):
        self.before["status"] = "failed"
        self.before["cases"].append(dict(self.before["cases"][0], id="extra-selected-case"))
        self.after = observation("b", self.after["timestamp"], (24,26.4))
        evidence = self.evidence(self.before, self.after)
        row = evidence["changes"][-1]
        self.assertEqual(row["status"], "compared")
        self.assertEqual(row["reference"]["case_order"], ["tiny-10k", "extra-selected-case"])
        parsed = Fragment(changes.markdown(evidence, limit=1))
        self.assertIn("case composition/order differs", parsed.text(parsed.rows()[0][5]))
        self.assertIn("repeat count differs", parsed.text(parsed.rows()[0][5]))

    def test_content_source_ids_survive_earlier_filename_insertion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "runs").mkdir()
            (root / "runs/b.json").write_text(json.dumps(self.after))
            runs, sources = report.load_result_records(root)
            before = changes.build(runs, sources)["runs"][0]["sources"]
            (root / "runs/a.json").write_text(json.dumps(self.before))
            runs, sources = report.load_result_records(root)
            after = changes.build(runs, sources)["runs"][-1]["sources"]
            self.assertEqual(before, after)
            self.assertEqual(before[0]["artifact_id"], "sha256:" + before[0]["sha256"])

    def test_rendering_failure_precedes_output_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "results.json").write_text(json.dumps(self.before))
            with mock.patch.object(changes, "html_report", side_effect=ValueError("render failed")):
                with self.assertRaisesRegex(ValueError, "render failed"):
                    report.render(root, root / "site")
            self.assertFalse((root / "site").exists())

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
                self.assertIn(expected, changes.markdown(evidence))

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
                self.assertIn("unqualified-repository", changes.markdown(evidence))

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
