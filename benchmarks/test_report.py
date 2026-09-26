"""Behavior tests for the standalone benchmark history report."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


def sample_run(run_id="a" * 32, timestamp="2026-09-26T12:00:00Z", status="complete"):
    return {
        "schema_version": 1,
        "run_id": run_id,
        "timestamp": timestamp,
        "status": status,
        "revision": {"commit": "1234567890abcdef", "branch": "main", "dirty": False},
        "context": {
            "runner_label": "CI x86",
            "topology": "loopback",
            "cache_policy": "source-warm",
            "timing_policy": "command completion",
            "source": "/mnt/src",
            "destination": "/mnt/dst",
        },
        "tools": {"rcp": {"path": "/bin/rcp", "version": "0.40", "sha256": "abc"}},
        "cases": [{"id": "tiny-10k", "name": "Tiny files"}],
        "variants": [{"id": "rcp-default", "name": "rcp"}],
        "trials": [{
            "case_id": "tiny-10k", "variant_id": "rcp-default", "iteration": iteration,
            "elapsed_seconds": seconds, "exit_codes": [0], "status": "ok",
            "commands": [["rcp", "src", "dst"]], "validation": {"ok": True}, "logs": [],
        } for iteration, seconds in ((1, 1.1), (2, 1.2), (3, 1.3))],
        "summaries": [{
            "series_id": "1" * 64, "case_id": "tiny-10k", "variant_id": "rcp-default",
            "unit": "seconds", "median": 1.2, "minimum": 1.1, "maximum": 1.3,
            "stdev": 0.1, "samples": [1.1, 1.2, 1.3], "files_per_second": 8333.3,
        }],
    }


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def render(self, input_path):
        output = self.root / "site"
        result = subprocess.run(
            [sys.executable, "-m", "benchmarks.report", str(input_path), "--output", str(output)],
            capture_output=True, text=True, cwd=Path(__file__).resolve().parent.parent,
        )
        return result, output

    def write(self, path, run):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(run), encoding="utf-8")

    def test_single_run_generates_readable_standalone_report(self):
        run = sample_run()
        source = self.root / "results.json"
        self.write(source, run)
        process, output = self.render(source)
        self.assertEqual(process.returncode, 0, process.stderr)
        history = json.loads((output / "history.json").read_text())
        page = (output / "index.html").read_text()
        self.assertEqual(history["runs"], [run])
        self.assertIn('id="chart"', page)
        self.assertIn("history-data", page)
        self.assertIn("tiny-10k", page)
        self.assertNotIn("<script src=", page)
        self.assertNotIn("<link href=", page)

    def test_history_keeps_incompatible_series_and_failed_runs_visible(self):
        first = sample_run()
        second = sample_run("b" * 32, "2026-09-27T12:00:00Z")
        second["summaries"][0]["series_id"] = "2" * 64
        failed = sample_run("c" * 32, "2026-09-28T12:00:00Z", "failed")
        failed["summaries"] = []
        failed["trials"][0]["status"] = "failed"
        failed["error"] = "copy exited 1"
        runs = self.root / "history" / "runs"
        for run in (failed, second, first):
            self.write(runs / f'{run["run_id"]}.json', run)
        process, output = self.render(runs.parent)
        self.assertEqual(process.returncode, 0, process.stderr)
        history = json.loads((output / "history.json").read_text())
        self.assertEqual([run["run_id"] for run in history["runs"]], ["a" * 32, "b" * 32, "c" * 32])
        self.assertEqual(len({run["summaries"][0]["series_id"] for run in history["runs"][:2]}), 2)
        self.assertEqual(history["runs"][2]["status"], "failed")
        self.assertIn("copy exited 1", (output / "index.html").read_text())

    def test_identical_duplicate_run_is_idempotent_but_conflict_fails(self):
        run = sample_run()
        runs = self.root / "history" / "runs"
        self.write(runs / "first.json", run)
        self.write(runs / "second.json", run)
        process, output = self.render(runs.parent)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(len(json.loads((output / "history.json").read_text())["runs"]), 1)
        run["error"] = "conflicting metadata"
        self.write(runs / "second.json", run)
        process, _ = self.render(runs.parent)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("duplicate run_id", process.stderr)

    def test_malformed_records_fail_without_producing_site(self):
        run = sample_run()
        run["summaries"][0]["median"] = -1
        source = self.root / "results.json"
        self.write(source, run)
        process, output = self.render(source)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("median", process.stderr)
        self.assertFalse((output / "index.html").exists())

    def test_early_failure_without_selected_workload_remains_visible(self):
        run = sample_run(status="failed")
        run["cases"] = []
        run["variants"] = []
        run["trials"] = []
        run["summaries"] = []
        run["error"] = "binary unavailable"
        source = self.root / "results.json"
        self.write(source, run)
        process, output = self.render(source)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(json.loads((output / "history.json").read_text())["runs"][0]["error"], "binary unavailable")

    def test_complete_run_without_measurements_is_invalid(self):
        run = sample_run()
        run["trials"] = []
        run["summaries"] = []
        source = self.root / "results.json"
        self.write(source, run)
        process, _ = self.render(source)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("summaries", process.stderr)

    def test_interrupted_trial_does_not_make_running_result_invalid(self):
        run = sample_run(status="failed")
        run["trials"][0]["status"] = "running"
        run["trials"][0].pop("elapsed_seconds")
        run["error"] = "cache preparation failed"
        source = self.root / "results.json"
        self.write(source, run)
        process, output = self.render(source)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(json.loads((output / "history.json").read_text())["runs"][0]["trials"][0]["status"], "running")

    def test_nonstandard_json_number_is_rejected_even_in_extra_metadata(self):
        source = self.root / "results.json"
        source.write_text(json.dumps(sample_run()).replace('"dirty": false', '"dirty": false, "free_memory": NaN'))
        process, output = self.render(source)
        self.assertNotEqual(process.returncode, 0)
        self.assertFalse((output / "index.html").exists())

    def test_duplicate_json_keys_are_rejected(self):
        source = self.root / "results.json"
        source.write_text(json.dumps(sample_run()).replace('"status": "complete"', '"status": "failed", "status": "complete"'))
        process, output = self.render(source)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("duplicate JSON key", process.stderr)
        self.assertFalse((output / "index.html").exists())

    def test_complete_run_rejects_claimed_success_with_failed_child(self):
        run = sample_run()
        run["trials"][0]["exit_codes"] = [1]
        source = self.root / "results.json"
        self.write(source, run)
        process, output = self.render(source)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("exit_codes", process.stderr)
        self.assertFalse((output / "index.html").exists())

    def test_complete_run_rejects_summary_not_backed_by_trials(self):
        run = sample_run()
        run["summaries"][0]["median"] = 1.25
        source = self.root / "results.json"
        self.write(source, run)
        process, output = self.render(source)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("median", process.stderr)
        self.assertFalse((output / "index.html").exists())

    def test_duplicate_case_id_is_rejected(self):
        run = sample_run()
        run["cases"].append(dict(run["cases"][0]))
        source = self.root / "results.json"
        self.write(source, run)
        process, _ = self.render(source)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("duplicate case", process.stderr)

    def test_untrusted_metadata_cannot_close_embedded_script_or_inject_html(self):
        run = sample_run()
        payload = '</script><img src=x onerror="alert(1)">'
        run["context"]["runner_label"] = payload
        run["error"] = payload
        source = self.root / "results.json"
        self.write(source, run)
        process, output = self.render(source)
        self.assertEqual(process.returncode, 0, process.stderr)
        page = (output / "index.html").read_text()
        self.assertNotIn(payload, page)
        self.assertNotIn("<img src=x", page)
        self.assertIn("\\u003c/script\\u003e", page)
        self.assertEqual(json.loads((output / "history.json").read_text())["runs"][0]["context"]["runner_label"], payload)


if __name__ == "__main__":
    unittest.main()
