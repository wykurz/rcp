"""Behavior tests for the standalone benchmark history report."""

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from benchmarks import report


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
        "cases": [{"id": "tiny-10k", "description": "Tiny files", "directory_widths": [10, 1, 1], "files_per_leaf": 1024, "file_size_bytes": 1024}],
        "variants": [{"id": "rcp-default", "description": "rcp default copy", "tool": "rcp", "args": ["--summary"], "processes": 1}],
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

    def dashboard_state(self, output):
        if not shutil.which("node"):
            self.skipTest("Node is unavailable for dashboard execution")
        script = r'''
const fs = require("fs"), vm = require("vm");
const html = fs.readFileSync(process.argv[1], "utf8");
const payload = html.match(/<script type="application\/json" id="history-data">([\s\S]*?)<\/script>/)[1];
const code = html.match(/<script>\s*([\s\S]*?)<\/script>/)[1];
class Element {
  constructor(tag) { this.tagName = tag; this.children = []; this.attributes = {}; this.style = {}; this.value = ""; this.textContent = ""; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = [...children]; }
  setAttribute(key, value) { this.attributes[key] = value; }
  addEventListener() {}
}
const ids = Object.fromEntries(["stats", "case-filter", "variant-filter", "group-filter", "series-filter", "chart", "legend", "run-rows"].map(id => [id, new Element(id)]));
ids["history-data"] = new Element("script"); ids["history-data"].textContent = payload;
const document = {getElementById: id => ids[id], createElement: tag => new Element(tag), createElementNS: (_, tag) => new Element(tag)};
vm.runInNewContext(code, {document});
const descendants = element => [element, ...element.children.flatMap(descendants)];
const chart = descendants(ids.chart), rows = descendants(ids["run-rows"]);
console.log(JSON.stringify({points: chart.filter(item => item.tagName === "circle").length, lines: chart.filter(item => item.tagName === "polyline").length, labels: descendants(ids.legend).map(item => item.textContent).join(" "), table: rows.map(item => item.textContent).join(" ")}));
'''
        process = subprocess.run(["node", "-e", script, str(output / "index.html")], capture_output=True, text=True)
        self.assertEqual(process.returncode, 0, process.stderr)
        return json.loads(process.stdout)

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
        run["summaries"] = []
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

    def test_interrupted_run_charts_only_prior_completed_case(self):
        for status in ("failed", "running"):
            with self.subTest(status=status):
                run = sample_run(status=status)
                run["cases"].append({"id": "later", "description": "Later workload", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1})
                run["trials"].append({"case_id": "later", "variant_id": "rcp-default", "iteration": 1, "elapsed_seconds": 0.4, "exit_codes": [1] if status == "failed" else [], "status": status, "commands": [["rcp"]], "validation": {"ok": False}, "logs": []})
                run["error"] = "later workload interrupted"
                source = self.root / "results.json"
                self.write(source, run)
                process, output = self.render(source)
                self.assertEqual(process.returncode, 0, process.stderr)
                dashboard = self.dashboard_state(output)
                self.assertEqual(dashboard["points"], 1)
                self.assertIn("Tiny files", dashboard["labels"])
                self.assertIn("rcp default copy", dashboard["labels"])
                self.assertIn(status, dashboard["table"])

    def test_failed_run_rejects_summary_with_failed_matching_trial(self):
        run = sample_run(status="failed")
        run["trials"][1]["status"] = "failed"
        run["trials"][1]["validation"]["ok"] = False
        source = self.root / "results.json"
        self.write(source, run)
        process, output = self.render(source)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("samples", process.stderr)
        self.assertFalse((output / "index.html").exists())

    def test_failed_run_rejects_summary_from_incomplete_case(self):
        run = sample_run(status="failed")
        run["variants"].append({"id": "rsync-a", "description": "rsync archive copy", "tool": "rsync", "args": ["-a"], "processes": 1})
        run["trials"].append({"case_id": "tiny-10k", "variant_id": "rsync-a", "iteration": 1, "elapsed_seconds": 0.4, "exit_codes": [1], "status": "failed", "commands": [["rsync"]], "validation": {"ok": False}, "logs": []})
        source = self.root / "results.json"
        self.write(source, run)
        process, output = self.render(source)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("incomplete case", process.stderr)
        self.assertFalse((output / "index.html").exists())

    def test_failed_run_rejects_case_with_unequal_variant_repetitions(self):
        run = sample_run(status="failed")
        run["variants"].append({"id": "rsync-a", "description": "rsync archive copy", "tool": "rsync", "args": ["-a"], "processes": 1})
        run["trials"].append({"case_id": "tiny-10k", "variant_id": "rsync-a", "iteration": 1, "elapsed_seconds": 0.8, "exit_codes": [0], "status": "ok", "commands": [["rsync"]], "validation": {"ok": True}, "logs": []})
        run["summaries"].append({"series_id": "2" * 64, "case_id": "tiny-10k", "variant_id": "rsync-a", "unit": "seconds", "median": 0.8, "minimum": 0.8, "maximum": 0.8, "stdev": 0.0, "samples": [0.8], "files_per_second": 12500.0})
        source = self.root / "results.json"
        self.write(source, run)
        process, output = self.render(source)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("repetitions", process.stderr)
        self.assertFalse((output / "index.html").exists())

    def test_unknown_revision_dirty_state_is_accepted_and_shown(self):
        run = sample_run()
        run["revision"] = {"commit": None, "branch": None, "dirty": None}
        source = self.root / "results.json"
        self.write(source, run)
        process, output = self.render(source)
        self.assertEqual(process.returncode, 0, process.stderr)
        dashboard = self.dashboard_state(output)
        self.assertIn("working tree unknown", dashboard["table"])

    def test_shared_parser_rejects_ambiguous_and_nonfinite_json(self):
        raw = json.dumps(sample_run())
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            report.parse_result(raw.replace('"status": "complete"', '"status": "failed", "status": "complete"'))
        with self.assertRaisesRegex(ValueError, "invalid JSON number"):
            report.parse_result(raw.replace('"dirty": false', '"dirty": false, "extra": Infinity'))

    def test_shared_parser_rejects_exponent_overflow_in_extra_metadata(self):
        raw = json.dumps(sample_run()).replace('"dirty": false', '"dirty": false, "extra": 1e999')
        with self.assertRaisesRegex(ValueError, "nonfinite"):
            report.parse_result(raw)
        source = self.root / "results.json"
        source.write_text(raw)
        process, output = self.render(source)
        self.assertNotEqual(process.returncode, 0)
        self.assertFalse((output / "index.html").exists())


if __name__ == "__main__":
    unittest.main()
