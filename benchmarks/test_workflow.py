import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml


class BenchmarkWorkflowTests(unittest.TestCase):
    def workflow(self):
        return yaml.safe_load((Path(__file__).resolve().parent.parent / ".depot/workflows/benchmarks.yml").read_text())

    def measure_arguments(self, *, event, mode="loopback", baseline="", case="all", cache="linux-drop-caches"):
        measure = next(step for step in self.workflow()["jobs"]["benchmark"]["steps"] if step.get("name") == "Measure copies")
        script = 'just() { printf "%s\\n" "$@"; }\n' + measure["run"]
        environment = {**os.environ, "BENCHMARK_EVENT": event, "BENCHMARK_MODE": mode, "SELECTED_CASE": case, "SELECTED_CACHE": cache, "RUNNER_TEMP": "/tmp", "RCP_BENCH_BASELINE_BIN": baseline}
        result = subprocess.run(["bash", "-c", script], env=environment, capture_output=True, text=True, check=True)
        arguments = result.stdout.splitlines()
        self.assertEqual(arguments[0], "benchmark-run")
        return arguments

    def values(self, arguments, option):
        return [arguments[index + 1] for index, item in enumerate(arguments) if item == option]

    def test_pr_smoke_checks_balance_each_mode_with_baseline(self):
        for mode, repetitions in (("local", "5"), ("loopback", "4")):
            with self.subTest(mode=mode):
                arguments = self.measure_arguments(event="pull_request", mode=mode, baseline="/tmp/base-binaries")
                self.assertEqual(self.values(arguments, "--repetitions"), [repetitions])
                self.assertEqual(self.values(arguments, "--baseline-bin-dir"), ["/tmp/base-binaries"])
                self.assertEqual(self.values(arguments, "--purpose"), ["smoke"])
                self.assertEqual(self.values(arguments, "--cache"), ["source-warm"])
                self.assertEqual(self.values(arguments, "--mode"), [mode])

    def test_main_push_is_only_a_smoke_check(self):
        arguments = self.measure_arguments(event="push")
        self.assertEqual(self.values(arguments, "--purpose"), ["smoke"])
        self.assertEqual(self.values(arguments, "--case"), ["tiny-10k", "medium-4k", "large-100"])
        self.assertNotIn("--baseline-bin-dir", arguments)

    def test_scheduled_performance_runs_balance_each_mode(self):
        for mode, repetitions in (("local", "4"), ("loopback", "3")):
            with self.subTest(mode=mode):
                arguments = self.measure_arguments(event="schedule", mode=mode)
                self.assertEqual(self.values(arguments, "--repetitions"), [repetitions])
                self.assertEqual(self.values(arguments, "--purpose"), ["performance"])
                self.assertEqual(self.values(arguments, "--cache"), ["linux-drop-caches"])
                self.assertEqual(self.values(arguments, "--case"), ["tiny-1m", "medium-128k", "large-120"])
                self.assertEqual(self.values(arguments, "--timeout"), ["1200"])

    def test_manual_performance_run_honors_case_and_cache(self):
        arguments = self.measure_arguments(event="workflow_dispatch", mode="local", case="large-120", cache="source-warm")
        self.assertEqual(self.values(arguments, "--case"), ["large-120"])
        self.assertEqual(self.values(arguments, "--cache"), ["source-warm"])
        self.assertEqual(self.values(arguments, "--purpose"), ["performance"])

    def test_history_attempts_both_modes_and_dispatches_after_partial_failure(self):
        history = self.workflow()["jobs"]["history"]
        publish = next(step for step in history["steps"] if step.get("id") == "publish")
        dispatch = next(step for step in history["steps"] if step.get("name") == "Request dashboard update")
        self.assertEqual(dispatch["if"], "always() && steps.publish.outputs.published == 'true'")
        for failed_mode in ("local", "loopback"):
            with self.subTest(failed_mode=failed_mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for mode in ("local", "loopback"):
                    result = root / "downloaded-results" / f"benchmark-results-{mode}" / "results.json"
                    result.parent.mkdir(parents=True)
                    result.write_text("{}")
                script = 'python3() { printf "%s\\n" "$*" >> "$ATTEMPTS"; [[ "$*" != *"benchmark-results-$FAILED_MODE/"* ]]; }\n' + publish["run"]
                environment = {**os.environ, "GITHUB_OUTPUT": str(root / "output"), "GITHUB_REPOSITORY": "test/repo", "ATTEMPTS": str(root / "attempts"), "FAILED_MODE": failed_mode}
                result = subprocess.run(["bash", "-c", script], cwd=root, env=environment, capture_output=True, text=True)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertEqual(len((root / "attempts").read_text().splitlines()), 2)
                self.assertEqual((root / "output").read_text(), "published=true\n")

    def test_each_mode_keeps_a_distinct_artifact(self):
        job = self.workflow()["jobs"]["benchmark"]
        self.assertEqual(job["strategy"]["matrix"]["mode"], ["local", "loopback"])
        upload = next(step for step in job["steps"] if step.get("name") == "Upload measurements and logs")
        self.assertEqual(upload["with"]["name"], "benchmark-results-${{ matrix.mode }}")
        history = self.workflow()["jobs"]["history"]
        self.assertNotIn("'push'", history["if"])
        download = next(step for step in history["steps"] if step.get("id") == "measurements")
        self.assertEqual(download["with"]["pattern"], "benchmark-results-*")
        self.assertNotIn("merge-multiple", download["with"])


if __name__ == "__main__":
    unittest.main()
