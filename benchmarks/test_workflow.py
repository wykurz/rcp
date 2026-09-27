import os
import subprocess
import unittest
from pathlib import Path

import yaml


class BenchmarkWorkflowTests(unittest.TestCase):
    def measure_arguments(self, *, event, baseline=""):
        workflow = yaml.safe_load((Path(__file__).resolve().parent.parent / ".depot/workflows/benchmarks.yml").read_text())
        measure = next(step for step in workflow["jobs"]["benchmark"]["steps"] if step.get("name") == "Measure copies")
        script = 'just() { printf "%s\\n" "$@"; }\n' + measure["run"]
        environment = {**os.environ, "BENCHMARK_EVENT": event, "SELECTED_CASE": "", "SELECTED_CACHE": "source-warm", "RUNNER_TEMP": "/tmp", "RCP_BENCH_BASELINE_BIN": baseline}
        result = subprocess.run(["bash", "-c", script], env=environment, capture_output=True, text=True, check=True)
        arguments = result.stdout.splitlines()
        self.assertEqual(arguments[0], "benchmark-run")
        return arguments

    def test_pr_baseline_uses_four_repetitions(self):
        arguments = self.measure_arguments(event="pull_request", baseline="/tmp/base-binaries")
        self.assertEqual(arguments[arguments.index("--repetitions") + 1], "4")
        self.assertEqual(arguments[arguments.index("--baseline-bin-dir") + 1], "/tmp/base-binaries")

    def test_run_without_baseline_uses_three_repetitions(self):
        arguments = self.measure_arguments(event="push")
        self.assertEqual(arguments[arguments.index("--repetitions") + 1], "3")
        self.assertNotIn("--baseline-bin-dir", arguments)


if __name__ == "__main__":
    unittest.main()
