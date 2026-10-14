"""Regression tests for auditing and rendering the preserved measurement packet."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from xml.etree import ElementTree

import analyze


ANALYSIS_DIR = Path(__file__).resolve().parent


class AuditTests(unittest.TestCase):
    def test_timeout_resources_remain_observations_of_unfinished_attempts(self):
        _, censored, _ = analyze.audit_packet(ANALYSIS_DIR / 'raw')
        outcomes = {row['variant']: row for row in censored}
        self.assertEqual(outcomes['cp-1x'].get('measurements'), {
            'user_cpu_seconds': 16.92, 'system_cpu_seconds': 464.41, 'max_rss_kib': 3104,
        })
        self.assertEqual(outcomes['rsync-1x'].get('measurements'), {
            'user_cpu_seconds': None, 'system_cpu_seconds': None, 'max_rss_kib': None,
        })
        self.assertTrue(all(row['outcome'] == 'timed_out' and row['n'] == 1 for row in censored))

    def test_rejects_optimized_python_before_verifying_or_writing(self):
        for flags, optimize in [(["-O"], None), ([], "1"), (["-S", "-OO"], None)]:
            with self.subTest(flags=flags, optimize=optimize), tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary) / "analysis"
                figures = Path(temporary) / "figures"
                environment = os.environ.copy()
                environment.pop("PYTHONOPTIMIZE", None)
                if optimize is not None:
                    environment["PYTHONOPTIMIZE"] = optimize
                result = subprocess.run(
                    [sys.executable, *flags, str(ANALYSIS_DIR / "analyze.py"),
                     "--output", str(output), "--figures-output", str(figures)],
                    env=environment, capture_output=True, text=True, check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("assertions", result.stderr)
                self.assertEqual(result.stdout, "")
                self.assertFalse(output.exists())
                self.assertFalse(figures.exists())


class FigureTests(unittest.TestCase):
    def test_default_rendering_preserves_measurements_and_only_writes_report_figures(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "analysis"
            figures = Path(temporary) / "figures"
            result = subprocess.run(
                [sys.executable, str(ANALYSIS_DIR / "analyze.py"),
                 "--output", str(output), "--figures-output", str(figures)],
                capture_output=True, text=True, check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads((output / "analysis.json").read_text())
            recorded = json.loads((ANALYSIS_DIR / "analysis.json").read_text())
            for field in ["audit", "coverage", "superseded_equivalent_samples", "censored", "results", "comparisons"]:
                self.assertEqual(payload[field], recorded[field], field)
            self.assertEqual({path.name for path in figures.iterdir()}, {
                "local-overview-local-copy.svg", "local-overview-network-copy.svg",
                "parallelism-local-lustre-copy.svg", "parallelism-local-weka-copy.svg",
                "parallelism-local-nfs-copy.svg", "parallelism-local-xfs-copy.svg",
                "parallelism-local-zfs-copy.svg", "parallelism-remote-xfs-copy.svg",
                "linux-source.svg", "default-vs-best-tested.svg",
            })

    def test_default_heatmap_renders_supplied_comparison_values_in_workload_order(self):
        comparisons = json.loads((ANALYSIS_DIR / "analysis.json").read_text())["comparisons"]
        for row in comparisons:
            if row["source_path"] == "local/lustre/10x100x1G.json":
                row["default_over_best_tested_rcp"]["copy_seconds"] = {"ratio": 2.25, "best_variant": "rcp-128"}
            elif row["source_path"] == "remote/10x10x10x10x1024x1K.json":
                row["default_over_best_tested_rcp"]["copy_seconds"] = {"ratio": 1.375, "best_variant": "rcp-default"}
        with tempfile.TemporaryDirectory() as temporary:
            analyze.default_ratios(list(reversed(comparisons)), Path(temporary))
            figure = ElementTree.parse(Path(temporary) / "default-vs-best-tested.svg")
            labels = [node.text for node in figure.iter("{http://www.w3.org/2000/svg}text")
                      if node.text is not None and node.text.startswith(("+", "best: "))]
            self.assertEqual(len(labels), 48)
            self.assertEqual(labels[:2], ["+125.0%", "best: 128"])
            self.assertEqual(labels[-2:], ["+37.5%", "best: default"])


if __name__ == "__main__":
    unittest.main()
