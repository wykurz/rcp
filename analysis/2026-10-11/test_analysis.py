"""Regression tests for auditing and rendering the preserved measurement packet."""

import copy
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile
import unittest
from xml.etree import ElementTree

import analyze


ANALYSIS_DIR = Path(__file__).resolve().parent


class AuditTests(unittest.TestCase):
    @contextmanager
    def changed_timeout_packet(self, change):
        with tempfile.TemporaryDirectory() as temporary:
            packet = Path(temporary) / 'raw'
            shutil.copytree(ANALYSIS_DIR / 'raw', packet)
            relative = 'local/nfs/10x10x10x10x1024x1K.json'
            path = packet / relative
            document = json.loads(path.read_text())
            change(document['right_censored_outcomes'])
            path.write_text(json.dumps(document))
            manifest = packet / 'SHA256SUMS'
            lines = manifest.read_text().splitlines()
            manifest.write_text('\n'.join(
                f'{analyze.digest(path.read_bytes())}  {relative}' if line.split()[-1] == relative else line
                for line in lines) + '\n')
            yield packet

    def test_timeout_logs_are_verified_even_when_the_outer_packet_is_resealed(self):
        for index in (0, 1):
            for key in ('command_output', 'gnu_time_output'):
                with self.subTest(index=index, key=key):
                    def change(outcomes):
                        outcomes[index][key]['text'] += 'modified log'
                    with self.changed_timeout_packet(change) as packet:
                        with self.assertRaisesRegex((AssertionError, ValueError), key):
                            analyze.audit_packet(packet)

    def test_timeout_records_must_support_the_documented_twelve_hour_lower_bound(self):
        for field, value in [('copy_seconds', 43199), ('copy_seconds', float('nan')),
                             ('copy_seconds', float('inf')), ('status', 'ok'), ('exit_code', 0),
                             ('failure_phase', 'validation'), ('variant', 'rcp-default'),
                             ('iteration', 2)]:
            with self.subTest(field=field):
                def change(outcomes):
                    outcomes[0]['record'][field] = value
                with self.changed_timeout_packet(change) as packet:
                    with self.assertRaisesRegex((AssertionError, ValueError), 'timeout'):
                        analyze.audit_packet(packet)

    def test_counts_logs_from_both_successful_and_unfinished_attempts(self):
        _, _, audit = analyze.audit_packet(ANALYSIS_DIR / 'raw')
        self.assertEqual(audit['embedded_log_hashes_verified'], 1528)

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


class SelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        results, _, _ = analyze.audit_packet(ANALYSIS_DIR / 'raw')
        cls.document = next(d for d in results if d['source_path'] == 'local/zfs/10x100x1G.json')

    def test_interleaved_repetitions_retain_one_complete_latest_trial_set(self):
        document = copy.deepcopy(self.document)
        variants = analyze.variants(document)
        for name, seconds in [('dcp-default', (1, 4, 5)), ('dcp-32', (2, 3, 6))]:
            for sample, second in zip(variants[name]['samples'], seconds):
                sample['recorded_utc'] = f'2026-10-11T00:00:0{second}+00:00'
        selected, superseded = analyze.select_latest_equivalent([document])
        retained = analyze.variants(selected[0])['dcp-32']
        self.assertEqual(retained['samples'], variants['dcp-32']['samples'])
        self.assertEqual(retained['statistics'], variants['dcp-32']['statistics'])
        self.assertEqual([row['kept']['source_variant'] for row in superseded], ['dcp-32'] * 3)

    def test_latest_alias_wins_as_a_set_even_when_its_mean_is_slower(self):
        document = copy.deepcopy(self.document)
        variants = analyze.variants(document)
        for sample in variants['dcp-default']['samples']:
            sample['recorded_utc'] = f"2099-01-01T00:00:0{sample['iteration']}+00:00"
        selected, _ = analyze.select_latest_equivalent([document])
        retained = analyze.variants(selected[0])['dcp-32']
        self.assertGreater(variants['dcp-default']['statistics']['copy_seconds']['mean'],
                           variants['dcp-32']['statistics']['copy_seconds']['mean'])
        self.assertEqual(retained['samples'], variants['dcp-default']['samples'])
        self.assertEqual(retained['statistics'], variants['dcp-default']['statistics'])

    def test_equally_recent_trial_sets_are_rejected_as_ambiguous(self):
        document = copy.deepcopy(self.document)
        variants = analyze.variants(document)
        for variant in (variants['dcp-default'], variants['dcp-32']):
            for sample in variant['samples']:
                sample['recorded_utc'] = f"2026-10-11T00:00:0{sample['iteration']}+00:00"
        with self.assertRaisesRegex(ValueError, 'Ambiguous'):
            analyze.select_latest_equivalent([document])


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
