"""Check table coverage, accounting scopes, and synchronization with the analysis."""

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import tables


ANALYSIS_DIR = Path(__file__).resolve().parent
REPORTS_DIR = ANALYSIS_DIR.parents[1] / 'docs/reports'


class ResultsTableTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.payload = json.loads((ANALYSIS_DIR / 'analysis.json').read_text())

    def test_renders_selected_statistics_without_recomputing_samples(self):
        payload = copy.deepcopy(self.payload)
        variant = next(v for v in payload['results'][0]['variants'] if v['variant'] == 'rcp-default')
        variant['statistics']['copy_seconds'].update(mean=2, min=1, max=4)
        variant['statistics']['cpu_total_seconds'] = {'mean': 25, 'min': 21, 'max': 29}
        variant['statistics']['max_rss_kib'].update(mean=3072, min=1024, max=8192)
        rendered = tables.render_results(payload)
        self.assertIn('2.000 [1.000–4.000]', rendered)
        self.assertIn('25.000 [21.000–29.000]', rendered)
        self.assertIn('3.0 [1.0–8.0]', rendered)
        self.assertIn('25.000', tables.summary_blocks(payload)['network'])

    def test_covers_every_selected_configuration_in_stable_order(self):
        rendered = tables.render_results(self.payload)
        successful_rows = 0
        timeout_rows = 0
        for document in self.payload['results']:
            section = rendered.split(f"[Original measurements]({tables.raw_link(document)})", 1)[1]
            section = section.split('\n##', 1)[0]
            rows = [line for line in section.splitlines() if line.startswith('| ')][2:]
            successful = [row for row in rows if 'Timed out' not in row]
            self.assertCountEqual([row.split('|')[1].strip() for row in successful],
                                  [tables.recipe_label(v['variant']) for v in document['variants']])
            successful_rows += len(successful)
            timeout_rows += len(rows) - len(successful)
        self.assertEqual(successful_rows, 234)
        self.assertEqual(timeout_rows, 2)
        reversed_payload = copy.deepcopy(self.payload)
        reversed_payload['results'].reverse()
        for document in reversed_payload['results']:
            document['variants'].reverse()
        self.assertEqual(rendered, tables.render_results(reversed_payload))

    def test_report_summaries_only_include_baselines_with_recorded_means(self):
        blocks = tables.summary_blocks(self.payload)
        expected_rows = {'network': 24, 'local': 16, 'remote': 12, 'linux': 6}
        for name, expected in expected_rows.items():
            rows = [line for line in blocks[name].splitlines() if line.startswith('| ')][2:]
            data_rows = [row for row in rows if row.split('|')[1].strip() in
                         {'1 GB', '100 MB', '1 MB', '1 kB', 'Local', 'Remote'}]
            self.assertEqual(len(data_rows), expected, name)
            self.assertNotIn('| rcp 8 ', blocks[name])
        linux = blocks['linux']
        self.assertIn('2.484', linux)
        self.assertIn('160.1', linux)
        self.assertIn('9.9', linux)

    def test_timeouts_have_no_completion_mean_or_invented_resources(self):
        payload = copy.deepcopy(self.payload)
        payload['censored'][0]['measurements'] = {
            'user_cpu_seconds': 16.92, 'system_cpu_seconds': 464.41, 'max_rss_kib': 3104,
            'cpu_total_seconds': 481.33,
        }
        payload['censored'][1]['measurements'] = {
            'user_cpu_seconds': None, 'system_cpu_seconds': None, 'max_rss_kib': None,
            'cpu_total_seconds': None,
        }
        rendered = tables.render_results(payload)
        rows = [line for line in rendered.splitlines() if '**Timed out ≥43,200 s**' in line]
        self.assertEqual(len(rows), 2)
        cells = [[cell.strip() for cell in row.split('|')[1:-1]] for row in rows]
        self.assertIn(['cp', '**Timed out ≥43,200 s**', '481.330', '3.0'], cells)
        self.assertIn(['rsync', '**Timed out ≥43,200 s**', '—', '—'], cells)

    def test_default_dcp_alias_is_included_in_local_and_network_summaries(self):
        payload = copy.deepcopy(self.payload)
        for document in payload['results']:
            for variant in document['variants']:
                if variant['variant'] == 'dcp-32':
                    variant['variant'] = 'dcp-default'
        self.assertEqual(tables.summary_blocks(payload), tables.summary_blocks(self.payload))

    def test_unresolved_dcp_aliases_are_rejected_instead_of_publishing_duplicate_labels(self):
        payload = copy.deepcopy(self.payload)
        document = next(d for d in payload['results'] if d['source_path'] == 'local/zfs/10x100x1G.json')
        alias = copy.deepcopy(next(v for v in document['variants'] if v['variant'] == 'dcp-32'))
        alias['variant'] = 'dcp-default'
        document['variants'].append(alias)
        for render in (tables.summary_blocks, tables.render_results):
            with self.subTest(render=render.__name__), self.assertRaisesRegex(ValueError, 'Duplicate.*recipe'):
                render(payload)

    def test_role_statistics_keep_each_process_separate(self):
        variant = next(v for d in self.payload['results'] if d['scope'] == 'remote'
                       and d['fileset']['name'] == '10x100x1G'
                       for v in d['variants'] if v['variant'] == 'rcp-default')
        summaries = tables.role_statistics(variant)
        self.assertEqual(set(summaries), {'master', 'source', 'destination'})
        source_cpu = [s['rcp_runtime_stats']['roles']['source']['cpu_total_seconds']
                      for s in variant['samples']]
        self.assertAlmostEqual(summaries['source']['cpu_total_seconds']['mean'], sum(source_cpu) / 3)
        self.assertEqual(summaries['source']['cpu_total_seconds']['min'], min(source_cpu))
        self.assertFalse(summaries['source']['peak_rss_exact'])
        self.assertGreater(summaries['source']['cpu_total_seconds']['mean'], 500)
        self.assertLess(variant['statistics']['user_cpu_seconds']['mean'], 0.1)

    def test_missing_role_samples_are_rejected_instead_of_averaged_as_zero(self):
        variant = copy.deepcopy(next(v for d in self.payload['results'] if d['scope'] == 'remote'
                                     for v in d['variants'] if 'rcp_runtime_stats' in v['samples'][0]))
        del variant['samples'][1]['rcp_runtime_stats']['roles']['destination']
        with self.assertRaisesRegex(ValueError, 'role'):
            tables.role_statistics(variant)

    def test_check_reports_stale_tables_without_rewriting_documents(self):
        with tempfile.TemporaryDirectory() as temporary:
            reports = Path(temporary)
            source = (REPORTS_DIR / 'performance.md').read_text()
            for name in tables.SUMMARY_IDS:
                if tables.marker(name) not in source:
                    source += f'\n{tables.marker(name)}\n\n{tables.marker(name, end=True)}\n'
            (reports / 'performance.md').write_text(source)
            command = [sys.executable, str(ANALYSIS_DIR / 'tables.py'), '--reports-dir', str(reports)]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            result = subprocess.run(command + ['--check'], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            page = reports / 'performance-results.md'
            stale = page.read_text().replace('230.589', '999.000', 1)
            self.assertNotEqual(page.read_text(), stale)
            page.write_text(stale)
            result = subprocess.run(command + ['--check'], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('performance-results.md', result.stderr)
            self.assertEqual(page.read_text(), stale)

    def test_missing_summary_markers_fail_before_writing(self):
        with tempfile.TemporaryDirectory() as temporary:
            reports = Path(temporary)
            (reports / 'performance.md').write_text('# Custom report\n')
            with self.assertRaisesRegex(ValueError, 'marker'):
                tables.write_reports(self.payload, reports)
            self.assertFalse((reports / 'performance-results.md').exists())

    def test_check_accepts_formatter_changes_to_table_divider_widths(self):
        with tempfile.TemporaryDirectory() as temporary:
            reports = Path(temporary)
            (reports / 'performance.md').write_text((REPORTS_DIR / 'performance.md').read_text())
            tables.write_reports(self.payload, reports)
            page = reports / 'performance-results.md'
            original = page.read_text()
            formatted = original.replace('| --- | --- |', '| ----------------- | ------------------ |')
            self.assertNotEqual(original, formatted)
            page.write_text(formatted)
            tables.write_reports(self.payload, reports, check=True)
            self.assertEqual(page.read_text(), formatted)


if __name__ == '__main__':
    unittest.main()
