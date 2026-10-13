"""Behavior checks for publishing maintained reports beside benchmark history."""

from html.parser import HTMLParser
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class Elements(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.tags = []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))


class ReportSiteTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / 'repo'
        self.output = self.root / 'project-site'
        self.docs = self.repo / 'docs/reports'
        self.docs.mkdir(parents=True)
        (self.repo / 'README.md').write_text('# Usage\n')
        (self.repo / 'assets').mkdir()
        (self.repo / 'assets/logo.svg').write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')
        (self.docs / 'figures').mkdir()
        (self.docs / 'figures/copy.svg').write_text('<svg xmlns="http://www.w3.org/2000/svg"><title>Copy times</title></svg>')
        (self.docs / 'README.md').write_text('# Reports\n\n[Performance](performance.md)\n')
        (self.docs / 'performance.md').write_text(
            '# Copy performance\n\n## Local copies\n\n'
            '[Usage](../../README.md#usage)\n\n![Copy times](figures/copy.svg)\n\n'
            '| Tool | Seconds |\n| --- | ---: |\n| rcp | 300 |\n\n'
            '## Local copies\n\n[Reports](README.md)\n')

    def build(self):
        return subprocess.run(
            [sys.executable, str(Path(__file__).with_name('report_site.py')),
             '--root', str(self.repo), '--output', str(self.output)],
            text=True, capture_output=True,
        )

    def test_report_links_and_assets_work_under_a_project_prefix(self):
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stderr)
        page = self.output / 'reports/performance/index.html'
        elements = Elements(page.read_text())
        images = [attrs['src'] for tag, attrs in elements.tags if tag == 'img']
        self.assertIn('../figures/copy.svg', images)
        for image in images:
            self.assertFalse(image.startswith('/'))
            self.assertTrue((page.parent / image).is_file(), image)
        links = [attrs['href'] for tag, attrs in elements.tags if tag == 'a']
        self.assertIn('https://github.com/wykurz/rcp/blob/main/README.md#usage', links)
        self.assertIn('../index.html', links)
        self.assertIn('../figures/copy.svg', links)
        index = Elements((self.output / 'reports/index.html').read_text())
        self.assertIn('performance/index.html', [attrs.get('href') for tag, attrs in index.tags if tag == 'a'])
        self.assertEqual((self.output / 'reports/figures/copy.svg').read_bytes(), (self.docs / 'figures/copy.svg').read_bytes())
        self.assertFalse(list(self.output.rglob('*.png')))

    def test_heading_navigation_has_unique_working_targets(self):
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stderr)
        elements = Elements((self.output / 'reports/performance/index.html').read_text())
        ids = [attrs['id'] for _, attrs in elements.tags if 'id' in attrs]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertIn('local-copies', ids)
        self.assertIn('local-copies-1', ids)
        anchors = [attrs['href'][1:] for tag, attrs in elements.tags if tag == 'a' and attrs.get('href', '').startswith('#')]
        self.assertIn('local-copies', anchors)
        self.assertIn('local-copies-1', anchors)
        self.assertTrue(set(anchors) <= set(ids))

    def test_preserves_existing_benchmark_dashboard(self):
        self.output.mkdir()
        (self.output / 'index.html').write_text('existing dashboard')
        (self.output / 'history.json').write_text('{"runs": []}')
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.output / 'index.html').read_text(), 'existing dashboard')
        self.assertEqual((self.output / 'history.json').read_text(), '{"runs": []}')

    def test_missing_figure_fails_build(self):
        (self.docs / 'figures/copy.svg').unlink()
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Missing report reference', result.stderr)
        self.assertFalse((self.output / 'reports/performance/index.html').exists())

    def test_missing_repository_link_fails_build(self):
        (self.repo / 'README.md').unlink()
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Missing report reference', result.stderr)

    def test_renamed_repository_heading_fails_build(self):
        (self.repo / 'README.md').write_text('# Getting started\n')
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Missing repository section', result.stderr)
        self.assertIn('../../README.md#usage', result.stderr)
        self.assertFalse((self.output / 'reports/performance/index.html').exists())

    def test_repository_section_links_support_formatting_and_duplicate_headings(self):
        (self.repo / 'README.md').write_text(
            '# Usage\n\n## Local copy acceleration (`--reflink`)\n\n'
            '## Local copy acceleration (`--reflink`)\n')
        with (self.docs / 'performance.md').open('a') as document:
            document.write('\n[Reflink](../../README.md#local-copy-acceleration---reflink-1)\n')
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_rejects_fragment_only_images(self):
        with (self.docs / 'performance.md').open('a') as document:
            document.write('\n\n![Missing](#missing-figure)\n')
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Report images must be local SVG files', result.stderr)

    def test_rejects_missing_report_section_links(self):
        for reference in ('#missing', 'performance.md#missing'):
            with self.subTest(reference=reference):
                (self.docs / 'performance.md').write_text(f'# Performance\n\n[Missing]({reference})\n')
                result = self.build()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('Missing report section', result.stderr)

    def test_accepts_existing_local_and_cross_report_sections(self):
        with (self.docs / 'performance.md').open('a') as document:
            document.write('\n\n[Here](#local-copies-1)\n\n[Index](README.md#reports)\n')
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_preserved_measurement_packet_matches_original_checksums(self):
        evidence = Path(__file__).resolve().parent.parent / 'analysis/2026-10-11'
        origin = json.loads((evidence / 'packet-origin.json').read_text())
        packet = evidence / 'raw'
        manifest = (packet / 'SHA256SUMS').read_bytes()
        self.assertEqual(hashlib.sha256(manifest).hexdigest(), origin['packet_checksums_sha256'])
        expected_files = {'SHA256SUMS'}
        for line in manifest.decode().splitlines():
            expected, name = line.split(None, 1)
            name = name.lstrip('*')
            self.assertNotIn(name, expected_files)
            expected_files.add(name)
            self.assertEqual(hashlib.sha256((packet / name).read_bytes()).hexdigest(), expected, name)
        actual_files = {path.relative_to(packet).as_posix() for path in packet.rglob('*') if path.is_file()}
        self.assertEqual(actual_files, expected_files)
        self.assertEqual(len(actual_files), origin['files'])


if __name__ == '__main__':
    unittest.main()
