"""Behavior checks for publishing maintained reports beside benchmark history."""

from html.parser import HTMLParser
import hashlib
import json
from pathlib import Path
import shutil
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

    def build(self, *flags):
        return subprocess.run(
            [sys.executable, str(Path(__file__).with_name('report_site.py')),
             '--root', str(self.repo), '--output', str(self.output), *flags],
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
        result = self.build('--with-benchmark-history')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.output / 'index.html').read_text(), 'existing dashboard')
        self.assertEqual((self.output / 'history.json').read_text(), '{"runs": []}')

    def test_history_navigation_does_not_depend_on_dashboard_metadata(self):
        from benchmarks.report import render
        from benchmarks.test_report import sample_run
        run = sample_run()
        run['context']['runner_label'] = 'rcp-report-index'
        source = self.root / 'results.json'
        source.write_text(json.dumps(run))
        render(source, self.output, 'reports/index.html')
        dashboard = (self.output / 'index.html').read_bytes()
        result = self.build('--with-benchmark-history')
        self.assertEqual(result.returncode, 0, result.stderr)
        page = Elements((self.output / 'reports/performance/index.html').read_text())
        self.assertIn('../../index.html', [attrs.get('href') for tag, attrs in page.tags if tag == 'a'])
        self.assertEqual((self.output / 'index.html').read_bytes(), dashboard)

    def test_standalone_rebuild_keeps_history_links_pointing_to_documentation(self):
        for _ in range(2):
            result = self.build()
            self.assertEqual(result.returncode, 0, result.stderr)
            page = Elements((self.output / 'reports/performance/index.html').read_text())
            self.assertIn('https://github.com/wykurz/rcp/blob/main/docs/benchmarking.md#view-results',
                          [attrs.get('href') for tag, attrs in page.tags if tag == 'a'])

    def test_combined_build_requires_an_existing_dashboard(self):
        result = self.build('--with-benchmark-history')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Missing benchmark dashboard', result.stderr)
        self.assertFalse(self.output.exists())

    def test_theme_choice_survives_navigation_and_blocked_storage(self):
        if not shutil.which('node'):
            self.skipTest('Node is unavailable for theme execution')
        from benchmarks.report import render
        from benchmarks.test_report import sample_run
        source = self.root / 'results.json'
        source.write_text(json.dumps(sample_run()))
        render(source, self.output, 'reports/index.html')
        result = self.build('--with-benchmark-history')
        self.assertEqual(result.returncode, 0, result.stderr)
        pages = [self.output / path for path in ('index.html', 'changes.html', 'reports/index.html',
                                                'reports/performance/index.html')]
        for page in pages:
            controls = [attrs for tag, attrs in Elements(page.read_text()).tags
                        if tag == 'button' and 'data-theme-toggle' in attrs]
            self.assertEqual(len(controls), 1, page)
            self.assertEqual(controls[0]['type'], 'button')
            self.assertIn('hidden', controls[0])
        script = r'''
const fs = require('fs'), vm = require('vm'), assert = require('node:assert/strict');
const pages = process.argv.slice(1).map(path => fs.readFileSync(path, 'utf8'));
const saved = new Map();
const storage = {getItem: key => saved.get(key), setItem: (key, value) => saved.set(key, value)};
function open(html, localStorage) {
  const code = html.match(/<script id="site-theme">([\s\S]*?)<\/script>/)[1];
  const button = {hidden: true, attributes: {}, textContent: '',
    setAttribute(key, value) { this.attributes[key] = value; },
    addEventListener(event, handler) { assert.equal(event, 'click'); this.click = handler; }};
  const document = {documentElement: {dataset: {}},
    querySelector: selector => { assert.equal(selector, '[data-theme-toggle]'); return button; },
    addEventListener(event, handler) { assert.equal(event, 'DOMContentLoaded'); this.ready = handler; }};
  const window = {addEventListener(event, handler) { assert.equal(event, 'pageshow'); this.restore = handler; }};
  vm.runInNewContext(code, {document, window, localStorage});
  const beforeReady = document.documentElement.dataset.theme;
  document.ready();
  assert.equal(button.hidden, false);
  return {button, beforeReady, theme: () => document.documentElement.dataset.theme,
    back: () => window.restore?.({persisted: true})};
}
const first = open(pages[0], storage);
assert.equal(first.beforeReady, 'dark');
assert.equal(first.button.attributes['aria-label'], 'Switch to light theme');
first.button.click();
assert.equal(first.theme(), 'light');
assert.equal(first.button.attributes['aria-label'], 'Switch to dark theme');
for (const html of pages) {
  const page = open(html, storage);
  const cached = open(html, storage);
  assert.equal(page.beforeReady, 'light');
  page.button.click();
  assert.equal(page.theme(), 'dark');
  cached.back();
  assert.equal(cached.theme(), 'dark');
  assert.equal(cached.button.attributes['aria-label'], 'Switch to light theme');
  assert.equal(open(html, storage).beforeReady, 'dark');
  page.button.click();
}
for (const value of ['invalid', 'system', '']) {
  for (const key of saved.keys()) saved.set(key, value);
  assert.equal(open(pages[0], storage).beforeReady, 'dark');
}
for (const localStorage of [undefined, {
  getItem() { throw new Error('storage denied'); },
  setItem() { throw new Error('storage denied'); }
}, {getItem() { return 'light'; }, setItem() { throw new Error('quota exceeded'); }}]) {
  for (const html of pages) {
    const page = open(html, localStorage);
    page.button.click();
    assert.notEqual(page.theme(), page.beforeReady);
  }
}
'''
        result = subprocess.run(['node', '-e', script, *map(str, pages)], text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

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
