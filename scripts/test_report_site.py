"""Behavior checks for publishing maintained reports beside benchmark history."""

from html.parser import HTMLParser
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from urllib.parse import unquote, urlsplit


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
        (self.repo / 'README.md').write_text(
            '# Usage\n\n## Examples\n\n## Documentation\n\n## Installation\n\n'
            '## Copy semantics\n\n## Permissions and ownership (`rchm`)\n')
        (self.repo / 'assets').mkdir()
        (self.repo / 'assets/logo.svg').write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')
        (self.docs / 'figures').mkdir()
        (self.docs / 'figures/copy.svg').write_text('<svg xmlns="http://www.w3.org/2000/svg"><title>Copy times</title></svg>')
        (self.docs / 'README.md').write_text('# Reports\n\n[Performance](performance.md)\n')
        (self.docs / 'performance-results.md').write_text('# Complete results\n')
        (self.docs / 'performance.md').write_text(
            '# Copy performance\n\n## Local copies\n\n'
            '[Usage](../../README.md#usage)\n\n![Copy times](figures/copy.svg)\n\n'
            '| Tool | Seconds |\n| --- | ---: |\n| rcp | 300 |\n\n'
            '## Local copies\n\n[Reports](README.md)\n'
            '## Workloads\n\n'
            '| File size | Regular files | Total file bytes |\n| --- | --- | --- |\n'
            '| 1 MB | 1,024,000 | 1.024 TB |\n\n'
            '#### Local Lustre summary\n\n'
            '| File size | Recipe | Wall time (s) |\n| --- | --- | --- |\n'
            '| 1 MB | rcp auto | 519.931 |\n| 1 MB | dcp · 32 ranks | 5,966.359 |\n\n'
            '#### Local Weka summary\n\n'
            '| File size | Recipe | Wall time (s) |\n| --- | --- | --- |\n'
            '| 1 MB | rcp auto | 364.740 |\n| 1 MB | dcp · 32 ranks | 3,848.908 |\n\n'
            '## 5 Linux source tree\n\n'
            '| Scope | Recipe | Wall time (s) |\n| --- | --- | --- |\n'
            '| Local | rcp auto | 2.484 |\n| Local | cp | 2.497 |\n')

    def build(self, *flags, env=None):
        return subprocess.run(
            [sys.executable, '-B', str(Path(__file__).with_name('report_site.py')),
             '--root', str(self.repo), '--output', str(self.output), *flags],
            text=True, capture_output=True, env=env,
        )

    def aws_record(self, run='123', *, passed=True):
        from infra.aws_benchmark import production_export as public
        value = dict(schema=public.SCHEMA, run_key='repo-655073755-run-' + run,
            retirement_sha256='1' * 64, benchmark_valid=passed, cleanup_verified=True,
            validation=public.VALIDATION, series_contract=public.results.SERIES_CONTRACT,
            identity=dict(run=run + '-1', run_url=f'https://github.com/wykurz/rcp/actions/runs/{run}/attempts/1',
                suite='bulk-1g-metadata', baseline='a' * 40, candidate='a' * 40, controller='b' * 40),
            comparison='A/A qualification', measurements=[], note='Two local pairs do not establish broad variance.')
        if passed:
            value.update(timestamp='2026-10-22T19:00:00Z', verification={'policy': 'metadata-size'},
                measurements=[dict(case_id='bulk', variant_id='candidate', series_id='aws-local-ext4-v1',
                    median=620.5, samples=[620, 621])])
        return value

    def aws_history(self, records):
        repository = self.root / 'aws-history.git'
        environment = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM='1',
            GIT_AUTHOR_NAME='Offline test', GIT_AUTHOR_EMAIL='test@example.invalid',
            GIT_COMMITTER_NAME='Offline test', GIT_COMMITTER_EMAIL='test@example.invalid')
        def git(*args, source=None):
            result = subprocess.run(['git', '--git-dir=' + str(repository), *args], input=source,
                text=True, capture_output=True, env=environment, check=True)
            return result.stdout.strip()
        git('init', '--bare')
        if records is None:
            return repository
        entries = []
        for name, (mode, value) in records.items():
            raw = value if isinstance(value, str) else json.dumps(value)
            blob = git('hash-object', '-w', '--stdin', source=raw)
            entries.append(f'{mode} blob {blob}\t{name}\n')
        tree = git('mktree', source=''.join(entries))
        outer = git('mktree', source=f'040000 tree {tree}\trecords\n')
        commit = git('commit-tree', outer, '-m', 'Offline public history')
        git('update-ref', 'refs/heads/aws-benchmark-history', commit)
        return repository

    def retained_batch(self, records=(), **updates):
        from infra.aws_benchmark.production_publish import inventory
        directory = Path(tempfile.mkdtemp(prefix='retained-', dir=self.root))
        entries = []
        for value in records:
            raw = json.dumps(value, separators=(',', ':')).encode()
            (directory / (value['run_key'] + '.json')).write_bytes(raw)
            entries.append(dict(run_key=value['run_key'], sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw)))
        index = dict(schema='rcp-production-retained-reports-v1', repository_id='655073755',
            reader_revision='c' * 40, observed_at='2026-11-07T12:00:00+00:00', retention_days=90,
            discovery_complete=True, processing_complete=True, active=dict(state='idle', retired=False),
            observations=[dict(state='ready', retired=True, run_key=item['run_key']) for item in entries], records=entries)
        index.update(updates)
        (directory / 'index.json').write_text(json.dumps(index))
        return directory, index['reader_revision'], inventory(directory)

    def build_retained(self, batch, *flags, **kwargs):
        directory, revision, digest = batch
        return self.build('--aws-records', str(directory), '--aws-reader-revision', revision,
                          '--aws-inventory-sha256', digest, *flags, **kwargs)

    def assert_site_links_resolve(self):
        for page in self.output.rglob('*.html'):
            for tag, attrs in Elements(page.read_text()).tags:
                value = attrs.get('href') if tag in {'a', 'link'} else attrs.get('src')
                if not value:
                    continue
                url = urlsplit(value)
                if url.scheme or url.netloc:
                    continue
                self.assertFalse(url.path.startswith('/'), value)
                target = (page.parent / unquote(url.path)).resolve() if url.path else page
                if target.is_dir():
                    target /= 'index.html'
                self.assertTrue(target.is_file(), (page, value))
                if url.fragment and target.suffix == '.html':
                    self.assertIn(unquote(url.fragment), [a.get('id') for _, a in Elements(target.read_text()).tags])

    def test_retained_directory_cli_uses_no_git_and_preserves_frozen_json_and_links(self):
        value = self.aws_record()
        value.update(schema='rcp-production-public-report-v3', qualification_outcome='passed',
                     collected_at='2026-11-06T18:00:00+00:00', timestamp='1999-01-01T00:00:00Z')
        batch = self.retained_batch([value])
        result = self.build_retained(batch, env=dict(os.environ, PATH=''))
        self.assertEqual(result.returncode, 0, result.stderr)
        index = (self.output / 'aws-benchmarks/index.html').read_text()
        self.assertIn('Still-retained records under the 90-day AWS lifecycle; deletion timing can vary.', index)
        self.assertNotIn('Rolling 90-day', index)
        self.assertIn('not permanent history', index)
        self.assertIn('Reader revision', index)
        self.assertIn(batch[2], index)
        self.assertNotIn('History commit', index)
        self.assertIn('2026-11-06T18:00:00+00:00', index)
        self.assertNotIn('1999-01-01', index)
        self.assertIn('not independently attested', index)
        self.assertIn('does not prove content integrity', index)
        self.assertIn('href="aws-benchmarks/index.html">AWS benchmarks', (self.output / 'index.html').read_text())
        copied = self.output / 'aws-benchmarks/records' / (value['run_key'] + '.json')
        self.assertEqual(copied.read_bytes(), (batch[0] / copied.name).read_bytes())
        detail = (self.output / 'aws-benchmarks/runs' / (value['run_key'] + '.html')).read_text()
        self.assertIn('Collected at', detail)
        self.assertIn(value['collected_at'], detail)
        self.assertNotIn('1999-01-01', detail)
        self.assert_site_links_resolve()

    def test_complete_empty_retained_snapshot_is_visible_and_honest(self):
        result = self.build_retained(self.retained_batch())
        self.assertEqual(result.returncode, 0, result.stderr)
        page = (self.output / 'aws-benchmarks/index.html').read_text()
        self.assertIn('Discovery and processing completed; no retained public records were found.', page)
        self.assertIn('Discovery: complete. Record processing: complete.', page)
        self.assertIn('<td>idle</td>', page)
        self.assertNotIn('History commit', page)
        self.assertIn('AWS benchmarks', (self.output / 'reports/index.html').read_text())
        self.assert_site_links_resolve()

    def test_held_pending_and_deferred_snapshot_is_not_empty_success_and_escapes_data(self):
        active = dict(state='pending', retired=False, run_key='repo-655073755-run-123',
                      phase='<script>phase</script>', reason='Waiting & watching "lease"')
        observations = [dict(state='held', retired=False, run_key='repo-655073755-run-124',
                             reason='<img src=x onerror=alert(1)>'),
                        dict(state='deferred', retired=False, run_key='repo-655073755-run-125',
                             reason='Remaining records need a later snapshot')]
        result = self.build_retained(self.retained_batch(active=active, observations=observations, processing_complete=False))
        self.assertEqual(result.returncode, 0, result.stderr)
        page = (self.output / 'aws-benchmarks/index.html').read_text()
        self.assertIn('No public records are available to display', page)
        self.assertNotIn('no retained public records were found', page)
        self.assertIn('Record processing: incomplete', page)
        self.assertIn('not a fresh physical teardown check', page)
        for state in ('pending', 'held', 'deferred'):
            self.assertIn('<td>' + state + '</td>', page)
        self.assertIn('&lt;script&gt;phase&lt;/script&gt;', page)
        self.assertIn('&lt;img src=x onerror=alert(1)&gt;', page)
        self.assertIn('Waiting &amp; watching &quot;lease&quot;', page)
        self.assertNotIn('<script>phase', page)
        self.assertFalse(any('onerror' in attrs for _, attrs in Elements(page).tags))
        self.assert_site_links_resolve()

    def test_retained_directory_requires_all_proofs_and_rejects_legacy_ambiguity(self):
        batch = self.retained_batch()
        choices = [('--aws-records', str(batch[0])), ('--aws-reader-revision', batch[1]),
                   ('--aws-inventory-sha256', batch[2])]
        for omitted in range(3):
            flags = [item for index, pair in enumerate(choices) if index != omitted for item in pair]
            with self.subTest(omitted=omitted):
                result = self.build(*flags)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('required together', result.stderr)
                self.assertFalse(self.output.exists())
        result = self.build_retained(batch, '--aws-history', str(self.root / 'absent.git'))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('not both', result.stderr)
        self.assertFalse(self.output.exists())

    def test_retained_directory_rejects_proof_or_public_record_mismatch_before_output(self):
        from infra.aws_benchmark.production_publish import inventory
        batch = self.retained_batch([self.aws_record()])
        for revision, digest in (('d' * 40, batch[2]), (batch[1], '0' * 64), ('invalid', batch[2]), (batch[1], 'invalid')):
            with self.subTest(revision=revision, digest=digest):
                result = self.build_retained((batch[0], revision, digest))
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.output.exists())
        path = batch[0] / (self.aws_record()['run_key'] + '.json')
        value = json.loads(path.read_text())
        value['private'] = '<script>private retained data</script>'
        path.write_text(json.dumps(value))
        index_path = batch[0] / 'index.json'
        index = json.loads(index_path.read_text())
        index['records'][0].update(bytes=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        index_path.write_text(json.dumps(index))
        result = self.build_retained((batch[0], batch[1], inventory(batch[0])))
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())

    def test_retained_directory_rejects_unavailable_discovery_and_unbounded_observations(self):
        for changes in (dict(discovery_complete=False),
                        dict(active=dict(state='held', retired=False, reason='x' * 513))):
            with self.subTest(changes=changes):
                result = self.build_retained(self.retained_batch(**changes))
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.output.exists())

    def test_retained_directory_rejects_symlinks_extra_html_and_oversized_record(self):
        from infra.aws_benchmark.production_export import MAX_PUBLIC_RECORD
        from infra.aws_benchmark.production_publish import inventory
        batch = self.retained_batch()
        alias = self.root / 'retained-link'
        alias.symlink_to(batch[0], target_is_directory=True)
        result = self.build_retained((alias, batch[1], batch[2]))
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())
        extra = batch[0] / 'retained.html'
        extra.write_text('<script>never execute or publish</script>')
        result = self.build_retained((batch[0], batch[1], inventory(batch[0])))
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())
        extra.unlink()
        value = self.aws_record()
        batch = self.retained_batch([value])
        path = batch[0] / (value['run_key'] + '.json')
        outside = self.root / 'outside-record.json'
        outside.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(outside)
        result = self.build_retained(batch)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())
        path.unlink()
        path.write_bytes(b' ' * (MAX_PUBLIC_RECORD + 1))
        result = self.build_retained((batch[0], batch[1], inventory(batch[0])))
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())

    def test_retained_public_values_are_escaped_and_unavailable_outcomes_keep_their_meaning(self):
        value = self.aws_record('125', passed=False)
        value.update(schema='rcp-production-public-report-v3', qualification_outcome='evidence-unavailable',
                     collected_at='2026-11-06T18:00:00+00:00')
        value['identity']['suite'] = '<script>suite</script>'
        value['comparison'] = '<img src=x onerror=alert(1)>'
        result = self.build_retained(self.retained_batch([value]))
        self.assertEqual(result.returncode, 0, result.stderr)
        for relative in ('aws-benchmarks/index.html', 'aws-benchmarks/runs/' + value['run_key'] + '.html'):
            text = (self.output / relative).read_text()
            self.assertIn('&lt;script&gt;suite&lt;/script&gt;', text)
            self.assertNotIn('<script>suite', text)
            self.assertNotIn('<img src=x', text)
            self.assertIn('unavailable; workload outcome unknown', text)
            self.assertNotIn('failed; excluded from performance history', text)

    def test_retained_refresh_removes_expired_records_but_preserves_depot_and_docs(self):
        self.test_preserves_existing_benchmark_dashboard()
        depot = {p.name: p.read_bytes() for p in (self.output / 'ci-benchmarks').iterdir()}
        value = self.aws_record()
        first = self.build_retained(self.retained_batch([value]), '--with-benchmark-history')
        self.assertEqual(first.returncode, 0, first.stderr)
        old = self.output / 'aws-benchmarks/records' / (value['run_key'] + '.json')
        old_page = self.output / 'aws-benchmarks/runs' / (value['run_key'] + '.html')
        self.assertTrue(old.exists())
        report = (self.output / 'reports/performance/index.html').read_bytes()
        second = self.build_retained(self.retained_batch(), '--with-benchmark-history')
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertFalse(old.exists())
        self.assertFalse(old_page.exists())
        self.assertEqual({p.name: p.read_bytes() for p in (self.output / 'ci-benchmarks').iterdir()}, depot)
        self.assertEqual((self.output / 'reports/performance/index.html').read_bytes(), report)
        self.assert_site_links_resolve()

    def test_invalid_retained_input_preserves_existing_generated_site(self):
        batch = self.retained_batch([self.aws_record()])
        result = self.build_retained(batch)
        self.assertEqual(result.returncode, 0, result.stderr)
        before = {p.relative_to(self.output): p.read_bytes() for p in self.output.rglob('*') if p.is_file()}
        result = self.build_retained((batch[0], batch[1], '0' * 64))
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual({p.relative_to(self.output): p.read_bytes() for p in self.output.rglob('*') if p.is_file()}, before)

    def test_retained_input_and_output_overlap_is_rejected_without_changing_evidence(self):
        batch = self.retained_batch([self.aws_record()])
        before = {p.name: p.read_bytes() for p in batch[0].iterdir()}
        for output in (batch[0], batch[0] / 'site', batch[0].parent):
            with self.subTest(output=output):
                result = self.build_retained(batch, '--output', str(output))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('must not overlap', result.stderr)
                self.assertEqual({p.name: p.read_bytes() for p in batch[0].iterdir()}, before)
        self.assertFalse((batch[0] / 'site').exists())

    def test_retained_refresh_rejects_output_symlinks_and_special_files_without_deleting_targets(self):
        batch = self.retained_batch()
        self.output.mkdir()
        outside = self.root / 'outside'
        outside.mkdir()
        sentinel = outside / 'keep.txt'
        sentinel.write_text('preserve')
        target = self.output / 'aws-benchmarks'
        target.symlink_to(outside, target_is_directory=True)
        result = self.build_retained(batch)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(sentinel.read_text(), 'preserve')
        target.unlink()
        target.mkdir()
        child = target / 'unsafe'
        child.symlink_to(sentinel)
        result = self.build_retained(batch)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(sentinel.read_text(), 'preserve')
        child.unlink()
        if hasattr(os, 'mkfifo'):
            os.mkfifo(child)
            result = self.build_retained(batch)
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue(child.exists())
        self.assertFalse((self.output / 'index.html').exists())

    def test_aws_history_navigation_requires_actual_records(self):
        for records in (None, {}):
            with self.subTest(records=records):
                repository = self.aws_history(records)
                result = self.build('--aws-history', str(repository))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn('AWS benchmarks', (self.output / 'index.html').read_text())
                self.assertFalse((self.output / 'aws-benchmarks/index.html').exists())

    def test_aws_records_render_separately_with_resolving_links_and_public_json(self):
        passed, failed = self.aws_record(), self.aws_record('124', passed=False)
        records = {value['run_key'] + '.json': ('100644', value) for value in (passed, failed)}
        repository = self.aws_history(records)
        result = self.build('--aws-history', str(repository))
        self.assertEqual(result.returncode, 0, result.stderr)
        landing = (self.output / 'index.html').read_text()
        self.assertIn('href="aws-benchmarks/index.html">AWS benchmarks', landing)
        self.assertNotIn('ci-benchmarks', landing)
        index = (self.output / 'aws-benchmarks/index.html').read_text()
        self.assertIn('does not prove content integrity', index)
        self.assertIn('failed; excluded from performance history', index)
        self.assertLess(index.index('124-1'), index.index('123-1'))
        for value in (passed, failed):
            record = self.output / 'aws-benchmarks/records' / (value['run_key'] + '.json')
            self.assertEqual(json.loads(record.read_text()), value)
            page = self.output / 'aws-benchmarks/runs' / (value['run_key'] + '.html')
            self.assertIn('Cleanup', page.read_text())
            self.assertIn('verified', page.read_text())
            self.assertNotIn('<script', page.read_text())
        for page in self.output.rglob('*.html'):
            for tag, attrs in Elements(page.read_text()).tags:
                value = attrs.get('href') if tag in {'a', 'link'} else attrs.get('src')
                if not value:
                    continue
                url = urlsplit(value)
                if url.scheme or url.netloc:
                    continue
                self.assertFalse(url.path.startswith('/'), value)
                target = (page.parent / unquote(url.path)).resolve() if url.path else page
                if target.is_dir():
                    target /= 'index.html'
                self.assertTrue(target.is_file(), (page, value))
                if url.fragment and target.suffix == '.html':
                    self.assertIn(unquote(url.fragment), [a.get('id') for _, a in Elements(target.read_text()).tags])

    def test_public_history_explains_unattested_timings_and_hides_legacy_worker_time(self):
        value = self.aws_record()
        value['timestamp'] = '1999-01-01\u202e00:00:00+00:00'
        history = self.aws_history({value['run_key'] + '.json': ('100644', value)})
        result = self.build('--aws-history', str(history))
        self.assertEqual(result.returncode, 0, result.stderr)
        for relative in ('aws-benchmarks/index.html', 'aws-benchmarks/runs/' + value['run_key'] + '.html'):
            text = (self.output / relative).read_text()
            self.assertIn('consistency checks passed', text)
            self.assertIn('not independently attested', text)
            self.assertIn('self-reported', text)
            self.assertNotIn('1999-01-01', text)
            self.assertNotIn('\u202e', text)

    def test_unavailable_evidence_is_not_labeled_as_a_failed_workload(self):
        value = self.aws_record('125', passed=False)
        value.update(schema='rcp-production-public-report-v2', qualification_outcome='evidence-unavailable',
            note='Safely retired with unavailable evidence; workload success or failure is unknown.')
        repository = self.aws_history({value['run_key'] + '.json': ('100644', value)})
        result = self.build('--aws-history', str(repository))
        self.assertEqual(result.returncode, 0, result.stderr)
        index = (self.output / 'aws-benchmarks/index.html').read_text()
        self.assertIn('unavailable; workload outcome unknown', index)
        self.assertNotIn('failed; excluded from performance history', index)
        record = self.output / 'aws-benchmarks/records' / (value['run_key'] + '.json')
        self.assertEqual(json.loads(record.read_text()), value)
        page = self.output / 'aws-benchmarks/runs' / (value['run_key'] + '.html')
        self.assertIn('unavailable; workload outcome unknown', page.read_text())
        self.assertNotIn('<td>failed</td>', page.read_text())

    def test_aws_public_record_strings_are_escaped(self):
        value = self.aws_record()
        value['identity']['suite'] = '<script>alert(1)</script>'
        value['comparison'] = '<img src=x onerror=alert(1)>'
        repository = self.aws_history({value['run_key'] + '.json': ('100644', value)})
        result = self.build('--aws-history', str(repository))
        self.assertEqual(result.returncode, 0, result.stderr)
        for page in (self.output / 'aws-benchmarks/index.html',
                     self.output / 'aws-benchmarks/runs' / (value['run_key'] + '.html')):
            text = page.read_text()
            self.assertIn('&lt;script&gt;', text)
            self.assertNotIn('<img src=x', text)
            self.assertNotIn('<script>alert', text)

    def test_invalid_aws_history_fails_before_writing_site(self):
        value = self.aws_record()
        for records in (
            {'repo-655073755-run-123.json': ('120000', 'outside.json')},
            {'repo-1-run-123.json': ('100644', value)},
            {'repo-655073755-run-124.json': ('100644', value)},
            {'repo-655073755-run-123.json': ('100644', dict(value, private='unexpected'))},
            {'repo-655073755-run-123.json': ('100644', '{malformed')},
            {'unexpected.html': ('100644', '<script>raw retained HTML</script>')},
        ):
            with self.subTest(records=records):
                repository = self.aws_history(records)
                result = self.build('--aws-history', str(repository))
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.output.exists())

    def test_aws_history_keeps_depot_dashboard_bytes_and_navigation_separate(self):
        self.test_preserves_existing_benchmark_dashboard()
        history = self.output / 'ci-benchmarks'
        prior = {path.name: path.read_bytes() for path in history.iterdir()}
        value = self.aws_record()
        repository = self.aws_history({value['run_key'] + '.json': ('100644', value)})
        result = self.build('--with-benchmark-history', '--aws-history', str(repository))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual({path.name: path.read_bytes() for path in history.iterdir()}, prior)
        header = next(attrs for tag, attrs in Elements((self.output / 'index.html').read_text()).tags if tag == 'header')
        self.assertEqual(header['class'], 'site-header')
        links = [attrs.get('href', '') for tag, attrs in Elements((self.output / 'reports/index.html').read_text()).tags if tag == 'a']
        self.assertIn('../aws-benchmarks/index.html', links)
        self.assertFalse(any('ci-benchmarks' in link for link in links))

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
        history = self.output / 'ci-benchmarks'
        history.mkdir(parents=True)
        (history / 'index.html').write_text('existing dashboard')
        (history / 'history.json').write_text('{"runs": []}')
        for name in ('changes.json', 'changes.md', 'changes.html'):
            (history / name).write_text('existing ' + name)
        result = self.build('--with-benchmark-history')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((history / 'index.html').read_text(), 'existing dashboard')
        self.assertEqual((history / 'history.json').read_text(), '{"runs": []}')
        for name in ('history.json', 'changes.json', 'changes.md'):
            self.assertEqual((self.output / name).read_bytes(), (history / name).read_bytes())
        self.assertIn('ci-benchmarks/changes.html', (self.output / 'changes.html').read_text())
        self.assertIn('Fast file operations.', (self.output / 'index.html').read_text())

    def test_history_navigation_does_not_depend_on_dashboard_metadata(self):
        from benchmarks.report import render
        from benchmarks.test_report import sample_run
        run = sample_run()
        run['context']['runner_label'] = 'rcp-report-index'
        source = self.root / 'results.json'
        source.write_text(json.dumps(run))
        history = self.output / 'ci-benchmarks'
        render(source, history, '../reports/index.html')
        dashboard = (history / 'index.html').read_bytes()
        result = self.build('--with-benchmark-history')
        self.assertEqual(result.returncode, 0, result.stderr)
        page = Elements((self.output / 'reports/performance/index.html').read_text())
        self.assertIn('../../index.html#top', [attrs.get('href') for tag, attrs in page.tags if tag == 'a'])
        self.assertEqual((history / 'index.html').read_bytes(), dashboard)

    def test_repeated_builds_keep_ci_history_out_of_public_navigation(self):
        for _ in range(2):
            result = self.build()
            self.assertEqual(result.returncode, 0, result.stderr)
            page = Elements((self.output / 'reports/performance/index.html').read_text())
            self.assertNotIn('Benchmark history', (self.output / 'reports/performance/index.html').read_text())
            self.assertFalse(any('ci-benchmarks' in attrs.get('href', '') for _, attrs in page.tags))

    def test_landing_links_and_assets_resolve_with_a_shared_report_header(self):
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stderr)
        landing = (self.output / 'index.html').read_text()
        self.assertIn('Fast file operations.', landing)
        self.assertIn('local rcp and cp both take about 2.5 seconds', landing)
        self.assertNotIn('http-equiv="refresh"', landing)
        for tool in ('rcp', 'rrm', 'rchm', 'rcmp', 'rlink', 'filegen'):
            self.assertIn(tool, landing)
        for page in self.output.rglob('*.html'):
            elements = Elements(page.read_text())
            self.assertEqual(len([1 for tag, attrs in elements.tags if tag == 'header']), 1)
            for tag, attrs in elements.tags:
                value = attrs.get('href') if tag in {'a', 'link'} else attrs.get('src')
                if not value:
                    continue
                url = urlsplit(value)
                if url.scheme or url.netloc:
                    continue
                self.assertFalse(url.path.startswith('/'), value)
                target = (page.parent / unquote(url.path)).resolve() if url.path else page
                if target.is_dir():
                    target /= 'index.html'
                self.assertTrue(target.is_file(), (page, value))
                if url.fragment and target.suffix == '.html':
                    ids = [attrs.get('id') for _, attrs in Elements(target.read_text()).tags]
                    self.assertIn(unquote(url.fragment), ids, (page, value))
            styles = [attrs['href'] for tag, attrs in elements.tags if tag == 'link' and attrs.get('rel') == 'stylesheet']
            self.assertTrue(any(value.endswith('/site.css') for value in styles))
        links = [attrs.get('href') for tag, attrs in Elements(landing).tags if tag == 'a']
        self.assertIn('#top', links)
        self.assertIn('#tools', links)
        self.assertIn('reports/performance/index.html', links)

    def test_missing_featured_report_fails_before_writing_the_site(self):
        (self.docs / 'performance-results.md').unlink()
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Missing landing-page report', result.stderr)
        self.assertFalse(self.output.exists())

    def test_renamed_landing_and_header_readme_sections_fail_before_writing(self):
        readme = self.repo / 'README.md'
        original = readme.read_text()
        for heading in ('Examples', 'Documentation', 'Copy semantics', 'Permissions and ownership (`rchm`)'):
            with self.subTest(heading=heading):
                readme.write_text(original.replace(heading, 'Renamed section'))
                result = self.build()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('Missing repository section', result.stderr)
                page = 'reports/index.html' if heading == 'Documentation' else 'index.html'
                self.assertIn(f'section in {page}:', result.stderr)
                self.assertFalse(self.output.exists())

    def test_absolute_repository_links_in_report_bodies_are_still_checked(self):
        report = self.docs / 'performance.md'
        original = report.read_text()
        for reference in ('https://github.com/wykurz/rcp#renamed',
                          'https://github.com/wykurz/rcp/blob/main/README.md#renamed'):
            with self.subTest(reference=reference):
                report.write_text(original + f'\n[Guide]({reference})\n')
                result = self.build()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('Missing repository section in reports/performance/index.html:', result.stderr)
                self.assertIn(reference, result.stderr)
                self.assertFalse(self.output.exists())

    def test_featured_measurements_follow_report_tables_and_share_a_chart_scale(self):
        report = self.docs / 'performance.md'
        report.write_text(report.read_text().replace('519.931', '1,200.000').replace('5,966.359', '12,000.000')
                          .replace('1,024,000', '2,048,000').replace('1.024 TB', '2.048 TB')
                          .replace('2.484', '4.000'))
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stderr)
        landing = (self.output / 'index.html').read_text()
        self.assertIn('20.0 min', landing)
        self.assertIn('200.0 min', landing)
        self.assertIn('--bar-width: 10.0%', landing)
        self.assertIn('--bar-width: 100.0%', landing)
        self.assertIn('2,048,000 files × 1 MB (2.048 TB)', landing)
        self.assertIn('rcp takes about 4.0 seconds', landing)
        self.assertNotIn('October 2026 measurements', landing)

    def test_missing_or_ambiguous_featured_measurement_fails_before_writing(self):
        report = self.docs / 'performance.md'
        original = report.read_text()
        row = '| 1 MB | rcp auto | 519.931 |'
        for replacement in ('', row + '\n' + row, row.replace('519.931', 'NaN')):
            with self.subTest(replacement=replacement):
                report.write_text(original.replace(row, replacement))
                result = self.build()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('landing-page measurement', result.stderr)
                self.assertFalse(self.output.exists())

    def test_combined_build_requires_an_existing_dashboard(self):
        result = self.build('--with-benchmark-history')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Missing benchmark dashboard', result.stderr)
        self.assertFalse(self.output.exists())

    def test_old_dashboard_bookmarks_preserve_run_targets_without_redirecting_public_anchors(self):
        if not shutil.which('node'):
            self.skipTest('Node is unavailable for redirect execution')
        from benchmarks.report import render
        from benchmarks.test_report import sample_run
        source = self.root / 'results.json'
        source.write_text(json.dumps(sample_run()))
        render(source, self.output / 'ci-benchmarks', '../reports/index.html')
        result = self.build('--with-benchmark-history')
        self.assertEqual(result.returncode, 0, result.stderr)
        script = r'''
const fs = require('fs'), vm = require('vm'), assert = require('node:assert/strict');
function redirect(page, fragment, nextFragment) {
  const code = fs.readFileSync(page, 'utf8').match(/<script id="legacy-dashboard">([\s\S]*?)<\/script>/)[1];
  const targets = [];
  const location = {hash: fragment, search: '?from=summary', replace: value => targets.push(value)};
  let changed;
  vm.runInNewContext(code, {location, window: {addEventListener(event, handler) {
    assert.equal(event, 'hashchange'); changed = handler;
  }}});
  if (nextFragment) { location.hash = nextFragment; changed(); }
  return targets;
}
for (const hash of ['', '#top', '#tools', '#performance', '#get-started']) {
  assert.deepEqual(redirect(process.argv[1], hash), []);
}
assert.deepEqual(redirect(process.argv[1], '#run-old-id'), ['ci-benchmarks/index.html?from=summary#run-old-id']);
assert.deepEqual(redirect(process.argv[1], '#tools', '#run-another-id'), ['ci-benchmarks/index.html?from=summary#run-another-id']);
assert.deepEqual(redirect(process.argv[2], '#results'), ['ci-benchmarks/changes.html?from=summary#results']);
'''
        result = subprocess.run(['node', '-e', script, str(self.output / 'index.html'),
                                 str(self.output / 'changes.html')], text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_all_command_examples_are_available_without_javascript(self):
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stderr)
        elements = Elements((self.output / 'index.html').read_text())
        panels = [attrs for _, attrs in elements.tags if attrs.get('role') == 'tabpanel']
        self.assertEqual({panel['id'] for panel in panels}, {'panel-rcp', 'panel-rrm', 'panel-rchm'})
        self.assertTrue(all('hidden' not in panel for panel in panels))
        controls = [attrs for _, attrs in elements.tags if attrs.get('role') == 'tablist']
        self.assertTrue(all('hidden' in attrs for attrs in controls))

    def test_theme_choice_survives_navigation_and_blocked_storage(self):
        if not shutil.which('node'):
            self.skipTest('Node is unavailable for theme execution')
        from benchmarks.report import render
        from benchmarks.test_report import sample_run
        source = self.root / 'results.json'
        source.write_text(json.dumps(sample_run()))
        render(source, self.output / 'ci-benchmarks', '../reports/index.html')
        result = self.build('--with-benchmark-history')
        self.assertEqual(result.returncode, 0, result.stderr)
        pages = [self.output / path for path in ('index.html', 'ci-benchmarks/index.html', 'ci-benchmarks/changes.html', 'reports/index.html',
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
        self.assertIn('Missing repository reference', result.stderr)

    def test_renamed_repository_heading_fails_build(self):
        readme = self.repo / 'README.md'
        readme.write_text(readme.read_text().replace('# Usage', '# Getting started'))
        result = self.build()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Missing repository section', result.stderr)
        self.assertIn('../../README.md#usage', result.stderr)
        self.assertFalse((self.output / 'reports/performance/index.html').exists())

    def test_repository_section_links_support_formatting_and_duplicate_headings(self):
        with (self.repo / 'README.md').open('a') as readme:
            readme.write('\n## Local copy acceleration (`--reflink`)\n\n'
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
                report = self.docs / 'performance.md'
                report.write_text(report.read_text() + f'\n[Missing]({reference})\n')
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
