"""Regression checks for experiment capture and safe result projection."""

import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from benchmarks import measurements, report, run, sanitized
from benchmarks.test_measurements import build
from benchmarks.test_report import sample_run


class ExperimentBoundaries(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def fixture(self):
        binary = self.root / 'bin'
        binary.mkdir()
        scripts = dict(filegen="import pathlib,sys\nif '--version' in sys.argv: print('filegen 1'); sys.exit(0)\np=pathlib.Path(sys.argv[1])/'filegen'/'0'; p.mkdir(parents=True); (p/'file').write_bytes(b'x')",
                       rcp="import shutil,sys\nif '--version' in sys.argv: print('rcp 1'); sys.exit(0)\nif '--help' in sys.argv: sys.exit(0)\nshutil.copytree(sys.argv[-2],sys.argv[-1])",
                       rcpd="print('rcpd 1')")
        for name, script in scripts.items():
            path = binary / name
            path.write_text('#!' + sys.executable + '\n' + script + '\n')
            path.chmod(0o755)
        manifest = self.root / 'manifest.json'
        manifest.write_text(json.dumps(dict(schema_version=1, cases=[dict(id='tiny', directory_widths=[1], files_per_leaf=1, file_size_bytes=1)], variants=[dict(id='rcp-default', tool='rcp', args=[], processes=1)])))
        declared = build(hashlib.sha256((binary / 'rcp').read_bytes()).hexdigest())
        provenance = self.root / 'build.json'
        provenance.write_text(json.dumps(dict(schema_version=1, builds={'rcp': declared})))
        args = ['--manifest', str(manifest), '--case', 'tiny', '--variant', 'rcp-default', '--bin-dir', str(binary), '--source-root', str(self.root), '--destination-root', str(self.root), '--cache', 'uncontrolled', '--no-timings', '--repetitions', '1', '--build-provenance', str(provenance)]
        return args

    def test_provenance_only_captures_environment_and_separates_series(self):
        args = self.fixture()
        records = []
        for value in ('synthetic-one', 'synthetic-two'):
            with mock.patch.dict(os.environ, {'MALLOC_CONF': value}):
                record = run.main([*args, '--output', str(self.root / value)])
            records.append(report.validate_result(record))
            self.assertEqual(record['context']['measurement_environment']['MALLOC_CONF'], value)
            self.assertNotIn('phase_seconds', record)
        self.assertNotEqual(records[0]['summaries'][0]['series_id'], records[1]['summaries'][0]['series_id'])

    def test_surrogate_environment_survives_runner_report_and_sanitized_export(self):
        args = self.fixture()
        with mock.patch.dict(os.environ, {'RUST_LOG': 'synthetic-\udcff'}):
            record = run.main([*args, '--output', str(self.root / 'run')])
        source = self.root / 'run/results.json'
        self.assertEqual(report.parse_result(source.read_text()), record)
        output = self.root / 'site'
        output.mkdir()
        (output / 'history.json').write_text('old report')
        report.render(source, output)
        history = json.loads((output / 'history.json').read_text())
        self.assertEqual(history['runs'][0]['context']['measurement_environment']['RUST_LOG'], 'synthetic-\udcff')
        envelope = sanitized.export_results(source, self.root / 'sanitized')
        self.assertEqual(envelope['runs'][0]['measurement_environment']['RUST_LOG'], dict(present=True, value_withheld=True))
        self.assertNotIn('synthetic-', json.dumps(envelope))

    def test_projection_drops_guessable_value_and_configuration_hashes(self):
        record = sample_run()
        record['context']['topology'] = 'local'
        environment = {'RUST_LOG': 'debug', 'TOKIO_WORKER_THREADS': '12', 'LD_LIBRARY_PATH': '/home/synthetic-user/.nix-profile/lib:'}
        record['context']['measurement_environment'] = environment
        record['tools']['rcp']['sha256'] = 'd' * 64
        declaration = build('d' * 64)
        record['context']['build_provenance'] = dict(qualification='caller-declared; executable hashes verified, source claims not attested', input_sha256='e' * 64, builds={'rcp': declaration})
        exported = sanitized.project_run(record, [])
        serialized = json.dumps(exported)
        for value in environment.values():
            self.assertNotIn(hashlib.sha256(value.encode()).hexdigest(), serialized)
        configuration = {key: declaration[key] for key in ('target', 'profile', 'features', 'rustflags', 'rustc')}
        self.assertNotIn(hashlib.sha256(json.dumps(configuration, sort_keys=True, separators=(',', ':')).encode()).hexdigest(), serialized)
        self.assertNotIn('value_sha256', serialized)
        self.assertNotIn('configuration_sha256', serialized)
        self.assertTrue(exported['build_provenance']['builds']['rcp']['configuration_text_withheld'])

    def test_series_identity_canonicalizes_features_but_preserves_flag_order(self):
        record = sample_run()
        record['tools']['rcp']['sha256'] = 'd' * 64
        declaration = build('d' * 64)
        declaration.update(features=['feature-b', 'feature-a'], rustflags=['-Copt-level=2', '-Copt-level=3'])
        context = dict(measurement_environment={}, build_provenance=dict(builds={'rcp': declaration}))
        original = copy.deepcopy(context)
        def series(value):
            measurements.validate_builds(value['build_provenance']['builds'], record['tools'])
            return run.series_id(record['cases'][0], record['variants'][0], 'uncontrolled', 'local',
                                 'synthetic', {}, record['tools'], experiment=measurements.identity(value))
        expected = series(context)
        self.assertEqual(context, original)
        reordered = copy.deepcopy(context)
        reordered['build_provenance']['builds']['rcp']['features'].reverse()
        self.assertEqual(series(reordered), expected)
        reordered['build_provenance']['builds']['rcp']['features'].append('feature-c')
        self.assertNotEqual(series(reordered), expected)
        reversed_flags = copy.deepcopy(context)
        reversed_flags['build_provenance']['builds']['rcp']['rustflags'].reverse()
        self.assertNotEqual(series(reversed_flags), expected)

    def test_render_preserves_unrelated_pairs_json_in_reused_directory(self):
        source = self.root / 'input.json'
        source.write_text(json.dumps(sample_run()))
        output = self.root / 'site'
        output.mkdir()
        unrelated = b'{"unrelated":true}\n'
        (output / 'pairs.json').write_bytes(unrelated)
        report.render(source, output)
        self.assertEqual((output / 'pairs.json').read_bytes(), unrelated)
        self.assertTrue((output / 'history.json').exists())

    def test_staging_failure_leaves_previous_report_files_unchanged(self):
        source = self.root / 'input.json'
        source.write_text(json.dumps(sample_run()))
        output = self.root / 'site'
        output.mkdir()
        for name in ('history.json', 'index.html'):
            (output / name).write_text('previous ' + name)
        write_bytes = Path.write_bytes
        def fail(path, data):
            if path.name == 'changes.json':
                raise OSError('synthetic staging failure')
            return write_bytes(path, data)
        with mock.patch.object(Path, 'write_bytes', fail), self.assertRaisesRegex(OSError, 'staging failure'):
            report.render(source, output)
        for name in ('history.json', 'index.html'):
            self.assertEqual((output / name).read_text(), 'previous ' + name)
        self.assertFalse(list(output.glob('.render-*')))

    def test_invalid_numeric_import_does_not_touch_existing_output(self):
        record = sample_run()
        record['phase_seconds'] = {'total': 10 ** 400}
        source = self.root / 'input.json'
        source.write_text(json.dumps(record))
        output = self.root / 'site'
        output.mkdir()
        (output / 'history.json').write_text('previous')
        with self.assertRaises(ValueError):
            report.render(source, output)
        self.assertEqual((output / 'history.json').read_text(), 'previous')

    def test_sanitized_failed_write_removes_only_its_new_directory(self):
        source = self.root / 'input.json'
        source.write_text(json.dumps(sample_run()))
        output = self.root / 'sanitized'
        with mock.patch.object(sanitized.os, 'fsync', side_effect=OSError('synthetic fsync failure')), self.assertRaises(OSError):
            sanitized.export_results(source, output)
        self.assertFalse(output.exists())
        self.assertTrue(source.exists())

    def test_series_identity_uses_the_resolved_resource_locale(self):
        base = sample_run()
        variant = dict(base['variants'][0], args=['--summary', '--max-files-in-flight=32'])
        captured = []
        original = hashlib.sha256
        def digest(raw):
            captured.append(json.loads(raw))
            return original(raw)
        with mock.patch.object(run.hashlib, 'sha256', side_effect=digest):
            run.series_id(base['cases'][0], variant, 'uncontrolled', 'local', 'synthetic', {}, {}, operation_revision=1, experiment={'local_resources': {'policy': measurements.POLICY}})
        self.assertEqual(captured[-1]['child_locale'], 'C')
        self.assertEqual(measurements.child_locale(variant), 'inherited')

    def test_paired_timing_rejection_precedes_tool_probes(self):
        args = ['--source-root', str(self.root), '--destination-root', str(self.root), '--output', str(self.root / 'rejected'), '--paired-seed', '1', '--repetitions', '2']
        with mock.patch.object(run, '_tool', side_effect=AssertionError('probe must not run')), self.assertRaisesRegex(ValueError, '--no-timings'):
            run.main(args)
        record = report.parse_result((self.root / 'rejected/results.json').read_text())
        self.assertEqual(record['status'], 'failed')
        self.assertEqual(record['trials'], [])

    def test_preparation_excludes_persistence_cost(self):
        class StopBeforeCopy(Exception):
            pass
        clock = [0.0]
        snapshots = []
        case = dict(id='synthetic', directory_widths=[1], files_per_leaf=1, file_size_bytes=1)
        variant = dict(id='rcp-default', tool='rcp', args=['--summary', '--max-files-in-flight=1'], processes=1)
        def persist(_output, record):
            clock[0] += .25
            snapshots.append(copy.deepcopy(record))
        def tool(path, *args):
            return dict(path=str(path), version='GNU synthetic', sha256='a' * 64)
        args = ['--source-root', str(self.root), '--destination-root', str(self.root), '--output', str(self.root / 'costs'), '--bin-dir', str(self.root), '--case', 'synthetic', '--variant', 'rcp-default', '--local-resources', '--resource-time', str(self.root / 'time'), '--no-timings', '--cache', 'uncontrolled', '--repetitions', '1']
        with mock.patch.object(run, '_persist', side_effect=persist), mock.patch.object(run, 'load_manifest', return_value=dict(cases=[case], variants=[variant])), mock.patch.object(run, '_tool', side_effect=tool), mock.patch.object(run, '_revision', return_value={}), mock.patch.object(run, 'environment', return_value={}), mock.patch.object(run, '_supports_timings', return_value=False), mock.patch.object(run.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, stdout='', stderr='')), mock.patch.object(run.time, 'monotonic', side_effect=lambda: clock[0]), mock.patch.object(run, 'scan_tree', return_value=dict(counts=run.expected_counts(case), digest='synthetic', entries=[])), mock.patch.object(run, '_prepare_cache', return_value=None), mock.patch.object(run, 'execute_commands', side_effect=StopBeforeCopy), self.assertRaises(StopBeforeCopy):
            run.main(args)
        self.assertEqual(snapshots[-1]['trials'][0]['phase_seconds']['preparation'], 0)


if __name__ == '__main__':
    unittest.main()
