import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from benchmarks import measurements, pairs, report, run, sanitized


TIME = shutil.which('time')
GNU_TIME = sys.platform == 'linux' and TIME is not None and 'GNU' in subprocess.run([TIME, '--version'], capture_output=True, text=True).stdout


def build(sha):
    return dict(binary_sha256=sha, source_revision='a'*40, source_dirty=False, patch_sha256=None,
                cargo_lock_sha256='b'*64, flake_lock_sha256='c'*64, target='x86_64-unknown-linux-musl',
                profile='release', features=[], rustflags=['--cfg','tokio_unstable'], rustc='rustc 1.95.0')


class PairTests(unittest.TestCase):
    def record(self, count=4):
        config = pairs.configuration(123, count)
        variants = [dict(id=key, tool='rcp', args=[], processes=1) for key in (pairs.CANDIDATE, pairs.REFERENCE)]
        rows = [dict(case_id=case, variant_id=variant, iteration=iteration, status='ok', elapsed_seconds=2 if variant==pairs.CANDIDATE else 4,
                     pairing=pairs.trial_metadata(config, case, iteration, position))
                for case in ('tiny', 'other') for iteration in range(1,count+1)
                for position,variant in enumerate(pairs.order(config,case,iteration))]
        summaries = [dict(case_id=case, variant_id=role) for case in ('tiny','other') for role in pairs.ROLES]
        return dict(context=dict(pairing=config, topology='local', timing_request='disabled', timing_collection=dict.fromkeys(pairs.ROLES,'disabled')), variants=variants, cases=[dict(id=case) for case in ('tiny','other')], trials=rows, summaries=summaries, status='complete')

    def test_seeded_balance_and_explicit_indices(self):
        record = self.record()
        pairs.validate(record)
        self.assertEqual(record, self.record())
        for index in range(0,len(record['trials']),4):
            block = record['trials'][index:index+4]
            self.assertEqual(block[0]['pairing']['order'], block[2]['pairing']['order'][::-1])
        comparisons = pairs.comparisons(record)
        self.assertEqual(len(comparisons),8)
        self.assertEqual({item['candidate_over_reference'] for item in comparisons},{.5})
        for item in comparisons:
            self.assertEqual(record['trials'][item['candidate_trial']]['variant_id'],pairs.CANDIDATE)
            self.assertEqual(record['trials'][item['reference_trial']]['variant_id'],pairs.REFERENCE)
        orders = {tuple(pairs.order(pairs.configuration(seed,2),'tiny',1)) for seed in range(20)}
        self.assertEqual(len(orders),2)

    def test_missing_reordered_and_forged_pairs_rejected(self):
        for mutation in (lambda r:r['trials'].pop(0), lambda r:r['trials'].reverse(),
                         lambda r:r['trials'][0]['pairing'].update(position=False),
                         lambda r:r['trials'][0]['pairing'].update(pair=999),
                         lambda r:r['context']['pairing'].update(seed=124)):
            record = self.record()
            mutation(record)
            with self.assertRaises(ValueError):pairs.validate(record)

    def test_partial_case_has_no_comparisons(self):
        record = self.record()
        record.update(status='failed', trials=record['trials'][:3], summaries=[])
        pairs.validate(record)
        self.assertEqual(pairs.comparisons(record),[])
        record['summaries']=[dict(case_id='tiny')]
        with self.assertRaises(ValueError):pairs.validate(record)

    def test_failed_trial_is_excluded_even_with_committed_case_summaries(self):
        record = self.record(count=2)
        before = pairs.comparisons(record)
        self.assertEqual(len(before), 4)
        record['trials'][1]['status'] = 'failed'
        expected = [item for item in before if 1 not in (item['candidate_trial'], item['reference_trial'])]
        self.assertEqual(len(expected), 3)
        self.assertEqual(pairs.comparisons(record), expected)

    def test_invalid_configuration_and_legacy(self):
        for seed,count in ((True,2),(-1,2),(2**64,2),(0,1),(0,3),(0,True)):
            with self.assertRaises(ValueError):pairs.configuration(seed,count)
        legacy=dict(context={},trials=[])
        pairs.validate(legacy)
        self.assertEqual(pairs.comparisons(legacy),[])
        self.assertIsNone(measurements.identity({}))
        with self.assertRaises(ValueError):pairs.validate(dict(context=dict(pairing=None),trials=[]))
        variants=self.record()['variants']
        for mode, selection in (('loopback',variants),('local',variants[:1]),('local',[dict(v,processes=2) for v in variants])):
            with self.assertRaises(ValueError):pairs.validate_selection(mode,selection)


class ResourceSchemaTests(unittest.TestCase):
    def test_strict_resource_values_and_missing_bytes(self):
        metrics={key:0 for key in (*measurements.FLOATS,*measurements.COUNTS,'exit_code')}
        measurements.validate_metrics(metrics)
        for change in (dict(user_seconds=float('nan')),dict(system_seconds=-1),dict(max_rss_kib=True),dict(exit_code=256),dict(extra=0)):
            with self.assertRaises(ValueError):measurements.validate_metrics({**metrics,**change})
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'resources'
            self.assertEqual(measurements.collect(path,True)['status'],'unavailable')
            path.write_text('{"user_seconds":0,"user_seconds":1}')
            self.assertEqual(measurements.collect(path,True)['status'],'invalid')
            path.write_text(json.dumps(dict(metrics,exit_code=2)))
            self.assertEqual(measurements.collect(path,True)['status'],'invalid')
            self.assertEqual(measurements.collect(path,False)['status'],'unavailable')

    def test_explicit_null_environment_is_rejected(self):
        with self.assertRaises(ValueError):
            measurements.validate(dict(context=dict(measurement_environment=None),trials=[]))

    def test_build_binding_and_identity(self):
        sha='d'*64
        declaration={'rcp':build(sha)}
        measurements.validate_builds(declaration,{'rcp':{'sha256':sha}})
        for change in (dict(binary_sha256='e'*64),dict(source_revision='short'),dict(source_dirty=True),dict(features=['x','x']),dict(extra='secret')):
            with self.assertRaises(ValueError):measurements.validate_builds({'rcp':{**declaration['rcp'],**change}},{'rcp':{'sha256':sha}})
        context=dict(pairing=pairs.configuration(1,2),build_provenance=dict(builds=declaration))
        identity=measurements.identity(context)
        context['pairing']=pairs.configuration(2,4)
        context['build_provenance']['builds']['rcp']['source_revision']='e'*40
        self.assertEqual(identity,measurements.identity(context))
        context['build_provenance']['builds']['rcp']['target']='other'
        self.assertNotEqual(identity,measurements.identity(context))


@unittest.skipUnless(GNU_TIME,'requires Linux GNU time')
class ResourceExecutionTests(unittest.TestCase):
    def execute(self, root, name, script, **kwargs):
        return run.execute_commands([ [sys.executable,'-c',script] ],root/name,5,resource_time=TIME,**kwargs)

    def test_cpu_rss_attribution_avoids_parent_preexec_memory(self):
        # the time supervisor execs before it forks: the large runner must not set the child RSS floor
        resident=bytearray(96*1024*1024)
        for index in range(0,len(resident),4096):resident[index]=1
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            before=time.process_time()
            while time.process_time()-before < .15:pass
            idle=self.execute(root,'idle','import time; time.sleep(.1)')
            busy=self.execute(root,'busy',"import time; x=bytearray(24*1024*1024); start=time.process_time()\nwhile time.process_time()-start<.2: pass")
            self.assertTrue(idle['ok']);self.assertTrue(busy['ok'])
            small=idle['resources']['metrics'];large=busy['resources']['metrics']
            self.assertLess(small['user_seconds']+small['system_seconds'],.15)
            self.assertGreaterEqual(large['user_seconds']+large['system_seconds'],.18)
            self.assertLess(small['max_rss_kib'],64*1024)
            self.assertGreater(large['max_rss_kib'],small['max_rss_kib']+16*1024)
            self.assertEqual(idle['resources']['raw_sha256'],hashlib.sha256((root/'idle/resources.json').read_bytes()).hexdigest())

    def test_locale_is_stable_and_failed_exit_preserved(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(os.environ,{'LC_ALL':'private-locale','LANG':'private-locale'}):
            root=Path(directory)
            outcome=self.execute(root,'locale',"import os; assert os.environ['LC_ALL']=='C'; assert os.environ['LANG']=='C'")
            self.assertTrue(outcome['ok'])
            failed=self.execute(root,'failed','raise SystemExit(7)')
            self.assertFalse(failed['ok'])
            self.assertIsNone(failed['resources']['metrics'])
            self.assertEqual(failed['resources']['reason'],'execution_failed')

    def test_timeout_and_invalid_measurement_clean_up_descendants(self):
        for invalid in (False,True):
            with self.subTest(invalid=invalid),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);marker=root/'survived'
                descendant=f"import time; from pathlib import Path; time.sleep(.6); Path({str(marker)!r}).touch()"
                spawn=f"import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',{descendant!r}]); "
                if invalid:
                    supervisor=root/'broken-time'
                    supervisor.write_text('#!'+sys.executable+'\n'+spawn+"from pathlib import Path; Path(sys.argv[sys.argv.index('--output')+1]).write_text('invalid')\n")
                    supervisor.chmod(0o755)
                    outcome=run.execute_commands([['unused']],root/'logs',2,resource_time=str(supervisor))
                    self.assertEqual(outcome['resources']['status'],'invalid')
                else:
                    outcome=run.execute_commands([[sys.executable,'-c',spawn+'time.sleep(10)']],root/'logs',.1,resource_time=TIME)
                    self.assertTrue(outcome['timed_out'])
                self.assertFalse(outcome['ok'])
                time.sleep(.7)
                self.assertFalse(marker.exists())

    def test_signal_cancellation_reaps_measured_process_group(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);marker=root/'survived';ready=root/'ready'
            child=f"import time; from pathlib import Path; Path({str(ready)!r}).touch(); time.sleep(1); Path({str(marker)!r}).touch()"
            script=f"from benchmarks.run import execute_commands; execute_commands({[[sys.executable,'-c',child]]!r},{str(root/'logs')!r},10,resource_time={TIME!r})"
            process=subprocess.Popen([sys.executable,'-c',script],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            try:
                deadline=time.monotonic()+5
                while not ready.exists() and time.monotonic()<deadline:time.sleep(.01)
                self.assertTrue(ready.exists())
                process.send_signal(signal.SIGINT)
                self.assertNotEqual(process.wait(timeout=5),0)
                time.sleep(1.1)
                self.assertFalse(marker.exists())
            finally:
                if process.poll() is None:process.kill();process.wait()

    def test_full_runner_pairs_provenance_and_sanitization(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);binary=root/'bin';binary.mkdir()
            scripts=dict(filegen="import pathlib,sys\nif '--version' in sys.argv: print('filegen 1'); sys.exit(0)\np=pathlib.Path(sys.argv[1])/'filegen'/'0'; p.mkdir(parents=True); (p/'file').write_bytes(b'x')",
                         rcp="import shutil,sys\nif '--version' in sys.argv: print('rcp 1'); sys.exit(0)\nshutil.copytree(sys.argv[-2],sys.argv[-1])",
                         rcpd="print('rcpd 1')")
            for name,script in scripts.items():
                path=binary/name;path.write_text('#!'+sys.executable+'\n'+script+'\n');path.chmod(0o755)
            manifest=root/'manifest.json'
            manifest.write_text(json.dumps(dict(schema_version=1,cases=[dict(id='tiny',directory_widths=[1],files_per_leaf=1,file_size_bytes=1)],variants=[dict(id='rcp-default',tool='rcp',args=[],processes=1)])))
            sha=hashlib.sha256((binary/'rcp').read_bytes()).hexdigest()
            provenance=root/'build.json'
            declared=build(sha);declared.update(rustc='private-build-path',features=['private-feature'])
            provenance.write_text(json.dumps(dict(schema_version=1,builds={'rcp':declared,'rcp-baseline':declared})))
            args=['--manifest',str(manifest),'--case','tiny','--variant','rcp-default','--bin-dir',str(binary),'--baseline-bin-dir',str(binary),'--source-root',str(root),'--destination-root',str(root),'--cache','source-verified','--no-timings','--paired-seed','42','--repetitions','2','--local-resources','--resource-time',TIME,'--build-provenance',str(provenance)]
            with mock.patch.dict(os.environ,{'MALLOC_CONF':'private-environment'}):
                result=run.main([*args,'--output',str(root/'out')])
            self.assertEqual(report.parse_result((root/'out/results.json').read_text()),result)
            self.assertEqual(len(pairs.comparisons(result)),2)
            self.assertEqual(len(result['trials']),4)
            for row in result['trials']:
                self.assertEqual(row['resources']['status'],'complete')
                self.assertEqual(set(row['phase_seconds']),{'preparation','verification','cleanup'})
                self.assertTrue(row['cache_validation']['ok']);self.assertTrue(row['source_validation']['ok'])
                self.assertEqual(row['measurement_commands'][0][0],TIME)
                self.assertEqual(row['commands'][0][0],str(binary/'rcp'))
            self.assertFalse(list(root.glob('rcp-bench-*')))
            result['pairing'] = dict(pair='private-top-level-pair',block=1,position=0,order=['rcp-default'])
            result['resources'] = dict(status='private-top-level-resource')
            exported=sanitized.project_run(result,[])
            self.assertEqual(len(exported['paired_comparisons']),2)
            text=json.dumps(exported)
            for private in ('private-build-path','private-feature','private-environment','private-top-level-pair','private-top-level-resource',str(root)):
                self.assertNotIn(private,text)
            self.assertEqual(exported['trials'][0]['resources']['metrics'],result['trials'][0]['resources']['metrics'])
            # a valid partial prefix remains readable and exportable without inventing a pair
            partial=copy.deepcopy(result);partial.update(status='failed',summaries=[],trials=partial['trials'][:1])
            partial['trials'][0].update(status='failed',resources=dict(status='invalid',metrics=None,raw_sha256=hashlib.sha256(b'malformed resource data').hexdigest(),error='private-error'))
            report.validate_result(partial)
            self.assertEqual(sanitized.project_run(partial,[])['paired_comparisons'],[])
            tampered=copy.deepcopy(result);tampered['trials'][0]['resources']['metrics']['exit_code']=9
            with self.assertRaises(ValueError):report.validate_result(tampered)
            # preserve ordinary unpaired schema/series behavior when no new flags are selected
            legacy=run.main(args[:args.index('--paired-seed')]+['--repetitions','1','--output',str(root/'legacy')])
            report.validate_result(legacy)
            self.assertNotIn('pairing',legacy['context']);self.assertNotIn('resources',legacy['trials'][0])
            self.assertNotIn('phase_seconds',legacy)

            broken=root/'broken-time'
            broken.write_text('#!'+sys.executable+"\nimport pathlib,sys\nif '--version' in sys.argv: print('GNU time test'); sys.exit(0)\npathlib.Path(sys.argv[sys.argv.index('--output')+1]).write_text('broken')\n")
            broken.chmod(0o755)
            with self.assertRaisesRegex(RuntimeError,'trial'):
                run.main([*args,'--resource-time',str(broken),'--output',str(root/'broken')])
            failed=report.parse_result((root/'broken/results.json').read_text())
            self.assertEqual(len(failed['trials']),1)
            self.assertEqual(failed['trials'][0]['resources']['status'],'invalid')
            self.assertEqual(pairs.comparisons(failed),[])
            self.assertEqual(sanitized.project_run(failed,[])['paired_comparisons'],[])
            self.assertTrue(Path(failed['context']['failure_artifacts']['source_scratch']).exists())
            output=root/'reject-provenance-loopback'
            with mock.patch.object(run,'_tool') as probe, mock.patch.object(sys,'stderr',io.StringIO()) as stderr, self.assertRaises(SystemExit) as rejected:
                run.main([*args,'--mode','loopback','--output',str(output)])
            self.assertEqual(rejected.exception.code,2)
            self.assertTrue(stderr.getvalue().endswith('error: build provenance requires local mode; runner environment does not describe remote payloads\n'))
            probe.assert_not_called()
            self.assertFalse(output.exists())
            for extra in (['--repetitions','3'],['--mode','loopback'],['--files-in-flight','8,32']):
                output=root/('reject-'+str(len(list(root.iterdir()))))
                selected=args[:args.index('--build-provenance')] if extra==['--mode','loopback'] else args
                with mock.patch.object(run,'_tool',side_effect=AssertionError('probe must not run')),self.assertRaisesRegex(ValueError,'paired'):
                    run.main([*selected,*extra,'--output',str(output)])
                rejected=report.parse_result((output/'results.json').read_text())
                self.assertEqual(rejected['status'],'failed')
                self.assertEqual(rejected['trials'],[])


if __name__=='__main__':unittest.main()
