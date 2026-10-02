"""All-row private-text projection; tiny JSON fixtures only."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from benchmarks import report
from benchmarks.test_report import sample_run

PRIVATE='private-user-host-secret-/home/alice/key'


class SanitizedTests(unittest.TestCase):
    def api(self):
        self.assertIsNotNone(getattr(report,'sanitized',None),'sanitized export module missing')
        return report.sanitized

    def failed_run(self):
        run=sample_run(status='failed');run['summaries']=[]
        run['error']=PRIVATE
        run['trials'][0].update(status='failed',validation={'ok':False,'error':PRIVATE},timed_out=True)
        run['trials'][1].update(status='running',elapsed_seconds=None,exit_codes=[],validation={'ok':False})
        run['trials'][2].update(status='failed',elapsed_seconds=.25,exit_codes=[2],validation={'ok':False})
        return run

    def test_all_attempts_order_durations_and_unknown_proof_survive(self):
        run=self.failed_run();original=copy.deepcopy(run)
        exported=self.api().project_run(run,[dict(artifact_id='source-1',sha256='f'*64)])
        self.assertEqual(exported['run_id'],run['run_id']);self.assertEqual(exported['status'],'failed')
        self.assertEqual([row['ordinal'] for row in exported['trials']],[0,1,2])
        self.assertEqual([row['elapsed_seconds'] for row in exported['trials']],[1.1,None,.25])
        self.assertEqual([row['status'] for row in exported['trials']],['failed','running','failed'])
        self.assertEqual(exported['trials'][2]['exit_codes'],[2])
        self.assertEqual(exported['trials'][0]['proofs']['source']['qualification'],'unknown')
        self.assertEqual(exported['ratios'],[]);self.assertEqual(run,original)

    def test_allowlist_withholds_injected_free_text_and_unknown_fields(self):
        run=self.failed_run()
        run['revision'].update(commit=PRIVATE,branch=PRIVATE)
        run['context'].update(runner_label=PRIVATE,source=PRIVATE,destination=PRIVATE,repository=PRIVATE,run_url=PRIVATE,
            environment=dict(kernel=PRIVATE,architecture=PRIVATE,cpu_model=PRIVATE,cpu_quota=PRIVATE,filesystem=dict(source=dict(filesystem_type='ext4',mount_source=PRIVATE,mountpoint=PRIVATE,mount_options=['rw','lowerdir='+PRIVATE]))))
        run['tools']['rcp'].update(path=PRIVATE,version=PRIVATE,sha256=PRIVATE)
        run['cases'][0].update(id=PRIVATE,description=PRIVATE)
        run['variants'][0].update(id=PRIVATE,description=PRIVATE,args=[PRIVATE])
        for row in run['trials']:
            row.update(case_id=PRIVATE,variant_id=PRIVATE,commands=[[PRIVATE,'--password='+PRIVATE]],logs=[dict(stdout=PRIVATE)],unknown=dict(secret=PRIVATE))
        run['unknown']=PRIVATE
        exported=self.api().project_run(run,[dict(artifact_id='source-1',sha256='f'*64)])
        self.assertNotIn(PRIVATE,json.dumps(exported));self.assertNotIn('/home/alice',json.dumps(exported))
        self.assertIsNone(exported['revision']['commit']);self.assertIsNone(exported['tools']['rcp']['sha256'])
        self.assertEqual(exported['conditions']['filesystem']['source']['mount_flags'],['rw'])
        self.assertTrue(exported['redaction']['free_text_withheld'])
        self.assertEqual(len(exported['trials']),3)

    def test_cli_writes_only_export_and_preserves_exact_source_bytes_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'input.json';output=root/'shareable'
            raw=json.dumps(self.failed_run(),indent=4)+'\n';source.write_text(raw)
            child=subprocess.run([sys.executable,'-m','benchmarks.report',str(source),'--format','sanitized-json','--output',str(output)],capture_output=True,text=True,timeout=5)
            self.assertEqual(child.returncode,0,child.stderr)
            self.assertEqual(sorted(path.name for path in output.iterdir()),['export.json'])
            exported=json.loads((output/'export.json').read_text())
            self.assertEqual(exported['export_schema_version'],1)
            self.assertEqual(exported['runs'][0]['source_results'][0]['sha256'],hashlib.sha256(raw.encode()).hexdigest())
            self.assertNotIn(PRIVATE,(output/'export.json').read_text())

    def test_invalid_input_produces_no_partial_shareable_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'input.json';output=root/'shareable';source.write_text('{"schema_version": 1}')
            with self.assertRaises(ValueError):self.api().export_results(source,output)
            self.assertFalse(output.exists())

    def test_failed_exit_outcomes_withhold_text_and_preserve_negative_signal_codes(self):
        run=self.failed_run();run['trials'][0]['exit_codes']=[-15,PRIVATE,True]
        run['trials'][1]['exit_codes']={PRIVATE:-9,'second':None}
        rows=self.api().project_run(run,[])['trials']
        self.assertEqual(rows[0]['exit_codes'],[-15,None,None]);self.assertEqual(rows[1]['exit_codes'],[-9,None])
        self.assertNotIn(PRIVATE,json.dumps(rows))

    def comparisons(self):
        run=sample_run();candidate=run['variants'][0]
        for ordinal,(name,tool,args,scale) in enumerate((('rcp-baseline','rcp',['--summary'],2),('rsync-matched','rsync',['-rp','--stats'],4)),2):
            variant=dict(candidate,id=name,tool=tool,args=args);run['variants'].append(variant)
            rows=[]
            for original in run['trials'][:3]:
                row=copy.deepcopy(original);row.update(variant_id=name,elapsed_seconds=original['elapsed_seconds']*scale);rows.append(row)
            run['trials'].extend(rows)
            summary=copy.deepcopy(run['summaries'][0]);summary.update(variant_id=name,series_id=str(ordinal)*64,
                median=1.2*scale,minimum=1.1*scale,maximum=1.3*scale,stdev=.1*scale,samples=[row['elapsed_seconds'] for row in rows]);run['summaries'].append(summary)
        return run

    def test_numeric_ratios_are_same_completed_case_and_explicit_even_in_failed_run(self):
        run=self.comparisons();run['status']='failed';later=copy.deepcopy(run['cases'][0]);later['id']='unfinished';run['cases'].append(later)
        row=copy.deepcopy(run['trials'][0]);row.update(case_id='unfinished',status='failed',validation={'ok':False});run['trials'].append(row)
        ratios=self.api().project_run(run,[])['ratios']
        self.assertEqual([ratio['kind'] for ratio in ratios],['baseline','rsync-matched'])
        self.assertEqual([ratio['value'] for ratio in ratios],[.5,.25])
        for ratio in ratios:
            self.assertEqual(ratio['numerator']['series_id'],'1'*64);self.assertEqual(ratio['run_status'],'failed')
            self.assertTrue(ratio['completed_case']);self.assertFalse(ratio['acceptance_evaluated'])
        run['variants'][2]['args']=['-a'];self.assertEqual(len(self.api().project_run(run,[])['ratios']),1)

    def test_known_symbolic_operands_are_projected_without_binding_paths_or_credentials(self):
        n=self.api()
        description=dict(bindings=dict(source=PRIVATE,private_identity=PRIVATE),operands=[
            dict(index=0,classification='opaque-private',bindings=[],symbolic=None),
            dict(index=1,classification='bound-path',bindings=['source'],symbolic='${source-host}:${source}/'),
            dict(index=2,classification='credential',bindings=[],symbolic=PRIVATE),
            dict(index=3,classification='bound-path',bindings=['source'],symbolic=PRIVATE),
            dict(index=4,classification='bound-path',bindings=['private_identity'],symbolic='${private_identity}')])
        projected=n.command(['--summary','192.0.2.2:'+PRIVATE+'/',PRIVATE,PRIVATE,PRIVATE],description,0,'source','rcpd')
        self.assertEqual(projected['argv'],['--summary','${source-host}:${source}/',None,None,None])
        self.assertFalse(projected['fully_reproducible']);self.assertNotIn(PRIVATE,json.dumps(projected))
        legacy=n.command([PRIVATE],None,0,None,None);self.assertFalse(legacy['fully_reproducible'])

    def test_malformed_bindings_and_credentials_never_gain_reproduction_qualification(self):
        n=self.api()
        for operand in (dict(index=0,classification='bound-path',bindings=[{}],symbolic='${source}'),
                        dict(index=0,classification='credential',bindings=None,symbolic='--summary'),
                        dict(index=99,classification='bound-path',bindings=['source'],symbolic='${source}')):
            projected=n.command(['--summary'],dict(bindings={'source':PRIVATE},operands=[operand]),0,'master','rcp')
            self.assertFalse(projected['fully_reproducible']);self.assertNotIn(PRIVATE,json.dumps(projected))
        mismatched=n.command([PRIVATE],dict(bindings={'source':'different'},operands=[dict(index=0,classification='bound-path',bindings=['source'],symbolic='${source}')]),0,'source','rcpd')
        self.assertEqual(mismatched['argv'],[None])

    def test_failed_owned_evidence_resources_hashes_and_timings_remain_qualified(self):
        from benchmarks import operations,transport
        from benchmarks.test_transport import evidence,role_records
        run=self.failed_run();run['context'].update(operation_contract_revision=1,summary_locale='C',owned_transport=transport.semantics(2),ssh_transport_profile=transport.PROFILE,
            timing_collection={'rcp-default':'coarse'})
        before=evidence();raw=json.dumps(before);roles=role_records()
        for role,record in roles.items():record.update(tool_identity_key='rcp' if role=='master' else 'rcpd',trial_index=0,command_index=0,argv_description=dict(bindings={'source':PRIVATE},operands=[]))
        for row in run['trials']:
            row.update(operation='fresh',child_locale='C',expected_transfer=operations.transfer_counts('fresh',dict(files=10240,bytes=10485760)),
                transport=dict(ok=True,outer_exit_code=19,outer_waited=True,pins_before={'rcp':'a'*64,'private':PRIVATE},pins_after={'rcp':'b'*64},preflight_raw=raw,postflight=before,roles=roles,network=dict(ok=True,preflight_sha256=hashlib.sha256(raw.encode()).hexdigest())),
                timings=dict(status='coarse',reports=[dict(schema_version=1,identifier='rcp-master',pid=123,scopes=[dict(name=PRIVATE,count=1,finished=1,interrupted=0,total_seconds=.5,mean_seconds=.5,p50_seconds=.5,p95_seconds=.5,max_seconds=.5)])]))
        projected=self.api().project_run(run,[]);row=projected['trials'][0]
        self.assertEqual(row['transport']['qualification'],'recorded-unqualified')
        self.assertEqual(row['transport']['pins_before'],{'rcp':'a'*64});self.assertEqual(row['transport']['pins_after'],{'rcp':'b'*64})
        self.assertEqual(row['transport']['requested_rtt_ms'],2);self.assertEqual(row['transport']['resources']['source']['fd_soft'],97)
        self.assertEqual(row['evidence'][0]['sha256'],hashlib.sha256(raw.encode()).hexdigest())
        self.assertEqual(row['timings']['reports'][0]['scopes'][0]['total_seconds'],.5)
        self.assertIsNone(row['timings']['reports'][0]['scopes'][0]['name']);self.assertNotIn(PRIVATE,json.dumps(projected))

    def test_successful_row_does_not_qualify_unused_seed_or_unverified_cache_bits(self):
        from benchmarks import operations,run as runner
        run=sample_run();counts=runner.expected_counts(run['cases'][0]);run['cases'][0]['fixture_digest']='f'*64
        run['context'].update(operation_contract_revision=1,summary_locale='C')
        for row in run['trials']:
            content=dict(ok=True,counts=counts,digest='f'*64)
            metadata=dict(ok=True,entries=counts['directories']+counts['files']+1,checked_fields=['mode'])
            source=copy.deepcopy(content);source['metadata']=dict(metadata,checked_fields=['mode','uid','gid','mtime_ns'])
            expected=operations.transfer_counts('fresh',counts)
            row.update(operation='fresh',child_locale='C',expected_transfer=expected,copy_summary=expected,validation=content,
                       metadata_validation=metadata,source_validation=source,seed_validation={'ok':True},cache_validation={'ok':True})
        proofs=self.api().project_run(run,[])['trials'][0]['proofs']
        self.assertEqual(proofs['content']['qualification'],'passed');self.assertEqual(proofs['source']['qualification'],'passed')
        self.assertEqual(proofs['seed']['qualification'],'recorded-unqualified');self.assertEqual(proofs['cache']['qualification'],'recorded-unqualified')

    def owned_run(self):
        from benchmarks import operations,transport,run as runner
        from benchmarks.test_transport import LifecycleGateTests
        run=sample_run();case=run['cases'][0];case.update(directory_widths=[1],files_per_leaf=1,file_size_bytes=3,fixture_digest='f'*64)
        counts=runner.expected_counts(case);run['context'].update(operation_contract_revision=1,summary_locale='C',owned_transport=transport.semantics(2),ssh_transport_profile=transport.PROFILE)
        run['tools']={name:dict(path=path,sha256=sha,version='1.0') for name,path,sha in [('rcp','/candidate/rcp','a'*64),('rcpd','/candidate/rcpd','b'*64),('ssh','/tools/ssh','b'*64)]}
        inner=LifecycleGateTests().provisional()
        for role,value in inner['roles'].items():value.update(tool_identity_key='rcp' if role=='master' else 'rcpd',command_index=0,trial_index=0)
        qualified=transport.qualify(inner,0,{'ok':True,'launcher_reaped':True,'launcher_pid':42,'before':{'namespaces':{kind:kind+':[1]' for kind in transport.KINDS},'uid':1001,'gid':42,'links':[],'routes':[],'qdiscs':[]},'after':{'namespaces':{kind:kind+':[1]' for kind in transport.KINDS},'uid':1001,'gid':42,'links':[],'routes':[],'qdiscs':[]}},'rcp',{'rcp':'/candidate/rcp','rcpd':'/candidate/rcpd'},{'rcp':'a'*64,'rcpd':'b'*64},3,run['tools']['ssh'])
        for row in run['trials']:
            content=dict(ok=True,counts=counts,digest='f'*64);metadata=dict(ok=True,entries=3,checked_fields=['mode']);source=dict(content,metadata=dict(metadata,checked_fields=['mode','uid','gid','mtime_ns']))
            row.update(operation='fresh',child_locale='C',expected_transfer=operations.transfer_counts('fresh',counts),copy_summary=operations.transfer_counts('fresh',counts),validation=content,metadata_validation=metadata,source_validation=source,transport=qualified['transport'])
        return run

    def test_successful_owned_projection_preserves_observed_capacity_and_selected_role_identity(self):
        row=self.api().project_run(self.owned_run(),[])['trials'][0]
        self.assertEqual(row['transport']['qualification'],'passed');self.assertEqual(row['transport']['capacity']['logical_F'],17)
        self.assertEqual(row['transport']['capacity']['effective_E'],11);self.assertEqual(row['transport']['capacity']['pending_P'],33)
        self.assertEqual(row['role_commands'][1]['tool_identity_key'],'rcpd')
        self.assertNotIn('net:[',json.dumps(row));self.assertNotIn('/candidate/',json.dumps(row))

    def test_role_label_cannot_relabel_a_validated_candidate_or_baseline_executable(self):
        for baseline in (False,True):
            run=self.owned_run()
            if baseline:
                run['variants'][0]['id']='rcp-baseline';run['summaries'][0]['variant_id']='rcp-baseline'
                run['tools']['rcp-baseline']=copy.deepcopy(run['tools']['rcp']);run['tools']['rcpd-baseline']=copy.deepcopy(run['tools']['rcpd'])
                for row in run['trials']:row['variant_id']='rcp-baseline'
            for row in run['trials']:
                row['transport']['roles']['master']['tool_identity_key']='rsync'
                row['transport']['roles']['source']['tool_identity_key']='rcpd' if baseline else 'rcpd-baseline'
            report.validate_result(run)
            commands=self.api().project_run(run,[])['trials'][0]['role_commands']
            for command in commands[:2]:
                self.assertIsNone(command['tool_identity_key']);self.assertFalse(command['fully_reproducible'])
                self.assertEqual(command['identity_qualification'],'descriptive-mismatch')
            self.assertEqual(commands[0]['selected_tool_identity_key'],'rcp-baseline' if baseline else 'rcp')

    def test_failed_or_missing_role_binding_never_gains_reproduction_qualification(self):
        run=self.owned_run();run['status']='failed';run['summaries']=[]
        for row in run['trials']:row['status']='failed';row['validation']={'ok':False}
        run['trials'][0]['transport']['roles']['master']['tool_identity_key']='rsync'
        run['trials'][0]['transport']['roles']['source'].pop('tool_identity_key')
        commands=self.api().project_run(run,[])['trials'][0]['role_commands']
        self.assertIsNone(commands[0]['tool_identity_key']);self.assertIsNone(commands[1]['tool_identity_key'])
        self.assertTrue(all(not command['fully_reproducible'] for command in commands))
        self.assertEqual(commands[2]['identity_qualification'],'recorded-unqualified')

    def test_baseline_source_annotation_is_retained_without_inferring_executable_build(self):
        n=self.api();run=self.failed_run();pin='1c92750a3711c2b5469c622180a828dcaf7d1f3b'
        run['context']['baseline_commit']=pin
        exported=n.project_run(run,[])
        self.assertEqual(exported['conditions']['baseline_source_pin'],dict(commit=pin,qualification='declared-unverified'))
        self.assertIsNone(exported['tools']['rcp']['build_source'])
        run['context']['baseline_commit']=PRIVATE
        exported=n.project_run(run,[]);self.assertNotIn(PRIVATE,json.dumps(exported));self.assertEqual(exported['conditions']['baseline_source_pin'],dict(commit=None,qualification='withheld'))
        run['context'].pop('baseline_commit');self.assertEqual(n.project_run(run,[])['conditions']['baseline_source_pin'],dict(commit=None,qualification='unknown'))

    def test_duplicate_sources_preserve_byte_hashes_and_conflicts_never_create_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);history=root/'runs';history.mkdir();run=self.failed_run()
            (history/'a.json').write_text(json.dumps(run));(history/'b.json').write_text(json.dumps(run,indent=2))
            exported=self.api().export_results(root,root/'share')
            self.assertEqual(len(exported['runs']),1);self.assertEqual(len(exported['runs'][0]['source_results']),2)
            self.assertNotEqual(*[item['sha256'] for item in exported['runs'][0]['source_results']])
            run['error']='different';(history/'b.json').write_text(json.dumps(run))
            with self.assertRaisesRegex(ValueError,'conflicting'):self.api().export_results(root,root/'conflict')
            self.assertFalse((root/'conflict').exists())
            with self.assertRaises(FileExistsError):self.api().export_results(history/'a.json',root/'share')


if __name__=='__main__':unittest.main()
