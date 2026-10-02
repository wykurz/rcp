"""Synthetic owned transport contracts; no SSH or namespace launch."""
import copy
import contextlib
import io
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import signal
import sys
import tempfile
import unittest
from unittest import mock

from benchmarks import run


def evidence(rtt=2):
    return dict(schema_version=1, trial_id='1'*32, requested_rtt_ms=rtt,
        profile='rootless-veth-bridge-per-trial-v1', lifetime='trial',
        endpoints={role:dict(ip=ip,uid=1001,gid=42,netns=f'net:[{index}]')
                   for role,ip,index in [('source','192.0.2.2',3),('client','192.0.2.1',4)]},
        isolation=dict(outer_netns='net:[2]',host_netns='net:[1]',userns='user:[2]',host_userns='user:[1]',
                       mntns='mnt:[2]',host_mntns='mnt:[1]',pidns='pid:[2]',host_pidns='pid:[1]',uid=1001,gid=42),
        routes={role:[dict(dst=peer,dev=f'{role}_peer',prefsrc=ip)]
                for role,ip,peer in [('client','192.0.2.1','192.0.2.2'),('source','192.0.2.2','192.0.2.1')]},
        ping=dict(samples_ms=[rtt+.1]*3,median_ms=rtt+.1,loss_percent=0),
        tcp_echo=dict(samples_ms=[rtt+.2]*3,median_ms=rtt+.2),
        qdiscs={direction:[dict(kind='netem',root=True,handle='8001:',
            options=dict(delay=dict(delay=rtt/2000,jitter=0,correlation=0),limit=100000),
            drops=0,packets=40,bytes=5000,backlog=0,qlen=0)] for direction in ('to_source','to_client')},
        account=dict(home='/owned',home_uid=1001,home_mode=0o700,passwd_readonly=True,passwd_sha256='f'*64,nss_matches=True,other_fields_preserved=True),
        launcher_script_sha256='d'*64,
        ssh=dict(launcher_path='/owned/bin/ssh',launcher_sha256='a'*64,binary_path='/tools/ssh',binary_sha256='b'*64,
                 config_path='/owned/ssh_config',config_sha256='c'*64,known_hosts_path='/owned/known_hosts',known_hosts_sha256='e'*64))


def role_records(tool='rcp'):
    roles=('master','source','destination') if tool=='rcp' else ('source','destination')
    value={role:dict(binary='/candidate/'+('rcp' if role=='master' else 'rcpd') if tool=='rcp' else '/tools/rsync',
                    argv=[],netns='net:[3]' if role=='source' else 'net:[4]',userns='user:[2]',mntns='mnt:[2]',pidns='pid:[2]',
                    uid=1001,gid=42,ssh_connection='192.0.2.1 43210 192.0.2.2 2222' if role=='source' else '127.0.0.1 43211 127.0.0.1 2222' if role=='destination' and tool=='rcp' else '',
                    fd_soft=97,fd_hard=512,cpu_affinity=[3,9],cpu_quota='200000 100000',no_new_privileges=1,capabilities=0,
                    capability_sets={key:0 for key in ('CapInh','CapPrm','CapEff','CapBnd','CapAmb')})
           for role in roles}
    if tool=='rcp':
        value['master']['argv']=['--summary','--force-remote','--rcpd-path=/owned/rcpd','192.0.2.2:/source','/destination']
        common=['--pending-writes-multiplier=3']
        value['source']['argv']=[*common,'--role=source','--max-connections=11']
        value['destination']['argv']=[*common,'--role=destination','--max-connections=11','--resolved-automatic-files-in-flight=17']
    return value


class TransportContractTests(unittest.TestCase):
    def transport(self):
        self.assertIsNotNone(getattr(run,'transport',None),'owned transport missing')
        return run.transport

    def test_cli_omitted_and_zero_rtt_are_distinct_and_local_is_rejected(self):
        self.assertIsNone(run._arguments(['--output','/unused']).rtt_ms)
        self.assertEqual(run._arguments(['--output','/unused','--mode','loopback','--rtt-ms','0']).rtt_ms,0)
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit): run._arguments(['--output','/unused','--rtt-ms','0'])

    def test_typed_planner_routes_single_commands_without_rewriting_localhost_paths(self):
        n=self.transport()
        endpoint=n.SourceEndpoint('192.0.2.2',Path('/owned/bin/ssh'))
        command=run.plan_commands(dict(tool='rcp',args=['--summary'],processes=1),Path('/source/localhost:text'),Path('/destination/localhost:text'),dict(rcp='/owned/rcp',rcpd='/owned/rcpd'),'loopback',source_endpoint=endpoint)[0]
        self.assertEqual(command[-2:],['192.0.2.2:/source/localhost:text','/destination/localhost:text'])
        self.assertIn('--rcpd-path=/owned/rcpd',command)
        command=run.plan_commands(dict(tool='rsync',args=['-rp','--stats'],processes=1),Path('/source'),Path('/destination'),dict(rsync='/owned/rsync'),'loopback',source_endpoint=endpoint)[0]
        self.assertIn('--rsh=/owned/bin/ssh',command)
        self.assertEqual(command[-2:],['192.0.2.2:/source/','/destination/'])

    def test_owned_scope_rejects_custom_parallel_and_routing_environment(self):
        n=self.transport()
        for variant in (dict(tool='rcp',args=['--summary','--max-files-in-flight=4'],processes=1),dict(tool='rsync',args=['-rp','--stats'],processes=10),dict(tool='rsync',args=['-a'],processes=1)):
            with self.assertRaises(ValueError):n.validate_request('loopback',0,[variant],{})
        with self.assertRaises(ValueError):n.validate_request('loopback',0,[dict(tool='rsync',args=['-rp','--stats'],processes=1)],{'RSYNC_RSH':'other ssh'})
        n.validate_request('loopback',0,[dict(tool='rcp',args=['--summary'],processes=1)],{})

    def test_evidence_rejects_bypassed_route_delay_queue_loss_and_wrong_namespace(self):
        n=self.transport();n.validate_evidence(evidence(),2)
        for mutation in ('route','delay','backlog','qlen','loss','uid','mnt','pid','ssh'):
            value=evidence()
            if mutation=='route':value['routes']['client'][0]['dev']='lo'
            if mutation=='delay':value['qdiscs']['to_client'][0]['options']['delay']['delay']=0
            if mutation in ('backlog','qlen'):value['qdiscs']['to_client'][0][mutation]=1
            if mutation=='loss':value['qdiscs']['to_source'][0]['drops']=1
            if mutation=='uid':value['isolation']['uid']=0
            if mutation=='mnt':value['isolation']['mntns']=value['isolation']['host_mntns']
            if mutation=='pid':value['isolation']['pidns']=value['isolation']['host_pidns']
            if mutation=='ssh':value['ssh']['launcher_sha256']='no'
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):n.validate_evidence(value,2)

    def test_pair_binds_exact_bytes_both_directions_and_fresh_payload(self):
        n=self.transport();before=evidence();payload=json.dumps(before).encode();sha=hashlib.sha256(payload).hexdigest();after=copy.deepcopy(before);after['preflight_sha256']=sha
        for q in after['qdiscs'].values():q[0].update(packets=100,bytes=15000)
        self.assertEqual(n.validate_pair(before,after,sha,10000)['byte_deltas'],dict(to_source=10000,to_client=10000))
        for mutation in ('hash','packets','identity','payload'):
            value=copy.deepcopy(after)
            if mutation=='hash':value['preflight_sha256']='f'*64
            if mutation=='packets':value['qdiscs']['to_source'][0]['packets']=40
            if mutation=='identity':value['endpoints']['source']['netns']='net:[30]'
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):n.validate_pair(before,value,sha,10001 if mutation=='payload' else 10000)
        self.assertTrue(n.validate_pair(before,after,sha,0)['ok'])

    def test_actual_capacity_is_observed_and_not_a_cpu_or_fd_formula(self):
        n=self.transport();roles=role_records()
        result=n.validate_roles(roles,evidence(),'rcp',dict(rcp='/candidate/rcp',rcpd='/candidate/rcpd'))
        self.assertEqual(result['capacity'],dict(logical_F=17,source_configured_max_connections=11,effective_E=11,pending_multiplier=3,pending_P=33))
        for mutation in ('source_route','cardinality','capabilities','fd','F','multiplier','binary'):
            value=copy.deepcopy(roles)
            if mutation=='source_route':value['source']['ssh_connection']='127.0.0.1 1234 127.0.0.1 2222'
            if mutation=='cardinality':value.pop('destination')
            if mutation=='capabilities':value['master']['capabilities']=1
            if mutation=='fd':value['destination']['fd_soft']=0
            if mutation=='F':value['destination']['argv'][-1]='--resolved-automatic-files-in-flight=0'
            if mutation=='multiplier':value['destination']['argv'][0]='--pending-writes-multiplier=4'
            if mutation=='binary':value['source']['binary']='/wrong/rcpd'
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):n.validate_roles(value,evidence(),'rcp',dict(rcp='/candidate/rcp',rcpd='/candidate/rcpd'))

    def test_recorded_generated_automatic_argv_omits_file_limit_override(self):
        n=self.transport()
        common=['--max-workers=0','--max-blocking-threads=0','--ops-throttle=0','--iops-throttle=0','--chunk-size=0',
                '--overwrite-compare=size,mtime','--overwrite-manifest-max-entries=5000000','--overwrite',
                '--remote-copy-conn-timeout-sec=15','--remote-keepalive-sec=120','--network-profile=datacenter']
        roles={'source':dict(argv=['--role','source',*common,'--max-connections=100','--pending-writes-multiplier=4','--master-cert-fp='+'0'*64,'--bind-ip','192.0.2.2']),
               'destination':dict(argv=['--role','destination',*common,'--resolved-automatic-files-in-flight=20','--max-connections=20','--pending-writes-multiplier=4','--master-cert-fp='+'0'*64])}
        self.assertEqual(n.actual_capacity(roles),dict(logical_F=20,source_configured_max_connections=100,effective_E=20,pending_multiplier=4,pending_P=80))
        for extras in (['--max-files-in-flight=4'],['--max-files-in-flight=unlimited'],['--max-files-in-flight'],['--max-files-in-flight=bad'],['--max-files-in-flight=0','--max-files-in-flight=0'],['--max-files-in-flight','0'],['--max-open-files=0'],['--forwarded-legacy-files-in-flight=0']):
            value=copy.deepcopy(roles);value['source']['argv']+=extras
            with self.subTest(extras=extras),self.assertRaises(ValueError):n.actual_capacity(value)

    def test_every_mutation_requires_private_user_net_mount_pid_and_nonzero_uid(self):
        n=self.transport();host={kind:f'{kind}:[1]' for kind in ('user','net','mnt','pid')};host.update(uid=1001,gid=42)
        for bad in ('user','net','mnt','pid','uid'):
            def namespace(kind):return host[kind] if kind==bad else f'{kind}:[2]'
            with mock.patch.object(n,'ns',side_effect=namespace),mock.patch.object(n.os,'getuid',return_value=0 if bad=='uid' else 1001),mock.patch.object(n.os,'getgid',return_value=42),mock.patch.object(n,'run_command',side_effect=AssertionError('host mutation')):
                with self.subTest(bad=bad),self.assertRaises(RuntimeError):n.mutate(['ip','link','add','bridge0'],host,{})

    def test_failure_ledger_keeps_command_error_before_postflight_and_cleanup(self):
        n=self.transport();log=n.FailureLog()
        log.add('command',RuntimeError('first copy failure'))
        log.add('postflight',RuntimeError('bad drops'))
        log.add('teardown',RuntimeError('cannot reap'))
        self.assertEqual(log.primary['message'],'first copy failure')
        self.assertEqual([item['stage'] for item in log.diagnostics],['postflight','teardown'])

    def test_series_omitted_zero_and_per_trial_lifetime_do_not_pool(self):
        n=self.transport();case=dict(id='tiny',directory_widths=[1],files_per_leaf=100,file_size_bytes=1);variant=dict(tool='rcp',args=['--summary'],processes=1)
        args=(case,variant,'source-verified','loopback','runner',{},{});
        legacy=run.series_id(*args,operation_revision=1)
        zero=run.series_id(*args,operation_revision=1,owned_transport=n.semantics(0))
        self.assertNotEqual(legacy,zero)
        self.assertNotEqual(zero,run.series_id(*args,operation_revision=1,owned_transport=n.semantics(2)))
        changed={**n.semantics(0),'lifetime':'block'}
        self.assertNotEqual(zero,run.series_id(*args,operation_revision=1,owned_transport=changed))



class LifecycleGateTests(unittest.TestCase):
    def provisional(self,ssh_identity=None):
        n=run.transport;before=evidence()
        if ssh_identity is not None:
            before['ssh'].update(binary_path=ssh_identity['path'],binary_sha256=ssh_identity['sha256'])
        raw=json.dumps(before);sha=hashlib.sha256(raw.encode()).hexdigest();after=copy.deepcopy(before);after['preflight_sha256']=sha
        for q in after['qdiscs'].values():q[0].update(packets=100,bytes=15000)
        return dict(outcome=dict(ok=True,elapsed_seconds=.25,exit_codes=[0],timed_out=False,logs=[],commands=[['/candidate/rcp']]),
                    errors=[],preflight_raw=raw,postflight=after,roles=role_records(),
                    pins_before={'rcp':'a'*64,'rcpd':'b'*64},pins_after={'rcp':'a'*64,'rcpd':'b'*64},
                    cleanup=dict(ok=True,private_pid_namespace=True,remaining_pids=[],reaped=True))

    def test_success_requires_postflight_cleanup_waited_zero_and_host_audit(self):
        n=run.transport;inner=self.provisional();tools=dict(rcp='/candidate/rcp',rcpd='/candidate/rcpd');pins={'rcp':'a'*64,'rcpd':'b'*64}
        result=n.qualify(inner,0,{'ok':True},'rcp',tools,pins,10000,dict(path='/tools/ssh',sha256='b'*64))
        self.assertTrue(result['ok']);self.assertEqual(result['elapsed_seconds'],.25)
        for failure in ('postflight','cleanup','outer','host','pins','command'):
            value=copy.deepcopy(inner);code=0;audit={'ok':True}
            if failure=='postflight':value.pop('postflight')
            if failure=='cleanup':value['cleanup']['ok']=False
            if failure=='outer':code=17
            if failure=='host':audit['ok']=False
            if failure=='pins':value['pins_after']['rcpd']='f'*64
            if failure=='command':value['outcome']['ok']=False;value['errors']=[dict(stage='command',type='RuntimeError',message='copy first',time=1)]
            with self.subTest(failure=failure):
                result=n.qualify(value,code,audit,'rcp',tools,pins,10000,dict(path='/tools/ssh',sha256='b'*64))
                self.assertFalse(result['ok'])
                if failure=='command':self.assertEqual(result['failure']['message'],'copy first')

    def test_stage_lifecycle_attempts_postflight_and_cleanup_preserving_copy_failure(self):
        n=run.transport;events=[]
        def fail(stage):
            events.append(stage);raise RuntimeError(stage)
        result=n.run_stages(lambda:events.append('setup'),lambda:fail('command'),lambda:fail('postflight'),lambda:fail('teardown'))
        self.assertEqual(events,['setup','command','postflight','teardown'])
        self.assertEqual([e['stage'] for e in result['errors']],['command','postflight','teardown'])
        events.clear()
        result=n.run_stages(lambda:fail('setup'),lambda:events.append('command'),lambda:events.append('postflight'),lambda:events.append('teardown'))
        self.assertEqual(events,['setup','teardown']);self.assertEqual(result['errors'][0]['stage'],'setup')

    def test_cancellation_keeps_primary_and_still_postflights_and_tears_down(self):
        n=run.transport;events=[]
        def cancel():raise KeyboardInterrupt('cancel first')
        value=n.run_stages(lambda:dict(ok=True),cancel,lambda:events.append('postflight'),lambda:events.append('teardown'))
        self.assertEqual(events,['postflight','teardown'])
        self.assertEqual(value['errors'][0]['category'],'cancellation')
        self.assertEqual(value['errors'][0]['message'],'cancel first')

    def test_repeated_cancellation_during_grace_still_kills_and_reaps_held_child(self):
        n=run.transport;child=mock.Mock();failures=n.FailureLog()
        failures.add('outer_wait',KeyboardInterrupt('first cancellation'))
        child.poll.return_value=None
        child.wait.side_effect=[InterruptedError('second cancellation'),KeyboardInterrupt('third cancellation'),-signal.SIGKILL]
        before={number:signal.getsignal(number) for number in (signal.SIGTERM,signal.SIGINT)}
        self.assertEqual(n.stop_launcher(child,grace=.05,failures=failures),-signal.SIGKILL)
        child.terminate.assert_called_once();child.kill.assert_called_once()
        self.assertEqual(child.wait.call_count,3)
        self.assertEqual(failures.primary['message'],'first cancellation')
        self.assertEqual([error['message'] for error in failures.diagnostics],['second cancellation','third cancellation'])
        self.assertEqual({number:signal.getsignal(number) for number in before},before)

    def test_sigterm_and_sigint_during_teardown_are_deferred_until_reap(self):
        n=run.transport;child=mock.Mock();failures=n.FailureLog();child.poll.return_value=None
        failures.add('outer_wait',InterruptedError('original cancellation'))
        def cancelled_wait(**kwargs):
            signal.raise_signal(signal.SIGTERM);signal.raise_signal(signal.SIGINT)
            raise subprocess.TimeoutExpired('fake owned launcher',kwargs['timeout'])
        calls=[cancelled_wait,lambda **kwargs:-signal.SIGKILL]
        child.wait.side_effect=lambda **kwargs:calls.pop(0)(**kwargs)
        before={number:signal.getsignal(number) for number in (signal.SIGTERM,signal.SIGINT)}
        self.assertEqual(n.stop_launcher(child,grace=.05,failures=failures),-signal.SIGKILL)
        child.kill.assert_called_once()
        self.assertEqual(failures.primary['message'],'original cancellation')
        self.assertEqual(len(failures.diagnostics),2)
        self.assertEqual({number:signal.getsignal(number) for number in before},before)

    def test_cancellation_reaps_only_held_fake_child_with_bounded_kill(self):
        n=run.transport
        with tempfile.TemporaryDirectory() as directory:
            ready=Path(directory)/'ready'
            child=subprocess.Popen([sys.executable,'-c',"import signal,time,pathlib,sys;signal.signal(signal.SIGTERM,signal.SIG_IGN);pathlib.Path(sys.argv[1]).write_text('ready');time.sleep(30)",str(ready)])
            try:
                import time
                deadline=time.monotonic()+3
                while not ready.exists() and time.monotonic()<deadline:time.sleep(.01)
                self.assertTrue(ready.exists())
                result=n.stop_launcher(child,grace=.05)
                self.assertEqual(result,-signal.SIGKILL)
                self.assertIsNotNone(child.poll())
            finally:
                if child.poll() is None:child.kill();child.wait(timeout=2)


class RunnerTransportTests(unittest.TestCase):
    def test_owned_variants_reject_before_tools_and_fixture_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);manifest=root/'manifest.json'
            manifest.write_text(json.dumps(dict(schema_version=1,cases=[dict(id='tiny',directory_widths=[1],files_per_leaf=1,file_size_bytes=1)],variants=[dict(id='custom',tool='rcp',args=['--summary','--max-files-in-flight=4'],processes=1)])))
            with mock.patch.object(run,'_revision',return_value=dict(commit=None,branch=None,dirty=None)),mock.patch.object(run,'_tool',side_effect=AssertionError('must reject before tools')),self.assertRaisesRegex(ValueError,'owned RTT'):
                run.main(['--manifest',str(manifest),'--case','tiny','--variant','custom','--mode','loopback','--rtt-ms','0','--output',str(root/'out')])
            self.assertEqual(json.loads((root/'out'/'results.json').read_text())['trials'],[])

    def exercise(self,failure=None):
        import shutil
        from benchmarks import report
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);manifest=root/'manifest.json';output=root/'out';calls=[]
            variant=dict(id='rcp-default',tool='rcp',args=['--summary'],processes=1)
            manifest.write_text(json.dumps(dict(schema_version=1,cases=[dict(id='tiny',directory_widths=[1],files_per_leaf=1,file_size_bytes=3)],variants=[variant])))
            def generate(command,**kwargs):
                self.assertEqual(Path(command[0]).name,'filegen')
                source=Path(command[1])/'filegen';(source/'leaf').mkdir(parents=True);(source/'leaf'/'file').write_bytes(b'abc')
                return subprocess.CompletedProcess(command,0,'','')
            def execute(variant,source,destination,tools,logs,folder,timeout,rtt,operation,counts,timing_policy,timing_prefix,trial_index,**kwargs):
                saved=json.loads((output/'results.json').read_text());trial=saved['trials'][-1]
                self.assertEqual(trial['status'],'running');self.assertEqual(trial['commands'],[])
                self.assertEqual(trial['transport_artifacts'],str(folder));self.assertTrue(trial['cache_validation']['ok'])
                calls.append(trial_index)
                shutil.copytree(source,destination)
                logs.mkdir(parents=True);stdout=logs/'0.stdout.log';stderr=logs/'0.stderr.log';stdout.write_text('files copied: 1\nfiles unchanged: 0\n');stderr.write_text('')
                inner=LifecycleGateTests().provisional(kwargs['ssh_identity']);inner['roles']['master']['binary']=tools['rcp'];inner['roles']['source']['binary']=inner['roles']['destination']['binary']=tools['rcpd']
                inner['outcome'].update(commands=[[tools['rcp'],'--summary',f'192.0.2.2:{source}',str(destination)]],logs=[dict(stdout=str(stdout),stderr=str(stderr))])
                inner['pins_before']=inner['pins_after']=kwargs['expected_pins']
                raw_before=dict(namespaces={kind:f'{kind}:[1]' for kind in run.transport.KINDS},uid=1001,gid=42,links=[],routes=[],qdiscs=[])
                audit=dict(ok=True,launcher_reaped=True,launcher_pid=42,before=raw_before,after=raw_before)
                if failure=='postflight':inner['postflight']['qdiscs']['to_client'][0]['drops']=1
                if failure=='cleanup':inner['cleanup']['ok']=False
                if failure=='host':audit['ok']=False
                code=5 if failure=='outer' else 0
                return run.transport.qualify(inner,code,audit,'rcp',{key:tools[key] for key in ('rcp','rcpd')},kwargs['expected_pins'],counts['bytes'],kwargs['ssh_identity'])
            args=['--manifest',str(manifest),'--case','tiny','--variant','rcp-default','--mode','loopback','--rtt-ms','2','--source-root',str(root),'--destination-root',str(root),'--bin-dir','/candidate','--cache','source-verified','--no-timings','--repetitions','2','--output',str(output)]
            with mock.patch.object(run,'_revision',return_value=dict(commit=None,branch=None,dirty=None)),mock.patch.object(run,'environment',return_value={}),mock.patch.object(run,'_tool',side_effect=lambda path,*args:dict(path=str(path),sha256='a'*64,version='test')),mock.patch.object(run,'_supports_timings',return_value=False),mock.patch.object(run.subprocess,'run',side_effect=generate),mock.patch.object(run.transport,'execute_trial',side_effect=execute),mock.patch.object(run.shutil,'which',return_value='/tools/ssh'):
                if failure:
                    with self.assertRaises(RuntimeError):run.main(args)
                else:run.main(args)
            saved=json.loads((output/'results.json').read_text())
            if failure:
                self.assertEqual(calls,[0]);self.assertEqual(saved['status'],'failed');self.assertEqual(saved['summaries'],[])
                self.assertEqual(saved['trials'][0]['status'],'failed')
                self.assertTrue(Path(saved['trials'][0]['commands'][0][-1]).exists())
            else:
                self.assertEqual(calls,[0,1]);self.assertEqual(saved['status'],'complete')
            report.validate_result(saved)
            if failure:
                previous=copy.deepcopy(saved);previous['context']['owned_transport'].pop('account_policy')
                report.validate_result(previous)
            if not failure:
                for mutation in ('outer','wait','cleanup','postflight','proof','host','context','ssh_identity','account_policy','account_proof'):
                    bad=copy.deepcopy(saved);proof=bad['trials'][0]['transport']
                    if mutation=='account_policy':bad['context']['owned_transport'].pop('account_policy')
                    if mutation=='account_proof':proof['postflight'].pop('account')
                    if mutation=='ssh_identity':bad['tools']['ssh']['sha256']='f'*64
                    if mutation=='context':bad['context'].pop('owned_transport');proof['outer_exit_code']=19
                    if mutation=='outer':proof['outer_exit_code']=5
                    if mutation=='wait':proof['outer_waited']=False
                    if mutation=='cleanup':proof['cleanup']['reaped']=False
                    if mutation=='postflight':proof['postflight']['qdiscs']['to_source'][0]['drops']=1
                    if mutation=='proof':proof['network']['byte_deltas']['to_client']=1
                    if mutation=='host':proof['host_audit']['after']['links']=[{'ifname':'leaked'}]
                    with self.subTest(mutation=mutation),self.assertRaises(ValueError):report.validate_result(bad)

    def test_runner_admits_after_gate_and_report_recomputes_all_proofs(self):self.exercise()

    def test_runner_never_admits_next_row_or_summary_after_failed_transport(self):
        for failure in ('postflight','cleanup','outer','host'):
            with self.subTest(failure=failure):self.exercise(failure)


class OwnershipTests(unittest.TestCase):
    def test_private_proc_guard_prevents_any_pid_scan_or_kill_in_host_proc(self):
        n=run.transport;host={kind:f'{kind}:[1]' for kind in n.KINDS};host.update(uid=1001,gid=42)
        with mock.patch.object(n,'assert_isolated'),mock.patch.object(n.os,'getpid',return_value=1),mock.patch.object(n,'ns',return_value='pid:[2]'),mock.patch.object(n.os,'readlink',return_value='pid:[1]'),mock.patch.object(n.Path,'iterdir',side_effect=AssertionError('host proc scan')),mock.patch.object(n.os,'kill',side_effect=AssertionError('host kill')):
            with self.assertRaisesRegex(RuntimeError,'private proc'):n.cleanup_namespace(host,[])

    def test_private_cleanup_only_signals_and_reaps_guarded_owned_pid_namespace(self):
        n=run.transport;owned={10:'alive',11:'alive'};guards=[];signals=[];reaped=[]
        def kill(pid,signum):signals.append((pid,signum));owned[pid]='dead'
        def waitpid(pid,options):
            self.assertEqual((pid,options),(-1,os.WNOHANG))
            for candidate,state in list(owned.items()):
                if state=='dead':
                    owned.pop(candidate);reaped.append(candidate);return candidate,0
            return 0,0
        with mock.patch.object(n,'private_proc',side_effect=lambda host:guards.append('guard')),mock.patch.object(n.Path,'iterdir',side_effect=lambda:[Path('/proc')/str(pid) for pid in owned]),mock.patch.object(n.os,'kill',side_effect=kill),mock.patch.object(n.os,'waitpid',side_effect=waitpid),mock.patch.object(n.signal,'signal'):
            result=n.cleanup_namespace({},[])
        self.assertTrue(result['reaped']);self.assertEqual(signals,[(10,signal.SIGTERM),(11,signal.SIGTERM)])
        self.assertEqual(reaped,[10,11]);self.assertGreaterEqual(len(guards),5)

    def test_helper_socket_and_holder_paths_fail_before_any_host_action(self):
        n=run.transport
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'request.json';path.write_text(json.dumps(dict(host={},folder=directory)))
            for action in ('_holder','_echo','_probe'):
                with self.subTest(action=action),mock.patch.object(n,'assert_isolated',side_effect=RuntimeError('not isolated')),mock.patch.object(n.socket,'socket',side_effect=AssertionError('host socket')),mock.patch.object(n,'save',side_effect=AssertionError('host holder')):
                    with self.assertRaisesRegex(RuntimeError,'not isolated'):n.helper_main([action,str(path),'source'])

    def test_environment_clears_injected_route_agent_and_parent_remains_unchanged(self):
        n=run.transport
        with mock.patch.dict(os.environ,dict(RSYNC_RSH='unsafe',SSH_AUTH_SOCK='/private/agent',LD_PRELOAD='/bad',XDG_RUNTIME_DIR='/host/runtime',LC_ALL='de_DE.UTF-8')):
            before=dict(os.environ);environment=n.clean_environment('/owned/bin')
            self.assertEqual(set(environment),{'PATH','HOME','USER','LOGNAME','SHELL','LANG','LC_ALL'})
            self.assertEqual(environment['LC_ALL'],'C');self.assertTrue(environment['PATH'].startswith('/owned/bin:'))
            self.assertEqual(dict(os.environ),before)

    def test_role_classification_and_path_bindings_are_explicit(self):
        n=run.transport
        self.assertEqual(n.role_name('rcpd',['--role','source']),'source')
        self.assertEqual(n.role_name('rcpd',['--role=destination']),'destination')
        self.assertIsNone(n.role_name('rcpd',['--protocol-version']))
        self.assertEqual(n.role_name('rsync',['--server','--sender']),'source')
        with self.assertRaises(ValueError):n.role_name('rcpd',['--role','source','--role=destination'])
        request=dict(source='/s',destination='/d',folder='/owned',log_dir='/logs',timing_prefix='/timings/trace',tools={'rcpd':'/selected/rcpd'})
        value=n.argv_description(['--timings=/timings/trace','--master-cert-fp','secret','192.0.2.2:/s/','/d'],request)
        self.assertEqual(value['operands'][2]['classification'],'credential')
        self.assertIn('source',value['operands'][3]['bindings'])
        self.assertEqual(value['bindings']['private_identity'],'/owned/identity')
        self.assertEqual(n.argv_description(['/some-other-source'],request)['operands'][0]['bindings'],[])
        self.assertEqual(value['operands'][3]['symbolic'],'${source-host}:${source}/')

    def test_role_exec_fails_before_file_write_or_execution_outside_owned_endpoint(self):
        n=run.transport
        request=dict(host={},folder='/unused',tools={'rcp':'/unused/rcp'})
        with mock.patch.object(n,'assert_isolated',side_effect=RuntimeError('not isolated')),mock.patch.object(n.os,'execve',side_effect=AssertionError('host exec')):
            with self.assertRaisesRegex(RuntimeError,'not isolated'):n.record_role(request,'rcp',['--summary'])

    def test_mount_and_network_setup_cannot_run_after_guard_failure(self):
        n=run.transport
        with tempfile.TemporaryDirectory() as directory:
            request=dict(folder=directory,host={})
            trial=n.NamespaceTrial(request)
            with mock.patch.object(n,'assert_isolated',side_effect=RuntimeError('inherited mount namespace')),mock.patch.object(trial,'mutate',side_effect=AssertionError('host mutation')):
                with self.assertRaisesRegex(RuntimeError,'inherited mount'):trial.setup()


class SshConfigPathTests(unittest.TestCase):
    def test_literal_paths_preserve_openssh_argument_boundaries(self):
        quote=run.transport.ssh_config_path
        for path in ('/tmp/plain', '/tmp/space name', '/tmp/"double" and \'single\'',
                     '/tmp/back\\slash', '/tmp/escaped\\"quote', '/tmp/trailing\\',
                     '/tmp/%h-%%-${HOME}'):
            with self.subTest(path=path):
                self.assertEqual(shlex.split('HostKey '+quote(path)),['HostKey',path])
                self.assertTrue(quote(path).startswith('"') and quote(path).endswith('"'))

    def test_literal_paths_reject_line_and_nul_injection(self):
        for character in ('\n','\r','\0'):
            with self.subTest(character=character),self.assertRaisesRegex(ValueError,'SSH configuration path'):
                run.transport.ssh_config_path('/tmp/bad'+character+'HostKey other')


class PrivateAccountTests(unittest.TestCase):
    def account(self, home='/original/home'):
        import pwd
        return pwd.struct_passwd(('owned', 'x', os.getuid(), os.getgid(), 'Test User', home, '/bin/sh'))

    def test_private_passwd_changes_only_caller_home_and_rejects_ambiguous_fields(self):
        n=run.transport;account=self.account()
        original=f'root:x:0:0:root:/root:/bin/sh\nowned:x:{account.pw_uid}:{account.pw_gid}:Test User:/original/home:/bin/sh\n'
        self.assertEqual(n.render_account_database(original,account,Path('/tmp/owned-trial')),original.replace('/original/home','/tmp/owned-trial'))
        for text,home in ((original,Path('/tmp/bad:home')),(original,Path('/tmp/bad\nhome')),(original,Path('relative')),
                          (original+original.splitlines()[1]+'\n',Path('/tmp/owned')),('root:x:0:0:root:/root:/bin/sh\n',Path('/tmp/owned')),
                          (original.replace('Test User','different'),Path('/tmp/owned')),(original+'malformed\n',Path('/tmp/owned'))):
            with self.subTest(home=str(home)),self.assertRaises(ValueError):n.render_account_database(text,account,home)

    def test_account_view_preserves_opaque_locked_and_encoded_fields(self):
        import pwd
        n=run.transport
        for field in ('', '!', '*', '!!', '$6$synthetic$encoded', 'opaque-marker'):
            with self.subTest(field=field):
                account=pwd.struct_passwd(('owned', field, os.getuid(), os.getgid(), 'Test User', '/original/home', '/bin/sh'))
                original=f'root:*:0:0:root:/root:/bin/sh\nowned:{field}:{account.pw_uid}:{account.pw_gid}:Test User:/original/home:/bin/sh\n'
                self.assertEqual(n.render_account_database(original,account,Path('/tmp/owned-trial')),original.replace('/original/home','/tmp/owned-trial'))

    def test_account_bind_guards_all_writes_and_refuses_wrong_nss_lookup(self):
        n=run.transport
        with tempfile.TemporaryDirectory() as directory:
            trial=n.NamespaceTrial(dict(folder=directory,host={}))
            with mock.patch.object(n,'assert_isolated',side_effect=RuntimeError('not isolated')),mock.patch.object(trial,'mutate',side_effect=AssertionError('host mount')):
                with self.assertRaisesRegex(RuntimeError,'not isolated'):trial.setup_account()
            self.assertEqual(list(Path(directory).iterdir()),[])
            account=self.account();text=f'owned:x:{account.pw_uid}:{account.pw_gid}:Test User:/original/home:/bin/sh\n'
            with mock.patch.object(n,'assert_isolated'),mock.patch.object(n.pwd,'getpwuid',return_value=account),mock.patch.object(n.pwd,'getpwnam',return_value=account),mock.patch.object(n.Path,'read_text',return_value=text),mock.patch.object(trial,'mutate') as mutate:
                with self.assertRaisesRegex(RuntimeError,'account lookup'):trial.setup_account()
            self.assertEqual(mutate.call_count,2)

    def test_account_observation_rechecks_readonly_mount_digest_and_nss_fields(self):
        n=run.transport
        with tempfile.TemporaryDirectory() as directory:
            folder=Path(directory);folder.chmod(0o700)
            trial=n.NamespaceTrial(dict(folder=directory,host={}));original=self.account();effective=self.account(str(folder.resolve()))
            trial.private_account=original;trial.private_passwd_sha256='a'*64
            mounts='1 0 0:1 / /etc/passwd ro,nosuid - ext4 /owned/passwd rw\n'
            with mock.patch.object(n.pwd,'getpwuid',return_value=effective),mock.patch.object(n.pwd,'getpwnam',return_value=effective),mock.patch.object(n.Path,'read_text',return_value=mounts),mock.patch.object(n,'digest',return_value='a'*64):
                self.assertTrue(trial.account_evidence()['passwd_readonly'])
                for changed in ('mount','digest','nss','mode'):
                    if changed=='mount':patch=mock.patch.object(n.Path,'read_text',return_value=mounts.replace('ro,nosuid','rw,nosuid'))
                    if changed=='digest':patch=mock.patch.object(n,'digest',return_value='b'*64)
                    if changed=='nss':patch=mock.patch.object(n.pwd,'getpwnam',return_value=original)
                    if changed=='mode':folder.chmod(0o777);patch=contextlib.nullcontext()
                    with self.subTest(changed=changed),patch,self.assertRaises(RuntimeError):trial.account_evidence()
                folder.chmod(0o700)

    def test_mount_child_reads_held_parent_descriptor_after_source_name_replacement(self):
        n=run.transport
        with tempfile.TemporaryDirectory() as directory:
            folder=Path(directory);trial=n.NamespaceTrial(dict(folder=directory,host={},utilities={}))
            account=self.account();effective=self.account(str(folder.resolve()))
            text=f'owned:x:{account.pw_uid}:{account.pw_gid}:Test User:/original/home:/bin/sh\n'
            copied=folder/'observed';mounted=[]
            helper=folder/'tiny-mount'
            helper.write_text('#!'+sys.executable+"\nimport pathlib,sys\nassert '--no-canonicalize' in sys.argv\npathlib.Path(sys.argv[-1]).write_bytes(pathlib.Path(sys.argv[-2]).read_bytes())\n")
            helper.chmod(0o700)
            def mount(argv):
                mounted.append(argv)
                if '--bind' in argv:
                    (folder/'passwd').rename(folder/'held-passwd');(folder/'passwd').write_text('replacement')
                    n.mutate([str(helper),*argv[1:-1],str(copied)],{},trial.environment)
            def lookup(_):return effective if mounted else account
            with mock.patch.object(n,'assert_isolated'),mock.patch.object(n.pwd,'getpwuid',side_effect=lookup),mock.patch.object(n.pwd,'getpwnam',side_effect=lookup),mock.patch.object(n.Path,'read_text',return_value=text),mock.patch.object(trial,'mutate',side_effect=mount),mock.patch.object(trial,'account_evidence',return_value={}):
                trial.setup_account()
            self.assertEqual(copied.read_text(),text.replace('/original/home',str(folder.resolve())))
            self.assertEqual((folder/'passwd').read_text(),'replacement')

    def test_new_evidence_requires_readonly_private_home_and_preserved_account_proof(self):
        n=run.transport;value=evidence()
        value['account']=dict(home='/owned',home_uid=1001,home_mode=0o700,passwd_readonly=True,
                              passwd_sha256='f'*64,nss_matches=True,other_fields_preserved=True)
        n.validate_evidence(value,2)
        for field,bad in (('home','/tmp'),('home_uid',0),('home_mode',0o777),('passwd_readonly',False),('passwd_sha256','bad'),('nss_matches',False),('other_fields_preserved',False)):
            changed=copy.deepcopy(value);changed['account'][field]=bad
            with self.subTest(field=field),self.assertRaises(ValueError):n.validate_evidence(changed,2)
        value.pop('account')
        with self.assertRaises(ValueError):n.validate_evidence(value,2)

    def test_tmp_output_ssh_configuration_uses_effective_private_home_with_strict_modes(self):
        n=run.transport
        with tempfile.TemporaryDirectory() as directory:
            folder=Path(directory)/'space "quote" \'single\' \\slash %h ${HOME}';folder.mkdir(mode=0o700)
            account=self.account();effective=self.account(str(folder.resolve()))
            trial=n.NamespaceTrial(dict(folder=str(folder),host={},utilities={'nsenter':'nsenter','sshd':'sshd','ssh':'ssh'}))
            trial.endpoints={role:dict(pid=10+index,netns=f'net:[{10+index}]') for index,role in enumerate(n.ADDRESSES)}
            text=f'owned:x:{account.pw_uid}:{account.pw_gid}:Test User:/original/home:/bin/sh\n'
            mounted=[]
            def mount(argv):
                mounted.append(argv)
                if '--bind' in argv:
                    held=Path(argv[-2]);self.assertEqual(held.read_text(),text.replace('/original/home',str(folder.resolve())))
                    self.assertEqual(held.stat().st_mode & 0o777,0o600)
            def lookup(_):return effective if mounted else account
            def execute(argv):
                if argv[0]=='ssh-keygen':Path(argv[-1]+'.pub').write_text('synthetic public key\n')
            def answer(argv,*args):
                role='source' if n.ADDRESSES['source'] in argv else 'client'
                return subprocess.CompletedProcess(argv,0,stdout='\n'.join([str(os.getuid()),str(os.getgid()),trial.endpoints[role]['netns'],*[f'{kind}:[2]' for kind in ('user','mnt','pid')]])+'\n')
            real_read_text=Path.read_text
            def read_text(path,*args,**kwargs):return text if str(path)=='/etc/passwd' else real_read_text(path,*args,**kwargs)
            with mock.patch.object(n,'assert_isolated'),mock.patch.object(n.pwd,'getpwuid',side_effect=lookup),mock.patch.object(n.pwd,'getpwnam',side_effect=lookup),mock.patch.object(n.Path,'read_text',autospec=True,side_effect=read_text),mock.patch.object(trial,'mutate',side_effect=mount),mock.patch.object(trial,'run',side_effect=execute),mock.patch.object(trial,'account_evidence',return_value={}),mock.patch.object(trial,'owned'),mock.patch.object(n,'ready',side_effect=answer),mock.patch.object(n,'ns',side_effect=lambda kind:f'{kind}:[2]'):
                trial.setup_ssh()
            self.assertEqual(mounted[-1],['mount','-o','remount,bind,ro','/etc/passwd'])
            self.assertEqual(trial.environment['HOME'],str(folder.resolve()))
            for role in n.ADDRESSES:
                config=(folder/f'{role}.sshd_config').read_text()
                self.assertIn('StrictModes yes',config)
                directives={words[0]:words[1:] for line in config.splitlines() if (words:=shlex.split(line))}
                self.assertEqual(directives['HostKey'],[str(folder/f'{role}.hostkey')])
                self.assertEqual(directives['PidFile'],[str(folder/f'{role}.sshd.pid')])
                self.assertEqual(directives['AuthorizedKeysFile'],['%h/authorized_keys'])
            config=(folder/'ssh_config').read_text()
            directives={words[0]:words[1:] for line in config.splitlines() if (words:=shlex.split(line))}
            self.assertEqual(directives['IdentityFile'],['%d/identity'])
            self.assertEqual(directives['UserKnownHostsFile'],['%d/known_hosts'])
            # the SSH server walks the key's ancestors only up to the effective home: /tmp is excluded
            key=folder/'authorized_keys';cursor=key.parent;visited=[]
            while True:
                visited.append(cursor);self.assertEqual(cursor.stat().st_uid,os.getuid());self.assertEqual(cursor.stat().st_mode & 0o022,0)
                if cursor==Path(effective.pw_dir):break
                cursor=cursor.parent
            self.assertNotIn(Path('/tmp'),visited)
            self.assertNotEqual(Path('/tmp').stat().st_mode & 0o022,0)


class HostLauncherTests(unittest.TestCase):
    def test_script_entry_uses_canonical_module_for_real_planner_and_executor(self):
        n=run.transport
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);folder=root/'transport';folder.mkdir();(folder/'bin').mkdir()
            source=root/'source';source.mkdir();(source/'file').write_bytes(b'abc')
            executable=root/'tiny-tool'
            executable.write_text('#!'+sys.executable+"\nimport json,sys\nprint('tiny actual command')\nprint(json.dumps(sys.argv[1:]))\n")
            executable.chmod(0o700)
            driver=root/'python-driver';bootstrap=root/'bootstrap.py'
            import shlex
            driver.write_text('#!/bin/sh\nexec '+shlex.quote(sys.executable)+' '+shlex.quote(str(bootstrap))+' "$@"\n');driver.chmod(0o700)
            bootstrap.write_text("""import os,runpy,sys
from pathlib import Path
script=sys.argv[1]
sys.path.insert(0,str(Path(script).parent.parent))
real_readlink=os.readlink
def private_namespaces(path,*args,**kwargs):
    names={'user':'user:[2]','net':'net:[4]','mnt':'mnt:[2]','pid':'pid:[2]'}
    if str(path).startswith('/proc/self/ns/'):
        return names[Path(path).name]
    return real_readlink(path,*args,**kwargs)
os.readlink=private_namespaces
sys.executable=str(Path(__file__).with_name('python-driver'))
sys.argv=sys.argv[1:]
runpy.run_path(script,run_name='__main__')
""")
            ssh=folder/'bin'/'ssh';ssh.write_text('#!/bin/sh\nexit 0\n');ssh.chmod(0o700)
            n.save(folder/'client.ready.json',dict(netns='net:[4]'))
            n.save(folder/'source.ready.json',dict(netns='net:[3]'))
            n.save(folder/'evidence-before.json',evidence())
            tools={'rcp':str(executable),'rcpd':str(executable)};utilities={'python':str(driver)}
            request=dict(folder=str(folder),host={**{kind:f'{kind}:[1]' for kind in n.KINDS},'uid':os.getuid(),'gid':os.getgid()},
                helper_pins=n.helper_pins(),utilities=utilities,utility_pins=n.pin_tools(utilities),tools=tools,pins=n.pin_tools(tools),
                variant=dict(id='rcp-default',tool='rcp',args=['--summary'],processes=1),source=str(source),destination=str(root/'destination'),
                timing_policy='unsupported',timing_prefix=str(root/'trace'),log_dir=str(root/'command-logs'),timeout=3,operation='fresh',trial_index=0)
            n.save(folder/'request.json',request)
            environment={**os.environ,'PATH':str(folder/'bin')+':'+os.environ['PATH'],'LC_ALL':'C','LANG':'C'}
            result=subprocess.run([str(driver),str(n.SCRIPT),'_copy',str(folder/'request.json')],text=True,capture_output=True,timeout=5,env=environment)
            copied=json.loads((folder/'copy-result.json').read_text())
            self.assertIn('outcome',copied,copied.get('failure'))
            self.assertTrue(copied['outcome']['ok'],copied)
            self.assertEqual(copied['outcome']['exit_codes'],[0])
            command=copied['outcome']['commands'][0]
            self.assertEqual(command[-2:],['192.0.2.2:'+str(source),str(root/'destination')])
            output=(root/'command-logs'/'0.stdout.log').read_text().splitlines()
            self.assertEqual(output[0],'tiny actual command')
            self.assertEqual(json.loads(output[1]),command[1:])
            # no fake remote roles were supplied: command execution cannot claim owned admission
            self.assertEqual(result.returncode,1)
            self.assertEqual(copied['failure']['message'],'wrong role cardinality')

    def test_changed_selected_ssh_is_rejected_before_namespace_launch(self):
        n=run.transport
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            def which(name):
                self.assertNotEqual(name,'ssh','selected SSH must come from runner provenance')
                return '/tools/'+name
            with mock.patch.object(n.os,'getuid',return_value=1001),mock.patch.object(n.shutil,'which',side_effect=which),mock.patch.object(n,'pin_tools',side_effect=lambda tools:{key:'f'*64 if key=='ssh' else 'a'*64 for key in tools}),mock.patch.object(n.subprocess,'Popen',side_effect=AssertionError('must fail before launcher')),mock.patch.object(n,'host_state',side_effect=AssertionError('must fail before topology setup')):
                with self.assertRaisesRegex(ValueError,'SSH executable changed'):
                    n.execute_trial(dict(id='rcp-default',tool='rcp',args=['--summary'],processes=1),root/'source',root/'destination',dict(rcp='/candidate/rcp',rcpd='/candidate/rcpd',ssh='/tools/ssh'),root/'logs',root/'transport',1,2,'fresh',dict(bytes=100),'unsupported',root/'trace',0,expected_pins={'rcp':'a'*64,'rcpd':'a'*64},ssh_identity=dict(path='/tools/ssh',sha256='a'*64))

    def test_host_boundary_uses_actual_waited_exit_and_persists_request_before_launch(self):
        n=run.transport
        for code in (0,19):
            with self.subTest(code=code),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);folder=root/'transport';host=dict(namespaces={kind:f'{kind}:[1]' for kind in n.KINDS},uid=1001,gid=42,links=[],routes=[],qdiscs=[])
                child=mock.Mock(pid=4242,returncode=code)
                child.wait.return_value=code
                child.poll.return_value=code
                def launch(argv,**kwargs):
                    self.assertTrue((folder/'request.json').exists())
                    self.assertNotIn('--mount-proc',argv)
                    self.assertIn('--kill-child=SIGKILL',argv)
                    self.assertTrue(kwargs['start_new_session'])
                    inner=LifecycleGateTests().provisional(dict(path='/tools/ssh',sha256='a'*64))
                    inner['pins_before']=inner['pins_after']={'rcp':'a'*64,'rcpd':'a'*64}
                    n.save(folder/'provisional.json',inner)
                    return child
                with mock.patch.object(n.os,'getuid',return_value=1001),mock.patch.object(n.shutil,'which',side_effect=lambda name:'/tools/'+name),mock.patch.object(n,'host_state',return_value=host),mock.patch.object(n,'process_start_identity',return_value='1234'),mock.patch.object(n,'clean_environment',return_value={'PATH':'/tools','LC_ALL':'C'}),mock.patch.object(n,'pin_tools',side_effect=lambda tools:{key:'d'*64 if key=='python' else 'a'*64 for key in tools}),mock.patch.object(n,'digest',return_value='d'*64),mock.patch.object(n.subprocess,'Popen',side_effect=launch):
                    result=n.execute_trial(dict(id='rcp-default',tool='rcp',args=['--summary'],processes=1),root/'source',root/'destination',dict(rcp='/candidate/rcp',rcpd='/candidate/rcpd',ssh='/tools/ssh'),root/'logs',folder,1,2,'fresh',dict(bytes=100), 'unsupported',root/'trace',0,expected_pins={'rcp':'a'*64,'rcpd':'a'*64},ssh_identity=dict(path='/tools/ssh',sha256='a'*64))
                child.wait.assert_called_once()
                self.assertEqual(result['ok'],code==0)
                self.assertEqual(result['transport']['outer_exit_code'],code)
                self.assertTrue((folder/'qualified.json').exists())


class CopyBoundaryTests(unittest.TestCase):
    def test_host_hands_off_log_directory_to_real_inner_command_executor(self):
        n=run.transport;real_popen=subprocess.Popen
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);folder=root/'transport';logs=root/'command-logs';copy_entered=[False]
            host=dict(namespaces={kind:f'{kind}:[1]' for kind in n.KINDS},uid=1001,gid=42,links=[],routes=[],qdiscs=[])
            child=mock.Mock(pid=4242,returncode=0);child.wait.return_value=child.poll.return_value=0
            def which(name):return str(folder/'bin'/'ssh') if copy_entered[0] and name=='ssh' else '/tools/'+name
            def launch(argv,**kwargs):
                if argv[0]!='/tools/unshare':return real_popen(argv,**kwargs)
                request=json.loads((folder/'request.json').read_text())
                (folder/'bin').mkdir()
                inner=LifecycleGateTests().provisional(dict(path='/tools/ssh',sha256='a'*64))
                (folder/'evidence-before.json').write_text(inner['preflight_raw'])
                for role,value in inner['roles'].items():n.save(folder/f'role-{role}.json',value)
                copy_entered[0]=True
                code=n.copy_worker(request)
                copied=json.loads((folder/'copy-result.json').read_text())
                inner.update(copied)
                n.save(folder/'provisional.json',inner)
                self.assertEqual(code,0,copied.get('failure'))
                return child
            tiny=[sys.executable,'-c',"print('tiny inner child')"]
            with mock.patch.object(n.os,'getuid',return_value=1001),mock.patch.object(n.shutil,'which',side_effect=which),mock.patch.object(n,'host_state',return_value=host),mock.patch.object(n,'process_start_identity',return_value='1234'),mock.patch.object(n,'clean_environment',return_value={'PATH':os.environ['PATH'],'LC_ALL':'C','LANG':'C'}),mock.patch.object(n,'pin_tools',side_effect=lambda tools:{key:'d'*64 if key=='python' else 'a'*64 for key in tools}),mock.patch.object(n,'digest',return_value='d'*64),mock.patch.object(n,'endpoint_guard'),mock.patch.object(run,'plan_commands',return_value=[tiny]),mock.patch.dict(os.environ,{'LC_ALL':'C'}),mock.patch.object(n.subprocess,'Popen',side_effect=launch):
                result=n.execute_trial(dict(id='rcp-default',tool='rcp',args=['--summary'],processes=1),root/'source',root/'destination',dict(rcp='/candidate/rcp',rcpd='/candidate/rcpd',ssh='/tools/ssh'),logs,folder,3,2,'fresh',dict(bytes=100),'unsupported',root/'trace',0,expected_pins={'rcp':'a'*64,'rcpd':'a'*64},ssh_identity=dict(path='/tools/ssh',sha256='a'*64))
            self.assertTrue(result['ok'],result.get('failure'))
            self.assertEqual(result['exit_codes'],[0])
            self.assertEqual((logs/'0.stdout.log').read_text().strip(),'tiny inner child')


if __name__=='__main__':unittest.main()
