"""Explicit private-text projection of validated results; never copy raw dictionaries."""
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re

from benchmarks import measurements, pairs, clocks

EXPORT_SCHEMA_VERSION = 1
TOOLS = ('rcp', 'rcpd', 'rsync', 'cp', 'filegen', 'ssh', 'rcp-baseline', 'rcpd-baseline')
ROLES = ('master', 'source', 'destination')
HASH = re.compile(r'[0-9a-f]{64}\Z')
COMMIT = re.compile(r'[0-9a-f]{7,64}\Z')
COUNT_FIELDS = ('files', 'directories', 'bytes', 'files_copied', 'files_unchanged', 'bytes_copied', 'entries', 'stale_files')
PROOFS = {'content':'validation', 'seed':'seed_validation', 'cache':'cache_validation',
          'metadata':'metadata_validation', 'source':'source_validation'}
FILESYSTEMS = {'ext4','ext3','xfs','zfs','btrfs','tmpfs','overlay','nfs','nfs4','fuse','fuseblk','f2fs','bcachefs'}
MOUNT_FLAGS = {'rw','ro','nosuid','nodev','noexec','relatime','noatime','strictatime','sync','async','lazytime','acl','noacl','seclabel','dirsync'}
STAGES = {'setup','command','postflight','teardown','qualification','copy_worker','outer_wait','outer_launch',
          'outer_teardown','host_audit','inner_record','outer_teardown_cancellation'}
CATEGORIES = {'validation','execution','timeout','cancellation'}


def enum(value, allowed):
    return value if isinstance(value,str) and value in allowed else None


def number(value, integer=False, minimum=0):
    if type(value) not in ((int,) if integer else (int,float)) or value < minimum or (type(value) is float and not math.isfinite(value)):
        return None
    return value


def boolean(value):
    return value if type(value) is bool else None


def fingerprint(value):
    return value if isinstance(value,str) and HASH.fullmatch(value) else None


def counts(value):
    value = value if isinstance(value,dict) else {}
    return {key:number(value[key],integer=True) for key in COUNT_FIELDS if key in value}


def cpu_quota(value):
    if isinstance(value,str) and re.fullmatch(r'(?:max|[0-9]+) [0-9]+(?:; (?:max|[0-9]+) [0-9]+)*',value):
        return value
    return None


def version(value):
    # only complete, known version formats; arbitrary trailing build/output text is withheld
    patterns = [r'[0-9]+(?:\.[0-9]+){1,3}',
                r'(?:rcp|rcpd|filegen) [0-9]+(?:\.[0-9]+){1,3}',
                r'rsync  version [0-9]+(?:\.[0-9]+){1,3}  protocol version [0-9]+',
                r'cp \(GNU coreutils\) [0-9]+(?:\.[0-9]+){1,3}',
                r'OpenSSH_[0-9]+\.[0-9]+p[0-9]+(?:, OpenSSL [0-9]+\.[0-9]+\.[0-9]+ [0-9]+ [A-Z][a-z]{2} [0-9]{4})?']
    return value if isinstance(value,str) and any(re.fullmatch(pattern,value) for pattern in patterns) else None


def proof(value, admitted):
    value = value if isinstance(value,dict) else {}
    ok = boolean(value.get('ok'))
    qualification = 'unknown' if ok is None else 'failed' if not ok else 'passed' if admitted else 'recorded-unqualified'
    result = dict(qualification=qualification, recorded_ok=ok, counts=counts(value.get('counts')), digest=fingerprint(value.get('digest')))
    for key in ('entries','stale_files'):
        if key in value:result[key]=number(value[key],integer=True)
    fields=value.get('checked_fields')
    result['checked_fields']=[enum(field,{'mode','uid','gid','mtime_ns'}) for field in fields] if isinstance(fields,list) else None
    for nested in ('metadata','modes'):
        raw=value.get(nested)
        if isinstance(raw,dict):
            result[nested]=dict(recorded_ok=boolean(raw.get('ok')),entries=number(raw.get('entries'),integer=True),
                               checked_fields=[enum(field,{'mode','uid','gid','mtime_ns'}) for field in raw.get('checked_fields',[])] if isinstance(raw.get('checked_fields'),list) else None)
    for key in ('independent_files','exact_seed_mtimes'):
        if key in value:result[key]=boolean(value[key])
    return result


def conditions(context):
    environment=context.get('environment')
    environment=environment if isinstance(environment,dict) else {}
    filesystems=environment.get('filesystem')
    filesystems=filesystems if isinstance(filesystems,dict) else {}
    result=dict(runner_alias='runner-1',topology=enum(context.get('topology'),{'local','loopback'}),
                purpose=enum(context.get('purpose'),{'smoke','performance','diagnostic'}),
                cache_policy=enum(context.get('cache_policy'),{'uncontrolled','source-warm','source-verified','linux-drop-caches'}),
                operation_contract_revision=number(context.get('operation_contract_revision'),integer=True),
                cache_contract_revision=number(context.get('cache_contract_revision'),integer=True),
                child_summary_locale=enum(context.get('summary_locale'),{'C'}),
                measurement='command-completion; durability not measured',
                shared_host=True if context.get('topology') in ('local','loopback') else None,filesystem={})
    for key in ('effective_parallelism','fd_limit'):
        result[key]=number(environment.get(key),integer=True)
    baseline=context.get('baseline_commit')
    known=isinstance(baseline,str) and COMMIT.fullmatch(baseline) is not None
    result['baseline_source_pin']=dict(commit=baseline if known else None,qualification='declared-unverified' if known else 'unknown' if baseline is None else 'withheld')
    result['cpu_quota']=cpu_quota(environment.get('cpu_quota'))
    result['architecture']=enum(environment.get('architecture'),{'x86_64','aarch64','arm64','i686','armv7l','riscv64','ppc64le','s390x'})
    for side in ('source','destination'):
        raw=filesystems.get(side);raw=raw if isinstance(raw,dict) else {}
        flags=raw.get('mount_options');flags=flags if isinstance(flags,list) else []
        result['filesystem'][side]=dict(storage_alias='storage-'+side,filesystem_type=enum(raw.get('filesystem_type'),FILESYSTEMS),
                                       mount_flags=sorted({flag for flag in flags if isinstance(flag,str) and flag in MOUNT_FLAGS}),parameters_withheld=True)
    return result


def diagnostic(value, artifact_id):
    value=value if isinstance(value,dict) else {}
    return dict(stage=enum(value.get('stage'),STAGES) or 'unknown',category=enum(value.get('category'),CATEGORIES) or 'unknown',
                artifact_id=artifact_id,raw_text_withheld=True)


# command operands come from captured bindings, never guesses from path strings
SAFE_FLAGS = {'--summary','--overwrite','--force-remote','-rp','--stats','-a','-aH','--server','--sender','.',
              '--role','source','destination','--overwrite-compare=size,mtime','--network-profile=datacenter'}
NUMBER_FLAGS = {'--max-connections','--max-files-in-flight','--max-open-files','--pending-writes-multiplier',
                '--resolved-automatic-files-in-flight','--max-workers','--max-blocking-threads','--ops-throttle',
                '--iops-throttle','--chunk-size','--overwrite-manifest-max-entries','--remote-copy-conn-timeout-sec',
                '--remote-keepalive-sec'}
BINDINGS = {'source','destination','folder','log_dir','timing_prefix','ssh_launcher','ssh_config','known_hosts',
            *('tool:'+key for key in TOOLS),*('wrapper:'+key for key in ('rcp','rcpd','rsync'))}
PATH_FLAGS = {'--timings':'timing_prefix','--rcpd-path':'wrapper:rcpd','--rsync-path':'wrapper:rsync','--rsh':'ssh_launcher'}
TIMING_STATUS = {'disabled','coarse','unsupported','not_applicable'}
CAPACITY_FIELDS = ('logical_F','source_configured_max_connections','effective_E','pending_multiplier','pending_P')


def safe_flag(value):
    if not isinstance(value,str):return None
    if value in SAFE_FLAGS:return value
    flag,separator,argument=value.partition('=')
    if separator and flag in NUMBER_FLAGS and argument.isascii() and argument.isdecimal():return value
    if flag=='--role' and separator and argument in ROLES:return value
    return None


def symbolic_operand(actual, operand, bindings):
    if not isinstance(operand,dict) or operand.get('classification')!='bound-path':return None
    names=operand.get('bindings');symbol=operand.get('symbolic')
    if not isinstance(names,list) or len(names)!=1 or not isinstance(names[0],str) or names[0] not in BINDINGS:return None
    name=names[0];raw=bindings.get(name)
    if not isinstance(raw,str) or not isinstance(symbol,str):return None
    pairs=[('${'+name+'}',raw)]
    if name in ('source','destination'):pairs.append(('${'+name+'}/',raw+'/'))
    if name=='source':pairs.extend([('${source-host}:${source}', '192.0.2.2:'+raw),('${source-host}:${source}/','192.0.2.2:'+raw+'/')])
    pairs.extend((flag+'=${'+name+'}',flag+'='+raw) for flag,binding in PATH_FLAGS.items() if binding==name)
    return symbol if any(symbol==expected and actual==bound for expected,bound in pairs) else None


def command(argv, description, index, role, tool):
    argv=argv if isinstance(argv,list) else []
    description=description if isinstance(description,dict) else {}
    bindings=description.get('bindings');bindings=bindings if isinstance(bindings,dict) else {}
    operands=description.get('operands');operands=operands if isinstance(operands,list) else []
    by_index={};duplicate=set()
    for item in operands:
        if not isinstance(item,dict) or type(item.get('index')) is not int:continue
        slot=item['index']
        if slot in by_index:duplicate.add(slot)
        by_index[slot]=item
    result=[]
    for slot,actual in enumerate(argv):
        item=by_index.get(slot,{})
        labels=item.get('bindings')
        private_identity=isinstance(labels,list) and 'private_identity' in labels
        if slot in duplicate or item.get('classification')=='credential' or private_identity:
            result.append(None);continue
        result.append(symbolic_operand(actual,item,bindings) if item.get('classification')=='bound-path' else safe_flag(actual))
    complete=enum(tool,TOOLS) is not None and bool(description) and set(by_index)==set(range(len(argv))) and not duplicate and all(value is not None for value in result) and all(item.get('classification') in ('bound-path','opaque-private') for item in by_index.values())
    return dict(command_index=index,role=enum(role,ROLES),tool_identity_key=enum(tool,TOOLS),argv=result,
                fully_reproducible=complete,unclassified_operands_withheld=any(value is None for value in result))


def resources(value):
    value=value if isinstance(value,dict) else {}
    affinity=value.get('cpu_affinity')
    return dict(cpu_affinity=[number(cpu,integer=True) for cpu in affinity] if isinstance(affinity,list) else None,
                cpu_parallelism=number(value.get('cpu_parallelism',len(affinity) if isinstance(affinity,list) and all(type(cpu) is int and cpu>=0 for cpu in affinity) else None),integer=True),
                cpu_quota=cpu_quota(value.get('cpu_quota')),fd_soft=number(value.get('fd_soft'),integer=True),
                fd_hard=number(value.get('fd_hard'),integer=True,minimum=-1))


def timing(value):
    value=value if isinstance(value,dict) else {};reports=[]
    for index,raw in enumerate(value.get('reports',[]) if isinstance(value.get('reports'),list) else []):
        scopes=[]
        for ordinal,scope in enumerate(raw['scopes']):
            scopes.append(dict(alias=f'scope-{ordinal+1}',name=enum(scope.get('name'),{'operation','file.copy','directory.create'}),
                **{key:number(scope.get(key),integer=key in ('count','finished','interrupted')) for key in ('count','finished','interrupted','total_seconds','mean_seconds','p50_seconds','p95_seconds','max_seconds')}))
        reports.append(dict(alias=f'timing-{index+1}',role=enum(raw.get('identifier'),{'rcp-master','rcpd-source','rcpd-destination'}),scopes=scopes))
    return dict(status=enum(value.get('status'),TIMING_STATUS),reports=reports,identifiers_withheld=True)


def role_identity(variant, role, value, tools, proof, admitted):
    """A descriptive key cannot override the selected, bound executable identity."""
    kind = ('rcp' if role=='master' else 'rcpd') if variant.get('tool')=='rcp' else 'rsync' if variant.get('tool')=='rsync' and role in ('source','destination') else None
    selected = kind+'-baseline' if kind is not None and variant.get('id')=='rcp-baseline' else kind
    recorded = value.get('tool_identity_key')
    tool = tools.get(selected);tool = tool if isinstance(tool,dict) else {}
    sha = fingerprint(tool.get('sha256'))
    path = tool.get('path')
    bound = isinstance(path,str) and bool(path) and value.get('binary')==path and sha is not None
    for field in ('pins_before','pins_after'):
        pins=proof.get(field);pins=pins if isinstance(pins,dict) else {}
        bound=bound and pins.get(kind)==sha
    qualification = 'unknown' if selected is None or recorded is None else 'descriptive-mismatch' if recorded!=selected else 'unbound' if not bound else 'passed' if admitted else 'recorded-unqualified'
    return dict(selected_tool_identity_key=selected,tool_identity_key=selected if bound and recorded==selected else None,
                identity_qualification=qualification)


def owned_trial(trial, semantics, artifact_prefix, admitted, variant, tools):
    raw=trial.get('transport');raw=raw if isinstance(raw,dict) else {}
    semantics=semantics if isinstance(semantics,dict) else {}
    ok=boolean(raw.get('ok'))
    network=raw.get('network');network=network if isinstance(network,dict) else {}
    validation=raw.get('role_validation');validation=validation if isinstance(validation,dict) else {}
    capacity=validation.get('capacity');capacity=capacity if isinstance(capacity,dict) else {}
    roles=raw.get('roles');roles=roles if isinstance(roles,dict) else {}
    selected_resources=validation.get('resources');selected_resources=selected_resources if isinstance(selected_resources,dict) else {}
    result=dict(qualification='unknown' if ok is None else 'failed' if not ok else 'passed' if admitted else 'recorded-unqualified',
        revision=number(semantics.get('revision'),integer=True),profile=enum(semantics.get('profile'),{'rootless-veth-bridge-per-trial-v1'}),
        lifetime=enum(semantics.get('lifetime'),{'trial'}),requested_rtt_ms=number(semantics.get('requested_rtt_ms'),integer=True),
        account_policy=enum(semantics.get('account_policy'),{'private read-only passwd bind; caller home is owned trial directory; other fields preserved'}),
        outer_exit_code=number(raw.get('outer_exit_code'),integer=True,minimum=-255),outer_waited=boolean(raw.get('outer_waited')),
        capacity={key:number(capacity.get(key),integer=True) for key in CAPACITY_FIELDS},
        resources={role:resources(selected_resources.get(role,roles.get(role))) for role in ROLES if role in selected_resources or role in roles},
        packet_deltas={direction:number(network.get('packet_deltas',{}).get(direction),integer=True) for direction in ('to_source','to_client')} if isinstance(network.get('packet_deltas',{}),dict) else {},
        byte_deltas={direction:number(network.get('byte_deltas',{}).get(direction),integer=True) for direction in ('to_source','to_client')} if isinstance(network.get('byte_deltas',{}),dict) else {})
    for field in ('pins_before','pins_after'):
        pins=raw.get(field);pins=pins if isinstance(pins,dict) else {}
        result[field]={tool:fingerprint(pins.get(tool)) for tool in TOOLS if tool in pins}
    for field in ('cleanup','host_audit'):
        recorded=raw.get(field);recorded=recorded if isinstance(recorded,dict) else {}
        result[field]=dict(recorded_ok=boolean(recorded.get('ok')),reaped=boolean(recorded.get('reaped',recorded.get('launcher_reaped'))),
                           qualification='passed' if admitted and recorded.get('ok') is True else 'recorded-unqualified' if recorded.get('ok') is True else 'failed' if recorded.get('ok') is False else 'unknown')
    evidence=[];pre=raw.get('preflight_raw')
    if isinstance(pre,str):
        sha=hashlib.sha256(pre.encode()).hexdigest()
        evidence.append(dict(artifact_id=artifact_prefix+'-preflight',sha256=sha,digest_kind='original-utf8-preflight-bytes',
                             recorded_binding_matches=sha==fingerprint(network.get('preflight_sha256'))))
    post=raw.get('postflight')
    if isinstance(post,dict):
        sha=hashlib.sha256(json.dumps(post,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
        evidence.append(dict(artifact_id=artifact_prefix+'-postflight',sha256=sha,digest_kind='embedded-record-canonical-json'))
    commands=[]
    for role in ROLES:
        value=roles.get(role)
        if not isinstance(value,dict):continue
        identity=role_identity(variant,role,value,tools,raw,admitted)
        entry=command(value.get('argv'),value.get('argv_description'),number(value.get('command_index'),integer=True),role,identity['tool_identity_key'])
        entry.update(identity)
        entry['fully_reproducible']=entry['fully_reproducible'] and identity['identity_qualification']=='passed'
        entry['recorded_trial_index']=number(value.get('trial_index'),integer=True);commands.append(entry)
    return result,evidence,commands


def ratios(run, cases, variants):
    choices={value['id']:value for value in run['variants']};result=[]
    summaries={(value['case_id'],value['variant_id']):value for value in run['summaries']}
    for numerator in run['summaries']:
        candidate=choices[numerator['variant_id']]
        if candidate.get('tool')!='rcp' or candidate['id']=='rcp-baseline' or candidate.get('args')!=['--summary']:continue
        for comparator in run['variants']:
            kind='baseline' if comparator['id']=='rcp-baseline' and comparator.get('tool')=='rcp' and comparator.get('args')==candidate.get('args') else 'rsync-matched' if comparator.get('tool')=='rsync' and comparator.get('args')==['-rp','--stats'] else None
            denominator=summaries.get((numerator['case_id'],comparator['id']))
            if kind is None or denominator is None or comparator.get('processes')!=candidate.get('processes') or denominator['median']<=0:continue
            result.append(dict(kind=kind,case=cases[numerator['case_id']],numerator=dict(variant=variants[candidate['id']],series_id=numerator['series_id'],median_seconds=numerator['median']),
                denominator=dict(variant=variants[comparator['id']],series_id=denominator['series_id'],median_seconds=denominator['median']),value=numerator['median']/denominator['median'],
                interpretation='elapsed numerator divided by denominator; lower is faster',run_status=run['status'],completed_case=True,
                acceptance_evaluated=False,proof_contract=number(run['context'].get('operation_contract_revision'),integer=True)))
    return result


def project_run(run, source_results):
    from benchmarks import report
    report.validate_result(run)
    from benchmarks import operations
    selected_cases={case['id']:case for case in run['cases']}
    selected_variants={variant['id']:variant for variant in run['variants']}
    case_aliases={case['id']:f'case-{index+1}' for index,case in enumerate(run['cases'])}
    variant_aliases={variant['id']:f'variant-{index+1}' for index,variant in enumerate(run['variants'])}
    revision=run['revision'];commit=revision.get('commit')
    collection=run['context'].get('timing_collection');collection=collection if isinstance(collection,dict) else {}
    result=dict(run_id=run['run_id'],timestamp=dt.datetime.fromisoformat(run['timestamp'].replace('Z','+00:00')).isoformat(),
                status=run['status'],revision=dict(commit=commit if isinstance(commit,str) and COMMIT.fullmatch(commit) else None,
                dirty=boolean(revision.get('dirty')),executable_build_source_inferred=False),
                conditions=conditions(run['context']),tools={},cases=[],variants=[],trials=[],summaries=[],ratios=[],
                source_results=[dict(artifact_id=f'source-{index+1}',sha256=fingerprint(entry.get('sha256'))) for index,entry in enumerate(source_results)],
                redaction=dict(free_text_withheld=True,unknown_fields_withheld=True,aliases_scope='run-local',
                               commands_fully_reproducible=False,raw_artifacts='retained privately'))
    for key in TOOLS:
        if key not in run['tools']:continue
        raw=run['tools'][key];raw=raw if isinstance(raw,dict) else {}
        result['tools'][key]=dict(sha256=fingerprint(raw.get('sha256')),version=version(raw.get('version')),
                                 version_text_withheld=version(raw.get('version')) is None,build_source=None)
    for case in run['cases']:
        widths=case.get('directory_widths')
        result['cases'].append(dict(alias=case_aliases[case['id']],directory_widths=[number(value,integer=True) for value in widths] if isinstance(widths,list) else None,
            files_per_leaf=number(case.get('files_per_leaf'),integer=True),
            files_per_directory=number(case.get('files_per_directory'),integer=True),file_size_bytes=number(case.get('file_size_bytes'),integer=True),
            operation=enum(case.get('mode','fresh'),{'fresh','unchanged','partial'}),fixture_digest=fingerprint(case.get('fixture_digest')),
            realized_counts=counts(case.get('realized_counts'))))
    for variant in run['variants']:
        result['variants'].append(dict(alias=variant_aliases[variant['id']],tool=enum(variant.get('tool'),{'rcp','rsync','cp'}),processes=number(variant.get('processes'),integer=True),flags=[safe_flag(arg) for arg in variant.get('args',[])],args_withheld=any(safe_flag(arg) is None for arg in variant.get('args',[])),timing_collection=enum(collection.get(variant['id']),TIMING_STATUS)))
    for index,trial in enumerate(run['trials']):
        admitted=trial['status']=='ok' and run['context'].get('operation_contract_revision')==1
        operation=selected_cases[trial['case_id']].get('mode','fresh')
        exact_summary=operations.summary_supported(selected_variants[trial['variant_id']]) if admitted else False
        source_verified=run['context'].get('cache_policy')=='source-verified'
        checked=dict(content=admitted,seed=admitted and operation!='fresh',cache=admitted and source_verified,
                     metadata=admitted and exact_summary,source=admitted and (operation!='fresh' or exact_summary or source_verified))
        row=dict(ordinal=index,case=case_aliases[trial['case_id']],variant=variant_aliases[trial['variant_id']],iteration=trial['iteration'],
                 status=trial['status'],elapsed_seconds=trial.get('elapsed_seconds'),exit_codes=[number(code,integer=True,minimum=-255) for code in (trial['exit_codes'] if isinstance(trial['exit_codes'],list) else trial['exit_codes'].values())],
                 exit_code_collection='array' if isinstance(trial['exit_codes'],list) else 'legacy-object-values; keys withheld',
                 timed_out=boolean(trial.get('timed_out')),expected_transfer=counts(trial.get('expected_transfer')),observed_transfer=counts(trial.get('copy_summary')),
                 child_locale=enum(trial.get('child_locale'),{'C','inherited'}),short_sample=trial['elapsed_seconds']<10 if trial.get('elapsed_seconds') is not None else None,
                 proofs={name:proof(trial.get(field),checked[name]) for name,field in PROOFS.items()},commands=[],
                 failure=diagnostic(trial.get('failure'),f"{run['run_id']}-trial-{index}-diagnostic") if trial['status']!='ok' else None)
        row['timings']=timing(trial.get('timings'))
        row['commands']=[command(argv,None,ordinal,None,None) for ordinal,argv in enumerate(trial['commands'])]
        row['transport'],row['evidence'],role_commands=owned_trial(trial,run['context'].get('owned_transport'),f"{run['run_id']}-trial-{index}",trial['status']=='ok' and run['context'].get('owned_transport') is not None,selected_variants[trial['variant_id']],run['tools'])
        row['role_commands']=role_commands
        row['diagnostics']=[diagnostic(value,f"{run['run_id']}-trial-{index}-diagnostic-{ordinal}") for ordinal,value in enumerate(trial.get('diagnostics',[]) if isinstance(trial.get('diagnostics'),list) else [])]
        row['raw_logs']=dict(artifact_id=f"{run['run_id']}-trial-{index}-logs",sha256=None,digest_qualification='not recorded')
        result['trials'].append(row)
    for summary in run['summaries']:
        result['summaries'].append(dict(series_id=summary['series_id'],case=case_aliases[summary['case_id']],variant=variant_aliases[summary['variant_id']],
            samples=summary['samples'],unit='seconds',median=summary['median'],minimum=summary['minimum'],maximum=summary['maximum'],stdev=summary['stdev'],
            files_per_second=summary['files_per_second'],run_status=run['status'],acceptance_evaluated=False))
    project_experiment(run, result, case_aliases, variant_aliases)
    result['failure']=diagnostic(run.get('failure'),run['run_id']+'-diagnostic') if run['status']=='failed' else None
    result['ratios']=ratios(run,case_aliases,variant_aliases)
    return result


def export_results(input_path, output):
    from benchmarks import report
    runs,sources=report.load_result_records(Path(input_path))
    projected=[project_run(run,sources[run['run_id']]) for run in runs]
    envelope=dict(export_schema_version=EXPORT_SCHEMA_VERSION,runs=projected,
                  comparison_policy=dict(acceptance_evaluated=False,evaluation_screens=dict(rsync=1.20,baseline=1.05)))
    encoded=json.dumps(envelope,indent=2,allow_nan=False)+'\n'
    output=Path(output)
    output.mkdir(parents=True,exist_ok=False)
    temporary=output/'export.json.tmp'
    try:
        with temporary.open('x',encoding='utf-8') as handle:
            handle.write(encoded);handle.flush();os.fsync(handle.fileno())
        temporary.replace(output/'export.json')
    except BaseException:
        for cleanup in (lambda:temporary.unlink(missing_ok=True),output.rmdir):
            try:cleanup()
            except OSError:pass
        raise
    return envelope


def project_experiment(run, result, cases, variants):
    """Project only validated experiment constants, numeric fields and fingerprints."""
    context = run['context']
    config = context.get('pairing')
    if config is not None:
        result['pairing'] = dict(policy=pairs.POLICY, seed=config['seed'], pairs_per_case=config['pairs_per_case'],
                                block_pairs=2, candidate=variants.get(pairs.CANDIDATE), reference=variants.get(pairs.REFERENCE))
        result['paired_comparisons'] = [dict(case=cases[pair['case_id']], pair=pair['pair'], block=pair['block'],
            order=[variants[key] for key in pair['order']], candidate_trial=pair['candidate_trial'],
            reference_trial=pair['reference_trial'], candidate_over_reference=pair['candidate_over_reference'],
            run_status=run['status'], completed_case=True, acceptance_evaluated=False) for pair in pairs.comparisons(run)]
    if "command_clocks" in context:
        result["command_clocks"] = dict(policy=clocks.POLICY, qualification=clocks.QUALIFICATION)
    resources = context.get('local_resources')
    if resources is not None:
        result['local_resources'] = dict(policy=measurements.POLICY, scope=measurements.SCOPE,
                                        time_sha256=fingerprint(resources['time_sha256']), version_text_withheld=True)
    if 'measurement_environment' in context:
        result['measurement_environment'] = {key: dict(present=True, value_withheld=True)
            for key in measurements.ENVIRONMENT_KEYS if key in context['measurement_environment']}
    declared = context.get('build_provenance')
    if declared is not None:
        result['build_provenance'] = dict(qualification='caller-declared; executable hashes verified, source claims not attested',
                                        input_sha256=fingerprint(declared['input_sha256']), builds={})
        for key in ('rcp', 'rcp-baseline'):
            if key not in declared['builds']:
                continue
            build = declared['builds'][key]
            result['build_provenance']['builds'][key] = dict(
                binary_sha256=fingerprint(build['binary_sha256']), source_revision=build['source_revision'],
                source_dirty=build['source_dirty'], patch_sha256=fingerprint(build['patch_sha256']),
                cargo_lock_sha256=fingerprint(build['cargo_lock_sha256']), flake_lock_sha256=fingerprint(build['flake_lock_sha256']),
                configuration_text_withheld=True)
    if 'phase_seconds' in run:
        result['phase_seconds'] = {key:number(run['phase_seconds'][key]) for key in sorted(measurements.RUN_PHASES) if key in run['phase_seconds']}
    for original, projected in zip(run['trials'], result['trials']):
        if 'command_clocks' in original:
            projected['command_clocks'] = clocks.project(original['command_clocks'])
        if 'phase_seconds' in original:
            projected['phase_seconds'] = {key:number(original['phase_seconds'][key]) for key in sorted(measurements.TRIAL_PHASES) if key in original['phase_seconds']}
        if 'pairing' in original:
            metadata = original['pairing']
            projected['pairing'] = dict(pair=metadata['pair'], block=metadata['block'], position=metadata['position'],
                                       order=[variants[key] for key in metadata['order']])
        if 'resources' in original:
            projected['exit_status_scope'] = enum(original.get('exit_status_scope'), {measurements.EXIT_STATUS_SCOPE})
            projected['payload_status'] = enum(original.get('payload_status'), measurements.PAYLOAD_STATUSES)
            value = original['resources']
            projected['resources'] = dict(status=enum(value['status'], measurements.RESOURCE_STATUSES),
                metrics={key:number(value['metrics'][key],integer=key not in measurements.FLOATS) for key in (*measurements.FLOATS,*measurements.COUNTS,'exit_code')} if value['status']=='complete' else None,
                raw_sha256=fingerprint(value.get('raw_sha256')), reason=enum(value.get('reason'), measurements.UNAVAILABLE_REASONS), error_text_withheld='error' in value)
