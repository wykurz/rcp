"""Internal per-trial rootless transport. No host-network fallback is permitted."""
from dataclasses import dataclass
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import pwd
import re
import resource
import shlex
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import time
import uuid

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmarks import operations

SCRIPT = Path(__file__).resolve()
REVISION = 1
PROFILE = 'rootless-veth-bridge-per-trial-v1'
ADDRESSES = {'client': '192.0.2.1', 'source': '192.0.2.2'}
KINDS = ('user', 'net', 'mnt', 'pid')
DROP_CAPS = ['setpriv', '--ambient-caps=-all', '--inh-caps=-all', '--bounding-set=-all', '--no-new-privs', '--']
SETUP_SECONDS, WORKER_MARGIN, POSTFLIGHT_SECONDS, TEARDOWN_SECONDS = 90, 30, 30, 10


@dataclass(frozen=True)
class SourceEndpoint:
    host: str
    ssh_launcher: Path

    def __post_init__(self):
        if self.host != ADDRESSES['source'] or not self.ssh_launcher.is_absolute():
            raise ValueError('owned source endpoint requires its shaped address and absolute SSH launcher')


def semantics(rtt):
    if type(rtt) is not int or rtt not in (0, 2, 10):
        raise ValueError('owned RTT must be 0, 2, or 10 ms')
    return dict(revision=REVISION, profile=PROFILE, lifetime='trial', requested_rtt_ms=rtt,
                cache_order='runner cache preparation before per-trial setup',
                ssh_policy='pinned private config/keys; strict host keys; no agent/proxy/forwarding',
                account_policy='private read-only passwd bind; caller home is owned trial directory; other fields preserved',
                environment_policy='account HOME/USER/LOGNAME/SHELL, inherited PATH, private SSH first; LANG/LC_ALL=C; other variables cleared')


def validate_request(mode, rtt, variants, environment):
    semantics(rtt)
    if mode != 'loopback' or any(not operations.summary_supported(variant) for variant in variants):
        raise ValueError('owned RTT requires loopback single exact-summary rcp or rsync variants')
    if 'RSYNC_RSH' in environment:
        raise ValueError('RSYNC_RSH must be unset for the owned SSH route')


def ns(kind):
    return os.readlink(f'/proc/self/ns/{kind}')


def assert_isolated(host):
    if any(ns(kind) == host[kind] for kind in KINDS):
        raise RuntimeError('refusing mutation before private user/net/mount/PID namespace entry')
    if os.getuid() == 0 or os.getuid() != host['uid'] or os.getgid() != host['gid']:
        raise RuntimeError('namespace entry must preserve the nonzero caller UID and GID')


def remaining(deadline, maximum=None):
    result = deadline - time.monotonic()
    if result <= 0:
        raise TimeoutError('transport stage deadline exceeded')
    return min(result, maximum) if maximum is not None else result


def run_command(argv, **kwargs):
    return subprocess.run(argv, check=True, text=True, capture_output=True, timeout=kwargs.pop('timeout', 15), **kwargs)


def mutate(argv, host, environment, deadline=None):
    assert_isolated(host)
    return run_command(argv, env=environment, timeout=remaining(deadline, 15) if deadline is not None else 15)


def save(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('w') as handle:
        json.dump(value, handle, indent=2)
        handle.write('\n')
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for block in iter(lambda: handle.read(1048576), b''):
            result.update(block)
    return result.hexdigest()


class FailureLog:
    def __init__(self):
        self.errors = []

    def add(self, stage, error):
        category = 'timeout' if isinstance(error, (TimeoutError, subprocess.TimeoutExpired)) else 'cancellation' if isinstance(error, (InterruptedError, KeyboardInterrupt)) else 'validation' if isinstance(error, ValueError) else 'execution'
        self.errors.append(dict(stage=stage, category=category, type=type(error).__name__, message=str(error) or type(error).__name__, time=time.monotonic()))

    def merge(self, errors):
        self.errors.extend(errors)
        self.errors.sort(key=lambda entry: entry['time'])

    @property
    def primary(self):
        return self.errors[0] if self.errors else None

    @property
    def diagnostics(self):
        return self.errors[1:]


def _integer(value, minimum=0):
    return type(value) is int and value >= minimum


def _hash(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def root_netem(value, direction, rtt):
    entries = value['qdiscs'][direction]
    if len(entries) != 1 or entries[0].get('kind') != 'netem' or entries[0].get('root') is not True:
        raise ValueError('expected exactly one root netem qdisc per direction')
    q = entries[0]
    delay = q['options'].get('delay', 0)
    if isinstance(delay, dict):
        if delay.get('jitter', 0) != 0 or delay.get('correlation', 0) != 0:
            raise ValueError('unexpected delay distribution')
        delay = delay['delay']
    if not math.isclose(float(delay), rtt / 2000, abs_tol=1e-9) or q['options'].get('limit') != 100000:
        raise ValueError('wrong netem delay/queue limit')
    for key in ('drops', 'backlog', 'qlen'):
        if not _integer(q.get(key)) or q[key] != 0:
            raise ValueError('netem dropped or queued traffic')
    if any(not _integer(q.get(key)) for key in ('packets', 'bytes')):
        raise ValueError('invalid netem counters')
    return q


def validate_evidence(value, rtt):
    try:
        semantics(rtt)
        if type(value['schema_version']) is not int or value['schema_version'] != 1 or type(value['requested_rtt_ms']) is not int or value['requested_rtt_ms'] != rtt or value['profile'] != PROFILE or value['lifetime'] != 'trial':
            raise ValueError('wrong owned evidence schema/RTT/profile/lifetime')
        if not re.fullmatch('[0-9a-f]{32}', value['trial_id']):
            raise ValueError('invalid trial identity')
        isolation, endpoints = value['isolation'], value['endpoints']
        if set(endpoints) != set(ADDRESSES) or not _integer(isolation['uid'], 1) or not _integer(isolation['gid']):
            raise ValueError('incorrect endpoint cardinality/user')
        nets = [isolation['outer_netns'], isolation['host_netns']]
        for role, address in ADDRESSES.items():
            endpoint = endpoints[role]
            if endpoint['ip'] != address or endpoint['uid'] != isolation['uid'] or endpoint['gid'] != isolation['gid']:
                raise ValueError('endpoint identity differs from preserved user/address')
            nets.append(endpoint['netns'])
            route = value['routes'][role]
            peer = ADDRESSES['source' if role == 'client' else 'client']
            if len(route) != 1 or route[0].get('dst') != peer or route[0].get('dev') != role + '_peer' or route[0].get('prefsrc') != address or 'gateway' in route[0]:
                raise ValueError('route bypasses the owned veth endpoint')
        if len(set(nets)) != 4 or any(not re.fullmatch(r'net:\[\d+\]', name) for name in nets):
            raise ValueError('host/transit/endpoint network namespaces must be distinct')
        for kind in ('user', 'mnt', 'pid'):
            left, right = isolation[kind + 'ns'], isolation['host_' + kind + 'ns']
            if left == right or any(not re.fullmatch(rf'{kind}:\[\d+\]', name) for name in (left, right)):
                raise ValueError('inherited isolation namespace')
        for name in ('ping', 'tcp_echo'):
            probe = value[name]
            samples = probe['samples_ms']
            if len(samples) < 3 or any(type(item) not in (int, float) or not math.isfinite(item) or item <= 0 for item in samples):
                raise ValueError('invalid latency probe samples')
            median = statistics.median(samples)
            if not math.isclose(median, probe['median_ms'], abs_tol=1e-6) or abs(median - rtt) > max(1, .25*rtt):
                raise ValueError('latency probe does not match requested RTT')
        if value['ping']['loss_percent'] != 0:
            raise ValueError('ping packet loss')
        for direction in ('to_source', 'to_client'):
            root_netem(value, direction, rtt)
        for key in ('launcher', 'binary', 'config', 'known_hosts'):
            if not Path(value['ssh'][key + '_path']).is_absolute() or not _hash(value['ssh'][key + '_sha256']):
                raise ValueError('invalid SSH provenance')
        account=value['account']
        if account['home'] != str(Path(value['ssh']['config_path']).parent) or account['home_uid'] != isolation['uid'] or type(account['home_mode']) is not int or account['home_mode'] != 0o700 or any(account.get(key) is not True for key in ('passwd_readonly','nss_matches','other_fields_preserved')) or not _hash(account['passwd_sha256']):
            raise ValueError('invalid private account-home proof')
        if not _hash(value['launcher_script_sha256']):
            raise ValueError('invalid launcher provenance')
        return value
    except (KeyError, TypeError, OverflowError, AttributeError) as error:
        raise ValueError(f'malformed owned evidence: {error}') from error


def validate_pair(before, after, preflight_sha256, minimum_payload_bytes=0):
    rtt = before['requested_rtt_ms']
    validate_evidence(before, rtt)
    validate_evidence(after, rtt)
    if not _hash(preflight_sha256) or after.get('preflight_sha256') != preflight_sha256:
        raise ValueError('postflight does not bind exact preflight bytes')
    for key in ('trial_id', 'endpoints', 'isolation', 'ssh', 'account', 'routes', 'launcher_script_sha256'):
        if before[key] != after[key]:
            raise ValueError(f'network identity changed: {key}')
    packets, size = {}, {}
    for direction in ('to_source', 'to_client'):
        old, new = root_netem(before, direction, rtt), root_netem(after, direction, rtt)
        if old['options'] != new['options'] or old.get('handle') != new.get('handle'):
            raise ValueError('netem changed during trial')
        packets[direction] = new['packets'] - old['packets']
        size[direction] = new['bytes'] - old['bytes']
        if packets[direction] <= 0 or size[direction] <= 0:
            raise ValueError('traffic did not traverse both directions or counters reset')
    if not _integer(minimum_payload_bytes) or size['to_client'] < minimum_payload_bytes:
        raise ValueError('source-to-client bytes below fresh logical payload')
    return dict(ok=True, preflight_sha256=preflight_sha256, packet_deltas=packets, byte_deltas=size,
                minimum_payload_bytes=minimum_payload_bytes, requested_rtt_ms=rtt)


def option(argv, flag):
    values=[]
    for index,item in enumerate(argv):
        if item==flag:
            values.append(argv[index+1] if index+1<len(argv) else '')
        elif item.startswith(flag+'='):
            values.append(item.partition('=')[2])
    if len(values)!=1 or not values[0].isascii() or not values[0].isdecimal():
        raise ValueError(f'expected unique numeric {flag}')
    return int(values[0])


def actual_capacity(roles):
    source, destination = roles['source']['argv'], roles['destination']['argv']
    f = option(destination, '--resolved-automatic-files-in-flight')
    maximum, e = option(source, '--max-connections'), option(destination, '--max-connections')
    multiplier = option(source, '--pending-writes-multiplier')
    if f < 1 or maximum < 1 or e != min(f, maximum) or multiplier < 1 or multiplier != option(destination, '--pending-writes-multiplier') or any(arg==flag or arg.startswith(flag+'=') for argv in (source,destination) for arg in argv for flag in ('--max-files-in-flight','--max-open-files','--forwarded-legacy-files-in-flight')):
        raise ValueError('source/destination automatic concurrency negotiation differs')
    return dict(logical_F=f, source_configured_max_connections=maximum, effective_E=e,
                pending_multiplier=multiplier, pending_P=e * multiplier)


def validate_roles(roles, evidence, tool, tools):
    expected = {'master', 'source', 'destination'} if tool == 'rcp' else {'source', 'destination'}
    if set(roles) != expected:
        raise ValueError('wrong role cardinality')
    for role, actual in roles.items():
        endpoint = evidence['endpoints']['source' if role == 'source' else 'client']
        if any(actual[key] != endpoint[key] for key in ('netns', 'uid', 'gid')) or any(actual[kind + 'ns'] != evidence['isolation'][kind + 'ns'] for kind in ('user', 'mnt', 'pid')):
            raise ValueError('role escaped its owned endpoint/user')
        binary = tools['rcp' if role == 'master' else 'rcpd'] if tool == 'rcp' else tools['rsync']
        if actual['binary'] != str(binary) or actual['no_new_privileges'] != 1 or actual['capabilities'] != 0 or actual.get('capability_sets') != {key:0 for key in ('CapInh','CapPrm','CapEff','CapBnd','CapAmb')}:
            raise ValueError('wrong selected executable or retained capabilities')
        if not _integer(actual['fd_soft'], 1) or not _integer(actual['fd_hard'], -1) or (actual['fd_hard'] != -1 and actual['fd_soft'] > actual['fd_hard']):
            raise ValueError('invalid actual FD limits')
        affinity = actual['cpu_affinity']
        if not isinstance(affinity, list) or not affinity or any(not _integer(cpu) for cpu in affinity) or len(set(affinity)) != len(affinity) or not isinstance(actual['cpu_quota'], str):
            raise ValueError('invalid role CPU observation')
    source = roles['source']['ssh_connection'].split()
    if len(source) != 4 or source[0] != ADDRESSES['client'] or source[2] != ADDRESSES['source'] or source[3] != '2222' or not source[1].isdecimal():
        raise ValueError('source SSH bypassed shaped endpoint addresses')
    if tool == 'rcp':
        destination = roles['destination']['ssh_connection'].split()
        if len(destination) != 4 or destination[0] != '127.0.0.1' or destination[2:] != ['127.0.0.1', '2222']:
            raise ValueError('destination SSH is not private client localhost')
    return dict(ok=True, capacity=actual_capacity(roles) if tool == 'rcp' else None,
                resources={role:dict(cpu_parallelism=len(value['cpu_affinity']), cpu_affinity=value['cpu_affinity'], cpu_quota=value['cpu_quota'], fd_soft=value['fd_soft'], fd_hard=value['fd_hard']) for role, value in roles.items()})


def clean_environment(wrapper=None):
    account = pwd.getpwuid(os.getuid())
    value = dict(PATH=os.environ['PATH'], HOME=account.pw_dir, USER=account.pw_name,
                 LOGNAME=account.pw_name, SHELL=account.pw_shell, LANG='C', LC_ALL='C')
    if wrapper:
        value['PATH'] = f"{wrapper}:{value['PATH']}"
    return value


def ssh_config_path(path):
    """Quote a literal path for a directive that does not expand tokens or variables."""
    value = str(path)
    if any(character in value for character in '\r\n\0'):
        raise ValueError('SSH configuration path contains a line delimiter or NUL')
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'


def render_account_database(text, account, home):
    """Render the whole caller-readable account database, changing only the caller's home."""
    home = str(home)
    if not Path(home).is_absolute() or any(character in home for character in ':\r\n\0'):
        raise ValueError('private account home must be an absolute delimiter-free path')
    expected = [account.pw_name, account.pw_passwd, str(account.pw_uid), str(account.pw_gid),
                account.pw_gecos, account.pw_dir, account.pw_shell]
    lines, matches = [], 0
    for line in text.splitlines(keepends=True):
        fields = line.rstrip('\n').split(':')
        if len(fields) != 7 or any('\r' in field or '\0' in field for field in fields) or not fields[2].isdigit() or not fields[3].isdigit():
            raise ValueError('malformed private account file')
        if fields[0] == account.pw_name or int(fields[2]) == account.pw_uid:
            if fields != expected:
                raise ValueError('account file differs from effective caller identity')
            matches += 1
            fields[5] = home
            line = ':'.join(fields) + ('\n' if line.endswith('\n') else '')
        lines.append(line)
    if matches != 1:
        raise ValueError('private account requires one matching local passwd entry')
    return ''.join(lines)


def run_stages(setup, command, postflight, teardown):
    """One lifetime: postflight after any attempted copy, teardown on every exit."""
    value, failures = {}, FailureLog()
    stage = 'setup'
    try:
        value['setup'] = setup()
        stage = 'command'
        value['outcome'] = command()
        if not value['outcome']['ok']:
            raise RuntimeError('copy command failed or timed out')
    except BaseException as error:
        failures.add(stage, error)
    finally:
        if stage == 'command':
            try:
                value['postflight'] = postflight()
            except BaseException as error:
                failures.add('postflight', error)
        try:
            value['cleanup'] = teardown()
        except BaseException as error:
            failures.add('teardown', error)
    value['errors'] = failures.errors
    return value


@contextmanager
def deferred_teardown_signals(failures):
    """Defer repeat cancellation while the held namespace launcher is killed/reaped."""
    previous, seen = {}, set()
    def record(number, _frame):
        if previous[number] != signal.SIG_IGN and number not in seen:
            seen.add(number)
            failures.add('outer_teardown_cancellation', InterruptedError(f'signal {number} during owned teardown'))
    try:
        for number in (signal.SIGTERM, signal.SIGINT):
            previous[number] = signal.getsignal(number)
            signal.signal(number, record)
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def stop_launcher(child, grace=TEARDOWN_SECONDS, failures=None):
    """Signal only the held launcher; repeat cancellation cannot skip kill/reap."""
    failures = failures if failures is not None else FailureLog()
    with deferred_teardown_signals(failures):
        if child.poll() is None:
            try:
                child.terminate()
            except ProcessLookupError:
                pass
            try:
                return child.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                pass
            except (InterruptedError, KeyboardInterrupt) as error:
                failures.add('outer_teardown_cancellation', error)
            try:
                child.kill()
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + 2
        while True:
            try:
                return child.wait(timeout=remaining(deadline))
            except (InterruptedError, KeyboardInterrupt) as error:
                failures.add('outer_teardown_cancellation', error)


def qualify(inner, outer_code, host_audit, tool, tools, pins, minimum_payload_bytes, ssh_identity):
    failures = FailureLog()
    failures.merge(inner.get('errors', []))
    outcome = dict(inner.get('outcome') or dict(ok=False, elapsed_seconds=None, exit_codes=[], timed_out=False, logs=[], commands=[]))
    proof = dict(outer_exit_code=outer_code,
                 outer_waited=type(outer_code) is int, host_audit=host_audit, cleanup=inner.get('cleanup'))
    try:
        if not outcome.get('ok') or outcome.get('exit_codes') != [0] or outcome.get('timed_out'):
            raise ValueError('command did not complete successfully')
        raw = inner['preflight_raw']
        before = json.loads(raw)
        proof['semantics'] = semantics(before['requested_rtt_ms'])
        proof.update(preflight_raw=raw, postflight=inner['postflight'], roles=inner['roles'],
                     pins_before=inner['pins_before'], pins_after=inner['pins_after'])
        proof['network'] = validate_pair(before, proof['postflight'], hashlib.sha256(raw.encode()).hexdigest(), minimum_payload_bytes)
        if before['ssh']['binary_path'] != ssh_identity['path'] or before['ssh']['binary_sha256'] != ssh_identity['sha256']:
            raise ValueError('actual owned SSH differs from runner-selected identity')
        proof['role_validation'] = validate_roles(proof['roles'], before, tool, tools)
        if set(pins) != set(tools) or proof['pins_before'] != pins or proof['pins_after'] != pins:
            raise ValueError('selected executable hashes changed or were not observed before/after')
        cleanup = inner['cleanup']
        if cleanup != dict(ok=True, private_pid_namespace=True, remaining_pids=[], reaped=True):
            raise ValueError('private PID namespace cleanup was not proved')
        if type(outer_code) is not int or outer_code != 0:
            raise ValueError(f'actual namespace launcher exit was {outer_code}')
        if host_audit.get('ok') is not True:
            raise ValueError('host ownership audit changed or launcher remained')
    except (KeyError, TypeError, ValueError) as error:
        failures.add('qualification', error)
    proof['ok'] = not failures.errors
    outcome.update(ok=proof['ok'], transport=proof, failure=failures.primary, diagnostics=failures.diagnostics)
    return outcome


def pin_tools(tools):
    return {key: digest(path) for key, path in tools.items()}



def process_start_identity(pid):
    return Path(f'/proc/{pid}/stat').read_text().rpartition(') ')[2].split()[19]

def host_state():
    return dict(namespaces={kind: ns(kind) for kind in KINDS}, uid=os.getuid(), gid=os.getgid(),
                links=json.loads(run_command(['ip', '-j', 'link', 'show']).stdout),
                routes=json.loads(run_command(['ip', '-j', 'route', 'show', 'table', 'all']).stdout),
                qdiscs=json.loads(run_command(['tc', '-s', '-j', 'qdisc', 'show']).stdout))


def host_cleanup(before, child):
    after = host_state()
    return dict(ok=stable_host(before) == stable_host(after) and child.returncode is not None,
                before=before, after=after, launcher_pid=child.pid, launcher_reaped=child.returncode is not None)


def execute_trial(variant, source, destination, tools, log_dir, folder, timeout, rtt,
                  operation, counts, timing_policy, timing_prefix, trial_index, *, expected_pins, ssh_identity):
    """Host entry; no result is accepted before the actual launcher has been reaped."""
    validate_request('loopback', rtt, [variant], os.environ)
    if sys.platform != 'linux' or os.getuid() == 0:
        raise ValueError('owned transport requires Linux and the preserved nonzero caller UID')
    folder = Path(folder)
    folder.mkdir(mode=0o700, parents=True, exist_ok=False)
    log_dir = Path(log_dir)
    selected = {key: str(Path(tools[key]).absolute()) for key in (('rcp', 'rcpd') if variant['tool'] == 'rcp' else ('rsync',))}
    utilities = {}
    for name in ('unshare', 'nsenter', 'ip', 'tc', 'mount', 'setpriv', 'ssh', 'sshd', 'ssh-keygen', 'ping'):
        path = ssh_identity['path'] if name=='ssh' else shutil.which(name)
        if path is None:
            raise ValueError(f'owned transport prerequisite missing: {name}')
        utilities[name] = str(Path(path).absolute())
    if utilities['ssh'] != tools['ssh']:
        raise ValueError('owned SSH path differs from runner-selected tool')
    utility_pins = pin_tools(utilities)
    if utility_pins['ssh'] != ssh_identity['sha256']:
        raise ValueError('selected SSH executable changed since runner provenance')
    utilities['python'] = str(Path(sys.executable).absolute())
    before = host_state()
    host = {kind: before['namespaces'][kind] for kind in KINDS}
    host.update(uid=before['uid'], gid=before['gid'])
    pins = pin_tools(selected)
    if pins != expected_pins:
        raise ValueError('selected copy executables changed since runner provenance')
    request = dict(schema_version=1, trial_id=uuid.uuid4().hex, trial_index=trial_index,
                   source=str(source), destination=str(destination), tools=selected,
                   pins=pins, utility_pins={**utility_pins, 'python':digest(utilities['python'])}, utilities=utilities,
                   script_sha256=digest(SCRIPT), helper_pins=helper_pins(), host=host, variant=variant, timeout=timeout,
                   rtt=rtt, operation=operation, counts=counts, folder=str(folder),
                   log_dir=str(log_dir), timing_policy=timing_policy, timing_prefix=str(timing_prefix))
    save(folder / 'request.json', request)
    command = [utilities['unshare'], '--user', '--map-current-user', '--keep-caps', '--net', '--mount',
               '--pid', '--fork', '--kill-child=SIGKILL', sys.executable, str(SCRIPT), '_supervise', str(folder / 'request.json')]
    save(folder / 'launcher-command.json', command)
    errors = FailureLog()
    child, code = None, None
    stage = 'outer_launch'
    try:
        with (folder / 'launcher.stdout.log').open('w') as stdout, (folder / 'launcher.stderr.log').open('w') as stderr:
            child = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=clean_environment(), start_new_session=True)
            save(folder / 'launcher-identity.json', dict(pid=child.pid, start_identity=process_start_identity(child.pid)))
            stage = 'outer_wait'
            code = child.wait(timeout=timeout + SETUP_SECONDS + WORKER_MARGIN + POSTFLIGHT_SECONDS + TEARDOWN_SECONDS + 5)
    except BaseException as error:
        errors.add(stage, error)
    finally:
        if child is not None and code is None:
            try:
                code = stop_launcher(child, failures=errors)
            except BaseException as error:
                errors.add('outer_teardown', error)
                code = child.returncode
    try:
        inner = json.loads((folder / 'provisional.json').read_text())
    except (OSError, ValueError) as error:
        errors.add('inner_record', error)
        inner = {}
    errors.merge(inner.get('errors', []))
    inner['errors'] = errors.errors
    try:
        audit = host_cleanup(before, child) if child is not None else dict(ok=False)
    except BaseException as error:
        inner['errors'].append(dict(stage='host_audit', type=type(error).__name__, message=str(error), time=time.monotonic()))
        audit = dict(ok=False)
    result = qualify(inner, code, audit, variant['tool'], selected, pins, counts['bytes'] if operation == 'fresh' else 0, ssh_identity)
    result['transport']['artifacts'] = str(folder)
    result['diagnostic_artifacts'] = {name:str(folder/name) for name in ('request.json','launcher.stdout.log','launcher.stderr.log','copy.stdout.log','copy.stderr.log','copy-result.json','provisional.json','evidence-before.json','evidence-after.json')}
    save(folder / 'qualified.json', result)
    return result


def private_proc(host):
    assert_isolated(host)
    if os.getpid() != 1 or os.readlink('/proc/1/ns/pid') != ns('pid'):
        raise RuntimeError('refusing PID cleanup without private proc mounted for owned PID 1')


def cleanup_namespace(host, children):
    private_proc(host)
    deadline = time.monotonic() + TEARDOWN_SECONDS
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    def pids():
        private_proc(host)
        return sorted(int(item.name) for item in Path('/proc').iterdir() if item.name.isdecimal() and int(item.name) != 1)
    def reap():
        for child in children:
            child.poll()
        while True:
            try:
                pid, _ = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                break
            if pid == 0:
                break
    for signum, grace in ((signal.SIGTERM, 1), (signal.SIGKILL, TEARDOWN_SECONDS - 1)):
        until = min(deadline, time.monotonic() + grace)
        while True:
            reap()
            owned = pids()
            if not owned:
                return dict(ok=True, private_pid_namespace=True, remaining_pids=[], reaped=True)
            for pid in owned:
                private_proc(host)
                try:
                    os.kill(pid, signum)
                except ProcessLookupError:
                    pass
            if time.monotonic() >= until:
                break
            time.sleep(.02)
    raise RuntimeError(f'private PID cleanup deadline; remaining namespace PIDs={pids()}')


def ready(argv, environment, deadline, failure_path):
    last = ''
    until = min(deadline, time.monotonic() + 10)
    while time.monotonic() < until:
        try:
            return run_command(argv, env=environment, timeout=remaining(until, 2))
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
            last = str(error) + '\n' + str(getattr(error, 'stderr', ''))
            time.sleep(min(.1, max(0, until - time.monotonic())))
    Path(failure_path).write_text(last + '\n')
    raise RuntimeError(f'readiness deadline; diagnostic artifact {failure_path}')


def drained_qdiscs(read_qdiscs, rtt, failure_path, deadline):
    until, empty_since = min(deadline, time.monotonic() + 2), None
    latest = None
    try:
        while True:
            latest = read_qdiscs(remaining(until))
            for direction in ('to_source', 'to_client'):
                roots = latest[direction]
                if len(roots) != 1 or roots[0].get('kind') != 'netem' or not roots[0].get('root') or roots[0]['drops'] != 0:
                    raise ValueError('wrong netem root or packet drops while draining')
            now = time.monotonic()
            if all(latest[direction][0]['backlog'] == latest[direction][0]['qlen'] == 0 for direction in latest):
                empty_since = now if empty_since is None else empty_since
                if now - empty_since >= .05:
                    for direction in latest:
                        root_netem({'qdiscs': latest}, direction, rtt)
                    return latest
            else:
                empty_since = None
            time.sleep(min(.01, remaining(until)))
    except BaseException as error:
        save(failure_path, dict(error=str(error), qdiscs=latest))
        raise


class NamespaceTrial:
    """Private PID-1 owner, with one setup/copy/postflight/teardown sequence."""
    def __init__(self, request):
        self.request = request
        self.folder = Path(request['folder'])
        self.host = request['host']
        self.environment = clean_environment()
        self.children, self.logs, self.endpoints = [], [], {}
        self.deadline = time.monotonic() + SETUP_SECONDS
        self.launcher = self.folder / 'bin' / 'ssh'

    def utility_argv(self, argv):
        return [self.request['utilities'].get(argv[0], argv[0]), *argv[1:]]

    def run(self, argv):
        return run_command(self.utility_argv(argv), env=self.environment, timeout=remaining(self.deadline, 15))

    def mutate(self, argv):
        return mutate(self.utility_argv(argv), self.host, self.environment, self.deadline)

    def in_role(self, role, argv):
        return [self.request['utilities']['nsenter'], '--target', str(self.endpoints[role]['pid']), '--net', '--', *self.utility_argv(argv)]

    def owned(self, argv, log, environment=None):
        assert_isolated(self.host)
        remaining(self.deadline)
        handle = (self.folder / log).open('w')
        self.logs.append(handle)
        child = subprocess.Popen(self.utility_argv(argv), stdout=handle, stderr=subprocess.STDOUT, env=environment or self.environment)
        self.children.append(child)
        return child

    def setup(self):
        assert_isolated(self.host)
        if helper_pins()!=self.request['helper_pins'] or pin_tools(self.request['utilities'])!=self.request['utility_pins']:
            raise ValueError('transport helper/utility changed before namespace setup')
        if os.getpid() != 1:
            raise RuntimeError('supervisor must own PID 1')
        self.mutate(['mount', '--make-rprivate', '/'])
        self.mutate(['mount', '-t', 'proc', 'proc', '/proc'])
        private_proc(self.host)
        self.mutate(['ip', 'link', 'set', 'lo', 'up'])
        for role in ADDRESSES:
            path = self.folder / f'{role}.ready.json'
            self.owned(['unshare', '--net', sys.executable, str(SCRIPT), '_holder', str(self.folder / 'request.json'), role], f'{role}.holder.log')
            while not path.exists():
                remaining(self.deadline)
                time.sleep(.02)
            self.endpoints[role] = json.loads(path.read_text())
            if self.endpoints[role]['netns'] in (self.host['net'], ns('net')):
                raise RuntimeError('endpoint inherited transit/host network')
        if self.endpoints['client']['netns'] == self.endpoints['source']['netns']:
            raise RuntimeError('endpoint namespaces match')
        self.mutate(['ip', 'link', 'add', 'bridge0', 'type', 'bridge'])
        self.mutate(['ip', 'link', 'set', 'bridge0', 'up'])
        for role, address in ADDRESSES.items():
            device, peer = f'to_{role}', f'{role}_peer'
            self.mutate(['ip', 'link', 'add', device, 'type', 'veth', 'peer', 'name', peer])
            self.mutate(['ip', 'link', 'set', peer, 'netns', str(self.endpoints[role]['pid'])])
            for args in (['ip','link','set',device,'mtu','1500'], ['ip','link','set',device,'master','bridge0'], ['ip','link','set',device,'up']):
                self.mutate(args)
            for args in (['ip','link','set','lo','up'], ['ip','addr','add',address+'/24','dev',peer], ['ip','link','set',peer,'mtu','1500'], ['ip','link','set',peer,'up']):
                self.mutate(self.in_role(role, args))
            self.mutate(['tc','qdisc','add','dev',device,'root','netem','delay',f"{self.request['rtt']/2:g}ms",'limit','100000'])
            self.endpoints[role].update(ip=address, uid=os.getuid(), gid=os.getgid())
        self.setup_ssh()
        self.owned(self.in_role('source', [*DROP_CAPS, sys.executable, str(SCRIPT), '_echo', str(self.folder/'request.json')]), 'echo.log')
        ready(self.in_role('client', [*DROP_CAPS, sys.executable, str(SCRIPT), '_probe', str(self.folder/'request.json')]), self.environment, self.deadline, self.folder/'echo.readiness-failure.log')
        self.before = self.snapshot()
        save(self.folder/'evidence-before.json', self.before)
        self.preflight_raw = (self.folder/'evidence-before.json').read_text()
        return dict(ok=True)

    def setup_account(self):
        """Bind a private home view before SSH; never change host account or home files."""
        assert_isolated(self.host)
        account = pwd.getpwuid(os.getuid())
        home = self.folder.resolve(strict=True)
        rendered = render_account_database(Path('/etc/passwd').read_text(), account, home)
        # mount uses the held file, not a later resolution of the generated source name
        with (self.folder/'passwd').open('x+') as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
            self.mutate(['mount', '--no-canonicalize', '--bind', f'/proc/{os.getpid()}/fd/{handle.fileno()}', '/etc/passwd'])
            self.mutate(['mount', '-o', 'remount,bind,ro', '/etc/passwd'])
        expected = list(account)
        expected[5] = str(home)
        if list(pwd.getpwuid(account.pw_uid)) != expected or list(pwd.getpwnam(account.pw_name)) != expected:
            raise RuntimeError('private account lookup did not bind caller home')
        self.private_account = account
        self.private_passwd_sha256 = hashlib.sha256(rendered.encode()).hexdigest()
        self.account_evidence()
        self.environment = clean_environment()

    def account_evidence(self):
        home = self.folder.resolve(strict=True)
        expected = list(self.private_account)
        expected[5] = str(home)
        account = pwd.getpwuid(self.private_account.pw_uid)
        matches = list(account) == expected and list(pwd.getpwnam(account.pw_name)) == expected
        mounts = [line.split() for line in Path('/proc/self/mountinfo').read_text().splitlines() if len(line.split()) > 6 and line.split()[4] == '/etc/passwd']
        readonly = bool(mounts) and 'ro' in mounts[-1][5].split(',')
        passwd_sha256 = digest('/etc/passwd')
        info = home.stat()
        if not matches or not readonly or passwd_sha256 != self.private_passwd_sha256 or info.st_uid != account.pw_uid or info.st_mode & 0o777 != 0o700:
            raise RuntimeError('private account home/passwd binding changed or is not read-only')
        return dict(home=str(home),home_uid=info.st_uid,home_mode=info.st_mode & 0o777,passwd_readonly=readonly,
                    passwd_sha256=passwd_sha256,nss_matches=matches,other_fields_preserved=matches)

    def setup_ssh(self):
        self.setup_account()
        folder, account = self.folder, pwd.getpwuid(os.getuid())
        wrappers = folder/'bin'
        wrappers.mkdir(mode=0o700)
        self.run(['ssh-keygen','-q','-t','ed25519','-N','','-f',str(folder/'identity')])
        (folder/'authorized_keys').write_text((folder/'identity.pub').read_text())
        (folder/'authorized_keys').chmod(0o600)
        hosts = []
        for role, address in ADDRESSES.items():
            key = folder/f'{role}.hostkey'
            self.run(['ssh-keygen','-q','-t','ed25519','-N','','-f',str(key)])
            public = Path(str(key)+'.pub').read_text().strip()
            names = [address,'127.0.0.1','localhost'] if role=='client' else [address]
            hosts.extend(f'[{name}]:2222 {public}' for name in names)
            config = folder/f'{role}.sshd_config'
            config.write_text('\n'.join(['Port 2222', *[f'ListenAddress {name}' for name in names if name!='localhost'],
                f'HostKey {ssh_config_path(key)}', f'PidFile {ssh_config_path(folder/(role+".sshd.pid"))}', 'AuthorizedKeysFile "%h/authorized_keys"',
                f'AllowUsers {account.pw_name}', 'PasswordAuthentication no', 'KbdInteractiveAuthentication no',
                'UsePAM no', 'AuthenticationMethods publickey', 'PermitRootLogin no', 'StrictModes yes',
                'PermitUserEnvironment no', 'PermitUserRC no', 'DisableForwarding yes', 'UseDNS no',
                'PrintMotd no', 'LogLevel VERBOSE', 'MaxStartups 100:100:100'])+'\n')
            self.run(self.in_role(role,[*DROP_CAPS,self.request['utilities']['sshd'],'-t','-f',str(config)]))
            self.owned(self.in_role(role,[*DROP_CAPS,self.request['utilities']['sshd'],'-D','-e','-f',str(config)]),f'{role}.sshd.log')
        (folder/'known_hosts').write_text('\n'.join(hosts)+'\n')
        config=folder/'ssh_config'
        # home tokens insert the verified private account home without parsing its %, ${}, or quote characters
        config.write_text('\n'.join(['Host *',f'  User {account.pw_name}','  Port 2222', '  IdentityFile "%d/identity"',
            '  IdentityAgent none','  IdentitiesOnly yes','  UserKnownHostsFile "%d/known_hosts"',
            '  GlobalKnownHostsFile /dev/null','  StrictHostKeyChecking yes','  BatchMode yes','  ConnectTimeout 5',
            '  ForwardAgent no','  ClearAllForwardings yes','  ProxyCommand none','  ProxyJump none',
            '  ControlMaster no','  ControlPath none','Host localhost','  HostName 127.0.0.1'])+'\n')
        self.launcher.write_text(f'#!/bin/sh\nexec {shlex.quote(self.request["utilities"]["ssh"])} -F {shlex.quote(str(config))} "$@"\n')
        self.launcher.chmod(0o700)
        self.environment=clean_environment(wrappers)
        for role,address in ADDRESSES.items():
            answer=ready(self.in_role('client',[*DROP_CAPS,str(self.launcher),address,'id -u; id -g; readlink /proc/self/ns/net; readlink /proc/self/ns/user; readlink /proc/self/ns/mnt; readlink /proc/self/ns/pid']),self.environment,self.deadline,folder/f'{role}.readiness-failure.log')
            expected=[str(os.getuid()),str(os.getgid()),self.endpoints[role]['netns'],*[ns(kind) for kind in ('user','mnt','pid')]]
            if answer.stdout.splitlines()!=expected:
                raise RuntimeError(f'incorrect SSH {role} endpoint identity')
        ready(self.in_role('client',[*DROP_CAPS,str(self.launcher),'localhost','true']),self.environment,self.deadline,folder/'localhost.readiness-failure.log')

    def snapshot(self):
        ping=self.run(self.in_role('client',['ping','-n','-c','10','-i','0.02','-W','2',ADDRESSES['source']])).stdout
        samples=[float(value) for value in re.findall(r'time[=<]([0-9.]+) ms',ping)]
        loss=re.search(r'([0-9.]+)% packet loss',ping)
        if len(samples)!=10 or loss is None:
            raise ValueError('missing ping evidence')
        tcp=json.loads(self.run(self.in_role('client',[*DROP_CAPS,sys.executable,str(SCRIPT),'_probe',str(self.folder/'request.json')])).stdout)
        def qdiscs(timeout):
            return {f'to_{role}':json.loads(run_command([self.request['utilities']['tc'],'-s','-j','qdisc','show','dev',f'to_{role}'],env=self.environment,timeout=timeout/2).stdout) for role in ADDRESSES}
        queues=drained_qdiscs(qdiscs,self.request['rtt'],self.folder/'qdisc-failure.json',self.deadline)
        isolation=dict(outer_netns=ns('net'),host_netns=self.host['net'],uid=os.getuid(),gid=os.getgid())
        for kind in ('user','mnt','pid'):
            isolation.update({kind+'ns':ns(kind),'host_'+kind+'ns':self.host[kind]})
        ssh={}
        for key,path in dict(launcher=self.launcher,binary=self.request['utilities']['ssh'],config=self.folder/'ssh_config',known_hosts=self.folder/'known_hosts').items():
            ssh.update({key+'_path':str(path),key+'_sha256':digest(path)})
        value=dict(schema_version=1,trial_id=self.request['trial_id'],requested_rtt_ms=self.request['rtt'],profile=PROFILE,lifetime='trial',
                   endpoints=self.endpoints,isolation=isolation,ssh=ssh,account=self.account_evidence(),launcher_script_sha256=digest(SCRIPT),
                   routes={role:json.loads(self.run(self.in_role(role,['ip','-j','route','get',ADDRESSES['source' if role=='client' else 'client']])).stdout) for role in ADDRESSES},
                   ping=dict(samples_ms=samples,median_ms=statistics.median(samples),loss_percent=float(loss.group(1)),raw=ping),tcp_echo=tcp,qdiscs=queues)
        validate_evidence(value,self.request['rtt'])
        return value

    def command(self):
        self.deadline=time.monotonic()+self.request['timeout']+WORKER_MARGIN
        with (self.folder/'copy.stdout.log').open('w') as stdout,(self.folder/'copy.stderr.log').open('w') as stderr:
            process=subprocess.Popen(self.in_role('client',[*DROP_CAPS,sys.executable,str(SCRIPT),'_copy',str(self.folder/'request.json')]),env=self.environment,stdout=stdout,stderr=stderr)
            self.children.append(process)
            code=process.wait(timeout=remaining(self.deadline))
        value=json.loads((self.folder/'copy-result.json').read_text())
        self.copy=value
        if code!=0 or value.get('errors'):
            raise RuntimeError((value.get('failure') or {}).get('message',f'copy worker exit {code}'))
        return value['outcome']

    def postflight(self):
        self.deadline=time.monotonic()+POSTFLIGHT_SECONDS
        after=self.snapshot()
        after['preflight_sha256']=hashlib.sha256(self.preflight_raw.encode()).hexdigest()
        save(self.folder/'evidence-after.json',after)
        validate_pair(self.before,after,after['preflight_sha256'],self.request['counts']['bytes'] if self.request['operation']=='fresh' else 0)
        return after

    def teardown(self):
        try:
            return cleanup_namespace(self.host,self.children)
        finally:
            for handle in self.logs:
                handle.close()


def supervise(request):
    trial=NamespaceTrial(request)
    def interrupted(signum,_frame):
        raise InterruptedError(f'namespace supervisor received signal {signum}')
    signal.signal(signal.SIGTERM,interrupted)
    signal.signal(signal.SIGINT,interrupted)
    value=run_stages(trial.setup,trial.command,trial.postflight,trial.teardown)
    value.update(requested_rtt_ms=request['rtt'])
    if hasattr(trial,'preflight_raw'):
        value['preflight_raw']=trial.preflight_raw
    if hasattr(trial,'copy'):
        value.update({key:trial.copy[key] for key in ('roles','pins_before','pins_after') if key in trial.copy})
        value['outcome']=trial.copy.get('outcome',value.get('outcome'))
        value['errors'].extend(trial.copy.get('errors',[]))
        value['errors'].sort(key=lambda error:error['time'])
    save(Path(request['folder'])/'provisional.json',value)
    return 1 if value['errors'] else 0



def helper_pins():
    return {name:digest(SCRIPT.with_name(name)) for name in ('transport.py','run.py','operations.py')}


def cpu_quota():
    """Observe cgroup-v2 CPU limits along the current group's mounted ancestry."""
    membership=[line[3:] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::')]
    if len(membership)!=1:
        return 'unavailable (cgroup v2 membership absent)'
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        left,separator,right=line.partition(' - ')
        fields=left.split()
        if separator and right.split()[0]=='cgroup2':
            root,mountpoint=Path(fields[3]),Path(fields[4])
            try:
                relative=Path(membership[0]).relative_to(root)
            except ValueError:
                continue
            current=mountpoint/relative
            values=[]
            while current==mountpoint or mountpoint in current.parents:
                path=current/'cpu.max'
                if path.exists():values.append(path.read_text().strip())
                if current==mountpoint:break
                current=current.parent
            return '; '.join(values) if values else 'unavailable (cpu.max absent)'
    return 'unavailable (cgroup v2 mount absent)'

def role_name(kind, argv):
    if kind=='rcp':
        return 'master'
    if kind=='rsync':
        return 'source' if '--sender' in argv else 'destination'
    if argv==['--protocol-version']:
        return None
    values=[arg.partition('=')[2] for arg in argv if arg.startswith('--role=')]
    values += [argv[index+1] for index,arg in enumerate(argv[:-1]) if arg=='--role']
    if len(values)!=1 or values[0] not in ('source','destination'):
        raise ValueError('daemon wrapper requires exactly one source/destination role')
    return values[0]


def argv_description(argv, request):
    """Known bindings only; opaque operands stay private in any future export."""
    bindings={key:request[key] for key in ('source','destination','folder','log_dir','timing_prefix')}
    bindings.update({'tool:'+key:path for key,path in request['tools'].items()})
    bindings.update(ssh_launcher=str(Path(request['folder'])/'bin'/'ssh'),
                    ssh_config=str(Path(request['folder'])/'ssh_config'),
                    private_identity=str(Path(request['folder'])/'identity'),
                    known_hosts=str(Path(request['folder'])/'known_hosts'))
    bindings.update({'wrapper:'+key:str(Path(request['folder'])/'bin'/key) for key in request['tools']})
    exact={path:(name,'${'+name+'}') for name,path in bindings.items()}
    exact.update({request['source']+'/':('source','${source}/'), request['destination']+'/':('destination','${destination}/'),
                  ADDRESSES['source']+':'+request['source']:('source','${source-host}:${source}'),
                  ADDRESSES['source']+':'+request['source']+'/':('source','${source-host}:${source}/')})
    flags={'--timings':'timing_prefix','--rcpd-path':'wrapper:rcpd','--rsync-path':'wrapper:rsync','--rsh':'ssh_launcher'}
    description=[]
    for index,arg in enumerate(argv):
        credential=arg.startswith('--master-cert-fp=') or arg=='--master-cert-fp' or (index>0 and argv[index-1]=='--master-cert-fp')
        labels=[];symbolic=None
        if not credential and arg in exact:
            name,symbolic=exact[arg];labels=[name]
        if not credential:
            for flag,name in flags.items():
                if name in bindings and arg==flag+'='+bindings[name]:
                    labels=[name];symbolic=flag+'=${'+name+'}'
        description.append(dict(index=index,bindings=labels,classification='credential' if credential else 'bound-path' if labels else 'opaque-private',symbolic=symbolic))
    return dict(bindings=bindings,operands=description)



def record_role(request,kind,argv):
    assert_isolated(request['host'])
    role=role_name(kind,argv)
    if role is None:
        allowed={json.loads((Path(request['folder'])/f'{name}.ready.json').read_text())['netns'] for name in ADDRESSES}
        if ns('net') not in allowed:
            raise RuntimeError('protocol probe escaped owned endpoints')
    else:
        endpoint_guard(request,'source' if role=='source' else 'client')
    binary=request['tools'][kind]
    if role is not None:
        status={line.partition(':')[0]:line.partition(':')[2].strip() for line in Path('/proc/self/status').read_text().splitlines()}
        value=dict(role=role,trial_index=request['trial_index'],command_index=0,tool_identity_key=(kind+'-baseline') if request['variant']['id']=='rcp-baseline' else kind,
                   binary=binary,argv=argv,argv_description=argv_description(argv,request),
                   uid=os.getuid(),gid=os.getgid(),ssh_connection=os.environ.get('SSH_CONNECTION',''),
                   fd_soft=resource.getrlimit(resource.RLIMIT_NOFILE)[0],fd_hard=resource.getrlimit(resource.RLIMIT_NOFILE)[1],
                   cpu_affinity=sorted(os.sched_getaffinity(0)),cpu_quota=cpu_quota(),
                   no_new_privileges=int(status['NoNewPrivs']),capabilities=int(status['CapEff'],16),
                   capability_sets={key:int(status[key],16) for key in ('CapInh','CapPrm','CapEff','CapBnd','CapAmb')},
                   **{key+'ns':ns(key) for key in KINDS})
        path=Path(request['folder'])/f'role-{role}.json'
        with path.open('x') as handle:
            json.dump(value,handle);handle.flush();os.fsync(handle.fileno())
    os.execve(binary,[binary,*argv],clean_environment(Path(request['folder'])/'bin'))


def endpoint_guard(request,role):
    assert_isolated(request['host'])
    endpoint=json.loads((Path(request['folder'])/f'{role}.ready.json').read_text())
    if ns('net')!=endpoint['netns']:
        raise RuntimeError('helper escaped its selected endpoint')


def copy_worker(request):
    from benchmarks import run
    folder=Path(request['folder']);failures=FailureLog();value={}
    try:
        endpoint_guard(request,'client')
        before=json.loads((folder/'evidence-before.json').read_text())
        if helper_pins()!=request['helper_pins'] or pin_tools(request['utilities'])!=request['utility_pins']:
            raise ValueError('transport helper or utility executable changed before copy')
        if shutil.which('ssh')!=str(folder/'bin'/'ssh') or os.environ.get('LC_ALL')!='C':
            raise ValueError('copy environment bypasses private SSH or exact-summary locale')
        wrappers={}
        for kind in request['tools']:
            path=folder/'bin'/kind
            path.write_text(f'#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(str(SCRIPT))} _role {shlex.quote(str(folder/"request.json"))} {kind} "$@"\n')
            path.chmod(0o700)
            wrappers[kind]=str(path)
        wrapper_pins=pin_tools(wrappers)
        commands=run.plan_commands(request['variant'],Path(request['source']),Path(request['destination']),wrappers,'loopback',request['operation'],source_endpoint=SourceEndpoint(ADDRESSES['source'],folder/'bin'/'ssh'))
        if request['timing_policy']=='coarse':
            commands=[[command[0],f"--timings={request['timing_prefix']}",*command[1:]] for command in commands]
        value['pins_before']=pin_tools(request['tools'])
        if value['pins_before']!=request['pins']:
            raise ValueError('selected copy executables changed before timer')
        save(folder/'planned-command.json',dict(commands=commands,trial_index=request['trial_index'],command_index=0,
             argv_description=argv_description(commands[0],request)))
        value['outcome']=run.execute_commands(commands,Path(request['log_dir']),request['timeout'],stable_summary_locale=True)
        value['outcome']['commands']=commands
        if not value['outcome']['ok']:
            failures.add('command',RuntimeError(value['outcome'].get('launch_error') or 'copy command failed or timed out'))
        value['pins_after']=pin_tools(request['tools'])
        if value['pins_after']!=request['pins'] or pin_tools(wrappers)!=wrapper_pins or helper_pins()!=request['helper_pins'] or pin_tools(request['utilities'])!=request['utility_pins']:
            raise ValueError('selected executable/wrapper/helper changed during copy')
        value['roles']={path.stem[5:]:json.loads(path.read_text()) for path in folder.glob('role-*.json')}
        validate_roles(value['roles'],before,request['variant']['tool'],request['tools'])
    except BaseException as error:
        failures.add('copy_worker',error)
    value['errors']=failures.errors;value['failure']=failures.primary
    save(folder/'copy-result.json',value)
    return 1 if failures.errors else 0


def helper_main(argv):
    action,path,*arguments=argv
    request=json.loads(Path(path).read_text())
    if action=='_supervise':
        return supervise(request)
    if action=='_copy':
        return copy_worker(request)
    if action=='_role':
        record_role(request,arguments[0],arguments[1:])
    elif action=='_holder':
        role=arguments[0]
        assert_isolated(request['host'])
        save(Path(request['folder'])/f'{role}.ready.json',dict(pid=os.getpid(),netns=ns('net')))
        while True:time.sleep(3600)
    elif action=='_echo':
        endpoint_guard(request,'source')
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            listener.bind((ADDRESSES['source'],2233));listener.listen()
            while True:
                connection,_=listener.accept()
                with connection:
                    connection.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
                    while payload:=connection.recv(1):connection.sendall(payload)
    elif action=='_probe':
        endpoint_guard(request,'client')
        samples=[]
        with socket.create_connection((ADDRESSES['source'],2233),timeout=5) as connection:
            connection.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
            for _ in range(10):
                started=time.monotonic_ns();connection.sendall(b'x')
                if connection.recv(1)!=b'x':raise RuntimeError('invalid TCP echo payload')
                samples.append((time.monotonic_ns()-started)/1e6)
        print(json.dumps(dict(samples_ms=samples,median_ms=statistics.median(samples))))
    else:
        raise ValueError('unknown internal transport action')
    return 0


def series_observations(proof):
    return dict(capacity=proof['role_validation']['capacity'], resources=proof['role_validation']['resources'])


def validate_transport(proof, variant, tool_records, counts, operation, expected_semantics):
    """Recompute acceptance from embedded evidence, never an inner complete/ok bit."""
    try:
        keys = ('rcp', 'rcpd') if variant['tool'] == 'rcp' else ('rsync',)
        identities = {key: tool_records[key+'-baseline' if variant['id']=='rcp-baseline' else key] for key in keys}
        tools = {key: value['path'] for key,value in identities.items()}
        pins = {key: value['sha256'] for key,value in identities.items()}
        inner = {key:proof[key] for key in ('preflight_raw','postflight','roles','pins_before','pins_after','cleanup')}
        inner['outcome'] = dict(ok=True, exit_codes=[0], timed_out=False)
        result = qualify(inner, proof['outer_exit_code'], proof['host_audit'], variant['tool'], tools, pins,
                         counts['bytes'] if operation=='fresh' else 0, tool_records['ssh'])
        if proof.get('ok') is not True or proof.get('outer_waited') is not True or not result['ok']:
            raise ValueError('owned transport missing successful actual outer/postflight/cleanup gate')
        calculated = result['transport']
        if proof['semantics']!=expected_semantics or calculated['semantics']!=expected_semantics:
            raise ValueError('trial transport semantics differ from selected context')
        for key in ('network','role_validation'):
            if proof[key]!=calculated[key]:
                raise ValueError(f'inconsistent owned {key} proof')
        audit=proof['host_audit']
        if audit.get('launcher_reaped') is not True or not _integer(audit.get('launcher_pid'),1):
            raise ValueError('host audit must prove held launcher reaped')
        host=audit['before']
        before=json.loads(proof['preflight_raw'])
        isolation=before['isolation']
        if host['uid']!=isolation['uid'] or host['gid']!=isolation['gid'] or any(host['namespaces'][kind]!=isolation['host_'+kind+'ns'] for kind in KINDS):
            raise ValueError('host audit does not bind the private namespace entry')
        if stable_host(audit['before']) != stable_host(audit['after']):
            raise ValueError('host topology or identity changed')
        return proof
    except (KeyError, TypeError, AttributeError) as error:
        raise ValueError(f'malformed owned transport proof: {error}') from error


def stable_host(value):
    value=json.loads(json.dumps(value))
    for q in value['qdiscs']:
        for key in ('bytes','packets','drops','overlimits','requeues','backlog','qlen'):
            q.pop(key,None)
    return value


if __name__=='__main__':
    try:
        # direct-script workers must share the planner's canonical SourceEndpoint type
        from benchmarks.transport import helper_main as canonical_helper_main
        sys.exit(canonical_helper_main(sys.argv[1:]))
    except BaseException as error:
        if isinstance(error,SystemExit):raise
        print(f'transport helper failed: {type(error).__name__}: {error}',file=sys.stderr)
        sys.exit(1)
