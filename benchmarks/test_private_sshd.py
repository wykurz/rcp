"""The private SSH endpoint serves one run on loopback and leaves nothing behind."""

import contextlib
import io
import json
import os
from pathlib import Path
import pwd
import shutil
import signal
import stat
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock

from benchmarks import measurements, private_sshd

KEYGEN = r'''#!/usr/bin/env python3
import pathlib, sys
arguments = sys.argv[1:]
path = pathlib.Path(arguments[arguments.index("-f") + 1])
if arguments[arguments.index("-N") + 1] != "":
    sys.exit("a private key must be usable without a passphrase")
path.write_text("PRIVATE KEY of " + path.name + "\n")
path.with_name(path.name + ".pub").write_text("ssh-ed25519 AAAA" + path.name + " comment\n")
'''

# stands in for sshd: checks a configuration, or stays in the foreground until it is stopped
SSHD = r'''#!/usr/bin/env python3
import os, pathlib, signal, sys
state = pathlib.Path(os.environ["FAKE_STATE"])
if "-V" in sys.argv:
    print("OpenSSH_fake, for tests", file=sys.stderr)
    sys.exit(0)
config = pathlib.Path(sys.argv[sys.argv.index("-f") + 1]).read_text()
if "-t" in sys.argv:
    sys.exit(1 if os.environ.get("FAKE_SSHD") == "badconfig" else 0)
port = next(line.split()[1] for line in config.splitlines() if line.startswith("Port "))
with (state / "sshd.launches").open("a") as launches:
    launches.write(port + "\n")
count = len((state / "sshd.launches").read_text().splitlines())
if os.environ.get("FAKE_SSHD") == "busy-once" and count == 1:
    print("error: Bind to port " + port + " on 127.0.0.1 failed: Address already in use.", file=sys.stderr)
    sys.exit(255)
(state / "sshd.pid").write_text(str(os.getpid()))
# a login works only while this file says that something listens on the port
(state / ("listening-" + port)).write_text(str(os.getpid()))
print("Server listening on 127.0.0.1 port " + port + ".", file=sys.stderr, flush=True)
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
try:
    while True:
        signal.pause()
finally:
    (state / ("listening-" + port)).unlink()
'''

# stands in for the ssh client: records how it was called and runs the remote command locally
SSH = r'''#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys
state = pathlib.Path(os.environ["FAKE_STATE"])
with (state / "ssh.calls").open("a") as calls:
    calls.write(json.dumps(sys.argv[1:]) + "\n")
if "-V" in sys.argv:
    print("OpenSSH_fake, for tests", file=sys.stderr)
    sys.exit(0)
behaviour = os.environ.get("FAKE_SSH", "ok")
config = pathlib.Path(sys.argv[sys.argv.index("-F") + 1]).read_text()
port = next(line.split()[1] for line in config.splitlines() if line.strip().startswith("Port "))
listener = state / ("listening-" + port)
try:
    os.kill(int(listener.read_text()), 0)
except (OSError, ValueError):
    behaviour = "refused"
if behaviour == "refused":
    print("ssh: connect to host 127.0.0.1 port 1: Connection refused", file=sys.stderr)
    sys.exit(255)
if behaviour == "nologin":
    print("This account is currently not available.")
    sys.exit(1)
if behaviour == "hang":
    import time
    time.sleep(120)
if behaviour == "nohome":
    # what sshd writes into a session when it cannot enter the account's home directory
    print("Could not chdir to home directory /nonexistent: No such file or directory", file=sys.stderr)
    print("a shell's own complaint about that home", file=sys.stderr)
arguments, operands = sys.argv[1:], []
while arguments:
    argument = arguments.pop(0)
    if argument in ("-F", "-o", "-p", "-l", "-E", "-S"):
        arguments.pop(0)
    elif not argument.startswith("-"):
        operands = arguments
        break
sys.exit(subprocess.run(["/bin/sh", "-c", " ".join(operands)]).returncode if operands else 0)
'''

# GNU time as the wrapper calls it: runs the command and writes its figures in the given format
TIME = r'''#!/usr/bin/env python3
import subprocess, sys
if "--version" in sys.argv:
    print("time (GNU Time) for tests")
    sys.exit(0)
arguments = sys.argv[1:]
output, form = arguments[arguments.index("--output") + 1], arguments[arguments.index("--format") + 1]
code = subprocess.run(arguments[arguments.index("--") + 1:]).returncode
for key, value in (("%U", "1.25"), ("%S", "0.50"), ("%M", "2048"), ("%w", "3"), ("%c", "4"), ("%R", "5"),
                   ("%F", "6"), ("%x", str(code))):
    form = form.replace(key, value)
with open(output, "w") as stream:
    stream.write(form + "\n")
sys.exit(code)
'''

RCPD = "#!/bin/sh\necho \"rcpd ran: $*\"\n"


def tools(directory, **texts):
    """Write stand-in executables and return their paths by name."""
    directory.mkdir(exist_ok=True)
    paths = {}
    for name, text in texts.items():
        paths[name] = directory / name
        paths[name].write_text(text)
        paths[name].chmod(0o755)
    return paths


class EndpointTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / "state"
        self.state.mkdir()
        self.tools = tools(self.root / "tools", sshd=SSHD, ssh=SSH, keygen=KEYGEN, time=TIME, rcpd=RCPD)
        environment = mock.patch.dict(os.environ, {"FAKE_STATE": str(self.state)})
        environment.start()
        self.addCleanup(environment.stop)

    def endpoint(self, **options):
        options.setdefault("resource_time", self.tools["time"])
        endpoint = private_sshd.Endpoint(self.tools["rcpd"], sshd=self.tools["sshd"], ssh=self.tools["ssh"],
                                         keygen=self.tools["keygen"], log=self.root / "sshd.log", **options)
        self.addCleanup(endpoint.stop)
        return endpoint

    def test_start_serves_loopback_only_with_one_fresh_key(self):
        endpoint = self.endpoint()
        endpoint.start()
        directory = endpoint.directory
        self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        # keys and configuration never sit under the results, which are archived
        self.assertNotIn(self.root, directory.parents)
        self.assertEqual((directory / "authorized_keys").read_text(), (directory / "identity.pub").read_text())
        self.assertEqual(stat.S_IMODE((directory / "identity").stat().st_mode) & 0o077, 0)
        server = (directory / "sshd_config").read_text().splitlines()
        account = pwd.getpwuid(os.getuid()).pw_name
        for line in ("ListenAddress 127.0.0.1", "AddressFamily inet", "AuthenticationMethods publickey",
                     "PasswordAuthentication no", "KbdInteractiveAuthentication no", "UsePAM no",
                     "AllowUsers " + account, "DisableForwarding yes", "PermitUserRC no",
                     "PermitUserEnvironment no", f"AuthorizedKeysFile {directory}/authorized_keys",
                     f"HostKey {directory}/host_key"):
            self.assertIn(line, server)
        port = next(line.split()[1] for line in server if line.startswith("Port "))
        self.assertEqual((self.state / "sshd.launches").read_text().split(), [port])
        client = (directory / "ssh_config").read_text().splitlines()
        # the client's own remarks stay out of a session's output; its errors do not
        self.assertIn("  LogLevel ERROR", client)
        for line in ("  HostName 127.0.0.1", "  Port " + port, "  User " + account, "  BatchMode yes",
                     "  IdentitiesOnly yes", "  IdentityAgent none", f"  IdentityFile {directory}/identity",
                     f"  UserKnownHostsFile {directory}/known_hosts", "  GlobalKnownHostsFile /dev/null"):
            self.assertIn(line, client)
        self.assertEqual((directory / "known_hosts").read_text(),
                         f"[127.0.0.1]:{port} ssh-ed25519 AAAAhost_key\n")
        # sshd's own words are kept with the results, where a failed run is diagnosed
        self.assertIn("Server listening", (self.root / "sshd.log").read_text())
        self.assertEqual(endpoint.descriptor(), dict(
            profile="private-sshd-ipv4-loopback-v1", address="127.0.0.1",
            ssh=private_sshd.identity(self.tools["ssh"]), sshd=private_sshd.identity(self.tools["sshd"])))
        self.assertEqual(endpoint.descriptor()["ssh"]["version"], "OpenSSH_fake, for tests")
        # every session is marked from its first instruction, before the wrapper has run
        self.assertIn(f"SetEnv RCP_BENCH_ENDPOINT={endpoint.token}", server)
        # the readiness login gives up on a port that something else answers on, quickly
        self.assertIn(["-F", str(directory / "ssh_config"), "-o", "StrictHostKeyChecking=yes",
                       "-o", "ConnectTimeout=3", "127.0.0.1", "true"],
                      [json.loads(line) for line in (self.state / "ssh.calls").read_text().splitlines()])

    def test_the_copy_finds_the_launcher_before_any_other_ssh(self):
        endpoint = self.endpoint()
        endpoint.start()
        path = endpoint.environment()["PATH"].split(os.pathsep)
        self.assertEqual(path[0], str(endpoint.directory / "bin"))
        self.assertEqual(path[1:], os.environ["PATH"].split(os.pathsep))
        self.assertEqual(sorted(item.name for item in (endpoint.directory / "bin").iterdir()), ["ssh"])
        (self.state / "ssh.calls").unlink()
        # rcp turns host key checking off on its command line; the first setting wins
        subprocess.run(["ssh", "-o", "StrictHostKeyChecking=no", "-N", "127.0.0.1"], check=True,
                       env={**os.environ, **endpoint.environment()})
        call = json.loads((self.state / "ssh.calls").read_text())
        self.assertEqual(call, ["-F", str(endpoint.directory / "ssh_config"), "-o", "StrictHostKeyChecking=yes",
                                "-o", "StrictHostKeyChecking=no", "-N", "127.0.0.1"])

    def test_stop_ends_sshd_and_removes_every_key(self):
        endpoint = self.endpoint()
        endpoint.start()
        directory, pid = endpoint.directory, int((self.state / "sshd.pid").read_text())
        endpoint.stop()
        self.assertFalse(directory.exists())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        # stopping twice, or without ever starting, is harmless
        endpoint.stop()
        self.endpoint().stop()

    def test_a_login_the_account_refuses_fails_setup_with_its_reason(self):
        endpoint = self.endpoint()
        made, mkdtemp = [], tempfile.mkdtemp
        def recording(**options):
            made.append(mkdtemp(**options))
            return made[-1]
        with mock.patch.dict(os.environ, {"FAKE_SSH": "nologin"}), \
                mock.patch.object(private_sshd.tempfile, "mkdtemp", recording), \
                self.assertRaisesRegex(private_sshd.EndpointError, "This account is currently not available"):
            endpoint.start()
        # a failed setup leaves no listener and no key behind
        self.assertIsNone(endpoint.directory)
        with self.assertRaises(ProcessLookupError):
            os.kill(int((self.state / "sshd.pid").read_text()), 0)
        self.assertEqual(len(made), 1)
        self.assertFalse(Path(made[0]).exists())

    def test_a_login_that_prints_anything_of_its_own_fails_setup(self):
        # rcp reads the first line an rcpd prints as its readiness record: a session that says
        # something first, such as sshd's complaint about a missing home, breaks every remote copy
        endpoint = self.endpoint()
        started = time.monotonic()
        with mock.patch.dict(os.environ, {"FAKE_SSH": "nohome"}), self.assertRaisesRegex(
                private_sshd.EndpointError, "a login through the private sshd printed output of its own, which rcp "
                "would take for an rcpd's: Could not chdir to home directory /nonexistent: No such file or directory"):
            endpoint.start()
        # nothing to wait for: the same login would print the same line again
        self.assertLess(time.monotonic() - started, 10)
        self.assertIsNone(endpoint.directory)

    def test_a_stop_signal_while_the_endpoint_starts_ends_the_start_and_leaves_nothing(self):
        # a real signal, raised as the runner raises it: waiting for a helper must not absorb it
        def stop(_number, _frame):
            raise InterruptedError("SIGTERM")
        self.addCleanup(signal.signal, signal.SIGTERM, signal.signal(signal.SIGTERM, stop))
        endpoint = self.endpoint()
        timer = threading.Timer(1, os.kill, (os.getpid(), signal.SIGTERM))
        self.addCleanup(timer.cancel)
        started = time.monotonic()
        with mock.patch.dict(os.environ, {"FAKE_SSH": "hang"}), mock.patch.object(private_sshd, "COMMAND_SECONDS", 4), \
                mock.patch.object(private_sshd, "READY_SECONDS", 4), self.assertRaisesRegex(InterruptedError, "SIGTERM"):
            timer.start()
            endpoint.start()
        self.assertLess(time.monotonic() - started, 3.5)
        self.assertIsNone(endpoint.directory)
        with self.assertRaises(ProcessLookupError):
            os.kill(int((self.state / "sshd.pid").read_text()), 0)

    def test_an_account_sshd_cannot_serve_is_refused_before_anything_is_started(self):
        for patch, message in ((mock.patch.object(private_sshd.pwd, "getpwuid", side_effect=KeyError("uid")),
                                "the current user has no account entry"),
                               (mock.patch.object(private_sshd.os, "getuid", return_value=0),
                                "remote cells cannot run as root")):
            endpoint = self.endpoint()
            started = time.monotonic()
            with self.subTest(message=message), patch, self.assertRaisesRegex(private_sshd.EndpointError, message):
                endpoint.start()
            self.assertLess(time.monotonic() - started, 5)
            self.assertIsNone(endpoint.directory)
        self.assertFalse((self.state / "sshd.launches").exists())

    def test_a_cleanup_that_cannot_look_for_daemons_still_removes_the_keys(self):
        endpoint = self.endpoint()
        endpoint.start()
        directory, pid = endpoint.directory, int((self.state / "sshd.pid").read_text())
        with mock.patch.object(private_sshd.Endpoint, "reap", side_effect=PermissionError("no /proc")):
            endpoint.stop()
        self.assertFalse(directory.exists())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        # and a start that fails keeps its own reason
        with mock.patch.object(private_sshd.Endpoint, "reap", side_effect=PermissionError("no /proc")), \
                mock.patch.dict(os.environ, {"FAKE_SSH": "nologin"}), \
                self.assertRaisesRegex(private_sshd.EndpointError, "not available"):
            self.endpoint().start()

    def test_a_stop_that_is_interrupted_can_be_finished_later(self):
        endpoint = self.endpoint()
        endpoint.start()
        directory, process = endpoint.directory, endpoint.process
        with mock.patch.object(process, "wait", side_effect=InterruptedError("SIGTERM")), \
                self.assertRaises(InterruptedError):
            endpoint.stop()
        # sshd is still remembered, so the next attempt can end it
        self.assertIs(endpoint.process, process)
        endpoint.stop()
        self.assertIsNotNone(process.returncode)
        self.assertFalse(directory.exists())

    def test_a_stop_signal_while_reading_a_daemon_s_figures_is_not_taken_for_a_missing_file(self):
        endpoint = self.endpoint()
        endpoint.start()
        for role in ("source", "destination"):
            subprocess.run([str(endpoint.directory / "rcpd"), "--role", role], check=True, capture_output=True)
        with mock.patch.object(Path, "read_bytes", side_effect=InterruptedError("SIGTERM")), \
                self.assertRaises(InterruptedError):
            endpoint.roles(True)

    def test_a_listener_that_never_answers_fails_setup_within_its_limit(self):
        endpoint = self.endpoint()
        started = time.monotonic()
        with mock.patch.dict(os.environ, {"FAKE_SSH": "refused"}), mock.patch.object(private_sshd, "READY_SECONDS", .5), \
                self.assertRaisesRegex(private_sshd.EndpointError, "did not accept a login within 0.5 s.*Connection refused"):
            endpoint.start()
        self.assertLess(time.monotonic() - started, 10)
        self.assertIsNone(endpoint.directory)

    def test_a_configuration_sshd_rejects_fails_setup(self):
        with mock.patch.dict(os.environ, {"FAKE_SSHD": "badconfig"}), \
                self.assertRaisesRegex(private_sshd.EndpointError, "sshd rejected its configuration"):
            self.endpoint().start()

    def test_a_port_taken_between_choosing_and_listening_is_chosen_again(self):
        endpoint = self.endpoint()
        with mock.patch.dict(os.environ, {"FAKE_SSHD": "busy-once"}):
            endpoint.start()
        launches = (self.state / "sshd.launches").read_text().split()
        self.assertEqual(len(launches), 2)
        self.assertIn("  Port " + launches[1], (endpoint.directory / "ssh_config").read_text().splitlines())

    def test_a_missing_tool_is_named(self):
        endpoint = private_sshd.Endpoint(self.tools["rcpd"], sshd=self.root / "absent", ssh=self.tools["ssh"],
                                         keygen=self.tools["keygen"], log=self.root / "sshd.log")
        with self.assertRaisesRegex(private_sshd.EndpointError, "sshd is not an executable file"):
            endpoint.start()
        self.assertIsNone(endpoint.directory)

    def test_ensure_restarts_a_listener_that_died(self):
        endpoint = self.endpoint()
        endpoint.start()
        self.assertFalse(endpoint.ensure())
        first = int((self.state / "sshd.pid").read_text())
        os.kill(first, signal.SIGKILL)
        deadline = time.monotonic() + 5
        while endpoint.process.poll() is None and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue(endpoint.ensure())
        second = int((self.state / "sshd.pid").read_text())
        self.assertNotEqual(first, second)
        os.kill(second, 0)
        # the same port: the client configuration and the known host key stay valid
        launches = (self.state / "sshd.launches").read_text().split()
        self.assertEqual(launches[0], launches[1])

    def test_the_command_addresses_both_operands_on_loopback(self):
        endpoint = self.endpoint()
        endpoint.start()
        self.assertEqual(endpoint.command(["/bin/rcp", "--summary", "/data/source/set", "/data/destination/copy"]),
                         ["/bin/rcp", "--summary", "--force-remote", "--bind-ip=127.0.0.1",
                          f"--rcpd-path={endpoint.directory}/rcpd", "127.0.0.1:/data/source/set",
                          "127.0.0.1:/data/destination/copy"])

    def test_the_wrapper_measures_each_daemon_and_passes_other_calls_through(self):
        endpoint = self.endpoint()
        endpoint.start()
        wrapper = str(endpoint.directory / "rcpd")
        done = subprocess.run([wrapper, "--role", "source", "--bind-ip", "127.0.0.1"], capture_output=True, text=True)
        self.assertEqual((done.returncode, done.stdout), (0, "rcpd ran: --role source --bind-ip 127.0.0.1\n"))
        done = subprocess.run([wrapper, "--role=destination"], capture_output=True, text=True)
        self.assertEqual(done.stdout, "rcpd ran: --role=destination\n")
        done = subprocess.run([wrapper, "--protocol-version"], capture_output=True, text=True)
        self.assertEqual(done.stdout, "rcpd ran: --protocol-version\n")
        self.assertEqual(sorted(item.name for item in (endpoint.directory / "roles").iterdir()),
                         ["destination.json", "source.json"])
        roles = endpoint.roles(True)
        self.assertEqual(sorted(roles), ["destination", "source"])
        for role in roles.values():
            self.assertEqual(role["status"], "complete")
            self.assertEqual(role["metrics"], dict(user_seconds=1.25, system_seconds=.5, max_rss_kib=2048,
                voluntary_switches=3, involuntary_switches=4, minor_faults=5, major_faults=6, exit_code=0))
        # the figures of one copy are never read as the next copy's
        endpoint.clear()
        self.assertEqual(list((endpoint.directory / "roles").iterdir()), [])

    def test_a_daemon_without_figures_is_reported_unavailable_after_a_bounded_wait(self):
        endpoint = self.endpoint()
        endpoint.start()
        subprocess.run([str(endpoint.directory / "rcpd"), "--role", "source"], check=True, capture_output=True)
        started = time.monotonic()
        with mock.patch.object(private_sshd, "ROLE_WAIT_SECONDS", .3):
            roles = endpoint.roles(True)
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(roles["source"]["status"], "complete")
        self.assertEqual((roles["destination"]["status"], roles["destination"]["metrics"]), ("unavailable", None))
        # a copy that did not succeed has no figures worth waiting for
        with mock.patch.object(private_sshd, "ROLE_WAIT_SECONDS", 60):
            failed = endpoint.roles(False)
        self.assertEqual({role["status"] for role in failed.values()}, {"unavailable"})
        self.assertEqual(failed["source"], measurements.collect(self.root / "absent", False))

    def test_without_a_resource_supervisor_the_wrapper_only_marks_its_daemons(self):
        daemon = tools(self.root / "daemon", rcpd="#!/bin/sh\necho \"rcpd ran: $* as $RCP_BENCH_ENDPOINT\"\n")["rcpd"]
        endpoint = private_sshd.Endpoint(daemon, sshd=self.tools["sshd"], ssh=self.tools["ssh"],
                                         keygen=self.tools["keygen"], log=self.root / "sshd.log")
        self.addCleanup(endpoint.stop)
        endpoint.start()
        done = subprocess.run([str(endpoint.directory / "rcpd"), "--role", "source"], capture_output=True, text=True)
        self.assertEqual(done.stdout, f"rcpd ran: --role source as {endpoint.token}\n")
        self.assertEqual(list((endpoint.directory / "roles").iterdir()), [])
        self.assertIsNone(endpoint.roles(True))

    def test_the_self_check_starts_logs_in_and_stops(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            private_sshd.main(["--sshd", str(self.tools["sshd"]), "--ssh", str(self.tools["ssh"]),
                               "--ssh-keygen", str(self.tools["keygen"])])
        self.assertEqual(output.getvalue(), "private sshd endpoint ready: OpenSSH_fake, for tests\n")
        with self.assertRaises(ProcessLookupError):
            os.kill(int((self.state / "sshd.pid").read_text()), 0)

    def test_a_failed_self_check_shows_what_sshd_logged(self):
        # on an image build the console is all there is: sshd's own account says why a login failed
        with mock.patch.dict(os.environ, {"FAKE_SSH": "nologin"}), self.assertRaises(SystemExit) as failed:
            private_sshd.main(["--sshd", str(self.tools["sshd"]), "--ssh", str(self.tools["ssh"]),
                               "--ssh-keygen", str(self.tools["keygen"])])
        message = str(failed.exception)
        self.assertTrue(message.startswith("private sshd endpoint failed: the private sshd accepted a connection "
                                           "but the login failed: This account is currently not available."), message)
        self.assertRegex(message, r"\nsshd log:\nServer listening on 127\.0\.0\.1 port \d+\.$")
        with self.assertRaises(ProcessLookupError):
            os.kill(int((self.state / "sshd.pid").read_text()), 0)

    def test_reap_ends_only_what_was_started_through_the_wrapper(self):
        daemon = tools(self.root / "daemon", rcpd="#!/bin/sh\nexec sleep 300\n")["rcpd"]
        endpoint = private_sshd.Endpoint(daemon, sshd=self.tools["sshd"], ssh=self.tools["ssh"],
                                         keygen=self.tools["keygen"], log=self.root / "sshd.log",
                                         resource_time=self.tools["time"])
        self.addCleanup(endpoint.stop)
        endpoint.start()
        self.assertEqual(endpoint.reap(), 0)
        # a daemon that outlives its copy, started as rcp starts it: detached from the caller
        marked = subprocess.Popen([str(endpoint.directory / "rcpd"), "--role", "source"], start_new_session=True)
        other = subprocess.Popen(["sleep", "300"])
        self.addCleanup(other.wait)
        self.addCleanup(other.kill)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and endpoint.reap() == 0:
            time.sleep(.05)
        self.assertEqual(marked.wait(timeout=10), -signal.SIGKILL)
        # the supervisor and the daemon both carried the mark: neither is left
        while time.monotonic() < deadline and endpoint.reap():
            time.sleep(.05)
        self.assertEqual(endpoint.reap(), 0)
        self.assertIsNone(other.poll())


class RealEndpointTests(unittest.TestCase):
    """The same endpoint with the installed OpenSSH: a real login over loopback.

    Never skipped: every other test here uses stand-ins that accept any configuration, so
    without these nothing shows that a real sshd and ssh take what the endpoint writes.
    """

    def setUp(self):
        missing = [name for name in ("sshd", "ssh", "ssh-keygen") if private_sshd._find(name) is None]
        self.assertEqual(missing, [], "these tests need OpenSSH (the openssh-server and openssh-client packages)")
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.fakes = tools(self.root / "tools", time=TIME, rcpd=RCPD)
        self.endpoint = private_sshd.Endpoint(self.fakes["rcpd"], log=self.root / "sshd.log",
                                              resource_time=self.fakes["time"])
        self.addCleanup(self.endpoint.stop)
        self.endpoint.start()
        self.environment = {**os.environ, **self.endpoint.environment()}

    def test_a_host_key_the_run_did_not_make_is_refused_whatever_rcp_asks_for(self):
        endpoint = self.endpoint
        other = self.root / "other_key"
        subprocess.run([private_sshd._find("ssh-keygen"), "-q", "-t", "ed25519", "-N", "", "-f", str(other)], check=True)
        known = endpoint.directory / "known_hosts"
        address = known.read_text().split()[0]
        known.write_text(address + " " + " ".join(other.with_name("other_key.pub").read_text().split()[:2]) + "\n")
        done = subprocess.run(["ssh", "-o", "StrictHostKeyChecking=no", "127.0.0.1", "true"], capture_output=True,
                              text=True, env=self.environment, timeout=60)
        self.assertEqual(done.returncode, 255, done.stderr)
        self.assertIn("Host key verification failed", done.stderr)

    def test_a_session_that_has_not_reached_the_wrapper_is_found_and_ended(self):
        endpoint = self.endpoint
        session = subprocess.Popen(["ssh", "-o", "StrictHostKeyChecking=no", "127.0.0.1", "sleep", "300"],
                                   env=self.environment, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(session.wait)
        self.addCleanup(session.kill)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and endpoint.reap() == 0:
            time.sleep(.05)
        # the remote command is gone, so its client returns
        self.assertNotEqual(session.wait(timeout=20), 0)

    def test_a_command_runs_through_the_launcher_and_the_wrapper(self):
        root, endpoint, environment = self.root, self.endpoint, self.environment
        done = subprocess.run(["ssh", "-o", "StrictHostKeyChecking=no", "127.0.0.1",
                               str(endpoint.directory / "rcpd"), "--role", "destination"],
                              capture_output=True, text=True, env=environment, timeout=60)
        self.assertEqual((done.returncode, done.stdout), (0, "rcpd ran: --role destination\n"), done.stderr)
        self.assertEqual(endpoint.roles(True)["destination"]["status"], "complete")
        self.assertRegex(endpoint.descriptor()["ssh"]["version"], "OpenSSH")
        directory = endpoint.directory
        endpoint.stop()
        self.assertFalse(directory.exists())
        self.assertIn("Accepted publickey", (root / "sshd.log").read_text())


if __name__ == "__main__":
    unittest.main()
