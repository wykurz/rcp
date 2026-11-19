"""Private SSH endpoint on IPv4 loopback for the remote cells of a benchmark matrix.

A remote cell copies through rcp's remote path on one host: rcp reaches a source and a
destination rcpd over SSH and the daemons exchange data over loopback TCP. This module owns the
SSH side for one run:

- an unprivileged sshd that listens on 127.0.0.1 only and accepts one freshly made key;
- a launcher named `ssh`, found before any other, that sends every connection to that sshd;
  a login must print nothing of its own, because rcp reads the first line an rcpd prints as its
  readiness record (so the account needs a home directory that sshd can enter);
- a mark on every session (`SetEnv` in the sshd configuration), so that a daemon that outlives
  its copy can be found and ended. sshd starts each remote command in a session of its own,
  outside the copy command's process group.

Everything lives in one private directory under the temporary directory and is removed when the
run ends. Keys are made per run and never leave that directory: it is not under the results,
which are archived. Only IPv4 sockets are needed, and both copy operands name 127.0.0.1, so rcp
never has to enumerate network interfaces, which a sandbox without netlink sockets forbids.
"""

import argparse
import hashlib
import os
from pathlib import Path
import pwd
import re
import secrets
import shutil
import signal
import socket
import subprocess
import tempfile
import time

from benchmarks.signals import SignalCancellation

PROFILE = "private-sshd-ipv4-loopback-v1"
ADDRESS = "127.0.0.1"
# in the environment of every session, so a daemon that outlives its copy can be found
MARKER = "RCP_BENCH_ENDPOINT"
# seconds allowed for sshd to accept its first login, for a daemon to exit by itself after a
# copy that succeeded, and for sshd to exit when asked
READY_SECONDS = 30
EXIT_SECONDS = 5
STOP_SECONDS = 5
PORT_ATTEMPTS = 3
COMMAND_SECONDS = 30
# lines of sshd's log that a failed self-check shows
LOG_LINES = 40
# what may appear unquoted in an ssh configuration file and in a shell script
SAFE_PATH = re.compile(r"/[A-Za-z0-9_./+@-]+")
ACCOUNT = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}")


class EndpointError(RuntimeError):
    """The private SSH endpoint could not be set up or used; the message says why."""


def _capture(argv):
    """Run a helper to its end and return (exit status, stdout, stderr).

    Its output goes to temporary files, not pipes. Waiting on pipes goes through a selector
    that absorbs the exception a stop signal raises, and the helper would run on as if nobody
    had asked the run to stop.
    """
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        code = subprocess.run(argv, stdout=out, stderr=err, stdin=subprocess.DEVNULL,
                              timeout=COMMAND_SECONDS).returncode
        texts = []
        for stream in (out, err):
            stream.seek(0)
            texts.append(stream.read(65536).decode(errors="replace"))
        return code, *texts


def identity(path):
    """Describe an OpenSSH tool: where it is, its digest and what it calls itself."""
    path = Path(path)
    version = None
    try:
        code, out, err = _capture([str(path), "-V"])
        lines = (out.strip() or err.strip()).splitlines()
        # an sshd too old to know -V prints its usage and fails: its digest still identifies it
        if code == 0 and lines:
            version = lines[0][:256]
    except InterruptedError:
        raise
    except (OSError, subprocess.SubprocessError):
        pass
    return dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest(), version=version)


def _find(name):
    """Find an OpenSSH tool; sshd is usually installed where only root's PATH looks."""
    return shutil.which(name, path=os.pathsep.join((os.environ.get("PATH", os.defpath), "/usr/sbin", "/sbin")))


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((ADDRESS, 0))
        return probe.getsockname()[1]


def _last_line(text, first=False):
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[0 if first else -1][:512] if lines else ""


class Endpoint:
    """One run's private sshd and ssh launcher."""

    def __init__(self, *, log, sshd=None, ssh=None, keygen=None):
        self.log = Path(log)
        self.requested = dict(sshd=sshd, ssh=ssh, keygen=keygen)
        self.tools, self.identities = {}, {}
        self.token = secrets.token_hex(16)
        self.directory = self.process = self.port = self.account = None

    def start(self):
        """Make the keys and the configuration, start sshd and wait until a login works."""
        try:
            self._prepare()
            for _ in range(PORT_ATTEMPTS):
                self.port = _free_port()
                self._configure()
                if self._launch():
                    return
            raise EndpointError("the private sshd found no free loopback port")
        except BaseException:
            # a setup that fails leaves no listener and no key behind
            self.stop()
            raise

    def ensure(self):
        """Start sshd again if it has died; return whether it had to be restarted."""
        if self.process is not None and self.process.poll() is None:
            return False
        if self.directory is None:
            raise EndpointError("the private sshd was never started")
        # the same port, so the client configuration and the known host key stay valid
        if not self._launch():
            raise EndpointError("the private sshd could not listen on its port again")
        return True

    def stop(self):
        """End sshd and anything a session started, and remove the keys. Safe at any point.

        What a stop signal interrupts stays remembered, so calling again finishes the job.
        """
        process = self.process
        if process is not None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=STOP_SECONDS)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            self.process = None
        directory = self.directory
        if directory is not None:
            try:
                self.reap()
            except InterruptedError:
                raise
            except OSError:
                # looking for leftover daemons is housekeeping; the keys go regardless
                pass
            finally:
                shutil.rmtree(directory, ignore_errors=True)
                self.directory = None

    def environment(self):
        """Variables for the copy: the launcher is the first `ssh` it finds."""
        return dict(PATH=os.pathsep.join((str(self.directory / "bin"), os.environ.get("PATH", os.defpath))))

    @staticmethod
    def command(local, rcpd):
        """Turn a local copy command into the same copy through that rcpd, both ends on loopback."""
        *command, source, destination = local
        return [*command, "--force-remote", f"--bind-ip={ADDRESS}", f"--rcpd-path={rcpd}",
                f"{ADDRESS}:{source}", f"{ADDRESS}:{destination}"]

    def descriptor(self):
        """What the results record about how remote copies were made."""
        return dict(profile=PROFILE, address=ADDRESS, ssh=self.identities["ssh"], sshd=self.identities["sshd"])

    def reap(self, grace=0):
        """End every process a session started that is still running; return how many.

        A daemon that hangs is not in the copy command's process group: sshd starts it in a
        session of its own. Left alone it would keep working during the next copy. After a
        copy that succeeded a daemon may still be on its way out: the daemons get `grace`
        seconds to exit by themselves, and only what is left then is ended.
        """
        deadline = time.monotonic() + grace
        while True:
            running = self._marked()
            if not running or time.monotonic() >= deadline:
                break
            time.sleep(.05)
        count = 0
        for pid in running:
            try:
                os.kill(pid, signal.SIGKILL)
                count += 1
            except InterruptedError:
                raise
            except OSError:
                # it exited in the meantime
                continue
        return count

    def _marked(self):
        """Processes of this account that carry this endpoint's mark."""
        mark, owner, found = f"{MARKER}={self.token}".encode(), os.getuid(), []
        with os.scandir("/proc") as processes:
            entries = list(processes)
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            try:
                if entry.stat().st_uid == owner and \
                        mark in Path(entry.path, "environ").read_bytes().split(b"\0"):
                    found.append(int(entry.name))
            except InterruptedError:
                raise
            except OSError:
                # it has exited, or is not ours to look at
                continue
        return found

    def _run(self, argv, what):
        try:
            code, out, err = _capture(argv)
        except subprocess.TimeoutExpired:
            raise EndpointError(f"{what} did not finish within {COMMAND_SECONDS} s") from None
        if code:
            raise EndpointError(f"{what}: {_last_line(err) or _last_line(out) or 'no output'}")

    def _prepare(self):
        if os.getuid() == 0:
            # the private sshd serves the account that runs it and refuses root
            raise EndpointError("remote cells cannot run as root: use an unprivileged account")
        try:
            self.account = pwd.getpwuid(os.getuid()).pw_name
        except KeyError:
            raise EndpointError("the current user has no account entry, so sshd cannot log it in") from None
        for name, given in self.requested.items():
            label = "ssh-keygen" if name == "keygen" else name
            path = Path(given or _find(label) or label).absolute()
            if not path.is_file() or not os.access(path, os.X_OK):
                raise EndpointError(f"{label} is not an executable file: {path}")
            self.tools[name] = path
        directory = Path(tempfile.mkdtemp(prefix="rcp-bench-ssh-")).resolve()
        self.directory = directory
        for value, pattern, what in ((str(directory), SAFE_PATH, "temporary directory"),
                                     (self.account, ACCOUNT, "account name"),
                                     (str(self.tools["ssh"]), SAFE_PATH, "path of ssh")):
            if not pattern.fullmatch(value):
                raise EndpointError(f"the {what} cannot be written into an ssh configuration or a script: {value!r}")
        self.identities = {name: identity(self.tools[name]) for name in ("ssh", "sshd")}
        for name in ("identity", "host_key"):
            self._run([str(self.tools["keygen"]), "-q", "-t", "ed25519", "-N", "", "-C", "", "-f",
                       str(directory / name)], "ssh-keygen failed")
            (directory / name).chmod(0o600)
        (directory / "authorized_keys").write_text((directory / "identity.pub").read_text())
        (directory / "authorized_keys").chmod(0o600)
        (directory / "bin").mkdir(mode=0o700)
        # rcp passes StrictHostKeyChecking=no; the first value given wins, so the key stays checked
        # keep launcher bytes stable across runs; the harness fingerprints its executable
        self._script(directory / "bin" / "ssh",
                     'directory=${0%/*}\n'
                     f'exec {self.tools["ssh"]} -F "${{directory%/*}}/ssh_config" -o StrictHostKeyChecking=yes "$@"\n')

    @staticmethod
    def _script(path, body):
        path.write_text("#!/bin/sh\n" + body)
        path.chmod(0o700)

    def _configure(self):
        directory = self.directory
        # StrictModes would refuse keys under a shared sticky temporary directory; this one is
        # ours alone (mode 0700), which is what that check is for
        (directory / "sshd_config").write_text("\n".join((
            f"Port {self.port}", f"ListenAddress {ADDRESS}", "AddressFamily inet",
            f"HostKey {directory}/host_key", "PidFile none",
            f"AuthorizedKeysFile {directory}/authorized_keys", f"AllowUsers {self.account}",
            "AuthenticationMethods publickey", "PubkeyAuthentication yes", "PasswordAuthentication no",
            "KbdInteractiveAuthentication no", "UsePAM no", "PermitRootLogin no", "StrictModes no",
            "PermitUserEnvironment no", "PermitUserRC no", "DisableForwarding yes", "UseDNS no",
            "PrintMotd no", "LoginGraceTime 30", "LogLevel VERBOSE",
            # every session carries the mark from its first instruction
            f"SetEnv {MARKER}={self.token}")) + "\n")
        (directory / "ssh_config").write_text("\n".join((
            "Host *", f"  HostName {ADDRESS}", f"  Port {self.port}", f"  User {self.account}",
            "  AddressFamily inet", f"  IdentityFile {directory}/identity", "  IdentitiesOnly yes",
            "  IdentityAgent none", f"  UserKnownHostsFile {directory}/known_hosts",
            "  GlobalKnownHostsFile /dev/null", "  UpdateHostKeys no", "  BatchMode yes", "  LogLevel ERROR",
            "  ConnectTimeout 10", "  ForwardAgent no", "  ClearAllForwardings yes", "  ProxyCommand none",
            "  ProxyJump none")) + "\n")
        key = " ".join((directory / "host_key.pub").read_text().split()[:2])
        (directory / "known_hosts").write_text(f"[{ADDRESS}]:{self.port} {key}\n")

    def _logged(self, start):
        """Say what sshd has written to its log since `start`: its last lines, on the one line of an error."""
        with self.log.open("rb") as log:
            end = log.seek(0, os.SEEK_END)
            log.seek(max(start, end - 65536))
            lines = [line.strip() for line in log.read().decode(errors="replace").splitlines() if line.strip()]
        return "; sshd logged: " + " | ".join(line[:200] for line in lines[-3:]) if lines else "; sshd logged nothing"

    def _launch(self):
        """Start sshd and wait for a login to work; False when its port was taken meanwhile."""
        config = str(self.directory / "sshd_config")
        self._run([str(self.tools["sshd"]), "-t", "-f", config], "sshd rejected its configuration")
        self.log.parent.mkdir(parents=True, exist_ok=True)
        with self.log.open("ab") as log:
            written = log.tell()
            # a stop signal waits until the handle is kept: an sshd nobody remembers is never stopped.
            # A session of its own: a signal sent to the benchmark's terminal is not sshd's to handle
            with SignalCancellation():
                self.process = subprocess.Popen([str(self.tools["sshd"]), "-D", "-e", "-f", config], stdout=log,
                                                stderr=log, stdin=subprocess.DEVNULL, start_new_session=True)
        deadline, last = time.monotonic() + READY_SECONDS, ""
        # a short connection limit: a port that something else took answers, but not as sshd
        probe = [str(self.directory / "bin" / "ssh"), "-o", "ConnectTimeout=3", ADDRESS, "true"]
        while self.process.poll() is None:
            try:
                code, out, err = _capture(probe)
            except subprocess.TimeoutExpired:
                code, last = None, "the login did not answer"
            if code is not None:
                last = _last_line(err) or _last_line(out)
                if code == 0:
                    if last:
                        # rcp reads the first line an rcpd prints as its readiness record, so a
                        # session that says anything first, such as sshd's complaint about a home
                        # directory it cannot enter, would break every remote copy
                        first = _last_line(err, first=True) or _last_line(out, first=True)
                        raise EndpointError("a login through the private sshd printed output of its own, "
                                            f"which rcp would take for an rcpd's: {first}")
                    return True
                if code != 255:
                    # the connection worked and the account refused the command: waiting cannot help
                    raise EndpointError(f"the private sshd accepted a connection but the login failed: {last}")
            if time.monotonic() >= deadline:
                # the client can only say that nothing answered: whether sshd saw it come is in sshd's log
                raise EndpointError(f"the private sshd did not accept a login within {READY_SECONDS:g} s: {last}" +
                                    self._logged(written))
            time.sleep(.1)
        with self.log.open("rb") as log:
            log.seek(written)
            said = log.read(65536).decode(errors="replace")
        if "Address already in use" in said:
            return False
        raise EndpointError(f"the private sshd exited with status {self.process.returncode}: {_last_line(said)}")


def main(argv=None):
    """Start the endpoint, log in once and stop: shows that this machine can serve remote cells."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    for name in ("sshd", "ssh", "ssh-keygen"):
        parser.add_argument("--" + name, type=Path, help=f"{name} to use; by default the one on PATH")
    args = parser.parse_args(argv)
    with tempfile.TemporaryDirectory() as scratch:
        log = Path(scratch, "sshd.log")
        endpoint = Endpoint(log=log, sshd=args.sshd, ssh=args.ssh, keygen=args.ssh_keygen)
        try:
            endpoint.start()
            print("private sshd endpoint ready:", endpoint.descriptor()["sshd"]["version"] or "unknown version")
        except EndpointError as error:
            # sshd says why it refused a login only in its own log, and where this check runs
            # while an image is built there is nothing else to read afterwards
            try:
                said = "\n".join(log.read_text(errors="replace").splitlines()[-LOG_LINES:])
            except InterruptedError:
                raise
            except OSError:
                said = ""
            raise SystemExit(f"private sshd endpoint failed: {error}" + ("\nsshd log:\n" + said if said else ""))
        finally:
            endpoint.stop()


if __name__ == "__main__":
    main()
