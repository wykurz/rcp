"""Own one native literal-loopback smoke unit; no production/AWS integration.

This is for operator-reviewed binaries, not a general hostile-code sandbox.
Service start/stop are name-bound manager actions, not atomic InvocationID fences.
"""
import argparse
import fcntl
import ctypes
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import shutil
import stat
import subprocess
import sys
import time
import uuid

from benchmarks import literal_loopback as contract

FIXTURES = Path("/run/rcp-loopback-smoke")
EVIDENCE = Path("/var/lib/rcp-loopback-smoke")
MAX_INPUT = 256 * 1024 * 1024
FIXED_FILES = {"empty": b"", "nested/small": b"literal loopback\n" * 64,
               "large": bytes(range(256)) * 257}
PROPERTIES = ("Id", "LoadState", "ActiveState", "SubState", "Result", "ExecMainStatus",
              "Type", "RemainAfterExit", "InvocationID", "ControlGroup", "Description",
              "ExecMainStartTimestampMonotonic", "ExecMainExitTimestampMonotonic")
ENV = dict(PATH="/usr/sbin:/usr/bin:/sbin:/bin", LANG="C", LC_ALL="C", HOME="/root")


def encoded(value):
    return (json.dumps(value, sort_keys=True, allow_nan=False) + "\n").encode()


def held_read(directory, name, limit):
    contract.need(re.fullmatch(r"[A-Za-z0-9_.-]+", name), "simple held filename required")
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    with os.fdopen(fd, "rb") as stream:
        contract.need(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), "regular input required")
        raw = stream.read(limit + 1)
    contract.need(len(raw) <= limit, "held input exceeds bound")
    return raw


def write_new(directory, name, raw, mode=0o600):
    contract.need(re.fullmatch(r"[A-Za-z0-9_.-]+", name), "simple held filename required")
    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode, dir_fd=directory)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fchmod(stream.fileno(), mode)
        os.fsync(stream.fileno())
    os.fsync(directory)


def root_directory(path, *, create=False):
    """Walk root-owned components through held descriptors, rejecting symlinks."""
    path = Path(path)
    contract.need(path.is_absolute() and ".." not in path.parts, "absolute root-owned path required")
    current = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=current)
                    os.fsync(current)
                except FileExistsError:
                    pass
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
            try:
                info = os.fstat(child)
                contract.need(info.st_uid == 0 and not info.st_mode & 0o022,
                              "root-owned nonwritable ancestor required")
            except BaseException:
                os.close(child)
                raise
            os.close(current)
            current = child
        return current
    except BaseException:
        os.close(current)
        raise


class Journal:
    """Owner-only evidence and one-shot intents through a locked directory FD."""
    def __init__(self, directory):
        self.fd = root_directory(directory)
        self.lock = os.open("owner.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
                            0o600, dir_fd=self.fd)
        info = os.fstat(self.lock)
        contract.need(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and
                      not info.st_mode & 0o077, "private journal lock required")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(self.lock)
            os.close(self.fd)
            raise

    def one(self, name):
        try:
            return json.loads(held_read(self.fd, name + ".json", 131072))
        except FileNotFoundError:
            return None

    def put(self, name, value):
        old = self.one(name)
        if old is not None:
            contract.need(old == value, "immutable journal evidence conflicts")
        else:
            write_new(self.fd, name + ".json", encoded(value))

    def close(self):
        os.close(self.lock)
        os.close(self.fd)


def copy_reviewed_elf(source, directory, name, digest):
    contract.need(Path(source).is_absolute() and ".." not in Path(source).parts and
                  re.fullmatch(r"[0-9a-f]{64}", digest), "absolute reviewed binary and SHA256 required")
    fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        contract.need(stat.S_ISREG(info.st_mode) and 64 <= info.st_size <= MAX_INPUT,
                      "bounded regular reviewed binary required")
        raw = bytearray()
        while len(raw) <= MAX_INPUT:
            block = os.read(fd, min(1024 * 1024, MAX_INPUT + 1 - len(raw)))
            if not block:
                break
            raw.extend(block)
        contract.need(len(raw) == info.st_size and hashlib.sha256(raw).hexdigest() == digest,
                      "copied reviewed binary digest differs")
        contract.need(raw[:6] == b"\x7fELF\x02\x01" and int.from_bytes(raw[18:20], "little") == 62,
                      "reviewed Linux x86_64 ELF required")
        write_new(directory, name, raw, 0o755)
    finally:
        os.close(fd)


def command(argv, *, timeout=20, accepted=(0,)):
    result = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=ENV, timeout=timeout, check=False)
    contract.need(result.returncode in accepted, "trusted host tool failed: " + Path(argv[0]).name +
                  " (exit " + str(result.returncode) + ")")
    contract.need(len(result.stdout) <= 131072 and len(result.stderr) <= 131072,
                  "trusted host tool diagnostics exceed bound")
    return result


def parse_properties(raw):
    contract.need(len(raw) <= 65536, "unit property response exceeds bound")
    result = {}
    for line in raw.decode().splitlines():
        key, separator, value = line.partition("=")
        contract.need(separator and key in PROPERTIES and key not in result,
                      "unit property response malformed")
        result[key] = value
    return result


def require_cgroup2(directory):
    """Bind kernel-removal inference to an actual cgroup2 descriptor."""
    libc = ctypes.CDLL(None, use_errno=True)
    libc.fstatfs.argtypes = (ctypes.c_int, ctypes.c_void_p)
    libc.fstatfs.restype = ctypes.c_int
    buffer = ctypes.create_string_buffer(256)
    if libc.fstatfs(directory, buffer) != 0:
        raise OSError(ctypes.get_errno(), "fstatfs failed for held cgroup")
    contract.need(ctypes.c_long.from_buffer(buffer).value & 0xffffffff == 0x63677270,
                  "held directory is not a cgroup v2 filesystem")


def cgroup_empty(directory):
    """No parent/cached counters: the held original cgroup or its kernel removal."""
    require_cgroup2(directory)
    if os.fstat(directory).st_nlink == 0:
        # cgroup v2 rmdir requires no children or live processes (zombies may remain).
        return "held-original-cgroup-removed"
    events = contract._fields(held_read(directory, "cgroup.events", 4096).decode())
    contract.need(events.get("populated") == 0, "owned cgroup still has live descendants")
    return "held-original-cgroup-recursively-empty"


class NativePort:
    def __init__(self, plan):
        self.plan = plan
        self.name = contract.unit_name(plan["id"])
        self.host = FIXTURES / plan["id"]
        self.cg = None
        self.identity = None

    def inspect(self):
        result = command(["/usr/bin/systemctl", "show", "--no-pager",
                          "--property=" + ",".join(PROPERTIES), self.name], accepted=(0, 1, 4))
        value = parse_properties(result.stdout)
        contract.need(value.get("Id") == self.name, "manager returned a different unit")
        return value

    def start(self):
        name, properties = contract.service_properties(identifier=self.plan["id"],
            account=self.plan["account"], uid=self.plan["uid"], gid=self.plan["gid"],
            host_root=str(self.host))
        properties.update(CPUAccounting="yes", MemoryAccounting="yes",
                          Description=self.plan["description"], Restart="no")
        argv = ["/usr/bin/systemd-run", "--no-ask-password", "--quiet", "--unit=" + name]
        argv += ["--property=" + key + "=" + value for key, value in sorted(properties.items())]
        # Default systemd-run waits for exec startup, not service completion.
        # --wait would deadlock on the intentionally retained active/exited unit.
        argv += ["/usr/bin/python3", "-I", contract.GUEST + "/lib/literal_loopback_exec.py",
                 "_inner", contract.GUEST + "/plan.json"]
        command(argv)

    def capture(self, properties):
        contract.need(properties.get("Description") == self.plan["description"],
                      "unit plan identity differs")
        identity = contract.RetainedUnit.capture(self.plan["id"], properties)
        self.identity = identity
        parent, fd = None, None
        try:
            parent = root_directory("/sys/fs/cgroup/system.slice")
            fd = os.open(self.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            require_cgroup2(fd)
            contract.need(os.fstat(fd).st_dev == os.fstat(parent).st_dev,
                          "owned cgroup is not on the expected hierarchy")
        except (OSError, ValueError) as error:
            if fd is not None:
                os.close(fd)
            self.capture_error = type(error).__name__
            # The manager identity is sufficient for a matched stop, but not
            # for metrics or fixture deletion without the held cgroup proof.
        else:
            self.cg = fd
        finally:
            if parent is not None:
                os.close(parent)
        return identity

    def metrics(self, properties):
        contract.need(self.cg is not None and self.identity is not None,
                      "held cgroup identity missing")
        return self.identity.final_metrics(properties,
            events=held_read(self.cg, "cgroup.events", 4096).decode(),
            cpu_stat=held_read(self.cg, "cpu.stat", 4096).decode(),
            memory_peak=held_read(self.cg, "memory.peak", 128).decode())

    def validate_copy(self):
        home = root_directory(self.host)
        try:
            directory = os.open("home", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=home)
            try:
                result = json.loads(held_read(directory, "inner-result.json", 65536))
            finally:
                os.close(directory)
            contract.need(result.get("schema") == contract.SCHEMA and
                          result.get("profile") == contract.PROFILE and
                          result.get("status") == "copy-succeeded", "inner copy did not succeed")
            destination = os.open("destination", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                  dir_fd=home)
            try:
                copied = os.open("copied", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                 dir_fd=destination)
            finally:
                os.close(destination)
            try:
                contract.need(set(os.listdir(copied)) == {"empty", "nested", "large"},
                              "copied root inventory differs")
                nested = os.open("nested", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                 dir_fd=copied)
                try:
                    contract.need(os.listdir(nested) == ["small"], "copied nested inventory differs")
                    observed = {"nested/small": held_read(nested, "small", 2048)}
                finally:
                    os.close(nested)
                observed.update({name: held_read(copied, name, len(FIXED_FILES[name]))
                                 for name in ("empty", "large")})
                contract.need(observed == FIXED_FILES, "tiny native fixture content differs")
            finally:
                os.close(copied)
            return {"fixed_fixture_content_verified": True,
                    "command_seconds": result["command_seconds"], "identity": result["identity"]}
        finally:
            os.close(home)

    def stop(self, identity):
        current = self.inspect()
        identity.check(current)
        contract.need(current.get("Description") == self.plan["description"], "unit plan changed")
        # Cooperative manager ownership only: StopUnit has no InvocationID condition.
        command(["/usr/bin/systemctl", "stop", "--no-ask-password", self.name], timeout=20)

    def stopped(self, identity):
        current = self.inspect()
        contract.need(current.get("LoadState") == "not-found" or
                      (current.get("InvocationID") == identity.invocation_id and
                       current.get("ActiveState") in ("inactive", "failed")),
                      "owned unit is not inactive or its identity changed")
        contract.need(self.cg is not None, "held original cgroup proof unavailable; preserve fixture")
        return {"manager_inactive": True, "cgroup_proof": cgroup_empty(self.cg)}

    def remove_fixture(self):
        contract.need(shutil.rmtree.avoids_symlink_attacks, "FD-safe recursive cleanup unavailable")
        parent = root_directory(FIXTURES)
        try:
            info = os.stat(self.plan["id"], dir_fd=parent, follow_symlinks=False)
            contract.need(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and
                          not info.st_mode & 0o022, "owned fixture identity differs")
            shutil.rmtree(self.plan["id"], dir_fd=parent)
            os.fsync(parent)
        finally:
            os.close(parent)

    def close(self):
        if self.cg is not None:
            os.close(self.cg)
            self.cg = None


def run_owned(port, journal, plan, *, clock=time.monotonic, sleep=time.sleep):
    """One start, verified counters, and cleanup of only the retained invocation."""
    contract.need(journal.one("plan") == plan, "retained plan differs")
    previous = journal.one("result")
    if previous is not None:
        contract.need(previous.get("id") == plan["id"] and previous.get("schema") == contract.SCHEMA and
                      previous.get("profile") == contract.PROFILE and previous.get("cleanup") == "verified",
                      "completed historical result differs")
        return previous
    identity = None
    result = dict(schema=contract.SCHEMA, profile=contract.PROFILE, id=plan["id"],
                  status="failed", cleanup="held", metrics=None)
    deadline = clock() + contract.MAX_NATIVE_SECONDS + 30
    expected_start = {"id": plan["id"], "unit": plan["unit"],
        "plan_sha256": hashlib.sha256(encoded(plan)).hexdigest()}
    start_intent = journal.one("start-intent")
    contract.need(start_intent is None or start_intent == expected_start, "retained start intent differs")
    contract.need(start_intent is not None or
                  all(journal.one(key) is None for key in ("unit-identity", "stop-intent", "stopped")),
                  "later state exists without a start intent")
    try:
        if start_intent is None:
            contract.need(port.inspect().get("LoadState") == "not-found", "trial unit already exists")
            journal.put("start-intent", expected_start)
            try:
                port.start()
            except BaseException as error:
                journal.put("start-outcome-uncertain", {"error_type": type(error).__name__})
        # A prior/lost start is reconciled by observation only; never resend.
        properties = port.inspect()
        captured = port.capture(properties)
        evidence = dict(unit=captured.unit, invocation_id=captured.invocation_id,
                        cgroup=captured.cgroup)
        journal.put("unit-identity", evidence)
        identity = captured
        while True:
            contract.need(clock() < deadline, "native smoke deadline exceeded")
            properties = port.inspect()
            identity.check(properties)
            if properties.get("ActiveState") == "active" and properties.get("SubState") == "exited":
                result["metrics"] = port.metrics(properties)
                break
            contract.need(properties.get("ActiveState") not in ("failed", "inactive"),
                          "copy unit failed before measurement")
            sleep(.1)
        result["copy"] = port.validate_copy()
        result["status"] = "passed"
    except BaseException as error:
        result["failure"] = {"error_type": type(error).__name__, "message": str(error)[:512]}
    finally:
        if identity is not None:
            try:
                expected_stop = {"unit": identity.unit, "invocation_id": identity.invocation_id}
                stop_intent = journal.one("stop-intent")
                contract.need(stop_intent is None or stop_intent == expected_stop,
                              "retained stop intent differs")
                if stop_intent is None:
                    journal.put("stop-intent", expected_stop)
                    try:
                        port.stop(identity)
                    except BaseException as error:
                        journal.put("stop-outcome-uncertain", {"error_type": type(error).__name__})
                stopped = port.stopped(identity)
                journal.put("stopped", stopped)
                port.remove_fixture()
                journal.put("fixture-removed", {"id": plan["id"]})
                result["cleanup"] = "verified"
            except BaseException as error:
                result["cleanup_failure"] = {"error_type": type(error).__name__,
                                             "message": str(error)[:512]}
                result["status"] = "failed"
        try:
            if result["cleanup"] == "verified":
                journal.put("result", result)
            elif journal.one("held") is None:
                journal.put("held", result)
        finally:
            port.close()
    return result


def _mkdir(directory, name, mode=0o755):
    os.mkdir(name, mode, dir_fd=directory)
    os.fsync(directory)
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
    os.fchmod(fd, mode)
    return fd


def _public_key(path):
    raw = Path(path).read_bytes()
    contract.need(len(raw) <= 1024, "ephemeral public key exceeds bound")
    fields = raw.decode().split()
    contract.need(len(fields) == 3 and fields[0] == "ssh-ed25519" and
                  re.fullmatch(r"[A-Za-z0-9+/=]+", fields[1]), "unexpected public key format")
    return fields[:2]


def require_native_host():
    for path in ("/usr/bin/systemd-run", "/usr/bin/systemctl", "/usr/bin/ssh-keygen",
                 "/usr/bin/ssh", "/usr/sbin/sshd", "/usr/bin/python3"):
        contract.need(Path(path).is_file(), "required native tool is unavailable")
    contract.need({"cpu", "memory"} <= set(Path("/sys/fs/cgroup/cgroup.controllers").read_text().split()),
                  "cgroup v2 CPU and memory controllers required")


def prepare(args):
    """Root-only, fixed fixture preparation; no selected binary is run here."""
    contract.need(os.geteuid() == 0, "native owner requires explicit root execution")
    record = pwd.getpwnam(args.account)
    contract.need(record.pw_uid > 0 and record.pw_gid > 0 and
                  re.fullmatch(r"[0-9a-f]{40}", args.source_commit) and args.ack_reviewed_binaries,
                  "reviewed source and existing nonroot identity required")
    contract.need(shutil.rmtree.avoids_symlink_attacks, "FD-safe cleanup required")
    require_native_host()
    contract.need(set(os.getgrouplist(args.account, record.pw_gid)) <= {record.pw_gid},
                  "native smoke requires an account without extra host groups")
    identifier = uuid.uuid4().hex
    parents = [root_directory(path, create=True) for path in (FIXTURES, EVIDENCE)]
    fixture_parent, evidence_parent = parents
    fixture = None
    journal = None
    created = False
    try:
        for parent in parents:
            contract.need(not os.fstat(parent).st_mode & 0o077, "private root parent required")
        fixture = _mkdir(fixture_parent, identifier)
        created = True
        evidence = _mkdir(evidence_parent, identifier, 0o700)
        os.close(evidence)
        journal = Journal(EVIDENCE / identifier)
        children = {}
        try:
            for name in ("bin", "lib", "source", "home", "destination"):
                children[name] = _mkdir(fixture, name, 0o700 if name == "home" else 0o755)
            copy_reviewed_elf(args.rcp, children["bin"], "rcp", args.rcp_sha256)
            copy_reviewed_elf(args.rcpd, children["bin"], "rcpd.real", args.rcpd_sha256)
            helper_hashes = {}
            for name in ("literal_loopback.py", "literal_loopback_exec.py"):
                parent = os.open(Path(__file__).parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    raw = held_read(parent, name, 131072)
                finally:
                    os.close(parent)
                helper_hashes[name] = hashlib.sha256(raw).hexdigest()
                write_new(children["lib"], name, raw, 0o644)
            wrapper = ("#!/usr/bin/python3 -I\nimport sys\n"
                "sys.path.insert(0, '/opt/rcp-loopback/lib')\n"
                "from literal_loopback_exec import role_main\n"
                "role_main('/opt/rcp-loopback/plan.json', sys.argv[1:])\n")
            write_new(children["bin"], "rcpd", wrapper.encode(), 0o755)
            write_new(children["bin"], "ssh", contract.ssh_wrapper().encode(), 0o755)
            nested = _mkdir(children["source"], "nested")
            try:
                write_new(children["source"], "empty", FIXED_FILES["empty"], 0o644)
                write_new(children["source"], "large", FIXED_FILES["large"], 0o644)
                write_new(nested, "small", FIXED_FILES["nested/small"], 0o644)
            finally:
                os.close(nested)
            account = contract.account_view(Path("/etc/passwd").read_text(), args.account,
                                            record.pw_uid, record.pw_gid)
            write_new(fixture, "passwd", account.encode(), 0o644)
            write_new(fixture, "sshd_config", contract.ssh_server_config(args.account).encode(), 0o644)
            client = contract.ssh_client_config(args.account).encode()
            write_new(fixture, "ssh_config", client, 0o644)
            ssh = _mkdir(children["home"], ".ssh", 0o700)
            try:
                write_new(ssh, "config", client)
            finally:
                os.close(ssh)
            host = FIXTURES / identifier
            # These are new disposable test keys, unrelated to GitHub/AWS/login keys.
            # Private bytes stay in these files; never stdout, argv, environment or Git.
            for name in ("hostkey", "identity"):
                command(["/usr/bin/ssh-keygen", "-q", "-t", "ed25519", "-N", "",
                         "-C", "rcp-loopback-" + identifier, "-f", str(host / "home" / name)])
            host_key = _public_key(host / "home/hostkey.pub")
            identity_key = _public_key(host / "home/identity.pub")
            write_new(children["home"], "authorized_keys",
                      ("restrict " + " ".join(identity_key) + "\n").encode())
            write_new(children["home"], "known_hosts",
                      ("[127.0.0.1]:2222 " + " ".join(host_key) + "\n").encode())
            for name in (".", ".ssh", ".ssh/config", "hostkey", "hostkey.pub", "identity",
                         "identity.pub", "authorized_keys", "known_hosts"):
                os.chown(host / "home" / name, record.pw_uid, record.pw_gid, follow_symlinks=False)
            os.fchown(children["destination"], record.pw_uid, record.pw_gid)
            os.fchmod(children["destination"], 0o700)
            plan = dict(schema=contract.SCHEMA, profile=contract.PROFILE, id=identifier,
                unit=contract.unit_name(identifier), account=args.account, uid=record.pw_uid,
                gid=record.pw_gid, host_netns=os.readlink("/proc/self/ns/net"),
                source_commit=args.source_commit,
                source_provenance="operator assertion, not cryptographic build attestation",
                binary_sha256=dict(rcp=args.rcp_sha256, rcpd=args.rcpd_sha256),
                helper_sha256=helper_hashes,
                fixture={name: dict(size=len(raw), sha256=hashlib.sha256(raw).hexdigest())
                         for name, raw in FIXED_FILES.items()},
                trusted_native_smoke_only=True)
            plan["description"] = "rcp-loopback " + identifier + " " + hashlib.sha256(encoded(plan)).hexdigest()
            write_new(fixture, "plan.json", encoded(plan), 0o644)
            journal.put("plan", plan)
        finally:
            for fd in children.values():
                os.close(fd)
        return plan, journal
    except BaseException as error:
        if journal is not None:
            journal.put("preparation-failed", {"error_type": type(error).__name__,
                "no_service_start_requested": True})
            journal.close()
        if created:
            shutil.rmtree(identifier, dir_fd=fixture_parent)
            os.fsync(fixture_parent)
        raise
    finally:
        if fixture is not None:
            os.close(fixture)
        for fd in parents:
            os.close(fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--run-reviewed-native-smoke", action="store_true")
    mode.add_argument("--recover")
    parser.add_argument("--ack-reviewed-binaries", action="store_true")
    parser.add_argument("--source-commit")
    parser.add_argument("--account")
    parser.add_argument("--rcp")
    parser.add_argument("--rcp-sha256")
    parser.add_argument("--rcpd")
    parser.add_argument("--rcpd-sha256")
    args = parser.parse_args(argv)
    contract.need(os.geteuid() == 0, "native owner requires explicit root execution")
    os.umask(0o077)
    if args.recover:
        identifier = args.recover
        contract.unit_name(identifier)
        contract.need(not args.run_reviewed_native_smoke and
                      not any((args.account, args.source_commit, args.rcp, args.rcpd,
                               args.rcp_sha256, args.rcpd_sha256, args.ack_reviewed_binaries)),
                      "recovery cannot replace the retained plan or inputs")
        journal = Journal(EVIDENCE / identifier)
        plan = journal.one("plan")
        contract.need(plan is not None and plan["id"] == identifier and
                      plan["schema"] == contract.SCHEMA and plan["profile"] == contract.PROFILE,
                      "retained recovery plan differs")
    else:
        contract.need(all((args.account, args.source_commit, args.rcp, args.rcpd,
                           args.rcp_sha256, args.rcpd_sha256, args.ack_reviewed_binaries)),
                      "all reviewed input bindings required")
        plan, journal = prepare(args)
    try:
        result = run_owned(NativePort(plan), journal, plan)
        print(json.dumps(result, sort_keys=True))
        return 0 if result["status"] == "passed" and result["cleanup"] == "verified" else 1
    finally:
        journal.close()


if __name__ == "__main__":
    raise SystemExit(main())
