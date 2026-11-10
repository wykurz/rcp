"""Native literal-loopback executor helpers; no AWS or production integration.

Only reviewed binaries and an owned tiny fixture are supported by the initial
native smoke. This module is not a general hostile-code sandbox.
"""
import json
import os
import pwd
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time

if __package__:
    from . import literal_loopback as contract
else:
    sys.path.insert(0, str(Path(__file__).parent))
    import literal_loopback as contract

ROOT = Path(contract.GUEST)
HOME = ROOT / "home"


def save_new(path, value):
    with Path(path).open("x") as handle:
        json.dump(value, handle, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def read_plan(path):
    need_path = ROOT / "plan.json"
    contract.need(Path(path) == need_path, "private mounted plan path required")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        contract.need(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and
                      not info.st_mode & 0o022 and info.st_size <= 65536,
                      "plan must be a bounded root-owned read-only input")
        raw = os.read(fd, 65537)
        contract.need(len(raw) <= 65536, "plan exceeds input bound")
        value = json.loads(raw)
    finally:
        os.close(fd)
    contract.need(value.get("schema") == contract.SCHEMA and
                  contract.unit_name(value["id"]) == value["unit"], "private plan identity differs")
    return value


def clean_environment(plan):
    return dict(PATH=str(ROOT / "bin") + ":/usr/bin:/bin", HOME=str(HOME),
                USER=plan["account"], LOGNAME=plan["account"], SHELL="/bin/sh",
                LANG="C", LC_ALL="C")


def identity(plan, *, uid, gid, status, netns, interfaces):
    """Pure checks against the root-supplied private-unit identity."""
    contract.need(uid == plan["uid"] and gid == plan["gid"] and uid > 0,
                  "payload identity differs from nonroot plan")
    contract.need(status.get("NoNewPrivs") == "1" and all(
        status.get(name) is not None and int(status[name], 16) == 0
        for name in ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb")),
        "payload retained privilege")
    contract.need(isinstance(netns, str) and netns.startswith("net:[") and
                  netns != plan["host_netns"] and set(interfaces) == {"lo"},
                  "payload is not in a private loopback-only network")
    groups = status.get("Groups")
    contract.need(isinstance(groups, str) and all(value.isdigit() for value in groups.split()) and
                  set(map(int, groups.split())) <= {gid}, "payload retained extra host groups")
    return dict(uid=uid, gid=gid, supplementary_groups=sorted(set(map(int, groups.split()))),
                netns=netns, interfaces=list(interfaces),
                no_new_privileges=True, capabilities=0)


def verify_account(plan, by_name, by_uid):
    expected = [plan["account"], str(plan["uid"]), str(plan["gid"]), str(HOME), "/bin/sh"]
    for record in (by_name, by_uid):
        actual = [record.pw_name, str(record.pw_uid), str(record.pw_gid), record.pw_dir, record.pw_shell]
        contract.need(actual == expected, "private NSS name/UID/home/shell mapping differs")


def observed_identity(plan):
    verify_account(plan, pwd.getpwnam(plan["account"]), pwd.getpwuid(plan["uid"]))
    status = {a.strip(): b.strip() for line in Path("/proc/self/status").read_text().splitlines()
              for a, sep, b in [line.partition(":")] if sep}
    interfaces = [line.partition(":")[0].strip()
                  for line in Path("/proc/net/dev").read_text().splitlines()[2:]]
    return identity(plan, uid=os.geteuid(), gid=os.getegid(), status=status,
                    netns=os.readlink("/proc/self/ns/net"), interfaces=interfaces)


def remaining(deadline, maximum):
    value = min(maximum, deadline - time.monotonic())
    contract.need(value > 0, "private copy deadline expired before spawn")
    return value


def _run(argv, plan, timeout, *, stdout=subprocess.PIPE, stderr=subprocess.PIPE, deadline=None):
    if deadline is not None:
        timeout = remaining(deadline, timeout)
    contract.need(timeout > 0, "positive subprocess timeout required before spawn")
    return subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                          env=clean_environment(plan), timeout=timeout, check=True)


def role_main(path, argv):
    plan = read_plan(path)
    proof = observed_identity(plan)
    role = None
    for index, value in enumerate(argv):
        if value.startswith("--role="):
            contract.need(role is None, "duplicate daemon role")
            role = value.split("=", 1)[1]
        elif value == "--role":
            contract.need(role is None and index + 1 < len(argv), "missing or duplicate daemon role")
            role = argv[index + 1]
    if role is not None:
        contract.need(role in ("source", "destination"), "unexpected daemon role")
        connection = os.environ.get("SSH_CONNECTION", "").split()
        contract.need(len(connection) == 4 and connection[0] == connection[2] == "127.0.0.1"
                      and connection[3] == "2222", "daemon escaped private SSH route")
        save_new(HOME / ("role-" + role + ".json"),
                 dict(role=role, identity=proof, pid=os.getpid(),
                      binary_sha256=plan["binary_sha256"]["rcpd"]))
    else:
        contract.need(argv == ["--protocol-version"], "unqualified daemon probe")
    binary = str(ROOT / "bin/rcpd.real")
    os.execve(binary, [binary, *argv], clean_environment(plan))


def failure(result, stage, error):
    row = dict(stage=stage, type=type(error).__name__, message=str(error)[:1024])
    result.setdefault("errors", []).append(row)
    result.setdefault("failure", row)
    result["status"] = "failed"


def stop_server(server):
    """Reap the known Popen child; never let teardown hide the initiating error."""
    errors = []
    report = dict(terminate_requested=False, kill_requested=False, reaped=False,
                  initial_exit_code=None, final_exit_code=None)
    try:
        report["initial_exit_code"] = server.poll()
        if report["initial_exit_code"] is not None:
            errors.append(RuntimeError("private SSH parent exited before its owned stop"))
        else:
            try:
                report["terminate_requested"] = True
                server.terminate()
            except ProcessLookupError:
                pass
            except BaseException as error:
                errors.append(error)
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                report["kill_requested"] = True
                server.kill()
            except ProcessLookupError:
                pass
            except BaseException as error:
                errors.append(error)
            try:
                server.wait(timeout=5)
            except BaseException as error:
                errors.append(error)
        except BaseException as error:
            errors.append(error)
        report["final_exit_code"] = server.poll()
        report["reaped"] = report["final_exit_code"] is not None
        contract.need(report["reaped"], "private SSH child is not reaped")
    except BaseException as error:
        errors.append(error)
    return errors, report


def inner_main(path):
    plan = read_plan(path)
    observed = observed_identity(plan)
    server = None
    result = dict(schema=contract.SCHEMA, profile=contract.PROFILE, identity=observed,
                  status="failed", command_seconds=None)
    deadline = time.monotonic() + 150
    try:
        # Probes execute with the same unprivileged identity and private files.
        versions = [_run([str(ROOT / "bin" / tool), "--protocol-version"], plan, 5).stdout
                    for tool in ("rcp", "rcpd.real")]
        contract.need(versions[0] and len(versions[0]) <= 4096 and versions[0] == versions[1],
                      "copy binaries do not have matching protocol identity")
        _run(["/usr/sbin/sshd", "-t", "-f", str(ROOT / "sshd_config")], plan, 5)
        with (HOME / "sshd.log").open("x") as log:
            remaining(deadline, 150)
            server = subprocess.Popen(["/usr/sbin/sshd", "-D", "-e", "-f",
                str(ROOT / "sshd_config")], stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                env=clean_environment(plan))
            ready = False
            while time.monotonic() < deadline - 120:
                contract.need(server.poll() is None, "private SSH daemon exited before readiness")
                try:
                    _run(["/usr/bin/ssh", "-F", str(ROOT / "ssh_config"),
                          "127.0.0.1", "true"], plan, 3)
                    ready = True
                    break
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                    time.sleep(.1)
            contract.need(ready, "private SSH readiness failed")
            for role in ("source", "destination"):
                contract.need(not (HOME / ("role-" + role + ".json")).exists(),
                              "daemon role evidence predates the copy")
            remaining(deadline, 120)
            save_new(HOME / "copy-intent.json",
                     dict(schema=contract.SCHEMA, id=plan["id"], command=contract.copy_argv()))
            start = time.monotonic()
            with (HOME / "copy.stdout").open("x") as stdout, (HOME / "copy.stderr").open("x") as stderr:
                _run(contract.copy_argv(), plan, 120, deadline=deadline,
                     stdout=stdout, stderr=stderr)
            result["command_seconds"] = time.monotonic() - start
            roles = {role: json.loads((HOME / ("role-" + role + ".json")).read_text())
                     for role in ("source", "destination")}
            contract.need(all(row["identity"] == observed and
                              row["binary_sha256"] == plan["binary_sha256"]["rcpd"]
                              for row in roles.values()), "daemon identity or binary pins differ")
            contract.need(server.poll() is None, "private SSH parent exited during the copy")
            result.update(status="copy-succeeded", roles=roles)
    except BaseException as error:
        # Preserve bounded diagnostics without exception argv/environment dumps.
        failure(result, "copy", error)
    finally:
        if server is not None:
            errors, report = stop_server(server)
            result["private_ssh_cleanup"] = report
            for error in errors:
                failure(result, "private-ssh-cleanup", error)
        try:
            save_new(HOME / "inner-result.json", result)
        except BaseException as error:
            failure(result, "result-persistence", error)
    return 0 if result["status"] == "copy-succeeded" else 1


def main(argv):
    if len(argv) == 2 and argv[0] == "_inner":
        return inner_main(argv[1])
    if len(argv) >= 2 and argv[0] == "_role":
        return role_main(argv[1], argv[2:])
    raise ValueError("private inner/role mode required; use the separate native owner entry point")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
