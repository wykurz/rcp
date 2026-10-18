"""Remove inherited SSM from prepared images and verify its absence without AWS calls."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import time

UNITS = ("amazon-ssm-agent.service", "snap.amazon-ssm-agent.amazon-ssm-agent.service")
AGENT_PATHS = (
    "snap/amazon-ssm-agent", "var/snap/amazon-ssm-agent", "usr/bin/amazon-ssm-agent",
    "usr/bin/ssm-agent-worker", "usr/bin/ssm-document-worker", "usr/bin/ssm-session-worker",
    "usr/lib/amazon/ssm", "opt/aws/ssm",
)
STATE_PATHS = ("var/lib/amazon/ssm", "etc/amazon/ssm", "var/log/amazon/ssm", "root/snap/amazon-ssm-agent")


def invoke(argv, *, timeout=None, runner=subprocess.run):
    limit = timeout(30) if timeout else 30
    if limit <= 0:
        raise TimeoutError("SSM verification budget exhausted")
    return runner(argv, check=True, capture_output=True, text=True, timeout=limit,
                  env={**os.environ, "LC_ALL": "C"}).stdout


def units(command):
    output = command(["systemctl", "show", "--property=Id,LoadState,UnitFileState,ActiveState,MainPID", *UNITS])
    result = {}
    for block in output.strip().split("\n\n"):
        fields = dict(line.split("=", 1) for line in block.splitlines())
        name = fields.get("Id")
        if name not in UNITS or name in result or set(fields) != {
                "Id", "LoadState", "UnitFileState", "ActiveState", "MainPID"}:
            raise ValueError("unexpected SSM service status")
        result[name] = fields
    if set(result) != set(UNITS):
        raise ValueError("incomplete SSM service status")
    return result


def deb_installed(command):
    # query all package states: a missing single-package query otherwise resembles a command failure
    output = command(["dpkg-query", "-W", "-f=${binary:Package}\t${db:Status-Status}\n"])
    for line in output.splitlines():
        name, status = line.split("\t", 1)
        if name.split(":", 1)[0] == "amazon-ssm-agent" and status != "not-installed":
            return True
    return False


def snap_present(root):
    return (root / "snap/amazon-ssm-agent").exists() or any(
        (root / "var/lib/snapd/snaps").glob("amazon-ssm-agent_*.snap"))


def check_ssm_absent(*, timeout=None, runner=subprocess.run, root=Path("/")):
    def command(argv):
        return invoke(argv, timeout=timeout, runner=runner)
    for name, state in units(command).items():
        if (state["LoadState"] != "masked" or state["UnitFileState"] != "masked"
                or state["ActiveState"] != "inactive" or state["MainPID"] != "0"):
            raise ValueError("SSM service must be persistently masked and inactive: " + name)
    if deb_installed(command) or snap_present(root):
        raise ValueError("SSM Agent package remains installed")
    for relative in AGENT_PATHS + STATE_PATHS:
        path = root / relative
        if path.exists() or path.is_symlink():
            raise ValueError("inherited SSM Agent path remains: /" + relative)
    return {"ssm_agent": "absent", "services": "masked/inactive"}


def remove_ssm(*, runner=subprocess.run, root=Path("/"), clock=time.monotonic):
    end = clock() + 300
    def timeout(maximum):
        remaining = end - clock()
        if remaining <= 0:
            raise TimeoutError("SSM Agent removal exceeded five minutes")
        return min(maximum, remaining)
    def command(argv):
        return invoke(argv, timeout=timeout, runner=runner)
    present = [name for name, state in units(command).items() if state["LoadState"] != "not-found"]
    if present:
        command(["systemctl", "stop", *present])
    if snap_present(root):
        # --purge avoids an automatic saved snapshot of the agent's registration data
        command(["snap", "remove", "--purge", "amazon-ssm-agent"])
    if deb_installed(command):
        command(["dpkg", "--purge", "amazon-ssm-agent"])
    # snap-generated unit files live under /etc: mask only after package removal frees those names
    command(["systemctl", "mask", *UNITS])
    for relative in STATE_PATHS:
        path = root / relative
        if path.is_symlink():
            path.unlink()
        elif path.exists():
            shutil.rmtree(path)
    return check_ssm_absent(timeout=timeout, runner=runner, root=root)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remove-ssm", action="store_true", help="image preparation only: remove inherited SSM")
    args = parser.parse_args()
    if args.remove_ssm:
        if os.geteuid() != 0:
            raise ValueError("image preparation requires root")
        remove_ssm()
    else:
        check_ssm_absent()


if __name__ == "__main__":
    main()
