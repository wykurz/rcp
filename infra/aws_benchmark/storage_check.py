"""Image-build check: the image can serve benchmark cells on NFS mounts and on roots in memory.

Run as root by install-worker.sh once the harness is installed, never on a worker. It prepares
both kinds of storage as a worker's supervisor does (other_storage.py), on two directories that
stand in for the arrays; writes, links, reads and removes a file through every mount as the
benchmark account under the sandbox of the measurement unit; and takes everything down again.
An image that cannot do this fails its build here, instead of at the first paid run: a kernel
without the delay or the server, a packaged unit that still starts by itself, a mount the
sandbox cannot reach.
"""

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import pwd
import shutil
import subprocess

from . import other_storage, worker
from .common import RESERVE, Budget
from .large_storage import SIDES

UNIT = "rcp-storage-check"
SECONDS = 180
# on the image's own filesystem, which is exported as an array is on a worker
DATA = Path("/var/lib/rcp-storage-check")
STATE = Path("/run/rcp-storage-check")
# run in the unit with one MOUNT:DIRECT argument for each root: a file written through the mount
# is linked and read there, and is found under DIRECT when the tree is kept somewhere else
PROBE = """
import os, sys
for pair in sys.argv[1:]:
    mount, direct = pair.split(":")
    path = os.path.join(mount, "probe")
    with open(path, "x") as file:
        file.write("written through the mount")
    os.link(path, path + ".link")
    with open(path + ".link") as file:
        assert file.read() == "written through the mount", pair
    assert sorted(os.listdir(mount)) == ["probe", "probe.link"], (pair, os.listdir(mount))
    assert not direct or sorted(os.listdir(direct)) == ["probe", "probe.link"], (pair, os.listdir(direct))
    os.unlink(path + ".link")
    os.unlink(path)
    assert os.listdir(mount) == [], (pair, os.listdir(mount))
"""


def command(data):
    """The transient unit: the sandbox of a suite without remote cells around the probe."""
    properties = worker.sandbox(SECONDS, "1073741824")
    # the check's own directories stand in for the data mounts of a worker, and the build log,
    # not a discarded stream, must say why a check failed
    properties.update(ReadWritePaths=str(data), StandardOutput="journal+console", StandardError="journal+console")
    roots = [f"{data / other_storage.NFS_MOUNTS[side]}:{data / side}" for side in SIDES] + \
            [f"{data / other_storage.MEMORY_MOUNTS[side]}:" for side in SIDES]
    return (["/usr/bin/systemd-run", "--unit=" + UNIT, "--wait", "--quiet", "--collect"] +
            ["--property=" + key + "=" + value for key, value in properties.items()] + [*worker.ENVIRONMENT] +
            ["/usr/bin/python3", "-B", "-c", PROBE, *roots])


def teardown(data, state, budget, execute, *, kernel=other_storage.Kernel()):
    """Undo both preparations; return what could not be undone.

    Every step runs whatever became of the one before it: after a preparation that failed half
    way, what it did reach is still taken down.
    """
    steps = [["umount", str(data / mounts[side])] for mounts in other_storage.MOUNTS.values() for side in SIDES]
    steps += [["rpc.nfsd", "0"], ["exportfs", "-ua"],
              *(["systemctl", "stop", unit + ".service"] for unit in (other_storage.MOUNTD, other_storage.TRACKER)),
              ["tc", "qdisc", "del", "dev", "lo", "root"],
              ["umount", str(kernel.pipes)], ["umount", str(kernel.server)],
              ["sysctl", "--quiet", "--write", "fs.leases-enable=1"]]
    left = []
    for argv in steps:
        try:
            execute(argv, budget)
        except (subprocess.SubprocessError, OSError):
            left.append(" ".join(argv[:2]))
    for directory in (data, state):
        try:
            shutil.rmtree(directory)
        except OSError:
            left.append(str(directory))
    return left


def main():
    if os.geteuid() != 0:
        raise SystemExit("the storage image check requires root")
    account = pwd.getpwnam("rcp-benchmark")
    budget = Budget(datetime.now(timezone.utc) + timedelta(seconds=2 * SECONDS + 2 * RESERVE))
    problem = nfs = None
    try:
        for side in SIDES:
            (DATA / side).mkdir(parents=True)
            os.chown(DATA / side, account.pw_uid, account.pw_gid)
        STATE.mkdir()
        nfs = other_storage.prepare_nfs(DATA, STATE, budget, worker.execute)
        other_storage.prepare_memory(DATA, budget, account.pw_uid, account.pw_gid, worker.execute)
        code = subprocess.run(command(DATA), check=False, timeout=SECONDS + 60).returncode
        if code != 0:
            problem = f"the probe under the measurement sandbox exited with status {code}"
    except subprocess.TimeoutExpired as error:
        problem = f"{Path(str(error.cmd[0])).name} timed out"
    except (subprocess.CalledProcessError, OSError, ValueError) as error:
        # the build log is the operator's own: say all of it
        problem = f"{type(error).__name__}: {error}"
    left = teardown(DATA, STATE, budget, worker.execute)
    if problem is not None:
        raise SystemExit("this image cannot serve benchmark cells on other storage: " + problem)
    if left:
        raise SystemExit("the storage image check could not take down what it prepared: " + ", ".join(left))
    print(f"benchmark cells on other storage: NFS {nfs['version']} mounts with a round trip of "
          f"{nfs['round_trip_seconds'] * 1000:.2f} ms and roots in memory work under the measurement sandbox")


if __name__ == "__main__":
    main()
