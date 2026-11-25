"""Storage beside the two arrays, for the matrix cells that measure there.

`nfs`: the two arrays, served by the kernel NFS server on the loopback and mounted back, with a
fixed delay on the loopback, so that every operation costs a request to a server. `memory`: two
roots in memory. Both are prepared by the trusted supervisor before any candidate process exists,
as the arrays are (large_storage.py); the measurement unit only finds the mounts.

The image has the NFS packages installed and their services masked: the supervisor starts from
a machine where no NFS program runs, mounts what the server needs itself, and refuses a machine
where a server runs already.
"""

from dataclasses import dataclass
import os
from pathlib import Path
import socket
import statistics
import time

from .common import require
from .large_storage import SIDES

NFS_MOUNTS = {"source": "nfs-source", "destination": "nfs-destination"}
MEMORY_MOUNTS = {"source": "memory-source", "destination": "memory-destination"}
NFS_VERSION = "4.2"
# an NFS 4 server answers no more requests at once than it has threads, and the tools keep one
# request per file in flight: as many as the machine has CPUs, 96 on the worker
NFS_THREADS = 256
# added to every packet on the loopback: a request and its reply cost twice this
NFS_DELAY_MICROSECONDS = 2500
# what a round trip may measure before the mounts are trusted: the two delays, and little more
ROUND_TRIP_SECONDS = (0.0050, 0.0080)
NFS_PORT = 2049
# each root in memory, with limits that keep a runaway copy from taking the machine's memory. The
# bytes: ten million files of 1 KB take a page each, 39 GiB. The inodes: a hard link takes one of
# the limit as a file does, and the destination holds a copy of the ten million files and the
# tree that rlink makes of it
MEMORY_BYTES = 64 * 1024 ** 3
MEMORY_INODES = 24_000_000
# the two daemons a server for NFS 4 alone needs, as transient units. The client tracker has to
# run before the server starts, or the first create waits out the server's grace period; mountd
# answers the kernel's questions about exports and opens no socket of its own with the two old
# protocol versions off
TRACKER, MOUNTD = "rcp-nfsdcld", "rcp-nfs-mountd"


@dataclass(frozen=True)
class Kernel:
    """Where the kernel shows and takes the state of its NFS client and server."""

    # from Linux 6.12 a client that finds its server on the same host reads and writes files
    # directly, past the network ("LOCALIO"): the delay would then reach every request but those
    # for file data
    localio: Path = Path("/sys/module/nfs/parameters/localio_enabled")
    clients: Path = Path("/sys/fs/nfs")
    server: Path = Path("/proc/fs/nfsd")
    # where Ubuntu's nfs.conf tells the daemons to find the kernel's pipes
    pipes: Path = Path("/run/rpc_pipefs")


def _mountinfo():
    return Path("/proc/self/mountinfo").read_text()


def _mounted(inventory):
    """Each mount point of a mount inventory with its filesystem type and source."""
    found = {}
    for line in inventory.splitlines():
        before, separator, after = line.partition(" - ")
        left, right = before.split(), after.split()
        if separator and len(left) >= 5 and len(right) >= 2:
            found[left[4]] = (right[0], right[1])
    return found


def _round_trip(samples=9):
    """Seconds one round trip to the server takes: a connection is open after one."""
    times = []
    for _ in range(samples):
        started = time.monotonic()
        with socket.create_connection(("127.0.0.1", NFS_PORT), timeout=5):
            times.append(time.monotonic() - started)
    return statistics.median(times)


def prepare_nfs(data, state, budget, execute, *, kernel=Kernel(), inventory=_mountinfo, round_trip=_round_trip,
                pause=time.sleep):
    """Serve the two arrays over NFS on the loopback, delay the loopback and mount them back.

    `state` is the supervisor's own directory in memory. Returns what was set up, for the report.
    Nothing is trusted before it is checked: the daemons run, the server waits for no client of an
    earlier life, the mounts are the loopback exports, none takes the same-host shortcut, and a
    round trip costs what the delay says.
    """
    exports = {side: data / side for side in SIDES}
    mounts = {side: data / NFS_MOUNTS[side] for side in SIDES}
    # the tracker's record of clients: one from an earlier boot would have the server wait for them
    tracking = state / "nfsdcld"
    require(all(not os.path.lexists(path) for path in (*mounts.values(), tracking)),
            "NFS mount point or client record already exists")
    execute(["modprobe", "--all", "sch_netem", "nfsd", "nfs", "nfsv4"], budget)
    # before any mount: a mount keeps the choice it was made with
    if kernel.localio.exists():
        kernel.localio.write_text("N\n")
    # no delegations: a client then opens and closes every file at the server, whatever the
    # kernel's version thinks of handing out delegations, and a cache drop returns none one by one
    execute(["sysctl", "--quiet", "--write", "fs.leases-enable=0"], budget)
    present = _mounted(inventory())
    kernel.pipes.mkdir(exist_ok=True)
    for kind, source, target in (("nfsd", "nfsd", kernel.server), ("rpc_pipefs", "sunrpc", kernel.pipes)):
        if str(target) not in present:
            execute(["mount", "-t", kind, source, str(target)], budget)
    # a server someone else started would keep its listeners, its protocol versions and its exports
    require((kernel.server / "threads").read_text().strip() == "0", "an NFS server is already running")
    tracking.mkdir(mode=0o700)
    daemons = {TRACKER: ("/usr/sbin/nfsdcld", "--foreground", "--storagedir", str(tracking)),
               MOUNTD: ("/usr/sbin/rpc.mountd", "--foreground", "--no-nfs-version", "2", "--no-nfs-version", "3")}

    def running():
        for unit in daemons:
            execute(["systemctl", "is-active", "--quiet", unit + ".service"], budget)

    for unit, program in daemons.items():
        execute(["systemd-run", "--quiet", "--collect", "--unit=" + unit, *program], budget)
    # the tracker opens its pipe a moment after it starts, and a daemon that cannot start is gone by then
    pause(1)
    running()
    for side in SIDES:
        # async: the server answers without waiting for its disks, so the arrays add no latency of
        # their own to the delay
        execute(["exportfs", "-o", "rw,async,no_subtree_check", f"127.0.0.1:{exports[side]}"], budget)
    execute(["rpc.nfsd", "--host", "127.0.0.1", "--no-nfs-version", "3", str(NFS_THREADS)], budget)
    # on the supervisor's loopback only: the measurement unit has a network of its own
    execute(["tc", "qdisc", "add", "dev", "lo", "root", "netem", "delay", f"{NFS_DELAY_MICROSECONDS}us",
             "limit", "100000"], budget)
    for side in SIDES:
        mounts[side].mkdir()
        execute(["mount.nfs", f"127.0.0.1:{exports[side]}", str(mounts[side]), "-o", "vers=" + NFS_VERSION], budget)
    running()
    # a server that found no tracker, or a client to wait for, answers no create for 90 seconds
    require((kernel.server / "v4_end_grace").read_text().strip() == "Y", "the NFS server is in its grace period")
    for entry in sorted(kernel.clients.glob("*/localio")):
        require(entry.read_text().strip() == "0", "an NFS mount still takes the same-host shortcut")
    found = _mounted(inventory())
    for side in SIDES:
        require(found.get(str(mounts[side])) == ("nfs4", f"127.0.0.1:{exports[side]}"),
                "NFS mount is not the loopback export")
    seconds = round_trip()
    require(ROUND_TRIP_SECONDS[0] <= seconds <= ROUND_TRIP_SECONDS[1],
            f"an NFS round trip takes {seconds * 1000:.2f} ms, not the intended one")
    return dict(layout="nfs-loopback-v1", version=NFS_VERSION, threads=NFS_THREADS,
                delay_microseconds=NFS_DELAY_MICROSECONDS, round_trip_seconds=seconds,
                exports={side: str(path) for side, path in exports.items()},
                mounts={side: str(path) for side, path in mounts.items()})


def prepare_memory(data, budget, uid, gid, execute):
    """Mount the two roots in memory and hand them to the benchmark account."""
    mounts = {side: data / MEMORY_MOUNTS[side] for side in SIDES}
    require(all(not os.path.lexists(path) for path in mounts.values()), "memory root already exists")
    options = f"size={MEMORY_BYTES // 1024 ** 3}g,nr_inodes={MEMORY_INODES},nosuid,nodev,mode=0755"
    for side in SIDES:
        mounts[side].mkdir()
        execute(["mount", "-t", "tmpfs", "-o", options, "tmpfs", str(mounts[side])], budget)
        os.chown(mounts[side], uid, gid)
    return dict(layout="tmpfs-pair-v1", size_bytes=MEMORY_BYTES, inodes=MEMORY_INODES,
                mounts={side: str(path) for side, path in mounts.items()})
