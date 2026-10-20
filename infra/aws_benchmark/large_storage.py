"""Two disposable RAID0 filesystems, prepared before any candidate process exists."""

from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import stat

from benchmarks import capacity
from .common import require

MODEL = "Amazon EC2 NVMe Instance Storage"
EBS_MODEL = "Amazon Elastic Block Store"
SIDES = ("source", "destination")
INODES = 16_000_000
CHUNK_BYTES = 512 * 1024


def select(tree):
    selected = []
    devices = tree.get("blockdevices")
    require(isinstance(devices, list), "missing block-device inventory")
    for disk in devices:
        model = (disk.get("model") or "").strip()
        path = disk.get("path") or ""
        if path.startswith("/dev/nvme"):
            require(model in (MODEL, EBS_MODEL), "unexpected NVMe model")
        if model != MODEL:
            continue
        require(disk.get("type") == "disk" and re.fullmatch(r"/dev/nvme[0-9]+n1", path),
                "unexpected instance-store whole disk")
        require(not disk.get("children") and disk.get("mountpoints") in ([], [None])
                and disk.get("fstype") is None and disk.get("ro") is False,
                "instance-store disk is mounted, occupied or read-only")
        require(type(disk.get("size")) is int and 850_000_000_000 <= disk["size"] <= 950_000_000_000,
                "unexpected instance-store size")
        require(isinstance(disk.get("serial"), str) and 0 < len(disk["serial"].strip()) <= 64
                and isinstance(disk.get("maj:min"), str)
                and re.fullmatch(r"[0-9]+:[0-9]+", disk["maj:min"]), "missing disk identity")
        selected.append(disk)
    require(len(selected) == 4, "expected exactly four unused c5d.24xlarge instance-store disks")
    for field in ("path", "maj:min", "serial"):
        require(len({disk[field].strip() for disk in selected}) == 4, "duplicate disk identity")
    return sorted(selected, key=lambda disk: disk["serial"])


def device_number(info):
    return f"{os.major(info.st_rdev)}:{os.minor(info.st_rdev)}"


def open_block(path, stack):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    stack.callback(os.close, fd)
    info = os.fstat(fd)
    require(stat.S_ISBLK(info.st_mode), "expected held block device")
    return fd, device_number(info)


def unmounted(number):
    # mountinfo includes root, bind mounts and mounts not shown by the lsblk snapshot
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        require(len(fields) >= 10, "invalid mount inventory")
        require(fields[2] != number, "block device is mounted")


def blank(fd, execute, budget):
    result = json.loads(execute(["wipefs", "--no-act", "--json", "--output", "TYPE,OFFSET",
        f"/proc/self/fd/{fd}"], budget, pass_fds=(fd,), capture_output=True, text=True).stdout)
    require(isinstance(result, dict) and result.get("signatures") == [], "device has a storage signature")


def check_disk(disk, fd, number, execute, budget):
    require(number == disk["maj:min"], "opened disk identity changed")
    sysfs = Path("/sys/dev/block") / number
    require(not (sysfs / "partition").exists() and not list((sysfs / "holders").iterdir()),
            "instance-store disk is a partition or has holders")
    require((sysfs / "device/model").read_text().strip() == MODEL
            and (sysfs / "device/serial").read_text().strip() == disk["serial"].strip(),
            "held disk model/serial changed")
    require(int((sysfs / "size").read_text()) * 512 == disk["size"]
            and (sysfs / "ro").read_text().strip() == "0", "held disk capacity/read-only state changed")
    unmounted(number)
    blank(fd, execute, budget)


def check_array(number, members):
    sysfs = Path("/sys/dev/block") / number
    require((sysfs / "md/level").read_text().strip() == "raid0"
            and (sysfs / "md/raid_disks").read_text().strip() == "2"
            and (sysfs / "md/metadata_version").read_text().strip() == "1.2"
            and int((sysfs / "md/chunk_size").read_text()) == CHUNK_BYTES,
            "unexpected RAID geometry")
    slaves = list((sysfs / "slaves").iterdir())
    actual = [(slave / "dev").read_text().strip() for slave in slaves]
    require(len(actual) == 2 and set(actual) == set(members), "RAID members differ from admitted disks")
    require(not list((sysfs / "holders").iterdir()) and (sysfs / "ro").read_text().strip() == "0",
            "RAID device has holders or is read-only")
    require(int((sysfs / "size").read_text()) * 512 >= 1_600_000_000_000, "RAID device too small")
    unmounted(number)


def prepare(data, case, budget, uid, gid, execute):
    """Fail before formatting unless all disks and both newly created arrays pass."""
    arrays = {side: Path("/dev/md") / ("rcp-" + side) for side in SIDES}
    require(all(not os.path.lexists(path) for path in arrays.values()), "target RAID array already exists")
    require(all(not os.path.lexists(data / side) for side in (*SIDES, "output")),
            "benchmark mount/output path already exists")
    swaps = Path("/proc/swaps").read_text().splitlines()
    require(len(swaps) == 1 and swaps[0].startswith("Filename"), "active or unknown swap inventory")
    tree = json.loads(execute(["lsblk", "--json", "--bytes", "--paths", "--output",
        "PATH,TYPE,SIZE,MODEL,MOUNTPOINTS,FSTYPE,RO,MAJ:MIN,SERIAL"],
        budget, capture_output=True, text=True).stdout)
    disks = select(tree)
    with ExitStack() as stack:
        held = []
        for disk in disks:
            fd, number = open_block(disk["path"], stack)
            check_disk(disk, fd, number, execute, budget)
            held.append((fd, number))
        require(len({number for _, number in held}) == 4, "overlapping opened disks")
        # no candidate exists and only this trusted supervisor may change devices
        # pass held descriptors to the tools rather than resolving disk names again
        opened = {}
        for index, side in enumerate(SIDES):
            pair = held[index * 2:index * 2 + 2]
            fds = tuple(fd for fd, _ in pair)
            execute(["mdadm", "--create", str(arrays[side]), "--run", "--metadata=1.2",
                "--level=0", "--raid-devices=2", "--chunk=512", "--homehost=any",
                *[f"/proc/self/fd/{fd}" for fd in fds]], budget, pass_fds=fds)
            # /dev/md/name is normally a udev symlink; pin its resolved block node
            fd, number = open_block(arrays[side].resolve(strict=True), stack)
            check_array(number, [number for _, number in pair])
            blank(fd, execute, budget)
            opened[side] = (fd, number, [disk["path"] for disk in disks[index * 2:index * 2 + 2]])
        require(len({number for _, number, _ in opened.values()}) == 2
                and not ({number for _, number, _ in opened.values()} & {number for _, number in held}),
                "overlapping RAID/source/destination identity")
        for side, (fd, number, _) in opened.items():
            execute(["mkfs.ext4", "-q", "-b", "4096", "-N", str(INODES),
                "-E", "lazy_itable_init=0,lazy_journal_init=0", f"/proc/self/fd/{fd}"],
                budget, pass_fds=(fd,))
            filesystem = execute(["blkid", "-p", "-o", "value", "-s", "TYPE", f"/proc/self/fd/{fd}"],
                budget, pass_fds=(fd,), capture_output=True, text=True).stdout.strip()
            require(filesystem == "ext4", "unexpected formatted filesystem")
            mount = data / side
            mount.mkdir(parents=True, exist_ok=False)
            execute(["mount", "--no-canonicalize", "--internal-only", "-t", "ext4",
                    "-o", "nodev,nosuid", f"/proc/self/fd/{fd}", str(mount)],
                    budget, pass_fds=(fd,))
            require(mount.stat().st_dev == os.makedev(*map(int, number.split(":"))),
                    "mounted filesystem differs from held array")
        proof = capacity.check_pair(data / "source", data / "destination", case)
        for side in SIDES:
            os.chown(data / side, uid, gid)
        output = data / "output"
        output.mkdir()
        execute(["mount", "-t", "tmpfs", "-o", "size=128m,nosuid,nodev,noexec", "tmpfs", str(output)], budget)
        os.chown(output, uid, gid)
        return dict(layout="two-disjoint-raid0-ext4-v1", capacity=proof,
            arrays={side: dict(device=number, members=members, chunk_bytes=CHUNK_BYTES)
                    for side, (_, number, members) in opened.items()})
