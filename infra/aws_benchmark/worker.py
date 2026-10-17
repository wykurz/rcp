"""Root-owned one-shot supervisor, baked into the AMI; never execute mailbox scripts."""

import argparse
import io
import json
import os
from pathlib import Path
import pwd
import re
import signal
import socket
import stat
import struct
import subprocess
import threading
import zipfile

from .common import (BINARIES, MAX_BINARY, MAX_RESULT, RESERVE, SUITES, Budget, clients,
                     digest, get_bytes, get_json, put_json, require, validate_window)
from .cache_client import SOCKET
from .maintenance import check_quiet

ROOT = Path("/opt/rcp-benchmark")
STATE = Path("/run/rcp-benchmark")
DATA = Path("/mnt/rcp-benchmark")
UNIT = "rcp-measure"


def execute(argv, budget, **kwargs):
    return subprocess.run(argv, check=True, timeout=min(60, budget.left(RESERVE)), **kwargs)


def choose_disks(tree):
    selected = []
    for disk in tree["blockdevices"]:
        if (disk.get("model") or "").strip() != "Amazon EC2 NVMe Instance Storage":
            continue
        require(disk["type"] == "disk" and re.fullmatch(r"/dev/nvme[0-9]+n1", disk["path"]), "unexpected NVMe path")
        require(not disk.get("children") and not any(disk.get("mountpoints") or [])
                and not disk.get("fstype") and not disk.get("ro"), "instance disk is already in use")
        require(850_000_000_000 <= disk["size"] <= 950_000_000_000, "unexpected instance disk size")
        selected.append(disk["path"])
    require(len(selected) == 4, "expected all four unused c5d.24xlarge instance-store disks")
    return sorted(selected)[:2]


def prepare_storage(budget, uid, gid):
    tree = json.loads(execute(["lsblk", "--json", "--bytes", "--paths", "--output",
        "PATH,TYPE,SIZE,MODEL,MOUNTPOINTS,FSTYPE,RO"], budget, capture_output=True, text=True).stdout)
    disks = choose_disks(tree)
    # no untrusted process exists yet; candidate services get PrivateDevices and no
    # mount capability. Refuse filesystems/partitions/mounts rather than wiping them.
    for side, disk in zip(("source", "destination"), disks):
        mount = DATA / side
        mount.mkdir(parents=True, exist_ok=False)
        execute(["mkfs.ext4", "-q", "-E", "lazy_itable_init=0,lazy_journal_init=0", disk], budget)
        execute(["mount", "-o", "nodev,nosuid", disk, str(mount)], budget)
        os.chown(mount, uid, gid)
    output = DATA / "output"
    output.mkdir()
    execute(["mount", "-t", "tmpfs", "-o", "size=128m,nosuid,nodev,noexec", "tmpfs", str(output)], budget)
    os.chown(output, uid, gid)
    return disks


def handle_cache(connection, budget, uid):
    connection.settimeout(2)
    _, peer, _ = struct.unpack("3i", connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
    require(peer == uid and connection.recv(16) == b"drop\n", "invalid cache request")
    os.sync()
    budget.left(RESERVE)
    Path("/proc/sys/vm/drop_caches").write_text("3\n")
    connection.sendall(b"ok\n")


def cache_server(budget, uid):
    server = socket.socket(socket.AF_UNIX)
    server.bind(SOCKET)
    os.chmod(SOCKET, 0o666)
    server.listen(2)
    server.settimeout(1)
    def serve():
        with server:
            while True:
                try:
                    budget.left(RESERVE)
                    connection, _ = server.accept()
                except socket.timeout:
                    continue
                except TimeoutError:
                    return
                with connection:
                    try:
                        handle_cache(connection, budget, uid)
                    except (OSError, ValueError, TimeoutError):
                        pass
    threading.Thread(target=serve, daemon=True).start()


def command(window, seconds):
    properties = {
        "User": "rcp-benchmark", "Group": "rcp-benchmark", "Type": "exec",
        "RuntimeMaxSec": str(max(1, int(seconds))), "TimeoutStopSec": "10",
        "KillMode": "control-group", "SendSIGKILL": "yes", "NoNewPrivileges": "yes",
        "CapabilityBoundingSet": "", "PrivateNetwork": "yes", "PrivateDevices": "yes",
        "ProtectSystem": "strict", "ProtectHome": "yes", "ProtectKernelTunables": "yes",
        "ProtectKernelModules": "yes", "ProtectControlGroups": "yes", "RestrictSUIDSGID": "yes",
        "RestrictNamespaces": "yes", "LockPersonality": "yes", "RestrictAddressFamilies": "AF_UNIX",
        "MemoryMax": "180G", "TasksMax": "8192", "LimitNOFILE": "1048576",
        "LimitCORE": "0", "LimitFSIZE": "536870912", "UMask": "0077",
        "TemporaryFileSystem": "/tmp:size=64m /var/tmp:size=64m",
        "ReadWritePaths": str(DATA), "InaccessiblePaths": "/root /home /var/lib/cloud",
        "WorkingDirectory": str(ROOT), "StandardOutput": "null", "StandardError": "null",
    }
    argv = ["systemd-run", "--unit=" + UNIT, "--wait", "--quiet"]
    argv += ["--property=" + key + "=" + value for key, value in properties.items()]
    argv += ["--setenv=PATH=/opt/rcp-benchmark/bin:/usr/bin:/bin",
             "--setenv=PYTHONPATH=" + str(ROOT), "--setenv=LC_ALL=C", "--setenv=LANG=C",
             "--setenv=RCP_BENCH_BASELINE_COMMIT=" + window["baseline"],
             "--setenv=GITHUB_REPOSITORY=wykurz/rcp", "/usr/bin/python3", "-m", "benchmarks.run",
             "--bin-dir", str(STATE / "candidate"), "--baseline-bin-dir", str(STATE / "baseline"),
             "--mode", "local", "--variant", "rcp-default", "--paired-seed", "0",
             "--repetitions", "2" if window["suite"] == "aa-smoke" else "4",
             "--no-timings", "--local-resources", "--resource-time", "/usr/bin/time",
             "--purpose", "smoke" if window["suite"] == "aa-smoke" else "performance",
             "--cache", "source-warm" if window["suite"] == "aa-smoke" else "linux-drop-caches",
             "--timeout", "600", "--runner-label", "aws-c5d24-ext4-isolated-v1",
             "--source-root", str(DATA / "source"), "--destination-root", str(DATA / "destination"),
             "--source-storage-id", "c5d-local-ext4-source", "--destination-storage-id", "c5d-local-ext4-destination",
             "--output", str(DATA / "output" / "results")]
    for case in SUITES[window["suite"]]:
        argv += ["--case", case]
    return argv


def collect(output, metadata, budget):
    """Read only held, no-follow regular files after the entire candidate cgroup stops."""
    archive = io.BytesIO()
    total, count = 0, 0
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as result:
        result.writestr("worker.json", json.dumps(metadata, indent=2))
        def walk(fd, prefix, depth):
            nonlocal total, count
            # results/logs/<case>/<variant>/<iteration>/ is the harness's normal layout.
            require(depth <= 5, "result directory nesting limit exceeded")
            with os.scandir(fd) as entries:
                walk_entries(entries, fd, prefix, depth)
        def walk_entries(entries, fd, prefix, depth):
            nonlocal total, count
            for entry in entries:
                name = entry.name
                budget.left(60)
                count += 1
                require(count <= 1000 and len(name) <= 200, "result entry limit exceeded")
                require(name not in (".", "..") and "/" not in name and "\\" not in name, "unsafe result name")
                child = os.open(name, flags, dir_fd=fd)
                try:
                    info = os.fstat(child)
                    if stat.S_ISDIR(info.st_mode):
                        walk(child, prefix + name + "/", depth + 1)
                    else:
                        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, "non-regular or linked result")
                        require(info.st_size <= MAX_RESULT - total - 65536, "result byte limit exceeded")
                        with os.fdopen(os.dup(child), "rb") as stream:
                            data = stream.read(MAX_RESULT - total - 65536 + 1)
                        total += len(data)
                        require(total <= MAX_RESULT - 65536, "result byte limit exceeded")
                        result.writestr(prefix + name, data)
                finally:
                    os.close(child)
        fd = os.open(output, flags | os.O_DIRECTORY)
        try:
            # Candidate-controlled names cannot duplicate the supervisor's worker.json.
            walk(fd, "benchmark/", 0)
        finally:
            os.close(fd)
    data = archive.getvalue()
    require(len(data) <= MAX_RESULT, "archive byte limit exceeded")
    return data


def stop_unit():
    # successful transient units can already have been unloaded by systemd
    subprocess.run(["systemctl", "stop", UNIT], timeout=30, check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    state = subprocess.check_output(["systemctl", "show", UNIT, "--property=ActiveState", "--value"],
                                    timeout=10, text=True).strip()
    require(state in ("inactive", "failed"), "candidate service is still active")
    events = Path("/sys/fs/cgroup/system.slice/" + UNIT + ".service/cgroup.events")
    require(not events.exists() or "populated 0" in events.read_text().splitlines(),
            "candidate cgroup still contains processes")


def run(s3, bucket):
    window = get_json(s3, bucket, "control/window.json")
    budget = Budget(validate_window(window))
    def expired(signum, frame):
        raise TimeoutError("absolute worker deadline exhausted")
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, budget.left())
    request = get_json(s3, bucket, "input/request.json")
    require(all(request[k] == window[k] for k in ("run", "baseline", "candidate", "suite", "deadline")),
            "mailbox request does not match the operator window")
    require(set(request["sha256"]) == set(BINARIES), "unexpected binary list")
    status = {"run": window["run"], "success": False}
    metadata = {"window": window, "request": request,
                "harness_revision": (ROOT / "revision.txt").read_text().strip()}
    STATE.mkdir(mode=0o755, exist_ok=False)
    account = pwd.getpwnam("rcp-benchmark")
    stopped = True
    try:
        metadata["apt_units"] = check_quiet(lambda argv, *, timeout: subprocess.run(
            argv, check=True, text=True, capture_output=True,
            timeout=min(timeout, budget.left(RESERVE))).stdout)
        for key in BINARIES:
            budget.left(RESERVE)
            data = get_bytes(s3, bucket, "input/" + key, MAX_BINARY)
            require(digest(data) == request["sha256"][key], "binary digest mismatch")
            path = STATE / key
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(data)
            path.chmod(0o555)
        metadata["disks"] = prepare_storage(budget, account.pw_uid, account.pw_gid)
        cache_server(budget, account.pw_uid)
        try:
            stopped = False
            completed = subprocess.run(command(window, budget.left(RESERVE)),
                timeout=budget.left(RESERVE) + 15, check=False)
            status["success"] = completed.returncode == 0
            metadata["service_returncode"] = completed.returncode
        finally:
            # stop all descendants even if the systemd-run client was interrupted
            stop_unit()
            stopped = True
        # no disk cleanup traversal here: ephemeral disks die with the instance.
        # harness-owned cleanup and verification ran inside the same cgroup deadline.
    except Exception as error:
        status["success"] = False
        status["error"] = type(error).__name__
    try:
        require(stopped, "refusing to collect while candidate descendants may be alive")
        metadata["status"] = status.copy()
        data = collect(DATA / "output", metadata, budget)
        budget.left(30)
        s3.put_object(Bucket=bucket, Key="output/results.zip", Body=data, ContentType="application/zip")
        status["sha256"] = digest(data)
    except Exception as error:
        status.update(success=False, upload_error=type(error).__name__)
    budget.left()
    put_json(s3, bucket, "output/status.json", status)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()
    require(os.geteuid() == 0, "supervisor requires root")
    os.chdir(ROOT)
    run(clients(args.region)["s3"], args.bucket)


if __name__ == "__main__":
    main()
