"""Root-owned one-shot supervisor, baked into the AMI; never execute mailbox scripts."""

import argparse
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import errno
import io
import itertools
import json
import math
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
import time
import zipfile
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from benchmarks.run import load_manifest
from benchmarks.verification import METADATA_POLICY

from .common import (BINARIES, MAX_BINARY, MAX_RESULT, RESERVE, SUITES, Budget, admission_contract, clients,
                     DIRECT_PROTOCOL, DURABLE_MAX_RUNTIME, durable_worker_reserve,
                     archive_name_supported, completed_status, digest, error_code, get_bytes, get_json,
                     measurement_contract, verification_contract, put_json, require, retry_io, retryable_io, timestamp,
                     validate_admission, validate_request, validate_window,
                     WINDOW_KEY, REQUEST_KEY, RESULTS_KEY, STATUS_KEY)
from .cache_client import SOCKET
from .maintenance import check_quiet
from .guest_security import check_ssm_absent
from .common import LARGE_SUITES
from . import large_storage

ROOT = Path("/opt/rcp-benchmark")
STATE = Path("/run/rcp-benchmark")
DATA = Path("/mnt/rcp-benchmark")
UNIT = "rcp-measure"
DURABLE_STATE = Path("/var/lib/rcp-benchmark")
ASSIGNMENT_WAIT_SECONDS = 120


def execute(argv, budget, **kwargs):
    return subprocess.run(argv, check=True, timeout=min(60, budget.left(RESERVE)), **kwargs)


def prepare_storage(budget, uid, gid):
    large_storage.unused_targets(DATA)
    with ExitStack() as stack:
        disks, held = large_storage.open_disks(execute, budget, stack)
        selected = sorted(zip(disks, held), key=lambda value: value[0]["path"])[:2]
        # all four disks pass admission before any format; actions use the held devices
        for side, (_, (fd, number)) in zip(large_storage.SIDES, selected):
            mount = DATA / side
            large_storage.format_mount(fd, number, mount, budget, execute)
            os.chown(mount, uid, gid)
        large_storage.prepare_output(DATA, budget, uid, gid, execute)
        return [disk["path"] for disk, _ in selected]


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
                except TimeoutError:
                    return
                try:
                    connection, _ = server.accept()
                except socket.timeout:
                    continue
                with connection:
                    try:
                        handle_cache(connection, budget, uid)
                    except (OSError, ValueError, TimeoutError):
                        pass
    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    return thread


def command(window, seconds):
    contract = measurement_contract(window["suite"])
    large = window["suite"] in LARGE_SUITES
    file_limit, timeout = "536870912", "600"
    source_storage, destination_storage = "c5d-local-ext4-source", "c5d-local-ext4-destination"
    if large:
        file_limit, timeout = "1073741824", "1200"
        source_storage, destination_storage = "c5d-raid0-ext4-source-v1", "c5d-raid0-ext4-destination-v1"
    properties = {
        "User": "rcp-benchmark", "Group": "rcp-benchmark", "Type": "exec",
        "RuntimeMaxSec": str(max(1, int(seconds))), "TimeoutStopSec": "10",
        "KillMode": "control-group", "SendSIGKILL": "yes", "NoNewPrivileges": "yes",
        "CapabilityBoundingSet": "", "PrivateNetwork": "yes", "PrivateDevices": "yes",
        "ProtectSystem": "strict", "ProtectHome": "yes", "ProtectKernelTunables": "yes",
        "ProtectKernelModules": "yes", "ProtectControlGroups": "yes", "RestrictSUIDSGID": "yes",
        "RestrictNamespaces": "yes", "LockPersonality": "yes", "RestrictAddressFamilies": "AF_UNIX",
        "MemoryMax": "180G", "TasksMax": "8192", "LimitNOFILE": "1048576",
        "LimitCORE": "0", "LimitFSIZE": file_limit, "UMask": "0077",
        "TemporaryFileSystem": "/tmp:size=64m,mode=1777 /var/tmp:size=64m,mode=1777",
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
             "--mode", "local", "--variant", "rcp-default",
             "--paired-seed", str(contract["paired_seed"]), "--repetitions", str(contract["repetitions"]),
             "--no-timings", "--local-resources", "--resource-time", "/usr/bin/time",
             "--purpose", contract["purpose"], "--cache", contract["cache_policy"],
             "--timeout", timeout, "--runner-label", contract["runner_label"],
             "--source-root", str(DATA / "source"), "--destination-root", str(DATA / "destination"),
             "--source-storage-id", source_storage,
             "--destination-storage-id", destination_storage,
             "--output", str(DATA / "output" / "results")]
    if large:
        verification = verification_contract(window["suite"])
        argv += ["--manifest", str(ROOT / "benchmarks/large-cases.json"),
                 "--streaming-verification", "--verification-workers", str(verification["workers"]),
                 "--verification-inline-bytes", str(verification["inline_bytes"]),
                 "--verification-read-bytes", str(verification["read_bytes"]),
                 "--verification-batch-depth", str(verification["batch_depth"]), "--check-capacity"]
        if verification["policy"] == METADATA_POLICY:
            argv += ["--verification-mode", "metadata-size"]
    for case in SUITES[window["suite"]]:
        argv += ["--case", case]
    return argv


def collect(output, metadata, budget):
    """Read held no-follow files after cgroup teardown; preserve bounded diagnostics."""
    archive = io.BytesIO()
    total, count, omitted = 0, 0, 0
    omissions = []
    # Reserve trusted metadata and its ZIP headers; every other entry is charged
    # both local and central headers, including its UTF-8 name, before writing.
    available = MAX_RESULT - 1024 * 1024
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    def omit(name, size, kept):
        nonlocal omitted
        omitted += 1
        if len(omissions) < 128:
            # a filesystem surrogate must not poison the trusted metadata JSON
            display_name = name.encode("utf-8", errors="backslashreplace").decode("utf-8")
            omissions.append(dict(name=display_name[:512], size=size, archived_bytes=kept))
    def priority(entry):
        return (entry.name not in ("results", "results.json", "summary.md"), entry.name)
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as result:
        def walk(fd, prefix):
            nonlocal total, count
            with os.scandir(fd) as entries:
                batch = list(itertools.islice(entries, 1001 - count))
            count += len(batch)
            require(count <= 1000, "result entry limit exceeded")
            for entry in sorted(batch, key=priority):
                name = entry.name
                budget.left(60)
                member = prefix + name
                # this snapshot only filters entries that need no content read.
                # opened files still pass the held-descriptor checks below.
                observed = entry.stat(follow_symlinks=False)
                if (not archive_name_supported(member)
                        or not (stat.S_ISREG(observed.st_mode) or stat.S_ISDIR(observed.st_mode))):
                    omit(member, observed.st_size, 0)
                    continue
                try:
                    child = os.open(name, flags, dir_fd=fd)
                except OSError as error:
                    if error.errno not in (errno.ELOOP, errno.ENXIO):
                        raise
                    omit(member, entry.stat(follow_symlinks=False).st_size, 0)
                    continue
                try:
                    info = os.fstat(child)
                    if stat.S_ISDIR(info.st_mode):
                        # do not enter a directory whose children cannot be archived
                        if archive_name_supported(member + "/_"):
                            walk(child, member + "/")
                        else:
                            omit(member, info.st_size, 0)
                        continue
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                        omit(member, info.st_size, 0)
                        continue
                    overhead = 76 + 2 * len(member.encode("utf-8"))
                    remaining = max(0, available - total - overhead)
                    limit = min(1024 * 1024, remaining) if name.endswith(".log") else remaining
                    if info.st_size > limit and not name.endswith(".log") or limit == 0:
                        omit(member, info.st_size, 0)
                        continue
                    with os.fdopen(os.dup(child), "rb") as stream:
                        if info.st_size > limit:
                            # Logs retain their head and tail; the omission manifest
                            # makes this explicit, including the original size.
                            head = limit // 2
                            data = stream.read(head)
                            stream.seek(info.st_size - (limit - head))
                            data += stream.read(limit - head)
                            omit(member, info.st_size, len(data))
                        else:
                            data = stream.read(limit + 1)
                            require(len(data) <= limit, "result grew beyond byte limit")
                    total += overhead + len(data)
                    result.writestr(member, data)
                finally:
                    os.close(child)
        if output is not None:
            try:
                fd = os.open(output, flags | os.O_DIRECTORY)
            except FileNotFoundError:
                fd = None
            if fd is not None:
                try:
                    # Candidate names cannot duplicate trusted worker.json.
                    walk(fd, "benchmark/")
                finally:
                    os.close(fd)
        record = dict(metadata)
        if omitted:
            record.update(omitted_or_truncated=omissions, omitted_or_truncated_count=omitted)
        encoded = json.dumps(record, indent=2).encode()
        require(len(encoded) < 1024 * 1024 - 256, "worker metadata limit exceeded")
        result.writestr("worker.json", encoded)
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


class DeadlineExpired(BaseException):
    """Escape SDK/socket Exception handlers at the worker's absolute deadline."""


def expired(signum, frame):
    raise DeadlineExpired("absolute worker deadline exhausted")


def error_detail(error):
    # Supervisor-built command names and numeric outcomes are safe to retain.
    # Never copy argv, subprocess output, or transport request/response bodies.
    result = {"type": type(error).__name__[:128]}
    if isinstance(error, ValueError):
        result["message"] = str(error)[:512]
    if isinstance(error, (subprocess.CalledProcessError, subprocess.TimeoutExpired)):
        if isinstance(error.cmd, (list, tuple)) and error.cmd and isinstance(error.cmd[0], str):
            result["command"] = Path(error.cmd[0]).name[:128]
        if isinstance(error, subprocess.CalledProcessError):
            result["returncode"] = error.returncode
        elif isinstance(error.timeout, (int, float)) and math.isfinite(error.timeout):
            result["timeout"] = error.timeout
    code = error_code(error)
    if isinstance(code, str):
        result["code"] = code[:128]
    return result


def publish_report(s3, bucket, output, metadata, status, budget):
    """Attempt bounded archives independently, then always attempt terminal status."""
    data = None
    for index, candidate_output in enumerate((output, None)):
        metadata["status"] = status.copy()
        try:
            data = collect(candidate_output, metadata, budget)
            break
        except Exception as error:
            field = "collection_error" if index == 0 else "fallback_error"
            status.update(success=False, **{field: error_detail(error)})
    if data is not None:
        try:
            retry_io(lambda: s3.put_object(Bucket=bucket, Key=RESULTS_KEY, Body=data,
                     ContentType="application/zip"), budget, reserve=30)
            status["sha256"] = digest(data)
        except Exception as error:
            status.update(success=False, upload_error=error_detail(error))
    # DeadlineExpired escapes both archive handlers. Ordinary archive failures
    # never prevent status publication; exhausted status retries power off.
    retry_io(lambda: put_json(s3, bucket, STATUS_KEY, status), budget)


class NoMetadataRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, target):
        return None


def instance_identity(budget):
    """Read only the instance ID through IMDSv2, without proxy or credential access."""
    opener = build_opener(ProxyHandler({}), NoMetadataRedirect())
    endpoint = "http://169.254.169.254/latest/"
    request = Request(endpoint + "api/token", method="PUT",
                      headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
    with opener.open(request, timeout=min(2, budget.left(5))) as response:
        token = response.read(4097)
    require(0 < len(token) <= 4096 and all(32 < byte < 127 for byte in token), "invalid IMDSv2 token")
    request = Request(endpoint + "meta-data/instance-id",
                      headers={"X-aws-ec2-metadata-token": token.decode("ascii")})
    with opener.open(request, timeout=min(2, budget.left(5))) as response:
        identity = response.read(65).decode("ascii")
    require(re.fullmatch(r"i-[0-9a-f]{17}", identity), "invalid IMDS instance identity")
    return identity


def durable_window(s3, bucket, identity, budget):
    """Wait briefly for this instance's assignment; stale mail cannot admit any work."""
    while True:
        budget.left(5)
        try:
            value = retry_io(lambda: get_json(s3, bucket, WINDOW_KEY), budget, reserve=5)
        except Exception as error:
            # without ListBucket, a missing fixed key is reported as AccessDenied
            # exhausted transient read retries may poll again, but only under the
            # original assignment budget and alarm; no new deadline is created
            if error_code(error) not in ("NoSuchKey", "404", "AccessDenied", "403") and not retryable_io(error):
                raise
        else:
            require(isinstance(value, dict), "invalid durable mailbox window")
            if value.get("protocol") == DIRECT_PROTOCOL and value.get("instance_id") == identity:
                return value
        time.sleep(min(3, budget.left(5)))


def consume_durable_assignment(window):
    """Persist one-shot admission on the root disk, so reboot cannot replay the same worker."""
    DURABLE_STATE.mkdir(mode=0o700, exist_ok=True)
    directory = os.open(DURABLE_STATE, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(directory)
        require(info.st_uid == os.geteuid() and not stat.S_IMODE(info.st_mode) & 0o077,
                "durable state directory is not private to the supervisor")
        descriptor = os.open("durable-started.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=directory)
        with os.fdopen(descriptor, "w") as output:
            keys = ("protocol", "run", "instance_id", "launch_at", "deadline")
            if window["protocol"] == DIRECT_PROTOCOL:
                keys += ("attempt_id", "attempt_started_at")
            json.dump({key: window[key] for key in keys}, output)
            output.flush()
            os.fsync(output.fileno())
        os.fsync(directory)
    finally:
        os.close(directory)


def run(s3, bucket, *, direct=False):
    protocol = DIRECT_PROTOCOL if direct else None
    bounded = direct
    # A failed initial control read has no trustworthy run identity to report.
    # Bound it anyway; an uncaught failure powers off via the supervisor unit.
    started = datetime.now(timezone.utc)
    budget = Budget(started + timedelta(seconds=ASSIGNMENT_WAIT_SECONDS if bounded else 60))
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, budget.left())
    identity = instance_identity(budget) if bounded else None
    window = durable_window(s3, bucket, identity, budget) if bounded else \
        retry_io(lambda: get_json(s3, bucket, WINDOW_KEY), budget, reserve=5)
    require(isinstance(window, dict), "invalid worker window")
    require(window.get("protocol") == protocol, "worker launch protocol mismatch")
    # Trust no retained metadata or extended alarm until the complete window is
    # validated. Late valid windows still reach the admission failure report.
    deadline = validate_window(window, minimum=0)
    budget = Budget(deadline, ceiling_seconds=DURABLE_MAX_RUNTIME) if bounded else Budget(deadline)
    signal.setitimer(signal.ITIMER_REAL, budget.left())
    status = completed_status(window["run"])
    metadata = {"window": window, "worker_started_at": timestamp(started)}
    if bounded:
        metadata.update(guest_instance_id=identity, timing_basis="projected workload with explicit headroom; not a measured complete run",
                        runtime_origin="attempt_started_at")
    stopped = True
    try:
        request = retry_io(lambda: get_json(s3, bucket, REQUEST_KEY), budget, reserve=RESERVE)
        validate_request(request, window)
        validate_admission(window, window=True)
        metadata["request"] = request
        worker_reserve = durable_worker_reserve(window["plan"]) if bounded else \
            admission_contract(window["suite"])["worker"]
        if bounded:
            consume_durable_assignment(window)
        def admit_storage():
            remaining = budget.left()
            metadata["admission"] = {"checked_at": timestamp(datetime.now(timezone.utc)),
                "remaining_seconds": round(remaining, 3), "required_seconds": worker_reserve,
                "accepted": remaining >= worker_reserve}
            if remaining < worker_reserve:
                raise TimeoutError("suite worker admission deadline exhausted")
        admit_storage()
        metadata["guest_security"] = check_ssm_absent(
            timeout=lambda maximum: min(maximum, budget.left(RESERVE)))
        metadata["harness_revision"] = (ROOT / "revision.txt").read_text().strip()
        STATE.mkdir(mode=0o755, exist_ok=False)
        account = pwd.getpwnam("rcp-benchmark")
        metadata["apt_units"] = check_quiet(lambda argv, *, timeout: subprocess.run(
            argv, check=True, text=True, capture_output=True,
            timeout=min(timeout, budget.left(RESERVE))).stdout)
        for key in BINARIES:
            data = retry_io(lambda: get_bytes(s3, bucket, "input/" + key, MAX_BINARY), budget, reserve=RESERVE)
            require(digest(data) == request["sha256"][key], "binary digest mismatch")
            path = STATE / key
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(data)
            path.chmod(0o555)
        if window["suite"] in LARGE_SUITES:
            manifest = load_manifest(ROOT / "benchmarks/large-cases.json")
            cases = {case["id"]: case for case in manifest["cases"]}
            case_id, = SUITES[window["suite"]]
            require(case_id in cases, "suite case is missing from the large manifest")
            case = cases[case_id]
            admit_storage()
            metadata["storage"] = large_storage.prepare(DATA, case, budget, account.pw_uid, account.pw_gid, execute)
        else:
            admit_storage()
            metadata["disks"] = prepare_storage(budget, account.pw_uid, account.pw_gid)
        cache_server(budget, account.pw_uid)
        try:
            stopped = False
            completed = subprocess.run(command(window, budget.left(RESERVE)),
                timeout=budget.left(RESERVE) + 15, check=False)
            status["success"] = completed.returncode == 0
            metadata["service_returncode"] = completed.returncode
        except Exception as error:
            status["error"] = error_detail(error)
        finally:
            # Keep the measurement outcome even when stopping independently fails.
            try:
                stop_unit()
                stopped = True
            except Exception as error:
                status.update(success=False, stop_error=error_detail(error))
        # No disk traversal for cleanup: instance store dies with the instance.
    except Exception as error:
        status.update(success=False, error=error_detail(error))
    # Never read candidate-owned paths until all descendants are gone. Failed
    # teardown still reports the trusted outcome without touching those paths.
    publish_report(s3, bucket, DATA / "output" if stopped else None, metadata, status, budget)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--direct", action="store_true", help="require an immutable attempt-bound direct assignment")
    args = parser.parse_args()
    require(os.geteuid() == 0, "supervisor requires root")
    os.chdir(ROOT)
    run(clients(args.region)["s3"], args.bucket, direct=args.direct)


if __name__ == "__main__":
    main()
