"""Measure the cost of a process-scoped kernel profile during fresh local rcp copies."""

import argparse
from contextlib import contextmanager, ExitStack, suppress
import ctypes
import fcntl
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import resource
import select
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time

from benchmarks import cache, streaming
from benchmarks.signals import SignalCancellation
from benchmarks.strict_json import parse_json
from benchmarks.verification import METADATA_POLICY


EVENT = "cpu-clock:k"
LOG_LIMIT = 64 * 1024
REPORT_LIMIT = 4 * 1024 * 1024
REPORT_STACK_DEPTH = 5
REPORT_STACKS_PER_SYMBOL = 4
PERF_GRACE_SECONDS = 30
COPY_OPTIONS = ["--summary", "--output-format=json"]
CACHE_SOCKET = "/run/rcp-benchmark/cache.sock"


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    for option in ("candidate", "perf", "source", "destination-root", "output"):
        parser.add_argument("--" + option, type=Path, required=True)
    parser.add_argument("--baseline", type=Path, help="second prepared rcp build")
    parser.add_argument("--local-copy-handoff", choices=("fresh", "never"),
                        help="use one handoff policy in every arm (otherwise use each build's default)")
    parser.add_argument("--pairs", type=int, default=2, help="even number of off/on pairs per build (2–16)")
    parser.add_argument("--delay-seconds", type=float, default=60, help="time from launching rcp to attaching perf")
    parser.add_argument("--duration-seconds", type=float, default=30, help="perf sampling window in seconds")
    parser.add_argument("--frequency", type=int, default=49, help="requested samples per second per active CPU")
    parser.add_argument("--timeout-seconds", type=float, default=3600, help="time limit for one copy or cache preparation")
    parser.add_argument("--max-data-mib", type=int, default=128, help="hard file-size limit per recording (1–1024 MiB)")
    parser.add_argument("--cache", choices=("linux-drop-caches", "uncontrolled"), default="linux-drop-caches",
                        help="request sync/drop from the existing benchmark broker before every copy, or leave caches alone")
    args = parser.parse_args(argv)
    if not 2 <= args.pairs <= 16 or args.pairs % 2:
        parser.error("--pairs must be even and between 2 and 16")
    if not 1 <= args.frequency <= 10000 or not 1 <= args.max_data_mib <= 1024:
        parser.error("frequency must be 1–10000 Hz and max-data-mib must be 1–1024")
    if not (0 < args.timeout_seconds <= 86400 and 0 <= args.delay_seconds < args.timeout_seconds
            and .001 <= args.duration_seconds <= args.timeout_seconds - args.delay_seconds):
        parser.error("the delay and sampling window must fit within the copy timeout (at most 86400 seconds)")
    args.copy_options = COPY_OPTIONS.copy()
    if args.local_copy_handoff:
        args.copy_options.append("--local-copy-handoff=" + args.local_copy_handoff)
    return args


def preflight(args):
    if sys.platform != "linux" or not hasattr(os, "pidfd_open"):
        raise ValueError("kernel profiling requires Linux with pidfd support")
    if 0 in os.getresuid():
        raise ValueError("run as the ordinary benchmark user; the driver refuses root credentials")
    status = Path("/proc/self/status").read_text().splitlines()
    if any(int(line.split()[1], 16) for line in status
           if line.startswith(("CapEff:", "CapPrm:", "CapInh:", "CapAmb:"))):
        raise ValueError("the driver and copy require an ordinary account without process capabilities")
    args.source = args.source.resolve(strict=True)
    args.destination_root = args.destination_root.resolve(strict=True)
    if not args.source.is_dir() or not args.destination_root.is_dir():
        raise ValueError("source and destination-root must be existing directories")
    output = args.output.resolve()
    for left, right in itertools.combinations((args.source, args.destination_root, output), 2):
        if left.is_relative_to(right) or right.is_relative_to(left):
            raise ValueError(f"source, destination-root and output must not overlap: {left} and {right}")
    if os.path.lexists(args.output):
        raise ValueError("output must name a new private directory")
    args.output = output


class Executable:
    """Keep a descriptor to the measured executable from hashing through the final invocation."""

    def __init__(self, path, stack):
        self.path = path.resolve(strict=True)
        self.file = stack.enter_context(self.path.open("rb"))
        mode = os.fstat(self.file.fileno()).st_mode
        if not stat.S_ISREG(mode) or mode & (stat.S_ISUID | stat.S_ISGID):
            raise ValueError(f"expected a regular executable without set-ID bits: {path}")
        self.sha256 = hashlib.file_digest(self.file, "sha256").hexdigest()

    def popen(self, argv, *, pass_fds=(), **options):
        fd = self.file.fileno()
        return subprocess.Popen([self.path, *argv], executable=f"/proc/self/fd/{fd}", pass_fds=(fd, *pass_fds),
                                stdin=subprocess.DEVNULL, start_new_session=True, **options)


@contextmanager
def bounded_output(limit):
    # a kernel-enforced bound on the log alone; large destination files remain writable by rcp
    fd = os.memfd_create("rcp-profile-output", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    with os.fdopen(fd, "w+b", buffering=0) as stream:
        stream.truncate(limit)
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS, fcntl.F_SEAL_GROW)
        yield stream


def contents(stream):
    size = min(stream.tell(), os.fstat(stream.fileno()).st_size)
    return os.pread(stream.fileno(), size, 0).decode("utf-8", errors="replace")


@contextmanager
def child(executable, argv, **options):
    # leave the group leader unreaped while perf can still be attaching by numeric PID
    process = executable.popen(argv, **options)
    try:
        fd = os.pidfd_open(process.pid)
        try:
            yield process, fd
        finally:
            os.close(fd)
    finally:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def wait_for(fds, deadline, cancellation):
    """Observe exit without reaping. A process keeps its PID until its owner leaves child()."""
    while True:
        cancellation.checkpoint()
        remaining = deadline - time.monotonic()
        ready, _, _ = select.select(fds, [], [], min(.25, max(0, remaining)))
        if ready or remaining <= 0:
            return ready


def child_environment(*, profiler=False):
    env = {name: os.environ[name] for name in ("PATH", "HOME") if name in os.environ}
    env["LC_ALL"] = "C"
    if profiler:
        env.update(PERF_CONFIG="/dev/null", DEBUGINFOD_URLS="")
    return env


def perf_query(perf, argv, cancellation, limit, *, pass_fds=()):
    with bounded_output(limit) as stdout, bounded_output(LOG_LIMIT) as stderr:
        with child(perf, argv, stdout=stdout, stderr=stderr,
                   env=child_environment(profiler=True), pass_fds=pass_fds) as (process, fd):
            if not wait_for([fd], time.monotonic() + PERF_GRACE_SECONDS, cancellation):
                raise ValueError(f"perf {argv[0]} timed out")
        if process.returncode or stdout.tell() >= limit:
            raise ValueError(f"perf {argv[0]} failed or exceeded the output limit: {contents(stderr)[-2048:]}")
        return contents(stdout)


def build_id(perf, cancellation, executable=None):
    """Read kernel or executable build IDs with perf, including an explicit unavailable reason."""
    argv, fds = ["buildid-list", "-k"], ()
    if executable is not None:
        fd = executable.file.fileno()
        if os.pread(fd, 4, 0) != b"\x7fELF":
            return None, "the supplied executable is not an ELF file"
        # store artifacts can have a build owner different from this user or root.
        # -f reads only the already-opened, explicitly selected ELF.
        argv, fds = ["buildid-list", "-f", "-i", f"/proc/self/fd/{fd}"], (fd,)
    try:
        value = perf_query(perf, argv, cancellation, LOG_LIMIT, pass_fds=fds).strip()
        if not re.fullmatch(r"(?:[0-9a-fA-F]{2}){4,64}", value) or int(value, 16) == 0:
            raise ValueError("perf returned no usable build ID")
        return value.lower(), None
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        return None, str(error)


def named_symbol(name):
    return (bool(name) and not name.startswith("[") and name not in ("??", "unknown")
            and not re.fullmatch(r"(?:0x[0-9a-fA-F]+|[0-9a-fA-F]{8,})", name))


def quality(text):
    """Count self samples and samples represented by folded kernel callchains."""
    samples = symbolized = stack_samples = with_caller = 0
    remaining = 0
    lost = None
    event = False
    for line in text.splitlines():
        if line.startswith("# Total Lost Samples:"):
            lost = int(line.split(":")[1])
        elif line.startswith("# Samples:"):
            event = f"of event '{EVENT}'" in line
            remaining = 0
        elif event and line.strip() and not line.startswith("#"):
            first, sep, rest = line.partition("|")
            if first.strip().isdecimal() and sep:
                # perf may append columns such as IPC after the requested symbol field
                symbol = rest.partition("|")[0].strip()
                if not symbol:
                    raise ValueError("missing perf report symbol field")
                remaining = int(first)
                samples += remaining
                level, _, name = symbol.partition(" ")
                if level == "[k]" and named_symbol(name.strip()):
                    symbolized += remaining
            else:
                # folded 'count' rows start with their own sample count, then the leaf and its callers
                fields = line.split(None, 1)
                if len(fields) != 2 or not fields[0].isdecimal() or not 0 < int(fields[0]) <= remaining:
                    raise ValueError("unrecognized perf report callchain row")
                count = int(fields[0])
                remaining -= count
                stack_samples += count
                if any(named_symbol(frame.strip()) for frame in fields[1].split("|")[1:]):
                    with_caller += count
    if not samples:
        result = "no_samples"
    elif not symbolized:
        result = "unresolved"
    elif not with_caller:
        result = "no_symbolized_caller"
    elif lost != 0 or min(symbolized, with_caller) < samples:
        result = "partial"
    else:
        result = "symbolized"
    return dict(samples=samples, lost_samples=lost, symbolized_samples=symbolized,
                callchain_samples_checked=stack_samples, samples_with_symbolized_caller=with_caller, quality=result)


def cold_cache(timeout, cancellation):
    deadline = cache.deadline_after(timeout)
    def interrupt(number, _frame):
        raise InterruptedError(f"{signal.Signals(number).name} waiting for the cache broker; completion unknown")
    with ExitStack() as handlers, socket.socket(socket.AF_UNIX) as connection:
        # this synchronous request owns no child. Interrupt the wait even if the broker is in reclaim.
        for number in (signal.SIGINT, signal.SIGTERM):
            previous = signal.getsignal(number)
            if previous != signal.SIG_IGN:
                handlers.callback(signal.signal, number, previous)
                signal.signal(number, interrupt)
        cancellation.checkpoint()
        connection.settimeout(cache.remaining(deadline))
        connection.connect(CACHE_SOCKET)
        cache.send(connection, dict(version=cache.VERSION, operation="drop", deadline_ns=deadline), deadline,
                   cache.MAX_REQUEST)
        connection.shutdown(socket.SHUT_WR)
        result = cache.response(cache.receive(connection, deadline, cache.MAX_REPLY))
    if not result["ok"]:
        raise ValueError(f"cache broker failed at {result['stage']}")
    return result


class CaptureError(ValueError):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


@contextmanager
def pipe():
    read, write = os.pipe()
    with os.fdopen(read, "rb", buffering=0) as reader, os.fdopen(write, "wb", buffering=0) as writer:
        yield reader, writer


def profile_window(args, perf, copy, copy_fd, start, profile, cancellation):
    """Bracket perf's enable/disable operations on its private control pipes."""
    path = args.output / profile["filename"]
    maximum = args.max_data_mib * 1024 * 1024
    def limit_recording():
        _, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
        bound = maximum if hard == resource.RLIM_INFINITY else min(maximum, hard)
        resource.setrlimit(resource.RLIMIT_FSIZE, (bound, bound))

    ended = None
    def observe(events, deadline, *, need_live_target=False):
        nonlocal ended
        while True:
            limit = min(start + args.timeout_seconds, deadline) if ended is None else deadline
            watch = [*events, copy_fd] if ended is None else events
            ready = wait_for(watch, limit, cancellation)
            if copy_fd in ready:
                ended = time.monotonic()
                if need_live_target:
                    raise CaptureError("target_finished", "rcp exited before the sampling window finished")
                ready.remove(copy_fd)
            elif ended is None and not ready and limit < deadline:
                raise CaptureError("timed_out", "copy timeout during profiling")
            if ready or time.monotonic() >= deadline:
                return ready

    launched = time.monotonic()
    profile["recorder_start_seconds"] = launched - start
    with pipe() as (commands, controller), pipe() as (acknowledgments, replies), bounded_output(LOG_LIMIT) as log:
        control_fd, ack_fd = commands.fileno(), replies.fileno()
        command = ["record", "-e", EVENT, "-F", str(args.frequency), "--strict-freq", "-g", "--kernel-callchains",
                   "-p", str(copy.pid), "-D", "-1", "--control", f"fd:{control_fd},{ack_fd}", "--no-buildid-cache",
                   f"--max-size={args.max_data_mib}M", "-o", str(path)]
        try:
            with child(perf, command, pass_fds=(control_fd, ack_fd), stdout=subprocess.DEVNULL, stderr=log,
                       env=child_environment(profiler=True), umask=0o077, preexec_fn=limit_recording) as (recorder, fd):
                commands.close()
                replies.close()
                def acknowledged(operation, need_live_target):
                    sent = time.monotonic()
                    controller.write(operation.encode() + b"\n")
                    ready = observe([acknowledgments, fd], sent + PERF_GRACE_SECONDS, need_live_target=need_live_target)
                    if acknowledgments not in ready or acknowledgments.read(64).rstrip(b"\n\0") != b"ack":
                        raise CaptureError("unavailable", f"perf did not acknowledge {operation}")
                    return [sent - start, time.monotonic() - start]

                # ping is processed after setup; enable bounds exclude startup and map synthesis
                acknowledged("ping", True)
                enabled = profile["enable_bounds_seconds"] = acknowledged("enable", True)
                if observe([fd], start + enabled[1] + args.duration_seconds, need_live_target=True):
                    raise CaptureError("incomplete", "perf exited during the sampling window")
                profile["disable_bounds_seconds"] = acknowledged("disable", False)
                # the target may now finish while perf writes headers or resolves build IDs
                with suppress(BrokenPipeError):
                    controller.write(b"stop\n")
                if not observe([fd], time.monotonic() + PERF_GRACE_SECONDS):
                    raise CaptureError("timed_out", "perf did not finalize within its time budget")
            if recorder.returncode or not path.is_file():
                raise CaptureError("unavailable", "perf did not produce a completed recording")
            if path.stat().st_size >= maximum:
                raise CaptureError("incomplete", "perf reached the recording size limit")
            profile["status"] = "recorded"
        except (CaptureError, OSError, subprocess.SubprocessError) as error:
            diagnostic = contents(log)[-2048:].strip()
            profile.update(status=getattr(error, "status", "unavailable"),
                           error=f"{error}: {diagnostic}" if diagnostic else str(error))
        finally:
            profile["recorder_elapsed_seconds"] = time.monotonic() - launched
            if "recorder" in locals():
                profile["record_exit_code"] = recorder.returncode
            if path.is_file():
                profile["data"] = path.name
    return ended


def checked_copy(args, executable, perf, row, expected, destination, cancellation):
    profiled = row["profiled"]
    profile = row.get("profile")
    # only the ordinary rcp child gets no_new_privs. A separately provisioned perf may have its own capability
    libc = ctypes.CDLL(None, use_errno=True)
    def unprivileged():
        if libc.prctl(38, 1, 0, 0, 0) != 0:  # PR_SET_NO_NEW_PRIVS
            raise OSError(ctypes.get_errno(), "setting no_new_privs for rcp")
    argv = [*args.copy_options, str(args.source), str(destination)]
    with bounded_output(LOG_LIMIT) as stdout, bounded_output(LOG_LIMIT) as stderr:
        start = time.monotonic()
        deadline = start + args.timeout_seconds
        with child(executable, argv, preexec_fn=unprivileged, stdout=stdout, stderr=stderr,
                   env=child_environment()) as (copy, fd):
            ended = None
            if profiled:
                if wait_for([fd], start + args.delay_seconds, cancellation):
                    ended = time.monotonic()
                    profile.update(status="target_finished", error="rcp exited before the requested attachment")
                else:
                    ended = profile_window(args, perf, copy, fd, start, profile, cancellation)
            if ended is None and wait_for([fd], deadline, cancellation):
                ended = time.monotonic()
        row.update(exit_code=copy.returncode, elapsed_seconds=(ended or time.monotonic()) - start)
        if ended is None:
            row["status"] = "timed_out"
            raise ValueError("rcp exceeded its copy timeout")
        if copy.returncode:
            raise ValueError(f"rcp exited {copy.returncode}: {(contents(stderr) or contents(stdout))[-2048:]}")
        report = parse_json(contents(stdout).rstrip().rpartition("\n")[2])
    counts = expected["counts"]
    for key, wanted in (("files_copied", counts["files"]), ("files_unchanged", 0), ("bytes_copied", counts["bytes"])):
        if not isinstance(report, dict) or type(report.get(key)) is not int or report[key] != wanted:
            raise ValueError(f"rcp summary differs from the fixture for {key}")
    actual = streaming.scan_tree(destination, expected_counts=counts, policy=METADATA_POLICY)
    if actual["digest"] != expected["digest"]:
        raise ValueError("destination paths or sizes differ from the prepared fixture")
    row.update(counts=actual["counts"], files_per_second=counts["files"] / row["elapsed_seconds"],
               bytes_per_second=counts["bytes"] / row["elapsed_seconds"])
    # rcp emits a zeroed resource summary when it cannot measure its own process
    rss = report.get("peak_rss_bytes")
    if type(rss) is int and rss > 0:
        for key in ("cpu_time_kernel_ms", "cpu_time_user_ms"):
            if type(report.get(key)) is int and report[key] >= 0:
                row[key] = report[key]
                row[key + "_per_file"] = report[key] / counts["files"]
    if profiled and profile["status"] == "recorded":
        command = ["report", "--stdio", "--no-children", "--percent-limit", "0", "--no-inline",
                   "--max-stack", str(REPORT_STACK_DEPTH),
                   "--call-graph", f"folded,0,{REPORT_STACKS_PER_SYMBOL},callee,function,count",
                   "--sort", "symbol", "--fields", "sample,symbol", "--field-separator", "|",
                   "-i", str(args.output / profile["data"])]
        try:
            profile.update(quality(perf_query(perf, command, cancellation, REPORT_LIMIT)))
            if not profile["samples"]:
                raise ValueError("no kernel samples in the recording")
            if not profile["symbolized_samples"]:
                raise ValueError("no symbolized kernel samples in the recording")
            if not profile["samples_with_symbolized_caller"]:
                raise ValueError("no symbolized kernel caller in the checked callchains")
            profile["status"] = "complete"
        except (ValueError, OSError, subprocess.SubprocessError) as error:
            profile.update(status="unusable", error=str(error))
    if profiled and profile["status"] != "complete":
        raise ValueError(profile["error"])


def save(output, record):
    temporary = output / ".result.tmp"
    fd = os.open(temporary, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(record, handle, indent=2, allow_nan=False)
        handle.write("\n")
    temporary.replace(output / "result.json")


def run(args):
    preflight(args)
    with ExitStack() as stack:
        perf = Executable(args.perf, stack)
        builds = {"candidate": Executable(args.candidate, stack)}
        if args.baseline is not None:
            builds["baseline"] = Executable(args.baseline, stack)
        args.output.mkdir(mode=0o700)
        record = dict(status="running", kernel_release=os.uname().release, machine=os.uname().machine,
                      perf_sha256=perf.sha256, builds={k: dict(path=str(v.path), sha256=v.sha256) for k, v in builds.items()},
                      source=str(args.source), destination_root=str(args.destination_root), verification=METADATA_POLICY,
                      cache=args.cache, copy_options=args.copy_options, pairs_per_build=args.pairs, trials=[], pairs=[],
                      event=EVENT, delay_seconds=args.delay_seconds, duration_seconds=args.duration_seconds,
                      frequency=args.frequency, timeout_seconds=args.timeout_seconds, max_data_mib=args.max_data_mib,
                      report_stack_depth=REPORT_STACK_DEPTH, report_stacks_per_symbol=REPORT_STACKS_PER_SYMBOL)
        row = None
        try:
            with SignalCancellation() as cancellation:
                record["perf_version"] = perf_query(perf, ["--version"], cancellation, LOG_LIMIT).strip()
                record["kernel_build_id"], record["kernel_build_id_error"] = build_id(perf, cancellation)
                record["perf_build_id"], record["perf_build_id_error"] = build_id(perf, cancellation, perf)
                for name, binary in builds.items():
                    info = record["builds"][name]
                    info["elf_build_id"], info["build_id_error"] = build_id(perf, cancellation, binary)
                expected = streaming.scan_tree(args.source, policy=METADATA_POLICY)
                if not expected["counts"]["files"]:
                    raise ValueError("expected a prepared fixture with at least one regular file")
                record["fixture"] = dict(counts=expected["counts"], digest=expected["digest"])
                for pair in range(1, args.pairs + 1):
                    order = (False, True) if pair % 2 else (True, False)
                    for name in (builds if pair % 2 else reversed(builds)):
                        elapsed = {}
                        for profiled in order:
                            cancellation.checkpoint()
                            row = dict(build=name, sha256=builds[name].sha256, pair=pair, profiled=profiled,
                                       status="running")
                            if profiled:
                                row["profile"] = dict(status="pending", samples=None, lost_samples=None,
                                    symbolized_samples=None, callchain_samples_checked=None, samples_with_symbolized_caller=None,
                                    data=None, filename=f"{name}-{pair}.perf.data")
                            record["trials"].append(row)
                            save(args.output, record)
                            with tempfile.TemporaryDirectory(dir=args.destination_root, prefix="profile-copy-") as scratch:
                                if args.cache == "linux-drop-caches":
                                    row["cache_preparation"] = cold_cache(args.timeout_seconds, cancellation)
                                checked_copy(args, builds[name], perf, row, expected, Path(scratch) / "copy", cancellation)
                            row["status"] = "complete"
                            elapsed[profiled] = row["elapsed_seconds"]
                            save(args.output, record)
                            row = None
                        record["pairs"].append(dict(build=name, pair=pair,
                            overhead_percent=100 * (elapsed[True] / elapsed[False] - 1)))
                final = streaming.scan_tree(args.source, expected_counts=expected["counts"], policy=METADATA_POLICY)
                if final["metadata"] != expected["metadata"]:
                    raise ValueError("prepared source changed during the comparison")
                cancellation.checkpoint()
            record["status"] = "complete"
        except BaseException as error:
            record.update(status="failed", error=str(error) or type(error).__name__)
            if row is not None:
                if row["status"] == "running":
                    row["status"] = "failed"
                row["error"] = record["error"]
            raise
        finally:
            save(args.output, record)


def main(argv=None):
    try:
        run(arguments(argv))
    except (ValueError, OSError, subprocess.SubprocessError, KeyboardInterrupt) as error:
        print(f"kernel profile: {error or 'interrupted'}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
