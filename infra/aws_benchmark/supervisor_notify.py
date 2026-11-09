"""Same-main-PID systemd notifications and a bounded future worker-image self-test.

A barrier confirms processing, not acceptance of an unknown assignment. The image
self-test additionally proves that the installed manager extends a real timer.
"""

import argparse
import ctypes
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

BARRIER_USEC = 1_000_000
ACK_BOUND_SECONDS = 2
SELF_TEST_MARKER = "notify-self-test-child-passed"


def require(value, message):
    if not value:
        raise ValueError(message)


class Notifier:
    """One root main-process notifier; never delegate notification to a child PID."""
    def __init__(self, budget, *, unit="rcp-supervisor.service", initial_runtime_seconds=10800,
                 library=None, monotonic=time.monotonic):
        self.pid, self.monotonic = os.getpid(), monotonic
        self.socket = os.environ.get("NOTIFY_SOCKET")
        self.ready_attempted = self.ready_sent = self.extension_attempted = False
        require(os.geteuid() == 0 and isinstance(self.socket, str) and
                0 < len(self.socket) <= 4096 and self.socket[0] in ("/", "@") and
                all(32 < ord(char) < 127 for char in self.socket), "missing trusted notification context")
        require(isinstance(unit, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9.-]{0,127}[.]service", unit),
                "invalid notification unit")
        require(type(initial_runtime_seconds) is int and 0 < initial_runtime_seconds <= 10800,
                "invalid initial supervisor runtime")
        escaped = "".join(chr(byte) if chr(byte).isalnum() else "_" + format(byte, "02x")
                          for byte in unit.encode("ascii"))
        self.path = "/org/freedesktop/systemd1/unit/" + escaped
        def property_value(name):
            value = subprocess.run(["/usr/bin/busctl", "--system", "get-property", "org.freedesktop.systemd1",
                self.path, "org.freedesktop.systemd1.Service", name], check=True, capture_output=True,
                text=True, timeout=min(2, budget.left())).stdout.strip()
            return value
        require(property_value("Type") == 's "notify"' and property_value("NotifyAccess") == 's "main"',
                "supervisor must restrict notification authority to its notify main process")
        main = property_value("MainPID")
        require(re.fullmatch(r"u [1-9][0-9]{0,9}", main) is not None and int(main[2:]) == self.pid,
                "not the actual supervisor main PID")
        require(property_value("RuntimeMaxUSec") == "t " + str(initial_runtime_seconds * 1_000_000),
                "initial supervisor runtime differs")
        self.library = ctypes.CDLL("libsystemd.so.0", use_errno=True) if library is None else library
        self.library.sd_notify.argtypes = (ctypes.c_int, ctypes.c_char_p)
        self.library.sd_notify.restype = ctypes.c_int
        self.library.sd_notify_barrier.argtypes = (ctypes.c_int, ctypes.c_uint64)
        self.library.sd_notify_barrier.restype = ctypes.c_int

    def _send(self, state, budget):
        require(os.getpid() == self.pid and os.geteuid() == 0 and
                os.environ.get("NOTIFY_SOCKET") == self.socket, "supervisor notification identity changed")
        started = self.monotonic()
        wait = min(BARRIER_USEC, math.floor(budget.left() * 1_000_000))
        require(wait > 0, "supervisor notification budget exhausted")
        state = state() if callable(state) else state
        require(self.library.sd_notify(0, state.encode("ascii")) > 0, "supervisor notification was not sent")
        require(self.library.sd_notify_barrier(0, wait) > 0, "supervisor notification barrier failed")
        require(os.getpid() == self.pid and self.monotonic() - started <= ACK_BOUND_SECONDS,
                "supervisor notification acknowledgement exceeded its bound")

    def ready(self, budget):
        require(not self.ready_attempted, "supervisor readiness cannot be retried")
        self.ready_attempted = True
        self._send("READY=1", budget)
        self.ready_sent = True

    def extend(self, budget, *, maximum_seconds, shutdown_seconds):
        require(self.ready_sent and not self.extension_attempted, "supervisor extension requires unreplayed readiness")
        require(type(maximum_seconds) is int and 0 < maximum_seconds <= 28800 and
                type(shutdown_seconds) is int and 0 <= shutdown_seconds <= 300,
                "invalid supervisor extension bounds")
        # mark uncertainty before notification; never resend an ambiguous extension
        self.extension_attempted = True
        sent = []
        def state():
            remaining = min(maximum_seconds, budget.left() + shutdown_seconds)
            microseconds = math.floor((remaining - ACK_BOUND_SECONDS) * 1_000_000)
            require(microseconds > 0, "insufficient supervisor extension interval")
            sent.append(microseconds)
            return "EXTEND_TIMEOUT_USEC=" + str(microseconds)
        self._send(state, budget)
        return sent[0]


class _SelfTestBudget:
    def __init__(self):
        self.end = time.monotonic() + 8
    def left(self):
        value = self.end - time.monotonic()
        require(value > 0, "notify self-test deadline exhausted")
        return value


def self_test_child(unit):
    require(re.fullmatch(r"rcp-notify-selftest-[0-9a-f]{32}[.]service", unit) is not None,
            "invalid notify self-test identity")
    budget = _SelfTestBudget()
    notifier = Notifier(budget, unit=unit, initial_runtime_seconds=1)
    notifier.ready(budget)
    notifier.extend(budget, maximum_seconds=5, shutdown_seconds=0)
    time.sleep(2)
    print(SELF_TEST_MARKER)


def image_self_test():
    """Run only during an approved root AMI build, never on a production worker."""
    require(os.geteuid() == 0, "notify image self-test requires root")
    unit = "rcp-notify-selftest-" + uuid.uuid4().hex + ".service"
    root = Path(__file__).resolve().parents[2]
    argv = ["/usr/bin/systemd-run", "--quiet", "--wait", "--pipe", "--unit=" + unit,
        "--property=Type=notify", "--property=NotifyAccess=main", "--property=RuntimeMaxSec=1s",
        "--property=TimeoutStartSec=5s", "--property=TimeoutStopSec=1s", "--property=KillMode=control-group",
        "--property=FailureAction=none", "--property=SuccessAction=none", "--property=PrivateNetwork=yes",
        "--property=NoNewPrivileges=yes", "--property=ProtectSystem=strict", "--property=ProtectHome=yes",
        "--working-directory=" + str(root), "/usr/bin/python3", "-B", "-m", "infra.aws_benchmark.supervisor_notify",
        "--self-test-child", unit]
    try:
        value = subprocess.run(argv, check=False, capture_output=True, text=True, timeout=10)
        require(value.returncode == 0 and value.stdout.strip() == SELF_TEST_MARKER,
                "notify image self-test did not survive the original one-second runtime")
    finally:
        # exact generated unit only; no host-wide cleanup or poweroff action
        for action in ("stop", "reset-failed"):
            try:
                subprocess.run(["/usr/bin/systemctl", action, unit], check=False,
                    capture_output=True, timeout=2)
            except (OSError, subprocess.TimeoutExpired):
                pass
        # successful transient units may already be unloaded; prove final state
        # rather than requiring a stop of an already absent unit to succeed.
        current = subprocess.run(["/usr/bin/systemctl", "show", unit, "--property=LoadState",
            "--property=ActiveState", "--property=SubState"], check=False, capture_output=True,
            text=True, timeout=2)
        lines = current.stdout.splitlines()
        require(len(lines) == 3 and all("=" in line for line in lines), "invalid notify self-test cleanup readback")
        state = dict(line.split("=", 1) for line in lines)
        require(set(state) == {"LoadState", "ActiveState", "SubState"} and
                state["LoadState"] in ("loaded", "not-found") and state["ActiveState"] == "inactive" and
                state["SubState"] == "dead", "notify self-test cleanup requires reconciliation")
    return dict(schema="rcp-supervisor-notify-image-acceptance-v1", passed=True,
                initial_runtime_seconds=1, observed_child_sleep_seconds=2,
                notification_access="main", process_attribution="actual-main-pid", network="private")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--image-self-test", action="store_true")
    mode.add_argument("--self-test-child")
    args = parser.parse_args(argv)
    if args.image_self_test:
        print(json.dumps(image_self_test(), sort_keys=True))
    else:
        self_test_child(args.self_test_child)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("supervisor notification acceptance failed", file=sys.stderr)
        raise SystemExit(1)
