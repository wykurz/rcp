"""Freeze automatic APT maintenance in disposable benchmark images; no AWS calls."""

import argparse
import os
import subprocess
import time

TIMERS = ("apt-daily.timer", "apt-daily-upgrade.timer")
SERVICES = ("apt-daily.service", "apt-daily-upgrade.service")
UNITS = TIMERS + SERVICES
DRAIN_SECONDS = 300


def invoke(argv, *, timeout):
    return subprocess.run(argv, check=True, text=True, capture_output=True, timeout=timeout).stdout


def states(command=invoke, timeout=10):
    output = command(["systemctl", "show", "--property=Id,UnitFileState,ActiveState", *UNITS], timeout=timeout)
    result = {}
    for block in output.strip().split("\n\n"):
        fields = dict(line.split("=", 1) for line in block.splitlines())
        name = fields.get("Id")
        if name not in UNITS or name in result or set(fields) != {"Id", "UnitFileState", "ActiveState"}:
            raise ValueError("unexpected APT unit status")
        if fields["UnitFileState"] != "masked":
            raise ValueError("APT unit is not persistently masked: " + name)
        result[name] = fields
    if set(result) != set(UNITS):
        raise ValueError("incomplete APT unit status")
    return result


def check_quiet(command=invoke):
    result = states(command)
    if any(unit["ActiveState"] != "inactive" for unit in result.values()):
        raise ValueError("automatic APT maintenance is not inactive: " + activity_summary(result))
    return result


def activity_summary(result):
    return ", ".join(name + "=" + unit["ActiveState"] for name, unit in result.items())


def quiesce(command=invoke, now=time.monotonic, sleep=time.sleep):
    end = now() + DRAIN_SECONDS
    def bounded(argv, *, timeout=10):
        remaining = end - now()
        if remaining <= 0:
            raise TimeoutError("automatic APT maintenance did not drain")
        return command(argv, timeout=min(timeout, remaining))
    # stop timers before masking: systemd 255 reloads masked timers before --now stops them,
    # which can turn a waiting timer into failed/resources; never stop active apt/dpkg services
    bounded(["systemctl", "stop", *TIMERS])
    bounded(["systemctl", "mask", *UNITS])
    while True:
        result = states(bounded)
        if all(unit["ActiveState"] == "inactive" for unit in result.values()):
            return result
        if any(result[name]["ActiveState"] != "inactive" for name in TIMERS):
            raise ValueError("automatic APT timer did not stop: " + activity_summary(result))
        if any(result[name]["ActiveState"] not in {"inactive", "active", "activating", "deactivating", "reloading"}
               for name in SERVICES):
            raise ValueError("unexpected APT service activity state: " + activity_summary(result))
        remaining = end - now()
        if remaining <= 0:
            raise TimeoutError("automatic APT maintenance did not drain")
        sleep(min(2, remaining))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quiesce", action="store_true", help="persistently mask timers/services and drain active work")
    args = parser.parse_args()
    if args.quiesce:
        if os.geteuid() != 0:
            raise ValueError("image preparation requires root")
        quiesce()
    else:
        check_quiet()


if __name__ == "__main__":
    main()
