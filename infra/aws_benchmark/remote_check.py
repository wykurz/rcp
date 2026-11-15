"""Image-build check: the measurement unit can serve remote benchmark cells.

Run as root by install-worker.sh once the harness is installed, never on a worker. It starts
the private SSH endpoint as the benchmark account inside a transient unit that has the sandbox
of a remote matrix suite, logs in once over loopback and stops. An image that cannot do this
fails its build here, instead of at the first paid run.
"""

import os
import subprocess

from . import worker

UNIT = "rcp-remote-check"
SECONDS = 120


def command():
    """The transient unit: the sandbox of a remote suite around one login through the endpoint."""
    properties = worker.sandbox(SECONDS, "1073741824", loopback=True)
    # the data mounts exist only on a worker and the check writes nothing outside its /tmp
    del properties["ReadWritePaths"]
    # the build log, not a discarded stream, must say why a check failed
    properties.update(StandardOutput="journal+console", StandardError="journal+console")
    return (["/usr/bin/systemd-run", "--unit=" + UNIT, "--wait", "--quiet", "--collect"] +
            ["--property=" + key + "=" + value for key, value in properties.items()] + [*worker.ENVIRONMENT] +
            ["/usr/bin/python3", "-B", "-m", "benchmarks.private_sshd", *worker.OPENSSH])


def main():
    if os.geteuid() != 0:
        raise SystemExit("the remote-cell image check requires root")
    try:
        code = subprocess.run(command(), check=False, timeout=SECONDS + 60).returncode
    except subprocess.TimeoutExpired:
        code = None
    if code != 0:
        raise SystemExit("this image cannot serve remote benchmark cells: the private sshd login under the "
                         f"measurement sandbox {'timed out' if code is None else f'exited with status {code}'}")
    print("remote benchmark cells: private sshd login under the measurement sandbox succeeded")


if __name__ == "__main__":
    main()
