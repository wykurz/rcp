"""Bounded preservation measurements using the weekly build on an Actions runner."""

import argparse
import contextlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import tempfile

from benchmarks import private_sshd, run
from benchmarks.signals import SignalCancellation

MANIFEST = Path(__file__).with_name("preservation-cases.json")
VARIANT = dict(id="rcp-preserve", tool="rcp", args=["--summary", "--preserve-settings=all"], processes=1)


@contextlib.contextmanager
def endpoint_environment(endpoint):
    """Run the harness in this process so its existing cancellation owns every copy child."""
    changes = endpoint.environment() if endpoint is not None else {}
    previous = {key: os.environ.get(key) for key in changes}
    try:
        os.environ.update(changes)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@contextlib.contextmanager
def termination():
    def stop(_number, _frame):
        # verification may turn ordinary exceptions into failed proofs; termination must unwind
        raise KeyboardInterrupt("SIGTERM")
    previous = signal.signal(signal.SIGTERM, stop)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def _metadata(path):
    value = path.stat()
    return dict(mode=stat.S_IMODE(value.st_mode), uid=value.st_uid, gid=value.st_gid,
                atime_ns=value.st_atime_ns, mtime_ns=value.st_mtime_ns)


def metadata_probe(binaries, scratch, mode, variant):
    """Check one file's preserved metadata before a payload read can change its atime."""
    with tempfile.TemporaryDirectory(prefix="preserve-probe-", dir=scratch) as directory:
        source, destination = Path(directory) / "source", Path(directory) / "destination"
        payload = b"preservation probe\n"
        source.write_bytes(payload)
        source.chmod(0o641)
        os.utime(source, ns=(1600000000123456789, 1600000060987654321))
        expected = _metadata(source)
        command, = run.plan_commands(variant, source, destination,
                                    {name: str(binaries / name) for name in ("rcp", "rcpd")}, mode,
                                    private_loopback=mode == "loopback")
        subprocess.run(command, check=True, capture_output=True, text=True, timeout=30)
        observed = _metadata(destination)
        if observed != expected:
            raise RuntimeError(f"preserved metadata mismatch: expected {expected}, observed {observed}")
        if destination.read_bytes() != payload:
            raise RuntimeError("preservation probe payload mismatch")
        return dict(expected=expected, observed=observed, payload_matches=True)


def main(argv=None):
    with termination():
        return measure(argv)


def measure(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--scratch-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    parser.add_argument("--runner-label", default="local-preserve-v1")
    args = parser.parse_args(argv)
    binaries, output, scratch = (path.resolve() for path in (args.bin_dir, args.output, args.scratch_root))
    manifest = run.load_manifest(args.manifest)
    if manifest["variants"] != [VARIANT]:
        raise ValueError("preservation variant must retain its exact arguments and one-process rcp contract")
    variant, = manifest["variants"]
    output.mkdir(parents=True, exist_ok=False)
    snapshot = output / "manifest.json"
    snapshot.write_text(json.dumps(manifest) + "\n")
    scratch.mkdir(parents=True, exist_ok=True)
    evidence = {}
    for mode in ("local", "loopback"):
        endpoint = None
        evidence[mode] = dict(status="running")
        try:
            command = ["--manifest", str(snapshot),
                       "--variant", "rcp-preserve", "--bin-dir", str(binaries), "--mode", mode,
                       "--source-root", str(scratch), "--destination-root", str(scratch),
                       "--cache", "source-warm", "--no-timings", "--repetitions", "3", "--timeout", "120",
                       "--runner-label", args.runner_label, "--output", str(output / mode)]
            for case in manifest["cases"]:
                command += ["--case", case["id"]]
            if mode == "loopback":
                endpoint = private_sshd.Endpoint(log=scratch / "preservation-sshd.log")
                endpoint.start()
                command += ["--ssh-transport-profile", json.dumps(endpoint.descriptor(), sort_keys=True)]
            with endpoint_environment(endpoint):
                evidence[mode]["probe"] = metadata_probe(binaries, scratch, mode, variant)
                run.main(command, private_loopback=mode == "loopback")
            evidence[mode]["status"] = "complete"
        except (InterruptedError, KeyboardInterrupt):
            evidence[mode].update(status="interrupted")
            raise
        except Exception as error:
            evidence[mode].update(status="failed", error=str(error))
            print(f"{mode} preservation check failed: {error}", file=sys.stderr)
        finally:
            with SignalCancellation():
                try:
                    if endpoint is not None:
                        endpoint.stop()
                finally:
                    (output / "metadata.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return int(any(value["status"] != "complete" for value in evidence.values()))


if __name__ == "__main__":
    sys.exit(main())
