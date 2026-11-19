"""Weekly preservation checks retain separate topology results and metadata evidence."""

import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from benchmarks import run, weekly_preserve


class WeeklyPreserveTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.binaries = self.root / "binaries"
        self.output = self.root / "results"

    def test_weekly_fixture_has_explicit_preservation_and_bounded_work(self):
        manifest = run.load_manifest(weekly_preserve.MANIFEST)
        self.assertEqual(manifest["variants"], [{"id": "rcp-preserve", "tool": "rcp",
            "args": ["--summary", "--preserve-settings=all"], "processes": 1}])
        counts = [run.expected_counts(case) for case in manifest["cases"]]
        self.assertEqual([item["files"] for item in counts], [100000, 16000])
        self.assertEqual(sum(item["bytes"] for item in counts), 1150976000)

    def test_custom_manifest_cannot_change_the_measured_preservation_contract(self):
        original = run.load_manifest(weekly_preserve.MANIFEST)
        for arguments in (["--summary"], ["--summary", "--preserve-settings=all", "--no-encryption"]):
            manifest = {**original, "variants": [{**original["variants"][0], "args": arguments}]}
            path = self.root / "custom.json"
            path.write_text(json.dumps(manifest))
            with mock.patch.object(weekly_preserve.subprocess, "run") as execute, \
                    self.assertRaisesRegex(ValueError, "preservation variant"):
                weekly_preserve.main(["--bin-dir", str(self.binaries), "--scratch-root", str(self.root),
                                     "--output", str(self.output), "--manifest", str(path)])
            execute.assert_not_called()

    def test_private_loopback_binds_both_daemons_to_the_same_numeric_host(self):
        variant = run.load_manifest(weekly_preserve.MANIFEST)["variants"][0]
        tools = dict(rcp="/bin/rcp", rcpd="/bin/rcpd")
        command, = run.plan_commands(variant, "/source", "/destination", tools, "loopback", private_loopback=True)
        self.assertEqual(command[-2:], ["127.0.0.1:/source", "127.0.0.1:/destination"])
        self.assertIn("--bind-ip=127.0.0.1", command)
        self.assertIn("--rcpd-path=/bin/rcpd", command)
        self.assertIn("--preserve-settings=all", command)
        ordinary, = run.plan_commands(variant, "/source", "/destination", tools, "loopback")
        self.assertEqual(ordinary[-2:], ["localhost:/source", "/destination"])

    def copy_probe(self, command, **options):
        self.assertIn("--preserve-settings=all", command)
        source = command[-2].removeprefix("127.0.0.1:")
        before = Path(source).stat()
        destination = command[-1].removeprefix("127.0.0.1:")
        shutil.copy2(source, destination)
        os.utime(destination, ns=(before.st_atime_ns, before.st_mtime_ns))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    def test_probe_compares_nanosecond_atime_before_reading_the_destination(self):
        with mock.patch.object(weekly_preserve.subprocess, "run", side_effect=self.copy_probe):
            for mode in ("local", "loopback"):
                result = weekly_preserve.metadata_probe(self.binaries, self.root, mode, weekly_preserve.VARIANT)
                self.assertEqual(result["expected"], result["observed"])
                self.assertEqual(result["expected"]["mode"], 0o641)
                self.assertNotEqual(result["expected"]["atime_ns"], result["expected"]["mtime_ns"])
                self.assertTrue(result["payload_matches"])

    def test_probe_rejects_metadata_loss_even_with_matching_contents(self):
        def copy_without_mode(command, **options):
            result = self.copy_probe(command, **options)
            Path(command[-1]).chmod(0o600)
            return result
        with mock.patch.object(weekly_preserve.subprocess, "run", side_effect=copy_without_mode), \
                self.assertRaisesRegex(RuntimeError, "preserved metadata"):
            weekly_preserve.metadata_probe(self.binaries, self.root, "local", weekly_preserve.VARIANT)

    def test_failed_local_benchmark_still_attempts_loopback_and_stops_private_ssh(self):
        endpoint = mock.Mock()
        endpoint.environment.return_value = {"PATH": "/private/bin:" + os.environ["PATH"]}
        endpoint.descriptor.return_value = {"profile": "private-test", "ssh": {"sha256": "a" * 64}}
        benchmarks = []
        def execute(command, **options):
            benchmarks.append((command, dict(os.environ)))
            mode = command[command.index("--mode") + 1]
            output = Path(command[command.index("--output") + 1])
            output.mkdir(parents=True, exist_ok=True)
            (output / "results.json").write_text(json.dumps({"status": "failed" if mode == "local" else "complete"}))
            if mode == "local":
                raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 0)
        with mock.patch.object(weekly_preserve.private_sshd, "Endpoint",
                new=mock.Mock(return_value=endpoint, command=weekly_preserve.private_sshd.Endpoint.command)), \
                mock.patch.object(run, "main", side_effect=execute), \
                mock.patch.object(weekly_preserve.subprocess, "run", side_effect=self.copy_probe):
            status = weekly_preserve.main(["--bin-dir", str(self.binaries), "--scratch-root", str(self.root),
                                           "--output", str(self.output)])
        self.assertEqual(status, 1)
        self.assertEqual(len(benchmarks), 2)
        for (command, environment), mode in zip(benchmarks, ("local", "loopback")):
            self.assertEqual(command[command.index("--mode") + 1], mode)
            self.assertEqual([command[index + 1] for index, value in enumerate(command) if value == "--case"],
                             ["tiny-100k", "small-16k"])
            for option, value in (("--variant", "rcp-preserve"), ("--repetitions", "3"),
                                  ("--cache", "source-warm"), ("--timeout", "120"),
                                  ("--runner-label", "local-preserve-v1")):
                self.assertEqual(command[command.index(option) + 1], value)
            self.assertIn("--no-timings", command)
            self.assertNotIn("metadata-size", command)
            self.assertEqual(Path(command[command.index("--output") + 1]), self.output / mode)
            if mode == "loopback":
                self.assertTrue(environment["PATH"].startswith("/private/bin:"))
                self.assertIn("private-test", command[command.index("--ssh-transport-profile") + 1])
        endpoint.start.assert_called_once()
        endpoint.stop.assert_called_once()
        evidence = json.loads((self.output / "metadata.json").read_text())
        self.assertEqual(evidence["local"]["status"], "failed")
        self.assertEqual(evidence["loopback"]["status"], "complete")
        self.assertTrue((self.output / "local/results.json").exists())

    def test_failed_ssh_setup_still_cleans_up_and_retains_local_evidence(self):
        endpoint = mock.Mock()
        endpoint.start.side_effect = RuntimeError("cannot start ssh")
        with mock.patch.object(weekly_preserve.private_sshd, "Endpoint", return_value=endpoint), \
                mock.patch.object(weekly_preserve, "metadata_probe", return_value={"payload_matches": True}), \
                mock.patch.object(run, "main"):
            status = weekly_preserve.main(["--bin-dir", str(self.binaries), "--scratch-root", str(self.root),
                                           "--output", str(self.output)])
        self.assertEqual(status, 1)
        endpoint.stop.assert_called_once()
        evidence = json.loads((self.output / "metadata.json").read_text())
        self.assertEqual(evidence["local"]["status"], "complete")
        self.assertIn("cannot start ssh", evidence["loopback"]["error"])

    def test_termination_reaps_the_copy_and_stops_the_endpoint(self):
        script = r'''
import os, sys
from pathlib import Path
from benchmarks import run, weekly_preserve
root = Path(sys.argv[1])
class Endpoint:
    def __init__(self, **kwargs): pass
    def start(self): (root / "endpoint-started").touch()
    def stop(self): (root / "endpoint-stopped").touch()
    def environment(self): return {}
    def descriptor(self): return {"profile": "test"}
weekly_preserve.private_sshd.Endpoint = Endpoint
weekly_preserve.metadata_probe = lambda *args: {"payload_matches": True}
def benchmark(arguments, *, private_loopback=False):
    if private_loopback:
        child = "import os, time; from pathlib import Path; p=Path(" + repr(str(root / "child.pending")) + "); p.write_text(str(os.getpid())); p.rename(p.with_name('child')); time.sleep(120)"
        run.execute_commands([[sys.executable, "-c", child]], root / "logs", 120)
weekly_preserve.run.main = benchmark
weekly_preserve.main(["--bin-dir", str(root), "--scratch-root", str(root), "--output", str(root / "results")])
'''
        with subprocess.Popen([sys.executable, "-c", script, str(self.root)],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
            try:
                deadline = time.monotonic() + 10
                while not (self.root / "child").exists():
                    if process.poll() is not None or time.monotonic() > deadline:
                        self.fail("benchmark child did not start")
                    time.sleep(0.01)
                child = int((self.root / "child").read_text())
                process.send_signal(signal.SIGTERM)
                _, errors = process.communicate(timeout=10)
                self.assertNotEqual(process.returncode, 0, errors)
                self.assertTrue((self.root / "endpoint-stopped").exists(), errors)
                with self.assertRaises(ProcessLookupError):
                    os.kill(child, 0)
                evidence = json.loads((self.output / "metadata.json").read_text())
                self.assertEqual(evidence["local"]["status"], "complete")
                self.assertEqual(evidence["loopback"]["status"], "interrupted")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()

    def test_interruption_of_local_work_never_starts_the_next_topology(self):
        with mock.patch.object(weekly_preserve, "metadata_probe", side_effect=InterruptedError("SIGTERM")), \
                mock.patch.object(weekly_preserve.private_sshd, "Endpoint") as endpoint, \
                self.assertRaises(InterruptedError):
            weekly_preserve.main(["--bin-dir", str(self.binaries), "--scratch-root", str(self.root),
                                  "--output", str(self.output)])
        endpoint.assert_not_called()
        self.assertEqual(json.loads((self.output / "metadata.json").read_text()), {"local": {"status": "interrupted"}})

    def test_termination_during_verification_cannot_become_a_recoverable_failure(self):
        def benchmark(*args, **kwargs):
            with mock.patch.object(run, "scan_tree", side_effect=lambda *a: signal.raise_signal(signal.SIGTERM)):
                proof = run.validate_tree(self.root, self.root)
            if not proof["ok"]:
                raise RuntimeError("source verification failed")
        with mock.patch.object(weekly_preserve, "metadata_probe", return_value={}), \
                mock.patch.object(run, "main", side_effect=benchmark), \
                mock.patch.object(weekly_preserve.private_sshd, "Endpoint") as endpoint, \
                self.assertRaises(KeyboardInterrupt):
            weekly_preserve.main(["--bin-dir", str(self.binaries), "--scratch-root", str(self.root),
                                  "--output", str(self.output)])
        endpoint.assert_not_called()
        self.assertEqual(json.loads((self.output / "metadata.json").read_text()), {"local": {"status": "interrupted", "probe": {}}})

    def test_an_ordinary_verification_error_records_failure_and_attempts_the_other_topology(self):
        endpoint = mock.Mock()
        endpoint.environment.return_value = {}
        endpoint.descriptor.return_value = {}
        with mock.patch.object(weekly_preserve, "metadata_probe", return_value={}), \
                mock.patch.object(run, "main", side_effect=[ValueError("filegen count mismatch"), None]), \
                mock.patch.object(weekly_preserve.private_sshd, "Endpoint", return_value=endpoint):
            status = weekly_preserve.main(["--bin-dir", str(self.binaries), "--scratch-root", str(self.root),
                                          "--output", str(self.output)])
        self.assertEqual(status, 1)
        evidence = json.loads((self.output / "metadata.json").read_text())
        self.assertEqual(evidence["local"]["status"], "failed")
        self.assertEqual(evidence["loopback"]["status"], "complete")
        endpoint.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
