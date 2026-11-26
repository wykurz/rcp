"""Local kernel-profile calibration uses comparable copies and private, process-scoped evidence."""

import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


COPY = """
import json, os, pathlib, shutil, sys, time
settings = json.loads(pathlib.Path(__file__).resolve().with_name("settings.json").read_text())
assert "PROFILE_CREDENTIAL_SENTINEL" not in os.environ
source, destination = map(pathlib.Path, sys.argv[-2:])
options = ["--summary", "--output-format=json"]
if settings.get("EXPECTED_HANDOFF"):
    options.append("--local-copy-handoff=" + settings["EXPECTED_HANDOFF"])
assert sys.argv[1:-2] == options
assert os.getuid() != 0 and not destination.exists()
pathlib.Path(settings["PROFILE_PID"]).write_text(str(os.getpid()))
time.sleep(float(settings.get("COPY_SECONDS", ".30")))
if settings.get("COPY_MODE") == "missing":
    destination.mkdir()
else:
    shutil.copytree(source, destination)
measured = not settings.get("NO_CPU_STATS")
print(json.dumps(dict(files_copied=2, bytes_copied=5, files_unchanged=0,
                      peak_rss_bytes=4096 if measured else 0,
                      cpu_time_kernel_ms=6 if measured else 0, cpu_time_user_ms=0)))
"""

PERF = """
import json, os, pathlib, sys, time
settings = json.loads(pathlib.Path(__file__).resolve().with_name("settings.json").read_text())
assert "PROFILE_CREDENTIAL_SENTINEL" not in os.environ
args = sys.argv[1:]
if args == ["--version"]:
    print("perf version 6.12")
elif args == ["buildid-list", "-k"]:
    print("11223344556677889900aabbccddeeff10203040")
elif args[:3] == ["buildid-list", "-f", "-i"]:
    with open(args[3], "rb") as exe:
        assert exe.read(4) == b"\\x7fELF"
    print(settings.get("ELF_BUILD_ID", "1234567890abcdef1234567890abcdef12345678"))
elif args[0] == "record":
    assert args[args.index("-e") + 1] == "cpu-clock:k" and "-g" in args
    assert args[args.index("-F") + 1] == "49" and "--kernel-callchains" in args
    assert "-a" not in args and "--" not in args
    pid = int(args[args.index("-p") + 1])
    assert pathlib.Path(f"/proc/{pid}/status").exists()
    if settings.get("DENY_PROFILE"):
        sys.exit("perf_event_open: Permission denied")
    if "--control" in args:
        control, ack = map(int, args[args.index("--control") + 1].removeprefix("fd:").split(","))
        commands = os.fdopen(os.dup(control), "rb", buffering=0)
        for expected in (b"ping\\n", b"enable\\n", b"disable\\n", b"stop\\n"):
            assert commands.readline() == expected
            os.write(ack, b"ack\\n\\0")
    else:
        time.sleep(.065)
    if "FLUSH_SECONDS" in settings:
        time.sleep(float(settings["FLUSH_SECONDS"]))
    pathlib.Path(args[args.index("-o") + 1]).write_bytes(b"private sample data")
elif args[0] == "report":
    if settings.get("NO_SAMPLES"):
        print("# Total Lost Samples: 0")
    else:
        print("# Total Lost Samples: 1")
        print("# Samples: 8 of event 'cpu-clock:k'")
        leaf = "[unknown]" if settings.get("NO_SYMBOLS") else "vfs_write"
        print(" 7          |[k] " + leaf + "             |-      -")
        if args[args.index("--call-graph") + 1].startswith("folded"):
            if settings.get("NO_CALLERS"):
                print("3 " + leaf)
                print("4 " + leaf + "|0xffffffff81010004|[unknown]")
            else:
                print("3 " + leaf + "|ksys_write|do_syscall_64")
                print("2 " + leaf + "|[unknown]|do_syscall_64")
                print("1 " + leaf + "|[unknown]")
        print(" 1          |[k] 0xffffffff81010000       |-      -")
else:
    sys.exit("unexpected perf command")
"""


@unittest.skipUnless(sys.platform.startswith("linux"), "kernel profiling requires Linux")
class ProfileCopyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "fixture"
        self.source.mkdir()
        (self.source / "dir").mkdir()
        (self.source / "dir" / "a").write_bytes(b"ab")
        (self.source / "b").write_bytes(b"cde")
        self.scratch = self.root / "scratch"
        self.scratch.mkdir()
        self.sentinel = self.scratch / "other-data"
        self.sentinel.write_text("untouched")
        self.output = self.root / "results"
        self.candidate = self.executable("candidate", COPY)
        self.baseline = self.executable("baseline", COPY + "# historical build\n")
        self.perf = self.executable("perf", PERF)
        self.pid = self.root / "pid"
        self.credentials = {}
        if os.geteuid() == 0:
            # CI can run as root; the measured subprocess and its fixture still have an ordinary owner
            self.credentials = dict(user=65534, group=65534, extra_groups=[])
            for path in [self.root, *self.root.rglob("*")]:
                os.chown(path, 65534, 65534)

    def executable(self, name, body):
        path = self.root / name
        path.write_text(f"#!{sys.executable}\n" + body)
        path.chmod(0o755)
        return path

    def call(self, *extra, **settings):
        (self.root / "settings.json").write_text(json.dumps(dict(PROFILE_PID=str(self.pid), **settings)))
        return subprocess.run([sys.executable, "-m", "benchmarks.profile_copy",
            "--candidate", self.candidate, "--perf", self.perf, "--source", self.source,
            "--destination-root", self.scratch, "--output", self.output, "--cache", "uncontrolled",
            "--delay-seconds", ".05", "--duration-seconds", ".05", "--timeout-seconds", "4",
            *extra], text=True, capture_output=True, timeout=15, **self.credentials,
            env={**os.environ, "PROFILE_CREDENTIAL_SENTINEL": "must stay in the driver"})

    def results(self):
        return json.loads((self.output / "result.json").read_text())

    def test_balances_both_builds_and_keeps_checked_private_measurements(self):
        result = self.call("--baseline", self.baseline)
        self.assertEqual(result.returncode, 0, result.stderr)
        record = self.results()
        self.assertEqual(record["status"], "complete")
        self.assertEqual(record["perf_version"], "perf version 6.12")
        self.assertEqual(record["kernel_release"], os.uname().release)
        for build, path in [("candidate", self.candidate), ("baseline", self.baseline)]:
            trials = [row for row in record["trials"] if row["build"] == build]
            self.assertEqual([t["profiled"] for t in trials], [False, True, True, False])
            for row in trials:
                self.assertEqual(row["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
                self.assertEqual(row["counts"], {"files": 2, "bytes": 5, "directories": 1})
                self.assertEqual(row["cpu_time_kernel_ms_per_file"], 3)
                self.assertEqual(row["cpu_time_user_ms_per_file"], 0)
                self.assertGreater(row["elapsed_seconds"], 0)
                if row["profiled"]:
                    profile = row["profile"]
                    self.assertEqual((profile["status"], profile["samples"], profile["symbolized_samples"],
                                      profile["lost_samples"]), ("complete", 8, 7, 1))
                    self.assertEqual(profile["callchain_samples_checked"], 6)
                    self.assertEqual(profile["samples_with_symbolized_caller"], 5)
                    self.assertEqual((self.output / profile["data"]).read_bytes(), b"private sample data")
        self.assertEqual(stat.S_IMODE(self.output.stat().st_mode), 0o700)
        self.assertTrue(all(p.stat().st_mode & 0o077 == 0 for p in self.output.iterdir()))
        self.assertEqual(list(self.scratch.iterdir()), [self.sentinel])

    def test_unmeasured_process_stats_do_not_count_as_zero_cpu_time(self):
        result = self.call(NO_CPU_STATS=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        for row in self.results()["trials"]:
            self.assertNotIn("cpu_time_kernel_ms_per_file", row)
            self.assertNotIn("cpu_time_user_ms_per_file", row)

    def test_profile_permission_failure_keeps_independent_copy_time_and_reason(self):
        result = self.call(DENY_PROFILE="1")
        self.assertNotEqual(result.returncode, 0)
        record = self.results()
        self.assertEqual(record["status"], "failed")
        trial = record["trials"][-1]
        self.assertEqual(trial["counts"]["bytes"], 5)
        self.assertIn("Permission denied", trial["profile"]["error"])
        self.assertIsNone(trial["profile"]["samples"])

    def test_finishing_before_attach_never_supplies_a_profile(self):
        result = self.call("--delay-seconds", "1", COPY_SECONDS="0")
        self.assertNotEqual(result.returncode, 0)
        profile = self.results()["trials"][-1]["profile"]
        self.assertEqual(profile["status"], "target_finished")
        self.assertIsNone(profile["data"])

    def test_empty_destination_fails_even_when_summary_claims_all_files(self):
        result = self.call(COPY_MODE="missing", COPY_SECONDS="0")
        self.assertNotEqual(result.returncode, 0)
        trial = self.results()["trials"][0]
        self.assertEqual(trial["status"], "failed")
        self.assertNotIn("counts", trial)
        self.assertEqual(list(self.scratch.iterdir()), [self.sentinel])

    def test_no_samples_fails_evidence_quality(self):
        result = self.call(NO_SAMPLES="1")
        self.assertNotEqual(result.returncode, 0)
        profile = self.results()["trials"][-1]["profile"]
        self.assertEqual(profile["samples"], 0)
        self.assertNotEqual(profile["status"], "complete")

    def test_fixed_handoff_applies_to_every_copy_and_build_id_availability_is_explicit(self):
        result = self.call("--local-copy-handoff", "never", EXPECTED_HANDOFF="never")
        self.assertEqual(result.returncode, 0, result.stderr)
        record = self.results()
        self.assertEqual(record["copy_options"], ["--summary", "--output-format=json", "--local-copy-handoff=never"])
        self.assertEqual(record["kernel_build_id"], "11223344556677889900aabbccddeeff10203040")
        build = record["builds"]["candidate"]
        self.assertIsNone(build["elf_build_id"])
        self.assertIn("ELF", build["build_id_error"])

    def test_records_build_id_from_selected_elf_even_if_that_binary_cannot_copy(self):
        result = self.call("--candidate", sys.executable)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.results()["builds"]["candidate"]["elf_build_id"],
                         "1234567890abcdef1234567890abcdef12345678")

    def test_copy_exit_during_recorder_finalization_preserves_the_completed_window(self):
        result = self.call(FLUSH_SECONDS=".55")
        self.assertEqual(result.returncode, 0, result.stderr)
        for row in self.results()["trials"]:
            if row["profiled"]:
                profile = row["profile"]
                self.assertEqual(profile["status"], "complete")
                enable, disable = profile["enable_bounds_seconds"], profile["disable_bounds_seconds"]
                self.assertGreaterEqual(disable[0] - enable[1], .05)
                self.assertLess(disable[1], row["elapsed_seconds"])
                self.assertLess(row["elapsed_seconds"], profile["recorder_start_seconds"] +
                                profile["recorder_elapsed_seconds"])

    def test_all_unsymbolized_samples_are_unusable_for_attribution(self):
        result = self.call(NO_SYMBOLS="1")
        self.assertNotEqual(result.returncode, 0)
        profile = self.results()["trials"][-1]["profile"]
        self.assertEqual((profile["samples"], profile["symbolized_samples"], profile["quality"]), (8, 0, "unresolved"))
        self.assertNotEqual(profile["status"], "complete")

    def test_named_leaves_without_a_named_kernel_caller_are_unusable_for_stacks(self):
        result = self.call(NO_CALLERS=True)
        self.assertNotEqual(result.returncode, 0)
        profile = self.results()["trials"][-1]["profile"]
        self.assertEqual(profile["symbolized_samples"], 7)
        self.assertEqual(profile["callchain_samples_checked"], 7)
        self.assertEqual(profile["samples_with_symbolized_caller"], 0)
        self.assertEqual(profile["status"], "unusable")

    def test_copy_timeout_reaps_the_target_and_retains_caller_files(self):
        result = self.call("--timeout-seconds", ".25", COPY_SECONDS="30")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.results()["trials"][0]["status"], "timed_out")
        self.assertFalse(Path(f"/proc/{self.pid.read_text()}").exists())
        self.assertEqual(self.sentinel.read_text(), "untouched")
        self.assertEqual(list(self.scratch.iterdir()), [self.sentinel])

    def test_stop_interrupts_waiting_for_a_cache_broker_reply(self):
        from benchmarks import profile_copy
        after_stop = []
        def stop(*_):
            signal.raise_signal(signal.SIGTERM)
            after_stop.append(True)
            raise RuntimeError("broker never replied")
        with mock.patch.object(profile_copy.socket, "socket"), mock.patch.object(profile_copy.cache, "send"), \
             mock.patch.object(profile_copy.cache, "receive", side_effect=stop), self.assertRaises(InterruptedError):
            with profile_copy.SignalCancellation() as cancellation:
                profile_copy.cold_cache(1200, cancellation)
        self.assertFalse(after_stop)

    def test_overlapping_roots_in_either_direction_are_rejected_before_writes(self):
        for changes in [("--source", self.scratch), ("--destination-root", self.root),
                        ("--destination-root", self.source / "dir"), ("--output", self.source / "new"),
                        ("--output", self.root), ("--output", self.scratch / "new")]:
            with self.subTest(changes=changes):
                result = self.call(*changes)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("overlap", result.stderr)
                self.assertFalse(self.pid.exists())
                self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()
