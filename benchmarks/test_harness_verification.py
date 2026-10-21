"""Verification policy, interruptions and raw harness evidence regressions."""

import contextlib
import copy
import io
import os
from pathlib import Path
import signal
import tempfile
import unittest
from unittest import mock

from benchmarks import operations, run, streaming
from benchmarks.harness_test_support import run_fixture


class HarnessVerificationTests(unittest.TestCase):
    def test_streaming_only_options_require_explicit_streaming(self):
        for option in (["--verification-mode", "full-content"], ["--verification-mode", "metadata-size"],
                       ["--verification-workers", "8"], ["--verification-workers", "16"], ["--verification-inline-bytes", "0"], ["--check-capacity"]):
            with self.subTest(option=option), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                run._arguments(["--output", "unused", *option])
        args = run._arguments(["--output", "unused", "--streaming-verification"])
        self.assertEqual(args.verification_mode, "full-content")
        self.assertEqual(args.verification_workers, streaming.DEFAULT_WORKERS)

    def test_inline_threshold_and_capacity_options_have_effective_defaults_and_guards(self):
        base = ["--output", "unused", "--streaming-verification"]
        args = run._arguments(base)
        self.assertEqual(args.verification_inline_bytes, streaming.DEFAULT_INLINE_BYTES)
        self.assertEqual(args.capacity_policy, run.capacity.DEFAULT_POLICY)
        args = run._arguments([*base, "--verification-inline-bytes", "0", "--check-capacity",
                               "--capacity-byte-percent", "35", "--capacity-inode-percent", "40",
                               "--capacity-byte-reserve", "17", "--capacity-inode-reserve", "19"])
        self.assertEqual(args.verification_inline_bytes, 0)
        self.assertEqual(args.capacity_policy, dict(byte_percent=35, inode_percent=40,
                                                    byte_reserve=17, inode_reserve=19))
        for option in (["--verification-inline-bytes", "-1"], ["--verification-inline-bytes", "1000000001"],
                       ["--capacity-byte-percent", "20"], ["--capacity-inode-percent", "20"],
                       ["--capacity-byte-reserve", "0"], ["--capacity-inode-reserve", "0"],
                       ["--check-capacity", "--capacity-byte-percent", "-1"]):
            with self.subTest(option=option), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                run._arguments([*base, *option])

    def test_tuning_options_reach_initial_and_per_trial_verification_and_capacity(self):
        policy = dict(byte_percent=35, inode_percent=40, byte_reserve=17, inode_reserve=19)
        original_scan = streaming.scan_tree
        thresholds = []
        def scan(*args, **kwargs):
            thresholds.append(kwargs["inline_bytes"])
            return original_scan(*args, **kwargs)
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(run.capacity, "check_pair", return_value={}) as pair, \
                mock.patch.object(run.capacity, "check", return_value={}) as trial_capacity, \
                mock.patch.object(streaming, "scan_tree", side_effect=scan):
            root = Path(directory)
            record, events, _ = run_fixture(root, extra_args=["--verification-inline-bytes", "0",
                "--check-capacity", "--capacity-byte-percent", "35", "--capacity-inode-percent", "40",
                "--capacity-byte-reserve", "17", "--capacity-inode-reserve", "19"])
            self.assertEqual(record["status"], "complete")
            self.assertEqual(record["context"]["verification"]["inline_bytes"], 0)
            self.assertEqual(record["context"]["capacity_policy"], policy)
            self.assertEqual(thresholds, [0] * 9)
            self.assertEqual(events, ["verify"] + ["drop", "copy", "verify", "verify"] * 4)
            self.assertEqual(pair.call_count, 1)
            self.assertEqual(pair.call_args.kwargs, policy)
            self.assertEqual(trial_capacity.call_count, 4)
            self.assertTrue(all(call.kwargs == policy for call in trial_capacity.call_args_list))

    def test_legacy_source_interruptions_and_read_errors_never_claim_source_changed(self):
        for summary in (False, True):
            for error in (InterruptedError("SIGTERM"), PermissionError("read denied")):
                with self.subTest(summary=summary, error=repr(error)), tempfile.TemporaryDirectory() as directory:
                    def fault(stage, path):
                        if stage == "source":
                            raise error
                    record, events, _ = run_fixture(Path(directory), streaming_verification=False,
                                                    summary=summary, scan_fault=fault)
                    self.assertEqual(record["status"], "failed")
                    self.assertEqual(record["summaries"], [])
                    reason = "interrupted" if isinstance(error, InterruptedError) else "failed"
                    self.assertIn("source verification " + reason, record["error"])
                    self.assertNotIn("source changed", record["error"])
                    if summary:
                        self.assertEqual(events.count("copy"), 1)
                        trial = record["trials"][0]
                        self.assertTrue(trial["validation"]["ok"])
                        self.assertFalse(trial["source_validation"]["ok"])
                        self.assertEqual(trial["source_validation"].get("interrupted", False), reason == "interrupted")

    def test_legacy_cache_source_proof_failure_prevents_copy_and_retains_reason(self):
        for error in (InterruptedError("SIGTERM"), PermissionError("read denied")):
            with self.subTest(error=repr(error)), tempfile.TemporaryDirectory() as directory:
                scans = 0
                def fault(stage, path):
                    nonlocal scans
                    scans += 1
                    if scans == 2:
                        raise error
                record, events, _ = run_fixture(Path(directory), streaming_verification=False,
                                                scan_fault=fault, cache="source-verified")
                self.assertEqual(record["status"], "failed")
                self.assertEqual(events, ["verify", "drop", "verify"])
                self.assertNotIn("source changed", record["error"])
                trial = record["trials"][0]
                self.assertEqual(trial["status"], "failed")
                self.assertFalse(trial["cache_validation"]["ok"])
                self.assertEqual(trial["cache_validation"].get("interrupted", False), isinstance(error, InterruptedError))

    def test_real_sigterm_during_legacy_root_stat_is_recorded_as_interruption(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "file").write_bytes(b"fixture")
            expected = run.scan_tree(source)
            metadata = operations.metadata_tree(source)
            def interrupt(number, frame):
                raise InterruptedError("SIGTERM")
            previous = signal.signal(signal.SIGTERM, interrupt)
            original = Path.stat
            def interrupted_stat(path, *args, **kwargs):
                if path == source:
                    os.kill(os.getpid(), signal.SIGTERM)
                return original(path, *args, **kwargs)
            try:
                with mock.patch.object(Path, "stat", interrupted_stat):
                    proof = operations.validate_source(source, expected, metadata)
            finally:
                signal.signal(signal.SIGTERM, previous)
            self.assertEqual(proof, dict(ok=False, error="SIGTERM", interrupted=True))

    def test_source_metadata_read_interruptions_and_errors_remain_unproven(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "file").write_bytes(b"fixture")
            expected = run.scan_tree(source)
            metadata = operations.metadata_tree(source)
            for error in (InterruptedError("SIGTERM"), PermissionError("metadata denied")):
                with self.subTest(error=repr(error)), mock.patch.object(operations, "metadata_tree", side_effect=error):
                    proof = operations.validate_source(source, expected, metadata)
                    self.assertFalse(proof["ok"])
                    self.assertEqual(proof["digest"], expected["digest"])
                    self.assertEqual(proof.get("interrupted", False), isinstance(error, InterruptedError))
                    self.assertNotIn("source changed", operations.verification_failure(proof, "source"))

    def test_observed_legacy_source_metadata_mismatch_is_distinct_from_scan_error(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            file = source / "file"
            file.write_bytes(b"fixture")
            expected = run.scan_tree(source)
            metadata = operations.metadata_tree(source)
            os.utime(file, ns=(1, 1))
            proof = operations.validate_source(source, expected, metadata)
            self.assertFalse(proof["ok"])
            self.assertIn("metadata differs", proof["error"])
            self.assertNotIn("verification_error", proof)
            self.assertNotIn("interrupted", proof)

    def test_streaming_destination_interruption_stops_before_source_and_next_copy(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            with self.subTest(policy=policy), tempfile.TemporaryDirectory() as directory:
                def fault(stage, path):
                    if stage == "destination":
                        raise InterruptedError("SIGTERM")
                record, events, _ = run_fixture(Path(directory), policy=policy, scan_fault=fault)
                self.assertEqual(events, ["verify", "drop", "copy", "verify"])
                self.assertEqual(record["status"], "failed")
                self.assertEqual(record["summaries"], [])
                trial = record["trials"][0]
                self.assertEqual(trial["status"], "failed")
                self.assertTrue(trial["validation"]["interrupted"])
                self.assertNotIn("source_validation", trial)
                self.assertIn("destination verification interrupted: SIGTERM", record["error"])

    def test_streaming_summary_mismatch_fails_before_source_check(self):
        for policy in (streaming.POLICY, streaming.METADATA_POLICY):
            with self.subTest(policy=policy), tempfile.TemporaryDirectory() as directory:
                record, events, _ = run_fixture(Path(directory), policy=policy,
                    summary_text="files copied: 0\nfiles unchanged: 1\n")
                self.assertEqual(events, ["verify", "drop", "copy", "verify"])
                self.assertEqual(record["status"], "failed")
                self.assertIn("incorrect summary files copied", record["error"])
                self.assertNotIn("source_validation", record["trials"][0])

    def test_mount_source_keeps_fd_name_and_records_kernel_device_evidence(self):
        mountinfo = "42 1 9:127 / /scratch rw,noatime - ext4 /proc/self/fd/8 rw\n"
        records = {"/proc/self/mountinfo": mountinfo, "/sys/dev/block/9:127/uevent": "MAJOR=9\nMINOR=127\nDEVNAME=md127\n"}
        with mock.patch.object(run, "_read", side_effect=lambda path: records.get(path, "")):
            mount = run._mount("/scratch/source")
        self.assertEqual(mount["mount_source"], "/proc/self/fd/8")
        self.assertEqual(mount["kernel_device"], "9:127")
        self.assertEqual(mount["kernel_device_name"], "/dev/md127")
        from benchmarks.test_storage import StorageIdentityTests
        helper = StorageIdentityTests()
        helper.setUp()
        args = (helper.case, helper.variant, "source-warm", "local", "runner")
        original = run.series_id(*args, helper.environment, {})
        observed = copy.deepcopy(helper.environment)
        observed["filesystem"]["source"].update(kernel_device="9:127", kernel_device_name="/dev/md127")
        self.assertEqual(original, run.series_id(*args, observed, {}))


if __name__ == "__main__":
    unittest.main()
