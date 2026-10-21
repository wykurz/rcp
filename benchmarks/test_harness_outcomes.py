"""Real signals and typed stage failures preserve completed harness evidence."""

import contextlib
import errno
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from benchmarks import operations, run, streaming
from benchmarks.harness_test_support import run_fixture


@contextlib.contextmanager
def raising_sigterm():
    def interrupt(number, frame):
        raise InterruptedError("SIGTERM")
    previous = signal.signal(signal.SIGTERM, interrupt)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


class HarnessOutcomeTests(unittest.TestCase):
    def test_sigterm_at_legacy_enumeration_and_comparison_boundaries_is_never_a_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "file").write_bytes(b"fixture")
            expected = run.scan_tree(source)
            original = os.scandir
            def interrupted_scan(path):
                os.kill(os.getpid(), signal.SIGTERM)
                return original(path)
            with raising_sigterm(), mock.patch.object(run.os, "scandir", interrupted_scan):
                proof = run.validate_tree(source, source, expected)
            self.assertTrue(proof["interrupted"])
            self.assertNotIn("missing=", proof["error"])
            class InterruptedEntries(dict):
                def keys(self):
                    os.kill(os.getpid(), signal.SIGTERM)
                    return super().keys()
            with raising_sigterm():
                proof = run.validate_tree(source, source, dict(expected, entries=InterruptedEntries(expected["entries"])))
            self.assertTrue(proof["interrupted"])
            self.assertFalse(proof["ok"])

    def test_sigterm_in_source_warm_enumeration_prevents_success(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "file").write_bytes(b"fixture")
            original = os.scandir
            def interrupted_scan(path):
                os.kill(os.getpid(), signal.SIGTERM)
                return original(path)
            with raising_sigterm(), mock.patch.object(run.subprocess, "run"), \
                    mock.patch.object(run.os, "scandir", interrupted_scan), self.assertRaises(InterruptedError):
                run._prepare_cache("source-warm", source, 1)

    def test_sigint_in_legacy_verification_chains_previous_handler_and_keeps_keyboard_interrupt(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "file").write_bytes(b"fixture")
            expected = run.scan_tree(source)
            original = os.scandir
            called = []
            def prior(number, frame):
                called.append(number)
                raise KeyboardInterrupt()
            def interrupted_scan(path):
                os.kill(os.getpid(), signal.SIGINT)
                return original(path)
            previous = signal.signal(signal.SIGINT, prior)
            try:
                with mock.patch.object(run.os, "scandir", interrupted_scan), self.assertRaises(KeyboardInterrupt):
                    run.validate_tree(source, source, expected)
                self.assertEqual(called, [signal.SIGINT])
            finally:
                signal.signal(signal.SIGINT, previous)

    def test_observed_legacy_defects_differ_from_observation_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            (source / "file").write_bytes(b"fixture")
            expected = run.scan_tree(source)
            for defect in ("missing", "not-directory", "symlink", "fifo"):
                path = root / defect
                if defect == "not-directory":
                    path.write_bytes(b"x")
                elif defect != "missing":
                    path.mkdir()
                    if defect == "symlink":
                        (path / "file").symlink_to(source / "file")
                    else:
                        os.mkfifo(path / "file")
                proof = run.validate_tree(source, path, expected)
                self.assertFalse(proof["ok"])
                self.assertNotIn("verification_error", proof)
                self.assertNotIn("interrupted", proof)
            with mock.patch.object(run.os, "scandir", side_effect=OSError(errno.EIO, "device I/O")):
                self.assertTrue(run.validate_tree(source, source, expected)["verification_error"])
            unreadable = source / "file"
            original_open = Path.open
            def denied(path, *args, **kwargs):
                if path == unreadable:
                    raise PermissionError(errno.EACCES, "read denied", str(path))
                return original_open(path, *args, **kwargs)
            mode = unreadable.stat().st_mode
            unreadable.chmod(0o200)
            try:
                with mock.patch.object(Path, "open", denied):
                    proof = run.validate_tree(source, source, expected)
                self.assertFalse(proof["ok"])
                self.assertNotIn("verification_error", proof)
            finally:
                unreadable.chmod(mode)
            metadata = operations.metadata_tree(source)
            os.link(source / "file", root / "outside-link")
            proof = operations.validate_source(source, expected, metadata)
            self.assertFalse(proof["ok"])
            self.assertIn("hardlink", proof["error"])
            self.assertNotIn("verification_error", proof)

    def test_summary_failure_preserves_completed_content_and_observed_counters(self):
        for streaming_verification in (False, True):
            with self.subTest(streaming=streaming_verification), tempfile.TemporaryDirectory() as directory:
                record, events, _ = run_fixture(Path(directory), streaming_verification=streaming_verification,
                    summary_text="files copied: 0\nfiles unchanged: 1\n")
                trial = record["trials"][0]
                self.assertEqual(trial["status"], "failed")
                self.assertTrue(trial["validation"]["ok"])
                self.assertFalse(trial["summary_validation"]["ok"])
                self.assertEqual(trial["copy_summary"], dict(files_copied=0, files_unchanged=1))
                self.assertEqual(trial["failure"]["stage"], "summary")
                self.assertIn("incorrect summary", trial["error"])
                self.assertNotIn("source_validation", trial)
                self.assertEqual(events.count("copy"), 1)

    def test_metadata_failure_preserves_content_and_summary_and_is_typed(self):
        for error in (InterruptedError("SIGTERM"), OSError(errno.EIO, "device I/O"),
                      operations._MetadataMismatch("metadata differs")):
            with self.subTest(error=repr(error)), tempfile.TemporaryDirectory() as directory, \
                    mock.patch.object(operations, "validate_metadata", side_effect=error):
                record, events, _ = run_fixture(Path(directory), streaming_verification=False)
                trial = record["trials"][0]
                self.assertTrue(trial["validation"]["ok"])
                self.assertTrue(trial["summary_validation"]["ok"])
                self.assertEqual(trial["copy_summary"], trial["expected_transfer"])
                self.assertFalse(trial["metadata_validation"]["ok"])
                self.assertEqual(trial["metadata_validation"].get("interrupted", False), isinstance(error, InterruptedError))
                self.assertEqual(trial["metadata_validation"].get("verification_error", False), isinstance(error, OSError) and not isinstance(error, InterruptedError))
                self.assertEqual(trial["failure"]["stage"], "metadata")
                self.assertEqual(events.count("copy"), 1)

    def test_cache_and_capacity_cancellation_have_typed_failed_stage(self):
        for error in (InterruptedError("SIGTERM"), KeyboardInterrupt()):
            def interrupt():
                raise error
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as directory:
                record, events, _ = run_fixture(Path(directory), cache_fault=interrupt)
                trial = record["trials"][0]
                self.assertTrue(trial["interrupted"])
                self.assertTrue(trial["cache_validation"]["interrupted"])
                self.assertEqual(trial["failure"]["category"], "cancellation")
                self.assertNotIn("copy", events)
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(run.capacity, "check_pair", return_value={}), \
                mock.patch.object(run.capacity, "check", side_effect=ValueError("capacity refused")):
            record, events, _ = run_fixture(Path(directory), extra_args=["--check-capacity"])
            trial = record["trials"][0]
            self.assertEqual(trial["failure"]["stage"], "capacity")
            self.assertFalse(trial["validation"]["complete"])
            self.assertNotIn("interrupted", trial)
            self.assertNotIn("copy", events)

    def test_copy_cancellation_reaps_child_and_retains_exit_codes_and_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            original = subprocess.Popen
            children = []
            def launched(*args, **kwargs):
                child = original(*args, **kwargs)
                children.append(child)
                os.kill(os.getpid(), signal.SIGTERM)
                return child
            trial = dict(status="running", validation=dict(ok=False), exit_codes=[], logs=[])
            with raising_sigterm(), mock.patch.object(run.subprocess, "Popen", launched), self.assertRaises(InterruptedError):
                with run._TrialOutcome(trial) as active:
                    active.stage = "command"
                    run.execute_commands([[sys.executable, "-c", "import time; time.sleep(30)"]],
                                         Path(directory) / "logs", 5)
            self.assertEqual(len(children), 1)
            self.assertIsNotNone(children[0].poll())
            self.assertEqual(trial["exit_codes"], [children[0].returncode])
            self.assertEqual(len(trial["logs"]), 1)
            self.assertTrue(trial["interrupted"])
            self.assertTrue(trial["validation"]["interrupted"])
            self.assertEqual(trial["failure"]["kind"], "interrupted")
            self.assertEqual(trial["failure"]["stage"], "command")

    def test_sigint_stage_retains_partial_scan_evidence_and_completed_destination(self):
        partial = dict(ok=False, interrupted=True, complete=False, error="KeyboardInterrupt",
                       counts=dict(files=1, directories=0, bytes=7))
        error = KeyboardInterrupt()
        error.proof = partial
        destination = dict(ok=True, digest="completed")
        trial = dict(status="running", validation=destination)
        with self.assertRaises(KeyboardInterrupt):
            with run._TrialOutcome(trial) as active:
                active.stage = "source"
                raise error
        self.assertEqual(trial["source_validation"], partial)
        self.assertIs(trial["validation"], destination)
        self.assertEqual(trial["status"], "failed")

    def test_system_metadata_decoding_and_historical_fixture_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "uevent"
            path.write_bytes(b"DEVNAME=md127\nPARTNAME=bad-\xff\n")
            self.assertIn("DEVNAME=md127", run._read(path))
        self.assertTrue(run._fixture_policy().startswith("filegen --leaf-files --bufsize="))


if __name__ == "__main__":
    unittest.main()
