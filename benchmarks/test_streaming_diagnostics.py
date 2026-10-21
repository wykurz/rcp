"""Bounded verifier evidence and scheduling use small filesystem fixtures."""

from concurrent.futures import ThreadPoolExecutor
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

from benchmarks import streaming, verification


class StreamingDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "source"
        (self.source / "nested").mkdir(parents=True)
        (self.source / "nested/file").write_bytes(b"fixture")
        self.expected = streaming.scan_tree(self.source)
        self.destination = self.root / "destination"
        shutil.copytree(self.source, self.destination)

    def validate(self, expected=None):
        proof = streaming.validate_tree(self.destination, expected or self.expected)
        self.assertFalse(proof["ok"])
        self.assertLess(len(json.dumps(proof)), 1200)
        for key in ("digest", "modes_digest", "metadata_digest"):
            self.assertRegex(proof[key], r"^[0-9a-f]{64}$")
        return proof

    def test_mode_and_content_failures_keep_distinct_comparisons_and_complete_counts(self):
        file = self.destination / "nested/file"
        file.chmod(0o600)
        proof = self.validate()
        self.assertEqual(proof["error"], "fixture modes differ")
        self.assertTrue(proof["digest_match"])
        self.assertFalse(proof["modes_match"])
        self.assertTrue(proof["complete"])
        self.assertEqual(proof["counts"], self.expected["counts"])
        file.chmod(self.source.joinpath("nested/file").stat().st_mode & 0o777)
        file.write_bytes(b"changed")
        proof = self.validate()
        self.assertEqual(proof["error"], "fixture paths or contents differ")
        self.assertFalse(proof["digest_match"])
        self.assertTrue(proof["modes_match"])

    def test_missing_truncated_and_grown_entries_keep_observed_bounded_counts(self):
        file = self.destination / "nested/file"
        file.unlink()
        proof = self.validate()
        self.assertEqual(proof["counts"], dict(files=0, directories=1, bytes=0))
        self.assertTrue(proof["complete"])
        file.write_bytes(b"x")
        proof = self.validate()
        self.assertEqual(proof["counts"], dict(files=1, directories=1, bytes=1))
        self.assertTrue(proof["complete"])
        file.write_bytes(b"too-large")
        proof = self.validate()
        self.assertEqual(proof["counts"], dict(files=1, directories=1, bytes=9))
        self.assertFalse(proof["complete"])
        self.assertNotIn("path", proof)
        self.assertNotIn("verification_error", proof)

    def test_extra_early_entry_does_not_blame_later_unchanged_entry(self):
        for parent in ("a", "z"):
            (self.source / parent).mkdir()
            for name in ("1", "2", "3"):
                (self.source / parent / name).write_bytes(b"x")
        expected = streaming.scan_tree(self.source, policy=streaming.METADATA_POLICY)
        destination = self.root / "extra-tree"
        shutil.copytree(self.source, destination)
        (destination / "a/0").write_bytes(b"x")
        proof = streaming.validate_tree(destination, expected)
        self.assertFalse(proof["ok"])
        self.assertFalse(proof["complete"])
        self.assertNotIn("path", proof)
        self.assertTrue(proof["error"].startswith("fixture exceeded bounded"), proof)
        self.assertNotIn("verification_error", proof)
        self.assertEqual(proof["counts"]["files"], expected["counts"]["files"] + 1)

    def test_observed_root_defects_are_mismatches_and_io_failures_are_errors(self):
        for kind in ("missing", "file", "symlink", "mode"):
            with self.subTest(kind=kind):
                path = self.root / kind
                if kind == "file":
                    path.write_bytes(b"file")
                elif kind == "symlink":
                    path.symlink_to(self.source)
                elif kind == "mode":
                    path.mkdir()
                    path.chmod(0o200)
                    self.addCleanup(path.chmod, 0o700)
                proof = streaming.validate_tree(path, self.expected)
                self.assertFalse(proof["ok"], proof)
                self.assertNotIn("verification_error", proof)
        with mock.patch.object(streaming.os, "read", side_effect=OSError(errno.EIO, "injected read error")):
            proof = streaming.validate_tree(self.source, self.expected, source=True)
        self.assertTrue(proof["verification_error"])
        self.assertFalse(proof["complete"])

    def test_permission_diagnostic_does_not_swallow_interruption(self):
        error = PermissionError(errno.EACCES, "denied")
        with mock.patch.object(verification.os, "stat", side_effect=InterruptedError("SIGTERM")), \
                self.assertRaisesRegex(InterruptedError, "SIGTERM"):
            verification.observed_error(error, self.source)

    def test_thread_start_failure_is_bounded_verification_error_evidence(self):
        with mock.patch.object(ThreadPoolExecutor, "_adjust_thread_count", side_effect=RuntimeError("can't start new thread")):
            proof = streaming.validate_tree(self.source, self.expected, source=True, inline_bytes=0)
        self.assertFalse(proof["ok"])
        self.assertTrue(proof["verification_error"])
        self.assertFalse(proof["complete"])
        self.assertIn("can't start new thread", proof["error"])
        self.assertLess(len(json.dumps(proof)), 1200)

    def test_default_sends_decimal_megabyte_to_pool_and_keeps_tiny_inline(self):
        large = self.source / "megabyte"
        large.write_bytes(b"f" * 1_000_000)
        submitted = []
        original = ThreadPoolExecutor.submit
        def submit(pool, function, parent, name, relative):
            submitted.append(relative)
            return original(pool, function, parent, name, relative)
        with mock.patch.object(ThreadPoolExecutor, "submit", submit):
            streaming.scan_tree(self.source)
        self.assertEqual(submitted, ["megabyte"])

    def test_cached_record_encoder_keeps_exact_historical_bytes(self):
        for row in (["unicode-\u2603", "file", 7, "hash"], [".", 493, 1000, 1000, 1, 2, 3, 4]):
            expected = hashlib.sha256()
            data = json.dumps(row, ensure_ascii=True, separators=(",", ":")).encode("ascii")
            expected.update(len(data).to_bytes(8, "big"))
            expected.update(data)
            actual = hashlib.sha256()
            streaming._feed(actual, row)
            self.assertEqual(actual.digest(), expected.digest())

    def test_read_and_batch_tuning_preserve_fingerprints_and_validate_bounds(self):
        for index in range(21):
            (self.source / f"{index:02}").write_bytes(bytes(range(256)) * 32)
        expected = streaming.scan_tree(self.source)
        for read_bytes, batch_depth in ((4096, 1), (8192, 3), (16777216, 8)):
            self.assertEqual(streaming.scan_tree(self.source, workers=2, inline_bytes=0,
                            read_bytes=read_bytes, batch_depth=batch_depth), expected)
        with mock.patch.object(streaming.os, "open", side_effect=AssertionError("opened fixture")):
            for name, values in (("read_bytes", (0, True, 4095, 16777217)),
                                 ("batch_depth", (0, True, 1.5, 9))):
                for value in values:
                    with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                        streaming.scan_tree(self.source, **{name: value})

    def test_walk_errors_include_relative_path_and_partial_evidence(self):
        file = self.destination / "nested/file"
        file.unlink()
        file.symlink_to("/not-followed")
        proof = self.validate()
        self.assertEqual(proof["path"], "nested/file")
        self.assertIn("nested/file: unexpected symlink", proof["error"])
        self.assertFalse(proof["complete"])
        self.assertEqual(proof["counts"], dict(files=0, directories=1, bytes=0))

    def test_io_errors_retain_relative_path_and_do_not_claim_observed_mismatch(self):
        with mock.patch.object(streaming, "_file", side_effect=PermissionError("read denied")):
            proof = self.validate()
        self.assertEqual(proof["path"], "nested/file")
        self.assertTrue(proof["verification_error"])
        self.assertFalse(proof["complete"])
        self.assertIn("read denied", proof["error"])

    def test_metadata_and_single_chunk_files_do_not_submit_per_file_futures(self):
        with mock.patch.object(ThreadPoolExecutor, "submit", side_effect=AssertionError("submitted per-file future")):
            self.assertEqual(streaming.scan_tree(self.source), self.expected)
            with mock.patch.object(streaming.os, "fdopen", side_effect=AssertionError("payload read")):
                proof = streaming.scan_tree(self.source, policy=streaming.METADATA_POLICY)
                self.assertTrue(streaming.validate_tree(self.source, proof, source=True)["ok"])

    def test_large_file_workers_and_inline_scans_have_identical_ordered_fingerprints(self):
        for name in ("a", "z"):
            (self.source / name).write_bytes(b"another")
        expected = streaming.scan_tree(self.source)
        original = ThreadPoolExecutor.submit
        submitted = []
        def submit(pool, *args, **kwargs):
            submitted.append(1)
            return original(pool, *args, **kwargs)
        with mock.patch.object(streaming, "READ_SIZE", 1), \
                mock.patch.object(ThreadPoolExecutor, "submit", submit):
            for workers in (1, 2, 8):
                self.assertEqual(streaming.scan_tree(self.source, workers=workers, inline_bytes=0), expected)
        self.assertEqual(len(submitted), 6)

    def test_inline_threshold_is_tunable_and_rejects_invalid_values_before_open(self):
        with mock.patch.object(ThreadPoolExecutor, "submit", side_effect=AssertionError("submitted future")):
            self.assertEqual(streaming.scan_tree(self.source, inline_bytes=7), self.expected)
            self.assertEqual(streaming.scan_tree(self.source, workers=1, inline_bytes=0), self.expected)
        with mock.patch.object(streaming.os, "open", side_effect=AssertionError("opened fixture")):
            for value in (-1, True, 1.5, streaming.MAX_INLINE_BYTES + 1):
                with self.subTest(value=value), self.assertRaisesRegex(ValueError, "inline bytes"):
                    streaming.scan_tree(self.source, inline_bytes=value)


if __name__ == "__main__":
    unittest.main()
