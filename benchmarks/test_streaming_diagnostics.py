"""Bounded verifier evidence and scheduling use small filesystem fixtures."""

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

from benchmarks import streaming


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

    def test_missing_truncated_and_extra_entries_keep_observed_bounded_counts(self):
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
        self.assertEqual(proof["path"], "nested/file")
        self.assertNotIn("verification_error", proof)

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
