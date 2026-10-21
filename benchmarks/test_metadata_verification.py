"""Metadata-only verification uses tiny fixtures and mocked copy commands."""

import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from benchmarks import capacity, report, run, streaming
from benchmarks.harness_test_support import run_fixture


class MetadataVerificationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.source = self.root / "source"
        (self.source / "directory").mkdir(parents=True)
        (self.source / "directory/file").write_bytes(b"fixture-content")
        (self.source / "empty").write_bytes(b"")
        (self.source / "directory/file").chmod(0o644)

    def scan(self, root=None, **kwargs):
        return streaming.scan_tree(root or self.source, policy=streaming.METADATA_POLICY, **kwargs)

    def test_metadata_initial_destination_and_source_scans_never_read_payload(self):
        destination = self.root / "destination"
        shutil.copytree(self.source, destination)
        with mock.patch.object(streaming.os, "fdopen", side_effect=AssertionError("payload stream opened")), \
                mock.patch.object(streaming.os, "read", side_effect=AssertionError("payload read")):
            expected = self.scan(workers=1)
            for workers in (2, 8, 32):
                self.assertEqual(self.scan(workers=workers), expected)
            for target, source in ((destination, False), (self.source, True)):
                proof = streaming.validate_tree(target, expected, source=source)
                self.assertTrue(proof["ok"])
                self.assertEqual(proof["policy"], streaming.METADATA_POLICY)
                self.assertEqual(proof["counts"], dict(files=2, directories=1, bytes=15))
                self.assertEqual(proof["metadata"]["entries"], 4)
        self.assertLess(len(json.dumps(expected)), 500)
        full = streaming.scan_tree(self.source)
        self.assertEqual(full["policy"], streaming.POLICY)
        self.assertNotEqual(full["digest"], expected["digest"])
        self.assertNotIn("policy", streaming.validate_tree(self.source, full))

    def test_same_size_content_corruption_is_deliberately_not_content_proof(self):
        expected = self.scan()
        full = streaming.scan_tree(self.source)
        destination = self.root / "destination"
        shutil.copytree(self.source, destination)
        file = destination / "directory/file"
        before = file.stat()
        file.write_bytes(b"damaged-content")
        self.assertTrue(streaming.validate_tree(destination, expected)["ok"])
        self.assertFalse(streaming.validate_tree(destination, full)["ok"])
        os.utime(file, ns=(before.st_atime_ns, before.st_mtime_ns))
        self.assertTrue(streaming.validate_tree(destination, expected)["ok"])
        file = self.source / "directory/file"
        before = file.stat()
        file.write_bytes(b"damaged-content")
        os.utime(file, ns=(before.st_atime_ns, before.st_mtime_ns))
        self.assertEqual(file.stat().st_ino, before.st_ino)
        self.assertNotEqual(file.stat().st_ctime_ns, before.st_ctime_ns)
        self.assertTrue(streaming.validate_tree(self.source, expected)["ok"])
        proof = streaming.validate_tree(self.source, expected, source=True)
        self.assertFalse(proof["ok"])
        self.assertEqual(proof["error"], "source metadata changed")
        self.assertTrue(proof["digest_match"])
        self.assertFalse(proof["source_metadata_match"])

    def test_incomplete_or_wrong_structure_and_modes_fail_metadata_validation(self):
        expected = self.scan()
        for damage in ("missing", "extra", "truncated", "rename", "type", "mode", "directory-mode"):
            with self.subTest(damage=damage):
                destination = self.root / damage
                shutil.copytree(self.source, destination)
                source_expected = self.scan(destination)
                self.assertTrue(streaming.validate_tree(destination, source_expected, source=True)["ok"])
                file = destination / "directory/file"
                if damage == "missing":
                    file.unlink()
                elif damage == "extra":
                    (destination / "extra").write_bytes(b"")
                elif damage == "truncated":
                    file.write_bytes(b"incomplete")
                elif damage == "rename":
                    file.rename(file.with_name("other"))
                elif damage == "type":
                    file.unlink()
                    file.mkdir()
                elif damage == "mode":
                    file.chmod(0o600)
                else:
                    file.parent.chmod(0o700)
                proof = streaming.validate_tree(destination, expected)
                source_proof = streaming.validate_tree(destination, source_expected, source=True)
                self.assertFalse(proof["ok"])
                self.assertFalse(source_proof["ok"])
                self.assertNotIn("verification_error", proof)
                self.assertNotIn("verification_error", source_proof)
                self.assertEqual(proof["error"], source_proof["error"])
                if damage in ("mode", "directory-mode"):
                    self.assertTrue(source_proof["digest_match"])
                    self.assertFalse(source_proof["modes_match"])
                    self.assertFalse(source_proof["source_metadata_match"])
                elif damage == "rename":
                    self.assertFalse(source_proof["digest_match"])
                    self.assertFalse(source_proof["source_metadata_match"])

    def test_source_timestamp_is_revalidated_without_requiring_destination_timestamp(self):
        expected = self.scan()
        file = self.source / "directory/file"
        os.utime(file, ns=(1, 1))
        self.assertTrue(streaming.validate_tree(self.source, expected)["ok"])
        proof = streaming.validate_tree(self.source, expected, source=True)
        self.assertFalse(proof["ok"])
        self.assertEqual(proof["error"], "source metadata changed")
        self.assertEqual(proof["counts"], expected["counts"])
        self.assertEqual(proof["digest"], expected["digest"])
        self.assertTrue(proof["complete"])
        self.assertTrue(proof["digest_match"])
        self.assertTrue(proof["modes_match"])
        self.assertFalse(proof["source_metadata_match"])

    def test_metadata_retains_link_special_file_owner_acl_and_capacity_guards(self):
        for kind in ("symlink", "hardlink", "fifo"):
            with self.subTest(kind=kind):
                path = self.source / "invalid"
                if kind == "symlink":
                    path.symlink_to(self.source / "directory/file")
                elif kind == "hardlink":
                    os.link(self.source / "directory/file", path)
                else:
                    os.mkfifo(path)
                try:
                    with self.assertRaises(ValueError):
                        self.scan()
                finally:
                    path.unlink()
        for method in ("geteuid", "getegid"):
            foreign = getattr(os, method)() + 1
            with mock.patch.object(streaming.os, method, return_value=foreign):
                with self.assertRaisesRegex(ValueError, "foreign owner/group"):
                    self.scan()
        with mock.patch.object(streaming.os, "listxattr", return_value=["system.posix_acl_access"]):
            with self.assertRaisesRegex(ValueError, "ACL"):
                self.scan()
        with mock.patch.object(capacity, "MAX_FILE_BYTES", 14):
            with self.assertRaisesRegex(ValueError, "bounded"):
                self.scan()

    def test_observed_source_guards_are_mismatches_not_verifier_errors(self):
        expected = self.scan()
        for kind in ("symlink", "hardlink", "fifo", "special-mode", "foreign-owner", "acl"):
            with self.subTest(kind=kind):
                target = self.source / "directory/file"
                path = self.source / "invalid"
                with contextlib.ExitStack() as stack:
                    if kind == "symlink":
                        path.symlink_to(target)
                        stack.callback(path.unlink)
                    elif kind == "hardlink":
                        os.link(target, path)
                        stack.callback(path.unlink)
                    elif kind == "fifo":
                        os.mkfifo(path)
                        stack.callback(path.unlink)
                    elif kind == "special-mode":
                        target.chmod(0o2644)
                        stack.callback(target.chmod, 0o644)
                    elif kind == "foreign-owner":
                        stack.enter_context(mock.patch.object(streaming.os, "geteuid", return_value=os.geteuid() + 1))
                    else:
                        stack.enter_context(mock.patch.object(streaming.os, "listxattr", return_value=["system.posix_acl_access"]))
                    proof = streaming.validate_tree(self.source, expected, source=True)
                    self.assertFalse(proof["ok"], proof)
                    self.assertFalse(proof["complete"], proof)
                    self.assertNotIn("verification_error", proof)

    def test_metadata_checks_stability_on_the_same_no_follow_descriptor(self):
        original = os.fstat
        calls = 0
        def changing(fd):
            nonlocal calls
            calls += 1
            value = original(fd)
            if calls == 2:
                fields = {name: getattr(value, name) for name in dir(value) if name.startswith("st_")}
                return SimpleNamespace(**dict(fields, st_mtime_ns=value.st_mtime_ns + 1))
            return value
        parent = os.open(self.source / "directory", os.O_RDONLY | os.O_DIRECTORY)
        try:
            with mock.patch.object(streaming.os, "open", wraps=os.open) as opened, \
                    mock.patch.object(streaming.os, "fstat", side_effect=changing), \
                    self.assertRaisesRegex(ValueError, "changed"):
                streaming._file(parent, "file", streaming.METADATA_POLICY)
            opened.assert_called_once_with("file", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        finally:
            os.close(parent)

    def test_invalid_policy_and_worker_bounds_fail_before_opening(self):
        expected = self.scan()
        with mock.patch.object(streaming.os, "open", side_effect=AssertionError("opened fixture")):
            for policy in (None, "", "unknown"):
                with self.subTest(policy=policy):
                    with self.assertRaisesRegex(ValueError, "policy"):
                        streaming.scan_tree(self.source, policy=policy)
                    self.assertEqual(streaming.validate_tree(self.source, dict(expected, policy=policy)),
                        dict(ok=False, error="unsupported streaming verification policy", verification_error=True))
            for workers in (0, 33, True):
                with self.assertRaisesRegex(ValueError, "workers"):
                    self.scan(workers=workers)

    def test_explicit_metadata_cli_and_policy_preserve_full_content_default_identity(self):
        args = run._arguments(["--output", "unused"])
        self.assertEqual(args.verification_mode, "full-content")
        self.assertEqual(args.verification_workers, 8)
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            run._arguments(["--output", "unused", "--verification-mode", "metadata-size"])
        args = run._arguments(["--output", "unused", "--streaming-verification", "--verification-mode", "metadata-size"])
        self.assertEqual(args.verification_mode, "metadata-size")
        from benchmarks.test_storage import StorageIdentityTests
        helper = StorageIdentityTests()
        helper.setUp()
        identity = (helper.case, helper.variant, "source-warm", "local", "runner", helper.environment, {})
        policies = (None, dict(policy=streaming.POLICY, workers=8), dict(policy=streaming.METADATA_POLICY, workers=8, inline_bytes=streaming.DEFAULT_INLINE_BYTES))
        self.assertEqual(len({run.series_id(*identity, verification=policy) for policy in policies}), 3)


class MetadataIntegrationTests(unittest.TestCase):
    def _run(self, fault=None, outcome=None):
        with tempfile.TemporaryDirectory() as directory:
            record, events, summary = run_fixture(Path(directory), policy=streaming.METADATA_POLICY,
                                                  copy_fault=fault, outcome=outcome)
        self.assertIn("content integrity not checked", summary)
        return record, events

    def test_four_metadata_trials_keep_cache_boundary_and_disclose_validation_strength(self):
        record, events = self._run()
        self.assertEqual(record["status"], "complete")
        self.assertEqual(events, ["verify"] + ["drop", "copy", "verify", "verify"] * 4)
        self.assertEqual(record["context"]["verification"], dict(policy=streaming.METADATA_POLICY, workers=8,
                         inline_bytes=streaming.DEFAULT_INLINE_BYTES, read_bytes=streaming.DEFAULT_READ_SIZE,
                         batch_depth=streaming.DEFAULT_BATCH_MULTIPLIER))
        self.assertIn("same-size content corruption is not detected", record["context"]["verification_policy"])
        self.assertIn("payload contents are not checked", record["context"]["fixture_policy"])
        for trial in record["trials"]:
            for key in ("validation", "source_validation"):
                self.assertTrue(trial[key]["ok"])
                self.assertEqual(trial[key]["policy"], streaming.METADATA_POLICY)

    def test_successful_exit_does_not_certify_truncated_destination_or_changed_source(self):
        for side in ("source", "destination"):
            with self.subTest(side=side):
                def damage(source, destination):
                    target = source if side == "source" else destination
                    next(path for path in target.rglob("*") if path.is_file()).write_bytes(b"")
                record, events = self._run(fault=damage)
                self.assertEqual(record["status"], "failed")
                self.assertEqual(record["summaries"], [])
                self.assertEqual(events.count("copy"), 1)
                self.assertEqual(record["trials"][0]["exit_codes"], [0])
                self.assertEqual(record["trials"][0]["validation"]["ok"], side == "source")
                if side == "source":
                    self.assertFalse(record["trials"][0]["source_validation"]["ok"])

    def test_copy_failure_and_timeout_cannot_pass_even_with_complete_destination(self):
        for exit_code, timed_out in ((1, False), (0, True)):
            with self.subTest(exit_code=exit_code, timed_out=timed_out):
                record, events = self._run(outcome=dict(ok=False, exit_codes=[exit_code], timed_out=timed_out))
                self.assertEqual(record["status"], "failed")
                self.assertEqual(events, ["verify", "drop", "copy"])
                self.assertFalse(record["trials"][0]["validation"]["ok"])
                self.assertEqual(record["trials"][0]["validation"]["error"], "command failed or timed out")


if __name__ == "__main__":
    unittest.main()
