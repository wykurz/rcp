"""Large contracts use arithmetic, mocked capacity and small real filesystem fixtures."""

import contextlib
import copy
import io
import json
import os
from pathlib import Path
import shutil
import signal
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from benchmarks import capacity, report, run, streaming
from benchmarks.test_pair_boundaries import paired_record


class CapacityTests(unittest.TestCase):
    def setUp(self):
        self.cases = run.load_manifest(Path(__file__).with_name("large-cases.json"))["cases"]

    def test_decimal_shapes_and_block_rounding_match_historical_layouts(self):
        expected = [
            (1000, 10, 1_000_000_000_000, 1011, 1_000_001_536_000),
            (10240, 10, 1_024_000_000_000, 10251, 1_024_039_321_600),
            (1024000, 1110, 1_024_000_000_000, 1025111, 1_027_604_480_000),
            (10240000, 11110, 10_240_000_000, 10251111, 41_943_040_000),
        ]
        for case, (files, directories, logical, inodes, blocks) in zip(self.cases, expected):
            with self.subTest(case=case["id"]):
                self.assertEqual(run.expected_counts(case), dict(files=files, directories=directories, bytes=logical))
                need = capacity.requirements(case, 4096)
                self.assertEqual(need["fixture_inodes"], inodes)
                self.assertEqual(need["data_block_bytes"], blocks)
                self.assertEqual(need["logical_bytes"], logical)
                self.assertGreater(need["required_inodes"], inodes * 1.2)
                self.assertGreater(need["required_bytes"], blocks * 1.2)
                self.assertLess(need["required_inodes"], 16_000_000)
                self.assertLess(need["required_bytes"], 1_600_000_000_000)

    def test_actual_free_blocks_and_inodes_are_required_on_each_side(self):
        case = self.cases[-1]
        need = capacity.requirements(case, 4096)
        blocks = (need["required_bytes"] + 4095) // 4096
        with tempfile.TemporaryDirectory() as directory:
            for available_blocks, available_inodes, accepted in (
                    (blocks, need["required_inodes"], True),
                    (blocks - 1, need["required_inodes"], False),
                    (blocks, need["required_inodes"] - 1, False)):
                space = SimpleNamespace(f_frsize=4096, f_bavail=available_blocks, f_favail=available_inodes,
                                        f_blocks=blocks, f_files=need["required_inodes"])
                with mock.patch.object(capacity.os, "fstatvfs", return_value=space):
                    if accepted:
                        self.assertEqual(capacity.check(directory, case)["available_inodes"], available_inodes)
                    else:
                        with self.assertRaisesRegex(ValueError, "insufficient"):
                            capacity.check(directory, case)
            for changed in (dict(f_favail=-1), dict(f_favail=2**64 - 1), dict(f_bavail=blocks + 1),
                            dict(f_favail=need["required_inodes"] + 1), dict(f_blocks=True)):
                space = SimpleNamespace(**dict(f_frsize=4096, f_blocks=blocks, f_bavail=blocks,
                    f_files=need["required_inodes"], f_favail=need["required_inodes"]) | changed)
                with mock.patch.object(capacity.os, "fstatvfs", return_value=space):
                    with self.assertRaisesRegex(ValueError, "invalid|unknown"):
                        capacity.check(directory, case)

    def test_aliases_symlinks_and_unsupported_contracts_fail_closed(self):
        with mock.patch.object(capacity, "check", return_value=dict(device=7)):
            with self.assertRaisesRegex(ValueError, "distinct"):
                capacity.check_pair("/source", "/destination", self.cases[0])
        with tempfile.TemporaryDirectory() as directory:
            link = Path(directory) / "link"
            link.symlink_to(directory)
            with self.assertRaises(OSError):
                capacity.check(link, self.cases[0])
        for change in (dict(file_size_bytes=1_000_000_001), dict(mode="partial"),
                       dict(files_per_leaf=1_000_000_000), dict(directory_widths=[20000])):
            with self.subTest(change=change), self.assertRaises(ValueError):
                capacity.requirements(dict(self.cases[0], **change), 4096)
        for block in (0, 1000, 131072, True):
            with self.assertRaises(ValueError):
                capacity.requirements(self.cases[0], block)


class StreamingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        for leaf in ("a", "b", "c"):
            target = self.source / leaf
            target.mkdir()
            for index in range(37):
                (target / str(index)).write_bytes(bytes([index, 17, 255]) * 7)

    def test_digest_order_is_independent_of_worker_count_and_proof_is_compact(self):
        proof = streaming.scan_tree(self.source, 1)
        self.assertEqual(proof["counts"], dict(files=111, directories=3, bytes=2331))
        self.assertEqual(proof["entries"], 115)
        self.assertLess(len(json.dumps(proof)), 500)
        for workers in (2, 8, 32):
            self.assertEqual(streaming.scan_tree(self.source, workers), proof)
        copy_path = self.root / "copy"
        shutil.copytree(self.source, copy_path)
        self.assertTrue(streaming.validate_tree(copy_path, proof)["ok"])
        self.assertTrue(streaming.validate_tree(self.source, proof, source=True)["ok"])

    def test_full_content_names_counts_modes_and_source_timestamps_are_verified(self):
        proof = streaming.scan_tree(self.source)
        for damage in ("content", "missing", "extra", "rename", "mode", "mtime"):
            with self.subTest(damage=damage):
                destination = self.root / damage
                shutil.copytree(self.source, destination)
                file = destination / "a" / "0"
                if damage == "content":
                    file.write_bytes(b"y" * 21)
                elif damage == "missing":
                    file.unlink()
                elif damage == "extra":
                    (destination / "extra").write_bytes(b"")
                elif damage == "rename":
                    file.rename(file.with_name("renamed"))
                elif damage == "mode":
                    file.chmod(0o600)
                else:
                    os.utime(file, ns=(1, 1))
                self.assertFalse(streaming.validate_tree(destination, proof, source=True)["ok"])
                # destination timestamps are deliberately outside copy semantics
                self.assertEqual(streaming.validate_tree(destination, proof)["ok"], damage == "mtime")

    def test_links_special_files_acl_and_unbounded_entries_are_rejected(self):
        for kind in ("symlink", "hardlink", "fifo"):
            with self.subTest(kind=kind):
                path = self.source / "invalid"
                if kind == "symlink":
                    path.symlink_to(self.source / "a" / "0")
                elif kind == "hardlink":
                    os.link(self.source / "a" / "0", path)
                else:
                    os.mkfifo(path)
                try:
                    with self.assertRaises(ValueError):
                        streaming.scan_tree(self.source)
                finally:
                    path.unlink()
        with mock.patch.object(streaming.os, "listxattr", return_value=["system.posix_acl_access"]):
            with self.assertRaisesRegex(ValueError, "ACL"):
                streaming.scan_tree(self.source)
        with mock.patch.object(streaming, "MAX_DIRECTORY_ENTRIES", 3):
            with self.assertRaisesRegex(ValueError, "entry limit"):
                streaming.scan_tree(self.source)
        with mock.patch.object(streaming, "MAX_DEPTH", 0):
            with self.assertRaisesRegex(ValueError, "depth"):
                streaming.scan_tree(self.source)
        with self.assertRaisesRegex(ValueError, "counts"):
            streaming.scan_tree(self.source, expected_counts=dict(files=110, directories=3, bytes=2331))

    def test_held_file_descriptor_bounds_reads_and_detects_changes(self):
        original = streaming.os.fstat
        calls = 0
        def changing(fd):
            nonlocal calls
            calls += 1
            value = original(fd)
            if calls == 2:
                fields = {name: getattr(value, name) for name in dir(value) if name.startswith("st_")}
                return SimpleNamespace(**dict(fields, st_mtime_ns=value.st_mtime_ns + 1))
            return value
        parent = os.open(self.source / "a", os.O_RDONLY | os.O_DIRECTORY)
        try:
            with mock.patch.object(streaming.os, "fstat", side_effect=changing):
                with self.assertRaisesRegex(ValueError, "changed"):
                    streaming._file(parent, "0")
            with mock.patch.object(capacity, "MAX_FILE_BYTES", 20):
                with self.assertRaisesRegex(ValueError, "bounded"):
                    streaming._file(parent, "0")
        finally:
            os.close(parent)

    def test_new_verification_identity_does_not_change_legacy_series(self):
        from benchmarks.test_storage import StorageIdentityTests
        helper = StorageIdentityTests()
        helper.setUp()
        arguments = (helper.case, helper.variant, "source-warm", "local", "runner", helper.environment, {})
        legacy = run.series_id(*arguments)
        self.assertEqual(legacy, run.series_id(*arguments, verification=None))
        self.assertNotEqual(legacy, run.series_id(*arguments, verification=dict(policy=streaming.POLICY, workers=8)))
        record = paired_record()
        report.validate_result(record)
        record["context"]["verification"] = dict(policy=streaming.POLICY, workers=8)
        with self.assertRaisesRegex(ValueError, "streaming"):
            report.validate_result(record)
        for policy in (dict(policy="unknown", workers=8), dict(policy=streaming.POLICY, workers=0)):
            record["context"]["verification"] = policy
            with self.assertRaises(ValueError):
                report.validate_result(record)


class StreamingIntegrationTests(unittest.TestCase):
    def test_real_small_fixture_preserves_cold_copy_boundary_and_full_source_checks(self):
        from benchmarks.test_timings import TimingTests
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, manifest = TimingTests()._fixture(root)
            events = []
            original_scan = streaming.scan_tree
            def scan(*args, **kwargs):
                events.append("verify")
                return original_scan(*args, **kwargs)
            def execute(commands, logdir, timeout, **kwargs):
                events.append("copy")
                source, destination = map(Path, commands[0][-2:])
                shutil.copytree(source, destination)
                return dict(ok=True, elapsed_seconds=1.0, exit_codes=[0], timed_out=False, commands=commands, logs=[])
            # helper variant has no summary flag; content/source checks still apply
            with mock.patch.object(run, "environment", return_value={}), \
                    mock.patch.object(run, "_prepare_cache", side_effect=lambda *args: events.append("drop")), \
                    mock.patch.object(run, "execute_commands", side_effect=execute), \
                    mock.patch.object(streaming, "scan_tree", side_effect=scan), contextlib.redirect_stdout(io.StringIO()):
                record = run.main(["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default",
                    "--bin-dir", str(binary), "--baseline-bin-dir", str(binary), "--source-root", str(root),
                    "--destination-root", str(root), "--cache", "linux-drop-caches", "--no-timings",
                    "--paired-seed", "7", "--repetitions", "2", "--streaming-verification",
                    "--output", str(root / "out")])
            self.assertEqual(record["status"], "complete")
            self.assertEqual(events, ["verify"] + ["drop", "copy", "verify", "verify"] * 4)
            self.assertTrue(all(trial["source_validation"]["ok"] for trial in record["trials"]))
            report.validate_result(record)
            missing = copy.deepcopy(record)
            missing["trials"][0].pop("source_validation")
            with self.assertRaisesRegex(ValueError, "source_validation"):
                report.validate_result(missing)

    def _failed_source_verification(self, fault, reason):
        from benchmarks.test_timings import TimingTests
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, manifest = TimingTests()._fixture(root)
            copied_source = None
            events = []
            original_scan = streaming.scan_tree
            def scan(path, *args, **kwargs):
                events.append("verify")
                if path == copied_source:
                    fault(path)
                return original_scan(path, *args, **kwargs)
            def execute(commands, logdir, timeout, **kwargs):
                nonlocal copied_source
                events.append("copy")
                copied_source, destination = map(Path, commands[0][-2:])
                shutil.copytree(copied_source, destination)
                return dict(ok=True, elapsed_seconds=1.0, exit_codes=[0], timed_out=False, commands=commands, logs=[])
            with mock.patch.object(run, "environment", return_value={}), \
                    mock.patch.object(run, "_prepare_cache", side_effect=lambda *args: events.append("drop")), \
                    mock.patch.object(run, "execute_commands", side_effect=execute), \
                    mock.patch.object(streaming, "scan_tree", side_effect=scan), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "source verification " + reason):
                    run.main(["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default",
                        "--bin-dir", str(binary), "--baseline-bin-dir", str(binary), "--source-root", str(root),
                        "--destination-root", str(root), "--cache", "linux-drop-caches", "--no-timings",
                        "--paired-seed", "7", "--repetitions", "2", "--streaming-verification",
                        "--output", str(root / "out")])
            record = report.parse_result((root / "out/results.json").read_text())
            self.assertEqual(record["status"], "failed")
            self.assertEqual(record["summaries"], [])
            self.assertEqual(events, ["verify", "drop", "copy", "verify", "verify"])
            self.assertEqual(len(record["trials"]), 1)
            trial = record["trials"][0]
            self.assertEqual(trial["status"], "failed")
            self.assertFalse(trial["validation"]["ok"])
            self.assertFalse(trial["source_validation"]["ok"])
            self.assertEqual(trial["exit_codes"], [0])
            self.assertFalse(trial["timed_out"])
            self.assertEqual(trial["validation"]["digest"], record["cases"][0]["fixture_digest"])
            self.assertNotIn("source changed:", record["error"])
            # A successful copy and destination digest cannot substitute for source proof.
            forged = copy.deepcopy(record)
            forged["trials"][0]["status"] = "ok"
            forged["trials"][0]["validation"]["ok"] = True
            with self.assertRaisesRegex(ValueError, "source_validation"):
                report.validate_result(forged)
            return trial

    def test_sigterm_during_source_verification_is_interrupted_and_stays_failed(self):
        def interrupt(_signum, _frame):
            raise InterruptedError("SIGTERM")
        previous = signal.signal(signal.SIGTERM, interrupt)
        try:
            trial = self._failed_source_verification(lambda path: signal.raise_signal(signal.SIGTERM), "interrupted")
        finally:
            signal.signal(signal.SIGTERM, previous)
        self.assertEqual(trial["source_validation"], dict(ok=False, error="SIGTERM", interrupted=True))
        self.assertEqual(trial["validation"]["error"], "source verification interrupted: SIGTERM")

    def test_source_content_mismatch_stays_failed_with_specific_evidence(self):
        def change(source):
            file = next(path for path in source.rglob("*") if path.is_file())
            file.write_bytes(b"z")
        trial = self._failed_source_verification(change, "failed")
        self.assertNotIn("interrupted", trial["source_validation"])
        self.assertEqual(trial["source_validation"]["error"], "fixture paths, contents or modes differ")
        self.assertEqual(trial["validation"]["error"],
            "source verification failed: fixture paths, contents or modes differ")

    def test_other_source_verification_errors_do_not_claim_change_or_interruption(self):
        for error in (PermissionError("read denied"), ValueError("invalid fixture"), ValueError("SIGTERM")):
            with self.subTest(error=repr(error)):
                def fail(source):
                    raise error
                trial = self._failed_source_verification(fail, "failed")
                self.assertNotIn("interrupted", trial["source_validation"])
                self.assertEqual(trial["validation"]["error"], "source verification failed: " + str(error))

    def test_capacity_failure_precedes_fixture_generation(self):
        from benchmarks.test_timings import TimingTests
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, manifest = TimingTests()._fixture(root)
            with mock.patch.object(run, "environment", return_value={}), \
                    mock.patch.object(capacity, "check_pair", side_effect=ValueError("no fixture space")), \
                    mock.patch.object(streaming, "scan_tree") as scan, \
                    self.assertRaisesRegex(ValueError, "no fixture space"):
                run.main(["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default",
                    "--bin-dir", str(binary), "--source-root", str(root), "--destination-root", str(root),
                    "--cache", "linux-drop-caches", "--no-timings", "--streaming-verification",
                    "--check-capacity", "--output", str(root / "out")])
            scan.assert_not_called()
            self.assertFalse(list(root.rglob("file")))
            record = report.parse_result((root / "out/results.json").read_text())
            self.assertEqual(record["status"], "failed")
            self.assertEqual(record["trials"], [])


if __name__ == "__main__":
    unittest.main()
