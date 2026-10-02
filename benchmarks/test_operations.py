"""Tiny operation contracts; no copy executables, SSH, sync or Git mutation."""
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest import mock

from benchmarks import report, run
from benchmarks.test_report import sample_run


class OperationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def ops(self):
        self.assertIsNotNone(getattr(run, "operations", None), "operation support missing")
        return run.operations

    def tree(self):
        source = self.root / "source"
        source.mkdir()
        for leaf in ("a", "b"):
            directory = source / leaf
            directory.mkdir()
            for index in range(100):
                path = directory / f"{index:03d}"
                path.write_bytes(bytes([index, 17, 255]))
                path.chmod(0o640)
                os.utime(path, ns=(1700000000123456789, 1700000000123456789))
            directory.chmod(0o750)
            os.utime(directory, ns=(1700000000123456789, 1700000000123456789))
        source.chmod(0o750)
        os.utime(source, ns=(1700000000123456789, 1700000000123456789))
        return source, run.scan_tree(source)

    def manifest(self, mode=None, files=200, size=3):
        case = dict(id="tiny", directory_widths=[1], files_per_leaf=files, file_size_bytes=size)
        if mode is not None:
            case["mode"] = mode
        manifest = self.root / "manifest.json"
        manifest.write_text(json.dumps(dict(schema_version=1, cases=[case], variants=[dict(id="rcp-default", tool="rcp", args=["--summary"], processes=1)])))
        return manifest

    def test_manifest_accepts_bounded_operations_and_preserves_missing_mode(self):
        for mode in ("fresh", "unchanged", "partial", None):
            result = run.load_manifest(self.manifest(mode))["cases"][0]
            self.assertEqual(result.get("mode", "fresh"), mode or "fresh")
            if mode is None:
                self.assertNotIn("mode", result)

    def test_partial_constraints_reject_empty_rounding_and_large_files(self):
        for files, size in ((99, 1), (200, 1025)):
            with self.subTest(files=files, size=size), self.assertRaisesRegex(ValueError, "partial"):
                run.load_manifest(self.manifest("partial", files, size))

    def test_existing_cache_policies_keep_sync_warm_drop_and_uncontrolled_behaviors(self):
        source, _ = self.tree()
        for policy, expected in (("source-warm", [["sync"]]), ("linux-drop-caches", [["sync"], ["sudo", "-n", "sh", "-c", "printf 3 > /proc/sys/vm/drop_caches"]]), ("uncontrolled", [])):
            commands = []
            with mock.patch.object(run.subprocess, "run", side_effect=lambda command, **kwargs: commands.append(command)), mock.patch.object(run.sys, "platform", "linux"):
                run._prepare_cache(policy, source, 1)
            self.assertEqual(commands, expected)

    def test_receiver_manifest_has_six_operations_and_expected_geometry(self):
        manifest = run.load_manifest(Path(__file__).with_name("receiver-performance.json"))
        self.assertEqual([case.get("mode", "fresh") for case in manifest["cases"]], ["fresh", "fresh", "fresh", "fresh", "unchanged", "partial"])
        directory = next(case for case in manifest["cases"] if case["id"] == "directory-90k")
        self.assertEqual(run.expected_counts(directory), dict(directories=90090, files=90000, bytes=92160000))
        partial = next(case for case in manifest["cases"] if case["id"] == "tiny-partial")
        self.assertEqual(self.ops().transfer_counts("partial", run.expected_counts(partial)), dict(files_copied=10240, files_unchanged=1013760, bytes_copied=10485760))

    def test_report_malformed_legacy_collections_raise_validation_error(self):
        for field in ("cases", "trials"):
            value = sample_run()
            value[field] = None
            with self.subTest(field=field), self.assertRaises(ValueError):
                report.validate_result(value)

    def test_summary_child_uses_c_locale_and_preserves_parent_environment(self):
        inherited = {"LC_ALL": "de_DE.UTF-8", "LANG": "de_DE.UTF-8", "RCP_TEST_ENV": "retained"}
        command = [sys.executable, "-c", "import json,os; print(json.dumps({name:os.environ.get(name) for name in ('LC_ALL','LANG','RCP_TEST_ENV')})); print('Number of regular files transferred: '+('1,024' if os.environ.get('LC_ALL') == 'C' else '1.024')); print('Total transferred file size: '+('1,048,576' if os.environ.get('LC_ALL') == 'C' else '1.048.576')+' bytes')"]
        with mock.patch.dict(os.environ, inherited):
            before = dict(os.environ)
            outcome = run.execute_commands([command], self.root / "c-logs", 3, stable_summary_locale=True)
            self.assertTrue(outcome["ok"])
            text = Path(outcome["logs"][0]["stdout"]).read_text()
            self.assertEqual(json.loads(text.splitlines()[0]), dict(LC_ALL="C", LANG="C", RCP_TEST_ENV="retained"))
            self.assertEqual(dict(os.environ), before)
            variant = dict(tool="rsync", args=["-rp", "--stats"], processes=1)
            expected = dict(files_copied=1024, files_unchanged=0, bytes_copied=1048576)
            self.assertEqual(self.ops().validate_summary(variant, text, expected), expected)
            legacy = run.execute_commands([command], self.root / "legacy-logs", 3)
            self.assertTrue(legacy["ok"])
            legacy_text = Path(legacy["logs"][0]["stdout"]).read_text()
            self.assertEqual(json.loads(legacy_text.splitlines()[0]), inherited)
            self.assertEqual(dict(os.environ), before)

    def test_unknown_operation_rejected(self):
        with self.assertRaisesRegex(ValueError, "mode|operation"):
            run.load_manifest(self.manifest("delete"))

    def test_selection_is_one_percent_floor_and_uniform_leaf_spread(self):
        source, scan = self.tree()
        self.assertEqual(self.ops().select_stale(scan["entries"]), ["a/000", "b/000"])
        self.assertEqual(self.ops().partial_parameters(1000, 1024), (10, 240))
        uneven_percent = {f"{leaf}/{index:03d}": {"type": "file"} for leaf in ("c", "b", "a") for index in range(67)}
        self.assertEqual(self.ops().select_stale(uneven_percent), ["a/000", "b/000"])
        scan["entries"].pop("a/001")
        with self.assertRaisesRegex(ValueError, "uniform"):
            self.ops().select_stale(scan["entries"])

    def test_partial_seed_inverts_bytes_with_exact_timestamp_and_independent_files(self):
        source, scan = self.tree()
        ops = self.ops()
        metadata = ops.metadata_tree(source)
        dest = self.root / "dest"
        stale = ops.seed_destination(source, dest, scan, metadata, ["a/000", "b/000"])
        self.assertEqual((dest / "a/000").read_bytes(), bytes([255, 238, 0]))
        self.assertEqual((dest / "a/001").read_bytes(), bytes([1, 17, 255]))
        self.assertEqual((dest / "a/000").stat().st_mtime_ns, 1699999998123456789)
        proof = ops.validate_seed(source, dest, scan, metadata, stale)
        self.assertEqual(proof["stale_files"], 2)
        self.assertTrue(proof["independent_files"])
        self.assertEqual((dest / "a/001").stat().st_mode & 0o777, 0o640)

    def test_seed_rejects_corruption_missing_mode_mtime_and_source_hardlink(self):
        source, scan = self.tree()
        ops = self.ops()
        metadata = ops.metadata_tree(source)
        for damage in ("bytes", "missing", "mode", "mtime", "hardlink"):
            with self.subTest(damage=damage):
                dest = self.root / damage
                stale = ops.seed_destination(source, dest, scan, metadata, [])
                path = dest / "a/001"
                if damage == "bytes": path.write_bytes(b"bad")
                if damage == "missing": path.unlink()
                if damage == "mode": path.chmod(0o600)
                if damage == "mtime": os.utime(path, ns=(1, 1))
                if damage == "hardlink":
                    path.unlink(); os.link(source / "a/001", path)
                with self.assertRaises(ValueError):
                    ops.validate_seed(source, dest, scan, metadata, stale)

    def test_final_modes_do_not_require_rsync_timestamp_preservation(self):
        source, scan = self.tree()
        ops = self.ops()
        metadata = ops.metadata_tree(source)
        dest = self.root / "dest"
        ops.seed_destination(source, dest, scan, metadata, [])
        os.utime(dest / "a/001", ns=(1, 1))
        self.assertTrue(ops.validate_metadata(dest, metadata)["ok"])
        with self.assertRaises(ValueError):
            ops.validate_metadata(dest, metadata, timestamps=True)

    def test_update_planner_overwrites_intended_root_and_pins_daemon(self):
        source, _ = self.tree()
        dest = self.root / "seeded"
        dest.mkdir()
        command = run.plan_commands(dict(tool="rcp", args=["--summary"], processes=1), source, dest, dict(rcp="/candidate/rcp", rcpd="/candidate/rcpd"), "loopback", operation="partial")[0]
        self.assertIn("--overwrite", command)
        self.assertIn("--rcpd-path=/candidate/rcpd", command)
        self.assertEqual(command[-2:], [f"localhost:{source}", str(dest)])
        command = run.plan_commands(dict(tool="rsync", args=["-rp", "--stats"], processes=1), source, dest, dict(rsync="/rsync"), "local", operation="unchanged")[0]
        self.assertEqual(command[-2:], [str(source) + "/", str(dest) + "/"])

    def test_update_variants_rejected_before_any_fixture_or_tool_work(self):
        for tool, args, processes in (("cp", ["-a"], 1), ("rsync", ["-a"], 1), ("rsync", ["-rp", "--stats"], 10), ("rcp", ["--summary", "--delete"], 1)):
            manifest = self.manifest("unchanged")
            data = json.loads(manifest.read_text())
            data["variants"][0].update(tool=tool, args=args, processes=processes)
            manifest.write_text(json.dumps(data))
            output = self.root / f"out-{tool}-{processes}-{len(args)}"
            with mock.patch.object(run, "_revision", return_value=dict(commit=None, branch=None, dirty=None)), mock.patch.object(run, "_tool", side_effect=AssertionError("tool should not start")), self.assertRaisesRegex(ValueError, "update|operation"):
                run.main(["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default", "--output", str(output)])
            self.assertEqual(report.parse_result((output / "results.json").read_text())["trials"], [])

    def test_source_verified_has_no_sync_or_cache_drop_and_detects_source_damage(self):
        source, scan = self.tree()
        metadata = self.ops().metadata_tree(source)
        with mock.patch.object(run.subprocess, "run", side_effect=AssertionError("host cache command forbidden")):
            proof = run._prepare_cache("source-verified", source, 1, scan, metadata)
            self.assertTrue(proof["ok"])
            (source / "a/001").write_bytes(b"bad")
            with self.assertRaisesRegex(ValueError, "source"):
                run._prepare_cache("source-verified", source, 1, scan, metadata)

    def test_summary_counts_reject_duplicates_wrong_transfer_and_bytes(self):
        ops = self.ops()
        rcp = dict(tool="rcp", args=["--summary"], processes=1)
        rsync = dict(tool="rsync", args=["-rp", "--stats"], processes=1)
        expected = dict(files_copied=2, files_unchanged=198, bytes_copied=6)
        for variant, text in ((rcp, "files copied: 2\nfiles unchanged: 198\n"), (rsync, "Number of regular files transferred: 2\nTotal transferred file size: 6 bytes\n")):
            self.assertEqual(ops.validate_summary(variant, text, expected), expected)
            for bad in (text + text, text.replace(": 2", ": 3"), text.replace(": 198", ": 197").replace(": 6", ": 7")):
                with self.assertRaises(ValueError): ops.validate_summary(variant, bad, expected)

    def test_legacy_series_stays_stable_and_new_contract_distinguishes_operations(self):
        case = dict(id="tiny", directory_widths=[1], files_per_leaf=200, file_size_bytes=3)
        args = (case, dict(tool="rcp", args=["--summary"], processes=1), "uncontrolled", "local", "runner", {}, {})
        legacy = run.series_id(*args)
        self.assertEqual(legacy, "a27d704db61185db3ab2f03f42f7985e7faec6ccec2fd58968b62be1e641db39")
        self.assertNotEqual(legacy, run.series_id(*args, operation_revision=1))
        self.assertEqual(run.series_id(*args, operation_revision=1), run.series_id({**case, "mode": "fresh"}, *args[1:], operation_revision=1))
        self.assertNotEqual(run.series_id(*args, operation_revision=1), run.series_id({**case, "mode": "unchanged"}, *args[1:], operation_revision=1))
        self.assertEqual(report.validate_result(sample_run())["schema_version"], 1)


class RunnerOperationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.calls = []

    def run_case(self, operation, *, seed_error=False, bad_summary=False, damage=None, baseline=False, tool="rcp", all_directories=False):
        manifest = self.root / "manifest.json"
        variants = [dict(id="rcp-default" if tool == "rcp" else "rsync-matched", tool=tool,
                         args=["--summary"] if tool == "rcp" else ["-rp", "--stats"], processes=1)]
        placement = dict(files_per_directory=50) if all_directories else dict(files_per_leaf=100)
        manifest.write_text(json.dumps(dict(schema_version=1,
            cases=[dict(id="tiny", mode=operation, directory_widths=[1], file_size_bytes=3, **placement)], variants=variants)))
        output = self.root / "out"
        args = ["--manifest", str(manifest), "--case", "tiny", "--variant", variants[0]["id"],
                "--bin-dir", "/candidate", "--source-root", str(self.root), "--destination-root", str(self.root),
                "--cache", "source-verified", "--no-timings", "--repetitions", "2", "--output", str(output)]
        if baseline: args += ["--baseline-bin-dir", "/reference"]

        def generate(command, **kwargs):
            self.assertEqual(Path(command[0]).name, "filegen", "no host commands permitted")
            source = Path(command[1]) / "filegen"
            (source / "leaf").mkdir(parents=True)
            directories = [source, source / "leaf"] if all_directories else [source / "leaf"]
            for directory in directories:
                for index in range(50 if all_directories else 100):
                    path = directory / f"{index:03d}"
                    path.write_bytes(b"abc")
                    path.chmod(0o640)
                    os.utime(path, ns=(1700000000123456789, 1700000000123456789))
            return run.subprocess.CompletedProcess(command, 0, "", "")

        def execute(commands, logs, timeout, *, stable_summary_locale=False):
            # real persistence, planner, seed and validators surround this fake command boundary
            saved = json.loads((output / "results.json").read_text())
            trial = saved["trials"][-1]
            self.assertEqual(trial["status"], "running")
            self.assertTrue(trial["cache_validation"]["ok"])
            self.assertTrue(stable_summary_locale)
            self.assertEqual(trial["child_locale"], "C")
            if operation != "fresh":
                self.assertTrue(trial["seed_validation"]["ok"])
                self.assertEqual(trial["seed_validation"]["stale_files"], 1 if operation == "partial" else 0)
            command = commands[0]
            self.calls.append(command)
            self.assertEqual(command[0], "/reference/rcp" if trial["variant_id"] == "rcp-baseline" else "/candidate/rcp" if tool == "rcp" else "/mock/rsync")
            source, dest = Path(command[-2].removeprefix("localhost:").rstrip("/")), Path(command[-1].rstrip("/"))
            if operation != "fresh" and tool == "rcp": self.assertIn("--overwrite", command)
            shutil.copytree(source, dest, dirs_exist_ok=True)
            if damage == "mode": (dest / "leaf/000").chmod(0o600)
            if damage == "source": os.utime(source / "leaf/000", ns=(1, 1))
            logs.mkdir(parents=True)
            stdout, stderr = logs / "0.stdout.log", logs / "0.stderr.log"
            copied = {"fresh": 100, "unchanged": 0, "partial": 1}[operation]
            if bad_summary: copied += 1
            text = (f"files copied: {copied}\nfiles unchanged: {100-copied}\n" if tool == "rcp" else
                    f"Number of regular files transferred: {copied}\nTotal transferred file size: {copied*3} bytes\n")
            stdout.write_text(text); stderr.write_text("")
            return dict(ok=True, timed_out=False, launch_error=None, elapsed_seconds=0.25, exit_codes=[0],
                        logs=[dict(stdout=str(stdout), stderr=str(stderr))])

        with mock.patch.object(run, "_revision", return_value=dict(commit=None, branch=None, dirty=None)), \
             mock.patch.object(run, "_tool", side_effect=lambda p: dict(path=str(p), version="mock 1", sha256="a"*64)), \
             mock.patch.object(run, "_supports_timings", return_value=False), \
             mock.patch.object(run.shutil, "which", return_value="/mock/rsync"), \
             mock.patch.object(run.subprocess, "run", side_effect=generate), \
             mock.patch.object(run, "execute_commands", side_effect=execute):
            if seed_error:
                self.assertIsNotNone(getattr(run, "operations", None))
                def fail_seed(*args):
                    saved = json.loads((output / "results.json").read_text())
                    self.assertEqual(len(saved["trials"]), 1, "attempt must be persisted before seed")
                    self.assertEqual(saved["trials"][0]["status"], "running")
                    raise OSError("seed failed")
                with mock.patch.object(run.operations, "seed_destination", side_effect=fail_seed):
                    run.main(args)
            else:
                return run.main(args)

    def test_fresh_unchanged_partial_complete_with_exact_counts_and_owned_cleanup(self):
        for operation in ("fresh", "unchanged", "partial"):
            with self.subTest(operation=operation):
                self.root = Path(self.temp.name) / operation
                self.root.mkdir()
                result = self.run_case(operation)
                self.assertEqual(result["status"], "complete")
                self.assertEqual(result["trials"][0]["copy_summary"], dict(files_copied={"fresh":100,"unchanged":0,"partial":1}[operation], files_unchanged={"fresh":0,"unchanged":100,"partial":99}[operation], bytes_copied={"fresh":300,"unchanged":0,"partial":3}[operation]))
                self.assertFalse(list(self.root.glob("rcp-bench-*")))
                self.assertEqual(report.parse_result((self.root / "out/results.json").read_text()), result)

    def test_matched_rsync_partial_summaries_and_final_modes(self):
        result = self.run_case("partial", tool="rsync")
        self.assertEqual(result["trials"][0]["copy_summary"]["files_copied"], 1)
        self.assertEqual(result["trials"][0]["metadata_validation"]["checked_fields"], ["mode"])

    def test_all_directory_operations_report_and_export_root_files(self):
        for operation, copied in (("fresh", 100), ("unchanged", 0), ("partial", 1)):
            with self.subTest(operation=operation):
                self.root = Path(self.temp.name) / operation
                self.root.mkdir()
                result = self.run_case(operation, all_directories=True)
                self.assertEqual(result["status"], "complete")
                self.assertEqual(result["trials"][0]["copy_summary"], dict(
                    files_copied=copied, files_unchanged=100-copied, bytes_copied=3*copied))
                parsed = report.parse_result((self.root / "out/results.json").read_text())
                self.assertEqual(parsed, result)
                exported = report.sanitized.project_run(parsed, [])
                self.assertEqual(exported["cases"][0]["files_per_directory"], 50)
                self.assertIsNone(exported["cases"][0]["files_per_leaf"])
                self.assertEqual(exported["cases"][0]["realized_counts"],
                                 dict(directories=1, files=100, bytes=300))

    def test_seed_failure_retains_attempt_and_stops_all_commands(self):
        with self.assertRaisesRegex(OSError, "seed failed"):
            self.run_case("unchanged", seed_error=True)
        result = report.parse_result((self.root / "out/results.json").read_text())
        self.assertEqual(len(result["trials"]), 1)
        self.assertEqual(result["trials"][0]["status"], "failed")
        self.assertEqual(self.calls, [])
        self.assertTrue(Path(result["context"]["failure_artifacts"]["source_scratch"]).exists())

    def test_wrong_summary_aborts_retaining_destination(self):
        with self.assertRaisesRegex(RuntimeError, "summary"):
            self.run_case("partial", bad_summary=True)
        result = report.parse_result((self.root / "out/results.json").read_text())
        self.assertEqual(len(result["trials"]), 1)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(result["summaries"], [])
        self.assertTrue(any(Path(result["context"]["failure_artifacts"]["destination_scratch"]).rglob("000")))

    def test_final_mode_and_source_timestamp_damage_stop_subsequent_trials(self):
        for damage in ("mode", "source"):
            with self.subTest(damage=damage):
                self.root = Path(self.temp.name) / damage; self.root.mkdir(); self.calls=[]
                with self.assertRaisesRegex(RuntimeError, "metadata|source"):
                    self.run_case("partial", damage=damage)
                self.assertEqual(len(self.calls), 1)
                self.assertEqual(json.loads((self.root / "out/results.json").read_text())["summaries"], [])

    def test_baseline_uses_independent_seed_and_matching_full_release(self):
        result = self.run_case("unchanged", baseline=True)
        self.assertEqual([trial["variant_id"] for trial in result["trials"]], ["rcp-default", "rcp-baseline", "rcp-baseline", "rcp-default"])
        self.assertEqual(len({command[-1] for command in self.calls}), 4)

    def test_report_rejects_missing_update_proofs_and_incorrect_counts(self):
        result = self.run_case("partial")
        for field in ("seed_validation", "copy_summary", "metadata_validation", "source_validation", "cache_validation"):
            damaged = copy.deepcopy(result)
            damaged["trials"][0].pop(field)
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, field):
                report.validate_result(damaged)
        damaged = copy.deepcopy(result); damaged["trials"][0]["copy_summary"]["files_copied"] = 100
        with self.assertRaisesRegex(ValueError, "copy_summary"):
            report.validate_result(damaged)

    def test_report_cannot_remove_contract_or_accept_bool_counts(self):
        result = self.run_case("partial")
        for mutation in ("revision", "operation", "seed_count", "cache_count", "source_metadata", "destination", "variant", "locale"):
            damaged = copy.deepcopy(result)
            trial = damaged["trials"][0]
            if mutation == "revision": damaged["context"].pop("operation_contract_revision")
            if mutation == "operation": trial["operation"] = "fresh"
            if mutation == "seed_count": trial["seed_validation"]["stale_files"] = True
            if mutation == "cache_count": trial["cache_validation"]["counts"]["directories"] = True
            if mutation == "source_metadata": trial["source_validation"]["metadata"]["checked_fields"] = ["mode"]
            if mutation == "destination": trial["validation"].pop("digest")
            if mutation == "variant": damaged["variants"][0].pop("processes")
            if mutation == "locale": trial["child_locale"] = "inherited"
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                report.validate_result(damaged)


if __name__ == "__main__":
    unittest.main()
