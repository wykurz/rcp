import contextlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from benchmarks import report, run


class RunnerTests(unittest.TestCase):
    def test_cp_loopback_is_rejected_before_fixture_work(self):
        self._assert_rejected_before_fixture_work(
            [{"id": "cp-a", "tool": "cp", "args": ["-a"], "processes": 1}],
            ["--case", "tiny", "--variant", "cp-a", "--mode", "loopback"], "cp.*local",
        )

    def test_planner_rejects_remote_cp_before_resolving_tools(self):
        with self.assertRaisesRegex(ValueError, "cp.*local"):
            run.plan_commands({"id": "cp-a", "tool": "cp", "args": ["-a"], "processes": 1}, Path("source"), Path("destination"), {}, "loopback")

    def test_default_selections_include_cp_only_locally(self):
        for mode, expected in (("local", ["rcp-default", "rsync-a", "rsync-a-10", "cp-a"]), ("loopback", ["rcp-default", "rsync-a", "rsync-a-10"])):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                manifest = root / "manifest.json"
                manifest.write_text(json.dumps({"schema_version": 1, "cases": [{"id": "tiny-10k", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1}], "variants": [{"id": identifier, "tool": tool, "args": [], "processes": 1} for identifier, tool in (("rcp-default", "rcp"), ("rsync-a", "rsync"), ("rsync-a-10", "rsync"), ("cp-a", "cp"))]}))
                output = root / "out"
                with mock.patch.object(run, "_tool", side_effect=RuntimeError("stop before generation")), self.assertRaisesRegex(RuntimeError, "stop before generation"):
                    run.main(["--manifest", str(manifest), "--mode", mode, "--bin-dir", str(root), "--source-root", str(root), "--destination-root", str(root), "--output", str(output)])
                result = report.parse_result((output / "results.json").read_text())
                self.assertEqual([variant["id"] for variant in result["variants"]], expected)
                self.assertEqual(result["context"]["purpose"], "performance")

    def test_real_cp_copies_single_and_partitioned_fixtures(self):
        for size, buffer_size in ((7, 7), (1048577, 1048576)):
            with self.subTest(size=size), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                binary = root / "bin"
                binary.mkdir()
                filegen = binary / "filegen"
                filegen.write_text("#!/usr/bin/env python3\nimport pathlib,sys\nif '--version' in sys.argv: print('filegen 1'); sys.exit(0)\nprint(' '.join(sys.argv[2:]))\nfor index in range(2):\n p=pathlib.Path(sys.argv[1])/'filegen'/str(index); p.mkdir(parents=True); (p/'file').write_bytes(b'x'*int(sys.argv[4]))\n")
                filegen.chmod(0o755)
                manifest = root / "manifest.json"
                manifest.write_text(json.dumps({"schema_version": 1, "cases": [{"id": "tiny", "directory_widths": [2], "files_per_leaf": 1, "file_size_bytes": size}], "variants": [{"id": identifier, "tool": "cp", "args": ["-a"], "processes": processes} for identifier, processes in (("cp-a", 1), ("cp-parallel", 2))]}))
                output = root / "out"
                result = run.main(["--manifest", str(manifest), "--case", "tiny", "--variant", "cp-a", "--variant", "cp-parallel", "--purpose", "smoke", "--bin-dir", str(binary), "--source-root", str(root), "--destination-root", str(root), "--cache", "uncontrolled", "--repetitions", "1", "--output", str(output)])
                self.assertEqual(result["status"], "complete")
                self.assertEqual(result["context"]["purpose"], "smoke")
                self.assertEqual(set(result["tools"]), {"filegen", "cp"})
                self.assertEqual(len(result["tools"]["cp"]["sha256"]), 64)
                self.assertTrue(result["tools"]["cp"]["version"])
                self.assertEqual([trial["validation"]["ok"] for trial in result["trials"]], [True, True])
                single, parallel = (trial["commands"] for trial in result["trials"])
                self.assertEqual(len(single), 1)
                self.assertEqual(len(parallel), 2)
                for command in [*single, *parallel]:
                    self.assertEqual(command[1:-2], ["-a"])
                self.assertEqual(Path(single[0][-2]).name, "filegen")
                self.assertEqual({Path(command[-2]).name for command in parallel}, {"0", "1"})
                self.assertEqual(len({command[-1] for command in parallel}), 1)
                self.assertEqual((output / "logs" / "tiny.filegen.stdout.log").read_text().strip(), f"2 1 {size} --leaf-files --bufsize={buffer_size}")
                self.assertFalse(list(root.glob("rcp-bench-*")))

    def _assert_rejected_before_fixture_work(self, variants, selections, diagnostic, baseline=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "bin"
            binary.mkdir()
            marker = root / "tool-started"
            filegen = binary / "filegen"
            filegen.write_text(f"#!/usr/bin/env python3\nfrom pathlib import Path\nPath({str(marker)!r}).touch()\nraise SystemExit(99)\n")
            filegen.chmod(0o755)
            source = root / "source"
            destination = root / "destination"
            source.mkdir()
            destination.mkdir()
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"schema_version": 1, "cases": [{"id": "tiny", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1}], "variants": variants}))
            output = root / "out"
            arguments = ["--manifest", str(manifest), "--bin-dir", str(binary), "--source-root", str(source), "--destination-root", str(destination), "--output", str(output), *selections]
            if baseline:
                arguments += ["--baseline-bin-dir", str(binary)]
            with self.assertRaisesRegex(ValueError, diagnostic):
                run.main(arguments)
            self.assertFalse(marker.exists())
            self.assertEqual(list(source.iterdir()), [])
            self.assertEqual(list(destination.iterdir()), [])
            record = report.parse_result((output / "results.json").read_text())
            self.assertEqual(record["status"], "failed")
            self.assertEqual(record["trials"], [])

    def test_reserved_baseline_variant_is_rejected_even_when_unselected(self):
        for tool in ("rcp", "rsync"):
            with self.subTest(tool=tool):
                self._assert_rejected_before_fixture_work([
                    {"id": "rcp-default", "tool": "rcp", "args": [], "processes": 1},
                    {"id": "rcp-baseline", "tool": tool, "args": [], "processes": 1},
                ], ["--case", "tiny", "--variant", "rcp-default"], "reserved.*rcp-baseline")

    def test_repeated_selections_are_rejected_before_fixture_work(self):
        for option, value in (("--case", "tiny"), ("--variant", "rcp-default")):
            with self.subTest(option=option):
                self._assert_rejected_before_fixture_work(
                    [{"id": "rcp-default", "tool": "rcp", "args": [], "processes": 1}],
                    ["--case", "tiny", "--variant", "rcp-default", option, value], "duplicate",
                )

    def test_selector_rejects_repeated_case_and_variant_ids(self):
        for kind in ("case", "variant"):
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, "duplicate"):
                run._select([{"id": "copy"}], ["copy", "copy"], kind)

    def test_baseline_requires_rcp_tool_before_fixture_work(self):
        self._assert_rejected_before_fixture_work(
            [{"id": "rcp-default", "tool": "rsync", "args": ["-a"], "processes": 1}],
            ["--case", "tiny", "--variant", "rcp-default"], "baseline.*rcp", baseline=True,
        )

    def test_blank_runner_label_is_rejected_before_creating_output(self):
        for label in ("", "  ", "\t\n"):
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                output = root / "out"
                diagnostic = io.StringIO()
                with contextlib.redirect_stderr(diagnostic), self.assertRaises(SystemExit) as failure:
                    run.main(["--output", str(output), "--runner-label", label, "--bin-dir", str(root), "--source-root", str(root), "--destination-root", str(root)])
                self.assertEqual(failure.exception.code, 2)
                self.assertIn("--runner-label", diagnostic.getvalue())
                self.assertFalse(output.exists())
                self.assertEqual(list(root.iterdir()), [])

    def test_runner_label_trims_surrounding_whitespace(self):
        args = run._arguments(["--output", "/tmp/unused-benchmark-out", "--runner-label", "  Depot runner  "])
        self.assertEqual(args.runner_label, "Depot runner")

    def test_manifest_rejects_unknown_fields_and_duplicate_ids(self):
        with tempfile.TemporaryDirectory() as root:
            manifest = Path(root) / "cases.json"
            manifest.write_text(json.dumps({"schema_version": 1, "cases": [{"id": "small", "directory_widths": [2], "files_per_leaf": 1, "file_size_bytes": 1, "surprise": True}], "variants": [{"id": "copy", "tool": "rsync", "args": ["-a"], "processes": 1}]}))
            with self.assertRaisesRegex(ValueError, "surprise"):
                run.load_manifest(manifest)
            manifest.write_text(json.dumps({"schema_version": 1, "cases": [{"id": "small", "directory_widths": [2], "files_per_leaf": 1, "file_size_bytes": 1}] * 2, "variants": [{"id": "copy", "tool": "rsync", "args": ["-a"], "processes": 1}]}))
            with self.assertRaisesRegex(ValueError, "duplicate"):
                run.load_manifest(manifest)

    def test_expected_counts_include_all_directory_levels(self):
        self.assertEqual(run.expected_counts({"directory_widths": [2, 3], "files_per_leaf": 4, "file_size_bytes": 5}), {"directories": 8, "files": 24, "bytes": 120})

    def test_manifest_rejects_boolean_schema_version(self):
        with tempfile.TemporaryDirectory() as root:
            manifest = Path(root) / "cases.json"
            manifest.write_text(json.dumps({"schema_version": True, "cases": [{"id": "small", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1}], "variants": [{"id": "copy", "tool": "rsync", "args": ["-a"], "processes": 1}]}))
            with self.assertRaisesRegex(ValueError, "schema_version"):
                run.load_manifest(manifest)

    def test_partitioned_rsync_commands_cover_disjoint_top_level_directories(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "source"
            destination = Path(root) / "destination"
            source.mkdir()
            destination.mkdir()
            for index in range(3):
                (source / f"dir-{index}").mkdir()
            commands = run.plan_commands({"id": "rsync-a-10", "tool": "rsync", "args": ["-a"], "processes": 10}, source, destination, {"rsync": Path("/usr/bin/rsync")}, "local")
            self.assertEqual(len(commands), 3)
            self.assertEqual({command[-2] for command in commands}, {str(path) for path in source.iterdir()})
            self.assertEqual({command[-1] for command in commands}, {str(destination)})

    def test_parallel_commands_never_exceed_process_limit(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "source"
            source.mkdir()
            destination = Path(root) / "destination"
            destination.mkdir()
            for index in range(4):
                (source / f"dir-{index}").mkdir()
            with self.assertRaisesRegex(ValueError, "processes"):
                run.plan_commands({"id": "parallel", "tool": "rsync", "args": ["-a"], "processes": 2}, source, destination, {"rsync": Path("/usr/bin/rsync")}, "local")

    def test_partitioned_rcp_uses_explicit_child_destinations(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "source"
            source.mkdir()
            destination = Path(root) / "destination"
            destination.mkdir()
            for index in range(2):
                (source / f"dir-{index}").mkdir()
            commands = run.plan_commands({"id": "parallel", "tool": "rcp", "args": ["--summary"], "processes": 2}, source, destination, {"rcp": Path("/tmp/rcp")}, "local")
            self.assertEqual({command[-1] for command in commands}, {str(destination / "dir-0"), str(destination / "dir-1")})

    def test_nonzero_parallel_child_fails_after_reaping_every_child(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / "done"
            commands = [[sys.executable, "-c", "import sys; sys.exit(7)"], [sys.executable, "-c", f"from pathlib import Path; import time; time.sleep(.15); Path({str(marker)!r}).write_text('done')"]]
            outcome = run.execute_commands(commands, Path(root) / "logs", 2)
            self.assertFalse(outcome["ok"])
            self.assertEqual(outcome["exit_codes"], [7, 0])
            self.assertTrue(marker.exists())

    def test_timeout_terminates_all_process_groups(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / "survived"
            commands = [[sys.executable, "-c", f"import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',\"import time; time.sleep(2); open({str(marker)!r},'w').write('x')\"]); time.sleep(2)"]]
            outcome = run.execute_commands(commands, Path(root) / "logs", .1)
            self.assertFalse(outcome["ok"])
            self.assertTrue(outcome["timed_out"])
            import time
            time.sleep(.3)
            self.assertFalse(marker.exists())

    def test_interrupted_launch_terminates_started_child(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / "survived"
            commands = [[sys.executable, "-c", f"import time; time.sleep(.5); open({str(marker)!r},'w').write('x')"], [sys.executable, "-c", "pass"]]
            original = run.subprocess.Popen
            launches = 0
            def interrupt_second(*args, **kwargs):
                nonlocal launches
                launches += 1
                if launches == 2:
                    raise KeyboardInterrupt
                return original(*args, **kwargs)
            with mock.patch.object(run.subprocess, "Popen", side_effect=interrupt_second):
                with self.assertRaises(KeyboardInterrupt):
                    run.execute_commands(commands, Path(root) / "logs", 2)
            import time
            time.sleep(.6)
            self.assertFalse(marker.exists())

    def test_timeout_kills_descendants_after_leader_exits(self):
        with tempfile.TemporaryDirectory() as root:
            marker = Path(root) / "orphan"
            leader = f"import subprocess,sys; subprocess.Popen([sys.executable,'-c',\"import time; time.sleep(.5); open({str(marker)!r},'w').write('x')\"] )"
            commands = [[sys.executable, "-c", leader], [sys.executable, "-c", "import time; time.sleep(2)"]]
            outcome = run.execute_commands(commands, Path(root) / "logs", .1)
            self.assertTrue(outcome["timed_out"])
            import time
            time.sleep(.6)
            self.assertFalse(marker.exists())

    def test_signal_during_spawn_reaps_new_child(self):
        for signum, exception in ((signal.SIGINT, KeyboardInterrupt), (signal.SIGTERM, InterruptedError)):
            with self.subTest(signum=signum), tempfile.TemporaryDirectory() as root:
                child = None
                original_popen = run.subprocess.Popen
                previous_handler = signal.getsignal(signum)
                def interrupt(_signum, _frame):
                    raise exception(signum)
                def spawn_then_signal(*args, **kwargs):
                    nonlocal child
                    child = original_popen(*args, **kwargs)
                    os.kill(os.getpid(), signum)
                    return child
                signal.signal(signum, interrupt)
                try:
                    with mock.patch.object(run.subprocess, "Popen", side_effect=spawn_then_signal):
                        with self.assertRaises(exception):
                            run.execute_commands([[sys.executable, "-c", "import time; time.sleep(2)"]], Path(root) / "logs", 3)
                    self.assertIsNotNone(child)
                    self.assertIsNotNone(child.returncode, "spawned process was not reaped")
                    with self.assertRaises(ProcessLookupError):
                        os.kill(child.pid, 0)
                finally:
                    signal.signal(signum, previous_handler)
                    if child is not None and child.poll() is None:
                        child.kill()
                        child.wait()

    def test_signal_handlers_remain_stable_until_timed_out_child_is_reaped(self):
        with tempfile.TemporaryDirectory() as root:
            spawned = []
            unsafe_transitions = []
            original_popen = run.subprocess.Popen
            original_signal = run.signal.signal
            def track_spawn(*args, **kwargs):
                child = original_popen(*args, **kwargs)
                spawned.append(child)
                return child
            def track_handler(number, handler):
                if spawned and any(child.returncode is None for child in spawned):
                    unsafe_transitions.append(number)
                return original_signal(number, handler)
            with mock.patch.object(run.subprocess, "Popen", side_effect=track_spawn), mock.patch.object(run.signal, "signal", side_effect=track_handler):
                outcome = run.execute_commands([[sys.executable, "-c", "import time; time.sleep(2)"]], Path(root) / "logs", .05)
            self.assertTrue(outcome["timed_out"])
            self.assertTrue(all(child.returncode is not None for child in spawned))
            self.assertEqual(unsafe_transitions, [])

    def test_spawned_child_has_unblocked_sigterm(self):
        with tempfile.TemporaryDirectory() as root:
            command = [sys.executable, "-c", "import signal,sys; sys.exit(signal.SIGTERM in signal.pthread_sigmask(signal.SIG_BLOCK, []))"]
            outcome = run.execute_commands([command], Path(root) / "logs", 2)
            self.assertTrue(outcome["ok"])
            self.assertEqual(outcome["exit_codes"], [0])

    def test_ignored_sigterm_remains_ignored_while_child_runs(self):
        with tempfile.TemporaryDirectory() as root:
            previous = signal.getsignal(signal.SIGTERM)
            original_popen = run.subprocess.Popen
            def spawn_then_signal(*args, **kwargs):
                child = original_popen(*args, **kwargs)
                os.kill(os.getpid(), signal.SIGTERM)
                return child
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            try:
                with mock.patch.object(run.subprocess, "Popen", side_effect=spawn_then_signal):
                    outcome = run.execute_commands([[sys.executable, "-c", "pass"]], Path(root) / "logs", 2)
                self.assertTrue(outcome["ok"])
                self.assertEqual(signal.getsignal(signal.SIGTERM), signal.SIG_IGN)
            finally:
                signal.signal(signal.SIGTERM, previous)

    def test_tree_validation_detects_changed_content_and_extra_paths(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "source"
            destination = Path(root) / "destination"
            source.mkdir()
            destination.mkdir()
            (source / "a").write_bytes(b"ab")
            (destination / "a").write_bytes(b"cd")
            (destination / "extra").write_bytes(b"z")
            validation = run.validate_tree(source, destination)
            self.assertFalse(validation["ok"])
            self.assertIn("extra", validation["error"])

    def test_tree_scan_uses_one_nofollow_stat_per_path(self):
        with tempfile.TemporaryDirectory() as root:
            tree = Path(root) / "tree"
            child = tree / "child"
            child.mkdir(parents=True)
            leaf = child / "leaf"
            leaf.write_bytes(b"payload")
            original = Path.stat
            observations = []
            def checked_stat(path, *args, **kwargs):
                observations.append((Path(path), kwargs.get("follow_symlinks")))
                return original(path, *args, **kwargs)
            with mock.patch.object(Path, "stat", checked_stat):
                result = run.scan_tree(tree)
            self.assertEqual(result["counts"], {"directories": 1, "files": 1, "bytes": 7})
            self.assertEqual(observations, [(tree, False), (child, False), (leaf, False)])

    def test_tree_scan_rejects_symlink_and_special_entries(self):
        with tempfile.TemporaryDirectory() as root:
            tree = Path(root) / "tree"
            tree.mkdir()
            symlink = tree / "link"
            symlink.symlink_to(Path(root))
            with self.assertRaisesRegex(ValueError, "symlink"):
                run.scan_tree(tree)
            symlink.unlink()
            os.mkfifo(tree / "fifo")
            with self.assertRaisesRegex(ValueError, "special"):
                run.scan_tree(tree)

    def test_series_id_ignores_commit_and_scratch_but_tracks_meaningful_flags(self):
        case = {"id": "small", "directory_widths": [2], "files_per_leaf": 1, "file_size_bytes": 1}
        variant = {"id": "copy", "tool": "rcp", "args": ["--summary"], "processes": 1}
        environment = {"kernel": "Linux", "cpu_model": "Example", "filesystem": {"source": {"filesystem_type": "ext4", "mount_options": ["rw"], "mount_source": "/dev/a"}, "destination": {"filesystem_type": "xfs", "mount_options": ["rw"], "mount_source": "/dev/b"}}}
        tools = {"rsync": {"version": "3.2", "sha256": "a" * 64}}
        first = run.series_id(case, variant, "source-warm", "local", "runner", environment, tools)
        self.assertEqual(first, run.series_id(case, variant, "source-warm", "local", "runner", environment, tools))
        self.assertNotEqual(first, run.series_id(case, {**variant, "args": ["--summary", "--preserve-settings=all"]}, "source-warm", "local", "runner", environment, tools))
        self.assertNotEqual(first, run.series_id(case, variant, "linux-drop-caches", "local", "runner", environment, tools))
        self.assertEqual(first, run.series_id({**case, "description": "new prose"}, {**variant, "description": "new prose"}, "source-warm", "local", "runner", environment, tools))

    def test_series_id_ignores_overlay_allocation_paths_and_filegen_release(self):
        case = {"id": "small", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1}
        variant = {"id": "copy", "tool": "rcp", "args": ["--summary"], "processes": 1}
        def environment(device, lower, upper):
            mount = {"filesystem_type": "overlay", "mount_source": device, "mountpoint": upper, "mount_options": ["rw", f"lowerdir={lower}", f"upperdir={upper}", f"workdir={upper}/work", "relatime"]}
            return {"kernel": "Linux", "filesystem": {"source": mount, "destination": mount}}
        first_tools = {"filegen": {"version": "0.41.0", "sha256": "a" * 64}, "rcp": {"version": "0.41.0", "sha256": "a" * 64}, "rcpd": {"version": "0.41.0", "sha256": "a" * 64}, "rsync": {"version": "3.4", "sha256": "b" * 64}}
        later_tools = {"filegen": {"version": "0.42.0", "sha256": "c" * 64}, "rcp": {"version": "0.42.0", "sha256": "c" * 64}, "rcpd": {"version": "0.42.0", "sha256": "c" * 64}, "rsync": {"version": "3.4", "sha256": "b" * 64}}
        first = run.series_id(case, variant, "source-warm", "local", "runner", environment("/dev/loop1", "/layers/a", "/tmp/job1"), first_tools, storage_ids={"source": "ci-root", "destination": "ci-root"})
        changed_allocation = run.series_id(case, variant, "source-warm", "local", "runner", environment("/dev/loop77", "/layers/b", "/tmp/job2"), later_tools, storage_ids={"source": "ci-root", "destination": "ci-root"})
        self.assertEqual(first, changed_allocation)
        semantic_change = environment("/dev/loop77", "/layers/b", "/tmp/job2")
        semantic_change["filesystem"]["source"]["mount_options"] = ["ro", "relatime", "lowerdir=/layers/b", "upperdir=/tmp/job2", "workdir=/tmp/job2/work"]
        self.assertNotEqual(first, run.series_id(case, variant, "source-warm", "local", "runner", semantic_change, later_tools, storage_ids={"source": "ci-root", "destination": "ci-root"}))
        self.assertNotEqual(first, run.series_id(case, variant, "source-warm", "local", "runner", environment("/dev/loop77", "/layers/b", "/tmp/job2"), {**later_tools, "rsync": {"version": "3.5", "sha256": "d" * 64}}, storage_ids={"source": "ci-root", "destination": "ci-root"}))

    def test_series_id_uses_rsync_digest_without_executable_path(self):
        case = {"id": "tiny", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1}
        variant = {"id": "rsync-a", "tool": "rsync", "args": ["-a"], "processes": 1}
        environment = {"kernel": "Linux", "filesystem": {"source": {"filesystem_type": "xfs", "mount_options": ["rw"]}, "destination": {"filesystem_type": "xfs", "mount_options": ["rw"]}}}
        first = run.series_id(case, variant, "source-warm", "local", "runner", environment, {"rsync": {"version": "3.4", "sha256": "a" * 64, "path": "/usr/bin/rsync"}}, {"source": "disk-a", "destination": "disk-b"})
        relocated = run.series_id(case, variant, "source-warm", "local", "runner", environment, {"rsync": {"version": "3.4", "sha256": "a" * 64, "path": "/nix/store/rsync"}}, {"source": "disk-a", "destination": "disk-b"})
        rebuilt = run.series_id(case, variant, "source-warm", "local", "runner", environment, {"rsync": {"version": "3.4", "sha256": "b" * 64, "path": "/nix/store/rsync"}}, {"source": "disk-a", "destination": "disk-b"})
        self.assertEqual(first, relocated)
        self.assertNotEqual(first, rebuilt)

    def test_mountinfo_decodes_nested_paths_and_literal_escapes_once(self):
        with tempfile.TemporaryDirectory() as root:
            parent = Path(root) / "space name"
            nested = parent / "literal\\040 and tab\tname\nline"
            nested.mkdir(parents=True)
            def encoded(path):
                return str(path).replace("\\", r"\134").replace(" ", r"\040").replace("\t", r"\011").replace("\n", r"\012")
            mountinfo = f"1 0 0:1 / {encoded(parent)} rw - ext4 /dev/parent rw\n2 1 0:2 / {encoded(nested)} rw - xfs " + r"server:\134040literal\040name\012line\011tab" + " rw\n"
            with mock.patch.object(run, "_read", return_value=mountinfo):
                mount = run._mount(nested / "child")
            self.assertEqual(mount["mountpoint"], str(nested))
            self.assertEqual(mount["mount_source"], r"server:\040literal name" + "\nline\ttab")
            self.assertEqual(mount["filesystem_type"], "xfs")

    def test_loopback_rsync_pulls_from_localhost(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "source"
            source.mkdir()
            destination = Path(root) / "destination"
            command = run.plan_commands({"id": "rsync-a", "tool": "rsync", "args": ["-a"], "processes": 1}, source, destination, {"rsync": Path("/usr/bin/rsync")}, "loopback")[0]
            self.assertEqual(command[-2], f"localhost:{source}/")

    def test_failed_cache_preparation_persists_failed_result(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            output = root / "out"
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"schema_version": 1, "cases": [{"id": "tiny", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1}], "variants": [{"id": "rsync-a", "tool": "rsync", "args": ["-a"], "processes": 1}]}))
            binary = root / "bin"
            binary.mkdir()
            filegen = binary / "filegen"
            filegen.write_text("#!/usr/bin/env python3\nimport pathlib,sys\nif '--version' in sys.argv: print('filegen 1'); sys.exit(0)\np=pathlib.Path(sys.argv[1])/'filegen'/'dir'; p.mkdir(parents=True); (p/'file').write_bytes(b'x')\n")
            filegen.chmod(0o755)
            copy_marker = root / "copy-started"
            rsync = binary / "rsync"
            rsync.write_text(f"#!/usr/bin/env python3\nimport pathlib,sys\nif '--version' in sys.argv: print('rsync 1'); sys.exit(0)\npathlib.Path({str(copy_marker)!r}).write_text('started')\n")
            rsync.chmod(0o755)
            with mock.patch.object(run.shutil, "which", return_value=str(rsync)), mock.patch.object(run, "_prepare_cache", side_effect=RuntimeError("cache unavailable")):
                with self.assertRaisesRegex(RuntimeError, "cache unavailable"):
                    run.main(["--manifest", str(manifest), "--case", "tiny", "--variant", "rsync-a", "--output", str(output), "--bin-dir", str(binary), "--repetitions", "1"])
            record = json.loads((output / "results.json").read_text())
            self.assertEqual(record["status"], "failed")
            self.assertEqual(record["trials"][0]["status"], "failed")
            self.assertFalse(copy_marker.exists())
            self.assertTrue((output / "summary.md").exists())
            self.assertTrue((output / "logs").is_dir())

    def test_ambiguous_case_variant_names_have_distinct_trial_paths(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            binary = root / "bin"
            binary.mkdir()
            filegen = binary / "filegen"
            filegen.write_text("#!/usr/bin/env python3\nimport pathlib,sys\nif '--version' in sys.argv: print('filegen 1'); sys.exit(0)\np=pathlib.Path(sys.argv[1])/'filegen'/'dir'; p.mkdir(parents=True); (p/'file').write_bytes(b'x')\n")
            filegen.chmod(0o755)
            rsync = binary / "rsync"
            rsync.write_text("#!/usr/bin/env python3\nimport pathlib,shutil,sys\nif '--version' in sys.argv: print('rsync 1'); sys.exit(0)\nshutil.copytree(sys.argv[-2].rstrip('/'),sys.argv[-1].rstrip('/'),dirs_exist_ok=True)\n")
            rsync.chmod(0o755)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"schema_version": 1, "cases": [{"id": identifier, "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1} for identifier in ("a-b", "a")], "variants": [{"id": identifier, "tool": "rsync", "args": ["-a"], "processes": 1} for identifier in ("c", "b-c")]}))
            output = root / "out"
            with mock.patch.object(run.shutil, "which", return_value=str(rsync)):
                result = run.main(["--manifest", str(manifest), "--case", "a-b", "--case", "a", "--variant", "c", "--variant", "b-c", "--bin-dir", str(binary), "--cache", "uncontrolled", "--repetitions", "1", "--source-root", str(root), "--destination-root", str(root), "--output", str(output)])
            self.assertEqual(result["status"], "complete")
            self.assertEqual(len(result["trials"]), 4)
            self.assertTrue((output / "logs" / "a-b" / "c" / "1" / "0.stdout.log").is_file())
            self.assertTrue((output / "logs" / "a" / "b-c" / "1" / "0.stdout.log").is_file())

    def test_rebuilt_rsync_with_same_version_starts_new_series(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            binary = root / "bin"
            binary.mkdir()
            filegen = binary / "filegen"
            filegen.write_text("#!/usr/bin/env python3\nimport pathlib,sys\nif '--version' in sys.argv: print('filegen 1'); sys.exit(0)\np=pathlib.Path(sys.argv[1])/'filegen'/'dir'; p.mkdir(parents=True); (p/'file').write_bytes(b'x')\n")
            filegen.chmod(0o755)
            rsync = binary / "rsync"
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"schema_version": 1, "cases": [{"id": "tiny", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1}], "variants": [{"id": "rsync-a", "tool": "rsync", "args": ["-a"], "processes": 1}]}))
            results = []
            for build in (1, 2):
                rsync.write_text("#!/usr/bin/env python3\nimport pathlib,shutil,sys\nif '--version' in sys.argv: print('rsync 1'); sys.exit(0)\nshutil.copytree(sys.argv[-2].rstrip('/'),sys.argv[-1].rstrip('/'),dirs_exist_ok=True)\n" + f"# build {build}\n")
                rsync.chmod(0o755)
                with mock.patch.object(run.shutil, "which", return_value=str(rsync)):
                    results.append(run.main(["--manifest", str(manifest), "--case", "tiny", "--variant", "rsync-a", "--bin-dir", str(binary), "--cache", "uncontrolled", "--repetitions", "1", "--source-root", str(root), "--destination-root", str(root), "--source-storage-id", "test-source", "--destination-storage-id", "test-destination", "--output", str(root / f"out-{build}")]))
            self.assertEqual([result["status"] for result in results], ["complete", "complete"])
            self.assertEqual(results[0]["tools"]["rsync"]["version"], results[1]["tools"]["rsync"]["version"])
            self.assertNotEqual(results[0]["tools"]["rsync"]["sha256"], results[1]["tools"]["rsync"]["sha256"])
            self.assertNotEqual(results[0]["summaries"][0]["series_id"], results[1]["summaries"][0]["series_id"])

    def test_later_case_summary_failure_preserves_only_complete_case(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            binary = root / "bin"
            binary.mkdir()
            filegen = binary / "filegen"
            filegen.write_text("#!/usr/bin/env python3\nimport pathlib,sys\nif '--version' in sys.argv: print('filegen 1'); sys.exit(0)\np=pathlib.Path(sys.argv[1])/'filegen'/'dir'; p.mkdir(parents=True); (p/'file').write_bytes(b'x')\n")
            filegen.chmod(0o755)
            rsync = binary / "rsync"
            rsync.write_text("#!/usr/bin/env python3\nimport shutil,sys\nif '--version' in sys.argv: print('rsync 1'); sys.exit(0)\nshutil.copytree(sys.argv[-2].rstrip('/'),sys.argv[-1].rstrip('/'),dirs_exist_ok=True)\n")
            rsync.chmod(0o755)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"schema_version": 1, "cases": [{"id": case, "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1} for case in ("first", "later")], "variants": [{"id": variant, "tool": "rsync", "args": ["-a"], "processes": 1} for variant in ("copy-a", "copy-b")]}))
            original_series_id = run.series_id
            calls = 0
            def interrupt_second_later(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 4:
                    raise RuntimeError("summary interrupted")
                return original_series_id(*args, **kwargs)
            output = root / "out"
            with mock.patch.object(run.shutil, "which", return_value=str(rsync)), mock.patch.object(run, "series_id", side_effect=interrupt_second_later):
                with self.assertRaisesRegex(RuntimeError, "summary interrupted"):
                    run.main(["--manifest", str(manifest), "--case", "first", "--case", "later", "--variant", "copy-a", "--variant", "copy-b", "--bin-dir", str(binary), "--cache", "uncontrolled", "--repetitions", "1", "--source-root", str(root), "--destination-root", str(root), "--output", str(output)])
            record = report.parse_result((output / "results.json").read_text())
            self.assertEqual(record["status"], "failed")
            self.assertEqual({item["case_id"] for item in record["summaries"]}, {"first"})
            self.assertEqual(len(record["summaries"]), 2)
            scratch = Path(record["context"]["failure_artifacts"]["source_scratch"])
            self.assertFalse((scratch / "first").exists())
            self.assertTrue((scratch / "later" / "filegen").is_dir())

    def test_detached_checkout_has_valid_revision(self):
        with mock.patch.object(run, "_git", side_effect=["abc123", "", ""]):
            self.assertEqual(run._revision(), {"commit": "abc123", "branch": None, "dirty": False})

    def test_failed_git_queries_leave_revision_unknown(self):
        failed = subprocess.CompletedProcess(["git"], 1, "", "")
        with mock.patch.object(run.subprocess, "run", return_value=failed):
            self.assertEqual(run._revision(), {"commit": None, "branch": None, "dirty": None})

    def test_missing_git_executable_leaves_revision_unknown(self):
        with mock.patch.object(run.subprocess, "run", side_effect=FileNotFoundError("git")):
            self.assertEqual(run._revision(), {"commit": None, "branch": None, "dirty": None})

    def test_help_and_explicit_binary_dir_do_not_discover_default(self):
        with mock.patch.object(run, "_default_bin_dir", side_effect=AssertionError("eager discovery")):
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as help_exit:
                run._arguments(["--help"])
            self.assertEqual(help_exit.exception.code, 0)
            args = run._arguments(["--output", "/tmp/benchmark-out", "--bin-dir", "/tmp/prepared-bin"])
            self.assertEqual(args.bin_dir, Path("/tmp/prepared-bin"))

    def test_tool_without_version_output_has_clear_error(self):
        with tempfile.TemporaryDirectory() as root:
            binary = Path(root) / "silent"
            binary.write_text("#!/bin/sh\nexit 0\n")
            binary.chmod(0o755)
            with self.assertRaisesRegex(ValueError, "version output"):
                run._tool(binary)

    def test_tool_preserves_symlink_name_for_multicall_binaries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "multicall"
            binary.write_text("#!/usr/bin/env python3\nimport pathlib,sys\nprint(pathlib.Path(sys.argv[0]).name + ' 1')\n")
            binary.chmod(0o755)
            alias = root / "cp"
            alias.symlink_to(binary)
            tool = run._tool(alias)
            self.assertEqual(tool["path"], str(alias))
            self.assertEqual(tool["version"], "cp 1")
            self.assertEqual(tool["sha256"], run._tool(binary)["sha256"])

    def test_log_open_failure_closes_already_open_stdout(self):
        with tempfile.TemporaryDirectory() as root:
            original = Path.open
            opened = []
            def fail_stderr(path, *args, **kwargs):
                if Path(path).name == "0.stderr.log":
                    raise OSError("stderr unavailable")
                handle = original(path, *args, **kwargs)
                if Path(path).name == "0.stdout.log":
                    opened.append(handle)
                return handle
            with mock.patch.object(Path, "open", fail_stderr):
                with self.assertRaisesRegex(OSError, "stderr unavailable"):
                    run.execute_commands([[sys.executable, "-c", "pass"]], Path(root) / "logs", 1)
            self.assertEqual(len(opened), 1)
            self.assertTrue(opened[0].closed)

    def test_source_warm_aborts_when_sync_times_out(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root)
            (source / "file").write_bytes(b"payload")
            with mock.patch.object(run.subprocess, "run", side_effect=subprocess.TimeoutExpired("sync", .1)):
                with self.assertRaises(subprocess.TimeoutExpired):
                    run._prepare_cache("source-warm", source, .1)

    def test_sigint_persists_failed_trial_and_stops_child(self):
        self._assert_signal_persists_failed_trial(signal.SIGINT)

    def test_sigterm_persists_failed_trial_and_stops_child(self):
        self._assert_signal_persists_failed_trial(signal.SIGTERM)

    def _assert_signal_persists_failed_trial(self, signum):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            binary = root / "bin"
            binary.mkdir()
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"schema_version": 1, "cases": [{"id": "tiny", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1}], "variants": [{"id": "rsync-a", "tool": "rsync", "args": ["-a"], "processes": 1}]}))
            filegen = binary / "filegen"
            filegen.write_text("#!/usr/bin/env python3\nimport pathlib,sys\nif '--version' in sys.argv: print('filegen 1'); sys.exit(0)\np=pathlib.Path(sys.argv[1])/'filegen'/'dir'; p.mkdir(parents=True); (p/'file').write_bytes(b'x')\n")
            filegen.chmod(0o755)
            started = root / "started"
            survived = root / "survived"
            rsync = binary / "rsync"
            rsync.write_text(f"#!/usr/bin/env python3\nimport pathlib,sys,time\nif '--version' in sys.argv: print('rsync 1'); sys.exit(0)\npathlib.Path({str(started)!r}).write_text('started'); time.sleep(2); pathlib.Path({str(survived)!r}).write_text('survived')\n")
            rsync.chmod(0o755)
            output = root / "out"
            env = {**os.environ, "PATH": str(binary) + os.pathsep + os.environ["PATH"]}
            process = subprocess.Popen([sys.executable, "-m", "benchmarks.run", "--manifest", str(manifest), "--case", "tiny", "--variant", "rsync-a", "--bin-dir", str(binary), "--cache", "uncontrolled", "--repetitions", "1", "--source-root", str(root), "--destination-root", str(root), "--output", str(output)], cwd=Path(__file__).resolve().parent.parent, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                deadline = time.monotonic() + 5
                while not started.exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(.02)
                self.assertTrue(started.exists(), "copy child did not start")
                process.send_signal(signum)
                self.assertNotEqual(process.wait(timeout=5), 0)
                result = json.loads((output / "results.json").read_text())
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["trials"][0]["status"], "failed")
                self.assertTrue(result["context"]["failure_artifacts"]["destination_scratch"])
                self.assertFalse(survived.exists())
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    unittest.main()
