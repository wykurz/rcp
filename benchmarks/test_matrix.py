"""Matrix runs measure independent cells: one failure or hang never stops the others."""

import contextlib
import errno
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import unittest
from unittest import mock

from benchmarks import cache, matrix, run, streaming

FILEGEN = r'''#!/usr/bin/env python3
import itertools, os, pathlib, sys
if "--version" in sys.argv:
    print("filegen 1")
    sys.exit(0)
root, widths = pathlib.Path(sys.argv[1]), [int(item) for item in sys.argv[2].split(",")]
files, size = int(sys.argv[3]), int(sys.argv[4])
if os.environ.get("FAKE_FILEGEN_FAIL") == root.name:
    print("no space left", file=sys.stderr)
    sys.exit(7)
slow = os.environ.get("FAKE_FILEGEN_SLEEP", "=").split("=")
if slow[0] == root.name:
    import time
    time.sleep(float(slow[1]))
for index in itertools.product(*[range(width) for width in widths]):
    leaf = root.joinpath("filegen", *map(str, index))
    leaf.mkdir(parents=True)
    for number in range(files):
        (leaf / f"file{number}").write_bytes(b"x" * size)
'''

# what the stand-in rcp reports about itself, and what the runner records of it
REPORTED = dict(walltime_ms=7, cpu_time_user_ms=1250, cpu_time_kernel_ms=500, peak_rss_bytes=2 * 1024 * 1024 + 512,
                voluntary_context_switches=3, involuntary_context_switches=4, minor_page_faults=5,
                major_page_faults=6)
RECORDED = dict(user_seconds=1.25, system_seconds=.5, max_rss_kib=2048, voluntary_switches=3,
                involuntary_switches=4, minor_faults=5, major_faults=6, exit_code=0)

RCP = r'''#!/usr/bin/env python3
import json, os, pathlib, shutil, sys, time
old = os.environ.get("FAKE_RCP_BUILD") == "old"
if old and any(item.startswith("--output-format") for item in sys.argv[1:]):
    # a build from before the option existed: its argument parser refuses it, --version or not
    print("error: unexpected argument '--output-format' found", file=sys.stderr)
    sys.exit(2)
if "--version" in sys.argv:
    print("rcp 1")
    sys.exit(0)
if not old and "--output-format=json" not in sys.argv[1:-2]:
    sys.exit("the runner reads the JSON summary")
source, destination = pathlib.Path(sys.argv[-2]), pathlib.Path(sys.argv[-1])
plan = dict(item.split("=") for item in os.environ.get("FAKE_RCP", "").split(",") if item)
behaviour = plan.get(source.parent.name, "ok")
if behaviour.startswith("sleep"):
    time.sleep(float(behaviour[5:]))
if behaviour.startswith("flood"):
    # one line per file that could not be copied, as a copy that fails everywhere would write
    for number in range(int(behaviour[5:])):
        sys.stderr.write("line %07d walk child failed with: no space left on device, file %07d\n" % (number, number))
    sys.exit(3)
shutil.copytree(source, destination)
files = sum(len(names) for _, _, names in os.walk(destination))
copied = files - (1 if behaviour == "miscount" else 0)
if old:
    # the text summary is all such a build has
    print(f"copy:\n-----\nfiles copied: {copied}\nfiles unchanged: 0\n\nwalltime : 1.00ms")
    sys.exit(3 if behaviour == "exit" else 0)
summary = dict(files_copied=copied, files_unchanged=1 if behaviour == "unchanged" else 0, bytes_copied=files * 3)
if behaviour == "boolean":
    summary["files_copied"] = True
if behaviour != "nostats":
    summary.update(REPORTED)
if behaviour == "zerorss":
    summary["peak_rss_bytes"] = 0
if behaviour == "separator":
    # characters that some readers take for the end of a line, inside a string
    summary["note"] = "one two three\x85four"
if behaviour == "binary":
    # log lines that are not text, before the summary
    sys.stdout.buffer.write(bytes([255, 254]) + b"\n")
    sys.stdout.flush()
if behaviour == "big":
    # more log than is kept: its middle is cut, and the summary is at its end
    for number in range(40000):
        print("line %07d " % number + "x" * 70)
if behaviour == "text":
    # a build from before the option existed would not get this far; one that ignores it prints text
    print(f"files copied: {files}\nfiles unchanged: 0")
elif behaviour == "list":
    print(json.dumps([summary]))
elif behaviour == "nested":
    print("[" * 200000)
else:
    print("a line of the log")
    sys.stdout.flush()
    sys.stdout.buffer.write(json.dumps(summary, ensure_ascii=False).encode() + b"\n")
sys.exit(3 if behaviour == "exit" else 0)
'''.replace("REPORTED", repr(REPORTED))


class MatrixTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for name, text in (("filegen", FILEGEN), ("rcp", RCP), ("rcpd", "#!/bin/sh\necho 'rcpd 1'\n")):
            (self.bin / name).write_text(text)
            (self.bin / name).chmod(0o755)
        self.manifest = self.root / "manifest.json"
        self.manifest.write_text(json.dumps(dict(schema_version=1, cases=[
            dict(id=name, directory_widths=[2], files_per_leaf=2, file_size_bytes=3)
            for name in ("alpha", "beta", "gamma")],
            variants=[dict(id="rcp-default", tool="rcp", args=["--summary"], processes=1)])))
        for side in ("source", "destination"):
            (self.root / side).mkdir()

    def run_matrix(self, *extra, cells=("alpha:local", "beta:local", "gamma:local"), environment=None):
        arguments = ["--manifest", str(self.manifest), "--bin-dir", str(self.bin),
                     "--source-root", str(self.root / "source"), "--destination-root", str(self.root / "destination"),
                     "--cache", "uncontrolled", "--repetitions", "2", "--output", str(self.root / "out")]
        for cell in cells:
            arguments += ["--cell", cell]
        with mock.patch.dict(os.environ, environment or {}), contextlib.redirect_stdout(io.StringIO()):
            record = matrix.main([*arguments, *extra])
        saved = json.loads((self.root / "out/results.json").read_text())
        self.assertEqual(saved, json.loads(json.dumps(record)))
        matrix.validate(saved)
        return saved

    def outcomes(self, record):
        return [(cell["case_id"], cell["outcome"]["status"]) for cell in record["cells"]]

    def test_every_cell_completes_without_walking_any_tree(self):
        with mock.patch.object(run, "scan_tree", side_effect=AssertionError("tree scan")), \
                mock.patch.object(run, "validate_tree", side_effect=AssertionError("tree scan")), \
                mock.patch.object(streaming, "scan_tree", side_effect=AssertionError("tree scan")), \
                mock.patch.object(streaming, "validate_tree", side_effect=AssertionError("tree scan")):
            record = self.run_matrix()
        self.assertEqual(record["schema"], "rcp-benchmark-matrix-v1")
        self.assertEqual(record["status"], "complete")
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "complete"), ("gamma", "complete")])
        self.assertEqual(record["context"]["verification"], {"policy": "exit-status-summary-v1"})
        for cell in record["cells"]:
            self.assertEqual(cell["scenario"], "local")
            self.assertEqual([copy["status"] for copy in cell["copies"]], ["ok", "ok"])
            # only what rcp's summary was checked for is recorded: its file counts
            self.assertEqual([copy["copy_summary"] for copy in cell["copies"]],
                             [dict(files_copied=4, files_unchanged=0)] * 2)
            # and what rcp reports about itself, in the units of every other record
            self.assertEqual([copy["resources"] for copy in cell["copies"]],
                             [dict(status="complete", metrics=RECORDED)] * 2)
            for copy in cell["copies"]:
                # the summary is asked for as JSON; nothing wraps the command
                self.assertEqual(copy["command"][:3], [str(self.bin / "rcp"), "--summary", "--output-format=json"])
                self.assertEqual(len(copy["command"]), 5)
        self.assertEqual(record["context"]["summary_format"], "json")
        self.assertEqual(record["context"]["resources"], dict(policy="rcp-output-json-v1"))
        self.assertNotIn("local_resources", record["context"])
        for cell in record["cells"]:
            samples = [copy["elapsed_seconds"] for copy in cell["copies"]]
            self.assertEqual(cell["summary"]["samples"], samples)
            self.assertEqual(cell["summary"]["minimum"], min(samples))
            self.assertRegex(cell["cell_id"], r"^[0-9a-f]{64}$")
        self.assertEqual(len({cell["cell_id"] for cell in record["cells"]}), 3)
        # fixtures and destinations are removed as the run goes, so later cells have the space
        self.assertEqual(list((self.root / "source").iterdir()), [])
        self.assertEqual(list((self.root / "destination").iterdir()), [])
        self.assertIn("| alpha | local | complete |", (self.root / "out/summary.md").read_text())

    def test_a_failed_copy_fails_only_its_cell(self):
        record = self.run_matrix(environment={"FAKE_RCP": "beta=exit"})
        self.assertEqual(record["status"], "partial")
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "failed"), ("gamma", "complete")])
        beta = record["cells"][1]
        self.assertEqual(beta["outcome"]["stage"], "copy")
        # the reason says where the command's own words are, since they never leave the evidence
        # rcp writes what went wrong to its output; only its argument parser uses the error stream
        self.assertEqual(beta["outcome"]["message"],
                         "copy 1 failed with exit code 3; see 0.stdout.log and 0.stderr.log in logs/beta/local/1")
        for name in ("0.stdout.log", "0.stderr.log"):
            self.assertTrue((self.root / "out/logs/beta/local/1" / name).is_file())
        # a broken cell is not retried: its remaining copies are skipped
        self.assertEqual([copy["status"] for copy in beta["copies"]], ["failed"])
        # the stand-in printed a summary before it failed: nothing of it is recorded
        for key in ("copy_summary", "resources", "role_resources"):
            self.assertNotIn(key, beta["copies"][0])
        self.assertIsNone(beta["summary"])
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_a_summary_that_reports_too_few_files_fails_the_cell(self):
        record = self.run_matrix(environment={"FAKE_RCP": "alpha=miscount"})
        self.assertEqual(self.outcomes(record), [("alpha", "failed"), ("beta", "complete"), ("gamma", "complete")])
        self.assertEqual(record["cells"][0]["outcome"]["stage"], "summary")
        self.assertEqual(record["cells"][0]["outcome"]["message"],
                         "copy 1 reported an unexpected summary: files_copied is 3; expected 4")
        self.assertEqual(record["cells"][0]["copies"][0]["copy_summary"], dict(files_copied=3, files_unchanged=0))

    def test_a_hung_copy_times_out_and_later_cells_still_run(self):
        started = time.monotonic()
        record = self.run_matrix("--timeout", "4", environment={"FAKE_RCP": "beta=sleep60"})
        self.assertLess(time.monotonic() - started, 40)
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "timed_out"), ("gamma", "complete")])
        self.assertEqual(record["cells"][1]["outcome"]["stage"], "copy")
        self.assertEqual(record["cells"][1]["outcome"]["message"], "copy 1 did not finish within 4 s")
        self.assertEqual([copy["status"] for copy in record["cells"][1]["copies"]], ["timed_out"])
        self.assertEqual(record["status"], "partial")

    def test_a_failed_generation_fails_that_file_set_only(self):
        record = self.run_matrix(environment={"FAKE_FILEGEN_FAIL": "alpha"})
        self.assertEqual(self.outcomes(record), [("alpha", "failed"), ("beta", "complete"), ("gamma", "complete")])
        self.assertEqual(record["cells"][0]["outcome"]["stage"], "generation")
        self.assertEqual(record["cells"][0]["copies"], [])
        self.assertEqual(record["file_sets"][0]["generation"]["status"], "failed")
        self.assertEqual(record["file_sets"][1]["generation"]["status"], "ok")

    def test_a_copy_cut_by_the_end_of_the_allocation_is_not_a_failure(self):
        # alpha has several seconds to finish; beta then hangs into the end of the allocation
        deadline = time.monotonic_ns() + 8 * 10 ** 9
        with mock.patch.object(matrix, "DEADLINE_MARGIN", 1), mock.patch.object(matrix, "MINIMUM_COMMAND", .2):
            record = self.run_matrix("--timeout", "600", environment={
                "FAKE_RCP": "beta=sleep60", cache.WORKER_DEADLINE_ENV: str(deadline)})
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "interrupted"), ("gamma", "not_run")])
        self.assertEqual(record["cells"][1]["outcome"]["message"], "allocation ended during copy 1 of 2")
        self.assertEqual(record["cells"][2]["outcome"]["message"], "allocation ended before this cell")
        self.assertEqual(record["cells"][2]["copies"], [])
        self.assertEqual(record["status"], "partial")
        # removing millions of files could outlast what is left of the allocation, and the
        # machine's storage goes away with it: nothing is cleaned up once the allocation ends
        scratch, = (self.root / "source").iterdir()
        self.assertEqual([path.name for path in scratch.iterdir()], ["beta"])
        self.assertTrue(any((self.root / "destination").iterdir()))

    def test_a_stop_signal_during_the_capacity_check_ends_the_run(self):
        with mock.patch.object(matrix.capacity, "check_pair", side_effect=InterruptedError("SIGTERM")), \
                self.assertRaises(InterruptedError):
            self.run_matrix("--check-capacity")
        record = matrix.validate(json.loads((self.root / "out/results.json").read_text()))
        self.assertEqual((record["status"], record["error"]), ("failed", "SIGTERM"))
        self.assertEqual({status for _, status in self.outcomes(record)}, {"not_run"})

    def test_removal_is_a_command_with_a_time_limit_that_a_stop_signal_still_ends(self):
        tree = self.root / "tree"
        (tree / "leaf").mkdir(parents=True)
        (tree / "leaf/file").write_text("x")
        real = subprocess.run
        def stopping(command, *args, **kwargs):
            raise InterruptedError("SIGTERM")
        with mock.patch.object(matrix.subprocess, "run", side_effect=stopping), self.assertRaises(InterruptedError):
            matrix._remove(tree, 10)
        def slow(command, *args, **kwargs):
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        with mock.patch.object(matrix.subprocess, "run", side_effect=slow):
            self.assertEqual(matrix._remove(tree, 10), "timed_out")
        with mock.patch.object(matrix.subprocess, "run", side_effect=lambda command, *args, **kwargs:
                               real(["false"], *args, **kwargs)):
            self.assertEqual(matrix._remove(tree, 10), "failed")
        self.assertTrue(tree.exists())
        self.assertEqual(matrix._remove(tree, 10), "removed")
        self.assertFalse(tree.exists())
        self.assertEqual(matrix._remove(tree, 10), "removed")

    def test_each_copy_starts_with_the_previous_destination_and_file_set_gone(self):
        seen = []
        execute = run.execute_commands
        def copy(commands, *args, **kwargs):
            destination, source = Path(commands[0][-1]), Path(commands[0][-2])
            # what is on disk when a copy starts: other copies of this cell, and other file sets
            seen.append((sorted(path.name for path in destination.parent.iterdir()),
                         sorted(path.name for path in source.parent.parent.iterdir())))
            return execute(commands, *args, **kwargs)
        with mock.patch.object(run, "execute_commands", side_effect=copy):
            record = self.run_matrix()
        self.assertEqual(record["status"], "complete")
        # three 1 TB copies fit a destination only because each is removed before the next starts
        self.assertEqual(seen, [([], [case]) for case in ("alpha", "beta", "gamma") for _ in range(2)])
        self.assertNotIn("removal_failures", record)

    def test_a_removal_that_fails_is_counted_and_the_run_goes_on(self):
        real = subprocess.run
        def remove(command, *args, **kwargs):
            if command[0] == "rm" and "/alpha/" in command[-1] + "/":
                return real(["false"], *args, **kwargs)
            return real(command, *args, **kwargs)
        with mock.patch.object(matrix.subprocess, "run", side_effect=remove):
            record = self.run_matrix(cells=("alpha:local", "beta:local"))
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "complete")])
        # two destinations and the fixture of alpha could not be removed; the record says so
        self.assertEqual(record["removal_failures"], 3)

    def test_a_removal_cut_by_the_end_of_the_allocation_ends_the_run_in_order(self):
        real = subprocess.run
        def remove(command, *args, **kwargs):
            if command[0] == "rm":
                raise subprocess.TimeoutExpired(command, kwargs["timeout"])
            return real(command, *args, **kwargs)
        deadline = time.monotonic_ns() + 120 * 10 ** 9
        with mock.patch.object(matrix, "DEADLINE_MARGIN", 1), mock.patch.object(matrix, "MINIMUM_COMMAND", .2), \
                mock.patch.object(matrix.subprocess, "run", side_effect=remove):
            record = self.run_matrix("--timeout", "600", environment={cache.WORKER_DEADLINE_ENV: str(deadline)})
        # the first copy counted; its removal ran into the end of the allocation, so nothing else started
        self.assertEqual(self.outcomes(record), [("alpha", "interrupted"), ("beta", "not_run"), ("gamma", "not_run")])
        self.assertEqual(record["cells"][0]["outcome"]["message"], "allocation ended before copy 2 of 2")
        self.assertEqual([copy["status"] for copy in record["cells"][0]["copies"]], ["ok"])
        self.assertEqual(record["removal_failures"], 1)

    def test_a_stop_between_two_copies_says_which_copy_was_the_last(self):
        removed = []
        def remove(path, timeout, original=matrix._remove):
            removed.append(path)
            if len(removed) == 1:
                raise InterruptedError("SIGTERM")
            return original(path, timeout)
        with mock.patch.object(matrix, "_remove", side_effect=remove), self.assertRaises(InterruptedError):
            self.run_matrix(cells=("alpha:local",))
        record = matrix.validate(json.loads((self.root / "out/results.json").read_text()))
        cell, = record["cells"]
        self.assertEqual((cell["outcome"]["status"], cell["outcome"]["message"]),
                         ("interrupted", "run stopped after copy 1 of 2"))
        self.assertEqual([copy["status"] for copy in cell["copies"]], ["ok"])

    def test_results_are_saved_before_the_final_cleanup(self):
        removed = []
        def remove(path, timeout, original=matrix._remove):
            removed.append(Path(path).name)
            if Path(path).name.startswith("rcp-matrix-"):
                raise InterruptedError("SIGTERM")
            return original(path, timeout)
        with mock.patch.object(matrix, "_remove", side_effect=remove), self.assertRaises(InterruptedError):
            self.run_matrix(cells=("alpha:local",))
        record = matrix.validate(json.loads((self.root / "out/results.json").read_text()))
        self.assertEqual((record["status"], self.outcomes(record)), ("complete", [("alpha", "complete")]))
        self.assertNotIn("error", record)
        self.assertEqual(removed[-1][:11], "rcp-matrix-")

    def test_a_stop_during_generation_is_recorded_as_an_interrupted_generation(self):
        real = subprocess.run
        def launch(command, *args, **kwargs):
            if Path(command[1]).name == "beta":
                raise InterruptedError("SIGTERM")
            return real(command, *args, **kwargs)
        with mock.patch.object(matrix.subprocess, "run", side_effect=launch), self.assertRaises(InterruptedError):
            self.run_matrix()
        record = matrix.validate(json.loads((self.root / "out/results.json").read_text()))
        self.assertEqual([item["generation"]["status"] for item in record["file_sets"]], ["ok", "interrupted", "not_run"])
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "not_run"), ("gamma", "not_run")])
        self.assertEqual((record["status"], record["error"]), ("failed", "SIGTERM"))

    def test_generation_that_hangs_times_out_and_later_file_sets_still_run(self):
        record = self.run_matrix("--timeout", "4", environment={"FAKE_FILEGEN_SLEEP": "beta=60"})
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "timed_out"), ("gamma", "complete")])
        self.assertEqual((record["cells"][1]["outcome"]["stage"], record["cells"][1]["outcome"]["message"]),
                         ("generation", "file set generation did not finish within 4 s"))
        self.assertEqual(record["file_sets"][1]["generation"]["status"], "timed_out")

    def test_generation_cut_by_the_end_of_the_allocation_is_not_a_failure(self):
        deadline = time.monotonic_ns() + 8 * 10 ** 9
        with mock.patch.object(matrix, "DEADLINE_MARGIN", 1), mock.patch.object(matrix, "MINIMUM_COMMAND", .2):
            record = self.run_matrix("--timeout", "600", environment={
                "FAKE_FILEGEN_SLEEP": "beta=60", cache.WORKER_DEADLINE_ENV: str(deadline)})
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "not_run"), ("gamma", "not_run")])
        self.assertEqual((record["cells"][1]["outcome"]["stage"], record["cells"][1]["outcome"]["message"]),
                         ("generation", "allocation ended during generation"))
        self.assertEqual(record["file_sets"][1]["generation"]["status"], "interrupted")
        self.assertEqual(record["status"], "partial")

    def test_the_allocation_ending_between_two_copies_keeps_the_first_and_stops_in_order(self):
        ended = []
        execute = run.execute_commands
        def copy(*args, **kwargs):
            outcome = execute(*args, **kwargs)
            ended.append(True)
            return outcome
        with mock.patch.object(run, "execute_commands", side_effect=copy), \
                mock.patch.object(matrix.Allocation, "limit", lambda self, timeout: None if ended else (timeout, False)):
            record = self.run_matrix()
        self.assertEqual(self.outcomes(record), [("alpha", "interrupted"), ("beta", "not_run"), ("gamma", "not_run")])
        self.assertEqual((record["cells"][0]["outcome"]["stage"], record["cells"][0]["outcome"]["message"]),
                         ("copy", "allocation ended before copy 2 of 2"))
        self.assertEqual([copy["status"] for copy in record["cells"][0]["copies"]], ["ok"])
        # nothing was removed after the allocation ended: the first copy is still where it was made
        scratch, = (self.root / "destination").iterdir()
        self.assertTrue((scratch / "alpha/local/1").is_dir())

    def test_the_time_left_is_measured_again_after_cache_preparation(self):
        deadline = time.monotonic_ns() + 15 * 10 ** 9
        calls = []
        def prepare(*args, **kwargs):
            calls.append(kwargs["cache_timeout"])
            if len(calls) == 3:
                # the cache step of beta's first copy uses up what was left of the allocation
                time.sleep(max(0, (deadline - time.monotonic_ns()) / 10 ** 9 - 1.5))
        with mock.patch.object(matrix, "DEADLINE_MARGIN", 1), mock.patch.object(matrix, "MINIMUM_COMMAND", 1), \
                mock.patch.object(run, "_prepare_cache", side_effect=prepare):
            record = self.run_matrix("--timeout", "600", environment={cache.WORKER_DEADLINE_ENV: str(deadline)})
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "interrupted"), ("gamma", "not_run")])
        beta = record["cells"][1]
        self.assertEqual((beta["outcome"]["stage"], beta["outcome"]["message"]),
                         ("cache", "allocation ended after cache preparation before copy 1 of 2"))
        self.assertEqual([copy["status"] for copy in beta["copies"]], ["interrupted"])
        # the copy was never started with a limit computed before the cache step
        self.assertFalse((self.root / "out/logs/beta/local/1").exists())

    def test_a_copy_that_floods_its_log_leaves_a_bounded_head_and_tail(self):
        with mock.patch.object(matrix, "LOG_LIMIT", 8192):
            record = self.run_matrix(environment={"FAKE_RCP": "beta=flood20000"})
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "failed"), ("gamma", "complete")])
        log = (self.root / "out/logs/beta/local/1/0.stderr.log").read_bytes()
        # the output directory of a worker is small: one failing copy must not be able to fill it
        self.assertLessEqual(len(log), 8192)
        self.assertTrue(log.startswith(b"line 0000000 walk child failed"))
        self.assertTrue(log.rstrip().endswith(b"file 0019999"))
        self.assertRegex(log, rb"\n\[\.\.\. \d+ bytes omitted by the benchmark runner \.\.\.\]\n")
        # a log within the limit is kept as it was written
        self.assertEqual((self.root / "out/logs/alpha/local/1/0.stdout.log").read_text().splitlines()[0],
                         "a line of the log")

    def test_a_save_that_fails_is_retried_at_the_next_step_instead_of_ending_the_run(self):
        calls = []
        def persist(output, record, sampler=None, original=matrix._persist):
            calls.append(record["status"])
            if 4 <= len(calls) <= 9:
                raise OSError(errno.ENOSPC, "No space left on device")
            original(output, record, sampler)
        with mock.patch.object(matrix, "_persist", side_effect=persist):
            record = self.run_matrix()
        self.assertEqual(record["status"], "complete")
        self.assertEqual({status for _, status in self.outcomes(record)}, {"complete"})
        self.assertGreater(len(calls), 9)

    def test_inputs_that_would_make_the_results_invalid_are_refused_before_any_work(self):
        long_name = "x" * 65
        manifest = json.loads(self.manifest.read_text())
        manifest["cases"].append(dict(manifest["cases"][0], id=long_name))
        self.manifest.write_text(json.dumps(manifest))
        for extra, cells in ((("--runner-label", ""), ("alpha:local",)),
                             (("--source-storage-id", ""), ("alpha:local",)),
                             ((), (long_name + ":local",))):
            with self.subTest(extra=extra, cells=cells), self.assertRaises(ValueError):
                self.run_matrix(*extra, cells=cells)
            self.assertFalse((self.root / "out").exists())
            self.assertEqual(list((self.root / "source").iterdir()), [])

    def test_an_allocation_that_has_already_ended_runs_nothing_and_still_reports(self):
        record = self.run_matrix(environment={cache.WORKER_DEADLINE_ENV: str(time.monotonic_ns() + 10 ** 9)})
        self.assertEqual({status for _, status in self.outcomes(record)}, {"not_run"})
        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["file_sets"][0]["generation"]["status"], "not_run")

    def test_a_cache_preparation_failure_fails_only_its_cell(self):
        calls = []
        def prepare(*args, **kwargs):
            calls.append(kwargs)
            if len(calls) == 3:
                raise RuntimeError("cache preparation failed at drop: CalledProcessError")
        with mock.patch.object(run, "_prepare_cache", side_effect=prepare):
            record = self.run_matrix()
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "failed"), ("gamma", "complete")])
        self.assertEqual(record["cells"][1]["outcome"]["stage"], "cache")
        # the cache step gets the same generous limit as the copy, not the whole remaining allocation
        self.assertEqual({call["cache_timeout"] for call in calls}, {3600})

    def test_cache_preparation_that_runs_out_of_time_is_not_reported_as_a_failure(self):
        def out_of_time(*args, **kwargs):
            # how the cache step reports a deadline that passed between its two commands
            raise cache.PreparationError(dict(stage="sync", status="failed", error_type="TimeoutError"))
        with mock.patch.object(run, "_prepare_cache", side_effect=out_of_time):
            record = self.run_matrix(cells=("alpha:local",))
        cell, = record["cells"]
        self.assertEqual((cell["outcome"]["status"], cell["outcome"]["stage"]), ("timed_out", "cache"))
        shutil.rmtree(self.root / "out")
        # the same thing at the end of the allocation is the allocation ending, not rcp or the cache failing
        deadline = time.monotonic_ns() + 40 * 10 ** 9
        with mock.patch.object(matrix, "DEADLINE_MARGIN", 1), mock.patch.object(matrix, "MINIMUM_COMMAND", .2), \
                mock.patch.object(run, "_prepare_cache", side_effect=out_of_time):
            record = self.run_matrix("--timeout", "600", environment={cache.WORKER_DEADLINE_ENV: str(deadline)})
        self.assertEqual(self.outcomes(record), [("alpha", "interrupted"), ("beta", "not_run"), ("gamma", "not_run")])
        self.assertEqual(record["cells"][0]["outcome"]["message"],
                         "allocation ended during cache preparation before copy 1 of 2")

    def test_a_stop_signal_ends_the_run_and_keeps_what_was_measured(self):
        calls = []
        def prepare(*args, **kwargs):
            calls.append(kwargs)
            if len(calls) == 3:
                raise InterruptedError("SIGTERM")
        arguments = ["--manifest", str(self.manifest), "--bin-dir", str(self.bin),
                     "--source-root", str(self.root / "source"), "--destination-root", str(self.root / "destination"),
                     "--cache", "uncontrolled", "--repetitions", "2", "--output", str(self.root / "out"),
                     "--cell", "alpha:local", "--cell", "beta:local", "--cell", "gamma:local"]
        with mock.patch.object(run, "_prepare_cache", side_effect=prepare), self.assertRaises(InterruptedError):
            matrix.main(arguments)
        record = matrix.validate(json.loads((self.root / "out/results.json").read_text()))
        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["error"], "SIGTERM")
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "interrupted"), ("gamma", "not_run")])
        self.assertEqual(len(record["cells"][0]["summary"]["samples"]), 2)
        # the copy that was under way was interrupted, not failed, and the record says in which step
        self.assertEqual([copy["status"] for copy in record["cells"][1]["copies"]], ["interrupted"])
        self.assertEqual(record["cells"][1]["outcome"]["message"],
                         "run stopped during cache preparation before copy 1 of 2")
        self.assertEqual(record["cells"][2]["outcome"]["message"], "the run stopped before this cell")
        # a stopping run must exit within seconds: it leaves its scratch data where it is
        self.assertEqual(len(list((self.root / "source").iterdir())), 1)

    def test_system_telemetry_is_attached_to_the_run_and_each_copy(self):
        record = self.run_matrix("--system-telemetry", cells=("alpha:local",))
        self.assertIn(record["system_telemetry"]["status"], ("complete", "partial", "unavailable", "truncated"))
        self.assertIn("system_setup", record)
        phases = [phase["phase"] for phase in record["system_telemetry"]["phases"]]
        self.assertIn("generation", phases)
        for item in record["cells"][0]["copies"]:
            self.assertEqual([phase["phase"] for phase in item["system_telemetry"]["phases"]],
                             ["cache", "command", "summary", "cleanup"])

    def test_selection_is_validated_before_any_work(self):
        for cells, message in ((("alpha:local", "alpha:local"), "duplicate"), (("delta:local",), "unknown file set"),
                               (("alpha:wan",), "unknown scenario"), (("alpha",), "CASE:SCENARIO"), ((), "at least one")):
            with self.subTest(cells=cells), self.assertRaisesRegex(ValueError, message):
                self.run_matrix(cells=cells)
            self.assertFalse((self.root / "out").exists())

    def test_cells_of_one_file_set_share_its_fixture_in_selection_order(self):
        with mock.patch.object(matrix, "SCENARIOS", ("local", "again")), \
                mock.patch.object(matrix, "TOPOLOGY", {"local": "local", "again": "local"}):
            record = self.run_matrix(cells=("beta:again", "alpha:local", "beta:local"))
        self.assertEqual([(cell["case_id"], cell["scenario"]) for cell in record["cells"]],
                         [("beta", "again"), ("alpha", "local"), ("beta", "local")])
        self.assertEqual([item["case"]["id"] for item in record["file_sets"]], ["beta", "alpha"])
        self.assertEqual(record["status"], "complete")

    def test_an_unforeseen_error_costs_one_cell_and_the_run_goes_on(self):
        calls = []
        def summary(*args, original=matrix._json_summary, **kwargs):
            calls.append(args)
            if len(calls) == 3:
                raise RuntimeError("no such summary field")
            return original(*args, **kwargs)
        with mock.patch.object(matrix, "_json_summary", side_effect=summary):
            record = self.run_matrix()
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "failed"), ("gamma", "complete")])
        self.assertEqual((record["cells"][1]["outcome"]["stage"], record["cells"][1]["outcome"]["message"]),
                         ("harness", "unexpected RuntimeError: no such summary field"))
        self.assertEqual(record["status"], "partial")
        self.assertNotIn("error", record)
        # what the broken cell left behind is cleared, so the next file set has its space
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_an_unforeseen_error_while_preparing_a_file_set_costs_that_file_set_only(self):
        def launch(command, *args, original=matrix.subprocess.run, **kwargs):
            if Path(command[1]).name == "beta":
                raise OSError(8, "Exec format error")
            return original(command, *args, **kwargs)
        with mock.patch.object(matrix.subprocess, "run", side_effect=launch):
            record = self.run_matrix()
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "failed"), ("gamma", "complete")])
        self.assertEqual(record["cells"][1]["outcome"]["stage"], "harness")
        self.assertIn("unexpected OSError", record["cells"][1]["outcome"]["message"])
        self.assertEqual(record["file_sets"][1]["generation"]["status"], "failed")
        self.assertEqual(list((self.root / "source").iterdir()), [])

    def test_output_bytes_that_are_not_text_do_not_break_reading_the_summary(self):
        record = self.run_matrix(environment={"FAKE_RCP": "beta=binary"})
        # undecodable bytes in the log before a correct summary are not rcp failing to copy
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "complete"), ("gamma", "complete")])

    def test_a_copy_still_counts_when_its_summary_has_no_statistics(self):
        record = self.run_matrix(cells=("alpha:local",), environment={"FAKE_RCP": "alpha=nostats"})
        cell, = record["cells"]
        # a slow or failed copy is a result; figures that are missing are not rcp failing to copy
        self.assertEqual((cell["outcome"]["status"], [copy["status"] for copy in cell["copies"]]),
                         ("complete", ["ok", "ok"]))
        for copy in cell["copies"]:
            self.assertEqual((copy["resources"]["status"], copy["resources"]["metrics"]), ("unavailable", None))
            self.assertEqual(copy["resources"]["error"], "the summary has no statistics for this process")
        self.assertEqual(len(cell["summary"]["samples"]), 2)

    def test_statistics_that_are_not_counts_are_not_recorded(self):
        reported = "summary.update(" + repr(REPORTED) + ")"
        self.assertIn(reported, RCP)
        for index, value in enumerate((-1, 1.5, "12", True, None, 2 ** 70)):
            # the stand-in reports this value as its peak memory
            (self.bin / "rcp").write_text(RCP.replace(reported, reported[:-1] + f", peak_rss_bytes={value!r})"))
            arguments = ["--manifest", str(self.manifest), "--bin-dir", str(self.bin), "--source-root",
                         str(self.root / "source"), "--destination-root", str(self.root / "destination"),
                         "--cache", "uncontrolled", "--repetitions", "1", "--output", str(self.root / f"out-{index}"),
                         "--cell", "alpha:local"]
            with contextlib.redirect_stdout(io.StringIO()):
                record = matrix.main(arguments)
            copy = record["cells"][0]["copies"][0]
            with self.subTest(value=value):
                self.assertEqual((copy["status"], copy["resources"]["status"], copy["resources"]["metrics"]),
                                 ("ok", "unavailable", None))
                self.assertEqual(copy["resources"]["error"], "peak_rss_bytes is missing or is not a count")

    def test_a_peak_memory_of_zero_is_not_a_measurement(self):
        record = self.run_matrix(cells=("alpha:local",), environment={"FAKE_RCP": "alpha=zerorss"})
        cell, = record["cells"]
        self.assertEqual(cell["outcome"]["status"], "complete")
        for copy in cell["copies"]:
            # no process has a peak of zero: such figures were not read, and must not be plotted
            self.assertEqual((copy["status"], copy["resources"]["status"], copy["resources"]["metrics"]),
                             ("ok", "unavailable", None))
            self.assertEqual(copy["resources"]["error"], "peak_rss_bytes is zero: the process did not measure itself")

    def test_a_last_line_that_is_not_one_json_object_fails_the_cell_at_its_summary(self):
        record = self.run_matrix(environment={"FAKE_RCP": "alpha=list,beta=nested"})
        self.assertEqual(self.outcomes(record), [("alpha", "failed"), ("beta", "failed"), ("gamma", "complete")])
        for cell in record["cells"][:2]:
            # brackets nested too deep to parse are a summary that cannot be read, not a harness error
            self.assertEqual((cell["outcome"]["stage"], cell["outcome"]["message"]), (
                "summary", "copy 1 printed no JSON summary as its last line of output; "
                           f"see logs/{cell['case_id']}/local/1/0.stdout.log"))
            self.assertNotIn("resources", cell["copies"][0])

    def test_counts_that_are_wrong_or_are_not_numbers_fail_the_cell_at_its_summary(self):
        record = self.run_matrix(environment={"FAKE_RCP": "alpha=unchanged,beta=boolean"})
        self.assertEqual(self.outcomes(record), [("alpha", "failed"), ("beta", "failed"), ("gamma", "complete")])
        self.assertEqual([cell["outcome"]["message"] for cell in record["cells"][:2]], [
            "copy 1 reported an unexpected summary: files_unchanged is 1; expected 0",
            "copy 1 reported an unexpected summary: files_copied is missing; expected 4"])
        # true equals one in Python: a count is recorded only when it is a number
        self.assertEqual(record["cells"][1]["copies"][0]["copy_summary"], dict(files_unchanged=0))

    def test_characters_that_only_look_like_line_ends_do_not_split_the_summary(self):
        record = self.run_matrix(cells=("alpha:local",), environment={"FAKE_RCP": "alpha=separator"})
        self.assertEqual(self.outcomes(record), [("alpha", "complete")])
        self.assertEqual(record["cells"][0]["copies"][0]["resources"], dict(status="complete", metrics=RECORDED))

    def test_the_summary_is_read_from_the_end_of_a_log_that_was_cut(self):
        record = self.run_matrix(cells=("alpha:local",), environment={"FAKE_RCP": "alpha=big"})
        self.assertEqual(self.outcomes(record), [("alpha", "complete")])
        log = self.root / "out/logs/alpha/local/1/0.stdout.log"
        self.assertLessEqual(log.stat().st_size, matrix.LOG_LIMIT)
        self.assertIn(b"bytes omitted by the benchmark runner", log.read_bytes())
        self.assertEqual(record["cells"][0]["copies"][0]["resources"], dict(status="complete", metrics=RECORDED))

    def test_a_stop_signal_while_the_summary_is_read_ends_the_run(self):
        read_text = Path.read_text

        def interrupted(path, *arguments, **options):
            if path.name == "0.stdout.log":
                raise InterruptedError("SIGTERM")
            return read_text(path, *arguments, **options)
        with mock.patch.object(Path, "read_text", interrupted), self.assertRaises(InterruptedError):
            self.run_matrix(cells=("alpha:local", "beta:local"))
        record = json.loads((self.root / "out/results.json").read_text())
        # the signal is not taken for a summary that could not be read: nothing more is measured
        self.assertEqual(record["status"], "failed")
        self.assertEqual([(cell["case_id"], cell["outcome"]["status"], cell["outcome"]["message"])
                          for cell in record["cells"]],
                         [("alpha", "interrupted", "run stopped during copy 1 of 2"),
                          ("beta", "not_run", "the run stopped before this cell")])

    def test_a_build_without_the_json_summary_is_still_timed_and_has_no_figures(self):
        record = self.run_matrix(environment={"FAKE_RCP_BUILD": "old"})
        self.assertEqual((record["status"], self.outcomes(record)),
                         ("complete", [("alpha", "complete"), ("beta", "complete"), ("gamma", "complete")]))
        self.assertEqual(record["context"]["summary_format"], "text")
        # nothing measures such a build from outside either: it has no figures
        self.assertNotIn("resources", record["context"])
        for cell in record["cells"]:
            for copy in cell["copies"]:
                # the plain command, whose transfer counts are read from the text summary
                self.assertEqual(copy["command"][:2], [str(self.bin / "rcp"), "--summary"])
                self.assertEqual(len(copy["command"]), 4)
                self.assertEqual(copy["copy_summary"], dict(files_copied=4, files_unchanged=0))
                self.assertNotIn("resources", copy)
            self.assertEqual(len(cell["summary"]["samples"]), 2)

    def test_a_build_without_the_json_summary_still_fails_a_cell_on_a_wrong_count(self):
        record = self.run_matrix(environment={"FAKE_RCP_BUILD": "old", "FAKE_RCP": "alpha=miscount"})
        self.assertEqual(self.outcomes(record), [("alpha", "failed"), ("beta", "complete"), ("gamma", "complete")])
        self.assertEqual((record["cells"][0]["outcome"]["stage"], record["cells"][0]["outcome"]["message"]),
                         ("summary", "copy 1 reported an unexpected summary: files_copied is 3; expected 4"))

    def test_a_copy_that_prints_no_json_summary_fails_its_cell_and_says_what_was_expected(self):
        record = self.run_matrix(environment={"FAKE_RCP": "beta=text"})
        self.assertEqual(self.outcomes(record), [("alpha", "complete"), ("beta", "failed"), ("gamma", "complete")])
        beta = record["cells"][1]
        self.assertEqual((beta["outcome"]["stage"], beta["outcome"]["message"]), (
            "summary", "copy 1 printed no JSON summary as its last line of output; "
                       "see logs/beta/local/1/0.stdout.log"))
        self.assertEqual([copy["status"] for copy in beta["copies"]], ["failed"])
        self.assertNotIn("resources", beta["copies"][0])

    def test_the_runner_takes_no_resource_supervisor(self):
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()) as stderr:
            self.run_matrix("--resource-time", "/usr/bin/time", cells=("alpha:local",))
        self.assertIn("unrecognized arguments: --resource-time", stderr.getvalue())

    def test_validation_rejects_a_complete_cell_without_its_copies(self):
        record = self.run_matrix(cells=("alpha:local",))
        matrix.validate(record)
        for change in (lambda value: value["cells"][0]["copies"].pop(),
                       lambda value: value["cells"][0]["summary"].update(median=0),
                       lambda value: value["cells"][0]["outcome"].update(status="passed"),
                       lambda value: value.update(schema="rcp-benchmark-matrix-v0"),
                       lambda value: value.update(status="complete", cells=[]),
                       lambda value: value["cells"][0]["copies"][0].update(elapsed_seconds=float("nan"))):
            broken = json.loads(json.dumps(record))
            change(broken)
            with self.subTest(change=change), self.assertRaises(ValueError):
                matrix.validate(broken)

    def test_validation_covers_every_field_a_consumer_reads(self):
        record = self.run_matrix(cells=("alpha:local",))
        changes = (
            lambda value: value.update(revision=[]),
            lambda value: value["revision"].update(commit=7),
            lambda value: value["revision"].pop("dirty"),
            lambda value: value.update(timestamp="yesterday"),
            lambda value: value.update(timestamp="2026-11-20T08:30:00"),
            # one spelling only: readers that parse differently must never disagree about a time
            lambda value: value.update(timestamp="2026-11-20Z08:30:00+00:00"),
            lambda value: value.update(timestamp="2026-11-20 08:30:00+00:00"),
            lambda value: value.update(timestamp="2026-11-20T08:30:00Z"),
            lambda value: value.update(timestamp="2026-13-20T08:30:00+00:00"),
            lambda value: value["revision"].update(extra=dict(deep=[1])),
            # True equals 1 and False equals 0: a number is a number by type, not by comparison
            lambda value: (value["cells"][0]["copies"][0].update(elapsed_seconds=1),
                           value["cells"][0]["copies"][1].update(elapsed_seconds=1),
                           value["cells"][0]["summary"].update(median=True, minimum=1, maximum=1, samples=[1, 1])),
            lambda value: (value["cells"][0]["copies"][0].update(elapsed_seconds=1),
                           value["cells"][0]["copies"][1].update(elapsed_seconds=1),
                           value["cells"][0]["summary"].update(median=1, minimum=1, maximum=1, samples=[True, 1])),
            lambda value: value.update(timestamp=7),
            lambda value: value.update(run_id="../etc"),
            lambda value: value.update(tools=[]),
            lambda value: value["tools"]["rcp"].update(sha256="d" * 63),
            lambda value: value["tools"].update(extra=value["tools"]["rcp"]),
            lambda value: value["context"].update(purpose="smoke"),
            lambda value: value["context"].update(cache_policy=None),
            lambda value: value["context"].update(runner_label=["x"]),
            lambda value: value["context"].update(repository=None),
            lambda value: value["context"].update(timeout_seconds=0),
            lambda value: value["context"].update(repetitions=True),
            lambda value: value["context"].update(storage_ids=None),
            lambda value: value["file_sets"][0]["case"].update(id=["alpha"]),
            lambda value: value["file_sets"][0]["case"].update(id="al pha"),
            lambda value: value["file_sets"][0].update(expected_counts=None),
            lambda value: value["cells"][0].update(case_id={"a": 1}),
            lambda value: value["cells"][0]["variant"].update(id="rcp-tuned"),
            lambda value: value["cells"][0].update(variant=None),
            lambda value: value["cells"][0]["copies"][0].update(trial_index=-1),
            lambda value: value["cells"][0]["outcome"].update(message=7),
            lambda value: value["cells"][0]["outcome"].pop("message"),
            lambda value: value["cells"][0]["outcome"].pop("stage"),
            lambda value: value.update(error=7),
            lambda value: value.update(phase_seconds=[]),
        )
        for change in changes:
            broken = json.loads(json.dumps(record))
            change(broken)
            # a consumer catches one exception type, and its message never quotes the document
            with self.subTest(change=change), self.assertRaises(matrix.Invalid):
                matrix.validate(broken)

    def test_a_document_saved_before_the_tools_were_read_is_still_valid(self):
        record = self.run_matrix(cells=("alpha:local",))
        record.update(tools={}, status="failed", error="not executable")
        record["cells"][0].update(outcome=dict(status="not_run", stage=None, message="the run stopped before this cell"),
                                  copies=[], summary=None)
        matrix.validate(record)


if __name__ == "__main__":
    unittest.main()
