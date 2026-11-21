"""Tool cells time rcmp, rlink and rrm on the copies of a local cell and follow the rules of every other cell."""

import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock

from benchmarks import cache, matrix, run
from benchmarks.test_matrix import FILEGEN, RCP, RECORDED, REPORTED

# stands in for rcmp, rlink and rrm, by the name it is run under. A tool works on the copy that a
# local cell has just made, <destination>/<file set>/local/<copy>, and FAKE_<TOOL> says how it
# behaves for a file set: "alpha=exit" at every run, "alpha=exit@2" at the run after copy 2.
TOOL = r'''#!/usr/bin/env python3
import json, os, pathlib, shutil, sys, time
name = pathlib.Path(sys.argv[0]).name
if "--version" in sys.argv:
    print(name + " 1")
    sys.exit(0)
operands = 1 if name == "rrm" else 2
options, paths = sys.argv[1:-operands], [pathlib.Path(item) for item in sys.argv[-operands:]]
if options[-1] != "--output-format=json" or "--summary" not in options:
    sys.exit("the runner reads the JSON summary, asked for just before the operands")
copy = paths[1] if name == "rcmp" else paths[0]
case, iteration = copy.parent.parent.name, copy.name
plan = dict(item.split("=") for item in os.environ.get("FAKE_" + name.upper(), "").split(",") if item)
behaviour, _, when = plan.get(case, "ok").partition("@")
if when and when != iteration:
    behaviour = "ok"
if behaviour.startswith("sleep"):
    time.sleep(float(behaviour[5:]))
if behaviour == "exit":
    print("error: no such file or directory", file=sys.stderr)
    sys.exit(3)

def count(root):
    files = directories = 0
    for _, _, leaves in os.walk(root):
        directories += 1
        files += len(leaves)
    return files, directories

files, directories = count(copy)
short = 1 if behaviour == "miscount" else 0
if name == "rcmp":
    if "--quiet" not in options or "--metadata-compare=f:size" not in options:
        sys.exit("the comparison is by file size and prints no differences")
    if count(paths[0]) != (files, directories):
        sys.exit(1)
    kinds = {kind: dict(same=0, different=0, src_missing=0, dst_missing=0)
             for kind in ("file", "dir", "symlink", "other")}
    kinds["file"]["same"], kinds["dir"]["same"] = files - short, directories
    summary = dict(mismatch=kinds, src_bytes=files * 3, dst_bytes=files * 3, skipped={})
elif name == "rlink":
    shutil.copytree(copy, paths[1], copy_function=os.link)
    summary = dict(hard_links_created=files - short, hard_links_unchanged=0,
                   copy_summary=dict(directories_created=directories, files_copied=0))
else:
    if behaviour != "keep":
        shutil.rmtree(copy)
    summary = dict(files_removed=files - short, directories_removed=directories, bytes_removed=files * 3)
if behaviour == "boolean":
    summary["files_removed"] = True
if behaviour != "nostats":
    summary.update(REPORTED)
if behaviour == "text":
    print("files removed: %d" % files)
else:
    print("a line of the log")
    print(json.dumps(summary))
sys.exit(4 if behaviour in ("late", "keep") else 0)
'''.replace("REPORTED", repr(REPORTED))

ALL = ("alpha:local", "alpha:rcmp", "alpha:rlink", "alpha:rrm")


class ToolCellTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for name, text in (("filegen", FILEGEN), ("rcp", RCP), ("rcpd", "#!/bin/sh\necho 'rcpd 1'\n"),
                           ("rcmp", TOOL), ("rlink", TOOL), ("rrm", TOOL)):
            (self.bin / name).write_text(text)
            (self.bin / name).chmod(0o755)
        self.manifest = self.root / "manifest.json"
        # two leaf directories of two files: four files, and three directories with the root
        self.manifest.write_text(json.dumps(dict(schema_version=1, cases=[
            dict(id=name, directory_widths=[2], files_per_leaf=2, file_size_bytes=3) for name in ("alpha", "beta")],
            variants=[dict(id="rcp-default", tool="rcp", args=["--summary"], processes=1)])))
        for side in ("source", "destination"):
            (self.root / side).mkdir()

    def arguments(self, cells, output="out"):
        arguments = ["--manifest", str(self.manifest), "--bin-dir", str(self.bin),
                     "--source-root", str(self.root / "source"), "--destination-root", str(self.root / "destination"),
                     "--cache", "uncontrolled", "--repetitions", "2", "--output", str(self.root / output)]
        for cell in cells:
            arguments += ["--cell", cell]
        return arguments

    def run_matrix(self, *extra, cells=ALL, environment=None):
        with mock.patch.dict(os.environ, environment or {}), contextlib.redirect_stdout(io.StringIO()):
            record = matrix.main([*self.arguments(cells), *extra])
        saved = json.loads((self.root / "out/results.json").read_text())
        self.assertEqual(saved, json.loads(json.dumps(record)))
        matrix.validate(saved)
        return saved

    def saved(self):
        return matrix.validate(json.loads((self.root / "out/results.json").read_text()))

    def outcomes(self, record):
        return [(cell["case_id"], cell["scenario"], cell["outcome"]["status"]) for cell in record["cells"]]

    def cell(self, record, scenario, case="alpha"):
        return next(cell for cell in record["cells"] if (cell["case_id"], cell["scenario"]) == (case, scenario))

    def commands(self):
        """Run the matrix while noting which program each measured command starts, in order."""
        seen, execute = [], run.execute_commands
        def note(commands, *args, **kwargs):
            seen.append(Path(commands[0][0]).name)
            return execute(commands, *args, **kwargs)
        return seen, mock.patch.object(run, "execute_commands", side_effect=note)

    def test_every_tool_runs_after_each_local_copy_in_one_order(self):
        seen, patched = self.commands()
        with patched:
            # the order of the selection does not decide the order they run in
            record = self.run_matrix(cells=("alpha:local", "alpha:rrm", "alpha:rlink", "alpha:rcmp"))
        self.assertEqual(record["status"], "complete")
        self.assertEqual(self.outcomes(record), [("alpha", "local", "complete"), ("alpha", "rrm", "complete"),
                                                 ("alpha", "rlink", "complete"), ("alpha", "rcmp", "complete")])
        # compared while it is whole, linked, and then removed: rrm's run is what frees the copy
        self.assertEqual(seen, ["rcp", "rcmp", "rlink", "rrm"] * 2)
        for scenario in ("rcmp", "rlink", "rrm"):
            cell = self.cell(record, scenario)
            self.assertEqual([item["status"] for item in cell["copies"]], ["ok", "ok"])
            self.assertEqual([item["iteration"] for item in cell["copies"]], [1, 2])
            samples = [item["elapsed_seconds"] for item in cell["copies"]]
            self.assertEqual(cell["summary"]["samples"], samples)
            self.assertEqual(cell["summary"]["unit"], "seconds")
            # what each tool reports about itself, in the units of every other record
            self.assertEqual([item["resources"] for item in cell["copies"]],
                             [dict(status="complete", metrics=RECORDED)] * 2)
            for item in cell["copies"]:
                self.assertEqual(item["exit_codes"], [0])
                self.assertTrue((self.root / "out/logs/alpha" / scenario / str(item["iteration"]) /
                                 "0.stdout.log").is_file())
        # every measured command has an index of its own, in the order the commands ran
        indexes = {(cell["scenario"], item["iteration"]): item["trial_index"]
                   for cell in record["cells"] for item in cell["copies"]}
        self.assertEqual([indexes[key] for key in (("local", 1), ("rcmp", 1), ("rlink", 1), ("rrm", 1),
                                                   ("local", 2), ("rcmp", 2), ("rlink", 2), ("rrm", 2))],
                         list(range(8)))
        # each run names the copy it followed
        self.assertEqual([item["follows_trial_index"] for item in self.cell(record, "rrm")["copies"]], [0, 4])
        self.assertEqual(list((self.root / "source").iterdir()), [])
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_each_tool_is_run_as_documented_and_only_its_checked_counts_are_recorded(self):
        record = self.run_matrix()
        scratch = "rcp-matrix-"
        local = self.cell(record, "local")["copies"][0]["command"]
        source, copy = local[-2], local[-1]
        self.assertIn(scratch, copy)
        compare = self.cell(record, "rcmp")["copies"][0]
        self.assertEqual(compare["command"], [str(self.bin / "rcmp"), "--summary", "--quiet",
                                              "--metadata-compare=f:size", "--output-format=json", source, copy])
        self.assertEqual(compare["tool_summary"], {"mismatch.file.same": 4, "mismatch.dir.same": 3})
        link = self.cell(record, "rlink")["copies"][0]
        # the new tree is beside the copy, on the same filesystem, under the cell's own name
        links = str(Path(copy).parent.parent / "rlink" / "1")
        self.assertEqual(link["command"], [str(self.bin / "rlink"), "--summary", "--output-format=json", copy, links])
        self.assertEqual(link["tool_summary"], {"hard_links_created": 4, "copy_summary.directories_created": 3})
        remove = self.cell(record, "rrm")["copies"][0]
        self.assertEqual(remove["command"], [str(self.bin / "rrm"), "--summary", "--output-format=json", copy])
        self.assertEqual(remove["tool_summary"], {"files_removed": 4, "directories_removed": 3})
        self.assertEqual({name for name in record["tools"]}, {"filegen", "rcp", "rcpd", "rcmp", "rlink", "rrm"})
        self.assertEqual(record["tools"]["rrm"]["version"], "rrm 1")
        self.assertEqual([self.cell(record, scenario)["variant"]["id"] for scenario in ("local", "rcmp", "rlink", "rrm")],
                         ["rcp-default", "rcmp-size", "rlink-default", "rrm-default"])
        self.assertEqual(len({cell["cell_id"] for cell in record["cells"]}), 4)
        table = (self.root / "out/summary.md").read_text()
        for scenario in ("local", "rcmp", "rlink", "rrm"):
            self.assertIn(f"| alpha | {scenario} | complete |", table)

    def test_rrm_removes_the_copy_and_nothing_else_does(self):
        removed = []
        def remove(path, timeout, original=matrix._remove):
            removed.append(Path(path).relative_to(self.root).parts[2:] or ("scratch",))
            return original(path, timeout)
        with mock.patch.object(matrix, "_remove", side_effect=remove):
            record = self.run_matrix()
        self.assertEqual(record["status"], "complete")
        # rm removes what rlink made, the file set and the scratch directories; never a copy
        self.assertEqual(removed, [("alpha", "rlink", "1"), ("alpha", "rlink", "2"), ("alpha",),
                                   ("scratch",), ("scratch",)])
        self.assertNotIn("removal_failures", record)

    def test_without_an_rrm_cell_the_copy_is_removed_as_before(self):
        removed, seen = [], []
        def remove(path, timeout, original=matrix._remove):
            removed.append(Path(path).relative_to(self.root).parts[2:] or ("scratch",))
            return original(path, timeout)
        execute = run.execute_commands
        def note(commands, *args, **kwargs):
            seen.append(Path(commands[0][0]).name)
            return execute(commands, *args, **kwargs)
        with mock.patch.object(matrix, "_remove", side_effect=remove), \
                mock.patch.object(run, "execute_commands", side_effect=note):
            record = self.run_matrix(cells=("alpha:local", "alpha:rcmp"))
        self.assertEqual(self.outcomes(record), [("alpha", "local", "complete"), ("alpha", "rcmp", "complete")])
        self.assertEqual(seen, ["rcp", "rcmp"] * 2)
        self.assertEqual(removed, [("alpha", "local", "1"), ("alpha", "local", "2"), ("alpha",),
                                   ("scratch",), ("scratch",)])
        # tools that no cell selected are not asked for: this run needs no rrm and no rlink
        self.assertEqual(set(record["tools"]), {"filegen", "rcp", "rcpd", "rcmp"})

    def test_only_the_selected_tools_have_to_exist(self):
        for name in ("rcmp", "rlink", "rrm"):
            (self.bin / name).unlink()
        record = self.run_matrix(cells=("alpha:local",))
        self.assertEqual((record["status"], set(record["tools"])), ("complete", {"filegen", "rcp", "rcpd"}))

    def test_a_selected_tool_that_is_missing_ends_the_run_before_any_work(self):
        (self.bin / "rlink").unlink()
        with self.assertRaisesRegex(ValueError, "not executable"), contextlib.redirect_stdout(io.StringIO()):
            matrix.main(self.arguments(ALL))
        record = self.saved()
        self.assertEqual(record["status"], "failed")
        self.assertEqual({status for _, _, status in self.outcomes(record)}, {"not_run"})
        self.assertEqual(list((self.root / "source").iterdir()), [])

    def test_each_step_finds_the_tree_it_needs_and_none_it_does_not(self):
        seen, execute = [], run.execute_commands
        def note(commands, *args, **kwargs):
            command = commands[0]
            name = Path(command[0]).name
            # the copy is the last operand of every command but rlink's, which names its new tree last
            cell = Path(command[-2] if name == "rlink" else command[-1]).parent.parent
            # what is beside the copy when a command starts, as scenario/number
            seen.append((name, sorted(str(path.relative_to(cell)) for path in cell.glob("*/*"))))
            return execute(commands, *args, **kwargs)
        with mock.patch.object(run, "execute_commands", side_effect=note):
            self.run_matrix()
        self.assertEqual(seen, [("rcp", []), ("rcmp", ["local/1"]), ("rlink", ["local/1"]),
                                # what rlink made is gone before rrm is timed, and both before the next copy
                                ("rrm", ["local/1"]),
                                ("rcp", []), ("rcmp", ["local/2"]), ("rlink", ["local/2"]), ("rrm", ["local/2"])])

    def test_a_tool_that_fails_fails_only_its_cell(self):
        seen, patched = self.commands()
        with patched:
            record = self.run_matrix(environment={"FAKE_RLINK": "alpha=exit"})
        self.assertEqual(record["status"], "partial")
        self.assertEqual(self.outcomes(record), [("alpha", "local", "complete"), ("alpha", "rcmp", "complete"),
                                                 ("alpha", "rlink", "failed"), ("alpha", "rrm", "complete")])
        link = self.cell(record, "rlink")
        self.assertEqual((link["outcome"]["stage"], link["outcome"]["message"]),
                         ("command", "run 1 failed with exit code 3; see 0.stdout.log and 0.stderr.log in "
                                     "logs/alpha/rlink/1"))
        self.assertIsNone(link["summary"])
        # a broken cell is not retried after the next copy, and nothing of its summary is recorded
        self.assertEqual([item["status"] for item in link["copies"]], ["failed"])
        self.assertNotIn("tool_summary", link["copies"][0])
        self.assertNotIn("resources", link["copies"][0])
        self.assertEqual(seen, ["rcp", "rcmp", "rlink", "rrm", "rcp", "rcmp", "rrm"])
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_a_tool_that_fails_after_a_later_copy_keeps_its_earlier_runs_and_no_summary(self):
        record = self.run_matrix(environment={"FAKE_RCMP": "alpha=exit@2"})
        compare = self.cell(record, "rcmp")
        self.assertEqual((compare["outcome"]["status"], compare["outcome"]["stage"]), ("failed", "command"))
        self.assertEqual([item["status"] for item in compare["copies"]], ["ok", "failed"])
        self.assertIsNone(compare["summary"])
        self.assertEqual(self.cell(record, "local")["outcome"]["status"], "complete")

    def test_what_a_failed_rlink_left_behind_is_removed(self):
        (self.bin / "rlink").write_text(TOOL.replace('sys.exit(4 if behaviour in ("late", "keep") else 0)',
                                                     'sys.exit(4)'))
        record = self.run_matrix()
        self.assertEqual(self.cell(record, "rlink")["outcome"]["status"], "failed")
        self.assertEqual(self.cell(record, "rrm")["outcome"]["status"], "complete")
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_a_copy_that_rrm_did_not_remove_is_removed_anyway(self):
        for behaviour in ("exit", "keep"):
            with self.subTest(behaviour=behaviour):
                seen = []
                def copy(commands, *args, original=run.execute_commands, **kwargs):
                    command = commands[0]
                    if Path(command[0]).name == "rcp":
                        seen.append(sorted(path.name for path in Path(command[-1]).parent.iterdir()))
                    return original(commands, *args, **kwargs)
                with mock.patch.object(run, "execute_commands", side_effect=copy):
                    record = self.run_matrix(environment={"FAKE_RRM": "alpha=" + behaviour})
                remove = self.cell(record, "rrm")
                self.assertEqual((remove["outcome"]["status"], remove["outcome"]["stage"]), ("failed", "command"))
                self.assertEqual(self.cell(record, "local")["outcome"]["status"], "complete")
                # the second copy still started with the first one gone: a terabyte fits only once
                self.assertEqual(seen, [[], []])
                self.assertEqual(list((self.root / "destination").iterdir()), [])
                self.assertNotIn("removal_failures", record)
                (self.root / "out").rename(self.root / ("out-" + behaviour))

    def test_a_summary_that_reports_the_wrong_counts_fails_the_tool_cell(self):
        for tool, message in (
                ("RCMP", "run 1 reported an unexpected summary: mismatch.file.same is 3; expected 4"),
                ("RLINK", "run 1 reported an unexpected summary: hard_links_created is 3; expected 4"),
                ("RRM", "run 1 reported an unexpected summary: files_removed is 3; expected 4")):
            with self.subTest(tool=tool):
                record = self.run_matrix(environment={"FAKE_" + tool: "alpha=miscount"})
                cell = self.cell(record, tool.lower())
                self.assertEqual((cell["outcome"]["status"], cell["outcome"]["stage"], cell["outcome"]["message"]),
                                 ("failed", "summary", message))
                self.assertEqual({status for _, scenario, status in self.outcomes(record) if scenario != tool.lower()},
                                 {"complete"})
                (self.root / "out").rename(self.root / ("out-" + tool))

    def test_a_count_that_is_not_a_number_or_a_summary_that_is_not_json_fails_the_tool_cell(self):
        record = self.run_matrix(environment={"FAKE_RRM": "alpha=boolean", "FAKE_RCMP": "alpha=text"})
        remove, compare = self.cell(record, "rrm"), self.cell(record, "rcmp")
        self.assertEqual((remove["outcome"]["stage"], remove["outcome"]["message"]),
                         ("summary", "run 1 reported an unexpected summary: files_removed is missing; expected 4"))
        self.assertEqual(remove["copies"][0]["tool_summary"], dict(directories_removed=3))
        self.assertEqual((compare["outcome"]["stage"], compare["outcome"]["message"]),
                         ("summary", "run 1 printed no JSON summary as its last line of output; "
                                     "see logs/alpha/rcmp/1/0.stdout.log"))

    def test_a_tool_run_still_counts_when_its_summary_has_no_statistics(self):
        record = self.run_matrix(environment={"FAKE_RRM": "alpha=nostats"})
        remove = self.cell(record, "rrm")
        self.assertEqual(remove["outcome"]["status"], "complete")
        for item in remove["copies"]:
            self.assertEqual((item["resources"]["status"], item["resources"]["metrics"]), ("unavailable", None))

    def test_a_copy_that_fails_leaves_its_tools_without_a_measurement_and_says_why(self):
        seen, patched = self.commands()
        with patched:
            record = self.run_matrix(cells=(*ALL, "beta:local", "beta:rrm"), environment={"FAKE_RCP": "alpha=exit"})
        self.assertEqual(self.outcomes(record), [
            ("alpha", "local", "failed"), ("alpha", "rcmp", "failed"), ("alpha", "rlink", "failed"),
            ("alpha", "rrm", "failed"), ("beta", "local", "complete"), ("beta", "rrm", "complete")])
        for scenario in ("rcmp", "rlink", "rrm"):
            cell = self.cell(record, scenario)
            self.assertEqual((cell["outcome"]["stage"], cell["outcome"]["message"]), (
                "copy", "the local copy that this cell follows did not complete: copy 1 failed with exit code 3; "
                        "see 0.stdout.log and 0.stderr.log in logs/alpha/local/1"))
            self.assertEqual((cell["copies"], cell["summary"]), ([], None))
        self.assertEqual(seen, ["rcp", "rcp", "rrm", "rcp", "rrm"])

    def test_a_later_copy_that_fails_keeps_the_tool_runs_before_it(self):
        (self.bin / "rcp").write_text(RCP.replace(
            'behaviour = plan.get(source.parent.name, "ok")',
            'behaviour = plan.get(source.parent.name + "@" + destination.name, "ok")'))
        record = self.run_matrix(environment={"FAKE_RCP": "alpha@2=exit"})
        self.assertEqual(self.cell(record, "local")["outcome"]["status"], "failed")
        remove = self.cell(record, "rrm")
        self.assertEqual((remove["outcome"]["status"], remove["outcome"]["stage"]), ("failed", "copy"))
        self.assertEqual([item["status"] for item in remove["copies"]], ["ok"])
        self.assertIsNone(remove["summary"])

    def test_a_file_set_that_cannot_be_generated_settles_its_tool_cells_with_it(self):
        record = self.run_matrix(environment={"FAKE_FILEGEN_FAIL": "alpha"})
        self.assertEqual({(status, cell["outcome"]["stage"]) for cell in record["cells"]
                          for status in (cell["outcome"]["status"],)}, {("failed", "generation")})

    def test_a_hung_tool_times_out_and_everything_after_it_still_runs(self):
        started = time.monotonic()
        seen, patched = self.commands()
        with patched:
            record = self.run_matrix("--timeout", "4", environment={"FAKE_RRM": "alpha=sleep60@1"})
        self.assertLess(time.monotonic() - started, 40)
        remove = self.cell(record, "rrm")
        self.assertEqual((remove["outcome"]["status"], remove["outcome"]["stage"], remove["outcome"]["message"]),
                         ("timed_out", "command", "run 1 did not finish within 4 s"))
        self.assertEqual([item["status"] for item in remove["copies"]], ["timed_out"])
        self.assertEqual({status for _, scenario, status in self.outcomes(record) if scenario != "rrm"}, {"complete"})
        self.assertEqual(seen, ["rcp", "rcmp", "rlink", "rrm", "rcp", "rcmp", "rlink"])
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_a_tool_cut_by_the_end_of_the_allocation_is_not_a_failure(self):
        deadline = time.monotonic_ns() + 8 * 10 ** 9
        with mock.patch.object(matrix, "DEADLINE_MARGIN", 1), mock.patch.object(matrix, "MINIMUM_COMMAND", .2):
            record = self.run_matrix("--timeout", "600", cells=(*ALL, "beta:local"), environment={
                "FAKE_RLINK": "alpha=sleep60", cache.WORKER_DEADLINE_ENV: str(deadline)})
        self.assertEqual(self.outcomes(record), [
            ("alpha", "local", "interrupted"), ("alpha", "rcmp", "interrupted"), ("alpha", "rlink", "interrupted"),
            ("alpha", "rrm", "not_run"), ("beta", "local", "not_run")])
        self.assertEqual([self.cell(record, scenario)["outcome"]["message"] for scenario in ("rcmp", "rlink", "rrm")],
                         ["allocation ended before run 2 of 2", "allocation ended during run 1 of 2",
                          "allocation ended before this cell"])
        self.assertEqual(self.cell(record, "local")["outcome"]["message"], "allocation ended before copy 2 of 2")
        # nothing is removed once the allocation has ended: the copy is still where it was made
        scratch, = (self.root / "destination").iterdir()
        self.assertTrue((scratch / "alpha/local/1").is_dir())

    def test_the_allocation_ending_between_two_tools_stops_in_order(self):
        ended = []
        def limit(self, timeout):
            return None if ended else (timeout, False)
        execute = run.execute_commands
        def note(commands, *args, **kwargs):
            outcome = execute(commands, *args, **kwargs)
            if Path(commands[0][0]).name == "rcmp":
                ended.append(True)
            return outcome
        with mock.patch.object(run, "execute_commands", side_effect=note), \
                mock.patch.object(matrix.Allocation, "limit", limit):
            record = self.run_matrix()
        self.assertEqual([(scenario, status, self.cell(record, scenario)["outcome"]["message"])
                          for _, scenario, status in self.outcomes(record)], [
            ("local", "interrupted", "allocation ended before copy 2 of 2"),
            ("rcmp", "interrupted", "allocation ended before run 2 of 2"),
            ("rlink", "not_run", "allocation ended before this cell"),
            ("rrm", "not_run", "allocation ended before this cell")])
        self.assertEqual([item["status"] for item in self.cell(record, "rcmp")["copies"]], ["ok"])

    def test_the_allocation_ending_in_an_earlier_cell_of_the_file_set_leaves_the_tool_cells_not_run(self):
        ended = []
        execute = run.execute_commands
        def note(commands, *args, **kwargs):
            outcome = execute(commands, *args, **kwargs)
            ended.append(True)
            return outcome
        with mock.patch.object(run, "execute_commands", side_effect=note), \
                mock.patch.object(matrix.Allocation, "limit", lambda self, timeout: None if ended else (timeout, False)), \
                mock.patch.object(matrix, "SCENARIOS", ("local", "again", *matrix.TOOLS)), \
                mock.patch.object(matrix, "TOPOLOGY", dict(matrix.TOPOLOGY, again="local")):
            record = self.run_matrix(cells=("alpha:again", "alpha:local", "alpha:rrm"))
        self.assertEqual([(scenario, status, self.cell(record, scenario)["outcome"]["message"])
                          for _, scenario, status in self.outcomes(record)], [
            ("again", "interrupted", "allocation ended before copy 2 of 2"),
            ("local", "not_run", "allocation ended before this cell"),
            ("rrm", "not_run", "allocation ended before this cell")])

    def test_a_stop_signal_during_a_tool_run_says_which_run_it_was(self):
        execute = run.execute_commands
        def stopping(commands, *args, **kwargs):
            if Path(commands[0][0]).name == "rlink":
                raise InterruptedError("SIGTERM")
            return execute(commands, *args, **kwargs)
        with mock.patch.object(run, "execute_commands", side_effect=stopping), self.assertRaises(InterruptedError), \
                contextlib.redirect_stdout(io.StringIO()):
            matrix.main(self.arguments((*ALL, "beta:local")))
        record = self.saved()
        self.assertEqual((record["status"], record["error"]), ("failed", "SIGTERM"))
        self.assertEqual([(scenario, status, self.cell(record, scenario)["outcome"]["message"])
                          for case, scenario, status in self.outcomes(record) if case == "alpha"], [
            ("local", "interrupted", "run stopped after copy 1 of 2"),
            ("rcmp", "interrupted", "run stopped after run 1 of 2"),
            ("rlink", "interrupted", "run stopped during run 1 of 2"),
            ("rrm", "not_run", "the run stopped before this cell")])
        self.assertEqual([item["status"] for item in self.cell(record, "rlink")["copies"]], ["interrupted"])
        self.assertEqual(self.cell(record, "local", "beta")["outcome"]["message"], "the run stopped before this cell")

    def test_an_unforeseen_error_in_a_tool_run_costs_the_cells_that_were_not_finished(self):
        calls = []
        def summary(*args, original=matrix._json_summary, **kwargs):
            calls.append(args)
            if len(calls) == 3:
                # the third summary read is rlink's first
                raise RuntimeError("no such summary field")
            return original(*args, **kwargs)
        with mock.patch.object(matrix, "_json_summary", side_effect=summary):
            record = self.run_matrix(cells=(*ALL, "beta:local"))
        self.assertEqual(self.outcomes(record), [
            ("alpha", "local", "failed"), ("alpha", "rcmp", "failed"), ("alpha", "rlink", "failed"),
            ("alpha", "rrm", "failed"), ("beta", "local", "complete")])
        self.assertEqual({(cell["outcome"]["stage"], cell["outcome"]["message"]) for cell in record["cells"][:4]},
                         {("harness", "unexpected RuntimeError: no such summary field")})
        self.assertNotIn("error", record)
        # what the broken cells left behind is cleared, so the next file set has its space
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_tools_follow_local_copies_only(self):
        seen, patched = self.commands()
        removed = []
        def remove(path, timeout, original=matrix._remove):
            removed.append(Path(path).relative_to(self.root).parts[2:] or ("scratch",))
            return original(path, timeout)
        with patched, mock.patch.object(matrix, "_remove", side_effect=remove), \
                mock.patch.object(matrix, "SCENARIOS", ("local", "again", *matrix.TOOLS)), \
                mock.patch.object(matrix, "TOPOLOGY", dict(matrix.TOPOLOGY, again="local")):
            record = self.run_matrix(cells=("alpha:again", "alpha:local", "alpha:rrm"))
        self.assertEqual(record["status"], "complete")
        self.assertEqual(seen, ["rcp", "rcp", "rcp", "rrm", "rcp", "rrm"])
        self.assertEqual(len(self.cell(record, "rrm")["copies"]), 2)
        # copies of any other scenario are housekeeping, removed as before
        self.assertEqual(removed[:2], [("alpha", "again", "1"), ("alpha", "again", "2")])

    def test_a_tool_cell_needs_the_local_cell_of_its_file_set(self):
        for cells in (("alpha:rrm",), ("alpha:rcmp", "beta:local"), ("alpha:local", "beta:rlink")):
            with self.subTest(cells=cells), self.assertRaisesRegex(ValueError, "needs the local cell"):
                self.run_matrix(cells=cells)
            self.assertFalse((self.root / "out").exists())

    def test_system_telemetry_gives_every_tool_run_a_window_of_its_own(self):
        record = self.run_matrix("--system-telemetry")
        phases = {cell["scenario"]: [[phase["phase"] for phase in item["system_telemetry"]["phases"]]
                                     for item in cell["copies"]] for cell in record["cells"]}
        # rrm removes the copy, so the copy has no cleanup of its own; rm clears what rlink made
        self.assertEqual(phases, dict(local=[["cache", "command", "summary"]] * 2, rcmp=[["command"]] * 2,
                                      rlink=[["command", "cleanup"]] * 2, rrm=[["command"]] * 2))

    def test_validation_knows_the_tool_cells(self):
        record = self.run_matrix()
        index = [cell["scenario"] for cell in record["cells"]].index("rrm")
        for change in (lambda value: value["cells"][index]["variant"].update(id="rcp-default"),
                       lambda value: value["cells"][0]["variant"].update(id="rrm-default"),
                       lambda value: value["tools"].update(rchm=value["tools"]["rrm"]),
                       lambda value: value["cells"][index]["copies"][0].update(exit_codes=[1]),
                       lambda value: value["cells"][index]["summary"].update(median=0)):
            broken = json.loads(json.dumps(record))
            change(broken)
            with self.subTest(change=change), self.assertRaises(matrix.Invalid):
                matrix.validate(broken)


if __name__ == "__main__":
    unittest.main()
