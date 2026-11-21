"""Tool cells time rcmp, rlink and rrm on the copies of a copy cell and follow the rules of every other cell."""

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest import mock

from benchmarks import cache, matrix, run
from benchmarks.test_matrix import FILEGEN, RCP, RECORDED, REPORTED

# stands in for rcmp, rlink and rrm, by the name it is run under. A tool works on the copy that a
# copy cell has just made, <destination>/<file set>/<scenario>/<copy>, and FAKE_<TOOL> says how it
# behaves for a file set: "alpha=exit" at every run, "alpha=exit@2" at the run after copy 2.
# Without --summary it is rrm clearing a tree for the runner, and FAKE_RRM_CLEANUP says how:
# "fail" exits without removing anything and "hang" does not return.
TOOL = r'''#!/usr/bin/env python3
import json, os, pathlib, shutil, sys, time
name = pathlib.Path(sys.argv[0]).name
if "--version" in sys.argv:
    print(name + " 1")
    sys.exit(0)
operands = 1 if name == "rrm" else 2
options, paths = sys.argv[1:-operands], [pathlib.Path(item) for item in sys.argv[-operands:]]
if name == "rrm" and not options:
    how = os.environ.get("FAKE_RRM_CLEANUP", "")
    if how == "hang":
        time.sleep(60)
    if how == "fail":
        print("error: permission denied", file=sys.stderr)
        sys.exit(5)
    shutil.rmtree(paths[0])
    sys.exit(0)
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

ALL = ("alpha:local", "alpha:local-rcmp", "alpha:local-rlink", "alpha:local-rrm")


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

    def removals(self):
        """Run the matrix while noting every tree the runner clears: the program, and the tree below its scratch."""
        seen, launch = [], subprocess.run
        def note(command, *args, **kwargs):
            # a tool is also asked for its version, which clears nothing
            if Path(command[0]).name in ("rm", "rrm") and command[-1].startswith(str(self.root)):
                tree = Path(command[-1]).relative_to(self.root).parts[2:]
                seen.append((Path(command[0]).name, "/".join(tree) or "scratch"))
            return launch(command, *args, **kwargs)
        return seen, mock.patch.object(matrix.subprocess, "run", side_effect=note)

    def another_copy_scenario(self):
        """A second scenario that copies as `local` does, with tool cells of its own, as `remote` has."""
        cells = matrix._tool_cells(("local", "again"))
        return (mock.patch.object(matrix, "TOOLS", cells),
                mock.patch.object(matrix, "SCENARIOS", ("local", "again", *cells)),
                mock.patch.object(matrix, "TOPOLOGY", dict.fromkeys(("local", "again", *cells), "local")))

    def test_a_tool_cell_is_named_after_the_copies_it_follows_and_its_tool(self):
        self.assertEqual(list(matrix.TOOLS), ["local-rcmp", "local-rlink", "local-rrm",
                                              "remote-rcmp", "remote-rlink", "remote-rrm"])
        self.assertEqual(matrix.SCENARIOS, ("local", "remote", *matrix.TOOLS))
        self.assertEqual([(cell["follows"], cell["tool"]) for cell in matrix.TOOLS.values()],
                         [(copies, tool) for copies in ("local", "remote") for tool in ("rcmp", "rlink", "rrm")])
        # the same tool runs the same command whichever copies it follows
        self.assertEqual(matrix.TOOLS["remote-rrm"]["variant"], matrix.TOOLS["local-rrm"]["variant"])
        self.assertEqual({matrix.TOPOLOGY[scenario] for scenario in matrix.TOOLS}, {"local"})

    def test_every_tool_runs_after_each_copy_in_one_order(self):
        seen, patched = self.commands()
        with patched:
            # the order of the selection does not decide the order they run in
            record = self.run_matrix(cells=("alpha:local", "alpha:local-rrm", "alpha:local-rlink", "alpha:local-rcmp"))
        self.assertEqual(record["status"], "complete")
        self.assertEqual(self.outcomes(record), [
            ("alpha", "local", "complete"), ("alpha", "local-rrm", "complete"),
            ("alpha", "local-rlink", "complete"), ("alpha", "local-rcmp", "complete")])
        # compared while it is whole, linked, and then removed: rrm's run is what frees the copy
        self.assertEqual(seen, ["rcp", "rcmp", "rlink", "rrm"] * 2)
        for scenario in ("local-rcmp", "local-rlink", "local-rrm"):
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
        order = [(scenario, copy) for copy in (1, 2) for scenario in ("local", "local-rcmp", "local-rlink", "local-rrm")]
        self.assertEqual([indexes[key] for key in order], list(range(8)))
        # each run names the copy it followed
        self.assertEqual([item["follows_trial_index"] for item in self.cell(record, "local-rrm")["copies"]], [0, 4])
        self.assertEqual(list((self.root / "source").iterdir()), [])
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_each_tool_is_run_as_documented_and_only_its_checked_counts_are_recorded(self):
        record = self.run_matrix()
        local = self.cell(record, "local")["copies"][0]["command"]
        source, copy = local[-2], local[-1]
        self.assertIn("rcp-matrix-", copy)
        compare = self.cell(record, "local-rcmp")["copies"][0]
        self.assertEqual(compare["command"], [str(self.bin / "rcmp"), "--summary", "--quiet",
                                              "--metadata-compare=f:size", "--output-format=json", source, copy])
        self.assertEqual(compare["tool_summary"], {"mismatch.file.same": 4, "mismatch.dir.same": 3})
        link = self.cell(record, "local-rlink")["copies"][0]
        # the new tree is beside the copy, on the same filesystem, under the cell's own name
        links = str(Path(copy).parent.parent / "local-rlink" / "1")
        self.assertEqual(link["command"], [str(self.bin / "rlink"), "--summary", "--output-format=json", copy, links])
        self.assertEqual(link["tool_summary"], {"hard_links_created": 4, "copy_summary.directories_created": 3})
        remove = self.cell(record, "local-rrm")["copies"][0]
        self.assertEqual(remove["command"], [str(self.bin / "rrm"), "--summary", "--output-format=json", copy])
        self.assertEqual(remove["tool_summary"], {"files_removed": 4, "directories_removed": 3})
        self.assertEqual(set(record["tools"]), {"filegen", "rcp", "rcpd", "rcmp", "rlink", "rrm"})
        self.assertEqual(record["tools"]["rrm"]["version"], "rrm 1")
        scenarios = ("local", "local-rcmp", "local-rlink", "local-rrm")
        self.assertEqual([self.cell(record, scenario)["variant"]["id"] for scenario in scenarios],
                         ["rcp-default", "rcmp-size", "rlink-default", "rrm-default"])
        self.assertEqual(len({cell["cell_id"] for cell in record["cells"]}), 4)
        table = (self.root / "out/summary.md").read_text()
        for scenario in scenarios:
            self.assertIn(f"| alpha | {scenario} | complete |", table)

    def test_a_run_that_times_rrm_removes_everything_with_it(self):
        removed, patched = self.removals()
        with patched:
            record = self.run_matrix()
        self.assertEqual(record["status"], "complete")
        # the copies go in rrm's measured runs; what rlink made, the file set and the scratch
        # directories go with the same rrm, and rm is never run
        self.assertEqual(removed, [("rrm", "alpha/local-rlink/1"), ("rrm", "alpha/local-rlink/2"), ("rrm", "alpha"),
                                   ("rrm", "scratch"), ("rrm", "scratch")])
        self.assertEqual(record["context"]["removal"], "rrm")
        for key in ("removal_failures", "rrm_fallback"):
            self.assertNotIn(key, record)

    def test_a_run_without_an_rrm_cell_removes_everything_with_rm_as_before(self):
        seen, measured = self.commands()
        removed, patched = self.removals()
        with measured, patched:
            record = self.run_matrix(cells=("alpha:local", "alpha:local-rcmp"))
        self.assertEqual(self.outcomes(record), [("alpha", "local", "complete"), ("alpha", "local-rcmp", "complete")])
        self.assertEqual(seen, ["rcp", "rcmp"] * 2)
        self.assertEqual(removed, [("rm", "alpha/local/1"), ("rm", "alpha/local/2"), ("rm", "alpha"),
                                   ("rm", "scratch"), ("rm", "scratch")])
        self.assertEqual(record["context"]["removal"], "rm -rf")
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
        self.assertEqual(self.outcomes(record), [
            ("alpha", "local", "complete"), ("alpha", "local-rcmp", "complete"),
            ("alpha", "local-rlink", "failed"), ("alpha", "local-rrm", "complete")])
        link = self.cell(record, "local-rlink")
        self.assertEqual((link["outcome"]["stage"], link["outcome"]["message"]),
                         ("command", "run 1 failed with exit code 3; see 0.stdout.log and 0.stderr.log in "
                                     "logs/alpha/local-rlink/1"))
        self.assertIsNone(link["summary"])
        # a broken cell is not retried after the next copy, and nothing of its summary is recorded
        self.assertEqual([item["status"] for item in link["copies"]], ["failed"])
        self.assertNotIn("tool_summary", link["copies"][0])
        self.assertNotIn("resources", link["copies"][0])
        self.assertEqual(seen, ["rcp", "rcmp", "rlink", "rrm", "rcp", "rcmp", "rrm"])
        self.assertEqual(list((self.root / "destination").iterdir()), [])
        # a tool other than rrm that fails says nothing about how trees are cleared
        self.assertNotIn("rrm_fallback", record)

    def test_a_tool_that_fails_after_a_later_copy_keeps_its_earlier_runs_and_no_summary(self):
        record = self.run_matrix(environment={"FAKE_RCMP": "alpha=exit@2"})
        compare = self.cell(record, "local-rcmp")
        self.assertEqual((compare["outcome"]["status"], compare["outcome"]["stage"]), ("failed", "command"))
        self.assertEqual([item["status"] for item in compare["copies"]], ["ok", "failed"])
        self.assertIsNone(compare["summary"])
        self.assertEqual(self.cell(record, "local")["outcome"]["status"], "complete")

    def test_what_a_failed_rlink_left_behind_is_removed(self):
        (self.bin / "rlink").write_text(TOOL.replace('sys.exit(4 if behaviour in ("late", "keep") else 0)',
                                                     'sys.exit(4)'))
        record = self.run_matrix()
        self.assertEqual(self.cell(record, "local-rlink")["outcome"]["status"], "failed")
        self.assertEqual(self.cell(record, "local-rrm")["outcome"]["status"], "complete")
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_a_copy_that_a_measured_rrm_run_did_not_remove_goes_with_rm_and_so_does_the_rest(self):
        for behaviour in ("exit", "keep"):
            with self.subTest(behaviour=behaviour):
                seen = []
                def copy(commands, *args, original=run.execute_commands, **kwargs):
                    command = commands[0]
                    if Path(command[0]).name == "rcp":
                        seen.append(sorted(path.name for path in Path(command[-1]).parent.iterdir()))
                    return original(commands, *args, **kwargs)
                removed, patched = self.removals()
                with mock.patch.object(run, "execute_commands", side_effect=copy), patched:
                    record = self.run_matrix(cells=(*ALL, "beta:local", "beta:local-rrm"),
                                             environment={"FAKE_RRM": "alpha=" + behaviour})
                remove = self.cell(record, "local-rrm")
                self.assertEqual((remove["outcome"]["status"], remove["outcome"]["stage"]), ("failed", "command"))
                self.assertEqual(self.cell(record, "local")["outcome"]["status"], "complete")
                # the second copy still started with the first one gone: a terabyte fits only once
                self.assertEqual(seen[:2], [[], []])
                self.assertEqual(list((self.root / "destination").iterdir()), [])
                self.assertNotIn("removal_failures", record)
                # an rrm that failed once clears nothing more in this run: rm does, from that copy on
                self.assertEqual(record["rrm_fallback"], "a measured run of rrm failed")
                self.assertEqual({tool for tool, _ in removed[1:]}, {"rm"})
                self.assertEqual(removed[:2], [("rrm", "alpha/local-rlink/1"), ("rm", "alpha/local/1")])
                # the tool is still measured where it is asked for: the next file set has its runs
                self.assertEqual(self.cell(record, "local-rrm", "beta")["outcome"]["status"], "complete")
                (self.root / "out").rename(self.root / ("out-" + behaviour))

    def test_an_rrm_that_fails_at_clearing_a_tree_leaves_that_and_every_later_tree_to_rm(self):
        removed, patched = self.removals()
        with patched:
            record = self.run_matrix(environment={"FAKE_RRM_CLEANUP": "fail"})
        # the measured runs are rrm's own and still count: the run measures, it does not judge
        self.assertEqual((record["status"], {status for _, _, status in self.outcomes(record)}),
                         ("complete", {"complete"}))
        self.assertEqual(removed, [("rrm", "alpha/local-rlink/1"), ("rm", "alpha/local-rlink/1"),
                                   ("rm", "alpha/local-rlink/2"), ("rm", "alpha"), ("rm", "scratch"),
                                   ("rm", "scratch")])
        self.assertEqual(record["rrm_fallback"], "rrm exited with code 5 while clearing a tree")
        self.assertNotIn("removal_failures", record)
        self.assertEqual(list((self.root / "source").iterdir()), [])
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_an_rrm_that_hangs_at_clearing_a_tree_is_cut_and_rm_takes_over(self):
        started = time.monotonic()
        removed, patched = self.removals()
        with patched:
            record = self.run_matrix("--timeout", "3", environment={"FAKE_RRM_CLEANUP": "hang"})
        self.assertLess(time.monotonic() - started, 40)
        self.assertEqual(record["status"], "complete")
        self.assertEqual(removed[:2], [("rrm", "alpha/local-rlink/1"), ("rm", "alpha/local-rlink/1")])
        self.assertEqual({tool for tool, _ in removed[1:]}, {"rm"})
        self.assertEqual(record["rrm_fallback"], "rrm did not finish clearing a tree within 3 s")
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_an_rrm_cut_by_the_end_of_the_allocation_while_clearing_a_tree_did_not_fail(self):
        deadline = time.monotonic_ns() + 8 * 10 ** 9
        with mock.patch.object(matrix, "DEADLINE_MARGIN", 1), mock.patch.object(matrix, "MINIMUM_COMMAND", .2):
            record = self.run_matrix("--timeout", "600", environment={
                "FAKE_RRM_CLEANUP": "hang", cache.WORKER_DEADLINE_ENV: str(deadline)})
        # the tree that rlink made could not be cleared in the time left: that is the allocation
        # ending, as for rm, and says nothing about rrm
        self.assertNotIn("rrm_fallback", record)
        self.assertEqual(record["removal_failures"], 1)
        self.assertEqual(self.outcomes(record), [
            ("alpha", "local", "interrupted"), ("alpha", "local-rcmp", "interrupted"),
            ("alpha", "local-rlink", "interrupted"), ("alpha", "local-rrm", "not_run")])
        self.assertEqual([item["status"] for item in self.cell(record, "local-rlink")["copies"]], ["ok"])

    def test_a_tree_that_is_not_there_is_not_a_failure_of_rrm(self):
        # the copy fails before it writes anything: there is no tree to clear after it
        removed, patched = self.removals()
        with patched:
            record = self.run_matrix(cells=("alpha:local", "alpha:local-rrm"), environment={"FAKE_RCP": "alpha=flood5"})
        self.assertEqual(self.cell(record, "local")["outcome"]["status"], "failed")
        self.assertNotIn("rrm_fallback", record)
        self.assertEqual(removed, [("rrm", "alpha"), ("rrm", "scratch"), ("rrm", "scratch")])

    def test_a_summary_that_reports_the_wrong_counts_fails_the_tool_cell(self):
        for tool, message in (
                ("rcmp", "run 1 reported an unexpected summary: mismatch.file.same is 3; expected 4"),
                ("rlink", "run 1 reported an unexpected summary: hard_links_created is 3; expected 4"),
                ("rrm", "run 1 reported an unexpected summary: files_removed is 3; expected 4")):
            with self.subTest(tool=tool):
                record = self.run_matrix(environment={"FAKE_" + tool.upper(): "alpha=miscount"})
                cell = self.cell(record, "local-" + tool)
                self.assertEqual((cell["outcome"]["status"], cell["outcome"]["stage"], cell["outcome"]["message"]),
                                 ("failed", "summary", message))
                self.assertEqual({status for _, scenario, status in self.outcomes(record)
                                  if scenario != "local-" + tool}, {"complete"})
                (self.root / "out").rename(self.root / ("out-" + tool))

    def test_a_count_that_is_not_a_number_or_a_summary_that_is_not_json_fails_the_tool_cell(self):
        record = self.run_matrix(environment={"FAKE_RRM": "alpha=boolean", "FAKE_RCMP": "alpha=text"})
        remove, compare = self.cell(record, "local-rrm"), self.cell(record, "local-rcmp")
        self.assertEqual((remove["outcome"]["stage"], remove["outcome"]["message"]),
                         ("summary", "run 1 reported an unexpected summary: files_removed is missing; expected 4"))
        self.assertEqual(remove["copies"][0]["tool_summary"], dict(directories_removed=3))
        self.assertEqual((compare["outcome"]["stage"], compare["outcome"]["message"]),
                         ("summary", "run 1 printed no JSON summary as its last line of output; "
                                     "see logs/alpha/local-rcmp/1/0.stdout.log"))

    def test_a_tool_run_still_counts_when_its_summary_has_no_statistics(self):
        record = self.run_matrix(environment={"FAKE_RRM": "alpha=nostats"})
        remove = self.cell(record, "local-rrm")
        self.assertEqual(remove["outcome"]["status"], "complete")
        for item in remove["copies"]:
            self.assertEqual((item["resources"]["status"], item["resources"]["metrics"]), ("unavailable", None))

    def test_a_copy_that_fails_leaves_its_tools_without_a_measurement_and_says_why(self):
        seen, patched = self.commands()
        with patched:
            record = self.run_matrix(cells=(*ALL, "beta:local", "beta:local-rrm"),
                                     environment={"FAKE_RCP": "alpha=exit"})
        self.assertEqual(self.outcomes(record), [
            ("alpha", "local", "failed"), ("alpha", "local-rcmp", "failed"), ("alpha", "local-rlink", "failed"),
            ("alpha", "local-rrm", "failed"), ("beta", "local", "complete"), ("beta", "local-rrm", "complete")])
        for scenario in ("local-rcmp", "local-rlink", "local-rrm"):
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
        remove = self.cell(record, "local-rrm")
        self.assertEqual((remove["outcome"]["status"], remove["outcome"]["stage"]), ("failed", "copy"))
        self.assertEqual([item["status"] for item in remove["copies"]], ["ok"])
        self.assertIsNone(remove["summary"])

    def test_a_file_set_that_cannot_be_generated_settles_its_tool_cells_with_it(self):
        record = self.run_matrix(environment={"FAKE_FILEGEN_FAIL": "alpha"})
        self.assertEqual({(cell["outcome"]["status"], cell["outcome"]["stage"]) for cell in record["cells"]},
                         {("failed", "generation")})

    def test_a_hung_tool_times_out_and_everything_after_it_still_runs(self):
        started = time.monotonic()
        seen, patched = self.commands()
        with patched:
            record = self.run_matrix("--timeout", "4", environment={"FAKE_RRM": "alpha=sleep60@1"})
        self.assertLess(time.monotonic() - started, 40)
        remove = self.cell(record, "local-rrm")
        self.assertEqual((remove["outcome"]["status"], remove["outcome"]["stage"], remove["outcome"]["message"]),
                         ("timed_out", "command", "run 1 did not finish within 4 s"))
        self.assertEqual([item["status"] for item in remove["copies"]], ["timed_out"])
        self.assertEqual({status for _, scenario, status in self.outcomes(record) if scenario != "local-rrm"},
                         {"complete"})
        self.assertEqual(seen, ["rcp", "rcmp", "rlink", "rrm", "rcp", "rcmp", "rlink"])
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_a_tool_cut_by_the_end_of_the_allocation_is_not_a_failure(self):
        deadline = time.monotonic_ns() + 8 * 10 ** 9
        with mock.patch.object(matrix, "DEADLINE_MARGIN", 1), mock.patch.object(matrix, "MINIMUM_COMMAND", .2):
            record = self.run_matrix("--timeout", "600", cells=(*ALL, "beta:local"), environment={
                "FAKE_RLINK": "alpha=sleep60", cache.WORKER_DEADLINE_ENV: str(deadline)})
        self.assertEqual(self.outcomes(record), [
            ("alpha", "local", "interrupted"), ("alpha", "local-rcmp", "interrupted"),
            ("alpha", "local-rlink", "interrupted"), ("alpha", "local-rrm", "not_run"), ("beta", "local", "not_run")])
        self.assertEqual([self.cell(record, scenario)["outcome"]["message"]
                          for scenario in ("local-rcmp", "local-rlink", "local-rrm")],
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
            ("local-rcmp", "interrupted", "allocation ended before run 2 of 2"),
            ("local-rlink", "not_run", "allocation ended before this cell"),
            ("local-rrm", "not_run", "allocation ended before this cell")])
        self.assertEqual([item["status"] for item in self.cell(record, "local-rcmp")["copies"]], ["ok"])

    def test_the_allocation_ending_in_an_earlier_cell_of_the_file_set_leaves_the_tool_cells_not_run(self):
        ended = []
        execute = run.execute_commands
        def note(commands, *args, **kwargs):
            outcome = execute(commands, *args, **kwargs)
            ended.append(True)
            return outcome
        tools, scenarios, topology = self.another_copy_scenario()
        with mock.patch.object(run, "execute_commands", side_effect=note), tools, scenarios, topology, \
                mock.patch.object(matrix.Allocation, "limit", lambda self, timeout: None if ended else (timeout, False)):
            record = self.run_matrix(cells=("alpha:again", "alpha:local", "alpha:local-rrm"))
        self.assertEqual([(scenario, status, self.cell(record, scenario)["outcome"]["message"])
                          for _, scenario, status in self.outcomes(record)], [
            ("again", "interrupted", "allocation ended before copy 2 of 2"),
            ("local", "not_run", "allocation ended before this cell"),
            ("local-rrm", "not_run", "allocation ended before this cell")])

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
            ("local-rcmp", "interrupted", "run stopped after run 1 of 2"),
            ("local-rlink", "interrupted", "run stopped during run 1 of 2"),
            ("local-rrm", "not_run", "the run stopped before this cell")])
        self.assertEqual([item["status"] for item in self.cell(record, "local-rlink")["copies"]], ["interrupted"])
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
            ("alpha", "local", "failed"), ("alpha", "local-rcmp", "failed"), ("alpha", "local-rlink", "failed"),
            ("alpha", "local-rrm", "failed"), ("beta", "local", "complete")])
        self.assertEqual({(cell["outcome"]["stage"], cell["outcome"]["message"]) for cell in record["cells"][:4]},
                         {("harness", "unexpected RuntimeError: no such summary field")})
        self.assertNotIn("error", record)
        # what the broken cells left behind is cleared, so the next file set has its space
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_a_tool_cell_follows_the_copies_of_the_scenario_in_its_name_and_no_others(self):
        seen, patched = self.commands()
        tools, scenarios, topology = self.another_copy_scenario()
        with patched, tools, scenarios, topology:
            record = self.run_matrix(cells=("alpha:again", "alpha:again-rrm", "alpha:local", "alpha:local-rcmp",
                                            "alpha:local-rrm"))
        self.assertEqual((record["status"], {status for _, _, status in self.outcomes(record)}),
                         ("complete", {"complete"}))
        # the copies of each cell are followed by its own tools, and by those alone
        self.assertEqual(seen, ["rcp", "rrm"] * 2 + ["rcp", "rcmp", "rrm"] * 2)
        again, local = self.cell(record, "again-rrm"), self.cell(record, "local-rrm")
        self.assertEqual(([item["follows_trial_index"] for item in again["copies"]],
                          [item["follows_trial_index"] for item in local["copies"]]), ([0, 2], [4, 7]))
        self.assertIn("/alpha/again/1", again["copies"][0]["command"][-1])
        self.assertIn("/alpha/local/2", local["copies"][1]["command"][-1])
        # the same tool after other copies is another cell, with a series of its own
        self.assertNotEqual(again["cell_id"], local["cell_id"])
        self.assertEqual(again["variant"], local["variant"])

    def test_copies_that_no_rrm_cell_follows_are_cleared_with_the_runs_rrm_all_the_same(self):
        removed, patched = self.removals()
        tools, scenarios, topology = self.another_copy_scenario()
        with patched, tools, scenarios, topology:
            # rrm is timed after the copies of one cell only; the other cell's copies are followed by rcmp
            record = self.run_matrix(cells=("alpha:local", "alpha:local-rcmp", "alpha:again", "alpha:again-rrm"))
        self.assertEqual((record["status"], {status for _, _, status in self.outcomes(record)}),
                         ("complete", {"complete"}))
        # a copy that rcmp has compared and no rrm cell removes is not a failed run of rrm
        self.assertNotIn("rrm_fallback", record)
        self.assertEqual(removed, [("rrm", "alpha/local/1"), ("rrm", "alpha/local/2"), ("rrm", "alpha"),
                                   ("rrm", "scratch"), ("rrm", "scratch")])

    def test_a_failed_run_of_another_tool_leaves_the_clearing_to_rrm(self):
        removed, patched = self.removals()
        tools, scenarios, topology = self.another_copy_scenario()
        with patched, tools, scenarios, topology:
            # rcmp fails on a copy that no rrm cell removes: the runner has to clear that copy itself
            record = self.run_matrix(cells=("alpha:local", "alpha:local-rcmp", "alpha:again", "alpha:again-rrm"),
                                     environment={"FAKE_RCMP": "alpha=exit"})
        self.assertEqual(sorted(self.outcomes(record)), [
            ("alpha", "again", "complete"), ("alpha", "again-rrm", "complete"),
            ("alpha", "local", "complete"), ("alpha", "local-rcmp", "failed")])
        # only a run of rrm that failed says anything about rrm: it still clears every tree
        self.assertNotIn("rrm_fallback", record)
        self.assertEqual(removed, [("rrm", "alpha/local/1"), ("rrm", "alpha/local/2"), ("rrm", "alpha"),
                                   ("rrm", "scratch"), ("rrm", "scratch")])

    def test_a_tool_cell_needs_the_copy_cell_that_it_follows(self):
        for cells, copies in ((("alpha:local-rrm",), "local"), (("alpha:local-rcmp", "beta:local"), "local"),
                              (("alpha:local", "alpha:remote-rrm"), "remote"),
                              (("alpha:local", "beta:local-rlink"), "local")):
            with self.subTest(cells=cells), \
                    self.assertRaisesRegex(ValueError, f"a tool cell needs the {copies} cell of its file set"):
                self.run_matrix(cells=cells)
            self.assertFalse((self.root / "out").exists())

    def test_system_telemetry_gives_every_tool_run_a_window_of_its_own(self):
        record = self.run_matrix("--system-telemetry")
        phases = {cell["scenario"]: [[phase["phase"] for phase in item["system_telemetry"]["phases"]]
                                     for item in cell["copies"]] for cell in record["cells"]}
        # a command's window lasts until the next step starts. rrm's run removes the copy, so the
        # copy has no cleanup of its own; what rlink made is cleared in a window of its own
        self.assertEqual(phases, {"local": [["cache", "command"]] * 2, "local-rcmp": [["command"]] * 2,
                                  "local-rlink": [["command", "cleanup"]] * 2, "local-rrm": [["command"]] * 2})

    def test_validation_knows_the_tool_cells(self):
        record = self.run_matrix()
        index = [cell["scenario"] for cell in record["cells"]].index("local-rrm")
        for change in (lambda value: value["cells"][index]["variant"].update(id="rcp-default"),
                       lambda value: value["cells"][0]["variant"].update(id="rrm-default"),
                       lambda value: value["tools"].update(rchm=value["tools"]["rrm"]),
                       lambda value: value["cells"][index]["copies"][0].update(exit_codes=[1]),
                       lambda value: value["cells"][index]["summary"].update(median=0),
                       lambda value: value.update(rrm_fallback=7),
                       lambda value: value.update(rrm_fallback="")):
            broken = json.loads(json.dumps(record))
            change(broken)
            with self.subTest(change=change), self.assertRaises(matrix.Invalid):
                matrix.validate(broken)
        matrix.validate(dict(record, rrm_fallback="rrm exited with code 5 while clearing a tree"))


if __name__ == "__main__":
    unittest.main()
