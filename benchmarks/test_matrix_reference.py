"""Reference cells copy a file set with cp or rsync beside rcp's cell, within a multiple of what rcp took."""

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
from benchmarks.test_matrix import FILEGEN, RCP
from benchmarks.test_matrix_remote import RCP as REMOTE_RCP, RCPD
from benchmarks.test_matrix_tools import TOOL
from benchmarks.test_private_sshd import KEYGEN, SSH, SSHD, tools

# stands in for cp and rsync, by the name it is run under. A copy lands in
# <destination>/<file set>/<scenario>/<copy>, and FAKE_CP or FAKE_RSYNC says how the tool behaves for
# a file set, or for one scenario of it: "alpha=exit" at every copy, "alpha:remote-rsync=sleep60@1"
# at the first copy of that cell.
COPIER = r'''#!/usr/bin/env python3
import os, pathlib, shutil, subprocess, sys, time
name = pathlib.Path(sys.argv[0]).name
if "--version" in sys.argv:
    print(name + " (stand-in) 1")
    print("a second line that nobody records")
    sys.exit(0)
arguments = sys.argv[1:]
if arguments[0] != "-a":
    sys.exit("the copy is an archive copy")
options, (source, destination) = arguments[1:-2], arguments[-2:]
host = None
if name == "rsync":
    if not (source.endswith("/") and destination.endswith("/")):
        sys.exit("rsync copies what is in one directory into another, which the trailing slashes say")
    if ":" in source:
        host, _, source = source.partition(":")
        if host != "127.0.0.1" or options != ["--rsync-path=" + sys.argv[0]]:
            sys.exit("a pull names loopback and the rsync that the host runs")
    elif options:
        sys.exit("a copy between two directories takes no option of a pull")
elif options or ":" in source:
    sys.exit("cp takes the tree and its copy")
source, destination = pathlib.Path(source), pathlib.Path(destination)
case, scenario, iteration = source.parent.name, destination.parent.name, destination.name
plan = dict(item.split("=") for item in os.environ.get("FAKE_" + name.upper(), "").split(",") if item)
behaviour, _, when = plan.get(case + ":" + scenario, plan.get(case, "ok")).partition("@")
if when and when != iteration:
    behaviour = "ok"
if host is not None:
    # the tree is read through the host's sshd, as the sending rsync would be started through it
    listing = subprocess.run(["ssh", host, "ls", str(source)], stdout=subprocess.PIPE, text=True)
    if listing.returncode or not listing.stdout.split():
        sys.exit(12)
if behaviour == "half":
    # some of the tree is there when the copy is cut
    shutil.copytree(source, destination)
    time.sleep(60)
if behaviour == "orphan":
    # a process of the host that outlives the pull, in a session of its own as sshd starts them
    subprocess.Popen(["ssh", host, "sleep", "317"], start_new_session=True)
    time.sleep(60)
if behaviour.startswith("sleep"):
    time.sleep(float(behaviour[5:]))
if behaviour == "exit":
    print(name + ": some files could not be transferred", file=sys.stderr)
    sys.exit(23)
shutil.copytree(source, destination)
'''

CELLS = ("alpha:local", "alpha:local-cp", "alpha:local-rsync")


class ReferenceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.bin = tools(self.root / "bin", filegen=FILEGEN, rcp=RCP, rcpd="#!/bin/sh\necho 'rcpd 1'\n",
                         rcmp=TOOL, rlink=TOOL, rrm=TOOL)["rcp"].parent
        # what the system provides, and so not where the build is
        self.tools = tools(self.root / "system", cp=COPIER, rsync=COPIER)
        self.manifest = self.root / "manifest.json"
        self.manifest.write_text(json.dumps(dict(schema_version=1, cases=[
            dict(id=name, directory_widths=[2], files_per_leaf=2, file_size_bytes=3) for name in ("alpha", "beta")],
            variants=[dict(id="rcp-default", tool="rcp", args=["--summary"], processes=1)])))
        for side in ("source", "destination", "memory-source", "memory-destination"):
            (self.root / side).mkdir()
        # a mount shows the same tree under another path, as these links do
        for side in ("source", "destination"):
            (self.root / ("nfs-" + side)).symlink_to(self.root / side)

    def arguments(self, cells, *extra, limit="1000", cache_policy="uncontrolled", output="out"):
        """The runner's arguments. The limit is far away unless a test is about it: the stand-ins take
        a tenth of a second, and on a busy machine one of them may take ten times what another did."""
        arguments = ["--manifest", str(self.manifest), "--bin-dir", str(self.bin),
                     "--source-root", str(self.root / "source"), "--destination-root", str(self.root / "destination"),
                     "--cache", cache_policy, "--repetitions", "2", "--output", str(self.root / output),
                     "--cp", str(self.tools["cp"]), "--rsync", str(self.tools["rsync"])]
        if limit is not None:
            arguments += ["--reference-limit", limit]
        for cell in cells:
            arguments += ["--cell", cell]
        return [*arguments, *extra]

    def roots(self, *kinds):
        arguments = []
        for kind in kinds:
            arguments += [f"--{kind}-source-root", str(self.root / f"{kind}-source"),
                          f"--{kind}-destination-root", str(self.root / f"{kind}-destination")]
        return arguments

    def run_matrix(self, *extra, cells=CELLS, environment=None, output="out", **options):
        with mock.patch.dict(os.environ, environment or {}), contextlib.redirect_stdout(io.StringIO()):
            record = matrix.main(self.arguments(cells, *extra, output=output, **options))
        saved = json.loads((self.root / output / "results.json").read_text())
        self.assertEqual(saved, json.loads(json.dumps(record)))
        matrix.validate(saved)
        return saved

    def outcomes(self, record):
        return [(cell["case_id"], cell["scenario"], cell["outcome"]["status"]) for cell in record["cells"]]

    def cell(self, record, scenario, case="alpha"):
        return next(cell for cell in record["cells"] if (cell["case_id"], cell["scenario"]) == (case, scenario))

    def commands(self):
        """Run the matrix while noting every measured command, in order."""
        seen, execute = [], run.execute_commands
        def note(commands, *args, **kwargs):
            seen.append(list(map(str, commands[0])))
            return execute(commands, *args, **kwargs)
        return seen, mock.patch.object(run, "execute_commands", side_effect=note)

    def test_a_reference_cell_is_named_after_the_copies_it_stands_beside_and_its_tool(self):
        # cp copies between two directories of one host; rsync also pulls from a host
        self.assertEqual(list(matrix.REFERENCES), ["local-cp", "nfs-cp", "memory-cp", "local-rsync", "remote-rsync",
                                                   "nfs-rsync", "memory-rsync"])
        self.assertEqual({scenario: (cell["tool"], cell["beside"], cell["variant"]["id"], cell["variant"]["args"])
                          for scenario, cell in matrix.REFERENCES.items() if scenario.startswith(("local", "remote"))},
                         {"local-cp": ("cp", "local", "cp-a", ["-a"]),
                          "local-rsync": ("rsync", "local", "rsync-a", ["-a"]),
                          "remote-rsync": ("rsync", "remote", "rsync-a", ["-a"])})
        self.assertEqual(matrix.SCENARIOS[-len(matrix.REFERENCES):], tuple(matrix.REFERENCES))
        self.assertEqual({scenario: matrix.storage_of(scenario) for scenario in matrix.REFERENCES},
                         {"local-cp": "disks", "nfs-cp": "nfs", "memory-cp": "memory", "local-rsync": "disks",
                          "remote-rsync": "disks", "nfs-rsync": "nfs", "memory-rsync": "memory"})
        # only the pull needs the private sshd
        self.assertEqual([scenario for scenario in matrix.REFERENCES if matrix.TOPOLOGY[scenario] == "loopback"],
                         ["remote-rsync"])

    def test_each_tool_copies_the_file_set_as_documented_after_rcp_has(self):
        seen, patched = self.commands()
        with patched:
            record = self.run_matrix()
        self.assertEqual(record["status"], "complete")
        self.assertEqual(self.outcomes(record), [("alpha", "local", "complete"), ("alpha", "local-cp", "complete"),
                                                 ("alpha", "local-rsync", "complete")])
        fixture = record["file_sets"][0]["generation"]["command"][1] + "/filegen"
        scratch = Path(seen[0][-1]).parents[2]
        self.assertEqual([command[:1] + command[-2:] for command in seen[:2]],
                         [[str(self.bin / "rcp"), fixture, f"{scratch}/alpha/local/{copy}"] for copy in (1, 2)])
        self.assertEqual(seen[2:], [
            *([str(self.tools["cp"]), "-a", fixture, f"{scratch}/alpha/local-cp/{copy}"] for copy in (1, 2)),
            *([str(self.tools["rsync"]), "-a", fixture + "/", f"{scratch}/alpha/local-rsync/{copy}/"]
              for copy in (1, 2))])
        for scenario, variant in (("local-cp", "cp-a"), ("local-rsync", "rsync-a")):
            cell = self.cell(record, scenario)
            self.assertEqual((cell["variant"]["id"], cell["variant"]["tool"], cell["variant"]["args"]),
                             (variant, scenario.partition("-")[2], ["-a"]))
            self.assertEqual([(item["iteration"], item["status"], item["exit_codes"], item["timed_out"])
                              for item in cell["copies"]], [(1, "ok", [0], False), (2, "ok", [0], False)])
            samples = [item["elapsed_seconds"] for item in cell["copies"]]
            self.assertEqual(cell["summary"], dict(unit="seconds", median=sum(samples) / 2, minimum=min(samples),
                                                   maximum=max(samples), samples=samples))
            # neither tool says what it copied or what it used
            for item in cell["copies"]:
                self.assertTrue({"copy_summary", "resources", "tool_summary", "role_resources"}.isdisjoint(item))
        self.assertEqual(len({cell["cell_id"] for cell in record["cells"]}), 3)
        # every copy is removed once it has been timed
        self.assertEqual(list((self.root / "destination").iterdir()), [])
        self.assertEqual(list((self.root / "source").iterdir()), [])

    def test_the_tools_of_the_system_are_recorded_as_the_builds_are(self):
        record = self.run_matrix()
        self.assertEqual(set(record["tools"]), {"filegen", "rcp", "rcpd", "cp", "rsync"})
        for name in ("cp", "rsync"):
            tool = record["tools"][name]
            self.assertEqual((tool["path"], tool["version"]), (str(self.tools[name]), name + " (stand-in) 1"))
            self.assertRegex(tool["sha256"], "^[0-9a-f]{64}$")

    def test_only_the_selected_tools_have_to_exist(self):
        self.tools["rsync"].unlink()
        record = self.run_matrix(cells=("alpha:local", "alpha:local-cp"))
        self.assertEqual(record["status"], "complete")
        self.assertEqual(set(record["tools"]), {"filegen", "rcp", "rcpd", "cp"})

    def test_a_selected_tool_that_is_missing_ends_the_run_before_any_work(self):
        self.tools["rsync"].unlink()
        with self.assertRaisesRegex(ValueError, "not executable"):
            self.run_matrix()
        record = json.loads((self.root / "out/results.json").read_text())
        self.assertEqual(record["status"], "failed")
        self.assertEqual([item["generation"]["status"] for item in record["file_sets"]], ["not_run"])
        self.assertEqual(list((self.root / "source").iterdir()), [])

    def test_a_tool_that_is_not_named_is_the_one_on_the_search_path(self):
        arguments = self.arguments(CELLS)
        for name in ("--cp", "--rsync"):
            del arguments[arguments.index(name):arguments.index(name) + 2]
        path = os.pathsep.join((str(self.root / "system"), os.environ.get("PATH", os.defpath)))
        with mock.patch.dict(os.environ, {"PATH": path}), contextlib.redirect_stdout(io.StringIO()):
            record = matrix.main(arguments)
        self.assertEqual(record["status"], "complete")
        self.assertEqual({name: record["tools"][name]["path"] for name in ("cp", "rsync")},
                         {name: str(self.tools[name]) for name in ("cp", "rsync")})

    def test_a_tool_that_is_nowhere_on_the_search_path_ends_the_run_before_any_work(self):
        arguments = self.arguments(CELLS)
        del arguments[arguments.index("--rsync"):arguments.index("--rsync") + 2]
        with mock.patch.object(matrix.shutil, "which", return_value=None), \
                contextlib.redirect_stdout(io.StringIO()), \
                self.assertRaisesRegex(ValueError, "rsync is not on the search path; name it with --rsync"):
            matrix.main(arguments)
        self.assertEqual(list((self.root / "source").iterdir()), [])

    def test_a_copy_gets_ten_times_the_median_of_rcps_copies_unless_told_otherwise(self):
        # rcp is slow enough here for ten times its median to be seconds
        record = self.run_matrix(limit=None, environment={"FAKE_RCP": "alpha=sleep0.5"})
        self.assertEqual(matrix.REFERENCE_LIMIT, 10)
        self.assertEqual(record["context"]["reference_limit"], 10)
        median = self.cell(record, "local")["summary"]["median"]
        self.assertEqual([self.cell(record, scenario)["limit_seconds"] for scenario in ("local-cp", "local-rsync")],
                         [10 * median] * 2)
        self.assertNotIn("limit_seconds", self.cell(record, "local"))
        record = self.run_matrix(limit="250", output="told")
        self.assertEqual(record["context"]["reference_limit"], 250)
        self.assertEqual(self.cell(record, "local-cp")["limit_seconds"],
                         250 * self.cell(record, "local")["summary"]["median"])

    def test_a_run_without_reference_cells_says_nothing_of_a_limit(self):
        record = self.run_matrix(cells=("alpha:local",))
        self.assertNotIn("reference_limit", record["context"])
        self.assertEqual(set(record["tools"]), {"filegen", "rcp", "rcpd"})

    def test_a_copy_that_takes_longer_is_cut_off_and_not_repeated(self):
        started = time.monotonic()
        seen, patched = self.commands()
        with patched:
            # rcp is slow enough for three times its median to be seconds: the copies that are to
            # finish take a tenth of one, on a machine that is not busy
            record = self.run_matrix(limit="3", cells=(*CELLS, "beta:local", "beta:local-cp"),
                                     environment={"FAKE_RCP": "alpha=sleep1,beta=sleep1", "FAKE_CP": "alpha=half"})
        self.assertLess(time.monotonic() - started, 40)
        self.assertEqual(self.outcomes(record), [
            ("alpha", "local", "complete"), ("alpha", "local-cp", "timed_out"), ("alpha", "local-rsync", "complete"),
            ("beta", "local", "complete"), ("beta", "local-cp", "complete")])
        self.assertEqual(record["status"], "partial")
        cut = self.cell(record, "local-cp")
        limit = 3 * self.cell(record, "local")["summary"]["median"]
        self.assertEqual(cut["limit_seconds"], limit)
        self.assertEqual((cut["outcome"]["stage"], cut["outcome"]["message"]),
                         ("copy", f"copy 1 did not finish within {limit:.1f} s, 3 times the median of the local cell"))
        self.assertIsNone(cut["summary"])
        (item,) = cut["copies"]
        self.assertEqual((item["status"], item["timed_out"]), ("timed_out", True))
        self.assertGreaterEqual(item["elapsed_seconds"], limit)
        self.assertEqual([Path(command[0]).name for command in seen],
                         ["rcp", "rcp", "cp", "rsync", "rsync", "rcp", "rcp", "cp", "cp"])
        # what the tool had copied when it was cut goes like every other copy
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_a_copy_may_take_longer_than_the_limit_that_catches_hangs(self):
        record = self.run_matrix("--timeout", "3", limit="50", cells=CELLS[:2],
                                 environment={"FAKE_RCP": "alpha=sleep0.3", "FAKE_CP": "alpha=sleep4@1"})
        self.assertEqual(self.outcomes(record), [("alpha", "local", "complete"), ("alpha", "local-cp", "complete")])
        self.assertGreater(self.cell(record, "local-cp")["copies"][0]["elapsed_seconds"], 3)

    def test_a_copy_cut_by_the_end_of_the_allocation_is_not_one_that_took_too_long(self):
        deadline = time.monotonic_ns() + 8 * 10 ** 9
        with mock.patch.object(matrix, "DEADLINE_MARGIN", 1), mock.patch.object(matrix, "MINIMUM_COMMAND", .2):
            record = self.run_matrix("--timeout", "600", cells=(*CELLS, "beta:local"), environment={
                "FAKE_CP": "alpha=sleep60", cache.WORKER_DEADLINE_ENV: str(deadline)})
        self.assertEqual(self.outcomes(record), [
            ("alpha", "local", "complete"), ("alpha", "local-cp", "interrupted"), ("alpha", "local-rsync", "not_run"),
            ("beta", "local", "not_run")])
        cut = self.cell(record, "local-cp")
        self.assertEqual((cut["outcome"]["stage"], cut["outcome"]["message"]),
                         ("copy", "allocation ended during copy 1 of 2"))
        self.assertEqual([item["status"] for item in cut["copies"]], ["interrupted"])

    def test_the_allocation_ending_after_the_cache_step_leaves_the_copy_unstarted(self):
        calls = []
        def limit(self, timeout):
            calls.append(timeout)
            # the limit of the first reference copy is the first one asked for that is not the hang limit
            return None if timeout != 600 else (timeout, False)
        seen, patched = self.commands()
        with patched, mock.patch.object(matrix.Allocation, "limit", limit):
            record = self.run_matrix("--timeout", "600", cells=(*CELLS, "beta:local"))
        self.assertEqual(self.outcomes(record), [
            ("alpha", "local", "complete"), ("alpha", "local-cp", "interrupted"), ("alpha", "local-rsync", "not_run"),
            ("beta", "local", "not_run")])
        cut = self.cell(record, "local-cp")
        self.assertEqual((cut["outcome"]["stage"], cut["outcome"]["message"]),
                         ("copy", "allocation ended before copy 1 of 2"))
        self.assertEqual([item["status"] for item in cut["copies"]], ["interrupted"])
        self.assertEqual([Path(command[0]).name for command in seen], ["rcp", "rcp"])
        self.assertEqual(calls[-1], cut["limit_seconds"])

    def test_a_tool_that_fails_fails_only_its_cell(self):
        record = self.run_matrix(environment={"FAKE_CP": "alpha=exit@2"})
        self.assertEqual(self.outcomes(record), [("alpha", "local", "complete"), ("alpha", "local-cp", "failed"),
                                                 ("alpha", "local-rsync", "complete")])
        failed = self.cell(record, "local-cp")
        self.assertEqual((failed["outcome"]["stage"], failed["outcome"]["message"]), (
            "copy", "copy 2 failed with exit code 23; see 0.stdout.log and 0.stderr.log in logs/alpha/local-cp/2"))
        self.assertEqual([item["status"] for item in failed["copies"]], ["ok", "failed"])
        self.assertIsNone(failed["summary"])
        self.assertIn("some files could not be transferred",
                      (self.root / "out/logs/alpha/local-cp/2/0.stderr.log").read_text())
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_a_tool_that_cannot_be_started_fails_only_its_cell(self):
        self.tools["cp"].write_text("#!/nonexistent/interpreter\n")
        with mock.patch.object(run, "_tool", side_effect=lambda path: dict(path=str(path), version="1", sha256="0" * 64)):
            record = self.run_matrix()
        self.assertEqual(self.outcomes(record), [("alpha", "local", "complete"), ("alpha", "local-cp", "failed"),
                                                 ("alpha", "local-rsync", "complete")])
        self.assertRegex(self.cell(record, "local-cp")["outcome"]["message"], "^copy 1 could not be started: ")

    def test_cells_whose_rcp_cell_did_not_complete_have_no_limit_and_are_not_measured(self):
        seen, patched = self.commands()
        with patched:
            record = self.run_matrix(cells=(*CELLS, "beta:local", "beta:local-cp"), environment={"FAKE_RCP": "alpha=exit"})
        self.assertEqual(self.outcomes(record), [
            ("alpha", "local", "failed"), ("alpha", "local-cp", "failed"), ("alpha", "local-rsync", "failed"),
            ("beta", "local", "complete"), ("beta", "local-cp", "complete")])
        for scenario in ("local-cp", "local-rsync"):
            cell = self.cell(record, scenario)
            self.assertEqual((cell["outcome"]["stage"], cell["outcome"]["message"]), (
                "copy", "the local cell whose copies set this cell's time limit did not complete: copy 1 failed "
                        "with exit code 3; see 0.stdout.log and 0.stderr.log in logs/alpha/local/1"))
            self.assertEqual(cell["copies"], [])
            self.assertNotIn("limit_seconds", cell)
        self.assertEqual([Path(command[0]).name for command in seen], ["rcp", "rcp", "rcp", "cp", "cp"])

    def test_cells_whose_rcp_cell_hung_are_settled_as_it_was(self):
        record = self.run_matrix("--timeout", "2", cells=CELLS[:2], environment={"FAKE_RCP": "alpha=sleep60"})
        self.assertEqual(self.outcomes(record), [("alpha", "local", "timed_out"), ("alpha", "local-cp", "timed_out")])
        self.assertEqual(self.cell(record, "local-cp")["outcome"]["message"],
                         "the local cell whose copies set this cell's time limit did not complete: copy 1 did not "
                         "finish within 2 s")

    def test_the_allocation_ending_in_the_rcp_cell_leaves_the_reference_cells_not_run(self):
        deadline = time.monotonic_ns() + 8 * 10 ** 9
        with mock.patch.object(matrix, "DEADLINE_MARGIN", 1), mock.patch.object(matrix, "MINIMUM_COMMAND", .2):
            record = self.run_matrix("--timeout", "600", environment={
                "FAKE_RCP": "alpha=sleep60", cache.WORKER_DEADLINE_ENV: str(deadline)})
        self.assertEqual(self.outcomes(record), [("alpha", "local", "interrupted"), ("alpha", "local-cp", "not_run"),
                                                 ("alpha", "local-rsync", "not_run")])
        self.assertEqual(self.cell(record, "local-cp")["outcome"]["message"], "allocation ended before this cell")

    def test_a_file_set_that_cannot_be_generated_settles_its_reference_cells_with_it(self):
        record = self.run_matrix(environment={"FAKE_FILEGEN_FAIL": "alpha"})
        self.assertEqual({status for _, _, status in self.outcomes(record)}, {"failed"})
        self.assertEqual({cell["outcome"]["stage"] for cell in record["cells"]}, {"generation"})

    def test_a_reference_cell_needs_the_copy_cell_beside_it_before_it(self):
        for cells, copies in ((("alpha:local-cp",), "local"), (("alpha:local-rsync", "beta:local"), "local"),
                              (("alpha:local", "alpha:remote-rsync"), "remote"),
                              (("alpha:local-cp", "alpha:local"), "local"),
                              (("alpha:local", "beta:local-cp"), "local")):
            with self.subTest(cells=cells), self.assertRaisesRegex(
                    ValueError, f"a reference cell needs the {copies} cell of its file set before it"):
                self.run_matrix(cells=cells)
            self.assertFalse((self.root / "out").exists())

    def test_cp_does_not_copy_from_a_host(self):
        with self.assertRaisesRegex(ValueError, "unknown scenario: remote-cp"):
            self.run_matrix(cells=("alpha:remote", "alpha:remote-cp"))

    def test_a_limit_that_is_not_a_positive_number_is_refused_before_any_work(self):
        for limit in ("0", "-1", "nan", "inf"):
            with self.subTest(limit=limit), self.assertRaisesRegex(ValueError, "reference limit"):
                self.run_matrix(limit=limit)
            self.assertFalse((self.root / "out").exists())

    def test_the_tools_copy_beside_every_kind_of_copy_where_its_trees_are(self):
        events, execute = [], run.execute_commands
        def command(commands, *args, **kwargs):
            events.append(list(map(str, commands[0])))
            return execute(commands, *args, **kwargs)
        cells = ("alpha:nfs", "alpha:nfs-cp", "alpha:nfs-rsync", "beta:memory", "beta:memory-cp", "beta:memory-rsync")
        with mock.patch.object(run, "execute_commands", side_effect=command), \
             mock.patch.object(cache, "prepare", side_effect=lambda *args, **kwargs: events.append("cache")):
            record = self.run_matrix(*self.roots("nfs", "memory"), cells=cells, cache_policy="linux-drop-caches")
        self.assertEqual(record["status"], "complete")
        # a cache is dropped before each copy through a mount, by another tool as by rcp, and none in memory
        self.assertEqual(["cache" if event == "cache" else Path(event[0]).name for event in events],
                         ["cache", "rcp", "cache", "rcp", "cache", "cp", "cache", "cp", "cache", "rsync", "cache", "rsync",
                          "rcp", "rcp", "cp", "cp", "rsync", "rsync"])
        copies = [event for event in events if event != "cache"]
        def below(path, root):
            return Path(os.path.commonpath([path.rstrip("/"), self.root / root])) == self.root / root
        # what is measured goes through the mounts; the trees in memory are in the roots there
        for event in copies[:6]:
            self.assertTrue(below(event[-2], "nfs-source") and below(event[-1], "nfs-destination"), event)
        for event in copies[6:]:
            self.assertTrue(below(event[-2], "memory-source") and below(event[-1], "memory-destination"), event)
        for side in ("source", "destination", "memory-source", "memory-destination"):
            self.assertEqual(list((self.root / side).iterdir()), [], side)

    def test_reference_cells_stand_beside_tool_cells_and_the_runs_rrm_clears_their_copies(self):
        removed, launch = [], subprocess.run
        def note(command, *args, **kwargs):
            if Path(command[0]).name in ("rm", "rrm") and command[-1].startswith(str(self.root)):
                removed.append((Path(command[0]).name, "/".join(Path(command[-1]).relative_to(self.root).parts[2:])))
            return launch(command, *args, **kwargs)
        with mock.patch.object(matrix.subprocess, "run", side_effect=note):
            record = self.run_matrix(cells=("alpha:local", "alpha:local-rrm", "alpha:local-cp"))
        self.assertEqual(record["status"], "complete")
        self.assertEqual(record["context"]["removal"], "rrm")
        self.assertEqual([item for item in removed if "local-cp" in item[1]],
                         [("rrm", "alpha/local-cp/1"), ("rrm", "alpha/local-cp/2")])

    def test_system_telemetry_gives_every_reference_copy_the_windows_of_a_copy(self):
        record = self.run_matrix("--system-telemetry")
        phases = {cell["scenario"]: [[phase["phase"] for phase in item["system_telemetry"]["phases"]]
                                     for item in cell["copies"]] for cell in record["cells"]}
        self.assertEqual(phases, dict.fromkeys(("local", "local-cp", "local-rsync"),
                                               [["cache", "command", "cleanup"]] * 2))

    def test_the_summary_says_how_a_copy_by_another_tool_counts(self):
        self.run_matrix()
        text = (self.root / "out/summary.md").read_text()
        self.assertIn("A copy by cp or rsync counts on its exit status alone", text)
        self.assertIn("| alpha | local-cp | complete |", text)

    def test_validation_knows_the_reference_cells(self):
        record = self.run_matrix()
        index = [cell["scenario"] for cell in record["cells"]].index("local-cp")
        for change in (lambda value: value["cells"][index]["variant"].update(id="rcp-default"),
                       lambda value: value["cells"][0]["variant"].update(id="cp-a"),
                       lambda value: value["cells"][index]["variant"].update(id="rsync-a"),
                       lambda value: value["tools"].update(tar=value["tools"]["cp"]),
                       lambda value: value["context"].pop("reference_limit"),
                       lambda value: value["context"].update(reference_limit=0),
                       lambda value: value["context"].update(reference_limit=True),
                       lambda value: value["context"].update(reference_limit=float("inf")),
                       lambda value: value["cells"][index].update(limit_seconds=0),
                       lambda value: value["cells"][index].update(limit_seconds="1"),
                       lambda value: value["cells"][index].update(limit_seconds=2 * value["cells"][index]["limit_seconds"]),
                       # a cell that was measured was given a limit
                       lambda value: value["cells"][index].pop("limit_seconds"),
                       # and the cell whose copies set it comes first
                       lambda value: value["cells"].reverse(),
                       lambda value: value["cells"][0].update(limit_seconds=1.5),
                       lambda value: value["cells"][index]["copies"][0].update(exit_codes=[1])):
            broken = json.loads(json.dumps(record))
            change(broken)
            with self.subTest(change=change), self.assertRaises(matrix.Invalid):
                matrix.validate(broken)
        # a run whose reference cells were not measured still says what they would have been given
        unmeasured = self.run_matrix(environment={"FAKE_RCP": "alpha=exit"}, output="unmeasured")
        self.assertTrue(all("limit_seconds" not in cell for cell in unmeasured["cells"]))
        for change in (lambda value: value["context"].pop("reference_limit"),
                       lambda value: value["context"].update(reference_limit=0),
                       lambda value: value["context"].update(reference_limit="10")):
            broken = json.loads(json.dumps(unmeasured))
            change(broken)
            with self.subTest(change=change), self.assertRaises(matrix.Invalid):
                matrix.validate(broken)
        # a run without such cells does not name a limit
        record = self.run_matrix(cells=("alpha:local",), output="plain")
        with self.assertRaises(matrix.Invalid):
            matrix.validate(dict(record, context=dict(record["context"], reference_limit=10)))


class RemoteReferenceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / "state"
        self.state.mkdir()
        self.bin = self.root / "bin"
        tools(self.bin, filegen=FILEGEN, rcp=REMOTE_RCP, rcpd=RCPD)
        self.system = tools(self.root / "system", rsync=COPIER)
        self.tools = tools(self.root / "tools", sshd=SSHD, ssh=SSH, keygen=KEYGEN)
        self.manifest = self.root / "manifest.json"
        self.manifest.write_text(json.dumps(dict(schema_version=1, cases=[
            dict(id=name, directory_widths=[2], files_per_leaf=2, file_size_bytes=3) for name in ("alpha", "beta")],
            variants=[dict(id="rcp-default", tool="rcp", args=["--summary"], processes=1)])))
        for side in ("source", "destination"):
            (self.root / side).mkdir()
        environment = mock.patch.dict(os.environ, {"FAKE_STATE": str(self.state)})
        environment.start()
        self.addCleanup(environment.stop)

    def run_matrix(self, *extra, cells=("alpha:remote", "alpha:remote-rsync"), environment=None):
        arguments = ["--manifest", str(self.manifest), "--bin-dir", str(self.bin),
                     "--source-root", str(self.root / "source"), "--destination-root", str(self.root / "destination"),
                     "--cache", "uncontrolled", "--repetitions", "2", "--output", str(self.root / "out"),
                     "--sshd", str(self.tools["sshd"]), "--ssh", str(self.tools["ssh"]),
                     "--ssh-keygen", str(self.tools["keygen"]), "--rsync", str(self.system["rsync"]),
                     "--reference-limit", "1000"]
        for cell in cells:
            arguments += ["--cell", cell]
        with mock.patch.dict(os.environ, environment or {}), contextlib.redirect_stdout(io.StringIO()):
            record = matrix.main([*arguments, *extra])
        saved = json.loads((self.root / "out/results.json").read_text())
        self.assertEqual(saved, json.loads(json.dumps(record)))
        matrix.validate(saved)
        return saved

    def outcomes(self, record):
        return [(cell["case_id"], cell["scenario"], cell["outcome"]["status"]) for cell in record["cells"]]

    def test_rsync_pulls_the_file_set_from_the_private_endpoint(self):
        record = self.run_matrix()
        self.assertEqual(record["status"], "complete")
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "complete"), ("alpha", "remote-rsync", "complete")])
        fixture = record["file_sets"][0]["generation"]["command"][1] + "/filegen"
        rsync = str(self.system["rsync"])
        for item in record["cells"][1]["copies"]:
            command = item["command"]
            # the host's rsync is this one, wherever a session of the host would look for it
            self.assertEqual(command[:-1], [rsync, "-a", "--rsync-path=" + rsync, "127.0.0.1:" + fixture + "/"])
            self.assertRegex(command[-1], f"/alpha/remote-rsync/{item['iteration']}/$")
            self.assertNotIn("leftover_processes", item)
        # the tree was read through the endpoint's launcher: two copies by rcp with two daemons each,
        # and one listing for each copy by rsync
        calls = [json.loads(line) for line in (self.state / "ssh.calls").read_text().splitlines()]
        self.assertEqual(sum(call[-2:] == ["ls", fixture] for call in calls), 2)
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_a_pull_that_is_cut_off_leaves_no_process_of_the_host_behind(self):
        started = time.monotonic()
        record = self.run_matrix("--reference-limit", "3", environment={"FAKE_RSYNC": "alpha=orphan"})
        self.assertLess(time.monotonic() - started, 60)
        self.assertEqual(self.outcomes(record), [("alpha", "remote", "complete"), ("alpha", "remote-rsync", "timed_out")])
        self.assertEqual(record["cells"][1]["outcome"]["stage"], "copy")
        (cut,) = record["cells"][1]["copies"]
        self.assertGreaterEqual(cut["leftover_processes"], 1)
        # nothing sleeps on: what the session had started was ended with the copy
        deadline = time.monotonic() + 10
        def sleepers():
            found = []
            for entry in Path("/proc").iterdir():
                try:
                    if entry.name.isdecimal() and entry.joinpath("cmdline").read_bytes().split(b"\0")[:2] == [b"sleep", b"317"]:
                        found.append(entry.name)
                except OSError:
                    continue
            return found
        while sleepers() and time.monotonic() < deadline:
            time.sleep(.05)
        self.assertEqual(sleepers(), [])

    def test_reference_cells_beside_remote_copies_fail_with_them_when_the_endpoint_cannot_start(self):
        record = self.run_matrix(cells=("alpha:local", "alpha:remote", "alpha:remote-rsync"),
                                 environment={"FAKE_SSHD": "badconfig"})
        self.assertEqual(self.outcomes(record), [("alpha", "local", "complete"), ("alpha", "remote", "failed"),
                                                 ("alpha", "remote-rsync", "failed")])
        self.assertEqual({cell["outcome"]["stage"] for cell in record["cells"][1:]}, {"transport"})

    def test_a_suite_whose_only_remote_work_is_unreachable_generates_nothing(self):
        record = self.run_matrix(environment={"FAKE_SSHD": "badconfig"})
        self.assertEqual({status for _, _, status in self.outcomes(record)}, {"failed"})
        self.assertEqual([item["generation"]["status"] for item in record["file_sets"]], ["not_run"])


if __name__ == "__main__":
    unittest.main()
