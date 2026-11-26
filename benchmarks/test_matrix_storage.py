"""Cells on other storage: `nfs` copies between two mounts of the run's roots, `memory` between two roots of its own."""

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from benchmarks import cache, matrix, run
from benchmarks.test_matrix import FILEGEN, RCP
from benchmarks.test_matrix_tools import TOOL

NFS = ("alpha:nfs", "alpha:nfs-rcmp", "alpha:nfs-rlink", "alpha:nfs-rrm")
MEMORY = ("beta:memory", "beta:memory-rcmp", "beta:memory-rlink", "beta:memory-rrm")


class StorageTests(unittest.TestCase):
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
        self.manifest.write_text(json.dumps(dict(schema_version=1, cases=[
            dict(id=name, directory_widths=[2], files_per_leaf=2, file_size_bytes=3) for name in ("alpha", "beta")],
            variants=[dict(id="rcp-default", tool="rcp", args=["--summary"], processes=1)])))
        for side in ("source", "destination", "memory-source", "memory-destination"):
            (self.root / side).mkdir()
        # a mount shows the same tree under another path, as these links do
        for side in ("source", "destination"):
            (self.root / ("nfs-" + side)).symlink_to(self.root / side)

    def roots(self, *kinds):
        arguments = []
        for kind in kinds:
            arguments += [f"--{kind}-source-root", str(self.root / f"{kind}-source"),
                          f"--{kind}-destination-root", str(self.root / f"{kind}-destination"),
                          f"--{kind}-source-storage-id", f"{kind}-source-v1",
                          f"--{kind}-destination-storage-id", f"{kind}-destination-v1"]
        return arguments

    def arguments(self, cells, *extra, cache_policy="uncontrolled", output="out"):
        arguments = ["--manifest", str(self.manifest), "--bin-dir", str(self.bin),
                     "--source-root", str(self.root / "source"), "--destination-root", str(self.root / "destination"),
                     "--source-storage-id", "disk-source-v1", "--destination-storage-id", "disk-destination-v1",
                     "--cache", cache_policy, "--repetitions", "2", "--output", str(self.root / output)]
        for cell in cells:
            arguments += ["--cell", cell]
        return [*arguments, *extra]

    def run_matrix(self, cells, *extra, cache_policy="uncontrolled", output="out"):
        with contextlib.redirect_stdout(io.StringIO()):
            record = matrix.main(self.arguments(cells, *extra, cache_policy=cache_policy, output=output))
        saved = json.loads((self.root / output / "results.json").read_text())
        self.assertEqual(saved, json.loads(json.dumps(record)))
        matrix.validate(saved)
        return saved

    def cell(self, record, scenario, case):
        return next(cell for cell in record["cells"] if (cell["case_id"], cell["scenario"]) == (case, scenario))

    def launches(self):
        """Run the matrix while noting every program it starts outside a measurement: its name and arguments."""
        seen, launch = [], subprocess.run
        def note(command, *args, **kwargs):
            if "--version" not in command and "--output-format=json" not in command:
                seen.append([Path(command[0]).name, *map(str, command[1:])])
            return launch(command, *args, **kwargs)
        return seen, mock.patch.object(subprocess, "run", side_effect=note)

    def below(self, path, root):
        return Path(os.path.commonpath([path, self.root / root])) == self.root / root

    def test_nfs_rpc_observation_brackets_only_the_link_command_on_its_destination_mount(self):
        from benchmarks import nfs_rpc
        events = []
        execute, mount, prepare_root = run.execute_commands, run._mount, nfs_rpc.prepare_root
        def command(commands, *args, **kwargs):
            name = Path(commands[0][0]).name
            events.append(("start", name))
            outcome = execute(commands, *args, **kwargs)
            events.append(("end", name))
            return outcome
        def mounted(path):
            if Path(path).name == "nfs-destination":
                # this was the covering mount at setup, before the command's two snapshots
                return {"filesystem_type": "nfs4", "mountpoint": str(self.root)}
            return mount(path)
        def prepare(path):
            events.append(("root", str(path)))
            return prepare_root(path)
        def sample(path, *, resolve_after=False):
            events.append(("snapshot", str(path), resolve_after))
            return None
        with mock.patch.object(run, "execute_commands", side_effect=command), \
             mock.patch.object(run, "_mount", side_effect=mounted), \
             mock.patch.object(cache, "prepare", side_effect=lambda *args, **kwargs: events.append(("cache",))), \
             mock.patch.object(nfs_rpc, "prepare_root", side_effect=prepare), \
             mock.patch.object(nfs_rpc, "snapshot", side_effect=sample):
            record = self.run_matrix(NFS, *self.roots("nfs"), "--system-telemetry", cache_policy="linux-drop-caches")
        self.assertEqual(record["status"], "complete")
        self.assertEqual(sum(event[0] == "snapshot" for event in events), 4)
        configured, resolved = (str(self.root / name) for name in ("nfs-destination", "destination"))
        for index, event in enumerate(events):
            if event == ("start", "rlink"):
                self.assertEqual(events[index - 3:index + 3], [("root", configured), ("cache",),
                    ("snapshot", resolved, False), event, ("end", "rlink"), ("snapshot", configured, True)])
        for cell in record["cells"]:
            for item in cell["copies"]:
                if cell["scenario"] == "nfs-rlink":
                    self.assertEqual(item["nfs_rpc"], {"status": "unavailable"})
                else:
                    self.assertNotIn("nfs_rpc", item)

    def test_nfs_cells_measure_through_the_mounts(self):
        record = self.run_matrix(NFS, *self.roots("nfs"))
        self.assertEqual(record["status"], "complete")
        self.assertEqual([cell["outcome"]["status"] for cell in record["cells"]], ["complete"] * 4)
        for copy in self.cell(record, "nfs", "alpha")["copies"]:
            source, destination = copy["command"][-2:]
            self.assertTrue(self.below(source, "nfs-source"), source)
            self.assertTrue(self.below(destination, "nfs-destination"), destination)
        # every operand of a tool is a path through a mount: the source, the copy and the tree rlink makes
        for scenario, operands in (("nfs-rcmp", 2), ("nfs-rlink", 2), ("nfs-rrm", 1)):
            for item in self.cell(record, scenario, "alpha")["copies"]:
                for path in item["command"][-operands:]:
                    self.assertTrue(self.below(path, "nfs-source") or self.below(path, "nfs-destination"), path)
        self.assertEqual(self.cell(record, "nfs-rcmp", "alpha")["copies"][0]["tool_summary"],
                         {"mismatch.file.same": 4, "mismatch.dir.same": 3})

    def test_each_operand_goes_through_the_mount_of_its_own_side(self):
        # the roots of a run may overlap: here the destination root lies inside the source root, so
        # every destination path is below both
        inner = self.root / "source" / "inner"
        inner.mkdir()
        (self.root / "nfs-inner").symlink_to(inner)
        arguments = self.arguments(NFS, "--nfs-source-root", str(self.root / "nfs-source"),
                                   "--nfs-destination-root", str(self.root / "nfs-inner"))
        arguments[arguments.index("--destination-root") + 1] = str(inner)
        with contextlib.redirect_stdout(io.StringIO()):
            record = matrix.validate(matrix.main(arguments))
        self.assertEqual(record["status"], "complete")
        source, destination = str(self.root / "nfs-source") + os.sep, str(self.root / "nfs-inner") + os.sep
        sides = {"nfs": (source, destination), "nfs-rcmp": (source, destination),
                 "nfs-rlink": (destination, destination), "nfs-rrm": (destination,)}
        for scenario, expected in sides.items():
            for item in self.cell(record, scenario, "alpha")["copies"]:
                operands = item["command"][-len(expected):]
                self.assertEqual([path.startswith(mount) for path, mount in zip(operands, expected)],
                                 [True] * len(expected), (scenario, operands))

    def test_nfs_fixtures_and_unmeasured_removals_use_the_direct_paths(self):
        seen, patched = self.launches()
        with patched:
            self.run_matrix(NFS, *self.roots("nfs"))
        generated = [command for command in seen if command[0] == "filegen"]
        self.assertEqual(len(generated), 1)
        self.assertTrue(self.below(generated[0][1], "source"), generated[0])
        cleared = [command for command in seen if command[0] in ("rrm", "rm")]
        # what rlink made after each copy, the file set, and the two scratch directories of the run
        self.assertGreaterEqual(len(cleared), 3)
        for command in cleared:
            self.assertTrue(self.below(command[-1], "source") or self.below(command[-1], "destination"), command)
        self.assertEqual(list((self.root / "source").iterdir()), [])
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_nfs_cells_drop_caches_before_every_copy_and_every_tool(self):
        with mock.patch.object(cache, "prepare") as prepare:
            record = self.run_matrix((*NFS, "beta:local", "beta:local-rrm"), *self.roots("nfs"),
                                     cache_policy="linux-drop-caches")
        self.assertEqual(record["status"], "complete")
        # two copies and two runs of each of the three tools through the mounts; on the disks only the copies
        self.assertEqual(prepare.call_count, 2 + 3 * 2 + 2)
        for scenario in ("nfs", "nfs-rcmp", "nfs-rlink", "nfs-rrm"):
            for item in self.cell(record, scenario, "alpha")["copies"]:
                self.assertIn("cache_preparation", item, scenario)
                self.assertIn("preparation", item["phase_seconds"], scenario)
        for item in self.cell(record, "local-rrm", "beta")["copies"]:
            self.assertNotIn("cache_preparation", item)

    def failing_preparation(self, number, error):
        """Cache preparation that fails at its `number`-th call of the run and does nothing otherwise."""
        calls = []
        def prepare(*args, **kwargs):
            calls.append(None)
            if len(calls) == number:
                raise error
        return mock.patch.object(run, "_prepare_cache", side_effect=prepare)

    def test_a_cache_that_cannot_be_dropped_before_a_tool_costs_that_tool_cell_only(self):
        # the calls of a file set: copy 1, rcmp, rlink, rrm, copy 2, ...; the third is before rlink's first run
        seen, patched = self.launches()
        with patched, self.failing_preparation(3, OSError("cache broker unavailable")):
            record = self.run_matrix(NFS, *self.roots("nfs"), cache_policy="linux-drop-caches")
        self.assertEqual(record["status"], "partial")
        self.assertEqual({cell["scenario"]: cell["outcome"]["status"] for cell in record["cells"]},
                         {"nfs": "complete", "nfs-rcmp": "complete", "nfs-rlink": "failed", "nfs-rrm": "complete"})
        rlink = self.cell(record, "nfs-rlink", "alpha")
        self.assertEqual(rlink["outcome"]["stage"], "cache")
        self.assertEqual(rlink["outcome"]["message"],
                         "cache preparation before run 1 failed: cache broker unavailable")
        # the run that never started is recorded as such, and the cell is not tried after the second copy
        self.assertEqual([(item["status"], "elapsed_seconds" in item) for item in rlink["copies"]], [("failed", False)])
        # rrm did not fail: it keeps clearing the trees of the run
        self.assertNotIn("rrm_fallback", record)
        self.assertNotIn("rm", [command[0] for command in seen])
        self.assertEqual(list((self.root / "destination").iterdir()), [])

    def test_the_allocation_ending_in_a_tools_cache_preparation_ends_the_run_there(self):
        deadline = {cache.WORKER_DEADLINE_ENV: str(2 ** 62)}
        # asked before generation, before the copy's cache step, before the copy and before rcmp's cache step
        limits = iter([(5.0, True)] * 4)
        def limit(_self, _timeout):
            # the allocation is over when rcmp's cache step fails
            return next(limits, None)
        with mock.patch.dict(os.environ, deadline), mock.patch.object(matrix.Allocation, "limit", limit), \
                self.failing_preparation(2, TimeoutError("cache preparation deadline exceeded")):
            record = self.run_matrix(NFS, *self.roots("nfs"), cache_policy="linux-drop-caches")
        rcmp = self.cell(record, "nfs-rcmp", "alpha")
        self.assertEqual((rcmp["outcome"]["status"], rcmp["outcome"]["stage"]), ("interrupted", "cache"))
        self.assertEqual(rcmp["outcome"]["message"], "allocation ended during cache preparation before run 1 of 2")
        self.assertEqual([cell["outcome"]["status"] for cell in record["cells"]],
                         ["interrupted", "interrupted", "not_run", "not_run"])

    def test_memory_cells_use_their_own_roots_and_drop_no_cache(self):
        seen, patched = self.launches()
        with patched, mock.patch.object(cache, "prepare") as prepare:
            record = self.run_matrix(MEMORY, *self.roots("memory"), cache_policy="linux-drop-caches")
        self.assertEqual(record["status"], "complete")
        self.assertEqual(prepare.call_count, 0)
        generated = [command for command in seen if command[0] == "filegen"]
        self.assertEqual(len(generated), 1)
        self.assertTrue(self.below(generated[0][1], "memory-source"), generated[0])
        for copy in self.cell(record, "memory", "beta")["copies"]:
            source, destination = copy["command"][-2:]
            self.assertTrue(self.below(source, "memory-source"), source)
            self.assertTrue(self.below(destination, "memory-destination"), destination)
            self.assertNotIn("cache_preparation", copy)
        for side in ("source", "destination", "memory-source", "memory-destination"):
            self.assertEqual(list((self.root / side).iterdir()), [], side)

    def test_a_run_may_hold_cells_of_every_storage(self):
        record = self.run_matrix(("alpha:local", "alpha:local-rrm", *NFS, *MEMORY), *self.roots("nfs", "memory"))
        self.assertEqual(record["status"], "complete")
        self.assertEqual(len(record["cells"]), 10)
        self.assertEqual(sorted(record["context"]["storages"]), ["memory", "nfs"])
        nfs = record["context"]["storages"]["nfs"]
        self.assertEqual(nfs["storage_ids"], dict(source="nfs-source-v1", destination="nfs-destination-v1"))
        self.assertEqual(nfs["source"], str(self.root / "nfs-source"))
        self.assertIn("filesystem", nfs)

    def missing(self, kind, why):
        """Arguments that name a storage and say why this run has no roots on it."""
        return [f"--{kind}-unavailable", why, f"--{kind}-source-storage-id", f"{kind}-source-v1",
                f"--{kind}-destination-storage-id", f"{kind}-destination-v1"]

    def test_storage_that_could_not_be_prepared_costs_its_cells_only(self):
        seen, patched = self.launches()
        with patched:
            record = self.run_matrix(("alpha:local", "alpha:local-rrm", *NFS, *MEMORY),
                                     *self.missing("nfs", "the NFS server did not start"), *self.roots("memory"))
        self.assertEqual(record["status"], "partial")
        for case, scenario in (("alpha", "local"), ("alpha", "local-rrm"), *(cell.split(":") for cell in MEMORY)):
            self.assertEqual(self.cell(record, scenario, case)["outcome"]["status"], "complete", scenario)
        copy = self.cell(record, "nfs", "alpha")
        self.assertEqual(copy["outcome"], dict(status="failed", stage="storage", message="the NFS server did not start"))
        self.assertEqual(copy["copies"], [])
        for scenario in ("nfs-rcmp", "nfs-rlink", "nfs-rrm"):
            outcome = self.cell(record, scenario, "alpha")["outcome"]
            self.assertEqual((outcome["status"], outcome["stage"]), ("failed", "copy"))
            self.assertIn("the NFS server did not start", outcome["message"])
        # what the storage is stays on record, and that it was missing: a cell is identified by it
        self.assertEqual(record["context"]["storages"]["nfs"],
                         dict(unavailable="the NFS server did not start",
                              storage_ids=dict(source="nfs-source-v1", destination="nfs-destination-v1")))
        self.assertFalse(any(self.below(argument, "nfs-source") or self.below(argument, "nfs-destination")
                             for command in seen for argument in command[1:] if argument.startswith("/")))

    def test_a_file_set_whose_storage_is_missing_is_not_generated(self):
        seen, patched = self.launches()
        with patched:
            record = self.run_matrix(MEMORY, *self.missing("memory", "no room for the roots in memory"))
        self.assertEqual(record["status"], "failed")
        self.assertEqual([command for command in seen if command[0] == "filegen"], [])
        self.assertEqual(record["file_sets"][0]["generation"], dict(status="not_run"))
        self.assertEqual([cell["outcome"]["stage"] for cell in record["cells"]], ["storage", "copy", "copy", "copy"])
        self.assertEqual({cell["outcome"]["status"] for cell in record["cells"]}, {"failed"})

    def test_a_cell_keeps_its_identity_when_its_storage_is_missing(self):
        # the failure belongs on the line of the cell that would have been measured
        usual = self.run_matrix(NFS, *self.roots("nfs"))
        without = self.run_matrix(NFS, *self.missing("nfs", "why"), output="again")
        self.assertEqual([cell["cell_id"] for cell in without["cells"]], [cell["cell_id"] for cell in usual["cells"]])

    def test_missing_storage_has_no_roots(self):
        for extra, expected in ((self.roots("nfs"), "--nfs-unavailable"), (self.roots("nfs")[:2], "--nfs-unavailable")):
            with self.assertRaisesRegex(ValueError, expected):
                self.run_matrix(NFS, *extra, "--nfs-unavailable", "why")
            self.assertFalse((self.root / "out").exists())
        with self.assertRaisesRegex(ValueError, "--nfs-unavailable"):
            self.run_matrix(NFS, *self.missing("nfs", ""))

    def test_a_result_with_missing_storage_holds_no_measurement_there(self):
        record = self.run_matrix(NFS, *self.missing("nfs", "why"))
        matrix.validate(record)
        measured = self.run_matrix(NFS, *self.roots("nfs"), output="again")
        for change in (lambda storage, cells: storage.update(source="/somewhere"),
                       lambda storage, cells: storage.update(unavailable=""),
                       lambda storage, cells: storage.update(unavailable=3),
                       lambda storage, cells: storage.update(filesystem={}),
                       lambda storage, cells: cells.__setitem__(slice(None), measured["cells"])):
            changed = json.loads(json.dumps(record))
            change(changed["context"]["storages"]["nfs"], changed["cells"])
            with self.assertRaises(matrix.Invalid):
                matrix.validate(changed)

    def test_a_cell_without_its_roots_is_refused_before_any_work(self):
        for cells, roots, expected in ((NFS, (), "nfs"), (MEMORY, ("nfs",), "memory")):
            with self.assertRaisesRegex(ValueError, f"--{expected}-source-root"):
                self.run_matrix(cells, *self.roots(*roots))
            self.assertFalse((self.root / "out").exists())

    def test_a_file_set_is_generated_on_one_storage(self):
        with self.assertRaisesRegex(ValueError, "alpha"):
            self.run_matrix(("alpha:local", "alpha:memory"), *self.roots("memory"))
        self.assertFalse((self.root / "out").exists())

    def test_a_tool_cell_needs_the_copy_cell_of_its_own_storage(self):
        with self.assertRaisesRegex(ValueError, "needs the nfs cell"):
            self.run_matrix(("alpha:local", "alpha:nfs-rrm"), *self.roots("nfs"))

    def test_cells_on_the_disks_keep_their_identity(self):
        # the trend of a cell is keyed by this digest: a change here would end every line of the history
        case = dict(id="alpha", directory_widths=[2], files_per_leaf=2, file_size_bytes=3)
        context = dict(cache_policy="linux-drop-caches", verification=dict(policy=matrix.POLICY), runner_label="label",
                       storage_ids=dict(source="disk-source-v1", destination="disk-destination-v1"))
        variant = dict(id="rcp-default", tool="rcp", args=["--summary"], processes=1)
        self.assertEqual(matrix.cell_id(case, "local", variant, context),
                         "05f5229c1ad8dafb18021f9bf8dd975fd0cc6b6fd691e8ac9fefe713d22eb6ff")
        with_storages = dict(context, storages=dict(nfs=dict(storage_ids=dict(source="a", destination="b"))))
        self.assertEqual(matrix.cell_id(case, "local", variant, with_storages),
                         matrix.cell_id(case, "local", variant, context))

    def test_a_cell_on_other_storage_is_identified_by_that_storage(self):
        case = dict(id="alpha", directory_widths=[2], files_per_leaf=2, file_size_bytes=3)
        variant = dict(id="rcp-default", tool="rcp", args=["--summary"], processes=1)
        def context(nfs):
            return dict(cache_policy="linux-drop-caches", verification=dict(policy=matrix.POLICY), runner_label="label",
                        storage_ids=dict(source="disk-source-v1", destination="disk-destination-v1"),
                        storages=dict(nfs=dict(storage_ids=dict(source=nfs, destination="nfs-destination-v1"))))
        first = matrix.cell_id(case, "nfs", variant, context("nfs-5ms-v1"))
        self.assertNotEqual(first, matrix.cell_id(case, "local", variant, context("nfs-5ms-v1")))
        # another delay or another NFS version is another storage, and starts another line
        self.assertNotEqual(first, matrix.cell_id(case, "nfs", variant, context("nfs-10ms-v1")))
        self.assertEqual(first, matrix.cell_id(case, "nfs", variant, context("nfs-5ms-v1")))

    def test_a_result_names_only_the_storage_kinds_the_runner_knows(self):
        record = self.run_matrix(NFS, *self.roots("nfs"))
        for broken in (dict(tape=record["context"]["storages"]["nfs"]),
                       dict(nfs=dict(record["context"]["storages"]["nfs"], storage_ids=dict(source=1, destination="b"))),
                       dict(nfs=dict(record["context"]["storages"]["nfs"], source=3)), []):
            changed = json.loads(json.dumps(record))
            changed["context"]["storages"] = broken
            with self.assertRaises(matrix.Invalid):
                matrix.validate(changed)


if __name__ == "__main__":
    unittest.main()
